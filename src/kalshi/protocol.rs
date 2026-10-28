//! Kalshi websocket messages. Only the fields we use are modelled. Prices and
//! sizes arrive as decimal strings and are parsed straight to fixed point.

use std::fmt;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};
use serde_json::json;
use serde_json::value::RawValue;

use crate::book::{Level, SCALE, parse_fixed};

/// One cent in fixed point, for the legacy integer-cent fields.
const CENT: i64 = SCALE / 100;

/// The outer shape of every frame. `seq` is contiguous per subscription (`sid`)
/// and counts every frame on it, `ok` replies included, so gap detection
/// works on the envelope rather than on specific message types.
#[derive(Deserialize)]
pub struct Envelope<'a> {
    #[serde(rename = "type")]
    pub kind: &'a str,
    pub id: Option<u64>,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    pub msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
pub struct Snapshot {
    pub market_ticker: String,
    yes_dollars_fp: Option<Vec<(Fixed, Fixed)>>,
    no_dollars_fp: Option<Vec<(Fixed, Fixed)>>,
    yes: Option<Vec<(i64, i64)>>,
    no: Option<Vec<(i64, i64)>>,
}

impl Snapshot {
    /// Kalshi only has bids: YES bids, and NO bids. A NO bid at `p` is an
    /// offer to sell YES at `1 - p`, so NO bids become our asks.
    pub fn levels(&self) -> (Vec<Level>, Vec<Level>) {
        let bids = to_levels(self.yes_dollars_fp.as_deref(), self.yes.as_deref());
        let asks = to_levels(self.no_dollars_fp.as_deref(), self.no.as_deref())
            .into_iter()
            .map(|(price, qty)| (SCALE - price, qty))
            .collect();
        (bids, asks)
    }
}

fn to_levels(fixed: Option<&[(Fixed, Fixed)]>, cents: Option<&[(i64, i64)]>) -> Vec<Level> {
    match fixed {
        Some(levels) => levels.iter().map(|&(price, qty)| (price.0, qty.0)).collect(),
        None => cents
            .unwrap_or_default()
            .iter()
            .map(|&(price, qty)| (price * CENT, qty * SCALE))
            .collect(),
    }
}

#[derive(Deserialize)]
pub struct Delta {
    pub market_ticker: String,
    side: BookSide,
    price_dollars: Option<Fixed>,
    price: Option<i64>,
    delta_fp: Option<Fixed>,
    delta: Option<Fixed>,
    ts_ms: Option<i64>,
}

impl Delta {
    /// `(side, price, quantity change)` in our terms, see [`Snapshot::levels`].
    pub fn change(&self) -> Option<(Side, i64, i64)> {
        let price = match self.price_dollars {
            Some(price) => price.0,
            None => self.price? * CENT,
        };
        let delta = self.delta_fp.or(self.delta)?.0;
        Some(match self.side {
            BookSide::Yes => (Side::Buy, price, delta),
            BookSide::No => (Side::Sell, SCALE - price, delta),
        })
    }

    pub fn exchange_time(&self) -> Option<DateTime<Utc>> {
        self.ts_ms.and_then(DateTime::from_timestamp_millis)
    }
}

#[derive(Deserialize)]
pub struct ErrorMsg {
    pub code: Option<i64>,
    pub msg: Option<String>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum BookSide {
    Yes,
    No,
}

/// A fixed-point number that Kalshi may send as a decimal string (`"0.45"`)
/// or, in older fields, as a whole number of contracts.
#[derive(Clone, Copy)]
struct Fixed(i64);

impl<'de> Deserialize<'de> for Fixed {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FixedVisitor;

        impl Visitor<'_> for FixedVisitor {
            type Value = Fixed;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a decimal string or an integer")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Fixed, E> {
                parse_fixed(s).map(Fixed).ok_or_else(|| E::custom(format!("bad decimal {s:?}")))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Fixed, E> {
                v.checked_mul(SCALE).map(Fixed).ok_or_else(|| E::custom("number out of range"))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Fixed, E> {
                i64::try_from(v)
                    .ok()
                    .and_then(|v| v.checked_mul(SCALE))
                    .map(Fixed)
                    .ok_or_else(|| E::custom("number out of range"))
            }
        }

        deserializer.deserialize_any(FixedVisitor)
    }
}

pub fn subscribe(id: u64, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "subscribe",
        "params": { "channels": ["orderbook_delta"], "market_tickers": tickers },
    })
    .to_string()
}

pub fn delete_markets(id: u64, sid: u64, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sids": [sid], "market_tickers": tickers, "action": "delete_markets" },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_snapshot_and_maps_no_to_asks() {
        let raw = r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00","clock":4182057,
            "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],"no_dollars_fp":[["0.5400","20.00"]]}}"#;
        let env: Envelope = serde_json::from_str(raw).unwrap();
        assert_eq!((env.kind, env.sid, env.seq), ("orderbook_snapshot", Some(2), Some(2)));
        let snap: Snapshot = serde_json::from_str(env.msg.unwrap().get()).unwrap();
        let (bids, asks) = snap.levels();
        assert_eq!(bids, [(80_000, 300_000_000), (220_000, 333_000_000)]);
        assert_eq!(asks, [(460_000, 20_000_000)]);
    }

    #[test]
    fn parses_delta() {
        let raw = r#"{"market_ticker":"X","price_dollars":"0.960","delta_fp":"-54.00","side":"no","ts_ms":1669149841000}"#;
        let delta: Delta = serde_json::from_str(raw).unwrap();
        assert_eq!(delta.change(), Some((Side::Sell, 40_000, -54_000_000)));
        assert!(delta.exchange_time().is_some());
    }

    #[test]
    fn falls_back_to_legacy_cent_fields() {
        let raw = r#"{"market_ticker":"X","price":42,"delta":3,"side":"yes"}"#;
        let delta: Delta = serde_json::from_str(raw).unwrap();
        assert_eq!(delta.change(), Some((Side::Buy, 420_000, 3_000_000)));
    }
}
