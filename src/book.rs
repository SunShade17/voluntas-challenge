//! A price-level order book, plus the fixed-point helpers used everywhere else.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// Fixed-point scale used by the output schema: 1_000_000 = $1, or 1 contract.
pub const SCALE: i64 = 1_000_000;

/// `(price, quantity)`, both in fixed point.
pub type Level = (i64, i64);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Book {
    pub fn from_levels(bids: &[Level], asks: &[Level]) -> Self {
        let mut book = Self::default();
        for &(price, qty) in bids {
            *book.bids.entry(price).or_default() += qty;
        }
        for &(price, qty) in asks {
            *book.asks.entry(price).or_default() += qty;
        }
        book.bids.retain(|_, qty| *qty > 0);
        book.asks.retain(|_, qty| *qty > 0);
        book
    }

    /// Applies a relative change and returns the level's new absolute quantity.
    ///
    /// Returns `None` if the change would take the level below zero, which
    /// means this copy of the book has drifted from the exchange's.
    pub fn apply(&mut self, side: Side, price: i64, delta: i64) -> Option<i64> {
        let levels = self.levels_mut(side);
        let qty = levels.get(&price).copied().unwrap_or(0) + delta;
        if qty > 0 {
            levels.insert(price, qty);
        } else {
            levels.remove(&price);
        }
        (qty >= 0).then_some(qty)
    }

    pub fn to_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            // Best price first on both sides.
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}

/// Parses a decimal string such as `"0.4500"` or `"-12.00"` into fixed point,
/// without a round trip through `f64`.
pub fn parse_fixed(s: &str) -> Option<i64> {
    let s = s.trim();
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    // Extra precision is only acceptable if it is trailing zeros.
    let frac = if frac.len() > 6 { frac.trim_end_matches('0') } else { frac };
    let all_digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if (whole.is_empty() && frac.is_empty()) || frac.len() > 6 || !all_digits(whole) || !all_digits(frac) {
        return None;
    }

    let whole: i64 = if whole.is_empty() { 0 } else { whole.parse().ok()? };
    let mut frac_value: i64 = if frac.is_empty() { 0 } else { frac.parse().ok()? };
    for _ in frac.len()..6 {
        frac_value *= 10;
    }
    let value = whole.checked_mul(SCALE)?.checked_add(frac_value)?;
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimals_exactly() {
        assert_eq!(parse_fixed("0.4500"), Some(450_000));
        assert_eq!(parse_fixed("0.0001"), Some(100));
        assert_eq!(parse_fixed("-54.00"), Some(-54_000_000));
        assert_eq!(parse_fixed("300"), Some(300_000_000));
        assert_eq!(parse_fixed(".5"), Some(500_000));
        assert_eq!(parse_fixed("1.23000000"), Some(1_230_000));
        assert_eq!(parse_fixed("1.2345678"), None);
        assert_eq!(parse_fixed("abc"), None);
        assert_eq!(parse_fixed(""), None);
    }

    #[test]
    fn apply_tracks_absolute_quantity() {
        let mut book = Book::default();
        assert_eq!(book.apply(Side::Buy, 400_000, 5 * SCALE), Some(5 * SCALE));
        assert_eq!(book.apply(Side::Buy, 400_000, -2 * SCALE), Some(3 * SCALE));
        assert_eq!(book.apply(Side::Buy, 400_000, -3 * SCALE), Some(0));
        assert_eq!(book, Book::default());
        assert_eq!(book.apply(Side::Sell, 600_000, -SCALE), None);
    }

    #[test]
    fn snapshot_orders_best_first() {
        let book = Book::from_levels(&[(100, 1), (300, 1), (200, 1)], &[(900, 1), (700, 1)]);
        let snap = book.to_snapshot("X", Utc::now(), Utc::now());
        let bids: Vec<_> = snap.bids.iter().map(|l| l.price_level).collect();
        let asks: Vec<_> = snap.asks.iter().map(|l| l.price_level).collect();
        assert_eq!(bids, [300, 200, 100]);
        assert_eq!(asks, [700, 900]);
    }
}
