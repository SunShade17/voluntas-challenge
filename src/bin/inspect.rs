//! Reads a recorded file back with `AlignedSbeReader`, replays it into books,
//! and prints a summary. Usage: `cargo run --bin inspect -- data/<file>.sbe`

use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).context("usage: inspect <file.sbe>")?;
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

    let mut books: HashMap<String, Book> = HashMap::new();
    let (mut snapshots, mut updates, mut orphan_updates, mut crossed) = (0u64, 0u64, 0u64, 0u64);
    let (mut first, mut last) = (None, None);

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snap = SnapshotDomain::try_from_decoder(decoder)?;
                snapshots += 1;
                first.get_or_insert(snap.received_at);
                last = Some(snap.received_at);
                let book = books.entry(snap.symbol).or_default();
                book.bids = snap.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
                book.asks = snap.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                updates += 1;
                first.get_or_insert(update.backtest_timestamp);
                last = Some(update.backtest_timestamp);
                let Some(book) = books.get_mut(&update.symbol) else {
                    orphan_updates += 1;
                    continue;
                };
                let levels = match update.side {
                    Side::Buy => &mut book.bids,
                    Side::Sell => &mut book.asks,
                };
                if update.quantity > 0 {
                    levels.insert(update.price_level, update.quantity);
                } else {
                    levels.remove(&update.price_level);
                }
            }
            _ => anyhow::bail!("unexpected message type"),
        }
    }

    for book in books.values() {
        if let (Some((bid, _)), Some((ask, _))) = (book.bids.last_key_value(), book.asks.first_key_value()) {
            if bid >= ask {
                crossed += 1;
            }
        }
    }

    println!("file:            {path}");
    println!("time range:      {:?} .. {:?}", first, last);
    println!("snapshots:       {snapshots}");
    println!("updates:         {updates}");
    println!("symbols:         {}", books.len());
    println!("update before snapshot: {orphan_updates}");
    println!("crossed books at end:   {crossed}");

    let mut busiest: Vec<_> = books.iter().collect();
    busiest.sort_by_key(|(_, b)| std::cmp::Reverse(b.bids.len() + b.asks.len()));
    for (symbol, book) in busiest.into_iter().take(3) {
        let best_bid = book.bids.last_key_value().map(|(p, q)| (*p as f64 / 1e6, *q as f64 / 1e6));
        let best_ask = book.asks.first_key_value().map(|(p, q)| (*p as f64 / 1e6, *q as f64 / 1e6));
        println!("  {symbol}: best bid {best_bid:?}, best ask {best_ask:?}");
    }
    Ok(())
}
