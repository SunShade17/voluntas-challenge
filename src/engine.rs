//! The engine owns every order book. Each market is subscribed on several
//! sockets ("replicas"), and each replica keeps its own copy of the book. One
//! live replica per market is the *source*: only its updates are written out.
//! When the source dies or falls behind, another replica takes over and its
//! book is written as a snapshot first, since the copies can differ by
//! whatever the old source missed.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use crossbeam::channel::Sender;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};
use tracing::{info, warn};

use crate::book::{Book, Level};
use crate::kalshi::socket::SocketCmd;
use crate::output::Record;

/// Markets are split into shards; each shard is served by `redundancy`
/// identical sockets, the replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId {
    pub shard: usize,
    pub replica: usize,
}

impl fmt::Display for ConnId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "shard{}/r{}", self.shard, self.replica)
    }
}

pub enum FeedEvent {
    Started {
        conn: ConnId,
        cmds: UnboundedSender<SocketCmd>,
    },
    Snapshot {
        conn: ConnId,
        ticker: String,
        received_at: DateTime<Utc>,
        exchange_ts: Option<DateTime<Utc>>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Delta {
        conn: ConnId,
        ticker: String,
        received_at: DateTime<Utc>,
        exchange_ts: Option<DateTime<Utc>>,
        side: Side,
        price: i64,
        delta: i64,
    },
    /// The socket disconnected; its copies of the books are no longer current.
    Down { conn: ConnId },
    /// Markets that stopped trading.
    Closed { tickers: Vec<String> },
}

struct Replica {
    book: Book,
    live: bool,
    /// When this replica last received a change for the market.
    last_change: Instant,
    /// Set while another replica has seen a change this one hasn't:
    /// `(since, the other replica's change time we're waiting to pass)`.
    lagging: Option<(Instant, Instant)>,
}

struct Market {
    shard: usize,
    replicas: Vec<Replica>,
    source: Option<usize>,
    exchange_ts: Option<DateTime<Utc>>,
}

impl Market {
    fn new(shard: usize, redundancy: usize) -> Self {
        let now = Instant::now();
        Self {
            shard,
            replicas: (0..redundancy)
                .map(|_| Replica { book: Book::default(), live: false, last_change: now, lagging: None })
                .collect(),
            source: None,
            exchange_ts: None,
        }
    }

    fn newest_change(&self) -> Option<Instant> {
        self.replicas.iter().filter(|r| r.live).map(|r| r.last_change).max()
    }
}

#[derive(Default)]
struct Stats {
    updates: u64,
    snapshots: u64,
    failovers: u64,
    resubscribes: u64,
}

pub struct Engine {
    redundancy: usize,
    stale_after: Duration,
    /// Start a new output file at each UTC midnight.
    rotate_daily: bool,
    day: NaiveDate,
    markets: HashMap<String, Market>,
    sockets: HashMap<ConnId, UnboundedSender<SocketCmd>>,
    out: Sender<Record>,
    stats: Stats,
}

impl Engine {
    pub fn new(redundancy: usize, stale_after: Duration, rotate_daily: bool, out: Sender<Record>) -> Self {
        Self {
            redundancy,
            stale_after,
            rotate_daily,
            day: Utc::now().date_naive(),
            markets: HashMap::new(),
            sockets: HashMap::new(),
            out,
            stats: Stats::default(),
        }
    }

    /// Runs until every event sender (discovery and all sockets) is gone.
    pub async fn run(mut self, mut events: UnboundedReceiver<FeedEvent>) {
        // Full snapshots on each wall-clock minute.
        let now_ms = Utc::now().timestamp_millis();
        let to_next_minute = Duration::from_millis((60_000 - now_ms.rem_euclid(60_000)) as u64);
        let mut minute = interval_at(Instant::now() + to_next_minute, Duration::from_secs(60));
        minute.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut health = interval(Duration::from_secs(1));
        let mut report = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Some(event) => self.handle(event),
                    None => break,
                },
                _ = minute.tick() => self.snapshot_all(),
                _ = health.tick() => self.check_lagging(),
                _ = report.tick() => self.report(),
            }
        }
        self.report();
    }

    fn handle(&mut self, event: FeedEvent) {
        match event {
            FeedEvent::Started { conn, cmds } => {
                self.sockets.insert(conn, cmds);
            }
            FeedEvent::Snapshot { conn, ticker, received_at, exchange_ts, bids, asks } => {
                let redundancy = self.redundancy;
                let market = self
                    .markets
                    .entry(ticker.clone())
                    .or_insert_with(|| Market::new(conn.shard, redundancy));
                // Count the replica as caught up as of its snapshot, so the
                // other replicas aren't flagged as lagging behind it.
                let caught_up = market.newest_change().unwrap_or_else(Instant::now);
                let replica = &mut market.replicas[conn.replica];
                replica.book = Book::from_levels(&bids, &asks);
                replica.live = true;
                replica.last_change = caught_up;
                replica.lagging = None;

                // Publish only if this replica is (or becomes) the source.
                // Snapshots from the other replicas just reset their copy.
                let source_live = market
                    .source
                    .is_some_and(|s| s != conn.replica && market.replicas[s].live);
                if !source_live {
                    market.source = Some(conn.replica);
                    market.exchange_ts = exchange_ts.or(market.exchange_ts);
                    let book = &market.replicas[conn.replica].book;
                    let snapshot = book.to_snapshot(&ticker, received_at, exchange_ts.unwrap_or(received_at));
                    send(&self.out, Record::Snapshot(snapshot));
                    self.stats.snapshots += 1;
                }
            }
            FeedEvent::Delta { conn, ticker, received_at, exchange_ts, side, price, delta } => {
                let Some(market) = self.markets.get_mut(&ticker) else {
                    return;
                };
                let replica = &mut market.replicas[conn.replica];
                if !replica.live {
                    return;
                }
                replica.last_change = Instant::now();
                let Some(quantity) = replica.book.apply(side, price, delta) else {
                    warn!(%conn, %ticker, "book went negative, resubscribing");
                    replica.live = false;
                    request_resubscribe(&self.sockets, &mut self.stats, conn, &ticker);
                    if market.source == Some(conn.replica) {
                        fail_over(market, &ticker, &self.out, &mut self.stats);
                    }
                    return;
                };
                if market.source == Some(conn.replica) {
                    market.exchange_ts = exchange_ts.or(market.exchange_ts);
                    self.stats.updates += 1;
                    send(&self.out, Record::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp: exchange_ts.unwrap_or(received_at),
                        price_level: price,
                        quantity,
                        side,
                        symbol: ticker,
                    }));
                }
            }
            FeedEvent::Down { conn } => {
                for (ticker, market) in &mut self.markets {
                    if market.shard != conn.shard {
                        continue;
                    }
                    market.replicas[conn.replica].live = false;
                    if market.source == Some(conn.replica) {
                        fail_over(market, ticker, &self.out, &mut self.stats);
                    }
                }
            }
            FeedEvent::Closed { tickers } => {
                for ticker in tickers {
                    self.markets.remove(&ticker);
                }
            }
        }
    }

    /// Writes the source's book for every market. Each file therefore starts
    /// with a complete picture within a minute of opening.
    fn snapshot_all(&mut self) {
        let now = Utc::now();
        if self.rotate_daily && now.date_naive() != self.day {
            self.day = now.date_naive();
            send(&self.out, Record::Rotate);
        }
        for (ticker, market) in &self.markets {
            let Some(source) = market.source.filter(|&s| market.replicas[s].live) else {
                continue;
            };
            let book = &market.replicas[source].book;
            send(&self.out, Record::Snapshot(book.to_snapshot(ticker, now, market.exchange_ts.unwrap_or(now))));
            self.stats.snapshots += 1;
        }
    }

    /// A replica that keeps missing changes another replica received has a
    /// subscription that went quiet. Resubscribe it, and fail over if it was
    /// the source.
    fn check_lagging(&mut self) {
        let now = Instant::now();
        for (ticker, market) in &mut self.markets {
            let mut source_lost = false;
            for i in 0..market.replicas.len() {
                let newest_other = market
                    .replicas
                    .iter()
                    .enumerate()
                    .filter(|&(j, r)| j != i && r.live)
                    .map(|(_, r)| r.last_change)
                    .max();
                let replica = &mut market.replicas[i];
                if !replica.live {
                    continue;
                }
                match replica.lagging {
                    Some((_, target)) if replica.last_change >= target => replica.lagging = None,
                    Some((since, _)) if now.duration_since(since) > self.stale_after => {
                        let conn = ConnId { shard: market.shard, replica: i };
                        warn!(%conn, %ticker, "replica stopped receiving updates, resubscribing");
                        replica.live = false;
                        replica.lagging = None;
                        request_resubscribe(&self.sockets, &mut self.stats, conn, ticker);
                        source_lost |= market.source == Some(i);
                    }
                    Some(_) => {}
                    None => {
                        if let Some(other) = newest_other.filter(|&t| t > replica.last_change) {
                            replica.lagging = Some((now, other));
                        }
                    }
                }
            }
            if source_lost {
                fail_over(market, ticker, &self.out, &mut self.stats);
            }
        }
    }

    fn report(&self) {
        let live = self
            .markets
            .values()
            .filter(|m| m.source.is_some_and(|s| m.replicas[s].live))
            .count();
        info!(
            markets = self.markets.len(),
            live,
            updates = self.stats.updates,
            snapshots = self.stats.snapshots,
            failovers = self.stats.failovers,
            resubscribes = self.stats.resubscribes,
            "engine",
        );
    }
}

/// Makes the most recently updated live replica the source and writes its
/// book, so readers switch over to the new copy cleanly.
fn fail_over(market: &mut Market, ticker: &str, out: &Sender<Record>, stats: &mut Stats) {
    market.source = market
        .replicas
        .iter()
        .enumerate()
        .filter(|(_, r)| r.live)
        .max_by_key(|(_, r)| r.last_change)
        .map(|(i, _)| i);
    if let Some(source) = market.source {
        let now = Utc::now();
        let book = &market.replicas[source].book;
        send(out, Record::Snapshot(book.to_snapshot(ticker, now, market.exchange_ts.unwrap_or(now))));
        stats.failovers += 1;
        stats.snapshots += 1;
    }
}

fn request_resubscribe(
    sockets: &HashMap<ConnId, UnboundedSender<SocketCmd>>,
    stats: &mut Stats,
    conn: ConnId,
    ticker: &str,
) {
    if let Some(cmds) = sockets.get(&conn) {
        let _ = cmds.send(SocketCmd::Resubscribe(ticker.to_owned()));
        stats.resubscribes += 1;
    }
}

fn send(out: &Sender<Record>, record: Record) {
    // Only fails if the writer thread died; main reports that error on exit.
    let _ = out.send(record);
}
