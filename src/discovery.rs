//! Finds open markets, spreads them across sockets, and keeps the assignment
//! up to date as markets open and close.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::engine::{ConnId, FeedEvent};
use crate::kalshi::auth::Credentials;
use crate::kalshi::rest;
use crate::kalshi::socket::{self, SocketCmd, SocketConfig};

pub struct DiscoveryConfig {
    pub credentials: Arc<Credentials>,
    pub markets_per_socket: usize,
    pub redundancy: usize,
    pub max_sockets: usize,
    pub interval: Duration,
    pub recycle_every: Duration,
    /// Only follow the first N markets (for testing).
    pub max_markets: Option<usize>,
}

/// A group of markets served by `redundancy` sockets. Markets stay on their
/// shard for life, so adding or removing one never moves the others.
struct Shard {
    len: usize,
    sockets: Vec<mpsc::UnboundedSender<SocketCmd>>,
}

impl Shard {
    fn send(&self, cmd: impl Fn() -> SocketCmd) {
        for socket in &self.sockets {
            let _ = socket.send(cmd());
        }
    }
}

pub async fn run(cfg: DiscoveryConfig, events: mpsc::UnboundedSender<FeedEvent>, shutdown: watch::Receiver<bool>) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("building the HTTP client");
    let mut shards: Vec<Shard> = Vec::new();
    let mut home: HashMap<String, usize> = HashMap::new();
    let mut sockets = JoinSet::new();
    let mut tick = tokio::time::interval(cfg.interval);
    let mut stop = shutdown.clone();
    let mut warned_full = false;

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => break,
        }
        let mut open = match rest::open_markets(&http, crate::kalshi::REST_URL).await {
            Ok(open) => open,
            Err(e) => {
                warn!("market discovery failed: {e:#}");
                continue;
            }
        };
        open.sort();
        if let Some(max) = cfg.max_markets {
            open.truncate(max);
        }
        let open_set: HashSet<&String> = open.iter().collect();

        let closed: Vec<String> = home.keys().filter(|t| !open_set.contains(t)).cloned().collect();
        let mut removals: HashMap<usize, Vec<String>> = HashMap::new();
        for ticker in &closed {
            if let Some(shard) = home.remove(ticker) {
                shards[shard].len -= 1;
                removals.entry(shard).or_default().push(ticker.clone());
            }
        }
        for (shard, tickers) in removals {
            shards[shard].send(|| SocketCmd::Remove(tickers.clone()));
        }

        let mut additions: HashMap<usize, Vec<String>> = HashMap::new();
        let mut spawned = 0;
        let new: Vec<&String> = open.iter().filter(|t| !home.contains_key(*t)).collect();
        for ticker in new {
            let shard = match shards.iter().position(|s| s.len < cfg.markets_per_socket) {
                Some(shard) => shard,
                None if (shards.len() + 1) * cfg.redundancy <= cfg.max_sockets => {
                    shards.push(spawn_shard(&cfg, shards.len(), spawned, &events, &shutdown, &mut sockets));
                    spawned += 1;
                    shards.len() - 1
                }
                None => {
                    if !warned_full {
                        warn!(max_sockets = cfg.max_sockets, "socket limit reached, some markets are not followed");
                        warned_full = true;
                    }
                    break;
                }
            };
            shards[shard].len += 1;
            home.insert(ticker.clone(), shard);
            additions.entry(shard).or_default().push(ticker.clone());
        }
        let added: usize = additions.values().map(Vec::len).sum();
        for (shard, tickers) in additions {
            shards[shard].send(|| SocketCmd::Add(tickers.clone()));
        }
        if !closed.is_empty() {
            let _ = events.send(FeedEvent::Closed { tickers: closed.clone() });
        }
        info!(
            followed = home.len(),
            added,
            removed = closed.len(),
            sockets = shards.len() * cfg.redundancy,
            "market discovery"
        );
    }

    // The sockets got the same shutdown signal; wait for them to close.
    drop(shards);
    while sockets.join_next().await.is_some() {}
}

fn spawn_shard(
    cfg: &DiscoveryConfig,
    shard: usize,
    spawned_this_round: usize,
    events: &mpsc::UnboundedSender<FeedEvent>,
    shutdown: &watch::Receiver<bool>,
    tasks: &mut JoinSet<()>,
) -> Shard {
    let redundancy = cfg.redundancy as u32;
    let mut sockets = Vec::new();
    for replica in 0..cfg.redundancy {
        let conn = ConnId { shard, replica };
        let (cmds, cmd_rx) = mpsc::unbounded_channel();
        let _ = events.send(FeedEvent::Started { conn, cmds: cmds.clone() });
        let socket_cfg = SocketConfig {
            conn,
            credentials: cfg.credentials.clone(),
            start_delay: Duration::from_millis(100 * (spawned_this_round * cfg.redundancy + replica) as u64),
            // Replicas recycle half a period apart (for two replicas), so a
            // market never loses every socket at once; shards are offset by
            // a few seconds so we don't reconnect everything together.
            first_recycle: cfg.recycle_every
                + cfg.recycle_every * replica as u32 / redundancy
                + Duration::from_secs((shard as u64 * 7) % 300),
            recycle_every: cfg.recycle_every,
        };
        tasks.spawn(socket::run(socket_cfg, cmd_rx, events.clone(), shutdown.clone()));
        sockets.push(cmds);
    }
    Shard { len: 0, sockets }
}
