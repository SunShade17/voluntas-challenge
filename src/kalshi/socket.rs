//! One Kalshi websocket. Each socket owns its list of markets, so after a
//! disconnect or a scheduled recycle it reconnects and resubscribes by itself.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::{debug, info, warn};

use super::auth::Credentials;
use super::protocol::{self, Delta, Envelope, ErrorMsg, Snapshot};
use crate::engine::{ConnId, FeedEvent};

/// Tickers per subscribe command. Kalshi merges them into one subscription.
const SUBSCRIBE_BATCH: usize = 100;
/// Kalshi pings every 10s, so this much silence means the socket is dead.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

pub enum SocketCmd {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Drop and re-add one market to get a fresh snapshot for it.
    Resubscribe(String),
}

pub struct SocketConfig {
    pub conn: ConnId,
    pub credentials: Arc<Credentials>,
    /// Spreads out the initial connections so we don't open them all at once.
    pub start_delay: Duration,
    pub first_recycle: Duration,
    pub recycle_every: Duration,
}

enum Exit {
    Shutdown,
    Recycle,
}

pub async fn run(
    cfg: SocketConfig,
    mut cmds: mpsc::UnboundedReceiver<SocketCmd>,
    events: mpsc::UnboundedSender<FeedEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let conn = cfg.conn;
    let mut markets = BTreeSet::new();
    let mut delay = cfg.start_delay;
    let mut backoff = Duration::from_millis(250);
    let mut recycle_in = cfg.first_recycle;

    loop {
        tokio::select! {
            _ = sleep(delay) => {}
            _ = shutdown.changed() => return,
        }
        // Commands that arrived while disconnected only change the market list.
        while let Ok(cmd) = cmds.try_recv() {
            match cmd {
                SocketCmd::Add(tickers) => markets.extend(tickers),
                SocketCmd::Remove(tickers) => tickers.iter().for_each(|t| {
                    markets.remove(t);
                }),
                SocketCmd::Resubscribe(_) => {}
            }
        }

        let started = Instant::now();
        let result = session(&cfg, recycle_in, &mut markets, &mut cmds, &events, &mut shutdown).await;
        if matches!(result, Ok(Exit::Shutdown)) {
            info!(%conn, "websocket closed");
            return;
        }
        let _ = events.send(FeedEvent::Down { conn });

        match result {
            Ok(_) => {
                info!(%conn, "recycling websocket");
                delay = Duration::ZERO;
                recycle_in = cfg.recycle_every;
            }
            Err(e) => {
                if started.elapsed() > Duration::from_secs(60) {
                    backoff = Duration::from_millis(250);
                }
                warn!(%conn, "websocket failed: {e:#}; reconnecting in {backoff:?}");
                delay = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn session(
    cfg: &SocketConfig,
    recycle_in: Duration,
    markets: &mut BTreeSet<String>,
    cmds: &mut mpsc::UnboundedReceiver<SocketCmd>,
    events: &mpsc::UnboundedSender<FeedEvent>,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<Exit> {
    let conn = cfg.conn;
    let mut request = super::WS_URL.into_client_request()?;
    for (name, value) in cfg.credentials.headers("GET", super::WS_PATH) {
        request.headers_mut().insert(name, HeaderValue::from_str(&value)?);
    }
    let (ws, _) = timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
        .await
        .context("connect timed out")??;
    let (mut sink, mut stream) = ws.split();

    let mut subs = Subscriptions::default();
    let tickers: Vec<String> = markets.iter().cloned().collect();
    subs.subscribe(&mut sink, &tickers).await?;
    info!(%conn, markets = markets.len(), "connected");

    let recycle_at = Instant::now() + recycle_in;
    let mut last_frame = Instant::now();
    let mut watchdog = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                let _ = sink.send(Message::Close(None)).await;
                return Ok(Exit::Shutdown);
            }
            _ = sleep_until(recycle_at) => {
                let _ = sink.send(Message::Close(None)).await;
                return Ok(Exit::Recycle);
            }
            frame = stream.next() => {
                // Timestamp before parsing: this is the arrival time we record.
                let received_at = Utc::now();
                let frame = frame.context("server ended the stream")??;
                last_frame = Instant::now();
                match frame {
                    Message::Text(text) => subs.on_text(conn, text.as_str(), received_at, events)?,
                    Message::Close(reason) => bail!("server closed the connection: {reason:?}"),
                    // tungstenite queues the pong replies to pings itself.
                    _ => {}
                }
            }
            cmd = cmds.recv() => match cmd {
                Some(SocketCmd::Add(tickers)) => {
                    let new: Vec<String> = tickers.into_iter().filter(|t| markets.insert(t.clone())).collect();
                    subs.subscribe(&mut sink, &new).await?;
                }
                Some(SocketCmd::Remove(tickers)) => {
                    for ticker in &tickers {
                        markets.remove(ticker);
                    }
                    subs.unsubscribe(&mut sink, &tickers).await?;
                }
                Some(SocketCmd::Resubscribe(ticker)) => {
                    if markets.contains(&ticker) {
                        let one = std::slice::from_ref(&ticker);
                        subs.unsubscribe(&mut sink, one).await?;
                        subs.subscribe(&mut sink, one).await?;
                    }
                }
                None => return Ok(Exit::Shutdown),
            },
            _ = watchdog.tick() => {
                if last_frame.elapsed() > IDLE_TIMEOUT {
                    bail!("nothing received for {IDLE_TIMEOUT:?}");
                }
            }
        }
    }
}

/// Per-connection protocol state.
#[derive(Default)]
struct Subscriptions {
    next_id: u64,
    /// Last `seq` seen on each subscription.
    seqs: HashMap<u64, u64>,
    /// Which subscription each market lives on, needed to remove it.
    sids: HashMap<String, u64>,
}

impl Subscriptions {
    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    async fn subscribe<S>(&mut self, sink: &mut S, tickers: &[String]) -> anyhow::Result<()>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        for batch in tickers.chunks(SUBSCRIBE_BATCH) {
            let id = self.next_id();
            sink.send(Message::Text(protocol::subscribe(id, batch).into())).await?;
        }
        Ok(())
    }

    async fn unsubscribe<S>(&mut self, sink: &mut S, tickers: &[String]) -> anyhow::Result<()>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        let mut by_sid: HashMap<u64, Vec<String>> = HashMap::new();
        for ticker in tickers {
            if let Some(sid) = self.sids.remove(ticker) {
                by_sid.entry(sid).or_default().push(ticker.clone());
            }
        }
        for (sid, batch) in by_sid {
            let id = self.next_id();
            sink.send(Message::Text(protocol::delete_markets(id, sid, &batch).into())).await?;
        }
        Ok(())
    }

    fn on_text(
        &mut self,
        conn: ConnId,
        text: &str,
        received_at: DateTime<Utc>,
        events: &mpsc::UnboundedSender<FeedEvent>,
    ) -> anyhow::Result<()> {
        let env: Envelope = match serde_json::from_str(text) {
            Ok(env) => env,
            Err(e) => {
                warn!(%conn, "unparseable frame ({e}): {}", truncate(text));
                return Ok(());
            }
        };

        if let (Some(sid), Some(seq)) = (env.sid, env.seq) {
            if let Some(prev) = self.seqs.insert(sid, seq) {
                if seq != prev + 1 {
                    // We can't tell which market lost the message, so start over.
                    bail!("sequence gap on sid {sid}: {prev} -> {seq}");
                }
            }
        }

        let msg = env.msg.map(|m| m.get()).unwrap_or("null");
        match env.kind {
            "orderbook_snapshot" => {
                let snap: Snapshot = serde_json::from_str(msg).context("bad orderbook_snapshot")?;
                if let Some(sid) = env.sid {
                    self.sids.insert(snap.market_ticker.clone(), sid);
                }
                let (bids, asks) = snap.levels();
                let _ = events.send(FeedEvent::Snapshot {
                    conn,
                    ticker: snap.market_ticker,
                    received_at,
                    exchange_ts: env.sending_ts_ms.and_then(DateTime::from_timestamp_millis),
                    bids,
                    asks,
                });
            }
            "orderbook_delta" => {
                let delta: Delta = serde_json::from_str(msg).context("bad orderbook_delta")?;
                let Some((side, price, change)) = delta.change() else {
                    warn!(%conn, "delta without price or size: {}", truncate(text));
                    return Ok(());
                };
                let exchange_ts = delta
                    .exchange_time()
                    .or_else(|| env.sending_ts_ms.and_then(DateTime::from_timestamp_millis));
                let _ = events.send(FeedEvent::Delta {
                    conn,
                    ticker: delta.market_ticker,
                    received_at,
                    exchange_ts,
                    side,
                    price,
                    delta: change,
                });
            }
            "error" => {
                let err: Option<ErrorMsg> = serde_json::from_str(msg).ok();
                let (code, text) = err.map(|e| (e.code, e.msg)).unwrap_or_default();
                warn!(%conn, id = ?env.id, ?code, "kalshi error: {}", text.unwrap_or_default());
            }
            other => debug!(%conn, kind = other, "ignored frame"),
        }
        Ok(())
    }
}

fn truncate(text: &str) -> &str {
    match text.char_indices().nth(200) {
        Some((i, _)) => &text[..i],
        None => text,
    }
}
