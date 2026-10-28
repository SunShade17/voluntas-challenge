//! Records every Kalshi order book (MVE markets excluded) as SBE: a
//! `PriceLevelUpdate` per change and a full `Snapshot` of every book each
//! minute. See the README for the design.

mod book;
mod discovery;
mod engine;
mod kalshi;
mod output;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use clap::Parser;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::discovery::DiscoveryConfig;
use crate::engine::Engine;
use crate::kalshi::auth::Credentials;

#[derive(Parser)]
#[command(about = "Records every Kalshi order book to SBE files")]
struct Args {
    /// Kalshi API key ID.
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    key_id: String,
    /// Path to the Kalshi private key (Ed25519, PEM).
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    private_key: PathBuf,
    /// Output directory, or `-` to write the SBE stream to stdout (Unix only).
    #[arg(long, default_value = "data")]
    out: PathBuf,
    /// Markets per websocket.
    #[arg(long, default_value_t = 1500)]
    markets_per_socket: usize,
    /// Sockets subscribed to each market.
    #[arg(long, default_value_t = 2)]
    redundancy: usize,
    /// Kalshi allows roughly 200 connections per account.
    #[arg(long, default_value_t = 190)]
    max_sockets: usize,
    /// Seconds between market discovery passes.
    #[arg(long, default_value_t = 60)]
    discovery_secs: u64,
    /// Seconds between scheduled reconnects of each socket.
    #[arg(long, default_value_t = 1800)]
    recycle_secs: u64,
    /// Seconds a replica may miss updates another replica received before it
    /// is resubscribed.
    #[arg(long, default_value_t = 10)]
    stale_secs: u64,
    /// Only follow the first N markets (handy for testing).
    #[arg(long)]
    max_markets: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    anyhow::ensure!(args.redundancy >= 1, "--redundancy must be at least 1");

    let credentials = Arc::new(Credentials::load(args.key_id, &args.private_key)?);
    let to_stdout = args.out.as_os_str() == "-";

    let (records, record_rx) = crossbeam::channel::unbounded();
    let writer = output::spawn_writer(args.out, record_rx)?;
    let (events, event_rx) = mpsc::unbounded_channel();
    let (shutdown, shutdown_rx) = watch::channel(false);

    let engine = Engine::new(args.redundancy, Duration::from_secs(args.stale_secs), !to_stdout, records);
    let engine = tokio::spawn(engine.run(event_rx));
    let discovery = tokio::spawn(discovery::run(
        DiscoveryConfig {
            credentials,
            markets_per_socket: args.markets_per_socket,
            redundancy: args.redundancy,
            max_sockets: args.max_sockets,
            interval: Duration::from_secs(args.discovery_secs),
            recycle_every: Duration::from_secs(args.recycle_secs),
            max_markets: args.max_markets,
        },
        events,
        shutdown_rx,
    ));

    tokio::signal::ctrl_c().await?;
    info!("SIGINT received, closing websockets");
    shutdown.send_replace(true);
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("second SIGINT, exiting without flushing");
            std::process::exit(130);
        }
    });

    // Sockets close themselves on the signal. Once they and discovery are
    // gone, the engine drains its queue and drops the writer's channel, and
    // the writer flushes and closes the file.
    let abort = discovery.abort_handle();
    if tokio::time::timeout(Duration::from_secs(10), discovery).await.is_err() {
        warn!("websockets did not close within 10s, dropping them");
        abort.abort();
    }
    engine.await?;
    writer.join().map_err(|_| anyhow!("writer thread panicked"))??;
    info!("output file closed");
    Ok(())
}
