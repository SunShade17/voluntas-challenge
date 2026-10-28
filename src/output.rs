//! The writer thread. Encoding and disk I/O happen here so the async side
//! never blocks on the filesystem.

use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};

use chrono::Utc;
use crossbeam::channel::Receiver;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};
use tracing::info;

/// Must match the chunk size `AlignedSbeReader` reads with.
const CHUNK_SIZE: usize = 64 << 20;

pub enum Record {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
    /// Close the current file and start a new one.
    Rotate,
}

/// Spawns the writer. It runs until every `Record` sender is dropped, then
/// flushes the last chunk and closes the file.
///
/// `out` is a directory for timestamped files, or `-` for stdout (Unix only,
/// since the helper writer needs a path).
pub fn spawn_writer(out: PathBuf, records: Receiver<Record>) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let mut writer = open(&out)?;
    let handle = thread::Builder::new().name("sbe-writer".into()).spawn(move || {
        for record in records {
            match record {
                Record::Snapshot(snapshot) => writer.write_msg(&snapshot)?,
                Record::Update(update) => writer.write_msg(&update)?,
                Record::Rotate => {
                    writer.flush()?;
                    writer = open(&out)?;
                }
            }
        }
        writer.flush()
    })?;
    Ok(handle)
}

fn open(out: &Path) -> anyhow::Result<AlignedSbeWriter> {
    let path = if out == Path::new("-") {
        PathBuf::from("/dev/stdout")
    } else {
        std::fs::create_dir_all(out)?;
        out.join(format!("kalshi-orderbook-{}.sbe", Utc::now().format("%Y%m%dT%H%M%SZ")))
    };
    info!(path = %path.display(), "opened output file");
    AlignedSbeWriter::create(&path, CHUNK_SIZE)
}
