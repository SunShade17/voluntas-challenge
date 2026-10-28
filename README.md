# Kalshi order book collector

Subscribes to every open Kalshi market (multivariate/MVE markets excluded),
keeps an order book for each one, and records:

- a `PriceLevelUpdate` for every change, with the level's new absolute quantity
- a full `Snapshot` of every book on each wall-clock minute
- a `Snapshot` whenever a book's source socket changes (see below)

Everything is written with the template's `AlignedSbeWriter`, in the
challenge's fixed-point format (1_000_000 = $1 or 1 contract).

## Running

```
cp .env.example .env     # fill in your key ID and the path to your private key
cargo run --release
```

Ctrl+C closes every websocket, flushes the last chunk and closes the output
file. A second Ctrl+C exits immediately.

Useful flags (all have defaults, see `--help`):

| flag | default | |
|---|---|---|
| `--out` | `data` | output directory; `-` writes the SBE stream to stdout (Unix) |
| `--markets-per-socket` | 1500 | |
| `--redundancy` | 2 | sockets subscribed to each market |
| `--max-sockets` | 190 | Kalshi allows about 200 connections per account |
| `--discovery-secs` | 60 | how often to look for new and closed markets |
| `--recycle-secs` | 1800 | scheduled reconnect period per socket |
| `--stale-secs` | 10 | how long a replica may miss updates before it is resubscribed |
| `--max-markets` | | follow only the first N markets, for testing |

To check a recording:

```
cargo run --release --bin inspect -- data/kalshi-orderbook-<timestamp>.sbe
```

It reads the file back with `AlignedSbeReader`, replays it into books and
reports counts, any update that arrived before its market's snapshot, and any
book that ends up crossed.

## How it works

```
REST /markets ──► discovery ──► shards ──► sockets (2 per shard) ──► engine ──► writer thread ──► .sbe
```

- **Discovery** (`src/discovery.rs`) lists open markets every minute and diffs
  against what we follow. New markets fill the first shard with room, and a
  new shard (with its own sockets) is created when all are full. Markets never
  move between shards, so a new listing doesn't disturb existing
  subscriptions. Closed markets are unsubscribed and their books dropped.
- **Sockets** (`src/kalshi/socket.rs`) each own their market list, so a socket
  that drops reconnects (with backoff) and resubscribes by itself. Every frame
  is timestamped on arrival, before parsing.
- **Engine** (`src/engine.rs`) owns all the books on a single task, so there
  are no locks.
- **Writer** (`src/output.rs`) runs on its own OS thread behind a channel, so
  encoding and disk I/O never stall the sockets. A new file is started at
  each UTC midnight, followed by the next minute's snapshots.

Kalshi's book only has bids: YES bids and NO bids. A NO bid at `p` is an offer
to sell YES at `1 - p`, so NO bids become asks at `1_000_000 - p`. Prices and
sizes arrive as decimal strings (`"0.4500"`, `"-54.00"`) and are parsed
straight to integers, never through `f64`.

### Redundant sockets (extension 1)

Every shard is served by two sockets, and each keeps its own copy of the
shard's books. Per market, one live copy is the *source*, and only its
updates are written. If the source's socket drops, its sequence numbers skip,
or it stops receiving updates, the engine switches to the other copy and
writes that copy's book as a snapshot first. The copies can differ by
whatever the old source missed, so readers can't simply carry on with deltas.

Kalshi's docs describe a cross-connection `clock` that would allow merging
both streams message by message. Other people's testing found it missing in
practice, so this collector doesn't depend on it.

### Subscription monitoring (extension 2)

- **Sequence gaps.** `seq` is contiguous per subscription and counts every
  frame on it (`ok` replies included), so a gap means a lost message for *some*
  market on that socket. The socket reconnects, and the other replica keeps
  writing in the meantime.
- **Quiet subscriptions.** If one replica sees a change for a market and the
  other doesn't catch up within `--stale-secs`, the lagging one is
  resubscribed for that market, and the engine fails over first if it was the
  source. Comparing the two replicas avoids guessing per-market activity
  thresholds: a quiet market is quiet on both sockets.
- **Inconsistent books.** A delta that would take a level below zero means
  that copy has drifted, so it's treated the same way.
- **Dead sockets.** Kalshi pings every 10s, so 30s of silence means the
  socket is dead and it reconnects.
- **Scheduled recycling.** Every socket reconnects every 30 minutes. The two
  replicas of a shard are half a period apart, and shards are a few seconds
  apart, so a market never loses both sockets to a recycle.

### Exchange-specific vs generic (extension 3)

Everything Kalshi-specific (auth, REST discovery, message formats, the
socket's subscribe/unsubscribe commands) lives in `src/kalshi/`. The engine,
book, discovery sharding and writer only see `FeedEvent`s (snapshot, delta,
socket down). Adding another exchange means writing its `kalshi/`
equivalent. I didn't go as far as a trait, because I'd rather design it
against a second real exchange than guess at the shape.

## Decisions and limitations

- **Files, not stdout.** The spec mentions stdout, but the helper writer takes
  a path and the spec also says to close "the file you are currently writing
  to", so output goes to timestamped files and logs go to stderr. `--out -`
  writes the binary stream to `/dev/stdout` on Unix.
- **Discovery is a full REST scan.** It's simple and self-correcting, but slow
  when there are tens of thousands of markets. Kalshi's `market_lifecycle_v2`
  channel would pick up new markets within seconds; I'd add it next, keeping
  the REST scan as a periodic reconcile.
- **Chunked output.** `AlignedSbeWriter` buffers a 64 MiB chunk before writing
  it, so a hard crash (not Ctrl+C) loses up to one chunk. Smaller chunks would
  need a matching reader change.
- **Unbounded channels.** The sockets never block on the engine, because a
  blocked socket only moves the backlog to Kalshi's side (error 25,
  "buffer overflow"). The cost is that memory grows if the writer can't keep up.
- **Ed25519 keys only**, which is what Kalshi generates by default.
- **Windows.** The template's reader used `std::os::unix::fs::FileExt`, which
  doesn't exist on Windows. I added a small `cfg(windows)` shim around
  `seek_read` so the project builds on both.
