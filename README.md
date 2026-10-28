# Kalshi order book collector

Records order book data for every open Kalshi market, excluding multivariate
(MVE) markets. The collector keeps an order book in memory for each market and
writes:

- a `PriceLevelUpdate` for every change to a price level, carrying the level's
  new absolute quantity
- a full `Snapshot` of every book at the start of each minute
- a `Snapshot` whenever a market switches to a different source connection
  (see [Redundant connections](#redundant-connections-extension-1))

Output uses the template's `AlignedSbeWriter` and message schema. Prices and
quantities are fixed point, where 1_000_000 is $1 or one contract.

## Setup

A Kalshi API key is required. Only Ed25519 keys are supported, which is the
key type Kalshi generates by default.

```
cp .env.example .env
```

Set `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH` in `.env`, or pass them
as `--key-id` and `--private-key`.

## Running

```
cargo run --release
```

Recordings are written to `data/kalshi-orderbook-<UTC timestamp>.sbe` and logs
go to stderr. Ctrl+C (SIGINT) closes every websocket, writes the last buffered
chunk and closes the output file. A second Ctrl+C exits immediately without
flushing.

| Flag | Default | Description |
|---|---|---|
| `--out` | `data` | Output directory. `-` writes the SBE stream to `/dev/stdout` (Unix only). |
| `--markets-per-socket` | 1500 | Markets per websocket connection. |
| `--redundancy` | 2 | Connections subscribed to each market. |
| `--max-sockets` | 190 | Connection limit. Kalshi allows about 200 per account. |
| `--discovery-secs` | 60 | Interval between market discovery passes. |
| `--recycle-secs` | 1800 | Interval between scheduled reconnects of each connection. |
| `--stale-secs` | 10 | How long one copy of a book may trail the other before the two are compared. |
| `--max-markets` | none | Follow only the first N markets, for testing. |

## Checking a recording

```
cargo run --release --bin inspect -- data/kalshi-orderbook-<timestamp>.sbe
```

`inspect` reads the file with `AlignedSbeReader`, replays it into order books
and prints:

- the number of snapshots, updates and symbols
- updates that appear before their market's first snapshot
- books that are crossed at the end of the file
- each second in which 10 or more snapshots were written, which shows the
  per-minute batches

## Testing

`cargo test` runs unit tests for decimal parsing, order book updates and
Kalshi message parsing.

For a live check against a small set of markets:

```
cargo run -- --max-markets 300 --markets-per-socket 100
```

The first discovery pass pages through every open market before trimming the
list, so connections open after about a minute. Let it run past a couple of
minute boundaries, stop it with Ctrl+C and run `inspect` on the new file. Each
minute boundary should show one snapshot per market, with no updates before
snapshots and no crossed books. Test runs against production with 300 markets
matched this.

Not yet covered: a run across every open market, the 30-minute scheduled
reconnects, and the file rotation at midnight.

## Design

```
REST /markets -> discovery -> shards -> connections (2 per shard) -> engine -> writer thread -> .sbe file
```

### Market discovery

`src/discovery.rs` fetches the open markets once a minute
(`GET /trade-api/v2/markets?status=open&mve_filter=exclude`, 1000 per page)
and compares the result with the markets already followed. Markets that have
an `mve_collection_ticker` are skipped as well.

New markets go to the first shard with free capacity. When every shard is
full, a new shard and its connections are created, up to `--max-sockets`. A
market stays on the same shard for as long as it trades, so a new listing never
moves existing subscriptions. Markets that drop off the list are unsubscribed
and their books are discarded.

### Connections

Each connection (`src/kalshi/socket.rs`) keeps its own list of markets. After
a disconnect it reconnects with exponential backoff, capped at 30 s, and
subscribes to the whole list again. Tickers are subscribed 100 per command.
Every frame is timestamped as soon as it is read from the socket, before JSON
parsing.

### Engine and writer

`src/engine.rs` holds every order book on a single task, so book state needs no
locking. It decides which updates get written and produces the per-minute
snapshots.

`src/output.rs` runs on a dedicated OS thread and receives records over a
channel, which keeps encoding and disk writes from delaying websocket reads. At
the first minute tick after UTC midnight it closes the current file and opens a
new one, and that tick's snapshots become the first records in the new file.

### Prices and sides

Kalshi's order book holds only bids: YES bids and NO bids. A NO bid at price
`p` is equivalent to an offer to sell YES at `1 - p`. YES bids are therefore
written as bids (`Side::Buy`), and NO bids as asks (`Side::Sell`) at
`1_000_000 - p`.

Prices and sizes arrive as decimal strings such as `"0.4500"` and `"-54.00"`
and are converted directly to integers, without floating point. The older
integer-cent fields are used only when the decimal fields are missing.

### Timestamps

| Record | `received_at` / `backtest_timestamp` | `exchange_timestamp` |
|---|---|---|
| Update | arrival time of the frame | `ts_ms` from the delta, else the frame's `sending_ts_ms`, else arrival time |
| Snapshot received from Kalshi | arrival time of the frame | `sending_ts_ms`, else arrival time |
| Minute or failover snapshot | time it was written | last exchange timestamp seen for the market |

### Redundant connections (extension 1)

Each shard is served by two connections (`--redundancy`), and each connection
keeps its own copy of the shard's books. For every market one live copy is the
source, and only the source's updates are written. The source changes when its
connection drops, when that connection skips a sequence number, or when its
copy of the book turns out to be out of date (see below). The new source's book
is written as a snapshot before any further updates, because the two copies can
differ by whatever the old source missed.

Kalshi's documentation describes a `clock` field that is the same on every
connection and would allow the two streams to be merged message by message.
The collector does not use it, since it could not be confirmed that live
messages always include it.

### Subscription monitoring (extension 2)

`seq` is contiguous per subscription and counts every frame on it, including
`ok` replies. A gap means a message on that connection was lost, and it could
have belonged to any of its markets, so the connection reconnects and
resubscribes. The other connection keeps those markets covered in the
meantime.

A health check runs once a second. If one copy of a book has received a change
that the other copy still has not seen after `--stale-secs`, the two books are
compared. A connection that subscribed slightly later gets some changes already
applied in its initial snapshot and never receives them as deltas, so matching
books mean nothing was lost. If the books differ, the lagging connection is
resubscribed to that market, and if it was the source, the other copy takes
over. Comparing the two copies avoids per-market activity thresholds, because a
quiet market is quiet on both connections.

A delta that would take a level below zero also marks that copy as out of date,
with the same result.

Kalshi pings every connection every 10 s. A connection that receives nothing
for 30 s is treated as dead and reconnects.

Each connection also reconnects on a schedule, every 30 minutes by default. The
two connections of a shard are half a period apart and shards are offset by a
few seconds, so a scheduled reconnect never takes down both copies of a market.

### Exchange-specific code (extension 3)

Code specific to Kalshi lives in `src/kalshi/`: authentication, the REST
market list, message parsing, and the subscribe and unsubscribe commands. The
engine, order book and writer only see `FeedEvent` values (snapshot, delta,
connection down, markets closed). Discovery gets its market list from the
Kalshi REST module, but its sharding logic does not depend on the exchange.

Supporting another exchange means adding a module equivalent to `src/kalshi/`.
There is no shared `Exchange` trait yet, since its shape is easier to settle
against a second real exchange.

## Limitations

- Output goes to files. The challenge mentions stdout, but the provided writer
  takes a file path and the challenge also asks for the current file to be
  closed on SIGINT. `--out -` writes the binary stream to `/dev/stdout` on
  Unix systems.
- Discovery scans the full market list over REST on every pass. This is simple
  and recovers from any missed change on the next pass, but one pass over the
  production market list takes close to a minute. Kalshi's
  `market_lifecycle_v2` websocket channel reports new and closed markets within
  seconds and would be the next addition, with the REST scan kept as a periodic
  check.
- `AlignedSbeWriter` keeps a 64 MiB chunk in memory before writing it, so a
  crash (as opposed to Ctrl+C) can lose up to one chunk. The reader always
  opens files with 64 MiB chunks, so smaller chunks would need a reader change
  too.
- Channels between the connections, engine and writer are unbounded.
  Connections never wait on the engine, since blocking them would only move the
  backlog to Kalshi, which closes connections that fall behind (error 25,
  buffer overflow). Memory use grows if the writer cannot keep up.
- Extension 4 (latency experiments) is not implemented.
- The template's reader imports `std::os::unix::fs::FileExt`, which does not
  exist on Windows. A `cfg(windows)` shim based on `seek_read` in
  `src/util/aligned_sbe_reader.rs` lets the project build on Windows as well.
