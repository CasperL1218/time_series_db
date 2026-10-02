# Financial Stream Processing Engine (Java)

## Overview
This project started as a proposal for an append-only, disk-backed time
series database. After discussing scope with Professor Immanuel Trummer
(CS 5321: Practicum in Database Systems, Cornell University), the
**pre-fall-break milestone was narrowed**: instead of building persistence
first, the first deliverable is a small **in-memory stream processing
engine** for simulated financial data, built around **sliding windows**.
Persistence, filtering, aggregation, and querying are explicitly later
milestones (see [Roadmap](#roadmap) below).

The team is composed of four members: Casper Liao, Daniel Zlotnick, Saanvi
Ibrahimpatnam, and Jasmine Li.

## Current Milestone: Pre-Fall-Break Deliverable

> Build an in-memory Java stream-processing engine that consumes
> continuously generated synthetic financial events and maintains either a
> time-based or count-based sliding window. The window automatically adds
> new events and evicts expired ones, and continuously displays its current
> contents. Window type and size are configurable via command-line
> arguments.

### Architecture

```
FinancialDataGenerator  --generates-->  DataPoint
                                            |
                                            v
                                      StreamEngine.processUpdate(dp)
                                            |
                                            v
                                         Window  (interface)
                                        /        \
                                CountWindow      TimeWindow
                               (last N points)  (last T seconds)
                                            |
                                            v
                                   display current contents
```

- **`DataPoint`** — an immutable `(timestamp, symbol, price, quantity, side)`
  event. Data is never mutated once created, consistent with the project's
  append-only philosophy.
- **`FinancialDataGenerator`** — simulates a market feed: randomly picks a
  symbol, buy/sell side, quantity, and a price that random-walks per symbol
  so the series looks realistic.
- **`Window`** — interface shared by both window types: `add()` inserts a
  point and evicts whatever no longer belongs; `contents()` returns the
  current window, oldest first.
  - **`CountWindow`** — keeps the most recent `N` points in a `Deque`.
  - **`TimeWindow`** — keeps all points within `T` seconds of the latest
    point seen, in a `Deque`.
- **`StreamEngine`** — the core engine. `processUpdate(DataPoint)` feeds one
  event into the active window and notifies a listener (used here to print
  the window) after every update. This is the method the simulator calls
  for each generated event, and the integration point future milestones
  (filtering, aggregation) build on.
- **`Main`** — CLI driver: parses window type/size from `args`, wires the
  generator into the engine, and loops, printing each incoming event and the
  resulting window contents.

No disk I/O, SQL, or query language is involved yet — everything lives in
memory, per the professor's explicit guidance for this milestone.

### Build

Requires only a JDK (no external dependencies, no build tool needed):

```bash
javac -d out $(find src/main/java src/test/java -name "*.java")
```

PowerShell equivalent:
```powershell
javac -d out (Get-ChildItem -Recurse -Filter *.java src/main/java,src/test/java).FullName
```

### Run

```bash
java -cp out com.tsdb.Main <count|time> <size> [intervalMs] [numEvents]
```

- `count <N>` — keep the most recent `N` data points.
- `time <seconds>` — keep data points from the last `<seconds>` seconds.
- `intervalMs` — delay between simulated events, in ms (default `400`).
- `numEvents` — number of events to simulate; `0` or omitted runs forever
  (stop with Ctrl+C). Pass a positive number for a short, reproducible demo.

Examples:
```bash
java -cp out com.tsdb.Main count 5          # keep the last 5 trades, run until Ctrl+C
java -cp out com.tsdb.Main time 10          # keep the last 10 seconds of trades
java -cp out com.tsdb.Main count 5 50 8     # short, fast demo: 8 events, 50ms apart
```

Each tick prints the incoming event and the full current window, e.g.:

```
Incoming: 11:55:30.977 TSLA   BUY  $438.11     qty=140
Current window (2 point(s)):
  11:55:30.888 AAPL   BUY  $404.16     qty=391
  11:55:30.977 TSLA   BUY  $438.11     qty=140
```

### Tests

A small self-contained test harness (no external test framework) checks
eviction semantics for both window types and the engine's notify/count
behavior:

```bash
java -cp out com.tsdb.WindowTest
```

## Roadmap

```
VERSION 1 (this milestone)
  Synthetic financial stream -> count/time sliding windows -> display

VERSION 2
  Filtering (e.g. symbol == "AAPL", price > 200)

VERSION 3
  Aggregations over a window (avg, min, max, count)

VERSION 4
  Compound filter conditions (AND/OR)

VERSION 5 / final project
  Persistence to disk, larger datasets, real financial data,
  more advanced queries (range queries, OHLC bucketed aggregation,
  multi-series alignment) — see "Original Proposal" below.
```

## Original Proposal (Future Direction)

The original project pitch — retained here as the long-term vision this
milestone builds toward, once persistence becomes the focus again.

### Design Goals
- **Append-only**: data points, once written, are never updated or deleted.
- **Time-ordered storage**: since writes always move forward in time, the
  system avoids the complexity of general-purpose mutable index structures
  (e.g. B+-trees) in favor of time-ordered, segmented storage.
- **Predicate pushdown on time**: queries prune irrelevant data early by
  using segment-level time metadata, rather than scanning the full dataset.

### Functionality

#### 1. Data Ingestion
- Accepts write requests in the form `(series_id, timestamp, value)`.
- Supports batch ingestion of multiple points in a single call.
- Enforces append-only semantics; no update or delete operations are supported.

#### 2. Persistent Storage
- Data is persisted to disk and survives process restarts.
- Storage is organized by series and partitioned into time-based segment files.
- Points are stored in time order within each segment, enabling efficient
  range scans and binary search by timestamp.

#### 3. Basic Queries
- **Range query**: retrieve all points for a given series within a specified
  time interval.
- **Latest value**: retrieve the most recent point for a given series.

#### 4. Advanced Queries
- **Windowed aggregation**: compute OHLC (open/high/low/close), average,
  min, max, or count over fixed time buckets — analogous to candlestick
  bars in financial charting.
- **Multi-series query**: retrieve and align data across multiple series
  over the same time range, enabling comparisons between tickers.

#### 5. Query Execution Model
Each query is processed in three stages:
1. **Resolve** — look up the segment files associated with the requested series.
2. **Prune** — filter segments to those overlapping the requested time
   range (predicate pushdown), avoiding unnecessary disk reads.
3. **Scan and aggregate** — read only the relevant segments and compute the
   requested result (range output or bucketed aggregation).

### Out of Scope (for now)
- Updates and deletes
- Data compression/encoding
- Secondary indexing beyond time ordering
- Distribution or replication across multiple nodes
</content>
