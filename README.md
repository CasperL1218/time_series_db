# Financial Stream Processing Engine (Java)

## Overview
This project started as a proposal for an append-only, disk-backed time
series database. After discussing scope with Professor Immanuel Trummer
(CS 5321: Practicum in Database Systems, Cornell University), the
**pre-fall-break milestone was narrowed**: instead of building persistence
first, the first deliverable is a small **in-memory stream processing
engine** for simulated financial data, built around **sliding windows**.

Unlike a traditional database, where data sits still and queries come and
go, a stream processing engine keeps a **standing query** and updates its
answer as each new event arrives.

**Status:** sliding windows (V1) and filtering with AND/OR conditions
(V2 + V4) are implemented. Aggregation and persistence are later
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
                                         Filter  (interface)        -- rejected events are counted and dropped
                                   FieldFilter / AndFilter / OrFilter
                                            |  accepted
                                            v
                                         Window  (interface)        -- add() returns evicted points
                                        /        \
                                CountWindow      TimeWindow
                               (last N points)  (last T seconds)
                                            |
                                            v
                                     Aggregator(s)  (interface)     -- onInsert / onEvict (next milestone)
                                            |
                                            v
                                   display current contents
```

**Core**
- **`DataPoint`** — an immutable `(timestamp, symbol, price, quantity, side)`
  event. Data is never mutated once created, consistent with the project's
  append-only philosophy.
- **`FinancialDataGenerator`** — simulates a market feed: randomly picks a
  symbol, buy/sell side, quantity, and a price that random-walks per symbol
  so the series looks realistic.
- **`StreamEngine`** — the core engine. For each event,
  `processUpdate(DataPoint)` runs **filter → window insert/evict →
  aggregators → notify listener**. It returns whether the event was
  accepted and tracks how many events were processed and filtered out.

**Windows**
- **`Window`** — interface shared by both window types: `add()` inserts a
  point, evicts whatever no longer belongs, and **returns the evicted
  points** (so aggregators can stay in sync); `contents()` returns the
  current window, oldest first.
  - **`CountWindow`** — keeps the most recent `N` points in a `Deque`.
  - **`TimeWindow`** — keeps all points within `T` seconds of the latest
    point seen, in a `Deque`.

**Filtering**
- **`Filter`** — interface: `test(DataPoint)` decides whether an event
  enters the window. `Filter.ACCEPT_ALL` is the default (no filter).
- **`FieldFilter`** — one comparison on `symbol`, `side`, `price`, or
  `quantity` using `= != > < >= <=`.
- **`AndFilter` / `OrFilter`** — combine conditions (short-circuiting).
- **`FilterParser`** — turns a string like `"symbol=AAPL AND price>200"`
  into a `Filter`, with clear error messages for invalid input. Also
  renders any filter as an SQL `WHERE` clause.

**Aggregation (interface only — implementation is the next milestone)**
- **`Aggregator`** — `onInsert` / `onEvict` / `result()`, so aggregates
  like AVG or MAX are updated incrementally instead of rescanning the window.

**Entry points**
- **`Main`** — CLI driver for the window-only demo.
- **`FilterDemo`** — CLI driver for the filtered demo.

Everything lives in memory — no disk I/O and no query language. Per the
professor's guidance, the command-line arguments *are* the query.

### Build

Requires only a JDK (no external dependencies, no build tool needed):

```bash
rm -rf out
javac -d out $(find src/main/java src/test/java -name "*.java")
```

PowerShell equivalent:
```powershell
Remove-Item -Recurse -Force out -ErrorAction SilentlyContinue
javac -d out (Get-ChildItem -Recurse -Filter *.java src/main/java,src/test/java).FullName
```

### Run (window only)

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

### Run with a filter

Only trades that match the filter enter the window; everything else is
skipped and never takes up window space.

```bash
java -cp out com.tsdb.FilterDemo "<filter>" <count|time> <size> [intervalMs] [numEvents] [seed]
```

- `"<filter>"` — which trades to keep, in quotes (see syntax below). Use `""` for no filter.
- `count <N>` / `time <seconds>` — window type and size, same as above.
- `intervalMs` — delay between simulated events, in ms (default `400`).
- `numEvents` — number of events to simulate; `0` or omitted runs forever (Ctrl+C to stop).
- `seed` — optional random seed; the same seed produces the same trades every run.

Examples:
```bash
java -cp out com.tsdb.FilterDemo "symbol=AAPL" count 5                           # only AAPL trades, last 5
java -cp out com.tsdb.FilterDemo "symbol=AAPL AND price>200" time 10             # AAPL over $200, last 10s
java -cp out com.tsdb.FilterDemo "symbol=AAPL OR symbol=NVDA" count 5 300 30 42  # 30 events, repeatable
```

**Filter syntax**

| Part | Options |
|---|---|
| Fields | `symbol`, `side`, `price`, `quantity` (or `qty`) |
| Operators | `=` `!=` `>` `<` `>=` `<=` (text fields `symbol`/`side` only allow `=` and `!=`) |
| Combine | `AND`, `OR` — AND binds tighter than OR, like SQL. No parentheses. |
| Values | Case-insensitive; quotes optional (`symbol='AAPL'`); `side` must be `BUY` or `SELL` |

Each event is printed as `PASS` (entered the window) or `skip` (filtered
out), and the run starts by printing the equivalent SQL, e.g.:

```
Filter : symbol=AAPL AND price>200
SQL    : SELECT * FROM trades WHERE symbol = 'AAPL' AND price > 200
```

An invalid filter (e.g. `"price>abc"`) prints a clear error and exits.

### Tests

Self-contained test harnesses (no external test framework):

```bash
java -cp out com.tsdb.WindowTest           # window eviction            -> ALL TESTS PASSED
java -cp out com.tsdb.EngineContractTest   # filter -> window -> aggregators contract -> ALL CONTRACT TESTS PASSED
java -cp out com.tsdb.FilterTest           # operators, AND/OR, bad input -> ALL 64 FILTER TESTS PASSED
```

## Roadmap

```
VERSION 1  ✅ done
  Synthetic financial stream -> count/time sliding windows -> display

VERSION 2  ✅ done
  Filtering (e.g. symbol = AAPL, price > 200)

VERSION 3  (in progress)
  Aggregations over a window (avg, min, max, count), maintained incrementally

VERSION 4  ✅ done
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
