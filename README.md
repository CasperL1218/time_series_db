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

**Status:**
- ✅ Sliding windows, count-based and time-based (V1)
- ✅ Filtering, with compound AND/OR conditions (V2 + V4)
- 🚧 Aggregation (V3): the `Aggregator` interface is wired into the engine
  and covered by contract tests, but no concrete aggregators (AVG, MIN,
  MAX, ...) exist yet
- ⏳ Persistence is a later milestone (see [Roadmap](#roadmap))

The team has four members: Casper Liao, Daniel Zlotnick, Saanvi
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
                                     Aggregator(s)  (interface)     -- onInsert(new), then onEvict(each evicted)
                                            |
                                            v
                              onUpdate listener (e.g. CLI display)  -- only called for accepted events
```

**Core**
- **`DataPoint`**: an immutable `(timestamp, symbol, price, quantity, side)`
  trade event, where `side` is `BUY` or `SELL`. A point is never changed
  after it is created, which fits the project's append-only design.
- **`FinancialDataGenerator`**: simulates a market feed. Each call to
  `next()` picks one of `AAPL, MSFT, GOOG, NVDA, AMZN, TSLA`, a side, a
  quantity in `[1, 500]`, and a price that does a ±$1 random walk per
  symbol (starting somewhere in $50–$500). The generator keeps its own
  **simulated clock**, which moves forward by a random delta in
  `[minDelta, maxDelta)` ms per event. Pass it a `Random` with a fixed
  seed and it produces the same sequence every time.
- **`StreamEngine`**: the core engine. For each event,
  `processUpdate(DataPoint)` runs **filter → window insert/evict →
  aggregators → notify listener**. It returns `true` if the event passed
  the filter, and it counts how many events were processed in total and
  how many the filter rejected.

**Windows**
- **`Window`**: the interface both window types share. `add()` inserts a
  point, evicts whatever no longer belongs, and **returns the evicted
  points**, oldest first, so aggregators can stay in sync. `contents()`
  returns a copy of the current window, oldest first.
  - **`CountWindow`**: keeps the most recent `N` points in a `Deque`.
  - **`TimeWindow`**: keeps every point whose timestamp is within `T` ms
    of the **latest point's timestamp**, not wall-clock time. The boundary
    is inclusive, so a point exactly `T` ms old stays in the window.

**Filtering**
- **`Filter`**: an interface whose `test(DataPoint)` decides whether an
  event enters the window. `Filter.ACCEPT_ALL` is the default (no filter).
- **`FieldFilter`**: one comparison on `symbol`, `side`, `price`, or
  `quantity` using `= != > < >= <=`. Text values are compared without
  regard to case.
- **`AndFilter` / `OrFilter`**: combine conditions and stop evaluating as
  soon as the result is known (short-circuit).
- **`FilterParser`**: turns a string like `"symbol=AAPL AND price>200"`
  into a `Filter` and gives a clear error message for invalid input.
  `FilterParser.toSql(filter)` writes any filter out as an SQL `WHERE`
  clause.

**Aggregation (interface only; implementations are the next milestone)**
- **`Aggregator`**: `onInsert` / `onEvict` / `result()` / `describe()`.
  This lets aggregates like AVG or MAX update incrementally instead of
  rescanning the window. `StreamEngine` already calls these methods; no
  concrete aggregator classes exist yet.

**Entry points**
- **`Main`**: CLI driver for the window-only demo (no filter).
- **`FilterDemo`**: CLI driver for the filtered demo.

Everything runs in memory, with no disk I/O and no query language. Per the
professor's guidance, the command-line arguments *are* the query.

### Project Layout

```
src/main/java/com/tsdb/   engine, windows, filters, generator, CLI drivers
src/test/java/com/tsdb/   self-contained test harnesses (each has a main())
out/                      compiled classes (git-ignored)
```

### Build

You only need a **JDK 16 or newer** (the tests use `Stream.toList()`).
There are no external dependencies and no build tool:

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

- `count <N>`: keep the most recent `N` data points.
- `time <seconds>`: keep data points from the last `<seconds>` seconds of
  event time.
- `intervalMs`: real delay between simulated events, in ms (default `400`).
- `numEvents`: number of events to simulate. `0` or omitted runs forever
  (stop with Ctrl+C); pass a positive number for a short demo.

The simulated clock starts at the current time and moves forward by a
random amount between `intervalMs/2` and `2*intervalMs` per event. Event
timestamps therefore advance at roughly the same rate as real time, but
not exactly. Time windows always use these event timestamps, not the wall
clock.

Examples:
```bash
java -cp out com.tsdb.Main count 5          # keep the last 5 trades, run until Ctrl+C
java -cp out com.tsdb.Main time 10          # keep the last 10 seconds of trades
java -cp out com.tsdb.Main count 5 50 8     # short, fast demo: 8 events, 50ms apart
```

Each tick prints the incoming event and the full current window:

```
Stream engine started: count-based window (last 2 point(s))
Simulating 3 event(s).

Incoming: 10:18:03.797 AMZN   BUY  $144.18     qty=39
Current window (1 point(s)):
  10:18:03.797 AMZN   BUY  $144.18     qty=39

...

Shutting down. Processed 3 event(s).
```

### Run with a filter

Only trades that match the filter enter the window. Every other trade is
skipped, so it never takes up window space and never evicts anything.

```bash
java -cp out com.tsdb.FilterDemo "<filter>" <count|time> <size> [intervalMs] [numEvents] [seed]
```

- `"<filter>"`: which trades to keep, in quotes (syntax below). Use `""`
  for no filter.
- `count <N>` / `time <seconds>`: window type and size, same as above.
- `intervalMs`: real delay between simulated events, in ms (default `400`).
- `numEvents`: number of events to simulate. `0` or omitted runs forever
  (Ctrl+C to stop).
- `seed`: optional random seed. The same seed produces the same sequence
  of symbols, sides, prices, and quantities every run. Timestamps still
  start at the current time, so they differ between runs.

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
| Operators | `=` (or `==`) `!=` `>` `<` `>=` `<=`. The text fields `symbol` and `side` only allow `=` and `!=` |
| Combine | `AND`, `OR`. AND binds tighter than OR, like SQL. No parentheses. |
| Values | Case-insensitive; quotes are optional (`symbol='AAPL'`); no spaces inside a value; `side` must be `BUY` or `SELL`; numbers must be finite |

The run first prints the window, the filter, and the equivalent SQL. It
then marks each event `PASS` (entered the window, followed by the
window's contents) or `skip` (filtered out), and ends with a summary:

```
Window : count-based window (last 2 point(s))
Filter : symbol=AAPL OR symbol=NVDA
SQL    : SELECT * FROM trades WHERE symbol = 'AAPL' OR symbol = 'NVDA'

  PASS  10:18:03.970 AAPL   SELL $377.17     qty=33
        window now holds 1 point(s):
          10:18:03.970 AAPL   SELL $377.17     qty=33
  skip  10:18:03.986 TSLA   SELL $457.36     qty=227
  ...

Processed 5, passed 1, filtered out 4.
```

An invalid filter prints an error and exits with status 1. For example,
`"price>abc"` prints `Bad filter: price must be compared to a number, got 'abc'`.

### Tests

The tests are self-contained harnesses with no external test framework.
Each one exits with status 0 when everything passes and 1 when anything
fails:

```bash
java -cp out com.tsdb.WindowTest           # window eviction + engine notification  -> ALL TESTS PASSED
java -cp out com.tsdb.EngineContractTest   # filter -> window -> aggregator contract -> ALL CONTRACT TESTS PASSED
java -cp out com.tsdb.FilterTest           # operators, AND/OR, parsing, bad input   -> ALL 64 FILTER TESTS PASSED
```

## Roadmap

```
VERSION 1  ✅ done
  Synthetic financial stream -> count/time sliding windows -> display

VERSION 2  ✅ done
  Filtering (e.g. symbol = AAPL, price > 200)

VERSION 3  🚧 in progress
  Aggregations over a window (avg, min, max, count), maintained incrementally
  (Aggregator interface + engine wiring done; implementations pending)

VERSION 4  ✅ done
  Compound filter conditions (AND/OR)

VERSION 5 / final project
  Persistence to disk, larger datasets, real financial data,
  more advanced queries (range queries, OHLC bucketed aggregation,
  multi-series alignment). See "Original Proposal" below.
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
