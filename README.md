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
- ✅ Incremental aggregation: COUNT, SUM, AVERAGE, MIN, MAX, and GROUP BY (V3)
- ✅ Interactive shell: `GENERATE DATA window=count size=20` starts the
  generator, then queries (`FILTER ticker = AAPL AND price < 200 AGGREGATE AVERAGE`,
  `ALSO FILTER ...`) run against the moving window from the same terminal
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

In this project the window type and size are the `window=` and `size=`
parameters of the shell's `GENERATE DATA` command (see
[Run the shell](#run-the-shell)).

### Architecture

Each `StreamEngine` runs one event through this pipeline:

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
                                    onUpdate listener               -- the shell's live view; only called for accepted events
```

The shell runs the generator on a background thread and feeds every trade
to **two** engines with the same window type and size:

```
                           +--> StreamEngine "all"    (no filter)           keeps every trade, so a new FILTER
FinancialDataGenerator ----+                                                can start from the whole window
  (background thread)      +--> StreamEngine "query"  (current filter       what WINDOW and the aggregates show;
                                                       + aggregators)       rebuilt by every query command
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
- **`FieldFilter`**: one comparison on `symbol` (also called `ticker`),
  `side`, `price`, or `quantity` using `= != > < >= <=`. Text values are
  compared without regard to case.
- **`AndFilter` / `OrFilter`**: combine conditions and stop evaluating as
  soon as the result is known (short-circuit).
- **`FilterParser`**: turns a condition like `"ticker = AAPL AND price < 200"`
  into a `Filter` and gives a clear error message for invalid input.

**Aggregation**
- **`Aggregator`**: an interface with `onInsert` / `onEvict` / `result()`
  / `describe()`. The engine calls `onInsert` for each accepted point, then
  `onEvict` for each point that insert pushed out (oldest first), so the
  result always matches the window without rescanning it. `result()` is
  `NaN` for an empty window.
- **`FieldAggregator`**: shared base for aggregators over one numeric
  field (`price` or `quantity`). It rejects text fields and builds the
  `NAME field` label, e.g. `AVERAGE price`.
- **`CountAgg`**, **`SumAgg`**, **`AvgAgg`**: running count / sum /
  average, O(1) per event. `CountAgg` returns `0` (not `NaN`) for an empty
  window.
- **`MinAgg`** / **`MaxAgg`**: sliding-window min / max built on
  **`ExtremumAgg`**, a monotonic deque that drops points the new point
  beats or ties. Amortised O(1) per event; relies on FIFO eviction.
- **`GroupByAggregator`**: one inner aggregator per distinct `symbol` or
  `side`, for the shell's `GROUP BY`. A group appears with its first point
  and disappears when its last point is evicted. `results()` returns a
  sorted map of group → value; `result()` throws, since there is no single
  value.

**Entry point**
- **`Shell`**: the interactive shell described below. It parses the
  commands (aggregates itself, conditions with `FilterParser`), runs the
  generator thread and the two engines, and prints the live views.

Everything runs in memory, with no disk I/O.

### Project Layout

```
src/main/java/com/tsdb/   shell, engine, windows, filters, aggregators, generator
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

### Run the shell

The shell runs the data generator and the queries in one terminal. The
generator works in the background. `WINDOW` and `AGGREGATE` are **live**:
they print their result right away and again as the window moves (the
window after every trade, an aggregate line whenever a value changes),
until you press **Enter** (an empty line) or type another command, which
replaces them. `GENERATE DATA` and a `FILTER` without `AGGREGATE` print one
line and return to the prompt.

```bash
java -cp out com.tsdb.Shell
```

| Command | What it does |
|---|---|
| `GENERATE DATA window=<count\|time> size=<n> [interval=<ms>] [events=<n>]` | Start generating trades in the background. Labels can come in any order, and spaces around `=` are fine. `window=count size=N` keeps the last `N` trades; `window=time size=S` keeps the last `S` seconds of trades. `interval`: about how often a trade arrives, in ms (default `400`). `events`: how many trades to make (`0` or omitted = until `STOP DATA`). |
| `WINDOW` | Watch the current query's window trade by trade: the trades that match the current filter. |
| `AGGREGATE <aggregates>` | Watch aggregates over the current query's window. Aggregates are `AVERAGE`, `SUM`, `MIN`, `MAX` (each optionally followed by `price` or `qty`; `price` is the default) and `COUNT`, separated by commas: `AGGREGATE AVERAGE, MAX qty, COUNT`. `AVG` works too. |
| `AGGREGATE <aggregates> GROUP BY symbol\|side` | One value per ticker (or per side) for every aggregate in the list: `AGGREGATE AVERAGE, COUNT GROUP BY symbol`. `ticker` works too. `GROUP BY` goes last. |
| `FILTER <condition> [AGGREGATE <aggregates>]` | Start a **new** query over everything in the window: only the matching trades, now and as they arrive. Prints how many trades match; with `AGGREGATE`, watches the aggregates. `FILTER NONE` goes back to the whole window. |
| `ALSO FILTER <condition> [AGGREGATE <aggregates>]` | **Narrow** the current query: after `FILTER ticker = AAPL`, `ALSO FILTER price < 200` gives `ticker = AAPL AND price < 200`. Keeps (and watches) the query's aggregates unless you give new ones. Can be repeated. |
| Enter (empty line) | Stop watching. The generator keeps running. |
| `STOP DATA` | Stop the generator. The window stays and can still be queried (each query then prints once); `GENERATE DATA` starts over with a new window. |
| `HELP`, `EXIT` | List the commands; quit. |

**Condition syntax**

| Part | Options |
|---|---|
| Fields | `ticker` (or `symbol`), `side`, `price`, `qty` (or `quantity`) |
| Operators | `=` (or `==`) `!=` `>` `<` `>=` `<=`. `ticker` and `side` only allow `=` and `!=` |
| Combine | `AND`, `OR`. AND binds tighter than OR. No parentheses. |
| Values | Case-insensitive; quotes are optional (`ticker = 'AAPL'`); no spaces inside a value; tickers are `AAPL`, `MSFT`, `GOOG`, `NVDA`, `AMZN`, `TSLA`; `side` is `BUY` or `SELL`; numbers must be finite |

**How queries work.** A query is a standing filter plus its aggregators,
running in the "query" engine, so the aggregates update incrementally
(`onInsert` / `onEvict`) as trades enter and leave its window. `FILTER`
builds the query from the "all" engine's window, so it always starts from
everything, no matter what the previous query was. `ALSO FILTER` and
`AGGREGATE` build it from the current query's trades. Trades the filter
rejects never enter the query's window and never evict anything, so with a
`count` window the query holds the last `<size>` *matching* trades, and
with a `time` window its trades only age out when a newer matching trade
arrives. A live aggregate line starts with the time of the newest trade in
the query's window, and nothing prints while the values stay the same.

Commands and keywords are case-insensitive. A misspelled ticker
(`FILTER ticker = APPL`) is an error instead of silently emptying the
query, and a command with a mistake typed while watching prints the error
and keeps watching. When the generator finishes its `events`, the prompt
comes back by itself. An empty window gives `NaN` for sum/average/min/max
and `0` for count.

The generator's simulated clock starts at the current time and moves
forward by a random amount between `interval/2` and `2*interval` per
trade, so trade timestamps advance at roughly real-time speed. Time
windows always use these trade timestamps, not the wall clock.

Example session. Lines marked `<-` are what was typed; the rest is output:

```
tsdb> GENERATE DATA window=count size=5 interval=60
Generating trades into a count-based window (last 5 point(s)), about one every 60 ms until STOP DATA. WINDOW shows it.

tsdb> WINDOW
Watching every trade in a count-based window (last 5 point(s)). Press Enter to stop.
Window (5 trade(s)):
  12:49:11.728 GOOG   BUY  $202.75     qty=249
  12:49:11.834 AAPL   BUY  $88.77      qty=259
  12:49:11.927 GOOG   BUY  $203.05     qty=347
  12:49:12.011 MSFT   SELL $87.49      qty=488
  12:49:12.058 AAPL   BUY  $88.71      qty=93

Incoming: 12:49:12.092 GOOG   SELL $202.06     qty=136
Window (5 trade(s)):
  12:49:11.834 AAPL   BUY  $88.77      qty=259
  ...
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA stops it.

tsdb> AGGREGATE AVERAGE, MAX qty, COUNT
Watching AVERAGE price, MAX quantity, COUNT of every trade in a count-based window (last 5 point(s)). A new line prints whenever a value changes. Press Enter to stop.
  12:49:12.137  AVERAGE price = 156.79   MAX quantity = 488   COUNT = 5
  12:49:12.240  AVERAGE price = 184.11   MAX quantity = 488   COUNT = 5
  12:49:12.356  AVERAGE price = 184.50   MAX quantity = 486   COUNT = 5
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA stops it.

tsdb> AGGREGATE AVERAGE, COUNT GROUP BY symbol
Watching AVERAGE price GROUP BY symbol, COUNT GROUP BY symbol of every trade in a count-based window (last 5 point(s)). A new line prints whenever a value changes. Press Enter to stop.
  12:49:12.484
      AVERAGE price GROUP BY symbol: AAPL 89.40   GOOG 202.22   MSFT 88.32   TSLA 339.68
      COUNT GROUP BY symbol: AAPL 1   GOOG 2   MSFT 1   TSLA 1
  12:49:12.549
      AVERAGE price GROUP BY symbol: AAPL 89.25   GOOG 201.80   MSFT 88.32   TSLA 339.68
      COUNT GROUP BY symbol: AAPL 2   GOOG 1   MSFT 1   TSLA 1
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA stops it.

tsdb> FILTER ticker = nvda or ticker = tsla
New query symbol=NVDA OR symbol=TSLA: 0 of the 5 trade(s) in the window match.

tsdb> ALSO FILTER side = buy AGGREGATE AVERAGE, COUNT
Query narrowed to (symbol=NVDA OR symbol=TSLA) AND side=BUY: 0 of 0 trade(s) still match.
Watching AVERAGE price, COUNT of (symbol=NVDA OR symbol=TSLA) AND side=BUY in a count-based window (last 5 point(s)). A new line prints whenever a value changes. Press Enter to stop.
  (empty)       AVERAGE price = NaN   COUNT = 0
  12:49:12.882  AVERAGE price = 338.75   COUNT = 1
ALSO FILTER price < 1000                           <- typed while watching; keeps the aggregates
Query narrowed to (symbol=NVDA OR symbol=TSLA) AND side=BUY AND price<1000: 1 of 1 trade(s) still match.
Watching AVERAGE price, COUNT of (symbol=NVDA OR symbol=TSLA) AND side=BUY AND price<1000 in a count-based window (last 5 point(s)). A new line prints whenever a value changes. Press Enter to stop.
  12:49:12.882  AVERAGE price = 338.75   COUNT = 1
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA stops it.

tsdb> STOP DATA
Stopped the generator after 36 trade(s). The window can still be queried; GENERATE DATA starts over.

tsdb> AGGREGATE MAX qty GROUP BY ticker
Showing MAX quantity GROUP BY symbol of (symbol=NVDA OR symbol=TSLA) AND side=BUY AND price<1000 in a count-based window (last 5 point(s)). The generator is stopped, so this won't change.
  12:49:12.882
      MAX quantity GROUP BY symbol: TSLA 5
```

### Tests

The tests are self-contained harnesses with no external test framework.
Each one exits with status 0 when everything passes and 1 when anything
fails:

```bash
java -cp out com.tsdb.WindowTest           # window eviction            -> ALL TESTS PASSED
java -cp out com.tsdb.EngineContractTest   # filter -> window -> aggregators contract -> ALL CONTRACT TESTS PASSED
java -cp out com.tsdb.FilterTest           # operators, AND/OR, bad input -> ALL 66 FILTER TESTS PASSED
java -cp out com.tsdb.AggregatorTest       # aggregates vs brute force, group by, shell aggregate syntax -> ALL 51 AGGREGATOR TESTS PASSED
```

## Roadmap

```
VERSION 1  ✅ done
  Synthetic financial stream -> count/time sliding windows -> display

VERSION 2  ✅ done
  Filtering (e.g. symbol = AAPL, price > 200)

VERSION 3  ✅ done
  Aggregations over a window (count, sum, average, min, max, group by), maintained incrementally

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
