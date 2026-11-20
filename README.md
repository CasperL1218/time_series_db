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
- ✅ Interactive shell with a SQL-like language: `GENERATE DATA window=count size=20;`
  starts the generator, `SELECT AVERAGE, COUNT WHERE ticker = AAPL AND price < 200;`
  defines a query over the moving window, `WINDOW;` watches it live, and
  `ALSO WHERE side = BUY;` adds to it
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
                           +--> StreamEngine "all"    (no filter)           keeps every trade, so a new SELECT
FinancialDataGenerator ----+                                                can start from the whole window
  (background thread)      +--> StreamEngine "query"  (current filter       what WINDOW shows; rebuilt by
                                                       + aggregators)       every SELECT and ALSO
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
  `;`-terminated statements (the `SELECT` / `WHERE` / `GROUP BY` clauses
  itself, `WHERE` conditions with `FilterParser`), runs the
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
generator works in the background. The query language reads like SQL:
`SELECT` defines a query, `WINDOW` watches it, and `ALSO` adds to it.

- **Every statement ends with `;`** and can span several lines. While a
  statement is unfinished, the prompt changes from `tsdb>` to `->`.
- **`SELECT` starts a new query from scratch**, over everything in the
  window. **`ALSO` adds to the current query** instead of starting over.
- A query only defines what to look at and prints how many trades match.
  **`WINDOW;` watches it live**: the matching trades after every trade, or
  a line of aggregate values whenever a value changes. It keeps going until
  you press **Enter** (an empty line) or type another statement.

```bash
java -cp out com.tsdb.Shell
```

| Statement | What it does |
|---|---|
| `GENERATE DATA window=<count\|time> size=<n> [interval=<ms>] [events=<n>];` | Start generating trades in the background. Labels can come in any order, and spaces around `=` are fine. `window=count size=N` keeps the last `N` trades; `window=time size=S` keeps the last `S` seconds of trades. `interval`: about how often a trade arrives, in ms (default `400`). `events`: how many trades to make (`0` or omitted = until `STOP DATA`). The query starts as `SELECT *`. |
| `SELECT * [WHERE <condition>];` | A **new** query: the trades that match the condition (or every trade without `WHERE`), now and as they arrive. |
| `SELECT <aggregates> [WHERE <condition>] [GROUP BY symbol\|side];` | A **new** query computing aggregates over the matching trades. Aggregates are `AVERAGE`, `SUM`, `MIN`, `MAX` (each optionally followed by `price` or `qty`; `price` is the default) and `COUNT`, separated by commas, e.g. `SELECT AVERAGE, MAX qty, COUNT;`. `AVG` works too. `GROUP BY` gives one value per ticker (or per side) for every aggregate; `ticker` works too. |
| `ALSO [SELECT <aggregates>] [WHERE <condition>] [GROUP BY symbol\|side];` | **Add to** the current query, using at least one clause. `WHERE` narrows it: after `SELECT * WHERE ticker = AAPL;`, `ALSO WHERE price < 200;` gives `ticker = AAPL AND price < 200`. `SELECT` adds aggregates to the ones it has. `GROUP BY` replaces the grouping. Can be repeated. |
| `WINDOW;` | Watch the current query live. |
| Enter (empty line) | Stop watching. The generator keeps running. |
| `STOP DATA;` | Stop the generator. The window stays and can still be queried (`WINDOW` then prints once); `GENERATE DATA` starts over with a new window. |
| `HELP;`, `EXIT` | List the statements; quit. |

Clauses go in the order `SELECT`, `WHERE`, `GROUP BY`, as in SQL. The
full grammar (keywords are case-insensitive; line breaks count as spaces):

```
statement  := ( generate | select | also | WINDOW | STOP DATA | HELP ) ";"   |   EXIT
generate   := GENERATE DATA window=(count|time) size=<n> [interval=<ms>] [events=<n>]
select     := SELECT ( "*" | aggregates ) [WHERE condition] [GROUP BY group]
also       := ALSO [SELECT aggregates] [WHERE condition] [GROUP BY group]     -- at least one clause
aggregates := aggregate ( "," aggregate )*
aggregate  := (AVERAGE | AVG | SUM | MIN | MAX) [price | qty]   |   COUNT
group      := symbol | ticker | side
condition  := see "Condition syntax" below
```

**Coming from the old commands**

| Old | New |
|---|---|
| `FILTER ticker = AAPL` | `SELECT * WHERE ticker = AAPL;` |
| `FILTER ticker = AAPL AGGREGATE AVERAGE, COUNT` | `SELECT AVERAGE, COUNT WHERE ticker = AAPL;` then `WINDOW;` |
| `AGGREGATE AVERAGE GROUP BY symbol` | `SELECT AVERAGE GROUP BY symbol;` (starts over; use `ALSO SELECT AVERAGE;` to keep the current filter) |
| `ALSO FILTER price < 200` | `ALSO WHERE price < 200;` |
| `FILTER NONE` | `SELECT *;` |
| `WINDOW` (always listed trades) | `WINDOW;` (shows the query's trades, or its aggregates if it has any) |

Typing an old command prints an error that shows the new form.

**Condition syntax** (after `WHERE`)

| Part | Options |
|---|---|
| Fields | `ticker` (or `symbol`), `side`, `price`, `qty` (or `quantity`) |
| Operators | `=` (or `==`) `!=` `>` `<` `>=` `<=`. `ticker` and `side` only allow `=` and `!=` |
| Combine | `AND`, `OR`. AND binds tighter than OR. No parentheses. |
| Values | Case-insensitive; quotes are optional (`ticker = 'AAPL'`); no spaces inside a value; tickers are `AAPL`, `MSFT`, `GOOG`, `NVDA`, `AMZN`, `TSLA`; `side` is `BUY` or `SELL`; numbers must be finite |

**How queries work.** A query is a standing filter plus its aggregators,
running in the "query" engine, so the aggregates update incrementally
(`onInsert` / `onEvict`) as trades enter and leave its window. `SELECT`
builds the query from the "all" engine's window, so it always starts from
everything, no matter what the previous query was. `ALSO` builds it from
the current query's trades. Trades the filter rejects never enter the
query's window and never evict anything, so with a `count` window the
query holds the last `<size>` *matching* trades, and with a `time` window
its trades only age out when a newer matching trade arrives. A live
aggregate line starts with the time of the newest trade in the query's
window, and nothing prints while the values stay the same.

Keywords are case-insensitive. A misspelled ticker
(`WHERE ticker = APPL`) is an error instead of silently emptying the
query, and a statement with a mistake typed while watching prints the
error and keeps watching. A statement that fails changes nothing. When the
generator finishes its `events`, the prompt comes back by itself. An empty
window gives `NaN` for sum/average/min/max and `0` for count.

The generator's simulated clock starts at the current time and moves
forward by a random amount between `interval/2` and `2*interval` per
trade, so trade timestamps advance at roughly real-time speed. Time
windows always use these trade timestamps, not the wall clock.

Example session. Lines marked `<-` are what was typed; the rest is output:

```
tsdb> GENERATE DATA window=count size=5 interval=80;
Generating trades into a count-based window (last 5 point(s)), about one every 80 ms until STOP DATA. The query starts as SELECT *; WINDOW; watches it.

tsdb> WINDOW;
Watching SELECT * over a count-based window (last 5 point(s)). Press Enter to stop.
Window (5 trade(s)):
  11:50:07.737 MSFT   SELL $157.75     qty=237
  11:50:07.823 GOOG   SELL $369.51     qty=475
  11:50:07.912 AMZN   SELL $472.78     qty=33
  11:50:08.005 AAPL   BUY  $131.32     qty=453
  11:50:08.134 AAPL   BUY  $131.36     qty=108

Incoming: 11:50:08.215 GOOG   SELL $368.97     qty=418
Window (5 trade(s)):
  11:50:07.823 GOOG   SELL $369.51     qty=475
  ...
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA; stops it.

tsdb> SELECT AVERAGE, COUNT
   ->   WHERE ticker = NVDA OR ticker = TSLA
   ->   GROUP BY symbol;
New query SELECT AVERAGE price, COUNT WHERE symbol=NVDA OR symbol=TSLA GROUP BY symbol: 0 of the 5 trade(s) in the window match. WINDOW; watches it.

tsdb> WINDOW;
Watching SELECT AVERAGE price, COUNT WHERE symbol=NVDA OR symbol=TSLA GROUP BY symbol over a count-based window (last 5 point(s)). A new line prints whenever a value changes. Press Enter to stop.
  (empty)
      AVERAGE price GROUP BY symbol: (no trades)
      COUNT GROUP BY symbol: (no trades)
  11:50:08.664
      AVERAGE price GROUP BY symbol: TSLA 103.88
      COUNT GROUP BY symbol: TSLA 1
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA; stops it.

tsdb> ALSO WHERE side = buy;
Query is now SELECT AVERAGE price, COUNT WHERE (symbol=NVDA OR symbol=TSLA) AND side=BUY GROUP BY symbol: 0 of 1 trade(s) still match. WINDOW; watches it.

tsdb> ALSO SELECT MAX qty;
Query is now SELECT AVERAGE price, COUNT, MAX quantity WHERE (symbol=NVDA OR symbol=TSLA) AND side=BUY GROUP BY symbol: 0 of 0 trade(s) still match. WINDOW; watches it.

tsdb> WINDOW;
Watching SELECT AVERAGE price, COUNT, MAX quantity WHERE (symbol=NVDA OR symbol=TSLA) AND side=BUY GROUP BY symbol over a count-based window (last 5 point(s)). A new line prints whenever a value changes. Press Enter to stop.
  (empty)
      AVERAGE price GROUP BY symbol: (no trades)
      COUNT GROUP BY symbol: (no trades)
      MAX quantity GROUP BY symbol: (no trades)
  11:50:09.433
      AVERAGE price GROUP BY symbol: NVDA 467.02
      COUNT GROUP BY symbol: NVDA 1
      MAX quantity GROUP BY symbol: NVDA 63
                                                   <- Enter
Stopped watching. The generator keeps running; STOP DATA; stops it.

tsdb> STOP DATA;
Stopped the generator after 22 trade(s). The window can still be queried; GENERATE DATA starts over.

tsdb> SELECT COUNT, MIN, MAX;
New query SELECT COUNT, MIN price, MAX price: 5 of the 5 trade(s) in the window match. WINDOW; watches it.

tsdb> WINDOW;
Showing SELECT COUNT, MIN price, MAX price over a count-based window (last 5 point(s)). The generator is stopped, so this won't change.
  11:50:09.617  COUNT = 5   MIN price = 132.76   MAX price = 472.30
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
  SQL-like shell language: SELECT ... WHERE ... GROUP BY ...;  ALSO to refine;  WINDOW to watch

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
