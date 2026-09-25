# Time Series Database (Java)

## Overview
This project implements an append-only time series database in Java, developed as part of CS 5321: Practicum in Database Systems, taught by Professor Immanuel Trummer at Cornell University. The system is designed around ticker/stock price data as its primary use case, but the underlying storage and query model generalizes to any time-stamped numeric data.

The team is composed of four members: Casper Liao, Daniel Zlotnick, Saanvi Ibrahimpatnam, and Jasmine Li.

## Design Goals
The database is built with the following principles in mind:

- **Append-only**: data points, once written, are never updated or deleted.
- **Time-ordered storage**: since writes always move forward in time, the system avoids the complexity of general-purpose mutable index structures (e.g. B+-trees) in favor of time-ordered, segmented storage.
- **Predicate pushdown on time**: queries prune irrelevant data early by using segment-level time metadata, rather than scanning the full dataset.

## Functionality

### 1. Data Ingestion
- Accepts write requests in the form `(series_id, timestamp, value)`.
- Supports batch ingestion of multiple points in a single call.
- Enforces append-only semantics; no update or delete operations are supported.

### 2. Persistent Storage
- Data is persisted to disk and survives process restarts.
- Storage is organized by series and partitioned into time-based segment files.
- Points are stored in time order within each segment, enabling efficient range scans and binary search by timestamp.

### 3. Basic Queries
- **Range query**: retrieve all points for a given series within a specified time interval.
- **Latest value**: retrieve the most recent point for a given series.

### 4. Advanced Queries
- **Windowed aggregation**: compute OHLC (open/high/low/close), average, min, max, or count over fixed time buckets — analogous to candlestick bars in financial charting.
- **Multi-series query**: retrieve and align data across multiple series over the same time range, enabling comparisons between tickers.

### 5. Query Execution Model
Each query is processed in three stages:
1. **Resolve** — look up the segment files associated with the requested series.
2. **Prune** — filter segments to those overlapping the requested time range (predicate pushdown), avoiding unnecessary disk reads.
3. **Scan and aggregate** — read only the relevant segments and compute the requested result (range output or bucketed aggregation).

## Out of Scope
The following are explicitly excluded from the current phase of the project:
- Updates and deletes
- Data compression/encoding
- Secondary indexing beyond time ordering
- Distribution or replication across multiple nodes

## Demo Plan
The team will demonstrate the system using a synthetic ticker data generator, showing end-to-end ingestion followed by a range query and a windowed aggregation query.
