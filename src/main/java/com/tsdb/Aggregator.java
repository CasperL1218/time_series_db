package com.tsdb;

/**
 * An incrementally maintained aggregate over the current window contents.
 *
 * The engine calls onInsert() for every point that enters the window and
 * onEvict() for every point the window drops, in that order, so result()
 * always reflects exactly window.contents() -- without rescanning it.
 *
 * Owner: Person 2 (CountAgg, SumAgg, AvgAgg, MinAgg, MaxAgg, GroupByAggregator).
 */
public interface Aggregator {

    /** A point has entered the window. */
    void onInsert(DataPoint p);

    /** A point has left the window. Points are evicted oldest-first (FIFO). */
    void onEvict(DataPoint p);

    /** Current value over the window; Double.NaN if the window is empty. */
    double result();

    /** Display label, e.g. "AVG(price)". */
    String describe();
}
