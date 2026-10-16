package com.tsdb;

/** Number of points currently in the window. */
public final class CountAgg implements Aggregator {

    private long n = 0;

    @Override public void onInsert(DataPoint p) { n++; }
    @Override public void onEvict(DataPoint p)  { n--; }

    /** Returns 0 (not NaN) for an empty window, since a count of nothing is a real number. */
    @Override public double result() { return n; }

    @Override public String describe() { return "COUNT(*)"; }
}
