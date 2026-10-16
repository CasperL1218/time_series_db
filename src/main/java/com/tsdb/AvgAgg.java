package com.tsdb;

/** Running mean of a numeric field over the window. */
public final class AvgAgg extends FieldAggregator {

    private double sum = 0;
    private long n = 0;

    public AvgAgg(FieldFilter.Field field) { super(field); }

    @Override public void onInsert(DataPoint p) { sum += value(p); n++; }
    @Override public void onEvict(DataPoint p)  { sum -= value(p); n--; }

    @Override public double result() { return n == 0 ? Double.NaN : sum / n; }

    @Override protected String name() { return "AVG"; }
}
