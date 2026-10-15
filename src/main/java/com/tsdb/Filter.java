package com.tsdb;

/**
 * A predicate over incoming events. The engine applies the filter BEFORE the
 * window, so rejected events never occupy window space or reach aggregators.
 *
 * Owner: Person 1 (FieldFilter, AndFilter, OrFilter, FilterParser).
 */
public interface Filter {

    /** @return true if the point should enter the window. */
    boolean test(DataPoint p);

    /** Human-readable form for the display header, e.g. "symbol=AAPL AND price>200". */
    String describe();

    /** Default filter: lets every event through. */
    Filter ACCEPT_ALL = new Filter() {
        @Override public boolean test(DataPoint p) { return true; }
        @Override public String describe() { return "(none)"; }
    };
}
