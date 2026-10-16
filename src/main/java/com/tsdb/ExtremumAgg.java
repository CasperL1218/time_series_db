package com.tsdb;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Sliding-window min or max in amortised O(1) per event, using a monotonic
 * deque. The deque holds candidate points in insertion order with values
 * strictly improving towards the front, so the front is always the extremum.
 *
 * On insert, every point at the back that the new point beats (or ties) is
 * dropped: it is older and no better, so it can never be the extremum again.
 * On evict, the point is removed only if it is at the front; otherwise it was
 * already dropped as dominated. This relies on the engine's FIFO eviction
 * contract ({@link Aggregator#onEvict}).
 */
abstract class ExtremumAgg extends FieldAggregator {

    private final Deque<DataPoint> candidates = new ArrayDeque<>();

    protected ExtremumAgg(FieldFilter.Field field) { super(field); }

    /** True if {@code candidate} is at least as good as {@code incumbent}. */
    protected abstract boolean beatsOrTies(double candidate, double incumbent);

    @Override
    public void onInsert(DataPoint p) {
        double v = value(p);
        while (!candidates.isEmpty() && beatsOrTies(v, value(candidates.peekLast()))) {
            candidates.pollLast();
        }
        candidates.addLast(p);
    }

    @Override
    public void onEvict(DataPoint p) {
        if (candidates.peekFirst() == p) {
            candidates.pollFirst();
        }
    }

    @Override
    public double result() {
        return candidates.isEmpty() ? Double.NaN : value(candidates.peekFirst());
    }
}
