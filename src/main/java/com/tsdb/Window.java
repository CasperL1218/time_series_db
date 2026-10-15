package com.tsdb;

import java.util.List;

/**
 * A sliding window over a stream of {@link DataPoint}s. Implementations
 * decide the eviction policy (e.g. keep the last N points, or keep points
 * from the last T seconds) but share the same contract: add() both inserts
 * the new point and evicts whatever no longer belongs.
 *
 * add() returns the evicted points so that incremental aggregators can
 * subtract them from their running state (see {@link Aggregator#onEvict}).
 */
public interface Window {

    /**
     * Insert a new point and evict anything that falls outside the window.
     *
     * @return the points evicted by this call, oldest first; an empty list if
     *         nothing was evicted. Never null.
     */
    List<DataPoint> add(DataPoint point);

    /** Current contents of the window, ordered oldest first. */
    List<DataPoint> contents();

    /** Number of points currently in the window. */
    int size();

    /** Human-readable description of the window's configuration, for display. */
    String describe();
}
