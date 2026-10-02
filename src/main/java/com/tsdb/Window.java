package com.tsdb;

import java.util.List;

/**
 * A sliding window over a stream of {@link DataPoint}s. Implementations
 * decide the eviction policy (e.g. keep the last N points, or keep points
 * from the last T seconds) but share the same contract: add() both inserts
 * the new point and evicts whatever no longer belongs.
 */
public interface Window {

    /** Insert a new point and evict anything that falls outside the window. */
    void add(DataPoint point);

    /** Current contents of the window, ordered oldest first. */
    List<DataPoint> contents();

    /** Human-readable description of the window's configuration, for display. */
    String describe();
}
