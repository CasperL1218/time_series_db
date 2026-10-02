package com.tsdb;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Keeps all data points whose timestamp is within {@code windowMillis} of the
 * most recently added point. "Now" is defined as the timestamp of the latest
 * point seen, rather than wall-clock time, so the window behaves correctly
 * for both live and synthetic/replayed streams.
 */
public class TimeWindow implements Window {

    private final long windowMillis;
    private final Deque<DataPoint> buffer = new ArrayDeque<>();

    public TimeWindow(long windowMillis) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive, got " + windowMillis);
        }
        this.windowMillis = windowMillis;
    }

    @Override
    public synchronized void add(DataPoint point) {
        buffer.addLast(point);
        long cutoff = point.getTimestampMillis() - windowMillis;
        while (!buffer.isEmpty() && buffer.peekFirst().getTimestampMillis() < cutoff) {
            buffer.removeFirst();
        }
    }

    @Override
    public synchronized List<DataPoint> contents() {
        return new ArrayList<>(buffer);
    }

    @Override
    public String describe() {
        return "time-based window (last " + (windowMillis / 1000.0) + "s)";
    }
}
