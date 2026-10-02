package com.tsdb;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Keeps only the most recent {@code capacity} data points. When a new point
 * arrives and the window is already full, the oldest point is evicted.
 */
public class CountWindow implements Window {

    private final int capacity;
    private final Deque<DataPoint> buffer = new ArrayDeque<>();

    public CountWindow(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, got " + capacity);
        }
        this.capacity = capacity;
    }

    @Override
    public synchronized void add(DataPoint point) {
        buffer.addLast(point);
        while (buffer.size() > capacity) {
            buffer.removeFirst();
        }
    }

    @Override
    public synchronized List<DataPoint> contents() {
        return new ArrayList<>(buffer);
    }

    @Override
    public String describe() {
        return "count-based window (last " + capacity + " point(s))";
    }
}
