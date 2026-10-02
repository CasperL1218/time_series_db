package com.tsdb;

import java.util.function.Consumer;

/**
 * Minimal in-memory stream processing engine.
 *
 * Each incoming {@link DataPoint} is pushed into the active {@link Window},
 * which owns the eviction policy (count-based or time-based). After every
 * update the engine notifies an optional listener so callers (e.g. the CLI
 * driver) can display the window's current contents.
 *
 * This is intentionally the whole engine for the pre-fall-break milestone:
 * no persistence, no filtering, no aggregation yet. Those are later versions
 * layered on top of processUpdate()/Window without changing this contract.
 */
public class StreamEngine {

    private final Window window;
    private Consumer<Window> onUpdate;
    private long pointsProcessed = 0;

    public StreamEngine(Window window) {
        this.window = window;
    }

    /** Registers a callback invoked after each processed update, with the window. */
    public void setOnUpdate(Consumer<Window> listener) {
        this.onUpdate = listener;
    }

    /** Feeds one new event into the engine: insert, evict, notify. */
    public void processUpdate(DataPoint update) {
        window.add(update);
        pointsProcessed++;
        if (onUpdate != null) {
            onUpdate.accept(window);
        }
    }

    public Window getWindow() {
        return window;
    }

    public long getPointsProcessed() {
        return pointsProcessed;
    }
}
