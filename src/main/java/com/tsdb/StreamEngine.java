package com.tsdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * In-memory stream processing engine.
 *
 * Per event:  filter -> window insert/evict -> update aggregators -> notify.
 *
 *  - The {@link Filter} runs first; rejected events are counted and dropped.
 *  - The {@link Window} owns the eviction policy and reports what it evicted.
 *  - Each {@link Aggregator} is told about the insert and every eviction, so
 *    its result stays in sync with the window without rescanning it.
 *  - The listener (e.g. the CLI display) is notified only when an event was
 *    accepted, since otherwise nothing changed.
 */
public class StreamEngine {

    private final Window window;
    private final Filter filter;
    private final List<Aggregator> aggregators;
    private Consumer<Window> onUpdate;

    private long pointsProcessed = 0; // every event received
    private long pointsFiltered = 0;  // events rejected by the filter

    /** Window only: no filter, no aggregators (Version 1 behaviour). */
    public StreamEngine(Window window) {
        this(window, Filter.ACCEPT_ALL, Collections.emptyList());
    }

    public StreamEngine(Window window, Filter filter, List<Aggregator> aggregators) {
        if (window == null) throw new IllegalArgumentException("window must not be null");
        this.window = window;
        this.filter = filter == null ? Filter.ACCEPT_ALL : filter;
        this.aggregators = aggregators == null ? new ArrayList<>() : new ArrayList<>(aggregators);
    }

    /** Registers a callback invoked after each accepted update, with the window. */
    public void setOnUpdate(Consumer<Window> listener) {
        this.onUpdate = listener;
    }

    /**
     * Feeds one new event into the engine. Aggregators get onInsert for the
     * new point first, then onEvict for each point it pushed out, oldest first.
     *
     * @return true if the event passed the filter and entered the window.
     */
    public boolean processUpdate(DataPoint update) {
        pointsProcessed++;

        if (!filter.test(update)) {
            pointsFiltered++;
            return false;
        }

        List<DataPoint> evicted = window.add(update);

        for (Aggregator a : aggregators) {
            a.onInsert(update);
        }
        for (DataPoint e : evicted) {
            for (Aggregator a : aggregators) {
                a.onEvict(e);
            }
        }

        if (onUpdate != null) {
            onUpdate.accept(window);
        }
        return true;
    }

    public Window getWindow() { return window; }
    public Filter getFilter() { return filter; }
    public List<Aggregator> getAggregators() { return Collections.unmodifiableList(aggregators); }
    public long getPointsProcessed() { return pointsProcessed; }
    public long getPointsFiltered() { return pointsFiltered; }
}
