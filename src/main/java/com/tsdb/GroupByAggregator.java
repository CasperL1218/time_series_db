package com.tsdb;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Runs one inner aggregator per distinct value of a text field (symbol or
 * side), like SQL {@code SELECT AVG(price) ... GROUP BY symbol}.
 *
 * Groups appear when their first point enters the window and disappear when
 * their last point is evicted, so {@link #results()} lists exactly the groups
 * present in the window.
 *
 * Because a grouped aggregate has one value per group rather than one value,
 * {@link #result()} is not meaningful and throws; callers must use
 * {@link #results()} instead.
 */
public final class GroupByAggregator implements Aggregator {

    private static final class Group {
        final Aggregator agg;
        long n = 0;
        Group(Aggregator agg) { this.agg = agg; }
    }

    private final FieldFilter.Field groupField;
    private final Supplier<Aggregator> factory;
    private final String innerLabel;
    private final SortedMap<String, Group> groups = new TreeMap<>();

    /**
     * @param groupField the text field to group on (symbol or side)
     * @param factory    creates a fresh inner aggregator for each new group
     */
    public GroupByAggregator(FieldFilter.Field groupField, Supplier<Aggregator> factory) {
        if (groupField == null || groupField.numeric) {
            throw new IllegalArgumentException("GROUP BY needs a text field (symbol or side), got "
                    + (groupField == null ? "null" : groupField.label()));
        }
        if (factory == null) throw new IllegalArgumentException("factory must not be null");
        this.groupField = groupField;
        this.factory = factory;
        this.innerLabel = factory.get().describe();
    }

    private String keyOf(DataPoint p) {
        switch (groupField) {
            case SYMBOL: return p.getSymbol();
            case SIDE:   return p.getSide().name();
            default: throw new AssertionError(groupField);
        }
    }

    @Override
    public void onInsert(DataPoint p) {
        Group g = groups.computeIfAbsent(keyOf(p), k -> new Group(factory.get()));
        g.agg.onInsert(p);
        g.n++;
    }

    @Override
    public void onEvict(DataPoint p) {
        String key = keyOf(p);
        Group g = groups.get(key);
        if (g == null) {
            throw new IllegalStateException("evicted point for unknown group '" + key + "': " + p);
        }
        g.agg.onEvict(p);
        if (--g.n == 0) groups.remove(key);
    }

    /** Not supported: a grouped aggregate has one value per group. Use {@link #results()}. */
    @Override
    public double result() {
        throw new UnsupportedOperationException(describe() + " has one result per group; call results()");
    }

    /** Current value per group, keyed by group value in sorted order. */
    public SortedMap<String, Double> results() {
        SortedMap<String, Double> out = new TreeMap<>();
        for (var e : groups.entrySet()) out.put(e.getKey(), e.getValue().agg.result());
        return Collections.unmodifiableSortedMap(out);
    }

    @Override
    public String describe() {
        return innerLabel + " GROUP BY " + groupField.label();
    }

    public FieldFilter.Field getGroupField() { return groupField; }
    public String getInnerLabel() { return innerLabel; }
}
