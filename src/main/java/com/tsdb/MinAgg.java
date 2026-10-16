package com.tsdb;

/** Minimum of a numeric field over the window. */
public final class MinAgg extends ExtremumAgg {

    public MinAgg(FieldFilter.Field field) { super(field); }

    @Override protected boolean beatsOrTies(double candidate, double incumbent) { return candidate <= incumbent; }

    @Override protected String name() { return "MIN"; }
}
