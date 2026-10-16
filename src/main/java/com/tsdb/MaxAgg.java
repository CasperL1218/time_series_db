package com.tsdb;

/** Maximum of a numeric field over the window. */
public final class MaxAgg extends ExtremumAgg {

    public MaxAgg(FieldFilter.Field field) { super(field); }

    @Override protected boolean beatsOrTies(double candidate, double incumbent) { return candidate >= incumbent; }

    @Override protected String name() { return "MAX"; }
}
