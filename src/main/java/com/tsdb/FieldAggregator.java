package com.tsdb;

/**
 * Base for aggregators over one numeric field (price or quantity) of each
 * {@link DataPoint}. Subclasses maintain their running state; this class
 * owns the field choice, the value lookup and the "NAME(field)" label.
 */
abstract class FieldAggregator implements Aggregator {

    protected final FieldFilter.Field field;

    protected FieldAggregator(FieldFilter.Field field) {
        if (field == null || !field.numeric) {
            throw new IllegalArgumentException(name() + " needs a numeric field (price or quantity), got "
                    + (field == null ? "null" : field.label()));
        }
        this.field = field;
    }

    /** The aggregate's SQL-style name, e.g. "AVG". */
    protected abstract String name();

    /** The numeric value of this aggregator's field for the given point. */
    protected final double value(DataPoint p) {
        switch (field) {
            case PRICE:    return p.getPrice();
            case QUANTITY: return p.getQuantity();
            default: throw new AssertionError(field);
        }
    }

    @Override
    public String describe() {
        return name() + "(" + field.label() + ")";
    }

    public FieldFilter.Field getField() { return field; }
}
