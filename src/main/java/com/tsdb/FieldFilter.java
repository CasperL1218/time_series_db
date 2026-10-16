package com.tsdb;

import java.util.Locale;

/**
 * A single comparison between one field of a {@link DataPoint} and a constant,
 * example: {@code symbol=AAPL}, {@code side!=SELL}, {@code price>200},
 * {@code quantity<=100}.
 *
 * Text fields (symbol, side) support only = and !=.
 * Numeric fields (price, quantity) support = != > < >= <=.
 */
public final class FieldFilter implements Filter {

    public enum Field {
        SYMBOL(false), SIDE(false), PRICE(true), QUANTITY(true);

        final boolean numeric;
        Field(boolean numeric) { this.numeric = numeric; }

        /** Accepts "symbol", "side", "price", "quantity" or "qty". */
        public static Field fromName(String name) {
            switch (name.trim().toLowerCase(Locale.ROOT)) {
                case "symbol":   return SYMBOL;
                case "side":     return SIDE;
                case "price":    return PRICE;
                case "quantity":
                case "qty":      return QUANTITY;
                default:
                    throw new IllegalArgumentException("Unknown field '" + name
                            + "'. Expected one of: symbol, side, price, quantity");
            }
        }

        public String label() { return name().toLowerCase(Locale.ROOT); }
    }

    public enum Op {
        EQ("="), NE("!="), GT(">"), LT("<"), GE(">="), LE("<=");

        final String symbol;
        Op(String symbol) { this.symbol = symbol; }

        /** Accepts = (or ==), !=, >, <, >=, <=. */
        public static Op fromSymbol(String s) {
            switch (s) {
                case "=": case "==": return EQ;
                case "!=":           return NE;
                case ">":            return GT;
                case "<":            return LT;
                case ">=":           return GE;
                case "<=":           return LE;
                default: throw new IllegalArgumentException("Unknown operator '" + s + "'");
            }
        }

        boolean compare(int cmp) {
            switch (this) {
                case EQ: return cmp == 0;
                case NE: return cmp != 0;
                case GT: return cmp > 0;
                case LT: return cmp < 0;
                case GE: return cmp >= 0;
                case LE: return cmp <= 0;
                default: throw new AssertionError(this);
            }
        }
    }

    private final Field field;
    private final Op op;
    private final String textValue;   // used when field is symbol or side
    private final double numValue;    // used when field is price or quantity

    /** Text comparison: symbol or side. */
    public static FieldFilter text(Field field, Op op, String value) {
        return new FieldFilter(field, op, value, Double.NaN);
    }

    /** Numeric comparison: price or quantity. */
    public static FieldFilter numeric(Field field, Op op, double value) {
        return new FieldFilter(field, op, null, value);
    }

    private FieldFilter(Field field, Op op, String textValue, double numValue) {
        if (field == null || op == null) throw new IllegalArgumentException("field and op are required");
        this.field = field;
        this.op = op;
        if (field.numeric) {
            if (Double.isNaN(numValue)) {
                throw new IllegalArgumentException(field.label() + " needs a numeric value");
            }
            this.textValue = null;
            this.numValue = numValue;
        } else {
            if (op != Op.EQ && op != Op.NE) {
                throw new IllegalArgumentException("Operator '" + op.symbol + "' is not allowed on "
                        + field.label() + " (only = and != work on text fields)");
            }
            if (textValue == null || textValue.trim().isEmpty()) {
                throw new IllegalArgumentException(field.label() + " needs a value");
            }
            String v = textValue.trim().toUpperCase(Locale.ROOT);
            if (field == Field.SIDE && !v.equals("BUY") && !v.equals("SELL")) {
                throw new IllegalArgumentException("side must be BUY or SELL, got '" + textValue + "'");
            }
            this.textValue = v;
            this.numValue = Double.NaN;
        }
    }

    @Override
    public boolean test(DataPoint p) {
        switch (field) {
            case SYMBOL:   return op.compare(p.getSymbol().compareToIgnoreCase(textValue));
            case SIDE:     return op.compare(p.getSide().name().compareTo(textValue));
            case PRICE:    return op.compare(Double.compare(p.getPrice(), numValue));
            case QUANTITY: return op.compare(Double.compare(p.getQuantity(), numValue));
            default: throw new AssertionError(field);
        }
    }

    @Override
    public String describe() {
        return field.label() + op.symbol + valueText();
    }

    /** SQL-style form, e.g. "symbol = 'AAPL'" or "price > 200". */
    public String toSql() {
        String v = field.numeric ? valueText() : "'" + textValue + "'";
        return field.label() + " " + (op == Op.NE ? "<>" : op.symbol) + " " + v;
    }

    private String valueText() {
        if (!field.numeric) return textValue;
        return numValue == Math.rint(numValue) ? String.valueOf((long) numValue) : String.valueOf(numValue);
    }

    public Field getField() { return field; }
    public Op getOp() { return op; }
}
