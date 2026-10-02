package com.tsdb;

/**
 * A single simulated financial event: a trade of some quantity of a stock
 * symbol at a given price and time.
 *
 * Immutable — the whole system is append-only, so data points are never
 * mutated once created.
 */
public final class DataPoint {

    public enum Side { BUY, SELL }

    private final long timestampMillis;
    private final String symbol;
    private final double price;
    private final int quantity;
    private final Side side;

    public DataPoint(long timestampMillis, String symbol, double price, int quantity, Side side) {
        this.timestampMillis = timestampMillis;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.side = side;
    }

    public long getTimestampMillis() { return timestampMillis; }
    public String getSymbol() { return symbol; }
    public double getPrice() { return price; }
    public int getQuantity() { return quantity; }
    public Side getSide() { return side; }

    @Override
    public String toString() {
        return String.format("%-12s %-6s %-4s $%-10.2f qty=%-6d",
                formatTimestamp(timestampMillis), symbol, side, price, quantity);
    }

    /** Formats an epoch-millis timestamp as HH:mm:ss.SSS in the local zone. */
    public static String formatTimestamp(long epochMillis) {
        java.time.LocalTime t = java.time.Instant.ofEpochMilli(epochMillis)
                .atZone(java.time.ZoneId.systemDefault())
                .toLocalTime();
        return String.format("%02d:%02d:%02d.%03d",
                t.getHour(), t.getMinute(), t.getSecond(), t.getNano() / 1_000_000);
    }
}
