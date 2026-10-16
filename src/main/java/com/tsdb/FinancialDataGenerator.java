package com.tsdb;

import java.util.Random;

/**
 * Synthetic financial event source, standing in for a real market feed per
 * Prof. Trummer's guidance: randomly pick a symbol, a buy/sell side, a
 * quantity, and a price (via a small random walk per symbol so the series
 * looks realistic instead of pure noise).
 *
 * Timestamps come from a simulated clock that starts at {@code startMillis}
 * and advances by a random delta in [minDeltaMillis, maxDeltaMillis) per
 * event, independent of real time. Pass a seeded Random for repeatable output.
 */
public class FinancialDataGenerator {

    private static final String[] SYMBOLS = {"AAPL", "MSFT", "GOOG", "NVDA", "AMZN", "TSLA"};

    private final Random random;
    private final double[] lastPrice;
    private long clockMillis;
    private final long minDeltaMillis;
    private final long maxDeltaMillis;

    public FinancialDataGenerator(long startMillis, long minDeltaMillis, long maxDeltaMillis) {
        this(startMillis, minDeltaMillis, maxDeltaMillis, new Random());
    }

    public FinancialDataGenerator(long startMillis, long minDeltaMillis, long maxDeltaMillis, Random random) {
        if (minDeltaMillis <= 0 || maxDeltaMillis < minDeltaMillis) {
            throw new IllegalArgumentException("require 0 < minDeltaMillis <= maxDeltaMillis");
        }
        this.random = random;
        this.clockMillis = startMillis;
        this.minDeltaMillis = minDeltaMillis;
        this.maxDeltaMillis = maxDeltaMillis;
        this.lastPrice = new double[SYMBOLS.length];
        for (int i = 0; i < SYMBOLS.length; i++) {
            lastPrice[i] = 50 + random.nextDouble() * 450; // seed price in [$50, $500)
        }
    }

    /** Produces the next synthetic event and advances the simulated clock. */
    public DataPoint next() {
        int idx = random.nextInt(SYMBOLS.length);
        String symbol = SYMBOLS[idx];

        double drift = (random.nextDouble() - 0.5) * 2.0; // +/- $1.00 random walk per tick
        lastPrice[idx] = Math.max(1.0, lastPrice[idx] + drift);
        double price = Math.round(lastPrice[idx] * 100.0) / 100.0;

        int quantity = 1 + random.nextInt(500);
        DataPoint.Side side = random.nextBoolean() ? DataPoint.Side.BUY : DataPoint.Side.SELL;

        long delta = minDeltaMillis + (long) (random.nextDouble() * (maxDeltaMillis - minDeltaMillis));
        clockMillis += delta;

        return new DataPoint(clockMillis, symbol, price, quantity, side);
    }
}
