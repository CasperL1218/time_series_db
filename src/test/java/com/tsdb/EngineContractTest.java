package com.tsdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Checks the Step 0 contracts every teammate builds on:
 *  - Window.add() returns exactly the evicted points, oldest first
 *  - StreamEngine filters before the window
 *  - Aggregators see every insert and every eviction, so a running result
 *    always matches a brute-force recompute over window.contents()
 *
 * Run with:  java -cp out com.tsdb.EngineContractTest
 */
public class EngineContractTest {

    private static int failures = 0;

    public static void main(String[] args) {
        testCountWindowReturnsEvicted();
        testTimeWindowReturnsEvicted();
        testFilterRunsBeforeWindow();
        testAggregatorStaysInSyncWithWindow();

        if (failures == 0) {
            System.out.println("ALL CONTRACT TESTS PASSED");
        } else {
            System.out.println(failures + " CONTRACT TEST(S) FAILED");
            System.exit(1);
        }
    }

    private static void testCountWindowReturnsEvicted() {
        CountWindow w = new CountWindow(2);
        check("count: no eviction while filling", syms(w.add(dp(1, "A", 10))), List.of());
        check("count: no eviction at capacity", syms(w.add(dp(2, "B", 10))), List.of());
        check("count: evicts oldest on overflow", syms(w.add(dp(3, "C", 10))), List.of("A"));
        check("count: contents after eviction", syms(w.contents()), List.of("B", "C"));
    }

    private static void testTimeWindowReturnsEvicted() {
        TimeWindow w = new TimeWindow(1000);
        w.add(dp(0, "A", 10));
        w.add(dp(500, "B", 10));
        check("time: point exactly on boundary is kept", syms(w.add(dp(1000, "C", 10))), List.of());
        check("time: evicts every expired point at once", syms(w.add(dp(2600, "D", 10))), List.of("A", "B", "C"));
        check("time: size after eviction", w.size(), 1);
    }

    private static void testFilterRunsBeforeWindow() {
        Filter onlyA = new Filter() {
            public boolean test(DataPoint p) { return p.getSymbol().equals("A"); }
            public String describe() { return "symbol=A"; }
        };
        StreamEngine e = new StreamEngine(new CountWindow(10), onlyA, List.of());
        int[] notified = {0};
        e.setOnUpdate(w -> notified[0]++);
        check("filter: accepted returns true", e.processUpdate(dp(1, "A", 1)), true);
        check("filter: rejected returns false", e.processUpdate(dp(2, "B", 1)), false);
        e.processUpdate(dp(3, "A", 1));
        check("filter: window holds only accepted", syms(e.getWindow().contents()), List.of("A", "A"));
        check("filter: processed counts everything", e.getPointsProcessed(), 3L);
        check("filter: filtered count", e.getPointsFiltered(), 1L);
        check("filter: listener only on accepted", notified[0], 2);
    }

    private static void testAggregatorStaysInSyncWithWindow() {
        // Minimal running-sum aggregator, just to exercise the engine contract.
        Aggregator sum = new Aggregator() {
            double s = 0; int n = 0;
            public void onInsert(DataPoint p) { s += p.getPrice(); n++; }
            public void onEvict(DataPoint p)  { s -= p.getPrice(); n--; }
            public double result() { return n == 0 ? Double.NaN : s; }
            public String describe() { return "SUM(price)"; }
        };
        for (Window w : List.of(new CountWindow(5), new TimeWindow(3000))) {
            StreamEngine e = new StreamEngine(w, Filter.ACCEPT_ALL, List.of(sum));
            FinancialDataGenerator g = new FinancialDataGenerator(0, 100, 900, new java.util.Random(42));
            boolean ok = true;
            for (int i = 0; i < 2000; i++) {
                e.processUpdate(g.next());
                double brute = 0;
                for (DataPoint p : w.contents()) brute += p.getPrice();
                if (Math.abs(brute - sum.result()) > 1e-6) { ok = false; break; }
            }
            check("aggregator in sync with " + w.describe(), ok, true);
            // reset for next window type
            for (DataPoint p : w.contents()) sum.onEvict(p);
        }
    }

    // --- helpers ---
    private static DataPoint dp(long ts, String sym, double price) {
        return new DataPoint(ts, sym, price, 1, DataPoint.Side.BUY);
    }

    private static List<String> syms(List<DataPoint> pts) {
        List<String> out = new ArrayList<>();
        for (DataPoint p : pts) out.add(p.getSymbol());
        return out;
    }

    private static void check(String name, Object actual, Object expected) {
        if (Objects.equals(actual, expected)) {
            System.out.println("  ok   " + name);
        } else {
            failures++;
            System.out.println("  FAIL " + name + " -- expected " + expected + ", got " + actual);
        }
    }
}
