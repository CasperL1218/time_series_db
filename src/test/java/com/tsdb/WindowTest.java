package com.tsdb;

import java.util.List;
import java.util.Objects;

/**
 * Lightweight self-contained test harness (no external framework required)
 * covering the eviction semantics of CountWindow and TimeWindow.
 *
 * Run with:  java -cp out com.tsdb.WindowTest
 * Exits with status 0 and prints "ALL TESTS PASSED" if everything checks out,
 * otherwise prints each failure and exits with status 1.
 */
public class WindowTest {

    private static int failures = 0;

    public static void main(String[] args) {
        testCountWindowEviction();
        testCountWindowCapacityOne();
        testTimeWindowEviction();
        testTimeWindowKeepsBoundaryPoint();
        testStreamEngineNotifiesAndCounts();

        if (failures == 0) {
            System.out.println("ALL TESTS PASSED");
        } else {
            System.out.println(failures + " TEST(S) FAILED");
            System.exit(1);
        }
    }

    private static void testCountWindowEviction() {
        CountWindow w = new CountWindow(3);
        w.add(dp(1, "A"));
        w.add(dp(2, "B"));
        w.add(dp(3, "C"));
        check("count window holds exactly capacity after 3 adds",
                symbols(w.contents()), List.of("A", "B", "C"));

        w.add(dp(4, "D"));
        check("count window evicts oldest (A) on overflow",
                symbols(w.contents()), List.of("B", "C", "D"));

        w.add(dp(5, "E"));
        check("count window evicts oldest (B) on next overflow",
                symbols(w.contents()), List.of("C", "D", "E"));
    }

    private static void testCountWindowCapacityOne() {
        CountWindow w = new CountWindow(1);
        w.add(dp(1, "A"));
        w.add(dp(2, "B"));
        check("capacity-1 window keeps only the latest point", symbols(w.contents()), List.of("B"));
    }

    private static void testTimeWindowEviction() {
        TimeWindow w = new TimeWindow(60_000); // 60s window
        w.add(dp(0, "A"));
        w.add(dp(10_000, "B"));
        w.add(dp(59_000, "C"));
        check("time window keeps everything within 60s of latest",
                symbols(w.contents()), List.of("A", "B", "C"));

        w.add(dp(61_000, "D")); // A is now 61s behind the latest point -> evict
        check("time window evicts points older than the window",
                symbols(w.contents()), List.of("B", "C", "D"));
    }

    private static void testTimeWindowKeepsBoundaryPoint() {
        TimeWindow w = new TimeWindow(10_000); // 10s window
        w.add(dp(0, "A"));
        w.add(dp(9_999, "B")); // still within the window
        check("point just inside the window boundary is kept",
                symbols(w.contents()), List.of("A", "B"));
    }

    private static void testStreamEngineNotifiesAndCounts() {
        CountWindow w = new CountWindow(2);
        StreamEngine engine = new StreamEngine(w);
        int[] notifications = {0};
        engine.setOnUpdate(win -> notifications[0]++);

        engine.processUpdate(dp(1, "A"));
        engine.processUpdate(dp(2, "B"));
        engine.processUpdate(dp(3, "C"));

        check("engine processed 3 events", engine.getPointsProcessed(), 3L);
        check("engine notified listener 3 times", notifications[0], 3);
        check("engine's window reflects eviction", symbols(w.contents()), List.of("B", "C"));
    }

    private static DataPoint dp(long ts, String symbol) {
        return new DataPoint(ts, symbol, 100.0, 10, DataPoint.Side.BUY);
    }

    private static List<String> symbols(List<DataPoint> points) {
        return points.stream().map(DataPoint::getSymbol).toList();
    }

    private static void check(String label, Object actual, Object expected) {
        if (Objects.equals(actual, expected)) {
            System.out.println("[PASS] " + label);
        } else {
            System.out.println("[FAIL] " + label + " -- expected=" + expected + " actual=" + actual);
            failures++;
        }
    }
}
