package com.tsdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Tests for the aggregation layer and the shell's aggregate syntax.
 *
 * The core property: after every event, each aggregator's running result
 * equals a brute-force recompute over window.contents(). Checked by hand on
 * small sequences and by random streams through both window types.
 *
 * Run with:  java -cp out com.tsdb.AggregatorTest
 */
public class AggregatorTest {

    private static int failures = 0;
    private static int passed = 0;

    private static final FieldFilter.Field PRICE = FieldFilter.Field.PRICE;
    private static final FieldFilter.Field QTY = FieldFilter.Field.QUANTITY;

    public static void main(String[] args) {
        testEmptyWindow();
        testCountSumAvgByHand();
        testMinMaxByHand();
        testMinMaxWithTies();
        testRandomStreamsMatchBruteForce();
        testGroupBy();
        testGroupByMatchesBruteForce();
        testParser();
        testParserBadInput();

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " AGGREGATOR TESTS PASSED");
        } else {
            System.out.println(failures + " AGGREGATOR TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    private static void testEmptyWindow() {
        check("empty: count is 0", new CountAgg().result(), 0.0);
        check("empty: sum is NaN", Double.isNaN(new SumAgg(PRICE).result()), true);
        check("empty: avg is NaN", Double.isNaN(new AvgAgg(PRICE).result()), true);
        check("empty: min is NaN", Double.isNaN(new MinAgg(PRICE).result()), true);
        check("empty: max is NaN", Double.isNaN(new MaxAgg(PRICE).result()), true);
        check("empty: group-by has no groups",
                new GroupByAggregator(FieldFilter.Field.SYMBOL, CountAgg::new).results().isEmpty(), true);
    }

    private static void testCountSumAvgByHand() {
        CountAgg count = new CountAgg();
        SumAgg sum = new SumAgg(PRICE);
        AvgAgg avg = new AvgAgg(QTY);
        StreamEngine e = new StreamEngine(new CountWindow(3), Filter.ACCEPT_ALL, List.of(count, sum, avg));
        e.processUpdate(dp(1, "A", 10, 100));
        e.processUpdate(dp(2, "A", 20, 200));
        check("hand: count after 2", count.result(), 2.0);
        check("hand: sum after 2", sum.result(), 30.0);
        check("hand: average qty after 2", avg.result(), 150.0);
        e.processUpdate(dp(3, "A", 30, 300));
        e.processUpdate(dp(4, "A", 40, 400)); // evicts the 10/100 point
        check("hand: count stays at capacity", count.result(), 3.0);
        check("hand: sum drops evicted", sum.result(), 90.0);
        check("hand: average qty drops evicted", avg.result(), 300.0);
    }

    private static void testMinMaxByHand() {
        MinAgg min = new MinAgg(PRICE);
        MaxAgg max = new MaxAgg(PRICE);
        StreamEngine e = new StreamEngine(new CountWindow(3), Filter.ACCEPT_ALL, List.of(min, max));
        e.processUpdate(dp(1, "A", 5, 1));
        e.processUpdate(dp(2, "A", 50, 1));
        e.processUpdate(dp(3, "A", 20, 1));
        check("minmax: min of 5,50,20", min.result(), 5.0);
        check("minmax: max of 5,50,20", max.result(), 50.0);
        e.processUpdate(dp(4, "A", 30, 1)); // evicts 5 -> window 50,20,30
        check("minmax: min after evicting the min", min.result(), 20.0);
        check("minmax: max unchanged", max.result(), 50.0);
        e.processUpdate(dp(5, "A", 25, 1)); // evicts 50 -> window 20,30,25
        check("minmax: max after evicting the max", max.result(), 30.0);
        check("minmax: min unchanged", min.result(), 20.0);
    }

    private static void testMinMaxWithTies() {
        MinAgg min = new MinAgg(PRICE);
        MaxAgg max = new MaxAgg(PRICE);
        StreamEngine e = new StreamEngine(new CountWindow(2), Filter.ACCEPT_ALL, List.of(min, max));
        e.processUpdate(dp(1, "A", 7, 1));
        e.processUpdate(dp(2, "A", 7, 1));
        e.processUpdate(dp(3, "A", 9, 1)); // evicts first 7 -> window 7,9
        check("ties: min survives evicting a tied older point", min.result(), 7.0);
        check("ties: max", max.result(), 9.0);
        e.processUpdate(dp(4, "A", 9, 1)); // evicts 7 -> window 9,9
        check("ties: min after both 7s gone", min.result(), 9.0);
        e.processUpdate(dp(5, "A", 1, 1)); // evicts first 9 -> window 9,1
        check("ties: max survives evicting a tied older point", max.result(), 9.0);
    }

    private static void testRandomStreamsMatchBruteForce() {
        for (Window w : List.of(new CountWindow(7), new TimeWindow(2500))) {
            CountAgg count = new CountAgg();
            SumAgg sum = new SumAgg(PRICE);
            AvgAgg avg = new AvgAgg(QTY);
            MinAgg min = new MinAgg(PRICE);
            MaxAgg max = new MaxAgg(QTY);
            StreamEngine e = new StreamEngine(w, Filter.ACCEPT_ALL, List.of(count, sum, avg, min, max));
            FinancialDataGenerator g = new FinancialDataGenerator(0, 100, 900, new Random(7));
            String firstMismatch = null;
            for (int i = 0; i < 3000 && firstMismatch == null; i++) {
                e.processUpdate(g.next());
                List<DataPoint> c = w.contents();
                double bSum = 0, bQty = 0, bMin = Double.POSITIVE_INFINITY, bMax = Double.NEGATIVE_INFINITY;
                for (DataPoint p : c) {
                    bSum += p.getPrice();
                    bQty += p.getQuantity();
                    bMin = Math.min(bMin, p.getPrice());
                    bMax = Math.max(bMax, p.getQuantity());
                }
                if (count.result() != c.size()) firstMismatch = "count at event " + i;
                else if (Math.abs(sum.result() - bSum) > 1e-6) firstMismatch = "sum at event " + i;
                else if (Math.abs(avg.result() - bQty / c.size()) > 1e-6) firstMismatch = "avg at event " + i;
                else if (min.result() != bMin) firstMismatch = "min at event " + i;
                else if (max.result() != bMax) firstMismatch = "max at event " + i;
            }
            check("random stream matches brute force on " + w.describe(), firstMismatch, null);
        }
    }

    private static void testGroupBy() {
        GroupByAggregator g = new GroupByAggregator(FieldFilter.Field.SYMBOL, () -> new AvgAgg(PRICE));
        StreamEngine e = new StreamEngine(new CountWindow(3), Filter.ACCEPT_ALL, List.of(g));
        e.processUpdate(dp(1, "AAPL", 10, 1));
        e.processUpdate(dp(2, "NVDA", 100, 1));
        e.processUpdate(dp(3, "AAPL", 20, 1));
        check("groupby: two groups", g.results(), Map.of("AAPL", 15.0, "NVDA", 100.0));
        e.processUpdate(dp(4, "MSFT", 7, 1)); // evicts AAPL@10
        check("groupby: evicted point leaves its group", g.results(), Map.of("AAPL", 20.0, "NVDA", 100.0, "MSFT", 7.0));
        e.processUpdate(dp(5, "MSFT", 9, 1)); // evicts NVDA@100, last of its group
        check("groupby: empty group disappears", g.results(), Map.of("AAPL", 20.0, "MSFT", 8.0));
        check("groupby: describe", g.describe(), "AVERAGE price GROUP BY symbol");
        boolean threw = false;
        try { g.result(); } catch (UnsupportedOperationException ex) { threw = true; }
        check("groupby: result() refuses, use results()", threw, true);

        GroupByAggregator bySide = new GroupByAggregator(FieldFilter.Field.SIDE, CountAgg::new);
        bySide.onInsert(dp(1, "A", 1, 1));
        bySide.onInsert(new DataPoint(2, "A", 1, 1, DataPoint.Side.SELL));
        bySide.onInsert(dp(3, "A", 1, 1));
        check("groupby: by side", bySide.results(), Map.of("BUY", 2.0, "SELL", 1.0));
    }

    private static void testGroupByMatchesBruteForce() {
        GroupByAggregator g = new GroupByAggregator(FieldFilter.Field.SYMBOL, () -> new MaxAgg(PRICE));
        Window w = new TimeWindow(3000);
        StreamEngine e = new StreamEngine(w, Filter.ACCEPT_ALL, List.of(g));
        FinancialDataGenerator gen = new FinancialDataGenerator(0, 100, 900, new Random(11));
        String firstMismatch = null;
        for (int i = 0; i < 2000 && firstMismatch == null; i++) {
            e.processUpdate(gen.next());
            Map<String, Double> brute = new TreeMap<>();
            for (DataPoint p : w.contents()) brute.merge(p.getSymbol(), p.getPrice(), Math::max);
            if (!brute.equals(g.results())) firstMismatch = "event " + i + ": " + brute + " vs " + g.results();
        }
        check("groupby: random stream matches brute force", firstMismatch, null);
    }

    /** The shell's aggregate syntax, e.g. "AVERAGE, MAX qty, COUNT GROUP BY symbol". */
    private static void testParser() {
        List<Aggregator> aggs = build(" average , max qty, COUNT, avg quantity ");
        check("parser: four aggregates", describeAll(aggs),
                List.of("AVERAGE price", "MAX quantity", "COUNT", "AVERAGE quantity"));
        check("parser: types", aggs.get(0) instanceof AvgAgg && aggs.get(1) instanceof MaxAgg
                && aggs.get(2) instanceof CountAgg, true);
        check("parser: sum and min", describeAll(build("SUM quantity, MIN")), List.of("SUM quantity", "MIN price"));
        check("parser: group by wraps each aggregate", describeAll(build("average, count group by Symbol")),
                List.of("AVERAGE price GROUP BY symbol", "COUNT GROUP BY symbol"));
        check("parser: group by ticker means symbol", describeAll(build("MAX qty GROUP BY ticker")),
                List.of("MAX quantity GROUP BY symbol"));
        check("parser: group by side", describeAll(build("max qty GROUP BY side")),
                List.of("MAX quantity GROUP BY side"));
        Supplier<Aggregator> count = Shell.parseAggregates("COUNT").get(0);
        check("parser: each use gets a fresh aggregator", count.get() != count.get(), true);
    }

    private static void testParserBadInput() {
        expectBad("MEDIAN", "unknown aggregate");
        expectBad("AVERAGE symbol", "text field in numeric aggregate");
        expectBad("AVERAGE volume", "unknown field");
        expectBad("COUNT price", "count with field");
        expectBad("AVERAGE,", "trailing comma");
        expectBad("AVERAGE MAX", "missing comma");
        expectBad("avg(price)", "old function-call syntax");
        expectBad("AVERAGE GROUP BY price", "group by numeric field");
        expectBad("AVERAGE GROUP BY sector", "group by unknown field");
        expectBad("AVERAGE GROUP BY", "group by without a field");
        expectBad("AVERAGE GROUP BY symbol, COUNT", "group by not last");

        for (String name : List.of("FOO", "MEDIAN")) {
            try {
                Shell.parseAggregates(name);
                check("'" + name + "' is rejected", false, true);
            } catch (IllegalArgumentException e) {
                check("'" + name + "' reads as an unknown aggregate", e.getMessage().startsWith("Unknown aggregate"), true);
            }
        }
    }

    // --- helpers ---
    private static DataPoint dp(long ts, String sym, double price, int qty) {
        return new DataPoint(ts, sym, price, qty, DataPoint.Side.BUY);
    }

    private static List<Aggregator> build(String text) {
        List<Aggregator> out = new ArrayList<>();
        for (Supplier<Aggregator> s : Shell.parseAggregates(text)) out.add(s.get());
        return out;
    }

    private static List<String> describeAll(List<Aggregator> aggs) {
        List<String> out = new ArrayList<>();
        for (Aggregator a : aggs) out.add(a.describe());
        return out;
    }

    private static void expectBad(String text, String why) {
        try {
            Shell.parseAggregates(text);
            failures++;
            System.out.println("  FAIL bad input accepted (" + why + "): \"" + text + "\"");
        } catch (IllegalArgumentException e) {
            passed++;
            System.out.println("  ok   rejects " + why + ": " + e.getMessage());
        }
    }

    private static void check(String name, Object actual, Object expected) {
        if (Objects.equals(actual, expected)) {
            passed++;
            System.out.println("  ok   " + name);
        } else {
            failures++;
            System.out.println("  FAIL " + name + " -- expected " + expected + ", got " + actual);
        }
    }
}
