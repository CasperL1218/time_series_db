package com.tsdb;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Stand-alone demo for the aggregation layer: filter -> window -> aggregates.
 *
 * Usage:
 *   java -cp out com.tsdb.AggregateDemo "<aggregates>" "<filter>" <count|time> <size> [intervalMs] [numEvents] [seed]
 *
 * Examples:
 *   java -cp out com.tsdb.AggregateDemo "avg(price), max(price), count" "" count 5
 *   java -cp out com.tsdb.AggregateDemo "min(price), max(price)" "symbol=AAPL" time 10
 *   java -cp out com.tsdb.AggregateDemo "avg(price), count GROUP BY symbol" "side=BUY" count 20 50 40 42
 */
public class AggregateDemo {

    public static void main(String[] args) {
        if (args.length < 4) {
            usage();
            System.exit(1);
        }

        List<Aggregator> aggregators;
        Filter filter;
        try {
            aggregators = AggregatorParser.parse(args[0]);
            filter = FilterParser.parse(args[1]);
        } catch (IllegalArgumentException e) {
            System.err.println("Bad query: " + e.getMessage());
            System.exit(1);
            return;
        }

        Window window;
        long intervalMs, numEvents, seed;
        try {
            window = args[2].equalsIgnoreCase("time")
                    ? new TimeWindow(Long.parseLong(args[3]) * 1000L)
                    : args[2].equalsIgnoreCase("count") ? new CountWindow(Integer.parseInt(args[3])) : null;
            intervalMs = args.length >= 5 ? Long.parseLong(args[4]) : 400;
            numEvents  = args.length >= 6 ? Long.parseLong(args[5]) : 0;
            seed       = args.length >= 7 ? Long.parseLong(args[6]) : System.nanoTime();
        } catch (IllegalArgumentException e) {
            System.err.println("Bad argument: " + e.getMessage());
            usage();
            System.exit(1);
            return;
        }
        if (window == null) {
            usage();
            System.exit(1);
            return;
        }

        StreamEngine engine = new StreamEngine(window, filter, aggregators);
        FinancialDataGenerator gen = new FinancialDataGenerator(System.currentTimeMillis(),
                Math.max(1, intervalMs / 2), Math.max(2, intervalMs * 2), new Random(seed));

        System.out.println("Window : " + window.describe());
        System.out.println("Filter : " + filter.describe());
        System.out.println("SQL    : " + AggregatorParser.toSql(aggregators, filter));
        System.out.println(numEvents == 0 ? "Press Ctrl+C to stop.\n" : "");

        long emitted = 0;
        try {
            while (numEvents == 0 || emitted < numEvents) {
                DataPoint dp = gen.next();
                boolean accepted = engine.processUpdate(dp);
                System.out.println((accepted ? "  PASS  " : "  skip  ") + dp);
                if (accepted) {
                    System.out.println("        window holds " + window.size() + " point(s)");
                    for (Aggregator a : aggregators) printAggregate(a);
                }
                emitted++;
                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.printf("%nProcessed %d, passed %d, filtered out %d.%n",
                engine.getPointsProcessed(),
                engine.getPointsProcessed() - engine.getPointsFiltered(),
                engine.getPointsFiltered());
    }

    private static void printAggregate(Aggregator a) {
        if (a instanceof GroupByAggregator g) {
            System.out.println("        " + g.describe() + ":");
            for (Map.Entry<String, Double> e : g.results().entrySet()) {
                System.out.printf("          %-6s %s%n", e.getKey(), fmt(e.getValue()));
            }
        } else {
            System.out.printf("        %-14s = %s%n", a.describe(), fmt(a.result()));
        }
    }

    private static String fmt(double v) {
        return v == Math.rint(v) ? String.valueOf((long) v) : String.format("%.2f", v);
    }

    private static void usage() {
        System.err.println("Usage: java -cp out com.tsdb.AggregateDemo \"<aggregates>\" \"<filter>\" <count|time> <size> [intervalMs] [numEvents] [seed]");
        System.err.println("  aggregates: comma-separated count, sum(f), avg(f), min(f), max(f) with f = price | quantity,");
        System.err.println("              optionally followed by GROUP BY symbol | side");
        System.err.println("  filter:     same syntax as FilterDemo, or \"\" for none");
    }
}
