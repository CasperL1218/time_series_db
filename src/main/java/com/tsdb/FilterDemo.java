package com.tsdb;

/**
 * Stand-alone CLI demo for the filtering layer, kept separate from Main so it
 * can be run and shared on its own. Main does not take a filter yet; the plan
 * is for it to call FilterParser.parse() for a future --where flag.
 *
 * Prints the window, the filter and its SQL form, then marks each generated
 * event PASS (entered the window, followed by the window contents) or skip
 * (rejected by the filter), and ends with processed/passed/filtered counts.
 * The optional seed makes the generated trades repeatable; timestamps still
 * start at the current time.
 *
 * Usage:
 *   java -cp out com.tsdb.FilterDemo "<filter>" <count|time> <size> [intervalMs] [numEvents] [seed]
 *
 * Examples:
 *   java -cp out com.tsdb.FilterDemo "symbol=AAPL" count 5
 *   java -cp out com.tsdb.FilterDemo "side=BUY AND quantity>=400 OR symbol=NVDA" count 5 50 40 42
 */
public class FilterDemo {

    public static void main(String[] args) {
        if (args.length < 3) {
            usage();
            System.exit(1);
        }

        Filter filter;
        try {
            filter = FilterParser.parse(args[0]);
        } catch (IllegalArgumentException e) {
            System.err.println("Bad filter: " + e.getMessage());
            System.exit(1);
            return;
        }

        Window window;
        long intervalMs, numEvents, seed;
        try {
            window = args[1].equalsIgnoreCase("time")
                    ? new TimeWindow(Long.parseLong(args[2]) * 1000L)
                    : args[1].equalsIgnoreCase("count") ? new CountWindow(Integer.parseInt(args[2])) : null;
            intervalMs = args.length >= 4 ? Long.parseLong(args[3]) : 400;
            numEvents  = args.length >= 5 ? Long.parseLong(args[4]) : 0;
            seed       = args.length >= 6 ? Long.parseLong(args[5]) : System.nanoTime();
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

        StreamEngine engine = new StreamEngine(window, filter, java.util.List.of());
        FinancialDataGenerator gen = new FinancialDataGenerator(System.currentTimeMillis(),
                Math.max(1, intervalMs / 2), Math.max(2, intervalMs * 2), new java.util.Random(seed));

        System.out.println("Window : " + window.describe());
        System.out.println("Filter : " + filter.describe());
        System.out.println("SQL    : SELECT * FROM trades WHERE " + FilterParser.toSql(filter));
        System.out.println(numEvents == 0 ? "Press Ctrl+C to stop.\n" : "");

        long emitted = 0;
        try {
            while (numEvents == 0 || emitted < numEvents) {
                DataPoint dp = gen.next();
                boolean accepted = engine.processUpdate(dp);
                System.out.println((accepted ? "  PASS  " : "  skip  ") + dp);
                if (accepted) {
                    System.out.println("        window now holds " + window.size() + " point(s):");
                    for (DataPoint p : window.contents()) System.out.println("          " + p);
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

    private static void usage() {
        System.err.println("Usage: java -cp out com.tsdb.FilterDemo \"<filter>\" <count|time> <size> [intervalMs] [numEvents] [seed]");
        System.err.println("  filter examples: \"symbol=AAPL\"  \"price>200 AND side=BUY\"  \"symbol=AAPL OR symbol=NVDA\"");
        System.err.println("  fields: symbol, side, price, quantity   ops: = != > < >= <=   join with AND / OR");
    }
}
