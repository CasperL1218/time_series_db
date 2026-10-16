package com.tsdb;

import java.util.List;

/**
 * CLI demo driver for the pre-fall-break milestone: window only, no filter
 * (see FilterDemo for the filtered version). Prints each incoming event and
 * the full window after every update.
 *
 * Usage:
 *   java -cp out com.tsdb.Main <count|time> <size> [intervalMs] [numEvents]
 *
 *   count <N>        keep the most recent N data points
 *   time  <seconds>  keep data points from the last <seconds> seconds
 *   intervalMs       delay between simulated events, in ms (default 400)
 *   numEvents        number of events to simulate; 0 or omitted = run until Ctrl+C
 *
 * Examples:
 *   java -cp out com.tsdb.Main count 5
 *   java -cp out com.tsdb.Main time 10
 */
public class Main {

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
            return;
        }

        Window window = buildWindow(args[0], args[1]);
        if (window == null) {
            printUsage();
            System.exit(1);
            return;
        }

        long intervalMs;
        long numEvents;
        try {
            intervalMs = args.length >= 3 ? Long.parseLong(args[2]) : 400;
            numEvents = args.length >= 4 ? Long.parseLong(args[3]) : 0; // 0 = unlimited
        } catch (NumberFormatException e) {
            System.err.println("intervalMs and numEvents must be integers.");
            printUsage();
            System.exit(1);
            return;
        }

        StreamEngine engine = new StreamEngine(window);
        engine.setOnUpdate(Main::display);

        // Simulated clock advances intervalMs/2 .. 2*intervalMs per event, roughly tracking real time.
        FinancialDataGenerator generator = new FinancialDataGenerator(
                System.currentTimeMillis(), Math.max(1, intervalMs / 2), Math.max(2, intervalMs * 2));

        System.out.println("Stream engine started: " + window.describe());
        System.out.println(numEvents == 0 ? "Press Ctrl+C to stop.\n" : "Simulating " + numEvents + " event(s).\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                System.out.println("\nShutting down. Processed " + engine.getPointsProcessed() + " event(s).")));

        long emitted = 0;
        try {
            while (numEvents == 0 || emitted < numEvents) {
                DataPoint dp = generator.next();
                System.out.println("Incoming: " + dp);
                engine.processUpdate(dp);
                emitted++;
                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Window buildWindow(String type, String sizeArg) {
        try {
            switch (type.toLowerCase()) {
                case "count":
                    return new CountWindow(Integer.parseInt(sizeArg));
                case "time":
                    return new TimeWindow(Long.parseLong(sizeArg) * 1000L);
                default:
                    System.err.println("Unknown window type: " + type);
                    return null;
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid size argument: " + sizeArg);
            return null;
        }
    }

    private static void display(Window window) {
        List<DataPoint> contents = window.contents();
        System.out.println("Current window (" + contents.size() + " point(s)):");
        for (DataPoint dp : contents) {
            System.out.println("  " + dp);
        }
        System.out.println();
    }

    private static void printUsage() {
        System.err.println("Usage: java com.tsdb.Main <count|time> <size> [intervalMs] [numEvents]");
        System.err.println("  count <N>        keep the most recent N data points");
        System.err.println("  time  <seconds>  keep data points from the last <seconds> seconds");
        System.err.println("  intervalMs       delay between simulated events in ms (default 400)");
        System.err.println("  numEvents        number of events to simulate, 0/omitted = unlimited (Ctrl+C to stop)");
    }
}
