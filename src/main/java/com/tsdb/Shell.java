package com.tsdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Interactive shell: start the data generator, then watch and query its
 * window from the same terminal while it keeps running in the background.
 *
 * WINDOW and the aggregate commands are live: their result is printed right
 * away and again as the window moves (the window after every trade, an
 * aggregate line whenever a value changes), until Enter (an empty line) or
 * the next command, which replaces it. GENERATE DATA and a FILTER without
 * aggregates just print one line and return to the prompt.
 *
 * Commands (case-insensitive):
 * <pre>
 *   GENERATE DATA window=&lt;count|time&gt; size=&lt;n&gt; [interval=&lt;ms&gt;] [events=&lt;n&gt;]
 *                         start generating trades in the background (labels in any order)
 *   WINDOW                watch the current query's window, trade by trade
 *   AGGREGATE aggregates  watch aggregates over the current query's window. Aggregates
 *                         are AVERAGE | SUM | MIN | MAX [price|qty] and COUNT, price by
 *                         default, separated by commas, then optionally GROUP BY
 *                         symbol|side for one value per ticker (or side).
 *   FILTER condition [AGGREGATE aggregates]
 *                         a new query over everything in the window;
 *                         FILTER NONE goes back to the whole window
 *   ALSO FILTER condition [AGGREGATE aggregates]
 *                         narrow the current query (its filter AND condition), keeping
 *                         its aggregates (and watching them) unless new ones are given
 *   (Enter)               stop watching; the generator keeps running
 *   STOP DATA             stop the generator; the window can still be queried
 *   HELP, EXIT
 * </pre>
 * The condition uses the FilterParser syntax (ticker = AAPL, price < 200,
 * side = BUY, joined with AND / OR); ticker is another name for symbol.
 *
 * The generator feeds two engines with the same window type and size: one
 * keeps every trade, the other runs the current query, i.e. its filter and
 * its aggregators, which update incrementally as trades arrive and leave.
 * FILTER rebuilds the query from the first engine, so it sees the whole
 * window; ALSO FILTER and the aggregate commands rebuild it from the
 * query's own contents. Nothing is written to disk.
 *
 * Usage:
 *   java -cp out com.tsdb.Shell
 */
public class Shell {

    private static final Set<String> AGGREGATES = Set.of("AVERAGE", "AVG", "SUM", "MIN", "MAX", "COUNT");
    private static final Set<String> GENERATE_LABELS = Set.of("window", "size", "interval", "events");
    private static final String GENERATE_USAGE =
            "Usage: GENERATE DATA window=<count|time> size=<n> [interval=<ms>] [events=<n>]";
    private static final Pattern FILTER = Pattern.compile("(?:^|\\s)FILTER(?:\\s|$)", Pattern.CASE_INSENSITIVE);
    // Where the aggregates start in "FILTER <condition> AGGREGATE <aggregates>".
    private static final Pattern AGGREGATE_KEYWORD = Pattern.compile("(?:^|\\s)AGGREGATE(?:\\s|$)", Pattern.CASE_INSENSITIVE);
    // An aggregate name inside a condition means AGGREGATE was left out.
    private static final Pattern AGGREGATE_NAME = Pattern.compile(
            "(?:^|\\s)(?:" + String.join("|", AGGREGATES) + ")\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern AND_OR = Pattern.compile("\\s+(?:AND|OR)\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern SYMBOL_CONDITION = Pattern.compile(
            "^\\s*(?:symbol|ticker)\\s*(?:==|!=|=)\\s*['\"]?([A-Za-z]+)['\"]?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern AGGREGATE = Pattern.compile("^\\s*([A-Za-z]+)(?:\\s+([A-Za-z]+))?\\s*$");
    private static final Pattern GROUP_BY = Pattern.compile("\\s+GROUP\\s+BY(?:\\s+|$)", Pattern.CASE_INSENSITIVE);

    // The lock guards every field below except generator: the generator thread feeds trades and
    // prints live updates, the prompt thread swaps queries. Holding it while printing keeps a
    // command's output in one piece.
    private final Object lock = new Object();
    private StreamEngine all;                 // every trade, no filter; null until GENERATE DATA
    private StreamEngine query;               // the same trades through the current query's filter and aggregators
    private List<Supplier<Aggregator>> queryAggs = List.of(); // to rebuild the query's aggregators
    private Supplier<Window> newWindow;       // empty window of the same type and size, for rebuilding the query
    private boolean watching;                 // the query's result is printed after every change
    private String lastValues;                // the aggregate values printed last, so unchanged ones aren't repeated
    private boolean running;                  // the generator is still producing; unlike generator.isAlive(),
                                              // it turns false inside the lock, before the thread winds down
    private long generated;
    private Thread generator;

    public static void main(String[] args) throws IOException {
        new Shell().run();
    }

    private void run() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Start with e.g. GENERATE DATA window=count size=20. HELP lists every command, EXIT quits.\n");
        while (true) {
            if (!isWatching()) {
                System.out.print("tsdb> ");
                System.out.flush();
            }
            String line = in.readLine();
            if (line == null) { // end of input (Ctrl+D, or Ctrl+Z then Enter on Windows)
                System.out.println();
                break;
            }
            line = line.trim();
            boolean wasWatching = stopWatching(); // any input ends the live view; a new command starts its own
            if (line.isEmpty()) {
                if (wasWatching) System.out.println("Stopped watching. The generator keeps running; STOP DATA stops it.\n");
                continue;
            }
            if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) break;
            if (wasWatching) System.out.println();
            try {
                execute(line);
            } catch (IllegalArgumentException e) {
                System.out.println("Error: " + e.getMessage());
                if (wasWatching) resumeWatching(); // a failed command changes nothing, so a typo doesn't end the live view
            }
            if (!isWatching()) System.out.println();
        }
        stopGenerator();
    }

    private void execute(String line) {
        String[] words = line.split("\\s+");
        switch (words[0].toUpperCase(Locale.ROOT)) {
            case "GENERATE":
                generate(line);
                return;
            case "AGGREGATE":
                aggregate(line.substring(words[0].length()));
                return;
            case "STOP":
                if (words.length != 2 || !words[1].equalsIgnoreCase("DATA")) {
                    throw new IllegalArgumentException("STOP DATA stops the generator; press Enter to stop watching");
                }
                stopData();
                return;
            case "HELP":
                noArguments(words);
                printHelp();
                return;
            case "WINDOW":
                noArguments(words);
                requireData();
                synchronized (lock) {
                    setQuery(query.getFilter(), List.of(), query.getWindow().contents());
                    watch();
                }
                return;
            case "FILTER":
                filterThenAggregate(line.substring(words[0].length()), false);
                return;
            case "ALSO": {
                if (words.length < 2 || !words[1].equalsIgnoreCase("FILTER")) {
                    throw new IllegalArgumentException("ALSO goes before FILTER, e.g. ALSO FILTER price < 20");
                }
                String afterAlso = line.substring(words[0].length()).trim();
                filterThenAggregate(afterAlso.substring(words[1].length()), true);
                return;
            }
            default:
                if (AGGREGATES.contains(words[0].replaceAll("[^A-Za-z]", "").toUpperCase(Locale.ROOT))) {
                    throw new IllegalArgumentException("Put AGGREGATE first, e.g. AGGREGATE " + line);
                }
                throw new IllegalArgumentException("Unknown command '" + words[0] + "'. Type HELP to see the commands");
        }
    }

    // ---- GENERATE DATA / STOP DATA ----

    /** "GENERATE DATA window=count size=20 interval=400 events=100": labels in any order, spaces around = allowed. */
    private void generate(String line) {
        String[] words = line.replaceAll("\\s*=\\s*", "=").split("\\s+");
        if (words.length < 2 || !words[1].equalsIgnoreCase("DATA")) throw new IllegalArgumentException(GENERATE_USAGE);
        Map<String, String> params = new HashMap<>();
        for (int i = 2; i < words.length; i++) {
            String[] kv = words[i].split("=", 2);
            String label = kv[0].toLowerCase(Locale.ROOT);
            if (kv.length != 2 || label.isEmpty() || kv[1].isEmpty()) {
                throw new IllegalArgumentException("Parameters look like label=value, got '" + words[i] + "'. " + GENERATE_USAGE);
            }
            if (!GENERATE_LABELS.contains(label)) {
                throw new IllegalArgumentException("Unknown parameter '" + kv[0] + "'. Expected window, size, interval or events");
            }
            if (params.put(label, kv[1]) != null) throw new IllegalArgumentException(label + " is given twice");
        }
        if (!params.containsKey("window") || !params.containsKey("size")) {
            throw new IllegalArgumentException("GENERATE DATA needs window and size. " + GENERATE_USAGE);
        }
        if (isRunning()) {
            throw new IllegalArgumentException("Already generating. STOP DATA first to start over");
        }
        long size = wholeNumber(params, "size", 0);
        long intervalMs = wholeNumber(params, "interval", 400);
        long numEvents = wholeNumber(params, "events", 0); // 0 = until STOP DATA
        if (size <= 0 || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size must be a positive whole number, got " + size);
        }
        if (intervalMs < 0 || numEvents < 0) {
            throw new IllegalArgumentException("interval and events can't be negative");
        }
        Supplier<Window> factory;
        switch (params.get("window").toLowerCase(Locale.ROOT)) {
            case "count": factory = () -> new CountWindow((int) size); break;
            case "time":  factory = () -> new TimeWindow(size * 1000L); break;
            default:
                throw new IllegalArgumentException("window must be count or time, got '" + params.get("window") + "'");
        }
        Window window = factory.get();

        // Simulated clock advances intervalMs/2 .. 2*intervalMs per event, roughly tracking real time.
        FinancialDataGenerator source = new FinancialDataGenerator(System.currentTimeMillis(),
                Math.max(1, intervalMs / 2), Math.max(2, intervalMs * 2));
        synchronized (lock) {
            all = new StreamEngine(window);
            newWindow = factory;
            setQuery(Filter.ACCEPT_ALL, List.of(), List.of());
            generated = 0;
            running = true;
        }
        generator = new Thread(() -> produce(source, intervalMs, numEvents), "generator");
        generator.setDaemon(true); // don't keep the JVM alive after EXIT
        generator.start();

        System.out.println("Generating trades into a " + window.describe() + ", about one every " + intervalMs + " ms"
                + (numEvents == 0 ? " until STOP DATA." : ", " + numEvents + " in total.") + " WINDOW shows it.");
    }

    private static long wholeNumber(Map<String, String> params, String label, long ifMissing) {
        String v = params.get(label);
        if (v == null) return ifMissing;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " must be a whole number, got '" + v + "'");
        }
    }

    /** Generator thread: feeds each trade to both engines until STOP DATA, EXIT or numEvents. */
    private void produce(FinancialDataGenerator source, long intervalMs, long numEvents) {
        try {
            for (long i = 0; numEvents == 0 || i < numEvents; i++) {
                DataPoint dp = source.next();
                synchronized (lock) {
                    all.processUpdate(dp);
                    query.processUpdate(dp); // prints the update if watching
                    generated++;
                }
                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException e) {
            synchronized (lock) {
                running = false;
            }
            return; // STOP DATA or EXIT
        }
        synchronized (lock) {
            running = false;
            if (watching) { // nothing will change any more, so hand the prompt back
                query.setOnUpdate(null);
                watching = false;
                System.out.print("\nThe generator finished after " + generated + " trade(s).\n\ntsdb> ");
                System.out.flush();
            }
        }
    }

    private void stopData() {
        if (!isRunning()) {
            throw new IllegalArgumentException("The generator isn't running");
        }
        stopGenerator();
        System.out.println("Stopped the generator after " + generated + " trade(s). The window can still be queried;"
                + " GENERATE DATA starts over.");
    }

    private void stopGenerator() {
        if (generator == null) return;
        generator.interrupt();
        try {
            generator.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- Queries: AGGREGATE aggregates, or [ALSO] FILTER condition [AGGREGATE aggregates] ----

    /** AGGREGATE on its own: new aggregates over the current query's trades. */
    private void aggregate(String rest) {
        if (rest.isBlank()) {
            throw new IllegalArgumentException("AGGREGATE needs at least one aggregate, e.g. AGGREGATE AVERAGE, COUNT");
        }
        if (FILTER.matcher(rest).find()) {
            throw new IllegalArgumentException("FILTER goes first, e.g. FILTER ticker = AAPL AGGREGATE AVERAGE");
        }
        List<Supplier<Aggregator>> aggs = parseAggregates(rest);
        requireData();
        synchronized (lock) {
            setQuery(query.getFilter(), aggs, query.getWindow().contents());
            watch();
        }
    }

    /** Parses both halves before changing anything, so a typo in either one has no effect. */
    private void filterThenAggregate(String rest, boolean also) {
        Matcher m = AGGREGATE_KEYWORD.matcher(rest);
        boolean hasAggs = m.find();
        String condition = hasAggs ? rest.substring(0, m.start()) : rest;
        if (AGGREGATE_NAME.matcher(condition).find() || GROUP_BY.matcher(condition).find()) {
            throw new IllegalArgumentException("Aggregates go after AGGREGATE, e.g. FILTER side = BUY AGGREGATE AVERAGE GROUP BY symbol");
        }
        Filter filter = parseFilter(condition);
        if (hasAggs && rest.substring(m.end()).isBlank()) {
            throw new IllegalArgumentException("AGGREGATE needs at least one aggregate, e.g. AGGREGATE AVERAGE, COUNT");
        }
        List<Supplier<Aggregator>> aggs = hasAggs ? parseAggregates(rest.substring(m.end())) : List.of();
        if (also && filter == Filter.ACCEPT_ALL) {
            throw new IllegalArgumentException("ALSO FILTER needs a condition; FILTER NONE clears the filter");
        }

        requireData();
        synchronized (lock) {
            if (also) {
                Filter combined = and(query.getFilter(), filter);
                List<DataPoint> current = query.getWindow().contents();
                setQuery(combined, hasAggs ? aggs : queryAggs, current);
                System.out.println("Query narrowed to " + combined.describe() + ": " + query.getWindow().size()
                        + " of " + current.size() + " trade(s) still match.");
            } else {
                List<DataPoint> window = all.getWindow().contents();
                setQuery(filter, aggs, window);
                if (filter == Filter.ACCEPT_ALL) {
                    System.out.println("Filter cleared: back to all " + window.size() + " trade(s) in the window.");
                } else {
                    System.out.println("New query " + filter.describe() + ": " + query.getWindow().size()
                            + " of the " + window.size() + " trade(s) in the window match.");
                }
            }
            if (!query.getAggregators().isEmpty()) watch(); // without aggregates the line above is the whole answer
        }
    }

    /**
     * "AVERAGE, MAX qty, COUNT [GROUP BY symbol]" -> one factory per aggregate.
     * The field defaults to price; GROUP BY applies to every aggregate in the list.
     */
    static List<Supplier<Aggregator>> parseAggregates(String text) {
        FieldFilter.Field groupField = null;
        Matcher gb = GROUP_BY.matcher(text);
        if (gb.find()) {
            String name = text.substring(gb.end()).trim();
            if (!name.matches("[A-Za-z]+")) {
                throw new IllegalArgumentException("GROUP BY goes last, followed by symbol, ticker or side,"
                        + " e.g. AVERAGE, COUNT GROUP BY symbol");
            }
            groupField = FieldFilter.Field.fromName(name);
            if (groupField.numeric) {
                throw new IllegalArgumentException("GROUP BY needs symbol (or ticker) or side, got " + groupField.label());
            }
            text = text.substring(0, gb.start());
        }

        List<Supplier<Aggregator>> out = new ArrayList<>();
        for (String part : text.split(",", -1)) {
            Supplier<Aggregator> one = parseAggregate(part);
            FieldFilter.Field by = groupField;
            out.add(by == null ? one : () -> new GroupByAggregator(by, one));
        }
        return out;
    }

    /** "AVERAGE", "MAX qty", "COUNT" -> a factory for that aggregate. */
    private static Supplier<Aggregator> parseAggregate(String part) {
        if (part.isBlank()) throw new IllegalArgumentException("Missing an aggregate between commas");
        Matcher m = AGGREGATE.matcher(part);
        if (!m.matches()) {
            throw new IllegalArgumentException("Can't read '" + part.trim() + "'. Expected e.g. AVERAGE, MAX qty, COUNT");
        }
        String name = m.group(1).toUpperCase(Locale.ROOT);
        if (name.equals("COUNT")) {
            if (m.group(2) != null) throw new IllegalArgumentException("COUNT takes no field, just COUNT");
            return CountAgg::new;
        }
        Function<FieldFilter.Field, Aggregator> make;
        switch (name) {
            case "AVERAGE":
            case "AVG": make = AvgAgg::new; break;
            case "SUM": make = SumAgg::new; break;
            case "MIN": make = MinAgg::new; break;
            case "MAX": make = MaxAgg::new; break;
            default:
                throw new IllegalArgumentException("Unknown aggregate '" + m.group(1)
                        + "'. Expected AVERAGE, SUM, MIN, MAX or COUNT");
        }
        FieldFilter.Field field = m.group(2) == null ? FieldFilter.Field.PRICE : FieldFilter.Field.fromName(m.group(2));
        if (!field.numeric) {
            throw new IllegalArgumentException(name + " needs a numeric field (price or qty), got " + field.label());
        }
        return () -> make.apply(field);
    }

    /**
     * FILTER's condition in FilterParser syntax; NONE means no filter. A
     * ticker the generator never trades is rejected, since filtering on a
     * typo would quietly empty the window.
     */
    static Filter parseFilter(String text) {
        if (text.isBlank()) {
            throw new IllegalArgumentException("FILTER needs a condition, e.g. FILTER ticker = AAPL AND price < 200");
        }
        if (text.trim().equalsIgnoreCase("NONE")) return Filter.ACCEPT_ALL;

        Filter filter = FilterParser.parse(text);
        for (String condition : AND_OR.split(text)) {
            Matcher m = SYMBOL_CONDITION.matcher(condition);
            if (m.matches() && !FinancialDataGenerator.SYMBOLS.contains(m.group(1).toUpperCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Unknown ticker '" + m.group(1) + "'. Expected one of "
                        + String.join(", ", FinancialDataGenerator.SYMBOLS));
            }
        }
        return filter;
    }

    /** current AND extra, flattened so it reads "a AND b AND c" rather than nesting. */
    private static Filter and(Filter current, Filter extra) {
        if (current == Filter.ACCEPT_ALL) return extra;
        List<Filter> parts = new ArrayList<>();
        for (Filter f : List.of(current, extra)) {
            if (f instanceof AndFilter a) parts.addAll(a.getChildren());
            else parts.add(f);
        }
        return new AndFilter(parts);
    }

    /**
     * Swaps in a query engine with this filter and fresh aggregators, primed
     * with {@code points} (oldest first). Caller holds lock.
     */
    private void setQuery(Filter filter, List<Supplier<Aggregator>> aggs, List<DataPoint> points) {
        List<Aggregator> fresh = new ArrayList<>();
        for (Supplier<Aggregator> a : aggs) fresh.add(a.get());
        query = new StreamEngine(newWindow.get(), filter, fresh);
        for (DataPoint p : points) query.processUpdate(p);
        queryAggs = aggs;
    }

    // ---- Watching ----

    /**
     * Prints the query's result now and, while the generator runs, again as
     * its window changes, until Enter or the next command. Caller holds lock.
     */
    private void watch() {
        boolean live = running;
        String when = query.getAggregators().isEmpty() ? "" : " A new line prints whenever a value changes.";
        System.out.println((live ? "Watching " : "Showing ") + describeQuery()
                + (live ? "." + when + " Press Enter to stop." : ". The generator is stopped, so this won't change."));
        printResult(false);
        if (live) startWatching();
    }

    /** Caller holds lock. */
    private void startWatching() {
        query.setOnUpdate(w -> printResult(true));
        watching = true;
    }

    /** Picks the live view back up after a command that failed. */
    private void resumeWatching() {
        synchronized (lock) {
            if (!running) return;
            System.out.println("Still watching " + describeQuery() + ". Press Enter to stop.");
            startWatching();
        }
    }

    private boolean isWatching() {
        synchronized (lock) {
            return watching;
        }
    }

    private boolean isRunning() {
        synchronized (lock) {
            return running;
        }
    }

    /** Ends the live view, if there is one; returns whether there was. */
    private boolean stopWatching() {
        synchronized (lock) {
            boolean was = watching;
            if (query != null) query.setOnUpdate(null);
            watching = false;
            return was;
        }
    }

    // ---- Output ----

    /** e.g. "AVERAGE price, COUNT of symbol=NVDA in a count-based window (last 10 point(s))". Caller holds lock. */
    private String describeQuery() {
        Filter f = query.getFilter();
        String trades = (f == Filter.ACCEPT_ALL ? "every trade" : f.describe()) + " in a " + query.getWindow().describe();
        if (query.getAggregators().isEmpty()) return trades;
        StringJoiner names = new StringJoiner(", ");
        for (Aggregator a : query.getAggregators()) names.add(a.describe());
        return names + " of " + trades;
    }

    /**
     * Without aggregates: the query's window, after the trade that just came
     * in (if {@code update}). With aggregates: the time of the newest trade
     * and every aggregate's value, on one line (with GROUP BY, one line per
     * aggregate listing each group's value); an update whose values didn't
     * change prints nothing. Caller holds lock.
     */
    private void printResult(boolean update) {
        List<DataPoint> contents = query.getWindow().contents();
        DataPoint newest = contents.isEmpty() ? null : contents.get(contents.size() - 1); // the newest point is last
        List<Aggregator> aggs = query.getAggregators();
        if (aggs.isEmpty()) {
            if (update) System.out.println("\nIncoming: " + newest);
            System.out.println("Window (" + contents.size() + " trade(s)):");
            for (DataPoint p : contents) System.out.println("  " + p);
            return;
        }

        StringBuilder values = new StringBuilder();
        if (aggs.get(0) instanceof GroupByAggregator) { // GROUP BY applies to every aggregate in the list
            for (Aggregator a : aggs) {
                StringJoiner groups = new StringJoiner("   ").setEmptyValue("(no trades)");
                ((GroupByAggregator) a).results().forEach((group, v) -> groups.add(group + " " + fmt(v)));
                values.append("\n      ").append(a.describe()).append(": ").append(groups);
            }
        } else {
            StringJoiner line = new StringJoiner("   ", "  ", "");
            for (Aggregator a : aggs) line.add(a.describe() + " = " + fmt(a.result()));
            values.append(line);
        }
        String text = values.toString();
        if (update && text.equals(lastValues)) return;
        lastValues = text;
        String time = newest == null ? "(empty)" : DataPoint.formatTimestamp(newest.getTimestampMillis());
        System.out.printf("  %-12s%s%n", time, text);
    }

    /** Whole numbers without decimals; the 1e15 cap keeps the long cast exact. */
    private static String fmt(double v) {
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? String.valueOf((long) v) : String.format("%.2f", v);
    }

    private void requireData() {
        if (all == null) {
            throw new IllegalArgumentException("No data yet. Start the generator first, e.g. GENERATE DATA window=count size=20");
        }
    }

    private static void noArguments(String[] words) {
        if (words.length > 1) {
            throw new IllegalArgumentException(words[0].toUpperCase(Locale.ROOT) + " takes no arguments");
        }
    }

    private static void printHelp() {
        System.out.println("Commands (case-insensitive). WINDOW and the aggregates keep printing as the window moves,");
        System.out.println("until you press Enter or type another command, which replaces them.");
        System.out.println("  GENERATE DATA window=<count|time> size=<n> [interval=<ms>] [events=<n>]");
        System.out.println("                    Start generating trades in the background (labels in any order). The window");
        System.out.println("                    keeps the last <size> trades (count) or <size> seconds of trades (time).");
        System.out.println("                    About one trade every interval ms (default 400); events = how many trades,");
        System.out.println("                    0 or omitted = until STOP DATA. e.g. GENERATE DATA window=count size=20");
        System.out.println("  WINDOW            Watch the window trade by trade (only trades that match the current filter).");
        System.out.println("  AGGREGATE <aggregates>");
        System.out.println("                    Watch aggregates over that window; a line prints whenever a value changes.");
        System.out.println("                    AVERAGE, SUM, MIN, MAX [field] and COUNT, separated by commas; field is");
        System.out.println("                    price (default) or qty. e.g. AGGREGATE AVERAGE, MAX qty, COUNT");
        System.out.println("                    End with GROUP BY symbol|side for one value per ticker (or side):");
        System.out.println("                    AGGREGATE AVERAGE, COUNT GROUP BY symbol");
        System.out.println("  FILTER <condition> [AGGREGATE <aggregates>]");
        System.out.println("                    Start a new query over everything in the window: only matching trades,");
        System.out.println("                    now and as they arrive. Prints how many match; with aggregates, watches");
        System.out.println("                    them. FILTER NONE goes back to the whole window.");
        System.out.println("                    Conditions are <field> <op> <value> joined with AND / OR, e.g.");
        System.out.println("                    FILTER ticker = AAPL AND price < 200");
        System.out.println("                    fields: ticker (or symbol), side, price, qty   ops: = != > < >= <=");
        System.out.println("  ALSO FILTER <condition> [AGGREGATE <aggregates>]");
        System.out.println("                    Narrow the current query further. After FILTER ticker = AAPL,");
        System.out.println("                    ALSO FILTER price < 200 gives ticker = AAPL AND price < 200.");
        System.out.println("                    Keeps (and watches) the query's aggregates unless you give new ones.");
        System.out.println("  (Enter)           Stop watching. The generator keeps running.");
        System.out.println("  STOP DATA         Stop the generator. The window can still be queried.");
        System.out.println("  HELP, EXIT");
        System.out.println("Tickers: " + String.join(", ", FinancialDataGenerator.SYMBOLS));
    }
}
