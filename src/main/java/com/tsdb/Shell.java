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
 * Interactive shell: start the data generator, then define queries over its
 * window and watch them from the same terminal while it keeps running in the
 * background.
 *
 * Every statement ends with a semicolon and may span several lines; the
 * prompt changes to "->" while a statement is unfinished. A query only
 * defines what to look at; WINDOW watches it live (the matching trades, or
 * an aggregate line whenever a value changes) until Enter (an empty line) or
 * the next statement.
 *
 * Statements (keywords are case-insensitive):
 * <pre>
 *   GENERATE DATA window=&lt;count|time&gt; size=&lt;n&gt; [interval=&lt;ms&gt;] [events=&lt;n&gt;];
 *                         start generating trades in the background (labels in any order)
 *   SELECT * | aggregates [WHERE condition] [GROUP BY symbol|side];
 *                         a new query over everything in the window. Aggregates are
 *                         AVERAGE | SUM | MIN | MAX [price|qty] and COUNT, price by
 *                         default, separated by commas
 *   ALSO [SELECT aggregates] [WHERE condition] [GROUP BY symbol|side];
 *                         add to the current query: its filter AND condition, plus
 *                         the new aggregates; GROUP BY replaces the grouping
 *   WINDOW;               watch the current query live
 *   (Enter)               stop watching; the generator keeps running
 *   STOP DATA;            stop the generator; the window can still be queried
 *   HELP;  EXIT
 * </pre>
 * The condition uses the FilterParser syntax (ticker = AAPL, price < 200,
 * side = BUY, joined with AND / OR); ticker is another name for symbol.
 *
 * The generator feeds two engines with the same window type and size: one
 * keeps every trade, the other runs the current query, i.e. its filter and
 * its aggregators, which update incrementally as trades arrive and leave.
 * SELECT rebuilds the query from the first engine, so it sees the whole
 * window; ALSO rebuilds it from the query's own contents. Nothing is written
 * to disk.
 *
 * Usage:
 *   java -cp out com.tsdb.Shell
 */
public class Shell {

    private static final Set<String> AGGREGATES = Set.of("AVERAGE", "AVG", "SUM", "MIN", "MAX", "COUNT");
    private static final Set<String> GENERATE_LABELS = Set.of("window", "size", "interval", "events");
    private static final String GENERATE_USAGE =
            "Usage: GENERATE DATA window=<count|time> size=<n> [interval=<ms>] [events=<n>];";
    private static final Pattern SELECT_KEYWORD = Pattern.compile("^SELECT(?:\\s|$)", Pattern.CASE_INSENSITIVE);
    private static final Pattern WHERE = Pattern.compile("(?:^|\\s)WHERE(?:\\s|$)", Pattern.CASE_INSENSITIVE);
    // An aggregate name (or SELECT) inside a condition means the clauses are out of order.
    private static final Pattern AGGREGATE_NAME = Pattern.compile(
            "(?:^|\\s)(?:SELECT|" + String.join("|", AGGREGATES) + ")\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern AND_OR = Pattern.compile("\\s+(?:AND|OR)\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern SYMBOL_CONDITION = Pattern.compile(
            "^\\s*(?:symbol|ticker)\\s*(?:==|!=|=)\\s*['\"]?([A-Za-z]+)['\"]?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern AGGREGATE = Pattern.compile("^\\s*([A-Za-z]+)(?:\\s+([A-Za-z]+))?\\s*$");
    private static final Pattern GROUP_BY = Pattern.compile("\\s+GROUP\\s+BY(?:\\s+|$)", Pattern.CASE_INSENSITIVE);

    // The lock guards every field below except generator: the generator thread feeds trades and
    // prints live updates, the prompt thread swaps queries. Holding it while printing keeps a
    // statement's output in one piece.
    private final Object lock = new Object();
    private StreamEngine all;                 // every trade, no filter; null until GENERATE DATA
    private StreamEngine query;               // the same trades through the current query's filter and aggregators
    private List<Supplier<Aggregator>> queryAggs = List.of(); // the query's aggregates, before GROUP BY
    private FieldFilter.Field queryGroupBy;   // the query's GROUP BY field; null if none
    private Supplier<Window> newWindow;       // empty window of the same type and size, for rebuilding the query
    private boolean watching;                 // the query's result is printed after every change
    private String lastValues;                // the aggregate values printed last, so unchanged ones aren't repeated
    private boolean running;                  // the generator is still producing; unlike generator.isAlive(),
                                              // it turns false inside the lock, before the thread winds down
    private long generated;
    private Thread generator;

    /** The clauses of a SELECT or ALSO statement; null where a clause was left out. */
    static final class Clauses {
        List<Supplier<Aggregator>> select; // an empty list for SELECT *
        Filter where;
        FieldFilter.Field groupBy;
    }

    public static void main(String[] args) throws IOException {
        new Shell().run();
    }

    /** Reads lines until a ; ends a statement, then runs it. A line may also hold several statements. */
    private void run() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("End every statement with ; (it can span several lines). Start with e.g.\n"
                + "GENERATE DATA window=count size=20;  HELP; lists every statement, EXIT quits.\n");
        StringBuilder pending = new StringBuilder(); // the unfinished statement typed so far
        boolean watchedBefore = false;               // a live view was running when the pending statement began
        while (true) {
            if (!isWatching()) {
                System.out.print(pending.length() == 0 ? "tsdb> " : "   -> ");
                System.out.flush();
            }
            String line = in.readLine();
            if (line == null) { // end of input (Ctrl+D, or Ctrl+Z then Enter on Windows)
                System.out.println();
                break;
            }
            boolean wasWatching = stopWatching(); // any input ends the live view; WINDOW starts a new one
            String trimmed = line.trim();
            if (pending.length() == 0) {
                if (trimmed.isEmpty()) {
                    if (wasWatching) System.out.println("Stopped watching. The generator keeps running; STOP DATA; stops it.\n");
                    continue;
                }
                if (isExit(trimmed)) break;
                watchedBefore = wasWatching;
            } else if (trimmed.isEmpty()) {
                System.out.println("(The statement isn't finished: end it with ;)");
                continue;
            }
            pending.append(line).append('\n');

            List<String> statements = new ArrayList<>();
            for (int end; (end = pending.indexOf(";")) >= 0; pending.delete(0, end + 1)) {
                String s = pending.substring(0, end).trim();
                if (!s.isEmpty()) statements.add(s);
            }
            if (pending.toString().isBlank()) pending.setLength(0);
            if (statements.isEmpty()) continue;

            if (watchedBefore) System.out.println();
            for (int i = 0; i < statements.size(); i++) {
                String s = statements.get(i);
                if (isExit(s)) {
                    stopGenerator();
                    return;
                }
                boolean resume = i == 0 ? watchedBefore : stopWatching(); // the previous one may have started watching
                try {
                    execute(s);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error: " + e.getMessage());
                    if (resume) resumeWatching(); // a failed statement changes nothing, so a typo doesn't end the live view
                }
            }
            if (!isWatching()) System.out.println();
        }
        stopGenerator();
    }

    private static boolean isExit(String s) {
        return s.matches("(?i)(exit|quit)\\s*;?");
    }

    private void execute(String statement) {
        String[] words = statement.split("\\s+");
        String rest = statement.substring(words[0].length());
        switch (words[0].toUpperCase(Locale.ROOT)) {
            case "GENERATE":
                generate(statement);
                return;
            case "SELECT":
                query(rest, false);
                return;
            case "ALSO":
                query(rest, true);
                return;
            case "WINDOW":
                noArguments(words);
                requireData();
                synchronized (lock) {
                    watch();
                }
                return;
            case "STOP":
                if (words.length != 2 || !words[1].equalsIgnoreCase("DATA")) {
                    throw new IllegalArgumentException("STOP DATA; stops the generator; press Enter to stop watching");
                }
                stopData();
                return;
            case "HELP":
                noArguments(words);
                printHelp();
                return;
            case "FILTER":
                throw new IllegalArgumentException("FILTER is now WHERE: SELECT * WHERE ticker = AAPL; starts a query,"
                        + " ALSO WHERE price < 200; narrows it");
            case "AGGREGATE":
                throw new IllegalArgumentException("Aggregates now go after SELECT, e.g. SELECT AVERAGE, COUNT WHERE ticker = AAPL;");
            case "WHERE":
            case "GROUP":
                throw new IllegalArgumentException("Start with SELECT for a new query or ALSO to add to the current one,"
                        + " e.g. ALSO " + statement + ";");
            default:
                if (AGGREGATES.contains(words[0].replaceAll("[^A-Za-z]", "").toUpperCase(Locale.ROOT))) {
                    throw new IllegalArgumentException("Put SELECT first, e.g. SELECT " + statement + ";");
                }
                throw new IllegalArgumentException("Unknown statement '" + words[0] + "'. Type HELP; to see the statements");
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
            throw new IllegalArgumentException("Already generating. STOP DATA; first to start over");
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
            setQuery(Filter.ACCEPT_ALL, List.of(), null, List.of());
            generated = 0;
            running = true;
        }
        generator = new Thread(() -> produce(source, intervalMs, numEvents), "generator");
        generator.setDaemon(true); // don't keep the JVM alive after EXIT
        generator.start();

        System.out.println("Generating trades into a " + window.describe() + ", about one every " + intervalMs + " ms"
                + (numEvents == 0 ? " until STOP DATA." : ", " + numEvents + " in total.")
                + " The query starts as SELECT *; WINDOW; watches it.");
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

    // ---- Queries: SELECT ... and ALSO ... ----

    /**
     * SELECT starts a new query over the whole window; ALSO adds to the
     * current one (its filter AND the new condition, its aggregates plus the
     * new ones) over the trades the current query holds. Everything is parsed
     * before anything changes, so a typo has no effect.
     */
    private void query(String rest, boolean also) {
        Clauses c = parseClauses(rest, also);
        requireData();
        synchronized (lock) {
            if (also) {
                Filter filter = c.where == null ? query.getFilter() : and(query.getFilter(), c.where);
                List<Supplier<Aggregator>> aggs = new ArrayList<>(queryAggs);
                if (c.select != null) aggs.addAll(c.select);
                FieldFilter.Field by = c.groupBy != null ? c.groupBy : queryGroupBy;
                requireAggregatesFor(by, aggs);
                List<DataPoint> current = query.getWindow().contents();
                setQuery(filter, aggs, by, current);
                System.out.println("Query is now " + describeQuery() + ": " + query.getWindow().size()
                        + " of " + current.size() + " trade(s) still match. WINDOW; watches it.");
            } else {
                requireAggregatesFor(c.groupBy, c.select);
                List<DataPoint> window = all.getWindow().contents();
                setQuery(c.where == null ? Filter.ACCEPT_ALL : c.where, c.select, c.groupBy, window);
                System.out.println("New query " + describeQuery() + ": " + query.getWindow().size()
                        + " of the " + window.size() + " trade(s) in the window match. WINDOW; watches it.");
            }
        }
    }

    private static void requireAggregatesFor(FieldFilter.Field groupBy, List<Supplier<Aggregator>> aggs) {
        if (groupBy != null && aggs.isEmpty()) {
            throw new IllegalArgumentException("GROUP BY needs aggregates, e.g. SELECT AVERAGE, COUNT GROUP BY symbol;");
        }
    }

    /**
     * The clauses after SELECT ("* | aggregates [WHERE condition] [GROUP BY field]")
     * or after ALSO ("[SELECT aggregates] [WHERE condition] [GROUP BY field]",
     * at least one of them). Clauses go in that order.
     */
    static Clauses parseClauses(String text, boolean also) {
        Clauses c = new Clauses();
        String rest = " " + text.trim(); // GROUP_BY needs whitespace before GROUP
        Matcher gb = GROUP_BY.matcher(rest);
        if (gb.find()) {
            c.groupBy = parseGroupBy(rest.substring(gb.end()));
            rest = rest.substring(0, gb.start());
        }
        rest = rest.trim();

        String select = rest;
        Matcher w = WHERE.matcher(rest);
        if (w.find()) {
            select = rest.substring(0, w.start()).trim();
            String condition = rest.substring(w.end());
            if (AGGREGATE_NAME.matcher(condition).find()) {
                throw new IllegalArgumentException("Aggregates go after SELECT, before WHERE,"
                        + " e.g. SELECT AVERAGE WHERE side = BUY GROUP BY symbol;");
            }
            if (WHERE.matcher(condition).find()) {
                throw new IllegalArgumentException("WHERE appears twice; join conditions with AND / OR");
            }
            c.where = parseFilter(condition);
        }

        if (also) {
            if (!select.isEmpty()) {
                Matcher s = SELECT_KEYWORD.matcher(select);
                if (!s.find()) {
                    throw new IllegalArgumentException("ALSO is followed by SELECT, WHERE or GROUP BY,"
                            + " e.g. ALSO WHERE price < 200;");
                }
                select = select.substring(s.end()).trim();
                if (select.equals("*")) {
                    throw new IllegalArgumentException("ALSO SELECT adds aggregates; SELECT * on its own starts a new query");
                }
                c.select = parseSelectList(select);
            } else if (c.where == null && c.groupBy == null) {
                throw new IllegalArgumentException("ALSO needs SELECT, WHERE or GROUP BY, e.g. ALSO WHERE price < 200;");
            }
        } else {
            c.select = select.equals("*") ? List.of() : parseSelectList(select);
        }
        return c;
    }

    private static List<Supplier<Aggregator>> parseSelectList(String text) {
        if (text.isBlank()) {
            throw new IllegalArgumentException("SELECT needs * or aggregates, e.g. SELECT * WHERE ticker = AAPL;"
                    + " or SELECT AVERAGE, COUNT;");
        }
        List<Supplier<Aggregator>> out = new ArrayList<>();
        for (String part : text.split(",", -1)) out.add(parseAggregate(part));
        return out;
    }

    /**
     * "AVERAGE, MAX qty, COUNT [GROUP BY symbol]" -> one factory per aggregate.
     * The field defaults to price; GROUP BY applies to every aggregate in the list.
     */
    static List<Supplier<Aggregator>> parseAggregates(String text) {
        FieldFilter.Field groupField = null;
        Matcher gb = GROUP_BY.matcher(text);
        if (gb.find()) {
            groupField = parseGroupBy(text.substring(gb.end()));
            text = text.substring(0, gb.start());
        }
        return grouped(parseSelectList(text), groupField);
    }

    /** The field after GROUP BY: symbol (or ticker) or side, and nothing after it. */
    private static FieldFilter.Field parseGroupBy(String text) {
        String name = text.trim();
        if (!name.matches("[A-Za-z]+")) {
            throw new IllegalArgumentException("GROUP BY goes last, followed by symbol, ticker or side,"
                    + " e.g. SELECT AVERAGE, COUNT GROUP BY symbol;");
        }
        FieldFilter.Field field = FieldFilter.Field.fromName(name);
        if (field.numeric) {
            throw new IllegalArgumentException("GROUP BY needs symbol (or ticker) or side, got " + field.label());
        }
        return field;
    }

    /** Wraps each aggregate in a GroupByAggregator on {@code by}, or returns them as they are if by is null. */
    private static List<Supplier<Aggregator>> grouped(List<Supplier<Aggregator>> aggs, FieldFilter.Field by) {
        if (by == null) return aggs;
        List<Supplier<Aggregator>> out = new ArrayList<>();
        for (Supplier<Aggregator> one : aggs) out.add(() -> new GroupByAggregator(by, one));
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
     * WHERE's condition in FilterParser syntax. A ticker the generator never
     * trades is rejected, since filtering on a typo would quietly empty the window.
     */
    static Filter parseFilter(String text) {
        if (text.isBlank()) {
            throw new IllegalArgumentException("WHERE needs a condition, e.g. WHERE ticker = AAPL AND price < 200");
        }
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
     * Swaps in a query engine with this filter and fresh aggregators (grouped
     * by {@code groupBy} if it isn't null), primed with {@code points} (oldest
     * first). Caller holds lock.
     */
    private void setQuery(Filter filter, List<Supplier<Aggregator>> aggs, FieldFilter.Field groupBy,
                          List<DataPoint> points) {
        List<Aggregator> fresh = new ArrayList<>();
        for (Supplier<Aggregator> a : grouped(aggs, groupBy)) fresh.add(a.get());
        query = new StreamEngine(newWindow.get(), filter, fresh);
        for (DataPoint p : points) query.processUpdate(p);
        queryAggs = aggs;
        queryGroupBy = groupBy;
    }

    // ---- Watching ----

    /**
     * Prints the query's result now and, while the generator runs, again as
     * its window changes, until Enter or the next statement. Caller holds lock.
     */
    private void watch() {
        boolean live = running;
        String when = query.getAggregators().isEmpty() ? "" : " A new line prints whenever a value changes.";
        System.out.println((live ? "Watching " : "Showing ") + describeQuery() + " over a " + query.getWindow().describe()
                + (live ? "." + when + " Press Enter to stop." : ". The generator is stopped, so this won't change."));
        printResult(false);
        if (live) startWatching();
    }

    /** Caller holds lock. */
    private void startWatching() {
        query.setOnUpdate(w -> printResult(true));
        watching = true;
    }

    /** Picks the live view back up after a statement that failed. */
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

    /** The current query as a statement, e.g. "SELECT AVERAGE price, COUNT WHERE symbol=NVDA GROUP BY side". Caller holds lock. */
    private String describeQuery() {
        StringJoiner names = new StringJoiner(", ");
        for (Supplier<Aggregator> a : queryAggs) names.add(a.get().describe());
        StringBuilder sb = new StringBuilder("SELECT ").append(queryAggs.isEmpty() ? "*" : names.toString());
        Filter f = query.getFilter();
        if (f != Filter.ACCEPT_ALL) sb.append(" WHERE ").append(f.describe());
        if (queryGroupBy != null) sb.append(" GROUP BY ").append(queryGroupBy.label());
        return sb.toString();
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
            throw new IllegalArgumentException("No data yet. Start the generator first, e.g. GENERATE DATA window=count size=20;");
        }
    }

    private static void noArguments(String[] words) {
        if (words.length > 1) {
            throw new IllegalArgumentException(words[0].toUpperCase(Locale.ROOT) + " takes no arguments");
        }
    }

    private static void printHelp() {
        System.out.println("Statements (case-insensitive) end with ; and can span several lines.");
        System.out.println("  GENERATE DATA window=<count|time> size=<n> [interval=<ms>] [events=<n>];");
        System.out.println("                    Start generating trades in the background (labels in any order). The window");
        System.out.println("                    keeps the last <size> trades (count) or <size> seconds of trades (time).");
        System.out.println("                    About one trade every interval ms (default 400); events = how many trades,");
        System.out.println("                    0 or omitted = until STOP DATA. e.g. GENERATE DATA window=count size=20;");
        System.out.println("  SELECT * | <aggregates> [WHERE <condition>] [GROUP BY symbol|side];");
        System.out.println("                    A new query over everything in the window. * = the matching trades;");
        System.out.println("                    aggregates are AVERAGE, SUM, MIN, MAX [field] and COUNT, separated by");
        System.out.println("                    commas; field is price (default) or qty. Conditions are <field> <op> <value>");
        System.out.println("                    joined with AND / OR; fields: ticker (or symbol), side, price, qty;");
        System.out.println("                    ops: = != > < >= <=. e.g.");
        System.out.println("                      SELECT AVERAGE, MAX qty, COUNT");
        System.out.println("                        WHERE ticker = AAPL AND price < 200");
        System.out.println("                        GROUP BY side;");
        System.out.println("  ALSO [SELECT <aggregates>] [WHERE <condition>] [GROUP BY symbol|side];");
        System.out.println("                    Add to the current query instead of starting over: WHERE narrows it");
        System.out.println("                    (old condition AND new one), SELECT adds aggregates, GROUP BY regroups.");
        System.out.println("                    e.g. ALSO WHERE side = BUY;");
        System.out.println("  WINDOW;           Watch the current query live: its trades, or a line whenever an aggregate");
        System.out.println("                    changes.");
        System.out.println("  (Enter)           Stop watching. The generator keeps running.");
        System.out.println("  STOP DATA;        Stop the generator. The window can still be queried.");
        System.out.println("  HELP;  EXIT");
        System.out.println("Tickers: " + String.join(", ", FinancialDataGenerator.SYMBOLS));
    }
}
