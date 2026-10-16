package com.tsdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns an aggregate string into {@link Aggregator}s.
 *
 * Grammar (keywords and names are case-insensitive):
 * <pre>
 *   query  := agg ( , agg )* [ GROUP BY groupField ]
 *   agg    := count | count(*) | SUM(field) | AVG(field) | MIN(field) | MAX(field)
 *   field  := price | quantity | qty
 *   groupField := symbol | side
 * </pre>
 * With GROUP BY, each aggregate is wrapped in a {@link GroupByAggregator}.
 * Any malformed input throws IllegalArgumentException with a readable message.
 */
public final class AggregatorParser {

    private static final Pattern GROUP_BY = Pattern.compile("\\s+GROUP\\s+BY\\s+([A-Za-z_]+)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern AGG = Pattern.compile("^\\s*([A-Za-z]+)\\s*(?:\\(\\s*([A-Za-z_*]+)\\s*\\))?\\s*$");

    private AggregatorParser() {}

    public static List<Aggregator> parse(String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("Aggregate list is empty. Example: \"avg(price), max(price), count\"");
        }
        String body = text.trim();
        FieldFilter.Field groupField = null;
        Matcher gb = GROUP_BY.matcher(body);
        if (gb.find()) {
            groupField = FieldFilter.Field.fromName(gb.group(1));
            if (groupField.numeric) {
                throw new IllegalArgumentException("GROUP BY needs symbol or side, got '" + groupField.label() + "'");
            }
            body = body.substring(0, gb.start());
        }

        List<Aggregator> out = new ArrayList<>();
        for (String part : body.split(",", -1)) {
            Supplier<Aggregator> factory = parseOne(part);
            out.add(groupField == null ? factory.get() : new GroupByAggregator(groupField, factory));
        }
        return out;
    }

    private static Supplier<Aggregator> parseOne(String part) {
        Matcher m = AGG.matcher(part);
        if (!m.matches()) {
            throw new IllegalArgumentException("Cannot parse aggregate '" + part.trim()
                    + "'. Expected e.g. avg(price), min(qty), count");
        }
        String fn = m.group(1).toUpperCase(Locale.ROOT);
        String arg = m.group(2);

        if (fn.equals("COUNT")) {
            if (arg != null && !arg.equals("*")) {
                throw new IllegalArgumentException("count takes no field (use count or count(*)), got count(" + arg + ")");
            }
            return CountAgg::new;
        }
        if (arg == null) {
            throw new IllegalArgumentException(fn + " needs a field, e.g. " + fn + "(price)");
        }
        FieldFilter.Field field = FieldFilter.Field.fromName(arg);
        if (!field.numeric) {
            throw new IllegalArgumentException(fn + " needs a numeric field (price or quantity), got '" + field.label() + "'");
        }
        switch (fn) {
            case "SUM": return () -> new SumAgg(field);
            case "AVG": return () -> new AvgAgg(field);
            case "MIN": return () -> new MinAgg(field);
            case "MAX": return () -> new MaxAgg(field);
            default:
                throw new IllegalArgumentException("Unknown aggregate '" + m.group(1)
                        + "'. Expected one of: count, sum, avg, min, max");
        }
    }

    /** SQL-style SELECT clause for a parsed list, e.g. "SELECT AVG(price), COUNT(*) FROM trades GROUP BY symbol". */
    public static String toSql(List<Aggregator> aggs, Filter filter) {
        List<String> cols = new ArrayList<>();
        FieldFilter.Field groupField = null;
        for (Aggregator a : aggs) {
            if (a instanceof GroupByAggregator g) {
                groupField = g.getGroupField();
                cols.add(g.getInnerLabel());
            } else {
                cols.add(a.describe());
            }
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        if (groupField != null) sql.append(groupField.label()).append(", ");
        sql.append(String.join(", ", cols)).append(" FROM trades");
        if (filter != null && filter != Filter.ACCEPT_ALL) sql.append(" WHERE ").append(FilterParser.toSql(filter));
        if (groupField != null) sql.append(" GROUP BY ").append(groupField.label());
        return sql.toString();
    }
}
