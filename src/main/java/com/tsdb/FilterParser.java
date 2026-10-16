package com.tsdb;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns a filter string into a {@link Filter}.
 *
 * Grammar (no parentheses, keywords are case-insensitive):
 * <pre>
 *   expr      := andGroup ( OR andGroup )*
 *   andGroup  := condition ( AND condition )*
 *   condition := field op value
 *   field     := symbol | side | price | quantity | qty
 *   op        := = | == | != | > | < | >= | <=
 *   value     := a single token, optionally quoted (no spaces or = < > !)
 * </pre>
 * AND binds tighter than OR, like SQL:
 * {@code symbol=AAPL AND price>200 OR symbol=NVDA} means
 * {@code (symbol=AAPL AND price>200) OR symbol=NVDA}.
 *
 * Values may be quoted ('AAPL' or "AAPL"). Spaces around operators are fine,
 * but AND/OR must be surrounded by whitespace.
 * A null/blank string returns {@link Filter#ACCEPT_ALL}.
 * Any malformed input throws IllegalArgumentException with a readable message.
 */
public final class FilterParser {

    private static final Pattern OR_SPLIT  = Pattern.compile("\\s+OR\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern AND_SPLIT = Pattern.compile("\\s+AND\\s+", Pattern.CASE_INSENSITIVE);
    // Longest operators first so ">=" is not read as ">" followed by "=200".
    private static final Pattern CONDITION = Pattern.compile(
            "^\\s*([A-Za-z_]+)\\s*(==|!=|>=|<=|=|>|<)\\s*(.+?)\\s*$");

    private FilterParser() {}

    /**
     * Parses a full filter expression. Returns a FieldFilter, AndFilter or
     * OrFilter (single-element groups are not wrapped), or ACCEPT_ALL if blank.
     */
    public static Filter parse(String text) {
        if (text == null || text.trim().isEmpty()) {
            return Filter.ACCEPT_ALL;
        }
        String trimmed = text.trim();
        rejectDanglingKeyword(trimmed);

        List<Filter> orParts = new ArrayList<>();
        for (String orPart : OR_SPLIT.split(trimmed, -1)) {
            List<Filter> andParts = new ArrayList<>();
            for (String cond : AND_SPLIT.split(orPart, -1)) {
                andParts.add(parseCondition(cond));
            }
            orParts.add(andParts.size() == 1 ? andParts.get(0) : new AndFilter(andParts));
        }
        return orParts.size() == 1 ? orParts.get(0) : new OrFilter(orParts);
    }

    /** Parses one comparison such as "price>=200" or "symbol = 'AAPL'". */
    static FieldFilter parseCondition(String cond) {
        if (cond == null || cond.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty condition (check for doubled AND/OR)");
        }
        if (cond.contains("(") || cond.contains(")")) {
            throw new IllegalArgumentException("Parentheses are not supported: '" + cond.trim() + "'");
        }
        Matcher m = CONDITION.matcher(cond);
        if (!m.matches()) {
            throw new IllegalArgumentException("Can't parse condition '" + cond.trim()
                    + "'. Expected <field><op><value>, e.g. price>200");
        }
        FieldFilter.Field field = FieldFilter.Field.fromName(m.group(1));
        FieldFilter.Op op = FieldFilter.Op.fromSymbol(m.group(2));
        String raw = unquote(m.group(3));
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("Missing value in '" + cond.trim() + "'");
        }
        if (raw.matches(".*[=<>!].*") || raw.contains(" ")) {
            throw new IllegalArgumentException("Unexpected value '" + raw + "' in '" + cond.trim() + "'");
        }

        if (field.numeric) {
            double v;
            try {
                v = Double.parseDouble(raw);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(field.label() + " must be compared to a number, got '" + raw + "'");
            }
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new IllegalArgumentException(field.label() + " must be a finite number, got '" + raw + "'");
            }
            return FieldFilter.numeric(field, op, v);
        }
        return FieldFilter.text(field, op, raw);
    }

    /** SQL WHERE-clause form of any filter, e.g. for the display header or slides. */
    public static String toSql(Filter f) {
        if (f == Filter.ACCEPT_ALL) return "TRUE";
        return render(f, true);
    }

    /** describe() form, or SQL form when {@code sql} is true; used by AndFilter/OrFilter to render children. */
    static String render(Filter f, boolean sql) {
        if (!sql) return f.describe();
        if (f instanceof FieldFilter) return ((FieldFilter) f).toSql();
        if (f instanceof AndFilter)   return ((AndFilter) f).toSql();
        if (f instanceof OrFilter)    return ((OrFilter) f).toSql();
        return f.describe();
    }

    /** The split patterns need whitespace on both sides, so a leading/trailing AND/OR must be caught here. */
    private static void rejectDanglingKeyword(String s) {
        String u = " " + s.toUpperCase(java.util.Locale.ROOT) + " ";
        if (u.matches("^\\s*(AND|OR)\\s.*") || u.matches(".*\\s(AND|OR)\\s*$")) {
            throw new IllegalArgumentException("Filter can't start or end with AND/OR: '" + s + "'");
        }
    }

    /** Strips one pair of matching single or double quotes, if present. */
    private static String unquote(String v) {
        String t = v.trim();
        if (t.length() >= 2 && ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith("\"") && t.endsWith("\"")))) {
            return t.substring(1, t.length() - 1).trim();
        }
        return t;
    }
}
