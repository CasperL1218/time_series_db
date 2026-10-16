package com.tsdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Passes a point only if all subfilters filter pass. */
public final class AndFilter implements Filter {

    private final List<Filter> children;

    public AndFilter(List<Filter> children) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("AndFilter needs at least one condition");
        }
        this.children = Collections.unmodifiableList(new ArrayList<>(children));
    }

    @Override
    public boolean test(DataPoint p) {
        for (Filter f : children) {
            if (!f.test(p)) return false;
        }
        return true;
    }

    @Override
    public String describe() {
        return join(" AND ", false);
    }

    /** SQL-style form, like "symbol = 'AAPL' AND price > 200". */
    public String toSql() {
        return join(" AND ", true);
    }

    private String join(String sep, boolean sql) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) sb.append(sep);
            sb.append(FilterParser.render(children.get(i), sql));
        }
        return sb.toString();
    }

    public List<Filter> getChildren() { return children; }
}
