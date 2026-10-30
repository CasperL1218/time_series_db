package com.tsdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Passes a point only if every child filter passes (short-circuits on the first failure). */
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

    /** OR groups are wrapped in (), since AND binds tighter; the parser never builds these, but Shell's ALSO FILTER does. */
    @Override
    public String describe() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) sb.append(" AND ");
            Filter c = children.get(i);
            sb.append(c instanceof OrFilter ? "(" + c.describe() + ")" : c.describe());
        }
        return sb.toString();
    }

    public List<Filter> getChildren() { return children; }
}
