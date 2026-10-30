package com.tsdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Passes a point if any child filter passes (short-circuits on the first match). */
public final class OrFilter implements Filter {

    private final List<Filter> children;

    public OrFilter(List<Filter> children) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("OrFilter needs at least one condition");
        }
        this.children = Collections.unmodifiableList(new ArrayList<>(children));
    }

    @Override
    public boolean test(DataPoint p) {
        for (Filter f : children) {
            if (f.test(p)) return true;
        }
        return false;
    }

    /** AND groups are wrapped in () for clarity, even though AND already binds tighter. */
    @Override
    public String describe() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) sb.append(" OR ");
            Filter c = children.get(i);
            sb.append(c instanceof AndFilter ? "(" + c.describe() + ")" : c.describe());
        }
        return sb.toString();
    }
}
