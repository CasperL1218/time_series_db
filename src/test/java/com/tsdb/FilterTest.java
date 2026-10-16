package com.tsdb;

import java.util.List;
import java.util.Objects;

/**
 * Tests for the filtering layer
 * use expect function for the many filter-on-trade tests
 * use check function for other tests (e.g. parser, describe, sql)
 */
public class FilterTest {

    private static int failures = 0;
    private static int passed = 0;

    // Fixtures
    private static final DataPoint AAPL_BUY_250_100  = dp("AAPL", DataPoint.Side.BUY, 250.00, 100);
    private static final DataPoint AAPL_SELL_150_400 = dp("AAPL", DataPoint.Side.SELL, 150.00, 400);
    private static final DataPoint NVDA_BUY_200_50   = dp("NVDA", DataPoint.Side.BUY, 200.00, 50);
    private static final DataPoint MSFT_SELL_320_500 = dp("MSFT", DataPoint.Side.SELL, 320.50, 500);

    public static void main(String[] args) {
        testTextOperators();
        testNumericOperators();
        testBoundaries();
        testParserFormatting();
        testAnd();
        testOr();
        testPrecedence();
        testBlankIsAcceptAll();
        testDescribeAndSql();
        testEngineSkipsFilteredPoints();
        testBadInput();

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " FILTER TESTS PASSED");
        } else {
            System.out.println(failures + " FILTER TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    private static void testTextOperators() {
        expect("symbol=AAPL",  AAPL_BUY_250_100, true);
        expect("symbol=AAPL",  NVDA_BUY_200_50, false);
        expect("symbol!=AAPL", NVDA_BUY_200_50, true);
        expect("symbol!=AAPL", AAPL_BUY_250_100, false);
        expect("side=BUY",     AAPL_BUY_250_100, true);
        expect("side=BUY",     AAPL_SELL_150_400, false);
        expect("side!=BUY",    AAPL_SELL_150_400, true);
        expect("symbol=aapl",  AAPL_BUY_250_100, true);   // case-insensitive values
        expect("side=sell",    MSFT_SELL_320_500, true);
    }

    private static void testNumericOperators() {
        expect("price=250",    AAPL_BUY_250_100, true);
        expect("price!=250",   AAPL_BUY_250_100, false);
        expect("price>200",    AAPL_BUY_250_100, true);
        expect("price>200",    AAPL_SELL_150_400, false);
        expect("price<200",    AAPL_SELL_150_400, true);
        expect("price>=320.5", MSFT_SELL_320_500, true);
        expect("price<=150",   AAPL_SELL_150_400, true);
        expect("quantity>100", AAPL_SELL_150_400, true);
        expect("quantity<100", NVDA_BUY_200_50, true);
        expect("quantity=50",  NVDA_BUY_200_50, true);
        expect("qty<=100",     AAPL_BUY_250_100, true);   
    }

    private static void testBoundaries() {
        expect("price>200",  NVDA_BUY_200_50, false);  // strictly greater (no equal)
        expect("price>=200", NVDA_BUY_200_50, true);
        expect("price<200",  NVDA_BUY_200_50, false);
        expect("price<=200", NVDA_BUY_200_50, true);
    }

    private static void testParserFormatting() {
        expect("  price  >  200  ",     AAPL_BUY_250_100, true);   // spaces
        expect("symbol = 'AAPL'",       AAPL_BUY_250_100, true);   // single quotes
        expect("symbol=\"AAPL\"",       AAPL_BUY_250_100, true);   // double quotes
        expect("price==250",            AAPL_BUY_250_100, true);   // ==
        expect("PRICE>200 and Side=buy", AAPL_BUY_250_100, true);  // case-insensitive keywords/fields
    }

    private static void testAnd() {
        expect("symbol=AAPL AND price>200", AAPL_BUY_250_100, true);
        expect("symbol=AAPL AND price>200", AAPL_SELL_150_400, false);
        expect("symbol=AAPL AND price>200", MSFT_SELL_320_500, false);
        expect("symbol=AAPL AND side=SELL AND quantity>=400", AAPL_SELL_150_400, true);
    }

    private static void testOr() {
        expect("symbol=AAPL OR symbol=NVDA", NVDA_BUY_200_50, true);
        expect("symbol=AAPL OR symbol=NVDA", AAPL_SELL_150_400, true);
        expect("symbol=AAPL OR symbol=NVDA", MSFT_SELL_320_500, false);
    }

    private static void testPrecedence() {
        // and/or tests
        String q = "symbol=AAPL AND price>200 OR symbol=NVDA";
        expect(q, AAPL_BUY_250_100, true);    // left group
        expect(q, NVDA_BUY_200_50, true);     // right group
        expect(q, AAPL_SELL_150_400, false);  // AAPL but price too low, not NVDA
        check("precedence: top level is OR", FilterParser.parse(q) instanceof OrFilter, true);
    }

    private static void testBlankIsAcceptAll() {
        check("null -> ACCEPT_ALL",  FilterParser.parse(null) == Filter.ACCEPT_ALL, true);
        check("blank -> ACCEPT_ALL", FilterParser.parse("   ") == Filter.ACCEPT_ALL, true);
        check("ACCEPT_ALL passes everything", Filter.ACCEPT_ALL.test(MSFT_SELL_320_500), true);
    }

    private static void testDescribeAndSql() {
        Filter f = FilterParser.parse("symbol=AAPL AND price>200 OR side!=buy");
        check("describe() shows AND grouping", f.describe(), "(symbol=AAPL AND price>200) OR side!=BUY");
        check("toSql()", FilterParser.toSql(f), "(symbol = 'AAPL' AND price > 200) OR side <> 'BUY'");
        check("toSql(ACCEPT_ALL)", FilterParser.toSql(Filter.ACCEPT_ALL), "TRUE");
    }

    private static void testEngineSkipsFilteredPoints() {
        CountWindow w = new CountWindow(2);
        StreamEngine e = new StreamEngine(w, FilterParser.parse("symbol=AAPL"), List.of());
        e.processUpdate(AAPL_BUY_250_100);
        e.processUpdate(NVDA_BUY_200_50);
        e.processUpdate(MSFT_SELL_320_500);
        e.processUpdate(AAPL_SELL_150_400);
        // Without the filter, NVDA/MSFT would have pushed the first AAPL out of a size-2 window.
        check("engine: window holds only AAPL and nothing was wrongly evicted", w.size(), 2);
        check("engine: filtered count", e.getPointsFiltered(), 2L);
        check("engine: processed count", e.getPointsProcessed(), 4L);
    }

    private static void testBadInput() {
        bad("price>abc");             // non-numeric value
        bad("price>");                // missing value
        bad(">200");                  // missing field
        bad("volume>10");             // unknown field
        bad("price=>200");            // unknown operator
        bad("price<>200");            // unsupported operator
        bad("symbol>AAPL");           // ordering op on text field
        bad("side=HOLD");             // invalid side
        bad("price>200 AND");         // dangling AND
        bad("OR symbol=AAPL");        // leading OR
        bad("price>200 AND AND side=BUY"); // doubled keyword
        bad("(symbol=AAPL)");         // parentheses unsupported
        bad("symbol=AAPL price>200"); // missing AND/OR between conditions
        bad("price>NaN");             // not a finite number
        bad("symbol");                // no operator at all
    }

    // --- helpers ---

    private static void expect(String filter, DataPoint p, boolean expected) {
        boolean actual;
        try {
            actual = FilterParser.parse(filter).test(p);
        } catch (RuntimeException e) {
            failures++;
            System.out.println("  FAIL [" + filter + "] threw " + e);
            return;
        }
        check("[" + filter + "] on " + p.getSymbol() + " " + p.getSide() + " $" + p.getPrice()
                + " qty=" + p.getQuantity(), actual, expected);
    }

    private static void bad(String filter) {
        try {
            FilterParser.parse(filter);
            failures++;
            System.out.println("  FAIL expected error for [" + filter + "]");
        } catch (IllegalArgumentException e) {
            passed++;
            System.out.println("  ok   rejects [" + filter + "]: " + e.getMessage());
        }
    }

    private static void check(String name, Object actual, Object expected) {
        if (Objects.equals(actual, expected)) {
            passed++;
            System.out.println("  ok   " + name);
        } else {
            failures++;
            System.out.println("  FAIL " + name + " -- expected " + expected + ", got " + actual);
        }
    }

    private static DataPoint dp(String sym, DataPoint.Side side, double price, int qty) {
        return new DataPoint(0, sym, price, qty, side);
    }
}
