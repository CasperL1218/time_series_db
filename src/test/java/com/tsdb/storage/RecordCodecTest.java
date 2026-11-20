package com.tsdb.storage;

import com.tsdb.DataPoint;
import com.tsdb.FinancialDataGenerator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Tests for the 32-byte record format: bit-exact round trips (including
 * NaN, -0.0 and extreme longs/ints), detection of every single-byte flip,
 * symbol validation, and that only encode moves the buffer's position.
 *
 * Helpers: same() compares two points field by field (price by raw bits,
 * since DataPoint has no equals); bad() asserts encode rejects a point;
 * check() covers everything else.
 *
 * Run with:  java -cp out com.tsdb.storage.RecordCodecTest
 * Exits with status 0 if all pass, otherwise prints each failure and exits 1.
 */
public class RecordCodecTest {

    private static int failures = 0;
    private static int passed = 0;

    public static void main(String[] args) {
        testGeneratorRoundTrip();
        testEveryByteFlipIsDetected();
        testSymbols();
        testExtremeValues();
        testPositions();
        testBackToBack();
        testByteOrder();
        testAllZeroIsInvalid();

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " RECORD CODEC TESTS PASSED");
        } else {
            System.out.println(failures + " RECORD CODEC TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    private static void testGeneratorRoundTrip() {
        FinancialDataGenerator g = new FinancialDataGenerator(0, 1, 10, new Random(42));
        ByteBuffer buf = le(RecordCodec.RECORD_SIZE);
        int mismatches = 0;
        String first = null;
        for (int i = 0; i < 100_000; i++) {
            DataPoint p = g.next();
            buf.clear();
            RecordCodec.encode(p, buf);
            DataPoint q = RecordCodec.decode(buf, 0);
            if (!same(p, q)) {
                if (first == null) first = "#" + i + ": " + show(p) + " -> " + show(q);
                mismatches++;
            }
        }
        check("100,000 generated trades round-trip field for field" + (first == null ? "" : " (first: " + first + ")"),
                mismatches, 0);
    }

    private static void testEveryByteFlipIsDetected() {
        ByteBuffer buf = le(RecordCodec.RECORD_SIZE);
        RecordCodec.encode(dp(1_700_000_000_123L, "NVDA", 123.45, 77, DataPoint.Side.SELL), buf);
        check("freshly encoded record is valid", RecordCodec.isValid(buf, 0), true);
        int decodeMissed = 0, validMissed = 0;
        for (int i = 0; i < RecordCodec.RECORD_SIZE; i++) {
            buf.put(i, (byte) (buf.get(i) ^ 0xFF));
            try {
                RecordCodec.decode(buf, 0);
                decodeMissed++;
                System.out.println("  FAIL decode accepted a flip of byte " + i);
            } catch (CorruptRecordException expected) {
                // detected
            }
            if (RecordCodec.isValid(buf, 0)) {
                validMissed++;
                System.out.println("  FAIL isValid accepted a flip of byte " + i);
            }
            buf.put(i, (byte) (buf.get(i) ^ 0xFF)); // restore
        }
        check("decode throws CorruptRecordException for a flip of each of the 32 bytes", decodeMissed, 0);
        check("isValid is false for a flip of each of the 32 bytes", validMissed, 0);
        check("record is valid again once restored", RecordCodec.isValid(buf, 0), true);
    }

    private static void testSymbols() {
        bad("empty symbol", "");
        bad("null symbol", null);
        bad("8-char symbol", "ABCDEFGH");
        bad("non-ASCII symbol", "AAPLé");
        bad("symbol with a NUL", "AA\0PL");
        bad("symbol with a space", "BRK B");
        bad("symbol with a DEL (0x7F)", "AB\u007f");
        roundTrip("'BRK.B' round-trips", dp(5, "BRK.B", 1.5, 3, DataPoint.Side.BUY));
        roundTrip("7-char symbol round-trips", dp(5, "ABCDEFG", 1.5, 3, DataPoint.Side.BUY));
        roundTrip("1-char symbol round-trips", dp(5, "X", 1.5, 3, DataPoint.Side.BUY));
        roundTrip("'!' and '~' (0x21, 0x7E) round-trip", dp(5, "!~", 1.5, 3, DataPoint.Side.BUY));

        ByteBuffer buf = le(64);
        buf.position(7);
        try {
            RecordCodec.encode(dp(1, "", 1, 1, DataPoint.Side.BUY), buf);
        } catch (IllegalArgumentException expected) {
            // checked below
        }
        check("a rejected encode leaves the position alone", buf.position(), 7);
        check("a rejected encode writes nothing", allZero(buf, 0, 64), true);

        ByteBuffer nullSide = le(32);
        try {
            RecordCodec.encode(new DataPoint(1, "A", 1, 1, null), nullSide);
            failures++;
            System.out.println("  FAIL expected IllegalArgumentException for a null side");
        } catch (IllegalArgumentException e) {
            passed++;
            System.out.println("  ok   null side rejected: " + e.getMessage());
        }
    }

    private static void testExtremeValues() {
        long[] times = {Long.MAX_VALUE, Long.MIN_VALUE, 0};
        double[] prices = {-0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE, Double.MAX_VALUE, Double.longBitsToDouble(0x7ff8_dead_beef_0001L)};
        int[] qtys = {Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        int combos = 0, bad = 0;
        for (long t : times) {
            for (double pr : prices) {
                for (int q : qtys) {
                    for (DataPoint.Side s : DataPoint.Side.values()) {
                        DataPoint p = dp(t, "EXTRM", pr, q, s);
                        ByteBuffer buf = le(32);
                        RecordCodec.encode(p, buf);
                        if (!same(p, RecordCodec.decode(buf, 0))) {
                            bad++;
                            System.out.println("  FAIL extreme value did not round-trip: " + show(p));
                        }
                        combos++;
                    }
                }
            }
        }
        check(combos + " extreme timestamp/price/quantity/side combinations round-trip bit for bit "
                + "(incl. -0.0, a NaN payload)", bad, 0);
    }

    private static void testPositions() {
        ByteBuffer buf = le(100);
        buf.position(10);
        RecordCodec.encode(dp(42, "AAPL", 9.99, 1, DataPoint.Side.BUY), buf);
        check("encode advances the position by exactly 32", buf.position(), 42);
        RecordCodec.decode(buf, 10);
        check("decode leaves the position unchanged", buf.position(), 42);
        check("timestampAt reads the timestamp", RecordCodec.timestampAt(buf, 10), 42L);
        check("timestampAt leaves the position unchanged", buf.position(), 42);
        RecordCodec.isValid(buf, 10);
        check("isValid leaves the position unchanged", buf.position(), 42);
        check("encode wrote nothing before its offset", allZero(buf, 0, 10), true);
        check("encode wrote nothing after its 32 bytes", allZero(buf, 42, 100), true);

        ByteBuffer small = le(31);
        try {
            RecordCodec.encode(dp(1, "A", 1, 1, DataPoint.Side.BUY), small);
            failures++;
            System.out.println("  FAIL expected BufferOverflowException with 31 bytes remaining");
        } catch (java.nio.BufferOverflowException e) {
            passed++;
            System.out.println("  ok   encode with fewer than 32 bytes remaining overflows");
        }
    }

    private static void testBackToBack() {
        ByteBuffer buf = le(64 * 1024);
        FinancialDataGenerator g = new FinancialDataGenerator(1_000, 1, 10, new Random(7));
        List<DataPoint> in = new ArrayList<>();
        while (buf.remaining() >= RecordCodec.RECORD_SIZE) {
            DataPoint p = g.next();
            in.add(p);
            RecordCodec.encode(p, buf);
        }
        check("2,048 records fill a 64 KB buffer exactly", in.size() == 2048 && buf.remaining() == 0, true);
        int bad = 0;
        for (int i = 0; i < in.size(); i++) {
            int off = i * RecordCodec.RECORD_SIZE;
            if (!same(in.get(i), RecordCodec.decode(buf, off))
                    || RecordCodec.timestampAt(buf, off) != in.get(i).getTimestampMillis()) bad++;
        }
        check("records encoded back to back decode at their offsets", bad, 0);

        ByteBuffer direct = ByteBuffer.allocateDirect(64).order(ByteOrder.LITTLE_ENDIAN);
        DataPoint p = dp(99, "MSFT", 410.25, 12, DataPoint.Side.SELL);
        direct.position(32);
        RecordCodec.encode(p, direct);
        check("direct buffer round-trips at offset 32", same(p, RecordCodec.decode(direct, 32)), true);
        check("direct and heap encodings are byte-identical", bytesOf(direct, 32), bytesOf(encoded(p), 0));
    }

    private static void testByteOrder() {
        ByteBuffer big = ByteBuffer.allocate(32); // BIG_ENDIAN by default
        try {
            RecordCodec.encode(dp(1, "A", 1, 1, DataPoint.Side.BUY), big);
            failures++;
            System.out.println("  FAIL expected IllegalArgumentException for a BIG_ENDIAN buffer");
        } catch (IllegalArgumentException e) {
            passed++;
            System.out.println("  ok   encode rejects a BIG_ENDIAN buffer");
        }
        check("BIG_ENDIAN encode left the position alone", big.position(), 0);
    }

    private static void testAllZeroIsInvalid() {
        ByteBuffer zero = le(32);
        java.util.zip.CRC32C c = new java.util.zip.CRC32C();
        c.update(new byte[28]);
        System.out.printf("       (CRC32C of 28 zero bytes = 0x%08x)%n", c.getValue());
        check("CRC32C of 28 zero bytes is not 0", c.getValue() != 0, true);
        check("an all-zero 32-byte region is invalid", RecordCodec.isValid(zero, 0), false);
        boolean threw = false;
        try {
            RecordCodec.decode(zero, 0);
        } catch (CorruptRecordException e) {
            threw = true;
        }
        check("decode of an all-zero region throws CorruptRecordException", threw, true);
    }

    // --- helpers ---
    private static DataPoint dp(long ts, String sym, double price, int qty, DataPoint.Side side) {
        return new DataPoint(ts, sym, price, qty, side);
    }

    private static ByteBuffer le(int n) {
        return ByteBuffer.allocate(n).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static ByteBuffer encoded(DataPoint p) {
        ByteBuffer b = le(32);
        RecordCodec.encode(p, b);
        return b;
    }

    private static List<Byte> bytesOf(ByteBuffer b, int off) {
        List<Byte> out = new ArrayList<>();
        for (int i = 0; i < RecordCodec.RECORD_SIZE; i++) out.add(b.get(off + i));
        return out;
    }

    private static boolean allZero(ByteBuffer b, int from, int to) {
        for (int i = from; i < to; i++) if (b.get(i) != 0) return false;
        return true;
    }

    static boolean same(DataPoint a, DataPoint b) {
        return a.getTimestampMillis() == b.getTimestampMillis()
                && Objects.equals(a.getSymbol(), b.getSymbol())
                && Double.doubleToRawLongBits(a.getPrice()) == Double.doubleToRawLongBits(b.getPrice())
                && a.getQuantity() == b.getQuantity()
                && a.getSide() == b.getSide();
    }

    private static String show(DataPoint p) {
        return p.getTimestampMillis() + "/" + p.getSymbol() + "/"
                + Long.toHexString(Double.doubleToRawLongBits(p.getPrice())) + "/" + p.getQuantity() + "/" + p.getSide();
    }

    private static void roundTrip(String name, DataPoint p) {
        check(name, same(p, RecordCodec.decode(encoded(p), 0)), true);
    }

    private static void bad(String name, String symbol) {
        try {
            RecordCodec.encode(dp(1, symbol, 1.0, 1, DataPoint.Side.BUY), le(32));
            failures++;
            System.out.println("  FAIL expected IllegalArgumentException for " + name);
        } catch (IllegalArgumentException e) {
            passed++;
            System.out.println("  ok   " + name + " rejected: " + e.getMessage());
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
}
