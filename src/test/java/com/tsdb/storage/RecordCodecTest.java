package com.tsdb.storage;

import com.tsdb.DataPoint;
import com.tsdb.DataPoint.Side;
import com.tsdb.FinancialDataGenerator;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.zip.CRC32C;

/**
 * Tests for the 32-byte record format.
 *
 * The core property: decode(encode(p)) gives back exactly p, field for
 * field and bit for bit, and any damage to a stored record is detected.
 *
 * Run with:  java -cp out com.tsdb.storage.RecordCodecTest
 */
public class RecordCodecTest {

    private static int failures = 0;
    private static int passed = 0;

    public static void main(String[] args) {
        testExactByteLayout();
        testRoundTripSeededStream();
        testExtremeValues();
        testEveryByteChangeIsDetected();
        testTornAndZeroedRecords();
        testBadInput();
        testEncodeTouchesOnlyItsBytes();
        testUnalignedOffsetAndPosition();
        reportThroughput();

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " RECORD CODEC TESTS PASSED");
        } else {
            System.out.println(failures + " RECORD CODEC TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    /** Pins down the format byte by byte, so an accidental layout change fails loudly. */
    private static void testExactByteLayout() {
        long ts = 0x0102030405060708L;
        ByteBuffer buf = RecordCodec.allocate(1);
        RecordCodec.encode(new DataPoint(ts, "AAPL", 187.42, 250, Side.BUY), buf);

        check("layout: position advanced 32 bytes", buf.position(), 32);
        check("layout: timestamp is little-endian (lowest byte first)", bytes(buf, 0, 8),
                "08 07 06 05 04 03 02 01");
        check("layout: price is the raw IEEE-754 bits", buf.getLong(8), Double.doubleToRawLongBits(187.42));
        check("layout: quantity 250 is FA 00 00 00", bytes(buf, 16, 4), "fa 00 00 00");
        check("layout: BUY is 00", bytes(buf, 20, 1), "00");
        check("layout: symbol AAPL is ASCII, zero-padded", bytes(buf, 21, 7), "41 41 50 4c 00 00 00");

        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 0, 28);
        check("layout: last 4 bytes are CRC32C of bytes 0-27", buf.getInt(28), (int) crc.getValue());

        ByteBuffer sell = RecordCodec.allocate(1);
        RecordCodec.encode(new DataPoint(ts, "AAPL", 187.42, 250, Side.SELL), sell);
        check("layout: SELL is 01", bytes(sell, 20, 1), "01");
    }

    /** 100,000 generator trades written back to back, then read back and compared. */
    private static void testRoundTripSeededStream() {
        int n = 100_000;
        FinancialDataGenerator gen = new FinancialDataGenerator(0, 1, 10, new Random(42));
        List<DataPoint> trades = new ArrayList<>(n);
        ByteBuffer buf = RecordCodec.allocate(n);
        for (int i = 0; i < n; i++) {
            DataPoint p = gen.next();
            trades.add(p);
            RecordCodec.encode(p, buf);
        }
        check("round trip: buffer holds exactly 32 x " + n + " bytes", buf.position(), 32 * n);

        int mismatches = 0;
        int badTimestamps = 0;
        for (int i = 0; i < n; i++) {
            int off = i * RecordCodec.RECORD_SIZE;
            if (!sameTrade(RecordCodec.decode(buf, off), trades.get(i))) mismatches++;
            if (RecordCodec.timestampAt(buf, off) != trades.get(i).getTimestampMillis()) badTimestamps++;
        }
        check("round trip: all " + n + " seeded trades decode identically", mismatches, 0);
        check("round trip: timestampAt matches for all " + n, badTimestamps, 0);
    }

    private static void testExtremeValues() {
        List<DataPoint> cases = List.of(
                new DataPoint(Long.MIN_VALUE, "A", 0.0, Integer.MIN_VALUE, Side.BUY),
                new DataPoint(Long.MAX_VALUE, "ABCDEFG", -0.0, Integer.MAX_VALUE, Side.SELL),
                new DataPoint(0, "BRK.B", Double.MIN_VALUE, 0, Side.BUY),
                new DataPoint(-1, "brk-b", Double.MAX_VALUE, -1, Side.SELL),
                new DataPoint(1, "~!", Double.NaN, 1, Side.BUY),
                new DataPoint(2, "Z", Double.POSITIVE_INFINITY, 2, Side.SELL),
                new DataPoint(3, "Z", Double.NEGATIVE_INFINITY, 3, Side.BUY),
                new DataPoint(4, "Z", 0.1 + 0.2, 4, Side.SELL)); // 0.30000000000000004 must survive
        for (DataPoint p : cases) {
            ByteBuffer buf = RecordCodec.allocate(1);
            RecordCodec.encode(p, buf);
            check("extreme: " + p.getTimestampMillis() + " " + p.getSymbol() + " $" + p.getPrice()
                    + " qty=" + p.getQuantity(), sameTrade(RecordCodec.decode(buf, 0), p), true);
        }
    }

    /**
     * Replaces each of the 32 bytes with each of its 255 other values: 8,160
     * damaged records. CRC32C is guaranteed to catch every one, since any
     * single-byte change is an error within 32 consecutive bits.
     */
    private static void testEveryByteChangeIsDetected() {
        ByteBuffer good = RecordCodec.allocate(1);
        RecordCodec.encode(new DataPoint(1_730_870_073_909L, "NVDA", 431.17, 120, Side.SELL), good);
        check("damage: the original record is valid", RecordCodec.isValid(good, 0), true);

        int cases = 0;
        int missedByIsValid = 0;
        int missedByDecode = 0;
        for (int pos = 0; pos < RecordCodec.RECORD_SIZE; pos++) {
            for (int v = 0; v < 256; v++) {
                if ((byte) v == good.get(pos)) continue;
                ByteBuffer bad = copy(good);
                bad.put(pos, (byte) v);
                cases++;
                if (RecordCodec.isValid(bad, 0)) missedByIsValid++;
                try {
                    RecordCodec.decode(bad, 0);
                    missedByDecode++;
                } catch (CorruptRecordException expected) {
                    // detected
                }
            }
        }
        check("damage: tried every single-byte change", cases, 32 * 255);
        check("damage: isValid rejects all " + cases, missedByIsValid, 0);
        check("damage: decode throws for all " + cases, missedByDecode, 0);
    }

    /** What a crash can leave at the end of a file. */
    private static void testTornAndZeroedRecords() {
        ByteBuffer zeros = RecordCodec.allocate(1);
        check("torn: an all-zero record is invalid", RecordCodec.isValid(zeros, 0), false);

        ByteBuffer half = RecordCodec.allocate(1);
        RecordCodec.encode(new DataPoint(5, "TSLA", 250.0, 10, Side.BUY), half);
        for (int i = 16; i < 32; i++) half.put(i, (byte) 0);
        check("torn: a half-written record is invalid", RecordCodec.isValid(half, 0), false);

        ByteBuffer shortBuf = RecordCodec.allocate(1).limit(31);
        expectThrows("torn: decoding past the buffer's end", IndexOutOfBoundsException.class,
                () -> RecordCodec.decode(shortBuf, 0));
        expectThrows("torn: negative offset", IndexOutOfBoundsException.class,
                () -> RecordCodec.decode(RecordCodec.allocate(1), -1));
    }

    private static void testBadInput() {
        expectBadTrade("8-character symbol", new DataPoint(1, "ABCDEFGH", 1, 1, Side.BUY));
        expectBadTrade("empty symbol", new DataPoint(1, "", 1, 1, Side.BUY));
        expectBadTrade("null symbol", new DataPoint(1, null, 1, 1, Side.BUY));
        expectBadTrade("non-ASCII symbol", new DataPoint(1, "ÄAPL", 1, 1, Side.BUY));
        expectBadTrade("symbol with a space", new DataPoint(1, "BRK B", 1, 1, Side.BUY));
        expectBadTrade("symbol with a NUL byte", new DataPoint(1, "AB\0C", 1, 1, Side.BUY));
        expectBadTrade("null side", new DataPoint(1, "AAPL", 1, 1, null));
        expectThrows("rejects a null trade", IllegalArgumentException.class,
                () -> RecordCodec.encode(null, RecordCodec.allocate(1)));

        DataPoint ok = new DataPoint(1, "AAPL", 1, 1, Side.BUY);
        ByteBuffer bigEndian = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
        expectThrows("encode rejects a big-endian buffer", IllegalArgumentException.class,
                () -> RecordCodec.encode(ok, bigEndian));
        expectThrows("decode rejects a big-endian buffer", IllegalArgumentException.class,
                () -> RecordCodec.decode(bigEndian, 0));

        ByteBuffer tooSmall = RecordCodec.allocate(1);
        tooSmall.position(1);
        expectThrows("encode with only 31 bytes left", BufferOverflowException.class,
                () -> RecordCodec.encode(ok, tooSmall));
        check("too small: position unchanged after the failed encode", tooSmall.position(), 1);
        check("too small: no bytes written", bytes(tooSmall, 0, 32).replace("00", "").trim().isEmpty(), true);
    }

    private static void testEncodeTouchesOnlyItsBytes() {
        ByteBuffer buf = RecordCodec.allocate(3);
        for (int i = 0; i < buf.capacity(); i++) buf.put(i, (byte) 0x55);
        buf.position(32);
        RecordCodec.encode(new DataPoint(9, "MSFT", 410.5, 7, Side.SELL), buf);
        boolean before = true, after = true;
        for (int i = 0; i < 32; i++) before &= buf.get(i) == 0x55;
        for (int i = 64; i < 96; i++) after &= buf.get(i) == 0x55;
        check("isolation: bytes before the record untouched", before, true);
        check("isolation: bytes after the record untouched", after, true);
        check("isolation: position moved to the next record", buf.position(), 64);
    }

    /** Records don't have to sit on a 32-byte boundary, and decode never moves the position. */
    private static void testUnalignedOffsetAndPosition() {
        DataPoint p = new DataPoint(123, "GOOG", 175.25, 33, Side.BUY);
        ByteBuffer buf = ByteBuffer.allocate(100).order(RecordCodec.BYTE_ORDER);
        buf.position(7);
        RecordCodec.encode(p, buf);
        check("unaligned: decodes at offset 7", sameTrade(RecordCodec.decode(buf, 7), p), true);
        check("unaligned: decode leaves the position alone", buf.position(), 39);
    }

    /** Not a pass/fail check: a rough number for the team's benchmark notes. */
    private static void reportThroughput() {
        int n = 1_000_000;
        FinancialDataGenerator gen = new FinancialDataGenerator(0, 1, 10, new Random(7));
        DataPoint[] trades = new DataPoint[n];
        for (int i = 0; i < n; i++) trades[i] = gen.next();
        ByteBuffer buf = RecordCodec.allocate(n);

        long t0 = System.nanoTime();
        for (DataPoint p : trades) RecordCodec.encode(p, buf);
        long t1 = System.nanoTime();
        long checksum = 0;
        for (int i = 0; i < n; i++) checksum += RecordCodec.decode(buf, i * RecordCodec.RECORD_SIZE).getQuantity();
        long t2 = System.nanoTime();

        System.out.printf("  info encode %.1fM trades/s, decode %.1fM trades/s (1M trades, %d MB, no JIT warm-up; check %d)%n",
                n / ((t1 - t0) / 1e3), n / ((t2 - t1) / 1e3), buf.capacity() >> 20, checksum);
    }

    // ---- helpers ----

    /** Field-by-field equality; price compared by raw bits so NaN and -0.0 count as exact matches. */
    private static boolean sameTrade(DataPoint a, DataPoint b) {
        return a.getTimestampMillis() == b.getTimestampMillis()
                && Objects.equals(a.getSymbol(), b.getSymbol())
                && Double.doubleToRawLongBits(a.getPrice()) == Double.doubleToRawLongBits(b.getPrice())
                && a.getQuantity() == b.getQuantity()
                && a.getSide() == b.getSide();
    }

    private static ByteBuffer copy(ByteBuffer src) {
        ByteBuffer dst = ByteBuffer.allocate(src.capacity()).order(src.order());
        dst.put(0, src, 0, src.capacity());
        return dst;
    }

    /** Hex dump of len bytes starting at off, e.g. "fa 00 00 00". */
    private static String bytes(ByteBuffer buf, int off, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0) sb.append(' ');
            sb.append(String.format("%02x", buf.get(off + i) & 0xFF));
        }
        return sb.toString();
    }

    private static void expectBadTrade(String why, DataPoint p) {
        expectThrows("rejects " + why, IllegalArgumentException.class,
                () -> RecordCodec.encode(p, RecordCodec.allocate(1)));
    }

    private static void expectThrows(String name, Class<? extends Throwable> type, Runnable action) {
        try {
            action.run();
            failures++;
            System.out.println("  FAIL " + name + " -- expected " + type.getSimpleName() + ", nothing thrown");
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                passed++;
                System.out.println("  ok   " + name + (t.getMessage() == null ? "" : ": " + t.getMessage()));
            } else {
                failures++;
                System.out.println("  FAIL " + name + " -- expected " + type.getSimpleName() + ", got " + t);
            }
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
