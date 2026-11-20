package com.tsdb.storage;

import com.tsdb.DataPoint;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Tests for one segment file: the header (and each way it can be wrong),
 * appends and reads, lowerBound against brute force, and last-segment
 * recovery (torn tail moved aside byte for byte, file truncated).
 *
 * Helpers: write() makes a closed segment holding given points; mutate()
 * edits a file's bytes; rejected() asserts open throws CorruptStoreException;
 * check() covers everything else. Each test works in its own temp directory,
 * deleted afterwards.
 *
 * Run with:  java -cp out com.tsdb.storage.SegmentTest
 * Exits with status 0 if all pass, otherwise prints each failure and exits 1.
 */
public class SegmentTest {

    private static int failures = 0;
    private static int passed = 0;
    private static final int R = RecordCodec.RECORD_SIZE;

    public static void main(String[] args) throws IOException {
        testCreateWritesHeader();
        testBadHeaders();
        testAppendAndRead();
        testLowerBound();
        testRecoverTruncatedMidRecord();
        testRecoverFlippedByte();
        testRecoverTrailingGarbage();
        testRecoverDecreasingTimestamp();
        testRecoverBelowFloor();
        testRecoverBeyondCapacity();
        testNonLastMustBeExactlyFull();

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " SEGMENT TESTS PASSED");
        } else {
            System.out.println(failures + " SEGMENT TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    private static void testCreateWritesHeader() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            Segment s = Segment.create(dir, 1000, 64);
            s.close();
            Path f = dir.resolve("seg-00000000000000001000.tsd");
            check("create names the file seg-<20-digit baseSeq>.tsd", Files.exists(f), true);
            check("a new segment is exactly the 32-byte header", Files.size(f), 32L);
            ByteBuffer h = ByteBuffer.wrap(Files.readAllBytes(f)).order(ByteOrder.LITTLE_ENDIAN);
            check("header magic is TSDB", new String(h.array(), 0, 4, java.nio.charset.StandardCharsets.US_ASCII), "TSDB");
            check("header version is 1", h.getShort(4), (short) 1);
            check("header recordSize is 32", h.getShort(6), (short) 32);
            check("header baseSeq", h.getLong(8), 1000L);
            check("header capacity", h.getInt(16), 64);
            check("header reserved bytes are zero", h.getLong(20), 0L);
            java.util.zip.CRC32C c = new java.util.zip.CRC32C();
            c.update(h.array(), 0, 28);
            check("header CRC32C covers bytes 0..27", Integer.toUnsignedLong(h.getInt(28)), c.getValue());

            Segment o = Segment.open(f, 1000, true);
            check("open accepts a fresh segment, with 0 records", o.recordCount(), 0L);
            check("fresh segment has nothing torn", o.recoveredTornBytes(), 0L);
            check("open reads capacity from the header", o.capacity(), 64);
            o.close();

            boolean threw = false;
            try {
                Segment.create(dir, 1000, 64);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                threw = true;
            }
            check("create refuses to overwrite an existing segment", threw, true);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testBadHeaders() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            rejected("bad magic", dir, h -> h.put(0, (byte) 'X'), false, 0, true);
            rejected("bad version", dir, h -> h.putShort(4, (short) 2), true, 0, true);
            rejected("bad recordSize", dir, h -> h.putShort(6, (short) 64), true, 0, true);
            rejected("bad header CRC (reserved byte changed)", dir, h -> h.put(23, (byte) 1), false, 0, true);
            rejected("header CRC itself damaged", dir, h -> h.put(30, (byte) (h.get(30) ^ 0x10)), false, 0, true);
            rejected("zero capacity", dir, h -> h.putInt(16, 0), true, 0, true);
            rejected("header baseSeq differs from the file name", dir, h -> h.putLong(8, 5), true, 5, true);
            rejected("baseSeq differs from the expected value", dir, h -> {}, false, 7, true);
            rejected("non-last: baseSeq differs from the expected value", dir, h -> {}, false, 7, false);

            Path shortFile = dir.resolve(Segment.fileName(0));
            Files.write(shortFile, new byte[31]);
            expectCorrupt("a file shorter than the header", shortFile, 0, true, Long.MIN_VALUE);
            Files.delete(shortFile);
            Path badName = dir.resolve("seg-12.tsd");
            Files.write(badName, new byte[32]);
            expectCorrupt("a file whose name isn't seg-<20 digits>.tsd", badName, 12, true, Long.MIN_VALUE);
            Files.delete(badName);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testAppendAndRead() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            List<DataPoint> pts = points(300, new Random(1), 3);
            Segment s = Segment.create(dir, 0, 1000);
            int[] batches = {1, 2, 47, 100, 150};
            int done = 0;
            boolean lengthOk = true;
            for (int b : batches) {
                s.append(encode(pts.subList(done, done + b)), b);
                done += b;
                if (Files.size(s.path()) != 32 + 32L * done) lengthOk = false;
            }
            check("after each batch the file is 32 + 32*N bytes", lengthOk, true);
            check("recordCount after appends", s.recordCount(), 300L);
            check("firstTimestamp", s.firstTimestamp(), pts.get(0).getTimestampMillis());
            check("lastTimestamp", s.lastTimestamp(), pts.get(299).getTimestampMillis());
            int bad = 0;
            ByteBuffer one = le(R);
            for (int i = 0; i < 300; i++) {
                one.clear();
                s.read(i, one);
                if (!RecordCodecTest.same(pts.get(i), RecordCodec.decode(one, 0))) bad++;
            }
            check("read(i) returns record i for every i", bad, 0);
            ByteBuffer many = le(10 * R);
            s.read(290, many);
            check("read fills a multi-record buffer", RecordCodecTest.same(pts.get(299), RecordCodec.decode(many, 9 * R)), true);

            boolean threw = false;
            try {
                s.append(encode(points(701, new Random(2), 3)), 701);
            } catch (IllegalStateException e) {
                threw = true;
            }
            check("append beyond capacity throws and writes nothing", threw && Files.size(s.path()) == 32 + 32L * 300, true);
            s.close();

            Segment o = Segment.open(dir.resolve(Segment.fileName(0)), 0, true);
            check("reopened (last) segment has the same count", o.recordCount(), 300L);
            check("reopened first/last timestamps",
                    List.of(o.firstTimestamp(), o.lastTimestamp()),
                    List.of(pts.get(0).getTimestampMillis(), pts.get(299).getTimestampMillis()));
            o.close();
        } finally {
            deleteTree(dir);
        }
    }

    private static void testLowerBound() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            Random rnd = new Random(99);
            List<DataPoint> pts = points(1000, rnd, 3); // steps of 0..2 ms: many duplicates
            Segment s = Segment.create(dir, 0, 1000);
            s.append(encode(pts), 1000);
            long first = pts.get(0).getTimestampMillis(), last = pts.get(999).getTimestampMillis();
            List<Long> probes = new ArrayList<>(List.of(Long.MIN_VALUE, first - 1, first, first + 1, last - 1, last,
                    last + 1, Long.MAX_VALUE));
            for (int i = 0; i < 2000; i++) probes.add(first - 5 + (long) rnd.nextInt((int) (last - first) + 10));
            long[] limits = {1000, 999, 500, 1, 0};
            int bad = 0, checked = 0;
            for (long limit : limits) {
                for (long ts : probes) {
                    long brute = 0;
                    while (brute < limit && pts.get((int) brute).getTimestampMillis() < ts) brute++;
                    long got = s.lowerBound(ts, limit);
                    checked++;
                    if (got != brute) {
                        bad++;
                        if (bad <= 5) System.out.println("  FAIL lowerBound(" + ts + ", " + limit + ") = " + got + ", brute " + brute);
                    }
                }
            }
            long dups = 0;
            for (int i = 1; i < 1000; i++) if (pts.get(i).getTimestampMillis() == pts.get(i - 1).getTimestampMillis()) dups++;
            check("fixture has duplicate timestamps (" + dups + ")", dups > 100, true);
            check("lowerBound equals brute force for " + checked + " (ts, limit) pairs", bad, 0);
            s.close();
        } finally {
            deleteTree(dir);
        }
    }

    private static void testRecoverTruncatedMidRecord() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            Path f = write(dir, points(50, new Random(3), 5), 100);
            byte[] before = Files.readAllBytes(f);
            long cut = 32 + 30L * R + 17;
            try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "rw")) {
                raf.setLength(cut);
            }
            expectRecovered("truncated mid-record", dir, f, before, cut, 30);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testRecoverFlippedByte() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            Path f = write(dir, points(50, new Random(4), 5), 100);
            mutate(f, b -> b.put(32 + 20 * R + 12, (byte) (b.get(32 + 20 * R + 12) ^ 0x01)));
            expectRecovered("a flipped byte in record 20", dir, f, Files.readAllBytes(f), Files.size(f), 20);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testRecoverTrailingGarbage() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            Path f = write(dir, points(50, new Random(5), 5), 100);
            byte[] garbage = new byte[100];
            new Random(6).nextBytes(garbage);
            Files.write(f, garbage, java.nio.file.StandardOpenOption.APPEND);
            expectRecovered("100 bytes of trailing garbage", dir, f, Files.readAllBytes(f), Files.size(f), 50);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testRecoverDecreasingTimestamp() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            List<DataPoint> pts = points(50, new Random(7), 5);
            DataPoint p35 = pts.get(35);
            pts.set(35, new DataPoint(pts.get(34).getTimestampMillis() - 1, p35.getSymbol(), p35.getPrice(),
                    p35.getQuantity(), p35.getSide())); // valid CRC, but goes back in time
            Path f = write(dir, pts, 100);
            expectRecovered("a timestamp that decreases at record 35", dir, f, Files.readAllBytes(f), Files.size(f), 35);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testRecoverBelowFloor() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            List<DataPoint> pts = points(10, new Random(8), 5);
            Path f = write(dir, pts, 100);
            byte[] before = Files.readAllBytes(f);
            Segment s = Segment.open(f, 0, true, pts.get(0).getTimestampMillis() + 1);
            check("records before the previous segment's last timestamp are dropped", s.recordCount(), 0L);
            check("... all of them torn", s.recoveredTornBytes(), 10L * R);
            check("... file truncated to the header", Files.size(f), 32L);
            check("... torn file holds them byte for byte",
                    Arrays.equals(Files.readAllBytes(s.tornFile()), Arrays.copyOfRange(before, 32, before.length)), true);
            s.close();
        } finally {
            deleteTree(dir);
        }
    }

    private static void testRecoverBeyondCapacity() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            List<DataPoint> pts = points(12, new Random(9), 5);
            Path f = write(dir, pts.subList(0, 10), 10);
            Files.write(f, encode(pts.subList(10, 12)).array(), java.nio.file.StandardOpenOption.APPEND);
            expectRecovered("valid records past capacity", dir, f, Files.readAllBytes(f), Files.size(f), 10);
        } finally {
            deleteTree(dir);
        }
    }

    private static void testNonLastMustBeExactlyFull() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-seg");
        try {
            List<DataPoint> pts = points(10, new Random(10), 5);
            Path f = write(dir, pts, 10);
            Segment full = Segment.open(f, 0, false);
            check("a full non-last segment opens with count = capacity", full.recordCount(), 10L);
            check("... and its first/last timestamps",
                    List.of(full.firstTimestamp(), full.lastTimestamp()),
                    List.of(pts.get(0).getTimestampMillis(), pts.get(9).getTimestampMillis()));
            full.close();
            expectCorrupt("non-last whose first record is before the floor", f, 0, false, pts.get(0).getTimestampMillis() + 1);

            byte[] good = Files.readAllBytes(f);
            Files.write(f, Arrays.copyOf(good, good.length - R));
            expectCorrupt("non-last segment one record short", f, 0, false, Long.MIN_VALUE);
            Files.write(f, Arrays.copyOf(good, good.length - 5));
            expectCorrupt("non-last segment with a partial last record", f, 0, false, Long.MIN_VALUE);
            Files.write(f, Arrays.copyOf(good, good.length + R));
            expectCorrupt("non-last segment with 32 trailing bytes", f, 0, false, Long.MIN_VALUE);
            Files.write(f, Arrays.copyOf(good, good.length + 3));
            expectCorrupt("non-last segment with 3 trailing bytes", f, 0, false, Long.MIN_VALUE);
            byte[] badLast = good.clone();
            badLast[badLast.length - 1] ^= 0x40;
            Files.write(f, badLast);
            expectCorrupt("non-last segment whose last record fails its CRC", f, 0, false, Long.MIN_VALUE);
            check("rejected non-last segments are left untouched (no .torn file)", tornFiles(dir).size(), 0);
        } finally {
            deleteTree(dir);
        }
    }

    // --- helpers ---

    /**
     * Opens {@code f} as the last segment and checks it kept {@code goodRecords},
     * moved bytes [32 + 32*goodRecords, size) of {@code before} to a .torn file,
     * and truncated; then that a second open finds nothing more to repair and
     * that the segment accepts appends again.
     */
    private static void expectRecovered(String what, Path dir, Path f, byte[] before, long size, int goodRecords)
            throws IOException {
        long keep = 32 + (long) goodRecords * R;
        Segment s = Segment.open(f, 0, true);
        check(what + ": count is the good records before the problem", s.recordCount(), (long) goodRecords);
        check(what + ": recoveredTornBytes", s.recoveredTornBytes(), size - keep);
        check(what + ": file truncated to the last good record", Files.size(f), keep);
        List<Path> torn = tornFiles(dir);
        check(what + ": one .torn-* file next to the segment", torn.size() == 1 && torn.get(0).equals(s.tornFile()), true);
        check(what + ": torn file holds the dropped bytes byte for byte",
                Arrays.equals(Files.readAllBytes(s.tornFile()), Arrays.copyOfRange(before, (int) keep, (int) size)), true);
        check(what + ": kept bytes are unchanged",
                Arrays.equals(Files.readAllBytes(f), Arrays.copyOf(before, (int) keep)), true);
        long appended = 0;
        if (!s.isFull()) {
            DataPoint more = new DataPoint(Math.max(s.lastTimestamp(), 0) + 1, "NEXT", 1.0, 1, DataPoint.Side.BUY);
            s.append(encode(List.of(more)), 1);
            appended = 1;
        }
        s.close();

        Segment again = Segment.open(f, 0, true);
        check(what + ": reopening repairs nothing more" + (appended == 1 ? " and keeps a new append" : ""),
                List.of(again.recordCount(), again.recoveredTornBytes(), (long) tornFiles(dir).size()),
                List.of(goodRecords + appended, 0L, 1L));
        again.close();
    }

    private interface HeaderEdit { void apply(ByteBuffer header); }

    /**
     * Writes a fresh segment (baseSeq 0, or the base the edit puts in the
     * header for a name mismatch), applies {@code edit} to its header,
     * optionally recomputes the header CRC so only the intended field is
     * wrong, and expects open to throw.
     */
    private static void rejected(String what, Path dir, HeaderEdit edit, boolean fixCrc, long expectedBase, boolean isLast)
            throws IOException {
        Path f = write(dir, points(3, new Random(11), 5), 3);
        mutate(f, b -> {
            ByteBuffer h = b.slice(0, 32).order(ByteOrder.LITTLE_ENDIAN);
            edit.apply(h);
            if (fixCrc) {
                java.util.zip.CRC32C c = new java.util.zip.CRC32C();
                c.update(h.slice(0, 28));
                h.putInt(28, (int) c.getValue());
            }
        });
        expectCorrupt(what, f, expectedBase, isLast, Long.MIN_VALUE);
        Files.delete(f);
    }

    private static void expectCorrupt(String what, Path f, long expectedBase, boolean isLast, long floor) throws IOException {
        byte[] before = Files.readAllBytes(f);
        try {
            Segment s = Segment.open(f, expectedBase, isLast, floor);
            s.close();
            failures++;
            System.out.println("  FAIL expected CorruptStoreException for " + what);
        } catch (CorruptStoreException e) {
            boolean untouched = Arrays.equals(before, Files.readAllBytes(f));
            if (untouched) {
                passed++;
                System.out.println("  ok   " + what + " rejected: " + e.getMessage());
            } else {
                failures++;
                System.out.println("  FAIL " + what + " was rejected but the file changed");
            }
        }
    }

    /** A closed segment at baseSeq 0 holding {@code pts}. */
    private static Path write(Path dir, List<DataPoint> pts, int capacity) throws IOException {
        Segment s = Segment.create(dir, 0, capacity);
        if (!pts.isEmpty()) s.append(encode(pts), pts.size());
        s.close();
        return s.path();
    }

    private interface ByteEdit { void apply(ByteBuffer whole); }

    private static void mutate(Path f, ByteEdit edit) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(f)).order(ByteOrder.LITTLE_ENDIAN);
        edit.apply(b);
        Files.write(f, b.array());
    }

    /** n points with timestamps that step by 0..maxStep-1 ms from 1,000,000. */
    private static List<DataPoint> points(int n, Random rnd, int maxStep) {
        List<DataPoint> out = new ArrayList<>();
        long ts = 1_000_000;
        String[] syms = {"AAPL", "MSFT", "BRK.B", "NVDA"};
        for (int i = 0; i < n; i++) {
            ts += rnd.nextInt(maxStep);
            out.add(new DataPoint(ts, syms[rnd.nextInt(syms.length)], rnd.nextDouble() * 500, 1 + rnd.nextInt(500),
                    rnd.nextBoolean() ? DataPoint.Side.BUY : DataPoint.Side.SELL));
        }
        return out;
    }

    private static ByteBuffer encode(List<DataPoint> pts) {
        ByteBuffer b = le(pts.size() * R);
        for (DataPoint p : pts) RecordCodec.encode(p, b);
        return b.flip();
    }

    private static ByteBuffer le(int n) {
        return ByteBuffer.allocate(n).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static List<Path> tornFiles(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().contains(".torn-")).toList();
        }
    }

    static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : s.sorted(Comparator.reverseOrder()).toList()) Files.delete(p);
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
