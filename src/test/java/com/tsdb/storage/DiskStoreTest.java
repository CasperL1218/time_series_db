package com.tsdb.storage;

import com.tsdb.DataPoint;
import com.tsdb.FinancialDataGenerator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Tests for the store as a whole: appends against an in-memory oracle list,
 * buffered-record visibility, segment rolling, reopen, scanFromSeq and
 * resuming, scan(from, to) against brute force, the out-of-order rule,
 * simulated crashes, recovery and corruption at the directory level, the
 * directory lock, sync options, one writer racing two readers, and a large
 * scan under a small heap (in a child JVM).
 *
 * Most tests use tiny segments (100 records) and a tiny buffer (10 records)
 * so rolling and flushing happen constantly, and a one-hour sync interval so
 * what is buffered stays buffered until the test says otherwise.
 *
 * Helpers: points() checks a scan against the expected list; throwsA()
 * asserts an action fails with a given exception; check() covers everything
 * else. Each test works in its own temp directory, deleted afterwards.
 *
 * Run with:  java -cp out com.tsdb.storage.DiskStoreTest
 * Exits with status 0 if all pass, otherwise prints each failure and exits 1.
 */
public class DiskStoreTest {

    private static int failures = 0;
    private static int passed = 0;
    private static final int R = RecordCodec.RECORD_SIZE;
    private static final long HOUR = 3_600_000;

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--large-scan")) {
            System.exit(largeScanChild(Path.of(args[1])));
        }
        testAppendAndScanAll();
        testBufferedRecordsAreVisible();
        testSegmentRolling();
        testReopen();
        testScanFromSeq();
        testScanTimeRange();
        testOutOfOrderAndBadSymbol();
        testCrashLosesOnlyTheBuffer();
        testRecoverTornLastSegment();
        testCorruptDirectoriesAreRejected();
        testHeaderlessLastSegmentIsSetAside();
        testDirectoryLockAndClose();
        testSyncOptions();
        testOneWriterTwoReaders();
        testLargeScanUnderSmallHeap();

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " DISK STORE TESTS PASSED");
        } else {
            System.out.println(failures + " DISK STORE TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    private static void testAppendAndScanAll() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
            check("a fresh store creates no segment file", segmentFiles(dir).size(), 0);
            check("a fresh store is empty", List.of(s.nextSeq(), s.lastTimestamp()), List.of(0L, Long.MIN_VALUE));
            points("scanAll of an empty store", s.scanAll(), List.of());
            List<DataPoint> in = generate(1234, 1);
            boolean consecutive = true;
            for (int i = 0; i < in.size(); i++) if (s.append(in.get(i)) != i) consecutive = false;
            check("append returns consecutive seqs from 0", consecutive, true);
            points("scanAll equals what was appended", s.scanAll(), in);
            check("nextSeq", s.nextSeq(), 1234L);
            check("lastTimestamp", s.lastTimestamp(), in.get(1233).getTimestampMillis());
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testBufferedRecordsAreVisible() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
            List<DataPoint> in = generate(40, 2);
            List<DataPoint> oracle = new ArrayList<>();
            for (int i = 0; i < 5; i++) append(s, in, oracle);
            Path seg0 = dir.resolve(Segment.fileName(0));
            check("5 appends: all 5 still buffered, file is just the header",
                    List.of(s.stats().bufferedRecords(), Files.size(seg0)), List.of(5L, 32L));
            points("buffered-only records are returned by scanAll", s.scanAll(), oracle);

            RecordCursor before = s.scanAll();
            s.flush();
            check("after flush nothing is buffered and the file holds 5 records",
                    List.of(s.stats().bufferedRecords(), Files.size(seg0)), List.of(0L, 32L + 5 * R));
            points("after flush each record is returned exactly once", s.scanAll(), oracle);
            points("a cursor taken before the flush still reads the same 5", before, oracle);

            for (int i = 0; i < 3; i++) append(s, in, oracle);
            check("3 more: 5 on disk + 3 buffered", s.stats().bufferedRecords(), 3L);
            points("scanAll across the flushed and buffered parts has no gap or duplicate", s.scanAll(), oracle);

            RecordCursor snapshot = s.scanAll();
            for (int i = 0; i < 7; i++) append(s, in, oracle); // the buffer fills at 10 and is written right away
            check("filling the 10-record buffer writes it: 15 on disk, 0 buffered",
                    List.of(s.stats().bufferedRecords(), Files.size(seg0)), List.of(0L, 32L + 15 * R));
            points("a cursor sees only the records that existed when it was created", snapshot, oracle.subList(0, 8));

            boolean allEqual = true;
            for (int i = 0; i < 25; i++) {
                append(s, in, oracle);
                if (!same(drain(s.scanAll()), oracle)) allEqual = false;
            }
            check("scanAll equals the oracle after every one of 25 more appends", allEqual, true);
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testSegmentRolling() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            List<DataPoint> in = generate(1050, 3);
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                for (DataPoint p : in) s.append(p);
                check("1,050 records in 100-record segments: 11 segments", s.stats().segmentCount(), 11);
                s.flush();
                check("stats().bytesOnDisk equals the files' total size", s.stats().bytesOnDisk(), totalSize(dir));
                points("scanAll across 11 segments", s.scanAll(), in);
            }
            List<String> expectedNames = new ArrayList<>();
            for (long b = 0; b <= 1000; b += 100) expectedNames.add(String.format("seg-%020d.tsd", b));
            List<Path> files = segmentFiles(dir);
            check("segment files are named by baseSeq", files.stream().map(p -> p.getFileName().toString()).toList(),
                    expectedNames);
            boolean fullOk = true;
            for (int i = 0; i < 10; i++) if (Files.size(files.get(i)) != 32 + 100L * R) fullOk = false;
            check("the 10 rolled segments hold exactly 100 records each", fullOk, true);
            check("the last segment holds the remaining 50", Files.size(files.get(10)), 32 + 50L * R);
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testReopen() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            List<DataPoint> in = generate(1077, 4);
            List<DataPoint> first = in.subList(0, 777);
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                for (DataPoint p : first) s.append(p);
            }
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                check("reopen: nextSeq", s.nextSeq(), 777L);
                check("reopen: lastTimestamp", s.lastTimestamp(), first.get(776).getTimestampMillis());
                check("reopen: stats count, segments, nothing buffered, all synced, nothing torn",
                        List.of(s.stats().totalRecords(), (long) s.stats().segmentCount(), s.stats().bufferedRecords(),
                                s.stats().syncedRecords(), s.stats().recoveredTornBytes()),
                        List.of(777L, 8L, 0L, 777L, 0L));
                points("reopen: scanAll unchanged", s.scanAll(), first);
                check("append after reopen continues the sequence", s.append(in.get(777)), 777L);
                for (DataPoint p : in.subList(778, 1077)) s.append(p);
                points("scanAll after reopen = old + new records", s.scanAll(), in);
            }
            try (DiskStore s = DiskStore.open(dir, small(40, 10))) { // a different capacity for new segments
                points("second reopen (new capacity option) reads everything", s.scanAll(), in);
                List<DataPoint> more = generateAfter(in.get(1076).getTimestampMillis(), 100, 5);
                for (DataPoint p : more) s.append(p);
                List<DataPoint> all = new ArrayList<>(in);
                all.addAll(more);
                points("appends after a capacity change", s.scanAll(), all);
                s.flush();
                check("old segments keep 100 records, new ones use 40",
                        Files.size(dir.resolve(Segment.fileName(1100))), 32 + 40L * R);
            }
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testScanFromSeq() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
            List<DataPoint> in = generate(1255, 6);
            List<DataPoint> oracle = new ArrayList<>(in.subList(0, 1005));
            for (DataPoint p : oracle) s.append(p);
            check("fixture: the last 5 records are still buffered", s.stats().bufferedRecords(), 5L);
            long n = s.nextSeq();
            for (long k : new long[] {0, 517, 99, 100, 101, 999, 1000, 1001, n - 1, n}) {
                points("scanFromSeq(" + k + ")", s.scanFromSeq(k), oracle.subList((int) k, (int) n));
            }
            throwsA("scanFromSeq(nextSeq + 1)", IllegalArgumentException.class, () -> s.scanFromSeq(n + 1));
            throwsA("scanFromSeq(-1)", IllegalArgumentException.class, () -> s.scanFromSeq(-1));

            RecordCursor empty = s.scanFromSeq(n);
            check("scanFromSeq(nextSeq) is empty and its nextSeq() is nextSeq", List.of(empty.hasNext(), empty.nextSeq()),
                    List.of(false, n));
            throwsA("next() on an exhausted cursor", NoSuchElementException.class, empty::next);

            RecordCursor c = s.scanFromSeq(517);
            check("a fresh cursor's nextSeq() is its start", c.nextSeq(), 517L);
            for (int j = 0; j < 3; j++) c.next();
            check("after reading 3 records nextSeq() is 517 + 3", c.nextSeq(), 520L);
            c.hasNext();
            check("hasNext() doesn't advance nextSeq()", c.nextSeq(), 520L);
            points("resuming at a partly read cursor's nextSeq()", s.scanFromSeq(c.nextSeq()), oracle.subList(520, (int) n));
            c.close();
            check("a closed cursor has no next", c.hasNext(), false);

            RecordCursor all = s.scanAll();
            drain(all);
            check("an exhausted scanAll's nextSeq() is nextSeq", all.nextSeq(), n);
            List<DataPoint> more = in.subList(1005, 1255);
            for (DataPoint p : more) s.append(p);
            points("tail: scanFromSeq(cursor.nextSeq()) after 250 more appends returns exactly them",
                    s.scanFromSeq(all.nextSeq()), more);
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testScanTimeRange() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
            Random rnd = new Random(7);
            List<DataPoint> in = new ArrayList<>();
            long ts = 5_000;
            for (int i = 0; i < 1007; i++) {
                // Runs of equal timestamps at seq 95..105, 199..201 and 297..300 (each spans a segment
                // boundary), with a forced step on both sides so a run is exactly those records.
                boolean inRun = (i > 95 && i <= 105) || (i > 199 && i <= 201) || (i > 297 && i <= 300);
                boolean edge = i == 95 || i == 106 || i == 199 || i == 202 || i == 297 || i == 301;
                ts += inRun ? 0 : edge ? 1 : rnd.nextInt(3);
                in.add(dp(ts, "S" + (i % 7), i, i % 2 == 0 ? DataPoint.Side.BUY : DataPoint.Side.SELL));
            }
            for (DataPoint p : in) s.append(p);
            check("fixture: last 7 records are buffered", s.stats().bufferedRecords(), 7L);
            long boundaryTs = in.get(100).getTimestampMillis();
            points("equal timestamps spanning a segment boundary (seq 95..105)", s.scan(boundaryTs, boundaryTs),
                    in.subList(95, 106));
            points("equal timestamps at the end of a segment and into the next (seq 297..300)",
                    s.scan(in.get(300).getTimestampMillis(), in.get(300).getTimestampMillis()), in.subList(297, 301));

            long first = in.get(0).getTimestampMillis(), last = in.get(1006).getTimestampMillis();
            List<long[]> ranges = new ArrayList<>();
            long[] specials = {first, last, in.get(99).getTimestampMillis(), in.get(100).getTimestampMillis(),
                    in.get(1000).getTimestampMillis(), in.get(999).getTimestampMillis(), in.get(1003).getTimestampMillis()};
            for (long a : specials) {
                for (long b : specials) ranges.add(new long[] {a, b});
                ranges.add(new long[] {a, a});
                ranges.add(new long[] {a - 1, a + 1});
            }
            ranges.add(new long[] {Long.MIN_VALUE, Long.MAX_VALUE});
            ranges.add(new long[] {Long.MIN_VALUE, first - 1});  // entirely before
            ranges.add(new long[] {last + 1, Long.MAX_VALUE});   // entirely after
            ranges.add(new long[] {first - 100, first - 50});
            ranges.add(new long[] {last + 3, last + 1});         // from > to
            ranges.add(new long[] {in.get(1001).getTimestampMillis(), last}); // only buffered records
            while (ranges.size() < 500) {
                long a = first - 20 + rnd.nextInt((int) (last - first) + 40);
                long b = rnd.nextInt(10) == 0 ? a - rnd.nextInt(5) : a + rnd.nextInt(200);
                ranges.add(new long[] {a, b});
            }
            int bad = 0, empties = 0;
            for (long[] r : ranges) {
                List<DataPoint> brute = new ArrayList<>();
                for (DataPoint p : in) {
                    if (p.getTimestampMillis() >= r[0] && p.getTimestampMillis() <= r[1]) brute.add(p);
                }
                if (brute.isEmpty()) empties++;
                if (!same(drain(s.scan(r[0], r[1])), brute)) {
                    bad++;
                    if (bad <= 5) System.out.println("  FAIL scan(" + r[0] + ", " + r[1] + ") differs from brute force");
                }
            }
            check("scan(from, to) equals brute force for " + ranges.size() + " ranges (" + empties + " empty)", bad, 0);

            RecordCursor c = s.scan(in.get(500).getTimestampMillis(), in.get(510).getTimestampMillis());
            int read = drain(c).size();
            long expectNext = 0;
            while (expectNext < in.size() && in.get((int) expectNext).getTimestampMillis() <= in.get(510).getTimestampMillis()) expectNext++;
            check("an exhausted range cursor's nextSeq() is the first record after the range",
                    List.of(c.nextSeq(), read > 0), List.of(expectNext, true));
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testOutOfOrderAndBadSymbol() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try (DiskStore s = DiskStore.open(dir, small(2, 10))) {
            s.append(dp(100, "A", 1, DataPoint.Side.BUY));
            check("an equal timestamp is accepted", s.append(dp(100, "B", 2, DataPoint.Side.SELL)), 1L);
            StoreStats before = s.stats();
            throwsA("an older timestamp", IllegalArgumentException.class, () -> s.append(dp(99, "C", 3, DataPoint.Side.BUY)));
            throwsA("a bad symbol", IllegalArgumentException.class, () -> s.append(dp(100, "TOOLONGX", 3, DataPoint.Side.BUY)));
            throwsA("an empty symbol", IllegalArgumentException.class, () -> s.append(dp(100, "", 3, DataPoint.Side.BUY)));
            throwsA("a null side", IllegalArgumentException.class, () -> s.append(dp(100, "C", 3, null)));
            check("rejected appends change nothing (stats identical)", s.stats(), before);
            check("... and don't roll to a new segment even though the active one is full",
                    segmentFiles(dir).size(), 1);
            points("... and write nothing", s.scanAll(),
                    List.of(dp(100, "A", 1, DataPoint.Side.BUY), dp(100, "B", 2, DataPoint.Side.SELL)));
            check("the next good append still gets seq 2", s.append(dp(101, "C", 3, DataPoint.Side.BUY)), 2L);
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testCrashLosesOnlyTheBuffer() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            List<DataPoint> in = generate(300, 8);
            DiskStore s = DiskStore.open(dir, small(100, 10));
            for (DataPoint p : in.subList(0, 25)) s.append(p); // 20 written (two full buffers), 5 buffered
            s.closeWithoutFlush();
            s = DiskStore.open(dir, small(100, 10));
            check("crash: the 5 buffered records are lost, the 20 written survive", s.nextSeq(), 20L);
            points("crash: scanAll is the first 20", s.scanAll(), in.subList(0, 20));

            for (DataPoint p : in.subList(20, 27)) s.append(p);
            s.flush();
            for (DataPoint p : in.subList(27, 29)) s.append(p);
            s.closeWithoutFlush();
            s = DiskStore.open(dir, small(100, 10));
            check("crash: records covered by flush() survive, later buffered ones don't", s.nextSeq(), 27L);

            for (DataPoint p : in.subList(27, 230)) s.append(p); // rolls twice
            s.sync();
            s.append(in.get(230));
            s.closeWithoutFlush();
            s = DiskStore.open(dir, small(100, 10));
            check("crash: records covered by sync() survive across rolled segments", s.nextSeq(), 230L);
            points("crash: scanAll after the third crash", s.scanAll(), in.subList(0, 230));
            check("crash: a lost buffer is not reported as torn bytes", s.stats().recoveredTornBytes(), 0L);
            s.close();
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testRecoverTornLastSegment() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            List<DataPoint> in = generate(260, 9);
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                for (DataPoint p : in.subList(0, 250)) s.append(p);
            }
            Path last = dir.resolve(Segment.fileName(200));
            try (RandomAccessFile raf = new RandomAccessFile(last.toFile(), "rw")) {
                raf.setLength(32 + 30L * R + 5);
            }
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                check("torn tail: the 230 good records survive", s.nextSeq(), 230L);
                points("torn tail: scanAll", s.scanAll(), in.subList(0, 230));
                check("torn tail: recoveredTornBytes = the 5 dropped bytes", s.stats().recoveredTornBytes(), 5L);
                check("torn tail: a .torn-* file exists", tornFiles(dir).size(), 1);
                check("torn tail: lastTimestamp is the last good record's", s.lastTimestamp(),
                        in.get(229).getTimestampMillis());
                List<DataPoint> more = generateAfter(in.get(229).getTimestampMillis(), 10, 10);
                for (DataPoint p : more) s.append(p);
                List<DataPoint> expected = new ArrayList<>(in.subList(0, 230));
                expected.addAll(more);
                points("torn tail: the store accepts new appends", s.scanAll(), expected);
            }
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                check("torn tail: the next open finds nothing torn", List.of(s.nextSeq(), s.stats().recoveredTornBytes()),
                        List.of(240L, 0L));
            }
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testCorruptDirectoriesAreRejected() throws IOException {
        corrupt("a corrupted header in a middle segment", 250, dir -> {
            Path f = dir.resolve(Segment.fileName(100));
            byte[] b = Files.readAllBytes(f);
            b[9] ^= 0x01;
            Files.write(f, b);
        });
        corrupt("a missing middle segment (gap)", 250, dir -> Files.delete(dir.resolve(Segment.fileName(100))));
        corrupt("a missing first segment", 250, dir -> Files.delete(dir.resolve(Segment.fileName(0))));
        corrupt("a renamed segment (name and header disagree)", 250,
                dir -> Files.move(dir.resolve(Segment.fileName(200)), dir.resolve(Segment.fileName(300))));
        corrupt("a segment renamed to an unexpected seg-* name", 250,
                dir -> Files.move(dir.resolve(Segment.fileName(200)), dir.resolve(Segment.fileName(200) + ".bak")));
        corrupt("a short non-last segment", 250, dir -> {
            try (RandomAccessFile raf = new RandomAccessFile(dir.resolve(Segment.fileName(100)).toFile(), "rw")) {
                raf.setLength(raf.length() - R);
            }
        });
        corrupt("a non-last segment with trailing bytes", 250, dir ->
                Files.write(dir.resolve(Segment.fileName(0)), new byte[7], java.nio.file.StandardOpenOption.APPEND));
        corrupt("a non-last segment whose first record goes back in time", 250, dir -> {
            // Swap segment 0 and 1's contents in time: rewrite seg 100's first record with an old timestamp.
            Path f = dir.resolve(Segment.fileName(100));
            byte[] b = Files.readAllBytes(f);
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            DataPoint p = RecordCodec.decode(buf, 32);
            buf.position(32);
            RecordCodec.encode(new DataPoint(0, p.getSymbol(), p.getPrice(), p.getQuantity(), p.getSide()), buf);
            Files.write(f, b);
        });
    }

    private static void testHeaderlessLastSegmentIsSetAside() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            List<DataPoint> in = generate(310, 11);
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                for (DataPoint p : in.subList(0, 300)) s.append(p);
            }
            Files.write(dir.resolve(Segment.fileName(300)), new byte[10]); // died while creating the next segment
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                check("a last segment under 32 bytes is treated as absent", s.nextSeq(), 300L);
                check("... it is moved aside as .torn-*",
                        List.of(Files.exists(dir.resolve(Segment.fileName(300))), tornFiles(dir).size()), List.of(false, 1));
                check("... and its bytes count as recovered", s.stats().recoveredTornBytes(), 10L);
                for (DataPoint p : in.subList(300, 310)) s.append(p);
                points("... and the next append recreates the segment", s.scanAll(), in);
            }

            Path lone = Files.createTempDirectory("tsdb-store");
            try {
                Files.write(lone.resolve(Segment.fileName(0)), new byte[31]);
                try (DiskStore s = DiskStore.open(lone, small(100, 10))) {
                    check("a store whose only segment is headerless opens empty",
                            List.of(s.nextSeq(), (long) s.stats().segmentCount()), List.of(0L, 0L));
                }
            } finally {
                SegmentTest.deleteTree(lone);
            }
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testDirectoryLockAndClose() throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            DiskStore a = DiskStore.open(dir, small(100, 10));
            a.append(dp(1, "A", 1, DataPoint.Side.BUY));
            try {
                DiskStore b = DiskStore.open(dir, small(100, 10));
                b.close();
                failures++;
                System.out.println("  FAIL a second open of the same directory succeeded");
            } catch (IOException e) {
                boolean right = !(e instanceof CorruptStoreException) && e.getMessage().contains("data directory in use");
                check("a second open of the same directory throws IOException(\"data directory in use\")", right, true);
            }
            a.close();
            a.close();
            passed++;
            System.out.println("  ok   close twice is fine");
            try (DiskStore b = DiskStore.open(dir, small(100, 10))) {
                check("after close the directory opens again (and kept the record)", b.nextSeq(), 1L);
            }
            throwsA("append after close", IllegalStateException.class, () -> a.append(dp(2, "A", 1, DataPoint.Side.BUY)));
            throwsA("scanAll after close", IllegalStateException.class, a::scanAll);
            throwsA("scan after close", IllegalStateException.class, () -> a.scan(0, 1));
            throwsA("scanFromSeq after close", IllegalStateException.class, () -> a.scanFromSeq(0));
            throwsA("flush after close", IllegalStateException.class, a::flush);
            throwsA("sync after close", IllegalStateException.class, a::sync);
            throwsA("nextSeq after close", IllegalStateException.class, a::nextSeq);
            throwsA("stats after close", IllegalStateException.class, a::stats);

            Path missing = dir.resolve("not/yet/there");
            try (DiskStore c = DiskStore.open(missing, small(100, 10))) {
                check("open creates a missing directory", Files.isDirectory(missing) && c.nextSeq() == 0, true);
            }
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testSyncOptions() throws Exception {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            List<DataPoint> in = generate(400, 12);
            try (DiskStore s = DiskStore.open(dir, small(100, 10).withSyncIntervalMs(0))) {
                boolean always = true;
                for (DataPoint p : in.subList(0, 250)) {
                    s.append(p);
                    StoreStats st = s.stats();
                    if (st.syncedRecords() != s.nextSeq() || st.bufferedRecords() != 0) always = false;
                }
                check("syncIntervalMs = 0: syncedRecords == nextSeq after every append", always, true);
            }
            SegmentTest.deleteTree(dir);
            Files.createDirectories(dir);
            try (DiskStore s = DiskStore.open(dir, DiskStore.Options.defaults())) {
                for (DataPoint p : in.subList(0, 50)) s.append(p);
                StoreStats st = s.stats();
                check("default options: syncedRecords lags behind nextSeq",
                        st.syncedRecords() < s.nextSeq() && st.bufferedRecords() == 50, true);
                s.sync();
                check("after sync() syncedRecords == nextSeq and nothing is buffered",
                        List.of(s.stats().syncedRecords(), s.stats().bufferedRecords()), List.of(50L, 0L));
                s.append(in.get(50));
                check("an append right after sync() is not synced yet", s.stats().syncedRecords(), 50L);
                Thread.sleep(1100);
                s.append(in.get(51));
                check("an append after syncIntervalMs has passed fsyncs everything",
                        s.stats().syncedRecords(), 52L);
            }
            throwsA("withBufferBytes(33)", IllegalArgumentException.class, () -> DiskStore.Options.defaults().withBufferBytes(33));
            throwsA("withSegmentCapacity(0)", IllegalArgumentException.class, () -> DiskStore.Options.defaults().withSegmentCapacity(0));
            throwsA("withSyncIntervalMs(-1)", IllegalArgumentException.class, () -> DiskStore.Options.defaults().withSyncIntervalMs(-1));
            DiskStore.Options d = DiskStore.Options.defaults();
            check("defaults: 1,048,576 records/segment, 65,536-byte buffer, 1,000 ms",
                    List.of((long) d.segmentCapacity(), (long) d.bufferBytes(), d.syncIntervalMs()),
                    List.of(1_048_576L, 65_536L, 1000L));
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static void testOneWriterTwoReaders() throws Exception {
        Path dir = Files.createTempDirectory("tsdb-store");
        try (DiskStore s = DiskStore.open(dir, DiskStore.Options.defaults().withSegmentCapacity(1000).withBufferBytes(64 * R))) {
            List<DataPoint> oracle = generate(50_000, 13);
            AtomicReference<Throwable> error = new AtomicReference<>();
            AtomicInteger scans = new AtomicInteger(), badScans = new AtomicInteger(), growing = new AtomicInteger();
            Thread writer = new Thread(() -> {
                try {
                    for (DataPoint p : oracle) s.append(p);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                }
            }, "writer");
            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < 2; r++) {
                readers.add(new Thread(() -> {
                    try {
                        int lastSize = -1;
                        while (writer.isAlive() || lastSize < 0) {
                            List<DataPoint> got = drain(s.scanAll());
                            scans.incrementAndGet();
                            if (got.size() < lastSize) badScans.incrementAndGet(); // the log never shrinks
                            if (got.size() > lastSize && lastSize >= 0) growing.incrementAndGet();
                            lastSize = got.size();
                            if (got.size() > oracle.size() || !same(got, oracle.subList(0, got.size()))) {
                                badScans.incrementAndGet();
                            }
                        }
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                }, "reader-" + r));
            }
            writer.start();
            for (Thread t : readers) t.start();
            writer.join();
            for (Thread t : readers) t.join();
            check("concurrency: no thread threw" + (error.get() == null ? "" : " (" + error.get() + ")"), error.get(), null);
            check("concurrency: every one of " + scans.get() + " concurrent scans was an exact prefix of the oracle",
                    badScans.get(), 0);
            check("concurrency: readers saw the log grow while the writer ran (" + growing.get() + " times)",
                    growing.get() > 0, true);
            points("concurrency: the final scan has all 50,000", s.scanAll(), oracle);
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    /**
     * Runs this class in a child JVM with -Xmx16m to write and then scan
     * 200,000 records. Materializing them as DataPoints would need roughly
     * 20 MB, so passing shows the cursor streams in chunks.
     */
    private static void testLargeScanUnderSmallHeap() throws Exception {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            Process p = new ProcessBuilder(java, "-Xmx16m", "-cp", System.getProperty("java.class.path"),
                    DiskStoreTest.class.getName(), "--large-scan", dir.toString())
                    .redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes()).trim();
            int code = p.waitFor();
            check("200,000-record scan under -Xmx16m (child JVM: " + out + ")", code, 0);
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    /** Child side of testLargeScanUnderSmallHeap: 0 if the scan matches the regenerated stream. */
    private static int largeScanChild(Path dir) throws IOException {
        final int n = 200_000;
        try (DiskStore s = DiskStore.open(dir, DiskStore.Options.defaults().withSegmentCapacity(50_000))) {
            FinancialDataGenerator g = new FinancialDataGenerator(1_000_000, 1, 10, new Random(14));
            for (int i = 0; i < n; i++) s.append(g.next());
            s.sync();
            FinancialDataGenerator again = new FinancialDataGenerator(1_000_000, 1, 10, new Random(14));
            long count = 0;
            try (RecordCursor c = s.scanAll()) {
                while (c.hasNext()) {
                    if (!same(c.next(), again.next())) {
                        System.out.println("mismatch at seq " + count);
                        return 1;
                    }
                    count++;
                }
            }
            Runtime rt = Runtime.getRuntime();
            System.out.println("scanned " + count + " records, max heap " + rt.maxMemory() / (1024 * 1024) + " MB");
            return count == n ? 0 : 1;
        }
    }

    // --- helpers ---

    private interface Corruption { void apply(Path dir) throws IOException; }

    /** Builds a 250-record store in 100-record segments, damages it, and expects open to throw (twice). */
    private static void corrupt(String what, int records, Corruption damage) throws IOException {
        Path dir = Files.createTempDirectory("tsdb-store");
        try {
            try (DiskStore s = DiskStore.open(dir, small(100, 10))) {
                for (DataPoint p : generate(records, 15)) s.append(p);
            }
            damage.apply(dir);
            for (int attempt = 1; attempt <= 2; attempt++) {
                try {
                    DiskStore s = DiskStore.open(dir, small(100, 10));
                    s.close();
                    failures++;
                    System.out.println("  FAIL expected CorruptStoreException for " + what);
                    return;
                } catch (CorruptStoreException e) {
                    if (attempt == 2) {
                        passed++;
                        System.out.println("  ok   " + what + " rejected (and the lock released): " + e.getMessage());
                    }
                }
            }
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static DiskStore.Options small(int capacity, int bufferRecords) {
        return DiskStore.Options.defaults().withSegmentCapacity(capacity).withBufferBytes(bufferRecords * R)
                .withSyncIntervalMs(HOUR);
    }

    private static List<DataPoint> generate(int n, long seed) {
        FinancialDataGenerator g = new FinancialDataGenerator(1_000_000, 1, 10, new Random(seed));
        List<DataPoint> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(g.next());
        return out;
    }

    private static List<DataPoint> generateAfter(long ts, int n, long seed) {
        FinancialDataGenerator g = new FinancialDataGenerator(ts, 1, 10, new Random(seed));
        List<DataPoint> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(g.next());
        return out;
    }

    private static void append(DiskStore s, List<DataPoint> in, List<DataPoint> oracle) throws IOException {
        DataPoint p = in.get(oracle.size());
        s.append(p);
        oracle.add(p);
    }

    private static DataPoint dp(long ts, String sym, double price, DataPoint.Side side) {
        return new DataPoint(ts, sym, price, 1, side);
    }

    private static List<DataPoint> drain(RecordCursor c) {
        List<DataPoint> out = new ArrayList<>();
        try (c) {
            while (c.hasNext()) out.add(c.next());
        }
        return out;
    }

    private static boolean same(DataPoint a, DataPoint b) {
        return RecordCodecTest.same(a, b);
    }

    private static boolean same(List<DataPoint> a, List<DataPoint> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) if (!same(a.get(i), b.get(i))) return false;
        return true;
    }

    /** Drains the cursor and checks it equals {@code expected}, reporting the first difference. */
    private static void points(String name, RecordCursor c, List<DataPoint> expected) {
        List<DataPoint> got = drain(c);
        if (same(got, expected)) {
            passed++;
            System.out.println("  ok   " + name + " (" + got.size() + " record(s))");
            return;
        }
        failures++;
        int i = 0;
        while (i < Math.min(got.size(), expected.size()) && same(got.get(i), expected.get(i))) i++;
        System.out.println("  FAIL " + name + " -- expected " + expected.size() + " record(s), got " + got.size()
                + "; first difference at index " + i);
    }

    private interface Action { void run() throws Exception; }

    private static void throwsA(String name, Class<? extends Throwable> type, Action action) {
        try {
            action.run();
            failures++;
            System.out.println("  FAIL " + name + " -- expected " + type.getSimpleName());
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                passed++;
                System.out.println("  ok   " + name + " throws " + type.getSimpleName());
            } else {
                failures++;
                System.out.println("  FAIL " + name + " -- expected " + type.getSimpleName() + ", got " + t);
            }
        }
    }

    private static List<Path> segmentFiles(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> out = new ArrayList<>(s.filter(p -> Segment.baseSeqOf(p.getFileName().toString()) >= 0).toList());
            Collections.sort(out);
            return out;
        }
    }

    private static List<Path> tornFiles(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().contains(".torn-")).toList();
        }
    }

    private static long totalSize(Path dir) throws IOException {
        long total = 0;
        for (Path p : segmentFiles(dir)) total += Files.size(p);
        return total;
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
