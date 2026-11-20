package com.tsdb.storage;

import com.tsdb.DataPoint;
import com.tsdb.FinancialDataGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * End-to-end slice: the shell's real trade source, through the store, to
 * disk, through a close and a reopen, and back. The expected trades are not
 * kept from the write side but regenerated from the same seed, so the test
 * compares what came off disk against an independent copy.
 *
 * Run once with 1,000-record segments (10 segment files) and once with the
 * default options (everything in one segment).
 *
 * Run with:  java -cp out com.tsdb.storage.StorageRoundTripTest
 * Exits with status 0 if all pass, otherwise prints each failure and exits 1.
 */
public class StorageRoundTripTest {

    private static int failures = 0;
    private static int passed = 0;
    private static final int N = 10_000;

    public static void main(String[] args) throws IOException {
        roundTrip("1,000-record segments", DiskStore.Options.defaults().withSegmentCapacity(1000), 10);
        roundTrip("default options", DiskStore.Options.defaults(), 1);

        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + passed + " ROUND-TRIP TESTS PASSED");
        } else {
            System.out.println(failures + " ROUND-TRIP TEST(S) FAILED (" + passed + " passed)");
            System.exit(1);
        }
    }

    private static void roundTrip(String label, DiskStore.Options options, int expectedSegments) throws IOException {
        Path dir = Files.createTempDirectory("tsdb-roundtrip");
        try {
            FinancialDataGenerator source = new FinancialDataGenerator(1_000_000L, 1, 10, new Random(42));
            try (DiskStore s = DiskStore.open(dir, options)) {
                for (int i = 0; i < N; i++) s.append(source.next());
            }

            List<DataPoint> expected = regenerate();
            try (DiskStore s = DiskStore.open(dir, options)) {
                check(label + ": " + expectedSegments + " segment file(s) on disk", segmentFiles(dir), expectedSegments);
                check(label + ": nextSeq() == 10000", s.nextSeq(), (long) N);
                check(label + ": lastTimestamp() is the last generated timestamp", s.lastTimestamp(),
                        expected.get(N - 1).getTimestampMillis());
                check(label + ": nothing torn", s.stats().recoveredTornBytes(), 0L);
                compare(label + ": scanAll() after reopen equals the regenerated trades", s.scanAll(), expected);
                compare(label + ": scanFromSeq(5000) equals the second half", s.scanFromSeq(5000),
                        expected.subList(5000, N));
            }
        } finally {
            SegmentTest.deleteTree(dir);
        }
    }

    private static List<DataPoint> regenerate() {
        FinancialDataGenerator g = new FinancialDataGenerator(1_000_000L, 1, 10, new Random(42));
        List<DataPoint> out = new ArrayList<>(N);
        for (int i = 0; i < N; i++) out.add(g.next());
        return out;
    }

    /** Drains the cursor, comparing field by field (price by raw bits); reports the first difference. */
    private static void compare(String name, RecordCursor c, List<DataPoint> expected) {
        int i = 0;
        String problem = null;
        try (c) {
            while (c.hasNext()) {
                DataPoint got = c.next();
                if (i >= expected.size()) {
                    problem = "more records than expected";
                    break;
                }
                if (!RecordCodecTest.same(got, expected.get(i))) {
                    problem = "first difference at index " + i + ": got " + got + ", expected " + expected.get(i);
                    break;
                }
                i++;
            }
        }
        if (problem == null && i != expected.size()) problem = "got " + i + " record(s), expected " + expected.size();
        if (problem == null) {
            passed++;
            System.out.println("  ok   " + name + " (" + i + " record(s))");
        } else {
            failures++;
            System.out.println("  FAIL " + name + " -- " + problem);
        }
    }

    private static int segmentFiles(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return (int) s.filter(p -> Segment.baseSeqOf(p.getFileName().toString()) >= 0).count();
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
