package com.tsdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * What a cursor may read, fixed at the moment it was created: every record
 * with {@code seq < end}. Taken under DiskStore's lock, then used without it.
 *
 * Records {@code [0, tailBase)} are in the segment files, segment i holding
 * {@code counts[i]} of them; positional reads below those counts are safe
 * without the lock because those bytes never change. Records
 * {@code [tailBase, end)} were still in the write buffer, so they come from
 * {@code tail}, a private copy of it (at most one buffer's worth).
 */
final class Snapshot {

    private static final int R = RecordCodec.RECORD_SIZE;

    final Segment[] segs;
    final long[] counts;   // records on disk per segment, as of the snapshot
    final long[] lastTs;   // timestamp of each segment's last on-disk record (when counts[i] > 0)
    final ByteBuffer tail; // little-endian copy of the buffered records
    final long tailBase;   // seq of tail's first record = records on disk
    final long end;        // nextSeq at the snapshot

    Snapshot(Segment[] segs, long[] counts, long[] lastTs, ByteBuffer tail, long tailBase, long end) {
        this.segs = segs;
        this.counts = counts;
        this.lastTs = lastTs;
        this.tail = tail;
        this.tailBase = tailBase;
        this.end = end;
    }

    /** Index of the segment holding on-disk record {@code seq} (requires seq < tailBase). */
    int segmentOf(long seq) {
        int lo = 0, hi = segs.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (segs[mid].baseSeq() <= seq) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    /**
     * The first seq whose timestamp is >= ts, or {@code end} if none.
     *
     * Segments' last timestamps are nondecreasing, so the first segment
     * whose last record is >= ts is where the answer lies, if it is on disk
     * at all. Searching by last (not first) timestamp is what makes a run of
     * equal timestamps that spans a segment boundary start at its first
     * record. Only the newest segment can have nothing on disk yet; it counts
     * as a match so the search falls through to the buffered tail.
     */
    long lowerBound(long ts) throws IOException {
        int lo = 0, hi = segs.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (counts[mid] == 0 || lastTs[mid] >= ts) hi = mid;
            else lo = mid + 1;
        }
        if (lo < segs.length) {
            long idx = segs[lo].lowerBound(ts, counts[lo]);
            if (idx < counts[lo]) return segs[lo].baseSeq() + idx;
        }
        int a = 0, b = tail.capacity() / R;
        while (a < b) {
            int mid = (a + b) >>> 1;
            if (RecordCodec.timestampAt(tail, mid * R) < ts) a = mid + 1;
            else b = mid;
        }
        return tailBase + a;
    }
}
