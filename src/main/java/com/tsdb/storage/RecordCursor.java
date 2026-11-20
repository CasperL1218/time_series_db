package com.tsdb.storage;

import com.tsdb.DataPoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterates stored records in sequence order, oldest first, over the
 * snapshot taken when the scan started: later appends are not seen, and no
 * record is returned twice or skipped.
 *
 * Reads the files in 64 KB chunks (2,048 records), so memory stays constant
 * however long the scan. Runs entirely outside the store's lock; the
 * writer keeps appending meanwhile.
 *
 * {@link #nextSeq()} is the seq the next {@code next()} would return. Once
 * the cursor is exhausted, {@code store.scanFromSeq(cursor.nextSeq())}
 * picks up exactly the records appended since, which is how a later
 * "tail the log" reader will resume.
 *
 * Iterator methods can't throw IOException, so I/O errors surface as
 * UncheckedIOException (including ClosedChannelException if the store is
 * closed mid-scan), and a damaged record as CorruptRecordException. Not
 * thread-safe: one cursor per reader.
 */
public final class RecordCursor implements Iterator<DataPoint>, AutoCloseable {

    private static final int R = RecordCodec.RECORD_SIZE;
    private static final int CHUNK_RECORDS = 64 * 1024 / R;

    private final Snapshot snap;
    private final long toTs;     // stop at the first record after this (Long.MAX_VALUE: no bound)
    private long seq;            // the next record to return
    private DataPoint peeked;    // record seq, already read by hasNext()
    private boolean done;

    private ByteBuffer chunk;    // on-disk records [chunkBase, chunkBase + chunkRecords)
    private long chunkBase;
    private int chunkRecords;

    RecordCursor(Snapshot snap, long startSeq, long toTs) {
        this.snap = snap;
        this.seq = startSeq;
        this.toTs = toTs;
    }

    @Override
    public boolean hasNext() {
        if (peeked != null) return true;
        if (done) return false;
        if (seq >= snap.end) {
            done = true;
            return false;
        }
        DataPoint p;
        try {
            p = load(seq);
        } catch (IOException e) {
            throw new UncheckedIOException("reading seq " + seq, e);
        }
        if (p.getTimestampMillis() > toTs) {
            done = true;
            return false;
        }
        peeked = p;
        return true;
    }

    @Override
    public DataPoint next() {
        if (!hasNext()) throw new NoSuchElementException();
        DataPoint p = peeked;
        peeked = null;
        seq++;
        return p;
    }

    /** The seq of the record next() would return; once exhausted, where a resumed scan should start. */
    public long nextSeq() {
        return seq;
    }

    /** Ends the scan and drops its read buffer. The store itself stays open. */
    @Override
    public void close() {
        done = true;
        peeked = null;
        chunk = null;
    }

    private DataPoint load(long s) throws IOException {
        if (s >= snap.tailBase) {
            return RecordCodec.decode(snap.tail, (int) ((s - snap.tailBase) * R));
        }
        if (chunk == null || s < chunkBase || s >= chunkBase + chunkRecords) fill(s);
        return RecordCodec.decode(chunk, (int) ((s - chunkBase) * R));
    }

    /** Reads the next chunk, starting at on-disk record s and staying within its segment. */
    private void fill(long s) throws IOException {
        if (chunk == null) chunk = ByteBuffer.allocate(CHUNK_RECORDS * R).order(ByteOrder.LITTLE_ENDIAN);
        int i = snap.segmentOf(s);
        Segment seg = snap.segs[i];
        long idx = s - seg.baseSeq();
        int n = (int) Math.min(CHUNK_RECORDS, snap.counts[i] - idx);
        chunk.clear().limit(n * R);
        seg.read(idx, chunk);
        chunkBase = s;
        chunkRecords = n;
    }
}
