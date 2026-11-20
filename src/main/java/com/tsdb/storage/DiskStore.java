package com.tsdb.storage;

import com.tsdb.DataPoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * An append-only log of trades on disk: every DataPoint gets the next
 * sequence number (0, 1, 2, ...) and is kept in arrival order, which is also
 * time order. Reads are scans: everything, from a sequence number on, or a
 * time range.
 *
 * The windows only remember the last few trades; this keeps all of them.
 * It sits beside the engine, not under it: the shell appends each trade
 * here and, separately, feeds it to the engines.
 *
 * <b>Layout.</b> A directory of segment files (see {@link Segment}), each
 * holding a fixed number of 32-byte records (see {@link RecordCodec}), named
 * by the seq of their first record. The newest one is the active segment;
 * only it is ever written.
 *
 * <b>Writes.</b> {@link #append} encodes into a 64 KB buffer that holds
 * only active-segment records. The buffer is written to the OS when it
 * fills, and written plus fsynced once {@code syncIntervalMs} has passed
 * since the last fsync. There is no background thread, so that check
 * happens inside append: durability is within {@code syncIntervalMs} only
 * while appends keep arriving. Call {@link #sync} when they stop.
 * A process crash loses at most the unflushed buffer; an OS crash or power
 * loss can also lose what was written since the last fsync.
 *
 * <b>Reads.</b> A record is visible to scans as soon as append returns,
 * even while it is only in the buffer. A scan takes a snapshot under the
 * lock (counts, segment list, a copy of the buffer) and then reads without
 * it, so a long scan never blocks the writer.
 *
 * <b>Recovery.</b> {@link #open} repairs only what a crash mid-append
 * explains: a torn tail of the last segment is moved to a {@code .torn-*}
 * file and cut off. Anything else (bad header, gap, wrong length) is a
 * {@link CorruptStoreException}.
 *
 * <b>Concurrency.</b> One lock guards append, flush, sync, snapshots and
 * close; scans run outside it. A {@code .lock} file held with
 * {@link FileChannel#tryLock()} keeps a second process (or a second open
 * in this one) out of the directory.
 *
 * <b>Never call the store from a thread that may be interrupted.</b> Its
 * files are FileChannels, which are interruptible: an interrupt during (or
 * pending at the start of) any read, write or fsync closes that channel for
 * every thread, and the store fails with ClosedChannelException until it is
 * reopened. Stop a writer or reader with a flag instead of
 * Thread.interrupt().
 */
public final class DiskStore implements AutoCloseable {

    private static final int R = RecordCodec.RECORD_SIZE;
    private static final String LOCK_FILE = ".lock";
    private static final Pattern TORN = Pattern.compile("seg-\\d{20}\\.tsd\\.torn-\\d+");

    private final Path dir;
    private final Options options;
    private final long syncIntervalNanos;
    private final FileChannel lockChannel;
    private final FileLock fileLock;

    private final Object lock = new Object();
    // Everything below is guarded by lock.
    private final List<Segment> segments = new ArrayList<>(); // oldest first; the last is active
    private final ByteBuffer buffer;     // encoded records of the active segment not yet written
    private final ByteBuffer record = ByteBuffer.allocate(R).order(ByteOrder.LITTLE_ENDIAN); // encode scratch
    private long nextSeq;                // records ever appended, buffered ones included
    private long writtenSeq;             // records handed to the OS (in a segment file)
    private long syncedSeq;              // records covered by the last fsync
    private long lastTs = Long.MIN_VALUE;
    private long lastSyncNanos;
    private long recoveredTornBytes;
    private boolean closed;

    private DiskStore(Path dir, Options options, FileChannel lockChannel, FileLock fileLock) {
        this.dir = dir;
        this.options = options;
        this.syncIntervalNanos = TimeUnit.MILLISECONDS.toNanos(options.syncIntervalMs);
        this.lockChannel = lockChannel;
        this.fileLock = fileLock;
        this.buffer = ByteBuffer.allocate(options.bufferBytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Opens (creating if needed) the store in {@code dir}, recovering a torn
     * last segment.
     *
     * @throws IOException "data directory in use" if another store has it open
     * @throws CorruptStoreException if the files are damaged beyond a torn tail
     */
    public static DiskStore open(Path dir, Options options) throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(options, "options");
        Files.createDirectories(dir);
        FileChannel lockChannel = FileChannel.open(dir.resolve(LOCK_FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock fileLock;
        try {
            fileLock = lockChannel.tryLock();
        } catch (OverlappingFileLockException e) { // already held by this JVM
            fileLock = null;
        } catch (IOException | RuntimeException e) {
            lockChannel.close();
            throw e;
        }
        if (fileLock == null) {
            lockChannel.close();
            throw new IOException("data directory in use: " + dir);
        }
        DiskStore store = new DiskStore(dir, options, lockChannel, fileLock);
        try {
            store.recover();
        } catch (IOException | RuntimeException e) {
            store.releaseAll();
            throw e;
        }
        return store;
    }

    /** Opens every segment in order, checking they chain, and repairs the last one's tail. */
    private void recover() throws IOException {
        TreeMap<Long, Path> found = new TreeMap<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "seg-*")) {
            for (Path f : files) {
                String name = f.getFileName().toString();
                long base = Segment.baseSeqOf(name);
                if (base >= 0) {
                    found.put(base, f);
                } else if (!TORN.matcher(name).matches()) {
                    throw new CorruptStoreException("unexpected file " + name + " in " + dir
                            + " (looks like a segment but isn't named seg-<20 digits>.tsd)");
                }
            }
        }
        List<Path> paths = new ArrayList<>(found.values());

        // A last file without even a whole header was being created when the process died: set it aside.
        if (!paths.isEmpty()) {
            Path last = paths.get(paths.size() - 1);
            long size = Files.size(last);
            if (size < Segment.HEADER_SIZE) {
                Segment.moveAside(last);
                recoveredTornBytes += size;
                paths.remove(paths.size() - 1);
            }
        }

        long expected = 0;
        long floor = Long.MIN_VALUE;
        for (int i = 0; i < paths.size(); i++) {
            boolean isLast = i == paths.size() - 1;
            Segment s = Segment.open(paths.get(i), expected, isLast, floor);
            segments.add(s);
            recoveredTornBytes += s.recoveredTornBytes();
            expected = s.baseSeq() + s.recordCount();
            if (s.recordCount() > 0) floor = s.lastTimestamp();
        }
        nextSeq = writtenSeq = syncedSeq = expected;
        lastTs = floor;
        if (!segments.isEmpty()) active().force(); // so "synced" is true of what the last run left in the page cache
        lastSyncNanos = System.nanoTime();
    }

    /**
     * Appends {@code p} and returns its sequence number. Readable by scans
     * as soon as this returns; durable per the class comment.
     *
     * @throws IllegalArgumentException if p is older than lastTimestamp(), or its symbol or side is invalid;
     *         nothing is written
     * @throws IOException if writing fails; the record may or may not be kept (it is buffered first)
     */
    public long append(DataPoint p) throws IOException {
        synchronized (lock) {
            ensureOpen();
            long ts = p.getTimestampMillis();
            if (ts < lastTs) {
                throw new IllegalArgumentException("out of order: timestamp " + ts + " is before the last stored " + lastTs);
            }
            record.clear();
            RecordCodec.encode(p, record); // validates before anything changes
            record.flip();

            Segment active = active();
            if (active == null || nextSeq - active.baseSeq() == active.capacity()) {
                if (active != null) syncLocked(); // a segment is complete and durable before the next one exists
                active = Segment.create(dir, nextSeq, options.segmentCapacity);
                segments.add(active);
            }
            if (!buffer.hasRemaining()) flushLocked(); // only if an earlier flush failed
            buffer.put(record);
            long seq = nextSeq++;
            lastTs = ts;

            if (options.syncIntervalMs == 0 || System.nanoTime() - lastSyncNanos >= syncIntervalNanos) {
                syncLocked();
            } else if (!buffer.hasRemaining()) {
                flushLocked();
            }
            return seq;
        }
    }

    /** Writes buffered records to the OS (no fsync): they then survive a process crash. */
    public void flush() throws IOException {
        synchronized (lock) {
            ensureOpen();
            flushLocked();
        }
    }

    /** Flush, then fsync the active segment: everything appended so far survives an OS crash. */
    public void sync() throws IOException {
        synchronized (lock) {
            ensureOpen();
            syncLocked();
        }
    }

    /** Every record, oldest first. */
    public RecordCursor scanAll() {
        return new RecordCursor(snapshot(), 0, Long.MAX_VALUE);
    }

    /**
     * Records from {@code seq} on. {@code seq == nextSeq()} gives an empty
     * cursor whose nextSeq() is seq, so a reader can resume from where an
     * exhausted cursor stopped.
     *
     * @throws IllegalArgumentException unless 0 <= seq <= nextSeq()
     */
    public RecordCursor scanFromSeq(long seq) {
        Snapshot snap = snapshot();
        if (seq < 0 || seq > snap.end) {
            throw new IllegalArgumentException("seq " + seq + " is outside [0, " + snap.end + "]");
        }
        return new RecordCursor(snap, seq, Long.MAX_VALUE);
    }

    /**
     * Records with {@code fromTs <= timestamp <= toTs}, oldest first. Empty
     * if fromTs > toTs. The start is found by binary search, so the cost is
     * the matching records plus a logarithmic seek.
     */
    public RecordCursor scan(long fromTs, long toTs) {
        Snapshot snap = snapshot();
        try {
            return new RecordCursor(snap, snap.lowerBound(fromTs), toTs);
        } catch (IOException e) {
            throw new UncheckedIOException("searching for timestamp " + fromTs, e);
        }
    }

    /** Records ever appended, including buffered ones; also the seq the next append gets. */
    public long nextSeq() {
        synchronized (lock) {
            ensureOpen();
            return nextSeq;
        }
    }

    /** Timestamp of the newest record, or Long.MIN_VALUE if the store is empty. */
    public long lastTimestamp() {
        synchronized (lock) {
            ensureOpen();
            return lastTs;
        }
    }

    public StoreStats stats() {
        synchronized (lock) {
            ensureOpen();
            return new StoreStats(dir, segments.size(), nextSeq,
                    (long) Segment.HEADER_SIZE * segments.size() + (long) R * writtenSeq,
                    nextSeq - writtenSeq, syncedSeq, lastTs, recoveredTornBytes);
        }
    }

    /** Syncs, then closes every file and releases the directory lock. Idempotent. */
    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (closed) return;
            try {
                syncLocked();
            } finally {
                closed = true;
                releaseAll();
            }
        }
    }

    /**
     * For tests: closes everything WITHOUT writing the buffer, as if the
     * process died. What was already written to the OS stays; the buffered
     * records are lost.
     */
    void closeWithoutFlush() throws IOException {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            releaseAll();
        }
    }

    // ---- internals; callers hold lock ----

    private Segment active() {
        return segments.isEmpty() ? null : segments.get(segments.size() - 1);
    }

    private void flushLocked() throws IOException {
        int n = buffer.position() / R;
        if (n == 0) return;
        // Write from a duplicate so a failed write leaves the buffer intact for a retry.
        ByteBuffer out = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).flip();
        active().append(out, n);
        buffer.clear();
        writtenSeq += n;
    }

    private void syncLocked() throws IOException {
        flushLocked();
        if (syncedSeq < writtenSeq) active().force(); // older segments were fsynced when they filled
        syncedSeq = writtenSeq;
        lastSyncNanos = System.nanoTime();
    }

    private Snapshot snapshot() {
        synchronized (lock) {
            ensureOpen();
            int k = segments.size();
            Segment[] segs = segments.toArray(new Segment[0]);
            long[] counts = new long[k];
            long[] last = new long[k];
            for (int i = 0; i < k; i++) {
                counts[i] = segs[i].recordCount();
                last[i] = segs[i].lastTimestamp();
            }
            int bytes = buffer.position();
            ByteBuffer tail = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
            tail.put(0, buffer, 0, bytes);
            return new Snapshot(segs, counts, last, tail, writtenSeq, nextSeq);
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("store is closed: " + dir);
    }

    /** Closes every channel and releases the directory lock, keeping the first error. */
    private void releaseAll() throws IOException {
        IOException first = null;
        for (Segment s : segments) {
            try {
                s.close();
            } catch (IOException e) {
                if (first == null) first = e;
            }
        }
        try {
            fileLock.release();
        } catch (IOException e) {
            if (first == null) first = e;
        }
        try {
            lockChannel.close();
        } catch (IOException e) {
            if (first == null) first = e;
        }
        if (first != null) throw first;
    }

    /**
     * Tuning knobs, immutable. {@code defaults()}: 1,048,576 records per
     * segment (32 MiB), a 64 KB buffer (2,048 records), fsync at most every
     * 1,000 ms. Tests shrink the first two so rolling and flushing happen
     * constantly.
     */
    public static final class Options {
        public static final int DEFAULT_SEGMENT_CAPACITY = 1 << 20;
        public static final int DEFAULT_BUFFER_BYTES = 64 * 1024;
        public static final long DEFAULT_SYNC_INTERVAL_MS = 1000;

        private final int segmentCapacity;
        private final int bufferBytes;
        private final long syncIntervalMs;

        private Options(int segmentCapacity, int bufferBytes, long syncIntervalMs) {
            this.segmentCapacity = segmentCapacity;
            this.bufferBytes = bufferBytes;
            this.syncIntervalMs = syncIntervalMs;
        }

        public static Options defaults() {
            return new Options(DEFAULT_SEGMENT_CAPACITY, DEFAULT_BUFFER_BYTES, DEFAULT_SYNC_INTERVAL_MS);
        }

        /** Records per new segment (existing segments keep the capacity in their header). */
        public Options withSegmentCapacity(int records) {
            if (records <= 0) throw new IllegalArgumentException("segment capacity must be positive, got " + records);
            return new Options(records, bufferBytes, syncIntervalMs);
        }

        /** Write-buffer size; a positive multiple of 32 (whole records). */
        public Options withBufferBytes(int bytes) {
            if (bytes < R || bytes % R != 0) {
                throw new IllegalArgumentException("buffer must be a positive multiple of " + R + " bytes, got " + bytes);
            }
            return new Options(segmentCapacity, bytes, syncIntervalMs);
        }

        /** Longest gap between fsyncs while appends keep arriving; 0 = fsync every append. */
        public Options withSyncIntervalMs(long ms) {
            if (ms < 0) throw new IllegalArgumentException("sync interval can't be negative, got " + ms);
            return new Options(segmentCapacity, bufferBytes, ms);
        }

        public int segmentCapacity() { return segmentCapacity; }
        public int bufferBytes() { return bufferBytes; }
        public long syncIntervalMs() { return syncIntervalMs; }

        @Override
        public String toString() {
            return "Options[segmentCapacity=" + segmentCapacity + ", bufferBytes=" + bufferBytes
                    + ", syncIntervalMs=" + syncIntervalMs + "]";
        }
    }
}
