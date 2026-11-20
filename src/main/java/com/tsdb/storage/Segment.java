package com.tsdb.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;

/**
 * One file of the log, holding up to {@code capacity} consecutive records.
 * <pre>
 *   seg-&lt;20-digit baseSeq&gt;.tsd
 *   header (32 bytes, little-endian):
 *     0..3  magic "TSDB"      4..5  int16 version = 1     6..7  int16 recordSize = 32
 *     8..15 int64 baseSeq     16..19 int32 capacity       20..27 reserved zeros
 *     28..31 CRC32C of bytes 0..27
 *   record i at byte 32 + 32*i, sequence number baseSeq + i
 * </pre>
 * The header is the same size as a record, so record offsets stay a plain
 * multiplication. The sequence number isn't stored per record: it follows
 * from the position, which is why a segment must never have a hole.
 *
 * Only the last segment of a store can be partly written, so only it is
 * scanned and repaired on open. Older segments are checked by header and
 * length, and only their first and last records are read.
 *
 * All I/O is positional ({@code read(buf, pos)} / {@code write(buf, pos)}),
 * never memory-mapped, so readers on other threads can share the channel
 * with the writer and a file can always be truncated or deleted (a mapped
 * file can't be, on Windows). Not thread-safe for writers: DiskStore calls
 * append and force under its lock. Readers must stay below a record count
 * they got from that lock, since bytes below it never change.
 */
final class Segment {

    static final int HEADER_SIZE = 32;
    static final short VERSION = 1;
    private static final int RECORD = RecordCodec.RECORD_SIZE;
    private static final byte[] MAGIC = {'T', 'S', 'D', 'B'};
    private static final Pattern NAME = Pattern.compile("seg-(\\d{20})\\.tsd");
    private static final int SCAN_CHUNK = 64 * 1024;

    private final Path path;
    private final FileChannel channel;
    private final long baseSeq;
    private final int capacity;
    private long count;               // records in the file; only DiskStore's writer changes it
    private long firstTs = Long.MIN_VALUE, lastTs = Long.MIN_VALUE;
    private long recoveredTornBytes;
    private Path tornFile;

    private Segment(Path path, FileChannel channel, long baseSeq, int capacity, long count) {
        this.path = path;
        this.channel = channel;
        this.baseSeq = baseSeq;
        this.capacity = capacity;
        this.count = count;
    }

    /** "seg-00000000000000001000.tsd" for baseSeq 1000. */
    static String fileName(long baseSeq) {
        return String.format("seg-%020d.tsd", baseSeq);
    }

    /** The baseSeq in a segment file name, or -1 if the name isn't one. */
    static long baseSeqOf(String fileName) {
        Matcher m = NAME.matcher(fileName);
        if (!m.matches()) return -1;
        try {
            return Long.parseLong(m.group(1));
        } catch (NumberFormatException e) { // 20 digits can exceed Long.MAX_VALUE
            return -1;
        }
    }

    /** Creates {@code dir/seg-<baseSeq>.tsd} holding just the header. Fails if the file exists. */
    static Segment create(Path dir, long baseSeq, int capacity) throws IOException {
        if (baseSeq < 0) throw new IllegalArgumentException("baseSeq must be >= 0, got " + baseSeq);
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive, got " + capacity);
        Path path = dir.resolve(fileName(baseSeq));
        FileChannel ch = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            writeFully(ch, header(baseSeq, capacity), 0);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
        syncDirectory(dir);
        return new Segment(path, ch, baseSeq, capacity, 0);
    }

    /** Opens an existing segment with no timestamp floor; see {@link #open(Path, long, boolean, long)}. */
    static Segment open(Path path, long expectedBaseSeq, boolean isLast) throws IOException {
        return open(path, expectedBaseSeq, isLast, Long.MIN_VALUE);
    }

    /**
     * Opens an existing segment, checking that its header names
     * {@code expectedBaseSeq} (and agrees with the file name).
     *
     * Not last: the file must be exactly full ({@code 32 + 32*capacity}
     * bytes) and its first and last records valid, with the first at or
     * after {@code floorTs} (the previous segment's last timestamp).
     *
     * Last: every record is checked (CRC, side, timestamps nondecreasing
     * from {@code floorTs}). From the first bad or partial record on, the
     * bytes are copied to {@code <name>.torn-<epochMillis>} and the file is
     * truncated to the good records before them.
     *
     * @throws CorruptStoreException for anything recovery won't repair
     */
    static Segment open(Path path, long expectedBaseSeq, boolean isLast, long floorTs) throws IOException {
        String name = path.getFileName().toString();
        long nameBase = baseSeqOf(name);
        if (nameBase < 0) throw new CorruptStoreException(name + ": not a segment file name");

        FileChannel ch = FileChannel.open(path, isLast
                ? new StandardOpenOption[] {StandardOpenOption.READ, StandardOpenOption.WRITE}
                : new StandardOpenOption[] {StandardOpenOption.READ});
        try {
            long size = ch.size();
            if (size < HEADER_SIZE) {
                throw new CorruptStoreException(name + ": " + size + " byte(s), shorter than the 32-byte header");
            }
            ByteBuffer h = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            readFully(ch, h, 0);
            int capacity = checkHeader(name, h, nameBase, expectedBaseSeq);
            Segment s = new Segment(path, ch, nameBase, capacity, 0);
            if (isLast) {
                s.recover(size, floorTs);
            } else {
                s.checkFull(size, floorTs);
            }
            return s;
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /** Validates the header; returns its capacity. */
    private static int checkHeader(String name, ByteBuffer h, long nameBase, long expectedBaseSeq)
            throws CorruptStoreException {
        for (int i = 0; i < MAGIC.length; i++) {
            if (h.get(i) != MAGIC[i]) throw new CorruptStoreException(name + ": bad magic, not a TSDB segment");
        }
        if (h.getShort(4) != VERSION) {
            throw new CorruptStoreException(name + ": unsupported version " + h.getShort(4) + " (expected " + VERSION + ")");
        }
        if (h.getShort(6) != RECORD) {
            throw new CorruptStoreException(name + ": record size " + h.getShort(6) + " (expected " + RECORD + ")");
        }
        if (Integer.toUnsignedLong(h.getInt(28)) != headerCrc(h)) {
            throw new CorruptStoreException(name + ": header CRC mismatch");
        }
        long base = h.getLong(8);
        int capacity = h.getInt(16);
        if (capacity <= 0) throw new CorruptStoreException(name + ": capacity " + capacity + " in header");
        if (base != nameBase) {
            throw new CorruptStoreException(name + ": header says baseSeq " + base + " but the file name says " + nameBase);
        }
        if (base != expectedBaseSeq) {
            throw new CorruptStoreException(name + ": baseSeq " + base + " but the previous segments end at seq "
                    + expectedBaseSeq + " (a segment is missing or renamed)");
        }
        return capacity;
    }

    /** A non-last segment: exactly full, with valid first and last records in order. */
    private void checkFull(long size, long floorTs) throws IOException {
        String name = path.getFileName().toString();
        long expected = HEADER_SIZE + (long) RECORD * capacity;
        if (size != expected) {
            throw new CorruptStoreException(name + ": " + size + " bytes, but a full segment of " + capacity
                    + " record(s) is " + expected + " (only the last segment may be partial)");
        }
        count = capacity;
        ByteBuffer r = ByteBuffer.allocate(RECORD).order(ByteOrder.LITTLE_ENDIAN);
        firstTs = checkedTimestamp(r, 0);
        lastTs = checkedTimestamp(r, count - 1);
        if (firstTs < floorTs) {
            throw new CorruptStoreException(name + ": first timestamp " + firstTs
                    + " is before the previous segment's last " + floorTs);
        }
        if (lastTs < firstTs) {
            throw new CorruptStoreException(name + ": last timestamp " + lastTs + " is before its first " + firstTs);
        }
    }

    private long checkedTimestamp(ByteBuffer r, long idx) throws IOException {
        r.clear();
        read(idx, r);
        if (!RecordCodec.isValid(r, 0)) {
            throw new CorruptStoreException(path.getFileName() + ": record " + idx + " (seq " + (baseSeq + idx)
                    + ") fails its CRC or side check");
        }
        return RecordCodec.timestampAt(r, 0);
    }

    /** The last segment: keep the longest valid prefix, move the rest aside, truncate. */
    private void recover(long size, long floorTs) throws IOException {
        long available = Math.min((size - HEADER_SIZE) / RECORD, capacity); // whole records within capacity
        ByteBuffer chunk = ByteBuffer.allocate(SCAN_CHUNK).order(ByteOrder.LITTLE_ENDIAN);
        long prev = floorTs;
        long good = 0;
        scan:
        while (good < available) {
            int n = (int) Math.min(available - good, SCAN_CHUNK / RECORD);
            chunk.clear().limit(n * RECORD);
            read(good, chunk);
            for (int i = 0; i < n; i++) {
                int off = i * RECORD;
                long ts = RecordCodec.timestampAt(chunk, off);
                if (!RecordCodec.isValid(chunk, off) || ts < prev) break scan;
                if (good == 0) firstTs = ts;
                prev = ts;
                lastTs = ts;
                good++;
            }
        }
        count = good;

        long keep = HEADER_SIZE + good * RECORD;
        if (keep < size) {
            tornFile = moveTail(keep, size);
            channel.truncate(keep);
            channel.force(true);
            recoveredTornBytes = size - keep;
        }
    }

    /** Copies bytes [from, to) into a new .torn-* file next to this one and fsyncs it. */
    private Path moveTail(long from, long to) throws IOException {
        Path torn = newTornPath(path);
        try (FileChannel out = FileChannel.open(torn, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(SCAN_CHUNK);
            long pos = from;
            while (pos < to) {
                buf.clear().limit((int) Math.min(SCAN_CHUNK, to - pos));
                readFully(channel, buf, pos);
                buf.flip();
                writeFully(out, buf, pos - from);
                pos += buf.limit();
            }
            out.force(true);
        }
        syncDirectory(torn.getParent());
        return torn;
    }

    /** {@code <name>.torn-<epochMillis>} that doesn't exist yet (bumping the millis on a clash). */
    static Path newTornPath(Path file) throws IOException {
        long millis = System.currentTimeMillis();
        while (true) {
            Path p = file.resolveSibling(file.getFileName() + ".torn-" + millis);
            if (!Files.exists(p)) return p;
            millis++;
        }
    }

    /** Moves a whole file aside as {@code <name>.torn-<epochMillis>}; returns where it went. */
    static Path moveAside(Path file) throws IOException {
        while (true) {
            Path torn = newTornPath(file);
            try {
                Path moved = Files.move(file, torn);
                syncDirectory(file.getParent());
                return moved;
            } catch (FileAlreadyExistsException raced) {
                // someone took that name between the check and the move; try the next one
            }
        }
    }

    long baseSeq() { return baseSeq; }
    int capacity() { return capacity; }
    Path path() { return path; }
    long recordCount() { return count; }
    boolean isFull() { return count == capacity; }
    /** Timestamp of record 0; only meaningful when recordCount() > 0. */
    long firstTimestamp() { return firstTs; }
    /** Timestamp of the last record written; only meaningful when recordCount() > 0. */
    long lastTimestamp() { return lastTs; }
    /** Bytes open() dropped from the end of this (last) segment, 0 if none. */
    long recoveredTornBytes() { return recoveredTornBytes; }
    /** Where open() put the dropped bytes, or null. */
    Path tornFile() { return tornFile; }

    /**
     * Writes {@code records} encoded records from {@code src} (position to
     * limit) after the existing ones. Caller holds DiskStore's lock.
     */
    void append(ByteBuffer src, int records) throws IOException {
        if (src.remaining() != records * RECORD) {
            throw new IllegalArgumentException(src.remaining() + " bytes is not " + records + " record(s)");
        }
        if (records <= 0) return;
        if (count + records > capacity) {
            throw new IllegalStateException("segment " + baseSeq + " holds " + count + " of " + capacity
                    + " record(s), can't add " + records);
        }
        int start = src.position();
        long newFirst = RecordCodec.timestampAt(src, start);
        long newLast = RecordCodec.timestampAt(src, start + (records - 1) * RECORD);
        writeFully(channel, src, HEADER_SIZE + count * RECORD);
        if (count == 0) firstTs = newFirst;
        lastTs = newLast;
        count += records;
    }

    /** Reads {@code dst.remaining()} bytes starting at record {@code idx}, filling dst. */
    void read(long idx, ByteBuffer dst) throws IOException {
        if (idx < 0 || idx * RECORD + dst.remaining() > (long) capacity * RECORD) {
            throw new IndexOutOfBoundsException("record " + idx + " (+" + dst.remaining() + " bytes) is outside segment "
                    + baseSeq + " of capacity " + capacity);
        }
        readFully(channel, dst, HEADER_SIZE + idx * RECORD);
    }

    /**
     * Index of the first record in [0, limit) whose timestamp is >= ts, or
     * limit if there is none. Binary search on the raw timestamps (no CRC
     * check); limit must not exceed a record count the caller already saw.
     */
    long lowerBound(long ts, long limit) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        long lo = 0, hi = limit;
        while (lo < hi) {
            long mid = (lo + hi) >>> 1;
            b.clear();
            readFully(channel, b, HEADER_SIZE + mid * RECORD);
            if (b.getLong(0) < ts) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** fsync the record bytes (force(false): the file length is implied by the data written). */
    void force() throws IOException {
        channel.force(false);
    }

    void close() throws IOException {
        channel.close();
    }

    // ---- helpers ----

    /**
     * Best-effort fsync of a directory, so a newly created or renamed file's
     * directory entry survives a power loss along with its fsynced data.
     * Windows can't open a directory as a FileChannel, so any IOException is
     * ignored: there, NTFS journals the entry itself.
     */
    static void syncDirectory(Path dir) {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException ignored) {
            // not supported on this platform/filesystem; nothing more we can do
        }
    }

    static ByteBuffer header(long baseSeq, int capacity) {
        ByteBuffer h = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        h.put(MAGIC).putShort(VERSION).putShort((short) RECORD).putLong(baseSeq).putInt(capacity);
        // bytes 20..27 stay zero (reserved)
        h.putInt(28, (int) headerCrc(h));
        h.clear();
        return h;
    }

    private static long headerCrc(ByteBuffer h) {
        CRC32C c = new CRC32C();
        c.update(h.slice(0, 28));
        return c.getValue();
    }

    private static void readFully(FileChannel ch, ByteBuffer dst, long pos) throws IOException {
        while (dst.hasRemaining()) {
            int n = ch.read(dst, pos);
            if (n < 0) throw new EOFException("unexpected end of file at byte " + pos);
            pos += n;
        }
    }

    private static void writeFully(FileChannel ch, ByteBuffer src, long pos) throws IOException {
        while (src.hasRemaining()) {
            pos += ch.write(src, pos);
        }
    }
}
