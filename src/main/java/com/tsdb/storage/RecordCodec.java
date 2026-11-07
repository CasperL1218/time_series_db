package com.tsdb.storage;

import com.tsdb.DataPoint;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Converts one {@link DataPoint} to exactly 32 bytes and back. This is the
 * on-disk record format; see docs/STORAGE_FORMAT.md.
 *
 * <pre>
 *   offset  size  field       encoding
 *   ------  ----  ----------  ------------------------------------------------
 *    0      8     timestamp   int64, epoch milliseconds
 *    8      8     price       IEEE-754 double, raw bits (round-trips exactly)
 *   16      4     quantity    int32
 *   20      1     side        0 = BUY, 1 = SELL
 *   21      7     symbol      ASCII, 1-7 printable characters, zero-padded
 *   28      4     checksum    CRC32C of bytes 0..27
 * </pre>
 *
 * All multi-byte numbers are little-endian, so every buffer passed in must
 * use {@link #BYTE_ORDER}; {@link #allocate(int)} returns one that does.
 *
 * Fixed width is the point of the format: record i of a file starts at
 * {@code header + 32 * i}, so a reader can jump to any record or binary-search
 * by timestamp without an index. The checksum catches torn or damaged records
 * after a crash. A CRC detects every error confined to 32 consecutive bits, so
 * any change to a single byte is always detected.
 *
 * The codec checks only what the format needs (symbol shape, side present).
 * It stores any timestamp, price and quantity exactly as given; deciding
 * what values make sense is the caller's job.
 *
 * Stateless and thread-safe.
 */
public final class RecordCodec {

    /** Bytes per record. A power of two, so records never straddle a 4 KB disk page. */
    public static final int RECORD_SIZE = 32;

    /** Longest symbol the format can hold (covers all US tickers, e.g. BRK.B). */
    public static final int MAX_SYMBOL_LENGTH = 7;

    /** Byte order of every multi-byte field. Native order on x86 and Apple/ARM chips. */
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    // Field offsets within a record.
    static final int TIMESTAMP_OFFSET = 0;
    static final int PRICE_OFFSET = 8;
    static final int QUANTITY_OFFSET = 16;
    static final int SIDE_OFFSET = 20;
    static final int SYMBOL_OFFSET = 21;
    static final int CHECKSUM_OFFSET = 28; // also the number of bytes the checksum covers

    private static final byte SIDE_BUY = 0;
    private static final byte SIDE_SELL = 1;

    private RecordCodec() {}

    /** A zeroed, little-endian buffer with room for {@code records} records. */
    public static ByteBuffer allocate(int records) {
        if (records < 0) throw new IllegalArgumentException("records can't be negative, got " + records);
        return ByteBuffer.allocate(Math.multiplyExact(records, RECORD_SIZE)).order(BYTE_ORDER);
    }

    /**
     * Writes {@code p} as 32 bytes at {@code out.position()} and advances the
     * position by 32. Nothing outside those 32 bytes is touched.
     *
     * @throws IllegalArgumentException if the symbol isn't 1-7 printable ASCII
     *         characters, the side is missing, or {@code out} isn't little-endian
     * @throws BufferOverflowException if fewer than 32 bytes remain; the
     *         buffer is left unchanged
     */
    public static void encode(DataPoint p, ByteBuffer out) {
        if (p == null) throw new IllegalArgumentException("trade must not be null");
        requireByteOrder(out);
        String symbol = p.getSymbol();
        String problem = symbolProblem(symbol);
        if (problem != null) throw new IllegalArgumentException(problem);
        byte side = sideCode(p.getSide());
        if (out.remaining() < RECORD_SIZE) throw new BufferOverflowException();

        // Absolute puts: write by offset, then move the position once at the end.
        int off = out.position();
        out.putLong(off + TIMESTAMP_OFFSET, p.getTimestampMillis());
        out.putLong(off + PRICE_OFFSET, Double.doubleToRawLongBits(p.getPrice()));
        out.putInt(off + QUANTITY_OFFSET, p.getQuantity());
        out.put(off + SIDE_OFFSET, side);
        for (int i = 0; i < MAX_SYMBOL_LENGTH; i++) {
            out.put(off + SYMBOL_OFFSET + i, i < symbol.length() ? (byte) symbol.charAt(i) : 0);
        }
        out.putInt(off + CHECKSUM_OFFSET, checksum(out, off));
        out.position(off + RECORD_SIZE);
    }

    /**
     * Reads the record starting at absolute offset {@code off}. The buffer's
     * position is not changed, so a reader can decode records in any order.
     *
     * @throws CorruptRecordException if the checksum doesn't match or a field
     *         holds a value the format never writes
     * @throws IndexOutOfBoundsException if the record doesn't fit before the buffer's limit
     */
    public static DataPoint decode(ByteBuffer in, int off) {
        requireByteOrder(in);
        Objects.checkFromIndexSize(off, RECORD_SIZE, in.limit());
        String problem = recordProblem(in, off);
        if (problem != null) throw new CorruptRecordException(off, problem);

        long timestamp = in.getLong(off + TIMESTAMP_OFFSET);
        double price = Double.longBitsToDouble(in.getLong(off + PRICE_OFFSET));
        int quantity = in.getInt(off + QUANTITY_OFFSET);
        DataPoint.Side side = in.get(off + SIDE_OFFSET) == SIDE_BUY ? DataPoint.Side.BUY : DataPoint.Side.SELL;
        return new DataPoint(timestamp, readSymbol(in, off), price, quantity, side);
    }

    /**
     * Just the timestamp of the record at {@code off}, without checking the
     * checksum or building a DataPoint. Meant for binary search over records
     * that recovery has already validated.
     */
    public static long timestampAt(ByteBuffer in, int off) {
        requireByteOrder(in);
        Objects.checkFromIndexSize(off, RECORD_SIZE, in.limit());
        return in.getLong(off + TIMESTAMP_OFFSET);
    }

    /**
     * True if the record at {@code off} is complete and undamaged: the checksum
     * matches and every field holds a value encode() could have written.
     * Used by crash recovery to find where the valid data ends.
     */
    public static boolean isValid(ByteBuffer in, int off) {
        requireByteOrder(in);
        Objects.checkFromIndexSize(off, RECORD_SIZE, in.limit());
        return recordProblem(in, off) == null;
    }

    // ---- helpers ----

    /** CRC32C of the 28 data bytes, read by absolute index (position is ignored). */
    private static int checksum(ByteBuffer buf, int off) {
        CRC32C crc = new CRC32C();
        crc.update(buf.slice(off, CHECKSUM_OFFSET));
        return (int) crc.getValue();
    }

    /** Why the record at off is invalid, or null if it is valid. Checksum first: it's the likely failure. */
    private static String recordProblem(ByteBuffer in, int off) {
        int stored = in.getInt(off + CHECKSUM_OFFSET);
        int actual = checksum(in, off);
        if (stored != actual) {
            return String.format("checksum mismatch (stored %08x, computed %08x)", stored, actual);
        }
        byte side = in.get(off + SIDE_OFFSET);
        if (side != SIDE_BUY && side != SIDE_SELL) return "side byte is " + side + ", expected 0 or 1";

        // Symbol: 1-7 printable characters, then only zero bytes.
        int len = symbolLength(in, off);
        if (len == 0) return "symbol is empty";
        for (int i = 0; i < MAX_SYMBOL_LENGTH; i++) {
            byte b = in.get(off + SYMBOL_OFFSET + i);
            if (i < len ? !isSymbolChar(b) : b != 0) return "symbol bytes are malformed";
        }
        return null;
    }

    /** Number of bytes before the first zero byte in the symbol field (7 if there is none). */
    private static int symbolLength(ByteBuffer in, int off) {
        int len = 0;
        while (len < MAX_SYMBOL_LENGTH && in.get(off + SYMBOL_OFFSET + len) != 0) len++;
        return len;
    }

    private static String readSymbol(ByteBuffer in, int off) {
        byte[] bytes = new byte[symbolLength(in, off)];
        in.get(off + SYMBOL_OFFSET, bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    /** Why a symbol can't be stored, or null if it can. */
    private static String symbolProblem(String symbol) {
        if (symbol == null || symbol.isEmpty()) return "symbol must not be empty";
        if (symbol.length() > MAX_SYMBOL_LENGTH) {
            return "symbol '" + symbol + "' is longer than " + MAX_SYMBOL_LENGTH + " characters";
        }
        for (int i = 0; i < symbol.length(); i++) {
            char c = symbol.charAt(i);
            if (c > 0x7E || !isSymbolChar((byte) c)) {
                return "symbol '" + symbol + "' may only contain printable ASCII characters without spaces";
            }
        }
        return null;
    }

    /** Printable ASCII, excluding space: '!' (0x21) through '~' (0x7E). */
    private static boolean isSymbolChar(byte b) {
        return b >= 0x21 && b <= 0x7E;
    }

    private static byte sideCode(DataPoint.Side side) {
        if (side == null) throw new IllegalArgumentException("side must not be null");
        switch (side) {
            case BUY:  return SIDE_BUY;
            case SELL: return SIDE_SELL;
            default: throw new AssertionError(side);
        }
    }

    private static void requireByteOrder(ByteBuffer buf) {
        if (buf == null) throw new IllegalArgumentException("buffer must not be null");
        if (buf.order() != BYTE_ORDER) {
            throw new IllegalArgumentException("buffer must be little-endian:"
                    + " use RecordCodec.allocate(n) or buffer.order(RecordCodec.BYTE_ORDER)");
        }
    }
}
