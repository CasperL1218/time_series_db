package com.tsdb.storage;

import com.tsdb.DataPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;

/**
 * The on-disk form of one trade: a fixed 32-byte, little-endian record.
 * <pre>
 *   offset size field
 *        0    8 timestampMillis (int64)
 *        8    8 price, as Double.doubleToRawLongBits, so it round-trips bit for bit
 *       16    4 quantity (int32)
 *       20    1 side: 0 = BUY, 1 = SELL, anything else is corrupt
 *       21    7 symbol: 1..7 printable ASCII chars (0x21..0x7E), zero-padded
 *       28    4 CRC32C of bytes 0..27, as an unsigned 32-bit value
 * </pre>
 * A fixed size is what makes the rest of storage simple: record i of a
 * segment sits at a computable offset, so a sequence number needs no index
 * and a time lookup is a binary search over the file.
 *
 * Every method uses absolute get/put at an explicit offset, so it never
 * moves the buffer's position except where documented (encode). Pure: no
 * I/O, and no state beyond a per-thread CRC32C that is reset on every use.
 */
public final class RecordCodec {

    public static final int RECORD_SIZE = 32, MAX_SYMBOL_LEN = 7;

    private static final int TS = 0, PRICE = 8, QTY = 16, SIDE = 20, SYMBOL = 21, CRC = 28;
    private static final byte BUY = 0, SELL = 1;

    // CRC32C is not thread-safe; one per thread keeps isValid allocation-free after warm-up.
    private static final ThreadLocal<CRC32C> CRC32 = ThreadLocal.withInitial(CRC32C::new);

    private RecordCodec() {}

    /**
     * Writes {@code p} at {@code out.position()} and advances it by 32.
     * Validates everything first, so a rejected point writes nothing.
     *
     * @throws IllegalArgumentException for a bad symbol, a null side, or a buffer that isn't LITTLE_ENDIAN
     * @throws java.nio.BufferOverflowException if fewer than 32 bytes remain
     */
    public static void encode(DataPoint p, ByteBuffer out) {
        requireLittleEndian(out);
        String symbol = p.getSymbol();
        checkSymbol(symbol);
        if (p.getSide() == null) throw new IllegalArgumentException("side is null");
        if (out.remaining() < RECORD_SIZE) throw new java.nio.BufferOverflowException();

        int off = out.position();
        out.putLong(off + TS, p.getTimestampMillis());
        out.putLong(off + PRICE, Double.doubleToRawLongBits(p.getPrice()));
        out.putInt(off + QTY, p.getQuantity());
        out.put(off + SIDE, p.getSide() == DataPoint.Side.BUY ? BUY : SELL);
        for (int i = 0; i < MAX_SYMBOL_LEN; i++) {
            out.put(off + SYMBOL + i, i < symbol.length() ? (byte) symbol.charAt(i) : 0);
        }
        out.putInt(off + CRC, (int) crc(out, off));
        out.position(off + RECORD_SIZE);
    }

    /**
     * Reads the record at {@code off} without moving the position.
     *
     * @throws CorruptRecordException if the CRC or the side byte is wrong
     */
    public static DataPoint decode(ByteBuffer in, int off) {
        requireLittleEndian(in);
        long stored = Integer.toUnsignedLong(in.getInt(off + CRC));
        long actual = crc(in, off);
        if (stored != actual) {
            throw new CorruptRecordException(String.format(
                    "CRC mismatch at buffer offset %d: stored %08x, computed %08x", off, stored, actual));
        }
        byte side = in.get(off + SIDE);
        if (side != BUY && side != SELL) {
            throw new CorruptRecordException("bad side byte " + side + " at buffer offset " + off);
        }
        int len = 0;
        while (len < MAX_SYMBOL_LEN && in.get(off + SYMBOL + len) != 0) len++;
        byte[] sym = new byte[len];
        for (int i = 0; i < len; i++) sym[i] = in.get(off + SYMBOL + i);

        return new DataPoint(in.getLong(off + TS),
                new String(sym, StandardCharsets.US_ASCII),
                Double.longBitsToDouble(in.getLong(off + PRICE)),
                in.getInt(off + QTY),
                side == BUY ? DataPoint.Side.BUY : DataPoint.Side.SELL);
    }

    /** The timestamp of the record at {@code off}, without a CRC check: for binary search. */
    public static long timestampAt(ByteBuffer in, int off) {
        requireLittleEndian(in);
        return in.getLong(off + TS);
    }

    /** Whether the record at {@code off} passes the CRC and side checks. Allocates nothing for heap buffers. */
    public static boolean isValid(ByteBuffer in, int off) {
        requireLittleEndian(in);
        byte side = in.get(off + SIDE);
        return (side == BUY || side == SELL)
                && Integer.toUnsignedLong(in.getInt(off + CRC)) == crc(in, off);
    }

    /** CRC32C of bytes off..off+27. Heap buffers go through the backing array to avoid a slice object. */
    private static long crc(ByteBuffer b, int off) {
        CRC32C c = CRC32.get();
        c.reset();
        if (b.hasArray()) {
            c.update(b.array(), b.arrayOffset() + off, CRC);
        } else {
            c.update(b.slice(off, CRC));
        }
        return c.getValue();
    }

    private static void checkSymbol(String s) {
        if (s == null || s.isEmpty() || s.length() > MAX_SYMBOL_LEN) {
            throw new IllegalArgumentException("symbol must be 1 to " + MAX_SYMBOL_LEN + " characters, got "
                    + (s == null ? "null" : "'" + s + "'"));
        }
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch < 0x21 || ch > 0x7E) {
                throw new IllegalArgumentException(String.format(
                        "symbol must be printable ASCII without spaces, got '%s' (char 0x%02x at %d)",
                        s.replaceAll("[^\\x21-\\x7E]", "?"), (int) ch, i));
            }
        }
    }

    private static void requireLittleEndian(ByteBuffer b) {
        if (b.order() != ByteOrder.LITTLE_ENDIAN) {
            throw new IllegalArgumentException("buffer must be ByteOrder.LITTLE_ENDIAN");
        }
    }
}
