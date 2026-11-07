package com.tsdb.storage;

/**
 * Thrown when stored bytes don't form a valid record: the checksum doesn't
 * match, or a field holds a value the format never writes. After a crash this
 * usually means a torn write at the end of a file.
 *
 * Unchecked, because callers on the normal read path can't recover from it;
 * crash recovery calls {@link RecordCodec#isValid} instead of catching it.
 */
public class CorruptRecordException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int offset;

    public CorruptRecordException(int offset, String problem) {
        super("corrupt record at byte offset " + offset + ": " + problem);
        this.offset = offset;
    }

    /** Byte offset of the bad record within the buffer it was read from. */
    public int getOffset() { return offset; }
}
