package com.tsdb.storage;

/**
 * A 32-byte record failed its integrity check: the CRC doesn't match or the
 * side byte is neither BUY nor SELL.
 *
 * Unchecked because it is raised while decoding a buffer, deep inside
 * iteration. A record that was fsynced and later fails here means the disk
 * or another process damaged the file, not that the caller can retry.
 */
public final class CorruptRecordException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CorruptRecordException(String message) {
        super(message);
    }
}
