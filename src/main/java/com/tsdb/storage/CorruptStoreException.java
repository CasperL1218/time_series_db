package com.tsdb.storage;

import java.io.IOException;

/**
 * The data directory is damaged in a way recovery won't repair on its own:
 * a bad segment header, a gap in the sequence numbers, a full segment of
 * the wrong length, a stray file that looks like a segment.
 *
 * Only a torn tail of the last segment is repaired automatically, because a
 * crash mid-append explains it. Anything else would mean guessing which
 * bytes are real, so open() refuses and leaves the files for a person to
 * inspect.
 */
public final class CorruptStoreException extends IOException {

    private static final long serialVersionUID = 1L;

    public CorruptStoreException(String message) {
        super(message);
    }
}
