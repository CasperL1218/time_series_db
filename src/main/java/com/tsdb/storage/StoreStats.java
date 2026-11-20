package com.tsdb.storage;

import java.nio.file.Path;

/**
 * A point-in-time summary of a DiskStore, taken under its lock.
 *
 * @param dataDir            the store's directory
 * @param segmentCount       segment files, including the active one
 * @param totalRecords       records ever appended (= nextSeq), buffered ones included
 * @param bytesOnDisk        bytes written to segment files (headers + records), excluding the buffer
 * @param bufferedRecords    appended but not yet written to the OS; a process crash loses these
 * @param syncedRecords      records covered by the last fsync; an OS crash can lose the rest
 * @param lastTimestamp      newest timestamp, Long.MIN_VALUE if empty
 * @param recoveredTornBytes bytes open() moved aside from a torn last segment, 0 if none
 */
public record StoreStats(Path dataDir, int segmentCount, long totalRecords, long bytesOnDisk,
                         long bufferedRecords, long syncedRecords, long lastTimestamp, long recoveredTornBytes) {}
