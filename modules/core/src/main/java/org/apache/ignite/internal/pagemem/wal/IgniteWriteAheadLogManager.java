/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagemem.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface IgniteWriteAheadLogManager extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * @return {@code true} If we have to always write full pages.
     */
    public boolean isAlwaysWriteFullPages();

    /**
     * @return {@code true} if WAL will perform fair syncs on fsync call.
     */
    public boolean isFullSync();

    /**
     * @return Current serializer version.
     */
    public int serializerVersion();

    /**
     * Resumes logging after start. When WAL manager is started, it will skip logging any updates until this
     * method is called to avoid logging changes induced by the state restore procedure.
     *
     * @throws IgniteCheckedException If fails.
     */
    public void resumeLogging(WALPointer lastWrittenPtr) throws IgniteCheckedException;

    /**
     * Appends the given log entry to the write-ahead log.
     *
     * @param entry Entry to log.
     * @return WALPointer that may be passed to {@link #flush(WALPointer, boolean)} method to make sure the record is
     *      written to the log.
     * @throws IgniteCheckedException If failed to construct log entry.
     * @throws StorageException If IO error occurred while writing log entry.
     */
    public WALPointer log(WALRecord entry) throws IgniteCheckedException, StorageException;

    /**
     * Appends the given log entry to the write-ahead log. If entry logging leads to rollover, caller can specify
     * whether to write the entry to the current segment or to th next one.
     *
     * @param entry Entry to log.
     * @param rolloverType Rollover type.
     * @return WALPointer that may be passed to {@link #flush(WALPointer, boolean)} method to make sure the record is
     *      written to the log.
     * @throws IgniteCheckedException If failed to construct log entry.
     * @throws StorageException If IO error occurred while writing log entry.
     *
     * @see RolloverType
     */
    public WALPointer log(WALRecord entry, RolloverType rolloverType)
        throws IgniteCheckedException, StorageException;

    /**
     * Makes sure that all log entries written to the log up until the specified pointer are actually written
     * to the underlying storage.
     *
     * @param ptr Optional pointer to write. If {@code null}, will sync up to the latest record.
     * @param explicitFsync If true, data will be synced to the storage device on hardware level.
     * @throws IgniteCheckedException If failed to write.
     * @throws StorageException If IO exception occurred during the write. If an exception is thrown from this
     *      method, the WAL will be invalidated and the node will be stopped.
     * @return Last WAL position which was flushed to WAL segment file. May be greater than or equal to a {@code ptr}.
     * May be {@code null}, it means nothing has been flushed.
     */
    public WALPointer flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException;

    /**
     * Reads WAL record by the specified pointer.
     *
     * @param ptr WAL pointer.
     * @return WAL record.
     * @throws IgniteCheckedException If failed to read.
     * @throws StorageException If IO error occurred while reading WAL entries.
     */
    public WALRecord read(WALPointer ptr) throws IgniteCheckedException, StorageException;

    /**
     * Invoke this method to iterate over the written log entries.
     *
     * @param start Optional WAL pointer from which to start iteration.
     * @return Records iterator.
     * @throws IgniteException If failed to start iteration.
     * @throws StorageException If IO error occurred while reading WAL entries.
     */
    public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException;

    /**
     * Invoke this method to iterate over the written log entries.
     *
     * @param start Optional WAL pointer from which to start iteration.
     * @param recordDeserializeFilter Specify a filter to skip WAL records. Those records will not be explicitly deserialized.
     * @return Records iterator.
     * @throws IgniteException If failed to start iteration.
     * @throws StorageException If IO error occurred while reading WAL entries.
     */
    public WALIterator replay(
        WALPointer start,
        @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordDeserializeFilter
    ) throws IgniteCheckedException, StorageException;

    /**
     * Invoke this method to reserve WAL history since provided pointer and prevent it's deletion.
     *
     * NOTE: If the {@link DataStorageConfiguration#getMaxWalArchiveSize()} is exceeded, the segment will be released.
     *
     * @param start WAL pointer.
     * @return {@code True} if the reservation was successful.
     */
    public boolean reserve(WALPointer start);

    /**
     * Invoke this method to release WAL history since provided pointer that was previously reserved.
     *
     * @param start WAL pointer.
     */
    public void release(WALPointer start);

    /**
     * Gives a hint to WAL manager to clear entries logged before the given pointer.
     * If entries are needed for binary recovery, they will not be affected.
     * Some entries may be reserved eg for historical rebalance and they also will not be affected.
     *
     * @param high Upper border to which WAL segments will be deleted.
     * @return Number of deleted WAL segments.
     */
    public int truncate(@Nullable WALPointer high);

    /**
     * Notifies {@code this} about latest checkpoint pointer.
     * <p>
     * Current implementations, in fact, react by keeping all WAL segments uncompacted starting from index prior to
     * the index of {@code ptr}. Compaction implies filtering out physical records and ZIP compression.
     *
     * @param ptr Pointer for which it is safe to compact the log.
     */
    public void notchLastCheckpointPtr(WALPointer ptr);

    /**
     * @return Current segment index.
     */
    public long currentSegment();

    /**
     * @return Total number of segments in the WAL archive.
     */
    public int walArchiveSegments();

    /**
     * @return Last archived segment index.
     */
    public long lastArchivedSegment();

    /**
     * @return Last compacted segment index.
     */
    public long lastCompactedSegment();

    /**
     * Checks if WAL segment is under lock or reserved
     *
     * @param ptr Pointer to check.
     * @return True if given pointer is located in reserved segment.
     */
    public boolean reserved(WALPointer ptr);

    /**
     * Checks if WAL segments is under lock or reserved.
     *
     * @param low Pointer since which WAL is locked or reserved. If {@code null}, checks from the oldest segment.
     * @param high Pointer for which WAL is locked or reserved.
     * @return Number of reserved WAL segments.
     */
    public int reserved(WALPointer low, WALPointer high);

    /**
     * Checks WAL disabled for cache group.
     *
     * @param grpId Group id.
     */
    public boolean disabled(int grpId);

    /**
     * Getting local WAL segment size.
     *
     * @param idx Absolute segment index.
     * @return Segment size, {@code 0} if size is unknown.
     */
    long segmentSize(long idx);

    /**
     * Get last written pointer.
     *
     * @return Last written pointer.
     */
    WALPointer lastWritePointer();

    /**
     * Start automatically releasing segments when reaching {@link DataStorageConfiguration#getMaxWalArchiveSize()}.
     */
    void startAutoReleaseSegments();
}
