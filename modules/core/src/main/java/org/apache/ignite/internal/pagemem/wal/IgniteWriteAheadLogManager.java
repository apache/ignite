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
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

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
     */
    public void resumeLogging(WALPointer lastWrittenPtr) throws IgniteCheckedException;

    /**
     * Appends the given log entry to the write-ahead log.
     *
     * @param entry entry to log.
     * @return WALPointer that may be passed to {@link #flush(WALPointer, boolean)} method to make sure the record is
     *      written to the log.
     * @throws IgniteCheckedException If failed to construct log entry.
     * @throws StorageException If IO error occurred while writing log entry.
     */
    public WALPointer log(WALRecord entry) throws IgniteCheckedException, StorageException;

    /**
     * Makes sure that all log entries written to the log up until the specified pointer are actually written
     * to the underlying storage.
     *
     * @param ptr Optional pointer to write. If {@code null}, will sync up to the latest record.
     * @param explicitFsync If true, data will be synced to the storage device on hardware level.
     * @throws IgniteCheckedException If failed to write.
     * @throws StorageException If IO exception occurred during the write. If an exception is thrown from this
     *      method, the WAL will be invalidated and the node will be stopped.
     */
    public void flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException;

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
     * Invoke this method to reserve WAL history since provided pointer and prevent it's deletion.
     *
     * @param start WAL pointer.
     * @throws IgniteException If failed to reserve.
     */
    public boolean reserve(WALPointer start) throws IgniteCheckedException;

    /**
     * Invoke this method to release WAL history since provided pointer that was previously reserved.
     *
     * @param start WAL pointer.
     * @throws IgniteException If failed to release.
     */
    public void release(WALPointer start) throws IgniteCheckedException;

    /**
     * Gives a hint to WAL manager to clear entries logged before the given pointer. Some entries before the
     * the given pointer will be kept because there is a configurable WAL history size. Those entries may be used
     * for partial partition rebalancing.
     *
     * @param low Pointer since which WAL will be truncated. If null, WAL will be truncated from the oldest segment.
     * @param high Pointer for which it is safe to clear the log.
     * @return Number of deleted WAL segments.
     */
    public int truncate(WALPointer low, WALPointer high);

    /**
     * Gives a hint to WAL manager to compact WAL until given pointer (exclusively).
     * Compaction implies filtering out physical records and ZIP compression.
     *
     * @param ptr Pointer for which it is safe to compact the log.
     */
    public void allowCompressionUntil(WALPointer ptr);

    /**
     * @return Total number of segments in the WAL archive.
     */
    public int walArchiveSegments();

    /**
     * Checks if WAL segment is under lock or reserved
     * @param ptr Pointer to check.
     * @return True if given pointer is located in reserved segment.
     */
    public boolean reserved(WALPointer ptr);

    /**
     * Checks WAL disabled for cache group.
     *
     * @param grpId Group id.
     */
    public boolean disabled(int grpId);
}
