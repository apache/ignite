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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface PageMemoryEx extends PageMemory {
    /**
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param restore Determines if the page is locked for restore.
     * @return ByteBuffer for modifying the page.
     */
    long writeLock(int grpId, long pageId, long page, boolean restore);

    /**
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param walPlc {@code True} if page should be recorded to WAL, {@code false} if the page must not
     *                                be recorded and {@code null} for the default behavior.
     * @param dirtyFlag Determines whether the page was modified since the last checkpoint.
     * @param restore Determines if the page is locked for restore.
     */
    void writeUnlock(int grpId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag, boolean restore);

    /**
     * Gets or allocates metadata page for specified grpId.
     *
     * @param grpId Group ID.
     * @return Meta page for grpId.
     * @throws IgniteCheckedException If failed.
     */
    public long metaPageId(int grpId) throws IgniteCheckedException;

    /**
     * Gets or allocates partition metadata page for specified grpId and partId.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return Meta page for grpId and partId.
     * @throws IgniteCheckedException If failed.
     */
    public long partitionMetaPageId(int grpId, int partId) throws IgniteCheckedException;

    /**
     * @see #acquirePage(int, long)
     * Will not read page from file if it is not present in memory
     *
     * @param grpId Cache group ID.
     * @param pageId Page id.
     * @param restore Get page for restore
     * @throws IgniteCheckedException If failed.
     * @throws StorageException If page reading failed from storage.
     * @return Page.
     */
    public long acquirePage(int grpId, long pageId, boolean restore) throws IgniteCheckedException;

    /**
     * Heuristic method which allows a thread to check if it safe to start memory struture modifications
     * in regard with checkpointing.
     *
     * @return {@code False} if there are too many dirty pages and a thread should wait for a
     *      checkpoint to begin.
     */
    public boolean safeToUpdate();

    /**
     * Gets a collection of dirty page IDs since the last checkpoint. If a dirty page is being written after
     * the checkpointing operation begun, the modifications will be written to a temporary buffer which will
     * be flushed to the main memory after the checkpointing finished. This method must be called when no
     * concurrent operations on pages are performed.
     *
     * @return Collection of dirty page IDs.
     * @throws IgniteException If checkpoint has been already started and was not finished.
     */
    public GridMultiCollectionWrapper<FullPageId> beginCheckpoint() throws IgniteException;

    /**
     * Finishes checkpoint operation.
     */
    public void finishCheckpoint();

    /**
     * Gets page byte buffer for the checkpoint procedure.
     *
     * @param pageId Page ID to get byte buffer for. The page ID must be present in the collection returned by
     *      the {@link #beginCheckpoint()} method call.
     * @param outBuf Temporary buffer to write changes into.
     * @param tracker Checkpoint metrics tracker.
     * @return {@code Partition generation} if data was read, {@code null} otherwise (data already saved to storage).
     * @throws IgniteException If failed to obtain page data.
     */
    @Nullable public Integer getForCheckpoint(FullPageId pageId, ByteBuffer outBuf, CheckpointMetricsTracker tracker);

    /**
     * Marks partition as invalid / outdated.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return New partition generation (growing 1-based partition file version).
     */
    public int invalidate(int grpId, int partId);

    /**
     * Clears internal metadata of destroyed cache group.
     *
     * @param grpId Cache group ID.
     */
    public void onCacheGroupDestroyed(int grpId);

    /**
     * Asynchronously clears pages satisfying the given predicate.
     *
     * @param pred Predicate for cache group id, pageId.
     * @param cleanDirty Flag indicating that dirty pages collection should be cleaned.
     * @return Future that will be completed when all pages are cleared.
     */
    public IgniteInternalFuture<Void> clearAsync(LoadedPagesMap.KeyPredicate pred, boolean cleanDirty);
}
