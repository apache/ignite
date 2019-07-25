/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Page memory with some persistence related additions.
 */
public interface PageMemoryEx extends PageMemory {
    /**
     * @param absPtr Absolute pointer to read lock.
     * @param pageId Page ID.
     * @param force Force flag.
     * @param touch Update page timestamp.
     * @return Pointer to the page read buffer.
     */
    long readLock(long absPtr, long pageId, boolean force, boolean touch);

    /**
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param restore Determines if the page is locked for restore memory (crash recovery).
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
     * Will read page from file if it is not present in memory
     *
     * @param grpId Cache group ID.
     * @param pageId Page id.
     * @param restore Get page for restore
     * @throws IgniteCheckedException If failed.
     * @throws StorageException If page reading failed from storage.
     * @return Page.
     */
    public long acquirePage(int grpId, long pageId, IoStatisticsHolder statHldr,
        boolean restore) throws IgniteCheckedException;

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
     * Gets a collection of dirty page IDs since the last checkpoint and dirty pages with user data are presented. If a
     * dirty page is being written after the checkpointing operation begun, the modifications will be written to a
     * temporary buffer which will be flushed to the main memory after the checkpointing finished. This method must be
     * called when no concurrent operations on pages are performed.
     *
     * @return Couple of collection of dirty page IDs and flag. The flag is {@code true}, if since last checkpoint at
     * least one page with user data (not relates with system cache) became a dirty, and {@code false} otherwise.
     * @throws IgniteException If checkpoint has been already started and was not finished.
     */
    public IgniteBiTuple<GridMultiCollectionWrapper<FullPageId>, Boolean> beginCheckpointEx() throws IgniteException;

    /**
     * Finishes checkpoint operation.
     */
    public void finishCheckpoint();

    /**
     * Prepare page for write during checkpoint.
     *{@link PageStoreWriter} will be called when the page will be ready to write.
     *
     * @param pageId Page ID to get byte buffer for. The page ID must be present in the collection returned by
     *      the {@link #beginCheckpoint()} method call.
     * @param buf Temporary buffer to write changes into.
     * @param pageWriter Checkpoint page write context.
     * @param tracker Checkpoint metrics tracker.
     * @throws IgniteCheckedException If failed to obtain page data.
     */
     public void checkpointWritePage(
         FullPageId pageId,
         ByteBuffer buf,
         PageStoreWriter pageWriter,
         CheckpointMetricsTracker tracker
     ) throws IgniteCheckedException;

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
