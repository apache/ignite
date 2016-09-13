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

package org.apache.ignite.internal.pagemem.store;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;

import java.nio.ByteBuffer;

/**
 *
 */
public interface IgnitePageStoreManager extends GridCacheSharedManager {
    /**
     * Callback called when a cache is starting.
     *
     * @param ccfg Cache configuration of the cache being started.
     * @throws IgniteCheckedException If failed to handle cache start callback.
     */
    public void onBeforeCacheStart(CacheConfiguration ccfg) throws IgniteCheckedException;

    /**
     * Callback called when a cache is stopping. After this callback is invoked, no data associated with
     * the given cache will be stored on disk.
     *
     * @param cacheCtx Cache context of the cache being stopped.
     * @throws IgniteCheckedException If failed to handle cache destroy callback.
     */
    public void onAfterCacheDestroy(GridCacheContext cacheCtx) throws IgniteCheckedException;

    /**
     * Callback called when a partition is created on the local node.
     *
     * @param cacheId Cache ID where the partition is being created.
     * @param partId ID of the partition being created.
     * @throws IgniteCheckedException If failed to handle partition create callback.
     */
    public void onPartitionCreated(int cacheId, int partId) throws IgniteCheckedException;

    /**
     * Callback called when a partition for the given cache is evicted from the local node.
     * After this callback is invoked, no data associated with the partition will be stored on disk.
     *
     * @param cacheId Cache ID of the evicted partition.
     * @param partId Partition ID.
     * @throws IgniteCheckedException If failed to handle partition destroy callback.
     */
    public void onPartitionDestroyed(int cacheId, int partId) throws IgniteCheckedException;

    /**
     * Reads a page for the given cache ID. Cache ID may be {@code 0} if the page is a meta page.
     *
     * @param cacheId Cache ID.
     * @param pageId PageID to read.
     * @param pageBuf Page buffer to write to.
     * @throws IgniteCheckedException If failed to read the page.
     */
    public void read(int cacheId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException;

    /**
     * Checks if page exists.
     *
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     * @param flag Allocation flag.
     * @return {@code True} if page exists.
     * @throws IgniteCheckedException If failed.
     */
    public boolean exists(int cacheId, int partId, byte flag) throws IgniteCheckedException;

    /**
     * Reads a header of apage store.
     *
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     * @param flag Allocation flags.
     * @param buf Buffer to write to.
     * @throws IgniteCheckedException If failed.
     */
    public void readHeader(int cacheId, int partId, byte flag, ByteBuffer buf) throws IgniteCheckedException;

    /**
     * Writes the page for the given cache ID. Cache ID may be {@code 0} if the page is a meta page.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IgniteCheckedException If failed to write page.
     */
    public void write(int cacheId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException;

    /**
     * Gets page offset within the page store file.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Page offset.
     * @throws IgniteCheckedException If failed.
     */
    public long pageOffset(int cacheId, long pageId) throws IgniteCheckedException;

    /**
     * Makes sure that all previous writes to the store has been written to disk.
     *
     * @param cacheId Cache ID to sync.
     * @param flags Allocation flags.
     * @param partId Partition ID to sync.
     * @throws IgniteCheckedException If IO error occurred while running sync.
     */
    public void sync(int cacheId, byte flags, int partId) throws IgniteCheckedException;

    /**
     * Allocates a page for the given page space.
     *
     * @param cacheId Cache ID.
     * @param partId Partition ID. Used only if {@code flags} is equal to {@link PageMemory#FLAG_DATA}.
     * @param flags Page allocation flags.
     * @return Allocated page ID.
     * @throws IgniteCheckedException If IO exception occurred while allocating a page ID.
     */
    public long allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException;

    /**
     * Gets total number of allocated pages for the given space.
     *
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     * @param flags Flags.
     * @return Number of allocated pages.
     * @throws IgniteCheckedException If failed.
     */
    public int pages(int cacheId, int partId, byte flags) throws IgniteCheckedException;

    /**
     * Gets meta page ID for specified cache.
     *
     * @param cacheId Cache ID.
     * @return Meta page ID.
     */
    public long metaPageId(int cacheId);

    /**
     * Clears store.
     *
     * @param cacheId Cache ID.
     * @param flags Allocation flags.
     * @param partId Partition ID.
     * @throws IgniteCheckedException If failed.
     */
    public void clear(int cacheId, byte flags, int partId) throws IgniteCheckedException;
}
