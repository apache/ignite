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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

/**
 *
 */
public interface IgnitePageStoreManager extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * Invoked before starting checkpoint recover.
     */
    public void beginRecover();

    /**
     * Invoked after checkpoint recover is finished.
     */
    public void finishRecover() throws IgniteCheckedException;

    /**
     * Initializes disk store structures.
     *
     * @param cacheId Cache id.
     * @param partitions Partitions count.
     * @param workingDir Working directory.
     * @param tracker Allocation tracker.
     * @throws IgniteCheckedException If failed.
     */
    void initialize(int cacheId, int partitions, String workingDir, AllocatedPageTracker tracker)
        throws IgniteCheckedException;

    /**
     * Callback called when a cache is starting.
     *
     * @param grpDesc Cache group descriptor.
     * @param cacheData Cache data of the cache being started.
     * @throws IgniteCheckedException If failed to handle cache start callback.
     */
    public void initializeForCache(CacheGroupDescriptor grpDesc, StoredCacheData cacheData)
        throws IgniteCheckedException;

    /**
     * Initializes disk cache store structures.
     */
    public void initializeForMetastorage() throws IgniteCheckedException;

    /**
     * Callback called when a cache is stopping. After this callback is invoked, no data associated with
     * the given cache will be stored on disk.
     *
     * @param grp Cache group being stopped.
     * @param destroy Flag indicating if the cache is being destroyed and data should be cleaned.
     * @throws IgniteCheckedException If failed to handle cache destroy callback.
     */
    public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteCheckedException;

    /**
     * Callback called when a partition is created on the local node.
     *
     * @param grpId Cache group ID where the partition is being created.
     * @param partId ID of the partition being created.
     * @throws IgniteCheckedException If failed to handle partition create callback.
     */
    public void onPartitionCreated(int grpId, int partId) throws IgniteCheckedException;

    /**
     * Callback called when a partition for the given cache is evicted from the local node.
     * After this callback is invoked, no data associated with the partition will be stored on disk.
     *
     * @param grpId Cache group ID of the evicted partition.
     * @param partId Partition ID.
     * @param tag Partition tag (growing 1-based partition file version).
     * @throws IgniteCheckedException If failed to handle partition destroy callback.
     */
    public void onPartitionDestroyed(int grpId, int partId, int tag) throws IgniteCheckedException;

    /**
     * Reads a page for the given cache ID. Cache ID may be {@code 0} if the page is a meta page.
     *
     * @param grpId Cache group ID.
     * @param pageId PageID to read.
     * @param pageBuf Page buffer to write to.
     * @throws IgniteCheckedException If failed to read the page.
     */
    public void read(int grpId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException;

    /**
     * Checks if partition store exists.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return {@code True} if partition store exists.
     * @throws IgniteCheckedException If failed.
     */
    public boolean exists(int grpId, int partId) throws IgniteCheckedException;

    /**
     * Reads a header of a page store.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param buf Buffer to write to.
     * @throws IgniteCheckedException If failed.
     */
    public void readHeader(int grpId, int partId, ByteBuffer buf) throws IgniteCheckedException;

    /**
     * Writes the page for the given cache ID. Cache ID may be {@code 0} if the page is a meta page.
     *
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IgniteCheckedException If failed to write page.
     */
    public void write(int grpId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException;

    /**
     * Gets page offset within the page store file.
     *
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return Page offset.
     * @throws IgniteCheckedException If failed.
     */
    public long pageOffset(int grpId, long pageId) throws IgniteCheckedException;

    /**
     * Makes sure that all previous writes to the store has been written to disk.
     *
     * @param grpId Cache group ID to sync.
     * @param partId Partition ID to sync.
     * @throws IgniteCheckedException If IO error occurred while running sync.
     */
    public void sync(int grpId, int partId) throws IgniteCheckedException;

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @throws IgniteCheckedException If failed.
     */
    public void ensure(int grpId, int partId) throws IgniteCheckedException;

    /**
     * Allocates a page for the given page space.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID. Used only if {@code flags} is equal to {@link PageMemory#FLAG_DATA}.
     * @param flags Page allocation flags.
     * @return Allocated page ID.
     * @throws IgniteCheckedException If IO exception occurred while allocating a page ID.
     */
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException;

    /**
     * Gets total number of allocated pages for the given space.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Number of allocated pages.
     * @throws IgniteCheckedException If failed.
     */
    public int pages(int grpId, int partId) throws IgniteCheckedException;

    /**
     * Gets meta page ID for specified cache.
     *
     * @param grpId Cache group ID.
     * @return Meta page ID.
     */
    public long metaPageId(int grpId);

    /**
     * @return Saved cache configurations.
     * @throws IgniteCheckedException If failed.
     */
    public Map<String, StoredCacheData> readCacheConfigurations() throws IgniteCheckedException;

    /**
     * @param cacheData Cache configuration.
     * @param overwrite Whether stored configuration should be overwritten if it exists.
     * @throws IgniteCheckedException If failed.
     */
    public void storeCacheData(StoredCacheData cacheData, boolean overwrite) throws IgniteCheckedException;

    /**
     * Remove cache configuration data file.
     *
     * @param cacheData Cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    public void removeCacheData(StoredCacheData cacheData) throws IgniteCheckedException;

    /**
     * @param grpId Cache group ID.
     * @return {@code True} if index store for given cache group existed before node started.
     */
    public boolean hasIndexStore(int grpId);

    /**
     * @param grpDesc Cache group descriptor.
     */
    public void beforeCacheGroupStart(CacheGroupDescriptor grpDesc);

    /**
     * Calculates number of pages currently allocated for given cache group.
     *
     * @param grpId cache group id.
     * @return number of pages.
     */
    public long pagesAllocated(int grpId);

    /**
     * Cleanup persistent space for cache.
     *
     * @param cacheConfiguration Cache configuration of cache which should be cleanup.
     */
    public void cleanupPersistentSpace(CacheConfiguration cacheConfiguration) throws IgniteCheckedException;

    /**
     * Cleanup persistent space for all caches.
     */
    public void cleanupPersistentSpace() throws IgniteCheckedException;

    /**
     * Creates and initializes cache work directory retrieved from {@code cacheCfg}.
     *
     * @param cacheCfg Cache configuration.
     * @return {@code True} if work directory already exists.
     *
     * @throws IgniteCheckedException If failed.
     */
    public boolean checkAndInitCacheWorkDir(CacheConfiguration cacheCfg) throws IgniteCheckedException;
}
