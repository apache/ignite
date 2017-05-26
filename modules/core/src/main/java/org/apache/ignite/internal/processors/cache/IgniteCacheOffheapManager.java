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

package org.apache.ignite.internal.processors.cache;

import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@SuppressWarnings("WeakerAccess")
public interface IgniteCacheOffheapManager extends GridCacheManager {
    /**
     * Partition counter update callback. May be overridden by plugin-provided subclasses.
     *
     * @param part Partition.
     * @param cntr Partition counter.
     */
    public void onPartitionCounterUpdated(int part, long cntr);

    /**
     * Initial counter will be updated on state restore only
     *
     * @param part Partition
     * @param cntr New initial counter
     */
    public void onPartitionInitialCounterUpdated(int part, long cntr);

    /**
     * Partition counter provider. May be overridden by plugin-provided subclasses.
     *
     * @param part Partition ID.
     * @return Last updated counter.
     */
    public long lastUpdatedPartitionCounter(int part);

    /**
     * @param entry Cache entry.
     * @return Cached row, if available, null otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheDataRow read(GridCacheMapEntry entry) throws IgniteCheckedException;

    /**
     * @param key Key.
     * @return Cached row, if available, null otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheDataRow read(KeyCacheObject key) throws IgniteCheckedException;

    /**
     * @param p Partition.
     * @return Data store.
     * @throws IgniteCheckedException If failed.
     */
    public CacheDataStore createCacheDataStore(int p) throws IgniteCheckedException;

    /**
     * @return Iterable over all existing cache data stores.
     */
    public Iterable<CacheDataStore> cacheDataStores();

    /**
     * @param part Partition.
     * @return Data store.
     */
    public CacheDataStore dataStore(GridDhtLocalPartition part);

    /**
     * @param p Partition ID.
     * @param store Data store.
     */
    public void destroyCacheDataStore(int p, CacheDataStore store) throws IgniteCheckedException;

    /**
     * TODO: GG-10884, used on only from initialValue.
     */
    public boolean containsKey(GridCacheMapEntry entry);

    /**
     * @param c Closure.
     * @throws IgniteCheckedException If failed.
     */
    public boolean expire(IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c, int amount) throws IgniteCheckedException;

    /**
     * Gets the number of entries pending expire.
     *
     * @return Number of pending entries.
     * @throws IgniteCheckedException If failed to get number of pending entries.
     */
    public long expiredSize() throws IgniteCheckedException;

    /**
     * @param key Key.
     * @param part Partition.
     * @param c Tree update closure.
     * @throws IgniteCheckedException If failed.
     */
    public void invoke(KeyCacheObject key, GridDhtLocalPartition part, OffheapInvokeClosure c)
        throws IgniteCheckedException;

    /**
     * @param key  Key.
     * @param val  Value.
     * @param ver  Version.
     * @param expireTime Expire time.
     * @param partId Partition number.
     * @param oldRow Old row if available.
     * @param part Partition.
     * @throws IgniteCheckedException If failed.
     */
    public void update(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        int partId,
        GridDhtLocalPartition part,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException;

    /**
     * @param key Key.
     * @param partId Partition number.
     * @param part Partition.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(
        KeyCacheObject key,
        int partId,
        GridDhtLocalPartition part
    ) throws IgniteCheckedException;

    /**
     * @param ldr Class loader.
     * @return Number of undeployed entries.
     */
    public int onUndeploy(ClassLoader ldr);

    /**
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Rows iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> iterator(boolean primary, boolean backup, final AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * @param part Partition.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> iterator(final int part) throws IgniteCheckedException;

    /**
     * @param part Partition.
     * @param partCntr Partition counter to get historical data if available.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteRebalanceIterator rebalanceIterator(int part, AffinityTopologyVersion topVer, Long partCntr)
        throws IgniteCheckedException;

    /**
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @param keepBinary Keep binary flag.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<Cache.Entry<K, V>> entriesIterator(final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary) throws IgniteCheckedException;

    /**
     * @param part Partition.
     * @return Iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridCloseableIterator<KeyCacheObject> keysIterator(final int part) throws IgniteCheckedException;

    /**
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Entries count.
     * @throws IgniteCheckedException If failed.
     */
    public long entriesCount(boolean primary, boolean backup, AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    public void clear(boolean readers);

    /**
     * @param part Partition.
     * @return Number of entries in given partition.
     */
    public long entriesCount(int part);

    /**
     * @return Offheap allocated size.
     */
    public long offHeapAllocatedSize();

    /**
     * @return Global remove ID counter.
     */
    public GridAtomicLong globalRemoveId();

    /**
     * @param idxName Index name.
     * @return Root page for index tree.
     * @throws IgniteCheckedException If failed.
     */
    public RootPage rootPageForIndex(String idxName) throws IgniteCheckedException;

    /**
     * @param idxName Index name.
     * @throws IgniteCheckedException If failed.
     */
    public void dropRootPageForIndex(String idxName) throws IgniteCheckedException;

    /**
     * @return Reuse list for index tree.
     */
    public ReuseList reuseListForIndex(String idxName) throws IgniteCheckedException;

    /**
     *
     * @return Number of entries.
     */
    public long entriesCount();

    /**
     *
     */
    interface OffheapInvokeClosure extends IgniteTree.InvokeClosure<CacheDataRow> {
        /**
         * @return Old row.
         */
        @Nullable public CacheDataRow oldRow();
    }

    /**
     *
     */
    interface CacheDataStore {
        /**
         * @return Partition ID.
         */
        int partId();

        /**
         * @return Store name.
         */
        String name();

        /**
         * @param size Size to init.
         * @param updCntr Update counter to init.
         */
        void init(long size, long updCntr);

        /**
         * @return Size.
         */
        int size();

        /**
         * @return Update counter.
         */
        long updateCounter();

        /**
         *
         */
        void updateCounter(long val);

        /**
         * @return Next update counter.
         */
        public long nextUpdateCounter();

        /**
         * @return Initial update counter.
         */
        public Long initialUpdateCounter();

        /**
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param oldRow Old row.
         * @return New row.
         * @throws IgniteCheckedException If failed.
         */
        CacheDataRow createRow(KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @param part Partition.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param oldRow Old row if available.
         * @throws IgniteCheckedException If failed.
         */
        void update(KeyCacheObject key,
            int part,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @param c Closure.
         * @throws IgniteCheckedException If failed.
         */
        public void invoke(KeyCacheObject key, OffheapInvokeClosure c) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @param partId Partition number.
         * @throws IgniteCheckedException If failed.
         */
        public void remove(KeyCacheObject key, int partId) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @return Data row.
         * @throws IgniteCheckedException If failed.
         */
        public CacheDataRow find(KeyCacheObject key) throws IgniteCheckedException;

        /**
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException;

        /**
         * @param lower Lower bound.
         * @param upper Upper bound.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException;

        /**
         * Destroys the tree associated with the store.
         *
         * @throws IgniteCheckedException If failed.
         */
        public void destroy() throws IgniteCheckedException;

        /**
         * @return Row store.
         */
        public RowStore rowStore();

        /**
         * @param cntr Counter.
         */
        void updateInitialCounter(long cntr);
    }
}
