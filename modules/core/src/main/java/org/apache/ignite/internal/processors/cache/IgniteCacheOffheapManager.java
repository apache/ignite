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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRowCacheAware;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@SuppressWarnings("WeakerAccess")
public interface IgniteCacheOffheapManager {
    /**
     * @param ctx Context.
     * @param grp Cache group.
     * @throws IgniteCheckedException If failed.
     */
    public void start(GridCacheSharedContext ctx, CacheGroupContext grp) throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException;

    /**
     * Callback on node stop.
     */
    public void onKernalStop();

    /**
     * @param cacheId Cache ID.
     * @param destroy Destroy data flag. Setting to <code>true</code> will remove all cache data.
     */
    public void stopCache(int cacheId, boolean destroy);

    /**
     * Prepare cache group to stop (due to cache destroy or cluster deactivate).
     */
    public void prepareToStop();

    /**
     * Stop cache group (due to cache destroy or cluster deactivate).
     */
    public void stop();

    /**
     * Pre-create single partition that resides in page memory or WAL and restores their state.
     *
     * @param p Partition id.
     * @param recoveryState Partition recovery state.
     * @return Processing time in millis.
     * @throws IgniteCheckedException If failed.
     */
    long restoreStateOfPartition(int p, @Nullable Integer recoveryState) throws IgniteCheckedException;

    /**
     * Pre-create partitions that resides in page memory or WAL and restores their state.
     *
     * @throws IgniteCheckedException If failed.
     */
    void restorePartitionStates() throws IgniteCheckedException;

    /**
     * Confirm that partition states are restored. This method should be called after restoring state of all partitions
     * in group using {@link #restoreStateOfPartition(int, Integer)}.
     */
    void confirmPartitionStatesRestored();

    /**
     * @param entry Cache entry.
     * @return Cached row, if available, null otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheDataRow read(GridCacheMapEntry entry) throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @return Cached row, if available, null otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheDataRow read(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException;

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
     * @param part Local partition or {@code null} if a related cache group is <tt>LOCAL</tt>.
     * @return Cache data store associated with given partition or the cache data store for a <tt>LOCAL</tt> cache group.
     */
    public CacheDataStore dataStore(@Nullable GridDhtLocalPartition part);

    /**
     * @param store Data store.
     * @throws IgniteCheckedException If failed.
     */
    public void destroyCacheDataStore(CacheDataStore store) throws IgniteCheckedException;

    /**
     * TODO: GG-10884, used on only from initialValue.
     */
    public boolean containsKey(GridCacheMapEntry entry);

    /**
     * @param cctx Cache context.
     * @param c Closure.
     * @param amount Limit of processed entries by single call, {@code -1} for no limit.
     * @return {@code True} if unprocessed expired entries remains.
     * @throws IgniteCheckedException If failed.
     */
    public boolean expire(GridCacheContext cctx, IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c, int amount)
        throws IgniteCheckedException;

    /**
     * Gets the number of entries pending expire.
     *
     * @return Number of pending entries.
     * @throws IgniteCheckedException If failed to get number of pending entries.
     */
    public long expiredSize() throws IgniteCheckedException;

    /**
     * Checks if the cache has entries pending expire.
     *
     * @return {@code True} if there are entries pending expire.
     * @throws IgniteCheckedException If failed to get number of pending entries.
     */
    public boolean hasEntriesPendingExpire(int cacheId) throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param part Partition.
     * @param c Tree update closure.
     * @throws IgniteCheckedException If failed.
     */
    public void invoke(GridCacheContext cctx, KeyCacheObject key, GridDhtLocalPartition part, OffheapInvokeClosure c)
        throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @param oldRow Old row if available.
     * @param part Partition.
     * @throws IgniteCheckedException If failed.
     */
    public void update(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        GridDhtLocalPartition part,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param partId Partition number.
     * @param part Partition.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(
        GridCacheContext cctx,
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
     * @param cacheId Cache ID.
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @param dataPageScanEnabled Flag to enable data page scan.
     * @return Rows iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> cacheIterator(int cacheId,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer,
        Boolean dataPageScanEnabled
    ) throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @param dataPageScanEnabled Flag to enable data page scan.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, final int part,
        Boolean dataPageScanEnabled) throws IgniteCheckedException;

    /**
     * @param part Partition number.
     * @return Iterator for given partition.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> partitionIterator(final int part) throws IgniteCheckedException;

    /**
     * @param part Partition number.
     * @param topVer Topology version.
     * @return Iterator for given partition that will reserve partition state until it is closed.
     * @throws IgniteCheckedException If failed.
     */
    public GridCloseableIterator<CacheDataRow> reservedIterator(final int part, final AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * @param parts Partitions.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteRebalanceIterator rebalanceIterator(IgniteDhtDemandedPartitionsMap parts, AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @param primary {@code True} if need to return primary entries.
     * @param backup {@code True} if need to return backup entries.
     * @param topVer Topology version.
     * @param keepBinary Keep binary flag.
     * @param dataPageScanEnabled Flag to enable data page scan.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<Cache.Entry<K, V>> cacheEntriesIterator(
        GridCacheContext cctx,
        final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary,
        Boolean dataPageScanEnabled
    ) throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridCloseableIterator<KeyCacheObject> cacheKeysIterator(int cacheId, final int part)
        throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Entries count.
     */
    public long cacheEntriesCount(int cacheId, boolean primary, boolean backup, AffinityTopologyVersion topVer);

    /**
     * Store entries.
     *
     * @param part Local partition.
     * @param infos Entry infos.
     * @param initPred Applied to all created rows. Each row that not matches the predicate is removed.
     * @throws IgniteCheckedException If failed.
     */
    public void storeEntries(GridDhtLocalPartition part, Iterator<GridCacheEntryInfo> infos,
        IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException;

    /**
     * Clears offheap entries.
     *
     * @param cctx Cache context.
     * @param readers {@code True} to clear readers.
     */
    public void clearCache(GridCacheContext cctx, boolean readers);

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Number of entries in given partition.
     */
    public long cacheEntriesCount(int cacheId, int part);

    /**
     * @return Offheap allocated size.
     */
    public long offHeapAllocatedSize();

    /**
     * @return Global remove ID counter.
     */
    public GridAtomicLong globalRemoveId();

    /**
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @param segment Segment.
     * @return Root page for index tree.
     * @throws IgniteCheckedException If failed.
     */
    public RootPage rootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @throws IgniteCheckedException If failed.
     */
    public @Nullable RootPage findRootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException;

    /**
     * Dropping the root page of the index tree.
     *
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @param segment Segment index.
     * @return Dropped root page of the index tree.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable RootPage dropRootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException;

    /**
     * Renaming the root page of the index tree.
     *
     * @param cacheId Cache id.
     * @param oldIdxName Old name of the index tree.
     * @param newIdxName New name of the index tree.
     * @param segment Segment index.
     * @return Renamed root page of the index tree.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable RootPage renameRootPageForIndex(
        int cacheId,
        String oldIdxName,
        String newIdxName,
        int segment
    ) throws IgniteCheckedException;

    /**
     * @param idxName Index name.
     * @return Reuse list for index tree.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseList reuseListForIndex(String idxName) throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @return Number of entries.
     */
    public long cacheEntriesCount(int cacheId);

    /**
     * Preload a partition. Must be called under partition reservation for DHT caches.
     *
     * @param pardId Partition id.
     * @throws IgniteCheckedException If failed.
     */
    public void preloadPartition(int pardId) throws IgniteCheckedException;

    /**
     *
     */
    interface OffheapInvokeClosure extends IgniteTree.InvokeClosure<CacheDataRow> {
        /**
         * @return Old row.
         */
        @Nullable public CacheDataRow oldRow();

        /**
         * Flag that indicates if oldRow was expired during invoke.
         * @return {@code true} if old row was expired, {@code false} otherwise.
         */
        public boolean oldRowExpiredFlag();
    }

    /**
     *
     */
    interface CacheDataStore {

        /**
         * @return Cache data tree object.
         */
        public CacheDataTree tree();

        /**
         * Initialize data store if it exists.
         *
         * @return {@code True} if initialized.
         */
        boolean init();

        /**
         * @return Partition ID.
         */
        int partId();

        /**
         * @param cacheId Cache ID.
         * @return Size.
         */
        long cacheSize(int cacheId);

        /**
         * @return Cache sizes if store belongs to group containing multiple caches.
         */
        Map<Integer, Long> cacheSizes();

        /**
         * @return Total size.
         */
        long fullSize();

        /**
         * @return {@code True} if there are no items in the store.
         */
        boolean isEmpty();

        /**
         * Updates size metric for particular cache.
         *
         * @param cacheId Cache ID.
         * @param delta Size delta.
         */
        void updateSize(int cacheId, long delta);

        /**
         * @return Update counter (LWM).
         */
        long updateCounter();

        /**
         * @return Highest applied update counter.
         */
        long highestAppliedCounter();

        /**
         * @return Reserved counter (HWM).
         */
        long reservedCounter();

        /**
         * @return Update counter or {@code null} if store is not yet created.
         */
        @Nullable PartitionUpdateCounter partUpdateCounter();

        /**
         * @param delta Delta.
         */
        long reserve(long delta);

        /**
         * @param val Update counter.
         */
        void updateCounter(long val);

        /**
         * Updates counters from start value by delta value.
         * @param start Start.
         * @param delta Delta.
         */
        boolean updateCounter(long start, long delta);

        /**
         * @return Next update counter.
         */
        public long nextUpdateCounter();

        /**
         * Returns current value and updates counter by delta.
         *
         * @param delta Delta.
         * @return Current value.
         */
        public long getAndIncrementUpdateCounter(long delta);

        /**
         * @return Initial update counter.
         */
        public long initialUpdateCounter();

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param oldRow Old row.
         * @return New row.
         * @throws IgniteCheckedException If failed.
         */
        CacheDataRow createRow(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException;

        /**
         * Insert rows into page memory.
         *
         * @param rows Rows.
         * @param initPred Applied to all rows. Each row that not matches the predicate is removed.
         * @throws IgniteCheckedException If failed.
         */
        public void insertRows(Collection<DataRowCacheAware> rows,
            IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param oldRow Old row if available.
         * @throws IgniteCheckedException If failed.
         */
        void update(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param c Closure.
         * @throws IgniteCheckedException If failed.
         */
        public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param partId Partition number.
         * @throws IgniteCheckedException If failed.
         */
        public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @return Data row.
         * @throws IgniteCheckedException If failed.
         */
        public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException;

        /**
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException;

        /**
         * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException;

        /**
         * @param cacheId Cache ID.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException;

        /**
         * @param cacheId Cache ID.
         * @param lower Lower bound.
         * @param upper Upper bound.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException;

        /**
         * @param cacheId Cache ID.
         * @param lower Lower bound.
         * @param upper Upper bound.
         * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper, Object x) throws IgniteCheckedException;

        /**
         * Destroys the tree associated with the store.
         *
         * @throws IgniteCheckedException If failed.
         */
        public void destroy() throws IgniteCheckedException;

        /**
         * Mark store as destroyed.
         */
        public void markDestroyed() throws IgniteCheckedException;

        /**
         * @return {@code true} If marked as destroyed.
         */
        public boolean destroyed();

        /**
         * Clears all the records associated with logical cache with given ID.
         *
         * @param cacheId Cache ID.
         * @throws IgniteCheckedException If failed.
         */
        public void clear(int cacheId) throws IgniteCheckedException;

        /**
         * @return Row store.
         */
        public RowStore rowStore();

        /**
         * @param start Counter.
         * @param delta Delta.
         */
        public void updateInitialCounter(long start, long delta);

        /**
         * Inject rows cache cleaner.
         *
         * @param rowCacheCleaner Rows cache cleaner.
         */
        public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner);

        /**
         * Return PendingTree for data store.
         *
         * @return PendingTree instance.
         */
        public PendingEntriesTree pendingTree();

        /**
         * Flushes pending update counters closing all possible gaps.
         *
         * @return Even-length array of pairs [start, end] for each gap.
         */
        GridLongList finalizeUpdateCounters();

        /**
         * Preload a store into page memory.
         * @throws IgniteCheckedException If failed.
         */
        public void preload() throws IgniteCheckedException;

        /**
         * Reset counter for partition.
         */
        void resetUpdateCounter();

        /**
         * Reset the initial value of the partition counter.
         */
        void resetInitialUpdateCounter();

        /**
         * Partition storage.
         */
        public PartitionMetaStorage<SimpleDataRow> partStorage();
    }
}
