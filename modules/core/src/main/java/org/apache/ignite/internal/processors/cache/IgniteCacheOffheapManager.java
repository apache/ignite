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

import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.lang.IgniteBiTuple;
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
     *
     */
    public void onKernalStop();

    /**
     * @param cacheId Cache ID.
     * @param destroy Destroy data flag. Setting to <code>true</code> will remove all cache data.
     */
    public void stopCache(int cacheId, boolean destroy);

    /**
     *
     */
    public void stop();

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
     * @param part Partition.
     * @return Data store.
     */
    public CacheDataStore dataStore(GridDhtLocalPartition part);

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
     * @return Cached row, if available, null otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheDataRow mvccRead(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot ver)
        throws IgniteCheckedException;

    /**
     * For testing only.
     *
     * @param cctx Cache context.
     * @param key Key.
     * @return All stored versions for given key.
     * @throws IgniteCheckedException If failed.
     */
    public List<IgniteBiTuple<Object, MvccVersion>> mvccAllVersions(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException;

    /**
     * Returns iterator over the all row versions for the given key.
     *
     * @param cctx Cache context.
     * @param key Key.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Iterator over all versions.
     * @throws IgniteCheckedException If failed.
     */
    GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx, KeyCacheObject key, Object x)
        throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @return {@code True} if value was inserted.
     * @throws IgniteCheckedException If failed.
     */
    default boolean mvccInitialValue(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime
    ) throws IgniteCheckedException {
        return mvccInitialValue(entry, val, ver, expireTime, null, null);
    }

    /**
     * @param entry Entry.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @param mvccVer MVCC version.
     * @param newMvccVer New MVCC version.
     * @return {@code True} if value was inserted.
     * @throws IgniteCheckedException If failed.
     */
    public boolean mvccInitialValue(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer
    ) throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @param mvccVer MVCC version.
     * @param newMvccVer New MVCC version.
     * @param txState Tx state hint for the mvcc version.
     * @param newTxState Tx state hint for the new mvcc version.
     * @return {@code True} if value was inserted.
     * @throws IgniteCheckedException If failed.
     */
    public boolean mvccInitialValueIfAbsent(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte txState,
        byte newTxState
    ) throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @param val Value.
     * @param ver Cache version.
     * @param expireTime Expire time.
     * @param mvccSnapshot MVCC snapshot.
     * @param primary {@code True} if on primary node.
     * @param needHistory Flag to collect history.
     * @param noCreate Flag indicating that row should not be created if absent.
     * @param filter Filter.
     * @param retVal Flag to return previous value.
     * @param entryProc Entry processor.
     * @param invokeArgs Entry processor invoke arguments.
     * @return Update result.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public MvccUpdateResult mvccUpdate(
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        boolean primary,
        boolean needHistory,
        boolean noCreate,
        @Nullable CacheEntryPredicate filter,
        boolean retVal,
        EntryProcessor entryProc,
        Object[] invokeArgs) throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @param mvccSnapshot MVCC snapshot.
     * @param primary {@code True} if on primary node.
     * @param needHistory Flag to collect history.
     * @param filter Filter.
     * @param retVal Flag to return previous value.
     * @return Update result.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public MvccUpdateResult mvccRemove(
        GridCacheMapEntry entry,
        MvccSnapshot mvccSnapshot,
        boolean primary,
        boolean needHistory,
        @Nullable CacheEntryPredicate filter,
        boolean retVal) throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @param mvccSnapshot MVCC snapshot.
     * @return Update result.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public MvccUpdateResult mvccLock(
        GridCacheMapEntry entry,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @param val Value.
     * @param ver Version.
     * @param mvccVer MVCC version.
     * @param newMvccVer New MVCC version.
     * @return {@code True} if value was inserted.
     * @throws IgniteCheckedException If failed.
     */
    public boolean mvccUpdateRowWithPreloadInfo(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState
    ) throws IgniteCheckedException;

    /**
     * @param primary {@code True} if on primary node.
     * @param entry Entry.
     * @param val Value.
     * @param ver Cache version.
     * @param expireTime Expire time.
     * @param mvccSnapshot MVCC snapshot.
     * @return Transactions to wait for before finishing current transaction.
     * @throws IgniteCheckedException If failed.
     */
    GridLongList mvccUpdateNative(
            boolean primary,
            GridCacheMapEntry entry,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException;

    /**
     * @param primary {@code True} if on primary node.
     * @param entry Entry.
     * @param mvccSnapshot MVCC snapshot.
     * @return Transactions to wait for before finishing current transaction.
     * @throws IgniteCheckedException If failed.
     */
    GridLongList mvccRemoveNative(
            boolean primary,
            GridCacheMapEntry entry,
            MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException;

    /**
     * @param entry Entry.
     * @throws IgniteCheckedException If failed.
     */
    public void mvccRemoveAll(GridCacheMapEntry entry)
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
     *
     * @param cacheId Cache ID.
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Rows iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> cacheIterator(int cacheId,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer,
        @Nullable MvccSnapshot mvccSnapshot)
        throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public default GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, final int part)
        throws IgniteCheckedException {
        return cachePartitionIterator(cacheId, part, null);
    }

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @param mvccSnapshot MVCC snapshot.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, final int part,
        @Nullable MvccSnapshot mvccSnapshot) throws IgniteCheckedException;

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
    // TODO: MVCC>
    public IgniteRebalanceIterator rebalanceIterator(IgniteDhtDemandedPartitionsMap parts, AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * @param cctx Cache context.
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @param keepBinary Keep binary flag.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    // TODO: MVCC>
    public <K, V> GridCloseableIterator<Cache.Entry<K, V>> cacheEntriesIterator(
        GridCacheContext cctx,
        final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary) throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Iterator.
     * @throws IgniteCheckedException If failed.
     */
    // TODO: MVCC>
    public GridCloseableIterator<KeyCacheObject> cacheKeysIterator(int cacheId, final int part)
        throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Entries count.
     * @throws IgniteCheckedException If failed.
     */
    // TODO: MVCC>
    public long cacheEntriesCount(int cacheId, boolean primary, boolean backup, AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

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
     * @return Root page for index tree.
     * @throws IgniteCheckedException If failed.
     */
    public RootPage rootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @throws IgniteCheckedException If failed.
     */
    public void dropRootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException;

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
     * @param part Partition.
     * @return Number of entries.
     */
    public long totalPartitionEntriesCount(int part);

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
         * @param cacheSizes Cache sizes if store belongs to group containing multiple caches.
         */
        void init(long size, long updCntr, @Nullable Map<Integer, Long> cacheSizes);

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
         * Updates size metric for particular cache.
         *
         * @param cacheId Cache ID.
         * @param delta Size delta.
         */
        void updateSize(int cacheId, long delta);

        /**
         * @return Update counter.
         */
        long updateCounter();

        /**
         * @param val Update counter.
         */
        void updateCounter(long val);

        /**
         * Updates counters from start value by delta value.
         *
         * @param start Start.
         * @param delta Delta
         */
        void updateCounter(long start, long delta);

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
         * @param cctx Cache context.
         * @param cleanupRows Rows to cleanup.
         * @throws IgniteCheckedException If failed.
         * @return Cleaned rows count.
         */
        public int cleanup(GridCacheContext cctx, @Nullable List<MvccLinkAwareSearchRow> cleanupRows)
            throws IgniteCheckedException;

        /**
         *
         * @param cctx Cache context.
         * @param row Row.
         * @throws IgniteCheckedException
         */
        public void updateTxState(GridCacheContext cctx, CacheSearchRow row)
            throws IgniteCheckedException;

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
         * @param val Value.
         * @param ver Version.
         * @param mvccVer MVCC version.
         * @param newMvccVer New MVCC version.
         * @return {@code True} if new value was inserted.
         * @throws IgniteCheckedException If failed.
         */
        boolean mvccInitialValue(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param mvccVer MVCC version.
         * @param newMvccVer New MVCC version.
         * @param txState Tx state hint for the mvcc version.
         * @param newTxState Tx state hint for the new mvcc version.
         * @return {@code True} if new value was inserted.
         * @throws IgniteCheckedException If failed.
         */
        boolean mvccInitialValueIfAbsent(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer,
            byte txState,
            byte newTxState) throws IgniteCheckedException;

        /**
         *
         * @param cctx Grid cache context.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expiration time.
         * @param mvccVer Mvcc version.
         * @param newMvccVer New mvcc version.
         * @return {@code true} on success.
         * @throws IgniteCheckedException, if failed.
         */
        boolean mvccUpdateRowWithPreloadInfo(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer,
            byte mvccTxState,
            byte newMvccTxState) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param mvccSnapshot MVCC snapshot.
         * @param filter Filter.
         * @param entryProc Entry processor.
         * @param invokeArgs Entry processor invoke arguments.
         * @param primary {@code True} if update is executed on primary node.
         * @param needHistory Flag to collect history.
         * @param noCreate Flag indicating that row should not be created if absent.
         * @param retVal Flag to return previous value.
         * @return Update result.
         * @throws IgniteCheckedException If failed.
         */
        MvccUpdateResult mvccUpdate(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccSnapshot mvccSnapshot,
            @Nullable CacheEntryPredicate filter,
            EntryProcessor entryProc,
            Object[] invokeArgs,
            boolean primary,
            boolean needHistory,
            boolean noCreate,
            boolean retVal) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param mvccSnapshot MVCC snapshot.
         * @param filter Filter.
         * @param primary {@code True} if update is executed on primary node.
         * @param needHistory Flag to collect history.
         * @param retVal Flag to return previous value.
         * @return List of transactions to wait for.
         * @throws IgniteCheckedException If failed.
         */
        MvccUpdateResult mvccRemove(
            GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot mvccSnapshot,
            @Nullable CacheEntryPredicate filter,
            boolean primary,
            boolean needHistory,
            boolean retVal) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param mvccSnapshot MVCC snapshot.
         * @return List of transactions to wait for.
         * @throws IgniteCheckedException If failed.
         */
        MvccUpdateResult mvccLock(
            GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param primary {@code True} if update is executed on primary node.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param mvccSnapshot MVCC snapshot.
         * @return Update result.
         * @throws IgniteCheckedException If failed.
         */
        @Nullable GridLongList mvccUpdateNative(
                GridCacheContext cctx,
                boolean primary,
                KeyCacheObject key,
                CacheObject val,
                GridCacheVersion ver,
                long expireTime,
                MvccSnapshot mvccSnapshot) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param primary {@code True} if update is executed on primary node.
         * @param key Key.
         * @param mvccSnapshot MVCC snapshot.
         * @return List of transactions to wait for.
         * @throws IgniteCheckedException If failed.
         */
        @Nullable GridLongList mvccRemoveNative(GridCacheContext cctx,
                                      boolean primary,
                                      KeyCacheObject key,
                                      MvccSnapshot mvccSnapshot) throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @throws IgniteCheckedException If failed.
         */
        void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException;

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
         * Returns iterator over the all row versions for the given key.
         *
         * @param cctx Cache context.
         * @param key Key.
         * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
         * @return Iterator over all versions.
         * @throws IgniteCheckedException If failed.
         */
        GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx, KeyCacheObject key, Object x)
            throws IgniteCheckedException;

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @return Data row.
         * @throws IgniteCheckedException If failed.
         */
        public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot snapshot)
            throws IgniteCheckedException;

        /**
         * For testing only.
         *
         * @param cctx Cache context.
         * @param key Key.
         * @return All stored versions for given key.
         * @throws IgniteCheckedException If failed.
         */
        List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx, KeyCacheObject key)
            throws IgniteCheckedException;

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
         * @param mvccSnapshot MVCC snapshot.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot) throws IgniteCheckedException;

        /**
         * @param cacheId Cache ID.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException;

        /**
         * @param cacheId Cache ID.
         * @param mvccSnapshot Mvcc snapshot.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(int cacheId, MvccSnapshot mvccSnapshot)
            throws IgniteCheckedException;

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
         * @param cacheId Cache ID.
         * @param lower Lower bound.
         * @param upper Upper bound.
         * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
         * @param snapshot Mvcc snapshot.
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper, Object x, MvccSnapshot snapshot) throws IgniteCheckedException;

        /**
         * Destroys the tree associated with the store.
         *
         * @throws IgniteCheckedException If failed.
         */
        public void destroy() throws IgniteCheckedException;

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
         * @param cntr Counter.
         */
        void updateInitialCounter(long cntr);

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
         * @throws IgniteCheckedException
         */
        PendingEntriesTree pendingTree();
    }
}
