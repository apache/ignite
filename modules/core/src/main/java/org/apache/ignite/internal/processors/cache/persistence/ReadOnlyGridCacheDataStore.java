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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * todo CHECK with flag in gridcachedatastore
 */
public class ReadOnlyGridCacheDataStore implements CacheDataStore {
    /** */
    private final CacheDataStore delegate;

    /** */
    private final NoopRowStore rowStore;

    /** */
    private volatile PartitionUpdateCounter cntr;

    /** todo remove unused args */
    public ReadOnlyGridCacheDataStore(
        CacheGroupContext grp,
        GridCacheSharedContext ctx,
        CacheDataStore delegate,
        int grpId
    ) {
        this.delegate = delegate;

        try {
            rowStore = new NoopRowStore(grp, new NoopFreeList(grp.dataRegion(), ctx.kernalContext()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void reinit() {
        cntr = delegate.partUpdateCounter();

        assert cntr != null;

        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        return cntr.next();
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void resetUpdateCounter() {
        cntr.reset();
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        return cntr.reserve(delta);//delegate.getAndIncrementUpdateCounter(delta);
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        try {
            cntr.update(val);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean updateCounter(long start, long delta) {
        return cntr.update(start, delta);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return cntr.finalizeUpdateCounters();
    }

    /** {@inheritDoc} */
    @Override public int partId() {
        return delegate.partId();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        return delegate.cacheSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        return delegate.cacheSizes();
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        return delegate.fullSize();
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        delegate.updateSize(cacheId, delta);
    }

    /** {@inheritDoc} */
    @Override public boolean init() {
        return delegate.init();
    }

    /** {@inheritDoc} */
    @Override public long reservedCounter() {
        return delegate.reservedCounter();
    }

    /** {@inheritDoc} */
    @Override public @Nullable PartitionUpdateCounter partUpdateCounter() {
        return cntr;
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        return cntr.reserve(delta);
    }

    /** {@inheritDoc} */
    @Override public void updateInitialCounter(long start, long delta) {
        cntr.updateInitial(start, delta);
    }

    /** {@inheritDoc} */
    @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
        delegate.setRowCacheCleaner(rowCacheCleaner);
    }

    /** {@inheritDoc} */
    @Override public PendingEntriesTree pendingTree() {
        return delegate.pendingTree();
    }

    /** {@inheritDoc} */
    @Override public PartitionMetaStorage<SimpleDataRow> partStorage() {
        return delegate.partStorage();
    }

    /** {@inheritDoc} */
    @Override public void preload() throws IgniteCheckedException {
        delegate.preload();
    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        IgniteCacheOffheapManager.OffheapInvokeClosure clo
    ) throws IgniteCheckedException {
        // Assume we've performed an invoke operation on the B+ Tree and find nothing.
        // Emulating that always inserting/removing a new value.
        clo.call(null);
    }

    /** {@inheritDoc} */
    @Override public void remove(
        GridCacheContext cctx,
        KeyCacheObject key,
        int partId
    ) throws IgniteCheckedException {
        // todo think
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow createRow(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        @Nullable CacheDataRow oldRow
    ) {
        assert oldRow == null;

        if (key.partition() < 0)
            key.partition(delegate.partId());

        return new DataRow(key, val, ver, delegate.partId(), expireTime, cctx.cacheId());
    }

    /** {@inheritDoc} */
    @Override public void insertRows(Collection<DataRowCacheAware> rows, IgnitePredicateX<CacheDataRow> initPred){
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
        return delegate.cursor();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        return delegate.cursor(mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        return delegate.cursor(cacheId, mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
        KeyCacheObject upper) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        delegate.destroy();
    }

    /** {@inheritDoc} */
    @Override public void clear(int cacheId) throws IgniteCheckedException {
        // todo
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public RowStore rowStore() {
        return rowStore;
    }

    /** {@inheritDoc} */
    @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void update(GridCacheContext cctx, KeyCacheObject key, CacheObject val, GridCacheVersion ver,
        long expireTime, @Nullable CacheDataRow oldRow) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int cleanup(GridCacheContext cctx, @Nullable List<MvccLinkAwareSearchRow> cleanupRows) {
        // No-op.
        return 0;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        // todo think about evictions
        return null;
    }

    /** {@inheritDoc} */
    @Override public void removeWithTombstone(GridCacheContext cctx, KeyCacheObject key, GridCacheVersion ver,
        GridDhtLocalPartition part) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long tombstonesCount() {
        // todo think
        return delegate.tombstonesCount();
    }

    @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot snapshot) throws IgniteCheckedException {
        return delegate.mvccFind(cctx, key, snapshot);
    }

    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx,
        KeyCacheObject key) throws IgniteCheckedException {
        return delegate.mvccFindAllVersions(cctx, key);
    }

    @Override public boolean mvccInitialValue(GridCacheContext cctx, KeyCacheObject key, @Nullable CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer) {
        return false;
    }

    @Override public boolean mvccApplyHistoryIfAbsent(GridCacheContext cctx, KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist) {
        return false;
    }

    @Override public boolean mvccUpdateRowWithPreloadInfo(GridCacheContext cctx, KeyCacheObject key,
        @Nullable CacheObject val, GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer, byte mvccTxState, byte newMvccTxState) {
        return false;
    }

    @Override public MvccUpdateResult mvccUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccSnapshot mvccSnapshot, @Nullable CacheEntryPredicate filter,
        EntryProcessor entryProc, Object[] invokeArgs, boolean primary, boolean needHist, boolean noCreate,
        boolean needOldVal, boolean retVal, boolean keepBinary) {
        // todo empty result .. new MvccUpdateDataRow( PREV_NULL);
        assert false;

        return null;
    }

    @Override public MvccUpdateResult mvccRemove(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter, boolean primary, boolean needHistory, boolean needOldVal,
        boolean retVal) throws IgniteCheckedException {
        return delegate.mvccRemove(cctx, key, mvccSnapshot, filter, primary, needHistory, needOldVal, retVal);
    }

    @Override public MvccUpdateResult mvccLock(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        return delegate.mvccLock(cctx, key, mvccSnapshot);
    }

    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        delegate.mvccRemoveAll(cctx, key);
    }

    @Override public void mvccApplyUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx, KeyCacheObject key,
        CacheDataRowAdapter.RowData x) throws IgniteCheckedException {
        return delegate.mvccAllVersionsCursor(cctx, key, x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(boolean withTombstones) throws IgniteCheckedException {
        return delegate.cursor(withTombstones);
    }

    /** {@inheritDoc} */
    @Override
    public GridCursor<? extends CacheDataRow> cursor(CacheDataRowAdapter.RowData x) throws IgniteCheckedException {
        return delegate.cursor(x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        boolean withTombstones) throws IgniteCheckedException {
        return delegate.cursor(cacheId, withTombstones);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        CacheDataRowAdapter.RowData x) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper, x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        CacheDataRowAdapter.RowData x, MvccSnapshot snapshot, boolean withTombstones) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper, x, snapshot, withTombstones);
    }

    /** */
    private static class NoopRowStore extends RowStore {
        /**
         * @param grp Cache group.
         * @param freeList Free list.
         */
        public NoopRowStore(CacheGroupContext grp, FreeList freeList) {
            super(grp, freeList);
        }

        /** {@inheritDoc} */
        @Override public void removeRow(long link, IoStatisticsHolder statHolder) {
            // todo
        }

        /** {@inheritDoc} */
        @Override public void addRow(CacheDataRow row, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean updateRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public <S, R> void updateDataRow(long link, PageHandler<S, R> pageHnd, S arg,
            IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
            // No-op.
        }


    }

    /** */
    private static class NoopFreeList extends CacheFreeList {
        /** */
        public NoopFreeList(DataRegion region, GridKernalContext ctx) throws IgniteCheckedException {
            super(0, null, null, region, null, 0, false, null, ctx);
        }

        /** {@inheritDoc} */
        @Override public void insertDataRow(CacheDataRow row, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void insertDataRows(Collection<CacheDataRow> rows, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean updateDataRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) {
            // No-op.

            return true;
        }

        /** {@inheritDoc} */
        @Override public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) {
            // todo
        }

        /** {@inheritDoc} */
        @Override public void dumpStatistics(IgniteLogger log) {

        }

        /** {@inheritDoc} */
        @Override public Object updateDataRow(long link, PageHandler pageHnd, Object arg,
            IoStatisticsHolder statHolder) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void saveMetadata(IoStatisticsHolder statHolder) throws IgniteCheckedException {
            super.saveMetadata(statHolder);
        }
    }
}
