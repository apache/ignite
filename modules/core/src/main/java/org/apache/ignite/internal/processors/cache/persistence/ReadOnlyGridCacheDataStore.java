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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
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
import org.apache.ignite.internal.processors.cache.PartitionAtomicUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionMvccTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * todo CHECK with flag in gridcachedatastore
 */
public class ReadOnlyGridCacheDataStore implements CacheDataStore {
    /** */
    private static final boolean FAIL_NODE_ON_PARTITION_INCONSISTENCY = Boolean.getBoolean(
        IgniteSystemProperties.IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY);

    /** */
    private final CacheDataStore delegate;

    /** */
    private final NoopRowStore rowStore;

    /** */
    private final IgniteLogger log;

    /** */
    private final GridCacheSharedContext ctx;

    /** */
    private volatile PartitionUpdateCounter cntr;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final int partId;

    /**
     * @param grp Cache group.
     * @param ctx Context.
     * @param delegate Delegated cache data store, all read operations are performed in this store.
     * @param partId Partition ID.
     */
    public ReadOnlyGridCacheDataStore(
        CacheGroupContext grp,
        GridCacheSharedContext ctx,
        CacheDataStore delegate,
        int partId
    ) {
        this.delegate = delegate;
        this.grp = grp;
        this.partId = partId;
        this.ctx = ctx;
        this.log = ctx.logger(getClass());

        try {
            rowStore = new NoopRowStore(grp, new NoopFreeList(grp.dataRegion(), ctx.kernalContext()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void reinit() {
        PartitionUpdateCounter readCntr;

        if (grp.mvccEnabled())
            readCntr = new PartitionMvccTxUpdateCounterImpl();
        else if (grp.hasAtomicCaches() || !grp.persistenceEnabled())
            readCntr = new PartitionAtomicUpdateCounterImpl();
        else
            readCntr = new PartitionTxUpdateCounterImpl();

        PartitionUpdateCounter cntr0 = delegate.partUpdateCounter();

        if (cntr0 != null)
            readCntr.init(cntr0.get(), cntr0.getBytes());

        cntr = readCntr;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        // Fast-eviction is possible if delegate was not initialized.
        // todo assert !delegate.intiialized();

        return delegate.isEmpty();
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
    @Override public long updateCounter() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void resetUpdateCounter() {
        cntr.reset();
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        return cntr.reserve(delta);
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        try {
            cntr.update(val);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to update partition counter. " +
                "Most probably a node with most actual data is out of topology or data streamer is used " +
                "in preload mode (allowOverride=false) concurrently with cache transactions [grpName=" +
                grp.name() + ", partId=" + partId + ']', e);

            if (FAIL_NODE_ON_PARTITION_INCONSISTENCY)
                ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
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
    @Override public long reservedCounter() {
        return cntr.reserved();
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
    @Override public int partId() {
        return delegate.partId();
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
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        delegate.updateSize(cacheId, delta);
    }

    /** {@inheritDoc} */
    @Override public boolean init() {
        // return delegate.init();

        return true;
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
        // No-op.
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
        return 0;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
        return delegate.cursor(x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
        return delegate.cursor(cacheId);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        Object x) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper, x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        Object x, MvccSnapshot snapshot) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper, x, snapshot);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot snapshot) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx,
        KeyCacheObject key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean mvccInitialValue(GridCacheContext cctx, KeyCacheObject key, @Nullable CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean mvccApplyHistoryIfAbsent(GridCacheContext cctx, KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean mvccUpdateRowWithPreloadInfo(GridCacheContext cctx, KeyCacheObject key,
        @Nullable CacheObject val, GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer, byte mvccTxState, byte newMvccTxState) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccSnapshot mvccSnapshot, @Nullable CacheEntryPredicate filter,
        EntryProcessor entryProc, Object[] invokeArgs, boolean primary, boolean needHist, boolean noCreate,
        boolean needOldVal, boolean retVal, boolean keepBinary) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccRemove(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter, boolean primary, boolean needHistory, boolean needOldVal,
        boolean retVal) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccLock(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot mvccSnapshot) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void mvccApplyUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx, KeyCacheObject key,
        Object x) {
        throw new UnsupportedOperationException();
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
            // No-op.
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
            return true;
        }

        /** {@inheritDoc} */
        @Override public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void dumpStatistics(IgniteLogger log) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object updateDataRow(long link, PageHandler pageHnd, Object arg, IoStatisticsHolder statHolder) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void saveMetadata(IoStatisticsHolder statHolder) {
            // No-op.
        }
    }
}
