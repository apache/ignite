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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccMarkUpdatedRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteRebalanceIteratorImpl;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionImpl;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateDataRowNative;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.ResultType;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccFirstRowTreeClosure;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccMaxSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccMinSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccSnapshotSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_START_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;

/**
 *
 */
@SuppressWarnings("PublicInnerClass")
public class IgniteCacheOffheapManagerImpl implements IgniteCacheOffheapManager {
    /** */
    protected GridCacheSharedContext ctx;

    /** */
    protected CacheGroupContext grp;

    /** */
    protected IgniteLogger log;

    /** */
    private CacheDataStore locCacheDataStore;

    /** */
    protected final ConcurrentMap<Integer, CacheDataStore> partDataStores = new ConcurrentHashMap<>();

    /** */
    protected PendingEntriesTree pendingEntries;

    /** */
    private volatile boolean hasPendingEntries;

    /** */
    private final GridAtomicLong globalRmvId = new GridAtomicLong(U.currentTimeMillis() * 1000_000);

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private int updateValSizeThreshold;

    /** */
    protected GridStripedLock partStoreLock = new GridStripedLock(Runtime.getRuntime().availableProcessors());

    /** {@inheritDoc} */
    @Override public GridAtomicLong globalRemoveId() {
        return globalRmvId;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext ctx, CacheGroupContext grp) throws IgniteCheckedException {
        this.ctx = ctx;
        this.grp = grp;
        this.log = ctx.logger(getClass());

        updateValSizeThreshold = ctx.database().pageSize() / 2;

        if (grp.affinityNode()) {
            ctx.database().checkpointReadLock();

            try {
                initDataStructures();

                if (grp.isLocal())
                    locCacheDataStore = createCacheDataStore(0);
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /** {@inheritDoc} */
    public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException {
        if (cctx.affinityNode() && cctx.ttl().eagerTtlEnabled() && pendingEntries == null) {
            String name = "PendingEntries";

            long rootPage = allocateForTree();

            pendingEntries = new PendingEntriesTree(
                grp,
                name,
                grp.dataRegion().pageMemory(),
                rootPage,
                grp.reuseList(),
                true);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDataStructures() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stopCache(int cacheId, final boolean destroy) {
        if (destroy && grp.affinityNode())
            removeCacheData(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        try {
            for (CacheDataStore store : cacheDataStores())
                destroyCacheDataStore(store);

            if (pendingEntries != null)
                pendingEntries.destroy();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        busyLock.block();
    }

    /**
     * @param cacheId Cache ID.
     */
    private void removeCacheData(int cacheId) {
        assert grp.affinityNode();

        try {
            if (grp.sharedGroup()) {
                assert cacheId != CU.UNDEFINED_CACHE_ID;
                assert ctx.database().checkpointLockIsHeldByThread();

                for (CacheDataStore store : cacheDataStores())
                    store.clear(cacheId);

                if (pendingEntries != null) {
                    PendingRow row = new PendingRow(cacheId);

                    GridCursor<PendingRow> cursor = pendingEntries.find(row, row, PendingEntriesTree.WITHOUT_KEY);

                    while (cursor.next()) {
                        boolean res = pendingEntries.removex(cursor.get());

                        assert res;
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * @param part Partition.
     * @return Data store for given entry.
     */
    public CacheDataStore dataStore(GridDhtLocalPartition part) {
        if (grp.isLocal())
            return locCacheDataStore;
        else {
            assert part != null;

            return part.dataStore();
        }
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(int cacheId) {
        long size = 0;

        for (CacheDataStore store : cacheDataStores())
            size += store.cacheSize(cacheId);

        return size;
    }

    /** {@inheritDoc} */
    @Override public int totalPartitionEntriesCount(int p) {
        if (grp.isLocal())
            return locCacheDataStore.fullSize();
        else {
            GridDhtLocalPartition part = grp.topology().localPartition(p, AffinityTopologyVersion.NONE, false, true);

            return part != null ? part.dataStore().fullSize() : 0;
        }
    }

    /**
     * @param p Partition.
     * @return Partition data.
     */
    @Nullable private CacheDataStore partitionData(int p) {
        if (grp.isLocal())
            return locCacheDataStore;
        else {
            GridDhtLocalPartition part = grp.topology().localPartition(p, AffinityTopologyVersion.NONE, false, true);

            return part != null ? part.dataStore() : null;
        }
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(
        int cacheId,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        if (grp.isLocal())
            return cacheEntriesCount(cacheId, 0);
        else {
            long cnt = 0;

            Iterator<CacheDataStore> it = cacheData(primary, backup, topVer);

            while (it.hasNext())
                cnt += it.next().cacheSize(cacheId);

            return cnt;
        }
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(int cacheId, int part) {
        CacheDataStore store = partitionData(part);

        return store == null ? 0 : store.cacheSize(cacheId);
    }

    /**
     * @param primary Primary data flag.
     * @param backup Primary data flag.
     * @param topVer Topology version.
     * @return Data stores iterator.
     */
    private Iterator<CacheDataStore> cacheData(boolean primary, boolean backup, AffinityTopologyVersion topVer) {
        assert primary || backup;

        if (grp.isLocal())
            return singletonIterator(locCacheDataStore);
        else {
            final Iterator<GridDhtLocalPartition> it = grp.topology().currentLocalPartitions().iterator();

            if (primary && backup) {
                return F.iterator(it, new IgniteClosure<GridDhtLocalPartition, CacheDataStore>() {
                    @Override public CacheDataStore apply(GridDhtLocalPartition part) {
                        return part.dataStore();
                    }
                }, true);
            }

            final Set<Integer> parts = primary ? grp.affinity().primaryPartitions(ctx.localNodeId(), topVer) :
                grp.affinity().backupPartitions(ctx.localNodeId(), topVer);

            return F.iterator(it, new IgniteClosure<GridDhtLocalPartition, CacheDataStore>() {
                    @Override public CacheDataStore apply(GridDhtLocalPartition part) {
                        return part.dataStore();
                    }
                }, true,
                new IgnitePredicate<GridDhtLocalPartition>() {
                    @Override public boolean apply(GridDhtLocalPartition part) {
                        return parts.contains(part.id());
                    }
                });
        }
    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        GridDhtLocalPartition part,
        OffheapInvokeClosure c)
        throws IgniteCheckedException {
        dataStore(part).invoke(cctx, key, c);
    }

    /** {@inheritDoc} */
    @Override public void update(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        GridDhtLocalPartition part,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        assert expireTime >= 0;

        dataStore(part).update(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccInitialValue(
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer) throws IgniteCheckedException {
        return dataStore(entry.localPartition()).mvccInitialValue(
            entry.context(),
            entry.key(),
            val,
            ver,
            expireTime,
            mvccVer,
            newMvccVer);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccUpdate(
        boolean primary,
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        assert entry.lockedByCurrentThread();

        return dataStore(entry.localPartition()).mvccUpdate(entry.context(),
            primary,
            entry.key(),
            val,
            ver,
            expireTime,
            mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccRemove(
        boolean primary,
        GridCacheMapEntry entry,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        assert entry.lockedByCurrentThread();

        return dataStore(entry.localPartition()).mvccRemove(entry.context(),
            primary,
            entry.key(),
            mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridLongList mvccUpdateNative(
        boolean primary,
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        return dataStore(entry.localPartition()).mvccUpdateNative(entry.context(),
            primary,
            entry.key(),
            val,
            ver,
            expireTime,
            mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridLongList mvccRemoveNative(
        boolean primary,
        GridCacheMapEntry entry,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        return dataStore(entry.localPartition()).mvccRemoveNative(entry.context(),
            primary,
            entry.key(),
            mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheMapEntry entry) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return;

        dataStore(entry.localPartition()).mvccRemoveAll(entry.context(), entry.key());
    }

    /** {@inheritDoc} */
    @Override public void remove(
        GridCacheContext cctx,
        KeyCacheObject key,
        int partId,
        GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        dataStore(part).remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override @Nullable public CacheDataRow read(GridCacheMapEntry entry)
        throws IgniteCheckedException {
        KeyCacheObject key = entry.key();

        assert grp.isLocal() || entry.localPartition() != null : entry;

        return dataStore(entry.localPartition()).find(entry.context(), key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheDataRow read(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(cctx, key);

        CacheDataRow row = dataStore != null ? dataStore.find(cctx, key) : null;

        assert row == null || row.value() != null : row;

        return row;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheDataRow mvccRead(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot ver)
        throws IgniteCheckedException {
        assert ver != null;

        CacheDataStore dataStore = dataStore(cctx, key);

        CacheDataRow row = dataStore != null ? dataStore.mvccFind(cctx, key, ver) : null;

        assert row == null || row.value() != null : row;

        return row;
    }

    /** {@inheritDoc} */
    @Override public List<T2<Object, MvccVersion>> mvccAllVersions(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(cctx, key);

        return dataStore != null ? dataStore.mvccFindAllVersions(cctx, key) :
            Collections.<T2<Object, MvccVersion>>emptyList();
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @return Data store.
     */
    @Nullable private CacheDataStore dataStore(GridCacheContext cctx, KeyCacheObject key) {
        if (grp.isLocal())
            return locCacheDataStore;

        GridDhtLocalPartition part = grp.topology().localPartition(cctx.affinity().partition(key), null, false);

        return part != null ? dataStore(part) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(GridCacheMapEntry entry) {
        try {
            return read(entry) != null;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to read value", e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCounterUpdated(int part, long cntr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionInitialCounterUpdated(int part, long cntr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long lastUpdatedPartitionCounter(int part) {
        return 0;
    }

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    @SuppressWarnings("unchecked")
    @Override public void clearCache(GridCacheContext cctx, boolean readers) {
        GridCacheVersion obsoleteVer = null;

        try (GridCloseableIterator<CacheDataRow> it = grp.isLocal() ? iterator(cctx.cacheId(), cacheDataStores().iterator(), null) :
            evictionSafeIterator(cctx.cacheId(), cacheDataStores().iterator())) {
            while (it.hasNext()) {
                cctx.shared().database().checkpointReadLock();

                try {
                    KeyCacheObject key = it.next().key();

                    try {
                        if (obsoleteVer == null)
                            obsoleteVer = ctx.versions().next();

                        GridCacheEntryEx entry = cctx.cache().entryEx(key);

                        entry.clear(obsoleteVer, readers);
                    }
                    catch (GridDhtInvalidPartitionException ignore) {
                        // Ignore.
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to clear cache entry: " + key, e);
                    }
                }
                finally {
                    cctx.shared().database().checkpointReadUnlock();
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to close iterator", e);
        }
    }

    /** {@inheritDoc} */
    @Override public int onUndeploy(ClassLoader ldr) {
        // TODO: GG-11141.
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        // TODO GG-10884.
        return 0;
    }

    /**
     * @param primary {@code True} if need return primary entries.
     * @param backup {@code True} if need return backup entries.
     * @param topVer Topology version to use.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<Cache.Entry<K, V>> cacheEntriesIterator(
        final GridCacheContext cctx,
        final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary) throws IgniteCheckedException {
        final Iterator<CacheDataRow> it = cacheIterator(cctx.cacheId(), primary, backup, topVer, null);

        return new GridCloseableIteratorAdapter<Cache.Entry<K, V>>() {
            /** */
            private CacheEntryImplEx next;

            @Override protected Cache.Entry<K, V> onNext() {
                CacheEntryImplEx ret = next;

                next = null;

                return ret;
            }

            @Override protected boolean onHasNext() {
                if (next != null)
                    return true;

                while (true) {
                    CacheDataRow nextRow = null;

                    if (it.hasNext())
                        nextRow = it.next();

                    if (nextRow != null) {
                        KeyCacheObject key = nextRow.key();
                        CacheObject val = nextRow.value();

                        Object key0 = cctx.unwrapBinaryIfNeeded(key, keepBinary, false);
                        Object val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false);

                        next = new CacheEntryImplEx(key0, val0, nextRow.version());

                        return true;
                    }

                    return false;
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<KeyCacheObject> cacheKeysIterator(int cacheId, final int part)
        throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        final GridCursor<? extends CacheDataRow> cur =
            data.cursor(cacheId, null, null, CacheDataRowAdapter.RowData.KEY_ONLY);

        return new GridCloseableIteratorAdapter<KeyCacheObject>() {
            /** */
            private KeyCacheObject next;

            @Override protected KeyCacheObject onNext() {
                KeyCacheObject res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                while (cur.next()) {
                    CacheDataRow row = cur.get();

                    next = row.key();

                    break;
                }

                return next != null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cacheIterator(
        int cacheId,
        boolean primary,
        boolean backups,
        final AffinityTopologyVersion topVer,
        @Nullable MvccSnapshot mvccSnapshot)
        throws IgniteCheckedException {
        return iterator(cacheId, cacheData(primary, backups, topVer), mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, int part,
        @Nullable MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(cacheId, singletonIterator(data), mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> partitionIterator(int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(CU.UNDEFINED_CACHE_ID, singletonIterator(data), null);
    }

    /**
     *
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @param mvccSnapshot Mvcc snapshot.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> iterator(final int cacheId,
        final Iterator<CacheDataStore> dataIt,
        final MvccSnapshot mvccSnapshot)
    {
        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private GridCursor<? extends CacheDataRow> cur;

            /** */
            private int curPart;

            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                while (true) {
                    if (cur == null) {
                        if (dataIt.hasNext()) {
                            CacheDataStore ds = dataIt.next();

                            curPart = ds.partId();

                            if (mvccSnapshot == null)
                                cur = cacheId == CU.UNDEFINED_CACHE_ID ? ds.cursor() : ds.cursor(cacheId);
                            else {
                                cur = cacheId == CU.UNDEFINED_CACHE_ID ?
                                    ds.cursor(mvccSnapshot) : ds.cursor(cacheId, mvccSnapshot);
                            }
                        }
                        else
                            break;
                    }

                    if (cur.next()) {
                        next = cur.get();
                        next.key().partition(curPart);

                        break;
                    }
                    else
                        cur = null;
                }

                return next != null;
            }
        };
    }

    /**
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> evictionSafeIterator(final int cacheId, final Iterator<CacheDataStore> dataIt) {
        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private GridCursor<? extends CacheDataRow> cur;

            /** */
            private GridDhtLocalPartition curPart;

            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                while (true) {
                    if (cur == null) {
                        if (dataIt.hasNext()) {
                            CacheDataStore ds = dataIt.next();

                            if (!reservePartition(ds.partId()))
                                continue;

                            cur = cacheId == CU.UNDEFINED_CACHE_ID ? ds.cursor() : ds.cursor(cacheId);
                        }
                        else
                            break;
                    }

                    if (cur.next()) {
                        next = cur.get();
                        next.key().partition(curPart.id());

                        break;
                    }
                    else {
                        cur = null;

                        releaseCurrentPartition();
                    }
                }

                return next != null;
            }

            /** */
            private void releaseCurrentPartition() {
                GridDhtLocalPartition p = curPart;

                assert p != null;

                curPart = null;

                p.release();
            }

            /**
             * @param partId Partition number.
             * @return {@code True} if partition was reserved.
             */
            private boolean reservePartition(int partId) {
                GridDhtLocalPartition p = grp.topology().localPartition(partId);

                if (p != null && p.reserve()) {
                    curPart = p;

                    return true;
                }

                return false;
            }

            /** {@inheritDoc} */
            @Override protected void onClose() throws IgniteCheckedException {
                if (curPart != null)
                    releaseCurrentPartition();
            }
        };
    }

    /**
     * @param item Item.
     * @return Single item iterator.
     */
    private <T> Iterator<T> singletonIterator(final T item) {
        return new Iterator<T>() {
            /** */
            private boolean hasNext = true;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return hasNext;
            }

            /** {@inheritDoc} */
            @Override public T next() {
                if (hasNext) {
                    hasNext = false;

                    return item;
                }

                throw new NoSuchElementException();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long allocateForTree() throws IgniteCheckedException {
        ReuseList reuseList = grp.reuseList();

        long pageId;

        if (reuseList == null || (pageId = reuseList.takeRecycledPage()) == 0L)
            pageId = grp.dataRegion().pageMemory().allocatePage(grp.groupId(), INDEX_PARTITION, FLAG_IDX);

        return pageId;
    }

    /** {@inheritDoc} */
    @Override public RootPage rootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException {
        long pageId = allocateForTree();

        return new RootPage(new FullPageId(pageId, grp.groupId()), true);
    }

    /** {@inheritDoc} */
    @Override public void dropRootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseListForIndex(String idxName) {
        return grp.reuseList();
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<CacheDataRow> reservedIterator(int part,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        final GridDhtLocalPartition loc = grp.topology().localPartition(part, topVer, false);

        if (loc == null || !loc.reserve())
            return null;

        // It is necessary to check state after reservation to avoid race conditions.
        if (loc.state() != OWNING) {
            loc.release();

            return null;
        }

        CacheDataStore data = partitionData(part);

        final GridCursor<? extends CacheDataRow> cur = data.cursor();

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                if (cur.next())
                    next = cur.get();

                return next != null;
            }

            @Override protected void onClose() throws IgniteCheckedException {
                assert loc != null && loc.state() == OWNING && loc.reservations() > 0;

                loc.release();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public IgniteRebalanceIterator rebalanceIterator(IgniteDhtDemandedPartitionsMap parts,
        final AffinityTopologyVersion topVer)
        throws IgniteCheckedException {

        final TreeMap<Integer, GridCloseableIterator<CacheDataRow>> iterators = new TreeMap<>();
        Set<Integer> missing = null;

        for (Integer p : parts.fullSet()) {
            GridCloseableIterator<CacheDataRow> partIter = reservedIterator(p, topVer);

            if (partIter == null) {
                if (missing == null)
                    missing = new HashSet<>();

                missing.add(p);
            }
            else
                iterators.put(p, partIter);
        }

        IgniteRebalanceIterator iter = new IgniteRebalanceIteratorImpl(iterators, historicalIterator(parts.historicalMap()));

        if (missing != null) {
            for (Integer p : missing)
                iter.setPartitionMissing(p);
        }

        return iter;
    }

    /**
     * @param partCntrs Partition counters map.
     * @return Historical iterator.
     */
    @Nullable protected IgniteHistoricalIterator historicalIterator(CachePartitionPartialCountersMap partCntrs)
        throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public final CacheDataStore createCacheDataStore(int p) throws IgniteCheckedException {
        CacheDataStore dataStore;

        partStoreLock.lock(p);

        try {
            assert !partDataStores.containsKey(p);

            dataStore = createCacheDataStore0(p);

            partDataStores.put(p, dataStore);
        }
        finally {
            partStoreLock.unlock(p);
        }

        return dataStore;
    }

    /**
     * @param p Partition.
     * @return Cache data store.
     * @throws IgniteCheckedException If failed.
     */
    protected CacheDataStore createCacheDataStore0(int p)
        throws IgniteCheckedException {
        final long rootPage = allocateForTree();

        CacheDataRowStore rowStore = new CacheDataRowStore(grp, grp.freeList(), p);

        String idxName = treeName(p);

        CacheDataTree dataTree = new CacheDataTree(
            grp,
            idxName,
            grp.reuseList(),
            rowStore,
            rootPage,
            true);

        return new CacheDataStoreImpl(p, idxName, rowStore, dataTree);
    }

    /** {@inheritDoc} */
    @Override public Iterable<CacheDataStore> cacheDataStores() {
        if (grp.isLocal())
            return Collections.singleton(locCacheDataStore);

        return new Iterable<CacheDataStore>() {
            @Override public Iterator<CacheDataStore> iterator() {
                return partDataStores.values().iterator();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public final void destroyCacheDataStore(CacheDataStore store) throws IgniteCheckedException {
        int p = store.partId();

        partStoreLock.lock(p);

        try {
            boolean removed = partDataStores.remove(p, store);

            assert removed;

            destroyCacheDataStore0(store);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            partStoreLock.unlock(p);
        }
    }

    /**
     * @param store Cache data store.
     * @throws IgniteCheckedException If failed.
     */
    protected void destroyCacheDataStore0(CacheDataStore store) throws IgniteCheckedException {
        store.destroy();
    }

    /**
     * @param p Partition.
     * @return Tree name for given partition.
     */
    protected final String treeName(int p) {
        return BPlusTree.treeName("p-" + p, "CacheData");
    }

    /** {@inheritDoc} */
    @Override public boolean expire(
        GridCacheContext cctx,
        IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c,
        int amount
    ) throws IgniteCheckedException {
        assert !cctx.isNear() : cctx.name();

        if (hasPendingEntries && pendingEntries != null) {
            GridCacheVersion obsoleteVer = null;

            long now = U.currentTimeMillis();

            GridCursor<PendingRow> cur;

            if (grp.sharedGroup())
                cur = pendingEntries.find(new PendingRow(cctx.cacheId()), new PendingRow(cctx.cacheId(), now, 0));
            else
                cur = pendingEntries.find(null, new PendingRow(CU.UNDEFINED_CACHE_ID, now, 0));

            if (!cur.next())
                return false;

            int cleared = 0;

            cctx.shared().database().checkpointReadLock();

            try {
                do {
                    PendingRow row = cur.get();

                    if (amount != -1 && cleared > amount)
                        return true;

                    if (row.key.partition() == -1)
                        row.key.partition(cctx.affinity().partition(row.key));

                    assert row.key != null && row.link != 0 && row.expireTime != 0 : row;

                    if (pendingEntries.removex(row)) {
                        if (obsoleteVer == null)
                            obsoleteVer = ctx.versions().next();

                        c.apply(cctx.cache().entryEx(row.key), obsoleteVer);
                    }

                    cleared++;
                }
                while (cur.next());
            }
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public long expiredSize() throws IgniteCheckedException {
        return pendingEntries != null ? pendingEntries.size() : 0;
    }

    /**
     *
     */
    protected class CacheDataStoreImpl implements CacheDataStore {
        /** */
        private final int partId;

        /** Tree name. */
        private String name;

        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final CacheDataTree dataTree;

        /** Update counter. */
        protected final AtomicLong cntr = new AtomicLong();

        /** Partition size. */
        private final AtomicLong storageSize = new AtomicLong();

        /** */
        private final ConcurrentMap<Integer, AtomicLong> cacheSizes = new ConcurrentHashMap<>();

        /** Initial update counter. */
        protected long initCntr;

        /** Mvcc remove handler. */
        private final PageHandler<MvccVersion, Boolean> mvccUpdateMarker = new MarkUpdatedHandler();

        /**
         * @param partId Partition number.
         * @param name Name.
         * @param rowStore Row store.
         * @param dataTree Data tree.
         */
        public CacheDataStoreImpl(
            int partId,
            String name,
            CacheDataRowStore rowStore,
            CacheDataTree dataTree
        ) {
            this.partId = partId;
            this.name = name;
            this.rowStore = rowStore;
            this.dataTree = dataTree;
        }

        /**
         * @param cacheId Cache ID.
         */
        void incrementSize(int cacheId) {
            storageSize.incrementAndGet();

            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                if (size == null) {
                    AtomicLong old = cacheSizes.putIfAbsent(cacheId, size = new AtomicLong());

                    if (old != null)
                        size = old;
                }

                size.incrementAndGet();
            }
        }

        /**
         * @param cacheId Cache ID.
         */
        void decrementSize(int cacheId) {
            storageSize.decrementAndGet();

            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                if (size == null)
                    return;

                size.decrementAndGet();
            }
        }

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public int cacheSize(int cacheId) {
            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                return size != null ? (int)size.get() : 0;
            }

            return (int)storageSize.get();
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Long> cacheSizes() {
            if (!grp.sharedGroup())
                return null;

            Map<Integer, Long> res = new HashMap<>();

            for (Map.Entry<Integer, AtomicLong> e : cacheSizes.entrySet())
                res.put(e.getKey(), e.getValue().longValue());

            return res;
        }

        /** {@inheritDoc} */
        @Override public int fullSize() {
            return (int)storageSize.get();
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            return cntr.get();
        }

        /**
         * @param val Update index value.
         */
        @Override public void updateCounter(long val) {
            while (true) {
                long val0 = cntr.get();

                if (val0 >= val)
                    break;

                if (cntr.compareAndSet(val0, val))
                    break;
            }
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /**
         * @param cctx Cache context.
         * @param oldRow Old row.
         * @param dataRow New row.
         * @return {@code True} if it is possible to update old row data.
         * @throws IgniteCheckedException If failed.
         */
        private boolean canUpdateOldRow(GridCacheContext cctx, @Nullable CacheDataRow oldRow, DataRow dataRow)
            throws IgniteCheckedException {
            if (oldRow == null || cctx.queries().enabled() || grp.mvccEnabled())
                return false;

            if (oldRow.expireTime() != dataRow.expireTime())
                return false;

            // Use grp.sharedGroup() flag since it is possible cacheId is not yet set here.
            boolean sizeWithCacheId = grp.sharedGroup();

            int oldLen = oldRow.size();

            if (oldLen > updateValSizeThreshold)
                return false;

            int newLen = dataRow.size();

            return oldLen == newLen;
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                dataTree.invoke(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY, c);

                switch (c.operationType()) {
                    case PUT: {
                        assert c.newRow() != null : c;

                        CacheDataRow oldRow = c.oldRow();

                        finishUpdate(cctx, c.newRow(), oldRow);

                        break;
                    }

                    case REMOVE: {
                        CacheDataRow oldRow = c.oldRow();

                        finishRemove(cctx, key, oldRow);

                        break;
                    }

                    case NOOP:
                        break;

                    default:
                        assert false : c.operationType();
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow createRow(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            DataRow dataRow = new DataRow(key, val, ver, partId, expireTime, cacheId);

            if (canUpdateOldRow(cctx, oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow))
                dataRow.link(oldRow.link());
            else {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                rowStore.addRow(dataRow);
            }

            assert dataRow.link() != 0 : dataRow;

            if (grp.sharedGroup() && dataRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                dataRow.cacheId(cctx.cacheId());

            return dataRow;
        }

        /** {@inheritDoc} */
        @Override public boolean mvccInitialValue(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer)
            throws IgniteCheckedException
        {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);

                boolean newVal = false;

                // null is passed for loaded from store.
                MvccSnapshot mvccSnapshot;

                if (mvccVer == null) {
                    mvccSnapshot = new MvccSnapshotWithoutTxs(1L, MVCC_START_CNTR, 0L);

                    newVal = true;
                }
                else
                    mvccSnapshot = new MvccSnapshotWithoutTxs(mvccVer.coordinatorVersion(), mvccVer.counter(), 0);


                if (val != null)
                    val.valueBytes(coCtx);

                MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    key,
                    val,
                    ver,
                    expireTime,
                    mvccSnapshot,
                    newMvccVer,
                    partId,
                    false, // TODO IGNITE-7185
                    cctx);

                if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                    updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                    rowStore.addRow(updateRow);

                    updateRow.cacheId(cctx.cacheId());
                }
                else
                    rowStore.addRow(updateRow);

                if (grp.sharedGroup() && updateRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                    updateRow.cacheId(cctx.cacheId());

                if (newVal) {
                    int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                    GridCursor<CacheDataRow> cur = dataTree.find(
                        new MvccMaxSearchRow(cacheId, key),
                        new MvccMinSearchRow(cacheId, key),
                        CacheDataRowAdapter.RowData.KEY_ONLY);

                    while (cur.next()) {
                        CacheDataRow row = cur.get();

                        assert row.link() != 0;

                        boolean rmvd = dataTree.removex(row);

                        assert rmvd;

                        rowStore.removeRow(row.link());
                    }
                }

                dataTree.putx(updateRow);

                if (val != null) {
                    incrementSize(cctx.cacheId());

                    if (cctx.queries().enabled())
                        cctx.queries().store(updateRow, null, true, false);
                }
            }
            finally {
                busyLock.leaveBusy();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccUpdate(
            GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            assert mvccSnapshot != null;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    key,
                    val,
                    ver,
                    expireTime,
                    mvccSnapshot,
                    null,
                    partId,
                    primary,
                    cctx);

                dataTree.visit(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                if (res == ResultType.LOCKED // cannot update locked
                    || res == ResultType.VERSION_MISMATCH) // cannot update on write conflict
                    return updateRow;
                else if (res == ResultType.VERSION_FOUND) {
                    // Do nothing, except cleaning up not needed versions
                    cleanup(cctx, updateRow.cleanupRows());

                    return updateRow;
                }

                if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                    updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                    rowStore.addRow(updateRow);

                    updateRow.cacheId(cctx.cacheId());
                }
                else
                    rowStore.addRow(updateRow);

                boolean old = dataTree.putx(updateRow);

                assert !old;

                CacheDataRow oldRow = null;
                if (res == ResultType.PREV_NOT_NULL) {
                    oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    oldRow.key(key);

                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, mvccSnapshot);
                }
                else {
                    assert res == ResultType.PREV_NULL;

                    incrementSize(cctx.cacheId());
                }

                GridCacheQueryManager qryMgr = cctx.queries();

                if (qryMgr.enabled())
                    qryMgr.store(updateRow, null, true, false);

                updatePendingEntries(cctx, updateRow, oldRow);

                cleanup(cctx, updateRow.cleanupRows());

                return updateRow;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccRemove(GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            assert mvccSnapshot != null;
            assert primary || mvccSnapshot.activeTransactions().size() == 0 : mvccSnapshot;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);

                MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    key,
                    null,
                    null,
                    0,
                    mvccSnapshot,
                    null,
                    partId,
                    primary,
                    cctx);

                dataTree.visit(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                if (res == ResultType.LOCKED // cannot update locked
                    || res == ResultType.VERSION_MISMATCH) // cannot update on write conflict
                    return updateRow;
                else if (res == ResultType.VERSION_FOUND) {
                    // Do nothing, except cleaning up not needed versions
                    cleanup(cctx, updateRow.cleanupRows());

                    return updateRow;
                }
                else if (res == ResultType.PREV_NOT_NULL) {
                    decrementSize(cacheId);

                    CacheDataRow oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, mvccSnapshot);

                    clearPendingEntries(cctx, oldRow);
                }

                cleanup(cctx, updateRow.cleanupRows());

                return updateRow;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public GridLongList mvccUpdateNative(
            GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            assert mvccSnapshot != null;
            assert primary || mvccSnapshot.activeTransactions().size() == 0 : mvccSnapshot;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                MvccUpdateDataRowNative updateRow = new MvccUpdateDataRowNative(
                    key,
                    val,
                    ver,
                    expireTime,
                    mvccSnapshot,
                    null,
                    partId,
                    cctx);

                dataTree.iterate(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                if (res == ResultType.VERSION_FOUND) {
                    // Do nothing, except cleaning up not needed versions
                    cleanup(cctx, updateRow.cleanupRows());

                    return null;
                }

                if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                    updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                    rowStore.addRow(updateRow);

                    updateRow.cacheId(cctx.cacheId());
                }
                else
                    rowStore.addRow(updateRow);

                boolean old = dataTree.putx(updateRow);

                assert !old;

                CacheDataRow oldRow = null;
                if (res == ResultType.PREV_NOT_NULL) {
                    oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    oldRow.key(key);

                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, mvccSnapshot);
                }
                else {
                    assert res == ResultType.PREV_NULL;

                    incrementSize(cctx.cacheId());
                }

                GridCacheQueryManager qryMgr = cctx.queries();

                if (qryMgr.enabled())
                    qryMgr.store(updateRow, null, true, false);

                updatePendingEntries(cctx, updateRow, oldRow);

                cleanup(cctx, updateRow.cleanupRows());

                return updateRow.activeTransactions();
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public GridLongList mvccRemoveNative(GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            assert mvccSnapshot != null;
            assert primary || mvccSnapshot.activeTransactions().size() == 0 : mvccSnapshot;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);

                MvccUpdateDataRowNative updateRow = new MvccUpdateDataRowNative(
                    key,
                    null,
                    null,
                    0,
                    mvccSnapshot,
                    null,
                    partId,
                    cctx);

                dataTree.iterate(new MvccMaxSearchRow(cacheId, key) , new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                if (res == ResultType.VERSION_FOUND) {
                    assert !primary : updateRow;

                    // Do nothing, except cleaning up not needed versions
                    cleanup(cctx, updateRow.cleanupRows());

                    return null;
                }
                else if (res == ResultType.PREV_NOT_NULL) {
                    decrementSize(cacheId);

                    CacheDataRow oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, mvccSnapshot);

                    clearPendingEntries(cctx, oldRow);
                }

                cleanup(cctx, updateRow.cleanupRows());

                return updateRow.activeTransactions();
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            boolean cleanup = cctx.queries().enabled() || hasPendingEntries;

            GridCursor<CacheDataRow> cur = dataTree.find(
                new MvccMaxSearchRow(cacheId, key),
                new MvccMinSearchRow(cacheId, key),
                cleanup ? CacheDataRowAdapter.RowData.NO_KEY : CacheDataRowAdapter.RowData.LINK_ONLY
            );

            boolean first = true;

            while (cur.next()) {
                CacheDataRow row = cur.get();

                row.key(key);

                assert row.link() != 0 : row;

                boolean rmvd = dataTree.removex(row);

                assert rmvd : row;

                if (cleanup) {
                    if (cctx.queries().enabled())
                        cctx.queries().remove(key, row);

                    if (first)
                        clearPendingEntries(cctx, row);
                }

                rowStore.removeRow(row.link());

                if (first) {
                    if (row.newMvccCoordinatorVersion() == 0)
                        decrementSize(cctx.cacheId());

                    first = false;
                }
            }
        }

        /**
         * @param cctx Cache context.
         * @param cleanupRows Rows to cleanup.
         * @throws IgniteCheckedException If failed.
         */
        @Override public void cleanup(GridCacheContext cctx, @Nullable List<MvccLinkAwareSearchRow> cleanupRows)
            throws IgniteCheckedException {
            if (cleanupRows != null) {
                GridCacheQueryManager qryMgr = cctx.queries();

                for (int i = 0; i < cleanupRows.size(); i++) {
                    MvccLinkAwareSearchRow cleanupRow = cleanupRows.get(i);

                    assert cleanupRow.link() != 0 : cleanupRow;

                    CacheDataRow oldRow = dataTree.remove(cleanupRow);

                    if (oldRow != null) { // oldRow == null means it was cleaned by another cleanup process.
                        assert oldRow.mvccCounter() == cleanupRow.mvccCounter();

                        if (qryMgr.enabled())
                            qryMgr.remove(oldRow.key(), oldRow);

                        clearPendingEntries(cctx, oldRow);

                        rowStore.removeRow(cleanupRow.link());
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void update(GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            assert oldRow == null || oldRow.link() != 0L : oldRow;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                assert oldRow == null || oldRow.cacheId() == cacheId : oldRow;

                DataRow dataRow = new DataRow(key, val, ver, partId, expireTime, cacheId);

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                CacheDataRow old;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                if (canUpdateOldRow(cctx, oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow)) {
                    old = oldRow;

                    dataRow.link(oldRow.link());
                }
                else {
                    rowStore.addRow(dataRow);

                    assert dataRow.link() != 0 : dataRow;

                    if (grp.sharedGroup() && dataRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                        dataRow.cacheId(cctx.cacheId());

                    if (oldRow != null) {
                        old = oldRow;

                        dataTree.putx(dataRow);
                    }
                    else
                        old = dataTree.put(dataRow);
                }

                finishUpdate(cctx, dataRow, old);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param newRow New row.
         * @param oldRow Old row if available.
         * @throws IgniteCheckedException If failed.
         */
        private void finishUpdate(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow oldRow)
            throws IgniteCheckedException {
            if (oldRow == null)
                incrementSize(cctx.cacheId());

            KeyCacheObject key = newRow.key();

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.store(newRow, oldRow, true, false);

            updatePendingEntries(cctx, newRow, oldRow);

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (newRow.link() != oldRow.link())
                    rowStore.removeRow(oldRow.link());
            }

            updateIgfsMetrics(cctx, key, (oldRow != null ? oldRow.value() : null), newRow.value());
        }

        /**
         * @param cctx Cache context.
         * @param newRow
         * @param oldRow
         * @throws IgniteCheckedException If failed.
         */
        private void updatePendingEntries(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow oldRow)
            throws IgniteCheckedException
        {
            long expireTime = newRow.expireTime();

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (pendingEntries != null && oldRow.expireTime() != 0)
                    pendingEntries.removex(new PendingRow(cacheId, oldRow.expireTime(), oldRow.link()));
            }

            if (pendingEntries != null && expireTime != 0) {
                pendingEntries.putx(new PendingRow(cacheId, expireTime, newRow.link()));

                hasPendingEntries = true;
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                CacheDataRow oldRow = dataTree.remove(new SearchRow(cacheId, key));

                finishRemove(cctx, key, oldRow);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param oldRow Removed row.
         * @throws IgniteCheckedException If failed.
         */
        private void finishRemove(GridCacheContext cctx, KeyCacheObject key, @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            if (oldRow != null) {
                clearPendingEntries(cctx, oldRow);

                decrementSize(cctx.cacheId());
            }

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.remove(key, oldRow);

            if (oldRow != null)
                rowStore.removeRow(oldRow.link());

            updateIgfsMetrics(cctx, key, (oldRow != null ? oldRow.value() : null), null);
        }

        /**
         * @param cctx
         * @param oldRow
         * @throws IgniteCheckedException
         */
        private void clearPendingEntries(GridCacheContext cctx, CacheDataRow oldRow)
            throws IgniteCheckedException {
            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            assert oldRow.link() != 0 : oldRow;
            assert cacheId == CU.UNDEFINED_CACHE_ID || oldRow.cacheId() == cacheId :
                "Incorrect cache ID [expected=" + cacheId + ", actual=" + oldRow.cacheId() + "].";

            if (pendingEntries != null && oldRow.expireTime() != 0)
                pendingEntries.removex(new PendingRow(cacheId, oldRow.expireTime(), oldRow.link()));
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            CacheDataRow row;

            if (grp.mvccEnabled()) {
                MvccFirstRowTreeClosure clo = new MvccFirstRowTreeClosure(cctx);

                dataTree.iterate(
                    new MvccMaxSearchRow(cacheId, key),
                    new MvccMinSearchRow(cacheId, key),
                    clo
                );

                row = clo.row();
            }
            else
                row = dataTree.findOne(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY);

            afterRowFound(row, key);

            return row;
        }

        /** {@inheritDoc} */
        @Override public List<T2<Object, MvccVersion>> mvccFindAllVersions(
            GridCacheContext cctx,
            KeyCacheObject key)
            throws IgniteCheckedException
        {
            assert grp.mvccEnabled();

            // Note: this method is intended for testing only.

            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            GridCursor<CacheDataRow> cur = dataTree.find(
                new MvccMaxSearchRow(cacheId, key),
                new MvccMinSearchRow(cacheId, key)
            );

            List<T2<Object, MvccVersion>> res = new ArrayList<>();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                MvccVersion mvccCntr = new MvccVersionImpl(row.mvccCoordinatorVersion(), row.mvccCounter());

                CacheObject val = row.value();

                Object val0 = val != null ? val.value(cctx.cacheObjectContext(), false) : null;

                res.add(new T2<>(val0, mvccCntr));
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow mvccFind(GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot snapshot) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            MvccSnapshotSearchRow clo = new MvccSnapshotSearchRow(cctx, key, snapshot);

            dataTree.iterate(
                clo,
                new MvccMinSearchRow(cacheId, key),
                clo
            );

            CacheDataRow row = clo.row();

            afterRowFound(row, key);

            return row;
        }

        /**
         * @param row Row.
         * @param key Key.
         * @throws IgniteCheckedException If failed.
         */
        private void afterRowFound(@Nullable CacheDataRow row, KeyCacheObject key) throws IgniteCheckedException {
            if (row != null) {
                row.key(key);

                grp.dataRegion().evictionTracker().touchPage(row.link());
            }
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            return dataTree.find(null, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
            return dataTree.find(null, null, x);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot)
            throws IgniteCheckedException {

            if (mvccSnapshot != null) {
                assert grp.mvccEnabled();

                // TODO replace with TreeRowClosure
                return new MvccCursor(grp.singleCacheContext(), dataTree.find(null, null), mvccSnapshot);
            }

            return dataTree.find(null, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
            return cursor(cacheId, null, null);
        }

        /** {@inheritDoc}
         * @param cacheId*/
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            return cursor(cacheId, null, null, null, mvccSnapshot);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException {
            return cursor(cacheId, lower, upper, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper, Object x) throws IgniteCheckedException {
            return cursor(cacheId, lower, upper, null, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper, Object x, MvccSnapshot snapshot) throws IgniteCheckedException {
            SearchRow lowerRow;
            SearchRow upperRow;

            if (grp.sharedGroup()) {
                assert cacheId != CU.UNDEFINED_CACHE_ID;

                lowerRow = lower != null ? new SearchRow(cacheId, lower) : new SearchRow(cacheId);
                upperRow = upper != null ? new SearchRow(cacheId, upper) : new SearchRow(cacheId);
            }
            else {
                lowerRow = lower != null ? new SearchRow(CU.UNDEFINED_CACHE_ID, lower) : null;
                upperRow = upper != null ? new SearchRow(CU.UNDEFINED_CACHE_ID, upper) : null;
            }

            if (snapshot != null) {
                assert grp.mvccEnabled();

                GridCacheContext cctx =
                    grp.sharedGroup() ? grp.shared().cacheContext(cacheId) : grp.singleCacheContext();

                // TODO replace with TreeRowClosure
                return new MvccCursor(cctx, dataTree.find(lowerRow, upperRow, x), snapshot);
            }

            return dataTree.find(lowerRow, upperRow, x);
        }

        /** {@inheritDoc} */
        @Override public void destroy() throws IgniteCheckedException {
            final AtomicReference<IgniteCheckedException> exception = new AtomicReference<>();

            dataTree.destroy(new IgniteInClosure<CacheSearchRow>() {
                @Override public void apply(CacheSearchRow row) {
                    try {
                        rowStore.removeRow(row.link());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Fail remove row [link=" + row.link() + "]");

                        IgniteCheckedException ex = exception.get();

                        if (ex == null)
                            exception.set(e);
                        else
                            ex.addSuppressed(e);
                    }
                }
            });

            if (exception.get() != null)
                throw new IgniteCheckedException("Fail destroy store", exception.get());
        }

        /** {@inheritDoc} */
        @Override public void clear(int cacheId) throws IgniteCheckedException {
            assert cacheId != CU.UNDEFINED_CACHE_ID;
            assert ctx.database().checkpointLockIsHeldByThread();

            if (cacheSize(cacheId) == 0)
                return;

            Exception ex = null;

            GridCursor<? extends CacheDataRow> cur =
                cursor(cacheId, null, null, CacheDataRowAdapter.RowData.KEY_ONLY);

            while (cur.next()) {
                CacheDataRow row = cur.get();

                assert row.link() != 0 : row;

                try {
                    boolean res = dataTree.removex(row);

                    assert res : row;

                    rowStore.removeRow(row.link());

                    decrementSize(cacheId);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Fail remove row [link=" + row.link() + "]");

                    if (ex == null)
                        ex = e;
                    else
                        ex.addSuppressed(e);
                }
            }

            if (ex != null)
                throw new IgniteCheckedException("Fail destroy store", ex);
        }

        /** {@inheritDoc} */
        @Override public RowStore rowStore() {
            return rowStore;
        }

        /**
         * @return Next update index.
         */
        @Override public long nextUpdateCounter() {
            return cntr.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public long initialUpdateCounter() {
            return initCntr;
        }

        /** {@inheritDoc} */
        @Override public void updateInitialCounter(long cntr) {
            if (updateCounter() < cntr)
                updateCounter(cntr);

            initCntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
            rowStore().setRowCacheCleaner(rowCacheCleaner);
        }

        /** {@inheritDoc} */
        @Override public void init(long size, long updCntr, @Nullable Map<Integer, Long> cacheSizes) {
            initCntr = updCntr;
            storageSize.set(size);

            cntr.set(updCntr);

            if (cacheSizes != null) {
                for (Map.Entry<Integer, Long> e : cacheSizes.entrySet())
                    this.cacheSizes.put(e.getKey(), new AtomicLong(e.getValue()));
            }
        }

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param oldVal Old value.
         * @param newVal New value.
         */
        private void updateIgfsMetrics(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject oldVal,
            CacheObject newVal
        ) {
            GridCacheAdapter cache = cctx.cache();
            if (cache == null) {
                return;
            }

            // In case we deal with IGFS cache, count updated data
            if (cache.isIgfsDataCache() &&
                !cctx.isNear() &&
                ctx.kernalContext()
                    .igfsHelper()
                    .isIgfsBlockKey(key.value(cctx.cacheObjectContext(), false))) {
                int oldSize = valueLength(cctx, oldVal);
                int newSize = valueLength(cctx, newVal);

                int delta = newSize - oldSize;

                if (delta != 0)
                    cache.onIgfsDataSizeChanged(delta);
            }
        }

        /**
         * Isolated method to get length of IGFS block.
         *
         * @param cctx Cache context.
         * @param val Value.
         * @return Length of value.
         */
        private int valueLength(GridCacheContext cctx, @Nullable CacheObject val) {
            if (val == null)
                return 0;

            byte[] bytes = val.value(cctx.cacheObjectContext(), false);

            if (bytes != null)
                return bytes.length;
            else
                return 0;
        }

        /** */
        private final class MvccCursor implements GridCursor<CacheDataRow> {
            private final GridCacheContext cctx;
            /** */

            private final GridCursor<? extends CacheDataRow> cur;

            /** */
            private final MvccSnapshot snapshot;

            /** */
            private CacheDataRow curRow;

            /**
             * @param cctx
             * @param cur Cursor.
             * @param snapshot MVCC snapshot.
             */
            MvccCursor(GridCacheContext cctx,
                GridCursor<? extends CacheDataRow> cur, MvccSnapshot snapshot) {
                this.cctx = cctx;
                this.cur = cur;
                this.snapshot = snapshot;
            }

            @Override public boolean next() throws IgniteCheckedException {
                KeyCacheObject curKey = curRow != null ? curRow.key() : null;

                curRow = null;

                while (cur.next()) {
                    CacheDataRow row = cur.get();

                    if (curKey != null && row.key().equals(curKey))
                        continue;

                    long rowCrd = row.mvccCoordinatorVersion();
                    long rowCntr = row.mvccCounter();
                    long rowNewCrd = row.newMvccCoordinatorVersion();
                    long rowNewCntr = row.newMvccCounter();

                    if (isVisible(cctx, snapshot, rowCrd, rowCntr, rowNewCrd, rowNewCntr)) {
                        curRow = row;

                        break;
                    }
                }

                return curRow != null;
            }

            @Override public CacheDataRow get() throws IgniteCheckedException {
                return curRow;
            }
        }

    }

    /**
     * Mvcc remove handler.
     */
    private final class MarkUpdatedHandler extends PageHandler<MvccVersion, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc,
            MvccVersion newVer, int itemId) throws IgniteCheckedException {
            assert grp.mvccEnabled();

            DataPageIO iox = (DataPageIO)io;

            DataPagePayload data = iox.readPayload(pageAddr, itemId, grp.dataRegion().pageMemory().pageSize());

            assert data.payloadSize() >= MVCC_INFO_SIZE : "MVCC info should be fit on the very first data page.";

            long newCrd = iox.newMvccCoordinator(pageAddr, data.offset());
            long newCntr = iox.newMvccCounter(pageAddr, data.offset());

            assert newCrd == 0 || grp.shared().coordinators().state(newCrd, newCntr) == TxState.ABORTED;

            iox.markRemoved(pageAddr, data.offset(), newVer);

            if (isWalDeltaRecordNeeded(grp.dataRegion().pageMemory(), cacheId, pageId, page, ctx.wal(), walPlc))
                ctx.wal().log(new DataPageMvccMarkUpdatedRecord(cacheId, pageId, itemId,
                    newVer.coordinatorVersion(), newVer.counter()));

            return Boolean.TRUE;
        }
    }
}
