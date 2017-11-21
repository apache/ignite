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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersionWithoutTxs;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCounter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccLongList;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.MvccCleanupRow;
import org.apache.ignite.internal.processors.cache.tree.MvccKeyMaxVersionBound;
import org.apache.ignite.internal.processors.cache.tree.MvccKeyMinVersionBound;
import org.apache.ignite.internal.processors.cache.tree.MvccRemoveRow;
import org.apache.ignite.internal.processors.cache.tree.MvccSearchRow;
import org.apache.ignite.internal.processors.cache.tree.MvccUpdateRow;
import org.apache.ignite.internal.processors.cache.tree.MvccVersionBasedSearchRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
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
import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.MVCC_START_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.unmaskCoordinatorVersion;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.versionForRemovedValue;

/**
 *
 */
@SuppressWarnings("PublicInnerClass")
public class IgniteCacheOffheapManagerImpl implements IgniteCacheOffheapManager {
    // TODO IGNITE-3478
    public final boolean IGNITE_FAKE_MVCC_STORAGE = IgniteSystemProperties.getBoolean("IGNITE_FAKE_MVCC_STORAGE", false);

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
    private GridStripedLock partStoreLock = new GridStripedLock(Runtime.getRuntime().availableProcessors());

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

        // TODO IGNITE-3478
        if (grp.mvccEnabled())
            log.info("IgniteCacheOffheapManagerImpl start, fakeMvcc=" + IGNITE_FAKE_MVCC_STORAGE);

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
    public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException{
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
        MvccCoordinatorVersion mvccVer) throws IgniteCheckedException {
        return dataStore(entry.localPartition()).mvccInitialValue(
            entry.context(),
            entry.key(),
            val,
            ver,
            expireTime,
            mvccVer);
    }

    /** {@inheritDoc} */
    @Override public GridLongList mvccUpdate(
        boolean primary,
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccCoordinatorVersion mvccVer) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        return dataStore(entry.localPartition()).mvccUpdate(entry.context(),
            primary,
            entry.key(),
            val,
            ver,
            expireTime,
            mvccVer);
    }

    /** {@inheritDoc} */
    @Override public GridLongList mvccRemove(
        boolean primary,
        GridCacheMapEntry entry,
        MvccCoordinatorVersion mvccVer
    ) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        return dataStore(entry.localPartition()).mvccRemove(entry.context(),
            primary,
            entry.key(),
            mvccVer);
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
    @Nullable @Override public CacheDataRow mvccRead(GridCacheContext cctx, KeyCacheObject key, MvccCoordinatorVersion ver)
        throws IgniteCheckedException {
        assert ver != null;

        CacheDataStore dataStore = dataStore(cctx, key);

        CacheDataRow row = dataStore != null ? dataStore.mvccFind(cctx, key, ver) : null;

        assert row == null || row.value() != null : row;

        return row;
    }

    /** {@inheritDoc} */
    @Override public List<T2<Object, MvccCounter>> mvccAllVersions(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(cctx, key);

        return dataStore != null ? dataStore.mvccFindAllVersions(cctx, key) :
            Collections.<T2<Object, MvccCounter>>emptyList();
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

                try{
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
        };
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<KeyCacheObject> cacheKeysIterator(int cacheId, final int part) throws IgniteCheckedException {
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

                if (cur.next()) {
                    CacheDataRow row = cur.get();

                    next = row.key();
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
        @Nullable MvccCoordinatorVersion mvccVer)
        throws IgniteCheckedException {
        return iterator(cacheId, cacheData(primary, backups, topVer), mvccVer);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(cacheId, singletonIterator(data), null);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> partitionIterator(int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(CU.UNDEFINED_CACHE_ID, singletonIterator(data), null);
    }

    /**
     * TODO IGNITE-3478, review usages, pass correct version.
     *
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @param mvccVer Mvcc version.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> iterator(final int cacheId,
        final Iterator<CacheDataStore> dataIt,
        final MvccCoordinatorVersion mvccVer)
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

                            cur = ds.cursor(cacheId, mvccVer);
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
    @Override public IgniteRebalanceIterator rebalanceIterator(int part, AffinityTopologyVersion topVer, Long partCntr)
        throws IgniteCheckedException {
        final GridIterator<CacheDataRow> it = partitionIterator(part);

        return new IgniteRebalanceIterator() {
            @Override public boolean historical() {
                return false;
            }

            @Override public boolean hasNextX() throws IgniteCheckedException {
                return it.hasNextX();
            }

            @Override public CacheDataRow nextX() throws IgniteCheckedException {
                return it.nextX();
            }

            @Override public void removeX() throws IgniteCheckedException {
                it.removeX();
            }

            @Override public Iterator<CacheDataRow> iterator() {
                return it.iterator();
            }

            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public CacheDataRow next() {
                return it.next();
            }

            @Override public void close() {

            }

            @Override public boolean isClosed() {
                return false;
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
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
            if (oldRow == null || cctx.queries().enabled())
                return false;

            if (oldRow.expireTime() != dataRow.expireTime())
                return false;

            // Use grp.sharedGroup() flag since it is possible cacheId is not yet set here.
            boolean sizeWithCacheId = grp.sharedGroup();

            int oldLen = FreeListImpl.getRowSize(oldRow, sizeWithCacheId);

            if (oldLen > updateValSizeThreshold)
                return false;

            int newLen = FreeListImpl.getRowSize(dataRow, sizeWithCacheId);

            return oldLen == newLen;
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

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
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException
        {
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
            MvccCoordinatorVersion mvccVer)
            throws IgniteCheckedException
        {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);

                MvccUpdateRow updateRow;

                boolean newVal = false;

                // TODO IGNITE-3478: null is passed for loaded from store, need handle better.
                if (mvccVer == null) {
                    mvccVer = new MvccCoordinatorVersionWithoutTxs(1L, MVCC_START_CNTR, 0L);

                    newVal = true;
                }
                else
                    assert val != null || versionForRemovedValue(mvccVer.coordinatorVersion());

                if (val != null) {
                    val.valueBytes(coCtx);

                    updateRow = new MvccUpdateRow(
                        key,
                        val,
                        ver,
                        expireTime,
                        mvccVer,
                        false,
                        partId,
                        cacheId);
                }
                else {
                    updateRow = new MvccRemoveRow(
                        key,
                        mvccVer,
                        false,
                        partId,
                        cacheId);
                }

                if (grp.sharedGroup() && updateRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                    updateRow.cacheId(cctx.cacheId());

                rowStore.addRow(updateRow);

                if (newVal) {
                    GridCursor<CacheDataRow> cur = dataTree.find(
                        new MvccSearchRow(cacheId, key, Long.MAX_VALUE, Long.MAX_VALUE),
                        new MvccSearchRow(cacheId, key, 1L, 1L),
                        CacheDataRowAdapter.RowData.KEY_ONLY);

                    while (cur.next()) {
                        CacheDataRow row = cur.get();

                        assert row.link() != 0;

                        boolean rmvd = dataTree.removex(row);

                        assert rmvd;

                        rowStore.removeRow(row.link());
                    }
                }

                boolean old = dataTree.putx(updateRow);

                assert !old;

                if (val != null) {
                    incrementSize(cctx.cacheId());

                    if (cctx.queries().enabled())
                        cctx.queries().store(updateRow, mvccVer, null, true);
                }
            }
            finally {
                busyLock.leaveBusy();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public GridLongList mvccUpdate(
            GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccCoordinatorVersion mvccVer) throws IgniteCheckedException {
            assert mvccVer != null;
            assert primary || mvccVer.activeTransactions().size() == 0 : mvccVer;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                boolean needOld = hasPendingEntries || cctx.isQueryEnabled();

                MvccUpdateRow updateRow = new MvccUpdateRow(
                    key,
                    val,
                    ver,
                    expireTime,
                    mvccVer,
                    needOld,
                    partId,
                    cacheId);

                dataTree.iterate(updateRow, new MvccKeyMinVersionBound(cacheId, key), updateRow);

                MvccUpdateRow.UpdateResult res = updateRow.updateResult();

                if (res == MvccUpdateRow.UpdateResult.VERSION_FOUND) {
                    assert !primary : updateRow;

                    cleanup(cctx, updateRow.cleanupRows(), false);

                    return null;
                }
                else {
                    if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                        updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                        rowStore.addRow(updateRow);

                        updateRow.cacheId(cacheId);
                    }
                    else
                        rowStore.addRow(updateRow);

                    boolean old = dataTree.putx(updateRow);

                    assert !old;

                    if (res == MvccUpdateRow.UpdateResult.PREV_NULL)
                        incrementSize(cctx.cacheId());
                }

                CacheDataRow oldRow = updateRow.oldRow();

                if (oldRow != null)
                    oldRow.key(key);

                GridCacheQueryManager qryMgr = cctx.queries();

                if (qryMgr.enabled())
                    qryMgr.store(updateRow, mvccVer, oldRow, true);

                updatePendingEntries(cctx, updateRow, oldRow);

                cleanup(cctx, updateRow.cleanupRows(), false);

                return updateRow.activeTransactions();
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public GridLongList mvccRemove(GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            MvccCoordinatorVersion mvccVer) throws IgniteCheckedException {
            assert mvccVer != null;
            assert primary || mvccVer.activeTransactions().size() == 0 : mvccVer;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);

                boolean needOld = hasPendingEntries || cctx.isQueryEnabled();

                MvccRemoveRow updateRow = new MvccRemoveRow(
                    key,
                    mvccVer,
                    needOld,
                    partId,
                    cacheId);

                dataTree.iterate(updateRow, new MvccKeyMinVersionBound(cacheId, key), updateRow);

                MvccUpdateRow.UpdateResult res = updateRow.updateResult();

                if (res == MvccUpdateRow.UpdateResult.VERSION_FOUND) {
                    assert !primary : updateRow;

                    cleanup(cctx, updateRow.cleanupRows(), false);

                    return null;
                }
                else {
                    if (res == MvccUpdateRow.UpdateResult.PREV_NOT_NULL)
                        decrementSize(cacheId);

                    long rmvRowLink = cleanup(cctx, updateRow.cleanupRows(), true);

                    if (rmvRowLink == 0) {
                        if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                            updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                            rowStore.addRow(updateRow);

                            updateRow.cacheId(cacheId);
                        }
                        else
                            rowStore.addRow(updateRow);
                    }
                    else
                        updateRow.link(rmvRowLink);

                    assert updateRow.link() != 0L;

                    boolean old = dataTree.putx(updateRow);

                    assert !old;
                }

                CacheDataRow oldRow = updateRow.oldRow();

                if (oldRow != null) {
                    assert oldRow.link() != 0 : oldRow;

                    oldRow.key(key);

                    GridCacheQueryManager qryMgr = cctx.queries();

                    if (qryMgr.enabled())
                        qryMgr.remove(key, oldRow, mvccVer);

                    clearPendingEntries(cctx, oldRow);
                }

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
                new MvccSearchRow(cacheId, key, Long.MAX_VALUE, Long.MAX_VALUE),
                new MvccSearchRow(cacheId, key, 1, 1),
                cleanup ? CacheDataRowAdapter.RowData.NO_KEY : CacheDataRowAdapter.RowData.LINK_ONLY);

            boolean first = true;

            while (cur.next()) {
                CacheDataRow row = cur.get();

                row.key(key);

                assert row.link() != 0 : row;

                boolean rmvd = dataTree.removex(row);

                assert rmvd : row;

                boolean rmvdVal = versionForRemovedValue(row.mvccCoordinatorVersion());

                if (cleanup && !rmvdVal) {
                    if (cctx.queries().enabled())
                        cctx.queries().remove(key, row, null);

                    if (first)
                        clearPendingEntries(cctx, row);
                }

                rowStore.removeRow(row.link());

                if (first) {
                    if (!rmvdVal)
                        decrementSize(cctx.cacheId());

                    first = false;
                }
            }
        }

        /**
         * @param cctx Cache context.
         * @param cleanupRows Rows to cleanup.
         * @param findRmv {@code True} if need keep removed row entry.
         * @return Removed row link of {@code 0} if not found.
         * @throws IgniteCheckedException If failed.
         */
        private long cleanup(GridCacheContext cctx, @Nullable List<MvccCleanupRow> cleanupRows, boolean findRmv)
            throws IgniteCheckedException {
            long rmvRowLink = 0;

            if (cleanupRows != null) {
                GridCacheQueryManager qryMgr = cctx.queries();

                for (int i = 0; i < cleanupRows.size(); i++) {
                    MvccCleanupRow cleanupRow = cleanupRows.get(i);

                    assert cleanupRow.link() != 0 : cleanupRow;

                    if (qryMgr.enabled() && !versionForRemovedValue(cleanupRow.mvccCoordinatorVersion())) {
                        CacheDataRow oldRow = dataTree.remove(cleanupRow);

                        assert oldRow != null : cleanupRow;

                        qryMgr.remove(oldRow.key(), oldRow, null);
                    }
                    else {
                        boolean rmvd = dataTree.removex(cleanupRow);

                        assert rmvd;
                    }

                    if (findRmv &&
                        rmvRowLink == 0 &&
                        versionForRemovedValue(cleanupRow.mvccCoordinatorVersion())) {
                        rmvRowLink = cleanupRow.link();
                    }
                    else
                        rowStore.removeRow(cleanupRow.link());
                }
            }

            return rmvRowLink;
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
                qryMgr.store(newRow, null, oldRow, true);

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
                qryMgr.remove(key, oldRow, null);

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
                MvccKeyMaxVersionBound searchRow = new MvccKeyMaxVersionBound(cacheId, key);

                dataTree.iterate(
                    searchRow,
                    new MvccKeyMinVersionBound(cacheId, key),
                    searchRow // Use the same instance as closure to do not create extra object.
                );

                row = searchRow.row();
            }
            else
                row = dataTree.findOne(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY);

            afterRowFound(row, key);

            return row;
        }

        /** {@inheritDoc} */
        @Override public List<T2<Object, MvccCounter>> mvccFindAllVersions(
            GridCacheContext cctx,
            KeyCacheObject key)
            throws IgniteCheckedException
        {
            assert grp.mvccEnabled();

            // Note: this method is intended for testing only.

            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            GridCursor<CacheDataRow> cur = dataTree.find(
                new MvccSearchRow(cacheId, key, Long.MAX_VALUE, Long.MAX_VALUE),
                new MvccSearchRow(cacheId, key, 1, 1));

            List<T2<Object, MvccCounter>> res = new ArrayList<>();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                MvccCounter mvccCntr = new MvccCounter(row.mvccCoordinatorVersion(), row.mvccCounter());

                CacheObject val = row.value();

                Object val0 = val != null ? val.value(cctx.cacheObjectContext(), false) : null;

                res.add(new T2<>(val0, mvccCntr));
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow mvccFind(GridCacheContext cctx,
            KeyCacheObject key,
            MvccCoordinatorVersion ver) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            MvccVersionBasedSearchRow lower = new MvccVersionBasedSearchRow(cacheId, key, ver);

            dataTree.iterate(
                lower,
                new MvccKeyMinVersionBound(cacheId, key),
                lower // Use the same instance as closure to do not create extra object.
            );

            CacheDataRow row = lower.row();

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
        @Override public GridCursor<? extends CacheDataRow> cursor(MvccCoordinatorVersion ver)
            throws IgniteCheckedException {

            if (ver != null) {
                assert grp.mvccEnabled();

                // TODO IGNITE-3478: more optimal cursor, e.g. pass some 'isVisible' closure.
                return new MvccCursor(dataTree.find(null, null), ver);
            }

            return dataTree.find(null, null);
        }

        /** {@inheritDoc}
         * @param cacheId*/
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
            return cursor(cacheId, null, null);
        }

        /** {@inheritDoc}
         * @param cacheId*/
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            MvccCoordinatorVersion ver) throws IgniteCheckedException {
            return cursor(cacheId, null, null, null, ver);
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
            KeyCacheObject upper, Object x, MvccCoordinatorVersion ver) throws IgniteCheckedException {
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

            if (ver != null) {
                assert grp.mvccEnabled();

                // TODO IGNITE-3478: more optimal cursor, e.g. pass some 'isVisible' closure.
                return new MvccCursor(dataTree.find(lowerRow, upperRow, x), ver);
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
            // In case we deal with IGFS cache, count updated data
            if (cctx.cache().isIgfsDataCache() &&
                !cctx.isNear() &&
                ctx.kernalContext()
                    .igfsHelper()
                    .isIgfsBlockKey(key.value(cctx.cacheObjectContext(), false))) {
                int oldSize = valueLength(cctx, oldVal);
                int newSize = valueLength(cctx, newVal);

                int delta = newSize - oldSize;

                if (delta != 0)
                    cctx.cache().onIgfsDataSizeChanged(delta);
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
            /** */
            private final GridCursor<? extends CacheDataRow> cur;
            /** */
            private final MvccCoordinatorVersion ver;
            /** */
            private CacheDataRow curRow;

            /** */
            MvccCursor(GridCursor<? extends CacheDataRow> cur, MvccCoordinatorVersion ver) {
                this.cur = cur;
                this.ver = ver;
            }

            @Override public boolean next() throws IgniteCheckedException {
                KeyCacheObject curKey = curRow != null ? curRow.key() : null;

                curRow = null;

                while (cur.next()) {
                    CacheDataRow row = cur.get();

                    long rowCrdVerMasked = row.mvccCoordinatorVersion();

                    if (ver != null) {
                        long rowCrdVer = unmaskCoordinatorVersion(rowCrdVerMasked);

                        if (rowCrdVer > ver.coordinatorVersion())
                            continue;

                        if (rowCrdVer == ver.coordinatorVersion() && row.mvccCounter() > ver.counter())
                            continue;

                        MvccLongList txs = ver.activeTransactions();

                        if (txs != null && rowCrdVer == ver.coordinatorVersion() && txs.contains(row.mvccCounter()))
                            continue;
                    }

                    if (curKey != null && row.key().equals(curKey))
                        continue;

                    if (versionForRemovedValue(rowCrdVerMasked)) {
                        curKey = row.key();

                        continue;
                    }

                    curRow = row;

                    break;
                }

                return curRow != null;
            }

            @Override public CacheDataRow get() throws IgniteCheckedException {
                return curRow;
            }
        }
    }
}
