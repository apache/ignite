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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteRebalanceIteratorImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRowCacheAware;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_ETERNAL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public class IgniteCacheOffheapManagerImpl implements IgniteCacheOffheapManager {
    /** The maximum number of entries that can be preloaded under checkpoint read lock. */
    public static final int PRELOAD_SIZE_UNDER_CHECKPOINT_LOCK = 100;

    /** Batch size for cache removals during destroy. */
    private static final int BATCH_SIZE = 1000;

    /** */
    protected GridCacheSharedContext ctx;

    /** */
    protected CacheGroupContext grp;

    /** */
    protected IgniteLogger log;

    /** */
    private PendingEntriesTree pendingEntries;

    /** */
    private final GridAtomicLong globalRmvId = new GridAtomicLong(U.currentTimeMillis() * 1000_000);

    /** */
    protected final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    protected GridStripedLock partStoreLock = new GridStripedLock(Runtime.getRuntime().availableProcessors());

    /** */
    private final AtomicBoolean stopping = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public GridAtomicLong globalRemoveId() {
        return globalRmvId;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext ctx, CacheGroupContext grp) throws IgniteCheckedException {
        this.ctx = ctx;
        this.grp = grp;
        this.log = ctx.logger(getClass());

        if (grp.affinityNode()) {
            ctx.database().checkpointReadLock();

            try {
                initDataStructures();
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException {
        initPendingTree(cctx);
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    protected void initPendingTree(GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        assert !cctx.group().persistenceEnabled();

        if (cctx.affinityNode() && cctx.ttl().eagerTtlEnabled() && pendingEntries == null) {
            String pendingEntriesTreeName = cctx.name() + "##PendingEntries";

            long rootPage = allocateForTree();

            pendingEntries = new PendingEntriesTree(
                grp,
                pendingEntriesTreeName,
                grp.dataRegion().pageMemory(),
                rootPage,
                grp.reuseList(),
                true,
                ctx.diagnostic().pageLockTracker(),
                FLAG_IDX
            );
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDataStructures() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stopCache(int cacheId, boolean destroy) {
        if (destroy && grp.affinityNode())
            removeCacheData(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        GridKernalContext kctx = ctx.kernalContext();

        // For in-memory mode, if we stop caches on grid stopping or cluster deactivation, skip data deletion from
        // the trees and just close trees to release resources.
        if (kctx.gateway().getState() == GridKernalState.STOPPING
            || kctx.state().clusterState().state() == ClusterState.INACTIVE) {
            for (CacheDataStore store : cacheDataStores())
                store.tree().close();

            if (pendingEntries != null)
                pendingEntries.close();

            return;
        }

        // In other cases (cache stop, for example) perform destroy with data deletion (through tree iteration).
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
    @Override public long restoreStateOfPartition(int p, @Nullable Integer recoveryState) throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void restorePartitionStates() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void confirmPartitionStatesRestored() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        if (stopping.compareAndSet(false, true)) // Avoid concurrent blocking by prepareToStop and onKernalStop.
            busyLock.block();
    }

    /** {@inheritDoc} */
    @Override public void prepareToStop() {
        if (stopping.compareAndSet(false, true)) // Avoid concurrent blocking by prepareToStop and onKernalStop.
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

                // Clear non-persistent pending tree if needed.
                if (pendingEntries != null) {
                    PendingRow row = new PendingRow(cacheId);

                    while (pendingEntries.removex(row, row, -1));
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @return Data store.
     */
    @Nullable private CacheDataStore dataStore(GridCacheContext<?, ?> cctx, KeyCacheObject key) {
        return dataStore(cctx.affinity().partition(key), false);
    }

    /** {@inheritDoc} */
    @Override public CacheDataStore dataStore(@Nullable GridDhtLocalPartition part) {
        assert part != null;

        return part.dataStore();
    }

    /**
     * @param partId Partition id.
     * @param includeRenting {@code true} if includeRenting partitions must also be shown.
     * @return Related partition cache data store or {@code null} if partition haven't been initialized.
     */
    @Nullable private CacheDataStore dataStore(int partId, boolean includeRenting) {
        GridDhtLocalPartition part = grp.topology().localPartition(partId, AffinityTopologyVersion.NONE, false, includeRenting);

        return part == null ? null : part.dataStore();
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(int cacheId) {
        long size = 0;

        for (CacheDataStore store : cacheDataStores())
            size += store.cacheSize(cacheId);

        return size;
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int partId) throws IgniteCheckedException {
        throw new IgniteCheckedException("Operation only applicable to caches with enabled persistence");
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(
        int cacheId,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer
    ) {
        long cnt = 0;

        Iterator<CacheDataStore> it = cacheData(primary, backup, topVer);

        while (it.hasNext())
            cnt += it.next().cacheSize(cacheId);

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(int cacheId, int part) {
        CacheDataStore store = dataStore(part, true);

        return store == null ? 0 : store.cacheSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public Iterable<CacheDataStore> cacheDataStores() {
        return cacheDataStores(F.alwaysTrue());
    }

    /**
     * @param filter Filtering predicate.
     * @return Iterable over all existing cache data stores except which one is marked as <tt>destroyed</tt>.
     */
    private Iterable<CacheDataStore> cacheDataStores(
        IgnitePredicate<GridDhtLocalPartition> filter
    ) {
        return F.iterator(grp.topology().currentLocalPartitions(), GridDhtLocalPartition::dataStore, true,
                filter, p -> !p.dataStore().destroyed());
    }

    /**
     * @param primary Primary data flag.
     * @param backup Backup data flag.
     * @param topVer Topology version.
     * @return Data stores iterator.
     */
    private Iterator<CacheDataStore> cacheData(boolean primary, boolean backup, AffinityTopologyVersion topVer) {
        assert primary || backup;

        IgnitePredicate<GridDhtLocalPartition> filter;

        if (primary && backup)
            filter = F.alwaysTrue();
        else {
            IntSet parts = ImmutableIntSet.wrap(primary ? grp.affinity().primaryPartitions(ctx.localNodeId(), topVer) :
                grp.affinity().backupPartitions(ctx.localNodeId(), topVer));

            filter = part -> parts.contains(part.id());
        }

        return cacheDataStores(filter).iterator();
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
    @Override public void remove(
        GridCacheContext cctx,
        KeyCacheObject key,
        int partId,
        GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        dataStore(part).remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheDataRow read(GridCacheMapEntry entry)
        throws IgniteCheckedException {
        KeyCacheObject key = entry.key();

        assert entry.localPartition() != null : entry;

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
    @Override public boolean containsKey(GridCacheMapEntry entry) {
        try {
            return read(entry) != null;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to read value", e);

            return false;
        }
    }

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    @Override public void clearCache(GridCacheContext cctx, boolean readers) {
        GridCacheVersion obsoleteVer = null;

        try (GridCloseableIterator<CacheDataRow> it = evictionSafeIterator(cctx.cacheId(), cacheDataStores().iterator())) {
            while (it.hasNext()) {
                cctx.shared().database().checkpointReadLock();

                try {
                    KeyCacheObject key = it.next().key();

                    try {
                        if (obsoleteVer == null)
                            obsoleteVer = cctx.cache().nextVersion();

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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<Cache.Entry<K, V>> cacheEntriesIterator(
        GridCacheContext cctx,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer,
        boolean keepBinary,
        Boolean dataPageScanEnabled
    ) {
        Iterator<CacheDataRow> it = cacheIterator(cctx.cacheId(), primary, backup,
            topVer, dataPageScanEnabled);

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

                    Object key0 = cctx.unwrapBinaryIfNeeded(key, keepBinary, false, null);
                    Object val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false, null);

                    next = new CacheEntryImplEx(key0, val0, nextRow.version());

                    return true;
                }

                return false;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<KeyCacheObject> cacheKeysIterator(int cacheId, int part)
        throws IgniteCheckedException {
        CacheDataStore data = dataStore(part, true);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        GridCursor<? extends CacheDataRow> cur =
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
        AffinityTopologyVersion topVer,
        Boolean dataPageScanEnabled
    ) {
        return iterator(cacheId, cacheData(primary, backups, topVer), dataPageScanEnabled);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, int part,
        Boolean dataPageScanEnabled) {
        CacheDataStore data = dataStore(part, true);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(cacheId, singletonIterator(data), dataPageScanEnabled);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> partitionIterator(int part) {
        CacheDataStore data = dataStore(part, true);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(CU.UNDEFINED_CACHE_ID, singletonIterator(data), null);
    }

    /**
     *
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @param dataPageScanEnabled Flag to enable data page scan.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> iterator(int cacheId,
        Iterator<CacheDataStore> dataIt,
        Boolean dataPageScanEnabled
    ) {
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
                    try {
                        if (cur == null) {
                            if (dataIt.hasNext()) {
                                CacheDataStore ds = dataIt.next();

                                curPart = ds.partId();

                                // Data page scan is disabled by default for scan queries.
                                // TODO https://issues.apache.org/jira/browse/IGNITE-11998
                                CacheDataTree.setDataPageScanEnabled(false);

                                try {
                                    cur = cacheId == CU.UNDEFINED_CACHE_ID ? ds.cursor() : ds.cursor(cacheId);
                                }
                                finally {
                                    CacheDataTree.setDataPageScanEnabled(false);
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
                    catch (IgniteCheckedException ex) {
                        throw new IgniteCheckedException("Failed to get next data row due to underlying cursor " +
                            "invalidation", ex);
                    }
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
    private GridCloseableIterator<CacheDataRow> evictionSafeIterator(int cacheId, Iterator<CacheDataStore> dataIt) {
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
     * @param <T> Type of item.
     */
    private <T> Iterator<T> singletonIterator(T item) {
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
    @Override public RootPage rootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException {
        long pageId = allocateForTree();

        return new RootPage(new FullPageId(pageId, grp.groupId()), true);
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage findRootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage dropRootPageForIndex(
        int cacheId,
        String idxName,
        int segment
    ) throws IgniteCheckedException {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage renameRootPageForIndex(
        int cacheId,
        String oldIdxName,
        String newIdxName,
        int segment
    ) throws IgniteCheckedException {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseListForIndex(String idxName) {
        return grp.reuseList();
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<CacheDataRow> reservedIterator(int part, AffinityTopologyVersion topVer) {
        GridDhtLocalPartition loc = grp.topology().localPartition(part, topVer, false);

        if (loc == null || !loc.reserve())
            return null;

        // It is necessary to check state after reservation to avoid race conditions.
        if (loc.state() != OWNING) {
            loc.release();

            return null;
        }

        CacheDataStore data = dataStore(loc);

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private CacheDataRow next;

            /** */
            private GridCursor<? extends CacheDataRow> cur;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (cur == null)
                    cur = data.cursor();

                if (next != null)
                    return true;

                if (cur.next())
                    next = cur.get();

                boolean hasNext = next != null;

                if (!hasNext)
                    cur = null;

                return hasNext;
            }

            @Override protected void onClose() throws IgniteCheckedException {
                assert loc != null && loc.state() == OWNING && loc.reservations() > 0
                    : "Partition should be in OWNING state and has at least 1 reservation: " + loc;

                loc.release();

                cur = null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public IgniteRebalanceIterator rebalanceIterator(IgniteDhtDemandedPartitionsMap parts,
        AffinityTopologyVersion topVer)
        throws IgniteCheckedException {

        TreeMap<Integer, GridCloseableIterator<CacheDataRow>> iterators = new TreeMap<>();

        Set<Integer> missing = new HashSet<>();

        for (Integer p : parts.fullSet()) {
            GridCloseableIterator<CacheDataRow> partIter = reservedIterator(p, topVer);

            if (partIter == null) {
                missing.add(p);

                continue;
            }

            iterators.put(p, partIter);
        }

        IgniteHistoricalIterator historicalIter = historicalIterator(parts.historicalMap(), missing);

        IgniteRebalanceIterator iter = new IgniteRebalanceIteratorImpl(iterators, historicalIter);

        for (Integer p : missing)
            iter.setPartitionMissing(p);

        return iter;
    }

    /**
     * @param partCntrs Partition counters map.
     * @param missing Set of partitions need to populate if partition is missing or failed to reserve.
     * @return Historical iterator.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected IgniteHistoricalIterator historicalIterator(CachePartitionPartialCountersMap partCntrs, Set<Integer> missing)
        throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void storeEntries(GridDhtLocalPartition part, Iterator<GridCacheEntryInfo> infos,
        IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(part);

        List<DataRowCacheAware> batch = new ArrayList<>(PRELOAD_SIZE_UNDER_CHECKPOINT_LOCK);

        while (infos.hasNext()) {
            GridCacheEntryInfo info = infos.next();

            assert info.ttl() == TTL_ETERNAL : info.ttl();

            batch.add(new DataRowCacheAware(info.key(),
                info.value(),
                info.version(),
                part.id(),
                info.expireTime(),
                info.cacheId(),
                grp.storeCacheIdInDataPage()));

            if (batch.size() == PRELOAD_SIZE_UNDER_CHECKPOINT_LOCK || !infos.hasNext()) {
                ctx.database().checkpointReadLock();

                try {
                    dataStore.insertRows(batch, initPred);
                }
                finally {
                    ctx.database().checkpointReadUnlock();
                }

                batch.clear();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public final CacheDataStore createCacheDataStore(int p) throws IgniteCheckedException {
        partStoreLock.lock(p);

        try {
            return createCacheDataStore0(p);
        }
        finally {
            partStoreLock.unlock(p);
        }
    }

    /**
     * @param p Partition.
     * @return Cache data store.
     * @throws IgniteCheckedException If failed.
     */
    protected CacheDataStore createCacheDataStore0(int p) throws IgniteCheckedException {
        long rootPage = allocateForTree();

        CacheDataRowStore rowStore = new CacheDataRowStore(grp, grp.freeList(), p);

        String dataTreeName = grp.cacheOrGroupName() + "-" + treeName(p);

        CacheDataTree dataTree = new CacheDataTree(
            grp,
            dataTreeName,
            grp.reuseList(),
            rowStore,
            rootPage,
            true,
            ctx.diagnostic().pageLockTracker(),
            FLAG_IDX
        );

        return new CacheDataStoreImpl(p, rowStore, dataTree, () -> pendingEntries, grp, busyLock, log, null);
    }

    /** {@inheritDoc} */
    @Override public final void destroyCacheDataStore(CacheDataStore store) throws IgniteCheckedException {
        int p = store.partId();

        partStoreLock.lock(p);

        try {
            if (store.destroyed())
                return;

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

        assert pendingEntries != null;

        int cleared = expireInternal(cctx, c, amount);

        return amount != -1 && cleared >= amount;
    }

    /**
     * @param cctx Cache context.
     * @param c Closure.
     * @param amount Limit of processed entries by single call, {@code -1} for no limit.
     * @return cleared entries count.
     * @throws IgniteCheckedException If failed.
     */
    private int expireInternal(
        GridCacheContext cctx,
        IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c,
        int amount
    ) throws IgniteCheckedException {
        GridCacheVersion obsoleteVer = null;

        cctx.shared().database().checkpointReadLock();

        try {
            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            if (!busyLock.enterBusy())
                return 0;

            try {
                int cleared = 0;

                do {
                    List<PendingRow> rows = pendingEntries.remove(new PendingRow(cacheId, Long.MIN_VALUE, 0),
                        new PendingRow(cacheId, U.currentTimeMillis(), 0), amount - cleared);

                    if (rows.isEmpty())
                        break;

                    for (PendingRow row : rows) {
                        if (row.key.partition() == -1)
                            row.key.partition(cctx.affinity().partition(row.key));

                        assert row.key != null && row.link != 0 && row.expireTime != 0 : row;

                        if (obsoleteVer == null)
                            obsoleteVer = cctx.cache().nextVersion();

                        GridCacheEntryEx entry = cctx.cache().entryEx(row.key instanceof KeyCacheObjectImpl
                            ? new ExpiredKeyCacheObject((KeyCacheObjectImpl)row.key, row.expireTime, row.link, obsoleteVer)
                            : row.key);

                        if (entry != null)
                            c.apply(entry, obsoleteVer);
                    }

                    cleared += rows.size();
                }
                while (amount < 0 || cleared < amount);

                return cleared;
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long expiredSize() throws IgniteCheckedException {
        return pendingEntries != null ? pendingEntries.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean hasEntriesPendingExpire(int cacheId) throws IgniteCheckedException {
        if (pendingEntries == null)
            return false;

        if (grp.sharedGroup()) {
            PendingRow row = new PendingRow(cacheId);

            GridCursor<PendingRow> cursor = pendingEntries.find(row, row, PendingEntriesTree.WITHOUT_KEY);

            return cursor.next();
        }
        else
            return !pendingEntries.isEmpty();
    }

    /**
     *
     */
    public static class CacheDataStoreImpl implements CacheDataStore {
        /** */
        private final int partId;

        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final CacheDataTree dataTree;

        /** */
        private final Supplier<PendingEntriesTree> pendingEntries;

        /** */
        private final CacheGroupContext grp;

        /** */
        private final GridSpinBusyLock busyLock;

        /** Update counter. */
        protected final PartitionUpdateCounter pCntr;

        /** Partition size. */
        private final LongAdder storageSize = new LongAdder();

        /** */
        private final IntMap<AtomicLong> cacheSizes = new IntRWHashMap<>();

        /** */
        private final IgniteLogger log;

        /** */
        private final Boolean failNodeOnPartitionInconsistency = Boolean.getBoolean(
            IgniteSystemProperties.IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY
        );

        /** */
        private final int updateValSizeThreshold;

        /** */
        private volatile GridQueryRowCacheCleaner rowCacheCleaner;

        /**
         * @param partId Partition number.
         * @param rowStore Row store.
         * @param dataTree Data tree.
         */
        public CacheDataStoreImpl(
            int partId,
            CacheDataRowStore rowStore,
            CacheDataTree dataTree,
            Supplier<PendingEntriesTree> pendingEntries,
            CacheGroupContext grp,
            GridSpinBusyLock busyLock,
            IgniteLogger log,
            @Nullable Supplier<GridQueryRowCacheCleaner> cleaner
        ) {
            this.partId = partId;
            this.rowStore = rowStore;
            this.dataTree = dataTree;
            this.pendingEntries = pendingEntries;
            this.grp = grp;
            this.busyLock = busyLock;
            this.log = log;

            PartitionUpdateCounter delegate = !grp.persistenceEnabled() || grp.hasAtomicCaches() ?
                new PartitionUpdateCounterVolatileImpl(grp) :
                new PartitionUpdateCounterTrackingImpl(grp);

            pCntr = grp.shared().logger(PartitionUpdateCounterDebugWrapper.class).isDebugEnabled() ?
                new PartitionUpdateCounterDebugWrapper(partId, delegate) : new PartitionUpdateCounterErrorWrapper(partId, delegate);

            updateValSizeThreshold = grp.shared().database().pageSize() / 2;

            if (cleaner == null)
                rowStore.setRowCacheCleaner(() -> rowCacheCleaner);
            else
                rowStore.setRowCacheCleaner(cleaner);
        }

        /** {@inheritDoc} */
        @Override public CacheDataTree tree() {
            return dataTree;
        }

        /**
         * @param cacheId Cache ID.
         */
        void incrementSize(int cacheId) {
            updateSize(cacheId, 1);
        }

        /**
         * @param cacheId Cache ID.
         */
        void decrementSize(int cacheId) {
            updateSize(cacheId, -1);
        }

        /** {@inheritDoc} */
        @Override public boolean init() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public long cacheSize(int cacheId) {
            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                return size != null ? (int)size.get() : 0;
            }

            return storageSize.sum();
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Long> cacheSizes() {
            if (!grp.sharedGroup())
                return null;

            Map<Integer, Long> res = new HashMap<>();

            cacheSizes.forEach((key, val) -> res.put(key, val.longValue()));

            return res;
        }

        /** {@inheritDoc} */
        @Override public long fullSize() {
            return storageSize.sum();
        }

        /**
         * @return {@code True} if there are no items in the store.
         */
        @Override public boolean isEmpty() {
            return storageSize.sum() == 0;
        }

        /** {@inheritDoc} */
        @Override public void updateSize(int cacheId, long delta) {
            storageSize.add(delta);

            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                if (size == null) {
                    AtomicLong old = cacheSizes.putIfAbsent(cacheId, size = new AtomicLong());

                    if (old != null)
                        size = old;
                }

                size.addAndGet(delta);
            }
        }

        /** {@inheritDoc} */
        @Override public long nextUpdateCounter() {
            return pCntr.next();
        }

        /** {@inheritDoc} */
        @Override public long initialUpdateCounter() {
            return pCntr.initial();
        }

        /** {@inheritDoc}
         * @param start Start.
         * @param delta Delta.
         */
        @Override public void updateInitialCounter(long start, long delta) {
            pCntr.updateInitial(start, delta);
        }

        /** {@inheritDoc} */
        @Override public long getAndIncrementUpdateCounter(long delta) {
            return pCntr.reserve(delta);
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            return pCntr.get();
        }

        /** {@inheritDoc} */
        @Override public long highestAppliedCounter() {
            return pCntr.highestAppliedCounter();
        }

        /** {@inheritDoc} */
        @Override public long reservedCounter() {
            return pCntr.reserved();
        }

        /** {@inheritDoc} */
        @Override public PartitionUpdateCounter partUpdateCounter() {
            return pCntr;
        }

        /** {@inheritDoc} */
        @Override public long reserve(long delta) {
            return pCntr.reserve(delta);
        }

        /** {@inheritDoc} */
        @Override public void updateCounter(long val) {
            try {
                pCntr.update(val);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to update partition counter. " +
                    "Most probably a node with most actual data is out of topology or data streamer is used " +
                    "in preload mode (allowOverride=false) concurrently with cache transactions [grpName=" +
                    grp.cacheOrGroupName() + ", partId=" + partId + ']', e);

                if (failNodeOnPartitionInconsistency)
                    grp.shared().kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean updateCounter(long start, long delta) {
            return pCntr.update(start, delta);
        }

        /** {@inheritDoc} */
        @Override public GridLongList finalizeUpdateCounters() {
            return pCntr.finalizeUpdateCounters();
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

            int oldLen = oldRow.size();

            // Use grp.sharedGroup() flag since it is possible cacheId is not yet set here.
            if (!grp.storeCacheIdInDataPage() && grp.sharedGroup() && oldRow.cacheId() != CU.UNDEFINED_CACHE_ID)
                oldLen -= 4;

            if (oldLen > updateValSizeThreshold)
                return false;

            int newLen = dataRow.size();

            return oldLen == newLen;
        }

        /** */
        private IgniteCheckedException operationCancelledException() {
            if (grp.isPreparedToStop()) {
                return new IgniteCheckedException("Operation has been cancelled (cache group: " +
                    grp.cacheOrGroupName() + " is stopping).");
            }
            else
                return new NodeStoppingException("Operation has been cancelled (node is stopping).");
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw operationCancelledException();

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            try {
                invoke0(cctx, new SearchRow(cacheId, key), c);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param row Search row.
         * @param c Closure.
         * @throws IgniteCheckedException If failed.
         */
        private void invoke0(GridCacheContext cctx, CacheSearchRow row, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            assert cctx.shared().database().checkpointLockIsHeldByThread();

            dataTree.invoke(row, CacheDataRowAdapter.RowData.NO_KEY, c);

            switch (c.operationType()) {
                case PUT: {
                    assert c.newRow() != null : c;

                    CacheDataRow oldRow = c.oldRow();

                    finishUpdate(cctx, c.newRow(), oldRow, c.oldRowExpiredFlag());

                    break;
                }

                case REMOVE: {
                    CacheDataRow oldRow = c.oldRow();

                    finishRemove(cctx, row.key(), oldRow);

                    break;
                }

                case NOOP:
                case IN_PLACE:
                    break;

                default:
                    assert false : c.operationType();
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

            DataRow dataRow = makeDataRow(key, val, ver, expireTime, cacheId);

            if (canUpdateOldRow(cctx, oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow, grp.statisticsHolderData()))
                dataRow.link(oldRow.link());
            else {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                rowStore.addRow(dataRow, grp.statisticsHolderData());
            }

            assert dataRow.link() != 0 : dataRow;

            if (grp.sharedGroup() && dataRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                dataRow.cacheId(cctx.cacheId());

            return dataRow;
        }

        /** {@inheritDoc} */
        @Override public void insertRows(Collection<DataRowCacheAware> rows,
            IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw operationCancelledException();

            try {
                rowStore.addRows(F.view(rows, row -> row.value() != null), grp.statisticsHolderData());

                boolean cacheIdAwareGrp = grp.sharedGroup() || grp.storeCacheIdInDataPage();

                for (DataRowCacheAware row : rows) {
                    row.storeCacheId(cacheIdAwareGrp);

                    if (!initPred.applyx(row) && row.value() != null)
                        rowStore.removeRow(row.link(), grp.statisticsHolderData());
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param key Cache key.
         * @param val Cache value.
         * @param ver Version.
         * @param expireTime Expired time.
         * @param cacheId Cache id.
         * @return Made data row.
         */
        @NotNull private DataRow makeDataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, long expireTime,
            int cacheId) {
            if (key.partition() == -1)
                key.partition(partId);

            return new DataRow(key, val, ver, partId, expireTime, cacheId);
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
                throw operationCancelledException();

            try {
                int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                assert oldRow == null || oldRow.cacheId() == cacheId : oldRow;

                DataRow dataRow = makeDataRow(key, val, ver, expireTime, cacheId);

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                CacheDataRow old;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                if (canUpdateOldRow(cctx, oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow, grp.statisticsHolderData())) {
                    old = oldRow;

                    dataRow.link(oldRow.link());
                }
                else {
                    rowStore.addRow(dataRow, grp.statisticsHolderData());

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
            finishUpdate(cctx, newRow, oldRow, false);
        }

        /**
         * @param cctx Cache context.
         * @param newRow New row.
         * @param oldRow Old row if available.
         * @param oldRowExpired Old row expiration flag
         * @throws IgniteCheckedException If failed.
         */
        private void finishUpdate(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow oldRow, boolean oldRowExpired)
            throws IgniteCheckedException {
            if (oldRow == null && !oldRowExpired)
                incrementSize(cctx.cacheId());

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.store(newRow, oldRow, true);

            updatePendingEntries(cctx, newRow, oldRow);

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (newRow.link() != oldRow.link())
                    rowStore.removeRow(oldRow.link(), grp.statisticsHolderData());
            }
        }

        /**
         * @param cctx Cache context.
         * @param newRow New row.
         * @param oldRow Old row.
         * @throws IgniteCheckedException If failed.
         */
        private void updatePendingEntries(
            GridCacheContext cctx,
            CacheDataRow newRow,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            long expireTime = newRow.expireTime();

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (pendingTree() != null && oldRow.expireTime() != 0)
                    pendingTree().removex(new PendingRow(cacheId, oldRow.expireTime(), oldRow.link()));
            }

            if (pendingTree() != null && expireTime != 0) {
                pendingTree().putx(new PendingRow(cacheId, expireTime, newRow.link()));

                if (!cctx.ttl().hasPendingEntries())
                    cctx.ttl().hasPendingEntries(true);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw operationCancelledException();

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
                if (!(key instanceof ExpiredKeyCacheObject)
                    || ((ExpiredKeyCacheObject)key).expireTime != oldRow.expireTime()
                    || ((ExpiredKeyCacheObject)key).link != oldRow.link()
                    || !Objects.equals(((ExpiredKeyCacheObject)key).ver, oldRow.version())
                )
                    clearPendingEntries(cctx, oldRow);

                decrementSize(cctx.cacheId());
            }

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.remove(key, oldRow);

            if (oldRow != null)
                rowStore.removeRow(oldRow.link(), grp.statisticsHolderData());
        }

        /**
         * @param cctx Cache context.
         * @param oldRow Old row.
         * @throws IgniteCheckedException If failed.
         */
        private void clearPendingEntries(GridCacheContext cctx, CacheDataRow oldRow)
            throws IgniteCheckedException {
            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            assert oldRow.link() != 0 : oldRow;
            assert cacheId == CU.UNDEFINED_CACHE_ID || oldRow.cacheId() == cacheId :
                "Incorrect cache ID [expected=" + cacheId + ", actual=" + oldRow.cacheId() + "].";

            if (pendingTree() != null && oldRow.expireTime() != 0)
                pendingTree().removex(new PendingRow(cacheId, oldRow.expireTime(), oldRow.link()));
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            CacheDataRow row = dataTree.findOne(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY);

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
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
            return cursor(cacheId, null, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException {
            return cursor(cacheId, lower, upper, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper, Object x) throws IgniteCheckedException {
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

            return dataTree.find(lowerRow, upperRow, x);
        }

        /** {@inheritDoc} */
        @Override public void destroy() throws IgniteCheckedException {
            AtomicReference<IgniteCheckedException> exRef = new AtomicReference<>();

            dataTree.destroy(row -> {
                try {
                    rowStore.removeRow(row.link(), grp.statisticsHolderData());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to remove row [link=" + row.link() + "]");

                    IgniteCheckedException ex = exRef.get();

                    if (ex == null)
                        exRef.set(e);
                    else
                        ex.addSuppressed(e);
                }
            }, false);

            if (exRef.get() != null)
                throw new IgniteCheckedException("Failed to destroy store", exRef.get());
        }

        /** {@inheritDoc} */
        @Override public void markDestroyed() {
            dataTree.markDestroyed();
        }

        /** {@inheritDoc} */
        @Override public boolean destroyed() {
            return dataTree.destroyed();
        }

        /** {@inheritDoc} */
        @Override public void clear(int cacheId) throws IgniteCheckedException {
            assert cacheId != CU.UNDEFINED_CACHE_ID;

            if (cacheSize(cacheId) == 0)
                return;

            Exception ex = null;

            GridCursor<? extends CacheDataRow> cur =
                cursor(cacheId, null, null, CacheDataRowAdapter.RowData.KEY_ONLY);

            int rmv = 0;

            while (cur.next()) {
                if (++rmv == BATCH_SIZE) {
                    grp.shared().database().checkpointReadUnlock();

                    rmv = 0;

                    grp.shared().database().checkpointReadLock();
                }

                CacheDataRow row = cur.get();

                assert row.link() != 0 : row;

                try {
                    boolean res = dataTree.removex(row);

                    assert res : row;

                    rowStore.removeRow(row.link(), grp.statisticsHolderData());

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

            // Allow checkpointer to progress if a partition contains less than BATCH_SIZE keys.
            grp.shared().database().checkpointReadUnlock();

            grp.shared().database().checkpointReadLock();
        }

        /** {@inheritDoc} */
        @Override public RowStore rowStore() {
            return rowStore;
        }

        /** {@inheritDoc} */
        @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
            this.rowCacheCleaner = rowCacheCleaner;
        }

        /**
         * @param size Size to init.
         * @param updCntr Update counter.
         * @param cacheSizes Cache sizes if store belongs to group containing multiple caches.
         * @param cntrUpdData Counter updates.
         */
        public void restoreState(long size, long updCntr, @Nullable Map<Integer, Long> cacheSizes, byte[] cntrUpdData) {
            pCntr.init(updCntr, cntrUpdData);

            storageSize.reset();
            storageSize.add(size);

            if (cacheSizes != null) {
                for (Map.Entry<Integer, Long> e : cacheSizes.entrySet())
                    this.cacheSizes.put(e.getKey(), new AtomicLong(e.getValue()));
            }
        }

        /** {@inheritDoc} */
        @Override public PendingEntriesTree pendingTree() {
            return pendingEntries.get();
        }

        /** {@inheritDoc} */
        @Override public void preload() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void resetUpdateCounter() {
            pCntr.reset();
        }

        /** {@inheritDoc} */
        @Override public void resetInitialUpdateCounter() {
            pCntr.resetInitialCounter();
        }

        /** {@inheritDoc} */
        @Override public PartitionMetaStorage<SimpleDataRow> partStorage() {
            return null;
        }
    }

    /**
     * This entry key is used to indicate that an expired entry has already been deleted from
     * PendingEntriesTree and doesn't need to participate in PendingEntriesTree cleanup again.
     */
    protected static class ExpiredKeyCacheObject extends KeyCacheObjectImpl {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private long expireTime;

        /** */
        private long link;

        /** */
        private GridCacheVersion ver;

        /** */
        public ExpiredKeyCacheObject(KeyCacheObjectImpl keyCacheObj, long expireTime, long link, GridCacheVersion ver) {
            super(keyCacheObj.val, keyCacheObj.valBytes, keyCacheObj.partition());

            this.expireTime = expireTime;
            this.link = link;
            this.ver = ver;
        }

        /** */
        public ExpiredKeyCacheObject() {
        }
    }
}
