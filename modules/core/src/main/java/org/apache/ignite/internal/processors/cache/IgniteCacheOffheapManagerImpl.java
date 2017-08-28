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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 *
 */
@SuppressWarnings("PublicInnerClass")
public class IgniteCacheOffheapManagerImpl implements IgniteCacheOffheapManager {
    /** */
    private static final int UNDEFINED_CACHE_ID = 0;

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
                grp.memoryPolicy().pageMemory(),
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
     *
     */
    private void removeCacheData(int cacheId) {
        assert grp.affinityNode();

        try {
            if (grp.sharedGroup()) {
                assert cacheId != UNDEFINED_CACHE_ID;

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
    @Override public void updateIndexes(GridCacheContext cctx, KeyCacheObject key, GridDhtLocalPartition part)
        throws IgniteCheckedException {
        dataStore(part).updateIndexes(cctx, key);
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
    @Nullable @Override public CacheDataRow read(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        CacheDataRow row;

        if (cctx.isLocal())
            row = locCacheDataStore.find(cctx, key);
        else {
            GridDhtLocalPartition part = cctx.topology().localPartition(cctx.affinity().partition(key), null, false);

            row = part != null ? dataStore(part).find(cctx, key) : null;
        }

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

        try (GridCloseableIterator<CacheDataRow> it = grp.isLocal() ? iterator(cctx.cacheId(), cacheDataStores().iterator()) :
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
        final Iterator<CacheDataRow> it = cacheIterator(cctx.cacheId(), primary, backup, topVer);

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
        final AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        return iterator(cacheId, cacheData(primary, backups, topVer));
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(cacheId, singletonIterator(data));
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> partitionIterator(int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(UNDEFINED_CACHE_ID, singletonIterator(data));
    }

    /**
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> iterator(final int cacheId, final Iterator<CacheDataStore> dataIt) {
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
                            cur = cacheId == UNDEFINED_CACHE_ID ? ds.cursor() : ds.cursor(cacheId);
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

                            cur = cacheId == UNDEFINED_CACHE_ID ? ds.cursor() : ds.cursor(cacheId);
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
            pageId = grp.memoryPolicy().pageMemory().allocatePage(grp.groupId(), INDEX_PARTITION, FLAG_IDX);

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
                cur = pendingEntries.find(null, new PendingRow(UNDEFINED_CACHE_ID, now, 0));

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
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

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
            int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

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

            if (grp.sharedGroup() && dataRow.cacheId() == UNDEFINED_CACHE_ID)
                dataRow.cacheId(cctx.cacheId());

            return dataRow;
        }

        /** {@inheritDoc} */
        @Override public void update(GridCacheContext cctx,KeyCacheObject key,

            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            assert oldRow == null || oldRow.link() != 0L : oldRow;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

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

                    if (grp.sharedGroup() && dataRow.cacheId() == UNDEFINED_CACHE_ID)
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

            long expireTime = newRow.expireTime();

            GridCacheQueryManager qryMgr = cctx.queries();

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

            if (qryMgr.enabled()) {
                if (oldRow != null) {
                    qryMgr.store(key,
                        partId,
                        oldRow.value(),
                        oldRow.version(),
                        newRow.value(),
                        newRow.version(),
                        expireTime,
                        newRow.link());
                }
                else {
                    qryMgr.store(key,
                        partId,
                        null, null,
                        newRow.value(),
                        newRow.version(),
                        expireTime,
                        newRow.link());
                }
            }

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (pendingEntries != null && oldRow.expireTime() != 0)
                    pendingEntries.removex(new PendingRow(cacheId, oldRow.expireTime(), oldRow.link()));

                if (newRow.link() != oldRow.link())
                    rowStore.removeRow(oldRow.link());
            }

            if (pendingEntries != null && expireTime != 0) {
                pendingEntries.putx(new PendingRow(cacheId, expireTime, newRow.link()));

                hasPendingEntries = true;
            }

            updateIgfsMetrics(cctx, key, (oldRow != null ? oldRow.value() : null), newRow.value());
        }

        /** {@inheritDoc} */
        @Override public void updateIndexes(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            int cacheId = grp.sharedGroup() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

            CacheDataRow row = dataTree.findOne(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY);

            if (row != null) {
                row.key(key);

                GridCacheQueryManager qryMgr = cctx.queries();

                qryMgr.store(
                    key,
                    partId,
                    null,
                    null,
                    row.value(),
                    row.version(),
                    row.expireTime(),
                    row.link());
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

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
            CacheObject val = null;
            GridCacheVersion ver = null;

            if (oldRow != null) {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

                assert oldRow.link() != 0 : oldRow;
                assert cacheId == UNDEFINED_CACHE_ID || oldRow.cacheId() == cacheId :
                    "Incorrect cache ID [expected=" + cacheId + ", actual=" + oldRow.cacheId() + "].";

                if (pendingEntries != null && oldRow.expireTime() != 0)
                    pendingEntries.removex(new PendingRow(cacheId, oldRow.expireTime(), oldRow.link()));

                decrementSize(cctx.cacheId());

                val = oldRow.value();

                ver = oldRow.version();
            }

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.remove(key, partId, val, ver);

            if (oldRow != null)
                rowStore.removeRow(oldRow.link());

            updateIgfsMetrics(cctx, key, (oldRow != null ? oldRow.value() : null), null);
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : UNDEFINED_CACHE_ID;

            CacheDataRow row = dataTree.findOne(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY);

            if (row != null) {
                row.key(key);

                grp.memoryPolicy().evictionTracker().touchPage(row.link());
            }

            return row;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            return dataTree.find(null, null);
        }

        /** {@inheritDoc}
         * @param cacheId*/
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
                assert cacheId != UNDEFINED_CACHE_ID;

                lowerRow = lower != null ? new SearchRow(cacheId, lower) : new SearchRow(cacheId);
                upperRow = upper != null ? new SearchRow(cacheId, upper) : new SearchRow(cacheId);
            }
            else {
                lowerRow = lower != null ? new SearchRow(UNDEFINED_CACHE_ID, lower) : null;
                upperRow = upper != null ? new SearchRow(UNDEFINED_CACHE_ID, upper) : null;
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
            assert cacheId != UNDEFINED_CACHE_ID;

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
    }

    /**
     *
     */
    private static class SearchRow implements CacheSearchRow {
        /** */
        private final KeyCacheObject key;

        /** */
        private final int hash;

        /** */
        private final int cacheId;

        /**
         * @param cacheId Cache ID.
         * @param key Key.
         */
        SearchRow(int cacheId, KeyCacheObject key) {
            this.key = key;
            this.hash = key.hashCode();
            this.cacheId = cacheId;
        }

        /**
         * Instantiates a new fake search row as a logic cache based bound.
         *
         * @param cacheId Cache ID.
         */
        SearchRow(int cacheId) {
            this.key = null;
            this.hash = 0;
            this.cacheId = cacheId;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public int hash() {
            return hash;
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return cacheId;
        }
    }

    /**
     *
     */
    private class DataRow extends CacheDataRowAdapter {
        /** */
        protected int part;

        /** */
        protected int hash;

        /**
         * @param hash Hash code.
         * @param link Link.
         * @param part Partition.
         * @param rowData Required row data.
         */
        DataRow(int hash, long link, int part, CacheDataRowAdapter.RowData rowData) {
            super(link);

            this.hash = hash;

            this.part = part;

            try {
                // We can not init data row lazily because underlying buffer can be concurrently cleared.
                initFromLink(grp, rowData);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            if (key != null)
                key.partition(part);
        }

        /**
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param part Partition.
         * @param expireTime Expire time.
         * @param cacheId Cache ID.
         */
        DataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long expireTime, int cacheId) {
            super(0);

            this.hash = key.hashCode();
            this.key = key;
            this.val = val;
            this.ver = ver;
            this.part = part;
            this.expireTime = expireTime;
            this.cacheId = cacheId;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return part;
        }

        /** {@inheritDoc} */
        @Override public int hash() {
            return hash;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            this.link = link;
        }

        /**
         * @param cacheId Cache ID.
         */
        void cacheId(int cacheId) {
            this.cacheId = cacheId;
        }
    }

    /**
     *
     */
    protected static class CacheDataTree extends BPlusTree<CacheSearchRow, CacheDataRow> {
        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final CacheGroupContext grp;

        /**
         * @param grp Ccahe group.
         * @param name Tree name.
         * @param reuseList Reuse list.
         * @param rowStore Row store.
         * @param metaPageId Meta page ID.
         * @param initNew Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        public CacheDataTree(
            CacheGroupContext grp,
            String name,
            ReuseList reuseList,
            CacheDataRowStore rowStore,
            long metaPageId,
            boolean initNew
        ) throws IgniteCheckedException {
            super(name,
                grp.groupId(),
                grp.memoryPolicy().pageMemory(),
                grp.shared().wal(),
                grp.offheap().globalRemoveId(),
                metaPageId,
                reuseList,
                grp.sharedGroup() ? CacheIdAwareDataInnerIO.VERSIONS : DataInnerIO.VERSIONS,
                grp.sharedGroup() ? CacheIdAwareDataLeafIO.VERSIONS : DataLeafIO.VERSIONS);

            assert rowStore != null;

            this.rowStore = rowStore;
            this.grp = grp;

            initTree(initNew);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<CacheSearchRow> iox, long pageAddr, int idx, CacheSearchRow row)
            throws IgniteCheckedException {
            RowLinkIO io = (RowLinkIO)iox;

            int cmp;

            if (grp.sharedGroup()) {
                assert row.cacheId() != UNDEFINED_CACHE_ID : "Cache ID is not provided: " + row;

                int cacheId = io.getCacheId(pageAddr, idx);

                assert cacheId != UNDEFINED_CACHE_ID : "Cache ID is not stored";

                cmp = Integer.compare(cacheId, row.cacheId());

                if (cmp != 0)
                    return cmp;

                if (row.key() == null) {
                    assert row.getClass() == SearchRow.class : row;

                    // A search row with a cache ID only is used as a cache bound.
                    // The found position will be shifted until the exact cache bound is found;
                    // See for details:
                    // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                    // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                    return cmp;
                }
            }

            cmp = Integer.compare(io.getHash(pageAddr, idx), row.hash());

            if (cmp != 0)
                return cmp;

            long link = io.getLink(pageAddr, idx);

            assert row.key() != null : row;

            return compareKeys(row.key(), link);
        }

        /** {@inheritDoc} */
        @Override protected CacheDataRow getRow(BPlusIO<CacheSearchRow> io, long pageAddr, int idx, Object flags)
            throws IgniteCheckedException {
            long link = ((RowLinkIO)io).getLink(pageAddr, idx);
            int hash = ((RowLinkIO)io).getHash(pageAddr, idx);
            int cacheId = ((RowLinkIO)io).getCacheId(pageAddr, idx);

            CacheDataRowAdapter.RowData x = flags != null ?
                (CacheDataRowAdapter.RowData)flags :
                CacheDataRowAdapter.RowData.FULL;

            return rowStore.dataRow(cacheId, hash, link, x);
        }

        /**
         * @param key Key.
         * @param link Link.
         * @return Compare result.
         * @throws IgniteCheckedException If failed.
         */
        private int compareKeys(KeyCacheObject key, final long link) throws IgniteCheckedException {
            byte[] bytes = key.valueBytes(grp.cacheObjectContext());

            final long pageId = pageId(link);
            final long page = acquirePage(pageId);
            try {
                long pageAddr = readLock(pageId, page); // Non-empty data page must not be recycled.

                assert pageAddr != 0L : link;

                try {
                    DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                    DataPagePayload data = io.readPayload(pageAddr,
                        itemId(link),
                        pageSize());

                    if (data.nextLink() == 0) {
                        long addr = pageAddr + data.offset();

                        if (grp.storeCacheIdInDataPage())
                            addr += 4; // Skip cache id.

                        final int len = PageUtils.getInt(addr, 0);

                        int lenCmp = Integer.compare(len, bytes.length);

                        if (lenCmp != 0)
                            return lenCmp;

                        addr += 5; // Skip length and type byte.

                        final int words = len / 8;

                        for (int i = 0; i < words; i++) {
                            int off = i * 8;

                            long b1 = PageUtils.getLong(addr, off);
                            long b2 = GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF + off);

                            int cmp = Long.compare(b1, b2);

                            if (cmp != 0)
                                return cmp;
                        }

                        for (int i = words * 8; i < len; i++) {
                            byte b1 = PageUtils.getByte(addr, i);
                            byte b2 = bytes[i];

                            if (b1 != b2)
                                return b1 > b2 ? 1 : -1;
                        }

                        return 0;
                    }
                }
                finally {
                    readUnlock(pageId, page, pageAddr);
                }
            }
            finally {
                releasePage(pageId, page);
            }

            // TODO GG-11768.
            CacheDataRowAdapter other = new CacheDataRowAdapter(link);
            other.initFromLink(grp, CacheDataRowAdapter.RowData.KEY_ONLY);

            byte[] bytes1 = other.key().valueBytes(grp.cacheObjectContext());
            byte[] bytes2 = key.valueBytes(grp.cacheObjectContext());

            int lenCmp = Integer.compare(bytes1.length, bytes2.length);

            if (lenCmp != 0)
                return lenCmp;

            final int len = bytes1.length;
            final int words = len / 8;

            for (int i = 0; i < words; i++) {
                int off = GridUnsafe.BYTE_ARR_INT_OFF + i * 8;

                long b1 = GridUnsafe.getLong(bytes1, off);
                long b2 = GridUnsafe.getLong(bytes2, off);

                int cmp = Long.compare(b1, b2);

                if (cmp != 0)
                    return cmp;
            }

            for (int i = words * 8; i < len; i++) {
                byte b1 = bytes1[i];
                byte b2 = bytes2[i];

                if (b1 != b2)
                    return b1 > b2 ? 1 : -1;
            }

            return 0;
        }
    }

    /**
     *
     */
    protected class CacheDataRowStore extends RowStore {
        /** */
        private final int partId;

        /**
         * @param grp Cache group.
         * @param freeList Free list.
         * @param partId Partition number.
         */
        public CacheDataRowStore(CacheGroupContext grp, FreeList freeList, int partId) {
            super(grp, freeList);

            this.partId = partId;
        }

        /**
         * @param cacheId Cache ID.
         * @param hash Hash code.
         * @param link Link.
         * @return Search row.
         */
        private CacheSearchRow keySearchRow(int cacheId, int hash, long link) {
            DataRow dataRow = new DataRow(hash, link, partId, CacheDataRowAdapter.RowData.KEY_ONLY);

            if (dataRow.cacheId() == UNDEFINED_CACHE_ID && grp.sharedGroup())
                dataRow.cacheId(cacheId);

            return dataRow;
        }

        /**
         * @param cacheId Cache ID.
         * @param hash Hash code.
         * @param link Link.
         * @param rowData Required row data.
         * @return Data row.
         */
        private CacheDataRow dataRow(int cacheId, int hash, long link, CacheDataRowAdapter.RowData rowData) {
            DataRow dataRow = new DataRow(hash, link, partId, rowData);

            if (dataRow.cacheId() == UNDEFINED_CACHE_ID && grp.sharedGroup())
                dataRow.cacheId(cacheId);

            return dataRow;
        }
    }

    /**
     *
     */
    private interface RowLinkIO {
        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Row link.
         */
        public long getLink(long pageAddr, int idx);

        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Key hash code.
         */
        public int getHash(long pageAddr, int idx);

        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Cache ID or {@code 0} if cache ID is not defined.
         */
        public int getCacheId(long pageAddr, int idx);
    }

    /**
     *
     */
    private static abstract class AbstractDataInnerIO extends BPlusInnerIO<CacheSearchRow> implements RowLinkIO {
        /**
         * @param type Page type.
         * @param ver Page format version.
         * @param canGetRow If we can get full row from this page.
         * @param itemSize Single item size on page.
         */
        protected AbstractDataInnerIO(int type, int ver, boolean canGetRow, int itemSize) {
            super(type, ver, canGetRow, itemSize);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, CacheSearchRow row) {
            assert row.link() != 0;

            PageUtils.putLong(pageAddr, off, row.link());
            PageUtils.putInt(pageAddr, off + 8, row.hash());

            if (storeCacheId()) {
                assert row.cacheId() != UNDEFINED_CACHE_ID : row;

                PageUtils.putInt(pageAddr, off + 12, row.cacheId());
            }
        }

        /** {@inheritDoc} */
        @Override public CacheSearchRow getLookupRow(BPlusTree<CacheSearchRow, ?> tree, long pageAddr, int idx) {
            int cacheId = getCacheId(pageAddr, idx);
            int hash = getHash(pageAddr, idx);
            long link = getLink(pageAddr, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(cacheId, hash, link);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<CacheSearchRow> srcIo, long srcPageAddr,
            int srcIdx) {
            int hash = ((RowLinkIO)srcIo).getHash(srcPageAddr, srcIdx);
            long link = ((RowLinkIO)srcIo).getLink(srcPageAddr, srcIdx);
            int off = offset(dstIdx);

            PageUtils.putLong(dstPageAddr, off, link);
            PageUtils.putInt(dstPageAddr, off + 8, hash);

            if (storeCacheId()) {
                int cacheId = ((RowLinkIO)srcIo).getCacheId(srcPageAddr, srcIdx);

                assert cacheId != UNDEFINED_CACHE_ID;

                PageUtils.putInt(dstPageAddr, off + 12, cacheId);
            }
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            assert idx < getCount(pageAddr) : idx;

            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getHash(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, offset(idx) + 8);
        }

        /** {@inheritDoc} */
        @Override public void visit(long pageAddr, IgniteInClosure<CacheSearchRow> c) {
            int cnt = getCount(pageAddr);

            for (int i = 0; i < cnt; i++)
                c.apply(new CacheDataRowAdapter(getLink(pageAddr, i)));
        }

        /**
         * @return {@code True} if cache ID has to be stored.
         */
        protected abstract boolean storeCacheId();
    }

    /**
     *
     */
    private static abstract class AbstractDataLeafIO extends BPlusLeafIO<CacheSearchRow> implements RowLinkIO {
        /**
         * @param type Page type.
         * @param ver Page format version.
         * @param itemSize Single item size on page.
         */
        protected AbstractDataLeafIO(int type, int ver, int itemSize) {
            super(type, ver, itemSize);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, CacheSearchRow row) {
            assert row.link() != 0;

            PageUtils.putLong(pageAddr, off, row.link());
            PageUtils.putInt(pageAddr, off + 8, row.hash());

            if (storeCacheId()) {
                assert row.cacheId() != UNDEFINED_CACHE_ID;

                PageUtils.putInt(pageAddr, off + 12, row.cacheId());
            }
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<CacheSearchRow> srcIo, long srcPageAddr,
            int srcIdx) {
            int hash = ((RowLinkIO)srcIo).getHash(srcPageAddr, srcIdx);
            long link = ((RowLinkIO)srcIo).getLink(srcPageAddr, srcIdx);
            int off = offset(dstIdx);

            PageUtils.putLong(dstPageAddr, off, link);
            PageUtils.putInt(dstPageAddr, off + 8, hash);

            if (storeCacheId()) {
                int cacheId = ((RowLinkIO)srcIo).getCacheId(srcPageAddr, srcIdx);

                assert cacheId != UNDEFINED_CACHE_ID;

                PageUtils.putInt(dstPageAddr, off + 12, cacheId);
            }
        }

        /** {@inheritDoc} */
        @Override public CacheSearchRow getLookupRow(BPlusTree<CacheSearchRow, ?> tree, long buf, int idx) {
            int cacheId = getCacheId(buf, idx);
            int hash = getHash(buf, idx);
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(cacheId, hash, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            assert idx < getCount(pageAddr) : idx;

            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getHash(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, offset(idx) + 8);
        }

        /** {@inheritDoc} */
        @Override public void visit(long pageAddr, IgniteInClosure<CacheSearchRow> c) {
            int cnt = getCount(pageAddr);

            for (int i = 0; i < cnt; i++)
                c.apply(new CacheDataRowAdapter(getLink(pageAddr, i)));
        }

        /**
         * @return {@code True} if cache ID has to be stored.
         */
        protected abstract boolean storeCacheId();
    }

    /**
     *
     */
    public static final class DataInnerIO extends AbstractDataInnerIO {
        /** */
        public static final IOVersions<DataInnerIO> VERSIONS = new IOVersions<>(
            new DataInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        DataInnerIO(int ver) {
            super(T_DATA_REF_INNER, ver, true, 12);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return UNDEFINED_CACHE_ID;
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return false;
        }
    }

    /**
     *
     */
    public static final class DataLeafIO extends AbstractDataLeafIO {
        /** */
        public static final IOVersions<DataLeafIO> VERSIONS = new IOVersions<>(
            new DataLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        DataLeafIO(int ver) {
            super(T_DATA_REF_LEAF, ver, 12);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return UNDEFINED_CACHE_ID;
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return false;
        }
    }

    /**
     *
     */
    public static final class CacheIdAwareDataInnerIO extends AbstractDataInnerIO {
        /** */
        public static final IOVersions<CacheIdAwareDataInnerIO> VERSIONS = new IOVersions<>(
            new CacheIdAwareDataInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        CacheIdAwareDataInnerIO(int ver) {
            super(T_CACHE_ID_AWARE_DATA_REF_INNER, ver, true, 16);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, offset(idx) + 12);
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return true;
        }
    }

    /**
     *
     */
    public static final class CacheIdAwareDataLeafIO extends AbstractDataLeafIO {
        /** */
        public static final IOVersions<CacheIdAwareDataLeafIO> VERSIONS = new IOVersions<>(
            new CacheIdAwareDataLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        CacheIdAwareDataLeafIO(int ver) {
            super(T_CACHE_ID_AWARE_DATA_REF_LEAF, ver, 16);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, offset(idx) + 12);
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return true;
        }
    }

    /**
     *
     */
    private static class PendingRow {
        /** Expire time. */
        private long expireTime;

        /** Link. */
        private long link;

        /** Cache ID. */
        private int cacheId;

        /** */
        private KeyCacheObject key;

        /**
         * Creates a new instance which represents an upper or lower bound
         * inside a logical cache.
         *
         * @param cacheId Cache ID.
         */
        public PendingRow(int cacheId) {
            this.cacheId = cacheId;
        }

        /**
         * @param cacheId Cache ID.
         * @param expireTime Expire time.
         * @param link Link
         */
        PendingRow(int cacheId, long expireTime, long link) {
            assert expireTime != 0;

            this.cacheId = cacheId;
            this.expireTime = expireTime;
            this.link = link;
        }

        /**
         * @param grp Cache group.
         * @return Row.
         * @throws IgniteCheckedException If failed.
         */
        PendingRow initKey(CacheGroupContext grp) throws IgniteCheckedException {
            CacheDataRowAdapter rowData = new CacheDataRowAdapter(link);
            rowData.initFromLink(grp, CacheDataRowAdapter.RowData.KEY_ONLY);

            key = rowData.key();

            return this;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PendingRow.class, this);
        }
    }

    /**
     *
     */
    protected static class PendingEntriesTree extends BPlusTree<PendingRow, PendingRow> {
        /** */
        private final static Object WITHOUT_KEY = new Object();

        /** */
        private final CacheGroupContext grp;

        /**
         * @param grp Cache group.
         * @param name Tree name.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         * @param initNew Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        public PendingEntriesTree(
            CacheGroupContext grp,
            String name,
            PageMemory pageMem,
            long metaPageId,
            ReuseList reuseList,
            boolean initNew)
            throws IgniteCheckedException {
            super(name,
                grp.groupId(),
                pageMem,
                grp.shared().wal(),
                grp.offheap().globalRemoveId(),
                metaPageId,
                reuseList,
                grp.sharedGroup() ? CacheIdAwarePendingEntryInnerIO.VERSIONS : PendingEntryInnerIO.VERSIONS,
                grp.sharedGroup() ? CacheIdAwarePendingEntryLeafIO.VERSIONS : PendingEntryLeafIO.VERSIONS);

            this.grp = grp;

            initTree(initNew);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<PendingRow> iox, long pageAddr, int idx, PendingRow row)
            throws IgniteCheckedException {
            PendingRowIO io = (PendingRowIO)iox;

            int cmp;

            if (grp.sharedGroup()) {
                assert row.cacheId != UNDEFINED_CACHE_ID : "Cache ID is not provided!";
                assert io.getCacheId(pageAddr, idx) != UNDEFINED_CACHE_ID : "Cache ID is not stored!";

                cmp = Integer.compare(io.getCacheId(pageAddr, idx), row.cacheId);

                if (cmp != 0)
                    return cmp;

                if(cmp == 0 && row.expireTime == 0 && row.link == 0) {
                    // A search row with a cach ID only is used as a cache bound.
                    // The found position will be shifted until the exact cache bound is found;
                    // See for details:
                    // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                    // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                    return cmp;
                }
            }

            long expireTime = io.getExpireTime(pageAddr, idx);

            cmp = Long.compare(expireTime, row.expireTime);

            if (cmp != 0)
                return cmp;

            if (row.link == 0L)
                return 0;

            long link = io.getLink(pageAddr, idx);

            return Long.compare(link, row.link);
        }

        /** {@inheritDoc} */
        @Override protected PendingRow getRow(BPlusIO<PendingRow> io, long pageAddr, int idx, Object flag)
            throws IgniteCheckedException {
            PendingRow row = io.getLookupRow(this, pageAddr, idx);

            return flag == WITHOUT_KEY ? row : row.initKey(grp);
        }
    }

    /**
     *
     */
    private interface PendingRowIO {
        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Expire time.
         */
        long getExpireTime(long pageAddr, int idx);

        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Link.
         */
        long getLink(long pageAddr, int idx);

        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Cache ID or {@code 0} if Cache ID is not defined.
         */
        int getCacheId(long pageAddr, int idx);
    }

    /**
     *
     */
    private static abstract class AbstractPendingEntryInnerIO extends BPlusInnerIO<PendingRow> implements PendingRowIO {
        /**
         * @param type Page type.
         * @param ver Page format version.
         * @param canGetRow If we can get full row from this page.
         * @param itemSize Single item size on page.
         */
        protected AbstractPendingEntryInnerIO(int type, int ver, boolean canGetRow, int itemSize) {
            super(type, ver, canGetRow, itemSize);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, PendingRow row) throws IgniteCheckedException {
            assert row.link != 0;
            assert row.expireTime != 0;

            PageUtils.putLong(pageAddr, off, row.expireTime);
            PageUtils.putLong(pageAddr, off + 8, row.link);

            if (storeCacheId()) {
                assert row.cacheId != UNDEFINED_CACHE_ID;

                PageUtils.putInt(pageAddr, off + 16, row.cacheId);
            }
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr,
            int dstIdx,
            BPlusIO<PendingRow> srcIo,
            long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            int dstOff = offset(dstIdx);

            long link = ((PendingRowIO)srcIo).getLink(srcPageAddr, srcIdx);
            long expireTime = ((PendingRowIO)srcIo).getExpireTime(srcPageAddr, srcIdx);

            PageUtils.putLong(dstPageAddr, dstOff, expireTime);
            PageUtils.putLong(dstPageAddr, dstOff + 8, link);

            if (storeCacheId()) {
                int cacheId = ((PendingRowIO)srcIo).getCacheId(srcPageAddr, srcIdx);

                assert cacheId != UNDEFINED_CACHE_ID;

                PageUtils.putInt(dstPageAddr, dstOff + 16, cacheId);
            }
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, long pageAddr, int idx)
            throws IgniteCheckedException {
            return new PendingRow(getCacheId(pageAddr, idx), getExpireTime(pageAddr, idx), getLink(pageAddr, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx) + 8);
        }

        /**
         * @return {@code True} if cache ID has to be stored.
         */
        protected abstract boolean storeCacheId();
    }

    /**
     *
     */
    private static abstract class AbstractPendingEntryLeafIO extends BPlusLeafIO<PendingRow> implements PendingRowIO {
        /**
         * @param type Page type.
         * @param ver Page format version.
         * @param itemSize Single item size on page.
         */
        protected AbstractPendingEntryLeafIO(int type, int ver, int itemSize) {
            super(type, ver, itemSize);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, PendingRow row) throws IgniteCheckedException {
            assert row.link != 0;
            assert row.expireTime != 0;

            PageUtils.putLong(pageAddr, off, row.expireTime);
            PageUtils.putLong(pageAddr, off + 8, row.link);

            if (storeCacheId()) {
                assert row.cacheId != UNDEFINED_CACHE_ID;

                PageUtils.putInt(pageAddr, off + 16, row.cacheId);
            }
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr,
            int dstIdx,
            BPlusIO<PendingRow> srcIo,
            long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            int dstOff = offset(dstIdx);

            long link = ((PendingRowIO)srcIo).getLink(srcPageAddr, srcIdx);
            long expireTime = ((PendingRowIO)srcIo).getExpireTime(srcPageAddr, srcIdx);

            PageUtils.putLong(dstPageAddr, dstOff, expireTime);
            PageUtils.putLong(dstPageAddr, dstOff + 8, link);

            if (storeCacheId()) {
                int cacheId = ((PendingRowIO)srcIo).getCacheId(srcPageAddr, srcIdx);

                assert cacheId != UNDEFINED_CACHE_ID;

                PageUtils.putInt(dstPageAddr, dstOff + 16, cacheId);
            }
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, long pageAddr, int idx)
            throws IgniteCheckedException {
            return new PendingRow(getCacheId(pageAddr, idx), getExpireTime(pageAddr, idx), getLink(pageAddr, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx) + 8);
        }

        /**
         * @return {@code True} if cache ID has to be stored.
         */
        protected abstract boolean storeCacheId();
    }

    /**
     *
     */
    public static final class PendingEntryInnerIO extends AbstractPendingEntryInnerIO {
        /** */
        public static final IOVersions<PendingEntryInnerIO> VERSIONS = new IOVersions<>(
            new PendingEntryInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        PendingEntryInnerIO(int ver) {
            super(T_PENDING_REF_INNER, ver, true, 16);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return UNDEFINED_CACHE_ID;
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return false;
        }
    }

    /**
     *
     */
    public static final class PendingEntryLeafIO extends AbstractPendingEntryLeafIO {
        /** */
        public static final IOVersions<PendingEntryLeafIO> VERSIONS = new IOVersions<>(
            new PendingEntryLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        PendingEntryLeafIO(int ver) {
            super(T_PENDING_REF_LEAF, ver, 16);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return UNDEFINED_CACHE_ID;
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return false;
        }
    }

    /**
     *
     */
    public static final class CacheIdAwarePendingEntryInnerIO extends AbstractPendingEntryInnerIO {
        /** */
        public static final IOVersions<CacheIdAwarePendingEntryInnerIO> VERSIONS = new IOVersions<>(
            new CacheIdAwarePendingEntryInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        CacheIdAwarePendingEntryInnerIO(int ver) {
            super(T_CACHE_ID_AWARE_PENDING_REF_INNER, ver, true, 20);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, offset(idx) + 16);
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return true;
        }
    }

    /**
     *
     */
    public static final class CacheIdAwarePendingEntryLeafIO extends AbstractPendingEntryLeafIO {
        /** */
        public static final IOVersions<CacheIdAwarePendingEntryLeafIO> VERSIONS = new IOVersions<>(
            new CacheIdAwarePendingEntryLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        CacheIdAwarePendingEntryLeafIO(int ver) {
            super(T_CACHE_ID_AWARE_PENDING_REF_LEAF, ver, 20);
        }


        @Override public int getCacheId(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, offset(idx) + 16);
        }

        /** {@inheritDoc} */
        @Override protected boolean storeCacheId() {
            return true;
        }
    }
}
