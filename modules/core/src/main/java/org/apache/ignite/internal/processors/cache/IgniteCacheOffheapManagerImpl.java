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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.database.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.local.GridLocalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
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
public class IgniteCacheOffheapManagerImpl extends GridCacheManagerAdapter implements IgniteCacheOffheapManager {
    /** */
    private boolean indexingEnabled;

    /** */
    // TODO GG-11208 need restore size after restart.
    private CacheDataStore locCacheDataStore;

    /** */
    protected final ConcurrentMap<Integer, CacheDataStore> partDataStores = new ConcurrentHashMap<>();

    /** */
    protected final CacheDataStore rmvdStore = new CacheDataStoreImpl(-1, null, null, null);

    /** */
    protected PendingEntriesTree pendingEntries;

    /** */
    private static final PendingRow START_PENDING_ROW = new PendingRow(Long.MIN_VALUE, 0);

    /** */
    private final GridAtomicLong globalRmvId = new GridAtomicLong(U.currentTimeMillis() * 1000_000);

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private int updateValSizeThreshold;

    /** {@inheritDoc} */
    @Override public GridAtomicLong globalRemoveId() {
        return globalRmvId;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        indexingEnabled = GridQueryProcessor.isEnabled(cctx.config());

        updateValSizeThreshold = cctx.kernalContext().config().getMemoryConfiguration().getPageSize() / 2;

        if (cctx.affinityNode()) {
            if (cctx.kernalContext().clientNode()) {
                assert cctx.isLocal() : cctx.name();

                cctx.shared().database().init();
            }

            cctx.shared().database().checkpointReadLock();

            try {
                initDataStructures();

                if (cctx.isLocal()) {
                    assert cctx.cache() instanceof GridLocalCache : cctx.cache();

                    locCacheDataStore = createCacheDataStore(0);
                }
            }
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDataStructures() throws IgniteCheckedException {
        if (cctx.shared().ttl().eagerTtlEnabled()) {
            String name = "PendingEntries";

            long rootPage = allocateForTree();

            pendingEntries = new PendingEntriesTree(cctx,
                name,
                cctx.shared().database().pageMemory(),
                rootPage,
                cctx.shared().database().globalReuseList(),
                true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(final boolean cancel, final boolean destroy) {
        super.stop0(cancel, destroy);

        if (destroy && cctx.affinityNode())
            destroyCacheDataStructures(destroy);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        busyLock.block();
    }

    /**
     *
     */
    protected void destroyCacheDataStructures(boolean destroy) {
        assert cctx.affinityNode();

        try {
            if (locCacheDataStore != null)
                locCacheDataStore.destroy();

            if (pendingEntries != null)
                pendingEntries.destroy();

            for (CacheDataStore store : partDataStores.values())
                store.destroy();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * @param part Partition.
     * @return Data store for given entry.
     */
    private CacheDataStore dataStore(GridDhtLocalPartition part) {
        if (cctx.isLocal())
            return locCacheDataStore;
        else {
            assert part != null;

            return part.dataStore();
        }
    }

    /** {@inheritDoc} */
    @Override public long entriesCount() {
        if (cctx.isLocal())
            return locCacheDataStore.size();

        long size = 0;

        for (CacheDataStore store : partDataStores.values())
            size += store.size();

        return size;
    }

    /**
     * @param p Partition.
     * @return Partition data.
     */
    @Nullable private CacheDataStore partitionData(int p) {
        if (cctx.isLocal())
            return locCacheDataStore;
        else {
            GridDhtLocalPartition part = cctx.topology().localPartition(p, AffinityTopologyVersion.NONE, false);

            return part != null ? part.dataStore() : null;
        }
    }

    /** {@inheritDoc} */
    @Override public long entriesCount(
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        if (cctx.isLocal())
            return entriesCount(0);
        else {
            ClusterNode locNode = cctx.localNode();

            long cnt = 0;

            for (GridDhtLocalPartition locPart : cctx.topology().currentLocalPartitions()) {
                if (primary) {
                    if (cctx.affinity().primary(locNode, locPart.id(), topVer)) {
                        cnt += locPart.size();

                        continue;
                    }
                }

                if (backup) {
                    if (cctx.affinity().backup(locNode, locPart.id(), topVer))
                        cnt += locPart.size();
                }
            }

            return cnt;
        }
    }

    /** {@inheritDoc} */
    @Override public long entriesCount(int part) {
        if (cctx.isLocal()){
            assert part == 0;

            return locCacheDataStore.size();
        }
        else {
            GridDhtLocalPartition locPart = cctx.topology().localPartition(part, AffinityTopologyVersion.NONE, false);

            return locPart == null ? 0 : locPart.size();
        }
    }

    /**
     * @param primary Primary data flag.
     * @param backup Primary data flag.
     * @param topVer Topology version.
     * @return Data stores iterator.
     */
    private Iterator<CacheDataStore> cacheData(boolean primary, boolean backup, AffinityTopologyVersion topVer) {
        assert primary || backup;

        if (cctx.isLocal())
            return Collections.singleton(locCacheDataStore).iterator();
        else {
            final Iterator<GridDhtLocalPartition> it = cctx.topology().currentLocalPartitions().iterator();

            if (primary && backup) {
                return F.iterator(it, new IgniteClosure<GridDhtLocalPartition, CacheDataStore>() {
                    @Override public CacheDataStore apply(GridDhtLocalPartition part) {
                        return part.dataStore();
                    }
                }, true);
            }

            final Set<Integer> parts = primary ? cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer) :
                cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

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
    @Override public void update(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        int partId,
        GridDhtLocalPartition part,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        assert expireTime >= 0;

        dataStore(part).update(key, partId, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public void remove(
        KeyCacheObject key,
        int partId,
        GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        dataStore(part).remove(key, partId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override @Nullable public CacheDataRow read(GridCacheMapEntry entry)
        throws IgniteCheckedException {
        KeyCacheObject key = entry.key();

        assert cctx.isLocal() || entry.localPartition() != null : entry;

        return dataStore(entry.localPartition()).find(key);
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
    @Override public void clear(boolean readers) {
        GridCacheVersion obsoleteVer = null;

        GridIterator<CacheDataRow> it = rowsIterator(true, true, null);

        while (it.hasNext()) {
            KeyCacheObject key = it.next().key();

            try {
                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

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
    @Override public void writeAll(Iterable<GridCacheBatchSwapEntry> swapped) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param primary {@code True} if need return primary entries.
     * @param backup {@code True} if need return backup entries.
     * @param topVer Topology version to use.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<Cache.Entry<K, V>> entriesIterator(final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary) throws IgniteCheckedException {
        final Iterator<CacheDataRow> it = rowsIterator(primary, backup, topVer);

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
    @Override public GridCloseableIterator<KeyCacheObject> keysIterator(final int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        final GridCursor<? extends CacheDataRow> cur = data.cursor();

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
    @Override public GridIterator<CacheDataRow> iterator(boolean primary, boolean backups,
        final AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        return rowsIterator(primary, backups, topVer);
    }

    /**
     * @param primary Primary entries flag.
     * @param backups Backup entries flag.
     * @param topVer Topology version.
     * @return Iterator.
     */
    private GridIterator<CacheDataRow> rowsIterator(boolean primary, boolean backups, AffinityTopologyVersion topVer) {
        final Iterator<CacheDataStore> dataIt = cacheData(primary, backups, topVer);

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private GridCursor<? extends CacheDataRow> cur;

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
                        if (dataIt.hasNext())
                            cur = dataIt.next().cursor();
                        else
                            break;
                    }

                    if (cur.next()) {
                        next = cur.get();

                        break;
                    }
                    else
                        cur = null;
                }

                return next != null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> iterator(int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

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
        };
    }

    /**
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long allocateForTree() throws IgniteCheckedException {
        ReuseList reuseList = cctx.shared().database().globalReuseList();

        long pageId;

        if (reuseList == null || (pageId = reuseList.takeRecycledPage()) == 0L)
            pageId = cctx.shared().database().pageMemory().allocatePage(cctx.cacheId(), INDEX_PARTITION, FLAG_IDX);

        return pageId;
    }

    /** {@inheritDoc} */
    @Override public RootPage rootPageForIndex(String idxName) throws IgniteCheckedException {
        long pageId = allocateForTree();

        return new RootPage(new FullPageId(pageId, cctx.cacheId()), true);
    }

    /** {@inheritDoc} */
    @Override public void dropRootPageForIndex(String idxName) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseListForIndex(String idxName) {
        return cctx.shared().database().globalReuseList();
    }

    /** {@inheritDoc} */
    @Override public IgniteRebalanceIterator rebalanceIterator(int part, AffinityTopologyVersion topVer, Long partCntr)
        throws IgniteCheckedException {
        final GridIterator<CacheDataRow> it = iterator(part);

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
        CacheDataStore dataStore = null;
        CacheDataStore oldStore = null;

        do {
            dataStore = createCacheDataStore0(p);

            oldStore = partDataStores.putIfAbsent(p, dataStore);
        }
        while (oldStore != null);

        return dataStore;
    }

    /**
     * @param p Partition.
     * @return Cache data store.
     * @throws IgniteCheckedException If failed.
     */
    protected CacheDataStore createCacheDataStore0(int p)
        throws IgniteCheckedException {
        IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

        final long rootPage = allocateForTree();

        FreeList freeList = cctx.shared().database().globalFreeList();

        CacheDataRowStore rowStore = new CacheDataRowStore(cctx, freeList);

        String idxName = treeName(p);

        CacheDataTree dataTree = new CacheDataTree(idxName,
            cctx.shared().database().globalReuseList(),
            rowStore,
            cctx,
            dbMgr.pageMemory(),
            rootPage,
            true);

        return new CacheDataStoreImpl(p, idxName, rowStore, dataTree);
    }

    /** {@inheritDoc} */
    @Override public Iterable<CacheDataStore> cacheDataStores() {
        if (cctx.isLocal())
            return Collections.singleton(locCacheDataStore);

        return new Iterable<CacheDataStore>() {
            @Override public Iterator<CacheDataStore> iterator() {
                return partDataStores.values().iterator();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void destroyCacheDataStore(int p, CacheDataStore store) throws IgniteCheckedException {
        try {
            partDataStores.remove(p, store);

            store.destroy();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
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
        IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c,
        int amount
    ) throws IgniteCheckedException {
        if (pendingEntries != null) {
            GridCacheVersion obsoleteVer = null;

            long now = U.currentTimeMillis();

            GridCursor<PendingRow> cur = pendingEntries.find(START_PENDING_ROW, new PendingRow(now, 0));

            int cleared = 0;

            while (cur.next()) {
                PendingRow row = cur.get();

                if (amount != -1 && cleared > amount)
                    return true;

                assert row.key != null && row.link != 0 && row.expireTime != 0 : row;

                if (pendingEntries.remove(row) != null) {
                    if (obsoleteVer == null)
                        obsoleteVer = cctx.versions().next();

                    c.apply(cctx.cache().entryEx(row.key), obsoleteVer);
                }

                cleared++;
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
        protected final AtomicLong storageSize = new AtomicLong();

        /** Initialized update counter. */
        protected Long initCntr = 0L;

        /**
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

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public int size() {
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
         * @param oldRow Old row.
         * @param dataRow New row.
         * @return {@code True} if it is possible to update old row data.
         * @throws IgniteCheckedException If failed.
         */
        private boolean canUpdateOldRow(@Nullable CacheDataRow oldRow, DataRow dataRow)
            throws IgniteCheckedException {
            if (oldRow == null || indexingEnabled)
                return false;

            CacheObjectContext coCtx = cctx.cacheObjectContext();

            int oldLen = oldRow.key().valueBytesLength(coCtx) + oldRow.value().valueBytesLength(coCtx);

            if (oldLen > updateValSizeThreshold)
                return false;

            int newLen = dataRow.key().valueBytesLength(coCtx) + dataRow.value().valueBytesLength(coCtx);

            return oldLen == newLen;
        }

        /** {@inheritDoc} */
        @Override public void update(KeyCacheObject key,
            int p,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            assert oldRow == null || oldRow.link() != 0L : oldRow;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                DataRow dataRow = new DataRow(key, val, ver, p, expireTime);

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                // Make sure value bytes initialized.
                key.valueBytes(coCtx);
                val.valueBytes(coCtx);

                CacheDataRow old;

                boolean rmvOld = true;

                if (canUpdateOldRow(oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow)) {
                    old = oldRow;

                    dataRow.link(oldRow.link());

                    rmvOld = false;
                }
                else {
                    rowStore.addRow(dataRow);

                    assert dataRow.link() != 0 : dataRow;

                    if (oldRow != null) {
                        old = oldRow;

                        dataTree.putx(dataRow);
                    }
                    else
                        old = dataTree.put(dataRow);

                    if (old == null)
                        storageSize.incrementAndGet();
                }

                if (indexingEnabled) {
                    GridCacheQueryManager qryMgr = cctx.queries();

                    assert qryMgr.enabled();

                    if (old != null)
                        qryMgr.store(key, p, old.value(), old.version(), val, ver, expireTime, dataRow.link());
                    else
                        qryMgr.store(key, p, null, null, val, ver, expireTime, dataRow.link());
                }

                if (old != null) {
                    assert old.link() != 0 : old;

                    if (pendingEntries != null && old.expireTime() != 0)
                        pendingEntries.removex(new PendingRow(old.expireTime(), old.link()));

                    if (rmvOld)
                        rowStore.removeRow(old.link());
                }

                if (pendingEntries != null && expireTime != 0)
                    pendingEntries.putx(new PendingRow(expireTime, dataRow.link()));

                updateIgfsMetrics(key, (old != null ? old.value() : null), val);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(KeyCacheObject key, int partId) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                CacheDataRow dataRow = dataTree.remove(new SearchRow(key));

                CacheObject val = null;
                GridCacheVersion ver = null;

                if (dataRow != null) {
                    assert dataRow.link() != 0 : dataRow;

                    if (pendingEntries != null && dataRow.expireTime() != 0)
                        pendingEntries.removex(new PendingRow(dataRow.expireTime(), dataRow.link()));

                    storageSize.decrementAndGet();

                    val = dataRow.value();

                    ver = dataRow.version();
                }

                if (indexingEnabled) {
                    GridCacheQueryManager qryMgr = cctx.queries();

                    assert qryMgr.enabled();

                    qryMgr.remove(key, partId, val, ver);
                }

                if (dataRow != null)
                    rowStore.removeRow(dataRow.link());

                updateIgfsMetrics(key, (dataRow != null ? dataRow.value() : null), null);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(KeyCacheObject key) throws IgniteCheckedException {
            key.valueBytes(cctx.cacheObjectContext());

            CacheDataRow row = dataTree.findOne(new SearchRow(key), dataTree.noKeyC);

            if (row != null)
                ((CacheDataRowAdapter)row).key(key);

            return row;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            return dataTree.find(null, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException {
            SearchRow lowerRow = null;
            SearchRow upperRow = null;

            if (lower != null)
                lowerRow = new SearchRow(lower);

            if (upper != null)
                upperRow = new SearchRow(upper);

            return dataTree.find(lowerRow, upperRow);
        }

        /** {@inheritDoc} */
        @Override public void destroy() throws IgniteCheckedException {
            dataTree.destroy();
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
        @Override public Long initialUpdateCounter() {
            return initCntr;
        }

        /** {@inheritDoc} */
        @Override public void updateInitialCounter(long cntr) {
            if (updateCounter() < cntr)
                updateCounter(cntr);

            initCntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public void init(long size, long updCntr) {
            initCntr = updCntr;
            storageSize.set(size);
            cntr.set(updCntr);
        }

        /**
         * @param key Key.
         * @param oldVal Old value.
         * @param newVal New value.
         */
        private void updateIgfsMetrics(
            KeyCacheObject key,
            CacheObject oldVal,
            CacheObject newVal
            ) throws IgniteCheckedException {
            // In case we deal with IGFS cache, count updated data
            if (cctx.cache().isIgfsDataCache() &&
                !cctx.isNear() &&
                cctx.kernalContext()
                    .igfsHelper()
                    .isIgfsBlockKey(key.value(cctx.cacheObjectContext(), false))) {
                int oldSize = valueLength(oldVal);
                int newSize = valueLength(newVal);

                int delta = newSize - oldSize;

                if (delta != 0)
                    cctx.cache().onIgfsDataSizeChanged(delta);
            }
        }

        /**
         * Isolated method to get length of IGFS block.
         *
         * @param val Value.
         * @return Length of value.
         */
        private int valueLength(@Nullable CacheObject val) {
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

        /**
         * @param key Key.
         */
        SearchRow(KeyCacheObject key) {
            this.key = key;

            hash = key.hashCode();
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
    }

    /**
     *
     */
    private class DataRow extends CacheDataRowAdapter {
        /** */
        protected int part = -1;

        /** */
        protected int hash;

        /**
         * @param hash Hash code.
         * @param link Link.
         * @param rowData Required row data.
         */
        DataRow(int hash, long link, CacheDataRowAdapter.RowData rowData) {
            super(link);

            this.hash = hash;

            part = PageIdUtils.partId(link);

            try {
                // We can not init data row lazily because underlying buffer can be concurrently cleared.
                initFromLink(cctx, rowData);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param part Partition.
         * @param expireTime Expire time.
         */
        DataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long expireTime) {
            super(0);

            this.hash = key.hashCode();
            this.key = key;
            this.val = val;
            this.ver = ver;
            this.part = part;
            this.expireTime = expireTime;
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
    }

    /**
     *
     */
    protected static class CacheDataTree extends BPlusTree<CacheSearchRow, CacheDataRow> {
        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final GridCacheContext cctx;

        /** */
        private final RowClosure<CacheSearchRow, CacheDataRow> noKeyC = new RowClosure<CacheSearchRow, CacheDataRow>() {
            @Override public CacheDataRow row(BPlusIO<CacheSearchRow> io, long pageAddr, int idx)
                throws IgniteCheckedException {
                int hash = ((RowLinkIO)io).getHash(pageAddr, idx);
                long link = ((RowLinkIO)io).getLink(pageAddr, idx);

                return rowStore.dataRow(hash, link, CacheDataRowAdapter.RowData.NO_KEY);
            }
        };

        /**
         * @param name Tree name.
         * @param reuseList Reuse list.
         * @param rowStore Row store.
         * @param cctx Context.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param initNew Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        public CacheDataTree(
            String name,
            ReuseList reuseList,
            CacheDataRowStore rowStore,
            GridCacheContext cctx,
            PageMemory pageMem,
            long metaPageId,
            boolean initNew
        ) throws IgniteCheckedException {
            super(name, cctx.cacheId(), pageMem, cctx.shared().wal(), cctx.offheap().globalRemoveId(), metaPageId,
                reuseList, DataInnerIO.VERSIONS, DataLeafIO.VERSIONS);

            assert rowStore != null;

            this.rowStore = rowStore;
            this.cctx = cctx;

            initTree(initNew);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<CacheSearchRow> io, long pageAddr, int idx, CacheSearchRow row)
            throws IgniteCheckedException {
            int hash = ((RowLinkIO)io).getHash(pageAddr, idx);

            int cmp = Integer.compare(hash, row.hash());

            if (cmp != 0)
                return cmp;

            long link = ((RowLinkIO)io).getLink(pageAddr, idx);

            assert row.key() != null : row;

            return compareKeys(row.key(), link);
        }

        /** {@inheritDoc} */
        @Override protected CacheDataRow getRow(BPlusIO<CacheSearchRow> io, long pageAddr, int idx)
            throws IgniteCheckedException {
            int hash = ((RowLinkIO)io).getHash(pageAddr, idx);
            long link = ((RowLinkIO)io).getLink(pageAddr, idx);

            return rowStore.dataRow(hash, link, CacheDataRowAdapter.RowData.FULL);
        }

        /**
         * @param key Key.
         * @param link Link.
         * @return Compare result.
         * @throws IgniteCheckedException If failed.
         */
        private int compareKeys(KeyCacheObject key, final long link) throws IgniteCheckedException {
            byte[] bytes = key.valueBytes(cctx.cacheObjectContext());

            PageMemory pageMem = cctx.shared().database().pageMemory();

            try (Page page = page(pageId(link))) {
                long pageAddr = page.getForReadPointer(); // Non-empty data page must not be recycled.

                assert pageAddr != 0L : link;

                try {
                    DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                    DataPagePayload data = io.readPayload(pageAddr,
                        itemId(link),
                        pageMem.pageSize());

                    if (data.nextLink() == 0) {
                        long addr = pageAddr + data.offset();

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
                    page.releaseRead();
                }
            }

            // TODO GG-11768.
            CacheDataRowAdapter other = new CacheDataRowAdapter(link);
            other.initFromLink(cctx, CacheDataRowAdapter.RowData.KEY_ONLY);

            byte[] bytes1 = other.key().valueBytes(cctx.cacheObjectContext());
            byte[] bytes2 = key.valueBytes(cctx.cacheObjectContext());

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
        /**
         * @param cctx Cache context.
         * @param freeList Free list.
         */
        public CacheDataRowStore(GridCacheContext<?, ?> cctx, FreeList freeList) {
            super(cctx, freeList);
        }

        /**
         * @param hash Hash code.
         * @param link Link.
         * @return Search row.
         */
        private CacheSearchRow keySearchRow(int hash, long link) {
            return new DataRow(hash, link, CacheDataRowAdapter.RowData.KEY_ONLY);
        }

        /**
         * @param hash Hash code.
         * @param link Link.
         * @param rowData Required row data.
         * @return Data row.
         */
        private CacheDataRow dataRow(int hash, long link, CacheDataRowAdapter.RowData rowData) {
            return new DataRow(hash, link, rowData);
        }
    }

    /**
     * @param buf Buffer.
     * @param off Offset.
     * @param link Link.
     * @param hash Hash.
     */
    private static void store0(ByteBuffer buf, int off, long link, int hash) {
        buf.putLong(off, link);
        buf.putInt(off + 8, hash);
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param link Link.
     * @param hash Hash.
     */
    private static void store0(long pageAddr, int off, long link, int hash) {
        PageUtils.putLong(pageAddr, off, link);
        PageUtils.putInt(pageAddr, off + 8, hash);
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
    }

    /**
     *
     */
    public static final class DataInnerIO extends BPlusInnerIO<CacheSearchRow> implements RowLinkIO {
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

        /** {@inheritDoc} */
        @Override public void storeByOffset(ByteBuffer buf, int off, CacheSearchRow row) throws IgniteCheckedException {
            assert row.link() != 0;

            store0(buf, off, row.link(), row.hash());
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, CacheSearchRow row) {
            assert row.link() != 0;

            store0(pageAddr, off, row.link(), row.hash());
        }

        /** {@inheritDoc} */
        @Override public CacheSearchRow getLookupRow(BPlusTree<CacheSearchRow, ?> tree, long pageAddr, int idx) {
            int hash = getHash(pageAddr, idx);
            long link = getLink(pageAddr, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(hash, link);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<CacheSearchRow> srcIo, long srcPageAddr,
            int srcIdx) {
            int hash = ((RowLinkIO)srcIo).getHash(srcPageAddr, srcIdx);
            long link = ((RowLinkIO)srcIo).getLink(srcPageAddr, srcIdx);

            store0(dstPageAddr, offset(dstIdx), link, hash);
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
    }

    /**
     *
     */
    public static final class DataLeafIO extends BPlusLeafIO<CacheSearchRow> implements RowLinkIO {
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

        /** {@inheritDoc} */
        @Override public void storeByOffset(ByteBuffer buf, int off, CacheSearchRow row) throws IgniteCheckedException {
            assert row.link() != 0;

            store0(buf, off, row.link(), row.hash());
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, CacheSearchRow row) {
            assert row.link() != 0;

            store0(pageAddr, off, row.link(), row.hash());
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<CacheSearchRow> srcIo, long srcPageAddr,
            int srcIdx) {
            store0(dstPageAddr, offset(dstIdx), getLink(srcPageAddr, srcIdx), getHash(srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public CacheSearchRow getLookupRow(BPlusTree<CacheSearchRow, ?> tree, long buf, int idx) {
            int hash = getHash(buf, idx);
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(hash, link);
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
    }

    /**
     *
     */
    private static class PendingRow {
        /** Expire time. */
        private long expireTime;

        /** Link. */
        private long link;

        /** */
        private KeyCacheObject key;

        /**
         * @param expireTime Expire time.
         * @param link Link
         */
        PendingRow(long expireTime, long link) {
            assert expireTime != 0;

            this.expireTime = expireTime;
            this.link = link;
        }

        /**
         * @param cctx Context.
         * @param expireTime Expire time.
         * @param link Link.
         * @return Row.
         * @throws IgniteCheckedException If failed.
         */
        static PendingRow createRowWithKey(GridCacheContext cctx, long expireTime, long link)
            throws IgniteCheckedException {
            PendingRow row = new PendingRow(expireTime, link);

            CacheDataRowAdapter rowData = new CacheDataRowAdapter(link);

            rowData.initFromLink(cctx, CacheDataRowAdapter.RowData.KEY_ONLY);

            row.key = rowData.key();

            return row;
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
        private final GridCacheContext cctx;

        /**
         * @param cctx Cache context.
         * @param name Tree name.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         * @param initNew Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        public PendingEntriesTree(
            GridCacheContext cctx,
            String name,
            PageMemory pageMem,
            long metaPageId,
            ReuseList reuseList,
            boolean initNew)
            throws IgniteCheckedException {
            super(name,
                cctx.cacheId(),
                pageMem,
                cctx.shared().wal(),
                cctx.offheap().globalRemoveId(),
                metaPageId,
                reuseList,
                PendingEntryInnerIO.VERSIONS,
                PendingEntryLeafIO.VERSIONS);

            this.cctx = cctx;

            initTree(initNew);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<PendingRow> io, long pageAddr, int idx, PendingRow row)
            throws IgniteCheckedException {
            long expireTime = ((PendingRowIO)io).getExpireTime(pageAddr, idx);

            int cmp = Long.compare(expireTime, row.expireTime);

            if (cmp != 0)
                return cmp;

            if (row.link == 0L)
                return 0;

            long link = ((PendingRowIO)io).getLink(pageAddr, idx);

            return Long.compare(link, row.link);
        }

        /** {@inheritDoc} */
        @Override protected PendingRow getRow(BPlusIO<PendingRow> io, long pageAddr, int idx)
            throws IgniteCheckedException {
            return io.getLookupRow(this, pageAddr, idx);
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
    }

    /**
     *
     */
    public static class PendingEntryInnerIO extends BPlusInnerIO<PendingRow> implements PendingRowIO {
        /** */
        public static final IOVersions<PendingEntryInnerIO> VERSIONS = new IOVersions<>(
            new PendingEntryInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        PendingEntryInnerIO(int ver) {
            super(T_PENDING_REF_INNER, ver, true, 8 + 8);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(ByteBuffer buf, int off, PendingRow row) throws IgniteCheckedException {
            assert row.link != 0;
            assert row.expireTime != 0;

            buf.putLong(off, row.expireTime);
            buf.putLong(off + 8, row.link);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, PendingRow row) throws IgniteCheckedException {
            assert row.link != 0;
            assert row.expireTime != 0;

            PageUtils.putLong(pageAddr, off, row.expireTime);
            PageUtils.putLong(pageAddr, off + 8, row.link);
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
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, long pageAddr, int idx)
            throws IgniteCheckedException {
            return PendingRow.createRowWithKey(((PendingEntriesTree)tree).cctx,
                getExpireTime(pageAddr, idx),
                getLink(pageAddr, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx) + 8);
        }
    }

    /**
     *
     */
    public static class PendingEntryLeafIO extends BPlusLeafIO<PendingRow> implements PendingRowIO {
        /** */
        public static final IOVersions<PendingEntryLeafIO> VERSIONS = new IOVersions<>(
            new PendingEntryLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        PendingEntryLeafIO(int ver) {
            super(T_PENDING_REF_LEAF, ver, 8 + 8);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(ByteBuffer buf, int off, PendingRow row) throws IgniteCheckedException {
            assert row.link != 0;
            assert row.expireTime != 0;

            buf.putLong(off, row.expireTime);
            buf.putLong(off + 8, row.link);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, PendingRow row) throws IgniteCheckedException {
            assert row.link != 0;
            assert row.expireTime != 0;

            PageUtils.putLong(pageAddr, off, row.expireTime);
            PageUtils.putLong(pageAddr, off + 8, row.link);
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
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, long pageAddr, int idx)
            throws IgniteCheckedException {
            return PendingRow.createRowWithKey(((PendingEntriesTree)tree).cctx,
                getExpireTime(pageAddr, idx),
                getLink(pageAddr, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx) + 8);
        }
    }
}
