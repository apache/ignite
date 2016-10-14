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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
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

/**
 *
 */
@SuppressWarnings("PublicInnerClass")
public class IgniteCacheOffheapManagerImpl extends GridCacheManagerAdapter implements IgniteCacheOffheapManager {
    /** */
    private boolean indexingEnabled;

    /** */
    protected CacheDataStore locCacheDataStore;

    /** */
    protected final ConcurrentMap<Integer, CacheDataStore> partDataStores = new ConcurrentHashMap<>();

    /** */
    protected PendingEntriesTree pendingEntries;

    /** */
    private static final PendingRow START_PENDING_ROW = new PendingRow(Long.MIN_VALUE, 0);

    /** */
    private final GridAtomicLong globalRmvId = new GridAtomicLong(U.currentTimeMillis() * 1000_000);

    /** {@inheritDoc} */
    @Override public GridAtomicLong globalRemoveId() {
        return globalRmvId;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        indexingEnabled = GridQueryProcessor.isEnabled(cctx.config());

        if (cctx.affinityNode()) {
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
        if (cctx.ttl().eagerTtlEnabled()) {
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
    @Override public long entriesCount(boolean primary, boolean backup,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        if (cctx.isLocal())
            return 0; // TODO: GG-11208.
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
        if (cctx.isLocal())
            return 0; // TODO: GG-11208.
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
        GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        assert expireTime >= 0;

        dataStore(part).update(key, partId, val, ver, expireTime);
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
    @Override public void clear(GridDhtLocalPartition part) throws IgniteCheckedException {
        GridIterator<CacheDataRow> iterator = iterator(part.id());

        while (iterator.hasNext()) {
            CacheDataRow row = iterator.next();

            remove(row.key(), part.id(), part);
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
        long pageId = cctx.shared().database().globalReuseList().takeRecycledPage();

        if (pageId == 0L)
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
        CacheDataStore dataStore = createCacheDataStore0(p);

        partDataStores.put(p, dataStore);

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
    @Override public void expire(
        IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c) throws IgniteCheckedException {
        if (pendingEntries != null) {
            GridCacheVersion obsoleteVer = null;

            long now = U.currentTimeMillis();

            GridCursor<PendingRow> cur = pendingEntries.find(START_PENDING_ROW, new PendingRow(now, 0));

            while (cur.next()) {
                PendingRow row = cur.get();

                assert row.key != null && row.link != 0 && row.expireTime != 0 : row;

                if (pendingEntries.remove(row) != null) {
                    if (obsoleteVer == null)
                        obsoleteVer = cctx.versions().next();

                    c.apply(cctx.cache().entryEx(row.key), obsoleteVer);
                }
            }
        }
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
        protected long initCntr;

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

        /** {@inheritDoc} */
        @Override public void update(KeyCacheObject key,
            int p,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime) throws IgniteCheckedException {
            DataRow dataRow = new DataRow(key.hashCode(), key, val, ver, p, expireTime);

            // Make sure value bytes initialized.
            key.valueBytes(cctx.cacheObjectContext());
            val.valueBytes(cctx.cacheObjectContext());

            rowStore.addRow(dataRow);

            assert dataRow.link() != 0 : dataRow;

            DataRow old = dataTree.put(dataRow);

            if (old == null)
                storageSize.incrementAndGet();

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
                    pendingEntries.remove(new PendingRow(old.expireTime(), old.link()));

                rowStore.removeRow(old.link());
            }

            if (pendingEntries != null && expireTime != 0) {
                pendingEntries.put(new PendingRow(expireTime, dataRow.link()));

                cctx.ttl().onPendingEntryAdded(expireTime);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(KeyCacheObject key, int partId) throws IgniteCheckedException {
            DataRow dataRow = dataTree.remove(new KeySearchRow(key.hashCode(), key, 0));

            CacheObject val = null;
            GridCacheVersion ver = null;

            if (dataRow != null) {
                assert dataRow.link() != 0 : dataRow;

                if (pendingEntries != null && dataRow.expireTime() != 0)
                    pendingEntries.remove(new PendingRow(dataRow.expireTime(), dataRow.link()));

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
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(KeyCacheObject key)
            throws IgniteCheckedException {
            return dataTree.findOne(new KeySearchRow(key.hashCode(), key, 0));
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            return dataTree.find(null, null);
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
        @Override public long initialUpdateCounter() {
            return initCntr;
        }

        /** {@inheritDoc} */
        @Override public void init(long size, long updCntr) {
            initCntr = updCntr;
            storageSize.set(size);
            cntr.set(updCntr);
        }
    }

    /**
     *
     */
    private class KeySearchRow extends CacheDataRowAdapter {
        /** */
        protected int hash;

        /**
         * @param hash Hash code.
         * @param key Key.
         * @param link Link.
         */
        KeySearchRow(int hash, KeyCacheObject key, long link) {
            super(link);

            this.key = key;
            this.hash = hash;
        }

        /**
         * Init data.
         *
         * @param keyOnly Initialize only key.
         */
        protected final void initData(boolean keyOnly) {
            if (key != null)
                return;

            assert link() != 0;

            try {
                initFromLink(cctx, keyOnly);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e.getMessage(), e);
            }
        }

        /**
         * @return Key.
         */
        @Override public KeyCacheObject key() {
            initData(true);

            return key;
        }
    }

    /**
     *
     */
    private class DataRow extends KeySearchRow {
        /** */
        protected int part = -1;

        /**
         * @param hash Hash code.
         * @param link Link.
         */
        DataRow(int hash, long link) {
            super(hash, null, link);

            part = PageIdUtils.partId(link);

            // We can not init data row lazily because underlying buffer can be concurrently cleared.
            initData(false);
        }

        /**
         * @param hash Hash code.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param part Partition.
         * @param expireTime Expire time.
         */
        DataRow(int hash, KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long expireTime) {
            super(hash, key, 0);

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
        @Override public void link(long link) {
            this.link = link;
        }
    }

    /**
     *
     */
    protected static class CacheDataTree extends BPlusTree<KeySearchRow, DataRow> {
        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final GridCacheContext cctx;

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

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<KeySearchRow> io, ByteBuffer buf, int idx, KeySearchRow row)
            throws IgniteCheckedException {
            int hash = ((RowLinkIO)io).getHash(buf, idx);

            int cmp = Integer.compare(hash, row.hash);

            if (cmp != 0)
                return cmp;

            KeySearchRow row0 = io.getLookupRow(this, buf, idx);

            return compareKeys(row0.key(), row.key());
        }

        /** {@inheritDoc} */
        @Override protected DataRow getRow(BPlusIO<KeySearchRow> io, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            int hash = ((RowLinkIO)io).getHash(buf, idx);
            long link = ((RowLinkIO)io).getLink(buf, idx);

            return rowStore.dataRow(hash, link);
        }

        /**
         * @param key1 First key.
         * @param key2 Second key.
         * @return Compare result.
         * @throws IgniteCheckedException If failed.
         */
        private int compareKeys(CacheObject key1, CacheObject key2) throws IgniteCheckedException {
            byte[] bytes1 = key1.valueBytes(cctx.cacheObjectContext());
            byte[] bytes2 = key2.valueBytes(cctx.cacheObjectContext());

            int len = Math.min(bytes1.length, bytes2.length);

            for (int i = 0; i < len; i++) {
                byte b1 = bytes1[i];
                byte b2 = bytes2[i];

                if (b1 != b2)
                    return b1 > b2 ? 1 : -1;
            }

            return Integer.compare(bytes1.length, bytes2.length);
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
        private KeySearchRow keySearchRow(int hash, long link) {
            return new KeySearchRow(hash, null, link);
        }

        /**
         * @param hash Hash code.
         * @param link Link.
         * @return Data row.
         */
        private DataRow dataRow(int hash, long link) {
            return new DataRow(hash, link);
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
     *
     */
    private interface RowLinkIO {
        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Row link.
         */
        public long getLink(ByteBuffer buf, int idx);

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Key hash code.
         */
        public int getHash(ByteBuffer buf, int idx);
    }

    /**
     *
     */
    public static final class DataInnerIO extends BPlusInnerIO<KeySearchRow> implements RowLinkIO {
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
        @Override public void storeByOffset(ByteBuffer buf, int off, KeySearchRow row) {
            assert row.link() != 0;

            store0(buf, off, row.link(), row.hash);
        }

        /** {@inheritDoc} */
        @Override public KeySearchRow getLookupRow(BPlusTree<KeySearchRow,?> tree, ByteBuffer buf, int idx) {
            int hash = getHash(buf, idx);
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(hash, link);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<KeySearchRow> srcIo, ByteBuffer src,
            int srcIdx) {
            int hash = ((RowLinkIO)srcIo).getHash(src, srcIdx);
            long link = ((RowLinkIO)srcIo).getLink(src, srcIdx);

            store0(dst, offset(dstIdx), link, hash);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            assert idx < getCount(buf) : idx;

            return buf.getLong(offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getHash(ByteBuffer buf, int idx) {
            return buf.getInt(offset(idx) + 8);
        }
    }

    /**
     *
     */
    public static final class DataLeafIO extends BPlusLeafIO<KeySearchRow> implements RowLinkIO {
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
        @Override public void storeByOffset(ByteBuffer buf, int off, KeySearchRow row) {
            assert row.link() != 0;

            store0(buf, off, row.link(), row.hash);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<KeySearchRow> srcIo, ByteBuffer src,
            int srcIdx) {
            store0(dst, offset(dstIdx), getLink(src, srcIdx), getHash(src, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public KeySearchRow getLookupRow(BPlusTree<KeySearchRow,?> tree, ByteBuffer buf, int idx) {

            int hash = getHash(buf, idx);
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(hash, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            assert idx < getCount(buf) : idx;

            return buf.getLong(offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getHash(ByteBuffer buf, int idx) {
            return buf.getInt(offset(idx) + 8);
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

            rowData.initFromLink(cctx, true);

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

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<PendingRow> io, ByteBuffer buf, int idx, PendingRow row)
            throws IgniteCheckedException {
            long expireTime = ((PendingRowIO)io).getExpireTime(buf, idx);

            int cmp = Long.compare(expireTime, row.expireTime);

            if (cmp != 0)
                return cmp;

            if (row.link == 0L)
                return 0;

            long link = ((PendingRowIO)io).getLink(buf, idx);

            return Long.compare(link, row.link);
        }

        /** {@inheritDoc} */
        @Override protected PendingRow getRow(BPlusIO<PendingRow> io, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            return io.getLookupRow(this, buf, idx);
        }
    }

    /**
     *
     */
    private interface PendingRowIO {
        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Expire time.
         */
        long getExpireTime(ByteBuffer buf, int idx);

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Link.
         */
        long getLink(ByteBuffer buf, int idx);
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
        @Override public void store(ByteBuffer dst,
            int dstIdx,
            BPlusIO<PendingRow> srcIo,
            ByteBuffer src,
            int srcIdx) throws IgniteCheckedException {
            int dstOff = offset(dstIdx);

            long link = ((PendingRowIO)srcIo).getLink(src, srcIdx);
            long expireTime = ((PendingRowIO)srcIo).getExpireTime(src, srcIdx);

            dst.putLong(dstOff, expireTime);
            dst.putLong(dstOff + 8, link);
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            return PendingRow.createRowWithKey(((PendingEntriesTree)tree).cctx, getExpireTime(buf, idx), getLink(buf, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx));
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx) + 8);
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
        @Override public void store(ByteBuffer dst,
            int dstIdx,
            BPlusIO<PendingRow> srcIo,
            ByteBuffer src,
            int srcIdx) throws IgniteCheckedException {
            int dstOff = offset(dstIdx);

            long link = ((PendingRowIO)srcIo).getLink(src, srcIdx);
            long expireTime = ((PendingRowIO)srcIo).getExpireTime(src, srcIdx);

            dst.putLong(dstOff, expireTime);
            dst.putLong(dstOff + 8, link);
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            return PendingRow.createRowWithKey(((PendingEntriesTree)tree).cctx, getExpireTime(buf, idx), getLink(buf, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx));
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx) + 8);
        }
    }
}
