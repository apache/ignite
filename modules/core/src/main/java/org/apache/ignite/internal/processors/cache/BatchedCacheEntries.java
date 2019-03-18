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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.REMOVE;

/**
 * Batch of cache entries to optimize page memory processing.
 */
public class BatchedCacheEntries {
    /** */
    private final GridDhtLocalPartition part;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final LinkedHashMap<KeyCacheObject, CacheMapEntryInfo> infos = new LinkedHashMap<>();

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final boolean preload;

    /** */
    private List<GridDhtCacheEntry> entries;

    /** */
    private int skipped;

    /** */
    public BatchedCacheEntries(AffinityTopologyVersion topVer, int partId, GridCacheContext cctx, boolean preload) {
        this.topVer = topVer;
        this.cctx = cctx;
        this.preload = preload;
        this.part = cctx.topology().localPartition(partId, topVer, true, true);
    }

    /** */
    public void addEntry(KeyCacheObject key, CacheObject val, long expTime, long ttl, GridCacheVersion ver, GridDrType drType) {
        // todo remove `key` duplication (Map<Key, Entry<Key...)
        infos.put(key, new CacheMapEntryInfo(this, key, val, expTime, ttl, ver, drType));
    }

    /** */
    public Set<KeyCacheObject> keys() {
        return infos.keySet();
    }

    /** */
    public Collection<CacheMapEntryInfo> values() {
        return infos.values();
    }

    /** */
    public GridDhtLocalPartition part() {
        return part;
    }

    /** */
    public GridCacheContext context() {
        return cctx;
    }

    /** */
    public CacheMapEntryInfo get(KeyCacheObject key) {
        return infos.get(key);
    }

    /** */
    public boolean preload() {
        return preload;
    }

    /** */
    public void onRemove(KeyCacheObject key) {
        // todo  - remove from original collection
        ++skipped;
    }

    /** */
    public void onError(KeyCacheObject key, IgniteCheckedException e) {
        // todo  - remove from original collection
        ++skipped;
    }

    /** */
    public boolean skip(KeyCacheObject key) {
        // todo
        return false;
    }

    /** */
    public List<GridDhtCacheEntry> lock() {
        return entries = lockEntries(infos.values(), topVer);
    }

    /** */
    public void unlock() {
        unlockEntries(infos.values(), topVer);
    }

    /** */
    public int size() {
        return infos.size() - skipped;
    }

    /** */
    private List<GridDhtCacheEntry> lockEntries(Collection<CacheMapEntryInfo> list, AffinityTopologyVersion topVer)
        throws GridDhtInvalidPartitionException {
        List<GridDhtCacheEntry> locked = new ArrayList<>(list.size());

        while (true) {
            for (CacheMapEntryInfo info : list) {
                GridDhtCacheEntry entry = (GridDhtCacheEntry)cctx.cache().entryEx(info.key(), topVer);

                locked.add(entry);

                info.cacheEntry(entry);
            }

            boolean retry = false;

            for (int i = 0; i < locked.size(); i++) {
                GridCacheMapEntry entry = locked.get(i);

                if (entry == null)
                    continue;

                // todo ensure free space
                // todo check obsolete

                entry.lockEntry();

                if (entry.obsolete()) {
                    // Unlock all locked.
                    for (int j = 0; j <= i; j++) {
                        if (locked.get(j) != null)
                            locked.get(j).unlockEntry();
                    }

                    // Clear entries.
                    locked.clear();

                    // Retry.
                    retry = true;

                    break;
                }
            }

            if (!retry)
                return locked;
        }
    }

    /**
     * Releases java-level locks on cache entries
     * todo carefully think about possible reorderings in locking/unlocking.
     *
     * @param locked Locked entries.
     * @param topVer Topology version.
     */
    private void unlockEntries(Collection<CacheMapEntryInfo> locked, AffinityTopologyVersion topVer) {
        // Process deleted entries before locks release.
        assert cctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        // todo Common skip list.
        Collection<KeyCacheObject> skip = null;

        int size = locked.size();

        try {
            for (CacheMapEntryInfo info : locked) {
                GridCacheMapEntry entry = info.cacheEntry();

                if (entry != null && entry.deleted()) {
                    if (skip == null)
                        skip = U.newHashSet(locked.size());

                    skip.add(entry.key());
                }

                try {
                    info.updateCacheEntry();
                } catch (IgniteCheckedException e) {
                    skip.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (CacheMapEntryInfo info : locked) {
                GridCacheMapEntry entry = info.cacheEntry();
                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (CacheMapEntryInfo info : locked) {
            GridDhtCacheEntry entry = info.cacheEntry();
            if (entry != null)
                entry.onUnlock();
        }

        if (skip != null && skip.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (CacheMapEntryInfo info : locked) {
            GridCacheMapEntry entry = info.cacheEntry();
            if (entry != null && (skip == null || !skip.contains(entry.key())))
                entry.touch();
        }
    }

    /** */
    public class BatchUpdateClosure implements IgniteCacheOffheapManager.OffheapInvokeAllClosure {
        /** */
        private static final long serialVersionUID = -4782459128689696534L;

        /** */
        private final List<T3<IgniteTree.OperationType, CacheDataRow, CacheDataRow>> resBatch = new ArrayList<>(entries.size());

        /** */
        private final int cacheId = context().group().storeCacheIdInDataPage() ? context().cacheId() : CU.UNDEFINED_CACHE_ID;

        /** */
        private final int partId = part().id();

        /** {@inheritDoc} */
        @Override public void call(@Nullable Collection<T2<CacheDataRow, CacheSearchRow>> rows) throws IgniteCheckedException {
            List<CacheDataRow> newRows = new ArrayList<>(16);

            for (T2<CacheDataRow, CacheSearchRow> t2 : rows) {
                CacheDataRow oldRow = t2.get1();

                KeyCacheObject key = t2.get2().key();

                CacheMapEntryInfo newRowInfo = get(key);

                try {
                    if (newRowInfo.needUpdate(oldRow)) {
                        CacheDataRow newRow;

                        CacheObject val = newRowInfo.value();

                        if (val != null) {
                            if (oldRow != null) {
                                // todo think about batch updates
                                newRow = context().offheap().dataStore(part()).createRow(
                                    context(),
                                    key,
                                    newRowInfo.value(),
                                    newRowInfo.version(),
                                    newRowInfo.expireTime(),
                                    oldRow);
                            }
                            else {
                                CacheObjectContext coCtx = context().cacheObjectContext();
                                // todo why we need this
                                val.valueBytes(coCtx);
                                key.valueBytes(coCtx);

                                if (key.partition() == -1)
                                    key.partition(partId);

                                newRow = new DataRow(key, val, newRowInfo.version(), partId, newRowInfo.expireTime(), cacheId);

                                newRows.add(newRow);
                            }

                            IgniteTree.OperationType treeOp = oldRow != null && oldRow.link() == newRow.link() ?
                                NOOP : PUT;

                            resBatch.add(new T3<>(treeOp, oldRow, newRow));
                        }
                        else {
                            // todo we should pass key somehow to remove old row (because in particular case oldRow should not contain key)
                            newRow = new DataRow(key, null, null, 0, 0, 0);

                            resBatch.add(new T3<>(oldRow != null ? REMOVE : NOOP, oldRow, newRow));
                        }
                    }
                }
                catch (GridCacheEntryRemovedException e) {
                    onRemove(key);
                }
            }

            if (!newRows.isEmpty())
                context().offheap().dataStore(part()).rowStore().addRows(newRows, cctx.group().statisticsHolderData());
        }

        /** {@inheritDoc} */
        @Override public Collection<T3<IgniteTree.OperationType, CacheDataRow, CacheDataRow>> result() {
            return resBatch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(CacheDataRow row) {
            return false;
        }
    }

    /** */
    public static class CacheMapEntryInfo {
        /** todo think about remove */
        private final BatchedCacheEntries batch;

        /** */
        private final KeyCacheObject key;

        /** */
        private final CacheObject val;

        /** */
        private final long expTime;

        /** */
        private final long ttl;

        /** */
        private final GridCacheVersion ver;

        /** */
        private final GridDrType drType;

        /** */
        private GridDhtCacheEntry entry;

        /** */
        private boolean update;

        /** */
        public CacheMapEntryInfo(
            BatchedCacheEntries batch,
            KeyCacheObject key,
            CacheObject val,
            long expTime,
            long ttl,
            GridCacheVersion ver,
            GridDrType drType
        ) {
            this.batch = batch;
            this.key = key;
            this.val = val;
            this.expTime = expTime;
            this.ver = ver;
            this.drType = drType;
            this.ttl = ttl;
        }

        /**
         * @return Key.
         */
        public KeyCacheObject key() {
            return key;
        }

        /**
         * @return Version.
         */
        public GridCacheVersion version() {
            return ver;
        }

        /**
         * @return Value.
         */
        public CacheObject value() {
            return val;
        }

        /**
         * @return Expire time.
         */
        public long expireTime() {
            return expTime;
        }

        /**
         * @param entry Cache entry.
         */
        public void cacheEntry(GridDhtCacheEntry entry) {
            this.entry = entry;
        }

        /**
         * @return Cache entry.
         */
        public GridDhtCacheEntry cacheEntry() {
            return entry;
        }

        /** */
        public void updateCacheEntry() throws IgniteCheckedException {
            if (!update)
                return;

            entry.finishInitialUpdate(val, expTime, ttl, ver, batch.topVer, drType, null, batch.preload);
        }

        /** */
        public boolean needUpdate(CacheDataRow row) throws GridCacheEntryRemovedException {
            GridCacheVersion currVer = row != null ? row.version() : entry.version();

            GridCacheContext cctx = batch.context();

            boolean isStartVer = cctx.versions().isStartVersion(currVer);

            boolean update0;

            if (cctx.group().persistenceEnabled()) {
                if (!isStartVer) {
                    if (cctx.atomic())
                        update0 = ATOMIC_VER_COMPARATOR.compare(currVer, version()) < 0;
                    else
                        update0 = currVer.compareTo(version()) < 0;
                }
                else
                    update0 = true;
            }
            else
                update0 = (isStartVer && row == null);

            update0 |= (!batch.preload() && entry.deletedUnlocked());

            update = update0;

            return update0;
        }
    }
}
