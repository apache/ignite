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

import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
@SuppressWarnings("WeakerAccess")
public interface IgniteCacheOffheapManager extends GridCacheManager {
    /**
     * Partition counter update callback. May be overridden by plugin-provided subclasses.
     *
     * @param part Partition.
     * @param cntr Partition counter.
     */
    public void onPartitionCounterUpdated(int part, long cntr);

    /**
     * Partition counter provider. May be overridden by plugin-provided subclasses.
     *
     * @param part Partition ID.
     * @return Last updated counter.
     */
    public long lastUpdatedPartitionCounter(int part);

    /**
     * @return Reuse list.
     */
    public ReuseList reuseList();

    /**
     * @return Free list.
     */
    public FreeList freeList();

    /**
     * @param entry Cache entry.
     * @return Cached object entry, if available, null otherwise.
     * @throws IgniteCheckedException If failed.
     */
    public CacheObjectEntry read(GridCacheMapEntry entry) throws IgniteCheckedException;

    /**
     * @param p Partition.
     * @param lsnr Listener.
     * @return Data store.
     * @throws IgniteCheckedException If failed.
     */
    public CacheDataStore createCacheDataStore(int p, CacheDataStore.Listener lsnr) throws IgniteCheckedException;

    /**
     * TODO: GG-10884, used on only from initialValue.
     */
    public boolean containsKey(GridCacheMapEntry entry);

    /**
     * @param key  Key.
     * @param val  Value.
     * @param ver  Version.
     * @param expireTime Expire time.
     * @param part Partition.
     * @return UpdateInfo object that contains the info about the old entry and the offheap link to new
     *          entry.
     * @throws IgniteCheckedException If failed.
     */
    public UpdateInfo update(
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            int partId,
            GridDhtLocalPartition part
    ) throws IgniteCheckedException;

    /**
     * @param key Key.
     * @param prevVal Previous value.
     * @param prevVer Previous version.
     * @param part Partition.
     * @return removed entry info
     * @throws IgniteCheckedException If failed.
     */
    public CacheObjectEntry remove(
            KeyCacheObject key,
            CacheObject prevVal,
            GridCacheVersion prevVer,
            int partId,
            GridDhtLocalPartition part
    ) throws IgniteCheckedException;

    /**
     * @param ldr Class loader.
     * @return Number of undeployed entries.
     */
    public int onUndeploy(ClassLoader ldr);

    /**
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Rows iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> iterator(boolean primary, boolean backup, final AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * @param part Partition.
     * @return Partition data iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridIterator<CacheDataRow> iterator(final int part) throws IgniteCheckedException;

    /**
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @param keepBinary Keep binary flag.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<Cache.Entry<K, V>> entriesIterator(final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary) throws IgniteCheckedException;

    /**
     * @param part Partition.
     * @return Iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridCloseableIterator<KeyCacheObject> keysIterator(final int part) throws IgniteCheckedException;

    /**
     * @param primary Primary entries flag.
     * @param backup Backup entries flag.
     * @param topVer Topology version.
     * @return Entries count.
     * @throws IgniteCheckedException If failed.
     */
    public long entriesCount(boolean primary, boolean backup, AffinityTopologyVersion topVer)
        throws IgniteCheckedException;

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    public void clear(boolean readers);

    /**
     * @param part Partition.
     * @return Number of entries in given partition.
     */
    public long entriesCount(int part);

    /**
     * @return Offheap allocated size.
     */
    public long offHeapAllocatedSize();

    // TODO GG-10884: moved from GridCacheSwapManager.
    void writeAll(Iterable<GridCacheBatchSwapEntry> swapped) throws IgniteCheckedException;

    /**
     * @return PendingEntries container that is used by TTL manager.
     * @throws IgniteCheckedException If failed.
     */
    PendingEntries createPendingEntries() throws IgniteCheckedException;

    /**
     *
     */
    interface CacheDataStore {
        /**
         * @param key Key.
         * @param part Partition.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @return UpdateInfo object that contains the info about the old entry and the offheap link to new
         *          entry.
         * @throws IgniteCheckedException If failed.
         */
        UpdateInfo update(KeyCacheObject key,
            int part,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @return removed entry
         * @throws IgniteCheckedException If failed.
         */
        public CacheObjectEntry remove(KeyCacheObject key) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @return Cached object entry.
         * @throws IgniteCheckedException If failed.
         */
        public CacheObjectEntry find(KeyCacheObject key) throws IgniteCheckedException;

        /**
         * @return Data cursor.
         * @throws IgniteCheckedException If failed.
         */
        public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException;

        /**
         * Data store listener.
         */
        interface Listener {
            /**
             * On new entry inserted.
             */
            void onInsert();

            /**
             * On entry removed.
             */
            void onRemove();
        }
    }

    /**
     * The wrapper to return data loaded from paged memory
     */
    class CacheObjectEntry {
        /** Key object. */
        private final KeyCacheObject key;

        /** Value object. */
        private final CacheObject val;

        /** Version. */
        private final GridCacheVersion ver;

        /** Expire time. */
        private final long expireTime;

        /** Offheap row link. */
        private final long link;

        /**
         * @param val Object.
         * @param ver Version.
         * @param expireTime Expire time.
         * @param link
         */
        public CacheObjectEntry(KeyCacheObject key, CacheObject val, GridCacheVersion ver, long expireTime, long link) {
            this.key = key;
            this.val = val;
            this.ver = ver;
            this.expireTime = expireTime;
            this.link = link;
        }

        /**
         *
         */
        public CacheObject value() {
            return val;
        }

        /**
         *
         */
        public GridCacheVersion version() {
            return ver;
        }

        /**
         *
         */
        public long expireTime() {
            return expireTime;
        }

        /**
         *
         */
        public long link() {
            return link;
        }

        /**
         *
         */
        public KeyCacheObject key() {
            return key;
        }

        @Override public String toString() {
            return S.toString(CacheObjectEntry.class, this);
        }
    }

    /**
     * The container to store entries with expiration time.
     * It is used by TTL manager but implemented on the offhep manager because B+tree is
     * used to store entries. Also the implementation uses features of paged memory
     * storage of entries
     */
    interface PendingEntries {

        /**
         * @param entry Entry.
         */
        void addTrackedEntry(GridCacheMapEntry entry);

        /**
         * @param entry Entry.
         */
        void removeTrackedEntry(GridCacheMapEntry entry);

        /**
         * @param entry Entry.
         */
        void removeTrackedEntry(IgniteCacheOffheapManager.CacheObjectEntry entry);

        /**
         *
         */
        ExpiredEntriesCursor expired(long time);

        /**
         *
         */
        int pendingSize();

        /**
         *
         */
        long firstExpired();
    }

    /**
     *
     */
    interface ExpiredEntriesCursor extends GridCursor<GridCacheEntryEx> {
        /**
         * Remove all items that
         */
        void removeAll();
    }

    class UpdateInfo {

        private final long newLink;
        private final CacheObjectEntry oldEntry;

        public UpdateInfo(long link, CacheObjectEntry entry) {
            newLink = link;
            oldEntry = entry;
        }

        public long newLink() {
            return newLink;
        }

        public CacheObjectEntry oldEntry() {
            return oldEntry;
        }
    }

}
