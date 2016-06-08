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
import org.apache.ignite.lang.IgniteBiTuple;

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
     * @return Value tuple, if available.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteBiTuple<CacheObject, GridCacheVersion> read(GridCacheMapEntry entry) throws IgniteCheckedException;

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
     * @throws IgniteCheckedException If failed.
     */
    public void update(
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
     * @throws IgniteCheckedException If failed.
     */
    public void remove(
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
     *
     */
    interface CacheDataStore {
        /**
         * @param key Key.
         * @param part Partition.
         * @param val Value.
         * @param ver Version.
         * @param expireTime Expire time.
         * @throws IgniteCheckedException If failed.
         */
        void update(KeyCacheObject key,
            int part,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @throws IgniteCheckedException If failed.
         */
        public void remove(KeyCacheObject key) throws IgniteCheckedException;

        /**
         * @param key Key.
         * @return Value/version tuple.
         * @throws IgniteCheckedException If failed.
         */
        public IgniteBiTuple<CacheObject, GridCacheVersion> find(KeyCacheObject key) throws IgniteCheckedException;

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
}
