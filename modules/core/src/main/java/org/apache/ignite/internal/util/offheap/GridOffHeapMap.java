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

package org.apache.ignite.internal.util.offheap;

import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.CX2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Off-heap map.
 */
public interface GridOffHeapMap<K> {
    /**
     * Gets partition this map belongs to.
     *
     * @return Partition this map belongs to.
     */
    public int partition();

    /**
     * Gets load factor of this map.
     *
     * @return Load factor.
     */
    public float loadFactor();

    /**
     * Gets concurrency level for this map.
     *
     * @return Concurrency level.
     */
    public int concurrency();

    /**
     * Checks if given key is contained in the map.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return {@code True} if key is contained in the map.
     */
    public boolean contains(int hash, byte[] keyBytes);

    /**
     * Gets value bytes for given key.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return Value bytes.
     */
    @Nullable public byte[] get(int hash, byte[] keyBytes);

    /**
     * Gets value pointer for given key.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return Value pointer.
     */
    @Nullable public IgniteBiTuple<Long, Integer> valuePointer(int hash, byte[] keyBytes);

    /**
     * Enables eviction for given key.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     */
    public void enableEviction(int hash, byte[] keyBytes);

    /**
     * Removes value from off-heap map.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return Removed value bytes.
     */
    @Nullable public byte[] remove(int hash, byte[] keyBytes);

    /**
     * Removes value from off-heap map without returning it.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return {@code True} if value was removed.
     */
    public boolean removex(int hash, byte[] keyBytes);

    /**
     * Puts key and value bytes into the map potentially replacing
     * existing entry.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @param valBytes Value bytes.
     * @return {@code True} if new entry was created, {@code false} if existing value was updated.
     */
    public boolean put(int hash, byte[] keyBytes, byte[] valBytes);

    /**
     * Inserts new entry into the map without comparing if there is
     * a mapping for given key already stored in map.
     * <p>
     * Use with caution whenever certain that inserting a new value
     * without current mapping.
     *
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @param valBytes Value bytes.
     */
    public void insert(int hash, byte[] keyBytes, byte[] valBytes);

    /**
     * Gets number of elements in the map.
     *
     * @return Number of elements in the map.
     */
    public long totalSize();

    /**
     * Gets number of elements in the map.
     *
     * @return Number of elements in the map.
     */
    public long size();

    /**
     * Gets total available memory size.
     *
     * @return Memory size.
     */
    public long memorySize();

    /**
     * Gets memory allocated for map entries so far.
     *
     * @return Allocated memory.
     */
    public long allocatedSize();

    /**
     * Gets memory allocated for map internal structure so far.
     *
     * @return Allocated memory.
     */
    public long systemAllocatedSize();

    /**
     * Gets available memory.
     *
     * @return Available memory.
     */
    public long freeSize();

    /**
     * Destructs this map and deallocates all memory.
     */
    public void destruct();

    /**
     * Gets iterator over map.
     *
     * @return Iterator over map.
     */
    public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator();

    /**
     * Gets iterator over map.
     *
     * @param c Key/value closure.
     * @return Iterator over map.
     */
    public <T> GridCloseableIterator<T> iterator(@Nullable CX2<T2<Long, Integer>, T2<Long, Integer>, T> c);

    /**
     * Sets callback for when entries are evicted due to memory constraints.
     * The parameter into closure is key bytes.
     *
     * @param lsnr Evict listener.
     * @return {@code True} if evict listener was added, {@code false} if
     *      another listener already exists or {@code LRU} is disabled.
     */
    public boolean evictListener(GridOffHeapEvictListener lsnr);

    /**
     * Adds off-heap event listener.
     *
     * @param lsnr Listener.
     * @return {@code True} if event listener was added, {@code false} if
     *      another listener already exists.
     */
    public boolean eventListener(GridOffHeapEventListener lsnr);
}