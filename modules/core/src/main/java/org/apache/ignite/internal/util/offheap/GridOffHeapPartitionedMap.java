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

import java.util.Set;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.CX2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Off-heap map.
 */
public interface GridOffHeapPartitionedMap {
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
     * Gets number of partitions.
     *
     * @return Number of partitions.
     */
    public int partitions();

    /**
     * Checks if given key is contained in the map.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return {@code True} if key is contained in the map.
     */
    public boolean contains(int p, int hash, byte[] keyBytes);

    /**
     * Gets value bytes for given key.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return Value bytes.
     */
    @Nullable public byte[] get(int p, int hash, byte[] keyBytes);

    /**
     * Gets value pointer for given key. While pointer is in use eviction is
     * disabled for corresponding entry. Eviction for entry is enabled when {@link #put} or
     * {@link #enableEviction} is called.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return Value pointer.
     */
    @Nullable public IgniteBiTuple<Long, Integer> valuePointer(int p, int hash, byte[] keyBytes);

    /**
     * Enables eviction for entry.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     */
    public void enableEviction(int p, int hash, byte[] keyBytes);

    /**
     * Removes value from off-heap map.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return Removed value bytes.
     */
    @Nullable public byte[] remove(int p, int hash, byte[] keyBytes);

    /**
     * Removes value from off-heap map without returning it.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @return {@code True} if value was removed.
     */
    public boolean removex(int p, int hash, byte[] keyBytes);

    /**
     * Puts key and value bytes into the map potentially replacing
     * existing entry.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @param valBytes Value bytes.
     * @return {@code True} if new entry was created, {@code false} if existing value was updated.
     */
    public boolean put(int p, int hash, byte[] keyBytes, byte[] valBytes);

    /**
     * Inserts new entry into the map without comparing if there is
     * a mapping for given key already stored in map.
     * <p>
     * Use with caution whenever certain that inserting a new value
     * without current mapping.
     *
     * @param p Partition.
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @param valBytes Value bytes.
     */
    public void insert(int p, int hash, byte[] keyBytes, byte[] valBytes);

    /**
     * Gets number of elements in the map.
     *
     * @return Number of elements in the map.
     */
    public long size();


    /**
     * Gets number of elements in the map.
     *
     * @param parts Partitions.
     * @return Number of elements in the map.
     */
    public long size(Set<Integer> parts);

    /**
     * Gets total available memory size.
     *
     * @return Memory size.
     */
    public long memorySize();

    /**
     * Gets size of a memory allocated for map entries so far.
     *
     * @return Allocated memory size.
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
     * Gets iterator over the whole map.
     *
     * @return Iterator over the whole map.
     */
    public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator();

    /**
     * Gets iterator over the whole map.
     *
     * @param c Key/value closure.
     * @return Iterator over the whole map.
     */
    public <T> GridCloseableIterator<T> iterator(CX2<T2<Long, Integer>, T2<Long, Integer>, T> c);

    /**
     * Gets iterator over the partition.
     *
     * @param c Key/value closure.
     * @param part Partition.
     * @return Iterator over the partition.
     */
    public <T> GridCloseableIterator<T> iterator(CX2<T2<Long, Integer>, T2<Long, Integer>, T> c, int part);

    /**
     * Gets iterator over certain partition.
     *
     * @param p Partition.
     * @return Iterator over certain partition.
     */
    public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator(int p);

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