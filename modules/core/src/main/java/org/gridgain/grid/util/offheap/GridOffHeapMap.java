/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

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
    @Nullable public GridBiTuple<Long, Integer> getPointer(int hash, byte[] keyBytes);

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
    public GridCloseableIterator<GridBiTuple<byte[], byte[]>> iterator();

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
