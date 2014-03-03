/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.cache.eviction.*;

/**
 * Defines set of memory modes. Memory modes help control whether cache entries are
 * stored on heap memory, offheap memory, or in swap space.
 *
 * @author @java.author
 * @version @java.version
 */
public enum GridCacheMemoryMode {
    /**
     * Entries will be stored on-heap first. The onheap tiered storage works as follows:
     * <nl>
     * <li>Entries are cached on heap memory first.</li>
     * <li>
     *     If offheap memory is enabled and eviction policy evicts an entry from heap memory, entry will
     *     be moved to offheap memory. If offheap memory is disabled, then entry is simply discarded.
     * </li>
     * <li>
     *     If swap space is enabled and offheap memory fills up, then entry will be evicted into swap space.
     *     If swap space is disabled, then entry will be discarded. If swap is enabled and offheap memory
     *     is disabled, then entry will be evicted directly from heap memory into swap.
     * </li>
     * </nl>
     * <p>
     * <b>Note</b> that heap memory evictions are handled by configured {@link GridCacheEvictionPolicy}
     * implementation. By default, no eviction policy is enabled, so entries never leave heap
     * memory space unless explicitly removed.
     */
    ONHEAP_TIERED,

    /**
     * Works the same as {@link #ONHEAP_TIERED}, except that entries never end up in heap memory and get
     * stored in offheap memory right away. Entries get cached in offheap memory first and then
     * get evicted to swap, if one is configured.
     */
    OFFHEAP_TIERED,

    /**
     * Entry keys will be stored on heap memory, and values will be stored in offheap memory. Note
     * that in this mode entries can be evicted only to swap. The evictions will happen according
     * to configured {@link GridCacheEvictionPolicy}.
     */
    OFFHEAP_VALUES,
}
