/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.apache.ignite.cache;

import org.jetbrains.annotations.*;

/**
 * Enumeration of all supported cache peek modes. Peek modes can be passed into various
 * {@code 'GridCacheProjection.peek(..)'} and {@code GridCacheEntry.peek(..)} methods,
 * such as {@link org.gridgain.grid.cache.GridCacheProjection#peek(Object, java.util.Collection)},
 * {@link org.gridgain.grid.cache.GridCacheEntry#peek()}, and others.
 * <p>
 * The following modes are supported:
 * <ul>
 * <li>{@link #ALL}</li>
 * <li>{@link #NEAR}</li>
 * <li>{@link #PRIMARY}</li>
 * <li>{@link #BACKUP}</li>
 * <li>{@link #ONHEAP}</li>
 * <li>{@link #OFFHEAP}</li>
 * <li>{@link #SWAP}</li>
 * </ul>
 */
public enum CachePeekMode {
    /** Peeks into all available cache storages. */
    ALL,

    /**
     * Peek into near cache only (don't peek into partitioned cache).
     * In case of {@link org.gridgain.grid.cache.GridCacheMode#LOCAL} cache, behaves as {@link #ALL} mode.
     */
    NEAR,

    /**
     * Peek value from primary copy of partitioned cache only (skip near cache).
     */
    PRIMARY,

    /**
     * Peek value from backup copies of partitioned cache only (skip near cache).
     */
    BACKUP,

    /** Peeks value from the on-heap storage only. */
    ONHEAP,

    /** Peeks value from the off-heap storage only, without loading off-heap value into cache. */
    OFFHEAP,

    /** Peeks value from the swap storage only, without loading swapped value into cache. */
    SWAP;

    /** Enumerated values. */
    private static final CachePeekMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CachePeekMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
