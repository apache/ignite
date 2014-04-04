/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.cache;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Enumeration of all supported cache peek modes. Peek modes can be passed into various
 * {@code 'GridCacheProjection.peek(..)'} and {@code GridCacheEntry.peek(..)} methods,
 * such as {@link GridCacheProjection#peek(Object, Collection)},
 * {@link GridCacheEntry#peek()}, and others.
 * <p>
 * The following modes are supported:
 * <ul>
 * <li>{@link #TX}</li>
 * <li>{@link #GLOBAL}</li>
 * <li>{@link #SMART}</li>
 * <li>{@link #SWAP}</li>
 * <li>{@link #DB}</li>
 * </ul>
 */
public enum GridCachePeekMode {
    /** Peeks value only from in-transaction memory of an ongoing transaction, if any. */
    TX,

    /** Peeks at cache global (not in-transaction) memory. */
    GLOBAL,

    /**
     * In this mode value is peeked from in-transaction memory first using {@link #TX}
     * mode and then, if it has not been found there, {@link #GLOBAL} mode is used to
     * search in committed cached values.
     */
    SMART,

    /** Peeks value only from off-heap or cache swap storage without loading swapped value into cache. */
    SWAP,

    /** Peek value from the underlying persistent storage without loading this value into cache. */
    DB,

    /**
     * Peek value from near cache only (don't peek from partitioned cache).
     * In case of {@link GridCacheMode#LOCAL} or {@link GridCacheMode#REPLICATED} cache,
     * behaves as {@link #GLOBAL} mode.
     */
    NEAR_ONLY,

    /**
     * Peek value from partitioned cache only (skip near cache).
     * In case of {@link GridCacheMode#LOCAL} or {@link GridCacheMode#REPLICATED} cache,
     * behaves as {@link #GLOBAL} mode.
     */
    PARTITIONED_ONLY;

    /** Enumerated values. */
    private static final GridCachePeekMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCachePeekMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
