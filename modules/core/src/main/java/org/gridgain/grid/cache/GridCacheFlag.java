/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

/**
 * Cache projection flags that specify projection behaviour. This flags can be explicitly passed into
 * the following methods on {@link GridCacheProjection}:
 * <ul>
 * <li>{@link GridCacheProjection#flagsOn(GridCacheFlag...)}</li>
 * <li>{@link GridCacheProjection#flagsOff(GridCacheFlag...)}</li>
 * </ul>
 * Also, some flags, like {@link #LOCAL}, or {@link #READ} may be implicitly set whenever
 * creating new projections and passing entries to predicate filters.
 */
public enum GridCacheFlag {
    /**
     * Only operations that don't require any communication with
     * other cache nodes are allowed. This flag is automatically set
     * on underlying projection for all the entries that are given to
     * predicate filters to make sure that no distribution happens
     * from inside of predicate evaluation.
     */
    LOCAL,

    /**
     * Only operations that don't change cached data are allowed.
     * This flag is automatically set on underlying projection for
     * all the entries that are given to predicate filters to make
     * sure that data cannot be updated during predicate evaluation.
     */
    READ,

    /**
     * Clone values prior to returning them to user.
     * <p>
     * Whenever values are returned from cache, they cannot be directly updated
     * as cache holds the same references internally. If it is needed to
     * update values that are returned from cache, this flag will provide
     * automatic cloning of values prior to returning so they can be directly
     * updated.
     *
     * @see GridCacheConfiguration#getCloner()
     */
    CLONE,

    /** Skips store, i.e. no read-through and no write-through behavior. */
    SKIP_STORE,

    /** Skip swap space for reads and writes. */
    SKIP_SWAP,

    /** Always get data from primary node (never from backup). */
    GET_PRIMARY,

    /** Synchronous commit. */
    SYNC_COMMIT,

    /**
     * Switches a cache projection to work in {@code 'invalidation'} mode.
     * Instead of updating remote entries with new values, small invalidation
     * messages will be sent to set the values to {@code null}.
     *
     * @see GridCacheTx#isInvalidate()
     * @see GridCacheConfiguration#isInvalidate()
     */
    INVALIDATE,

    /**
     * Skips version check during {@link GridCacheProjection#transform(Object, org.apache.ignite.lang.IgniteClosure)} writes in
     * {@link GridCacheAtomicityMode#ATOMIC} mode. By default, in {@code ATOMIC} mode, whenever
     * {@code transform(...)} is called, cache values (and not the {@code transform} closure) are sent from primary
     * node to backup nodes to ensure proper update ordering.
     * <p>
     * By setting this flag, version check is skipped, and the {@code transform} closure is applied on both, primary
     * and backup nodes. Use this flag for better performance if you are sure that there are no
     * concurrent updates happening for the same key when {@code transform(...)} method is called.
     */
    FORCE_TRANSFORM_BACKUP;

    /** */
    private static final GridCacheFlag[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheFlag fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
