/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache;

import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

/**
 * TODO:
 * 1. Get rid of SKIP_STORE, SKIP_SWAP, LOCAL, READ, CLONE
 * 2. Other properties should be moved to cache configuration.
 * 3. This enum should become obsolete and removed.
 */
public enum CacheFlag {
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
     * @see IgniteTx#isInvalidate()
     * @see org.gridgain.grid.cache.GridCacheConfiguration#isInvalidate()
     */
    INVALIDATE,

    /**
     * Skips version check during {@link org.gridgain.grid.cache.GridCacheProjection#transform(Object, GridClosure)} writes in
     * {@link org.gridgain.grid.cache.GridCacheAtomicityMode#ATOMIC} mode. By default, in {@code ATOMIC} mode, whenever
     * {@code transform(...)} is called, cache values (and not the {@code transform} closure) are sent from primary
     * node to backup nodes to ensure proper update ordering.
     * <p>
     * By setting this flag, version check is skipped, and the {@code transform} closure is applied on both, primary
     * and backup nodes. Use this flag for better performance if you are sure that there are no
     * concurrent updates happening for the same key when {@code transform(...)} method is called.
     */
    FORCE_TRANSFORM_BACKUP;

    /** */
    private static final CacheFlag[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static CacheFlag fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
