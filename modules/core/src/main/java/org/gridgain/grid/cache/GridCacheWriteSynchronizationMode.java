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
 * Mode indicating how GridGain should wait for write replies from other nodes. Default
 * value is {@link #FULL_ASYNC}}, which means that GridGain will not wait for responses from
 * participating nodes. This means that by default remote nodes may get their state updated slightly after
 * any of the cache write methods complete, or after {@link IgniteTx#commit()} method completes.
 * <p>
 * Note that regardless of write synchronization mode, cache data will always remain fully
 * consistent across all participating nodes.
 * <p>
 * Write synchronization mode may be configured via {@link GridCacheConfiguration#getWriteSynchronizationMode()}
 * configuration property.
 */
public enum GridCacheWriteSynchronizationMode {
    /**
     * Flag indicating that GridGain should wait for write or commit replies from all nodes.
     * This behavior guarantees that whenever any of the atomic or transactional writes
     * complete, all other participating nodes which cache the written data have been updated.
     */
    FULL_SYNC,

    /**
     * Flag indicating that GridGain will not wait for write or commit responses from participating nodes,
     * which means that remote nodes may get their state updated a bit after any of the cache write methods
     * complete, or after {@link IgniteTx#commit()} method completes.
     */
    FULL_ASYNC,

    /**
     * This flag only makes sense for {@link GridCacheMode#PARTITIONED} mode. When enabled, GridGain
     * will wait for write or commit to complete on {@code primary} node, but will not wait for
     * backups to be updated.
     */
    PRIMARY_SYNC;

    /** Enumerated values. */
    private static final GridCacheWriteSynchronizationMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheWriteSynchronizationMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
