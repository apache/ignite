/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.jetbrains.annotations.*;

/**
 * Cache write ordering mode. This enumeration is taken into account only in {@link GridCacheAtomicityMode#ATOMIC}
 * atomicity mode. Write ordering mode determines which node assigns the write version, sender or the primary node.
 * <p>
 * For example, {@link #CLOCK} mode assigns write versions on a sender node which generally leads to better
 * performance in {@link GridCacheWriteSynchronizationMode#FULL_SYNC} synchronization mode, since in this case
 * sender can send write requests to primary and backups at the same time. Otherwise, if ordering mode is
 * {@link #PRIMARY}, it would only send request to primary node, which in turn will assign write version
 * and forward it to backups.
 * <p>
 * {@link #CLOCK} mode will be automatically configured only with {@link GridCacheWriteSynchronizationMode#FULL_SYNC}
 * write synchronization mode, as for other synchronization modes it does not render better performance.
 *
 * @author @java.author
 * @version @java.version
 */
public enum GridCacheAtomicWriteOrderMode {
    /**
     * In this mode, write versions are assigned on a sender node which generally leads to better
     * performance in {@link GridCacheWriteSynchronizationMode#FULL_SYNC} synchronization mode, since in this case
     * sender can send write requests to primary and backups at the same time.
     * <p>
     * This mode will be automatically configured only with {@link GridCacheWriteSynchronizationMode#FULL_SYNC}
     * write synchronization mode, as for other synchronization modes it does not render better performance.
     */
    CLOCK,

    /**
     * Cache version is assigned only on primary node. This means that sender will only send write request
     * to primary node, which in turn will assign write version and forward it to backups.
     */
    PRIMARY;

    /** Enumerated values. */
    private static final GridCacheAtomicWriteOrderMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheAtomicWriteOrderMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
