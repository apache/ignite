/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import org.jetbrains.annotations.*;

/**
 * This enumeration defines different types of communication
 * message processing by the communication manager.
 */
public enum GridIoPolicy {
    /** Public execution pool. */
    PUBLIC_POOL,

    /** P2P execution pool. */
    P2P_POOL,

    /** System execution pool. */
    SYSTEM_POOL,

    /** Management execution pool. */
    MANAGEMENT_POOL,

    /** Affinity fetch pool. */
    AFFINITY_POOL,

    /** Utility cache execution pool. */
    UTILITY_CACHE_POOL;

    /** Enum values. */
    private static final GridIoPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridIoPolicy fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
