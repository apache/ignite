/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.jetbrains.annotations.*;

/**
 * Partition states.
 */
public enum GridDhtPartitionState {
    /** Partition is being loaded from another node. */
    MOVING,

    /** This node is either a primary or backup owner. */
    OWNING,

    /** This node is neither primary or back up owner. */
    RENTING,

    /** Partition has been evicted from cache. */
    EVICTED;

    /** Enum values. */
    private static final GridDhtPartitionState[] VALS = values();

    /**
     * @param ord Ordinal value.
     * @return Enum value.
     */
    @Nullable public static GridDhtPartitionState fromOrdinal(int ord) {
        return ord < 0 || ord >= VALS.length ? null : VALS[ord];
    }

    /**
     * @return {@code True} if state is active or owning.
     */
    public boolean active() {
        return this != EVICTED;
    }
}
