/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender;

import org.jetbrains.annotations.*;

/**
 * Data center replication sender hub load balancing mode. Determines to which sender hub next batch will be sent
 * from the sender cache.
 */
public enum GridDrSenderHubLoadBalancingMode {
    /**
     * Balance sender hubs in random fashion.
     */
    DR_RANDOM,

    /**
     * Balance sender hubs in round-robin fashion.
     */
    DR_ROUND_ROBIN;

    /** Enumerated values. */
    private static final GridDrSenderHubLoadBalancingMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridDrSenderHubLoadBalancingMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
