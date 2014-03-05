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
 * This enum defines mode in which partitioned cache operates.
 * <p>
 * Partitioned distribution mode can be configured via {@link GridCacheConfiguration#getDistributionMode()}
 * configuration property.
 */
public enum GridCacheDistributionMode {
    /**
     * Mode in which local node does not cache any data and communicates with other cache nodes
     * via remote calls.
     */
    CLIENT_ONLY,

    /**
     * Mode in which local node will not be either primary or backup node for any keys, but will cache
     * recently accessed keys in a smaller near cache. Amount of recently accessed keys to cache is
     * controlled by near eviction policy.
     *
     * @see GridCacheConfiguration#getNearEvictionPolicy()
     */
    NEAR_ONLY,

    /**
     * Mode in which local node may store primary and/or backup keys, and also will cache recently accessed keys.
     * Amount of recently accessed keys to cache is controlled by near eviction policy.
     * @see GridCacheConfiguration#getNearEvictionPolicy()
     */
    NEAR_PARTITIONED,

    /**
     * Mode in which local node may store primary or backup keys, but does not cache recently accessed keys
     * in near cache.
     */
    PARTITIONED_ONLY;

    /** Enumerated values. */
    private static final GridCacheDistributionMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheDistributionMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
