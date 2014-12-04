/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.receiver;

import org.jetbrains.annotations.*;

/**
 * Data center replication receiver cache conflict resolver mode.
 * <p>
 * Each cache entry has data center ID. In case cache update is performed and either old or new cache entry has data
 * center ID which differs from local data center ID defined by {@link org.gridgain.grid.IgniteConfiguration#getDataCenterId()}, then
 * we consider such situation as conflict.
 * <p>
 * If both entries participating in conflict have the same data cneter ID (i.e. they both were replicated from the same
 * remote data center), then GridGain can potentially resolve this conflict automatically based on entry metadata.
 * <p>
 * But in case old and new entries have different data center IDs (i.e. in active-active scenario when cache is updated
 * in both data centers participating in data center replication, or when cache is updated from multiple remote data
 * centers), then explicit conflict resolution is required.
 * <p>
 * This enumeration provides several different strategies for conflict resolution.
 */
public enum GridDrReceiverCacheConflictResolverMode {
    /**
     * GridGain will automatically resolve conflicts when possible (i.e. when both old and new entries have the same
     * data center ID). In all other situations, conflict resolution will be delegated to
     * {@link GridDrReceiverCacheConflictResolver} configured through
     * {@link GridDrReceiverCacheConfiguration#getConflictResolver()}. In case conflict resolver is not configured,
     * GridGain will overwrite old entry with a new one.
     * <p>
     * This mode is default.
     */
    DR_AUTO,

    /**
     * GridGain will always delegate to conflict resolver. This applies to all possible cases, even if both old and new
     * entries have local data center ID and therefore are not considered conflicting when cache is not data center
     * replication receiver.
     * <p>
     * In this mode {@link GridDrReceiverCacheConfiguration#getConflictResolver()} is mandatory.
     */
    DR_ALWAYS;

    /** Enumerated values. */
    private static final GridDrReceiverCacheConflictResolverMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridDrReceiverCacheConflictResolverMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
