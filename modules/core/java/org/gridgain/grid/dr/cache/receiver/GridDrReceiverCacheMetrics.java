/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.receiver;

/**
 * Metrics for data center replication receiver cache.
 */
public interface GridDrReceiverCacheMetrics {
    /**
     * Gets total amount of cache entries received from receiver hub.
     *
     * @return Total amount of cache entries received from receiver hub.
     */
    public long entriesReceived();

    /**
     * Gets total amount of conflicts resolved by using new value. See
     * {@link GridDrReceiverCacheConflictContext#useNew()} for more info.
     *
     * @return Total amount of conflicts resolved by using new value.
     */
    public long conflictNew();

    /**
     * Gets total amount of conflicts resolved by using old value. See
     * {@link GridDrReceiverCacheConflictContext#useOld()} for more info.
     *
     * @return Total amount of conflicts resolved by using old value.
     */
    public long conflictOld();

    /**
     * Gets total amount of conflicts resolved by merge. See
     * {@link GridDrReceiverCacheConflictContext#merge(Object, long)} for more info.
     *
     * @return Total amount of conflicts resolved by merge.
     */
    public long conflictMerge();
}
