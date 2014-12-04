/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.receiver;

import org.gridgain.grid.cache.*;

import org.gridgain.grid.dr.*;
import org.jetbrains.annotations.*;

/**
 * Data center replication receiver cache conflict context. Whenever conflict which cannot be resolved automatically
 * occurs this entity is passed to {@link GridDrReceiverCacheConflictResolver} to perform explicit conflict resolution.
 * <p>
 * Contains all data necessary for conflict resolution.
 */
public interface GridDrReceiverCacheConflictContext<K, V> {
    /**
     * Gets old (existing) cache entry.
     *
     * @return Old (existing) cache entry.
     */
    public GridDrEntry<K, V> oldEntry();

    /**
     * Gets new cache entry.
     *
     * @return New cache entry.
     */
    public GridDrEntry<K, V> newEntry();

    /**
     * Force data center replication receiver cache to ignore new entry and leave old (existing) entry unchanged.
     */
    public void useOld();

    /**
     * Force data center replication receiver cache to apply new entry thus overwriting old (existing) entry.
     * <p>
     * Note that updates from remote data centers always have explicit TTL , while local data center
     * updates will only have explicit TTL in case {@link GridCacheEntry#timeToLive(long)} was called
     * before update. In the latter case new entry will pick TTL of the old (existing) entry, even
     * if it was set through update from remote data center. it means that depending on concurrent
     * update timings new update might pick unexpected TTL. For example, consider that three updates
     * of the same key are performed: local update with explicit TTL (1) followed by another local
     * update without explicit TTL (2) and one remote update (3). In this case you might expect that
     * update (2) will pick TTL set during update (1). However, in case update (3) occurrs between (1)
     * and (2) and it overwrites (1) during conflict resolution, then update (2) will pick TTL of
     * update (3). To have predictable TTL in such cases you should either always set it explicitly
     * through {@code GridCacheEntry.timeToLive(long)} or use {@link #merge(Object, long)}.
     */
    public void useNew();

    /**
     * Force data center replication engine to use neither old, nor new, but some other value passed
     * as argument. In this case old value will be replaced with merge value and update will be
     * considered as local (i.e. {@link org.gridgain.grid.IgniteConfiguration#getDataCenterId()} will be used).
     * <p>
     * Also in case of merge you have to specify new TTL explicitly. For unlimited TTL use {@code 0}.
     *
     * @param mergeVal Merge value or {@code null} to force remove.
     * @param ttl Time to live in milliseconds.
     */
    public void merge(@Nullable V mergeVal, long ttl);
}
