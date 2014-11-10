/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.dr.*;

import java.util.*;

/**
 * Replication manager class which processes all replication events.
 */
public interface GridCacheDrManager<K, V> extends GridCacheManager<K, V> {
    /**
     * @return Data center ID.
     */
    public byte dataCenterId();

    /**
     * Check whether DR conflict resolution is required.
     *
     * @param oldVer Old version.
     * @param newVer New version.
     * @return {@code True} in case DR is required.
     */
    public boolean needResolve(GridCacheVersion oldVer, GridCacheVersion newVer);

    /**
     * Resolves DR conflict.
     *
     * @param key Key.
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     * @return Conflict resolution result.
     * @throws GridException In case of exception.
     */
    public GridDrReceiverConflictContextImpl<K, V> resolveConflict(K key, GridDrEntry<K, V> oldEntry,
        GridDrEntry<K, V> newEntry) throws GridException;

    /**
     * Perform replication.
     *
     * @param entry Replication entry.
     * @param drType Replication type.
     * @throws GridException If failed.
     */
    public void replicate(GridDrRawEntry<K, V> entry, GridDrType drType)throws GridException;

    /**
     * Process partitions "before exchange" event.
     *
     * @param topVer Topology version.
     * @param left {@code True} if exchange has been caused by node leave.
     * @throws GridException If failed.
     */
    public void beforeExchange(long topVer, boolean left) throws GridException;

    /**
     * @return {@code True} is DR is enabled.
     */
    public boolean enabled();

    /**
     * @return {@code True} if receives DR data.
     */
    public boolean receiveEnabled();

    /**
     * In case some partition is evicted, we remove entries of this partition from backup queue.
     *
     * @param part Partition.
     */
    public void partitionEvicted(int part);

    /**
     * Callback for received entries from receiver hub.
     *
     * @param entriesCnt Number of received entries.
     */
    public void onReceiveCacheEntriesReceived(int entriesCnt);

    /**
     * Resets metrics for current cache.
     */
    public void resetMetrics();
}
