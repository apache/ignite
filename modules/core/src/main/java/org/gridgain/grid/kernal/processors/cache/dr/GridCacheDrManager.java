/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.jetbrains.annotations.*;

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
     * Handles DR for atomic cache.
     *
     * @param e Cache entry.
     * @param op Operation.
     * @param writeObj New value.
     * @param valBytes New value byte.
     * @param ttl TTL.
     * @param drTtl DR TTL.
     * @param drExpireTime DR expire time
     * @param drVer DR version.
     * @return DR result.
     * @throws GridException If update failed.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    public GridDrResolveResult<V> resolveAtomic(GridCacheEntryEx<K, V> e,
         GridCacheOperation op,
         @Nullable Object writeObj,
         @Nullable byte[] valBytes,
         long ttl,
         long drTtl,
         long drExpireTime,
         @Nullable GridCacheVersion drVer) throws GridException, GridCacheEntryRemovedException;

    /**
     * Handles DR for transactional cache.
     *
     * @param e Cache entry.
     * @param txEntry Transaction entry.
     * @param newVer Version.
     * @param op Operation.
     * @param newVal New value.
     * @param newValBytes New value bytes.
     * @param newTtl TTL.
     * @param newDrExpireTime DR expire time
     * @return DR result.
     * @throws GridException If update failed.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    public GridDrResolveResult<V> resolveTx(
        GridCacheEntryEx<K, V> e,
        GridCacheTxEntry<K, V> txEntry,
        GridCacheVersion newVer,
        GridCacheOperation op,
        V newVal,
        byte[] newValBytes,
        long newTtl,
        long newDrExpireTime) throws GridException, GridCacheEntryRemovedException;

    /**
     * Performs replication.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param drType Replication type.
     * @throws GridException If failed.
     */
    public void replicate(K key,
        @Nullable byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        GridDrType drType)throws GridException;

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
