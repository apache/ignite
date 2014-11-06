/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.dr.*;

import java.util.*;

/**
 * Replication manager class which processes all replication events.
 */
public interface GridCacheDrManager<K, V> extends GridCacheManager<K, V> {
    /**
     * Perform replication.
     *
     * @param entry Replication entry.
     * @param drType Replication type.
     */
    public void replicate(GridDrRawEntry<K, V> entry, GridDrType drType);

    /**
     * Process partitions "before exchange" event.
     *
     * @param topVer Topology version.
     * @param left {@code True} if exchange has been caused by node leave.
     * @throws GridException If failed.
     */
    public void beforeExchange(long topVer, boolean left) throws GridException;

    /**
     * In case some partition is evicted, we remove entries of this partition from backup queue.
     *
     * @param part Partition.
     */
    public void partitionEvicted(int part);

    /**
     * Initiate state transfer.
     *
     * @param dataCenterIds Target data center IDs.
     * @return Future that will be completed when all state transfer batches are sent.
     */
    public GridFuture<?> stateTransfer(Collection<Byte> dataCenterIds);

    /**
     * List currently active state transfers.
     *
     * @return List of currently active state transfers.
     * @throws GridException If failed.
     */
    public Collection<GridDrStateTransferDescriptor> listStateTransfers() throws GridException;

    /**
     * Pauses data center replication.
     *
     * @throws GridException If failed.
     */
    public void pause() throws GridException;

    /**
     * Resumes data center replication.
     *
     * @throws GridException If failed.
     */
    public void resume() throws GridException;

    /**
     * Get DR pause state.
     *
     * @return DR pause state.
     */
    public GridDrStatus drPauseState();

    /**
     * @return Count of keys enqueued for data center replication.
     */
    public int queuedKeysCount();

    /**
     * @return Size of backup data center replication queue.
     */
    public int backupQueueSize();

    /**
     * @return Count of data center replication batches awaiting to be send.
     */
    public int batchWaitingSendCount();

    /**
     * @return Count of data center replication batches awaiting acknowledge from sender hub.
     */
    public int batchWaitingAcknowledgeCount();

    /**
     * @return Count of available sender hubs.
     */
    public int senderHubsCount();
}
