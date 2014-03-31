/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.receiver;

/**
 * Data center replication receiver hub metrics for outgoing data, i.e. data transferred from receiver hub to
 * receiver caches.
 */
public interface GridDrReceiverHubOutMetrics {
    /**
     * Gets amount of batches waiting to be stored in receiver caches.
     *
     * @return Amount of batches waiting to be stored in receiver caches.
     */
    public int batchesSent();

    /**
     * Gets amount of entries waiting to be stored in receiver caches.
     *
     * @return Amount of entries waiting to be stored in receiver caches.
     */
    public long entriesSent();

    /**
     * Gets amount of bytes waiting to be stored.
     *
     * @return Amount of bytes waiting to be stored.
     */
    public long bytesSent();

    /**
     * Gets amount of batches stored in receiver caches.
     *
     * @return Amount of batches stored in receiver caches.
     */
    public int batchesAcked();

    /**
     * Gets amount of cache entries stored in receiver caches.
     *
     * @return Amount of cache entries stored in receiver caches.
     */
    public long entriesAcked();

    /**
     * Gets amount of bytes stored in receiver caches.
     *
     * @return Amount of bytes stored in receiver caches.
     */
    public long bytesAcked();

    /**
     * Gets average time in milliseconds between sending batch to receiver cache nodes and successfully storing it.
     *
     * @return Average time in milliseconds between sending batch to receiver cache nodes and successfully storing it.
     */
    public double averageBatchAckTime();

    /**
     * Checks whether metrics are empty, i.e. all parameters are zero.
     *
     * @return {@code True} if metrics are empty.
     */
    public boolean empty();
}
