/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender;

/**
 * Data center replication sender hub metrics for incoming data, i.e. data transferred from sender caches to
 * sender hub.
 */
public interface GridDrSenderHubInMetrics {
    /**
     * Gets amount of batches received from sender caches.
     *
     * @return Amount of batches received from sender caches.
     */
    public int batchesReceived();

    /**
     * Gets amount of cache entries received from sender caches.
     *
     * @return Amount of cache entries received from sender caches.
     */
    public long entriesReceived();

    /**
     * Gets amount of bytes received from sender caches.
     *
     * @return Amount of bytes received from sender caches.
     */
    public long bytesReceived();

    /**
     * Checks whether metrics are empty, i.e. all parameters are zero.
     *
     * @return {@code True} if metrics are empty.
     */
    public boolean empty();
}
