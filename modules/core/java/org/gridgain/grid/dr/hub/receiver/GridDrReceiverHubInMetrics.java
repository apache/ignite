/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.receiver;

/**
 * Data center replication receiver hub metrics for incoming data, i.e. data transferred from remote sender hubs to
 * receiver hub.
 */
public interface GridDrReceiverHubInMetrics {
    /**
     * Gets amount of batches received from remote sender hubs.
     *
     * @return Amount of batches received from remote sender hubs.
     */
    public int batchesReceived();

    /**
     * Gets amount of cache entries received from remote sender hubs.
     *
     * @return Amount of cache entries received from remote sender hubs.
     */
    public long entriesReceived();

    /**
     * Gets total amount of bytes received from remote sender hubs.
     *
     * @return Total amount of bytes received from remote sender hubs.
     */
    public long bytesReceived();

    /**
     * Checks whether metrics are empty, i.e. all parameters are zero.
     *
     * @return {@code True} if metrics are empty.
     */
    public boolean empty();
}
