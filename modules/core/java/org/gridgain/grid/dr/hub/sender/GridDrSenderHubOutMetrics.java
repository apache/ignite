/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender;

/**
 * Data center replication sender hub metrics for outgoing data, i.e. data transferred from sender hub to
 * remote receiver hubs.
 */
public interface GridDrSenderHubOutMetrics {
    /**
     * Gets amount of batches sent to receiver hubs.
     *
     * @return Amount of batches sent to receiver hubs.
     */
    public int batchesSent();

    /**
     * Gets amount of bytes sent to receiver hubs.
     *
     * @return Amount of bytes sent to receiver hubs.
     */
    public long entriesSent();

    /**
     * Gets amount of bytes sent to receiver hubs.
     *
     * @return Amount of bytes sent to receiver hubs.
     */
    public long bytesSent();

    /**
     * Gets amount of sent batches with received acknowledgement from receiver hubs.
     *
     * @return Amount of sent batches with received acknowledgement from receiver hubs.
     */
    public int batchesAcked();

    /**
     * Gets amount of sent entries with received acknowledgement from receiver hubs.
     *
     * @return Amount of sent entries with received acknowledgement from receiver hubs.
     */
    public long entriesAcked();

    /**
     * Gets amount of sent bytes with received acknowledgement from receiver hubs.
     *
     * @return Amount of sent bytes with received acknowledgement from receiver hubs.
     */
    public long bytesAcked();

    /**
     * Gets average time in milliseconds between sending batch for the first time and receiving acknowledgement for it.
     *
     * @return Average time in milliseconds between sending batch for the first time and receiving acknowledgement
     *     for it.
     */
    public double averageBatchAckTime();

    /**
     * Checks whether metrics are empty, i.e. all parameters are zero.
     *
     * @return {@code True} if metrics are empty.
     */
    public boolean empty();
}
