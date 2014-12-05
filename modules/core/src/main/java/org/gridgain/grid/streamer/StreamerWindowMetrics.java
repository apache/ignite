/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer;

/**
 * Streamer window metrics.
 */
public interface StreamerWindowMetrics {
    /**
     * Gets window name.
     *
     * @return Window name.
     */
    public String name();

    /**
     * Gets window size.
     *
     * @return Window size.
     */
    public int size();

    /**
     * Gets eviction queue size.
     *
     * @return Eviction queue size.
     */
    public int evictionQueueSize();
}
