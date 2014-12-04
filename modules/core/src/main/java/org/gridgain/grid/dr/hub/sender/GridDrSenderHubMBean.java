/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender;

import org.apache.ignite.mbean.*;

/**
 * This interface defines JMX view on data center replication sender hub.
 */
@IgniteMBeanDescription("MBean that provides access to sender hub descriptor.")
public interface GridDrSenderHubMBean {
    /**
     * Gets metrics (statistics) for this sender hub.
     *
     * @return Sender hub metrics.
     */
    @IgniteMBeanDescription("Formatted sender hub metrics.")
    public String metricsFormatted();

    /**
     * Gets maximum wait queue size. When there are too many replication requests coming to sender hub and it
     * cannot process them at the same speed, it is necessary to restrict maximum waiting requests size in
     * order to avoid out-of-memory issues.
     *
     * @return Maximum wait queue size.
     */
    @IgniteMBeanDescription("Maximum size of wait queue.")
    public int getMaxQueueSize();

    /**
     * Gets health check frequency in milliseconds. This frequency shows how often disconnected nodes will try
     * reconnect and how often connected nodes will be checked for necessity to send a ping request.
     *
     * @return Ping frequency.
     */
    @IgniteMBeanDescription("Health check frequency in milliseconds.")
    public long getHealthCheckFrequency();

    /**
     * Gets system request timeout. Replication processor performs two types of system requests: handshake and ping.
     * Handshake request contains information required by receiving side and is only sent when network connection
     * is established. Ping request just checks that receiving side is reachable and responds when there was no
     * reads from it for too long.
     * In case any of these two requests was sent to receiving hub and there is no response for too long, it
     * indicates a problem with either connectivity or receiving hub itself. Thus, when the given timeout is
     * exceeded, remote sender hub node is disconnected.
     *
     * @return System request timeout in milliseconds.
     */
    @IgniteMBeanDescription("System request timeout.")
    public long getSystemRequestTimeout();

    /**
     * Gets read timeout in milliseconds. In case difference between current time and the time when last read operation
     * occurred is greater than this timeout, ping request will be sent to remote sender hub.
     *
     * @return Read timeout in milliseconds.
     */
    @IgniteMBeanDescription("Read timeout in milliseconds.")
    public long getReadTimeout();

    /**
     * Gets maximum failed connection attempts.
     * When this limit is reached, remote data center sender hub node gets disconnected.
     *
     * @return Maximum failed connect attempts.
     */
    @IgniteMBeanDescription("Maximum failed connection attempts.")
    public int getMaxFailedConnectAttempts();

    /**
     * Maximum amount of errors received from remote data center sender hub node.
     * When this limit is reached, remote data center sender hub node gets disconnected.
     *
     * @return Maximum errors.
     */
    @IgniteMBeanDescription("Maximum amount of errors received from the remote data center.")
    public int getMaxErrors();

    /**
     * Timeout after which node can be reconnected in case it was previously disconnected due to a failure.
     *
     * @return Reconnect-on-failure timeout.
     */
    @IgniteMBeanDescription("Node reconnection timeout after failure.")
    public long getReconnectOnFailureTimeout();

    /**
     * Gets cache names this sender hub works with.
     *
     * @return Cache names this sender hub works with.
     */
    @IgniteMBeanDescription("Cache names.")
    public String getCacheNames();

    /**
     * Gets formatted remote data centers configuration info.
     *
     * @return Formatted remote data centers configuration info.
     */
    @IgniteMBeanDescription("Formatted remote data centers configuration info.")
    public String remoteDataCentersFormatted();
}
