/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.receiver;

import org.gridgain.grid.util.mbean.*;

/**
 * This interface defines JMX view on data center replication receiver hub.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean that provides access to receiver hub descriptor.")
public interface GridDrReceiverHubMBean {
    /**
     * Get metrics (statistics) for this receiver hub.
     *
     * @return Receiver hub metrics.
     */
    @GridMBeanDescription("Formatted receiver hub metrics.")
    public String metricsFormatted();

    /**
     * Gets local host name receiving hub server is bound to.
     *
     * @return Local host name.
     */
    @GridMBeanDescription("Local hostname.")
    public String getLocalInboundHost();

    /**
     * Gets local port receiver hub is bound to.
     *
     * @return Local port.
     */
    @GridMBeanDescription("Local port.")
    public int getLocalInboundPort();

    /**
     * Gets number of selector threads in receiver hub's TCP server.
     *
     * @return Amount of server NIO threads.
     */
    @GridMBeanDescription("Number of selector threads in receiver hub's TCP server.")
    public int getSelectorCount();

    /**
     * Gets number of threads responsible for sender hub requests processing.
     *
     * @return Amount of server worker threads.
     */
    @GridMBeanDescription("Amount of server worker threads.")
    public int getWorkerThreads();

    /**
     * Gets message queue limit for incoming and outgoing messages.
     *
     * @return Message queue limit.
     */
    @GridMBeanDescription("Message queue limit for incoming and outgoing messages.")
    public int getMessageQueueLimit();

    /**
     * Whether to use TCP_NODELAY mode.
     *
     * @return TCP_NODELAY mode flag.
     */
    @GridMBeanDescription("TCP_NODELAY mode flag.")
    public boolean isTcpNodelay();

    /**
     * Whether to use direct buffer when processing sender hub requests.
     *
     * @return Direct buffer flag.
     */
    @GridMBeanDescription("Direct buffer flag.")
    public boolean isDirectBuffer();

    /**
     * Gets idle timeout for sender hub socket connection.
     *
     * @return Idle timeout for sender hub socket connection.
     */
    @GridMBeanDescription("Idle timeout for sender hub socket connection.")
    public long getIdleTimeout();

    /**
     * Gets write timeout for sender hub socket connection.
     *
     * @return Write timeout.
     */
    @GridMBeanDescription("Write timeout for sender hub socket connection.")
    public long getWriteTimeout();

    /**
     * Gets data center replication data loader flush frequency.
     *
     * @return Data flush frequency.
     */
    @GridMBeanDescription("Data flush frequency.")
    public long getFlushFrequency();

    /**
     * Gets data center replication data loader per node buffer size.
     *
     * @return Buffer size.
     */
    @GridMBeanDescription("DR data loader per node buffer size.")
    public int getPerNodeBufferSize();

    /**
     * Gets per node parallel load operations.
     *
     * @return Per node parallel load operations.
     */
    @GridMBeanDescription("Parallel load operations per node.")
    public int getPerNodeParallelLoadOperations();
}
