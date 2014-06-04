package org.gridgain.grid.spi.communication.tcp;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * MBean provide access to TCP-based communication SPI.
 */
@GridMBeanDescription("MBean provide access to TCP-based communication SPI.")
public interface GridTcpCommunicationSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets local host address for socket binding.
     * Beside loopback address physical node could have
     * several other ones, but only one is assigned to grid node.
     *
     * @return Grid node IP address.
     */
    @GridMBeanDescription("Grid node IP address.")
    public String getLocalAddress();

    /**
     * Gets local port for socket binding.
     *
     * @return Port number.
     */
    @GridMBeanDescription("Port number.")
    public int getLocalPort();

    /**
     * Gets local port for shared memory communication.
     *
     * @return Port number.
     */
    @GridMBeanDescription("Shared memory endpoint port number.")
    public int getSharedMemoryPort();

    /**
     * Gets maximum number of local ports tried if all previously
     * tried ports are occupied.
     *
     * @return Local port range.
     */
    @GridMBeanDescription("Local port range.")
    public int getLocalPortRange();

    /**
     * Gets maximum idle connection time upon which idle connections
     * will be closed.
     *
     * @return Maximum idle connection time.
     */
    @GridMBeanDescription("Maximum idle connection time.")
    public long getIdleConnectionTimeout();

    /**
     * Gets flag that indicates whether direct or heap allocated buffer is used.
     *
     * @return Flag that indicates whether direct or heap allocated buffer is used.
     */
    @GridMBeanDescription("Flag that indicates whether direct or heap allocated buffer is used.")
    public boolean isDirectBuffer();

    /**
     * Gets count of selectors used in TCP server. Default value equals to the
     * number of CPUs available in the system.
     *
     * @return Count of selectors in TCP server.
     */
    @GridMBeanDescription("Count of selectors used in TCP server.")
    public int getSelectorsCount();

    /**
     * Gets flag defining whether asynchronous (NIO) or synchronous (blocking) IO
     * should be used to send messages.
     *
     * @return {@code True} if asynchronous IO should be used to send messages.
     */
    @GridMBeanDescription("Asynchronous send.")
    public boolean isAsyncSend();

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    @GridMBeanDescription("Sent messages count.")
    public int getSentMessagesCount();

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    @GridMBeanDescription("Sent bytes count.")
    public long getSentBytesCount();

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    @GridMBeanDescription("Received messages count.")
    public int getReceivedMessagesCount();

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    @GridMBeanDescription("Received bytes count.")
    public long getReceivedBytesCount();

    /**
     * Gets outbound messages queue size.
     *
     * @return Outbound messages queue size.
     */
    @GridMBeanDescription("Outbound messages queue size.")
    public int getOutboundMessagesQueueSize();

    /**
     * Gets connect timeout used when establishing connection
     * with remote nodes.
     *
     * @return Connect timeout.
     */
    @GridMBeanDescription("Connect timeout.")
    public long getConnectTimeout();

    /**
     * Gets maximum connect timeout.
     *
     * @return Maximum connect timeout.
     */
    @GridMBeanDescription("Maximum connect timeout.")
    public long getMaxConnectTimeout();

    /**
     * Gets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     *
     * @return Reconnects count.
     */
    @GridMBeanDescription("Reconnect count on connection failure.")
    public int getReconnectCount();

    /**
     * Gets value for {@code TCP_NODELAY} socket option.
     *
     * @return {@code True} if TCP delay is disabled.
     */
    @GridMBeanDescription("TCP_NODELAY socket option value.")
    public boolean isTcpNoDelay();

    /**
     * Gets connection buffer flush frequency.
     * <p>
     * Client connections to other nodes in topology use buffered output.
     * This frequency defines how often system will advice to flush
     * connection buffer.
     *
     * @return Flush frequency.
     */
    @GridMBeanDescription("Connection buffer flush frequency.")
    public long getConnectionBufferFlushFrequency();

    /**
     * Sets connection buffer flush frequency.
     * <p>
     * Client connections to other nodes in topology use buffered output.
     * This frequency defines how often system will advice to flush
     * connection buffer.
     * <p>
     * If not provided, default value is {@link GridTcpCommunicationSpi#DFLT_CONN_BUF_FLUSH_FREQ}.
     * <p>
     * This property is used only if {@link #getConnectionBufferSize()} is greater than {@code 0}.
     *
     * @param connBufFlushFreq Flush frequency.
     * @see #getConnectionBufferSize()
     */
    @GridMBeanDescription("Sets connection buffer flush frequency.")
    public void setConnectionBufferFlushFrequency(long connBufFlushFreq);

    /**
     * Gets connection buffer size.
     * <p>
     * If set to {@code 0} connection buffer is disabled.
     *
     * @return Connection buffer size.
     */
    @GridMBeanDescription("Connection buffer size.")
    public int getConnectionBufferSize();

    /**
     * Gets flag defining whether direct send buffer should be used.
     *
     * @return {@code True} if direct buffers should be used.
     */
    @GridMBeanDescription("Direct send buffer.")
    public boolean isDirectSendBuffer();

    /**
     * Gets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link GridTcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket receive buffer size.
     */
    @GridMBeanDescription("Socket receive buffer.")
    public int getSocketReceiveBuffer();

    /**
     * Gets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link GridTcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket send buffer size.
     */
    @GridMBeanDescription("Socket send buffer.")
    public int getSocketSendBuffer();

    /**
     * Gets flag indicating whether dual-socket connection between nodes should be enforced. If set to
     * {@code true}, two separate connections will be established between communicating nodes: one for outgoing
     * messages, and one for incoming. When set to {@code false}, single {@code TCP} connection will be used
     * for both directions.
     * <p>
     * This flag is useful on some operating systems, when {@code TCP_NODELAY} flag is disabled and
     * messages take too long to get delivered.
     *
     * @return Whether dual-socket connection should be enforced.
     */
    @GridMBeanDescription("Dual-socket connection.")
    public boolean isDualSocketConnection();

    /**
     * Gets message queue limit for incoming and outgoing messages.
     * <p>
     * This parameter only used when {@link #isAsyncSend()} set to {@code true}.
     *
     * @return Send queue size limit.
     */
    @GridMBeanDescription("Message queue size limit.")
    public int getMessageQueueLimit();

    /**
     * Gets the minimum number of messages for this SPI, that are buffered
     * prior to sending.
     *
     * @return Minimum buffered message count.
     */
    @GridMBeanDescription("Minimum buffered message count.")
    public int getMinimumBufferedMessageCount();

    /**
     * Gets the buffer size ratio for this SPI. As messages are sent,
     * the buffer size is adjusted using this ratio.
     *
     * @return Buffer size ratio.
     */
    @GridMBeanDescription("Buffer size ratio.")
    public double getBufferSizeRatio();
}
