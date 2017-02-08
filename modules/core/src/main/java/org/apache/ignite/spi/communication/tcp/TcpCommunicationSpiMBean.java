/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * MBean provide access to TCP-based communication SPI.
 */
@MXBeanDescription("MBean provide access to TCP-based communication SPI.")
public interface TcpCommunicationSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets local host address for socket binding.
     * Beside loopback address physical node could have
     * several other ones, but only one is assigned to grid node.
     *
     * @return Grid node IP address.
     */
    @MXBeanDescription("Grid node IP address.")
    public String getLocalAddress();

    /**
     * Gets local port for socket binding.
     *
     * @return Port number.
     */
    @MXBeanDescription("Port number.")
    public int getLocalPort();

    /**
     * Returns {@code true} if {@code TcpCommunicationSpi} should
     * maintain connection for outgoing and incoming messages separately.
     * In this case total number of connections between local and some remote node
     * is {@link #getConnectionsPerNode()} * 2.
     * <p>
     * Returns {@code false} if each connection of {@link #getConnectionsPerNode()}
     * should be used for outgoing and incoming messages.
     * <p>
     * Default is {@code false}.
     *
     * @return {@code true} to use paired connections and {@code false} otherwise.
     * @see #getConnectionsPerNode()
     */
    @MXBeanDescription("Paired connections used.")
    public boolean isUsePairedConnections();

    /**
     * Gets number of connections to each remote node. if {@link #isUsePairedConnections()}
     * is {@code true} then number of connections is doubled and half is used for incoming and
     * half for outgoing messages.
     *
     * @return Number of connections per node.
     * @see #isUsePairedConnections()
     */
    @MXBeanDescription("Connections per node.")
    public int getConnectionsPerNode();

    /**
     * Gets local port for shared memory communication.
     *
     * @return Port number.
     */
    @MXBeanDescription("Shared memory endpoint port number.")
    public int getSharedMemoryPort();

    /**
     * Gets maximum number of local ports tried if all previously
     * tried ports are occupied.
     *
     * @return Local port range.
     */
    @MXBeanDescription("Local port range.")
    public int getLocalPortRange();

    /**
     * Gets maximum idle connection time upon which idle connections
     * will be closed.
     *
     * @return Maximum idle connection time.
     */
    @MXBeanDescription("Maximum idle connection time.")
    public long getIdleConnectionTimeout();

    /**
     * Gets flag that indicates whether direct or heap allocated buffer is used.
     *
     * @return Flag that indicates whether direct or heap allocated buffer is used.
     */
    @MXBeanDescription("Flag that indicates whether direct or heap allocated buffer is used.")
    public boolean isDirectBuffer();

    /**
     * Gets count of selectors used in TCP server. Default value equals to the
     * number of CPUs available in the system.
     *
     * @return Count of selectors in TCP server.
     */
    @MXBeanDescription("Count of selectors used in TCP server.")
    public int getSelectorsCount();

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    @MXBeanDescription("Sent messages count.")
    public int getSentMessagesCount();

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    @MXBeanDescription("Sent bytes count.")
    public long getSentBytesCount();

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    @MXBeanDescription("Received messages count.")
    public int getReceivedMessagesCount();

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    @MXBeanDescription("Received bytes count.")
    public long getReceivedBytesCount();

    /**
     * Gets outbound messages queue size.
     *
     * @return Outbound messages queue size.
     */
    @MXBeanDescription("Outbound messages queue size.")
    public int getOutboundMessagesQueueSize();

    /**
     * Gets connect timeout used when establishing connection
     * with remote nodes.
     *
     * @return Connect timeout.
     */
    @MXBeanDescription("Connect timeout.")
    public long getConnectTimeout();

    /**
     * Gets maximum connect timeout.
     *
     * @return Maximum connect timeout.
     */
    @MXBeanDescription("Maximum connect timeout.")
    public long getMaxConnectTimeout();

    /**
     * Gets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     *
     * @return Reconnects count.
     */
    @MXBeanDescription("Reconnect count on connection failure.")
    public int getReconnectCount();

    /**
     * Defines how many non-blocking {@code selector.selectNow()} should be made before
     * falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
     * Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
     *
     * @return Selector thread busy-loop iterations.
     */
    @MXBeanDescription("Selector thread busy-loop iterations.")
    public long getSelectorSpins();

    /**
     * Gets value for {@code TCP_NODELAY} socket option.
     *
     * @return {@code True} if TCP delay is disabled.
     */
    @MXBeanDescription("TCP_NODELAY socket option value.")
    public boolean isTcpNoDelay();

    /**
     * Gets connection buffer flush frequency.
     * <p>
     * Client connections to other nodes in topology use buffered output.
     * This frequency defines how often system will advice to flush
     * connection buffer.
     *
     * @return Flush frequency.
     * @deprecated Not used anymore.
     */
    @Deprecated
    @MXBeanDescription("Connection buffer flush frequency.")
    public long getConnectionBufferFlushFrequency();

    /**
     * Sets connection buffer flush frequency.
     * <p>
     * Client connections to other nodes in topology use buffered output.
     * This frequency defines how often system will advice to flush
     * connection buffer.
     * <p>
     * This property is used only if {@link #getConnectionBufferSize()} is greater than {@code 0}.
     *
     * @param connBufFlushFreq Flush frequency.
     * @see #getConnectionBufferSize()
     * @deprecated Not used anymore.
     */
    @Deprecated
    @MXBeanDescription("Sets connection buffer flush frequency.")
    public void setConnectionBufferFlushFrequency(long connBufFlushFreq);

    /**
     * Gets connection buffer size.
     * <p>
     * If set to {@code 0} connection buffer is disabled.
     *
     * @return Connection buffer size.
     * @deprecated Not used anymore.
     */
    @Deprecated
    @MXBeanDescription("Connection buffer size.")
    public int getConnectionBufferSize();

    /**
     * Gets flag defining whether direct send buffer should be used.
     *
     * @return {@code True} if direct buffers should be used.
     */
    @MXBeanDescription("Direct send buffer.")
    public boolean isDirectSendBuffer();

    /**
     * Gets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket receive buffer size.
     */
    @MXBeanDescription("Socket receive buffer.")
    public int getSocketReceiveBuffer();

    /**
     * Gets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket send buffer size.
     */
    @MXBeanDescription("Socket send buffer.")
    public int getSocketSendBuffer();

    /**
     * Gets message queue limit for incoming and outgoing messages.
     *
     * @return Send queue size limit.
     */
    @MXBeanDescription("Message queue size limit.")
    public int getMessageQueueLimit();

    /**
     * Gets the minimum number of messages for this SPI, that are buffered
     * prior to sending.
     *
     * @return Minimum buffered message count.
     * @deprecated Not used anymore.
     */
    @Deprecated
    @MXBeanDescription("Minimum buffered message count.")
    public int getMinimumBufferedMessageCount();

    /**
     * Gets socket write timeout for TCP connections. If message can not be written to
     * socket within this time then connection is closed and reconnect is attempted.
     *
     * @return Socket write timeout for TCP connections.
     */
    @MXBeanDescription("Socket write timeout.")
    public long getSocketWriteTimeout();

    /**
     * Gets number of received messages per connection to node after which acknowledgment message is sent.
     *
     * @return Number of received messages after which acknowledgment is sent.
     */
    @MXBeanDescription("Number of received messages after which acknowledgment is sent.")
    public int getAckSendThreshold();

    /**
     * Gets maximum number of stored unacknowledged messages per connection to node.
     * If number of unacknowledged messages exceeds this number then connection to node is
     * closed and reconnect is attempted.
     *
     * @return Maximum number of unacknowledged messages.
     */
    @MXBeanDescription("Maximum number of unacknowledged messages.")
    public int getUnacknowledgedMessagesBufferSize();

    /**
     * Gets slow client queue limit.
     * <p/>
     * When set to a positive number, communication SPI will monitor clients outbound queue sizes and will drop
     * those clients whose queue exceeded this limit.
     *
     * @return Slow client queue limit.
     */
    @MXBeanDescription("Slow client queue limit.")
    public int getSlowClientQueueLimit();

    /**
     * Dumps SPI per-connection stats to logs.
     */
    @MXBeanDescription("Dumps SPI statistics to logs.")
    public void dumpStats();
}
