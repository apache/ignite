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

package org.apache.ignite.spi.communication.tcp.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_ADDRS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_EXT_ADDRS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_FORCE_CLIENT_SERVER_CONNECTIONS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_HOST_NAMES;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_PAIRED_CONN;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_PORT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_SHMEM_PORT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DISABLED_CLIENT_PORT;
import static org.apache.ignite.spi.communication.tcp.internal.GridNioServerWrapper.MAX_CONN_PER_NODE;

/**
 * Only may implement it TcpCommunicationSpi.
 */
public abstract class TcpCommunicationConfigInitializer extends IgniteSpiAdapter implements CommunicationSpi<Message> {
    /** Config. */
    protected final TcpCommunicationConfiguration cfg = new TcpCommunicationConfiguration();

    /** Attribute names. */
    protected AttributeNames attributeNames;

    /** Shared memory server. */
    protected IpcSharedMemoryServerEndpoint shmemSrv;

    /** Statistics. */
    protected TcpCommunicationMetricsListener metricsLsnr;

    /** Connection policy. */
    protected ConnectionPolicy connPlc = new FirstConnectionPolicy();

    /** Tracing. */
    protected Tracing tracing;

    /**
     * Sets address resolver.
     *
     * @param addrRslvr Address resolver.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setAddressResolver(AddressResolver addrRslvr) {
        // Injection should not override value already set by Spring or user.
        if (cfg.addrRslvr() == null)
            cfg.addrRslvr(addrRslvr);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setAddressResolver(AddressResolver)}.
     *
     * @return Address resolver.
     */
    public AddressResolver getAddressResolver() {
        return cfg.addrRslvr();
    }

    /**
     * Injects resources.
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    @Override protected void injectResources(Ignite ignite) {
        super.injectResources(ignite);

        if (ignite != null) { // null when service is destroying.
            setAddressResolver(ignite.configuration().getAddressResolver());
            setLocalAddress(ignite.configuration().getLocalHost());
            tracing = ignite instanceof IgniteEx ? ((IgniteEx)ignite).context().tracing() : new NoopTracing();
        }
    }

    /**
     * Sets local host address for socket binding. Note that one node could have
     * additional addresses beside the loopback one. This configuration
     * parameter is optional.
     *
     * @param locAddr IP address. Default value is any available local
     *      IP address.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setLocalAddress(String locAddr) {
        // Injection should not override value already set by Spring or user.
        if (cfg.localAddress() == null)
            cfg.localAddress(locAddr);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setLocalAddress(String)}.
     *
     * @return Grid node IP address.
     */
    public String getLocalAddress() {
        return cfg.localAddress();
    }

    /**
     * Sets local port for socket binding.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_PORT}.
     *
     * @param locPort Port number.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setLocalPort(int locPort) {
        cfg.localPort(locPort);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setLocalPort(int)}.
     *
     * @return Port number.
     */
    public int getLocalPort() {
        return cfg.localPort();
    }

    /**
     * Sets local port range for local host ports (value must greater than or equal to <tt>0</tt>).
     * If provided local port (see {@link #setLocalPort(int)}} is occupied,
     * implementation will try to increment the port number for as long as it is less than
     * initial value plus this range.
     * <p>
     * If port range value is <tt>0</tt>, then implementation will try bind only to the port provided by
     * {@link #setLocalPort(int)} method and fail if binding to this port did not succeed.
     * <p>
     * Local port range is very useful during development when more than one grid nodes need to run
     * on the same physical machine.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_PORT_RANGE}.
     *
     * @param locPortRange New local port range.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setLocalPortRange(int locPortRange) {
        A.ensure(locPortRange >= 0, "The port range must be positive.");

        cfg.localPortRange(locPortRange);

        return (TcpCommunicationSpi)this;
    }

    /**
     * See {@link #setLocalPortRange(int)}.
     *
     * @return Local Port range.
     */
    public int getLocalPortRange() {
        return cfg.localPortRange();
    }

    /**
     * See {@link #setUsePairedConnections(boolean)}.
     *
     * @return {@code true} to use paired connections and {@code false} otherwise.
     */
    public boolean isUsePairedConnections() {
        return cfg.usePairedConnections();
    }

    /**
     * Set this to {@code true} if {@code TcpCommunicationSpi} should
     * maintain connection for outgoing and incoming messages separately.
     * In this case total number of connections between local and each remote node
     * is {@link #getConnectionsPerNode()} * 2.
     * <p>
     * Set this to {@code false} if each connection of {@link #getConnectionsPerNode()}
     * should be used for outgoing and incoming messages. In this case total number
     * of connections between local and each remote node is {@link #getConnectionsPerNode()}.
     * <p>
     * Default is {@code false}.
     *
     * @param usePairedConnections {@code true} to use paired connections and {@code false} otherwise.
     * @return {@code this} for chaining.
     * @see #getConnectionsPerNode()
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setUsePairedConnections(boolean usePairedConnections) {
        cfg.usePairedConnections(usePairedConnections);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Sets number of connections to each remote node. if {@link #isUsePairedConnections()}
     * is {@code true} then number of connections is doubled and half is used for incoming and
     * half for outgoing messages.
     *
     * @param maxConnectionsPerNode Number of connections per node.
     * @return {@code this} for chaining.
     * @see #isUsePairedConnections()
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setConnectionsPerNode(int maxConnectionsPerNode) {
        cfg.connectionsPerNode(maxConnectionsPerNode);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setConnectionsPerNode(int)}.
     *
     * @return Number of connections per node.
     */
    public int getConnectionsPerNode() {
        return cfg.connectionsPerNode();
    }

    /**
     * Sets local port to accept shared memory connections.
     * <p>
     * If set to {@code -1} shared memory communication will be disabled.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_SHMEM_PORT}.
     *
     * @param shmemPort Port number.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSharedMemoryPort(int shmemPort) {
        cfg.shmemPort(shmemPort);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setSharedMemoryPort(int)}.
     *
     * @return Port number.
     */
    public int getSharedMemoryPort() {
        return cfg.shmemPort();
    }

    /**
     * Sets maximum idle connection timeout upon which a connection
     * to client will be closed.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_IDLE_CONN_TIMEOUT}.
     *
     * @param idleConnTimeout Maximum idle connection time.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setIdleConnectionTimeout(long idleConnTimeout) {
        cfg.idleConnectionTimeout(idleConnTimeout);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setIdleConnectionTimeout(long)}.
     *
     * @return Maximum idle connection time.
     */
    public long getIdleConnectionTimeout() {
        return cfg.idleConnectionTimeout();
    }

    /**
     * See {@link #setSocketWriteTimeout(long)}.
     *
     * @return Socket write timeout for TCP connections.
     */
    public long getSocketWriteTimeout() {
        return cfg.socketWriteTimeout();
    }

    /**
     * Sets socket write timeout for TCP connection. If message can not be written to
     * socket within this time then connection is closed and reconnect is attempted.
     * <p>
     * Default to {@link TcpCommunicationSpi#DFLT_SOCK_WRITE_TIMEOUT}.
     *
     * @param sockWriteTimeout Socket write timeout for TCP connection.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSocketWriteTimeout(long sockWriteTimeout) {
        cfg.socketWriteTimeout(sockWriteTimeout);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setAckSendThreshold(int)}.
     *
     * @return Number of received messages after which acknowledgment is sent.
     */
    public int getAckSendThreshold() {
        return cfg.ackSendThreshold();
    }

    /**
     * Sets number of received messages per connection to node after which acknowledgment message is sent.
     * <p>
     * Default to {@link TcpCommunicationSpi#DFLT_ACK_SND_THRESHOLD}.
     *
     * @param ackSndThreshold Number of received messages after which acknowledgment is sent.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setAckSendThreshold(int ackSndThreshold) {
        cfg.ackSendThreshold(ackSndThreshold);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setUnacknowledgedMessagesBufferSize(int)}.
     *
     * @return Maximum number of unacknowledged messages.
     */
    public int getUnacknowledgedMessagesBufferSize() {
        return cfg.unackedMsgsBufferSize();
    }

    /**
     * Sets maximum number of stored unacknowledged messages per connection to node.
     * If number of unacknowledged messages exceeds this number then connection to node is
     * closed and reconnect is attempted.
     *
     * @param unackedMsgsBufSize Maximum number of unacknowledged messages.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setUnacknowledgedMessagesBufferSize(int unackedMsgsBufSize) {
        cfg.unackedMsgsBufferSize(unackedMsgsBufSize);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Sets connect timeout used when establishing connection
     * with remote nodes.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_CONN_TIMEOUT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param connTimeout Connect timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setConnectTimeout(long connTimeout) {
        cfg.connectionTimeout(connTimeout);

        failureDetectionTimeoutEnabled(false);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setConnectTimeout(long)}.
     *
     * @return Connect timeout.
     */
    public long getConnectTimeout() {
        return cfg.connectionTimeout();
    }

    /**
     * Sets maximum connect timeout. If handshake is not established within connect timeout,
     * then SPI tries to repeat handshake procedure with increased connect timeout.
     * Connect timeout can grow till maximum timeout value,
     * if maximum timeout value is reached then the handshake is considered as failed.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_MAX_CONN_TIMEOUT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param maxConnTimeout Maximum connect timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setMaxConnectTimeout(long maxConnTimeout) {
        cfg.maxConnectionTimeout(maxConnTimeout);

        failureDetectionTimeoutEnabled(false);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Gets maximum connect timeout.
     *
     * @return Maximum connect timeout.
     */
    public long getMaxConnectTimeout() {
        return cfg.maxConnectionTimeout();
    }

    /**
     * Sets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_RECONNECT_CNT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param reconCnt Maximum number of reconnection attempts.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setReconnectCount(int reconCnt) {
        cfg.reconCount(reconCnt);

        failureDetectionTimeoutEnabled(false);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Gets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     *
     * @return Reconnects count.
     */
    public int getReconnectCount() {
        return cfg.reconCount();
    }

    /**
     * Sets flag to allocate direct or heap buffer in SPI.
     * If value is {@code true}, then SPI will use {@link ByteBuffer#allocateDirect(int)} call.
     * Otherwise, SPI will use {@link ByteBuffer#allocate(int)} call.
     * <p>
     * If not provided, default value is {@code true}.
     *
     * @param directBuf Flag indicates to allocate direct or heap buffer in SPI.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setDirectBuffer(boolean directBuf) {
        cfg.directBuffer(directBuf);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Gets flag that indicates whether direct or heap allocated buffer is used.
     *
     * @return Flag that indicates whether direct or heap allocated buffer is used.
     */
    public boolean isDirectBuffer() {
        return cfg.directBuffer();
    }

    /**
     * Gets flag defining whether direct send buffer should be used.
     *
     * @return {@code True} if direct buffers should be used.
     */
    public boolean isDirectSendBuffer() {
        return cfg.directSendBuffer();
    }

    /**
     * Sets whether to use direct buffer for sending.
     *
     * If not provided default is {@code false}.
     *
     * @param directSndBuf {@code True} to use direct buffers for send.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setDirectSendBuffer(boolean directSndBuf) {
        cfg.directSendBuffer(directSndBuf);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Sets the count of selectors te be used in TCP server.
     * <p/>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_SELECTORS_CNT}.
     *
     * @param selectorsCnt Selectors count.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSelectorsCount(int selectorsCnt) { ;
        cfg.selectorsCount(selectorsCnt);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setSelectorsCount(int)}.
     *
     * @return Count of selectors in TCP server.
     */
    public int getSelectorsCount() {
        return cfg.selectorsCount();
    }

    /**
     * See {@link #setSelectorSpins(long)}.
     *
     * @return Selector thread busy-loop iterations.
     */
    public long getSelectorSpins() {
        return cfg.selectorSpins();
    }

    /**
     * Defines how many non-blocking {@code selector.selectNow()} should be made before
     * falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
     * Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
     *
     * @param selectorSpins Selector thread busy-loop iterations.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSelectorSpins(long selectorSpins) {
        cfg.selectorSpins(selectorSpins);

        return (TcpCommunicationSpi) this;
    }

    /** */
    @IgniteExperimental
    public void setConnectionRequestor(ConnectionRequestor connectionRequestor) {
        cfg.connectionRequestor(connectionRequestor);
    }

    /**
     * Sets value for {@code TCP_NODELAY} socket option. Each
     * socket will be opened using provided value.
     * <p>
     * Setting this option to {@code true} disables Nagle's algorithm
     * for socket decreasing latency and delivery time for small messages.
     * <p>
     * For systems that work under heavy network load it is advisable to
     * set this value to {@code false}.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_TCP_NODELAY}.
     *
     * @param tcpNoDelay {@code True} to disable TCP delay.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setTcpNoDelay(boolean tcpNoDelay) {
        cfg.tcpNoDelay(tcpNoDelay);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Gets value for {@code TCP_NODELAY} socket option.
     *
     * @return {@code True} if TCP delay is disabled.
     */
    public boolean isTcpNoDelay() {
        return cfg.tcpNoDelay();
    }

    /**
     * Gets value for {@code FILTER_REACHABLE_ADDRESSES} socket option.
     *
     * @return {@code True} if needed to filter reachable addresses.
     */
    public boolean isFilterReachableAddresses() {
        return cfg.filterReachableAddresses();
    }

    /**
     * Setting this option to {@code true} enables filter for reachable
     * addresses on creating tcp client.
     * <p>
     * Usually its advised to set this value to {@code false}.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_FILTER_REACHABLE_ADDRESSES}.
     *
     * @param filterReachableAddresses {@code True} to filter reachable addresses.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setFilterReachableAddresses(boolean filterReachableAddresses) {
        cfg.filterReachableAddresses(filterReachableAddresses);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Sets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockRcvBuf Socket receive buffer size.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSocketReceiveBuffer(int sockRcvBuf) {
        cfg.socketReceiveBuffer(sockRcvBuf);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setSocketReceiveBuffer(int)}.
     *
     * @return Socket receive buffer size.
     */
    public int getSocketReceiveBuffer() {
        return cfg.socketReceiveBuffer();
    }

    /**
     * Sets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockSndBuf Socket send buffer size.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSocketSendBuffer(int sockSndBuf) {
cfg.socketSendBuffer(sockSndBuf);

        return (TcpCommunicationSpi) this;
    }

    /**
     * See {@link #setSocketSendBuffer(int)}.
     *
     * @return Socket send buffer size.
     */
    public int getSocketSendBuffer() {
        return cfg.socketSendBuffer();
    }

    /**
     * Sets message queue limit for incoming and outgoing messages.
     * <p>
     * When set to positive number send queue is limited to the configured value.
     * {@code 0} disables the size limitations.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_MSG_QUEUE_LIMIT}.
     *
     * @param msgQueueLimit Send queue size limit.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setMessageQueueLimit(int msgQueueLimit) {
        cfg.messageQueueLimit(msgQueueLimit);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Gets message queue limit for incoming and outgoing messages.
     *
     * @return Send queue size limit.
     */
    public int getMessageQueueLimit() {
        return cfg.messageQueueLimit();
    }

    /**
     * See {@link #setSlowClientQueueLimit(int)}.
     *
     * @return Slow client queue limit.
     */
    public int getSlowClientQueueLimit() {
        return cfg.slowClientQueueLimit();
    }

    /** {@inheritDoc} */
    @Override public void failureDetectionTimeoutEnabled(boolean enabled) {
        super.failureDetectionTimeoutEnabled(enabled);

        cfg.failureDetectionTimeoutEnabled(enabled);
    }

    /** {@inheritDoc} */
    @Override public boolean failureDetectionTimeoutEnabled() {
        final boolean spiVal = super.failureDetectionTimeoutEnabled();
        final boolean cfgVal = cfg.failureDetectionTimeoutEnabled();

        assert spiVal == cfgVal : "Inconsistent value [spi=" + spiVal + ", cfg=" + cfgVal + "]";

        return spiVal;
    }

    /** {@inheritDoc} */
    @Override public long failureDetectionTimeout() {
        final long spiVal = super.failureDetectionTimeout();
        final long cfgVal = cfg.failureDetectionTimeout();

        assert spiVal == cfgVal : "Inconsistent value [spi=" + spiVal + ", cfg=" + cfgVal + "]";

        return spiVal;
    }

    /**
     * @return Force client to server connections flag.
     *
     * @see #setForceClientToServerConnections(boolean)
     */
    @IgniteExperimental
    public boolean forceClientToServerConnections() {
        return cfg.forceClientToSrvConnections();
    }

    /**
     * Applicable for clients only. Sets PSI in the mode when server node cannot open TCP connection to the current
     * node. Possile reasons for that may be specific network configurations or security rules.
     * In this mode, when server needs the connection with client, it uses {@link DiscoverySpi} protocol to notify
     * client about it. After that client opens the required connection from its side.
     */
    @IgniteExperimental
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setForceClientToServerConnections(boolean forceClientToSrvConnections) {
        cfg.forceClientToSrvConnections(forceClientToSrvConnections);

        return (TcpCommunicationSpi) this;
    }

    /**
     * Sets slow client queue limit.
     * <p/>
     * When set to a positive number, communication SPI will monitor clients outbound message queue sizes and will drop
     * those clients whose queue exceeded this limit.
     * <p/>
     * This value should be set to less or equal value than {@link #getMessageQueueLimit()} which controls
     * message back-pressure for server nodes. The default value for this parameter is {@code 0}
     * which means {@code unlimited}.
     *
     * @param slowClientQueueLimit Slow client queue limit.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSlowClientQueueLimit(int slowClientQueueLimit) {
        cfg.slowClientQueueLimit(slowClientQueueLimit);

        return (TcpCommunicationSpi) this;
    }

    /**
     * @return Bound TCP server port.
     */
    public int boundPort() {
        return cfg.boundTcpPort();
    }

    /** {@inheritDoc} */
    @Override public TcpCommunicationSpi setName(String name) {
        super.setName(name);

        return (TcpCommunicationSpi) this;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        initFailureDetectionTimeout();

        if (Boolean.TRUE.equals(ignite.configuration().isClientMode()))
            assertParameter(cfg.localPort() > 1023 || cfg.localPort() == -1, "localPort > 1023 || localPort == -1");
        else
            assertParameter(cfg.localPort() > 1023, "localPort > 1023");

        assertParameter(cfg.localPort() <= 0xffff, "locPort < 0xffff");
        assertParameter(cfg.localPortRange() >= 0, "locPortRange >= 0");
        assertParameter(cfg.idleConnectionTimeout() > 0, "idleConnTimeout > 0");
        assertParameter(cfg.socketReceiveBuffer() >= 0, "sockRcvBuf >= 0");
        assertParameter(cfg.socketSendBuffer() >= 0, "sockSndBuf >= 0");
        assertParameter(cfg.messageQueueLimit() >= 0, "msgQueueLimit >= 0");
        assertParameter(cfg.shmemPort() > 0 || cfg.shmemPort() == -1, "shmemPort > 0 || shmemPort == -1");
        assertParameter(cfg.selectorsCount() > 0, "selectorsCnt > 0");
        assertParameter(cfg.connectionsPerNode() > 0, "connectionsPerNode > 0");
        assertParameter(cfg.connectionsPerNode() <= MAX_CONN_PER_NODE, "connectionsPerNode <= 1024");

        if (!failureDetectionTimeoutEnabled()) {
            assertParameter(cfg.reconCount() > 0, "reconnectCnt > 0");
            assertParameter(cfg.connectionTimeout() >= 0, "connTimeout >= 0");
            assertParameter(cfg.maxConnectionTimeout() >= cfg.connectionTimeout(), "maxConnTimeout >= connTimeout");
        }

        assertParameter(cfg.socketWriteTimeout() >= 0, "sockWriteTimeout >= 0");
        assertParameter(cfg.ackSendThreshold() > 0, "ackSndThreshold > 0");
        assertParameter(cfg.unackedMsgsBufferSize() >= 0, "unackedMsgsBufSize >= 0");

        if (cfg.unackedMsgsBufferSize() > 0) {
            assertParameter(cfg.unackedMsgsBufferSize() >= cfg.messageQueueLimit() * 5,
                "Specified 'unackedMsgsBufSize' is too low, it should be at least 'msgQueueLimit * 5'.");

            assertParameter(cfg.unackedMsgsBufferSize() >= cfg.ackSendThreshold() * 5,
                "Specified 'unackedMsgsBufSize' is too low, it should be at least 'ackSndThreshold * 5'.");
        }

        // Set local node attributes.
        try {
            IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(cfg.localHost());

            if (cfg.localPort() != -1 && addrs.get1().isEmpty() && addrs.get2().isEmpty())
                throw new IgniteCheckedException("No network addresses found (is networking enabled?).");

            Collection<InetSocketAddress> extAddrs = cfg.addrRslvr() == null ? null :
                U.resolveAddresses(cfg.addrRslvr(), F.flat(Arrays.asList(addrs.get1(), addrs.get2())), cfg.boundTcpPort());

            Map<String, Object> res = new HashMap<>(5);

            boolean setEmptyHostNamesAttr = !getBoolean(IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES, false) &&
                (!F.isEmpty(cfg.localAddress()) && cfg.localHost().getHostAddress().equals(cfg.localAddress())) &&
                !cfg.localHost().isAnyLocalAddress() &&
                !cfg.localHost().isLoopbackAddress();

            res.put(createSpiAttributeName(ATTR_ADDRS), addrs.get1());
            res.put(createSpiAttributeName(ATTR_HOST_NAMES), setEmptyHostNamesAttr ? emptyList() : addrs.get2());
            res.put(createSpiAttributeName(ATTR_PORT), cfg.boundTcpPort() == -1 ? DISABLED_CLIENT_PORT : cfg.boundTcpPort());
            res.put(createSpiAttributeName(ATTR_SHMEM_PORT), cfg.boundTcpShmemPort() >= 0 ? cfg.boundTcpShmemPort() : null);
            res.put(createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);
            res.put(createSpiAttributeName(ATTR_PAIRED_CONN), cfg.usePairedConnections());
            res.put(createSpiAttributeName(ATTR_FORCE_CLIENT_SERVER_CONNECTIONS), cfg.forceClientToSrvConnections());

            return res;
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to resolve local host to addresses: " + cfg.localHost(), e);
        }
    }

    /**
     * Creates new shared memory communication server.
     *
     * @return Server.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected IpcSharedMemoryServerEndpoint resetShmemServer() throws IgniteCheckedException {
        if (cfg.boundTcpShmemPort() >= 0)
            throw new IgniteCheckedException("Shared memory server was already created on port " + cfg.boundTcpShmemPort());

        if (cfg.shmemPort() == -1 || U.isWindows())
            return null;

        IgniteCheckedException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        for (int port = cfg.shmemPort(); port < cfg.shmemPort() + cfg.localPortRange(); port++) {
            try {
                IgniteConfiguration icfg = ignite.configuration();

                IpcSharedMemoryServerEndpoint srv =
                    new IpcSharedMemoryServerEndpoint(log, icfg.getNodeId(), igniteInstanceName, icfg.getWorkDirectory());

                srv.setPort(port);

                srv.omitOutOfResourcesWarning(true);

                srv.start();

                cfg.boundTcpShmemPort(port);

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled())
                    log.info("Successfully bound shared memory communication to TCP port [port=" + cfg.boundTcpShmemPort() +
                        ", locHost=" + cfg.localHost() + ']');

                return srv;
            }
            catch (IgniteCheckedException e) {
                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + cfg.localHost() + ']');
            }
        }

        // If free port wasn't found.
        throw new IgniteCheckedException("Failed to bind shared memory communication to any port within range [startPort=" +
            cfg.localPort() + ", portRange=" + cfg.localPortRange() + ", locHost=" + cfg.localHost() + ']', lastEx);
    }
}
