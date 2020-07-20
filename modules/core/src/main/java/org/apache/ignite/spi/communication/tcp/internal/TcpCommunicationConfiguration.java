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

import java.io.Serializable;
import java.net.InetAddress;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_ACK_SND_THRESHOLD;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_CONN_PER_NODE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_CONN_TIMEOUT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_FILTER_REACHABLE_ADDRESSES;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_IDLE_CONN_TIMEOUT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_MAX_CONN_TIMEOUT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_MSG_QUEUE_LIMIT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_RECONNECT_CNT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_SELECTORS_CNT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_SHMEM_PORT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_SOCK_BUF_SIZE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_SOCK_WRITE_TIMEOUT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_TCP_NODELAY;

/**
 * Class of configuration for {@link TcpCommunicationSpi} segregation.
 */
public class TcpCommunicationConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 5471893193030200809L;

    /** Address resolver. */
    private AddressResolver addrRslvr;

    /** Local IP address. */
    private String locAddr;

    /** Local port which node uses. */
    private int locPort = DFLT_PORT;

    /** Local port range. */
    private int locPortRange = DFLT_PORT_RANGE;

    /** Local port which node uses to accept shared memory connections. */
    private int shmemPort = DFLT_SHMEM_PORT;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf = true;

    /** Allocate direct buffer or heap buffer. */
    private boolean directSndBuf;

    /** Idle connection timeout. */
    private long idleConnTimeout = DFLT_IDLE_CONN_TIMEOUT;

    /** Connect timeout. */
    private long connTimeout = DFLT_CONN_TIMEOUT;

    /** Maximum connect timeout. */
    private long maxConnTimeout = DFLT_MAX_CONN_TIMEOUT;

    /** Reconnect attempts count. */
    private int reconCnt = DFLT_RECONNECT_CNT;

    /** Socket send buffer. */
    private int sockSndBuf = DFLT_SOCK_BUF_SIZE;

    /** Socket receive buffer. */
    private int sockRcvBuf = DFLT_SOCK_BUF_SIZE;

    /** Message queue limit. */
    private int msgQueueLimit = DFLT_MSG_QUEUE_LIMIT;

    /** Use paired connections. */
    private boolean usePairedConnections;

    /** Connections per node. */
    private int connectionsPerNode = DFLT_CONN_PER_NODE;

    /** {@code TCP_NODELAY} option value for created sockets. */
    private boolean tcpNoDelay = DFLT_TCP_NODELAY;

    /** {@code FILTER_REACHABLE_ADDRESSES} option value for created sockets. */
    private boolean filterReachableAddrs = DFLT_FILTER_REACHABLE_ADDRESSES;

    /** Number of received messages after which acknowledgment is sent. */
    private int ackSndThreshold = DFLT_ACK_SND_THRESHOLD;

    /** Maximum number of unacknowledged messages. */
    private int unackedMsgsBufSize;

    /** Socket write timeout. */
    private long sockWriteTimeout = DFLT_SOCK_WRITE_TIMEOUT;

    /** Bound port. */
    private int boundTcpPort = -1;

    /** Bound port for shared memory server. */
    private int boundTcpShmemPort = -1;

    /** Count of selectors to use in TCP server. */
    private int selectorsCnt = DFLT_SELECTORS_CNT;

    /** Complex variable that represents this node IP address. */
    private volatile InetAddress locHost;

    /** Slow client queue limit. */
    private int slowClientQueueLimit;

    /** Failure detection timeout usage switch. */
    private boolean failureDetectionTimeoutEnabled = true;

    /**
     * Failure detection timeout. Initialized with the value of {@link IgniteConfiguration#getFailureDetectionTimeout()}.
     */
    private long failureDetectionTimeout;

    /**
     * Defines how many non-blocking {@code selector.selectNow()} should be made before falling into {@code
     * selector.select(long)} in NIO server. Long value. Default is {@code 0}. Can be set to {@code Long.MAX_VALUE} so
     * selector threads will never block.
     */
    private long selectorSpins = IgniteSystemProperties.getLong("IGNITE_SELECTOR_SPINS", 0L);

    /**
     *
     */
    private boolean forceClientToSrvConnections;

    /** Address resolver. */
    public AddressResolver addrRslvr() {
        return addrRslvr;
    }

    /**
     * @param rslvr Resolver.
     */
    public void addrRslvr(AddressResolver rslvr) {
        addrRslvr = rslvr;
    }

    /**
     * @return Local IP address.
     */
    public String localAddress() {
        return locAddr;
    }

    /**
     * @param locAddr New local IP address.
     */
    public void localAddress(String locAddr) {
        this.locAddr = locAddr;
    }

    /**
     * @return Local port which node uses.
     */
    public int localPort() {
        return locPort;
    }

    /**
     * @param locPort New local port which node uses.
     */
    public void localPort(int locPort) {
        this.locPort = locPort;
    }

    /**
     * @return Local port range.
     */
    public int localPortRange() {
        return locPortRange;
    }

    /**
     * @param locPortRange New local port range.
     */
    public void localPortRange(int locPortRange) {
        this.locPortRange = locPortRange;
    }

    /**
     * @return Local port which node uses to accept shared memory connections.
     */
    public int shmemPort() {
        return shmemPort;
    }

    /**
     * @param shmemPort New local port which node uses to accept shared memory connections.
     */
    public void shmemPort(int shmemPort) {
        this.shmemPort = shmemPort;
    }

    /**
     * @return Allocate direct buffer or heap buffer.
     */
    public boolean directBuffer() {
        return directBuf;
    }

    /**
     * @param directBuf New allocate direct buffer or heap buffer.
     */
    public void directBuffer(boolean directBuf) {
        this.directBuf = directBuf;
    }

    /**
     * @return Allocate direct buffer or heap buffer.
     */
    public boolean directSendBuffer() {
        return directSndBuf;
    }

    /**
     * @param directSndBuf New allocate direct buffer or heap buffer.
     */
    public void directSendBuffer(boolean directSndBuf) {
        this.directSndBuf = directSndBuf;
    }

    /**
     * @return Idle connection timeout.
     */
    public long idleConnectionTimeout() {
        return idleConnTimeout;
    }

    /**
     * @param idleConnTimeout New idle connection timeout.
     */
    public void idleConnectionTimeout(long idleConnTimeout) {
        this.idleConnTimeout = idleConnTimeout;
    }

    /**
     * @return Connect timeout.
     */
    public long connectionTimeout() {
        return connTimeout;
    }

    /**
     * @param connTimeout New connect timeout.
     */
    public void connectionTimeout(long connTimeout) {
        this.connTimeout = connTimeout;
    }

    /**
     * @return Maximum connect timeout.
     */
    public long maxConnectionTimeout() {
        return maxConnTimeout;
    }

    /**
     * @param maxConnTimeout New maximum connect timeout.
     */
    public void maxConnectionTimeout(long maxConnTimeout) {
        this.maxConnTimeout = maxConnTimeout;
    }

    /**
     * @return Reconnect attempts count.
     */
    public int reconCount() {
        return reconCnt;
    }

    /**
     * @param reconCnt New reconnect attempts count.
     */
    public void reconCount(int reconCnt) {
        this.reconCnt = reconCnt;
    }

    /**
     * @return Socket send buffer.
     */
    public int socketSendBuffer() {
        return sockSndBuf;
    }

    /**
     * @param sockSndBuf New socket send buffer.
     */
    public void socketSendBuffer(int sockSndBuf) {
        this.sockSndBuf = sockSndBuf;
    }

    /**
     * @return Socket receive buffer.
     */
    public int socketReceiveBuffer() {
        return sockRcvBuf;
    }

    /**
     * @param sockRcvBuf New socket receive buffer.
     */
    public void socketReceiveBuffer(int sockRcvBuf) {
        this.sockRcvBuf = sockRcvBuf;
    }

    /**
     * @return Message queue limit.
     */
    public int messageQueueLimit() {
        return msgQueueLimit;
    }

    /**
     * @param msgQueueLimit New message queue limit.
     */
    public void messageQueueLimit(int msgQueueLimit) {
        this.msgQueueLimit = msgQueueLimit;
    }

    /**
     * @return Use paired connections.
     */
    public boolean usePairedConnections() {
        return usePairedConnections;
    }

    /**
     * @param usePairedConnections New use paired connections.
     */
    public void usePairedConnections(boolean usePairedConnections) {
        this.usePairedConnections = usePairedConnections;
    }

    /**
     * @return Connections per node.
     */
    public int connectionsPerNode() {
        return connectionsPerNode;
    }

    /**
     * @param connectionsPerNode New connections per node.
     */
    public void connectionsPerNode(int connectionsPerNode) {
        this.connectionsPerNode = connectionsPerNode;
    }

    /**
     * @return Option value for created sockets.
     */
    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @param tcpNoDelay New option value for created sockets.
     */
    public void tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * @return Option value for created sockets.
     */
    public boolean filterReachableAddresses() {
        return filterReachableAddrs;
    }

    /**
     * @param filterReachableAddrs New option value for created sockets.
     */
    public void filterReachableAddresses(boolean filterReachableAddrs) {
        this.filterReachableAddrs = filterReachableAddrs;
    }

    /**
     * @return Number of received messages after which acknowledgment is sent.
     */
    public int ackSendThreshold() {
        return ackSndThreshold;
    }

    /**
     * @param ackSndThreshold New number of received messages after which acknowledgment is sent.
     */
    public void ackSendThreshold(int ackSndThreshold) {
        this.ackSndThreshold = ackSndThreshold;
    }

    /**
     * @return Maximum number of unacknowledged messages.
     */
    public int unackedMsgsBufferSize() {
        return unackedMsgsBufSize;
    }

    /**
     * @param unackedMsgsBufSize New maximum number of unacknowledged messages.
     */
    public void unackedMsgsBufferSize(int unackedMsgsBufSize) {
        this.unackedMsgsBufSize = unackedMsgsBufSize;
    }

    /**
     * @return Socket write timeout.
     */
    public long socketWriteTimeout() {
        return sockWriteTimeout;
    }

    /**
     * @param sockWriteTimeout New socket write timeout.
     */
    public void socketWriteTimeout(long sockWriteTimeout) {
        this.sockWriteTimeout = sockWriteTimeout;
    }

    /**
     * @return Bound port.
     */
    public int boundTcpPort() {
        return boundTcpPort;
    }

    /**
     * @param boundTcpPort New bound port.
     */
    public void boundTcpPort(int boundTcpPort) {
        this.boundTcpPort = boundTcpPort;
    }

    /**
     * @return Bound port for shared memory server.
     */
    public int boundTcpShmemPort() {
        return boundTcpShmemPort;
    }

    /**
     * @param boundTcpShmemPort New bound port for shared memory server.
     */
    public void boundTcpShmemPort(int boundTcpShmemPort) {
        this.boundTcpShmemPort = boundTcpShmemPort;
    }

    /**
     * @return Count of selectors to use in TCP server.
     */
    public int selectorsCount() {
        return selectorsCnt;
    }

    /**
     * @param selectorsCnt New count of selectors to use in TCP server.
     */
    public void selectorsCount(int selectorsCnt) {
        this.selectorsCnt = selectorsCnt;
    }

    /**
     * @return Complex variable that represents this node IP address.
     */
    public InetAddress localHost() {
        return locHost;
    }

    /**
     * @param locHost New complex variable that represents this node IP address.
     */
    public void localHost(InetAddress locHost) {
        this.locHost = locHost;
    }

    /**
     * @return Defines how many non-blocking  should be made before falling into  in NIO server. Long value. Default is
     * . Can be set to  so selector threads will never block.
     */
    public long selectorSpins() {
        return selectorSpins;
    }

    /**
     * @param selectorSpins New defines how many non-blocking  should be made before falling into  in NIO server. Long
     * value. Default is . Can be set to  so selector threads will never block.
     */
    public void selectorSpins(long selectorSpins) {
        this.selectorSpins = selectorSpins;
    }

    /**
     * @return Slow client queue limit.
     */
    public int slowClientQueueLimit() {
        return slowClientQueueLimit;
    }

    /**
     * @param slowClientQueueLimit New slow client queue limit.
     */
    public void slowClientQueueLimit(int slowClientQueueLimit) {
        this.slowClientQueueLimit = slowClientQueueLimit;
    }

    /**
     * @return Failure detection timeout usage switch.
     */
    public boolean failureDetectionTimeoutEnabled() {
        return failureDetectionTimeoutEnabled;
    }

    /**
     * @param failureDetectionTimeoutEnabled New failure detection timeout usage switch.
     */
    public void failureDetectionTimeoutEnabled(boolean failureDetectionTimeoutEnabled) {
        this.failureDetectionTimeoutEnabled = failureDetectionTimeoutEnabled;
    }

    /**
     * Returns failure detection timeout used by {@link TcpDiscoverySpi} and {@link TcpCommunicationSpi}.
     * <p>
     * Default is {@link IgniteConfiguration#DFLT_FAILURE_DETECTION_TIMEOUT}.
     *
     * @return Failure detection timeout in milliseconds.
     * @see IgniteConfiguration#setFailureDetectionTimeout(long)
     */
    public long failureDetectionTimeout() {
        return failureDetectionTimeout;
    }

    /**
     * @param failureDetectionTimeout New failure detection timeout. Initialized with the value of .
     */
    public void failureDetectionTimeout(long failureDetectionTimeout) {
        this.failureDetectionTimeout = failureDetectionTimeout;
        if (this.failureDetectionTimeoutEnabled)
            this.failureDetectionTimeout = failureDetectionTimeout;
    }

    /**
     * @return Force client to server connections flag.
     *
     * @see #forceClientToSrvConnections(boolean)
     */
    public boolean forceClientToSrvConnections() {
        return forceClientToSrvConnections;
    }

    /**
     * Applicable for clients only. Sets PSI in the mode when server node cannot open TCP connection to the current
     * node. Possile reasons for that may be specific network configurations or security rules.
     * In this mode, when server needs the connection with client, it uses {@link DiscoverySpi} protocol to notify
     * client about it. After that client opens the required connection from its side.
     */
    public void forceClientToSrvConnections(boolean forceClientToSrvConnections) {
        this.forceClientToSrvConnections = forceClientToSrvConnections;
    }
}
