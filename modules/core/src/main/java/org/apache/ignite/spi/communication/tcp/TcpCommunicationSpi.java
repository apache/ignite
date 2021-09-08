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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioMessageReaderFactory;
import org.apache.ignite.internal.util.nio.GridNioMessageWriterFactory;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.ClusterStateProvider;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationDiscoveryEventListener;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationListenerEx;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationWorker;
import org.apache.ignite.spi.communication.tcp.internal.ConnectGateway;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.internal.FirstConnectionPolicy;
import org.apache.ignite.spi.communication.tcp.internal.GridNioServerWrapper;
import org.apache.ignite.spi.communication.tcp.internal.InboundConnectionHandler;
import org.apache.ignite.spi.communication.tcp.internal.NodeUnreachableException;
import org.apache.ignite.spi.communication.tcp.internal.RoundRobinConnectionPolicy;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConfigInitializer;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConnectionCheckFuture;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationSpiMBeanImpl;
import org.apache.ignite.spi.communication.tcp.internal.TcpConnectionIndexAwareMessage;
import org.apache.ignite.spi.communication.tcp.internal.TcpHandshakeExecutor;
import org.apache.ignite.spi.communication.tcp.internal.shmem.ShmemAcceptWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.NOOP;
import static org.apache.ignite.spi.communication.tcp.internal.TcpConnectionIndexAwareMessage.UNDEFINED_CONNECTION_INDEX;

/**
 * <tt>TcpCommunicationSpi</tt> is default communication SPI which uses
 * TCP/IP protocol and Java NIO to communicate with other nodes.
 * <p>
 * To enable communication with other nodes, this SPI adds {@link #ATTR_ADDRS} and {@link #ATTR_PORT} local node
 * attributes (see {@link ClusterNode#attributes()}.
 * <p>
 * At startup, this SPI tries to start listening to local port specified by {@link #setLocalPort(int)} method. If local
 * port is occupied, then SPI will automatically increment the port number until it can successfully bind for listening.
 * {@link #setLocalPortRange(int)} configuration parameter controls maximum number of ports that SPI will try before it
 * fails. Port range comes very handy when starting multiple grid nodes on the same machine or even in the same VM. In
 * this case all nodes can be brought up without a single change in configuration.
 * <p>
 * This SPI caches connections to remote nodes so it does not have to reconnect every time a message is sent. By
 * default, idle connections are kept active for {@link #DFLT_IDLE_CONN_TIMEOUT} period and then are closed. Use {@link
 * #setIdleConnectionTimeout(long)} configuration parameter to configure you own idle connection timeout.
 * <h1 class="header">Failure Detection</h1>
 * Configuration defaults (see Configuration section below and {@link IgniteConfiguration#getFailureDetectionTimeout()})
 * for details) are chosen to make possible for communication SPI work reliably on most of hardware and virtual
 * deployments, but this has made failure detection time worse.
 * <p>
 * If it's needed to tune failure detection then it's highly recommended to do this using {@link
 * IgniteConfiguration#setFailureDetectionTimeout(long)}. This failure timeout automatically controls the following
 * parameters: {@link #getConnectTimeout()}, {@link #getMaxConnectTimeout()}, {@link #getReconnectCount()}. If any of
 * those parameters is set explicitly, then the failure timeout setting will be ignored.
 * <p>
 * If it's required to perform advanced settings of failure detection and {@link IgniteConfiguration#getFailureDetectionTimeout()}
 * is unsuitable then various {@code TcpCommunicationSpi} configuration parameters may be used.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>Address resolver (see {@link #setAddressResolver(AddressResolver)}</li>
 * <li>Node local IP address (see {@link #setLocalAddress(String)})</li>
 * <li>Node local port number (see {@link #setLocalPort(int)})</li>
 * <li>Local port range (see {@link #setLocalPortRange(int)}</li>
 * <li>Use paired connections (see {@link #setUsePairedConnections(boolean)}</li>
 * <li>Connections per node (see {@link #setConnectionsPerNode(int)})</li>
 * <li>Shared memory port (see {@link #setSharedMemoryPort(int)}</li>
 * <li>Idle connection timeout (see {@link #setIdleConnectionTimeout(long)})</li>
 * <li>Direct or heap buffer allocation (see {@link #setDirectBuffer(boolean)})</li>
 * <li>Direct or heap buffer allocation for sending (see {@link #setDirectSendBuffer(boolean)})</li>
 * <li>Count of selectors and selector threads for NIO server (see {@link #setSelectorsCount(int)})</li>
 * <li>Selector thread busy-loop iterations (see {@link #setSelectorSpins(long)}</li>
 * <li>{@code TCP_NODELAY} socket option for sockets (see {@link #setTcpNoDelay(boolean)})</li>
 * <li>Filter reachable addresses (see {@link #setFilterReachableAddresses(boolean)} </li>
 * <li>Message queue limit (see {@link #setMessageQueueLimit(int)})</li>
 * <li>Slow client queue limit (see {@link #setSlowClientQueueLimit(int)})</li>
 * <li>Connect timeout (see {@link #setConnectTimeout(long)})</li>
 * <li>Maximum connect timeout (see {@link #setMaxConnectTimeout(long)})</li>
 * <li>Reconnect attempts count (see {@link #setReconnectCount(int)})</li>
 * <li>Socket receive buffer size (see {@link #setSocketReceiveBuffer(int)})</li>
 * <li>Socket send buffer size (see {@link #setSocketSendBuffer(int)})</li>
 * <li>Socket write timeout (see {@link #setSocketWriteTimeout(long)})</li>
 * <li>Number of received messages after which acknowledgment is sent (see {@link #setAckSendThreshold(int)})</li>
 * <li>Maximum number of unacknowledged messages (see {@link #setUnacknowledgedMessagesBufferSize(int)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * TcpCommunicationSpi is used by default and should be explicitly configured only if some SPI configuration parameters
 * need to be overridden.
 * <pre name="code" class="java">
 * TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
 *
 * // Override local port.
 * commSpi.setLocalPort(4321);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default communication SPI.
 * cfg.setCommunicationSpi(commSpi);
 *
 * // Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * TcpCommunicationSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="communicationSpi"&gt;
 *             &lt;bean class="org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi"&gt;
 *                 &lt;!-- Override local port. --&gt;
 *                 &lt;property name="localPort" value="4321"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 *
 * @see CommunicationSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
@IgniteSpiConsistencyChecked(optional = false)
public class TcpCommunicationSpi extends TcpCommunicationConfigInitializer {
    /** IPC error message. */
    public static final String OUT_OF_RESOURCES_TCP_MSG = "Failed to allocate shared memory segment " +
        "(switching to TCP, may be slower).";

    /** Node attribute that is mapped to node IP addresses (value is <tt>comm.tcp.addrs</tt>). */
    public static final String ATTR_ADDRS = "comm.tcp.addrs";

    /** Node attribute that is mapped to node host names (value is <tt>comm.tcp.host.names</tt>). */
    public static final String ATTR_HOST_NAMES = "comm.tcp.host.names";

    /** Node attribute that is mapped to node port number (value is <tt>comm.tcp.port</tt>). */
    public static final String ATTR_PORT = "comm.tcp.port";

    /** Node attribute that is mapped to node port number (value is <tt>comm.shmem.tcp.port</tt>). */
    public static final String ATTR_SHMEM_PORT = "comm.shmem.tcp.port";

    /** Node attribute that is mapped to node's external addresses (value is <tt>comm.tcp.ext-addrs</tt>). */
    public static final String ATTR_EXT_ADDRS = "comm.tcp.ext-addrs";

    /** Attr paired connection. */
    public static final String ATTR_PAIRED_CONN = "comm.tcp.pairedConnection";

    /** Default port which node sets listener to (value is <tt>47100</tt>). */
    public static final int DFLT_PORT = 47100;

    /** Default port which node sets listener for shared memory connections (value is <tt>48100</tt>). */
    public static final int DFLT_SHMEM_PORT = -1;

    /** Default idle connection timeout (value is <tt>10</tt>min). */
    public static final long DFLT_IDLE_CONN_TIMEOUT = 10 * 60_000;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Default connection timeout (value is <tt>5000</tt>ms). */
    public static final long DFLT_CONN_TIMEOUT = 5000;

    /** Default Maximum connection timeout (value is <tt>600,000</tt>ms). */
    public static final long DFLT_MAX_CONN_TIMEOUT = 10 * 60 * 1000;

    /** Default reconnect attempts count (value is <tt>10</tt>). */
    public static final int DFLT_RECONNECT_CNT = 10;

    /** Default message queue limit per connection (for incoming and outgoing . */
    public static final int DFLT_MSG_QUEUE_LIMIT = GridNioServer.DFLT_SEND_QUEUE_LIMIT;

    /**
     * Default count of selectors for TCP server equals to {@code "Math.max(4, Runtime.getRuntime().availableProcessors()
     * / 2)"}.
     */
    public static final int DFLT_SELECTORS_CNT = Math.max(4, Runtime.getRuntime().availableProcessors() / 2);

    /** Connection index meta for session. */
    public static final int CONN_IDX_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Node consistent id meta for session. */
    public static final int CONSISTENT_ID_META = GridNioSessionMetaKey.nextUniqueKey();

    /**
     * Default local port range (value is <tt>100</tt>). See {@link #setLocalPortRange(int)} for details.
     */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default value for {@code TCP_NODELAY} socket option (value is <tt>true</tt>). */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default value for {@code FILTER_REACHABLE_ADDRESSES} socket option (value is <tt>false</tt>). */
    public static final boolean DFLT_FILTER_REACHABLE_ADDRESSES = false;

    /** Default received messages threshold for sending ack. */
    public static final int DFLT_ACK_SND_THRESHOLD = 32;

    /** Default socket write timeout. */
    public static final long DFLT_SOCK_WRITE_TIMEOUT = 2000;

    /** Default connections per node. */
    public static final int DFLT_CONN_PER_NODE = 1;

    /** Node ID message type. */
    public static final short NODE_ID_MSG_TYPE = -1;

    /** Recovery last received ID message type. */
    public static final short RECOVERY_LAST_ID_MSG_TYPE = -2;

    /** Handshake message type. */
    public static final short HANDSHAKE_MSG_TYPE = -3;

    /** Handshake wait message type. */
    public static final short HANDSHAKE_WAIT_MSG_TYPE = -28;

    /** Communication metrics group name. */
    public static final String COMMUNICATION_METRICS_GROUP_NAME = MetricUtils.metricName("communication", "tcp");

    /** Sent messages metric name. */
    public static final String SENT_MESSAGES_METRIC_NAME = "sentMessagesCount";

    /** Sent messages metric description. */
    public static final String SENT_MESSAGES_METRIC_DESC = "Total number of messages sent by current node";

    /** Received messages metric name. */
    public static final String RECEIVED_MESSAGES_METRIC_NAME = "receivedMessagesCount";

    /** Received messages metric description. */
    public static final String RECEIVED_MESSAGES_METRIC_DESC = "Total number of messages received by current node";

    /** Sent messages by type metric name. */
    public static final String SENT_MESSAGES_BY_TYPE_METRIC_NAME = "sentMessagesByType";

    /** Sent messages by type metric description. */
    public static final String SENT_MESSAGES_BY_TYPE_METRIC_DESC =
        "Total number of messages with given type sent by current node";

    /** Received messages by type metric name. */
    public static final String RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME = "receivedMessagesByType";

    /** Received messages by type metric description. */
    public static final String RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC =
        "Total number of messages with given type received by current node";

    /** Sent messages by node consistent id metric name. */
    public static final String SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME = "sentMessagesToNode";

    /** Sent messages by node consistent id metric description. */
    public static final String SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC =
        "Total number of messages sent by current node to the given node";

    /** Received messages by node consistent id metric name. */
    public static final String RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME = "receivedMessagesFromNode";

    /** Received messages by node consistent id metric description. */
    public static final String RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC =
        "Total number of messages received by current node from the given node";

    /** Client nodes might have port {@code 0} if they have no server socket opened. */
    public static final Integer DISABLED_CLIENT_PORT = 0;

    /** Connect gate. */
    private final ConnectGateway connectGate = new ConnectGateway();

    /** Context initialization latch. */
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** Shared memory accept worker. */
    private volatile ShmemAcceptWorker shmemAcceptWorker;

    /** Stopping flag (set to {@code true} when SPI gets stopping signal). */
    private volatile boolean stopping;

    /** Incoming message listener. */
    private volatile CommunicationListener<Message> lsnr;

    /** Client pool. */
    private volatile ConnectionClientPool clientPool;

    /** Recovery and idle clients handler. */
    private volatile CommunicationWorker commWorker;

    /** Server listener. */
    private volatile InboundConnectionHandler srvLsnr;

    /** Disco listener. */
    private volatile GridLocalEventListener discoLsnr;

    /** Nio server wrapper. */
    private volatile GridNioServerWrapper nioSrvWrapper;

    /** State provider. */
    private volatile ClusterStateProvider stateProvider;

    /**
     *
     */
    public static final String ATTR_FORCE_CLIENT_SERVER_CONNECTIONS = "comm.force.client.srv.connections";

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Logger. */
    @LoggerResource(categoryName = "org.apache.ignite.internal.diagnostic")
    private IgniteLogger diagnosticLog;

    /**
     * {@inheritDoc} This call should be change after refactoring. It produces dependency hell. Because {@link
     * GridIoManager} set it after self construct.
     */
    @Deprecated
    @Override public void setListener(CommunicationListener<Message> lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * @return Listener.
     */
    public CommunicationListener getListener() {
        return lsnr;
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        // Listener could be not initialized yet, but discovery thread could try to aggregate metrics.
        if (metricsLsnr == null)
            return 0;

        return metricsLsnr.sentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        // Listener could be not initialized yet, but discovery thread could try to aggregate metrics.
        if (metricsLsnr == null)
            return 0;

        return metricsLsnr.sentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        // Listener could be not initialized yet, but discovery thread could try to aggregate metrics.
        if (metricsLsnr == null)
            return 0;

        return metricsLsnr.receivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        // Listener could be not initialized yet, but discovery thread could try to aggregate metrics.
        if (metricsLsnr == null)
            return 0;

        return metricsLsnr.receivedBytesCount();
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> getReceivedMessagesByType() {
        return metricsLsnr.receivedMessagesByType();
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> getReceivedMessagesByNode() {
        return metricsLsnr.receivedMessagesByNode();
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> getSentMessagesByType() {
        return metricsLsnr.sentMessagesByType();
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> getSentMessagesByNode() {
        return metricsLsnr.sentMessagesByNode();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        GridNioServer<Message> srv = nioSrvWrapper.nio();

        return srv != null ? srv.outboundMessagesQueueSize() : 0;
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        metricsLsnr.resetMetrics();
    }

    /**
     * @param consistentId Consistent id of the node.
     * @param nodeId Left node ID.
     */
    void onNodeLeft(Object consistentId, UUID nodeId) {
        assert nodeId != null;

        metricsLsnr.onNodeLeft(consistentId);
        clientPool.onNodeLeft(nodeId);
    }

    /**
     * @param nodeId Target node ID.
     * @return Future.
     */
    public IgniteInternalFuture<String> dumpNodeStatistics(final UUID nodeId) {
        StringBuilder sb = new StringBuilder("Communication SPI statistics [rmtNode=").append(nodeId).append(']').append(U.nl());

        dumpInfo(sb, nodeId);

        GridNioServer<Message> nioSrvr = nioSrvWrapper.nio();

        if (nioSrvr != null) {
            sb.append("NIO sessions statistics:");

            IgnitePredicate<GridNioSession> p = (IgnitePredicate<GridNioSession>)ses -> {
                ConnectionKey connId = ses.meta(CONN_IDX_META);

                return connId != null && nodeId.equals(connId.nodeId());
            };

            return nioSrvr.dumpStats(sb.toString(), p);
        }
        else {
            sb.append(U.nl()).append("GridNioServer is null.");

            return new GridFinishedFuture<>(sb.toString());
        }
    }

    /**
     * Dumps SPI per-connection stats to logs.
     */
    public void dumpStats() {
        final IgniteLogger log = this.diagnosticLog;

        if (log != null) {
            StringBuilder sb = new StringBuilder();

            dumpInfo(sb, null);

            U.warn(log, sb.toString());

            GridNioServer<Message> nioSrvr = nioSrvWrapper.nio();

            if (nioSrvr != null) {
                nioSrvr.dumpStats().listen(fut -> {
                    try {
                        U.warn(log, fut.get());
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to dump NIO server statistics: " + e, e);
                    }
                });
            }
        }
    }

    /**
     * @param sb Message builder.
     * @param dstNodeId Target node ID.
     */
    private void dumpInfo(StringBuilder sb, UUID dstNodeId) {
        sb.append("Communication SPI recovery descriptors: ").append(U.nl());

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> entry : nioSrvWrapper.recoveryDescs().entrySet()) {
            GridNioRecoveryDescriptor desc = entry.getValue();

            if (dstNodeId != null && !dstNodeId.equals(entry.getKey().nodeId()))
                continue;

            sb.append("    [key=").append(entry.getKey())
                .append(", msgsSent=").append(desc.sent())
                .append(", msgsAckedByRmt=").append(desc.acked())
                .append(", msgsRcvd=").append(desc.received())
                .append(", lastAcked=").append(desc.lastAcknowledged())
                .append(", reserveCnt=").append(desc.reserveCount())
                .append(", descIdHash=").append(System.identityHashCode(desc))
                .append(']').append(U.nl());
        }

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> entry : nioSrvWrapper.outRecDescs().entrySet()) {
            GridNioRecoveryDescriptor desc = entry.getValue();

            if (dstNodeId != null && !dstNodeId.equals(entry.getKey().nodeId()))
                continue;

            sb.append("    [key=").append(entry.getKey())
                .append(", msgsSent=").append(desc.sent())
                .append(", msgsAckedByRmt=").append(desc.acked())
                .append(", reserveCnt=").append(desc.reserveCount())
                .append(", connected=").append(desc.connected())
                .append(", reserved=").append(desc.reserved())
                .append(", descIdHash=").append(System.identityHashCode(desc))
                .append(']').append(U.nl());
        }

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> entry : nioSrvWrapper.inRecDescs().entrySet()) {
            GridNioRecoveryDescriptor desc = entry.getValue();

            if (dstNodeId != null && !dstNodeId.equals(entry.getKey().nodeId()))
                continue;

            sb.append("    [key=").append(entry.getKey())
                .append(", msgsRcvd=").append(desc.received())
                .append(", lastAcked=").append(desc.lastAcknowledged())
                .append(", reserveCnt=").append(desc.reserveCount())
                .append(", connected=").append(desc.connected())
                .append(", reserved=").append(desc.reserved())
                .append(", handshakeIdx=").append(desc.handshakeIndex())
                .append(", descIdHash=").append(System.identityHashCode(desc))
                .append(']').append(U.nl());
        }

        sb.append("Communication SPI clients: ").append(U.nl());

        for (Map.Entry<UUID, GridCommunicationClient[]> entry : clientPool.entrySet()) {
            UUID clientNodeId = entry.getKey();

            if (dstNodeId != null && !dstNodeId.equals(clientNodeId))
                continue;

            GridCommunicationClient[] clients0 = entry.getValue();

            for (GridCommunicationClient client : clients0) {
                if (client != null) {
                    sb.append("    [node=").append(clientNodeId)
                        .append(", client=").append(client)
                        .append(']').append(U.nl());
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        final Function<UUID, ClusterNode> nodeGetter = (nodeId) -> getSpiContext().node(nodeId);
        final Supplier<ClusterNode> locNodeSupplier = () -> getSpiContext().localNode();
        final Supplier<Ignite> igniteExSupplier = this::ignite;
        final Function<UUID, Boolean> pingNode = (nodeId) -> getSpiContext().pingNode(nodeId);
        final Supplier<FailureProcessor> failureProcessorSupplier =
            () -> ignite instanceof IgniteEx ? ((IgniteEx)ignite).context().failure() : null;
        final Supplier<Boolean> isStopped = () -> getSpiContext().isStopping();

        this.igniteInstanceName = igniteInstanceName;

        cfg.failureDetectionTimeout(ignite.configuration().getFailureDetectionTimeout());

        attributeNames = new AttributeNames(
            createSpiAttributeName(ATTR_PAIRED_CONN),
            createSpiAttributeName(ATTR_SHMEM_PORT),
            createSpiAttributeName(ATTR_ADDRS),
            createSpiAttributeName(ATTR_HOST_NAMES),
            createSpiAttributeName(ATTR_EXT_ADDRS),
            createSpiAttributeName(ATTR_PORT),
            createSpiAttributeName(ATTR_FORCE_CLIENT_SERVER_CONNECTIONS));

        boolean client = Boolean.TRUE.equals(ignite().configuration().isClientMode());

        this.stateProvider = new ClusterStateProvider(
            ignite,
            locNodeSupplier,
            this,
            isStopped,
            () -> super.getSpiContext(),
            log,
            igniteExSupplier
        );

        try {
            cfg.localHost(U.resolveLocalHost(cfg.localAddress()));
        }
        catch (IOException e) {
            throw new IgniteSpiException("Failed to initialize local address: " + cfg.localAddress(), e);
        }

        if (cfg.connectionsPerNode() > 1)
            connPlc = new RoundRobinConnectionPolicy(cfg);
        else
            connPlc = new FirstConnectionPolicy();

        this.srvLsnr = resolve(ignite, new InboundConnectionHandler(
            log,
            cfg,
            nodeGetter,
            locNodeSupplier,
            stateProvider,
            clientPool,
            commWorker,
            connectGate,
            failureProcessorSupplier,
            attributeNames,
            metricsLsnr,
            nioSrvWrapper,
            ctxInitLatch,
            client,
            igniteExSupplier,
            new CommunicationListener<Message>() {
                @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
                    notifyListener(nodeId, msg, msgC);
                }

                @Override public void onDisconnected(UUID nodeId) {
                    if (lsnr != null)
                        lsnr.onDisconnected(nodeId);
                }
            }
        ));

        TcpHandshakeExecutor tcpHandshakeExecutor = resolve(ignite, new TcpHandshakeExecutor(
            log,
            stateProvider,
            cfg.directBuffer()
        ));

        this.nioSrvWrapper = resolve(ignite, new GridNioServerWrapper(
            log,
            cfg,
            attributeNames,
            tracing,
            nodeGetter,
            locNodeSupplier,
            connectGate,
            stateProvider,
            this::getExceptionRegistry,
            commWorker,
            ignite.configuration(),
            this.srvLsnr,
            getName(),
            getWorkersRegistry(ignite),
            ignite instanceof IgniteEx ? ((IgniteEx)ignite).context().metric() : null,
            this::createTcpClient,
            new CommunicationListenerEx<Message>() {
                @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
                    notifyListener(nodeId, msg, msgC);
                }

                @Override public void onDisconnected(UUID nodeId) {
                    if (lsnr != null)
                        lsnr.onDisconnected(nodeId);
                }

                @Override public void onChannelOpened(UUID rmtNodeId, Message initMsg, Channel channel) {
                    if (lsnr instanceof CommunicationListenerEx)
                        ((CommunicationListenerEx<Message>)lsnr).onChannelOpened(rmtNodeId, initMsg, channel);
                }
            },
            tcpHandshakeExecutor
        ));

        this.srvLsnr.setNioSrvWrapper(nioSrvWrapper);

        this.clientPool = resolve(ignite, new ConnectionClientPool(
            cfg,
            attributeNames,
            log,
            metricsLsnr,
            locNodeSupplier,
            nodeGetter,
            null,
            getWorkersRegistry(ignite),
            this,
            stateProvider,
            nioSrvWrapper,
            getName()
        ));

        this.srvLsnr.setClientPool(clientPool);

        nioSrvWrapper.clientPool(clientPool);

        discoLsnr = new CommunicationDiscoveryEventListener(clientPool, metricsLsnr);

        try {
            shmemSrv = resetShmemServer();
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to start shared memory communication server.", e);
        }

        try {
            // This method potentially resets local port to the value
            // local node was bound to.
            nioSrvWrapper.nio(nioSrvWrapper.resetNioServer());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to initialize TCP server: " + cfg.localHost(), e);
        }

        boolean forceClientToSrvConnections = forceClientToServerConnections() || cfg.localPort() == -1;

        if (cfg.usePairedConnections() && forceClientToSrvConnections) {
            throw new IgniteSpiException("Node using paired connections " +
                "is not allowed to start in forced client to server connections mode.");
        }

        assert cfg.localHost() != null;

        // Start SPI start stopwatch.
        startStopwatch();

        if (log.isDebugEnabled()) {
            log.debug(configInfo("locAddr", cfg.localAddress()));
            log.debug(configInfo("locPort", cfg.localPort()));
            log.debug(configInfo("locPortRange", cfg.localPortRange()));
            log.debug(configInfo("idleConnTimeout", cfg.idleConnectionTimeout()));
            log.debug(configInfo("directBuf", cfg.directBuffer()));
            log.debug(configInfo("directSendBuf", cfg.directSendBuffer()));
            log.debug(configInfo("selectorsCnt", cfg.selectorsCount()));
            log.debug(configInfo("tcpNoDelay", cfg.tcpNoDelay()));
            log.debug(configInfo("sockSndBuf", cfg.socketSendBuffer()));
            log.debug(configInfo("sockRcvBuf", cfg.socketReceiveBuffer()));
            log.debug(configInfo("shmemPort", cfg.shmemPort()));
            log.debug(configInfo("msgQueueLimit", cfg.messageQueueLimit()));
            log.debug(configInfo("connectionsPerNode", cfg.connectionsPerNode()));

            if (failureDetectionTimeoutEnabled()) {
                log.debug(configInfo("connTimeout", cfg.connectionTimeout()));
                log.debug(configInfo("maxConnTimeout", cfg.maxConnectionTimeout()));
                log.debug(configInfo("reconCnt", cfg.reconCount()));
            }
            else
                log.debug(configInfo("failureDetectionTimeout", failureDetectionTimeout()));

            log.debug(configInfo("sockWriteTimeout", cfg.socketWriteTimeout()));
            log.debug(configInfo("ackSndThreshold", cfg.ackSendThreshold()));
            log.debug(configInfo("unackedMsgsBufSize", cfg.unackedMsgsBufferSize()));
        }

        if (!cfg.tcpNoDelay())
            U.quietAndWarn(log, "'TCP_NO_DELAY' for communication is off, which should be used with caution " +
                "since may produce significant delays with some scenarios.");

        if (cfg.slowClientQueueLimit() > 0 && cfg.messageQueueLimit() > 0 && cfg.slowClientQueueLimit() >= cfg.messageQueueLimit()) {
            U.quietAndWarn(log, "Slow client queue limit is set to a value greater than or equal to message " +
                "queue limit (slow client queue limit will have no effect) [msgQueueLimit=" + cfg.messageQueueLimit() +
                ", slowClientQueueLimit=" + cfg.slowClientQueueLimit() + ']');
        }

        if (cfg.messageQueueLimit() == 0)
            U.quietAndWarn(log, "Message queue limit is set to 0 which may lead to " +
                "potential OOMEs when running cache operations in FULL_ASYNC or PRIMARY_SYNC modes " +
                "due to message queues growth on sender and receiver sides.");

        if (shmemSrv != null) {

            MessageFactory msgFactory = new MessageFactory() {
                private MessageFactory impl;

                @Nullable @Override public Message create(short type) {
                    if (impl == null)
                        impl = getSpiContext().messageFactory();

                    assert impl != null;

                    return impl.create(type);
                }
            };

            GridNioMessageWriterFactory writerFactory = new GridNioMessageWriterFactory() {
                private MessageFormatter formatter;

                @Override public MessageWriter writer(GridNioSession ses) throws IgniteCheckedException {
                    if (formatter == null)
                        formatter = getSpiContext().messageFormatter();

                    assert formatter != null;

                    ConnectionKey connKey = ses.meta(CONN_IDX_META);

                    return connKey != null ? formatter.writer(connKey.nodeId()) : null;
                }
            };

            GridNioMessageReaderFactory readerFactory = new GridNioMessageReaderFactory() {
                private MessageFormatter formatter;

                @Override public MessageReader reader(GridNioSession ses, MessageFactory msgFactory)
                    throws IgniteCheckedException {
                    if (formatter == null)
                        formatter = getSpiContext().messageFormatter();

                    assert formatter != null;

                    ConnectionKey connKey = ses.meta(CONN_IDX_META);

                    return connKey != null ? formatter.reader(connKey.nodeId(), msgFactory) : null;
                }
            };

            shmemAcceptWorker = new ShmemAcceptWorker(
                igniteInstanceName,
                srvLsnr,
                shmemSrv,
                metricsLsnr,
                log,
                msgFactory,
                writerFactory,
                readerFactory,
                tracing
            );

            new IgniteThread(shmemAcceptWorker).start();
        }

        nioSrvWrapper.start();

        this.commWorker = new CommunicationWorker(
            igniteInstanceName,
            log,
            cfg,
            attributeNames,
            clientPool,
            failureProcessorSupplier,
            nodeGetter,
            pingNode,
            this::getExceptionRegistry,
            nioSrvWrapper,
            getWorkersRegistry(ignite),
            getName()
        );

        this.srvLsnr.communicationWorker(commWorker);
        this.nioSrvWrapper.communicationWorker(commWorker);

        new IgniteSpiThread(igniteInstanceName, commWorker.name(), log) {
            @Override protected void body() {
                commWorker.run();
            }
        }.start();

        // Ack start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} } */
    @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        if (cfg.boundTcpPort() > 0)
            spiCtx.registerPort(cfg.boundTcpPort(), IgnitePortProtocol.TCP);

        // SPI can start without shmem port.
        if (cfg.boundTcpShmemPort() > 0)
            spiCtx.registerPort(cfg.boundTcpShmemPort(), IgnitePortProtocol.TCP);

        spiCtx.addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        metricsLsnr = new TcpCommunicationMetricsListener(ignite, spiCtx);

        registerMBean(
            igniteInstanceName,
            new TcpCommunicationSpiMBeanImpl(this, metricsLsnr, cfg, stateProvider),
            TcpCommunicationSpiMBean.class
        );

        srvLsnr.metricsListener(metricsLsnr);
        clientPool.metricsListener(metricsLsnr);
        ((CommunicationDiscoveryEventListener)discoLsnr).metricsListener(metricsLsnr);

        if (shmemAcceptWorker != null)
            shmemAcceptWorker.metricsListener(metricsLsnr);

        ctxInitLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiContext getSpiContext() {
        if (ctxInitLatch.getCount() > 0) {
            if (log.isDebugEnabled())
                log.debug("Waiting for context initialization.");

            try {
                U.await(ctxInitLatch);

                if (log.isDebugEnabled())
                    log.debug("Context has been initialized.");
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Thread has been interrupted while waiting for SPI context initialization.", e);
            }
        }

        return super.getSpiContext();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        assert stopping;

        unregisterMBean();

        // Stop TCP server.
        if (nioSrvWrapper != null)
            nioSrvWrapper.stop();

        if (commWorker != null) {
            commWorker.stop();
            U.cancel(commWorker);
            U.join(commWorker, log);
        }

        U.cancel(shmemAcceptWorker);
        U.join(shmemAcceptWorker, log);

        if (srvLsnr != null)
            srvLsnr.stop();

        // Force closing on stop (safety).
        if (clientPool != null) {
            clientPool.stop();
            clientPool.forceClose();
        }

        // Clear resources.
        if (nioSrvWrapper != null)
            nioSrvWrapper.clear();

        cfg.boundTcpPort(-1);

        // Ack stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        stopping = true;

        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        connectGate.stopped();

        getSpiContext().deregisterPorts();

        getSpiContext().removeLocalEventListener(discoLsnr);
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {
        connectGate.disconnected(reconnectFut);

        clientPool.forceClose();

        IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Failed to connect client node disconnected.");

        clientPool.completeFutures(err);

        nioSrvWrapper.recoveryDescs().clear();
        nioSrvWrapper.inRecDescs().clear();
        nioSrvWrapper.outRecDescs().clear();
    }

    /** {@inheritDoc} */
    @Override public void onClientReconnected(boolean clusterRestarted) {
        connectGate.reconnected();
    }

    /** {@inheritDoc} */
    @Override protected void checkConfigurationConsistency0(IgniteSpiContext spiCtx, ClusterNode node, boolean starting)
        throws IgniteSpiException {
        // These attributes are set on node startup in any case, so we MUST receive them.
        checkAttributePresence(node, createSpiAttributeName(ATTR_ADDRS));
        checkAttributePresence(node, createSpiAttributeName(ATTR_HOST_NAMES));
        checkAttributePresence(node, createSpiAttributeName(ATTR_PORT));
    }

    /**
     * @param remote Destination cluster node to communicate with.
     * @param initMsg Configuration channel attributes wrapped into the message.
     * @return The future, which will be finished on channel ready.
     * @throws IgniteSpiException If fails.
     */
    public IgniteInternalFuture<Channel> openChannel(
        ClusterNode remote,
        Message initMsg
    ) throws IgniteSpiException {
        return nioSrvWrapper.openChannel(remote, initMsg);
    }

    /**
     * Checks {@link Ignite} implementation type and calls {@link GridResourceProcessor#resolve(Object)} or returns original.
     */
    private <T> T resolve(Ignite ignite, T instance) {
        return ignite instanceof IgniteKernal ? ((IgniteKernal)ignite).context().resource().resolve(instance) : instance;
    }

    /**
     * Checks that node has specified attribute and prints warning if it does not.
     *
     * @param node Node to check.
     * @param attrName Name of the attribute.
     */
    private void checkAttributePresence(ClusterNode node, String attrName) {
        if (node.attribute(attrName) == null)
            U.warn(log, "Remote node has inconsistent configuration (required attribute was not found) " +
                "[attrName=" + attrName + ", nodeId=" + node.id() +
                "spiCls=" + U.getSimpleName(TcpCommunicationSpi.class) + ']');
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        sendMessage0(node, msg, null);
    }

    /**
     * @param nodes Nodes to check connection with.
     * @return Result future (each bit in result BitSet contains connection status to corresponding node).
     */
    public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
        TcpCommunicationConnectionCheckFuture fut = new TcpCommunicationConnectionCheckFuture(
            this,
            log.getLogger(TcpCommunicationConnectionCheckFuture.class),
            nioSrvWrapper.nio(),
            nodes);

        long timeout = failureDetectionTimeoutEnabled() ? failureDetectionTimeout() : cfg.connectionTimeout();

        if (log.isInfoEnabled())
            log.info("Start check connection process [nodeCnt=" + nodes.size() + ", timeout=" + timeout + ']');

        fut.init(timeout);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Sends given message to destination node. Note that characteristics of the exchange such as durability, guaranteed
     * delivery or error notification is dependant on SPI implementation.
     *
     * @param node Destination node.
     * @param msg Message to send.
     * @param ackC Ack closure.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error during sending the message. Note
     * that this is not guaranteed that failed communication will result in thrown exception as this is dependant on SPI
     * implementation.
     */
    public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        sendMessage0(node, msg, ackC);
    }

    /**
     * @param node Destination node.
     * @param msg Message to send.
     * @param ackC Ack closure.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error during sending the message. Note
     * that this is not guaranteed that failed communication will result in thrown exception as this is dependant on SPI
     * implementation.
     */
    private void sendMessage0(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        assert node != null;
        assert msg != null;

        IgniteLogger log = this.log;

        if (log != null && log.isTraceEnabled())
            log.trace("Sending message with ack to node [node=" + node + ", msg=" + msg + ']');

        if (stateProvider.isLocalNodeDisconnected()) {
            throw new IgniteSpiException("Failed to send a message to remote node because local node has " +
                "been disconnected [rmtNodeId=" + node.id() + ']');
        }

        ClusterNode locNode = getLocalNode();

        if (locNode == null)
            throw new IgniteSpiException("Local node has not been started or fully initialized " +
                "[isStopping=" + getSpiContext().isStopping() + ']');

        if (node.id().equals(locNode.id()))
            notifyListener(node.id(), msg, NOOP);
        else {
            GridCommunicationClient client = null;

            int connIdx;

            Message connIdxMsg = msg instanceof GridIoMessage ? ((GridIoMessage)msg).message() : msg;

            if (connIdxMsg instanceof TcpConnectionIndexAwareMessage) {
                int msgConnIdx = ((TcpConnectionIndexAwareMessage)connIdxMsg).connectionIndex();

                connIdx = msgConnIdx == UNDEFINED_CONNECTION_INDEX ? connPlc.connectionIndex() : msgConnIdx;
            }
            else
                connIdx = connPlc.connectionIndex();

            try {
                boolean retry;

                do {
                    client = clientPool.reserveClient(node, connIdx);

                    UUID nodeId = null;

                    if (!client.async())
                        nodeId = node.id();

                    retry = client.sendMessage(nodeId, msg, ackC);

                    client.release();

                    if (retry) {
                        clientPool.removeNodeClient(node.id(), client);

                        ClusterNode node0 = getSpiContext().node(node.id());

                        if (node0 == null)
                            throw new IgniteCheckedException("Failed to send message to remote node " +
                                "(node has left the grid): " + node.id());
                    }

                    client = null;
                }
                while (retry);
            }
            catch (Throwable t) {
                if (stopping)
                    throw new IgniteSpiException("Node is stopping.", t);

                // NodeUnreachableException should not be explicitly logged. Error message will appear if inverse
                // connection attempt fails as well.
                if (!(t instanceof NodeUnreachableException))
                    log.error("Failed to send message to remote node [node=" + node + ", msg=" + msg + ']', t);

                if (t instanceof Error)
                    throw (Error)t;

                if (t instanceof RuntimeException)
                    throw (RuntimeException)t;

                throw new IgniteSpiException("Failed to send message to remote node: " + node, t);
            }
            finally {
                if (client != null && clientPool.removeNodeClient(node.id(), client))
                    client.forceClose();
            }
        }
    }

    /**
     * @param node Node.
     * @param filterReachableAddrs Filter addresses flag.
     * @return Node addresses.
     * @throws IgniteCheckedException If node does not have addresses.
     */
    public Collection<InetSocketAddress> nodeAddresses(ClusterNode node, boolean filterReachableAddrs)
        throws IgniteCheckedException {
        return CommunicationTcpUtils.nodeAddresses(node, filterReachableAddrs, attributeNames, () -> getSpiContext().localNode());
    }

    /**
     * Establish TCP connection to remote node and returns client.
     *
     * @param node Remote node.
     * @param connIdx Connection index.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
        return nioSrvWrapper.createTcpClient(node, connIdx, false);
    }

    /**
     * Process errors if TCP/IP {@link GridNioSession} creation to remote node hasn't been performed.
     *
     * @param node Remote node.
     * @param addrs Remote node addresses.
     * @param errs TCP client creation errors.
     * @throws IgniteCheckedException If failed.
     */
    protected void processSessionCreationError(
        ClusterNode node,
        Collection<InetSocketAddress> addrs,
        IgniteCheckedException errs
    ) throws IgniteCheckedException {
        nioSrvWrapper.processSessionCreationError(node, addrs, errs);
    }

    /**
     * @param node Node.
     * @return {@code True} if remote current node cannot receive TCP connections. Applicable for client nodes only.
     */
    private boolean forceClientToServerConnections(ClusterNode node) {
        Boolean forceClientToSrvConnections = node.attribute(createSpiAttributeName(ATTR_FORCE_CLIENT_SERVER_CONNECTIONS));

        return Boolean.TRUE.equals(forceClientToSrvConnections);
    }

    /**
     * @param sndId Sender ID.
     * @param msg Communication message.
     * @param msgC Closure to call when message processing finished.
     */
    protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
        MTC.span().addLog(() -> "Communication listeners notified");

        if (this.lsnr != null)
            // Notify listener of a new message.
            this.lsnr.onMessage(sndId, msg, msgC);
        else if (log.isDebugEnabled())
            log.debug("Received communication message without any registered listeners (will ignore, " +
                "is node stopping?) [senderNodeId=" + sndId + ", msg=" + msg + ']');
    }

    /**
     * Stops service threads to simulate node failure.
     *
     * FOR TEST PURPOSES ONLY!!!
     *
     * @deprecated you should you DI and get instances of [nioSrvWrapper, commWorker, clientPool] via it.
     */
    @TestOnly
    @Deprecated
    public void simulateNodeFailure() {
        if (nioSrvWrapper.nio() != null)
            nioSrvWrapper.nio().stop();

        if (commWorker != null)
            U.interrupt(commWorker.runner());

        U.join(commWorker, log);

        clientPool.forceClose();
    }

    /**
     * @param msg Error message.
     * @param e Exception.
     */
    private void onException(String msg, Exception e) {
        getExceptionRegistry().onException(msg, e);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpCommunicationSpi.class, this);
    }

    /**
     * Write message type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Message type.
     */
    public static void writeMessageType(ByteBuffer buf, short type) {
        buf.put((byte)(type & 0xFF));
        buf.put((byte)((type >> 8) & 0xFF));
    }

    /**
     * Concatenates the two parameter bytes to form a message type value.
     *
     * @param b0 The first byte.
     * @param b1 The second byte.
     */
    public static short makeMessageType(byte b0, byte b1) {
        return (short)((b1 & 0xFF) << 8 | b0 & 0xFF);
    }

    /**
     * @param ignite Ignite.
     */
    private static WorkersRegistry getWorkersRegistry(Ignite ignite) {
        return ignite instanceof IgniteEx ? ((IgniteEx)ignite).context().workersRegistry() : null;
    }
}
