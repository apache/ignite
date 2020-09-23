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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteTooManyOpenFilesException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.GridManager;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.managers.tracing.GridTracingManager;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcToNioAdapter;
import org.apache.ignite.internal.util.ipc.shmem.IpcOutOfSystemResourcesException;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridConnectionBytesVerifyFilter;
import org.apache.ignite.internal.util.nio.GridDirectParser;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioMessageReaderFactory;
import org.apache.ignite.internal.util.nio.GridNioMessageTracker;
import org.apache.ignite.internal.util.nio.GridNioMessageWriterFactory;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.nio.GridNioTracerFilter;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.nio.GridShmemCommunicationClient;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.nio.ssl.BlockingSslHandler;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.nio.ssl.GridSslMeta;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMBeanAdapter;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.TimeoutStrategy;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationListenerEx;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionRequestFuture;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionRequestor;
import org.apache.ignite.spi.communication.tcp.internal.HandshakeException;
import org.apache.ignite.spi.communication.tcp.internal.NodeUnreachableException;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConnectionCheckFuture;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationNodeConnectionCheckFuture;
import org.apache.ignite.spi.communication.tcp.internal.TcpConnectionIndexAwareMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static java.util.Collections.emptyList;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.IgniteFeatures.CHANNEL_COMMUNICATION;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable.traceName;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.SSL_META;
import static org.apache.ignite.plugin.extensions.communication.Message.DIRECT_TYPE_SIZE;
import static org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConnectionCheckFuture.SES_FUT_META;
import static org.apache.ignite.spi.communication.tcp.internal.TcpConnectionIndexAwareMessage.UNDEFINED_CONNECTION_INDEX;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.ALREADY_CONNECTED;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NEED_WAIT;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NODE_STOPPING;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.UNKNOWN_NODE;

/**
 * <tt>TcpCommunicationSpi</tt> is default communication SPI which uses
 * TCP/IP protocol and Java NIO to communicate with other nodes.
 * <p>
 * To enable communication with other nodes, this SPI adds {@link #ATTR_ADDRS}
 * and {@link #ATTR_PORT} local node attributes (see {@link ClusterNode#attributes()}.
 * <p>
 * At startup, this SPI tries to start listening to local port specified by
 * {@link #setLocalPort(int)} method. If local port is occupied, then SPI will
 * automatically increment the port number until it can successfully bind for
 * listening. {@link #setLocalPortRange(int)} configuration parameter controls
 * maximum number of ports that SPI will try before it fails. Port range comes
 * very handy when starting multiple grid nodes on the same machine or even
 * in the same VM. In this case all nodes can be brought up without a single
 * change in configuration.
 * <p>
 * This SPI caches connections to remote nodes so it does not have to reconnect every
 * time a message is sent. By default, idle connections are kept active for
 * {@link #DFLT_IDLE_CONN_TIMEOUT} period and then are closed. Use
 * {@link #setIdleConnectionTimeout(long)} configuration parameter to configure
 * you own idle connection timeout.
 * <h1 class="header">Failure Detection</h1>
 * Configuration defaults (see Configuration section below and
 * {@link IgniteConfiguration#getFailureDetectionTimeout()}) for details) are chosen to make possible for
 * communication SPI work reliably on most of hardware and virtual deployments, but this has made failure detection
 * time worse.
 * <p>
 * If it's needed to tune failure detection then it's highly recommended to do this using
 * {@link IgniteConfiguration#setFailureDetectionTimeout(long)}. This failure timeout automatically controls the
 * following parameters: {@link #getConnectTimeout()}, {@link #getMaxConnectTimeout()},
 * {@link #getReconnectCount()}. If any of those parameters is set explicitly, then the failure timeout setting will be
 * ignored.
 * <p>
 * If it's required to perform advanced settings of failure detection and
 * {@link IgniteConfiguration#getFailureDetectionTimeout()} is unsuitable then various {@code TcpCommunicationSpi}
 * configuration parameters may be used.
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
 * TcpCommunicationSpi is used by default and should be explicitly configured
 * only if some SPI configuration parameters need to be overridden.
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
 * @see CommunicationSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
@IgniteSpiConsistencyChecked(optional = false)
public class TcpCommunicationSpi extends IgniteSpiAdapter implements CommunicationSpi<Message> {
    /** Time threshold to log too long connection establish. */
    private static final int CONNECTION_ESTABLISH_THRESHOLD_MS = 100;

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

    /** */
    public static final String ATTR_PAIRED_CONN = "comm.tcp.pairedConnection";

    /** */
    public static final String ATTR_FORCE_CLIENT_SERVER_CONNECTIONS = "comm.force.client.srv.connections";

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
     * Default count of selectors for TCP server equals to
     * {@code "Math.max(4, Runtime.getRuntime().availableProcessors() / 2)"}.
     */
    public static final int DFLT_SELECTORS_CNT = Math.max(4, Runtime.getRuntime().availableProcessors() / 2);

    /** Default initial connect/handshake timeout in case of failure detection enabled. */
    private static final int DFLT_INITIAL_TIMEOUT = 500;

    /** Default initial delay in case of target node is still out of topology. */
    private static final int DFLT_NEED_WAIT_DELAY = 200;

    /** Default delay between reconnects attempts in case of temporary network issues. */
    private static final int DFLT_RECONNECT_DELAY = 50;

    /**
     * Version when client is ready to wait to connect to server (could be needed when client tries to open connection
     * before it starts being visible for server)
     */
    private static final IgniteProductVersion VERSION_SINCE_CLIENT_COULD_WAIT_TO_CONNECT = IgniteProductVersion.fromString("2.1.4");

    /** Connection index meta for session. */
    public static final int CONN_IDX_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Node consistent id meta for session. */
    public static final int CONSISTENT_ID_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Message tracker meta for session. */
    private static final int TRACKER_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Channel meta used for establishing channel connections. */
    private static final int CHANNEL_FUT_META = GridNioSessionMetaKey.nextUniqueKey();

    /**
     * Default local port range (value is <tt>100</tt>).
     * See {@link #setLocalPortRange(int)} for details.
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

    /** Maximum {@link GridNioSession} connections per node. */
    public static final int MAX_CONN_PER_NODE = 1024;

    /** No-op runnable. */
    private static final IgniteRunnable NOOP = () -> {};

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

    /** */
    public static final String SENT_MESSAGES_METRIC_NAME = "sentMessagesCount";

    /** */
    public static final String SENT_MESSAGES_METRIC_DESC = "Total number of messages sent by current node";

    /** */
    public static final String RECEIVED_MESSAGES_METRIC_NAME = "receivedMessagesCount";

    /** */
    public static final String RECEIVED_MESSAGES_METRIC_DESC = "Total number of messages received by current node";

    /** */
    public static final String SENT_MESSAGES_BY_TYPE_METRIC_NAME = "sentMessagesByType";

    /** */
    public static final String SENT_MESSAGES_BY_TYPE_METRIC_DESC =
        "Total number of messages with given type sent by current node";

    /** */
    public static final String RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME = "receivedMessagesByType";

    /** */
    public static final String RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC =
        "Total number of messages with given type received by current node";

    /** */
    public static final String SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME = "sentMessagesToNode";

    /** */
    public static final String SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC =
        "Total number of messages sent by current node to the given node";

    /** */
    public static final String RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME = "receivedMessagesFromNode";

    /** */
    public static final String RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC =
        "Total number of messages received by current node from the given node";

    /** Client nodes might have port {@code 0} if they have no server socket opened. */
    public static final Integer DISABLED_CLIENT_PORT = 0;

    /** */
    private ConnectGateway connectGate;

    /** */
    private ConnectionPolicy connPlc = new FirstConnectionPolicy();

    /** Channel connection index provider. */
    private ConnectionPolicy chConnPlc;

    /** */
    private boolean enableForcibleNodeKill = getBoolean(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);

    /** */
    private boolean enableTroubleshootingLog = getBoolean(IgniteSystemProperties.IGNITE_TROUBLESHOOTING_LOGGER);

    /** Server listener. */
    private final GridNioServerListener<Message> srvLsnr =
        new GridNioServerListenerAdapter<Message>() {
            @Override public void onSessionWriteTimeout(GridNioSession ses) {
                LT.warn(log, "Communication SPI session write timed out (consider increasing " +
                    "'socketWriteTimeout' " + "configuration property) [remoteAddr=" + ses.remoteAddress() +
                    ", writeTimeout=" + sockWriteTimeout + ']');

                if (log.isDebugEnabled())
                    log.debug("Closing communication SPI session on write timeout [remoteAddr=" + ses.remoteAddress() +
                        ", writeTimeout=" + sockWriteTimeout + ']');

                ses.close();
            }

            @Override public void onConnected(GridNioSession ses) {
                if (ses.accepted()) {
                    if (log.isInfoEnabled())
                        log.info("Accepted incoming communication connection [locAddr=" + ses.localAddress() +
                            ", rmtAddr=" + ses.remoteAddress() + ']');

                    try {
                        boolean client = Boolean.TRUE.equals(ignite().configuration().isClientMode());

                        if (client || ctxInitLatch.getCount() == 0 || !isHandshakeWaitSupported()) {
                            if (log.isDebugEnabled())
                                log.debug("Sending local node ID to newly accepted session: " + ses);

                            ses.sendNoFuture(nodeIdMessage(), null);
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Sending handshake wait message to newly accepted session: " + ses);

                            ses.sendNoFuture(new HandshakeWaitMessage(), null);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send message: " + e, e);
                    }
                }
                else {
                    if (log.isInfoEnabled())
                        log.info("Established outgoing communication connection [locAddr=" + ses.localAddress() +
                            ", rmtAddr=" + ses.remoteAddress() + ']');
                }
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                ConnectionKey connId = ses.meta(CONN_IDX_META);

                if (connId != null) {
                    if (connId.dummy())
                        return;

                    UUID id = connId.nodeId();

                    GridCommunicationClient[] nodeClients = clients.get(id);

                    if (nodeClients != null) {
                        for (GridCommunicationClient client : nodeClients) {
                            if (client instanceof GridTcpNioCommunicationClient &&
                                ((GridTcpNioCommunicationClient)client).session() == ses) {
                                client.close();

                                removeNodeClient(id, client);
                            }
                        }
                    }

                    if (!stopping) {
                        GridNioRecoveryDescriptor outDesc = ses.outRecoveryDescriptor();

                        if (outDesc != null) {
                            if (outDesc.nodeAlive(getSpiContext().node(id))) {
                                if (!outDesc.messagesRequests().isEmpty()) {
                                    if (log.isDebugEnabled())
                                        log.debug("Session was closed but there are unacknowledged messages, " +
                                            "will try to reconnect [rmtNode=" + outDesc.node().id() + ']');

                                    DisconnectedSessionInfo disconnectData =
                                        new DisconnectedSessionInfo(outDesc, connId.connectionIndex());

                                    commWorker.addProcessDisconnectRequest(disconnectData);
                                }
                            }
                            else
                                outDesc.onNodeLeft();
                        }
                    }

                    CommunicationListener<Message> lsnr0 = lsnr;

                    if (lsnr0 != null)
                        lsnr0.onDisconnected(id);
                }
            }

            /**
             * @param ses Session.
             * @param msg Message.
             */
            private void onFirstMessage(final GridNioSession ses, Message msg) {
                UUID sndId;

                ConnectionKey connKey;

                if (msg instanceof NodeIdMessage) {
                    sndId = U.bytesToUuid(((NodeIdMessage)msg).nodeIdBytes(), 0);
                    connKey = new ConnectionKey(sndId, 0, -1);
                }
                else {
                    assert msg instanceof HandshakeMessage : msg;

                    HandshakeMessage msg0 = (HandshakeMessage)msg;

                    sndId = msg0.nodeId();
                    connKey = new ConnectionKey(sndId, msg0.connectionIndex(), msg0.connectCount());
                }

                if (log.isDebugEnabled())
                    log.debug("Remote node ID received: " + sndId);

                final ClusterNode rmtNode = getSpiContext().node(sndId);

                if (rmtNode == null) {
                    DiscoverySpi discoverySpi = ignite().configuration().getDiscoverySpi();

                    boolean unknownNode = true;

                    if (discoverySpi instanceof TcpDiscoverySpi) {
                        TcpDiscoverySpi tcpDiscoverySpi = (TcpDiscoverySpi)discoverySpi;

                        ClusterNode node0 = tcpDiscoverySpi.getNode0(sndId);

                        if (node0 != null) {
                            assert node0.isClient() : node0;

                            if (node0.version().compareTo(VERSION_SINCE_CLIENT_COULD_WAIT_TO_CONNECT) >= 0)
                                unknownNode = false;
                        }
                    }
                    else if (discoverySpi instanceof IgniteDiscoverySpi)
                        unknownNode = !((IgniteDiscoverySpi)discoverySpi).knownNode(sndId);

                    if (unknownNode) {
                        U.warn(log, "Close incoming connection, unknown node [nodeId=" + sndId + ", ses=" + ses + ']');

                        ses.send(new RecoveryLastReceivedMessage(UNKNOWN_NODE)).listen(fut -> ses.close());
                    }
                    else
                        ses.send(new RecoveryLastReceivedMessage(NEED_WAIT)).listen(fut -> ses.close());

                    return;
                }

                ses.addMeta(CONSISTENT_ID_META, rmtNode.consistentId());

                final ConnectionKey old = ses.addMeta(CONN_IDX_META, connKey);

                assert old == null;

                ClusterNode locNode = getSpiContext().localNode();

                if (ses.remoteAddress() == null)
                    return;

                assert msg instanceof HandshakeMessage : msg;

                HandshakeMessage msg0 = (HandshakeMessage)msg;

                if (log.isDebugEnabled())
                    log.debug("Received handshake message [locNodeId=" + locNode.id() + ", rmtNodeId=" + sndId +
                        ", msg=" + msg0 + ']');

                if (isChannelConnIdx(msg0.connectionIndex()))
                    ses.send(new RecoveryLastReceivedMessage(0));
                else if (usePairedConnections(rmtNode)) {
                    final GridNioRecoveryDescriptor recoveryDesc = inRecoveryDescriptor(rmtNode, connKey);

                    ConnectClosureNew c = new ConnectClosureNew(ses, recoveryDesc, rmtNode);

                    boolean reserve = recoveryDesc.tryReserve(msg0.connectCount(), c);

                    if (reserve)
                        connectedNew(recoveryDesc, ses, true);
                    else {
                        if (c.failed) {
                            ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                            closeStaleConnections(connKey);
                        }
                    }
                }
                else {
                    assert connKey.connectionIndex() >= 0 : connKey;

                    GridCommunicationClient[] curClients = clients.get(sndId);

                    GridCommunicationClient oldClient =
                        curClients != null && connKey.connectionIndex() < curClients.length ?
                            curClients[connKey.connectionIndex()] :
                            null;

                    boolean hasShmemClient = false;

                    if (oldClient != null) {
                        if (oldClient instanceof GridTcpNioCommunicationClient) {
                            if (log.isInfoEnabled())
                                log.info("Received incoming connection when already connected " +
                                    "to this node, rejecting [locNode=" + locNode.id() +
                                    ", rmtNode=" + sndId + ']');

                            ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                            closeStaleConnections(connKey);

                            return;
                        }
                        else {
                            assert oldClient instanceof GridShmemCommunicationClient;

                            hasShmemClient = true;
                        }
                    }

                    GridFutureAdapter<GridCommunicationClient> fut = new GridFutureAdapter<>();

                    GridFutureAdapter<GridCommunicationClient> oldFut = clientFuts.putIfAbsent(connKey, fut);

                    final GridNioRecoveryDescriptor recoveryDesc = inRecoveryDescriptor(rmtNode, connKey);

                    if (oldFut == null) {
                        curClients = clients.get(sndId);

                        oldClient = curClients != null && connKey.connectionIndex() < curClients.length ?
                            curClients[connKey.connectionIndex()] : null;

                        if (oldClient != null) {
                            if (oldClient instanceof GridTcpNioCommunicationClient) {
                                assert oldClient.connectionIndex() == connKey.connectionIndex() : oldClient;

                                if (log.isInfoEnabled())
                                    log.info("Received incoming connection when already connected " +
                                        "to this node, rejecting [locNode=" + locNode.id() +
                                        ", rmtNode=" + sndId + ']');

                                ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                                closeStaleConnections(connKey);

                                fut.onDone(oldClient);

                                return;
                            }
                            else {
                                assert oldClient instanceof GridShmemCommunicationClient;

                                hasShmemClient = true;
                            }
                        }

                        boolean reserved = recoveryDesc.tryReserve(msg0.connectCount(),
                            new ConnectClosure(ses, recoveryDesc, rmtNode, connKey, msg0, !hasShmemClient, fut));

                        if (log.isDebugEnabled())
                            log.debug("Received incoming connection from remote node " +
                                "[rmtNode=" + rmtNode.id() + ", reserved=" + reserved +
                                ", recovery=" + recoveryDesc + ']');

                        if (reserved) {
                            try {
                                GridTcpNioCommunicationClient client =
                                    connected(recoveryDesc, ses, rmtNode, msg0.received(), true, !hasShmemClient);

                                fut.onDone(client);
                            }
                            finally {
                                clientFuts.remove(connKey, fut);
                            }
                        }
                    }
                    else {
                        if (oldFut instanceof ConnectFuture && locNode.order() < rmtNode.order()) {
                            if (log.isInfoEnabled()) {
                                log.info("Received incoming connection from remote node while " +
                                    "connecting to this node, rejecting [locNode=" + locNode.id() +
                                    ", locNodeOrder=" + locNode.order() + ", rmtNode=" + rmtNode.id() +
                                    ", rmtNodeOrder=" + rmtNode.order() + ']');
                            }

                            ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));
                        }
                        else {
                            // The code below causes a race condition between shmem and TCP (see IGNITE-1294)
                            boolean reserved = recoveryDesc.tryReserve(msg0.connectCount(),
                                new ConnectClosure(ses, recoveryDesc, rmtNode, connKey, msg0, !hasShmemClient, fut));

                            GridTcpNioCommunicationClient client = null;

                            if (reserved)
                                client = connected(recoveryDesc, ses, rmtNode, msg0.received(), true, !hasShmemClient);

                            if (oldFut instanceof ConnectionRequestFuture && !oldFut.isDone())
                                oldFut.onDone(client);
                        }
                    }
                }
            }

            /**
             * @param connKey Connection key.
             */
            private void closeStaleConnections(ConnectionKey connKey) {
                for (GridNioSession ses0 : nioSrvr.sessions()) {
                    ConnectionKey key0 = ses0.meta(CONN_IDX_META);

                    if (ses0.accepted() && key0 != null &&
                        key0.nodeId().equals(connKey.nodeId()) &&
                        key0.connectionIndex() == connKey.connectionIndex() &&
                        key0.connectCount() < connKey.connectCount())
                        ses0.close();
                }
            }

            @Override public void onMessageSent(GridNioSession ses, Message msg) {
                Object consistentId = ses.meta(CONSISTENT_ID_META);

                if (consistentId != null)
                    metricsLsnr.onMessageSent(msg, consistentId);
            }

            private void onChannelCreate(
                GridSelectorNioSessionImpl ses,
                ConnectionKey connKey,
                Message msg
            ) {
                cleanupLocalNodeRecoveryDescriptor(connKey);

                ses.send(msg)
                    .listen(sendFut -> {
                        if (sendFut.error() != null) {
                            U.error(log, "Fail to send channel creation response to the remote node. " +
                                "Session will be closed [nodeId=" + connKey.nodeId() +
                                ", idx=" + connKey.connectionIndex() + ']', sendFut.error());

                            ses.close();

                            return;
                        }

                        ses.closeSocketOnSessionClose(false);

                        // Close session and send response.
                        ses.close().listen(closeFut -> {
                            if (closeFut.error() != null) {
                                U.error(log, "Nio session has not been properly closed " +
                                        "[nodeId=" + connKey.nodeId() + ", idx=" + connKey.connectionIndex() + ']',
                                    closeFut.error());

                                U.closeQuiet(ses.key().channel());

                                return;
                            }

                            notifyChannelEvtListener(connKey.nodeId(), ses.key().channel(), msg);
                        });
                    });
            }

            @Override public void onMessage(final GridNioSession ses, Message msg) {
                Span span = MTC.span();

                span.addLog(() -> "Communication received");
                span.addTag(SpanTags.MESSAGE, () -> traceName(msg));

                ConnectionKey connKey = ses.meta(CONN_IDX_META);

                if (connKey == null) {
                    assert ses.accepted() : ses;

                    if (!connectGate.tryEnter()) {
                        if (log.isDebugEnabled())
                            log.debug("Close incoming connection, failed to enter gateway.");

                        ses.send(new RecoveryLastReceivedMessage(NODE_STOPPING)).listen(fut -> ses.close());

                        return;
                    }

                    try {
                        onFirstMessage(ses, msg);
                    }
                    finally {
                        connectGate.leave();
                    }
                }
                else {
                    Object consistentId = ses.meta(CONSISTENT_ID_META);

                    assert consistentId != null;

                    if (isChannelConnIdx(connKey.connectionIndex())) {
                        if (ses.meta(CHANNEL_FUT_META) == null)
                            onChannelCreate((GridSelectorNioSessionImpl)ses, connKey, msg);
                        else {
                            GridFutureAdapter<Channel> fut = ses.meta(CHANNEL_FUT_META);
                            GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

                            ses0.closeSocketOnSessionClose(false);

                            ses0.close().listen(f -> {
                                if (f.error() != null) {
                                    fut.onDone(f.error());

                                    return;
                                }

                                fut.onDone(ses0.key().channel());
                            });
                        }

                        return;
                    }

                    if (msg instanceof RecoveryLastReceivedMessage) {
                        metricsLsnr.onMessageReceived(msg, consistentId);

                        GridNioRecoveryDescriptor recovery = ses.outRecoveryDescriptor();

                        if (recovery != null) {
                            RecoveryLastReceivedMessage msg0 = (RecoveryLastReceivedMessage)msg;

                            if (log.isDebugEnabled()) {
                                log.debug("Received recovery acknowledgement [rmtNode=" + connKey.nodeId() +
                                    ", connIdx=" + connKey.connectionIndex() +
                                    ", rcvCnt=" + msg0.received() + ']');
                            }

                            recovery.ackReceived(msg0.received());
                        }

                        return;
                    }
                    else {
                        GridNioRecoveryDescriptor recovery = ses.inRecoveryDescriptor();

                        if (recovery != null) {
                            long rcvCnt = recovery.onReceived();

                            if (rcvCnt % ackSndThreshold == 0) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Send recovery acknowledgement [rmtNode=" + connKey.nodeId() +
                                        ", connIdx=" + connKey.connectionIndex() +
                                        ", rcvCnt=" + rcvCnt + ']');
                                }

                                ses.systemMessage(new RecoveryLastReceivedMessage(rcvCnt));

                                recovery.lastAcknowledged(rcvCnt);
                            }
                        }
                        else if (connKey.dummy()) {
                            assert msg instanceof NodeIdMessage : msg;

                            TcpCommunicationNodeConnectionCheckFuture fut = ses.meta(SES_FUT_META);

                            assert fut != null : msg;

                            fut.onConnected(U.bytesToUuid(((NodeIdMessage)msg).nodeIdBytes(), 0));

                            nioSrvr.closeFromWorkerThread(ses);

                            return;
                        }
                    }

                    metricsLsnr.onMessageReceived(msg, consistentId);

                    IgniteRunnable c;

                    if (msgQueueLimit > 0) {
                        GridNioMessageTracker tracker = ses.meta(TRACKER_META);

                        if (tracker == null) {
                            GridNioMessageTracker old = ses.addMeta(TRACKER_META, tracker =
                                new GridNioMessageTracker(ses, msgQueueLimit));

                            assert old == null;
                        }

                        tracker.onMessageReceived();

                        c = tracker;
                    }
                    else
                        c = NOOP;

                    notifyListener(connKey.nodeId(), msg, c);
                }
            }

            /** {@inheritDoc} */
            @Override public void onFailure(FailureType failureType, Throwable failure) {
                if (ignite instanceof IgniteEx)
                    ((IgniteEx)ignite).context().failure().process(new FailureContext(failureType, failure));
            }

            /**
             * @param recovery Recovery descriptor.
             * @param ses Session.
             * @param node Node.
             * @param rcvCnt Number of received messages.
             * @param sndRes If {@code true} sends response for recovery handshake.
             * @param createClient If {@code true} creates NIO communication client.
             * @return Client.
             */
            private GridTcpNioCommunicationClient connected(
                GridNioRecoveryDescriptor recovery,
                GridNioSession ses,
                ClusterNode node,
                long rcvCnt,
                boolean sndRes,
                boolean createClient) {
                ConnectionKey connKey = ses.meta(CONN_IDX_META);

                assert connKey != null && connKey.connectionIndex() >= 0 : connKey;
                assert !usePairedConnections(node);

                recovery.onHandshake(rcvCnt);

                ses.inRecoveryDescriptor(recovery);
                ses.outRecoveryDescriptor(recovery);

                nioSrvr.resend(ses);

                try {
                    if (sndRes)
                        nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(recovery.received()));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send message: " + e, e);
                }

                recovery.onConnected();

                GridTcpNioCommunicationClient client = null;

                if (createClient) {
                    client = new GridTcpNioCommunicationClient(connKey.connectionIndex(), ses, log);

                    addNodeClient(node, connKey.connectionIndex(), client);
                }

                return client;
            }

            /**
             * @param recovery Recovery descriptor.
             * @param ses Session.
             * @param sndRes If {@code true} sends response for recovery handshake.
             */
            private void connectedNew(
                GridNioRecoveryDescriptor recovery,
                GridNioSession ses,
                boolean sndRes) {
                try {
                    ses.inRecoveryDescriptor(recovery);

                    if (sndRes)
                        nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(recovery.received()));

                    recovery.onConnected();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send message: " + e, e);
                }
            }

            /**
             *
             */
            class ConnectClosureNew implements IgniteInClosure<Boolean> {
                /** */
                private static final long serialVersionUID = 0L;

                /** */
                private final GridNioSession ses;

                /** */
                private final GridNioRecoveryDescriptor recoveryDesc;

                /** */
                private final ClusterNode rmtNode;

                /** */
                private boolean failed;

                /**
                 * @param ses Incoming session.
                 * @param recoveryDesc Recovery descriptor.
                 * @param rmtNode Remote node.
                 */
                ConnectClosureNew(GridNioSession ses,
                    GridNioRecoveryDescriptor recoveryDesc,
                    ClusterNode rmtNode) {
                    this.ses = ses;
                    this.recoveryDesc = recoveryDesc;
                    this.rmtNode = rmtNode;
                }

                /** {@inheritDoc} */
                @Override public void apply(Boolean success) {
                    try {
                        failed = !success;

                        if (success) {
                            IgniteInClosure<IgniteInternalFuture<?>> lsnr = new IgniteInClosure<IgniteInternalFuture<?>>() {
                                @Override public void apply(IgniteInternalFuture<?> msgFut) {
                                    try {
                                        msgFut.get();

                                        connectedNew(recoveryDesc, ses, false);
                                    }
                                    catch (IgniteCheckedException e) {
                                        if (log.isDebugEnabled())
                                            log.debug("Failed to send recovery handshake " +
                                                "[rmtNode=" + rmtNode.id() + ", err=" + e + ']');

                                        recoveryDesc.release();
                                    }
                                }
                            };

                            nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(recoveryDesc.received()), lsnr);
                        }
                        else
                            nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(ALREADY_CONNECTED));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send message: " + e, e);
                    }
                }
            }

            /**
             *
             */
            @SuppressWarnings("PackageVisibleInnerClass")
            class ConnectClosure implements IgniteInClosure<Boolean> {
                /** */
                private static final long serialVersionUID = 0L;

                /** */
                private final GridNioSession ses;

                /** */
                private final GridNioRecoveryDescriptor recoveryDesc;

                /** */
                private final ClusterNode rmtNode;

                /** */
                private final HandshakeMessage msg;

                /** */
                private final GridFutureAdapter<GridCommunicationClient> fut;

                /** */
                private final boolean createClient;

                /** */
                private final ConnectionKey connKey;

                /**
                 * @param ses Incoming session.
                 * @param recoveryDesc Recovery descriptor.
                 * @param rmtNode Remote node.
                 * @param connKey Connection key.
                 * @param msg Handshake message.
                 * @param createClient If {@code true} creates NIO communication client..
                 * @param fut Connect future.
                 */
                ConnectClosure(GridNioSession ses,
                    GridNioRecoveryDescriptor recoveryDesc,
                    ClusterNode rmtNode,
                    ConnectionKey connKey,
                    HandshakeMessage msg,
                    boolean createClient,
                    GridFutureAdapter<GridCommunicationClient> fut) {
                    this.ses = ses;
                    this.recoveryDesc = recoveryDesc;
                    this.rmtNode = rmtNode;
                    this.connKey = connKey;
                    this.msg = msg;
                    this.createClient = createClient;
                    this.fut = fut;
                }

                /** {@inheritDoc} */
                @Override public void apply(Boolean success) {
                    if (success) {
                        try {
                            IgniteInClosure<IgniteInternalFuture<?>> lsnr = new IgniteInClosure<IgniteInternalFuture<?>>() {
                                @Override public void apply(IgniteInternalFuture<?> msgFut) {
                                    try {
                                        msgFut.get();

                                        GridTcpNioCommunicationClient client =
                                            connected(recoveryDesc, ses, rmtNode, msg.received(), false, createClient);

                                        fut.onDone(client);
                                    }
                                    catch (IgniteCheckedException e) {
                                        if (log.isDebugEnabled())
                                            log.debug("Failed to send recovery handshake " +
                                                "[rmtNode=" + rmtNode.id() + ", err=" + e + ']');

                                        recoveryDesc.release();

                                        fut.onDone();
                                    }
                                    finally {
                                        clientFuts.remove(connKey, fut);
                                    }
                                }
                            };

                            nioSrvr.sendSystem(ses, new RecoveryLastReceivedMessage(recoveryDesc.received()), lsnr);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to send message: " + e, e);
                        }
                    }
                    else {
                        try {
                            fut.onDone();
                        }
                        finally {
                            clientFuts.remove(connKey, fut);
                        }
                    }
                }
            }
        };

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Logger. */
    @LoggerResource(categoryName = "org.apache.ignite.internal.diagnostic")
    private IgniteLogger diagnosticLog;

    /** Local IP address. */
    private String locAddr;

    /** Complex variable that represents this node IP address. */
    private volatile InetAddress locHost;

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

    /** Slow client queue limit. */
    private int slowClientQueueLimit;

    /** NIO server. */
    private GridNioServer<Message> nioSrvr;

    /** Shared memory server. */
    private IpcSharedMemoryServerEndpoint shmemSrv;

    /** */
    private boolean usePairedConnections;

    /** */
    private int connectionsPerNode = DFLT_CONN_PER_NODE;

    /** {@code TCP_NODELAY} option value for created sockets. */
    private boolean tcpNoDelay = DFLT_TCP_NODELAY;

    /** {@code FILTER_REACHABLE_ADDRESSES} option value for created sockets. */
    private boolean filterReachableAddresses = DFLT_FILTER_REACHABLE_ADDRESSES;

    /** Number of received messages after which acknowledgment is sent. */
    private int ackSndThreshold = DFLT_ACK_SND_THRESHOLD;

    /** Maximum number of unacknowledged messages. */
    private int unackedMsgsBufSize;

    /** Socket write timeout. */
    private long sockWriteTimeout = DFLT_SOCK_WRITE_TIMEOUT;

    /** Recovery and idle clients handler. */
    private CommunicationWorker commWorker;

    /** Shared memory accept worker. */
    private ShmemAcceptWorker shmemAcceptWorker;

    /** Shared memory workers. */
    private final Collection<ShmemWorker> shmemWorkers = new ConcurrentLinkedDeque<>();

    /** Clients. */
    private final ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridConcurrentFactory.newMap();

    /** SPI listener. */
    private volatile CommunicationListener<Message> lsnr;

    /** Bound port. */
    private int boundTcpPort = -1;

    /** Bound port for shared memory server. */
    private int boundTcpShmemPort = -1;

    /** Count of selectors to use in TCP server. */
    private int selectorsCnt = DFLT_SELECTORS_CNT;

    /**
     * Defines how many non-blocking {@code selector.selectNow()} should be made before
     * falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
     * Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
     */
    private long selectorSpins = IgniteSystemProperties.getLong("IGNITE_SELECTOR_SPINS", 0L);

    /** */
    private boolean forceClientToSrvConnections;

    /** Address resolver. */
    private AddressResolver addrRslvr;

    /** Context initialization latch. */
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** Stopping flag (set to {@code true} when SPI gets stopping signal). */
    private volatile boolean stopping;

    /** Statistics. */
    private TcpCommunicationMetricsListener metricsLsnr;

    /** Client connect futures. */
    private final ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
        GridConcurrentFactory.newMap();

    /** */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs = GridConcurrentFactory.newMap();

    /** */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> outRecDescs = GridConcurrentFactory.newMap();

    /** */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> inRecDescs = GridConcurrentFactory.newMap();

    /** */
    private final GridLocalEventListener discoLsnr = new DiscoveryListener();

    /** Connection requestor. */
    private ConnectionRequestor connectionRequestor;

    /** Tracing. */
    protected Tracing tracing;

    /**
     * @return {@code True} if ssl enabled.
     */
    private boolean isSslEnabled() {
        return ignite.configuration().getSslContextFactory() != null;
    }

    /**
     * Sets address resolver.
     *
     * @param addrRslvr Address resolver.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setAddressResolver(AddressResolver addrRslvr) {
        // Injection should not override value already set by Spring or user.
        if (this.addrRslvr == null)
            this.addrRslvr = addrRslvr;

        return this;
    }

    /**
     * See {@link #setAddressResolver(AddressResolver)}.
     *
     * @return Address resolver.
     */
    public AddressResolver getAddressResolver() {
        return addrRslvr;
    }

    /**
     * Injects resources.
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    @Override protected void injectResources(Ignite ignite) {
        super.injectResources(ignite);

        if (ignite != null) {
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
        if (this.locAddr == null)
            this.locAddr = locAddr;

        return this;
    }

    /**
     * See {@link #setLocalAddress(String)}.
     *
     * @return Grid node IP address.
     */
    public String getLocalAddress() {
        return locAddr;
    }

    /**
     * Sets local port for socket binding.
     * <p>
     * If not provided, default value is {@link #DFLT_PORT}.
     *
     * @param locPort Port number.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setLocalPort(int locPort) {
        this.locPort = locPort;

        return this;
    }

    /**
     * See {@link #setLocalPort(int)}.
     *
     * @return Port number.
     */
    public int getLocalPort() {
        return locPort;
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
     * If not provided, default value is {@link #DFLT_PORT_RANGE}.
     *
     * @param locPortRange New local port range.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setLocalPortRange(int locPortRange) {
        this.locPortRange = locPortRange;

        return this;
    }

    /**
     * See {@link #setLocalPortRange(int)}.
     *
     * @return Local Port range.
     */
    public int getLocalPortRange() {
        return locPortRange;
    }

    /**
     * See {@link #setUsePairedConnections(boolean)}.
     *
     * @return {@code true} to use paired connections and {@code false} otherwise.
     */
    public boolean isUsePairedConnections() {
        return usePairedConnections;
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
        this.usePairedConnections = usePairedConnections;

        return this;
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
        this.connectionsPerNode = maxConnectionsPerNode;

        return this;
    }

    /**
     * See {@link #setConnectionsPerNode(int)}.
     *
     * @return Number of connections per node.
     */
    public int getConnectionsPerNode() {
        return connectionsPerNode;
    }

    /**
     * Sets local port to accept shared memory connections.
     * <p>
     * If set to {@code -1} shared memory communication will be disabled.
     * <p>
     * If not provided, default value is {@link #DFLT_SHMEM_PORT}.
     *
     * @param shmemPort Port number.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSharedMemoryPort(int shmemPort) {
        this.shmemPort = shmemPort;

        return this;
    }

    /**
     * See {@link #setSharedMemoryPort(int)}.
     *
     * @return Port number.
     */
    public int getSharedMemoryPort() {
        return shmemPort;
    }

    /**
     * Sets maximum idle connection timeout upon which a connection
     * to client will be closed.
     * <p>
     * If not provided, default value is {@link #DFLT_IDLE_CONN_TIMEOUT}.
     *
     * @param idleConnTimeout Maximum idle connection time.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setIdleConnectionTimeout(long idleConnTimeout) {
        this.idleConnTimeout = idleConnTimeout;

        return this;
    }

    /**
     * See {@link #setIdleConnectionTimeout(long)}.
     *
     * @return Maximum idle connection time.
     */
    public long getIdleConnectionTimeout() {
        return idleConnTimeout;
    }

    /**
     * See {@link #setSocketWriteTimeout(long)}.
     *
     * @return Socket write timeout for TCP connections.
     */
    public long getSocketWriteTimeout() {
        return sockWriteTimeout;
    }

    /**
     * Sets socket write timeout for TCP connection. If message can not be written to
     * socket within this time then connection is closed and reconnect is attempted.
     * <p>
     * Default to {@link #DFLT_SOCK_WRITE_TIMEOUT}.
     *
     * @param sockWriteTimeout Socket write timeout for TCP connection.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSocketWriteTimeout(long sockWriteTimeout) {
        this.sockWriteTimeout = sockWriteTimeout;

        return this;
    }

    /**
     * See {@link #setAckSendThreshold(int)}.
     *
     * @return Number of received messages after which acknowledgment is sent.
     */
    public int getAckSendThreshold() {
        return ackSndThreshold;
    }

    /**
     * Sets number of received messages per connection to node after which acknowledgment message is sent.
     * <p>
     * Default to {@link #DFLT_ACK_SND_THRESHOLD}.
     *
     * @param ackSndThreshold Number of received messages after which acknowledgment is sent.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setAckSendThreshold(int ackSndThreshold) {
        this.ackSndThreshold = ackSndThreshold;

        return this;
    }

    /**
     * See {@link #setUnacknowledgedMessagesBufferSize(int)}.
     *
     * @return Maximum number of unacknowledged messages.
     */
    public int getUnacknowledgedMessagesBufferSize() {
        return unackedMsgsBufSize;
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
        this.unackedMsgsBufSize = unackedMsgsBufSize;

        return this;
    }

    /**
     * Sets connect timeout used when establishing connection
     * with remote nodes.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link #DFLT_CONN_TIMEOUT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param connTimeout Connect timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setConnectTimeout(long connTimeout) {
        this.connTimeout = connTimeout;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * See {@link #setConnectTimeout(long)}.
     *
     * @return Connect timeout.
     */
    public long getConnectTimeout() {
        return connTimeout;
    }

    /**
     * Sets maximum connect timeout. If handshake is not established within connect timeout,
     * then SPI tries to repeat handshake procedure with increased connect timeout.
     * Connect timeout can grow till maximum timeout value,
     * if maximum timeout value is reached then the handshake is considered as failed.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_CONN_TIMEOUT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param maxConnTimeout Maximum connect timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setMaxConnectTimeout(long maxConnTimeout) {
        this.maxConnTimeout = maxConnTimeout;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * Gets maximum connect timeout.
     *
     * @return Maximum connect timeout.
     */
    public long getMaxConnectTimeout() {
        return maxConnTimeout;
    }

    /**
     * Sets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     * <p>
     * If not provided, default value is {@link #DFLT_RECONNECT_CNT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param reconCnt Maximum number of reconnection attempts.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setReconnectCount(int reconCnt) {
        this.reconCnt = reconCnt;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * Gets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     *
     * @return Reconnects count.
     */
    public int getReconnectCount() {
        return reconCnt;
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
        this.directBuf = directBuf;

        return this;
    }

    /**
     * Gets flag that indicates whether direct or heap allocated buffer is used.
     *
     * @return Flag that indicates whether direct or heap allocated buffer is used.
     */
    public boolean isDirectBuffer() {
        return directBuf;
    }

    /**
     * Gets flag defining whether direct send buffer should be used.
     *
     * @return {@code True} if direct buffers should be used.
     */
    public boolean isDirectSendBuffer() {
        return directSndBuf;
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
        this.directSndBuf = directSndBuf;

        return this;
    }

    /**
     * Sets the count of selectors te be used in TCP server.
     * <p/>
     * If not provided, default value is {@link #DFLT_SELECTORS_CNT}.
     *
     * @param selectorsCnt Selectors count.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSelectorsCount(int selectorsCnt) {
        this.selectorsCnt = selectorsCnt;

        return this;
    }

    /**
     * See {@link #setSelectorsCount(int)}.
     *
     * @return Count of selectors in TCP server.
     */
    public int getSelectorsCount() {
        return selectorsCnt;
    }

    /**
     * See {@link #setSelectorSpins(long)}.
     *
     * @return Selector thread busy-loop iterations.
     */
    public long getSelectorSpins() {
        return selectorSpins;
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
        this.selectorSpins = selectorSpins;

        return this;
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
     * If not provided, default value is {@link #DFLT_TCP_NODELAY}.
     *
     * @param tcpNoDelay {@code True} to disable TCP delay.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    /**
     * Gets value for {@code TCP_NODELAY} socket option.
     *
     * @return {@code True} if TCP delay is disabled.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Gets value for {@code FILTER_REACHABLE_ADDRESSES} socket option.
     *
     * @return {@code True} if needed to filter reachable addresses.
     */
    public boolean isFilterReachableAddresses() {
        return filterReachableAddresses;
    }

    /**
     * Setting this option to {@code true} enables filter for reachable
     * addresses on creating tcp client.
     * <p>
     * Usually its advised to set this value to {@code false}.
     * <p>
     * If not provided, default value is {@link #DFLT_FILTER_REACHABLE_ADDRESSES}.
     *
     * @param filterReachableAddresses {@code True} to filter reachable addresses.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setFilterReachableAddresses(boolean filterReachableAddresses) {
        this.filterReachableAddresses = filterReachableAddresses;

        return this;
    }

    /**
     * Sets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockRcvBuf Socket receive buffer size.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSocketReceiveBuffer(int sockRcvBuf) {
        this.sockRcvBuf = sockRcvBuf;

        return this;
    }

    /**
     * See {@link #setSocketReceiveBuffer(int)}.
     *
     * @return Socket receive buffer size.
     */
    public int getSocketReceiveBuffer() {
        return sockRcvBuf;
    }

    /**
     * Sets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockSndBuf Socket send buffer size.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setSocketSendBuffer(int sockSndBuf) {
        this.sockSndBuf = sockSndBuf;

        return this;
    }

    /**
     * See {@link #setSocketSendBuffer(int)}.
     *
     * @return Socket send buffer size.
     */
    public int getSocketSendBuffer() {
        return sockSndBuf;
    }

    /**
     * Sets message queue limit for incoming and outgoing messages.
     * <p>
     * When set to positive number send queue is limited to the configured value.
     * {@code 0} disables the size limitations.
     * <p>
     * If not provided, default is {@link #DFLT_MSG_QUEUE_LIMIT}.
     *
     * @param msgQueueLimit Send queue size limit.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpCommunicationSpi setMessageQueueLimit(int msgQueueLimit) {
        this.msgQueueLimit = msgQueueLimit;

        return this;
    }

    /**
     * Gets message queue limit for incoming and outgoing messages.
     *
     * @return Send queue size limit.
     */
    public int getMessageQueueLimit() {
        return msgQueueLimit;
    }

    /**
     * See {@link #setSlowClientQueueLimit(int)}.
     *
     * @return Slow client queue limit.
     */
    public int getSlowClientQueueLimit() {
        return slowClientQueueLimit;
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
        this.slowClientQueueLimit = slowClientQueueLimit;

        return this;
    }

    /**
     * @return Force client to server connections flag.
     *
     * @see #setForceClientToServerConnections(boolean)
     */
    @IgniteExperimental
    public boolean forceClientToServerConnections() {
        return forceClientToSrvConnections;
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
        this.forceClientToSrvConnections = forceClientToSrvConnections;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void setListener(CommunicationListener<Message> lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * @return Listener.
     */
    public CommunicationListener getListener() {
        return lsnr;
    }

    /** */
    @IgniteExperimental
    public void setConnectionRequestor(ConnectionRequestor connectionRequestor) {
        this.connectionRequestor = connectionRequestor;
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
        // Listener could be not initialized yet, but discovery thread clould try to aggregate metrics.
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
        GridNioServer<Message> srv = nioSrvr;

        return srv != null ? srv.outboundMessagesQueueSize() : 0;
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        metricsLsnr.resetMetrics();
    }

    /**
     * @param nodeId Target node ID.
     * @return Future.
     */
    public IgniteInternalFuture<String> dumpNodeStatistics(final UUID nodeId) {
        StringBuilder sb = new StringBuilder("Communication SPI statistics [rmtNode=").append(nodeId).append(']').append(U.nl());

        dumpInfo(sb, nodeId);

        GridNioServer<Message> nioSrvr = this.nioSrvr;

        if (nioSrvr != null) {
            sb.append("NIO sessions statistics:");

            IgnitePredicate<GridNioSession> p = new IgnitePredicate<GridNioSession>() {
                @Override public boolean apply(GridNioSession ses) {
                    ConnectionKey connId = ses.meta(CONN_IDX_META);

                    return connId != null && nodeId.equals(connId.nodeId());
                }
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

            GridNioServer<Message> nioSrvr = this.nioSrvr;

            if (nioSrvr != null) {
                nioSrvr.dumpStats().listen(new CI1<IgniteInternalFuture<String>>() {
                    @Override public void apply(IgniteInternalFuture<String> fut) {
                        try {
                            U.warn(log, fut.get());
                        }
                        catch (Exception e) {
                            U.error(log, "Failed to dump NIO server statistics: " + e, e);
                        }
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

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> entry : recoveryDescs.entrySet()) {
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

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> entry : outRecDescs.entrySet()) {
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

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> entry : inRecDescs.entrySet()) {
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

        for (Map.Entry<UUID, GridCommunicationClient[]> entry : clients.entrySet()) {
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
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        initFailureDetectionTimeout();

        if (Boolean.TRUE.equals(ignite.configuration().isClientMode()))
            assertParameter(locPort > 1023 || locPort == -1, "locPort > 1023 || locPort == -1");
        else
            assertParameter(locPort > 1023, "locPort > 1023");

        assertParameter(locPort <= 0xffff, "locPort < 0xffff");
        assertParameter(locPortRange >= 0, "locPortRange >= 0");
        assertParameter(idleConnTimeout > 0, "idleConnTimeout > 0");
        assertParameter(sockRcvBuf >= 0, "sockRcvBuf >= 0");
        assertParameter(sockSndBuf >= 0, "sockSndBuf >= 0");
        assertParameter(msgQueueLimit >= 0, "msgQueueLimit >= 0");
        assertParameter(shmemPort > 0 || shmemPort == -1, "shmemPort > 0 || shmemPort == -1");
        assertParameter(selectorsCnt > 0, "selectorsCnt > 0");
        assertParameter(connectionsPerNode > 0, "connectionsPerNode > 0");
        assertParameter(connectionsPerNode <= MAX_CONN_PER_NODE, "connectionsPerNode <= 1024");

        if (!failureDetectionTimeoutEnabled()) {
            assertParameter(reconCnt > 0, "reconnectCnt > 0");
            assertParameter(connTimeout >= 0, "connTimeout >= 0");
            assertParameter(maxConnTimeout >= connTimeout, "maxConnTimeout >= connTimeout");
        }

        assertParameter(sockWriteTimeout >= 0, "sockWriteTimeout >= 0");
        assertParameter(ackSndThreshold > 0, "ackSndThreshold > 0");
        assertParameter(unackedMsgsBufSize >= 0, "unackedMsgsBufSize >= 0");

        if (unackedMsgsBufSize > 0) {
            assertParameter(unackedMsgsBufSize >= msgQueueLimit * 5,
                "Specified 'unackedMsgsBufSize' is too low, it should be at least 'msgQueueLimit * 5'.");

            assertParameter(unackedMsgsBufSize >= ackSndThreshold * 5,
                "Specified 'unackedMsgsBufSize' is too low, it should be at least 'ackSndThreshold * 5'.");
        }

        if (connectionsPerNode > 1)
            connPlc = new RoundRobinConnectionPolicy();
        else
            connPlc = new FirstConnectionPolicy();

        chConnPlc = new ConnectionPolicy() {
            /** Sequential connection index provider. */
            private final AtomicInteger chIdx = new AtomicInteger(MAX_CONN_PER_NODE + 1);

            @Override public int connectionIndex() {
                return chIdx.incrementAndGet();
            }
        };

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new IgniteSpiException("Failed to initialize local address: " + locAddr, e);
        }

        try {
            shmemSrv = resetShmemServer();
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to start shared memory communication server.", e);
        }

        try {
            // This method potentially resets local port to the value
            // local node was bound to.
            nioSrvr = resetNioServer();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to initialize TCP server: " + locHost, e);
        }

        boolean forceClientToSrvConnections = forceClientToServerConnections() || locPort == -1;

        if (usePairedConnections && forceClientToSrvConnections) {
            throw new IgniteSpiException("Node using paired connections " +
                "is not allowed to start in forced client to server connections mode.");
        }

        // Set local node attributes.
        try {
            IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(locHost);

            if (locPort != -1 && addrs.get1().isEmpty() && addrs.get2().isEmpty())
                throw new IgniteCheckedException("No network addresses found (is networking enabled?).");

            Collection<InetSocketAddress> extAddrs = addrRslvr == null ? null :
                U.resolveAddresses(addrRslvr, F.flat(Arrays.asList(addrs.get1(), addrs.get2())), boundTcpPort);

            Map<String, Object> res = new HashMap<>(5);

            boolean setEmptyHostNamesAttr = !getBoolean(IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES, false) &&
                (!F.isEmpty(locAddr) && locHost.getHostAddress().equals(locAddr)) && !locHost.isAnyLocalAddress() &&
                !locHost.isLoopbackAddress();

            res.put(createSpiAttributeName(ATTR_ADDRS), addrs.get1());
            res.put(createSpiAttributeName(ATTR_HOST_NAMES), setEmptyHostNamesAttr ? emptyList() : addrs.get2());
            res.put(createSpiAttributeName(ATTR_PORT), boundTcpPort == -1 ? DISABLED_CLIENT_PORT : boundTcpPort);
            res.put(createSpiAttributeName(ATTR_SHMEM_PORT), boundTcpShmemPort >= 0 ? boundTcpShmemPort : null);
            res.put(createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);
            res.put(createSpiAttributeName(ATTR_PAIRED_CONN), usePairedConnections);
            res.put(createSpiAttributeName(ATTR_FORCE_CLIENT_SERVER_CONNECTIONS), forceClientToSrvConnections);

            return res;
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to resolve local host to addresses: " + locHost, e);
        }
    }

    /**
     * @return Bound TCP server port.
     */
    public int boundPort() {
        return boundTcpPort;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        assert locHost != null;

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("locAddr", locAddr));
            log.debug(configInfo("locPort", locPort));
            log.debug(configInfo("locPortRange", locPortRange));
            log.debug(configInfo("idleConnTimeout", idleConnTimeout));
            log.debug(configInfo("directBuf", directBuf));
            log.debug(configInfo("directSendBuf", directSndBuf));
            log.debug(configInfo("selectorsCnt", selectorsCnt));
            log.debug(configInfo("tcpNoDelay", tcpNoDelay));
            log.debug(configInfo("sockSndBuf", sockSndBuf));
            log.debug(configInfo("sockRcvBuf", sockRcvBuf));
            log.debug(configInfo("shmemPort", shmemPort));
            log.debug(configInfo("msgQueueLimit", msgQueueLimit));
            log.debug(configInfo("connectionsPerNode", connectionsPerNode));

            if (failureDetectionTimeoutEnabled()) {
                log.debug(configInfo("connTimeout", connTimeout));
                log.debug(configInfo("maxConnTimeout", maxConnTimeout));
                log.debug(configInfo("reconCnt", reconCnt));
            }
            else
                log.debug(configInfo("failureDetectionTimeout", failureDetectionTimeout()));

            log.debug(configInfo("sockWriteTimeout", sockWriteTimeout));
            log.debug(configInfo("ackSndThreshold", ackSndThreshold));
            log.debug(configInfo("unackedMsgsBufSize", unackedMsgsBufSize));
        }

        if (!tcpNoDelay)
            U.quietAndWarn(log, "'TCP_NO_DELAY' for communication is off, which should be used with caution " +
                "since may produce significant delays with some scenarios.");

        if (slowClientQueueLimit > 0 && msgQueueLimit > 0 && slowClientQueueLimit >= msgQueueLimit) {
            U.quietAndWarn(log, "Slow client queue limit is set to a value greater than or equal to message " +
                "queue limit (slow client queue limit will have no effect) [msgQueueLimit=" + msgQueueLimit +
                ", slowClientQueueLimit=" + slowClientQueueLimit + ']');
        }

        if (msgQueueLimit == 0)
            U.quietAndWarn(log, "Message queue limit is set to 0 which may lead to " +
                "potential OOMEs when running cache operations in FULL_ASYNC or PRIMARY_SYNC modes " +
                "due to message queues growth on sender and receiver sides.");

        registerMBean(igniteInstanceName, new TcpCommunicationSpiMBeanImpl(this), TcpCommunicationSpiMBean.class);

        connectGate = new ConnectGateway();

        if (shmemSrv != null) {
            shmemAcceptWorker = new ShmemAcceptWorker(shmemSrv);

            new IgniteThread(shmemAcceptWorker).start();
        }

        nioSrvr.start();

        commWorker = new CommunicationWorker(igniteInstanceName, log);

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
        if (boundTcpPort > 0)
            spiCtx.registerPort(boundTcpPort, IgnitePortProtocol.TCP);

        // SPI can start without shmem port.
        if (boundTcpShmemPort > 0)
            spiCtx.registerPort(boundTcpShmemPort, IgnitePortProtocol.TCP);

        spiCtx.addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctxInitLatch.countDown();

        metricsLsnr = new TcpCommunicationMetricsListener(ignite, spiCtx);
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

    /**
     * Recreates tpcSrvr socket instance.
     *
     * @return Server instance.
     * @throws IgniteCheckedException Thrown if it's not possible to create server.
     */
    private GridNioServer<Message> resetNioServer() throws IgniteCheckedException {
        if (boundTcpPort >= 0)
            throw new IgniteCheckedException("Tcp NIO server was already created on port " + boundTcpPort);

        IgniteCheckedException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        int lastPort = locPort == -1 ? -1
            : locPortRange == 0 ? locPort : locPort + locPortRange - 1;

        for (int port = locPort; port <= lastPort; port++) {
            try {
                MessageFactory msgFactory = new MessageFactory() {
                    private MessageFactory impl;

                    @Nullable @Override public Message create(short type) {
                        if (impl == null)
                            impl = getSpiContext().messageFactory();

                        assert impl != null;

                        return impl.create(type);
                    }
                };

                GridNioMessageReaderFactory readerFactory = new GridNioMessageReaderFactory() {
                    private IgniteSpiContext context;

                    private MessageFormatter formatter;

                    @Override public MessageReader reader(GridNioSession ses, MessageFactory msgFactory)
                        throws IgniteCheckedException {
                        final IgniteSpiContext ctx = TcpCommunicationSpi.super.getSpiContext();

                        if (formatter == null || context != ctx) {
                            context = ctx;

                            formatter = context.messageFormatter();
                        }

                        assert formatter != null;

                        ConnectionKey key = ses.meta(CONN_IDX_META);

                        return key != null ? formatter.reader(key.nodeId(), msgFactory) : null;
                    }
                };

                GridNioMessageWriterFactory writerFactory = new GridNioMessageWriterFactory() {
                    private IgniteSpiContext context;

                    private MessageFormatter formatter;

                    @Override public MessageWriter writer(GridNioSession ses) throws IgniteCheckedException {
                        final IgniteSpiContext ctx = TcpCommunicationSpi.super.getSpiContext();

                        if (formatter == null || context != ctx) {
                            context = ctx;

                            formatter = context.messageFormatter();
                        }

                        assert formatter != null;

                        ConnectionKey key = ses.meta(CONN_IDX_META);

                        return key != null ? formatter.writer(key.nodeId()) : null;
                    }
                };

                GridDirectParser parser = new GridDirectParser(log.getLogger(GridDirectParser.class),
                    msgFactory,
                    readerFactory);

                IgnitePredicate<Message> skipRecoveryPred = new IgnitePredicate<Message>() {
                    @Override public boolean apply(Message msg) {
                        return msg instanceof RecoveryLastReceivedMessage;
                    }
                };

                boolean clientMode = Boolean.TRUE.equals(ignite.configuration().isClientMode());

                IgniteBiInClosure<GridNioSession, Integer> queueSizeMonitor =
                    !clientMode && slowClientQueueLimit > 0 ? this::checkClientQueueSize : null;

                List<GridNioFilter> filters = new ArrayList<>();

                if (tracing instanceof GridTracingManager && ((GridManager)tracing).enabled())
                    filters.add(new GridNioTracerFilter(log, tracing));

                filters.add(new GridNioCodecFilter(parser, log, true));
                filters.add(new GridConnectionBytesVerifyFilter(log));

                if (isSslEnabled()) {
                    GridNioSslFilter sslFilter =
                        new GridNioSslFilter(ignite.configuration().getSslContextFactory().create(),
                            true, ByteOrder.LITTLE_ENDIAN, log);

                    sslFilter.directMode(true);

                    sslFilter.wantClientAuth(true);
                    sslFilter.needClientAuth(true);

                    filters.add(sslFilter);
                }

                GridNioServer.Builder<Message> builder = GridNioServer.<Message>builder()
                    .address(locHost)
                    .port(port)
                    .listener(srvLsnr)
                    .logger(log)
                    .selectorCount(selectorsCnt)
                    .igniteInstanceName(igniteInstanceName)
                    .serverName("tcp-comm")
                    .tcpNoDelay(tcpNoDelay)
                    .directBuffer(directBuf)
                    .byteOrder(ByteOrder.LITTLE_ENDIAN)
                    .socketSendBufferSize(sockSndBuf)
                    .socketReceiveBufferSize(sockRcvBuf)
                    .sendQueueLimit(msgQueueLimit)
                    .directMode(true)
                    .writeTimeout(sockWriteTimeout)
                    .selectorSpins(selectorSpins)
                    .filters(filters.toArray(new GridNioFilter[filters.size()]))
                    .writerFactory(writerFactory)
                    .skipRecoveryPredicate(skipRecoveryPred)
                    .messageQueueSizeListener(queueSizeMonitor)
                    .tracing(tracing)
                    .readWriteSelectorsAssign(usePairedConnections);

                if (ignite instanceof IgniteEx) {
                    IgniteEx igniteEx = (IgniteEx)ignite;

                    builder.workerListener(igniteEx.context().workersRegistry())
                        .metricRegistry(igniteEx.context().metric().registry(COMMUNICATION_METRICS_GROUP_NAME));
                }

                GridNioServer<Message> srvr = builder.build();

                boundTcpPort = port;

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled()) {
                    log.info("Successfully bound communication NIO server to TCP port " +
                        "[port=" + boundTcpPort +
                        ", locHost=" + locHost +
                        ", selectorsCnt=" + selectorsCnt +
                        ", selectorSpins=" + srvr.selectorSpins() +
                        ", pairedConn=" + usePairedConnections + ']');
                }

                srvr.idleTimeout(idleConnTimeout);

                return srvr;
            }
            catch (IgniteCheckedException e) {
                if (X.hasCause(e, SSLException.class))
                    throw new IgniteSpiException("Failed to create SSL context. SSL factory: "
                        + ignite.configuration().getSslContextFactory() + '.', e);

                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + locHost + ']');

                onException("Failed to bind to local port (will try next port within range) [port=" + port +
                    ", locHost=" + locHost + ']', e);
            }
        }

        // If free port wasn't found.
        throw new IgniteCheckedException("Failed to bind to any port within range [startPort=" + locPort +
            ", portRange=" + locPortRange + ", locHost=" + locHost + ']', lastEx);
    }

    /**
     * Creates new shared memory communication server.
     *
     * @return Server.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IpcSharedMemoryServerEndpoint resetShmemServer() throws IgniteCheckedException {
        if (boundTcpShmemPort >= 0)
            throw new IgniteCheckedException("Shared memory server was already created on port " + boundTcpShmemPort);

        if (shmemPort == -1 || U.isWindows())
            return null;

        IgniteCheckedException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        for (int port = shmemPort; port < shmemPort + locPortRange; port++) {
            try {
                IgniteConfiguration cfg = ignite.configuration();

                IpcSharedMemoryServerEndpoint srv =
                    new IpcSharedMemoryServerEndpoint(log, cfg.getNodeId(), igniteInstanceName, cfg.getWorkDirectory());

                srv.setPort(port);

                srv.omitOutOfResourcesWarning(true);

                srv.start();

                boundTcpShmemPort = port;

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled())
                    log.info("Successfully bound shared memory communication to TCP port [port=" + boundTcpShmemPort +
                        ", locHost=" + locHost + ']');

                return srv;
            }
            catch (IgniteCheckedException e) {
                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + locHost + ']');
            }
        }

        // If free port wasn't found.
        throw new IgniteCheckedException("Failed to bind shared memory communication to any port within range [startPort=" +
            locPort + ", portRange=" + locPortRange + ", locHost=" + locHost + ']', lastEx);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        assert stopping;

        unregisterMBean();

        // Stop TCP server.
        if (nioSrvr != null)
            nioSrvr.stop();

        U.cancel(shmemAcceptWorker);
        U.join(shmemAcceptWorker, log);

        U.cancel(shmemWorkers);
        U.join(shmemWorkers, log);

        shmemWorkers.clear();

        // Force closing on stop (safety).
        for (GridCommunicationClient[] clients0 : clients.values()) {
            for (GridCommunicationClient client : clients0) {
                if (client != null)
                    client.forceClose();
            }
        }

        for (GridFutureAdapter<GridCommunicationClient> fut : clientFuts.values()) {
            if (fut instanceof ConnectionRequestFuture) {
                // There's no way it would be done by itself at this point.
                fut.onDone(new IgniteSpiException("SPI is being stopped."));
            }
        }

        if (commWorker != null) {
            U.cancel(commWorker);
            U.join(commWorker, log);
        }

        // Clear resources.
        nioSrvr = null;
        commWorker = null;

        boundTcpPort = -1;

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

        if (connectGate != null)
            connectGate.stopped();

        getSpiContext().deregisterPorts();

        getSpiContext().removeLocalEventListener(discoLsnr);
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {
        connectGate.disconnected(reconnectFut);

        for (GridCommunicationClient[] clients0 : clients.values()) {
            for (GridCommunicationClient client : clients0) {
                if (client != null)
                    client.forceClose();
            }
        }

        IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Failed to connect client node disconnected.");

        for (GridFutureAdapter<GridCommunicationClient> clientFut : clientFuts.values())
            clientFut.onDone(err);

        recoveryDescs.clear();
        inRecDescs.clear();
        outRecDescs.clear();
    }

    /** {@inheritDoc} */
    @Override public void onClientReconnected(boolean clusterRestarted) {
        connectGate.reconnected();
    }

    /**
     * @param consistentId Consistent id of the node.
     * @param nodeId Left node ID.
     */
    void onNodeLeft(Object consistentId, UUID nodeId) {
        assert nodeId != null;

        metricsLsnr.onNodeLeft(consistentId);

        GridCommunicationClient[] clients0 = clients.remove(nodeId);

        if (clients0 != null) {
            for (GridCommunicationClient client : clients0) {
                if (client != null) {
                    if (log.isDebugEnabled())
                        log.debug("Forcing NIO client close since node has left [nodeId=" + nodeId +
                            ", client=" + client + ']');

                    client.forceClose();
                }
            }
        }
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
            nioSrvr,
            nodes);

        long timeout = failureDetectionTimeoutEnabled() ? failureDetectionTimeout() : connTimeout;

        if (log.isInfoEnabled())
            log.info("Start check connection process [nodeCnt=" + nodes.size() + ", timeout=" + timeout + ']');

        fut.init(timeout);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Sends given message to destination node. Note that characteristics of the
     * exchange such as durability, guaranteed delivery or error notification is
     * dependant on SPI implementation.
     *
     * @param node Destination node.
     * @param msg Message to send.
     * @param ackC Ack closure.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error during sending the message.
     *      Note that this is not guaranteed that failed communication will result
     *      in thrown exception as this is dependant on SPI implementation.
     */
    public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        sendMessage0(node, msg, ackC);
    }

    /**
     * @param node Destination node.
     * @param msg Message to send.
     * @param ackC Ack closure.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error during sending the message.
     *      Note that this is not guaranteed that failed communication will result
     *      in thrown exception as this is dependant on SPI implementation.
     */
    private void sendMessage0(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        assert node != null;
        assert msg != null;

        IgniteLogger log = this.log;

        if (log != null && log.isTraceEnabled())
            log.trace("Sending message with ack to node [node=" + node + ", msg=" + msg + ']');

        if (isLocalNodeDisconnected()) {
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
                    client = reserveClient(node, connIdx);

                    UUID nodeId = null;

                    if (!client.async())
                        nodeId = node.id();

                    retry = client.sendMessage(nodeId, msg, ackC);

                    client.release();

                    if (retry) {
                        removeNodeClient(node.id(), client);

                        ClusterNode node0 = getSpiContext().node(node.id());

                        if (node0 == null)
                            throw new ClusterTopologyCheckedException("Failed to send message to remote node " +
                                "(node has left the grid): " + node.id());
                    }

                    client = null;
                }
                while (retry);
            }
            catch (Throwable t) {
                if (stopping)
                    throw new IgniteSpiException("Node is stopping.", t);

                log.error("Failed to send message to remote node [node=" + node + ", msg=" + msg + ']', t);

                if (t instanceof Error)
                    throw (Error)t;

                if (t instanceof RuntimeException)
                    throw (RuntimeException)t;

                throw new IgniteSpiException("Failed to send message to remote node: " + node, t);
            }
            finally {
                if (client != null && removeNodeClient(node.id(), client))
                    client.forceClose();
            }
        }
    }

    /**
     * @return {@code True} if local node in disconnected state.
     */
    private boolean isLocalNodeDisconnected() {
        boolean disconnected = false;

        if (ignite instanceof IgniteKernal)
            disconnected = ((IgniteKernal)ignite).context().clientDisconnected();

        return disconnected;
    }

    /**
     * @param nodeId Node ID.
     * @param rmvClient Client to remove.
     * @return {@code True} if client was removed.
     */
    private boolean removeNodeClient(UUID nodeId, GridCommunicationClient rmvClient) {
        for (;;) {
            GridCommunicationClient[] curClients = clients.get(nodeId);

            if (curClients == null || rmvClient.connectionIndex() >= curClients.length || curClients[rmvClient.connectionIndex()] != rmvClient)
                return false;

            GridCommunicationClient[] newClients = Arrays.copyOf(curClients, curClients.length);

            newClients[rmvClient.connectionIndex()] = null;

            if (clients.replace(nodeId, curClients, newClients))
                return true;
        }
    }

    /**
     * @param node Node.
     * @param connIdx Connection index.
     * @param addClient Client to add.
     */
    private void addNodeClient(ClusterNode node, int connIdx, GridCommunicationClient addClient) {
        assert connectionsPerNode > 0 : connectionsPerNode;
        assert connIdx == addClient.connectionIndex() : addClient;

        if (connIdx >= connectionsPerNode) {
            assert !usePairedConnections(node);

            return;
        }

        for (;;) {
            GridCommunicationClient[] curClients = clients.get(node.id());

            assert curClients == null || curClients[connIdx] == null : "Client already created [node=" + node.id() +
                ", connIdx=" + connIdx +
                ", client=" + addClient +
                ", oldClient=" + curClients[connIdx] + ']';

            GridCommunicationClient[] newClients;

            if (curClients == null) {
                newClients = new GridCommunicationClient[connectionsPerNode];
                newClients[connIdx] = addClient;

                if (clients.putIfAbsent(node.id(), newClients) == null)
                    break;
            }
            else {
                newClients = curClients.clone();
                newClients[connIdx] = addClient;

                if (clients.replace(node.id(), curClients, newClients))
                    break;
            }
        }
    }

    /**
     * Returns existing or just created client to node.
     *
     * @param node Node to which client should be open.
     * @param connIdx Connection index.
     * @return The existing or just created client.
     * @throws IgniteCheckedException Thrown if any exception occurs.
     */
    private GridCommunicationClient reserveClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
        assert node != null;
        assert (connIdx >= 0 && connIdx < connectionsPerNode) || !usePairedConnections(node) : connIdx;

        if (getLocalNode().isClient()) {
            if (node.isClient()) {
                if (DISABLED_CLIENT_PORT.equals(node.attribute(createSpiAttributeName(ATTR_PORT))))
                    throw new IgniteSpiException("Cannot send message to the client node with no server socket opened.");
            }
        }

        UUID nodeId = node.id();

        while (true) {
            GridCommunicationClient[] curClients = clients.get(nodeId);

            GridCommunicationClient client = curClients != null && connIdx < curClients.length ?
                curClients[connIdx] : null;

            if (client == null) {
                if (stopping)
                    throw new IgniteSpiException("Node is stopping.");

                // Do not allow concurrent connects.
                GridFutureAdapter<GridCommunicationClient> fut = new ConnectFuture();

                ConnectionKey connKey = new ConnectionKey(nodeId, connIdx, -1);

                GridFutureAdapter<GridCommunicationClient> oldFut = clientFuts.putIfAbsent(connKey, fut);

                if (oldFut == null) {
                    try {
                        GridCommunicationClient[] curClients0 = clients.get(nodeId);

                        GridCommunicationClient client0 = curClients0 != null && connIdx < curClients0.length ?
                            curClients0[connIdx] : null;

                        if (client0 == null) {
                            client0 = createCommunicationClient(node, connIdx);

                            if (client0 != null) {
                                addNodeClient(node, connIdx, client0);

                                if (client0 instanceof GridTcpNioCommunicationClient) {
                                    GridTcpNioCommunicationClient tcpClient = ((GridTcpNioCommunicationClient)client0);

                                    if (tcpClient.session().closeTime() > 0 && removeNodeClient(nodeId, client0)) {
                                        if (log.isDebugEnabled())
                                            log.debug("Session was closed after client creation, will retry " +
                                                "[node=" + node + ", client=" + client0 + ']');

                                        client0 = null;
                                    }
                                }
                            }
                            else {
                                U.sleep(200);

                                if (getSpiContext().node(node.id()) == null)
                                    throw new ClusterTopologyCheckedException("Failed to send message " +
                                        "(node left topology): " + node);
                            }
                        }

                        fut.onDone(client0);
                    }
                    catch (NodeUnreachableException e) {
                        log.warning(e.getMessage());

                        fut = handleUnreachableNodeException(node, connIdx, fut, e);
                    }
                    catch (Throwable e) {
                        fut.onDone(e);

                        if (e instanceof IgniteTooManyOpenFilesException)
                            throw e;

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                    finally {
                        clientFuts.remove(connKey, fut);
                    }
                }
                else
                    fut = oldFut;

                client = fut.get();

                if (client == null) {
                    if (isLocalNodeDisconnected())
                        throw new IgniteCheckedException("Unable to create TCP client due to local node disconnecting.");
                    else
                        continue;
                }

                if (getSpiContext().node(nodeId) == null) {
                    if (removeNodeClient(nodeId, client))
                        client.forceClose();

                    throw new IgniteSpiException("Destination node is not in topology: " + node.id());
                }
            }

            assert connIdx == client.connectionIndex() : client;

            if (client.reserve())
                return client;
            else
                // Client has just been closed by idle worker. Help it and try again.
                removeNodeClient(nodeId, client);
        }
    }

    /**
     * Handles {@link NodeUnreachableException}. This means that the method will try to trigger client itself to open
     * connection. The only possible way of doing this is to use {@link #connectionRequestor}'s trigger and wait.
     * Specifics of triggers implementation technically should be considered unknown, but for now it's not true and we
     * expect that {@link NodeUnreachableException} won't be thrown in {@link IgniteDiscoveryThread}.
     *
     * @param node Node to open connection to.
     * @param connIdx Connection index.
     * @param fut Current future for opening connection.
     * @param e Curent exception.
     * @return New future that will return the client or error. {@code null} client is possible if newly opened
     *      connection has been closed by idle worker, at least that's what documentation says.
     * @throws IgniteCheckedException If trigerring failed or trigger is not configured.
     */
    private GridFutureAdapter<GridCommunicationClient> handleUnreachableNodeException(
        ClusterNode node,
        int connIdx,
        GridFutureAdapter<GridCommunicationClient> fut,
        NodeUnreachableException e
    ) throws IgniteCheckedException {
        if (connectionRequestor != null) {
            ConnectFuture fut0 = (ConnectFuture)fut;

            ConnectionRequestFuture triggerFut = new ConnectionRequestFuture();

            triggerFut.listen(f -> {
                try {
                    fut0.onDone(f.get());
                }
                catch (Throwable t) {
                    fut0.onDone(t);
                }
            });

            clientFuts.put(new ConnectionKey(node.id(), connIdx, -1), triggerFut);

            fut = triggerFut;

            try {
                connectionRequestor.request(node, connIdx);

                long failTimeout = failureDetectionTimeoutEnabled()
                    ? failureDetectionTimeout()
                    : getConnectTimeout();

                fut.get(failTimeout);
            }
            catch (IgniteCheckedException triggerException) {
                IgniteSpiException spiE = new IgniteSpiException(triggerException);

                spiE.addSuppressed(e);

                String msg = "Failed to wait for establishing inverse communication connection from node " + node;

                log.warning(msg, spiE);

                fut.onDone(spiE);

                throw spiE;
            }
        }
        else {
            fut.onDone(e);

            throw new IgniteCheckedException(e);
        }

        return fut;
    }

    /**
     * @param node Node to create client for.
     * @param connIdx Connection index.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private GridCommunicationClient createCommunicationClient(ClusterNode node, int connIdx)
        throws IgniteCheckedException {
        assert node != null;

        Integer shmemPort = node.attribute(createSpiAttributeName(ATTR_SHMEM_PORT));

        ClusterNode locNode = getSpiContext().localNode();

        if (locNode == null)
            throw new IgniteCheckedException("Failed to create NIO client (local node is stopping)");

        if (log.isDebugEnabled())
            log.debug("Creating NIO client to node: " + node);

        // If remote node has shared memory server enabled and has the same set of MACs
        // then we are likely to run on the same host and shared memory communication could be tried.
        if (shmemPort != null && U.sameMacs(locNode, node)) {
            try {
                // https://issues.apache.org/jira/browse/IGNITE-11126 Rework failure detection logic.
                GridCommunicationClient client = createShmemClient(
                    node,
                    connIdx,
                    shmemPort);

                if (log.isDebugEnabled())
                    log.debug("Shmem client created: " + client);

                return client;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(IpcOutOfSystemResourcesException.class))
                    // Has cause or is itself the IpcOutOfSystemResourcesException.
                    LT.warn(log, OUT_OF_RESOURCES_TCP_MSG);
                else if (getSpiContext().node(node.id()) != null)
                    LT.warn(log, e.getMessage());
                else if (log.isDebugEnabled())
                    log.debug("Failed to establish shared memory connection with local node (node has left): " +
                        node.id());
            }
        }

        final long start = System.currentTimeMillis();

        GridCommunicationClient client = createTcpClient(node, connIdx);

        final long time = System.currentTimeMillis() - start;

        if (time > CONNECTION_ESTABLISH_THRESHOLD_MS) {
            if (log.isInfoEnabled())
                log.info("TCP client created [client=" + clientString(client, node) + ", duration=" + time + "ms]");
        }
        else if (log.isDebugEnabled())
            log.debug("TCP client created [client=" + clientString(client, node) + ", duration=" + time + "ms]");

        return client;
    }

    /**
     * Returns the string representation of client with protection from null client value. If the client if null,
     * string representation is built from cluster node.
     *
     * @param client communication client
     * @param node cluster node to which the client tried to establish a connection
     * @return string representation of client
     * @throws IgniteCheckedException if failed
     */
    private String clientString(GridCommunicationClient client, ClusterNode node) throws IgniteCheckedException {
        if (client == null) {
            assert node != null;

            StringJoiner joiner = new StringJoiner(", ", "null, node addrs=[", "]");

            for (InetSocketAddress addr : nodeAddresses(node))
                joiner.add(addr.toString());

            return joiner.toString();
        }
        else
            return client.toString();
    }

    /**
     * @param node Node.
     * @param port Port.
     * @param connIdx Connection index.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private GridCommunicationClient createShmemClient(ClusterNode node,
        int connIdx,
        Integer port) throws IgniteCheckedException {
        int attempt = 1;

        int connectAttempts = 1;

        long connTimeout0 = connTimeout;

        IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(this,
            !node.isClient());

        while (true) {
            GridCommunicationClient client;

            try {
                client = new GridShmemCommunicationClient(
                    connIdx,
                    metricsLsnr.metricRegistry(),
                    port,
                    timeoutHelper.nextTimeoutChunk(connTimeout),
                    log,
                    getSpiContext().messageFormatter());
            }
            catch (IgniteCheckedException e) {
                if (timeoutHelper.checkFailureTimeoutReached(e))
                    throw e;

                // Reconnect for the second time, if connection is not established.
                if (connectAttempts < 2 && X.hasCause(e, ConnectException.class)) {
                    connectAttempts++;

                    continue;
                }

                throw e;
            }

            try {
                safeShmemHandshake(client, node.id(), timeoutHelper.nextTimeoutChunk(connTimeout0));
            }
            catch (IgniteSpiOperationTimeoutException e) {
                client.forceClose();

                if (failureDetectionTimeoutEnabled() && timeoutHelper.checkFailureTimeoutReached(e)) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake timed out (failure threshold reached) [failureDetectionTimeout=" +
                            failureDetectionTimeout() + ", err=" + e.getMessage() + ", client=" + client + ']');

                    throw e;
                }

                assert !failureDetectionTimeoutEnabled();

                if (log.isDebugEnabled())
                    log.debug("Handshake timed out (will retry with increased timeout) [timeout=" + connTimeout0 +
                        ", err=" + e.getMessage() + ", client=" + client + ']');

                if (attempt == reconCnt || connTimeout0 > maxConnTimeout) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake timedout (will stop attempts to perform the handshake) " +
                            "[timeout=" + connTimeout0 + ", maxConnTimeout=" + maxConnTimeout +
                            ", attempt=" + attempt + ", reconCnt=" + reconCnt +
                            ", err=" + e.getMessage() + ", client=" + client + ']');

                    throw e;
                }
                else {
                    attempt++;

                    connTimeout0 *= 2;

                    continue;
                }
            }
            catch (IgniteCheckedException | RuntimeException | Error e) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Caught exception (will close client) [err=" + e.getMessage() + ", client=" + client + ']');

                client.forceClose();

                throw e;
            }

            return client;
        }
    }

    /**
     * Checks client message queue size and initiates client drop if message queue size exceeds the configured limit.
     *
     * @param ses Node communication session.
     * @param msgQueueSize Message queue size.
     */
    private void checkClientQueueSize(GridNioSession ses, int msgQueueSize) {
        if (slowClientQueueLimit > 0 && msgQueueSize > slowClientQueueLimit) {
            ConnectionKey id = ses.meta(CONN_IDX_META);

            if (id != null) {
                ClusterNode node = getSpiContext().node(id.nodeId());

                if (node != null && node.isClient()) {
                    String msg = "Client node outbound message queue size exceeded slowClientQueueLimit, " +
                        "the client will be dropped " +
                        "(consider changing 'slowClientQueueLimit' configuration property) " +
                        "[srvNode=" + getSpiContext().localNode().id() +
                        ", clientNode=" + node +
                        ", slowClientQueueLimit=" + slowClientQueueLimit + ']';

                    U.quietAndWarn(log, msg);

                    getSpiContext().failNode(id.nodeId(), msg);
                }
            }
        }
    }

    /**
     * @param node Node.
     * @return Node addresses.
     * @throws IgniteCheckedException If failed.
     */
    private Collection<InetSocketAddress> nodeAddresses(ClusterNode node) throws IgniteCheckedException {
        return nodeAddresses(node, filterReachableAddresses);
    }

    /**
     * @param node Node.
     * @param filterReachableAddresses Filter addresses flag.
     * @return Node addresses.
     * @throws IgniteCheckedException If node does not have addresses.
     */
    public Collection<InetSocketAddress> nodeAddresses(ClusterNode node, boolean filterReachableAddresses)
        throws IgniteCheckedException {
        Collection<String> rmtAddrs0 = node.attribute(createSpiAttributeName(ATTR_ADDRS));
        Collection<String> rmtHostNames0 = node.attribute(createSpiAttributeName(ATTR_HOST_NAMES));
        Integer boundPort = node.attribute(createSpiAttributeName(ATTR_PORT));
        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        boolean isRmtAddrsExist = (!F.isEmpty(rmtAddrs0) && boundPort != null);
        boolean isExtAddrsExist = !F.isEmpty(extAddrs);

        if (!isRmtAddrsExist && !isExtAddrsExist)
            throw new IgniteCheckedException("Failed to send message to the destination node. Node doesn't have any " +
                "TCP communication addresses or mapped external addresses. Check configuration and make sure " +
                "that you use the same communication SPI on all nodes. Remote node id: " + node.id());

        LinkedHashSet<InetSocketAddress> addrs;

        // Try to connect first on bound addresses.
        if (isRmtAddrsExist) {
            List<InetSocketAddress> addrs0 = new ArrayList<>(U.toSocketAddresses(rmtAddrs0, rmtHostNames0, boundPort));

            boolean sameHost = U.sameMacs(getSpiContext().localNode(), node);

            addrs0.sort(U.inetAddressesComparator(sameHost));

            addrs = new LinkedHashSet<>(addrs0);
        }
        else
            addrs = new LinkedHashSet<>();

        // Then on mapped external addresses.
        if (isExtAddrsExist)
            addrs.addAll(extAddrs);

        if (log.isDebugEnabled())
            log.debug("Addresses resolved from attributes [rmtNode=" + node.id() + ", addrs=" + addrs +
                ", isRmtAddrsExist=" + isRmtAddrsExist + ']');

        if (filterReachableAddresses) {
            Set<InetAddress> allInetAddrs = U.newHashSet(addrs.size());

            for (InetSocketAddress addr : addrs) {
                // Skip unresolved as addr.getAddress() can return null.
                if (!addr.isUnresolved())
                    allInetAddrs.add(addr.getAddress());
            }

            List<InetAddress> reachableInetAddrs = U.filterReachable(allInetAddrs);

            if (reachableInetAddrs.size() < allInetAddrs.size()) {
                LinkedHashSet<InetSocketAddress> addrs0 = U.newLinkedHashSet(addrs.size());

                List<InetSocketAddress> unreachableInetAddr = new ArrayList<>(allInetAddrs.size() - reachableInetAddrs.size());

                for (InetSocketAddress addr : addrs) {
                    if (reachableInetAddrs.contains(addr.getAddress()))
                        addrs0.add(addr);
                    else
                        unreachableInetAddr.add(addr);
                }

                addrs0.addAll(unreachableInetAddr);

                addrs = addrs0;
            }

            if (log.isDebugEnabled())
                log.debug("Addresses to connect for node [rmtNode=" + node.id() + ", addrs=" + addrs + ']');
        }

        return addrs;
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
        GridNioSession ses = createNioSession(node, connIdx);

        return ses == null ?
            null : new GridTcpNioCommunicationClient(connIdx, ses, log);
    }

    /**
     * Returns the established TCP/IP connection between the current node and remote server. A handshake process of
     * negotiation between two communicating nodes will be performed before the {@link GridNioSession} created.
     * <p>
     *     The handshaking process contains of these steps:
     *
     *     <ol>
     *         <li>The local node opens a new {@link SocketChannel} in the <em>blocking</em> mode.</li>
     *         <li>The local node calls {@link SocketChannel#connect(SocketAddress)} to remote node.</li>
     *         <li>The remote GridNioAcceptWorker thread accepts new connection.</li>
     *         <li>The remote node sends back the {@link NodeIdMessage}.</li>
     *         <li>The local node reads NodeIdMessage from created channel.</li>
     *         <li>The local node sends the {@link HandshakeMessage2} to remote.</li>
     *         <li>The remote node processes {@link HandshakeMessage2} in {@link GridNioServerListener#onMessage(GridNioSession, Object)}.</li>
     *         <li>The remote node sends back the {@link RecoveryLastReceivedMessage}.</li>
     *     </ol>
     *
     *     The handshaking process ends.
     * </p>
     * <p>
     *     <em>Note.</em> The {@link HandshakeTimeoutObject} is created to control execution timeout during the
     *     whole handshaking process.
     * </p>
     *
     * @param node Remote node identifier to connect with.
     * @param connIdx Connection index based on configured {@link ConnectionPolicy}.
     * @return A {@link GridNioSession} connection representation.
     * @throws IgniteCheckedException If establish connection fails.
     */
    private GridNioSession createNioSession(ClusterNode node, int connIdx) throws IgniteCheckedException {
        boolean locNodeIsSrv = !getLocalNode().isClient() && !getLocalNode().isDaemon();

        if (!(Thread.currentThread() instanceof IgniteDiscoveryThread) && locNodeIsSrv) {
            if (node.isClient() && forceClientToServerConnections(node)) {
                String msg = "Failed to connect to node " + node.id() + " because it is started" +
                    " in 'forceClientToServerConnections' mode; inverse connection will be requested.";

                throw new NodeUnreachableException(msg);
            }
        }

        Collection<InetSocketAddress> addrs = nodeAddresses(node);

        GridNioSession ses = null;
        IgniteCheckedException errs = null;

        long totalTimeout;

        if (failureDetectionTimeoutEnabled())
            totalTimeout = node.isClient() ? clientFailureDetectionTimeout() : failureDetectionTimeout();
        else {
            totalTimeout = ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(
                connTimeout,
                maxConnTimeout,
                reconCnt
            );
        }

        Set<InetSocketAddress> failedAddrsSet = new HashSet<>();
        int skippedAddrs = 0;

        for (InetSocketAddress addr : addrs) {
            if (addr.isUnresolved()) {
                failedAddrsSet.add(addr);

                continue;
            }

            TimeoutStrategy connTimeoutStgy = new ExponentialBackoffTimeoutStrategy(
                totalTimeout,
                failureDetectionTimeoutEnabled() ? DFLT_INITIAL_TIMEOUT : connTimeout,
                maxConnTimeout
            );

            while (ses == null) { // Reconnection on handshake timeout.
                if (stopping)
                    throw new IgniteSpiException("Node is stopping.");

                if (isLocalNodeAddress(addr)) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping local address [addr=" + addr +
                            ", locAddrs=" + node.attribute(createSpiAttributeName(ATTR_ADDRS)) +
                            ", node=" + node + ']');

                    skippedAddrs++;

                    break;
                }

                long timeout = 0;

                connectGate.enter();

                try {
                    if (getSpiContext().node(node.id()) == null)
                        throw new ClusterTopologyCheckedException("Failed to send message (node left topology): " + node);

                    SocketChannel ch = openSocketChannel();

                    ch.configureBlocking(true);

                    ch.socket().setTcpNoDelay(tcpNoDelay);
                    ch.socket().setKeepAlive(true);

                    if (sockRcvBuf > 0)
                        ch.socket().setReceiveBufferSize(sockRcvBuf);

                    if (sockSndBuf > 0)
                        ch.socket().setSendBufferSize(sockSndBuf);

                    ConnectionKey connKey = new ConnectionKey(node.id(), connIdx, -1);

                    GridNioRecoveryDescriptor recoveryDesc = outRecoveryDescriptor(node, connKey);

                    assert recoveryDesc != null :
                        "Recovery descriptor not found [connKey=" + connKey + ", rmtNode=" + node.id() + ']';

                    if (!recoveryDesc.reserve()) {
                        U.closeQuiet(ch);

                        // Ensure the session is closed.
                        GridNioSession sesFromRecovery = recoveryDesc.session();

                        if (sesFromRecovery != null) {
                            while (sesFromRecovery.closeTime() == 0)
                                sesFromRecovery.close();
                        }

                        return null;
                    }

                    long rcvCnt;

                    Map<Integer, Object> meta = new HashMap<>();

                    GridSslMeta sslMeta = null;

                    try {
                        timeout = connTimeoutStgy.nextTimeout();

                        ch.socket().connect(addr, (int) timeout);

                        if (getSpiContext().node(node.id()) == null)
                            throw new ClusterTopologyCheckedException("Failed to send message (node left topology): " + node);

                        if (isSslEnabled()) {
                            meta.put(SSL_META.ordinal(), sslMeta = new GridSslMeta());

                            SSLEngine sslEngine = ignite.configuration().getSslContextFactory().create().createSSLEngine();

                            sslEngine.setUseClientMode(true);

                            sslMeta.sslEngine(sslEngine);
                        }

                        ClusterNode locNode = getLocalNode();

                        if (locNode == null)
                            throw new IgniteCheckedException("Local node has not been started or " +
                                "fully initialized [isStopping=" + getSpiContext().isStopping() + ']');

                        timeout = connTimeoutStgy.nextTimeout(timeout);

                        rcvCnt = safeTcpHandshake(ch,
                            node.id(),
                            timeout,
                            sslMeta,
                            new HandshakeMessage2(locNode.id(),
                                recoveryDesc.incrementConnectCount(),
                                recoveryDesc.received(),
                                connIdx));

                        if (rcvCnt == ALREADY_CONNECTED)
                            return null;
                        else if (rcvCnt == NODE_STOPPING)
                            throw new ClusterTopologyCheckedException("Remote node started stop procedure: " + node.id());
                        else if (rcvCnt == UNKNOWN_NODE)
                            throw new ClusterTopologyCheckedException("Remote node does not observe current node " +
                                "in topology : " + node.id());
                        else if (rcvCnt == NEED_WAIT) {
                            //check that failure timeout will be reached after sleep(outOfTopDelay).
                            if (connTimeoutStgy.checkTimeout(DFLT_NEED_WAIT_DELAY)) {
                                U.warn(log, "Handshake NEED_WAIT timed out (will stop attempts to perform the handshake) " +
                                    "[node=" + node.id() +
                                    ", connTimeoutStgy=" + connTimeoutStgy +
                                    ", addr=" + addr +
                                    ", failureDetectionTimeoutEnabled=" + failureDetectionTimeoutEnabled() +
                                    ", timeout=" + timeout + ']');

                                throw new ClusterTopologyCheckedException("Failed to connect to node " +
                                    "(current or target node is out of topology on target node within timeout). " +
                                        "Make sure that each ComputeTask and cache Transaction has a timeout set " +
                                        "in order to prevent parties from waiting forever in case of network issues " +
                                        "[nodeId=" + node.id() + ", addrs=" + addrs + ']');
                            }
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("NEED_WAIT received, handshake after delay [node = "
                                        + node + ", outOfTopologyDelay = " + DFLT_NEED_WAIT_DELAY + "ms]");

                                U.sleep(DFLT_NEED_WAIT_DELAY);

                                continue;
                            }
                        }
                        else if (rcvCnt < 0)
                            throw new IgniteCheckedException("Unsupported negative receivedCount [rcvCnt=" + rcvCnt +
                                ", senderNode=" + node + ']');

                        recoveryDesc.onHandshake(rcvCnt);

                        meta.put(CONSISTENT_ID_META, node.consistentId());
                        meta.put(CONN_IDX_META, connKey);
                        meta.put(GridNioServer.RECOVERY_DESC_META_KEY, recoveryDesc);

                        ses = nioSrvr.createSession(ch, meta, false, null).get();
                    }
                    finally {
                        if (ses == null) {
                            U.closeQuiet(ch);

                            if (recoveryDesc != null)
                                recoveryDesc.release();
                        }
                    }
                }
                catch (IgniteSpiOperationTimeoutException e) { // Handshake is timed out.
                    if (ses != null) {
                        ses.close();

                        ses = null;
                    }

                    onException("Handshake timed out (will retry with increased timeout) [connTimeoutStrategy=" + connTimeoutStgy +
                        ", addr=" + addr + ']', e);

                    if (log.isDebugEnabled())
                        log.debug("Handshake timed out (will retry with increased timeout) [connTimeoutStrategy=" + connTimeoutStgy +
                                ", addr=" + addr + ", err=" + e + ']'
                        );

                    if (connTimeoutStgy.checkTimeout()) {
                        U.warn(log, "Handshake timed out (will stop attempts to perform the handshake) " +
                            "[node=" + node.id() + ", connTimeoutStrategy=" + connTimeoutStgy +
                            ", err=" + e.getMessage() + ", addr=" + addr +
                            ", failureDetectionTimeoutEnabled=" + failureDetectionTimeoutEnabled() +
                            ", timeout=" + timeout + ']');

                        String msg = "Failed to connect to node (is node still alive?). " +
                            "Make sure that each ComputeTask and cache Transaction has a timeout set " +
                            "in order to prevent parties from waiting forever in case of network issues " +
                            "[nodeId=" + node.id() + ", addrs=" + addrs + ']';

                        if (errs == null)
                            errs = new IgniteCheckedException(msg, e);
                        else
                            errs.addSuppressed(new IgniteCheckedException(msg, e));

                        break;
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    throw e;
                }
                catch (Exception e) {
                    // Most probably IO error on socket connect or handshake.
                    if (ses != null) {
                        ses.close();

                        ses = null;
                    }

                    onException("Client creation failed [addr=" + addr + ", err=" + e + ']', e);

                    if (log.isDebugEnabled())
                        log.debug("Client creation failed [addr=" + addr + ", err=" + e + ']');

                    if (X.hasCause(e, "Too many open files", SocketException.class))
                        throw new IgniteTooManyOpenFilesException(e);

                    // check if timeout occured in case of unrecoverable exception
                    if (connTimeoutStgy.checkTimeout()) {
                        U.warn(log, "Connection timed out (will stop attempts to perform the connect) " +
                                "[node=" + node.id() + ", connTimeoutStgy=" + connTimeoutStgy +
                                ", failureDetectionTimeoutEnabled=" + failureDetectionTimeoutEnabled() +
                                ", timeout=" + timeout +
                                ", err=" + e.getMessage() + ", addr=" + addr + ']');

                        String msg = "Failed to connect to node (is node still alive?). " +
                                "Make sure that each ComputeTask and cache Transaction has a timeout set " +
                                "in order to prevent parties from waiting forever in case of network issues " +
                                "[nodeId=" + node.id() + ", addrs=" + addrs + ']';

                        if (errs == null)
                            errs = new IgniteCheckedException(msg, e);
                        else
                            errs.addSuppressed(new IgniteCheckedException(msg, e));

                        break;
                    }

                    // Inverse communication protocol works only for client nodes.
                    if (node.isClient() && isNodeUnreachableException(e))
                        failedAddrsSet.add(addr);

                    if (isRecoverableException(e))
                        U.sleep(DFLT_RECONNECT_DELAY);
                    else {
                        String msg = "Failed to connect to node due to unrecoverable exception (is node still alive?). " +
                                "Make sure that each ComputeTask and cache Transaction has a timeout set " +
                                "in order to prevent parties from waiting forever in case of network issues " +
                                "[nodeId=" + node.id() + ", addrs=" + addrs + ", err= " + e + ']';

                        if (errs == null)
                            errs = new IgniteCheckedException(msg, e);
                        else
                            errs.addSuppressed(new IgniteCheckedException(msg, e));

                        break;
                    }
                }
                finally {
                    connectGate.leave();
                }

                CommunicationWorker commWorker0 = commWorker;

                if (commWorker0 != null && commWorker0.runner() == Thread.currentThread())
                    commWorker0.updateHeartbeat();
            }

            if (ses != null)
                break;
        }

        if (ses == null) {
            if (!(Thread.currentThread() instanceof IgniteDiscoveryThread) && locNodeIsSrv) {
                if (node.isClient() && (addrs.size() - skippedAddrs == failedAddrsSet.size())) {
                    String msg = "Failed to connect to all addresses of node " + node.id() + ": " + failedAddrsSet +
                        "; inverse connection will be requested.";

                    throw new NodeUnreachableException(msg);
                }
            }

            processSessionCreationError(node, addrs, errs == null ? new IgniteCheckedException("No session found") : errs);
        }

        return ses;
    }

    /**
     * Opens a socket channel.
     *
     * @return A new socket channel.
     * @throws IOException If an I/O error occurs.
     */
    protected SocketChannel openSocketChannel() throws IOException {
        return SocketChannel.open();
    }

    /**
     * Closing connections to node.
     * NOTE: It is recommended only for tests.
     *
     * @param nodeId Node for which to close connections.
     * @throws IgniteCheckedException If occurs.
     */
    void closeConnections(UUID nodeId) throws IgniteCheckedException {
        GridCommunicationClient[] clients = this.clients.remove(nodeId);

        if (nonNull(clients)) {
            for (GridCommunicationClient client : clients)
                client.forceClose();
        }

        for (ConnectionKey connKey : clientFuts.keySet()) {
            if (!nodeId.equals(connKey.nodeId()))
                continue;

            GridFutureAdapter<GridCommunicationClient> fut = clientFuts.remove(connKey);

            if (nonNull(fut))
                fut.get().forceClose();
        }
    }

    /**
     * Check is passed  socket address belong to current node. This method should return true only if the passed
     * in address represent an address which will result in a connection to the local node.
     *
     * @param addr address to check.
     * @return true if passed address belongs to local node, otherwise false.
     */
    private boolean isLocalNodeAddress(InetSocketAddress addr) {
        return addr.getPort() == boundTcpPort
            && (locHost.equals(addr.getAddress())
                || addr.getAddress().isAnyLocalAddress()
                || (locHost.isAnyLocalAddress() && U.isLocalAddress(addr.getAddress())));
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
        assert errs != null;

        boolean commErrResolve = false;

        IgniteSpiContext ctx = getSpiContext();

        if (isRecoverableException(errs) && ctx.communicationFailureResolveSupported()) {
            commErrResolve = true;

            ctx.resolveCommunicationFailure(node, errs);
        }

        if (!commErrResolve && enableForcibleNodeKill) {
            if (ctx.node(node.id()) != null
                && node.isClient()
                && !getLocalNode().isClient()
                && isRecoverableException(errs)
            ) {
                // Only server can fail client for now, as in TcpDiscovery resolveCommunicationFailure() is not supported.
                String msg = "TcpCommunicationSpi failed to establish connection to node, node will be dropped from " +
                    "cluster [" + "rmtNode=" + node + ']';

                if (enableTroubleshootingLog)
                    U.error(log, msg, errs);
                else
                    U.warn(log, msg);

                ctx.failNode(node.id(), "TcpCommunicationSpi failed to establish connection to node [" +
                    "rmtNode=" + node +
                    ", errs=" + errs +
                    ", connectErrs=" + X.getSuppressedList(errs) + ']');
            }
        }

        throw errs;
    }

    /**
     * @param errs Error.
     * @return {@code True} if error was caused by some connection IO error or IgniteCheckedException due to timeout.
     */
    private boolean isRecoverableException(Exception errs) {
        return X.hasCause(
            errs,
            IOException.class,
            HandshakeException.class,
            IgniteSpiOperationTimeoutException.class
        );
    }

    /**
     * Checks if exception indicates that client is unreachable.
     *
     * @param e Exception to check.
     * @return {@code True} if exception shows that client is unreachable, {@code false} otherwise.
     */
    private boolean isNodeUnreachableException(Exception e) {
        return e instanceof SocketTimeoutException;
    }

    /** */
    private IgniteSpiOperationTimeoutException handshakeTimeoutException() {
        return new IgniteSpiOperationTimeoutException("Failed to perform handshake due to timeout " +
            "(consider increasing 'connectionTimeout' configuration property).");
    }

    /**
     * Performs handshake in timeout-safe way.
     *
     * @param client Client.
     * @param rmtNodeId Remote node.
     * @param timeout Timeout for handshake.
     * @throws IgniteCheckedException If handshake failed or wasn't completed withing timeout.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void safeShmemHandshake(
        GridCommunicationClient client,
        UUID rmtNodeId,
        long timeout
    ) throws IgniteCheckedException {
        HandshakeTimeoutObject<GridCommunicationClient> obj = new HandshakeTimeoutObject<>(client,
            U.currentTimeMillis() + timeout);

        addTimeoutObject(obj);

        try {
            client.doHandshake(new HandshakeClosure(rmtNodeId));
        }
        finally {
            if (obj.cancel())
                removeTimeoutObject(obj);
            else
                throw handshakeTimeoutException();
        }
    }

    /**
     * Performs handshake in timeout-safe way.
     *
     * @param ch Socket channel.
     * @param rmtNodeId Remote node.
     * @param timeout Timeout for handshake.
     * @param sslMeta Session meta.
     * @param msg {@link HandshakeMessage} or {@link HandshakeMessage2} to send.
     * @return Handshake response.
     * @throws IgniteCheckedException If handshake failed or wasn't completed withing timeout.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private long safeTcpHandshake(
        SocketChannel ch,
        UUID rmtNodeId,
        long timeout,
        GridSslMeta sslMeta,
        HandshakeMessage msg
    ) throws IgniteCheckedException {
        HandshakeTimeoutObject obj = new HandshakeTimeoutObject<>(ch, U.currentTimeMillis() + timeout);

        addTimeoutObject(obj);

        long rcvCnt;

        try {
            BlockingSslHandler sslHnd = null;

            ByteBuffer buf;

            // Step 1. Get remote node response with the remote nodeId value.
            if (isSslEnabled()) {
                assert sslMeta != null;

                sslHnd = new BlockingSslHandler(sslMeta.sslEngine(), ch, directBuf, ByteOrder.LITTLE_ENDIAN, log);

                if (!sslHnd.handshake())
                    throw new HandshakeException("SSL handshake is not completed.");

                ByteBuffer handBuff = sslHnd.applicationBuffer();

                if (handBuff.remaining() >= DIRECT_TYPE_SIZE) {
                    short msgType = makeMessageType(handBuff.get(0), handBuff.get(1));

                    if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                        return NEED_WAIT;
                }

                if (handBuff.remaining() < NodeIdMessage.MESSAGE_FULL_SIZE) {
                    buf = ByteBuffer.allocate(1000);

                    int read = ch.read(buf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node ID (connection closed).");

                    buf.flip();

                    buf = sslHnd.decode(buf);

                    if (handBuff.remaining() >= DIRECT_TYPE_SIZE) {
                        short msgType = makeMessageType(handBuff.get(0), handBuff.get(1));

                        if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                            return NEED_WAIT;
                    }
                }
                else
                    buf = handBuff;
            }
            else {
                buf = ByteBuffer.allocate(NodeIdMessage.MESSAGE_FULL_SIZE);

                for (int i = 0; i < NodeIdMessage.MESSAGE_FULL_SIZE; ) {
                    int read = ch.read(buf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node ID (connection closed).");

                    if (read >= DIRECT_TYPE_SIZE) {
                        short msgType = makeMessageType(buf.get(0), buf.get(1));

                        if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                            return NEED_WAIT;
                    }

                    i += read;
                }
            }

            UUID rmtNodeId0 = U.bytesToUuid(buf.array(), Message.DIRECT_TYPE_SIZE);

            if (!rmtNodeId.equals(rmtNodeId0))
                throw new HandshakeException("Remote node ID is not as expected [expected=" + rmtNodeId +
                    ", rcvd=" + rmtNodeId0 + ']');
            else if (log.isDebugEnabled())
                log.debug("Received remote node ID: " + rmtNodeId0);

            if (isSslEnabled()) {
                assert sslHnd != null;

                U.writeFully(ch, sslHnd.encrypt(ByteBuffer.wrap(U.IGNITE_HEADER)));
            }
            else
                U.writeFully(ch, ByteBuffer.wrap(U.IGNITE_HEADER));

            // Step 2. Prepare Handshake message to send to the remote node.
            if (log.isDebugEnabled())
                log.debug("Writing handshake message [rmtNode=" + rmtNodeId + ", msg=" + msg + ']');

            buf = ByteBuffer.allocate(msg.getMessageSize());

            buf.order(ByteOrder.LITTLE_ENDIAN);

            boolean written = msg.writeTo(buf, null);

            assert written;

            buf.flip();

            if (isSslEnabled()) {
                assert sslHnd != null;

                U.writeFully(ch, sslHnd.encrypt(buf));
            }
            else
                U.writeFully(ch, buf);

            if (log.isDebugEnabled())
                log.debug("Waiting for handshake [rmtNode=" + rmtNodeId + ']');

            // Step 3. Waiting for response from the remote node with their receive count message.
            if (isSslEnabled()) {
                assert sslHnd != null;

                buf = ByteBuffer.allocate(1000);
                buf.order(ByteOrder.LITTLE_ENDIAN);

                ByteBuffer decode = ByteBuffer.allocate(2 * buf.capacity());
                decode.order(ByteOrder.LITTLE_ENDIAN);

                for (int i = 0; i < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                    int read = ch.read(buf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node recovery handshake " +
                            "(connection closed).");

                    buf.flip();

                    ByteBuffer decode0 = sslHnd.decode(buf);

                    i += decode0.remaining();

                    decode = appendAndResizeIfNeeded(decode, decode0);

                    buf.clear();
                }

                decode.flip();

                rcvCnt = decode.getLong(Message.DIRECT_TYPE_SIZE);

                if (decode.limit() > RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE) {
                    decode.position(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

                    sslMeta.decodedBuffer(decode);
                }

                ByteBuffer inBuf = sslHnd.inputBuffer();

                if (inBuf.position() > 0)
                    sslMeta.encodedBuffer(inBuf);
            }
            else {
                buf = ByteBuffer.allocate(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

                buf.order(ByteOrder.LITTLE_ENDIAN);

                for (int i = 0; i < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                    int read = ch.read(buf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node recovery handshake " +
                            "(connection closed).");

                    i += read;
                }

                rcvCnt = buf.getLong(Message.DIRECT_TYPE_SIZE);
            }

            if (log.isDebugEnabled())
                log.debug("Received handshake message [rmtNode=" + rmtNodeId + ", rcvCnt=" + rcvCnt + ']');

            if (rcvCnt == -1) {
                if (log.isDebugEnabled())
                    log.debug("Connection rejected, will retry client creation [rmtNode=" + rmtNodeId + ']');
            }
        }
        catch (IOException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to read from channel: " + e);

            throw new IgniteCheckedException("Failed to read from channel.", e);
        }
        finally {
            if (obj.cancel())
                removeTimeoutObject(obj);
            else
                throw handshakeTimeoutException();
        }

        return rcvCnt;
    }

    /**
     * @param sndId Sender ID.
     * @param msg Communication message.
     * @param msgC Closure to call when message processing finished.
     */
    protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
        CommunicationListener<Message> lsnr = this.lsnr;

        MTC.span().addLog(() -> "Communication listeners notified");

        if (lsnr != null)
            // Notify listener of a new message.
            lsnr.onMessage(sndId, msg, msgC);
        else if (log.isDebugEnabled())
            log.debug("Received communication message without any registered listeners (will ignore, " +
                "is node stopping?) [senderNodeId=" + sndId + ", msg=" + msg + ']');
    }

    /**
     * @param nodeId The remote node id.
     * @param channel The configured channel to notify listeners with.
     * @param initMsg Channel initialization message with additional channel params.
     */
    private void notifyChannelEvtListener(UUID nodeId, Channel channel, Message initMsg) {
        if (log.isDebugEnabled())
            log.debug("Notify appropriate listeners due to a new channel opened: " + channel);

        CommunicationListener<Message> lsnr0 = lsnr;

        if (lsnr0 instanceof CommunicationListenerEx)
            ((CommunicationListenerEx<Message>)lsnr0).onChannelOpened(nodeId, initMsg, channel);
    }

    /**
     * @param target Target buffer to append to.
     * @param src Source buffer to get data.
     * @return Original or expanded buffer.
     */
    private ByteBuffer appendAndResizeIfNeeded(ByteBuffer target, ByteBuffer src) {
        if (target.remaining() < src.remaining()) {
            int newSize = Math.max(target.capacity() * 2, target.capacity() + src.remaining());

            ByteBuffer tmp = ByteBuffer.allocate(newSize);

            tmp.order(target.order());

            target.flip();

            tmp.put(target);

            target = tmp;
        }

        target.put(src);

        return target;
    }

    /**
     * Stops service threads to simulate node failure.
     *
     * FOR TEST PURPOSES ONLY!!!
     */
    @TestOnly
    public void simulateNodeFailure() {
        if (nioSrvr != null)
            nioSrvr.stop();

        if (commWorker != null)
            U.interrupt(commWorker.runner());

        U.join(commWorker, log);

        for (GridCommunicationClient[] clients0 : clients.values()) {
            for (GridCommunicationClient client : clients0) {
                if (client != null)
                    client.forceClose();
            }
        }
    }

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for outgoing connection.
     */
    private GridNioRecoveryDescriptor outRecoveryDescriptor(ClusterNode node, ConnectionKey key) {
        if (usePairedConnections(node))
            return recoveryDescriptor(outRecDescs, true, node, key);
        else
            return recoveryDescriptor(recoveryDescs, false, node, key);
    }

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for incoming connection.
     */
    private GridNioRecoveryDescriptor inRecoveryDescriptor(ClusterNode node, ConnectionKey key) {
        if (usePairedConnections(node))
            return recoveryDescriptor(inRecDescs, true, node, key);
        else
            return recoveryDescriptor(recoveryDescs, false, node, key);
    }

    /**
     * @param node Node.
     * @return {@code True} if can use in/out connection pair for communication.
     */
    private boolean usePairedConnections(ClusterNode node) {
        if (usePairedConnections) {
            Boolean attr = node.attribute(createSpiAttributeName(ATTR_PAIRED_CONN));

            return attr != null && attr;
        }

        return false;
    }

    /**
     * @param key The connection key to cleanup descriptors on local node.
     */
    private void cleanupLocalNodeRecoveryDescriptor(ConnectionKey key) {
        ClusterNode node = getLocalNode();

        if (usePairedConnections(node)) {
            inRecDescs.remove(key);
            outRecDescs.remove(key);
        }
        else
            recoveryDescs.remove(key);
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
     * @param recoveryDescs Descriptors map.
     * @param pairedConnections {@code True} if in/out connections pair is used for communication with node.
     * @param node Node.
     * @param key Connection key.
     * @return Recovery receive data for given node.
     */
    private GridNioRecoveryDescriptor recoveryDescriptor(
        ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs,
        boolean pairedConnections,
        ClusterNode node,
        ConnectionKey key) {
        GridNioRecoveryDescriptor recovery = recoveryDescs.get(key);

        if (recovery == null) {
            if (log.isDebugEnabled())
                log.debug("Missing recovery descriptor for the node (will create a new one) " +
                    "[locNodeId=" + getLocalNode().id() +
                    ", key=" + key + ", rmtNode=" + node + ']');

            int maxSize = Math.max(msgQueueLimit, ackSndThreshold);

            int queueLimit = unackedMsgsBufSize != 0 ? unackedMsgsBufSize : (maxSize * 128);

            GridNioRecoveryDescriptor old = recoveryDescs.putIfAbsent(key,
                recovery = new GridNioRecoveryDescriptor(pairedConnections, queueLimit, node, log));

            if (old != null) {
                recovery = old;

                if (log.isDebugEnabled())
                    log.debug("Will use existing recovery descriptor: " + recovery);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Initialized recovery descriptor [desc=" + recovery + ", maxSize=" + maxSize +
                        ", queueLimit=" + queueLimit + ']');
            }
        }

        return recovery;
    }

    /**
     * @param msg Error message.
     * @param e Exception.
     */
    private void onException(String msg, Exception e) {
        getExceptionRegistry().onException(msg, e);
    }

    /**
     * @return Node ID message.
     */
    private NodeIdMessage nodeIdMessage() {
        final UUID locNodeId = (ignite instanceof IgniteEx) ? ((IgniteEx)ignite).context().localNodeId() :
            safeLocalNodeId();

        return new NodeIdMessage(locNodeId);
    }

    /**
     * @return Local node ID.
     */
    private UUID safeLocalNodeId() {
        ClusterNode locNode = getLocalNode();

        UUID id;

        if (locNode == null) {
            U.warn(log, "Local node is not started or fully initialized [isStopping=" +
                getSpiContext().isStopping() + ']');

            id = new UUID(0, 0);
        }
        else
            id = locNode.id();

        return id;
    }

    /** {@inheritDoc} */
    @Override public TcpCommunicationSpi setName(String name) {
        super.setName(name);

        return this;
    }

    /**
     * Checks whether remote nodes support {@link HandshakeWaitMessage}.
     *
     * @return {@code True} if remote nodes support {@link HandshakeWaitMessage}.
     */
    private boolean isHandshakeWaitSupported() {
        DiscoverySpi discoSpi = ignite().configuration().getDiscoverySpi();

        if (discoSpi instanceof IgniteDiscoverySpi)
            return ((IgniteDiscoverySpi)discoSpi).allNodesSupport(IgniteFeatures.TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE);
        else {
            Collection<ClusterNode> nodes = discoSpi.getRemoteNodes();

            return IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpCommunicationSpi.class, this);
    }

    /**
     * This worker takes responsibility to shut the server down when stopping,
     * No other thread shall stop passed server.
     */
    private class ShmemAcceptWorker extends GridWorker {
        /** */
        private final IpcSharedMemoryServerEndpoint srv;

        /**
         * @param srv Server.
         */
        ShmemAcceptWorker(IpcSharedMemoryServerEndpoint srv) {
            super(igniteInstanceName, "shmem-communication-acceptor", TcpCommunicationSpi.this.log);

            this.srv = srv;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                while (!Thread.interrupted()) {
                    ShmemWorker e = new ShmemWorker(srv.accept());

                    shmemWorkers.add(e);

                    new IgniteThread(e).start();
                }
            }
            catch (IgniteCheckedException e) {
                if (!isCancelled())
                    U.error(log, "Shmem server failed.", e);
            }
            finally {
                srv.close();
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            srv.close();
        }
    }

    /**
     * Write message type to output stream.
     *
     * @param os Output stream.
     * @param type Message type.
     * @throws IOException On error.
     */
    private static void writeMessageType(OutputStream os, short type) throws IOException {
        os.write((byte)(type & 0xFF));
        os.write((byte)((type >> 8) & 0xFF));
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
        assert !remote.isLocal() : remote;
        assert initMsg != null;
        assert chConnPlc != null;
        assert nodeSupports(remote, CHANNEL_COMMUNICATION) : "Node doesn't support direct connection over socket channel " +
                "[nodeId=" + remote.id() + ']';

        ConnectionKey key = new ConnectionKey(remote.id(), chConnPlc.connectionIndex());

        GridFutureAdapter<Channel> chFut = new GridFutureAdapter<>();

        connectGate.enter();

        try {
            GridNioSession ses = createNioSession(remote, key.connectionIndex());

            assert ses != null : "Session must be established [remoteId=" + remote.id() + ", key=" + key + ']';

            cleanupLocalNodeRecoveryDescriptor(key);
            ses.addMeta(CHANNEL_FUT_META, chFut);

            // Send configuration message over the created session.
            ses.send(initMsg)
                .listen(f -> {
                    if (f.error() != null) {
                        GridFutureAdapter<Channel> rq = ses.meta(CHANNEL_FUT_META);

                        assert rq != null;

                        rq.onDone(f.error());

                        ses.close();

                        return;
                    }

                    addTimeoutObject(new IgniteSpiTimeoutObject() {
                        @Override public IgniteUuid id() {
                            return IgniteUuid.randomUuid();
                        }

                        @Override public long endTime() {
                            return U.currentTimeMillis() + connTimeout;
                        }

                        @Override public void onTimeout() {
                            // Close session if request not complete yet.
                            GridFutureAdapter<Channel> rq = ses.meta(CHANNEL_FUT_META);

                            assert rq != null;

                            if (rq.onDone(handshakeTimeoutException()))
                                ses.close();
                        }
                    });
                });

            return chFut;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Unable to create new channel connection to the remote node: " + remote, e);
        }
        finally {
            connectGate.leave();
        }
    }

    /**
     * @param connIdx Connection index to check.
     * @return {@code true} if connection index is related to the channel create request\response.
     */
    private boolean isChannelConnIdx(int connIdx) {
        return connIdx > MAX_CONN_PER_NODE;
    }

    /**
     *
     */
    private class DiscoveryListener implements GridLocalEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent : evt;
            assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

            ClusterNode node = ((DiscoveryEvent)evt).eventNode();

            onNodeLeft(node.consistentId(), node.id());
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }
    }

    /**
     *
     */
    private class ShmemWorker extends GridWorker {
        /** */
        private final IpcEndpoint endpoint;

        /**
         * @param endpoint Endpoint.
         */
        private ShmemWorker(IpcEndpoint endpoint) {
            super(igniteInstanceName, "shmem-worker", TcpCommunicationSpi.this.log);

            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
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

                IpcToNioAdapter<Message> adapter = new IpcToNioAdapter<>(
                    metricsLsnr.metricRegistry(),
                    log,
                    endpoint,
                    srvLsnr,
                    writerFactory,
                    new GridNioTracerFilter(log, tracing),
                    new GridNioCodecFilter(
                        new GridDirectParser(log.getLogger(GridDirectParser.class), msgFactory, readerFactory),
                        log,
                        true),
                    new GridConnectionBytesVerifyFilter(log)
                );

                adapter.serve();
            }
            finally {
                shmemWorkers.remove(this);

                endpoint.close();
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            endpoint.close();
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            endpoint.close();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ShmemWorker.class, this);
        }
    }

    /**
     *
     */
    private class CommunicationWorker extends GridWorker {
        /** */
        private final BlockingQueue<DisconnectedSessionInfo> q = new LinkedBlockingQueue<>();

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param log Logger.
         */
        private CommunicationWorker(String igniteInstanceName, IgniteLogger log) {
            super(igniteInstanceName, "tcp-comm-worker", log, getWorkersRegistry(ignite));
        }

        /** */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Tcp communication worker has been started.");

            Throwable err = null;

            try {
                while (!isCancelled()) {
                    DisconnectedSessionInfo disconnectData;

                    blockingSectionBegin();

                    try {
                        disconnectData = q.poll(idleConnTimeout, TimeUnit.MILLISECONDS);
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    if (disconnectData != null)
                        processDisconnect(disconnectData);
                    else
                        processIdle();

                    onIdle();
                }
            }
            catch (Throwable t) {
                if (!(t instanceof InterruptedException))
                    err = t;

                throw t;
            }
            finally {
                if (ignite instanceof IgniteEx) {
                    if (err == null && !stopping)
                        err = new IllegalStateException("Thread  " + getName() + " is terminated unexpectedly.");

                    if (err instanceof OutOfMemoryError)
                        ((IgniteEx)ignite).context().failure().process(new FailureContext(CRITICAL_ERROR, err));
                    else if (err != null)
                        ((IgniteEx)ignite).context().failure().process(
                            new FailureContext(SYSTEM_WORKER_TERMINATION, err));
                }
            }
        }

        /**
         *
         */
        private void processIdle() {
            cleanupRecovery();

            for (Map.Entry<UUID, GridCommunicationClient[]> e : clients.entrySet()) {
                UUID nodeId = e.getKey();

                for (GridCommunicationClient client : e.getValue()) {
                    if (client == null)
                        continue;

                    ClusterNode node = getSpiContext().node(nodeId);

                    if (node == null) {
                        if (log.isDebugEnabled())
                            log.debug("Forcing close of non-existent node connection: " + nodeId);

                        client.forceClose();

                        removeNodeClient(nodeId, client);

                        continue;
                    }

                    GridNioRecoveryDescriptor recovery = null;

                    if (!usePairedConnections(node) && client instanceof GridTcpNioCommunicationClient) {
                        recovery = recoveryDescs.get(new ConnectionKey(
                            node.id(), client.connectionIndex(), -1)
                        );

                        if (recovery != null && recovery.lastAcknowledged() != recovery.received()) {
                            RecoveryLastReceivedMessage msg = new RecoveryLastReceivedMessage(recovery.received());

                            if (log.isDebugEnabled())
                                log.debug("Send recovery acknowledgement on timeout [rmtNode=" + nodeId +
                                    ", rcvCnt=" + msg.received() + ']');

                            try {
                                nioSrvr.sendSystem(((GridTcpNioCommunicationClient)client).session(), msg);

                                recovery.lastAcknowledged(msg.received());
                            }
                            catch (IgniteCheckedException err) {
                                U.error(log, "Failed to send message: " + err, err);
                            }

                            continue;
                        }
                    }

                    long idleTime = client.getIdleTime();

                    if (idleTime >= idleConnTimeout) {
                        if (recovery == null && usePairedConnections(node))
                            recovery = outRecDescs.get(new ConnectionKey(
                                node.id(), client.connectionIndex(), -1)
                            );

                        if (recovery != null &&
                            recovery.nodeAlive(getSpiContext().node(nodeId)) &&
                            !recovery.messagesRequests().isEmpty()) {
                            if (log.isDebugEnabled())
                                log.debug("Node connection is idle, but there are unacknowledged messages, " +
                                    "will wait: " + nodeId);

                            continue;
                        }

                        if (log.isDebugEnabled())
                            log.debug("Closing idle node connection: " + nodeId);

                        if (client.close() || client.closed())
                            removeNodeClient(nodeId, client);
                    }
                }
            }

            for (GridNioSession ses : nioSrvr.sessions()) {
                GridNioRecoveryDescriptor recovery = ses.inRecoveryDescriptor();

                if (recovery != null && usePairedConnections(recovery.node())) {
                    assert ses.accepted() : ses;

                    sendAckOnTimeout(recovery, ses);
                }
            }
        }

        /**
         * @param recovery Recovery descriptor.
         * @param ses Session.
         */
        private void sendAckOnTimeout(GridNioRecoveryDescriptor recovery, GridNioSession ses) {
            if (recovery != null && recovery.lastAcknowledged() != recovery.received()) {
                RecoveryLastReceivedMessage msg = new RecoveryLastReceivedMessage(recovery.received());

                if (log.isDebugEnabled()) {
                    log.debug("Send recovery acknowledgement on timeout [rmtNode=" + recovery.node().id() +
                        ", rcvCnt=" + msg.received() +
                        ", lastAcked=" + recovery.lastAcknowledged() + ']');
                }

                try {
                    nioSrvr.sendSystem(ses, msg);

                    recovery.lastAcknowledged(msg.received());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send message: " + e, e);
                }
            }
        }

        /**
         *
         */
        private void cleanupRecovery() {
            cleanupRecovery(recoveryDescs);
            cleanupRecovery(inRecDescs);
            cleanupRecovery(outRecDescs);
        }

        /**
         * @param recoveryDescs Recovery descriptors to cleanup.
         */
        private void cleanupRecovery(ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs) {
            Set<ConnectionKey> left = null;

            for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> e : recoveryDescs.entrySet()) {
                if (left != null && left.contains(e.getKey()))
                    continue;

                GridNioRecoveryDescriptor recoveryDesc = e.getValue();

                if (!recoveryDesc.nodeAlive(getSpiContext().node(e.getKey().nodeId()))) {
                    if (left == null)
                        left = new HashSet<>();

                    left.add(e.getKey());
                }
            }

            if (left != null) {
                assert !left.isEmpty();

                for (ConnectionKey id : left) {
                    GridNioRecoveryDescriptor recoveryDesc = recoveryDescs.get(id);

                    if (recoveryDesc != null && recoveryDesc.onNodeLeft())
                        recoveryDescs.remove(id, recoveryDesc);
                }
            }
        }

        /**
         * @param sesInfo Disconnected session information.
         */
        private void processDisconnect(DisconnectedSessionInfo sesInfo) {
            GridNioRecoveryDescriptor recoveryDesc = sesInfo.recoveryDesc;

            ClusterNode node = recoveryDesc.node();

            if (!recoveryDesc.nodeAlive(getSpiContext().node(node.id())))
                return;

            try {
                if (log.isDebugEnabled())
                    log.debug("Recovery reconnect [rmtNode=" + recoveryDesc.node().id() + ']');

                GridCommunicationClient client = reserveClient(node, sesInfo.connIdx);

                client.release();
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Recovery reconnect failed, node stopping [rmtNode=" + recoveryDesc.node().id() + ']');
            }
            catch (IgniteTooManyOpenFilesException e) {
                onException(e.getMessage(), e);

                throw e;
            }
            catch (IgniteCheckedException | IgniteException e) {
                try {
                    if (recoveryDesc.nodeAlive(getSpiContext().node(node.id())) && getSpiContext().pingNode(node.id())) {
                        if (log.isDebugEnabled())
                            log.debug("Recovery reconnect failed, will retry " +
                                "[rmtNode=" + recoveryDesc.node().id() + ", err=" + e + ']');

                        addProcessDisconnectRequest(sesInfo);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Recovery reconnect failed, " +
                                "node left [rmtNode=" + recoveryDesc.node().id() + ", err=" + e + ']');

                        onException("Recovery reconnect failed, node left [rmtNode=" + recoveryDesc.node().id() + "]",
                            e);
                    }
                }
                catch (IgniteClientDisconnectedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ping node, client disconnected.");
                }
            }
        }

        /**
         * @param sesInfo Disconnected session information.
         */
        void addProcessDisconnectRequest(DisconnectedSessionInfo sesInfo) {
            boolean add = q.add(sesInfo);

            assert add;
        }
    }

    /**
     *
     */
    private static class ConnectFuture extends GridFutureAdapter<GridCommunicationClient> {
        // No-op.
    }

    /**
     *
     */
    private static class HandshakeTimeoutObject<T> implements IgniteSpiTimeoutObject {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final T obj;

        /** */
        private final long endTime;

        /** */
        private final AtomicBoolean done = new AtomicBoolean();

        /**
         * @param obj Client.
         * @param endTime End time.
         */
        private HandshakeTimeoutObject(T obj, long endTime) {
            assert obj != null;
            assert obj instanceof GridCommunicationClient || obj instanceof SelectableChannel;
            assert endTime > 0;

            this.obj = obj;
            this.endTime = endTime;
        }

        /**
         * @return {@code True} if object has not yet been timed out.
         */
        boolean cancel() {
            return done.compareAndSet(false, true);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (done.compareAndSet(false, true)) {
                // Close socket - timeout occurred.
                if (obj instanceof GridCommunicationClient)
                    ((GridCommunicationClient)obj).forceClose();
                else
                    U.closeQuiet((AutoCloseable)obj);
            }
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(HandshakeTimeoutObject.class, this);
        }
    }

    /**
     *
     */
    private class HandshakeClosure extends IgniteInClosure2X<InputStream, OutputStream> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final UUID rmtNodeId;

        /**
         * @param rmtNodeId Remote node ID.
         */
        private HandshakeClosure(UUID rmtNodeId) {
            this.rmtNodeId = rmtNodeId;
        }

        /** {@inheritDoc} */
        @Override public void applyx(InputStream in, OutputStream out) throws IgniteCheckedException {
            try {
                // Handshake.
                byte[] b = new byte[NodeIdMessage.MESSAGE_FULL_SIZE];

                int n = 0;

                while (n < NodeIdMessage.MESSAGE_FULL_SIZE) {
                    int cnt = in.read(b, n, NodeIdMessage.MESSAGE_FULL_SIZE - n);

                    if (cnt < 0)
                        throw new IgniteCheckedException("Failed to get remote node ID (end of stream reached)");

                    n += cnt;
                }

                // First 4 bytes are for length.
                UUID id = U.bytesToUuid(b, Message.DIRECT_TYPE_SIZE);

                if (!rmtNodeId.equals(id))
                    throw new IgniteCheckedException("Remote node ID is not as expected [expected=" + rmtNodeId +
                        ", rcvd=" + id + ']');
                else if (log.isDebugEnabled())
                    log.debug("Received remote node ID: " + id);
            }
            catch (SocketTimeoutException e) {
                throw new IgniteCheckedException("Failed to perform handshake due to timeout (consider increasing " +
                    "'connectionTimeout' configuration property).", e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to perform handshake.", e);
            }

            try {
                ClusterNode localNode = getLocalNode();

                if (localNode == null)
                    throw new IgniteSpiException("Local node has not been started or fully initialized " +
                        "[isStopping=" + getSpiContext().isStopping() + ']');

                UUID id = localNode.id();

                NodeIdMessage msg = new NodeIdMessage(id);

                out.write(U.IGNITE_HEADER);
                writeMessageType(out, NODE_ID_MSG_TYPE);
                out.write(msg.nodeIdBytes());

                out.flush();

                if (log.isDebugEnabled())
                    log.debug("Sent local node ID [locNodeId=" + id + ", rmtNodeId=" + rmtNodeId + ']');
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to perform handshake.", e);
            }
        }
    }

    /**
     *
     */
    private static class ConnectGateway {
        /** */
        private GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

        /** */
        private IgniteException err;

        /**
         *
         */
        void enter() {
            lock.readLock();

            if (err != null) {
                lock.readUnlock();

                throw err;
            }
        }

        /**
         * @return {@code True} if entered gateway.
         */
        boolean tryEnter() {
            lock.readLock();

            boolean res = err == null;

            if (!res)
                lock.readUnlock();

            return res;
        }

        /**
         *
         */
        void leave() {
            lock.readUnlock();
        }

        /**
         * @param reconnectFut Reconnect future.
         */
        void disconnected(IgniteFuture<?> reconnectFut) {
            lock.writeLock();

            err = new IgniteClientDisconnectedException(reconnectFut, "Failed to connect, client node disconnected.");

            lock.writeUnlock();
        }

        /**
         *
         */
        void reconnected() {
            lock.writeLock();

            try {
                if (err instanceof IgniteClientDisconnectedException)
                    err = null;
            }
            finally {
                lock.writeUnlock();
            }
        }

        /**
         *
         */
        void stopped() {
            lock.readLock();

            err = new IgniteException("Failed to connect, node stopped.");

            lock.readUnlock();
        }
    }

    /**
     *
     */
    private static class DisconnectedSessionInfo {
        /** */
        private final GridNioRecoveryDescriptor recoveryDesc;

        /** */
        private int connIdx;

        /**
         * @param recoveryDesc Recovery descriptor.
         * @param connIdx Connection index.
         */
        DisconnectedSessionInfo(@Nullable GridNioRecoveryDescriptor recoveryDesc, int connIdx) {
            this.recoveryDesc = recoveryDesc;
            this.connIdx = connIdx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DisconnectedSessionInfo.class, this);
        }
    }

    /**
     *
     */
    interface ConnectionPolicy {
        /**
         * @return Thread connection index.
         */
        int connectionIndex();
    }

    /** */
    private static class FirstConnectionPolicy implements ConnectionPolicy {
        /** {@inheritDoc} */
        @Override public int connectionIndex() {
            return 0;
        }
    }

    /** */
    private class RoundRobinConnectionPolicy implements ConnectionPolicy {
        /** {@inheritDoc} */
        @Override public int connectionIndex() {
            return (int)(U.safeAbs(Thread.currentThread().getId()) % connectionsPerNode);
        }
    }

    /**
     * MBean implementation for TcpCommunicationSpi.
     */
    private class TcpCommunicationSpiMBeanImpl extends IgniteSpiMBeanAdapter implements TcpCommunicationSpiMBean {
        /** {@inheritDoc} */
        TcpCommunicationSpiMBeanImpl(IgniteSpiAdapter spiAdapter) {
            super(spiAdapter);
        }

        /** {@inheritDoc} */
        @Override public String getLocalAddress() {
            return TcpCommunicationSpi.this.getLocalAddress();
        }

        /** {@inheritDoc} */
        @Override public int getLocalPort() {
            return TcpCommunicationSpi.this.getLocalPort();
        }

        /** {@inheritDoc} */
        @Override public int getLocalPortRange() {
            return TcpCommunicationSpi.this.getLocalPortRange();
        }

        /** {@inheritDoc} */
        @Override public boolean isUsePairedConnections() {
            return TcpCommunicationSpi.this.isUsePairedConnections();
        }

        /** {@inheritDoc} */
        @Override public int getConnectionsPerNode() {
            return TcpCommunicationSpi.this.getConnectionsPerNode();
        }

        /** {@inheritDoc} */
        @Override public int getSharedMemoryPort() {
            return TcpCommunicationSpi.this.getSharedMemoryPort();
        }

        /** {@inheritDoc} */
        @Override public long getIdleConnectionTimeout() {
            return TcpCommunicationSpi.this.getIdleConnectionTimeout();
        }

        /** {@inheritDoc} */
        @Override public long getSocketWriteTimeout() {
            return TcpCommunicationSpi.this.getSocketWriteTimeout();
        }

        /** {@inheritDoc} */
        @Override public int getAckSendThreshold() {
            return TcpCommunicationSpi.this.getAckSendThreshold();
        }

        /** {@inheritDoc} */
        @Override public int getUnacknowledgedMessagesBufferSize() {
            return TcpCommunicationSpi.this.getUnacknowledgedMessagesBufferSize();
        }

        /** {@inheritDoc} */
        @Override public long getConnectTimeout() {
            return TcpCommunicationSpi.this.getConnectTimeout();
        }

        /** {@inheritDoc} */
        @Override public long getMaxConnectTimeout() {
            return TcpCommunicationSpi.this.getMaxConnectTimeout();
        }

        /** {@inheritDoc} */
        @Override public int getReconnectCount() {
            return TcpCommunicationSpi.this.getReconnectCount();
        }

        /** {@inheritDoc} */
        @Override public boolean isDirectBuffer() {
            return TcpCommunicationSpi.this.isDirectBuffer();
        }

        /** {@inheritDoc} */
        @Override public boolean isDirectSendBuffer() {
            return TcpCommunicationSpi.this.isDirectSendBuffer();
        }

        /** {@inheritDoc} */
        @Override public int getSelectorsCount() {
            return TcpCommunicationSpi.this.getSelectorsCount();
        }

        /** {@inheritDoc} */
        @Override public long getSelectorSpins() {
            return TcpCommunicationSpi.this.getSelectorSpins();
        }

        /** {@inheritDoc} */
        @Override public boolean isTcpNoDelay() {
            return TcpCommunicationSpi.this.isTcpNoDelay();
        }

        /** {@inheritDoc} */
        @Override public int getSocketReceiveBuffer() {
            return TcpCommunicationSpi.this.getSocketReceiveBuffer();
        }

        /** {@inheritDoc} */
        @Override public int getSocketSendBuffer() {
            return TcpCommunicationSpi.this.getSocketSendBuffer();
        }

        /** {@inheritDoc} */
        @Override public int getMessageQueueLimit() {
            return TcpCommunicationSpi.this.getMessageQueueLimit();
        }

        /** {@inheritDoc} */
        @Override public int getSlowClientQueueLimit() {
            return TcpCommunicationSpi.this.getSlowClientQueueLimit();
        }

        /** {@inheritDoc} */
        @Override public void dumpStats() {
            TcpCommunicationSpi.this.dumpStats();
        }

        /** {@inheritDoc} */
        @Override public int getSentMessagesCount() {
            return TcpCommunicationSpi.this.getSentMessagesCount();
        }

        /** {@inheritDoc} */
        @Override public long getSentBytesCount() {
            return TcpCommunicationSpi.this.getSentBytesCount();
        }

        /** {@inheritDoc} */
        @Override public int getReceivedMessagesCount() {
            return TcpCommunicationSpi.this.getReceivedMessagesCount();
        }

        /** {@inheritDoc} */
        @Override public long getReceivedBytesCount() {
            return TcpCommunicationSpi.this.getReceivedBytesCount();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Long> getReceivedMessagesByType() {
            return TcpCommunicationSpi.this.getReceivedMessagesByType();
        }

        /** {@inheritDoc} */
        @Override public Map<UUID, Long> getReceivedMessagesByNode() {
            return TcpCommunicationSpi.this.getReceivedMessagesByNode();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Long> getSentMessagesByType() {
            return TcpCommunicationSpi.this.getSentMessagesByType();
        }

        /** {@inheritDoc} */
        @Override public Map<UUID, Long> getSentMessagesByNode() {
            return TcpCommunicationSpi.this.getSentMessagesByNode();
        }

        /** {@inheritDoc} */
        @Override public int getOutboundMessagesQueueSize() {
            return TcpCommunicationSpi.this.getOutboundMessagesQueueSize();
        }
    }
}
