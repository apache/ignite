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
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteOrder;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteTooManyOpenFilesException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.GridManager;
import org.apache.ignite.internal.managers.tracing.GridTracingManager;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.function.ThrowableBiFunction;
import org.apache.ignite.internal.util.function.ThrowableSupplier;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridConnectionBytesVerifyFilter;
import org.apache.ignite.internal.util.nio.GridDirectParser;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioMessageReaderFactory;
import org.apache.ignite.internal.util.nio.GridNioMessageWriterFactory;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.nio.GridNioTracerFilter;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.nio.ssl.GridSslMeta;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.TimeoutStrategy;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.ignite.internal.IgniteFeatures.CHANNEL_COMMUNICATION;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.SSL_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONSISTENT_ID_META;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.handshakeTimeoutException;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.isRecoverableException;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.nodeAddresses;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.usePairedConnections;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.ALREADY_CONNECTED;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NEED_WAIT;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NODE_STOPPING;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.UNKNOWN_NODE;

/**
 * Container for nio server with required dependencies.
 *
 * @deprecated Should be removed.
 */
public class GridNioServerWrapper {
    /** Default initial connect/handshake timeout in case of failure detection enabled. */
    private static final int DFLT_INITIAL_TIMEOUT = 500;

    /** Default initial delay in case of target node is still out of topology. */
    private static final int DFLT_NEED_WAIT_DELAY = 200;

    /** Default delay between reconnects attempts in case of temporary network issues. */
    private static final int DFLT_RECONNECT_DELAY = 50;

    /** Channel meta used for establishing channel connections. */
    static final int CHANNEL_FUT_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Maximum {@link GridNioSession} connections per node. */
    public static final int MAX_CONN_PER_NODE = 1024;

    /** Logger. */
    private final IgniteLogger log;

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** Attribute names. */
    private final AttributeNames attrs;

    /** Tracing. */
    private final Tracing tracing;

    /** Node getter. */
    private final Function<UUID, ClusterNode> nodeGetter;

    /** Local node supplier. */
    private final Supplier<ClusterNode> locNodeSupplier;

    /** Connect gate. */
    private final ConnectGateway connectGate;

    /** State provider. */
    private final ClusterStateProvider stateProvider;

    /** Exception registry supplier. */
    private final Supplier<IgniteExceptionRegistry> eRegistrySupplier;

    /** Ignite config. */
    private final IgniteConfiguration igniteCfg;

    /** Server listener. */
    private final GridNioServerListener<Message> srvLsnr;

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** Workers registry. */
    private final WorkersRegistry workersRegistry;

    /** Metric manager. */
    private final GridMetricManager metricMgr;

    /** Create tcp client fun. */
    private final ThrowableBiFunction<ClusterNode, Integer, GridCommunicationClient, IgniteCheckedException> createTcpClientFun;

    /** Recovery descs. */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs = GridConcurrentFactory.newMap();

    /** Out rec descs. */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> outRecDescs = GridConcurrentFactory.newMap();

    /** In rec descs. */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> inRecDescs = GridConcurrentFactory.newMap();

    /**
     *
     */
    private final CommunicationListener<Message> lsnr;

    /** Recovery and idle clients handler. */
    private volatile CommunicationWorker commWorker;

    /** Socket channel factory. */
    private volatile ThrowableSupplier<SocketChannel, IOException> socketChannelFactory = SocketChannel::open;

    /** Enable forcible node kill. */
    private boolean forcibleNodeKillEnabled = IgniteSystemProperties
        .getBoolean(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);

    /** NIO server. */
    private GridNioServer<Message> nioSrv;

    /** Stopping flag (set to {@code true} when SPI gets stopping signal). */
    private volatile boolean stopping = false;

    /** Channel connection index provider. */
    private ConnectionPolicy chConnPlc;

    /** Scheduled executor service which closed the socket if handshake timeout is out. **/
    private final ScheduledExecutorService handshakeTimeoutExecutorService;

    /** Executor for establishing a connection to a node. */
    private final TcpHandshakeExecutor tcpHandshakeExecutor;

    /**
     * @param log Logger.
     * @param cfg Config.
     * @param attributeNames Attribute names.
     * @param tracing Tracing.
     * @param nodeGetter Node getter.
     * @param locNodeSupplier Local node supplier.
     * @param connectGate Connect gate.
     * @param stateProvider State provider.
     * @param eRegistrySupplier Exception registry supplier.
     * @param commWorker Communication worker.
     * @param igniteCfg Ignite config.
     * @param srvLsnr Server listener.
     * @param igniteInstanceName Ignite instance name.
     * @param workersRegistry Workers registry.
     * @param tcpHandshakeExecutor Executor for establishing a connection to a node.
     * @param lsnr Listener
     */
    public GridNioServerWrapper(
        IgniteLogger log,
        TcpCommunicationConfiguration cfg,
        AttributeNames attributeNames,
        Tracing tracing,
        Function<UUID, ClusterNode> nodeGetter,
        Supplier<ClusterNode> locNodeSupplier,
        ConnectGateway connectGate,
        ClusterStateProvider stateProvider,
        Supplier<IgniteExceptionRegistry> eRegistrySupplier,
        CommunicationWorker commWorker,
        IgniteConfiguration igniteCfg,
        GridNioServerListener<Message> srvLsnr,
        String igniteInstanceName,
        WorkersRegistry workersRegistry,
        @Nullable GridMetricManager metricMgr,
        ThrowableBiFunction<ClusterNode, Integer, GridCommunicationClient, IgniteCheckedException> createTcpClientFun,
        CommunicationListener<Message> lsnr,
        TcpHandshakeExecutor tcpHandshakeExecutor
    ) {
        this.log = log;
        this.cfg = cfg;
        this.attrs = attributeNames;
        this.tracing = tracing;
        this.nodeGetter = nodeGetter;
        this.locNodeSupplier = locNodeSupplier;
        this.connectGate = connectGate;
        this.stateProvider = stateProvider;
        this.eRegistrySupplier = eRegistrySupplier;
        this.commWorker = commWorker;
        this.igniteCfg = igniteCfg;
        this.srvLsnr = srvLsnr;
        this.igniteInstanceName = igniteInstanceName;
        this.workersRegistry = workersRegistry;
        this.metricMgr = metricMgr;
        this.createTcpClientFun = createTcpClientFun;
        this.lsnr = lsnr;

        chConnPlc = new ConnectionPolicy() {
            /** Sequential connection index provider. */
            private final AtomicInteger chIdx = new AtomicInteger(MAX_CONN_PER_NODE + 1);

            @Override public int connectionIndex() {
                return chIdx.incrementAndGet();
            }
        };
        this.tcpHandshakeExecutor = tcpHandshakeExecutor;

        this.handshakeTimeoutExecutorService = newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(igniteInstanceName, "handshake-timeout-nio")
        );
    }

    /**
     * Starts nio server.
     */
    public void start() {
        nioSrv.start();
    }

    /**
     * Stops nio server.
     */
    public void stop() {
        if (nioSrv != null)
            nioSrv.stop();

        stopping = true;

        handshakeTimeoutExecutorService.shutdown();
    }

    /**
     * Clears resources for gc.
     */
    public void clear() {
        nioSrv = null;
    }

    /**
     * Returns the established TCP/IP connection between the current node and remote server. A handshake process of
     * negotiation between two communicating nodes will be performed before the {@link GridNioSession} created.
     * <p>
     * The handshaking process contains of these steps:
     *
     * <ol>
     * <li>The local node opens a new {@link SocketChannel} in the <em>blocking</em> mode.</li>
     * <li>The local node calls {@link SocketChannel#connect(SocketAddress)} to remote node.</li>
     * <li>The remote GridNioAcceptWorker thread accepts new connection.</li>
     * <li>The remote node sends back the {@link NodeIdMessage}.</li>
     * <li>The local node reads NodeIdMessage from created channel.</li>
     * <li>The local node sends the {@link HandshakeMessage2} to remote.</li>
     * <li>The remote node processes {@link HandshakeMessage2} in {@link GridNioServerListener#onMessage(GridNioSession,
     * Object)}.</li>
     * <li>The remote node sends back the {@link RecoveryLastReceivedMessage}.</li>
     * </ol>
     *
     * The handshaking process ends.
     * </p>
     * <p>
     * <em>Note.</em> The {@link HandshakeTimeoutObject} is created to control execution timeout during the
     * whole handshaking process.
     * </p>
     *
     * @param node Remote node identifier to connect with.
     * @param connIdx Connection index based on configured {@link ConnectionPolicy}.
     * @return A {@link GridNioSession} connection representation.
     * @throws IgniteCheckedException If establish connection fails.
     */
    public GridNioSession createNioSession(ClusterNode node, int connIdx) throws IgniteCheckedException {
        boolean locNodeIsSrv = !locNodeSupplier.get().isClient() && !locNodeSupplier.get().isDaemon();

        if (!(Thread.currentThread() instanceof IgniteDiscoveryThread) && locNodeIsSrv) {
            if (node.isClient() && forceClientToServerConnections(node)) {
                String msg = "Failed to connect to node " + node.id() + " because it is started" +
                    " in 'forceClientToServerConnections' mode; inverse connection will be requested.";

                throw new NodeUnreachableException(msg);
            }
        }

        Collection<InetSocketAddress> addrs = nodeAddresses(node, cfg.filterReachableAddresses(), attrs, locNodeSupplier);

        GridNioSession ses = null;
        IgniteCheckedException errs = null;

        long totalTimeout;

        if (cfg.failureDetectionTimeoutEnabled())
            totalTimeout = node.isClient() ? stateProvider.clientFailureDetectionTimeout() : cfg.failureDetectionTimeout();
        else {
            totalTimeout = ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(
                cfg.connectionTimeout(),
                cfg.maxConnectionTimeout(),
                cfg.reconCount()
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
                cfg.failureDetectionTimeoutEnabled() ? DFLT_INITIAL_TIMEOUT : cfg.connectionTimeout(),
                cfg.maxConnectionTimeout()
            );

            while (ses == null) { // Reconnection on handshake timeout.
                if (stopping)
                    throw new IgniteSpiException("Node is stopping.");

                if (isLocalNodeAddress(addr)) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping local address [addr=" + addr +
                            ", locAddrs=" + node.attribute(attrs.addresses()) +
                            ", node=" + node + ']');

                    skippedAddrs++;

                    break;
                }

                long timeout = 0;

                connectGate.enter();

                try {
                    if (nodeGetter.apply(node.id()) == null)
                        throw new ClusterTopologyCheckedException("Failed to send message (node left topology): " + node);

                    SocketChannel ch = socketChannelFactory.get();

                    ch.configureBlocking(true);

                    ch.socket().setTcpNoDelay(cfg.tcpNoDelay());
                    ch.socket().setKeepAlive(true);

                    if (cfg.socketReceiveBuffer() > 0)
                        ch.socket().setReceiveBufferSize(cfg.socketReceiveBuffer());

                    if (cfg.socketSendBuffer() > 0)
                        ch.socket().setSendBufferSize(cfg.socketSendBuffer());

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

                        ch.socket().connect(addr, (int)timeout);

                        if (nodeGetter.apply(node.id()) == null)
                            throw new ClusterTopologyCheckedException("Failed to send message (node left topology): " + node);

                        if (stateProvider.isSslEnabled()) {
                            meta.put(SSL_META.ordinal(), sslMeta = new GridSslMeta());

                            SSLEngine sslEngine = stateProvider.createSSLEngine();

                            sslEngine.setUseClientMode(true);

                            sslMeta.sslEngine(sslEngine);
                        }

                        ClusterNode locNode = locNodeSupplier.get();

                        if (locNode == null)
                            throw new IgniteCheckedException("Local node has not been started or " +
                                "fully initialized [isStopping=" + stateProvider.isStopping() + ']');

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
                        else if (rcvCnt == NODE_STOPPING) {
                            // Safe to remap on remote node stopping.
                            throw new ClusterTopologyCheckedException("Remote node started stop procedure: " + node.id());
                        }
                        else if (rcvCnt == UNKNOWN_NODE)
                            throw new IgniteCheckedException("Remote node does not observe current node " +
                                "in topology : " + node.id());
                        else if (rcvCnt == NEED_WAIT) {
                            // Should wait for discovery lag without applying connTimeoutStgy, otherwise cache operations
                            // may hang on waiting inexistent topology, see IgniteClientConnectTest for test
                            // scenarios with delayed client node join.
                            if (log.isDebugEnabled())
                                log.debug("NEED_WAIT received, handshake after delay [node = "
                                    + node + ", outOfTopologyDelay = " + DFLT_NEED_WAIT_DELAY + "ms]");

                            U.sleep(DFLT_NEED_WAIT_DELAY);

                            continue;
                        }
                        else if (rcvCnt < 0)
                            throw new IgniteCheckedException("Unsupported negative receivedCount [rcvCnt=" + rcvCnt +
                                ", senderNode=" + node + ']');

                        recoveryDesc.onHandshake(rcvCnt);

                        meta.put(CONSISTENT_ID_META, node.consistentId());
                        meta.put(CONN_IDX_META, connKey);
                        meta.put(GridNioServer.RECOVERY_DESC_META_KEY, recoveryDesc);

                        ses = nioSrv.createSession(ch, meta, false, null).get();
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

                    eRegistrySupplier.get().onException(
                        "Handshake timed out (will retry with increased timeout) [connTimeoutStrategy=" + connTimeoutStgy +
                        ", addr=" + addr + ']', e);

                    if (log.isDebugEnabled())
                        log.debug("Handshake timed out (will retry with increased timeout) [connTimeoutStrategy=" + connTimeoutStgy +
                            ", addr=" + addr + ", err=" + e + ']'
                        );

                    if (connTimeoutStgy.checkTimeout()) {
                        U.warn(log, "Handshake timed out (will stop attempts to perform the handshake) " +
                            "[node=" + node.id() + ", connTimeoutStrategy=" + connTimeoutStgy +
                            ", err=" + e.getMessage() + ", addr=" + addr +
                            ", failureDetectionTimeoutEnabled=" + cfg.failureDetectionTimeoutEnabled() +
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

                    eRegistrySupplier.get().onException("Client creation failed [addr=" + addr + ", err=" + e + ']', e);

                    if (log.isDebugEnabled())
                        log.debug("Client creation failed [addr=" + addr + ", err=" + e + ']');

                    if (X.hasCause(e, "Too many open files", SocketException.class))
                        throw new IgniteTooManyOpenFilesException(e);

                    // check if timeout occured in case of unrecoverable exception
                    if (connTimeoutStgy.checkTimeout()) {
                        U.warn(log, "Connection timed out (will stop attempts to perform the connect) " +
                            "[node=" + node.id() + ", connTimeoutStgy=" + connTimeoutStgy +
                            ", failureDetectionTimeoutEnabled=" + cfg.failureDetectionTimeoutEnabled() +
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
            // If local node and remote node are configured to use paired connections we won't even request
            // inverse connection so no point in throwing NodeUnreachableException
            if (!cfg.usePairedConnections() || !Boolean.TRUE.equals(node.attribute(attrs.pairedConnection()))) {
                if (!(Thread.currentThread() instanceof IgniteDiscoveryThread) && locNodeIsSrv) {
                    if (node.isClient() && (addrs.size() - skippedAddrs == failedAddrsSet.size())) {
                        String msg = "Failed to connect to all addresses of node " + node.id() + ": " + failedAddrsSet +
                            "; inverse connection will be requested.";

                        throw new NodeUnreachableException(msg);
                    }
                }
            }

            processSessionCreationError(node, addrs, errs == null ? new IgniteCheckedException("No session found") : errs);
        }

        return ses;
    }

    /**
     * Checks if exception indicates that client is unreachable.
     *
     * @param e Exception to check.
     * @return {@code True} if exception shows that client is unreachable, {@code false} otherwise.
     */
    private boolean isNodeUnreachableException(Exception e) {
        return e instanceof NodeUnreachableException || e instanceof SocketTimeoutException;
    }

    /**
     * Establish TCP connection to remote node and returns client.
     *
     * @param node Remote node.
     * @param connIdx Connection index.
     * @param backwardCompatibility It calls the method from protected class.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    public GridCommunicationClient createTcpClient(
        ClusterNode node,
        int connIdx,
        boolean backwardCompatibility
    ) throws IgniteCheckedException {
        if (backwardCompatibility)
            return createTcpClientFun.apply(node, connIdx);
        else {
            GridNioSession ses = createNioSession(node, connIdx);

            return ses == null ?
                null : new GridTcpNioCommunicationClient(connIdx, ses, log);
        }
    }

    /**
     * Returns original nio server instance.
     */
    public GridNioServer<Message> nio() {
        return nioSrv;
    }

    /**
     * @param srv Server.
     */
    public void nio(GridNioServer<Message> srv) {
        nioSrv = srv;
    }

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for incoming connection.
     */
    public GridNioRecoveryDescriptor inRecoveryDescriptor(ClusterNode node, ConnectionKey key) {
        if (cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection()))
            return recoveryDescriptor(inRecDescs, true, node, key);
        else
            return recoveryDescriptor(recoveryDescs, false, node, key);
    }

    /**
     * @return Recovery descs.
     */
    public ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs() {
        return recoveryDescs;
    }

    /**
     * @return Out rec descs.
     */
    public ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> outRecDescs() {
        return outRecDescs;
    }

    /**
     * @return In rec descs.
     */
    public ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> inRecDescs() {
        return inRecDescs;
    }

    /**
     * Process errors if TCP/IP {@link GridNioSession} creation to remote node hasn't been performed.
     *
     * @param node Remote node.
     * @param addrs Remote node addresses.
     * @param errs TCP client creation errors.
     * @throws IgniteCheckedException If failed.
     */
    public void processSessionCreationError(
        ClusterNode node,
        Collection<InetSocketAddress> addrs,
        IgniteCheckedException errs
    ) throws IgniteCheckedException {
        assert errs != null;

        boolean commErrResolve = false;

        IgniteSpiContext ctx = stateProvider.getSpiContext();

        if (isRecoverableException(errs) && ctx.communicationFailureResolveSupported()) {
            commErrResolve = true;

            ctx.resolveCommunicationFailure(node, errs);
        }

        if (!commErrResolve && forcibleNodeKillEnabled) {
            if (ctx.node(node.id()) != null
                && node.isClient()
                && !locNodeSupplier.get().isClient()
                && isRecoverableException(errs)
            ) {
                CommunicationTcpUtils.failNode(node,
                    ctx, errs, log);
            }
        }

        throw errs;
    }

    /**
     * Recreates tpcSrvr socket instance.
     *
     * @return Server instance.
     * @throws IgniteCheckedException Thrown if it's not possible to create server.
     */
    public GridNioServer<Message> resetNioServer() throws IgniteCheckedException {
        if (cfg.boundTcpPort() >= 0)
            throw new IgniteCheckedException("Tcp NIO server was already created on port " + cfg.boundTcpPort());

        IgniteCheckedException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        int lastPort = cfg.localPort() == -1 ? -1
            : cfg.localPortRange() == 0 ? cfg.localPort() : cfg.localPort() + cfg.localPortRange() - 1;

        for (int port = cfg.localPort(); port <= lastPort; port++) {
            try {
                MessageFactory msgFactory = new MessageFactory() {
                    private MessageFactory impl;

                    @Nullable @Override public Message create(short type) {
                        if (impl == null)
                            impl = stateProvider.getSpiContext().messageFactory();

                        assert impl != null;

                        return impl.create(type);
                    }
                };

                GridNioMessageReaderFactory readerFactory = new GridNioMessageReaderFactory() {
                    private IgniteSpiContext context;

                    private MessageFormatter formatter;

                    @Override public MessageReader reader(GridNioSession ses, MessageFactory msgFactory)
                        throws IgniteCheckedException {
                        final IgniteSpiContext ctx = stateProvider.getSpiContextWithoutInitialLatch();

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
                        final IgniteSpiContext ctx = stateProvider.getSpiContextWithoutInitialLatch();

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

                IgnitePredicate<Message> skipRecoveryPred = msg -> msg instanceof RecoveryLastReceivedMessage;

                boolean clientMode = Boolean.TRUE.equals(igniteCfg.isClientMode());

                IgniteBiInClosure<GridNioSession, Integer> queueSizeMonitor =
                    !clientMode && cfg.slowClientQueueLimit() > 0 ? this::checkClientQueueSize : null;

                List<GridNioFilter> filters = new ArrayList<>();

                if (tracing instanceof GridTracingManager && ((GridManager)tracing).enabled())
                    filters.add(new GridNioTracerFilter(log, tracing));

                filters.add(new GridNioCodecFilter(parser, log, true));
                filters.add(new GridConnectionBytesVerifyFilter(log));

                if (stateProvider.isSslEnabled()) {
                    GridNioSslFilter sslFilter = new GridNioSslFilter(
                        igniteCfg.getSslContextFactory().create(),
                        true,
                        ByteOrder.LITTLE_ENDIAN,
                        log,
                        metricMgr == null ? null : metricMgr.registry(COMMUNICATION_METRICS_GROUP_NAME));

                    sslFilter.directMode(true);

                    sslFilter.wantClientAuth(true);
                    sslFilter.needClientAuth(true);

                    filters.add(sslFilter);
                }

                GridNioServer.Builder<Message> builder = GridNioServer.<Message>builder()
                    .address(cfg.localHost())
                    .port(port)
                    .listener(srvLsnr)
                    .logger(log)
                    .selectorCount(cfg.selectorsCount())
                    .igniteInstanceName(igniteInstanceName)
                    .serverName("tcp-comm")
                    .tcpNoDelay(cfg.tcpNoDelay())
                    .directBuffer(cfg.directBuffer())
                    .byteOrder(ByteOrder.LITTLE_ENDIAN)
                    .socketSendBufferSize(cfg.socketSendBuffer())
                    .socketReceiveBufferSize(cfg.socketReceiveBuffer())
                    .sendQueueLimit(cfg.messageQueueLimit())
                    .directMode(true)
                    .writeTimeout(cfg.socketWriteTimeout())
                    .selectorSpins(cfg.selectorSpins())
                    .filters(filters.toArray(new GridNioFilter[filters.size()]))
                    .writerFactory(writerFactory)
                    .skipRecoveryPredicate(skipRecoveryPred)
                    .messageQueueSizeListener(queueSizeMonitor)
                    .tracing(tracing)
                    .readWriteSelectorsAssign(cfg.usePairedConnections());

                if (metricMgr != null) {
                    builder.workerListener(workersRegistry)
                        .metricRegistry(metricMgr.registry(COMMUNICATION_METRICS_GROUP_NAME));
                }

                GridNioServer<Message> srvr = builder.build();

                cfg.boundTcpPort(port);

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled()) {
                    log.info("Successfully bound communication NIO server to TCP port " +
                        "[port=" + cfg.boundTcpPort() +
                        ", locHost=" + cfg.localHost() +
                        ", selectorsCnt=" + cfg.selectorsCount() +
                        ", selectorSpins=" + srvr.selectorSpins() +
                        ", pairedConn=" + cfg.usePairedConnections() + ']');
                }

                srvr.idleTimeout(cfg.idleConnectionTimeout());

                return srvr;
            }
            catch (IgniteCheckedException e) {
                if (X.hasCause(e, SSLException.class))
                    throw new IgniteSpiException("Failed to create SSL context. SSL factory: "
                        + igniteCfg.getSslContextFactory() + '.', e);

                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + cfg.localHost() + ']');

                eRegistrySupplier.get().onException("Failed to bind to local port (will try next port within range) [port=" + port +
                    ", locHost=" + cfg.localHost() + ']', e);
            }
        }

        // If free port wasn't found.
        throw new IgniteCheckedException("Failed to bind to any port within range [startPort=" + cfg.localPort() +
            ", portRange=" + cfg.localPortRange() + ", locHost=" + cfg.localHost() + ']', lastEx);
    }

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for outgoing connection.
     */
    private GridNioRecoveryDescriptor outRecoveryDescriptor(ClusterNode node, ConnectionKey key) {
        if (cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection()))
            return recoveryDescriptor(outRecDescs, true, node, key);
        else
            return recoveryDescriptor(recoveryDescs, false, node, key);
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
                    "[locNodeId=" + locNodeSupplier.get().id() +
                    ", key=" + key + ", rmtNode=" + node + ']');

            int maxSize = Math.max(cfg.messageQueueLimit(), cfg.ackSendThreshold());

            int queueLimit = cfg.unackedMsgsBufferSize() != 0 ? cfg.unackedMsgsBufferSize() : (maxSize * 128);

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

    public void onChannelCreate(
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

                    Runnable closeRun = () -> {
                        // Close session if request not complete yet.
                        GridFutureAdapter<Channel> rq = ses.meta(CHANNEL_FUT_META);

                        assert rq != null;

                        if (rq.onDone(handshakeTimeoutException()))
                            ses.close();
                    };

                    handshakeTimeoutExecutorService.schedule(closeRun, cfg.connectionTimeout(), TimeUnit.MILLISECONDS);
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
     * @param key The connection key to cleanup descriptors on local node.
     */
    private void cleanupLocalNodeRecoveryDescriptor(ConnectionKey key) {

        if (usePairedConnections(locNodeSupplier.get(), attrs.pairedConnection())) {
            inRecDescs.remove(key);
            outRecDescs.remove(key);
        }
        else
            recoveryDescs.remove(key);
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
        HandshakeTimeoutObject timeoutObject = new HandshakeTimeoutObject(ch);

        handshakeTimeoutExecutorService.schedule(timeoutObject, timeout, TimeUnit.MILLISECONDS);

        try {
            return tcpHandshakeExecutor.tcpHandshake(ch, rmtNodeId, sslMeta, msg);
        }
        catch (IOException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to read from channel: " + e);

            throw new IgniteCheckedException("Failed to read from channel.", e);
        }
        finally {
            if (!timeoutObject.cancel())
                throw handshakeTimeoutException();
        }
    }

    /**
     * Check is passed  socket address belong to current node. This method should return true only if the passed in
     * address represent an address which will result in a connection to the local node.
     *
     * @param addr address to check.
     * @return true if passed address belongs to local node, otherwise false.
     */
    private boolean isLocalNodeAddress(InetSocketAddress addr) {
        return addr.getPort() == cfg.boundTcpPort()
            && (cfg.localHost().equals(addr.getAddress())
            || addr.getAddress().isAnyLocalAddress()
            || (cfg.localHost().isAnyLocalAddress() && U.isLocalAddress(addr.getAddress())));
    }

    /**
     * Checks client message queue size and initiates client drop if message queue size exceeds the configured limit.
     *
     * @param ses Node communication session.
     * @param msgQueueSize Message queue size.
     */
    private void checkClientQueueSize(GridNioSession ses, int msgQueueSize) {
        if (cfg.slowClientQueueLimit() > 0 && msgQueueSize > cfg.slowClientQueueLimit()) {
            ConnectionKey id = ses.meta(CONN_IDX_META);

            if (id != null) {
                ClusterNode node = stateProvider.getSpiContext().node(id.nodeId());

                if (node != null && node.isClient()) {
                    String msg = "Client node outbound message queue size exceeded slowClientQueueLimit, " +
                        "the client will be dropped " +
                        "(consider changing 'slowClientQueueLimit' configuration property) " +
                        "[srvNode=" + stateProvider.getSpiContext().localNode().id() +
                        ", clientNode=" + node +
                        ", slowClientQueueLimit=" + cfg.slowClientQueueLimit() + ']';

                    U.quietAndWarn(log, msg);

                    stateProvider.getSpiContext().failNode(id.nodeId(), msg);
                }
            }
        }
    }

    /**
     * @param commWorker New recovery and idle clients handler.
     */
    public void communicationWorker(CommunicationWorker commWorker) {
        this.commWorker = commWorker;
    }

    /**
     * @param pool Client pool.
     */
    public void clientPool(ConnectionClientPool pool) {
        /** Client pool for client futures. */
    }

    /**
     * @param sockChFactory New socket channel factory.
     */
    public void socketChannelFactory(ThrowableSupplier<SocketChannel, IOException> sockChFactory) {
        socketChannelFactory = sockChFactory;
    }

    /**
     * @param node Node.
     * @return {@code True} if remote current node cannot receive TCP connections. Applicable for client nodes only.
     */
    private boolean forceClientToServerConnections(ClusterNode node) {
        Boolean forceClientToSrvConnections = node.attribute(attrs.getForceClientServerConnections());

        return Boolean.TRUE.equals(forceClientToSrvConnections);
    }
}
