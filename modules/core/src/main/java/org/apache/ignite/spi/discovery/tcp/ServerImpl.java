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

package org.apache.ignite.spi.discovery.tcp;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.NodeValidationFailedEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryServerOnlyCustomMessage;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.messages.SpanContainer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable;
import org.apache.ignite.internal.util.GridBoundedLinkedHashSet;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryNotification;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.FutureTask;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNodesRing;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDummyWakeupMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRedirectToClient;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage;
import org.apache.ignite.spi.tracing.SpanStatus;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NODE_IDS_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.IgniteFeatures.TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_AUTHENTICATION_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LATE_AFFINITY_ASSIGNMENT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_COMPACT_FOOTER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_DFLT_SUID;
import static org.apache.ignite.internal.processors.security.SecurityUtils.nodeSecurityContext;
import static org.apache.ignite.spi.IgnitePortProtocol.TCP;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_NODE_IDS_HISTORY_SIZE;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.AUTH_FAILED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CHECK_FAILED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CONNECTED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CONNECTING;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DISCONNECTED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DISCONNECTING;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DUPLICATE_ID;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.LEFT;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.LOOPBACK_PROBLEM;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.RING_FAILED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.STOPPING;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.STATUS_OK;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.STATUS_RECON;

/**
 *
 */
@SuppressWarnings({"IfMayBeConditional"})
class ServerImpl extends TcpDiscoveryImpl {
    /** */
    private static final int ENSURED_MSG_HIST_SIZE =
        getInteger(IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE, DFLT_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE);

    /** */
    private static final TcpDiscoveryAbstractMessage WAKEUP = new TcpDiscoveryDummyWakeupMessage();

    /** Maximal interval of connection check to next node in the ring. */
    private static final long MAX_CON_CHECK_INTERVAL = 500;

    /** Interval of checking connection to next node in the ring. */
    private long connCheckInterval;

    /** Fundamental value for connection checking actions. */
    private long connCheckTick;

    /** */
    private IgniteThreadPoolExecutor utilityPool;

    /** Nodes ring. */
    @GridToStringExclude
    private final TcpDiscoveryNodesRing ring = new TcpDiscoveryNodesRing();

    /** Topology snapshots history. */
    private final SortedMap<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

    /** Socket readers. */
    private final Collection<SocketReader> readers = new LinkedList<>();

    /** TCP server for discovery SPI. */
    private TcpServer tcpSrvr;

    /** Message worker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private RingMessageWorker msgWorker;

    /** Thread executing message worker */
    private Thread msgWorkerThread;

    /** Client message workers. */
    private final ConcurrentMap<UUID, ClientMessageWorker> clientMsgWorkers = new ConcurrentHashMap<>();

    /** IP finder cleaner. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private IpFinderCleaner ipFinderCleaner;

    /** Statistics printer thread. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private StatisticsPrinter statsPrinter;

    /** Failed nodes (but still in topology). */
    private final Map<TcpDiscoveryNode, UUID> failedNodes = new HashMap<>();

    /** */
    private final Collection<UUID> failedNodesMsgSent = new HashSet<>();

    /** Leaving nodes (but still in topology). */
    private final Collection<TcpDiscoveryNode> leavingNodes = new HashSet<>();

    /** Collection to track joining nodes. */
    private Set<UUID> joiningNodes = new HashSet<>();

    /** Pending custom messages that should not be sent between NodeAdded and NodeAddFinished messages. */
    private Queue<TcpDiscoveryCustomEventMessage> pendingCustomMsgs = new ArrayDeque<>();

    /** Messages history used for client reconnect. */
    private final EnsuredMessageHistory msgHist = new EnsuredMessageHistory();

    /** If non-shared IP finder is used this flag shows whether IP finder contains local address. */
    private boolean ipFinderHasLocAddr;

    /** Addresses that do not respond during join requests send (for resolving concurrent start). */
    private final Collection<SocketAddress> noResAddrs = new GridConcurrentHashSet<>();

    /** Addresses that incoming join requests send were send from (for resolving concurrent start). */
    private final Collection<SocketAddress> fromAddrs = new GridConcurrentHashSet<>();

    /** Response on join request from coordinator (in case of duplicate ID or auth failure). */
    private final GridTuple<TcpDiscoveryAbstractMessage> joinRes = new GridTuple<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** Discovery state. */
    private TcpDiscoverySpiState spiState = DISCONNECTED;

    /** Last time received message from ring. */
    private volatile long lastRingMsgReceivedTime;

    /** Time of last sent and acknowledged message. */
    private volatile long lastRingMsgSentTime;

    /** */
    private volatile boolean nodeCompactRepresentationSupported =
        true; //assume that local node supports this feature

    /** Map with proceeding ping requests. */
    private final ConcurrentMap<InetSocketAddress, GridPingFutureAdapter<IgniteBiTuple<UUID, Boolean>>> pingMap =
        new ConcurrentHashMap<>();

    /**
     * Maximum size of history of IDs of server nodes ever tried to join current topology (ever sent join request).
     */
    private static final int JOINED_NODE_IDS_HISTORY_SIZE =
        getInteger(IGNITE_NODE_IDS_HISTORY_SIZE, DFLT_NODE_IDS_HISTORY_SIZE);

    /** History of all node UUIDs that current node has seen. */
    private final GridBoundedLinkedHashSet<UUID> nodesIdsHist =
        new GridBoundedLinkedHashSet<>(JOINED_NODE_IDS_HISTORY_SIZE);

    /**
     * @param adapter Adapter.
     */
    ServerImpl(TcpDiscoverySpi adapter) {
        super(adapter);
    }

    /** {@inheritDoc} */
    @Override public String getSpiState() {
        synchronized (mux) {
            return spiState.name();
        }
    }

    /** {@inheritDoc} */
    @Override public int getMessageWorkerQueueSize() {
        return msgWorker.queueSize();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID getCoordinator() {
        TcpDiscoveryNode crd = resolveCoordinator();

        return crd != null ? crd.id() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        assert nodeId != null;

        UUID locNodeId0 = getLocalNodeId();

        if (locNodeId0 != null && locNodeId0.equals(nodeId))
            // Return local node directly.
            return locNode;

        TcpDiscoveryNode node = ring.node(nodeId);

        if (node != null && !node.visible())
            return null;

        return node;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return upcast(ring.visibleRemoteNodes());
    }

    /** {@inheritDoc} */
    @Override public boolean allNodesSupport(IgniteFeatures feature) {
        // It is ok to see visible node without order here because attributes are available when node is created.
        return IgniteFeatures.allNodesSupports(upcast(ring.allNodes()), feature);
    }

    /** {@inheritDoc} */
    @Override public int boundPort() throws IgniteSpiException {
        if (tcpSrvr == null)
            tcpSrvr = new TcpServer(log);

        return tcpSrvr.port;
    }

    /** {@inheritDoc} */
    @Override public long connectionCheckInterval() {
        return connCheckInterval;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        synchronized (mux) {
            spiState = DISCONNECTED;
        }

        lastRingMsgReceivedTime = 0;

        lastRingMsgSentTime = 0;

        // Foundumental timeout value for actions related to connection check.
        connCheckTick = effectiveExchangeTimeout() / 3;

        // Since we take in account time of last sent message, the interval should be quite short to give enough piece
        // of failure detection timeout as send-and-acknowledge timeout of the message to send.
        connCheckInterval = Math.min(connCheckTick, MAX_CON_CHECK_INTERVAL);

        utilityPool = new IgniteThreadPoolExecutor("disco-pool",
            spi.ignite().name(),
            0,
            4,
            2000,
            new LinkedBlockingQueue<>());

        if (debugMode) {
            if (!log.isInfoEnabled())
                throw new IgniteSpiException("Info log level should be enabled for TCP discovery to work " +
                    "in debug mode.");

            debugLogQ = new ConcurrentLinkedDeque<>();

            U.quietAndWarn(log, "TCP discovery SPI is configured in debug mode.");
        }

        // Clear addresses collections.
        fromAddrs.clear();
        noResAddrs.clear();

        msgWorker = new RingMessageWorker(log);

        msgWorkerThread = new MessageWorkerDiscoveryThread(msgWorker, log);
        msgWorkerThread.start();

        if (tcpSrvr == null)
            tcpSrvr = new TcpServer(log);

        spi.initLocalNode(tcpSrvr.port, true);

        if (spi.locNodeAddrs.size() > 1 && log.isDebugEnabled()) {
            if (spi.failureDetectionTimeoutEnabled()) {
                log.debug("This node " + spi.locNode.id() + " has " + spi.locNodeAddrs.size() + " TCP " +
                    "addresses. Note that TcpDiscoverySpi.failureDetectionTimeout works per address sequentially. " +
                    "Setting of several addresses can prolong detection of current node failure.");
            }
            else {
                log.debug("This node " + spi.locNode.id() + " has " + spi.locNodeAddrs.size() + " TPC " +
                    "addresses. With exception of connRecoveryTimeout, timeouts and setting like sockTimeout, " +
                    "ackTimeout, reconCnt in TcpDiscoverySpi work per address sequentially. Setting of several " +
                    "addresses can prolong detection of current node failure.");
            }
        }

        locNode = spi.locNode;

        // Start TCP server thread after local node is initialized.
        new TcpServerThread(tcpSrvr, log).start();

        ring.localNode(locNode);

        if (spi.ipFinder.isShared())
            registerLocalNodeAddress();
        else {
            if (F.isEmpty(spi.ipFinder.getRegisteredAddresses()))
                throw new IgniteSpiException("Non-shared IP finder must have IP addresses specified in " +
                    "TcpDiscoveryIpFinder.getRegisteredAddresses() configuration property " +
                    "(specify list of IP addresses in configuration).");

            ipFinderHasLocAddr = spi.ipFinderHasLocalAddress();
        }

        if (spi.getStatisticsPrintFrequency() > 0 && log.isInfoEnabled()) {
            statsPrinter = new StatisticsPrinter();
            statsPrinter.start();
        }

        joinTopology();

        if (locNode.order() == 1)
            U.enhanceThreadName(msgWorkerThread, "crd");

        if (spi.ipFinder.isShared()) {
            ipFinderCleaner = new IpFinderCleaner();
            ipFinderCleaner.start();
        }

        spi.printStartInfo();
    }

    /** {@inheritDoc} */
    @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        spiCtx.registerPort(tcpSrvr.port, TCP);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        spiStop0(false);
    }

    /**
     * Stops SPI finally or stops SPI for restart.
     *
     * @param disconnect {@code True} if SPI is being disconnected.
     * @throws IgniteSpiException If failed.
     */
    private void spiStop0(boolean disconnect) throws IgniteSpiException {
        if (log.isDebugEnabled()) {
            if (disconnect)
                log.debug("Disconnecting SPI.");
            else
                log.debug("Preparing to start local node stop procedure.");
        }

        if (disconnect) {
            synchronized (mux) {
                spiState = DISCONNECTING;
            }
        }

        if (msgWorker != null && msgWorker.runner() != null && msgWorker.runner().isAlive() && !disconnect) {
            // Send node left message only if it is final stop.
            TcpDiscoveryNodeLeftMessage nodeLeftMsg = new TcpDiscoveryNodeLeftMessage(locNode.id());

            Span rootSpan = tracing.create(TraceableMessagesTable.traceName(nodeLeftMsg.getClass()))
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> locNode.id().toString())
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.CONSISTENT_ID),
                    () -> locNode.consistentId().toString())
                .addLog(() -> "Created");

            nodeLeftMsg.spanContainer().serializedSpanBytes(tracing.serialize(rootSpan));

            msgWorker.addMessage(nodeLeftMsg);

            rootSpan.addLog(() -> "Sent").end();

            synchronized (mux) {
                long timeout = spi.netTimeout;

                long thresholdNanos = System.nanoTime() + U.millisToNanos(timeout);

                while (spiState != LEFT && timeout > 0) {
                    try {
                        mux.wait(timeout);

                        timeout = U.nanosToMillis(thresholdNanos - System.nanoTime());
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();

                        break;
                    }
                }

                if (spiState == LEFT) {
                    if (log.isDebugEnabled())
                        log.debug("Verification for local node leave has been received from coordinator" +
                            " (continuing stop procedure).");
                }
                else if (log.isInfoEnabled()) {
                    log.info("No verification for local node leave has been received from coordinator" +
                        " (will stop node anyway).");
                }
            }
        }

        if (tcpSrvr != null)
            tcpSrvr.stop();

        tcpSrvr = null;

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = U.arrayList(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.interrupt(ipFinderCleaner);
        U.join(ipFinderCleaner, log);

        U.cancel(msgWorker);
        U.join(msgWorker, log);

        for (ClientMessageWorker clientWorker : clientMsgWorkers.values()) {
            if (clientWorker != null) {
                U.interrupt(clientWorker.runner());
                U.join(clientWorker.runner(), log);
            }
        }

        clientMsgWorkers.clear();

        IgniteUtils.shutdownNow(ServerImpl.class, utilityPool, log);

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);

        Collection<TcpDiscoveryNode> nodes = null;

        if (!disconnect)
            spi.printStopInfo();
        else {
            spi.getSpiContext().deregisterPorts();

            nodes = ring.visibleNodes();
        }

        long topVer = ring.topologyVersion();

        ring.clear();

        if (nodes != null) {
            // This is restart/disconnection and we need to fire FAIL event for each remote node.
            DiscoverySpiListener lsnr = spi.lsnr;

            if (lsnr != null) {
                Collection<ClusterNode> processed = new HashSet<>(nodes.size());

                for (TcpDiscoveryNode n : nodes) {
                    if (n.isLocal())
                        continue;

                    assert n.visible();

                    processed.add(n);

                    List<ClusterNode> top = U.arrayList(nodes, F.notIn(processed));

                    topVer++;

                    Map<Long, Collection<ClusterNode>> hist = updateTopologyHistory(topVer,
                        Collections.unmodifiableList(top));

                    lsnr.onDiscovery(
                        new DiscoveryNotification(EVT_NODE_FAILED, topVer, n, top, hist, null, null)
                    ).get();
                }
            }
        }

        printStatistics();

        spi.stats.clear();

        synchronized (mux) {
            // Clear stored data.
            leavingNodes.clear();
            failedNodes.clear();

            spiState = DISCONNECTED;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        assert nodeId != null;

        if (nodeId == getLocalNodeId())
            return true;

        TcpDiscoveryNode node = ring.node(nodeId);

        if (node == null)
            return false;

        if (!nodeAlive(nodeId))
            return false;

        long start = U.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Pinging node: " + nodeId);

        boolean res = pingNode(node);

        long end = System.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Finished node ping [nodeId=" + nodeId + ", res=" + res + ", time=" + (end - start) + "ms]");

        if (!res && node.clientRouterNodeId() == null && nodeAlive(nodeId)) {
            LT.warn(log, "Failed to ping node (status check will be initiated): " + nodeId);

            msgWorker.addMessage(createTcpDiscoveryStatusCheckMessage(locNode, locNode.id(), node.id()));
        }

        return res;
    }

    /**
     * Creates new instance of {@link TcpDiscoveryStatusCheckMessage} trying to choose most optimal constructor.
     *
     * @param creatorNode Creator node. It can be null when message will be sent from coordinator to creator node,
     * in this case it will not contain creator node addresses as it will be discarded by creator; otherwise,
     * this parameter must not be null.
     * @param creatorNodeId Creator node id.
     * @param failedNodeId Failed node id.
     * @return <code>null</code> if <code>creatorNode</code> is null and we cannot retrieve creator node from ring
     * by <code>creatorNodeId</code>, and new instance of {@link TcpDiscoveryStatusCheckMessage} in other cases.
     */
    private @Nullable TcpDiscoveryStatusCheckMessage createTcpDiscoveryStatusCheckMessage(
        @Nullable TcpDiscoveryNode creatorNode,
        UUID creatorNodeId,
        UUID failedNodeId
    ) {
        TcpDiscoveryStatusCheckMessage msg;

        if (nodeCompactRepresentationSupported) {
            TcpDiscoveryNode crd = resolveCoordinator();

            if (creatorNode == null)
                msg = new TcpDiscoveryStatusCheckMessage(creatorNodeId, null, failedNodeId);
            else {
                boolean sameMacs = creatorNode != null && crd != null && U.sameMacs(creatorNode, crd);

                msg = new TcpDiscoveryStatusCheckMessage(
                    creatorNode.id(),
                    spi.getNodeAddresses(creatorNode, sameMacs),
                    failedNodeId
                );
            }
        }
        else {
            if (creatorNode == null) {
                TcpDiscoveryNode node = ring.node(creatorNodeId);

                if (node == null)
                    return null;
                else
                    msg = new TcpDiscoveryStatusCheckMessage(node, failedNodeId);
            }
            else
                msg = new TcpDiscoveryStatusCheckMessage(creatorNode, failedNodeId);
        }

        return msg;
    }

    /**
     * Creates new instance of {@link TcpDiscoveryDuplicateIdMessage}, trying to choose most optimal constructor.
     *
     * @param creatorNodeId Creator node id.
     * @param node Node with duplicate ID.
     * @return new instance of {@link TcpDiscoveryDuplicateIdMessage}.
     */
    private TcpDiscoveryDuplicateIdMessage createTcpDiscoveryDuplicateIdMessage(
        UUID creatorNodeId,
        TcpDiscoveryNode node
    ) {
        TcpDiscoveryDuplicateIdMessage msg;

        if (nodeCompactRepresentationSupported)
            msg = new TcpDiscoveryDuplicateIdMessage(creatorNodeId, node.id());
        else
            msg = new TcpDiscoveryDuplicateIdMessage(creatorNodeId, node);

        return msg;
    }

    /**
     * For test purposes only.
     *
     * @return Current number of client workers.
     */
    int clientWorkersCount() {
        synchronized (mux) {
            return clientMsgWorkers.size();
        }
    }

    /**
     * Pings the remote node to see if it's alive.
     *
     * @param node Node.
     * @return {@code True} if ping succeeds.
     */
    private boolean pingNode(TcpDiscoveryNode node) {
        assert node != null;

        if (node.id().equals(getLocalNodeId()))
            return true;

        UUID clientNodeId = null;

        if (node.clientRouterNodeId() != null) {
            clientNodeId = node.id();

            node = ring.node(node.clientRouterNodeId());

            if (node == null || !nodeAlive(node.id()))
                return false;
        }

        for (InetSocketAddress addr : spi.getNodeAddresses(node, U.sameMacs(locNode, node))) {
            try {
                // ID returned by the node should be the same as ID of the parameter for ping to succeed.
                IgniteBiTuple<UUID, Boolean> t = pingNode(addr, node.id(), clientNodeId);

                if (t == null)
                    // Remote node left topology.
                    return false;

                boolean res = node.id().equals(t.get1()) && (clientNodeId == null || t.get2());

                if (res)
                    node.lastSuccessfulAddress(addr);

                return res;
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to ping node [node=" + node + ", err=" + e.getMessage() + ']');

                onException("Failed to ping node [node=" + node + ", err=" + e.getMessage() + ']', e);
                // continue;
            }
        }

        return false;
    }

    /**
     * Pings the node by its address to see if it's alive.
     *
     * @param addr Address of the node.
     * @param nodeId Node ID to ping. In case when client node ID is not null this node ID is an ID of the router node.
     * @param clientNodeId Client node ID.
     * @return ID of the remote node and "client exists" flag if node alive or {@code null} if the remote node has
     *         left a topology during the ping process.
     * @throws IgniteCheckedException If an error occurs.
     */
    @Nullable private IgniteBiTuple<UUID, Boolean> pingNode(InetSocketAddress addr, @Nullable UUID nodeId,
        @Nullable UUID clientNodeId) throws IgniteCheckedException {
        assert addr != null;

        UUID locNodeId = getLocalNodeId();

        IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi,
            clientNodeId == null);

        if (F.contains(spi.locNodeAddrs, addr)) {
            if (clientNodeId == null)
                return F.t(getLocalNodeId(), false);

            ClientMessageWorker clientWorker = clientMsgWorkers.get(clientNodeId);

            if (clientWorker == null)
                return F.t(getLocalNodeId(), false);

            boolean clientPingRes;

            try {
                clientPingRes = clientWorker.ping(timeoutHelper);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedCheckedException(e);
            }

            return F.t(getLocalNodeId(), clientPingRes);
        }

        GridPingFutureAdapter<IgniteBiTuple<UUID, Boolean>> fut = new GridPingFutureAdapter<>(nodeId);

        InetSocketAddress addrKey = addr;

        GridPingFutureAdapter<IgniteBiTuple<UUID, Boolean>> oldFut = pingMap.putIfAbsent(addrKey, fut);

        if (oldFut != null)
            return oldFut.get();
        else {
            Collection<Throwable> errs = null;

            try {
                Socket sock = null;

                int reconCnt = 0;

                boolean openedSock = false;

                while (true) {
                    try {
                        if (addr.isUnresolved())
                            addr = new InetSocketAddress(InetAddress.getByName(addr.getHostName()), addr.getPort());

                        sock = spi.createSocket();

                        fut.sock = sock;

                        sock = spi.openSocket(sock, addr, timeoutHelper);

                        openedSock = true;

                        spi.writeToSocket(sock, new TcpDiscoveryPingRequest(locNodeId, clientNodeId),
                            timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                        TcpDiscoveryPingResponse res = spi.readMessage(sock, null, timeoutHelper.nextTimeoutChunk(
                            spi.getAckTimeout()));

                        if (locNodeId.equals(res.creatorNodeId())) {
                            if (log.isDebugEnabled())
                                log.debug("Ping response from local node: " + res);

                            break;
                        }

                        IgniteBiTuple<UUID, Boolean> t = F.t(res.creatorNodeId(), res.clientExists());

                        fut.onDone(t);

                        return t;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        if (nodeId != null && !nodeAlive(nodeId)) {
                            log.warning("Failed to ping node [nodeId=" + nodeId + "]. Node has left or is " +
                                "leaving topology. Cause: " + e.getMessage());

                            fut.onDone((IgniteBiTuple<UUID, Boolean>)null);

                            return null;
                        }

                        if (errs == null)
                            errs = new ArrayList<>();

                        errs.add(e);

                        reconCnt++;

                        if (!openedSock && reconCnt == 2) {
                            log.warning("Failed to ping node [nodeId=" + nodeId + "]. Was unable to open the " +
                                "socket at all. Cause: " + e.getMessage());

                            break;
                        }

                        if (spi.failureDetectionTimeoutEnabled() && timeoutHelper.checkFailureTimeoutReached(e)) {
                            log.warning("Failed to ping node [nodeId=" + nodeId + "]. Reached the timeout " +
                                spi.failureDetectionTimeout() + "ms. Cause: " + e.getMessage());

                            break;
                        }
                        else if (!spi.failureDetectionTimeoutEnabled() && reconCnt == spi.getReconnectCount()) {
                            log.warning("Failed to ping node [nodeId=" + nodeId + "]. Reached the reconnection " +
                                "count " + spi.getReconnectCount() + ". Cause: " + e.getMessage());

                            break;
                        }

                        if (spi.isNodeStopping0()) {
                            log.warning("Failed to ping node [nodeId=" + nodeId + "]. Current node is " +
                                "stopping. Cause: " + e.getMessage());

                            break;
                        }
                    }
                    finally {
                        U.closeQuiet(sock);
                    }
                }
            }
            catch (Throwable t) {
                fut.onDone(t);

                if (t instanceof Error)
                    throw t;

                throw U.cast(t);
            }
            finally {
                if (!fut.isDone())
                    fut.onDone(U.exceptionWithSuppressed("Failed to ping node by address: " + addr, errs));

                boolean b = pingMap.remove(addrKey, fut);

                assert b;
            }

            return fut.get();
        }
    }

    /**
     * Interrupts all existed 'ping' request for the given node.
     *
     * @param node Node that may be pinged.
     */
    private void interruptPing(TcpDiscoveryNode node) {
        for (InetSocketAddress addr : spi.getNodeAddresses(node)) {
            GridPingFutureAdapter fut = pingMap.get(addr);

            if (fut != null && fut.sock != null) {
                if (fut.nodeId == null || fut.nodeId.equals(node.id()))
                    // Reference to the socket is not set to null. No need to assign it to a local variable.
                    U.closeQuiet(fut.sock);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        spiStop0(true);
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage evt) {
        try {
            TcpDiscoveryCustomEventMessage msg;

            if (((CustomMessageWrapper)evt).delegate() instanceof DiscoveryServerOnlyCustomMessage)
                msg = new TcpDiscoveryServerOnlyCustomEventMessage(getLocalNodeId(), evt,
                    U.marshal(spi.marshaller(), evt));
            else
                msg = new TcpDiscoveryCustomEventMessage(getLocalNodeId(), evt,
                    U.marshal(spi.marshaller(), evt));

            Span rootSpan = tracing.create(TraceableMessagesTable.traceName(msg.getClass()))
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> getLocalNodeId().toString())
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.CONSISTENT_ID),
                    () -> locNode.consistentId().toString())
                .addTag(SpanTags.MESSAGE_CLASS, () -> ((CustomMessageWrapper)evt).delegate().getClass().getSimpleName())
                .addLog(() -> "Created");

            // This root span will be parent both from local and remote nodes.
            msg.spanContainer().serializedSpanBytes(tracing.serialize(rootSpan));

            msgWorker.addMessage(msg);

            rootSpan.addLog(() -> "Sent").end();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal custom event: " + evt, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        TcpDiscoveryNode node = ring.node(nodeId);

        if (node != null) {
            TcpDiscoveryNodeFailedMessage msg = new TcpDiscoveryNodeFailedMessage(getLocalNodeId(),
                node.id(),
                node.internalOrder());

            msg.warning(warning);

            msg.force(true);

            msgWorker.addMessage(msg);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onMessageExchanged() {
        if (spi.failureDetectionTimeoutEnabled() && locNode != null)
            locNode.lastExchangeTime(U.currentTimeMillis(), System.nanoTime());
    }

    /**
     * Checks whether a node is alive or not.
     *
     * @param nodeId Node ID.
     * @return {@code True} if node is in the ring and is not being removed from.
     */
    private boolean nodeAlive(UUID nodeId) {
        // Is node alive or about to be removed from the ring?
        TcpDiscoveryNode node = ring.node(nodeId);

        boolean nodeAlive = node != null && node.visible();

        if (nodeAlive) {
            synchronized (mux) {
                nodeAlive = !F.transform(failedNodes.keySet(), F.node2id()).contains(nodeId) &&
                    !F.transform(leavingNodes, F.node2id()).contains(nodeId);
            }
        }

        return nodeAlive;
    }

    /**
     * Tries to join this node to topology.
     *
     * @throws IgniteSpiException If any error occurs.
     */
    private void joinTopology() throws IgniteSpiException {
        synchronized (mux) {
            assert spiState == CONNECTING || spiState == DISCONNECTED;

            spiState = CONNECTING;
        }

        SecurityCredentials locCred = (SecurityCredentials)locNode.getAttributes()
            .get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

        boolean auth = false;

        if (spi.nodeAuth != null && spi.nodeAuth.isGlobalNodeAuthentication()) {
            localAuthentication(locCred);

            auth = true;
        }

        // Marshal credentials for backward compatibility and security.
        marshalCredentials(locNode, locCred);

        DiscoveryDataPacket discoveryData = spi.collectExchangeData(new DiscoveryDataPacket(getLocalNodeId()));

        TcpDiscoveryJoinRequestMessage joinReqMsg = new TcpDiscoveryJoinRequestMessage(locNode, discoveryData);

        joinReqMsg.spanContainer().span(
            tracing.create(TraceableMessagesTable.traceName(joinReqMsg.getClass()))
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> locNode.id().toString())
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.CONSISTENT_ID),
                    () -> locNode.consistentId().toString())
                .addLog(() -> "Created")
        );

        tracing.messages().beforeSend(joinReqMsg);

        while (true) {
            if (!sendJoinRequestMessage(joinReqMsg)) {
                if (log.isDebugEnabled())
                    log.debug("Join request message has not been sent (local node is the first in the topology).");

                if (!auth && spi.nodeAuth != null)
                    localAuthentication(locCred);

                // TODO IGNITE-11272
                FutureTask<Void> fut = msgWorker.addTask(new FutureTask<Void>() {
                    @Override protected Void body() {
                        pendingCustomMsgs.clear();
                        msgWorker.pendingMsgs.reset(null, null, null);
                        msgWorker.next = null;
                        failedNodes.clear();
                        leavingNodes.clear();
                        failedNodesMsgSent.clear();

                        locNode.attributes().remove(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

                        locNode.order(1);
                        locNode.internalOrder(1);

                        spi.gridStartTime = U.currentTimeMillis();

                        locNode.visible(true);

                        ring.clear();

                        ring.topologyVersion(1);

                        synchronized (mux) {
                            topHist.clear();

                            spiState = CONNECTED;

                            mux.notifyAll();
                        }

                        notifyDiscovery(EVT_NODE_JOINED, 1, locNode, joinReqMsg.spanContainer());

                        return null;
                    }
                });

                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteSpiException(e);
                }

                msgWorker.nullifyDiscoData();

                break;
            }

            if (log.isDebugEnabled())
                log.debug("Join request message has been sent (waiting for coordinator response).");

            synchronized (mux) {
                long timeout = spi.netTimeout;

                long thresholdNanos = System.nanoTime() + U.millisToNanos(timeout);

                while (spiState == CONNECTING && timeout > 0) {
                    try {
                        mux.wait(timeout);

                        timeout = U.nanosToMillis(thresholdNanos - System.nanoTime());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteSpiException("Thread has been interrupted.", e);
                    }
                }

                if (spiState == CONNECTED)
                    break;
                else if (spiState == DUPLICATE_ID)
                    throw spi.duplicateIdError((TcpDiscoveryDuplicateIdMessage)joinRes.get());
                else if (spiState == AUTH_FAILED)
                    throw spi.authenticationFailedError((TcpDiscoveryAuthFailedMessage)joinRes.get());
                else if (spiState == CHECK_FAILED)
                    throw spi.checkFailedError((TcpDiscoveryCheckFailedMessage)joinRes.get());
                else if (spiState == RING_FAILED) {
                    throw new IgniteSpiException("Unable to connect to next nodes in a ring, it seems local node is " +
                        "experiencing connectivity issues or the rest of the cluster is undergoing massive restarts. " +
                        "Failing local node join to avoid case when one node fails a big part of cluster. To disable" +
                        " this behavior set TcpDiscoverySpi.setConnectionRecoveryTimeout() to 0. " +
                        "[connRecoveryTimeout=" + spi.connRecoveryTimeout + ", effectiveConnRecoveryTimeout="
                        + spi.getEffectiveConnectionRecoveryTimeout() + ']');
                }
                else if (spiState == LOOPBACK_PROBLEM) {
                    TcpDiscoveryLoopbackProblemMessage msg = (TcpDiscoveryLoopbackProblemMessage)joinRes.get();

                    boolean locHostLoopback = spi.locHost.isLoopbackAddress();

                    String firstNode = locHostLoopback ? "local" : "remote";

                    String secondNode = locHostLoopback ? "remote" : "local";

                    throw new IgniteSpiException("Failed to add node to topology because " + firstNode +
                        " node is configured to use loopback address, but " + secondNode + " node is not " +
                        "(consider changing 'localAddress' configuration parameter) " +
                        "[locNodeAddrs=" + U.addressesAsString(locNode) + ", rmtNodeAddrs=" +
                        U.addressesAsString(msg.addresses(), msg.hostNames()) +
                        ", creatorNodeId=" + msg.creatorNodeId() + ']');
                }
                else
                    LT.warn(log, "Node has not been connected to topology and will repeat join process. " +
                        "Check remote nodes logs for possible error messages. " +
                        "Note that large topology may require significant time to start. " +
                        "Increase 'TcpDiscoverySpi.networkTimeout' configuration property " +
                        "if getting this message on the starting nodes [networkTimeout=" + spi.netTimeout + ']');
            }
        }

        assert locNode.order() != 0;
        assert locNode.internalOrder() != 0;

        if (log.isDebugEnabled())
            log.debug("Discovery SPI has been connected to topology with order: " + locNode.internalOrder());

        joinReqMsg.spanContainer().span()
            .addTag(SpanTags.tag(SpanTags.NODE, SpanTags.ORDER), () -> String.valueOf(locNode.order()))
            .addLog(() -> "Joined to ring")
            .end();
    }

    /**
     * Authenticate local node.
     *
     * @param locCred Local security credentials for authentication.
     * @throws IgniteSpiException If any error occurs.
     */
    private void localAuthentication(SecurityCredentials locCred) {
        assert spi.nodeAuth != null;
        assert locCred != null || locNode.attribute(ATTR_AUTHENTICATION_ENABLED) != null;

        try {
            SecurityContext subj = spi.nodeAuth.authenticateNode(locNode, locCred);

            if (subj == null)
                throw new IgniteSpiException("Authentication failed for local node: " + locNode.id());

            Map<String, Object> attrs = new HashMap<>(locNode.attributes());

            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2, U.marshal(spi.marshaller(), subj));

            locNode.setAttributes(attrs);

        }
        catch (IgniteException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to authenticate local node (will shutdown local node).", e);
        }
    }

    /**
     * Tries to send join request message to a random node presenting in topology.
     * Address is provided by {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} and message is
     * sent to first node connection succeeded to.
     *
     * @param joinMsg Join request message.
     * @return {@code true} if send succeeded.
     * @throws IgniteSpiException If any error occurs.
     */
    private boolean sendJoinRequestMessage(TcpDiscoveryJoinRequestMessage joinMsg) throws IgniteSpiException {
        // Time when join process started.
        long joinStartNanos = 0;

        while (true) {
            Collection<InetSocketAddress> addrs = spi.resolvedAddresses();

            if (F.isEmpty(addrs))
                return false;

            boolean retry = false;
            boolean joinImpossible = false;

            Collection<Exception> errs = new ArrayList<>();

            for (InetSocketAddress addr : addrs) {
                try {
                    IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi, true);

                    Integer res;

                    try {
                        SecurityUtils.serializeVersion(1);

                        res = sendMessageDirectly(joinMsg, addr, timeoutHelper);
                    }
                    finally {
                        SecurityUtils.restoreDefaultSerializeVersion();
                    }

                    assert res != null;

                    noResAddrs.remove(addr);

                    //join timeout should not be reset if response was received from another CONNECTING node
                    //(only CONNECTING node sends back WAIT and CONTINUE_JOIN codes),
                    //otherwise two CONNECTING nodes can stuck in infinite loop sending join reqs to each other forever
                    if (res != RES_WAIT && res != RES_CONTINUE_JOIN)
                        joinStartNanos = 0;

                    switch (res) {
                        case RES_WAIT:
                            // Concurrent startup, try sending join request again or wait if no success.
                            retry = true;

                            break;
                        case RES_OK:
                            if (log.isDebugEnabled())
                                log.debug("Join request message has been sent to address [addr=" + addr +
                                    ", req=" + joinMsg + ']');

                            // Join request sending succeeded, wait for response from topology.
                            return true;

                        case RES_JOIN_IMPOSSIBLE:
                            joinImpossible = true;

                            break;

                        default:
                            // Concurrent startup, try next node.
                            if (res == RES_CONTINUE_JOIN) {
                                if (!fromAddrs.contains(addr))
                                    retry = true;
                            }
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Unexpected response to join request: " + res);

                                retry = true;
                            }

                            break;
                    }
                }
                catch (IgniteSpiException e) {
                    errs.add(e);

                    if (log.isDebugEnabled()) {
                        IOException ioe = X.cause(e, IOException.class);

                        log.debug("Failed to send join request message [addr=" + addr +
                            ", msg=" + (ioe != null ? ioe.getMessage() : e.getMessage()) + ']');

                        onException("Failed to send join request message [addr=" + addr +
                            ", msg=" + (ioe != null ? ioe.getMessage() : e.getMessage()) + ']', ioe);
                    }

                    noResAddrs.add(addr);
                }

                if (joinImpossible)
                    throw new IgniteSpiException("Impossible to continue join, " +
                        "check if local discovery and communication ports " +
                        "are not blocked with firewall [addr=" + addr +
                        ", req=" + joinMsg + ", discoLocalPort=" + spi.getLocalPort() +
                        ", discoLocalPortRange=" + spi.getLocalPortRange() + ']');
            }

            if (retry) {
                if (log.isDebugEnabled())
                    log.debug("Concurrent discovery SPI start has been detected (local node should wait).");

                try {
                    U.sleep(spi.getReconnectDelay());
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteSpiException("Thread has been interrupted.", e);
                }
            }
            else if (!spi.ipFinder.isShared() && !ipFinderHasLocAddr) {
                IgniteCheckedException e = null;

                if (!errs.isEmpty()) {
                    e = new IgniteCheckedException("Multiple connection attempts failed.");

                    for (Exception err : errs)
                        e.addSuppressed(err);
                }

                if (X.hasCause(e, ConnectException.class)) {
                    LT.warn(log, "Failed to connect to any address from IP finder " +
                        "(make sure IP finder addresses are correct and firewalls are disabled on all host machines): " +
                        toOrderedList(addrs), true);
                }

                if (spi.joinTimeout > 0) {
                    if (joinStartNanos == 0)
                        joinStartNanos = System.nanoTime();
                    else if (U.millisSinceNanos(joinStartNanos) > spi.joinTimeout)
                        throw new IgniteSpiException(
                            "Failed to connect to any address from IP finder within join timeout " +
                                "(make sure IP finder addresses are correct, and operating system firewalls are disabled " +
                                "on all host machines, or consider increasing 'joinTimeout' configuration property): " +
                                addrs, e);
                }

                try {
                    U.sleep(spi.getReconnectDelay());
                }
                catch (IgniteInterruptedCheckedException ex) {
                    throw new IgniteSpiException("Thread has been interrupted.", ex);
                }
            }
            else
                break;
        }

        return false;
    }

    /**
     * Establishes connection to an address, sends message and returns the response (if any).
     *
     * @param msg Message to send.
     * @param addr Address to send message to.
     * @param timeoutHelper Operation timeout helper.
     * @return Response read from the recipient or {@code null} if no response is supposed.
     * @throws IgniteSpiException If an error occurs.
     */
    @Nullable private Integer sendMessageDirectly(TcpDiscoveryAbstractMessage msg, InetSocketAddress addr,
        IgniteSpiOperationTimeoutHelper timeoutHelper)
        throws IgniteSpiException {
        assert msg != null;
        assert addr != null;

        Collection<Throwable> errs = null;

        long ackTimeout0 = spi.getAckTimeout();

        int connectAttempts = 1;

        int sslConnectAttempts = 3;

        boolean joinReqSent;

        UUID locNodeId = getLocalNodeId();

        int reconCnt = 0;

        while (true) {
            // Need to set to false on each new iteration,
            // since remote node may leave in the middle of the first iteration.
            joinReqSent = false;

            boolean openSock = false;

            Socket sock = null;

            try {
                long tsNanos = System.nanoTime();

                sock = spi.openSocket(addr, timeoutHelper);

                openSock = true;

                TcpDiscoveryHandshakeRequest req = new TcpDiscoveryHandshakeRequest(locNodeId);

                // Handshake.
                spi.writeToSocket(sock, req, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null, timeoutHelper.nextTimeoutChunk(
                    ackTimeout0));

                if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                    boolean ignore = false;

                    // During marshalling, SPI didn't know whether all nodes support compression as we didn't join yet.
                    // The only way to know is passing flag directly with handshake response.
                    if (!res.isDiscoveryDataPacketCompression())
                        ((TcpDiscoveryJoinRequestMessage)msg).gridDiscoveryData().unzipData(log);

                    synchronized (mux) {
                        for (TcpDiscoveryNode failedNode : failedNodes.keySet()) {
                            if (failedNode.id().equals(res.creatorNodeId())) {
                                if (log.isDebugEnabled())
                                    log.debug("Ignore response from node from failed list: " + res);

                                ignore = true;

                                break;
                            }
                        }
                    }

                    if (ignore)
                        break;
                }

                if (locNodeId.equals(res.creatorNodeId())) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake response from local node: " + res);

                    break;
                }

                // Send message.
                tsNanos = System.nanoTime();

                spi.writeToSocket(sock, msg, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                long tsNanos0 = System.nanoTime();

                if (debugMode)
                    debugLog(msg, "Message has been sent directly to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + res.creatorNodeId() + ']');

                if (log.isDebugEnabled())
                    log.debug("Message has been sent directly to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + res.creatorNodeId() + ']');

                // Connection has been established, but
                // join request may not be unmarshalled on remote host.
                // E.g. due to class not found issue.
                joinReqSent = msg instanceof TcpDiscoveryJoinRequestMessage;

                int receipt = spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                spi.stats.onMessageSent(msg, U.nanosToMillis(tsNanos0 - tsNanos));

                return receipt;
            }
            catch (ClassCastException e) {
                // This issue is rarely reproducible on AmazonEC2, but never
                // on dedicated machines.
                if (log.isDebugEnabled())
                    U.error(log, "Class cast exception on direct send: " + addr, e);

                onException("Class cast exception on direct send: " + addr, e);

                if (errs == null)
                    errs = new ArrayList<>();

                errs.add(e);
            }
            catch (IOException | IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.error("Exception on direct send: " + e.getMessage(), e);

                onException("Exception on direct send: " + e.getMessage(), e);

                if (errs == null)
                    errs = new ArrayList<>();

                errs.add(e);

                if (X.hasCause(e, SSLException.class)) {
                    if (--sslConnectAttempts == 0)
                        throw new IgniteException("Unable to establish secure connection. " +
                            "Was remote cluster configured with SSL? [rmtAddr=" + addr + ", errMsg=\"" + e.getMessage() + "\"]", e);

                    continue;
                }

                if (X.hasCause(e, StreamCorruptedException.class)) {
                    // StreamCorruptedException could be caused by remote node failover
                    if (connectAttempts < 2) {
                        connectAttempts++;

                        continue;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Connect failed with StreamCorruptedException, skip address: " + addr);

                    break;
                }

                if (spi.failureDetectionTimeoutEnabled() && timeoutHelper.checkFailureTimeoutReached(e))
                    break;

                if (!spi.failureDetectionTimeoutEnabled() && ++reconCnt == spi.getReconnectCount())
                    break;

                if (!openSock) {
                    // Reconnect for the second time, if connection is not established.
                    if (connectAttempts < 2) {
                        connectAttempts++;

                        continue;
                    }

                    break; // Don't retry if we can not establish connection.
                }

                if (!spi.failureDetectionTimeoutEnabled() && (e instanceof SocketTimeoutException ||
                    X.hasCause(e, SocketTimeoutException.class))) {
                    ackTimeout0 *= 2;

                    if (!checkAckTimeout(ackTimeout0))
                        break;
                }
            }
            finally {
                U.closeQuiet(sock);
            }
        }

        if (joinReqSent) {
            if (log.isDebugEnabled())
                log.debug("Join request has been sent, but receipt has not been read (returning RES_WAIT).");

            // Topology will not include this node,
            // however, warning on timed out join will be output.
            return RES_OK;
        }

        throw new IgniteSpiException(
            "Failed to send message to address [addr=" + addr + ", msg=" + msg + ']',
            U.exceptionWithSuppressed("Failed to send message to address " +
                "[addr=" + addr + ", msg=" + msg + ']', errs));
    }

    /**
     * Marshalls credentials with discovery SPI marshaller (will replace attribute value).
     *
     * @param node Node to marshall credentials for.
     * @param cred Credentials for marshall.
     * @throws IgniteSpiException If marshalling failed.
     */
    private void marshalCredentials(TcpDiscoveryNode node, SecurityCredentials cred) throws IgniteSpiException {
        try {
            // Use security-unsafe getter.
            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, spi.marshaller().marshal(cred));

            node.setAttributes(attrs);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal node security credentials: " + node.id(), e);
        }
    }

    /**
     * Unmarshalls credentials with discovery SPI marshaller (will not replace attribute value).
     *
     * @param node Node to unmarshall credentials for.
     * @return Security credentials.
     * @throws IgniteSpiException If unmarshal fails.
     */
    private SecurityCredentials unmarshalCredentials(TcpDiscoveryNode node) throws IgniteSpiException {
        try {
            byte[] credBytes = (byte[])node.getAttributes().get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

            if (credBytes == null)
                return null;

            return U.unmarshal(spi.marshaller(), credBytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to unmarshal node security credentials: " + node.id(), e);
        }
    }

    /**
     * @param type Event type.
     * @param topVer Topology version.
     * @param node Event node.
     */
    private void notifyDiscovery(int type, long topVer, TcpDiscoveryNode node) {
        notifyDiscovery(type, topVer, node, null);
    }

    /**
     * Notify external listener on discovery event.
     *
     * @param type Discovery event type. See {@link org.apache.ignite.events.DiscoveryEvent} for more details.
     * @param topVer Topology version.
     * @param node Remote node this event is connected with.
     */
    private boolean notifyDiscovery(int type, long topVer, TcpDiscoveryNode node, SpanContainer spanContainer) {
        assert type > 0;
        assert node != null;

        DiscoverySpiListener lsnr = spi.lsnr;

        TcpDiscoverySpiState spiState = spiStateCopy();

        DebugLogger log = type == EVT_NODE_METRICS_UPDATED ? traceLog : debugLog;

        if (lsnr != null && node.visible() && (spiState == CONNECTED || spiState == DISCONNECTING)) {
            if (log.isDebugEnabled())
                log.debug("Discovery notification [node=" + node + ", spiState=" + spiState +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');

            Collection<ClusterNode> top = upcast(ring.visibleNodes());

            Map<Long, Collection<ClusterNode>> hist = updateTopologyHistory(topVer, top);

            lsnr.onDiscovery(
                new DiscoveryNotification(type, topVer, node, top, hist, null, spanContainer)
            );

            return true;
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", spiState=" + spiState +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');
        }

        return false;
    }

    /**
     * Update topology history with new topology snapshots.
     *
     * @param topVer Topology version.
     * @param top Topology snapshot.
     * @return Copy of updated topology history.
     */
    @Nullable private Map<Long, Collection<ClusterNode>> updateTopologyHistory(long topVer, Collection<ClusterNode> top) {
        synchronized (mux) {
            if (topHist.containsKey(topVer))
                return null;

            topHist.put(topVer, top);

            while (topHist.size() > spi.topHistSize)
                topHist.remove(topHist.firstKey());

            if (log.isDebugEnabled())
                log.debug("Added topology snapshot to history, topVer=" + topVer + ", historySize=" + topHist.size());

            return new TreeMap<>(topHist);
        }
    }

    /**
     * Checks whether local node is coordinator. Nodes that are leaving or failed
     * (but are still in topology) are removed from search.
     *
     * @return {@code true} if local node is coordinator.
     */
    public boolean isLocalNodeCoordinator() {
        synchronized (mux) {
            boolean crd = spiState == CONNECTED && locNode.equals(resolveCoordinator());

            if (crd)
                spi.stats.onBecomingCoordinator();

            return crd;
        }
    }

    /**
     * @return Spi state copy.
     */
    private TcpDiscoverySpiState spiStateCopy() {
        TcpDiscoverySpiState state;

        synchronized (mux) {
            state = spiState;
        }

        return state;
    }

    /**
     * Resolves coordinator. Nodes that are leaving or failed (but are still in
     * topology) are removed from search.
     *
     * @return Coordinator node or {@code null} if there are no coordinator
     * (i.e. local node is the last one and is currently stopping).
     */
    @Nullable private TcpDiscoveryNode resolveCoordinator() {
        synchronized (mux) {
            Collection<TcpDiscoveryNode> excluded = F.concat(false, failedNodes.keySet(), leavingNodes);

            return ring.coordinator(excluded);
        }
    }

    /**
     * Prints SPI statistics.
     */
    private void printStatistics() {
        if (log.isInfoEnabled() && spi.statsPrintFreq > 0) {
            int failedNodesSize;
            int leavingNodesSize;
            int joiningNodesSize;
            int pendingCustomMsgsSize;

            synchronized (mux) {
                failedNodesSize = failedNodes.size();
                leavingNodesSize = leavingNodes.size();
                joiningNodesSize = joiningNodes.size();
                pendingCustomMsgsSize = pendingCustomMsgs.size();
            }

            Runtime runtime = Runtime.getRuntime();

            TcpDiscoveryNode coord = resolveCoordinator();

            if (log.isInfoEnabled())
                log.info("Discovery SPI statistics [statistics=" + spi.stats + ", spiState=" + spiStateCopy() +
                    ", coord=" + coord +
                    ", next=" + (msgWorker != null ? msgWorker.next : "N/A") +
                    ", intOrder=" + (locNode != null ? locNode.internalOrder() : "N/A") +
                    ", topSize=" + ring.allNodes().size() +
                    ", leavingNodesSize=" + leavingNodesSize +
                    ", failedNodesSize=" + failedNodesSize +
                    ", joiningNodesSize=" + joiningNodesSize +
                    ", pendingCustomMsgs=" + pendingCustomMsgsSize +
                    ", msgWorker.queue.size=" + (msgWorker != null ? msgWorker.queueSize() : "N/A") +
                    ", clients=" + ring.clientNodes().size() +
                    ", clientWorkers=" + clientMsgWorkers.size() +
                    ", lastUpdate=" + (locNode != null ? U.format(locNode.lastUpdateTime()) : "N/A") +
                    ", heapFree=" + runtime.freeMemory() / (1024 * 1024) +
                    "M, heapTotal=" + runtime.maxMemory() / (1024 * 1024) + "M]");
        }
    }

    /**
     * @param msg Message to prepare.
     * @param destNodeId Destination node ID.
     * @param msgs Messages to include.
     */
    private void prepareNodeAddedMessage(
        TcpDiscoveryAbstractMessage msg,
        UUID destNodeId,
        @Nullable Collection<PendingMessage> msgs,
        @Nullable IgniteUuid discardCustomMsgId
        ) {
        assert destNodeId != null;

        if (msg instanceof TcpDiscoveryNodeAddedMessage) {
            TcpDiscoveryNodeAddedMessage nodeAddedMsg = (TcpDiscoveryNodeAddedMessage)msg;

            TcpDiscoveryNode node = nodeAddedMsg.node();

            if (node.id().equals(destNodeId)) {
                Collection<TcpDiscoveryNode> allNodes = ring.allNodes();
                Collection<TcpDiscoveryNode> topToSnd = new ArrayList<>(allNodes.size());

                for (TcpDiscoveryNode n0 : allNodes) {
                    assert n0.internalOrder() != 0 : n0;

                    // Skip next node and nodes added after next
                    // in case this message is resent due to failures/leaves.
                    // There will be separate messages for nodes with greater
                    // internal order.
                    if (n0.internalOrder() < nodeAddedMsg.node().internalOrder())
                        topToSnd.add(n0);
                }

                nodeAddedMsg.topology(topToSnd);

                Collection<TcpDiscoveryAbstractMessage> msgs0 = null;

                if (msgs != null) {
                    msgs0 = new ArrayList<>(msgs.size());

                    for (PendingMessage pendingMsg : msgs) {
                        if (pendingMsg.msg != null)
                            msgs0.add(pendingMsg.msg);
                    }
                }

                // No need to send discardMsgId because we already filtered out
                // cleaned up messages.
                // TODO IGNITE-11271
                nodeAddedMsg.messages(msgs0, null, discardCustomMsgId);

                Map<Long, Collection<ClusterNode>> hist;

                synchronized (mux) {
                    hist = new TreeMap<>(topHist);
                }

                nodeAddedMsg.topologyHistory(hist);
            }
        }
    }

    /**
     * @param msg Message to clear.
     */
    private void clearNodeAddedMessage(TcpDiscoveryAbstractMessage msg) {
        if (msg instanceof TcpDiscoveryNodeAddedMessage) {
            // Nullify topology before registration.
            TcpDiscoveryNodeAddedMessage nodeAddedMsg = (TcpDiscoveryNodeAddedMessage)msg;

            nodeAddedMsg.topology(null);
            nodeAddedMsg.topologyHistory(null);
            nodeAddedMsg.messages(null, null, null);
            nodeAddedMsg.clearUnmarshalledDiscoveryData();
        }
    }

    /** {@inheritDoc} */
    @Override public void checkRingLatency(int maxHops) {
        TcpDiscoveryRingLatencyCheckMessage msg = new TcpDiscoveryRingLatencyCheckMessage(getLocalNodeId(), maxHops);

        if (log.isInfoEnabled())
            log.info("Latency check initiated: " + msg.id());

        msgWorker.addMessage(msg);
    }

    /** {@inheritDoc} */
    @Override void simulateNodeFailure() {
        U.warn(log, "Simulating node failure: " + getLocalNodeId());

        if (tcpSrvr != null) {
            tcpSrvr.stop();

            tcpSrvr = null;
        }

        U.interrupt(ipFinderCleaner);
        U.join(ipFinderCleaner, log);

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = U.arrayList(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.cancel(msgWorker);
        U.join(msgWorker, log);

        for (ClientMessageWorker msgWorker : clientMsgWorkers.values()) {
            if (msgWorker != null) {
                U.interrupt(msgWorker.runner());
                U.join(msgWorker.runner(), log);
            }
        }

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);
    }

    /** {@inheritDoc} */
    @Override public void brakeConnection() {
        Socket sock = msgWorker.sock;

        if (sock != null)
            U.closeQuiet(sock);
    }

    /** {@inheritDoc} */
    @Override public void reconnect() throws IgniteSpiException {
        throw new UnsupportedOperationException("Reconnect is not supported for server.");
    }

    /** {@inheritDoc} */
    @Override protected Collection<IgniteSpiThread> threads() {
        Collection<IgniteSpiThread> threads;

        synchronized (mux) {
            threads = new ArrayList<>(readers.size() + clientMsgWorkers.size() + 4);
            threads.addAll(readers);
        }

        for (ClientMessageWorker wrk : clientMsgWorkers.values()) {
            Thread t = wrk.runner();

            assert t instanceof IgniteSpiThread;

            threads.add((IgniteSpiThread)t);
        }

        TcpServer tcpSrvr0 = tcpSrvr;

        if (tcpSrvr0 != null) {
            Thread tcpSrvThread = tcpSrvr0.runner();

            if (tcpSrvThread != null) {
                assert tcpSrvThread instanceof IgniteSpiThread;

                threads.add((IgniteSpiThread)tcpSrvThread);
            }
        }

        threads.add(ipFinderCleaner);

        Thread msgWorkerThread = msgWorker.runner();

        if (msgWorkerThread != null) {
            assert msgWorkerThread instanceof IgniteSpiThread;

            threads.add((IgniteSpiThread)msgWorkerThread);
        }

        threads.add(statsPrinter);

        threads.removeAll(Collections.<IgniteSpiThread>singleton(null));

        return threads;
    }

    /** @return Complete timeout of single message exchange operation on established connection. */
    protected long effectiveExchangeTimeout() {
        return spi.failureDetectionTimeoutEnabled() ? spi.failureDetectionTimeout() :
            spi.getSocketTimeout() + spi.getAckTimeout();
    }

    /** {@inheritDoc} */
    @Override public void updateMetrics(UUID nodeId,
        ClusterMetrics metrics,
        Map<Integer, CacheMetrics> cacheMetrics,
        long tsNanos)
    {
        assert nodeId != null;
        assert metrics != null;

        TcpDiscoveryNode node = ring.node(nodeId);

        if (node != null) {
            node.setMetrics(metrics);
            node.setCacheMetrics(cacheMetrics);

            node.lastUpdateTimeNanos(tsNanos);

            notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), node);
        }
        else if (log.isDebugEnabled())
            log.debug("Received metrics from unknown node: " + nodeId);
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Simulates situation when next node is still alive but is bypassed
     * since it has been excluded from the ring, possibly, due to short time
     * network problems.
     * <p>
     * This method is intended for test purposes only.
     */
    void forceNextNodeFailure() {
        U.warn(log, "Next node will be forcibly failed (if any).");

        TcpDiscoveryNode next;

        synchronized (mux) {
            next = ring.nextNode(failedNodes.keySet());
        }

        if (next != null)
            msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(getLocalNodeId(), next.id(),
                next.internalOrder()));
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * This method is intended for test purposes only.
     *
     * @return Nodes ring.
     */
    TcpDiscoveryNodesRing ring() {
        return ring;
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo(IgniteLogger log) {
        if (!debugMode) {
            U.quietAndWarn(log, "Failed to dump debug info (discovery SPI was not configured " +
                "in debug mode, consider setting 'debugMode' configuration property to 'true').");

            return;
        }

        assert log.isInfoEnabled();

        StringBuilder b = new StringBuilder(U.nl());

        synchronized (mux) {
            b.append(">>>").append(U.nl());
            b.append(">>>").append("Dumping discovery SPI debug info.").append(U.nl());
            b.append(">>>").append(U.nl());

            b.append("Local node ID: ").append(getLocalNodeId()).append(U.nl()).append(U.nl());
            b.append("Local node: ").append(locNode).append(U.nl()).append(U.nl());
            b.append("SPI state: ").append(spiState).append(U.nl()).append(U.nl());

            b.append("Internal threads: ").append(U.nl());

            b.append("    Message worker: ").append(threadStatus(msgWorker.runner())).append(U.nl());

            b.append("    IP finder cleaner: ").append(threadStatus(ipFinderCleaner)).append(U.nl());
            b.append("    Stats printer: ").append(threadStatus(statsPrinter)).append(U.nl());

            b.append(U.nl());

            b.append("Socket readers: ").append(U.nl());

            for (SocketReader rdr : readers)
                b.append("    ").append(rdr).append(U.nl());

            b.append(U.nl());

            b.append("In-memory log messages: ").append(U.nl());

            for (String msg : debugLogQ)
                b.append("    ").append(msg).append(U.nl());

            b.append(U.nl());

            b.append("Leaving nodes: ").append(U.nl());

            for (TcpDiscoveryNode node : leavingNodes)
                b.append("    ").append(node.id()).append(U.nl());

            b.append(U.nl());

            b.append("Failed nodes: ").append(U.nl());

            for (TcpDiscoveryNode node : failedNodes.keySet())
                b.append("    ").append(node.id()).append(U.nl());

            b.append(U.nl());

            b.append("Stats: ").append(spi.stats).append(U.nl());
        }

        U.quietAndInfo(log, b.toString());
    }

    /** {@inheritDoc} */
    @Override public void dumpRingStructure(IgniteLogger log) {
        U.quietAndInfo(log, ring.toString());
    }

    /** {@inheritDoc} */
    @Override public long getCurrentTopologyVersion() {
        return ring.topologyVersion();
    }

    /**
     * @param msg Message.
     * @return {@code True} if recordable in debug mode.
     */
    private boolean recordable(TcpDiscoveryAbstractMessage msg) {
        return !(msg instanceof TcpDiscoveryMetricsUpdateMessage) &&
            !(msg instanceof TcpDiscoveryStatusCheckMessage) &&
            !(msg instanceof TcpDiscoveryDiscardMessage) &&
            !(msg instanceof TcpDiscoveryConnectionCheckMessage);
    }

    /**
     * Checks if two given {@link SecurityPermissionSet} objects contain the same permissions.
     * Each permission belongs to one of three groups : cache, task or system.
     *
     * @param locPerms The first set of permissions.
     * @param rmtPerms The second set of permissions.
     * @return {@code True} if given parameters contain the same permissions, {@code False} otherwise.
     */
    private boolean permissionsEqual(@Nullable SecurityPermissionSet locPerms,
        @Nullable SecurityPermissionSet rmtPerms) {
        if (locPerms == null || rmtPerms == null)
            return false;

        boolean dfltAllowMatch = locPerms.defaultAllowAll() == rmtPerms.defaultAllowAll();

        boolean bothHaveSamePerms = F.eqNotOrdered(rmtPerms.systemPermissions(), locPerms.systemPermissions()) &&
            F.eqNotOrdered(rmtPerms.cachePermissions(), locPerms.cachePermissions()) &&
            F.eqNotOrdered(rmtPerms.taskPermissions(), locPerms.taskPermissions());

        return dfltAllowMatch && bothHaveSamePerms;
    }

    /**
     * @param msg Message.
     * @param nodeId Node ID.
     */
    private static void removeMetrics(TcpDiscoveryMetricsUpdateMessage msg, UUID nodeId) {
        msg.removeMetrics(nodeId);
        msg.removeCacheMetrics(nodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServerImpl.class, this);
    }

    /**
     * Trying get node in any state (visible or not)
     * @param nodeId Node id.
     */
    ClusterNode getNode0(UUID nodeId) {
        assert nodeId != null;

        UUID locNodeId0 = getLocalNodeId();

        if (locNodeId0 != null && locNodeId0.equals(nodeId))
            // Return local node directly.
            return locNode;

        return ring.node(nodeId);
    }

    /**
     * Thread that cleans IP finder and keeps it in the correct state, unregistering
     * addresses of the nodes that has left the topology and re-registries missing addresses.
     * <p>
     * This thread should run only on coordinator node and will clean IP finder
     * if and only if {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()} is {@code true}.
     * <p>
     * Run with frequency {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi#ipFinderCleanFreq}
     */
    private class IpFinderCleaner extends IgniteSpiThread {
        /**
         * Constructor.
         */
        private IpFinderCleaner() {
            super(spi.ignite().name(), "tcp-disco-ip-finder-cleaner", log);

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("IP finder cleaner has been started.");

            while (!isInterrupted()) {
                Thread.sleep(spi.ipFinderCleanFreq);

                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping IP finder cleaner (SPI is not connected to topology).");

                    return;
                }

                if (!isLocalNodeCoordinator())
                    continue;

                if (spi.ipFinder.isShared())
                    cleanIpFinder();
            }
        }

        /**
         * Cleans IP finder.
         */
        private void cleanIpFinder() {
            assert spi.ipFinder.isShared();

            try {
                // Addresses that belongs to nodes in topology.
                Collection<InetSocketAddress> currAddrs = F.flatCollections(
                    F.viewReadOnly(
                        ring.allNodes(),
                        new C1<TcpDiscoveryNode, Collection<InetSocketAddress>>() {
                            @Override public Collection<InetSocketAddress> apply(TcpDiscoveryNode node) {
                                return node.clientRouterNodeId() == null ? spi.getNodeAddresses(node) :
                                    Collections.emptyList();
                            }
                        }
                    )
                );

                // Addresses registered in IP finder.
                Collection<InetSocketAddress> regAddrs = spi.registeredAddresses();

                P1<InetSocketAddress> p = new P1<InetSocketAddress>() {
                    private final Map<InetSocketAddress, Boolean> pingResMap = new HashMap<>();

                    @Override public boolean apply(InetSocketAddress addr) {
                        Boolean res = pingResMap.get(addr);

                        if (res == null) {
                            try {
                                res = pingNode(addr, null, null) != null;
                            }
                            catch (IgniteCheckedException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to ping node [addr=" + addr + ", err=" + e.getMessage() + ']');

                                res = false;
                            }
                            finally {
                                pingResMap.put(addr, res);
                            }
                        }

                        return !res;
                    }
                };

                ArrayList<InetSocketAddress> rmvAddrs = null;

                for (InetSocketAddress addr : regAddrs) {
                    boolean rmv = !F.contains(currAddrs, addr) && p.apply(addr);

                    if (rmv) {
                        if (rmvAddrs == null)
                            rmvAddrs = new ArrayList<>();

                        rmvAddrs.add(addr);
                    }
                }

                // Unregister dead-nodes addresses.
                if (rmvAddrs != null) {
                    spi.ipFinder.unregisterAddresses(rmvAddrs);

                    if (log.isDebugEnabled())
                        log.debug("Unregistered addresses from IP finder: " + rmvAddrs);
                }

                // Addresses that were removed by mistake (e.g. on segmentation).
                Collection<InetSocketAddress> missingAddrs = F.view(
                    currAddrs,
                    F.notContains(regAddrs)
                );

                // Re-register missing addresses.
                if (!missingAddrs.isEmpty()) {
                    spi.ipFinder.registerAddresses(missingAddrs);

                    if (log.isDebugEnabled())
                        log.debug("Registered missing addresses in IP finder: " + missingAddrs);
                }
            }
            catch (IgniteSpiException e) {
                LT.error(log, e, "Failed to clean IP finder up.");
            }
        }
    }

    /**
     * Sweeps failedNodes collection in msg and fills it with failed nodes observed by local node.
     *
     * @param msg {@link TcpDiscoveryAbstractMessage} to sweep failed nodes from.
     */
    private void sweepMessageFailedNodes(TcpDiscoveryAbstractMessage msg) {
        msg.failedNodes(null);

        synchronized (mux) {
            for (TcpDiscoveryNode n : failedNodes.keySet())
                msg.addFailedNode(n.id());
        }

        if (log.isDebugEnabled())
            log.debug("Message failed nodes were replaced with failed nodes observed by local node: " + msg.failedNodes());
    }

    /**
     * Adds failed nodes specified in the received message to the local failed nodes list.
     *
     * @param msg Message.
     */
    private void processMessageFailedNodes(TcpDiscoveryAbstractMessage msg) {
        Collection<UUID> msgFailedNodes = msg.failedNodes();

        if (msgFailedNodes != null) {
            UUID sndId = msg.senderNodeId();

            if (sndId != null) {
                if (ring.node(sndId) == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Ignore message failed nodes, sender node is not alive [nodeId=" + sndId +
                            ", failedNodes=" + msgFailedNodes + ']');
                    }

                    sweepMessageFailedNodes(msg);

                    return;
                }

                synchronized (mux) {
                    for (TcpDiscoveryNode failedNode : failedNodes.keySet()) {
                        if (failedNode.id().equals(sndId)) {
                            if (log.isDebugEnabled()) {
                                log.debug("Ignore message failed nodes, sender node is in fail list [nodeId=" + sndId +
                                    ", failedNodes=" + msgFailedNodes + ']');
                            }

                            sweepMessageFailedNodes(msg);

                            return;
                        }
                    }
                }
            }

            for (UUID nodeId : msgFailedNodes) {
                TcpDiscoveryNode failedNode = ring.node(nodeId);

                if (failedNode != null) {
                    if (!failedNode.isLocal()) {
                        boolean added = false;

                        synchronized (mux) {
                            if (!failedNodes.containsKey(failedNode)) {
                                failedNodes.put(failedNode, msg.senderNodeId() != null ? msg.senderNodeId() : getLocalNodeId());

                                added = true;
                            }
                        }

                        if (added && log.isDebugEnabled())
                            log.debug("Added node to failed nodes list [node=" + failedNode + ", msg=" + msg + ']');
                    }
                }
            }
        }
    }

    /** */
    private static WorkersRegistry getWorkerRegistry(TcpDiscoverySpi spi) {
        return spi.ignite() instanceof IgniteEx ? ((IgniteEx)spi.ignite()).context().workersRegistry() : null;
    }

    /**
     * Discovery messages history used for client reconnect.
     */
    private class EnsuredMessageHistory {
        /** Pending messages. */
        private final GridBoundedLinkedHashSet<TcpDiscoveryAbstractMessage>
            msgs = new GridBoundedLinkedHashSet<>(ENSURED_MSG_HIST_SIZE);

        /**
         * @param msg Adds message.
         */
        void add(TcpDiscoveryAbstractMessage msg) {
            assert spi.ensured(msg) && msg.verified() : msg;

            assert U.hasDeclaredAnnotation(msg, TcpDiscoveryRedirectToClient.class) : msg;

            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                TcpDiscoveryNodeAddedMessage addedMsg =
                    new TcpDiscoveryNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);

                msg = addedMsg;

                TcpDiscoveryNode node = addedMsg.node();

                if (node.clientRouterNodeId() != null && !msgs.contains(msg)) {
                    Collection<TcpDiscoveryNode> allNodes = ring.allNodes();

                    Collection<TcpDiscoveryNode> top = new ArrayList<>(allNodes.size());

                    for (TcpDiscoveryNode n0 : allNodes) {
                        assert n0.internalOrder() > 0 : n0;

                        if (n0.internalOrder() < node.internalOrder())
                            top.add(n0);
                    }

                    addedMsg.clientTopology(top);
                }

                // Do not need this data for client reconnect.
                if (addedMsg.gridDiscoveryData() != null)
                    addedMsg.clearDiscoveryData();
            }
            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                TcpDiscoveryNodeAddFinishedMessage addFinishMsg = (TcpDiscoveryNodeAddFinishedMessage)msg;

                if (addFinishMsg.clientDiscoData() != null) {
                    addFinishMsg = new TcpDiscoveryNodeAddFinishedMessage(addFinishMsg);

                    msg = addFinishMsg;

                    DiscoveryDataPacket discoData = addFinishMsg.clientDiscoData();

                    Set<Integer> mrgdCmnData = new HashSet<>();
                    Set<UUID> mrgdSpecData = new HashSet<>();

                    boolean allMerged = false;

                    for (TcpDiscoveryAbstractMessage msg0 : msgs) {

                        if (msg0 instanceof TcpDiscoveryNodeAddFinishedMessage) {
                            DiscoveryDataPacket existingDiscoData =
                                ((TcpDiscoveryNodeAddFinishedMessage)msg0).clientDiscoData();

                            if (existingDiscoData != null)
                                allMerged = discoData.mergeDataFrom(existingDiscoData, mrgdCmnData, mrgdSpecData);
                        }

                        if (allMerged)
                            break;
                    }
                }
            }
            else if (msg instanceof TcpDiscoveryNodeLeftMessage)
                clearClientAddFinished(msg.creatorNodeId());
            else if (msg instanceof TcpDiscoveryNodeFailedMessage)
                clearClientAddFinished(((TcpDiscoveryNodeFailedMessage)msg).failedNodeId());
            else if (msg instanceof TcpDiscoveryCustomEventMessage) {
                TcpDiscoveryCustomEventMessage msg0 = (TcpDiscoveryCustomEventMessage)msg;

                msg = new TcpDiscoveryCustomEventMessage(msg0);

                // We shoulgn't store deserialized message in the queue because of msg is transient.
                ((TcpDiscoveryCustomEventMessage)msg).clearMessage();
            }

            synchronized (msgs) {
                msgs.add(msg);
            }
        }

        /**
         * @param clientId Client node ID.
         */
        private void clearClientAddFinished(UUID clientId) {
            for (TcpDiscoveryAbstractMessage msg : msgs) {
                if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                    TcpDiscoveryNodeAddFinishedMessage addFinishMsg = (TcpDiscoveryNodeAddFinishedMessage)msg;

                    if (addFinishMsg.clientDiscoData() != null && clientId.equals(addFinishMsg.nodeId())) {
                        addFinishMsg.clientDiscoData(null);
                        addFinishMsg.clientNodeAttributes(null);

                        break;
                    }
                }
            }
        }

        /**
         * Gets messages starting from provided ID (exclusive). If such
         * message is not found, {@code null} is returned (this indicates
         * a failure condition when it was already removed from queue).
         *
         * @param lastMsgId Last message ID received on client. {@code Null} if client did not finish connect procedure.
         * @param node Client node.
         * @return Collection of messages.
         */
        @Nullable Collection<TcpDiscoveryAbstractMessage> messages(@Nullable IgniteUuid lastMsgId,
            TcpDiscoveryNode node)
        {
            assert node != null && node.clientRouterNodeId() != null : node;

            if (lastMsgId == null) {
                // Client connection failed before it received TcpDiscoveryNodeAddedMessage.
                List<TcpDiscoveryAbstractMessage> res = null;

                synchronized (msgs) {
                    for (TcpDiscoveryAbstractMessage msg : msgs) {
                        if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                            if (node.id().equals(((TcpDiscoveryNodeAddedMessage)msg).node().id()))
                                res = new ArrayList<>(msgs.size());
                        }

                        if (res != null)
                            res.add(prepare(msg, node.id()));
                    }
                }

                if (log.isDebugEnabled()) {
                    if (res == null)
                        log.debug("Failed to find node added message [node=" + node + ']');
                    else
                        log.debug("Found add added message [node=" + node + ", hist=" + res + ']');
                }

                return res;
            }
            else {
                Collection<TcpDiscoveryAbstractMessage> cp;

                boolean skip;

                synchronized (msgs) {
                    if (msgs.isEmpty())
                        return Collections.emptyList();

                    cp = new ArrayList<>(msgs.size());

                    skip = true;

                    for (TcpDiscoveryAbstractMessage msg : msgs) {
                        if (skip) {
                            if (msg.id().equals(lastMsgId))
                                skip = false;
                        }
                        else
                            cp.add(prepare(msg, node.id()));
                    }
                }

                cp = !skip ? cp : null;

                if (log.isDebugEnabled()) {
                    if (cp == null)
                        log.debug("Failed to find messages history [node=" + node + ", lastMsgId=" + lastMsgId + ']');
                    else
                        log.debug("Found messages history [node=" + node + ", hist=" + cp + ']');
                }

                return cp;
            }
        }

        /**
         * @param msg Message.
         * @param destNodeId Client node ID.
         * @return Prepared message.
         */
        private TcpDiscoveryAbstractMessage prepare(TcpDiscoveryAbstractMessage msg, UUID destNodeId) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                TcpDiscoveryNodeAddedMessage addedMsg = (TcpDiscoveryNodeAddedMessage)msg;

                if (addedMsg.node().id().equals(destNodeId)) {
                    assert addedMsg.clientTopology() != null : addedMsg;

                    TcpDiscoveryNodeAddedMessage msg0 = new TcpDiscoveryNodeAddedMessage(addedMsg);

                    prepareNodeAddedMessage(msg0, destNodeId, null, null);

                    msg0.topology(addedMsg.clientTopology());

                    return msg0;
                }
            }

            return msg;
        }
    }

    /**
     *
     */
    private static class PendingMessage {
        /** */
        TcpDiscoveryAbstractMessage msg;

        /** */
        final boolean customMsg;

        /** */
        final IgniteUuid id;

        /** */
        final boolean verified;

        /**
         * @param msg Message.
         */
        PendingMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null && msg.id() != null : msg;

            this.msg = msg;

            id = msg.id();
            customMsg = msg instanceof TcpDiscoveryCustomEventMessage;
            verified = msg.verified();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PendingMessage.class, this, "msg", msg, "customMsg", customMsg, "id", id);
        }
    }

    /**
     * Pending messages container.
     */
    private static class PendingMessages implements Iterable<TcpDiscoveryAbstractMessage> {
        /** */
        private static final int MAX = 1024;

        /** Pending messages. */
        private final Queue<PendingMessage> msgs = new ArrayDeque<>(MAX * 2);

        /** Processed custom message IDs. */
        private Set<IgniteUuid> procCustomMsgs = new GridBoundedLinkedHashSet<>(MAX * 2);

        /** Discarded message ID. */
        private IgniteUuid discardId;

        /** Discarded custom message ID. */
        private IgniteUuid customDiscardId;

        /**
         * Adds pending message and shrinks queue if it exceeds limit
         * (messages that were not discarded yet are never removed).
         *
         * @param msg Message to add.
         */
        void add(TcpDiscoveryAbstractMessage msg) {
            msgs.add(new PendingMessage(msg));

            while (msgs.size() > MAX) {
                PendingMessage queueHead = msgs.peek();

                assert queueHead != null;

                if (queueHead.customMsg && customDiscardId != null) {
                    if (queueHead.id.equals(customDiscardId))
                        customDiscardId = null;
                }
                else if (!queueHead.customMsg && discardId != null) {
                    if (queueHead.id.equals(discardId))
                        discardId = null;
                }
                else
                    break;

                msgs.poll();
            }
        }

        /**
         * Resets pending messages.
         *
         * @param msgs Message.
         * @param discardId Discarded message ID.
         * @param customDiscardId Discarded custom event message ID.
         */
        void reset(
            @Nullable Collection<TcpDiscoveryAbstractMessage> msgs,
            @Nullable IgniteUuid discardId,
            @Nullable IgniteUuid customDiscardId
        ) {
            this.msgs.clear();
            this.customDiscardId = null;
            this.discardId = null;

            if (msgs != null) {
                for (TcpDiscoveryAbstractMessage msg : msgs) {
                    PendingMessage pm = new PendingMessage(msg);

                    this.msgs.add(pm);
                }
            }

            this.discardId = discardId;
            this.customDiscardId = customDiscardId;
        }

        /**
         * Discards message with provided ID and all before it.
         *
         * @param id Discarded message ID.
         * @param custom {@code True} if discard for {@link TcpDiscoveryCustomEventMessage}.
         */
        void discard(IgniteUuid id, boolean custom) {
            if (!hasPendingMessage(custom, id))
                return;

            if (custom)
                customDiscardId = id;
            else
                discardId = id;

            cleanup();
        }

        /**
         * @param custom {@code true} if need to check for a custom message with the given ID.
         * @param id Message ID.
         * @return {@code true} if pending messages contain such a message.
         */
        private boolean hasPendingMessage(boolean custom, IgniteUuid id) {
            for (PendingMessage msg : msgs) {
                if (msg.customMsg == custom && msg.id.equals(id))
                    return true;
            }

            return false;
        }

        /**
         *
         */
        void cleanup() {
            Iterator<PendingMessage> msgIt = msgs.iterator();

            boolean skipMsg = discardId != null;
            boolean skipCustomMsg = customDiscardId != null;

            while (msgIt.hasNext()) {
                PendingMessage msg = msgIt.next();

                if (msg.customMsg) {
                    if (skipCustomMsg) {
                        assert customDiscardId != null;

                        if (F.eq(customDiscardId, msg.id)) {
                            msg.msg = null;

                            if (msg.verified)
                                return;
                        }
                    }
                }
                else {
                    if (skipMsg) {
                        assert discardId != null;

                        if (F.eq(discardId, msg.id)) {
                            msg.msg = null;

                            if (msg.verified)
                                return;
                        }
                    }
                }
            }
        }

        /**
         * Gets iterator for non-discarded messages.
         *
         * @return Non-discarded messages iterator.
         */
        @Override public Iterator<TcpDiscoveryAbstractMessage> iterator() {
            return new SkipIterator();
        }

        /**
         *
         */
        private class SkipIterator implements Iterator<TcpDiscoveryAbstractMessage> {
            /** Skip non-custom messages flag. */
            private boolean skipMsg = discardId != null;

            /** Skip custom messages flag. */
            private boolean skipCustomMsg = customDiscardId != null;

            /** Internal iterator. */
            private Iterator<PendingMessage> msgIt = msgs.iterator();

            /** Next message. */
            private TcpDiscoveryAbstractMessage next;

            {
                advance();
            }

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return next != null;
            }

            /** {@inheritDoc} */
            @Override public TcpDiscoveryAbstractMessage next() {
                if (next == null)
                    throw new NoSuchElementException();

                TcpDiscoveryAbstractMessage next0 = next;

                advance();

                return next0;
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException();
            }

            /**
             * Advances iterator to the next available item.
             */
            private void advance() {
                next = null;

                while (msgIt.hasNext()) {
                    PendingMessage msg0 = msgIt.next();

                    if (msg0.customMsg) {
                        if (skipCustomMsg) {
                            assert customDiscardId != null;

                            if (F.eq(customDiscardId, msg0.id) && msg0.verified)
                                skipCustomMsg = false;

                            continue;
                        }
                    }
                    else {
                        if (skipMsg) {
                            assert discardId != null;

                            if (F.eq(discardId, msg0.id) && msg0.verified)
                                skipMsg = false;

                            continue;
                        }
                    }

                    if (msg0.msg == null)
                        continue;

                    next = msg0.msg;

                    break;
                }
            }
        }
    }

    /**
     * Message worker for discovery messages processing.
     */
    private class RingMessageWorker extends MessageWorker<TcpDiscoveryAbstractMessage> {
        /** Next node. */
        private TcpDiscoveryNode next;

        /** Pending messages. */
        private final PendingMessages pendingMsgs = new PendingMessages();

        /** */
        private final ConcurrentLinkedQueue<FutureTask<Void>> tasks = new ConcurrentLinkedQueue<>();

        /** Last message that updated topology. */
        private TcpDiscoveryAbstractMessage lastMsg;

        /** Force pending messages send. */
        private boolean forceSndPending;

        /** Socket. */
        private Socket sock;

        /** Output stream. */
        private OutputStream out;

        /** Last time status message has been sent. */
        private long lastTimeStatusMsgSentNanos;

        /** Incoming metrics check frequency. */
        private long metricsCheckFreq = 3 * spi.metricsUpdateFreq + 50;

        /** Last time metrics update message has been sent. */
        private long lastTimeMetricsUpdateMsgSentNanos = System.nanoTime() - U.millisToNanos(spi.metricsUpdateFreq);

        /** */
        private long lastRingMsgTimeNanos;

        /** */
        private List<DiscoveryDataPacket> joiningNodesDiscoDataList;

        /** */
        private DiscoveryDataPacket gridDiscoveryData;

        /** Filter for {@link TcpDiscoveryMetricsUpdateMessage}s. */
        private final MetricsUpdateMessageFilter metricsMsgFilter = new MetricsUpdateMessageFilter();

        /** Thread local variable indicates that discovery manager was notified after message processing. */
        private final ThreadLocal<Boolean> notifiedDiscovery = ThreadLocal.withInitial(() -> false);

        /**
         * @param log Logger.
         */
        private RingMessageWorker(IgniteLogger log) {
            super("tcp-disco-msg-worker-[]", log, 10, getWorkerRegistry(spi));

            setBeforeEachPollAction(() -> {
                updateHeartbeat();

                onIdle();

                runTasks();
            });
        }

        /**
         * Adds a task to be executed in the message worker thread.
         *
         * @param task Task to run in the message worker thread.
         */
        FutureTask<Void> addTask(FutureTask<Void> task) {
            tasks.add(task);

            addMessage(WAKEUP);

            return task;
        }

        /**
         * Adds message to queue. Equivalent to {@code addMessage(msg, false)}.
         *
         * @param msg Message to add.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg) {
            addMessage(msg, false, false);
        }

        /**
         * Adds message to queue.
         *
         * @param msg Message to add.
         * @param ignoreHighPriority high priority messages will be added to the top of the queue.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg, boolean ignoreHighPriority) {
            addMessage(msg, ignoreHighPriority, false);
        }

        /**
         * Adds message to queue.
         *
         * @param msg Message to add.
         * @param ignoreHighPriority If {@code true}, high priority messages will be added to the top of the queue.
         * @param fromSocket If message has been read from socket (not created externally).
         */
        void addMessage(TcpDiscoveryAbstractMessage msg, boolean ignoreHighPriority, boolean fromSocket) {
            DebugLogger log = messageLogger(msg);

            if ((msg instanceof TcpDiscoveryStatusCheckMessage ||
                msg instanceof TcpDiscoveryJoinRequestMessage ||
                msg instanceof TcpDiscoveryCustomEventMessage ||
                msg instanceof TcpDiscoveryClientReconnectMessage) &&
                queue.contains(msg)) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring duplicate message: " + msg);

                return;
            }

            if (msg instanceof TraceableMessage) {
                TraceableMessage tMsg = (TraceableMessage) msg;

                // If we read this message from socket.
                if (fromSocket)
                    tracing.messages().afterReceive(tMsg);
                else { // If we're going to send this message.
                    if (!msg.verified() && tMsg.spanContainer().serializedSpanBytes() == null) {
                        Span rootSpan = tracing.create(TraceableMessagesTable.traceName(tMsg.getClass()))
                            .end();

                        // This root span will be parent both from local and remote nodes.
                        tMsg.spanContainer().serializedSpanBytes(tracing.serialize(rootSpan));
                    }
                }
            }

            boolean addFirst = msg.highPriority() && !ignoreHighPriority;

            if (msg instanceof TcpDiscoveryMetricsUpdateMessage) {
                if (metricsMsgFilter.addMessage((TcpDiscoveryMetricsUpdateMessage)msg))
                    addToQueue(msg, addFirst);
                else {
                    if (log.isDebugEnabled())
                        log.debug("Metric update message has been replaced in the worker's queue: " + msg);
                }
            }
            else
                addToQueue(msg, addFirst);
        }

        /**
         * @param msg Message to add.
         * @param addFirst If {@code true}, then the message will be added to a head of a worker's queue.
         */
        private void addToQueue(TcpDiscoveryAbstractMessage msg, boolean addFirst) {
            DebugLogger log = messageLogger(msg);

            if (addFirst) {
                queue.addFirst(msg);

                if (log.isDebugEnabled())
                    log.debug("Message has been added to a head of a worker's queue: " + msg);
            }
            else {
                queue.add(msg);

                if (log.isDebugEnabled())
                    log.debug("Message has been added to a worker's queue: " + msg);
            }
        }

        /** */
        @Override protected void body() throws InterruptedException {
            Throwable err = null;

            try {
                super.body();
            }
            catch (InterruptedException e) {
                if (!spi.isNodeStopping0() && spiStateCopy() != DISCONNECTING)
                    err = e;

                throw e;
            }
            catch (Throwable e) {
                if (!spi.isNodeStopping0() && spiStateCopy() != DISCONNECTING) {
                    final Ignite ignite = spi.ignite();

                    if (ignite != null) {
                        U.error(log, "TcpDiscoverSpi's message worker thread failed abnormally. " +
                            "Stopping the node in order to prevent cluster wide instability.", e);

                        new Thread(new Runnable() {
                            @Override public void run() {
                                try {
                                    IgnitionEx.stop(ignite.name(), true, ShutdownPolicy.IMMEDIATE, true);

                                    U.log(log, "Stopped the node successfully in response to TcpDiscoverySpi's " +
                                        "message worker thread abnormal termination.");
                                }
                                catch (Throwable e) {
                                    U.error(log, "Failed to stop the node in response to TcpDiscoverySpi's " +
                                        "message worker thread abnormal termination.", e);
                                }
                            }
                        }, "node-stop-thread").start();
                    }
                }

                err = e;

                // Must be processed by IgniteSpiThread as well.
                throw e;
            }
            finally {
                if (spi.ignite() instanceof IgniteEx) {
                    if (err == null && !spi.isNodeStopping0() && spiStateCopy() != DISCONNECTING)
                        err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly.");

                    FailureProcessor failure = ((IgniteEx)spi.ignite()).context().failure();

                    if (err instanceof OutOfMemoryError)
                        failure.process(new FailureContext(CRITICAL_ERROR, err));
                    else if (err != null)
                        failure.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
                }
            }
        }

        /** */
        private void nullifyDiscoData() {
            gridDiscoveryData = null;
            joiningNodesDiscoDataList = null;
        }

        /**
         *
         */
        protected void runTasks() {
            FutureTask<Void> task;

            while ((task = tasks.poll()) != null)
                task.run();
        }

        /** {@inheritDoc} */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            if (msg == WAKEUP)
                return;

            notifiedDiscovery.set(false);

            if (msg instanceof TraceableMessage) {
                TraceableMessage tMsg = (TraceableMessage) msg;

                tracing.messages().afterReceive(tMsg);
            }

            spi.startMessageProcess(msg);

            sendMetricsUpdateMessage();

            synchronized (mux) {
                if (spiState == RING_FAILED) {
                    if (log.isDebugEnabled())
                        log.debug("Discovery detected ring connectivity issues and will stop local node, " +
                            "ignoring message [msg=" + msg + ", locNode=" + locNode + ']');

                    if (msg instanceof TraceableMessage)
                        ((TraceableMessage) msg).spanContainer().span()
                            .addLog(() -> "Ring failed")
                            .setStatus(SpanStatus.ABORTED)
                            .end();

                    return;
                }
            }

            DebugLogger log = messageLogger(msg);

            if (log.isDebugEnabled())
                log.debug("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            if (debugMode)
                debugLog(msg, "Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            boolean ensured = spi.ensured(msg);

            if (!locNode.id().equals(msg.senderNodeId()) && ensured)
                lastRingMsgTimeNanos = System.nanoTime();

            if (locNode.internalOrder() == 0) {
                boolean proc = false;

                if (msg instanceof TcpDiscoveryNodeAddedMessage)
                    proc = ((TcpDiscoveryNodeAddedMessage)msg).node().equals(locNode);

                if (!proc) {
                    if (log.isDebugEnabled()) {
                        log.debug("Ignore message, local node order is not initialized [msg=" + msg +
                            ", locNode=" + locNode + ']');
                    }

                    if (msg instanceof TraceableMessage)
                        ((TraceableMessage) msg).spanContainer().span()
                            .addLog(() -> "Local node order not initialized")
                            .setStatus(SpanStatus.ABORTED)
                            .end();

                    return;
                }
            }

            spi.stats.onMessageProcessingStarted(msg);

            processMessageFailedNodes(msg);

            if (msg instanceof TcpDiscoveryJoinRequestMessage)
                processJoinRequestMessage((TcpDiscoveryJoinRequestMessage)msg);

            else if (msg instanceof TcpDiscoveryClientReconnectMessage) {
                if (sendMessageToRemotes(msg))
                    sendMessageAcrossRing(msg);
            }

            else if (msg instanceof TcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((TcpDiscoveryNodeAddFinishedMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((TcpDiscoveryNodeLeftMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((TcpDiscoveryNodeFailedMessage)msg);

            else if (msg instanceof TcpDiscoveryMetricsUpdateMessage)
                processMetricsUpdateMessage((TcpDiscoveryMetricsUpdateMessage)msg);

            else if (msg instanceof TcpDiscoveryStatusCheckMessage)
                processStatusCheckMessage((TcpDiscoveryStatusCheckMessage)msg);

            else if (msg instanceof TcpDiscoveryDiscardMessage)
                processDiscardMessage((TcpDiscoveryDiscardMessage)msg);

            else if (msg instanceof TcpDiscoveryCustomEventMessage)
                processCustomMessage((TcpDiscoveryCustomEventMessage)msg, false);

            else if (msg instanceof TcpDiscoveryClientPingRequest)
                processClientPingRequest((TcpDiscoveryClientPingRequest)msg);

            else if (msg instanceof TcpDiscoveryRingLatencyCheckMessage)
                processRingLatencyCheckMessage((TcpDiscoveryRingLatencyCheckMessage)msg);

            else if (msg instanceof TcpDiscoveryAuthFailedMessage)
                processAuthFailedMessage((TcpDiscoveryAuthFailedMessage)msg);
            else
                assert false : "Unknown message type: " + msg.getClass().getSimpleName();

            if (msg.senderNodeId() != null && !msg.senderNodeId().equals(getLocalNodeId())) {
                // Received a message from remote node.
                onMessageExchanged();
            }

            if (next != null && sock != null) {
                // Messages that change topology.
                if (msg instanceof TcpDiscoveryNodeLeftMessage || msg instanceof TcpDiscoveryNodeFailedMessage ||
                    msg instanceof TcpDiscoveryNodeAddFinishedMessage || msg instanceof TcpDiscoveryNodeAddedMessage) {
                    U.enhanceThreadName(U.id8(next.id()) + ' ' + sock.getInetAddress().getHostAddress()
                        + ":" + sock.getPort() + (isLocalNodeCoordinator() ? " crd" : ""));
                }
            }

            spi.stats.onMessageProcessingFinished(msg);

            if (msg instanceof TraceableMessage &&
                (msg instanceof TcpDiscoveryNodeAddedMessage
                    || msg instanceof TcpDiscoveryJoinRequestMessage
                    || notifiedDiscovery.get())) {
                TraceableMessage tMsg = (TraceableMessage) msg;

                tracing.messages().finishProcessing(tMsg);
            }
        }

        /**
         * Processes authentication failed message.
         *
         * @param authFailedMsg Authentication failed message.
         */
        private void processAuthFailedMessage(TcpDiscoveryAuthFailedMessage authFailedMsg) {
            try {
                sendDirectlyToClient(authFailedMsg.getTargetNodeId(), authFailedMsg);
            }
            catch (IgniteSpiException ex) {
                log.warning(
                    "Skipping send auth failed message to client due to some trouble with connection detected: "
                    + ex.getMessage()
                );
            }
        }

        /** {@inheritDoc} */
        @Override protected void noMessageLoop() {
            if (locNode == null)
                return;

            checkConnection();

            sendMetricsUpdateMessage();

            checkMetricsReceiving();

            checkPendingCustomMessages();

            checkFailedNodesList();
        }

        /**
         * @param msg Message.
         */
        private void sendMessageToClients(TcpDiscoveryAbstractMessage msg) {
            if (redirectToClients(msg)) {
                if (spi.ensured(msg))
                    msgHist.add(msg);

                byte[] msgBytes = null;

                for (ClientMessageWorker clientMsgWorker : clientMsgWorkers.values()) {
                    if (msgBytes == null) {
                        try {
                            msgBytes = U.marshal(spi.marshaller(), msg);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to marshal message: " + msg, e);

                            break;
                        }
                    }

                    TcpDiscoveryAbstractMessage msg0 = msg;
                    byte[] msgBytes0 = msgBytes;

                    if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                        TcpDiscoveryNodeAddedMessage nodeAddedMsg = (TcpDiscoveryNodeAddedMessage)msg;

                        TcpDiscoveryNode node = nodeAddedMsg.node();

                        if (clientMsgWorker.clientNodeId.equals(node.id())) {
                            try {
                                msg0 = U.unmarshal(spi.marshaller(), msgBytes,
                                    U.resolveClassLoader(spi.ignite().configuration()));

                                prepareNodeAddedMessage(msg0, clientMsgWorker.clientNodeId, null, null);

                                msgBytes0 = null;
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to create message copy: " + msg, e);
                            }
                        }
                    }

                    clientMsgWorker.addMessage(msg0, msgBytes0);
                }
            }
        }

        /**
         * Sends message across the ring.
         *
         * @param msg Message to send
         */
        @SuppressWarnings({"BreakStatementWithLabel", "LabeledStatement", "ContinueStatementWithLabel"})
        private void sendMessageAcrossRing(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            assert ring.hasRemoteNodes();

            for (IgniteInClosure<TcpDiscoveryAbstractMessage> msgLsnr : spi.sndMsgLsnrs)
                msgLsnr.apply(msg);

            if (msg instanceof TraceableMessage)
                tracing.messages().beforeSend((TraceableMessage) msg);

            sendMessageToClients(msg);

            List<TcpDiscoveryNode> failedNodes;

            TcpDiscoverySpiState state;

            synchronized (mux) {
                failedNodes = U.arrayList(ServerImpl.this.failedNodes.keySet());

                state = spiState;
            }

            Collection<Throwable> errs = null;

            boolean sent = false;

            boolean newNextNode = false;

            // Used only if spi.getEffectiveConnectionRecoveryTimeout > 0
            CrossRingMessageSendState sndState = null;

            UUID locNodeId = getLocalNodeId();

            ringLoop: while (true) {
                TcpDiscoveryNode newNext = ring.nextNode(failedNodes);

                if (newNext == null) {
                    if (log.isDebugEnabled())
                        log.debug("No next node in topology.");

                    if (debugMode)
                        debugLog(msg, "No next node in topology.");

                    if (ring.hasRemoteNodes() && !(msg instanceof TcpDiscoveryConnectionCheckMessage) &&
                        !(msg instanceof TcpDiscoveryStatusCheckMessage && msg.creatorNodeId().equals(locNodeId))) {
                        msg.senderNodeId(locNodeId);

                        addMessage(msg, true);
                    }

                    break;
                }

                if (!newNext.equals(next)) {
                    if (log.isDebugEnabled())
                        log.debug("New next node [newNext=" + newNext + ", formerNext=" + next +
                            ", ring=" + ring + ", failedNodes=" + failedNodes + ']');
                    else if (log.isInfoEnabled())
                        log.info("New next node [newNext=" + newNext + ']');

                    if (debugMode)
                        debugLog(msg, "New next node [newNext=" + newNext + ", formerNext=" + next +
                            ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

                    U.closeQuiet(sock);

                    sock = null;

                    next = newNext;

                    newNextNode = true;
                }
                else if (log.isTraceEnabled())
                    log.trace("Next node remains the same [nextId=" + next.id() +
                        ", nextOrder=" + next.internalOrder() + ']');

                final boolean sameHost = U.sameMacs(locNode, next);

                List<InetSocketAddress> locNodeAddrs = U.arrayList(locNode.socketAddresses());

                addr: for (InetSocketAddress addr : spi.getNodeAddresses(next, sameHost)) {
                    long ackTimeout0 = spi.getAckTimeout();

                    if (locNodeAddrs.contains(addr)) {
                        if (log.isDebugEnabled())
                            log.debug("Skip to send message to the local node (probably remote node has the same " +
                                "loopback address that local node): " + addr);

                        continue;
                    }

                    int reconCnt = 0;

                    IgniteSpiOperationTimeoutHelper timeoutHelper = null;

                    while (true) {
                        if (sock == null) {
                            // We re-create the helper here because it could be created earlier with wrong timeout on
                            // message sending like IgniteConfiguration.failureDetectionTimeout. Here we are in the
                            // state of conenction recovering and have to work with
                            // TcpDiscoverSpi.getEffectiveConnectionRecoveryTimeout()
                            if (timeoutHelper == null || sndState != null)
                                timeoutHelper = serverOperationTimeoutHelper(sndState, -1);

                            boolean success = false;

                            boolean openSock = false;

                            // Restore ring.
                            try {
                                sock = spi.openSocket(addr, timeoutHelper);

                                out = spi.socketStream(sock);

                                openSock = true;

                                // Handshake.
                                TcpDiscoveryHandshakeRequest hndMsg = new TcpDiscoveryHandshakeRequest(locNodeId);

                                // Topology treated as changes if next node is not available.
                                boolean changeTop = sndState != null && !sndState.isStartingPoint();

                                if (changeTop)
                                    hndMsg.changeTopology(ring.previousNodeOf(next).id());

                                if (log.isDebugEnabled()) {
                                    log.debug("Sending handshake [hndMsg=" + hndMsg + ", sndState=" + sndState +
                                        "] with timeout " + timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));
                                }

                                spi.writeToSocket(sock, out, hndMsg,
                                    timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                                if (log.isDebugEnabled()) {
                                    log.debug("Reading handshake response with timeout " +
                                        timeoutHelper.nextTimeoutChunk(ackTimeout0));
                                }

                                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null,
                                    timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                if (log.isDebugEnabled())
                                    log.debug("Handshake response: " + res);

                                // We should take previousNodeAlive flag into account
                                // only if we received the response from the correct node.
                                if (res.creatorNodeId().equals(next.id()) && res.previousNodeAlive() && sndState != null) {
                                    sndState.checkTimeout();

                                    // Remote node checked connection to it's previous and got success.
                                    boolean previousNode = sndState.markLastFailedNodeAlive();

                                    if (previousNode)
                                        failedNodes.remove(failedNodes.size() - 1);
                                    else {
                                        newNextNode = false;

                                        next = ring.nextNode(failedNodes);
                                    }

                                    U.closeQuiet(sock);

                                    sock = null;

                                    if (sndState.isFailed()) {
                                        segmentLocalNodeOnSendFail(failedNodes);

                                        return; // Nothing to do here.
                                    }

                                    if (previousNode)
                                        U.warn(log, "New next node has connection to it's previous, trying previous " +
                                            "again. [next=" + next + ']');

                                    continue ringLoop;
                                }

                                if (locNodeId.equals(res.creatorNodeId())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Handshake response from local node: " + res);

                                    U.closeQuiet(sock);

                                    sock = null;

                                    break;
                                }

                                UUID nextId = res.creatorNodeId();

                                long nextOrder = res.order();

                                if (!next.id().equals(nextId)) {
                                    // Node with different ID has bounded to the same port.
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to restore ring because next node ID received is not as " +
                                            "expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

                                    if (debugMode)
                                        debugLog(msg, "Failed to restore ring because next node ID received is not " +
                                            "as expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

                                    break;
                                }
                                else {
                                    // ID is as expected. Check node order.
                                    if (nextOrder != next.internalOrder()) {
                                        // Is next currently being added?
                                        boolean nextNew = (msg instanceof TcpDiscoveryNodeAddedMessage &&
                                            ((TcpDiscoveryNodeAddedMessage)msg).node().id().equals(nextId));

                                        if (!nextNew)
                                            nextNew = hasPendingAddMessage(nextId);

                                        if (!nextNew) {
                                            if (log.isDebugEnabled())
                                                log.debug("Failed to restore ring because next node order received " +
                                                    "is not as expected [expected=" + next.internalOrder() +
                                                    ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                            if (debugMode)
                                                debugLog(msg, "Failed to restore ring because next node order " +
                                                    "received is not as expected [expected=" + next.internalOrder() +
                                                    ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                            break;
                                        }
                                    }

                                    updateLastSentMessageTime();

                                    if (log.isDebugEnabled())
                                        log.debug("Initialized connection with next node: " + next.id());

                                    if (debugMode)
                                        debugLog(msg, "Initialized connection with next node: " + next.id());

                                    errs = null;

                                    success = true;

                                    next.lastSuccessfulAddress(addr);
                                }
                            }
                            catch (IOException | IgniteCheckedException e) {
                                if (errs == null)
                                    errs = new ArrayList<>();

                                errs.add(e);

                                if (log.isDebugEnabled())
                                    U.error(log, "Failed to connect to next node [msg=" + msg
                                        + ", err=" + e.getMessage() + ']', e);

                                onException("Failed to connect to next node [msg=" + msg + ", err=" + e + ']', e);

                                // Fastens failure detection.
                                if (sndState != null && sndState.checkTimeout()) {
                                    segmentLocalNodeOnSendFail(failedNodes);

                                    return; // Nothing to do here.
                                }

                                if (!openSock)
                                    break; // Don't retry if we can not establish connection.

                                if (!spi.failureDetectionTimeoutEnabled() && ++reconCnt == spi.getReconnectCount())
                                    break;

                                if (spi.failureDetectionTimeoutEnabled() && timeoutHelper.checkFailureTimeoutReached(e))
                                    break;
                                else if (!spi.failureDetectionTimeoutEnabled() && (e instanceof
                                    SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class))) {
                                    ackTimeout0 *= 2;

                                    if (!checkAckTimeout(ackTimeout0))
                                        break;
                                }

                                continue;
                            }
                            finally {
                                if (!success) {
                                    if (log.isDebugEnabled())
                                        log.debug("Closing socket to next: " + next);

                                    U.closeQuiet(sock);

                                    sock = null;
                                }
                                else {
                                    // Resetting timeout control object to let the code below to use a new one
                                    // for the next bunch of operations.
                                    timeoutHelper = null;
                                }
                            }
                        }

                        try {
                            boolean failure;

                            synchronized (mux) {
                                failure = ServerImpl.this.failedNodes.size() < failedNodes.size();
                            }

                            assert !forceSndPending || msg instanceof TcpDiscoveryNodeLeftMessage;

                            if (failure || forceSndPending || newNextNode) {
                                if (log.isDebugEnabled())
                                    log.debug("Pending messages will be sent [failure=" + failure +
                                        ", newNextNode=" + newNextNode +
                                        ", forceSndPending=" + forceSndPending +
                                        ", failedNodes=" + failedNodes + ']');

                                if (debugMode)
                                    debugLog(msg, "Pending messages will be sent [failure=" + failure +
                                        ", newNextNode=" + newNextNode +
                                        ", forceSndPending=" + forceSndPending +
                                        ", failedNodes=" + failedNodes + ']');

                                for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                                    long tsNanos = System.nanoTime();

                                    prepareNodeAddedMessage(pendingMsg, next.id(), pendingMsgs.msgs,
                                        pendingMsgs.customDiscardId);

                                    addFailedNodes(pendingMsg, failedNodes);

                                    if (timeoutHelper == null)
                                        timeoutHelper = serverOperationTimeoutHelper(sndState, lastRingMsgSentTime);

                                    try {
                                        spi.writeToSocket(sock, out, pendingMsg, timeoutHelper.nextTimeoutChunk(
                                            spi.getSocketTimeout()));
                                    }
                                    finally {
                                        clearNodeAddedMessage(pendingMsg);
                                    }

                                    long tsNanos0 = System.nanoTime();

                                    int res = spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                    updateLastSentMessageTime();

                                    spi.stats.onMessageSent(pendingMsg, U.nanosToMillis(tsNanos0 - tsNanos));

                                    if (log.isDebugEnabled())
                                        log.debug("Pending message has been sent to next node [msgId=" + msg.id() +
                                            ", pendingMsgId=" + pendingMsg.id() + ", next=" + next.id() +
                                            ", res=" + res + ']');

                                    if (debugMode)
                                        debugLog(msg, "Pending message has been sent to next node [msgId=" + msg.id() +
                                            ", pendingMsgId=" + pendingMsg.id() + ", next=" + next.id() +
                                            ", res=" + res + ']');

                                    // Resetting timeout control object to create a new one for the next bunch of
                                    // operations.
                                    timeoutHelper = null;
                                }
                            }

                            if (!(msg instanceof TcpDiscoveryConnectionCheckMessage))
                                prepareNodeAddedMessage(msg, next.id(), pendingMsgs.msgs,
                                    pendingMsgs.customDiscardId);

                            try {
                                SecurityUtils.serializeVersion(1);

                                long tsNanos = System.nanoTime();

                                if (timeoutHelper == null)
                                    timeoutHelper = serverOperationTimeoutHelper(sndState, lastRingMsgSentTime);

                                addFailedNodes(msg, failedNodes);

                                boolean latencyCheck = msg instanceof TcpDiscoveryRingLatencyCheckMessage;

                                if (latencyCheck && log.isInfoEnabled())
                                    log.info("Latency check message has been written to socket: " + msg.id());

                                spi.writeToSocket(newNextNode ? newNext : next,
                                    sock,
                                    out,
                                    msg,
                                    timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                                long tsNanos0 = System.nanoTime();

                                int res = spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                updateLastSentMessageTime();

                                if (latencyCheck && log.isInfoEnabled())
                                    log.info("Latency check message has been acked: " + msg.id());

                                spi.stats.onMessageSent(msg, U.nanosToMillis(tsNanos0 - tsNanos));

                                onMessageExchanged();

                                DebugLogger debugLog = messageLogger(msg);

                                if (debugLog.isDebugEnabled()) {
                                    debugLog.debug("Message has been sent to next node [msg=" + msg +
                                        ", next=" + next.id() +
                                        ", res=" + res + ']');
                                }

                                if (debugMode) {
                                    debugLog(msg, "Message has been sent to next node [msg=" + msg +
                                        ", next=" + next.id() +
                                        ", res=" + res + ']');
                                }
                            }
                            finally {
                                SecurityUtils.restoreDefaultSerializeVersion();

                                clearNodeAddedMessage(msg);
                            }

                            registerPendingMessage(msg);

                            sent = true;

                            break addr;
                        }
                        catch (IOException | IgniteCheckedException e) {
                            if (errs == null)
                                errs = new ArrayList<>();

                            errs.add(e);

                            if (log.isDebugEnabled())
                                U.error(log, "Failed to send message to next node [next=" + next.id() + ", msg=" + msg +
                                    ", err=" + e + ']', e);

                            onException("Failed to send message to next node [next=" + next.id() + ", msg=" + msg + ']',
                                e);

                            if (spi.failureDetectionTimeoutEnabled() && timeoutHelper.checkFailureTimeoutReached(e))
                                break;

                            if (!spi.failureDetectionTimeoutEnabled()) {
                                if (++reconCnt == spi.getReconnectCount())
                                    break;
                                else if (e instanceof SocketTimeoutException ||
                                    X.hasCause(e, SocketTimeoutException.class)) {
                                    ackTimeout0 *= 2;

                                    if (!checkAckTimeout(ackTimeout0))
                                        break;
                                }
                            }
                        }
                        finally {
                            forceSndPending = false;

                            if (!sent) {
                                if (log.isDebugEnabled())
                                    log.debug("Closing socket to next (not sent): " + next);

                                U.closeQuiet(sock);

                                sock = null;

                                if (log.isDebugEnabled())
                                    log.debug("Message has not been sent [next=" + next.id() + ", msg=" + msg +
                                        (!spi.failureDetectionTimeoutEnabled() ? ", i=" + reconCnt : "") + ']');
                            }
                        }
                    } // Try to reconnect.
                } // Iterating node's addresses.

                if (!sent) {
                    if (sndState == null && spi.getEffectiveConnectionRecoveryTimeout() > 0)
                        sndState = new CrossRingMessageSendState();
                    else if (sndState != null && sndState.checkTimeout()) {
                        segmentLocalNodeOnSendFail(failedNodes);

                        return; // Nothing to do here.
                    }

                    boolean failedNextNode = sndState == null || sndState.markNextNodeFailed();

                    if (failedNextNode && !failedNodes.contains(next)) {
                        failedNodes.add(next);

                        if (state == CONNECTED) {
                            Exception err = errs != null ?
                                U.exceptionWithSuppressed("Failed to send message to next node [msg=" + msg +
                                    ", next=" + U.toShortString(next) + ']', errs) :
                                null;

                            // If node existed on connection initialization we should check
                            // whether it has not gone yet.
                            U.warn(log, "Failed to send message to next node [msg=" + msg + ", next=" + next +
                                ", errMsg=" + (err != null ? err.getMessage() : "N/A") + ']');
                        }
                    }
                    else if (!failedNextNode && sndState != null && sndState.isBackward()) {
                        boolean prev = sndState.markLastFailedNodeAlive();

                        U.warn(log, "Failed to send message to next node, try previous [msg=" + msg +
                            ", next=" + next + ']');

                        if (prev)
                            failedNodes.remove(failedNodes.size() - 1);
                        else {
                            newNextNode = false;

                            next = ring.nextNode(failedNodes);
                        }
                    }

                    next = null;

                    errs = null;
                }
                else
                    break;
            }

            synchronized (mux) {
                failedNodes.removeAll(ServerImpl.this.failedNodes.keySet());
            }

            if (!failedNodes.isEmpty()) {
                if (state == CONNECTED) {
                    if (!sent && log.isDebugEnabled())
                        // Message has not been sent due to some problems.
                        log.debug("Message has not been sent: " + msg);

                    if (log.isDebugEnabled())
                        log.debug("Detected failed nodes: " + failedNodes);
                }

                synchronized (mux) {
                    for (TcpDiscoveryNode failedNode : failedNodes) {
                        if (!ServerImpl.this.failedNodes.containsKey(failedNode))
                            ServerImpl.this.failedNodes.put(failedNode, locNodeId);
                    }

                    for (TcpDiscoveryNode failedNode : failedNodes)
                        failedNodesMsgSent.add(failedNode.id());
                }

                for (TcpDiscoveryNode n : failedNodes)
                    msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder()));

                if (!sent) {
                    assert next == null : next;

                    if (log.isDebugEnabled())
                        log.debug("Pending messages will be resent to local node");

                    if (debugMode)
                        debugLog(msg, "Pending messages will be resent to local node");

                    processPendingMessagesLocally(msg);
                }

                LT.warn(log, "Local node has detected failed nodes and started cluster-wide procedure. " +
                        "To speed up failure detection please see 'Failure Detection' section under javadoc" +
                        " for 'TcpDiscoverySpi'");
            }
        }

        /**
         * Called when local node became the only alive node in topology.
         *
         * @param curMsg Currently processed messages. There are three options possible:<ul>
         *     <li>{@link TcpDiscoveryNodeLeftMessage} from the last node.</li>
         *     <li>{@link TcpDiscoveryNodeFailedMessage} from the last node.</li>
         *     <li>Any other message, but all other nodes failed while sending.</li>
         * </ul>
         */
        private void processPendingMessagesLocally(TcpDiscoveryAbstractMessage curMsg) {
            UUID locNodeId = getLocalNodeId();

            for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                prepareNodeAddedMessage(pendingMsg, locNodeId, pendingMsgs.msgs,
                    pendingMsgs.customDiscardId);

                pendingMsg.senderNodeId(locNodeId);

                msgWorker.addMessage(pendingMsg);

                if (log.isDebugEnabled())
                    log.debug("Pending message has been sent to local node [msg=" + curMsg.id() +
                        ", pendingMsg=" + pendingMsg + ']');

                if (debugMode) {
                    debugLog(curMsg, "Pending message has been sent to local node [msg=" + curMsg.id() +
                        ", pendingMsg=" + pendingMsg + ']');
                }
            }
        }

        /**
         * Segment local node on failed message send.
         */
        private void segmentLocalNodeOnSendFail(List<TcpDiscoveryNode> failedNodes) {
            String failedNodesStr = failedNodes == null ? "" : (", failedNodes=" + failedNodes);

            synchronized (mux) {
                if (spiState == CONNECTING) {
                    U.warn(log, "Unable to connect to next nodes in a ring, it seems local node is experiencing " +
                        "connectivity issues or the rest of the cluster is undergoing massive restarts. Failing " +
                        "local node join to avoid case when one node fails a big part of cluster. To disable" +
                        " this behavior set TcpDiscoverySpi.setConnectionRecoveryTimeout() to 0. " +
                        "[connRecoveryTimeout=" + spi.connRecoveryTimeout + ", effectiveConnRecoveryTimeout="
                        + spi.getEffectiveConnectionRecoveryTimeout() + failedNodesStr + ']');

                    spiState = RING_FAILED;

                    mux.notifyAll();

                    return;
                }
            }

            U.warn(log, "Unable to connect to next nodes in a ring, " +
                "it seems local node is experiencing connectivity issues. Segmenting local node " +
                "to avoid case when one node fails a big part of cluster. To disable" +
                " this behavior set TcpDiscoverySpi.setConnectionRecoveryTimeout() to 0. " +
                "[connRecoveryTimeout=" + spi.connRecoveryTimeout + ", effectiveConnRecoveryTimeout="
                + spi.getEffectiveConnectionRecoveryTimeout() + failedNodesStr + ']');

            notifyDiscovery(EVT_NODE_SEGMENTED, ring.topologyVersion(), locNode);
        }

        /**
         * Adds failed node IDs to the given discovery message. Will not clean the existing failed node IDs collection
         * from the message.
         *
         * @param msg Message to add failed node IDs.
         * @param failedNodes Failed nodes to add to the message.
         */
        private void addFailedNodes(TcpDiscoveryAbstractMessage msg, Collection<TcpDiscoveryNode> failedNodes) {
            if (!failedNodes.isEmpty()) {
                for (TcpDiscoveryNode failedNode : failedNodes) {
                    assert !failedNode.equals(next) : failedNode;

                    msg.addFailedNode(failedNode.id());
                }
            }
        }

        /**
         * @param msg Message.
         * @return Whether to redirect message to client nodes.
         */
        private boolean redirectToClients(TcpDiscoveryAbstractMessage msg) {
            return msg.verified() && U.hasDeclaredAnnotation(msg, TcpDiscoveryRedirectToClient.class);
        }

        /**
         * Registers pending message.
         *
         * @param msg Message to register.
         */
        private void registerPendingMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            if (spi.ensured(msg)) {
                pendingMsgs.add(msg);

                spi.stats.onPendingMessageRegistered();

                if (log.isDebugEnabled())
                    log.debug("Pending message has been registered: " + msg.id());
            }
        }

        /**
         * Checks whether pending messages queue contains unprocessed {@link TcpDiscoveryNodeAddedMessage} for
         * the node with {@code nodeId}.
         *
         * @param nodeId Node ID.
         * @return {@code true} if contains, {@code false} otherwise.
         */
        private boolean hasPendingAddMessage(UUID nodeId) {
            if (pendingMsgs.msgs.isEmpty())
                return false;

            for (PendingMessage pendingMsg : pendingMsgs.msgs) {
                if (pendingMsg.msg instanceof TcpDiscoveryNodeAddedMessage) {
                    TcpDiscoveryNodeAddedMessage addMsg = (TcpDiscoveryNodeAddedMessage)pendingMsg.msg;

                    if (addMsg.node().id().equals(nodeId) && addMsg.id().compareTo(pendingMsgs.discardId) > 0)
                        return true;
                }
            }

            return false;
        }

        /**
         * Processes join request message.
         *
         * @param msg Join request message.
         */
        private void processJoinRequestMessage(final TcpDiscoveryJoinRequestMessage msg) {
            assert msg != null;

            final TcpDiscoveryNode node = msg.node();

            final UUID locNodeId = getLocalNodeId();

            msg.spanContainer().span()
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> node.id().toString())
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.CONSISTENT_ID),
                    () -> node.consistentId().toString());

            if (locNodeId.equals(node.id())) {
                if (log.isDebugEnabled())
                    log.debug("Received join request for local node, dropping: " + msg);

                msg.spanContainer().span()
                    .addLog(() -> "Dropped")
                    .setStatus(SpanStatus.ABORTED)
                    .end();

                return;
            }

            if (!msg.client()) {
                boolean rmtHostLoopback = node.socketAddresses().size() == 1 &&
                    node.socketAddresses().iterator().next().getAddress().isLoopbackAddress();

                // This check is performed by the node joining node is connected to, but not by coordinator
                // because loopback problem message is sent directly to the joining node which may be unavailable
                // if coordinator resides on another host.
                if (spi.locHost.isLoopbackAddress() != rmtHostLoopback) {
                    String firstNode = rmtHostLoopback ? "remote" : "local";

                    String secondNode = rmtHostLoopback ? "local" : "remote";

                    String errMsg = "Failed to add node to topology because " + firstNode +
                        " node is configured to use loopback address, but " + secondNode + " node is not " +
                        "(consider changing 'localAddress' configuration parameter) " +
                        "[locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(node) + ']';

                    LT.warn(log, errMsg);

                    // Always output in debug.
                    if (log.isDebugEnabled())
                        log.debug(errMsg);

                    try {
                        trySendMessageDirectly(node, new TcpDiscoveryLoopbackProblemMessage(
                            locNodeId, locNode.addresses(), locNode.hostNames()));
                    }
                    catch (IgniteSpiException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send loopback problem message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']');

                        onException("Failed to send loopback problem message to node " +
                            "[node=" + node + ", err=" + e.getMessage() + ']', e);
                    }

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }
            }

            if (isLocalNodeCoordinator()) {
                TcpDiscoveryNode existingNode = ring.node(node.id());

                if (existingNode != null) {
                    if (!node.socketAddresses().equals(existingNode.socketAddresses())) {
                        if (!pingNode(existingNode)) {
                            U.warn(log, "Sending node failed message for existing node: " + node);

                            addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                                existingNode.id(), existingNode.internalOrder()));

                            // Ignore this join request since existing node is about to fail
                            // and new node can continue.
                            msg.spanContainer().span()
                                .addLog(() -> "Ignored")
                                .setStatus(SpanStatus.ABORTED)
                                .end();

                            return;
                        }

                        try {
                            trySendMessageDirectly(node, createTcpDiscoveryDuplicateIdMessage(locNodeId, existingNode));
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send duplicate ID message to node " +
                                    "[node=" + node + ", existingNode=" + existingNode +
                                    ", err=" + e.getMessage() + ']');

                            onException("Failed to send duplicate ID message to node " +
                                "[node=" + node + ", existingNode=" + existingNode + ']', e);
                        }

                        // Output warning.
                        LT.warn(log, "Ignoring join request from node (duplicate ID) [node=" + node +
                            ", existingNode=" + existingNode + ']');

                        // Ignore join request.
                        msg.spanContainer().span()
                            .addLog(() -> "Ignored")
                            .setStatus(SpanStatus.ABORTED)
                            .end();

                        return;
                    }

                    if (msg.client()) {
                        TcpDiscoveryClientReconnectMessage reconMsg = new TcpDiscoveryClientReconnectMessage(node.id(),
                            node.clientRouterNodeId(),
                            null);

                        reconMsg.verify(getLocalNodeId());

                        Collection<TcpDiscoveryAbstractMessage> msgs = msgHist.messages(null, node);

                        if (msgs != null) {
                            reconMsg.pendingMessages(msgs);

                            reconMsg.success(true);
                        }

                        if (log.isDebugEnabled())
                            log.debug("Send reconnect message to already joined client " +
                                "[clientNode=" + existingNode + ", msg=" + reconMsg + ']');

                        if (getLocalNodeId().equals(node.clientRouterNodeId())) {
                            ClientMessageWorker wrk = clientMsgWorkers.get(node.id());

                            if (wrk != null)
                                wrk.addMessage(reconMsg);
                            else if (log.isDebugEnabled())
                                log.debug("Failed to find client message worker " +
                                    "[clientNode=" + existingNode + ", msg=" + reconMsg + ']');
                        }
                        else {
                            if (sendMessageToRemotes(reconMsg))
                                sendMessageAcrossRing(reconMsg);
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Ignoring join request message since node is already in topology: " + msg);

                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }
                else {
                    if (!node.isClient() && !node.isDaemon()) {
                        if (nodesIdsHist.contains(node.id())) {
                            try {
                                trySendMessageDirectly(node, createTcpDiscoveryDuplicateIdMessage(locNodeId, node));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send duplicate ID message to node " +
                                        "[node=" + node +
                                        ", err=" + e.getMessage() + ']');

                                onException("Failed to send duplicate ID message to node: " + node, e);
                            }

                            msg.spanContainer().span()
                                .addLog(() -> "Ignored")
                                .setStatus(SpanStatus.ABORTED)
                                .end();

                            return;
                        }
                    }
                }

                if (spi.nodeAuth != null) {
                    // Authenticate node first.
                    try {
                        SecurityCredentials cred = unmarshalCredentials(node);

                        SecurityContext subj = spi.nodeAuth.authenticateNode(node, cred);

                        if (subj == null) {
                            // Node has not pass authentication.
                            LT.warn(log, "Authentication failed [nodeId=" + node.id() +
                                ", addrs=" + U.addressesAsString(node) + ']');

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                    U.addressesAsString(node));

                            try {
                                trySendMessageDirectly(
                                    node,
                                    new TcpDiscoveryAuthFailedMessage(locNodeId, spi.locHost, node.id())
                                );
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                onException("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }

                            // Ignore join request.
                            msg.spanContainer().span()
                                .addLog(() -> "Ignored")
                                .setStatus(SpanStatus.ABORTED)
                                .end();

                            return;
                        }
                        else {
                            String authFailedMsg = null;

                            if (!(subj instanceof Serializable)) {
                                // Node has not pass authentication.
                                LT.warn(log, "Authentication subject is not Serializable [nodeId=" + node.id() +
                                    ", addrs=" + U.addressesAsString(node) + ']');

                                authFailedMsg = "Authentication subject is not serializable";
                            }
                            else if (node.clientRouterNodeId() == null &&
                                !subj.systemOperationAllowed(SecurityPermission.JOIN_AS_SERVER))
                                authFailedMsg = "Node is not authorised to join as a server node";

                            if (authFailedMsg != null) {
                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug(authFailedMsg + " [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node));

                                try {
                                    trySendMessageDirectly(
                                        node,
                                        new TcpDiscoveryAuthFailedMessage(locNodeId, spi.locHost, node.id())
                                    );
                                }
                                catch (IgniteSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send unauthenticated message to node " +
                                            "[node=" + node + ", err=" + e.getMessage() + ']');
                                }

                                // Ignore join request.
                                msg.spanContainer().span()
                                    .addLog(() -> "Ignored")
                                    .setStatus(SpanStatus.ABORTED)
                                    .end();

                                return;
                            }

                            // Stick in authentication subject to node (use security-safe attributes for copy).
                            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

                            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2, U.marshal(spi.marshaller(), subj));

                            node.setAttributes(attrs);
                        }
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        LT.error(log, e, "Authentication failed [nodeId=" + node.id() + ", addrs=" +
                            U.addressesAsString(node) + ']');

                        if (log.isDebugEnabled())
                            log.debug("Failed to authenticate node (will ignore join request) [node=" + node +
                                ", err=" + e + ']');

                        onException("Failed to authenticate node (will ignore join request) [node=" + node +
                            ", err=" + e + ']', e);

                        // Ignore join request.
                        msg.spanContainer().span()
                            .addLog(() -> "Ignored")
                            .setStatus(SpanStatus.ABORTED)
                            .end();

                        return;
                    }
                }

                IgniteNodeValidationResult err;

                err = spi.getSpiContext().validateNode(node);

                if (err == null) {
                    try {
                        DiscoveryDataBag data = msg.gridDiscoveryData().unmarshalJoiningNodeData(
                            spi.marshaller(),
                            U.resolveClassLoader(spi.ignite().configuration()),
                            false,
                            log
                        );

                        err = spi.getSpiContext().validateNode(node, data);
                    }
                    catch (IgniteCheckedException e) {
                        err = new IgniteNodeValidationResult(node.id(), e.getMessage());
                    }
                }

                if (err != null) {
                    final IgniteNodeValidationResult err0 = err;

                    if (log.isDebugEnabled())
                        log.debug("Node validation failed [res=" + err + ", node=" + node + ']');

                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                spi.getSpiContext().recordEvent(new NodeValidationFailedEvent(locNode, node, err0));

                                boolean ping = node.id().equals(err0.nodeId()) ? pingNode(node) : pingNode(err0.nodeId());

                                if (!ping) {
                                    if (log.isDebugEnabled())
                                        log.debug("Conflicting node has already left, need to wait for event. " +
                                            "Will ignore join request for now since it will be recent [req=" + msg +
                                            ", err=" + err0.message() + ']');

                                    // Ignore join request.
                                    return;
                                }

                                LT.warn(log, err0.message());

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug(err0.message());

                                try {
                                    trySendMessageDirectly(node,
                                        new TcpDiscoveryCheckFailedMessage(err0.nodeId(), err0.sendMessage()));
                                }
                                catch (IgniteSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send hash ID resolver validation failed message to node " +
                                            "[node=" + node + ", err=" + e.getMessage() + ']');

                                    onException("Failed to send hash ID resolver validation failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']', e);
                                }
                            }
                        }
                    );

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                final String locMarsh = locNode.attribute(ATTR_MARSHALLER);
                final String rmtMarsh = node.attribute(ATTR_MARSHALLER);

                if (!F.eq(locMarsh, rmtMarsh)) {
                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                String errMsg = "Local node's marshaller differs from remote node's marshaller " +
                                    "(to make sure all nodes in topology have identical marshaller, " +
                                    "configure marshaller explicitly in configuration) " +
                                    "[locMarshaller=" + locMarsh + ", rmtMarshaller=" + rmtMarsh +
                                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                                LT.warn(log, errMsg);

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug(errMsg);

                                try {
                                    String sndMsg = "Local node's marshaller differs from remote node's marshaller " +
                                        "(to make sure all nodes in topology have identical marshaller, " +
                                        "configure marshaller explicitly in configuration) " +
                                        "[locMarshaller=" + rmtMarsh + ", rmtMarshaller=" + locMarsh +
                                        ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                        ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                        ", rmtNodeId=" + locNode.id() + ']';

                                    trySendMessageDirectly(node,
                                        new TcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                                }
                                catch (IgniteSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send marshaller check failed message to node " +
                                            "[node=" + node + ", err=" + e.getMessage() + ']');

                                    onException("Failed to send marshaller check failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']', e);
                                }
                            }
                        }
                    );

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                // If node have no value for this attribute then we treat it as true.
                final Boolean locMarshUseDfltSuid = locNode.attribute(ATTR_MARSHALLER_USE_DFLT_SUID);
                boolean locMarshUseDfltSuidBool = locMarshUseDfltSuid == null ? true : locMarshUseDfltSuid;

                final Boolean rmtMarshUseDfltSuid = node.attribute(ATTR_MARSHALLER_USE_DFLT_SUID);
                boolean rmtMarshUseDfltSuidBool = rmtMarshUseDfltSuid == null ? true : rmtMarshUseDfltSuid;

                Boolean locLateAssign = locNode.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);
                // Can be null only in tests.
                boolean locLateAssignBool = locLateAssign != null ? locLateAssign : false;

                if (locMarshUseDfltSuidBool != rmtMarshUseDfltSuidBool) {
                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                String errMsg = "Local node's " + IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID +
                                    " property value differs from remote node's value " +
                                    "(to make sure all nodes in topology have identical marshaller settings, " +
                                    "configure system property explicitly) " +
                                    "[locMarshUseDfltSuid=" + locMarshUseDfltSuid +
                                    ", rmtMarshUseDfltSuid=" + rmtMarshUseDfltSuid +
                                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                                String sndMsg = "Local node's " + IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID +
                                    " property value differs from remote node's value " +
                                    "(to make sure all nodes in topology have identical marshaller settings, " +
                                    "configure system property explicitly) " +
                                    "[locMarshUseDfltSuid=" + rmtMarshUseDfltSuid +
                                    ", rmtMarshUseDfltSuid=" + locMarshUseDfltSuid +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                nodeCheckError(
                                    node,
                                    errMsg,
                                    sndMsg);
                            }
                        });

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                // Validate compact footer flags.
                Boolean locMarshCompactFooter = locNode.attribute(ATTR_MARSHALLER_COMPACT_FOOTER);
                final boolean locMarshCompactFooterBool = locMarshCompactFooter != null ? locMarshCompactFooter : false;

                Boolean rmtMarshCompactFooter = node.attribute(ATTR_MARSHALLER_COMPACT_FOOTER);
                final boolean rmtMarshCompactFooterBool = rmtMarshCompactFooter != null ? rmtMarshCompactFooter : false;

                if (locMarshCompactFooterBool != rmtMarshCompactFooterBool) {
                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                String errMsg = "Local node's binary marshaller \"compactFooter\" property differs from " +
                                    "the same property on remote node (make sure all nodes in topology have the same value " +
                                    "of \"compactFooter\" property) [locMarshallerCompactFooter=" + locMarshCompactFooterBool +
                                    ", rmtMarshallerCompactFooter=" + rmtMarshCompactFooterBool +
                                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                                String sndMsg = "Local node's binary marshaller \"compactFooter\" property differs from " +
                                    "the same property on remote node (make sure all nodes in topology have the same value " +
                                    "of \"compactFooter\" property) [locMarshallerCompactFooter=" + rmtMarshCompactFooterBool +
                                    ", rmtMarshallerCompactFooter=" + locMarshCompactFooterBool +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                nodeCheckError(
                                    node,
                                    errMsg,
                                    sndMsg);
                            }
                        });

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                // Validate String serialization mechanism used by the BinaryMarshaller.
                final Boolean locMarshStrSerialVer2 = locNode.attribute(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2);
                final boolean locMarshStrSerialVer2Bool = locMarshStrSerialVer2 != null ? locMarshStrSerialVer2 : false;

                final Boolean rmtMarshStrSerialVer2 = node.attribute(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2);
                final boolean rmtMarshStrSerialVer2Bool = rmtMarshStrSerialVer2 != null ? rmtMarshStrSerialVer2 : false;

                if (locMarshStrSerialVer2Bool != rmtMarshStrSerialVer2Bool) {
                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                String errMsg = "Local node's " + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 +
                                    " property value differs from remote node's value " +
                                    "(to make sure all nodes in topology have identical marshaller settings, " +
                                    "configure system property explicitly) " +
                                    "[locMarshStrSerialVer2=" + locMarshStrSerialVer2 +
                                    ", rmtMarshStrSerialVer2=" + rmtMarshStrSerialVer2 +
                                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                                String sndMsg = "Local node's " + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 +
                                    " property value differs from remote node's value " +
                                    "(to make sure all nodes in topology have identical marshaller settings, " +
                                    "configure system property explicitly) " +
                                    "[locMarshStrSerialVer2=" + rmtMarshStrSerialVer2 +
                                    ", rmtMarshStrSerialVer2=" + locMarshStrSerialVer2 +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                nodeCheckError(
                                    node,
                                    errMsg,
                                    sndMsg);
                            }
                        });

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                Boolean rmtLateAssign = node.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);
                // Can be null only in tests.
                boolean rmtLateAssignBool = rmtLateAssign != null ? rmtLateAssign : false;

                if (locLateAssignBool != rmtLateAssignBool) {
                    String errMsg = "Local node's cache affinity assignment mode differs from " +
                        "the same property on remote node (make sure all nodes in topology have the same " +
                        "cache affinity assignment mode) [locLateAssign=" + locLateAssignBool +
                        ", rmtLateAssign=" + rmtLateAssignBool +
                        ", locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(node) +
                        ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                    String sndMsg = "Local node's cache affinity assignment mode differs from " +
                        "the same property on remote node (make sure all nodes in topology have the same " +
                        "cache affinity assignment mode) [locLateAssign=" + rmtLateAssignBool +
                        ", rmtLateAssign=" + locLateAssign +
                        ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                        ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                        ", rmtNodeId=" + locNode.id() + ']';

                    nodeCheckError(node, errMsg, sndMsg);

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                final Boolean locSrvcProcModeAttr = locNode.attribute(ATTR_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED);
                // Can be null only in module tests of discovery spi (without node startup).
                final Boolean locSrvcProcMode = locSrvcProcModeAttr != null ? locSrvcProcModeAttr : false;

                final Boolean rmtSrvcProcModeAttr = node.attribute(ATTR_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED);
                final boolean rmtSrvcProcMode = rmtSrvcProcModeAttr != null ? rmtSrvcProcModeAttr : false;

                if (!F.eq(locSrvcProcMode, rmtSrvcProcMode)) {
                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                String errMsg = "Local node's " + IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED +
                                    " property value differs from remote node's value " +
                                    "(to make sure all nodes in topology have identical service processor mode, " +
                                    "configure system property explicitly) " +
                                    "[locSrvcProcMode=" + locSrvcProcMode +
                                    ", rmtSrvcProcMode=" + rmtSrvcProcMode +
                                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                                String sndMsg = "Local node's " + IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED +
                                    " property value differs from remote node's value " +
                                    "(to make sure all nodes in topology have identical service processor mode, " +
                                    "configure system property explicitly) " +
                                    "[locSrvcProcMode=" + rmtSrvcProcMode +
                                    ", rmtSrvcProcMode=" + locSrvcProcMode +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                nodeCheckError(
                                    node,
                                    errMsg,
                                    sndMsg);
                            }
                        });

                    // Ignore join request.
                    msg.spanContainer().span()
                        .addLog(() -> "Ignored")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                // Handle join.
                node.internalOrder(ring.nextNodeOrder());

                if (log.isDebugEnabled())
                    log.debug("Internal order has been assigned to node: " + node);

                DiscoveryDataPacket data = msg.gridDiscoveryData();

                TcpDiscoveryNodeAddedMessage nodeAddedMsg = new TcpDiscoveryNodeAddedMessage(locNodeId,
                    node, data, spi.gridStartTime);

                nodeAddedMsg = tracing.messages().branch(nodeAddedMsg, msg);

                nodeAddedMsg.client(msg.client());

                processNodeAddedMessage(nodeAddedMsg);

                tracing.messages().finishProcessing(nodeAddedMsg);
            }
            else {
                if (sendMessageToRemotes(msg))
                    sendMessageAcrossRing(msg);
            }
        }

        /**
         * @param node Node.
         * @param name Attribute name.
         * @param dflt Default value.
         * @return Attribute value.
         */
        private boolean booleanAttribute(ClusterNode node, String name, boolean dflt) {
            Boolean attr = node.attribute(name);

            return attr != null ? attr : dflt;
        }

        /**
         * @param node Joining node.
         * @param errMsg Message to log.
         * @param sndMsg Message to send.
         */
        private void nodeCheckError(TcpDiscoveryNode node, String errMsg, String sndMsg) {
            LT.warn(log, errMsg);

            // Always output in debug.
            if (log.isDebugEnabled())
                log.debug(errMsg);

            try {
                trySendMessageDirectly(node, new TcpDiscoveryCheckFailedMessage(locNode.id(), sndMsg));
            }
            catch (IgniteSpiException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send marshaller check failed message to node " +
                        "[node=" + node + ", err=" + e.getMessage() + ']');

                onException("Failed to send marshaller check failed message to node " +
                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
            }
        }

        /** */
        private void trySendMessageDirectlyToAddrs(
            Collection<InetSocketAddress> addrs,
            @Nullable TcpDiscoveryNode node,
            TcpDiscoveryAbstractMessage msg
        ) {
            IgniteSpiException ex = null;

            for (InetSocketAddress addr : addrs) {
                try {
                    IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi, true);

                    sendMessageDirectly(msg, addr, timeoutHelper);

                    if (node != null)
                        node.lastSuccessfulAddress(addr);

                    return;
                }
                catch (IgniteSpiException e) {
                    ex = e;
                }
            }

            if (ex != null)
                throw ex;
        }

        /**
         * Tries to send a message to all node's available addresses.
         *
         * @param addrs Node addresses, used when node could not be found in <code>ring</code>.
         * @param nodeId Node ID to send message to.
         * @param msg Message.
         * @throws IgniteSpiException Last failure if all attempts failed.
         */
        private void trySendMessageDirectly(
            Collection<InetSocketAddress> addrs,
            UUID nodeId,
            TcpDiscoveryAbstractMessage msg
        ) {
            TcpDiscoveryNode node = ring.node(nodeId);

            if (node == null) {
                if (!F.isEmpty(addrs))
                    trySendMessageDirectlyToAddrs(addrs, null, msg);
                else
                    throw new IgniteSpiException("Node does not exist: " + nodeId);
            }
            else
                trySendMessageDirectly(node, msg);
        }

        /**
         * Tries to send a message to all node's available addresses.
         *
         * @param node Node to send message to.
         * @param msg Message.
         * @throws IgniteSpiException Last failure if all attempts failed.
         */
        private void trySendMessageDirectly(TcpDiscoveryNode node, TcpDiscoveryAbstractMessage msg)
            throws IgniteSpiException {
            if (node.clientRouterNodeId() != null) {
                TcpDiscoveryNode routerNode = ring.node(node.clientRouterNodeId());

                if (routerNode == null)
                    throw new IgniteSpiException("Router node for client does not exist: " + node);

                if (routerNode.clientRouterNodeId() != null)
                    throw new IgniteSpiException("Router node is a client node: " + node);

                if (routerNode.id().equals(getLocalNodeId())) {
                    sendDirectlyToClient(node.id(), msg);

                    return;
                }

                trySendMessageDirectly(routerNode, msg);

                return;
            }

            trySendMessageDirectlyToAddrs(spi.getNodeAddresses(node, U.sameMacs(locNode, node)), node, msg);
        }

        /**
         * @param clientNodeId Client node id.
         * @param msg Message to send.
         * @throws IgniteSpiException Last failure if all attempts failed.
         */
        private void sendDirectlyToClient(UUID clientNodeId, TcpDiscoveryAbstractMessage msg) {
            ClientMessageWorker worker = clientMsgWorkers.get(clientNodeId);

            if (worker == null)
                throw new IgniteSpiException("Client node already disconnected: " + clientNodeId);

            msg.verify(getLocalNodeId()); // Client worker require verified messages.

            worker.addMessage(msg);
        }

        /**
         * Processes node added message.
         *
         * For coordinator node method marks the messages as verified for rest of nodes to apply the
         * changes this message is issued for.
         *
         * Node added message is processed by other nodes only after coordinator verification.
         *
         * @param msg Node added message.
         * @deprecated Due to current protocol node add process cannot be dropped in the middle of the ring,
         *      if new node auth fails due to config inconsistency. So, we need to finish add
         *      and only then initiate failure.
         */
        @Deprecated
        private void processNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
            assert msg != null;

            TcpDiscoveryNode node = msg.node();

            assert node != null;

            msg.spanContainer().span()
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> node.id().toString())
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.CONSISTENT_ID),
                    () -> node.consistentId().toString());

            if (node.internalOrder() < locNode.internalOrder()) {
                if (!locNode.id().equals(node.id())) {
                    U.warn(log, "Discarding node added message since local node's order is greater " +
                            "[node=" + node + ", ring=" + ring + ", msg=" + msg + ']');

                    return;
                }
                else {
                    // TODO IGNITE-11272
                    if (log.isDebugEnabled())
                        log.debug("Received node added message with node order smaller than local node order " +
                            "(will appy) [node=" + node + ", ring=" + ring + ", msg=" + msg + ']');
                }
            }

            UUID locNodeId = getLocalNodeId();

            if (isLocalNodeCoordinator()) {
                if (msg.verified()) {
                    TcpDiscoveryNodeAddFinishedMessage addFinishMsg = new TcpDiscoveryNodeAddFinishedMessage(locNodeId,
                        node.id());

                    if (node.clientRouterNodeId() != null) {
                        addFinishMsg.clientDiscoData(msg.gridDiscoveryData());

                        addFinishMsg.clientNodeAttributes(node.attributes());
                    }

                    addFinishMsg = tracing.messages().branch(addFinishMsg, msg);

                    addFinishMsg.spanContainer().span()
                        .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> node.id().toString())
                        .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.CONSISTENT_ID),
                            () -> node.consistentId().toString());

                    processNodeAddFinishedMessage(addFinishMsg);

                    tracing.messages().finishProcessing(addFinishMsg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }

                msg.verify(locNodeId);

                msg.spanContainer().span()
                    .addLog(() -> "Verified");
            }
            else if (!locNodeId.equals(node.id()) && ring.node(node.id()) != null) {
                // Local node already has node from message in local topology.
                // Just pass it to coordinator via the ring.
                if (sendMessageToRemotes(msg))
                    sendMessageAcrossRing(msg);

                if (log.isDebugEnabled()) {
                    log.debug("Local node already has node being added. Passing TcpDiscoveryNodeAddedMessage to " +
                        "coordinator for final processing [ring=" + ring + ", node=" + node + ", locNode="
                        + locNode + ", msg=" + msg + ']');
                }

                if (debugMode) {
                    debugLog(msg, "Local node already has node being added. Passing TcpDiscoveryNodeAddedMessage to " +
                        "coordinator for final processing [ring=" + ring + ", node=" + node + ", locNode="
                        + locNode + ", msg=" + msg + ']');
                }

                msg.spanContainer().span()
                    .addLog(() -> "Bypassed to crd")
                    .setStatus(SpanStatus.OK)
                    .end();

                return;
            }

            if (msg.verified() && !locNodeId.equals(node.id())) {
                if (!node.isClient() && !node.isDaemon() && nodesIdsHist.contains(node.id())) {
                    U.warn(log, "Discarding node added message since local node has already seen " +
                        "joining node in topology [node=" + node + ", locNode=" + locNode + ", msg=" + msg + ']');

                    msg.spanContainer().span()
                        .addLog(() -> "Discarded")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                if (node.internalOrder() <= ring.maxInternalOrder()) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    if (debugMode)
                        debugLog(msg, "Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    msg.spanContainer().span()
                        .addLog(() -> "Discarded")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    return;
                }

                synchronized (mux) {
                    joiningNodes.add(node.id());
                }

                if (!isLocalNodeCoordinator() && spi.nodeAuth != null && spi.nodeAuth.isGlobalNodeAuthentication()) {
                    boolean authFailed = true;

                    try {
                        SecurityCredentials cred = unmarshalCredentials(node);

                        if (cred == null) {
                            if (log.isDebugEnabled())
                                log.debug(
                                    "Skipping global authentication for node (security credentials not found, " +
                                        "probably, due to coordinator has older version) " +
                                        "[nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node) +
                                        ", coord=" + ring.coordinator() + ']');

                            authFailed = false;
                        }
                        else {
                            SecurityContext subj = spi.nodeAuth.authenticateNode(node, cred);

                            SecurityContext coordSubj = nodeSecurityContext(
                                spi.marshaller(), U.resolveClassLoader(spi.ignite().configuration()), node
                            );

                            if (!permissionsEqual(getPermissions(coordSubj), getPermissions(subj))) {
                                // Node has not pass authentication.
                                LT.warn(log, "Authentication failed [nodeId=" + node.id() +
                                    ", addrs=" + U.addressesAsString(node) + ']');

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                        U.addressesAsString(node));
                            }
                            else
                                // Node will not be kicked out.
                                authFailed = false;
                        }
                    }
                    catch (IgniteException e) {
                        U.error(log, "Failed to verify node permissions consistency (will drop the node): " + node, e);
                    }
                    finally {
                        if (authFailed) {
                            try {
                                trySendMessageDirectly(
                                    node,
                                    new TcpDiscoveryAuthFailedMessage(locNodeId, spi.locHost, node.id())
                                );
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                onException("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }

                            addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, node.id(),
                                node.internalOrder()));
                        }
                    }
                }

                if (msg.client())
                    node.clientAliveTime(spi.clientFailureDetectionTimeout());

                boolean topChanged = ring.add(node);

                if (topChanged) {
                    assert !node.visible() : "Added visible node [node=" + node + ", locNode=" + locNode + ']';

                    DiscoveryDataPacket dataPacket = msg.gridDiscoveryData();

                    assert dataPacket != null : msg;

                    dataPacket.joiningNodeClient(msg.client());

                    if (dataPacket.hasJoiningNodeData()) {
                        if (spiState == CONNECTED) {
                            // Node already connected to the cluster can apply joining nodes' disco data immediately
                            spi.onExchange(dataPacket, U.resolveClassLoader(spi.ignite().configuration()));

                            if (!node.isDaemon())
                                spi.collectExchangeData(dataPacket);
                        }
                        else if (spiState == CONNECTING)
                            // Node joining to the cluster should postpone applying disco data of other joiners till
                            // receiving gridDiscoData (when NodeAddFinished message arrives)
                            joiningNodesDiscoDataList.add(dataPacket);
                    }

                    processMessageFailedNodes(msg);
                }

                if (log.isDebugEnabled())
                    log.debug("Added node to local ring [added=" + topChanged + ", node=" + node +
                        ", ring=" + ring + ']');
            }

            if (msg.verified() && locNodeId.equals(node.id())) {
                synchronized (mux) {
                    if (spiState == CONNECTING && locNode.internalOrder() != node.internalOrder()) {
                        // Initialize topology.
                        Collection<TcpDiscoveryNode> top = msg.topology();

                        if (top != null && !top.isEmpty()) {
                            spi.gridStartTime = msg.gridStartTime();

                            if (spi.nodeAuth != null && spi.nodeAuth.isGlobalNodeAuthentication()) {
                                TcpDiscoveryAbstractMessage authFail =
                                    new TcpDiscoveryAuthFailedMessage(locNodeId, spi.locHost, node.id());

                                try {
                                    ClassLoader ldr = U.resolveClassLoader(spi.ignite().configuration());

                                    SecurityContext rmCrd = nodeSecurityContext(
                                        spi.marshaller(), ldr, node
                                    );

                                    SecurityContext locCrd = nodeSecurityContext(
                                        spi.marshaller(), ldr, locNode
                                    );

                                    if (!permissionsEqual(getPermissions(locCrd), getPermissions(rmCrd))) {
                                        // Node has not pass authentication.
                                        LT.warn(log,
                                            "Failed to authenticate local node " +
                                                "(local authentication result is different from rest of topology) " +
                                                "[nodeId=" + node.id() + ", addrs=" + U.addressesAsString(node) + ']');

                                        joinRes.set(authFail);

                                        spiState = AUTH_FAILED;

                                        mux.notifyAll();

                                        return;
                                    }
                                }
                                catch (IgniteException e) {
                                    U.error(log, "Failed to verify node permissions consistency (will drop the node): " + node, e);

                                    joinRes.set(authFail);

                                    spiState = AUTH_FAILED;

                                    mux.notifyAll();

                                    return;
                                }
                            }

                            for (TcpDiscoveryNode n : top) {
                                assert n.internalOrder() < node.internalOrder() :
                                    "Invalid node [topNode=" + n + ", added=" + node + ']';

                                // Make all preceding nodes and local node visible.
                                n.visible(true);

                                if (nodeCompactRepresentationSupported) {
                                    nodeCompactRepresentationSupported =
                                        nodeSupports(
                                            n,
                                            TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION
                                        );
                                }
                            }

                            joiningNodes.clear();

                            locNode.setAttributes(node.attributes());

                            locNode.visible(true);

                            // Restore topology with all nodes visible.
                            ring.restoreTopology(top, node.internalOrder());

                            if (log.isDebugEnabled())
                                log.debug("Restored topology from node added message: " + ring);

                            gridDiscoveryData = msg.gridDiscoveryData();

                            joiningNodesDiscoDataList = new ArrayList<>();

                            topHist.clear();
                            topHist.putAll(msg.topologyHistory());

                            pendingMsgs.reset(msg.messages(), msg.discardedMessageId(),
                                msg.discardedCustomMessageId());

                            // Clear data to minimize message size.
                            msg.messages(null, null, null);
                            msg.topology(null);
                            msg.topologyHistory(null);
                            msg.clearDiscoveryData();
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Discarding node added message with empty topology: " + msg);

                            msg.spanContainer().span()
                                .addLog(() -> "Discarded")
                                .setStatus(SpanStatus.ABORTED)
                                .end();

                            return;
                        }
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node added message (this message has already been processed) " +
                                "[spiState=" + spiState +
                                ", msg=" + msg +
                                ", locNode=" + locNode + ']');

                        msg.spanContainer().span()
                            .addLog(() -> "Discarded")
                            .setStatus(SpanStatus.ABORTED)
                            .end();

                        return;
                    }
                }

                processMessageFailedNodes(msg);
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
        }

        /**
         * @param secCtx Security context.
         * @return Security permission set.
         */
        private @Nullable SecurityPermissionSet getPermissions(SecurityContext secCtx) {
            if (secCtx == null || secCtx.subject() == null)
                return null;

            return secCtx.subject().permissions();
        }

        /**
         * Processes node add finished message.
         *
         * @param msg Node add finished message.
         */
        private void processNodeAddFinishedMessage(TcpDiscoveryNodeAddFinishedMessage msg) {
            assert msg != null;

            UUID nodeId = msg.nodeId();

            assert nodeId != null;

            TcpDiscoveryNode node = ring.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message since node is not found " +
                        "[msg=" + msg + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Node to finish add: " + node);

            //we will need to recalculate this value since the topology changed
            if (nodeCompactRepresentationSupported) {
                nodeCompactRepresentationSupported =
                    nodeSupports(node, TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION);
            }

            boolean locNodeCoord = isLocalNodeCoordinator();

            UUID locNodeId = getLocalNodeId();

            if (locNodeCoord) {
                if (msg.verified()) {
                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }

                if (node.visible() && node.order() != 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node add finished message since node has already been added " +
                            "[node=" + node + ", msg=" + msg + ']');

                    return;
                }
                else
                    msg.topologyVersion(ring.incrementTopologyVersion());

                msg.verify(locNodeId);
            }

            long topVer = msg.topologyVersion();

            boolean fireEvt = false;

            if (msg.verified()) {
                assert topVer > 0 : "Invalid topology version: " + msg;

                if (node.order() == 0)
                    node.order(topVer);

                if (!node.visible()) {
                    node.visible(true);

                    fireEvt = true;
                }
            }

            synchronized (mux) {
                joiningNodes.remove(nodeId);
            }

            TcpDiscoverySpiState state = spiStateCopy();

            if (msg.verified() && !locNodeId.equals(nodeId) && state != CONNECTING && fireEvt) {
                spi.stats.onNodeJoined();

                // Make sure that node with greater order will never get EVT_NODE_JOINED
                // on node with less order.
                assert node.internalOrder() > locNode.internalOrder() : "Invalid order [node=" + node +
                    ", locNode=" + locNode + ", msg=" + msg + ", ring=" + ring + ']';

                if (spi.locNodeVer.equals(node.version()))
                    node.version(spi.locNodeVer);

                if (!locNodeCoord) {
                    boolean b = ring.topologyVersion(topVer);

                    assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                        ", lastMsg=" + lastMsg + ", spiState=" + state + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                if (state == CONNECTED) {
                    boolean notified = notifyDiscovery(EVT_NODE_JOINED, topVer, node, msg.spanContainer());

                    notifiedDiscovery.set(notified);

                    if (!node.isClient() && !node.isDaemon())
                        nodesIdsHist.add(node.id());
                }

                try {
                    if (spi.ipFinder.isShared() && locNodeCoord && node.clientRouterNodeId() == null)
                        spi.ipFinder.registerAddresses(node.socketAddresses());
                }
                catch (IgniteSpiException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to register new node address [node=" + node +
                            ", err=" + e.getMessage() + ']');

                    onException("Failed to register new node address [node=" + node +
                        ", err=" + e.getMessage() + ']', e);
                }
            }

            if (msg.verified() && locNodeId.equals(nodeId) && state == CONNECTING) {
                assert node != null;

                assert topVer > 0 : "Invalid topology version: " + msg;

                ring.topologyVersion(topVer);

                node.order(topVer);

                synchronized (mux) {
                    spiState = CONNECTED;

                    mux.notifyAll();
                }

                if (gridDiscoveryData != null)
                    spi.onExchange(gridDiscoveryData, U.resolveClassLoader(spi.ignite().configuration()));

                if (joiningNodesDiscoDataList != null) {
                    for (DiscoveryDataPacket dataPacket : joiningNodesDiscoDataList)
                        spi.onExchange(dataPacket, U.resolveClassLoader(spi.ignite().configuration()));
                }

                nullifyDiscoData();

                // Discovery manager must create local joined event before spiStart completes.
                boolean notified = notifyDiscovery(EVT_NODE_JOINED, topVer, locNode, msg.spanContainer());

                notifiedDiscovery.set(notified);
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);

            checkPendingCustomMessages();
        }

        /**
         * Processes latency check message.
         *
         * @param msg Latency check message.
         */
        private void processRingLatencyCheckMessage(TcpDiscoveryRingLatencyCheckMessage msg) {
            assert msg != null;

            if (msg.maxHopsReached()) {
                if (log.isInfoEnabled())
                    log.info("Latency check has been discarded (max hops reached) [id=" + msg.id() +
                        ", maxHops=" + msg.maxHops() + ']');

                return;
            }

            if (log.isInfoEnabled())
                log.info("Latency check processing: " + msg.id());

            if (ring.hasRemoteServerNodes())
                sendMessageAcrossRing(msg);
            else {
                if (log.isInfoEnabled())
                    log.info("Latency check has been discarded (no remote nodes): " + msg.id());
            }
        }

        /**
         * Processes node left message.
         *
         * @param msg Node left message.
         */
        private void processNodeLeftMessage(TcpDiscoveryNodeLeftMessage msg) {
            assert msg != null;

            UUID locNodeId = getLocalNodeId();

            UUID leavingNodeId = msg.creatorNodeId();

            msg.spanContainer().span()
                .addTag(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID), () -> leavingNodeId.toString());

            if (locNodeId.equals(leavingNodeId)) {
                if (msg.senderNodeId() == null) {
                    synchronized (mux) {
                        if (log.isDebugEnabled())
                            log.debug("Starting local node stop procedure.");

                        spiState = STOPPING;

                        mux.notifyAll();
                    }
                }

                if (msg.verified() || !ring.hasRemoteNodes() || msg.senderNodeId() != null) {
                    if (spi.ipFinder.isShared() && !ring.hasRemoteNodes()) {
                        try {
                            spi.ipFinder.unregisterAddresses(
                                U.resolveAddresses(spi.getAddressResolver(), locNode.socketAddresses()));
                        }
                        catch (IgniteSpiException e) {
                            U.error(log, "Failed to unregister local node address from IP finder.", e);
                        }
                    }

                    synchronized (mux) {
                        if (spiState == STOPPING) {
                            spiState = LEFT;

                            mux.notifyAll();
                        }
                    }

                    return;
                }

                sendMessageAcrossRing(msg);

                return;
            }

            if (ring.node(msg.senderNodeId()) == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node left message since sender node is not in topology: " + msg);

                return;
            }

            TcpDiscoveryNode leavingNode = ring.node(leavingNodeId);

            if (leavingNode != null) {
                synchronized (mux) {
                    leavingNodes.add(leavingNode);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Discarding node left message since node was not found: " + msg);

                msg.spanContainer().span()
                    .addLog(() -> "Discarded")
                    .setStatus(SpanStatus.ABORTED)
                    .end();

                return;
            }

            boolean locNodeCoord = isLocalNodeCoordinator();

            if (locNodeCoord) {
                if (msg.verified()) {
                    msg.spanContainer().span()
                        .addLog(() -> "Ring failed")
                        .setStatus(SpanStatus.ABORTED)
                        .end();

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }

                msg.verify(locNodeId);

                msg.spanContainer().span()
                    .addLog(() -> "Verified");
            }

            if (msg.verified() && !locNodeId.equals(leavingNodeId)) {
                //we will need to recalculate this value since the topology changed
                if (!nodeCompactRepresentationSupported) {
                    nodeCompactRepresentationSupported =
                        allNodesSupport(TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION);
                }

                TcpDiscoveryNode leftNode = ring.removeNode(leavingNodeId);

                interruptPing(leavingNode);

                assert leftNode != null : msg;

                if (log.isDebugEnabled())
                    log.debug("Removed node from topology: " + leftNode);

                long topVer;

                if (locNodeCoord) {
                    topVer = ring.incrementTopologyVersion();

                    msg.topologyVersion(topVer);
                }
                else {
                    topVer = msg.topologyVersion();

                    assert topVer > 0 : "Topology version is empty for message: " + msg;

                    boolean b = ring.topologyVersion(topVer);

                    assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                        ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                if (msg.client()) {
                    ClientMessageWorker wrk = clientMsgWorkers.remove(leavingNodeId);

                    if (wrk != null)
                        wrk.addMessage(msg);
                }
                else if (leftNode.equals(next) && sock != null) {
                    try {
                        spi.writeToSocket(sock, out, msg, spi.failureDetectionTimeoutEnabled() ?
                            spi.failureDetectionTimeout() : spi.getSocketTimeout());

                        if (log.isDebugEnabled())
                            log.debug("Sent verified node left message to leaving node: " + msg);
                    }
                    catch (IgniteCheckedException | IOException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send verified node left message to leaving node [msg=" + msg +
                                ", err=" + e.getMessage() + ']');

                        onException("Failed to send verified node left message to leaving node [msg=" + msg +
                            ", err=" + e.getMessage() + ']', e);
                    }
                    finally {
                        forceSndPending = true;

                        next = null;

                        U.closeQuiet(sock);
                    }
                }

                synchronized (mux) {
                    joiningNodes.remove(leftNode.id());
                }

                spi.stats.onNodeLeft();

                boolean notified = notifyDiscovery(EVT_NODE_LEFT, topVer, leftNode, msg.spanContainer());

                notifiedDiscovery.set(notified);

                synchronized (mux) {
                    failedNodes.remove(leftNode);

                    leavingNodes.remove(leftNode);

                    failedNodesMsgSent.remove(leftNode.id());
                }
            }

            if (sendMessageToRemotes(msg)) {
                try {
                    sendMessageAcrossRing(msg);
                }
                finally {
                    forceSndPending = false;
                }
            }
            else {
                forceSndPending = false;

                if (log.isDebugEnabled())
                    log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

                U.closeQuiet(sock);

                processPendingMessagesLocally(msg);
            }

            checkPendingCustomMessages();
        }

        /**
         * @param msg Message to send.
         * @return {@code True} if message should be send across the ring.
         */
        private boolean sendMessageToRemotes(TcpDiscoveryAbstractMessage msg) {
            if (ring.hasRemoteNodes())
                return true;

            sendMessageToClients(msg);

            return false;
        }

        /**
         * Processes node failed message.
         *
         * @param msg Node failed message.
         */
        private void processNodeFailedMessage(TcpDiscoveryNodeFailedMessage msg) {
            assert msg != null;

            UUID sndId = msg.senderNodeId();

            if (sndId != null) {
                TcpDiscoveryNode sndNode = ring.node(sndId);

                if (sndNode == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message sent from unknown node: " + msg);

                    return;
                }
                else {
                    boolean contains;

                    UUID creatorId = msg.creatorNodeId();

                    assert creatorId != null : msg;

                    synchronized (mux) {
                        contains = failedNodes.containsKey(sndNode) || ring.node(creatorId) == null;
                    }

                    if (contains) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node failed message sent from node which is about to fail: " + msg);

                        return;
                    }
                }
            }

            UUID failedNodeId = msg.failedNodeId();

            TcpDiscoveryNode failedNode = ring.node(failedNodeId);

            if (failedNode != null && failedNode.internalOrder() != msg.internalOrder()) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring node failed message since node internal order does not match " +
                        "[msg=" + msg + ", node=" + failedNode + ']');

                return;
            }

            if (failedNode != null) {
                assert !failedNode.isLocal() || !msg.verified() : msg;

                boolean skipUpdateFailedNodes = msg.force() && !msg.verified();

                if (!skipUpdateFailedNodes) {
                    synchronized (mux) {
                        if (!failedNodes.containsKey(failedNode))
                            failedNodes.put(failedNode, msg.senderNodeId() != null ? msg.senderNodeId() : getLocalNodeId());
                    }
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Discarding node failed message since node was not found: " + msg);

                return;
            }

            boolean locNodeCoord = isLocalNodeCoordinator();

            UUID locNodeId = getLocalNodeId();

            if (locNodeCoord) {
                if (msg.verified()) {
                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }
                else if (locNodeId.equals(failedNodeId)) {
                    segmentLocalNodeOnSendFail(null);

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified()) {
                //we will need to recalculate this value since the topology changed
                if (!nodeCompactRepresentationSupported) {
                    nodeCompactRepresentationSupported =
                        allNodesSupport(TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION);
                }

                failedNode = ring.removeNode(failedNodeId);

                interruptPing(failedNode);

                assert failedNode != null;

                long topVer;

                if (locNodeCoord) {
                    topVer = ring.incrementTopologyVersion();

                    msg.topologyVersion(topVer);
                }
                else {
                    topVer = msg.topologyVersion();

                    assert topVer > 0 : "Topology version is empty for message: " + msg;

                    boolean b = ring.topologyVersion(topVer);

                    assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                        ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                synchronized (mux) {
                    failedNodes.remove(failedNode);

                    leavingNodes.remove(failedNode);

                    failedNodesMsgSent.remove(failedNode.id());

                    if (!msg.force()) { // ClientMessageWorker will stop after sending force fail message.
                        ClientMessageWorker worker = clientMsgWorkers.remove(failedNode.id());

                        if (worker != null && worker.runner() != null)
                            worker.runner().interrupt();
                    }
                }

                if (msg.warning() != null && !msg.creatorNodeId().equals(getLocalNodeId())) {
                    ClusterNode creatorNode = ring.node(msg.creatorNodeId());

                    U.warn(log, "Received EVT_NODE_FAILED event with warning [" +
                        "nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : msg.creatorNodeId()) +
                        ", msg=" + msg.warning() + ']');
                }

                synchronized (mux) {
                    joiningNodes.remove(failedNode.id());
                }

                boolean notified = notifyDiscovery(EVT_NODE_FAILED, topVer, failedNode, msg.spanContainer());

                notifiedDiscovery.set(notified);

                spi.stats.onNodeFailed();
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
            else {
                if (log.isDebugEnabled())
                    log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

                U.closeQuiet(sock);

                processPendingMessagesLocally(msg);
            }

            checkPendingCustomMessages();
        }

        /**
         * Processes status check message.
         *
         * @param msg Status check message.
         */
        private void processStatusCheckMessage(final TcpDiscoveryStatusCheckMessage msg) {
            assert msg != null;

            UUID locNodeId = getLocalNodeId();

            if (msg.failedNodeId() != null) {
                if (locNodeId.equals(msg.failedNodeId())) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (suspect node is local node).");

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (local node is the sender of the status message).");

                    return;
                }

                if (isLocalNodeCoordinator() && ring.node(msg.creatorNodeId()) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (creator node is not in topology).");

                    return;
                }
            }
            else {
                if (isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
                    // Local node is real coordinator, it should respond and discard message.
                    if (ring.node(msg.creatorNodeId()) != null) {
                        // Sender is in topology, send message via ring.
                        msg.status(STATUS_OK);

                        sendMessageAcrossRing(msg);
                    }
                    else {
                        // Sender is not in topology, it should reconnect.
                        msg.status(STATUS_RECON);

                        utilityPool.execute(new Runnable() {
                            @Override public void run() {
                                synchronized (mux) {
                                    if (spiState == DISCONNECTED) {
                                        if (log.isDebugEnabled())
                                            log.debug("Ignoring status check request, SPI is already disconnected: " + msg);

                                        return;
                                    }
                                }

                                TcpDiscoveryStatusCheckMessage msg0 = msg;

                                if (F.contains(msg.failedNodes(), msg.creatorNodeId())) {
                                    msg0 = createTcpDiscoveryStatusCheckMessage(
                                        msg.creatorNode(),
                                        msg.creatorNodeId(),
                                        msg.failedNodeId());

                                    if (msg0 == null) {
                                        log.debug("Status check message discarded (creator node is not in topology).");

                                        return;
                                    }

                                    msg0.failedNodes(null);

                                    for (UUID failedNodeId : msg.failedNodes()) {
                                        if (!failedNodeId.equals(msg.creatorNodeId()))
                                            msg0.addFailedNode(failedNodeId);
                                    }
                                }

                                try {
                                    trySendMessageDirectly(msg0.creatorNodeAddrs(), msg0.creatorNodeId(), msg0);

                                    if (log.isDebugEnabled())
                                        log.debug("Responded to status check message " +
                                            "[recipient=" + msg0.creatorNodeId() + ", status=" + msg0.status() + ']');
                                }
                                catch (IgniteSpiException e) {
                                    if (e.hasCause(SocketException.class)) {
                                        if (log.isDebugEnabled())
                                            log.debug("Failed to respond to status check message (connection " +
                                                "refused) [recipient=" + msg0.creatorNodeId() + ", status=" +
                                                msg0.status() + ']');

                                        onException("Failed to respond to status check message (connection refused) " +
                                            "[recipient=" + msg0.creatorNodeId() + ", status=" + msg0.status() + ']', e);
                                    }
                                    else if (!spi.isNodeStopping0()) {
                                        if (pingNode(msg0.creatorNodeId())) {
                                            // Node exists and accepts incoming connections.
                                            U.error(log, "Failed to respond to status check message [recipient=" +
                                                msg0.creatorNodeId() + ", status=" + msg0.status() + ']', e);
                                        }
                                        else if (log.isDebugEnabled()) {
                                            log.debug("Failed to respond to status check message (did the node stop?)" +
                                                "[recipient=" + msg0.creatorNodeId() +
                                                ", status=" + msg0.status() + ']');
                                        }
                                    }
                                }
                            }
                        });
                    }

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null &&
                    U.millisSinceNanos(locNode.lastUpdateTimeNanos()) < spi.metricsUpdateFreq) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (local node receives updates).");

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null &&
                    spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (local node is not connected to topology).");

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                    if (spiStateCopy() != CONNECTED)
                        return;

                    if (msg.status() == STATUS_OK) {
                        if (log.isDebugEnabled())
                            log.debug("Received OK status response from coordinator: " + msg);
                    }
                    else if (msg.status() == STATUS_RECON) {
                        U.warn(log, "Node is out of topology (probably, due to short-time network problems).");

                        notifyDiscovery(EVT_NODE_SEGMENTED, ring.topologyVersion(), locNode);

                        return;
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Status value was not updated in status response: " + msg);

                    // Discard the message.
                    return;
                }
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes regular metrics update message. If a more recent message of the same kind has been received,
         * then it will be processed instead of the one taken from the queue.
         *
         * @param msg Metrics update message.
         */
        private void processMetricsUpdateMessage(TcpDiscoveryMetricsUpdateMessage msg) {
            assert msg != null;

            assert !msg.client();

            int laps = metricsMsgFilter.passedLaps(msg);

            msg = metricsMsgFilter.pollActualMessage(laps, msg);

            UUID locNodeId = getLocalNodeId();

            if (ring.node(msg.creatorNodeId()) == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding metrics update message issued by unknown node [msg=" + msg +
                        ", ring=" + ring + ']');

                return;
            }

            if (isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Discarding metrics update message issued by non-coordinator node: " + msg);

                return;
            }

            if (!isLocalNodeCoordinator() && locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Discarding metrics update message issued by local node (node is no more coordinator): " +
                        msg);

                return;
            }

            if (laps == 2) {
                if (log.isTraceEnabled())
                    log.trace("Discarding metrics update message that has made two passes: " + msg);

                return;
            }

            long tsNanos = System.nanoTime();

            if (spiStateCopy() == CONNECTED && msg.hasMetrics())
                processMsgCacheMetrics(msg, tsNanos);

            if (sendMessageToRemotes(msg)) {
                if (laps == 0 && spiStateCopy() == CONNECTED) {
                    // Message is on its first ring or just created on coordinator.
                    msg.setMetrics(locNodeId, spi.metricsProvider.metrics());
                    msg.setCacheMetrics(locNodeId, spi.metricsProvider.cacheMetrics());

                    for (Map.Entry<UUID, ClientMessageWorker> e : clientMsgWorkers.entrySet()) {
                        UUID nodeId = e.getKey();
                        ClusterMetrics metrics = e.getValue().metrics();

                        if (metrics != null)
                            msg.setClientMetrics(locNodeId, nodeId, metrics);

                        msg.addClientNodeId(nodeId);
                    }
                }
                else {
                    // Message is on its second ring.
                    removeMetrics(msg, locNodeId);

                    Collection<UUID> clientNodeIds = msg.clientNodeIds();

                    for (TcpDiscoveryNode clientNode : ring.clientNodes()) {
                        if (clientNode.visible()) {
                            if (clientNodeIds.contains(clientNode.id()))
                                clientNode.clientAliveTime(spi.clientFailureDetectionTimeout());
                            else {
                                if (!clientNode.clientAliveTimeSet())
                                    clientNode.clientAliveTime(spi.clientFailureDetectionTimeout());

                                boolean aliveCheck = clientNode.isClientAlive();

                                if (!aliveCheck && isLocalNodeCoordinator()) {
                                    boolean failedNode;

                                    synchronized (mux) {
                                        failedNode = failedNodes.containsKey(clientNode);
                                    }

                                    if (!failedNode) {
                                        U.warn(log, "Failing client node due to not receiving metrics updates " +
                                            "from client node within " +
                                            "'IgniteConfiguration.clientFailureDetectionTimeout' " +
                                            "(consider increasing configuration property) " +
                                            "[timeout=" + spi.clientFailureDetectionTimeout() + ", node=" + clientNode + ']');

                                        TcpDiscoveryNodeFailedMessage nodeFailedMsg = new TcpDiscoveryNodeFailedMessage(
                                            locNodeId, clientNode.id(), clientNode.internalOrder());

                                        processNodeFailedMessage(nodeFailedMsg);
                                    }
                                }
                            }
                        }
                    }
                }

                if (sendMessageToRemotes(msg))
                    sendMessageAcrossRing(msg);
            }
            else {
                locNode.lastUpdateTimeNanos(tsNanos);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), locNode);
            }
        }

        /**
         * Processes discard message and discards previously registered pending messages.
         *
         * @param msg Discard message.
         */
        private void processDiscardMessage(TcpDiscoveryDiscardMessage msg) {
            assert msg != null;

            IgniteUuid msgId = msg.msgId();

            assert msgId != null;

            if (isLocalNodeCoordinator()) {
                if (!getLocalNodeId().equals(msg.verifierNodeId()))
                    // Message is not verified or verified by former coordinator.
                    msg.verify(getLocalNodeId());
                else
                    // Discard the message.
                    return;
            }

            if (msg.verified())
                pendingMsgs.discard(msgId, msg.customMessageDiscard());

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * @param msg Message.
         */
        private void processClientPingRequest(final TcpDiscoveryClientPingRequest msg) {
            utilityPool.execute(new Runnable() {
                @Override public void run() {
                    synchronized (mux) {
                        if (spiState == DISCONNECTED) {
                            if (log.isDebugEnabled())
                                log.debug("Ignoring ping request, SPI is already disconnected: " + msg);

                            return;
                        }
                    }

                    final ClientMessageWorker worker = clientMsgWorkers.get(msg.creatorNodeId());

                    if (worker == null) {
                        if (log.isDebugEnabled())
                            log.debug("Ping request from dead client node, will be skipped: " + msg.creatorNodeId());
                    }
                    else {
                        boolean res;

                        try {
                            res = pingNode(msg.nodeToPing());
                        } catch (IgniteSpiException e) {
                            log.error("Failed to ping node [nodeToPing=" + msg.nodeToPing() + ']', e);

                            res = false;
                        }

                        TcpDiscoveryClientPingResponse pingRes = new TcpDiscoveryClientPingResponse(
                            getLocalNodeId(), msg.nodeToPing(), res);

                        pingRes.verify(getLocalNodeId());

                        worker.addMessage(pingRes);
                    }
                }
            });
        }

        /**
         * @param msg Message.
         * @param waitForNotification If {@code true} then thread will wait when discovery event notification has finished.
         */
        private void processCustomMessage(TcpDiscoveryCustomEventMessage msg, boolean waitForNotification) {
            if (isLocalNodeCoordinator()) {
                if (posponeUndeliveredMessages(msg))
                    return;

                if (!msg.verified()) {
                    msg.verify(getLocalNodeId());

                    msg.spanContainer().span()
                        .addLog(() -> "Verified");

                    msg.topologyVersion(ring.topologyVersion());

                    if (pendingMsgs.procCustomMsgs.add(msg.id())) {
                        notifyDiscoveryListener(msg, waitForNotification);

                        if (sendMessageToRemotes(msg))
                            sendMessageAcrossRing(msg);
                        else {
                            registerPendingMessage(msg);

                            processCustomMessage(msg, waitForNotification);
                        }
                    }

                    msg.message(null, msg.messageBytes());
                }
                else {
                    addMessage(new TcpDiscoveryDiscardMessage(getLocalNodeId(), msg.id(), true));

                    DiscoverySpiCustomMessage msgObj = null;

                    try {
                        msgObj = msg.message(spi.marshaller(), U.resolveClassLoader(spi.ignite().configuration()));
                    }
                    catch (Throwable e) {
                        U.error(log, "Failed to unmarshal discovery custom message.", e);
                    }

                    if (msgObj != null) {
                        DiscoverySpiCustomMessage nextMsg = msgObj.ackMessage();

                        if (nextMsg != null) {
                            try {
                                TcpDiscoveryCustomEventMessage ackMsg = new TcpDiscoveryCustomEventMessage(
                                    getLocalNodeId(), nextMsg, U.marshal(spi.marshaller(), nextMsg));

                                ackMsg.topologyVersion(msg.topologyVersion());

                                processCustomMessage(ackMsg, waitForNotification);
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to marshal discovery custom message.", e);
                            }
                        }
                    }
                }
            }
            else {
                TcpDiscoverySpiState state0;

                synchronized (mux) {
                    state0 = spiState;
                }

                if (msg.verified() && msg.topologyVersion() != ring.topologyVersion()) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding custom event message [msg=" + msg + ", ring=" + ring + ']');

                    return;
                }

                if (msg.verified() && state0 == CONNECTED && pendingMsgs.procCustomMsgs.add(msg.id())) {
                    assert msg.topologyVersion() == ring.topologyVersion() :
                        "msg: " + msg + ", topVer=" + ring.topologyVersion();

                    notifyDiscoveryListener(msg, waitForNotification);
                }

                // Clear msg field to prevent possible memory leak.
                msg.message(null, msg.messageBytes());

                if (sendMessageToRemotes(msg))
                    sendMessageAcrossRing(msg);
            }
        }

        /**
         * If new node is in the progress of being added we must store and resend undelivered messages.
         *
         * @param msg Processed message.
         * @return {@code true} If message was appended to pending queue.
         */
        private boolean posponeUndeliveredMessages(final TcpDiscoveryCustomEventMessage msg) {
            boolean joiningEmpty;

            synchronized (mux) {
                joiningEmpty = joiningNodes.isEmpty();

                if (log.isDebugEnabled())
                    log.debug("Delay custom message processing, there are joining nodes [msg=" + msg +
                        ", joiningNodes=" + joiningNodes + ']');
            }

            boolean delayMsg = msg.topologyVersion() == 0L && !joiningEmpty;

            if (delayMsg) {
                synchronized (mux) {
                    pendingCustomMsgs.add(msg);
                }

                return true;
            }

            return false;
        }

        /**
         * Checks failed nodes list and sends {@link TcpDiscoveryNodeFailedMessage} if failed node is still in the
         * ring and node detected failure left ring.
         */
        private void checkFailedNodesList() {
            List<TcpDiscoveryNodeFailedMessage> msgs = null;

            synchronized (mux) {
                if (!failedNodes.isEmpty()) {
                    for (Iterator<Map.Entry<TcpDiscoveryNode, UUID>> it = failedNodes.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<TcpDiscoveryNode, UUID> e = it.next();

                        TcpDiscoveryNode node = e.getKey();
                        UUID failSndNode = e.getValue();

                        if (ring.node(node.id()) == null) {
                            it.remove();

                            continue;
                        }

                        if (!nodeAlive(failSndNode) && !failedNodesMsgSent.contains(node.id())) {
                            if (msgs == null)
                                msgs = new ArrayList<>();

                            msgs.add(new TcpDiscoveryNodeFailedMessage(getLocalNodeId(), node.id(), node.internalOrder()));

                            failedNodesMsgSent.add(node.id());
                        }
                    }
                }

                if (!failedNodesMsgSent.isEmpty()) {
                    for (Iterator<UUID> it = failedNodesMsgSent.iterator(); it.hasNext(); ) {
                        UUID nodeId = it.next();

                        if (ring.node(nodeId) == null)
                            it.remove();
                    }
                }
            }

            if (msgs != null) {
                for (TcpDiscoveryNodeFailedMessage msg : msgs) {
                    U.warn(log, "Added node failed message for node from failed nodes list: " + msg);

                    addMessage(msg);
                }
            }
        }

        /**
         * Checks and flushes custom event messages if no nodes are attempting to join the grid.
         */
        private void checkPendingCustomMessages() {
            boolean joiningEmpty;

            synchronized (mux) {
                joiningEmpty = joiningNodes.isEmpty();
            }

            if (joiningEmpty && isLocalNodeCoordinator()) {
                TcpDiscoveryCustomEventMessage msg;

                while ((msg = pollPendingCustomMessage()) != null)
                    processCustomMessage(msg, true);
            }
        }

        /**
         * @return Pending custom message.
         */
        @Nullable private TcpDiscoveryCustomEventMessage pollPendingCustomMessage() {
            synchronized (mux) {
                return pendingCustomMsgs.poll();
            }
        }

        /**
         * @param msg Custom message.
         * @param waitForNotification If {@code true} thread will wait when discovery event notification has finished.
         */
        private void notifyDiscoveryListener(TcpDiscoveryCustomEventMessage msg, boolean waitForNotification) {
            DiscoverySpiListener lsnr = spi.lsnr;

            TcpDiscoverySpiState spiState = spiStateCopy();

            Map<Long, Collection<ClusterNode>> hist;

            synchronized (mux) {
                hist = new TreeMap<>(topHist);
            }

            Collection<ClusterNode> snapshot = hist.get(msg.topologyVersion());

            if (lsnr != null && (spiState == CONNECTED || spiState == DISCONNECTING)) {
                TcpDiscoveryNode node = ring.node(msg.creatorNodeId());

                if (node == null)
                    return;

                DiscoverySpiCustomMessage msgObj;

                try {
                    msgObj = msg.message(spi.marshaller(), U.resolveClassLoader(spi.ignite().configuration()));
                }
                catch (Throwable t) {
                    throw new IgniteException("Failed to unmarshal discovery custom message: " + msg, t);
                }

                IgniteFuture<?> fut = lsnr.onDiscovery(
                    new DiscoveryNotification(
                        DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
                        msg.topologyVersion(),
                        node,
                        snapshot,
                        hist,
                        msgObj,
                        msg.spanContainer())
                    );

                notifiedDiscovery.set(true);

                if (waitForNotification || msgObj.isMutable()) {
                    blockingSectionBegin();

                    try {
                        fut.get();
                    }
                    finally {
                        blockingSectionEnd();
                    }
                }

                if (msgObj.isMutable()) {
                    try {
                        msg.message(msgObj, U.marshal(spi.marshaller(), msgObj));
                    }
                    catch (Throwable t) {
                        throw new IgniteException("Failed to marshal mutable discovery message: " + msgObj, t);
                    }
                }
            }
        }

        /**
         * Sends metrics update message if needed.
         */
        private void sendMetricsUpdateMessage() {
            long elapsed = spi.metricsUpdateFreq - U.millisSinceNanos(lastTimeMetricsUpdateMsgSentNanos);

            if (elapsed > 0 || !isLocalNodeCoordinator())
                return;

            TcpDiscoveryMetricsUpdateMessage msg = new TcpDiscoveryMetricsUpdateMessage(getConfiguredNodeId());

            msg.verify(getLocalNodeId());

            msgWorker.addMessage(msg);

            lastTimeMetricsUpdateMsgSentNanos = System.nanoTime();
        }

        /**
         * Checks the last time a metrics update message received. If the time is bigger than {@code metricsCheckFreq}
         * than {@link TcpDiscoveryStatusCheckMessage} is sent across the ring.
         */
        private void checkMetricsReceiving() {
            if (lastTimeStatusMsgSentNanos < locNode.lastUpdateTimeNanos())
                lastTimeStatusMsgSentNanos = locNode.lastUpdateTimeNanos();

            long updateTimeNanos = Math.max(lastTimeStatusMsgSentNanos, lastRingMsgTimeNanos);

            if (U.millisSinceNanos(updateTimeNanos) < metricsCheckFreq)
                return;

            msgWorker.addMessage(createTcpDiscoveryStatusCheckMessage(locNode, locNode.id(), null));

            lastTimeStatusMsgSentNanos = System.nanoTime();
        }

        /**
         * Check connection to next node in the ring.
         */
        private void checkConnection() {
            long elapsed = (lastRingMsgSentTime + U.millisToNanos(connCheckInterval)) - System.nanoTime();

            if (elapsed > 0)
                return;

            if (ring.hasRemoteServerNodes())
                sendMessageAcrossRing(new TcpDiscoveryConnectionCheckMessage(locNode));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("%s, nextNode=[%s]", super.toString(), next);
        }
    }

    /**
     * Creates proper timeout helper taking in account current send state and ring state.
     *
     * @param sndState Current connection recovering state. Ignored if {@code null}.
     * @param lastOperationNanos Time of last related operation. Ignored if negative or 0.
     * @return Timeout helper.
     */
    private IgniteSpiOperationTimeoutHelper serverOperationTimeoutHelper(@Nullable CrossRingMessageSendState sndState,
        long lastOperationNanos) {
        long absoluteThreshold = -1;

        // Active send-state means we lost connection to next node and have to find another. We don't know how many
        // nodes failed. May be several failed in a row. But we got only one connectionRecoveryTimeout to establish new
        // connection. We should travers rest of the cluster with sliced timeout for each node.
        if (sndState != null)
            absoluteThreshold = Math.min(sndState.failTimeNanos, System.nanoTime() + U.millisToNanos(connCheckTick));

        return new IgniteSpiOperationTimeoutHelper(spi, true, lastOperationNanos, absoluteThreshold);
    }

    /** Fixates time of last sent message. */
    private void updateLastSentMessageTime() {
        lastRingMsgSentTime = System.nanoTime();
    }

    /** Thread that executes {@link TcpServer}'s code. */
    private class TcpServerThread extends IgniteSpiThread {
        /** */
        private final TcpServer worker;

        /**
         * @param worker Worker to be executed by this thread.
         * @param log Logger.
         */
        private TcpServerThread(TcpServer worker, IgniteLogger log) {
            super(worker.igniteInstanceName(), worker.name(), log);

            setPriority(spi.threadPri);

            this.worker = worker;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            worker.run();
        }
    }

    /**
     * Worker that accepts incoming TCP connections.
     * <p>
     * Tcp server will call provided closure when accepts incoming connection.
     * From that moment server is no more responsible for the socket.
     */
    private class TcpServer extends GridWorker {
        /** Socket TCP server listens to. */
        private ServerSocket srvrSock;

        /** Port to listen. */
        private int port;

        /**
         * @param log Logger.
         * @throws IgniteSpiException In case of error.
         */
        TcpServer(IgniteLogger log) throws IgniteSpiException {
            super(spi.ignite().name(), "tcp-disco-srvr-[]", log, getWorkerRegistry(spi));

            int lastPort = spi.locPortRange == 0 ? spi.locPort : spi.locPort + spi.locPortRange - 1;

            for (port = spi.locPort; port <= lastPort; port++) {
                try {
                    if (spi.isSslEnabled()) {
                        SSLServerSocket sslSock = (SSLServerSocket)spi.sslSrvSockFactory
                            .createServerSocket(port, 0, spi.locHost);

                        sslSock.setNeedClientAuth(true);

                        srvrSock = sslSock;
                    }
                    else
                        srvrSock = new ServerSocket(port, 0, spi.locHost);

                    if (log.isInfoEnabled()) {
                        log.info("Successfully bound to TCP port [port=" + port +
                            ", localHost=" + spi.locHost +
                            ", locNodeId=" + spi.ignite().configuration().getNodeId() +
                            ']');
                    }

                    return;
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to bind to local port (will try next port within range) " +
                            "[port=" + port + ", localHost=" + spi.locHost + ']');

                    onException("Failed to bind to local port. " +
                        "[port=" + port + ", localHost=" + spi.locHost + ']', e);
                }
            }

            // If free port wasn't found.
            throw new IgniteSpiException("Failed to bind TCP server socket (possibly all ports in range " +
                "are in use) [firstPort=" + spi.locPort + ", lastPort=" + lastPort +
                ", addr=" + spi.locHost + ']');
        }

        /** */
        @Override protected void body() {
            Throwable err = null;

            try {
                U.enhanceThreadName(":" + port);

                while (!isCancelled()) {
                    Socket sock;

                    blockingSectionBegin();

                    try {
                        sock = srvrSock.accept();
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    long tsNanos = System.nanoTime();

                    if (log.isInfoEnabled())
                        log.info("TCP discovery accepted incoming connection " +
                            "[rmtAddr=" + sock.getInetAddress() + ", rmtPort=" + sock.getPort() + ']');

                    SocketReader reader = new SocketReader(sock);

                    synchronized (mux) {
                        readers.add(reader);
                    }

                    if (log.isInfoEnabled())
                        log.info("TCP discovery spawning a new thread for connection " +
                            "[rmtAddr=" + sock.getInetAddress() + ", rmtPort=" + sock.getPort() + ']');

                    reader.start();

                    onIdle();
                }
            }
            catch (IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Failed to accept TCP connection.", e);

                onException("Failed to accept TCP connection.", e);

                if (!runner().isInterrupted()) {
                    err = e;

                    if (U.isMacInvalidArgumentError(e))
                        U.error(log, "Failed to accept TCP connection\n\t" + U.MAC_INVALID_ARG_MSG, e);
                    else
                        U.error(log, "Failed to accept TCP connection.", e);
                }
            }
            catch (Throwable t) {
                err = t;

                throw t;
            }
            finally {
                if (spi.ignite() instanceof IgniteEx) {
                    if (err == null && !spi.isNodeStopping0() && spiStateCopy() != DISCONNECTING)
                        err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly.");

                    FailureProcessor failure = ((IgniteEx)spi.ignite()).context().failure();

                    if (err instanceof OutOfMemoryError)
                        failure.process(new FailureContext(CRITICAL_ERROR, err));
                    else if (err != null)
                        failure.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
                }

                U.closeQuiet(srvrSock);
            }
        }

        /** */
        public void stop() {
            cancel();

            U.close(srvrSock, log);

            U.join(TcpServer.this, log);
        }
    }

    /**
     * Thread that reads messages from the socket created for incoming connections.
     */
    private class SocketReader extends IgniteSpiThread {
        /** Socket to read data from. */
        private final Socket sock;

        /** */
        private volatile UUID nodeId;

        /**
         * Constructor.
         *
         * @param sock Socket to read data from.
         */
        SocketReader(Socket sock) {
            super(spi.ignite().name(), "tcp-disco-sock-reader-[]", log);

            this.sock = sock;

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            UUID locNodeId = getConfiguredNodeId();

            ClientMessageWorker clientMsgWrk = null;

            SocketAddress rmtAddr = sock.getRemoteSocketAddress();

            if (log.isInfoEnabled())
                log.info("Started serving remote node connection [rmtAddr=" + rmtAddr +
                    ", rmtPort=" + sock.getPort() + ']');

            boolean srvSock;

            try {
                InputStream in;

                try {
                    // Set socket options.
                    spi.configureSocketOptions(sock);

                    int timeout = sock.getSoTimeout();

                    sock.setSoTimeout((int)spi.netTimeout);

                    for (IgniteInClosure<Socket> connLsnr : spi.incomeConnLsnrs)
                        connLsnr.apply(sock);

                    int rcvBufSize = sock.getReceiveBufferSize();

                    in = new BufferedInputStream(sock.getInputStream(), rcvBufSize > 0 ? rcvBufSize : 8192);

                    byte[] buf = new byte[4];
                    int read = 0;

                    while (read < buf.length) {
                        int r = in.read(buf, read, buf.length - read);

                        if (r >= 0)
                            read += r;
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failed to read magic header (too few bytes received) " +
                                    "[rmtAddr=" + rmtAddr +
                                    ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            LT.warn(log, "Failed to read magic header (too few bytes received) [rmtAddr=" +
                                rmtAddr + ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            return;
                        }
                    }

                    if (!Arrays.equals(buf, U.IGNITE_HEADER)) {
                        if (log.isDebugEnabled())
                            log.debug("Unknown connection detected (is some other software connecting to " +
                                "this Ignite port?" +
                                (!spi.isSslEnabled() ? " missed SSL configuration?" : "" ) +
                                ") [rmtAddr=" + rmtAddr + ", locAddr=" + sock.getLocalSocketAddress() + ']');

                        LT.warn(log, "Unknown connection detected (is some other software connecting to " +
                            "this Ignite port?" +
                            (!spi.isSslEnabled() ? " missing SSL configuration on remote node?" : "" ) +
                            ") [rmtAddr=" + sock.getInetAddress() + ']', true);

                        return;
                    }

                    // Restore timeout.
                    sock.setSoTimeout(timeout);

                    TcpDiscoveryAbstractMessage msg = spi.readMessage(sock, in, spi.netTimeout);

                    // Ping.
                    if (msg instanceof TcpDiscoveryPingRequest) {
                        if (!spi.isNodeStopping0()) {
                            TcpDiscoveryPingRequest req = (TcpDiscoveryPingRequest)msg;

                            if (log.isInfoEnabled())
                                log.info("Received ping request from the remote node " +
                                    "[rmtNodeId=" + msg.creatorNodeId() +
                                    ", rmtAddr=" + rmtAddr + ", rmtPort=" + sock.getPort() + "]");

                            TcpDiscoveryPingResponse res = new TcpDiscoveryPingResponse(locNodeId);

                            IgniteSpiOperationTimeoutHelper timeoutHelper =
                                new IgniteSpiOperationTimeoutHelper(spi, true);

                            if (req.clientNodeId() != null) {
                                ClientMessageWorker clientWorker = clientMsgWorkers.get(req.clientNodeId());

                                if (clientWorker != null)
                                    res.clientExists(clientWorker.ping(timeoutHelper));
                            }

                            spi.writeToSocket(sock, res, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                            if (!(sock instanceof SSLSocket))
                                sock.shutdownOutput();

                            if (log.isInfoEnabled())
                                log.info("Finished writing ping response " + "[rmtNodeId=" + msg.creatorNodeId() +
                                    ", rmtAddr=" + rmtAddr + ", rmtPort=" + sock.getPort() + "]");
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Ignore ping request, node is stopping.");

                        return;
                    }

                    // Handshake.
                    TcpDiscoveryHandshakeRequest req = (TcpDiscoveryHandshakeRequest)msg;

                    srvSock = !req.client();

                    UUID nodeId = req.creatorNodeId();

                    this.nodeId = nodeId;

                    U.enhanceThreadName(U.id8(nodeId) + ' ' + sock.getInetAddress().getHostAddress()
                        + ":" + sock.getPort() + (req.client() ? " client" : ""));

                    TcpDiscoveryHandshakeResponse res =
                        new TcpDiscoveryHandshakeResponse(locNodeId, locNode.internalOrder());

                    res.setDiscoveryDataPacketCompression(allNodesSupport(IgniteFeatures.DATA_PACKET_COMPRESSION));

                    if (req.client())
                        res.clientAck(true);
                    else if (req.changeTopology()) {
                        // Node cannot connect to it's next (for local node it's previous).
                        // Need to check connectivity to it.
                        long rcvdTime = lastRingMsgReceivedTime;
                        long now = System.nanoTime();
                        long timeThreshold = rcvdTime + U.millisToNanos(effectiveExchangeTimeout());

                        // We got message from previous in less than effective exchange timeout.
                        boolean ok = timeThreshold > now;
                        TcpDiscoveryNode previous = null;

                        if (ok) {
                            // Check case when previous node suddenly died. This will speed up
                            // node failing.
                            Set<TcpDiscoveryNode> failed;

                            synchronized (mux) {
                                failed = failedNodes.keySet();
                            }

                            previous = ring.previousNode(failed);

                            InetSocketAddress liveAddr = null;

                            if (previous != null && !previous.id().equals(nodeId) &&
                                (req.checkPreviousNodeId() == null || previous.id().equals(req.checkPreviousNodeId()))) {
                                Collection<InetSocketAddress> nodeAddrs = spi.getNodeAddresses(previous, false);

                                // The connection recovery connection to one node is connCheckTick.
                                // We need to suppose network delays. So we use half of this time.
                                int backwardCheckTimeout = (int)(connCheckTick / 2);

                                if (log.isDebugEnabled()) {
                                    log.debug("Remote node requests topology change. Checking connection to " +
                                        "previous [" + previous + "] with timeout " + backwardCheckTimeout);
                                }

                                liveAddr = checkConnection(new ArrayList<>(nodeAddrs), backwardCheckTimeout);

                                if (log.isInfoEnabled()) {
                                    log.info("Connection check to previous node done: [liveAddr=" + liveAddr
                                        + ", previousNode=" + U.toShortString(previous) + ", addressesToCheck=" +
                                        nodeAddrs + ", connectingNodeId=" + nodeId + ']');
                                }
                            }

                            // If local node was able to connect to previous, confirm that it's alive.
                            ok = liveAddr != null && (!liveAddr.getAddress().isLoopbackAddress()
                                || !locNode.socketAddresses().contains(liveAddr));
                        }

                        res.previousNodeAlive(ok);

                        if (log.isInfoEnabled()) {
                            log.info("Previous node alive status [alive=" + ok +
                                ", checkPreviousNodeId=" + req.checkPreviousNodeId() +
                                ", actualPreviousNode=" + previous +
                                ", lastMessageReceivedTime=" + rcvdTime + ", now=" + now +
                                ", connCheckInterval=" + connCheckInterval + ']');
                        }
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Sending handshake response [" + res + "] with timeout " +
                            spi.getEffectiveSocketTimeout(srvSock) + " to " + rmtAddr + ":" + sock.getPort());
                    }

                    spi.writeToSocket(sock, res, spi.getEffectiveSocketTimeout(srvSock));

                    // It can happen if a remote node is stopped and it has a loopback address in the list of addresses,
                    // the local node sends a handshake request message on the loopback address, so we get here.
                    if (locNodeId.equals(nodeId)) {
                        assert !req.client();

                        if (log.isDebugEnabled())
                            log.debug("Handshake request from local node: " + req);

                        return;
                    }

                    if (req.client()) {
                        ClientMessageWorker clientMsgWrk0 = new ClientMessageWorker(sock, nodeId, log);

                        while (true) {
                            ClientMessageWorker old = clientMsgWorkers.putIfAbsent(nodeId, clientMsgWrk0);

                            if (old == null)
                                break;

                            if (old.isDone() || (old.runner() != null && old.runner().isInterrupted())) {
                                clientMsgWorkers.remove(nodeId, old);

                                continue;
                            }

                            if (old.runner() != null)
                                old.runner().join(500);

                            old = clientMsgWorkers.putIfAbsent(nodeId, clientMsgWrk0);

                            if (old == null)
                                break;

                            if (log.isDebugEnabled())
                                log.debug("Already have client message worker, closing connection " +
                                    "[locNodeId=" + locNodeId +
                                    ", rmtNodeId=" + nodeId +
                                    ", workerSock=" + old.sock +
                                    ", sock=" + sock + ']');

                            return;
                        }

                        if (log.isDebugEnabled())
                            log.debug("Created client message worker [locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ", sock=" + sock + ']');

                        assert clientMsgWrk0 == clientMsgWorkers.get(nodeId);

                        clientMsgWrk = clientMsgWrk0;
                    }

                    if (log.isInfoEnabled()) {
                        log.info("Initialized connection with remote " +
                            (req.client() ? "client" : "server") +
                            " node [nodeId=" + nodeId +
                            ", rmtAddr=" + sock.getRemoteSocketAddress() + ']');
                    }
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e + ", sock=" + sock + ']', e);

                    if (X.hasCause(e, SSLException.class) && spi.isSslEnabled() && !spi.isNodeStopping0()) {
                        LT.warn(log, "Failed to initialize connection " +
                            "(missing SSL configuration on remote node?) " +
                            "[rmtAddr=" + sock.getInetAddress() + ']', true);

                        spi.stats.onSslConnectionRejected();
                    }
                    else if ((X.hasCause(e, ObjectStreamException.class) || !sock.isClosed())
                        && !spi.isNodeStopping0()) {
                        if (U.isMacInvalidArgumentError(e))
                            LT.error(log, e, "Failed to initialize connection [sock=" + sock + "]\n\t" +
                                U.MAC_INVALID_ARG_MSG);
                        else {
                            U.error(
                                log,
                                "Failed to initialize connection (this can happen due to short time " +
                                    "network problems and can be ignored if does not affect node discovery) " +
                                    "[sock=" + sock + ']',
                                e);
                        }
                    }

                    onException("Caught exception on handshake [err=" + e + ", sock=" + sock + ']', e);

                    return;
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e + ", sock=" + sock + ']', e);

                    onException("Caught exception on handshake [err=" + e + ", sock=" + sock + ']', e);

                    if (e.hasCause(SocketTimeoutException.class))
                        LT.warn(log, "Socket operation timed out on handshake " +
                            "(consider increasing 'networkTimeout' configuration property) " +
                            "[netTimeout=" + spi.netTimeout + ']');

                    else if (e.hasCause(ClassNotFoundException.class))
                        LT.warn(log, "Failed to read message due to ClassNotFoundException " +
                            "(make sure same versions of all classes are available on all nodes) " +
                            "[rmtAddr=" + rmtAddr +
                            ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');

                        // Always report marshalling problems.
                    else if (e.hasCause(ObjectStreamException.class) ||
                        (!sock.isClosed() && !e.hasCause(IOException.class)))
                        LT.error(log, e, "Failed to initialize connection [sock=" + sock + ']');

                    return;
                }

                long sockTimeout = spi.getEffectiveSocketTimeout(srvSock);

                while (!isInterrupted()) {
                    try {
                        SecurityUtils.serializeVersion(1);

                        TcpDiscoveryAbstractMessage msg = U.unmarshal(spi.marshaller(), in,
                            U.resolveClassLoader(spi.ignite().configuration()));

                        msg.senderNodeId(nodeId);

                        DebugLogger debugLog = messageLogger(msg);

                        if (debugLog.isDebugEnabled())
                            debugLog.debug("Message has been received: " + msg);

                        spi.stats.onMessageReceived(msg);

                        if (debugMode && recordable(msg))
                            debugLog(msg, "Message has been received: " + msg);

                        if (msg instanceof TcpDiscoveryConnectionCheckMessage) {
                            ringMessageReceived();

                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                            TcpDiscoveryJoinRequestMessage req = (TcpDiscoveryJoinRequestMessage)msg;

                            if (!req.responded()) {
                                boolean ok = processJoinRequestMessage(req, clientMsgWrk);

                                if (clientMsgWrk != null && ok)
                                    continue;
                                else
                                    // Direct join request - no need to handle this socket anymore.
                                    break;
                            }
                        }
                        else if (msg instanceof TcpDiscoveryClientReconnectMessage) {
                            TcpDiscoverySpiState state = spiStateCopy();

                            if (state == CONNECTED) {
                                spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                                if (clientMsgWrk != null && clientMsgWrk.runner() == null && !clientMsgWrk.isDone())
                                    new MessageWorkerThreadWithCleanup<>(clientMsgWrk, log).start();

                                processClientReconnectMessage((TcpDiscoveryClientReconnectMessage)msg);

                                continue;
                            }
                            else {
                                TcpDiscoveryClientReconnectMessage msg0 = (TcpDiscoveryClientReconnectMessage)msg;

                                // If message is received from previous node and node is connecting forward to next node.
                                if (!getLocalNodeId().equals(msg0.routerNodeId()) && state == CONNECTING) {
                                    spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                                    msgWorker.addMessage(msg);

                                    continue;
                                }

                                spi.writeToSocket(msg, sock, RES_CONTINUE_JOIN, sockTimeout);

                                break;
                            }
                        }
                        else if (msg instanceof TcpDiscoveryDuplicateIdMessage) {
                            // Send receipt back.
                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = DUPLICATE_ID;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Duplicate ID message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryAuthFailedMessage) {
                            // Send receipt back.
                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = AUTH_FAILED;

                                    mux.notifyAll();
                                }
                                else {
                                    UUID targetNode = ((TcpDiscoveryAuthFailedMessage)msg).getTargetNodeId();

                                    if (targetNode == null || targetNode.equals(locNodeId)) {
                                        if (log.isDebugEnabled())
                                            log.debug("Auth failed message has been ignored [msg=" + msg +
                                                ", spiState=" + spiState + ']');
                                    }
                                    else
                                        //It can happen when current node is router node and target node is client.
                                        msgWorker.addMessage(msg);
                                }
                            }

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryCheckFailedMessage) {
                            // Send receipt back.
                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = CHECK_FAILED;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    ClientMessageWorker worker = clientMsgWorkers.get(msg.creatorNodeId());

                                    if (worker != null) {
                                        msg.verify(getLocalNodeId());

                                        worker.addMessage(msg);
                                    }
                                    else {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Failed to find client message worker " +
                                                "[clientNode=" + msg.creatorNodeId() + ']');
                                        }
                                    }

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Check failed message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryLoopbackProblemMessage) {
                            // Send receipt back.
                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = LOOPBACK_PROBLEM;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Loopback problem message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryPingResponse) {
                            assert msg.client() : msg;

                            ClientMessageWorker clientWorker = clientMsgWorkers.get(msg.creatorNodeId());

                            if (clientWorker != null)
                                clientWorker.pingResult(true);

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryRingLatencyCheckMessage) {
                            ringMessageReceived();

                            if (log.isInfoEnabled())
                                log.info("Latency check message has been read: " + msg.id());

                            ((TcpDiscoveryRingLatencyCheckMessage)msg).onRead();
                        }

                        TcpDiscoveryClientMetricsUpdateMessage metricsUpdateMsg = null;

                        if (msg instanceof TcpDiscoveryClientMetricsUpdateMessage)
                            metricsUpdateMsg = (TcpDiscoveryClientMetricsUpdateMessage)msg;
                        else {
                            ringMessageReceived();

                            msgWorker.addMessage(msg, false, true);
                        }

                        // Send receipt back.
                        if (clientMsgWrk != null) {
                            TcpDiscoveryClientAckResponse ack = new TcpDiscoveryClientAckResponse(locNodeId, msg.id());

                            ack.verify(locNodeId);

                            clientMsgWrk.addMessage(ack);
                        }
                        else
                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                        if (metricsUpdateMsg != null)
                            processClientMetricsUpdateMessage(metricsUpdateMsg);
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Caught exception on message read [sock=" + sock +
                                ", locNodeId=" + locNodeId + ", rmtNodeId=" + nodeId + ']', e);

                        onException("Caught exception on message read [sock=" + sock +
                            ", locNodeId=" + locNodeId + ", rmtNodeId=" + nodeId + ']', e);

                        if (isInterrupted() || sock.isClosed())
                            return;

                        if (e.hasCause(ClassNotFoundException.class))
                            LT.warn(log, "Failed to read message due to ClassNotFoundException " +
                                "(make sure same versions of all classes are available on all nodes) " +
                                "[rmtNodeId=" + nodeId +
                                ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');

                        // Always report marshalling errors.
                        boolean err = e.hasCause(ObjectStreamException.class) ||
                            (nodeAlive(nodeId) && spiStateCopy() == CONNECTED && !X.hasCause(e, IOException.class));

                        if (err)
                            LT.error(log, e, "Failed to read message [sock=" + sock + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']');

                        return;
                    }
                    catch (IOException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Caught exception on message read [sock=" + sock + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']', e);

                        if (isInterrupted() || sock.isClosed())
                            return;

                        // Always report marshalling errors (although it is strange here).
                        boolean err = X.hasCause(e, ObjectStreamException.class) ||
                            (nodeAlive(nodeId) && spiStateCopy() == CONNECTED);

                        if (err)
                            LT.error(log, e, "Failed to send receipt on message [sock=" + sock +
                                ", locNodeId=" + locNodeId + ", rmtNodeId=" + nodeId + ']');

                        onException("Caught exception on message read [sock=" + sock + ", locNodeId=" + locNodeId +
                            ", rmtNodeId=" + nodeId + ']', e);

                        return;
                    }
                    finally {
                        SecurityUtils.restoreDefaultSerializeVersion();
                    }
                }
            }
            finally {
                if (clientMsgWrk != null) {
                    if (log.isDebugEnabled())
                        log.debug("Client connection failed [sock=" + sock + ", locNodeId=" + locNodeId +
                            ", rmtNodeId=" + nodeId + ']');

                    clientMsgWorkers.remove(nodeId, clientMsgWrk);

                    U.interrupt(clientMsgWrk.runner());
                }

                U.close(sock, log);

                if (log.isInfoEnabled())
                    log.info("Finished serving remote node connection [rmtAddr=" + rmtAddr +
                        ", rmtPort=" + sock.getPort() + ", rmtNodeId=" + nodeId + ']');

                if (isLocalNodeCoordinator() && !ring.hasRemoteServerNodes())
                    U.enhanceThreadName(msgWorkerThread, "crd");
            }
        }

        /**
         * Update last ring message received timestamp.
         */
        private void ringMessageReceived() {
            lastRingMsgReceivedTime = System.nanoTime();
        }

        /** @return Alive address if was able to connected to. {@code Null} otherwise. */
        private InetSocketAddress checkConnection(List<InetSocketAddress> addrs, int timeout) {
            AtomicReference<InetSocketAddress> liveAddrHolder = new AtomicReference<>();

            CountDownLatch latch = new CountDownLatch(addrs.size());

            int addrLeft = addrs.size();

            int threadsLeft = utilityPool.getMaximumPoolSize();

            AtomicInteger addrIdx = new AtomicInteger();

            while (addrLeft > 0) {
                int addrPerThread = addrLeft / threadsLeft + (addrLeft % threadsLeft > 0 ? 1 : 0);

                addrLeft -= addrPerThread;

                --threadsLeft;

                utilityPool.execute(new Thread() {
                    private final int addrsToCheck = addrPerThread;

                    /** */
                    @Override public void run() {
                        int perAddrTimeout = timeout / addrsToCheck;

                        for (int i = 0; i < addrsToCheck; ++i) {
                            InetSocketAddress addr = addrs.get(addrIdx.getAndIncrement());

                            try (Socket sock = new Socket()) {
                                if (liveAddrHolder.get() == null) {
                                    sock.connect(addr, perAddrTimeout);

                                    liveAddrHolder.compareAndSet(null, addr);
                                }
                            }
                            catch (Exception ignored) {
                                // No-op.
                            }
                            finally {
                                latch.countDown();
                            }
                        }
                    }
                });
            }

            try {
                latch.await(timeout, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                // No-op.
            }

            return liveAddrHolder.get();
        }

        /**
         * Processes client reconnect message.
         *
         * @param msg Client reconnect message.
         */
        private void processClientReconnectMessage(TcpDiscoveryClientReconnectMessage msg) {
            UUID nodeId = msg.creatorNodeId();

            UUID locNodeId = getLocalNodeId();

            boolean isLocNodeRouter = msg.routerNodeId().equals(locNodeId);

            TcpDiscoveryNode node = ring.node(nodeId);

            assert node == null || node.clientRouterNodeId() != null;

            if (node != null) {
                node.clientRouterNodeId(msg.routerNodeId());
                node.clientAliveTime(spi.clientFailureDetectionTimeout());
            }

            if (!msg.verified()) {
                if (isLocNodeRouter || isLocalNodeCoordinator()) {
                    if (node != null) {
                        Collection<TcpDiscoveryAbstractMessage> pending = msgHist.messages(msg.lastMessageId(), node);

                        if (pending != null) {
                            msg.verify(locNodeId);
                            msg.pendingMessages(pending);
                            msg.success(true);

                            if (log.isDebugEnabled())
                                log.debug("Accept client reconnect, restored pending messages " +
                                    "[locNodeId=" + locNodeId + ", clientNodeId=" + nodeId + ']');
                        }
                        else if (!isLocalNodeCoordinator()) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to restore pending messages for reconnecting client. " +
                                    "Forwarding reconnection message to coordinator " +
                                    "[locNodeId=" + locNodeId + ", clientNodeId=" + nodeId + ']');
                        }
                        else {
                            msg.verify(locNodeId);

                            if (log.isDebugEnabled())
                                log.debug("Failing reconnecting client node because failed to restore pending " +
                                    "messages [locNodeId=" + locNodeId + ", clientNodeId=" + nodeId + ']');

                            TcpDiscoveryNodeFailedMessage nodeFailedMsg = new TcpDiscoveryNodeFailedMessage(locNodeId,
                                node.id(), node.internalOrder());

                            msgWorker.addMessage(nodeFailedMsg);
                        }
                    }
                    else {
                        msg.verify(locNodeId);

                        if (log.isDebugEnabled())
                            log.debug("Reconnecting client node is already failed [nodeId=" + nodeId + ']');
                    }

                    if (msg.verified() && isLocNodeRouter) {
                        ClientMessageWorker wrk = clientMsgWorkers.get(nodeId);

                        if (wrk != null)
                            wrk.addMessage(msg);
                        else if (log.isDebugEnabled())
                            log.debug("Failed to reconnect client node (disconnected during the process) [locNodeId=" +
                                locNodeId + ", clientNodeId=" + nodeId + ']');
                    }
                    else
                        msgWorker.addMessage(msg);
                }
                else
                    msgWorker.addMessage(msg);
            }
            else {
                if (isLocalNodeCoordinator())
                    msgWorker.addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                if (isLocNodeRouter) {
                    ClientMessageWorker wrk = clientMsgWorkers.get(nodeId);

                    if (wrk != null)
                        wrk.addMessage(msg);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to reconnect client node (disconnected during the process) [locNodeId=" +
                            locNodeId + ", clientNodeId=" + nodeId + ']');
                }
                else if (ring.hasRemoteNodes() && !isLocalNodeCoordinator())
                    msgWorker.addMessage(msg);
            }
        }

        /**
         * Processes client metrics update message.
         *
         * @param msg Client metrics update message.
         */
        private void processClientMetricsUpdateMessage(TcpDiscoveryClientMetricsUpdateMessage msg) {
            assert msg.client();

            ClientMessageWorker wrk = clientMsgWorkers.get(msg.creatorNodeId());

            if (wrk != null)
                wrk.metrics(msg.metrics());
            else if (log.isDebugEnabled())
                log.debug("Received client metrics update message from unknown client node: " + msg);
        }

        /**
         * @param msg Join request message.
         * @param clientMsgWrk Client message worker to start.
         * @return Whether connection was successful.
         * @throws IOException If IO failed.
         */
        @SuppressWarnings({"IfMayBeConditional"})
        private boolean processJoinRequestMessage(TcpDiscoveryJoinRequestMessage msg,
            @Nullable ClientMessageWorker clientMsgWrk) throws IOException {
            assert msg != null;
            assert !msg.responded();

            TcpDiscoverySpiState state = spiStateCopy();

            long sockTimeout = spi.failureDetectionTimeoutEnabled() ? spi.failureDetectionTimeout() :
                spi.getSocketTimeout();

            if (state == CONNECTED) {
                TcpDiscoveryNode node = msg.node();

                // Check that joining node can accept incoming connections.
                if (node.clientRouterNodeId() == null) {
                    if (!pingJoiningNode(node)) {
                        spi.writeToSocket(msg, sock, RES_JOIN_IMPOSSIBLE, sockTimeout);

                        return false;
                    }
                }

                spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                if (log.isDebugEnabled())
                    log.debug("Responded to join request message [msg=" + msg + ", res=" + RES_OK + ']');

                msg.responded(true);

                if (clientMsgWrk != null && clientMsgWrk.runner() == null && !clientMsgWrk.isDone()) {
                    clientMsgWrk.clientVersion(U.productVersion(msg.node()));

                    new MessageWorkerThreadWithCleanup<>(clientMsgWrk, log).start();
                }

                msgWorker.addMessage(msg);

                return true;
            }
            else {
                spi.stats.onMessageProcessingStarted(msg);

                int res;

                SocketAddress rmtAddr = sock.getRemoteSocketAddress();

                if (state == CONNECTING) {
                    if (noResAddrs.contains(rmtAddr) ||
                        getLocalNodeId().compareTo(msg.creatorNodeId()) < 0)
                        // Remote node node has not responded to join request or loses UUID race.
                        res = RES_WAIT;
                    else
                        // Remote node responded to join request and wins UUID race.
                        res = RES_CONTINUE_JOIN;
                }
                else
                    // Local node is stopping. Remote node should try next one.
                    res = RES_CONTINUE_JOIN;

                spi.writeToSocket(msg, sock, res, sockTimeout);

                if (log.isDebugEnabled())
                    log.debug("Responded to join request message [msg=" + msg + ", res=" + res + ']');

                fromAddrs.addAll(msg.node().socketAddresses());

                spi.stats.onMessageProcessingFinished(msg);

                return false;
            }
        }

        /**
         * @param node Remote node to ping.
         *
         * @return {@code True} if ping was successful and {@code false} if ping failed for any reason.
         */
        private boolean pingJoiningNode(TcpDiscoveryNode node) {
            for (InetSocketAddress addr : spi.getNodeAddresses(node, false)) {
                try {
                    if (!(addr.getAddress().isLoopbackAddress() && locNode.socketAddresses().contains(addr))) {
                        IgniteBiTuple<UUID, Boolean> t = pingNode(addr, node.id(), null);

                        if (t != null)
                            return true;
                    }
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ping joining node, closing connection. [node=" + node +
                            ", err=" + e.getMessage() + ']');
                }
            }

            U.warn(log, "Failed to ping joining node, closing connection. [node=" + node + ']');

            return false;
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            super.interrupt();

            U.closeQuiet(sock);
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            U.closeQuiet(sock);

            synchronized (mux) {
                readers.remove(this);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Socket reader [id=" + getId() + ", name=" + getName() + ", nodeId=" + nodeId + ']';
        }
    }

    /**
     * SPI Statistics printer.
     */
    private class StatisticsPrinter extends IgniteSpiThread {
        /**
         * Constructor.
         */
        StatisticsPrinter() {
            super(spi.ignite().name(), "tcp-disco-stats-printer", log);

            assert spi.statsPrintFreq > 0;

            assert log.isInfoEnabled();

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Statistics printer has been started.");

            while (!isInterrupted()) {
                Thread.sleep(spi.statsPrintFreq);

                printStatistics();
            }
        }
    }

    /** */
    private class ClientMessageWorker extends MessageWorker<T2<TcpDiscoveryAbstractMessage, byte[]>> {
        /** Node ID. */
        private final UUID clientNodeId;

        /** Socket. */
        private final Socket sock;

        /** Current client metrics. */
        private volatile ClusterMetrics metrics;

        /** Last metrics update message receive time. */
        private volatile long lastMetricsUpdateMsgTimeNanos;

        /** */
        private final AtomicReference<GridFutureAdapter<Boolean>> pingFut = new AtomicReference<>();

        /** */
        private IgniteProductVersion clientVer;

        /**
         * @param sock Socket.
         * @param clientNodeId Node ID.
         * @param log Logger.
         */
        private ClientMessageWorker(Socket sock, UUID clientNodeId, IgniteLogger log) {
            super("tcp-disco-client-message-worker-[" + U.id8(clientNodeId)
                + ' ' + sock.getInetAddress().getHostAddress()
                + ":" + sock.getPort() + ']', log, Math.max(spi.metricsUpdateFreq, 10), null);

            this.sock = sock;
            this.clientNodeId = clientNodeId;

            lastMetricsUpdateMsgTimeNanos = System.nanoTime();
        }

        /**
         * @param clientVer Client version.
         */
        void clientVersion(IgniteProductVersion clientVer) {
            this.clientVer = clientVer;
        }

        /**
         * @return Current client metrics.
         */
        ClusterMetrics metrics() {
            return metrics;
        }

        /**
         * @param metrics New current client metrics.
         */
        void metrics(ClusterMetrics metrics) {
            lastMetricsUpdateMsgTimeNanos = System.nanoTime();

            this.metrics = metrics;
        }

        /**
         * @param msg Message.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg) {
            addMessage(msg, null);
        }

        /**
         * @param msg Message.
         * @param msgBytes Optional message bytes.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg, @Nullable byte[] msgBytes) {
            T2<TcpDiscoveryAbstractMessage, byte[]> t = new T2<>(msg, msgBytes);

            if (msg.highPriority())
                queue.addFirst(t);
            else
                queue.add(t);

            DebugLogger log = messageLogger(msg);

            if (log.isDebugEnabled())
                log.debug("Message has been added to client queue: " + msg);
        }

        /** {@inheritDoc} */
        @Override protected void processMessage(T2<TcpDiscoveryAbstractMessage, byte[]> msgT) {
            boolean success = false;

            TcpDiscoveryAbstractMessage msg = msgT.get1();

            try {
                assert msg.verified() : msg;

                byte[] msgBytes = msgT.get2();

                if (msgBytes == null)
                    msgBytes = U.marshal(spi.marshaller(), msg);

                DebugLogger msgLog = messageLogger(msg);

                if (msg instanceof TcpDiscoveryClientAckResponse) {
                    if (clientVer == null) {
                        ClusterNode node = spi.getNode(clientNodeId);

                        if (node != null)
                            clientVer = IgniteUtils.productVersion(node);
                        else if (msgLog.isDebugEnabled())
                            msgLog.debug("Skip sending message ack to client, fail to get client node " +
                                "[sock=" + sock + ", locNodeId=" + getLocalNodeId() +
                                ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');
                    }

                    if (clientVer != null) {
                        if (msgLog.isDebugEnabled())
                            msgLog.debug("Sending message ack to client [sock=" + sock + ", locNodeId="
                                + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');

                        spi.writeToSocket(sock, msg, msgBytes, spi.failureDetectionTimeoutEnabled() ?
                            spi.clientFailureDetectionTimeout() : spi.getSocketTimeout());
                    }
                }
                else {
                    if (msgLog.isDebugEnabled())
                        msgLog.debug("Redirecting message to client [sock=" + sock + ", locNodeId="
                            + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');

                    assert topologyInitialized(msg) : msg;

                    spi.writeToSocket(sock, msg, msgBytes, spi.getEffectiveSocketTimeout(false));
                }

                boolean clientFailed = msg instanceof TcpDiscoveryNodeFailedMessage &&
                    ((TcpDiscoveryNodeFailedMessage)msg).failedNodeId().equals(clientNodeId);

                assert !clientFailed || msg.force() : msg;

                success = !clientFailed;
            }
            catch (IgniteCheckedException | IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Client connection failed [sock=" + sock + ", locNodeId="
                        + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']', e);

                onException("Client connection failed [sock=" + sock + ", locNodeId="
                    + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']', e);
            }
            finally {
                if (!success) {
                    clientMsgWorkers.remove(clientNodeId, this);

                    U.interrupt(runner());

                    U.close(sock, log);
                }
            }
        }

        /**
         * @param msg Message.
         * @return {@code True} if topology initialized.
         */
        private boolean topologyInitialized(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                TcpDiscoveryNodeAddedMessage addedMsg = (TcpDiscoveryNodeAddedMessage)msg;

                if (clientNodeId.equals(addedMsg.node().id()))
                    return addedMsg.topology() != null;
            }

            return true;
        }

        /**
         * @param res Ping result.
         */
        public void pingResult(boolean res) {
            GridFutureAdapter<Boolean> fut = pingFut.getAndSet(null);

            if (fut != null)
                fut.onDone(res);
        }

        /**
         * @param timeoutHelper Timeout controller.
         * @return Ping result.
         * @throws InterruptedException If interrupted.
         */
        public boolean ping(IgniteSpiOperationTimeoutHelper timeoutHelper) throws InterruptedException {
            if (spi.isNodeStopping0())
                return false;

            GridFutureAdapter<Boolean> fut;

            while (true) {
                fut = pingFut.get();

                if (fut != null)
                    break;

                fut = new GridFutureAdapter<>();

                if (pingFut.compareAndSet(null, fut)) {
                    TcpDiscoveryPingRequest pingReq = new TcpDiscoveryPingRequest(getLocalNodeId(), clientNodeId);

                    pingReq.verify(getLocalNodeId());

                    addMessage(pingReq);

                    break;
                }
            }

            try {
                return fut.get(timeoutHelper.nextTimeoutChunk(spi.getAckTimeout()),
                    TimeUnit.MILLISECONDS);
            }
            catch (IgniteInterruptedCheckedException ignored) {
                throw new InterruptedException();
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                if (pingFut.compareAndSet(fut, null))
                    fut.onDone(false);

                return false;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteSpiException("Internal error: ping future cannot be done with exception", e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void tearDown() {
            pingResult(false);

            U.closeQuiet(sock);
        }

        /** {@inheritDoc} */
        @Override protected void noMessageLoop() {
            if (U.millisSinceNanos(lastMetricsUpdateMsgTimeNanos) > spi.clientFailureDetectionTimeout()) {
                TcpDiscoveryNode clientNode = ring.node(clientNodeId);

                if (clientNode != null) {
                    boolean failedNode;

                    synchronized (mux) {
                        failedNode = failedNodes.containsKey(clientNode);
                    }

                    if (!failedNode) {
                        String msg = "Client node considered as unreachable " +
                            "and will be dropped from cluster, " +
                            "because no metrics update messages received in interval: " +
                            "TcpDiscoverySpi.clientFailureDetectionTimeout() ms. " +
                            "It may be caused by network problems or long GC pause on client node, try to increase this " +
                            "parameter. " +
                            "[nodeId=" + clientNodeId +
                            ", clientFailureDetectionTimeout=" + spi.clientFailureDetectionTimeout() +
                            ']';

                        failNode(clientNodeId, msg);

                        U.warn(log, msg);
                    }
                }
            }
        }
    }

    /** */
    private class MessageWorkerThreadWithCleanup<T> extends MessageWorkerThread<MessageWorker<T>> {

        /** {@inheritDoc} */
        private MessageWorkerThreadWithCleanup(MessageWorker<T> worker, IgniteLogger log) {
            super(worker, log);
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            worker.tearDown();
        }
    }

    /** */
    private class MessageWorkerDiscoveryThread extends MessageWorkerThread<GridWorker> implements IgniteDiscoveryThread {
        /** {@inheritDoc} */
        private MessageWorkerDiscoveryThread(GridWorker worker, IgniteLogger log) {
            super(worker, log);
        }

        /** {@inheritDoc} */
        @Override public GridWorker worker() {
            return worker;
        }
    }

    /**
     * Slightly modified {@link IgniteSpiThread} intended to use with message workers.
     */
    private class MessageWorkerThread<W extends GridWorker> extends IgniteSpiThread {
        /**
         * Backed interrupted flag, once set, it is not affected by further {@link Thread#interrupted()} calls.
         */
        private volatile boolean interrupted;

        /** */
        protected final W worker;

        /** {@inheritDoc} */
        private MessageWorkerThread(W worker, IgniteLogger log) {
            super(worker.igniteInstanceName(), worker.name(), log);

            this.worker = worker;

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            worker.run();
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            interrupted = true;

            super.interrupt();
        }

        /** {@inheritDoc} */
        @Override public boolean isInterrupted() {
            return interrupted || super.isInterrupted();
        }
    }

    /**
     * Superclass for all message workers.
     *
     * @param <T> Message type.
     */
    private abstract class MessageWorker<T> extends GridWorker {
        /** Message queue. */
        protected final BlockingDeque<T> queue = new LinkedBlockingDeque<>();

        /** Polling timeout. */
        private final long pollingTimeout;

        /** */
        private Runnable beforeEachPoll;

        /**
         * @param name Worker name.
         * @param log Logger.
         * @param pollingTimeout Messages polling timeout.
         * @param lsnr Listener for life-cycle events.
         */
        protected MessageWorker(
            String name,
            IgniteLogger log,
            long pollingTimeout,
            @Nullable GridWorkerListener lsnr
        ) {
            super(spi.ignite().name(), name, log, lsnr);

            this.pollingTimeout = pollingTimeout;
        }

        /**
         * @param act action to be executed before each timed queue poll.
         */
        void setBeforeEachPollAction(Runnable act) {
            beforeEachPoll = act;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Message worker started [locNodeId=" + getConfiguredNodeId() + ']');

            while (!isCancelled()) {
                if (beforeEachPoll != null)
                    beforeEachPoll.run();

                T msg = queue.poll(pollingTimeout, TimeUnit.MILLISECONDS);

                if (msg == null)
                    noMessageLoop();
                else
                    processMessage(msg);
            }
        }

        /**
         * @return Current message queue size.
         */
        int queueSize() {
            return queue.size();
        }

        /**
         * Processes succeeding message.
         *
         * @param msg Message.
         */
        protected abstract void processMessage(T msg);

        /**
         * Called when there is no message to process giving ability to perform other activity.
         */
        protected void noMessageLoop() {
            // No-op.
        }

        /**
         * Actions to be done before worker termination.
         */
        protected void tearDown() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class GridPingFutureAdapter<R> extends GridFutureAdapter<R> {
        /** ID of node ping request is sent to. */
        private final UUID nodeId;

        /** Socket. */
        private volatile Socket sock;

        /**
         * @param nodeId ID of node ping request is sent to.
         */
        GridPingFutureAdapter(@Nullable UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * Returns socket associated with this ping future.
         *
         * @return Socket or {@code null} if no socket associated.
         */
        public Socket sock() {
            return sock;
        }

        /**
         * Associates socket with this ping future.
         *
         * @param sock Socket.
         */
        public void sock(Socket sock) {
            this.sock = sock;
        }
    }

    /**
     *
     */
    private enum RingMessageSendState {
        /** */
        STARTING_POINT,

        /** */
        FORWARD_PASS,

        /** */
        BACKWARD_PASS,

        /** */
        FAILED
    }

    /**
     * Initial state is {@link RingMessageSendState#STARTING_POINT}.<br>
     * States could be switched:<br>
     * {@link RingMessageSendState#STARTING_POINT} => {@link RingMessageSendState#FORWARD_PASS} when next node failed.<br>
     * {@link RingMessageSendState#FORWARD_PASS} => {@link RingMessageSendState#FORWARD_PASS} when new next node failed.<br>
     * {@link RingMessageSendState#FORWARD_PASS} => {@link RingMessageSendState#BACKWARD_PASS} when new next node has
     * connection to it's previous node and forces local node to try it again.<br>
     * {@link RingMessageSendState#BACKWARD_PASS} => {@link RingMessageSendState#BACKWARD_PASS} when previously tried node
     * has connection to it's previous and forces local node to try it again.<br>
     * {@link RingMessageSendState#BACKWARD_PASS} => {@link RingMessageSendState#STARTING_POINT} when local node came back
     * to initial next node and no topology changes should be performed.<br>
     * {@link RingMessageSendState#BACKWARD_PASS} => {@link RingMessageSendState#FAILED} when recovery timeout is over and
     * all new next nodes have connections to their previous nodes. That means local node has connectivity
     * issue and should be stopped.<br>
     */
    private class CrossRingMessageSendState {
        /** */
        private RingMessageSendState state = RingMessageSendState.STARTING_POINT;

        /** */
        private int failedNodes;

        /** */
        private final long failTimeNanos;

        /**
         *
         */
        CrossRingMessageSendState() {
            failTimeNanos = U.millisToNanos(spi.getEffectiveConnectionRecoveryTimeout()) + System.nanoTime();
        }

        /**
         * @return {@code True} if state is {@link RingMessageSendState#STARTING_POINT}.
         */
        boolean isStartingPoint() {
            return state == RingMessageSendState.STARTING_POINT;
        }

        /**
         * @return {@code True} if state is {@link RingMessageSendState#BACKWARD_PASS}.
         */
        boolean isBackward() {
            return state == RingMessageSendState.BACKWARD_PASS;
        }

        /**
         * @return {@code True} if state is {@link RingMessageSendState#FAILED}.
         */
        boolean isFailed() {
            return state == RingMessageSendState.FAILED;
        }

        /**
         * Marks next node as failed.
         *
         * @return {@code True} node marked as failed.
         */
        boolean markNextNodeFailed() {
            if (state == RingMessageSendState.STARTING_POINT || state == RingMessageSendState.FORWARD_PASS) {
                state = RingMessageSendState.FORWARD_PASS;

                failedNodes++;

                return true;
            }

            return false;
        }

        /**
         * Checks if message sending has completely failed due to the timeout. Sets {@code RingMessageSendState#FAILED}
         * if the timeout is reached.
         *
         * @return {@code True} if passed timeout is reached. {@code False} otherwise.
         */
        boolean checkTimeout() {
            if (System.nanoTime() >= failTimeNanos) {
                state = RingMessageSendState.FAILED;

                return true;
            }

            return false;
        }

        /**
         * Marks last failed node as alive.
         *
         * @return {@code False} if all failed nodes marked as alive or incorrect state.
         */
        boolean markLastFailedNodeAlive() {
            if (state == RingMessageSendState.FORWARD_PASS || state == RingMessageSendState.BACKWARD_PASS) {
                state = RingMessageSendState.BACKWARD_PASS;

                if (--failedNodes <= 0) {
                    failedNodes = 0;

                    state = RingMessageSendState.STARTING_POINT;
                }

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CrossRingMessageSendState.class, this);
        }
    }

    /**
     * Filter to keep track of the most recent {@link TcpDiscoveryMetricsUpdateMessage}s.
     */
    private class MetricsUpdateMessageFilter {
        /** The most recent unprocessed metrics update message, which is on its first discovery round trip. */
        private volatile TcpDiscoveryMetricsUpdateMessage actualFirstLapMetricsUpdate;

        /** The most recent unprocessed metrics update message, which is on its second discovery round trip. */
        private volatile TcpDiscoveryMetricsUpdateMessage actualSecondLapMetricsUpdate;

        /**
         * Adds the provided metrics update message to the worker's queue. If there is already a message in the queue,
         * that has passed the same number of discovery ring laps, then it's replaced with the provided one.
         *
         * @param msg Metrics update message that needs to be added to the worker's queue.
         * @return {@code True} if the message should be added to the worker's queue.
         * {@code False} if another message of the same kind is already in the queue.
         */
        private boolean addMessage(TcpDiscoveryMetricsUpdateMessage msg) {
            int laps = passedLaps(msg);

            if (laps == 2)
                return true;
            else {
                // The message should be added to the queue only if a similar message is not there already.
                // Otherwise one of actualFirstLapMetricsUpdate or actualSecondLapMetricsUpdate will be updated only.
                boolean addToQueue;

                if (laps == 0) {
                    addToQueue = actualFirstLapMetricsUpdate == null;

                    actualFirstLapMetricsUpdate = msg;
                }
                else {
                    assert laps == 1 : "Unexpected number of laps passed by a metric update message: " + laps;

                    addToQueue = actualSecondLapMetricsUpdate == null;

                    actualSecondLapMetricsUpdate = msg;
                }

                return addToQueue;
            }
        }

        /**
         * @param laps Number of discovery ring laps passed by the message.
         * @param msg Message taken from the queue.
         * @return The most recent message of the same kind received by the local node.
         */
        private TcpDiscoveryMetricsUpdateMessage pollActualMessage(int laps, TcpDiscoveryMetricsUpdateMessage msg) {
            if (laps == 0) {
                msg = actualFirstLapMetricsUpdate;

                actualFirstLapMetricsUpdate = null;
            }
            else if (laps == 1) {
                msg = actualSecondLapMetricsUpdate;

                actualSecondLapMetricsUpdate = null;
            }

            return msg;
        }

        /**
         * @param msg Metrics update message.
         * @return Number of laps, that the provided message passed.
         */
        private int passedLaps(TcpDiscoveryMetricsUpdateMessage msg) {
            UUID locNodeId = getLocalNodeId();

            boolean hasLocMetrics = hasMetrics(msg, locNodeId);

            if (locNodeId.equals(msg.creatorNodeId()) && !hasLocMetrics && msg.senderNodeId() != null)
                return 2;
            else if (msg.senderNodeId() == null || !hasLocMetrics)
                return 0;
            else
                return 1;
        }

        /**
         * @param msg Metrics update message to check.
         * @param nodeId Node ID for which the check should be performed.
         * @return {@code True} is the message contains metrics of the node with the provided ID.
         * {@code False} otherwise.
         */
        private boolean hasMetrics(TcpDiscoveryMetricsUpdateMessage msg, UUID nodeId) {
            return msg.hasMetrics(nodeId) || msg.hasCacheMetrics(nodeId);
        }
    }
}
