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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.cache.CacheAffinitySharedManager;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNodesRing;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRedirectToClient;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LATE_AFFINITY_ASSIGNMENT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_COMPACT_FOOTER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_DFLT_SUID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.spi.IgnitePortProtocol.TCP;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.AUTH_FAILED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CHECK_FAILED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CONNECTED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CONNECTING;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DISCONNECTED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DISCONNECTING;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DUPLICATE_ID;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.LEFT;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.LOOPBACK_PROBLEM;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.STOPPING;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.STATUS_OK;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.STATUS_RECON;

/**
 *
 */
class ServerImpl extends TcpDiscoveryImpl {
    /** */
    private static final int ENSURED_MSG_HIST_SIZE = getInteger(IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE, 512);

    /** */
    private static final IgniteProductVersion CUSTOM_MSG_ALLOW_JOINING_FOR_VERIFIED_SINCE =
        IgniteProductVersion.fromString("1.5.0");

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

    /** Client message workers. */
    protected ConcurrentMap<UUID, ClientMessageWorker> clientMsgWorkers = new ConcurrentHashMap8<>();

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
    protected TcpDiscoverySpiState spiState = DISCONNECTED;

    /** Map with proceeding ping requests. */
    private final ConcurrentMap<InetSocketAddress, GridPingFutureAdapter<IgniteBiTuple<UUID, Boolean>>> pingMap =
        new ConcurrentHashMap8<>();

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
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        synchronized (mux) {
            spiState = DISCONNECTED;
        }

        utilityPool = new IgniteThreadPoolExecutor("disco-pool",
            spi.ignite().name(),
            0,
            1,
            2000,
            new LinkedBlockingQueue<Runnable>());

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

        msgWorker = new RingMessageWorker();
        msgWorker.start();

        tcpSrvr = new TcpServer();

        spi.initLocalNode(tcpSrvr.port, true);

        locNode = spi.locNode;

        // Start TCP server thread after local node is initialized.
        tcpSrvr.start();

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

        spi.stats.onJoinStarted();

        joinTopology();

        spi.stats.onJoinFinished();

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

        if (msgWorker != null && msgWorker.isAlive() && !disconnect) {
            // Send node left message only if it is final stop.
            msgWorker.addMessage(new TcpDiscoveryNodeLeftMessage(locNode.id()));

            synchronized (mux) {
                long timeout = spi.netTimeout;

                long threshold = U.currentTimeMillis() + timeout;

                while (spiState != LEFT && timeout > 0) {
                    try {
                        mux.wait(timeout);

                        timeout = threshold - U.currentTimeMillis();
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

        U.interrupt(tcpSrvr);
        U.join(tcpSrvr, log);

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = U.arrayList(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.interrupt(ipFinderCleaner);
        U.join(ipFinderCleaner, log);

        U.interrupt(msgWorker);
        U.join(msgWorker, log);

        for (ClientMessageWorker clientWorker : clientMsgWorkers.values()) {
            U.interrupt(clientWorker);
            U.join(clientWorker, log);
        }

        clientMsgWorkers.clear();

        IgniteUtils.shutdownNow(ServerImpl.class, utilityPool, log);

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);

        Collection<TcpDiscoveryNode> rmts = null;

        if (!disconnect)
            spi.printStopInfo();
        else {
            spi.getSpiContext().deregisterPorts();

            rmts = ring.visibleRemoteNodes();
        }

        long topVer = ring.topologyVersion();

        ring.clear();

        if (rmts != null && !rmts.isEmpty()) {
            // This is restart/disconnection and remote nodes are not empty.
            // We need to fire FAIL event for each.
            DiscoverySpiListener lsnr = spi.lsnr;

            if (lsnr != null) {
                Collection<ClusterNode> processed = new HashSet<>();

                for (TcpDiscoveryNode n : rmts) {
                    assert n.visible();

                    processed.add(n);

                    List<ClusterNode> top = U.arrayList(rmts, F.notIn(processed));

                    topVer++;

                    Map<Long, Collection<ClusterNode>> hist = updateTopologyHistory(topVer,
                        Collections.unmodifiableList(top));

                    lsnr.onDiscovery(EVT_NODE_FAILED, topVer, n, top, hist, null);
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

        if (log.isDebugEnabled())
            log.debug("Pinging node: " + nodeId + "]");

        if (nodeId == getLocalNodeId())
            return true;

        TcpDiscoveryNode node = ring.node(nodeId);

        if (node == null)
            return false;

        if (!nodeAlive(nodeId))
            return false;

        boolean res = pingNode(node);

        if (!res && !node.isClient() && nodeAlive(nodeId)) {
            LT.warn(log, "Failed to ping node (status check will be initiated): " + nodeId);

            msgWorker.addMessage(new TcpDiscoveryStatusCheckMessage(locNode, node.id()));
        }

        return res;
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

        if (node.isClient()) {
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
    private @Nullable IgniteBiTuple<UUID, Boolean> pingNode(InetSocketAddress addr, @Nullable UUID nodeId,
        @Nullable UUID clientNodeId) throws IgniteCheckedException {
        assert addr != null;

        UUID locNodeId = getLocalNodeId();

        IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi);

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

        GridPingFutureAdapter<IgniteBiTuple<UUID, Boolean>> fut = new GridPingFutureAdapter<>();

        GridPingFutureAdapter<IgniteBiTuple<UUID, Boolean>> oldFut = pingMap.putIfAbsent(addr, fut);

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

                        long tstamp = U.currentTimeMillis();

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

                        spi.stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                        IgniteBiTuple<UUID, Boolean> t = F.t(res.creatorNodeId(), res.clientExists());

                        fut.onDone(t);

                        return t;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        if (nodeId != null && !nodeAlive(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to ping the node (has left or leaving topology): [nodeId=" + nodeId +
                                    ']');

                            fut.onDone((IgniteBiTuple<UUID, Boolean>)null);

                            return null;
                        }

                        if (errs == null)
                            errs = new ArrayList<>();

                        errs.add(e);

                        reconCnt++;

                        if (!openedSock && reconCnt == 2)
                            break;

                        if (timeoutHelper.checkFailureTimeoutReached(e))
                            break;
                        else if (!spi.failureDetectionTimeoutEnabled() && reconCnt == spi.getReconnectCount())
                            break;
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

                boolean b = pingMap.remove(addr, fut);

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

            if (fut != null && fut.sock != null)
                // Reference to the socket is not set to null. No need to assign it to a local variable.
                U.closeQuiet(fut.sock);
        }
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        spiStop0(true);
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage evt) {
        try {
            msgWorker.addMessage(new TcpDiscoveryCustomEventMessage(getLocalNodeId(), evt,
                U.marshal(spi.marshaller(), evt)));
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
            locNode.lastExchangeTime(U.currentTimeMillis());
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

        while (true) {
            if (!sendJoinRequestMessage()) {
                if (log.isDebugEnabled())
                    log.debug("Join request message has not been sent (local node is the first in the topology).");

                if (!auth && spi.nodeAuth != null)
                    localAuthentication(locCred);

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

                notifyDiscovery(EVT_NODE_JOINED, 1, locNode);

                break;
            }

            if (log.isDebugEnabled())
                log.debug("Join request message has been sent (waiting for coordinator response).");

            synchronized (mux) {
                long timeout = spi.netTimeout;

                long threshold = U.currentTimeMillis() + timeout;

                while (spiState == CONNECTING && timeout > 0) {
                    try {
                        mux.wait(timeout);

                        timeout = threshold - U.currentTimeMillis();
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();

                        throw new IgniteSpiException("Thread has been interrupted.");
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
                else if (spiState == LOOPBACK_PROBLEM) {
                    TcpDiscoveryLoopbackProblemMessage msg = (TcpDiscoveryLoopbackProblemMessage)joinRes.get();

                    boolean locHostLoopback = spi.locHost.isLoopbackAddress();

                    String firstNode = locHostLoopback ? "local" : "remote";

                    String secondNode = locHostLoopback ? "remote" : "local";

                    throw new IgniteSpiException("Failed to add node to topology because " + firstNode +
                        " node is configured to use loopback address, but " + secondNode + " node is not " +
                        "(consider changing 'localAddress' configuration parameter) " +
                        "[locNodeAddrs=" + U.addressesAsString(locNode) + ", rmtNodeAddrs=" +
                        U.addressesAsString(msg.addresses(), msg.hostNames()) + ']');
                }
                else
                    LT.warn(log, "Node has not been connected to topology and will repeat join process. " +
                        "Check remote nodes logs for possible error messages. " +
                        "Note that large topology may require significant time to start. " +
                        "Increase 'TcpDiscoverySpi.networkTimeout' configuration property " +
                        "if getting this message on the starting nodes [networkTimeout=" + spi.netTimeout + ']');
            }
        }

        locNode.attributes().remove(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

        assert locNode.order() != 0;
        assert locNode.internalOrder() != 0;

        if (log.isDebugEnabled())
            log.debug("Discovery SPI has been connected to topology with order: " + locNode.internalOrder());
    }

    /**
     * Authenticate local node.
     *
     * @param locCred Local security credentials for authentication.
     * @throws IgniteSpiException If any error occurs.
     */
    private void localAuthentication(SecurityCredentials locCred){
        assert spi.nodeAuth != null;
        assert locCred != null;

        try {
            SecurityContext subj = spi.nodeAuth.authenticateNode(locNode, locCred);

            if (subj == null)
                throw new IgniteSpiException("Authentication failed for local node: " + locNode.id());

            Map<String, Object> attrs = new HashMap<>(locNode.attributes());

            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT, U.marshal(spi.marshaller(), subj));

            locNode.setAttributes(attrs);

        } catch (IgniteException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to authenticate local node (will shutdown local node).", e);
        }
    }

    /**
     * Tries to send join request message to a random node presenting in topology.
     * Address is provided by {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} and message is
     * sent to first node connection succeeded to.
     *
     * @return {@code true} if send succeeded.
     * @throws IgniteSpiException If any error occurs.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean sendJoinRequestMessage() throws IgniteSpiException {
        TcpDiscoveryAbstractMessage joinReq = new TcpDiscoveryJoinRequestMessage(locNode,
            spi.collectExchangeData(getLocalNodeId()));

        // Time when it has been detected, that addresses from IP finder do not respond.
        long noResStart = 0;

        while (true) {
            Collection<InetSocketAddress> addrs = spi.resolvedAddresses();

            if (F.isEmpty(addrs))
                return false;

            boolean retry = false;
            Collection<Exception> errs = new ArrayList<>();

            for (InetSocketAddress addr : addrs) {
                try {
                    Integer res = sendMessageDirectly(joinReq, addr);

                    assert res != null;

                    noResAddrs.remove(addr);

                    // Address is responsive, reset period start.
                    noResStart = 0;

                    switch (res) {
                        case RES_WAIT:
                            // Concurrent startup, try sending join request again or wait if no success.
                            retry = true;

                            break;
                        case RES_OK:
                            if (log.isDebugEnabled())
                                log.debug("Join request message has been sent to address [addr=" + addr +
                                    ", req=" + joinReq + ']');

                            // Join request sending succeeded, wait for response from topology.
                            return true;

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
            }

            if (retry) {
                if (log.isDebugEnabled())
                    log.debug("Concurrent discovery SPI start has been detected (local node should wait).");

                try {
                    U.sleep(2000);
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

                if (e != null && X.hasCause(e, ConnectException.class)) {
                    LT.warn(log, "Failed to connect to any address from IP finder " +
                        "(make sure IP finder addresses are correct and firewalls are disabled on all host machines): " +
                        toOrderedList(addrs), true);
                }

                if (spi.joinTimeout > 0) {
                    if (noResStart == 0)
                        noResStart = U.currentTimeMillis();
                    else if (U.currentTimeMillis() - noResStart > spi.joinTimeout)
                        throw new IgniteSpiException(
                            "Failed to connect to any address from IP finder within join timeout " +
                                "(make sure IP finder addresses are correct, and operating system firewalls are disabled " +
                                "on all host machines, or consider increasing 'joinTimeout' configuration property): " +
                                addrs, e);
                }

                try {
                    U.sleep(2000);
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
     * @return Response read from the recipient or {@code null} if no response is supposed.
     * @throws IgniteSpiException If an error occurs.
     */
    @Nullable private Integer sendMessageDirectly(TcpDiscoveryAbstractMessage msg, InetSocketAddress addr)
        throws IgniteSpiException {
        assert msg != null;
        assert addr != null;

        Collection<Throwable> errs = null;

        long ackTimeout0 = spi.getAckTimeout();

        int connectAttempts = 1;

        boolean joinReqSent;

        UUID locNodeId = getLocalNodeId();

        IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi);

        int reconCnt = 0;

        while (true){
            // Need to set to false on each new iteration,
            // since remote node may leave in the middle of the first iteration.
            joinReqSent = false;

            boolean openSock = false;

            Socket sock = null;

            try {
                long tstamp = U.currentTimeMillis();

                sock = spi.openSocket(addr, timeoutHelper);

                openSock = true;

                TcpDiscoveryHandshakeRequest req = new TcpDiscoveryHandshakeRequest(locNodeId);

                // Handshake.
                spi.writeToSocket(sock, req, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null, timeoutHelper.nextTimeoutChunk(
                    ackTimeout0));

                if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                    boolean ignore = false;

                    synchronized (failedNodes) {
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

                spi.stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                // Send message.
                tstamp = U.currentTimeMillis();

                spi.writeToSocket(sock, msg, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                long tstamp0 = U.currentTimeMillis();

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

                spi.stats.onMessageSent(msg, tstamp0 - tstamp);

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

                if (timeoutHelper.checkFailureTimeoutReached(e))
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
     * Notify external listener on discovery event.
     *
     * @param type Discovery event type. See {@link org.apache.ignite.events.DiscoveryEvent} for more details.
     * @param topVer Topology version.
     * @param node Remote node this event is connected with.
     */
    private void notifyDiscovery(int type, long topVer, TcpDiscoveryNode node) {
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

            lsnr.onDiscovery(type, topVer, node, top, hist, null);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", spiState=" + spiState +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');
        }
    }

    /**
     * Upcasts collection type.
     *
     * @param c Initial collection.
     * @return Resulting collection.
     */
    private static <T extends R, R> Collection<R> upcast(Collection<T> c) {
        A.notNull(c, "c");

        return (Collection<R>)c;
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
    private boolean isLocalNodeCoordinator() {
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
        return resolveCoordinator(null);
    }

    /**
     * Resolves coordinator. Nodes that are leaving or failed (but are still in
     * topology) are removed from search as well as provided filter.
     *
     * @param filter Nodes to exclude when resolving coordinator (optional).
     * @return Coordinator node or {@code null} if there are no coordinator
     * (i.e. local node is the last one and is currently stopping).
     */
    @Nullable private TcpDiscoveryNode resolveCoordinator(
        @Nullable Collection<TcpDiscoveryNode> filter) {
        synchronized (mux) {
            Collection<TcpDiscoveryNode> excluded = F.concat(false, failedNodes.keySet(), leavingNodes);

            if (!F.isEmpty(filter))
                excluded = F.concat(false, excluded, filter);

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
     * @param discardMsgId Discarded message ID.
     */
    private void prepareNodeAddedMessage(
        TcpDiscoveryAbstractMessage msg,
        UUID destNodeId,
        @Nullable Collection<PendingMessage> msgs,
        @Nullable IgniteUuid discardMsgId,
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

                nodeAddedMsg.messages(msgs0, discardMsgId, discardCustomMsgId);

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
        }
    }

    /** {@inheritDoc} */
    @Override void simulateNodeFailure() {
        U.warn(log, "Simulating node failure: " + getLocalNodeId());

        U.interrupt(tcpSrvr);
        U.join(tcpSrvr, log);

        U.interrupt(ipFinderCleaner);
        U.join(ipFinderCleaner, log);

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = U.arrayList(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.interrupt(msgWorker);
        U.join(msgWorker, log);

        for (ClientMessageWorker msgWorker : clientMsgWorkers.values()) {
            U.interrupt(msgWorker);

            U.join(msgWorker, log);
        }

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);
    }

    /** {@inheritDoc} */
    @Override public void brakeConnection() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected IgniteSpiThread workerThread() {
        return msgWorker;
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

        synchronized (mux) {
            StringBuilder b = new StringBuilder(U.nl());

            b.append(">>>").append(U.nl());
            b.append(">>>").append("Dumping discovery SPI debug info.").append(U.nl());
            b.append(">>>").append(U.nl());

            b.append("Local node ID: ").append(getLocalNodeId()).append(U.nl()).append(U.nl());
            b.append("Local node: ").append(locNode).append(U.nl()).append(U.nl());
            b.append("SPI state: ").append(spiState).append(U.nl()).append(U.nl());

            b.append("Internal threads: ").append(U.nl());

            b.append("    Message worker: ").append(threadStatus(msgWorker)).append(U.nl());

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

            U.quietAndInfo(log, b.toString());
        }
    }

    /**
     * @param msg Message.
     * @return {@code True} if recordable in debug mode.
     */
    private boolean recordable(TcpDiscoveryAbstractMessage msg) {
        return !(msg instanceof TcpDiscoveryHeartbeatMessage) &&
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
    private boolean permissionsEqual(SecurityPermissionSet locPerms, SecurityPermissionSet rmtPerms) {
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
    private static void removeMetrics(TcpDiscoveryHeartbeatMessage msg, UUID nodeId) {
        msg.removeMetrics(nodeId);
        msg.removeCacheMetrics(nodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServerImpl.class, this);
    }

    /**
     * Thread that cleans IP finder and keeps it in the correct state, unregistering
     * addresses of the nodes that has left the topology.
     * <p>
     * This thread should run only on coordinator node and will clean IP finder
     * if and only if {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()} is {@code true}.
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

                if (!isLocalNodeCoordinator())
                    continue;

                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping IP finder cleaner (SPI is not connected to topology).");

                    return;
                }

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
                                return !node.isClient() ? spi.getNodeAddresses(node) :
                                    Collections.<InetSocketAddress>emptyList();
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
     * Adds failed nodes specified in the received message to the local failed nodes list.
     *
     * @param msg Message.
     */
    private void processMessageFailedNodes(TcpDiscoveryAbstractMessage msg) {
        if (msg.failedNodes() != null) {
            for (UUID nodeId : msg.failedNodes()) {
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

            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                TcpDiscoveryNodeAddedMessage addedMsg =
                    new TcpDiscoveryNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);

                msg = addedMsg;

                TcpDiscoveryNode node = addedMsg.node();

                if (node.isClient() && !msgs.contains(msg)) {
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
                addedMsg.oldNodesDiscoveryData(null);
            }
            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                TcpDiscoveryNodeAddFinishedMessage addFinishMsg = (TcpDiscoveryNodeAddFinishedMessage)msg;

                if (addFinishMsg.clientDiscoData() != null) {
                    addFinishMsg = new TcpDiscoveryNodeAddFinishedMessage(addFinishMsg);

                    msg = addFinishMsg;

                    Map<UUID, Map<Integer, byte[]>> discoData = addFinishMsg.clientDiscoData();

                    Set<UUID> replaced = null;

                    for (TcpDiscoveryAbstractMessage msg0 : msgs) {
                        if (msg0 instanceof TcpDiscoveryNodeAddFinishedMessage) {
                            Map<UUID, Map<Integer, byte[]>> existingDiscoData =
                                ((TcpDiscoveryNodeAddFinishedMessage)msg0).clientDiscoData();

                            // Check if already stored message contains the same data to do not store copies multiple times.
                            if (existingDiscoData != null) {
                                for (Map.Entry<UUID, Map<Integer, byte[]>> e : discoData.entrySet()) {
                                    UUID nodeId = e.getKey();

                                    if (F.contains(replaced, nodeId))
                                        continue;

                                    Map<Integer, byte[]> existingData = existingDiscoData.get(e.getKey());

                                    if (existingData != null && mapsEqual(e.getValue(), existingData)) {
                                        e.setValue(existingData);

                                        if (replaced == null)
                                            replaced = new HashSet<>();

                                        boolean add = replaced.add(nodeId);

                                        assert add;

                                        if (replaced.size() == discoData.size())
                                            break;
                                    }
                                }

                                if (replaced != null && replaced.size() == discoData.size())
                                    break;
                            }
                        }
                    }
                }
            }
            else if (msg instanceof TcpDiscoveryNodeLeftMessage)
                clearClientAddFinished(msg.creatorNodeId());
            else if (msg instanceof TcpDiscoveryNodeFailedMessage)
                clearClientAddFinished(((TcpDiscoveryNodeFailedMessage)msg).failedNodeId());

            msgs.add(msg);
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
         * @param m1 Map 1.
         * @param m2 Map 2.
         * @return {@code True} if maps contain the same data.
         */
        private boolean mapsEqual(Map<Integer, byte[]> m1, Map<Integer, byte[]> m2) {
            if (m1 == m2)
                return true;

            if (m1.size() == m2.size()) {
                for (Map.Entry<Integer, byte[]> e : m1.entrySet()) {
                    byte[] data = m2.get(e.getKey());

                    if (!Arrays.equals(e.getValue(), data))
                        return false;
                }

                return true;
            }

            return false;
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
            assert node != null && node.isClient() : node;

            if (lastMsgId == null) {
                // Client connection failed before it received TcpDiscoveryNodeAddedMessage.
                List<TcpDiscoveryAbstractMessage> res = null;

                for (TcpDiscoveryAbstractMessage msg : msgs) {
                    if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                        if (node.id().equals(((TcpDiscoveryNodeAddedMessage)msg).node().id()))
                            res = new ArrayList<>(msgs.size());
                    }

                    if (res != null)
                        res.add(prepare(msg, node.id()));
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
                if (msgs.isEmpty())
                    return Collections.emptyList();

                Collection<TcpDiscoveryAbstractMessage> cp = new ArrayList<>(msgs.size());

                boolean skip = true;

                for (TcpDiscoveryAbstractMessage msg : msgs) {
                    if (skip) {
                        if (msg.id().equals(lastMsgId))
                            skip = false;
                    }
                    else
                        cp.add(prepare(msg, node.id()));
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

                    prepareNodeAddedMessage(msg0, destNodeId, null, null, null);

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

        /**
         * @param msg Message.
         */
        PendingMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null && msg.id() != null : msg;

            this.msg = msg;

            id = msg.id();
            customMsg = msg instanceof TcpDiscoveryCustomEventMessage;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PendingMessage.class, this);
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
                PendingMessage polled = msgs.poll();

                assert polled != null;

                if (polled.id.equals(discardId))
                    break;
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

            if (msgs != null) {
                for (TcpDiscoveryAbstractMessage msg : msgs)
                    this.msgs.add(new PendingMessage(msg));
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
            if (custom)
                customDiscardId = id;
            else
                discardId = id;

            cleanup();
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

                            return;
                        }
                    }
                }
                else {
                    if (skipMsg) {
                        assert discardId != null;

                        if (F.eq(discardId, msg.id)) {
                            msg.msg = null;

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
        public Iterator<TcpDiscoveryAbstractMessage> iterator() {
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

                            if (F.eq(customDiscardId, msg0.id))
                                skipCustomMsg = false;

                            continue;
                        }
                    }
                    else {
                        if (skipMsg) {
                            assert discardId != null;

                            if (F.eq(discardId, msg0.id))
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
     * Message worker thread for messages processing.
     */
    private class RingMessageWorker extends MessageWorkerAdapter<TcpDiscoveryAbstractMessage> {
        /** Next node. */
        @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
        private TcpDiscoveryNode next;

        /** Pending messages. */
        private final PendingMessages pendingMsgs = new PendingMessages();

        /** Messages history used for client reconnect. */
        private final EnsuredMessageHistory msgHist = new EnsuredMessageHistory();

        /** Last message that updated topology. */
        private TcpDiscoveryAbstractMessage lastMsg;

        /** Force pending messages send. */
        private boolean forceSndPending;

        /** Socket. */
        private Socket sock;

        /** Output stream. */
        private OutputStream out;

        /** Last time status message has been sent. */
        private long lastTimeStatusMsgSent;

        /** Incoming heartbeats check frequency. */
        private long hbCheckFreq = (long)spi.maxMissedHbs * spi.hbFreq + 50;

        /** Last time heartbeat message has been sent. */
        private long lastTimeHbMsgSent;

        /** Time when the last status message has been sent. */
        private long lastTimeConnCheckMsgSent;

        /** Flag that keeps info on whether the threshold is reached or not. */
        private boolean failureThresholdReached;

        /** Connection check frequency. */
        private long connCheckFreq;

        /** Connection check threshold. */
        private long connCheckThreshold;

        /** */
        private long lastRingMsgTime;

        /**
         */
        RingMessageWorker() {
            super("tcp-disco-msg-worker", 10);

            initConnectionCheckFrequency();
        }

        /**
         * Adds message to queue.
         *
         * @param msg Message to add.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg) {
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

            if (msg.highPriority())
                queue.addFirst(msg);
            else
                queue.add(msg);

            if (log.isDebugEnabled())
                log.debug("Message has been added to queue: " + msg);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                super.body();
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
                                    IgnitionEx.stop(ignite.name(), true, true);

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

                // Must be processed by IgniteSpiThread as well.
                throw e;
            }
        }

        /**
         * Initializes connection check frequency. Used only when failure detection timeout is enabled.
         */
        private void initConnectionCheckFrequency() {
            if (spi.failureDetectionTimeoutEnabled())
                connCheckThreshold = spi.failureDetectionTimeout();
            else
                connCheckThreshold = Math.min(spi.getSocketTimeout(), spi.getHeartbeatFrequency());

            for (int i = 3; i > 0; i--) {
                connCheckFreq = connCheckThreshold / i;

                if (connCheckFreq > 10)
                    break;
            }

            assert connCheckFreq > 0;

            if (log.isDebugEnabled())
                log.debug("Connection check frequency is calculated: " + connCheckFreq);
        }

        /**
         * @param msg Message to process.
         */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            sendHeartbeatMessage();

            DebugLogger log = messageLogger(msg);

            if (log.isDebugEnabled())
                log.debug("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            if (debugMode)
                debugLog(msg, "Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            boolean ensured = spi.ensured(msg);

            if (!locNode.id().equals(msg.senderNodeId()) && ensured)
                lastRingMsgTime = U.currentTimeMillis();

            if (locNode.internalOrder() == 0) {
                boolean proc = false;

                if (msg instanceof TcpDiscoveryNodeAddedMessage)
                    proc = ((TcpDiscoveryNodeAddedMessage)msg).node().equals(locNode);

                if (!proc) {
                    if (log.isDebugEnabled()) {
                        log.debug("Ignore message, local node order is not initialized [msg=" + msg +
                            ", locNode=" + locNode + ']');
                    }

                    return;
                }
            }

            spi.stats.onMessageProcessingStarted(msg);

            processMessageFailedNodes(msg);

            if (msg instanceof TcpDiscoveryJoinRequestMessage)
                processJoinRequestMessage((TcpDiscoveryJoinRequestMessage)msg);

            else if (msg instanceof TcpDiscoveryClientReconnectMessage)
                processClientReconnectMessage((TcpDiscoveryClientReconnectMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((TcpDiscoveryNodeAddFinishedMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((TcpDiscoveryNodeLeftMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((TcpDiscoveryNodeFailedMessage)msg);

            else if (msg instanceof TcpDiscoveryHeartbeatMessage)
                processHeartbeatMessage((TcpDiscoveryHeartbeatMessage)msg);

            else if (msg instanceof TcpDiscoveryStatusCheckMessage)
                processStatusCheckMessage((TcpDiscoveryStatusCheckMessage)msg);

            else if (msg instanceof TcpDiscoveryDiscardMessage)
                processDiscardMessage((TcpDiscoveryDiscardMessage)msg);

            else if (msg instanceof TcpDiscoveryCustomEventMessage)
                processCustomMessage((TcpDiscoveryCustomEventMessage)msg);

            else if (msg instanceof TcpDiscoveryClientPingRequest)
                processClientPingRequest((TcpDiscoveryClientPingRequest)msg);

            else
                assert false : "Unknown message type: " + msg.getClass().getSimpleName();

            if (ensured && redirectToClients(msg))
                msgHist.add(msg);

            if (msg.senderNodeId() != null && !msg.senderNodeId().equals(getLocalNodeId())) {
                // Received a message from remote node.
                onMessageExchanged();

                // Reset the failure flag.
                failureThresholdReached = false;
            }

            spi.stats.onMessageProcessingFinished(msg);
        }

        /** {@inheritDoc} */
        @Override protected void noMessageLoop() {
            if (locNode == null)
                return;

            checkConnection();

            sendHeartbeatMessage();

            checkHeartbeatsReceiving();

            checkPendingCustomMessages();

            checkFailedNodesList();
        }

        /**
         * @param msg Message.
         */
        private void sendMessageToClients(TcpDiscoveryAbstractMessage msg) {
            if (redirectToClients(msg)) {
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

                                prepareNodeAddedMessage(msg0, clientMsgWorker.clientNodeId, null, null, null);

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

            sendMessageToClients(msg);

            Collection<TcpDiscoveryNode> failedNodes;

            TcpDiscoverySpiState state;

            synchronized (mux) {
                failedNodes = U.arrayList(ServerImpl.this.failedNodes.keySet());

                state = spiState;
            }

            Collection<Throwable> errs = null;

            boolean sent = false;

            boolean newNextNode = false;

            UUID locNodeId = getLocalNodeId();

            while (true) {
                TcpDiscoveryNode newNext = ring.nextNode(failedNodes);

                if (newNext == null) {
                    if (log.isDebugEnabled())
                        log.debug("No next node in topology.");

                    if (debugMode)
                        debugLog(msg, "No next node in topology.");

                    if (ring.hasRemoteNodes() && !(msg instanceof TcpDiscoveryConnectionCheckMessage) &&
                        !(msg instanceof TcpDiscoveryStatusCheckMessage && msg.creatorNodeId().equals(locNodeId))) {
                        msg.senderNodeId(locNodeId);

                        addMessage(msg);
                    }

                    break;
                }

                if (!newNext.equals(next)) {
                    if (log.isDebugEnabled())
                        log.debug("New next node [newNext=" + newNext + ", formerNext=" + next +
                            ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

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

                // Flag that shows whether next node exists and accepts incoming connections.
                boolean nextNodeExists = sock != null;

                final boolean sameHost = U.sameMacs(locNode, next);

                List<InetSocketAddress> locNodeAddrs = U.arrayList(locNode.socketAddresses());

                addr: for (InetSocketAddress addr : spi.getNodeAddresses(next, sameHost)) {
                    long ackTimeout0 = spi.getAckTimeout();

                    if (locNodeAddrs.contains(addr)){
                        if (log.isDebugEnabled())
                            log.debug("Skip to send message to the local node (probably remote node has the same " +
                                "loopback address that local node): " + addr);

                        continue;
                    }

                    int reconCnt = 0;

                    IgniteSpiOperationTimeoutHelper timeoutHelper = null;

                    while (true) {
                        if (sock == null) {
                            if (timeoutHelper == null)
                                timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi);

                            nextNodeExists = false;

                            boolean success = false;

                            boolean openSock = false;

                            // Restore ring.
                            try {
                                long tstamp = U.currentTimeMillis();

                                sock = spi.openSocket(addr, timeoutHelper);

                                out = spi.socketStream(sock);

                                openSock = true;

                                // Handshake.
                                spi.writeToSocket(sock, out, new TcpDiscoveryHandshakeRequest(locNodeId),
                                    timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null,
                                    timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                if (locNodeId.equals(res.creatorNodeId())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Handshake response from local node: " + res);

                                    U.closeQuiet(sock);

                                    sock = null;

                                    break;
                                }

                                spi.stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

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

                                if (!openSock)
                                    break; // Don't retry if we can not establish connection.

                                if (!spi.failureDetectionTimeoutEnabled() && ++reconCnt == spi.getReconnectCount())
                                    break;

                                if (timeoutHelper.checkFailureTimeoutReached(e))
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
                                    // Next node exists and accepts incoming messages.
                                    nextNodeExists = true;
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

                            if (failure || forceSndPending) {
                                if (log.isDebugEnabled())
                                    log.debug("Pending messages will be sent [failure=" + failure +
                                        ", newNextNode=" + newNextNode +
                                        ", forceSndPending=" + forceSndPending + ']');

                                if (debugMode)
                                    debugLog(msg, "Pending messages will be sent [failure=" + failure +
                                        ", newNextNode=" + newNextNode +
                                        ", forceSndPending=" + forceSndPending + ']');

                                for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                                    long tstamp = U.currentTimeMillis();

                                    prepareNodeAddedMessage(pendingMsg, next.id(), pendingMsgs.msgs,
                                        pendingMsgs.discardId, pendingMsgs.customDiscardId);

                                    if (timeoutHelper == null)
                                        timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi);

                                    try {
                                        spi.writeToSocket(sock, out, pendingMsg, timeoutHelper.nextTimeoutChunk(
                                            spi.getSocketTimeout()));
                                    }
                                    finally {
                                        clearNodeAddedMessage(pendingMsg);
                                    }

                                    long tstamp0 = U.currentTimeMillis();

                                    int res = spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                    spi.stats.onMessageSent(pendingMsg, tstamp0 - tstamp);

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

                            if (msg instanceof TcpDiscoveryConnectionCheckMessage) {
                                if (!next.version().greaterThanEqual(TcpDiscoverySpi.FAILURE_DETECTION_MAJOR_VER,
                                    TcpDiscoverySpi.FAILURE_DETECTION_MINOR_VER,
                                    TcpDiscoverySpi.FAILURE_DETECTION_MAINT_VER))
                                    // Preserve backward compatibility with nodes of older versions.
                                    msg = new TcpDiscoveryStatusCheckMessage(locNode, null);
                            }
                            else
                                prepareNodeAddedMessage(msg, next.id(), pendingMsgs.msgs, pendingMsgs.discardId,
                                    pendingMsgs.customDiscardId);

                            try {
                                long tstamp = U.currentTimeMillis();

                                if (timeoutHelper == null)
                                    timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi);

                                if (!failedNodes.isEmpty()) {
                                    for (TcpDiscoveryNode failedNode : failedNodes) {
                                        assert !failedNode.equals(next) : failedNode;

                                        msg.addFailedNode(failedNode.id());
                                    }
                                }

                                spi.writeToSocket(sock, out, msg, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                                long tstamp0 = U.currentTimeMillis();

                                int res = spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                spi.stats.onMessageSent(msg, tstamp0 - tstamp);

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

                            if (timeoutHelper.checkFailureTimeoutReached(e))
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
                    if (!failedNodes.contains(next)) {
                        failedNodes.add(next);

                        if (state == CONNECTED) {
                            Exception err = errs != null ?
                                U.exceptionWithSuppressed("Failed to send message to next node [msg=" + msg +
                                    ", next=" + U.toShortString(next) + ']', errs) :
                                null;

                            // If node existed on connection initialization we should check
                            // whether it has not gone yet.
                            if (nextNodeExists)
                                U.warn(log, "Failed to send message to next node [msg=" + msg + ", next=" + next +
                                    ", errMsg=" + (err != null ? err.getMessage() : "N/A") + ']');
                            else if (log.isDebugEnabled())
                                log.debug("Failed to send message to next node [msg=" + msg + ", next=" + next +
                                    ", errMsg=" + (err != null ? err.getMessage() : "N/A") + ']');
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

                    for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                        prepareNodeAddedMessage(pendingMsg, locNodeId, pendingMsgs.msgs, pendingMsgs.discardId,
                            pendingMsgs.customDiscardId);

                        pendingMsg.senderNodeId(locNodeId);

                        msgWorker.addMessage(pendingMsg);

                        if (log.isDebugEnabled())
                            log.debug("Pending message has been sent to local node [msg=" + msg.id() +
                                ", pendingMsgId=" + pendingMsg + ']');

                        if (debugMode) {
                            debugLog(msg, "Pending message has been sent to local node [msg=" + msg.id() +
                                ", pendingMsgId=" + pendingMsg + ']');
                        }
                    }
                }

                LT.warn(log, "Local node has detected failed nodes and started cluster-wide procedure. " +
                        "To speed up failure detection please see 'Failure Detection' section under javadoc" +
                        " for 'TcpDiscoverySpi'");
            }
        }

        /**
         * @param msg Message.
         * @return Whether to redirect message to client nodes.
         */
        private boolean redirectToClients(TcpDiscoveryAbstractMessage msg) {
            return msg.verified() && U.getAnnotation(msg.getClass(), TcpDiscoveryRedirectToClient.class) != null;
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
                    return;
                }
            }

            if (isLocalNodeCoordinator()) {
                TcpDiscoveryNode existingNode = ring.node(node.id());

                if (existingNode != null) {
                    if (!node.socketAddresses().equals(existingNode.socketAddresses())) {
                        if (!pingNode(existingNode)) {
                            addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                                existingNode.id(), existingNode.internalOrder()));

                            // Ignore this join request since existing node is about to fail
                            // and new node can continue.
                            return;
                        }

                        try {
                            trySendMessageDirectly(node, new TcpDiscoveryDuplicateIdMessage(locNodeId,
                                existingNode));
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

                    return;
                }

                if (spi.nodeAuth != null) {
                    // Authenticate node first.
                    try {
                        SecurityCredentials cred = unmarshalCredentials(node);

                        SecurityContext subj = spi.nodeAuth.authenticateNode(node, cred);

                        if (subj == null) {
                            // Node has not pass authentication.
                            LT.warn(log, "Authentication failed [nodeId=" + node.id() +
                                    ", addrs=" + U.addressesAsString(node) + ']',
                                "Authentication failed [nodeId=" + U.id8(node.id()) + ", addrs=" +
                                    U.addressesAsString(node) + ']');

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                    U.addressesAsString(node));

                            try {
                                trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId,
                                    spi.locHost));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                onException("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }

                            // Ignore join request.
                            return;
                        }
                        else {
                            if (!(subj instanceof Serializable)) {
                                // Node has not pass authentication.
                                LT.warn(log, "Authentication subject is not Serializable [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node) + ']',
                                    "Authentication subject is not Serializable [nodeId=" + U.id8(node.id()) +
                                        ", addrs=" +
                                        U.addressesAsString(node) + ']');

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug("Authentication subject is not serializable [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node));

                                try {
                                    trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId,
                                        spi.locHost));
                                }
                                catch (IgniteSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send unauthenticated message to node " +
                                            "[node=" + node + ", err=" + e.getMessage() + ']');
                                }

                                // Ignore join request.
                                return;
                            }

                            // Stick in authentication subject to node (use security-safe attributes for copy).
                            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

                            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT, U.marshal(spi.marshaller(), subj));

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
                        return;
                    }
                }

                final IgniteNodeValidationResult err = spi.getSpiContext().validateNode(node);

                if (err != null) {
                    if (log.isDebugEnabled())
                        log.debug("Node validation failed [res=" + err + ", node=" + node + ']');

                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                boolean ping = node.id().equals(err.nodeId()) ? pingNode(node) : pingNode(err.nodeId());

                                if (!ping) {
                                    if (log.isDebugEnabled())
                                        log.debug("Conflicting node has already left, need to wait for event. " +
                                            "Will ignore join request for now since it will be recent [req=" + msg +
                                            ", err=" + err.message() + ']');

                                    // Ignore join request.
                                    return;
                                }

                                LT.warn(log, err.message());

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug(err.message());

                                try {
                                    trySendMessageDirectly(node,
                                        new TcpDiscoveryCheckFailedMessage(locNodeId, err.sendMessage()));
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
                    return;
                }

                boolean rmtLateAssignBool;

                if (node.version().compareToIgnoreTimestamp(CacheAffinitySharedManager.LATE_AFF_ASSIGN_SINCE) >= 0) {
                    Boolean rmtLateAssign = node.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);
                    // Can be null only in tests.
                    rmtLateAssignBool = rmtLateAssign != null ? rmtLateAssign : false;
                }
                else
                    rmtLateAssignBool = false;

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
                    return;
                }

                final Boolean locSrvcCompatibilityEnabled = locNode.attribute(ATTR_SERVICES_COMPATIBILITY_MODE);

                if (node.version().compareToIgnoreTimestamp(GridServiceProcessor.LAZY_SERVICES_CFG_SINCE) >= 0) {
                    final Boolean rmtSrvcCompatibilityEnabled = node.attribute(ATTR_SERVICES_COMPATIBILITY_MODE);

                    if (!F.eq(locSrvcCompatibilityEnabled, rmtSrvcCompatibilityEnabled)) {
                        utilityPool.execute(
                            new Runnable() {
                                @Override public void run() {
                                    String errMsg = "Local node's " + IGNITE_SERVICES_COMPATIBILITY_MODE +
                                        " property value differs from remote node's value " +
                                        "(to make sure all nodes in topology have identical IgniteServices compatibility mode, " +
                                        "configure system property explicitly) " +
                                        "[locSrvcCompatibilityEnabled=" + locSrvcCompatibilityEnabled +
                                        ", rmtSrvcCompatibilityEnabled=" + rmtSrvcCompatibilityEnabled +
                                        ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                        ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                        ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                                    String sndMsg = "Local node's " + IGNITE_SERVICES_COMPATIBILITY_MODE +
                                        " property value differs from remote node's value " +
                                        "(to make sure all nodes in topology have identical IgniteServices compatibility mode, " +
                                        "configure system property explicitly) " +
                                        "[locSrvcCompatibilityEnabled=" + rmtSrvcCompatibilityEnabled +
                                        ", rmtSrvcCompatibilityEnabled=" + locSrvcCompatibilityEnabled +
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
                        return;
                    }
                }
                else if (Boolean.FALSE.equals(locSrvcCompatibilityEnabled)) {
                    utilityPool.execute(
                        new Runnable() {
                            @Override public void run() {
                                String errMsg = "Remote node doesn't support lazy services configuration and " +
                                    "cannot be joined to local node because local node's "
                                    + IGNITE_SERVICES_COMPATIBILITY_MODE + " property value explicitly set to 'false'" +
                                    "[locNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + node.id() + ']';

                                String sndMsg = "Local node doesn't support lazy services configuration and " +
                                    "cannot be joined to local node because remote node's "
                                    + IGNITE_SERVICES_COMPATIBILITY_MODE + " property value explicitly set to 'false'" +
                                    "[locNodeAddrs=" + U.addressesAsString(node) +
                                    ", rmtNodeAddrs=" + U.addressesAsString(locNode) +
                                    ", locNodeId=" + node.id() + ", rmtNodeId=" + locNode.id() + ']';

                                nodeCheckError(
                                    node,
                                    errMsg,
                                    sndMsg);
                            }
                        });

                    // Ignore join request.
                    return;
                }

                // Handle join.
                node.internalOrder(ring.nextNodeOrder());

                if (log.isDebugEnabled())
                    log.debug("Internal order has been assigned to node: " + node);

                TcpDiscoveryNodeAddedMessage nodeAddedMsg = new TcpDiscoveryNodeAddedMessage(locNodeId,
                    node, msg.discoveryData(), spi.gridStartTime);

                nodeAddedMsg.client(msg.client());

                processNodeAddedMessage(nodeAddedMsg);

                if (nodeAddedMsg.verified())
                    msgHist.add(nodeAddedMsg);
            }
            else if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
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

        /**
         * Tries to send a message to all node's available addresses.
         *
         * @param node Node to send message to.
         * @param msg Message.
         * @throws IgniteSpiException Last failure if all attempts failed.
         */
        private void trySendMessageDirectly(TcpDiscoveryNode node, TcpDiscoveryAbstractMessage msg)
            throws IgniteSpiException {
            if (node.isClient()) {
                TcpDiscoveryNode routerNode = ring.node(node.clientRouterNodeId());

                if (routerNode == null)
                    throw new IgniteSpiException("Router node for client does not exist: " + node);

                if (routerNode.isClient())
                    throw new IgniteSpiException("Router node is a client node: " + node);

                if (routerNode.id().equals(getLocalNodeId())) {
                    ClientMessageWorker worker = clientMsgWorkers.get(node.id());

                    if (worker == null)
                        throw new IgniteSpiException("Client node already disconnected: " + node);

                    msg.verify(getLocalNodeId()); // Client worker require verified messages.

                    worker.addMessage(msg);

                    return;
                }

                trySendMessageDirectly(routerNode, msg);

                return;
            }

            IgniteSpiException ex = null;

            for (InetSocketAddress addr : spi.getNodeAddresses(node, U.sameMacs(locNode, node))) {
                try {
                    sendMessageDirectly(msg, addr);

                    node.lastSuccessfulAddress(addr);

                    ex = null;

                    break;
                }
                catch (IgniteSpiException e) {
                    ex = e;
                }
            }

            if (ex != null)
                throw ex;
        }

        /**
         * Processes client reconnect message.
         *
         * @param msg Client reconnect message.
         */
        private void processClientReconnectMessage(TcpDiscoveryClientReconnectMessage msg) {
            UUID nodeId = msg.creatorNodeId();

            UUID locNodeId = getLocalNodeId();

            boolean isLocNodeRouter = locNodeId.equals(msg.routerNodeId());

            if (!msg.verified()) {
                TcpDiscoveryNode node = ring.node(nodeId);

                assert node == null || node.isClient();

                if (node != null) {
                    node.clientRouterNodeId(msg.routerNodeId());
                    node.aliveCheck(spi.maxMissedClientHbs);
                }

                if (isLocalNodeCoordinator()) {
                    msg.verify(locNodeId);

                    if (node != null) {
                        Collection<TcpDiscoveryAbstractMessage> pending = msgHist.messages(msg.lastMessageId(), node);

                        if (pending != null) {
                            msg.pendingMessages(pending);
                            msg.success(true);

                            if (log.isDebugEnabled())
                                log.debug("Accept client reconnect, restored pending messages " +
                                    "[locNodeId=" + locNodeId + ", clientNodeId=" + nodeId + ']');
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failing reconnecting client node because failed to restore pending " +
                                    "messages [locNodeId=" + locNodeId + ", clientNodeId=" + nodeId + ']');

                            TcpDiscoveryNodeFailedMessage nodeFailedMsg = new TcpDiscoveryNodeFailedMessage(locNodeId,
                                node.id(), node.internalOrder());

                            processNodeFailedMessage(nodeFailedMsg);

                            if (nodeFailedMsg.verified())
                                msgHist.add(nodeFailedMsg);
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Reconnecting client node is already failed [nodeId=" + nodeId + ']');

                    if (isLocNodeRouter) {
                        ClientMessageWorker wrk = clientMsgWorkers.get(nodeId);

                        if (wrk != null)
                            wrk.addMessage(msg);
                        else if (log.isDebugEnabled())
                            log.debug("Failed to reconnect client node (disconnected during the process) [locNodeId=" +
                                locNodeId + ", clientNodeId=" + nodeId + ']');
                    }
                    else {
                        if (sendMessageToRemotes(msg))
                            sendMessageAcrossRing(msg);
                    }
                }
                else {
                    if (sendMessageToRemotes(msg))
                        sendMessageAcrossRing(msg);
                }
            }
            else {
                if (isLocalNodeCoordinator())
                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                if (isLocNodeRouter) {
                    ClientMessageWorker wrk = clientMsgWorkers.get(nodeId);

                    if (wrk != null)
                        wrk.addMessage(msg);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to reconnect client node (disconnected during the process) [locNodeId=" +
                            locNodeId + ", clientNodeId=" + nodeId + ']');
                }
                else {
                    if (ring.hasRemoteNodes() && !isLocalNodeCoordinator())
                        sendMessageAcrossRing(msg);
                }
            }
        }

        /**
         * Processes node added message.
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

            if (node.internalOrder() < locNode.internalOrder()) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node added message since local node's order is greater " +
                        "[node=" + node + ", locNode=" + locNode + ", msg=" + msg + ']');

                return;
            }

            UUID locNodeId = getLocalNodeId();

            if (isLocalNodeCoordinator()) {
                if (msg.verified()) {
                    spi.stats.onRingMessageReceived(msg);

                    TcpDiscoveryNodeAddFinishedMessage addFinishMsg = new TcpDiscoveryNodeAddFinishedMessage(locNodeId,
                        node.id());

                    if (node.isClient()) {
                        addFinishMsg.clientDiscoData(msg.oldNodesDiscoveryData());

                        addFinishMsg.clientNodeAttributes(node.attributes());
                    }

                    processNodeAddFinishedMessage(addFinishMsg);

                    if (addFinishMsg.verified())
                        msgHist.add(addFinishMsg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }

                msg.verify(locNodeId);
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

                return;
            }

            if (msg.verified() && !locNodeId.equals(node.id())) {
                if (node.internalOrder() <= ring.maxInternalOrder()) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    if (debugMode)
                        debugLog(msg, "Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

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

                            SecurityContext coordSubj = U.unmarshal(spi.marshaller(),
                                node.<byte[]>attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT),
                                U.resolveClassLoader(spi.ignite().configuration()));

                            if (!permissionsEqual(coordSubj.subject().permissions(), subj.subject().permissions())) {
                                // Node has not pass authentication.
                                LT.warn(log, "Authentication failed [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node) + ']',
                                    "Authentication failed [nodeId=" + U.id8(node.id()) + ", addrs=" +
                                        U.addressesAsString(node) + ']');

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
                    catch (IgniteException | IgniteCheckedException e) {
                        U.error(log, "Failed to verify node permissions consistency (will drop the node): " + node, e);
                    }
                    finally {
                        if (authFailed) {
                            try {
                                trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId,
                                    spi.locHost));
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
                    node.aliveCheck(spi.maxMissedClientHbs);

                boolean topChanged = ring.add(node);

                if (topChanged) {
                    assert !node.visible() : "Added visible node [node=" + node + ", locNode=" + locNode + ']';

                    Map<Integer, byte[]> data = msg.newNodeDiscoveryData();

                    if (data != null)
                        spi.onExchange(node.id(), node.id(), data, U.resolveClassLoader(spi.ignite().configuration()));

                    msg.addDiscoveryData(locNodeId, spi.collectExchangeData(node.id()));

                    processMessageFailedNodes(msg);
                }

                if (log.isDebugEnabled())
                    log.debug("Added node to local ring [added=" + topChanged + ", node=" + node +
                        ", ring=" + ring + ']');
            }

            if (msg.verified() && locNodeId.equals(node.id())) {
                // Discovery data.
                Map<UUID, Map<Integer, byte[]>> dataMap;

                synchronized (mux) {
                    if (spiState == CONNECTING && locNode.internalOrder() != node.internalOrder()) {
                        // Initialize topology.
                        Collection<TcpDiscoveryNode> top = msg.topology();

                        if (top != null && !top.isEmpty()) {
                            spi.gridStartTime = msg.gridStartTime();

                            if (spi.nodeAuth != null && spi.nodeAuth.isGlobalNodeAuthentication()) {
                                TcpDiscoveryAbstractMessage authFail =
                                    new TcpDiscoveryAuthFailedMessage(locNodeId, spi.locHost);

                                try {
                                    ClassLoader cl = U.resolveClassLoader(spi.ignite().configuration());

                                    byte[] rmSubj = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);
                                    byte[] locSubj = locNode.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);

                                    SecurityContext rmCrd = spi.marshaller().unmarshal(rmSubj, cl);
                                    SecurityContext locCrd = spi.marshaller().unmarshal(locSubj, cl);

                                    if (!permissionsEqual(locCrd.subject().permissions(),
                                        rmCrd.subject().permissions())) {
                                        // Node has not pass authentication.
                                        LT.warn(log,
                                            "Failed to authenticate local node " +
                                                "(local authentication result is different from rest of topology) " +
                                                "[nodeId=" + node.id() + ", addrs=" + U.addressesAsString(node) + ']',
                                            "Authentication failed [nodeId=" + U.id8(node.id()) +
                                                ", addrs=" + U.addressesAsString(node) + ']');

                                        joinRes.set(authFail);

                                        spiState = AUTH_FAILED;

                                        mux.notifyAll();

                                        return;
                                    }
                                }
                                catch (IgniteCheckedException e) {
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
                            }

                            synchronized (mux) {
                                joiningNodes.clear();
                            }

                            locNode.setAttributes(node.attributes());

                            locNode.visible(true);

                            // Restore topology with all nodes visible.
                            ring.restoreTopology(top, node.internalOrder());

                            if (log.isDebugEnabled())
                                log.debug("Restored topology from node added message: " + ring);

                            dataMap = msg.oldNodesDiscoveryData();

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

                            return;
                        }
                    }
                    else  {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node added message (this message has already been processed) " +
                                "[spiState=" + spiState +
                                ", msg=" + msg +
                                ", locNode=" + locNode + ']');

                        return;
                    }
                }

                // Notify outside of synchronized block.
                if (dataMap != null) {
                    for (Map.Entry<UUID, Map<Integer, byte[]>> entry : dataMap.entrySet())
                        spi.onExchange(node.id(), entry.getKey(), entry.getValue(),
                            U.resolveClassLoader(spi.ignite().configuration()));
                }

                processMessageFailedNodes(msg);
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
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

            boolean locNodeCoord = isLocalNodeCoordinator();

            UUID locNodeId = getLocalNodeId();

            if (locNodeCoord) {
                if (msg.verified()) {
                    spi.stats.onRingMessageReceived(msg);

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

                if (state == CONNECTED)
                    notifyDiscovery(EVT_NODE_JOINED, topVer, node);

                try {
                    if (spi.ipFinder.isShared() && locNodeCoord && !node.isClient())
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

                // Discovery manager must create local joined event before spiStart completes.
                notifyDiscovery(EVT_NODE_JOINED, topVer, locNode);
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);

            checkPendingCustomMessages();
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

                return;
            }

            boolean locNodeCoord = isLocalNodeCoordinator();

            if (locNodeCoord) {
                if (msg.verified()) {
                    spi.stats.onRingMessageReceived(msg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified() && !locNodeId.equals(leavingNodeId)) {
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

                notifyDiscovery(EVT_NODE_LEFT, topVer, leftNode);

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
            long order = msg.order();

            TcpDiscoveryNode failedNode = ring.node(failedNodeId);

            if (failedNode != null && failedNode.internalOrder() != order) {
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
                    spi.stats.onRingMessageReceived(msg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified()) {
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

                        if (worker != null)
                            worker.interrupt();
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

                notifyDiscovery(EVT_NODE_FAILED, topVer, failedNode);

                spi.stats.onNodeFailed();
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
            else {
                if (log.isDebugEnabled())
                    log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

                U.closeQuiet(sock);
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
                                if (spiState == DISCONNECTED) {
                                    if (log.isDebugEnabled())
                                        log.debug("Ignoring status check request, SPI is already disconnected: " + msg);

                                    return;
                                }

                                TcpDiscoveryStatusCheckMessage msg0 = msg;

                                if (F.contains(msg.failedNodes(), msg.creatorNodeId())) {
                                    msg0 = new TcpDiscoveryStatusCheckMessage(msg);

                                    msg0.failedNodes(null);

                                    for (UUID failedNodeId : msg.failedNodes()) {
                                        if (!failedNodeId.equals(msg.creatorNodeId()))
                                            msg0.addFailedNode(failedNodeId);
                                    }
                                }

                                try {
                                    trySendMessageDirectly(msg0.creatorNode(), msg0);

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
                                        if (pingNode(msg0.creatorNode()))
                                            // Node exists and accepts incoming connections.
                                            U.error(log, "Failed to respond to status check message [recipient=" +
                                                msg0.creatorNodeId() + ", status=" + msg0.status() + ']', e);
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
                    U.currentTimeMillis() - locNode.lastUpdateTime() < spi.hbFreq) {
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
         * Processes regular heartbeat message.
         *
         * @param msg Heartbeat message.
         */
        private void processHeartbeatMessage(TcpDiscoveryHeartbeatMessage msg) {
            assert msg != null;

            assert !msg.client();

            UUID locNodeId = getLocalNodeId();

            if (ring.node(msg.creatorNodeId()) == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message issued by unknown node [msg=" + msg +
                        ", ring=" + ring + ']');

                return;
            }

            if (isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message issued by non-coordinator node: " + msg);

                return;
            }

            if (!isLocalNodeCoordinator() && locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message issued by local node (node is no more coordinator): " +
                        msg);

                return;
            }

            if (locNodeId.equals(msg.creatorNodeId()) && !hasMetrics(msg, locNodeId) && msg.senderNodeId() != null) {
                if (log.isTraceEnabled())
                    log.trace("Discarding heartbeat message that has made two passes: " + msg);

                return;
            }

            long tstamp = U.currentTimeMillis();

            if (spiStateCopy() == CONNECTED) {
                if (msg.hasMetrics()) {
                    for (Map.Entry<UUID, TcpDiscoveryHeartbeatMessage.MetricsSet> e : msg.metrics().entrySet()) {
                        UUID nodeId = e.getKey();

                        TcpDiscoveryHeartbeatMessage.MetricsSet metricsSet = e.getValue();

                        Map<Integer, CacheMetrics> cacheMetrics = msg.hasCacheMetrics(nodeId) ?
                            msg.cacheMetrics().get(nodeId) : Collections.<Integer, CacheMetrics>emptyMap();

                        updateMetrics(nodeId, metricsSet.metrics(), cacheMetrics, tstamp);

                        for (T2<UUID, ClusterMetrics> t : metricsSet.clientMetrics())
                            updateMetrics(t.get1(), t.get2(), cacheMetrics, tstamp);
                    }
                }
            }

            if (sendMessageToRemotes(msg)) {
                if ((locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null ||
                    !hasMetrics(msg, locNodeId)) && spiStateCopy() == CONNECTED) {
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
                                clientNode.aliveCheck(spi.maxMissedClientHbs);
                            else {
                                int aliveCheck = clientNode.decrementAliveCheck();

                                if (aliveCheck <= 0 && isLocalNodeCoordinator()) {
                                    boolean failedNode;

                                    synchronized (mux) {
                                        failedNode = failedNodes.containsKey(clientNode);
                                    }

                                    if (!failedNode) {
                                        TcpDiscoveryNodeFailedMessage nodeFailedMsg = new TcpDiscoveryNodeFailedMessage(
                                            locNodeId, clientNode.id(), clientNode.internalOrder());

                                        processNodeFailedMessage(nodeFailedMsg);

                                        if (nodeFailedMsg.verified())
                                            msgHist.add(nodeFailedMsg);
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
                locNode.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), locNode);
            }
        }

        /**
         * @param nodeId Node ID.
         * @param metrics Metrics.
         * @param cacheMetrics Cache metrics.
         * @param tstamp Timestamp.
         */
        private void updateMetrics(UUID nodeId,
            ClusterMetrics metrics,
            Map<Integer, CacheMetrics> cacheMetrics,
            long tstamp)
        {
            assert nodeId != null;
            assert metrics != null;

            TcpDiscoveryNode node = ring.node(nodeId);

            if (node != null) {
                node.setMetrics(metrics);
                node.setCacheMetrics(cacheMetrics);

                node.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), node);
            }
            else if (log.isDebugEnabled())
                log.debug("Received metrics from unknown node: " + nodeId);
        }

        /**
         * @param msg Message.
         */
        private boolean hasMetrics(TcpDiscoveryHeartbeatMessage msg, UUID nodeId) {
            return msg.hasMetrics(nodeId) || msg.hasCacheMetrics(nodeId);
        }

        /**
         * Processes discard message and discards previously registered pending messages.
         *
         * @param msg Discard message.
         */
        @SuppressWarnings("StatementWithEmptyBody")
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
                    if (spiState == DISCONNECTED) {
                        if (log.isDebugEnabled())
                            log.debug("Ignoring ping request, SPI is already disconnected: " + msg);

                        return;
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
         */
        private void processCustomMessage(TcpDiscoveryCustomEventMessage msg) {
            if (isLocalNodeCoordinator()) {
                boolean delayMsg;

                assert ring.minimumNodeVersion() != null : ring;

                boolean joiningEmpty;

                synchronized (mux) {
                    joiningEmpty = joiningNodes.isEmpty();
                }

                if (ring.minimumNodeVersion().compareTo(CUSTOM_MSG_ALLOW_JOINING_FOR_VERIFIED_SINCE) >= 0)
                    delayMsg = msg.topologyVersion() == 0L && !joiningEmpty;
                else
                    delayMsg = !joiningEmpty;

                if (delayMsg) {
                    if (log.isDebugEnabled()) {
                        synchronized (mux) {
                            log.debug("Delay custom message processing, there are joining nodes [msg=" + msg +
                                ", joiningNodes=" + joiningNodes + ']');
                        }
                    }

                    synchronized (mux) {
                        pendingCustomMsgs.add(msg);
                    }

                    return;
                }

                if (!msg.verified()) {
                    msg.verify(getLocalNodeId());
                    msg.topologyVersion(ring.topologyVersion());

                    if (pendingMsgs.procCustomMsgs.add(msg.id())) {
                        notifyDiscoveryListener(msg);

                        if (sendMessageToRemotes(msg))
                            sendMessageAcrossRing(msg);
                        else
                            processCustomMessage(msg);
                    }

                    msg.message(null, msg.messageBytes());
                }
                else {
                    addMessage(new TcpDiscoveryDiscardMessage(getLocalNodeId(), msg.id(), true));

                    spi.stats.onRingMessageReceived(msg);

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

                                processCustomMessage(ackMsg);

                                if (ackMsg.verified())
                                    msgHist.add(ackMsg);
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

                    notifyDiscoveryListener(msg);
                }

                if (msg.verified())
                    msg.message(null, msg.messageBytes());

                if (sendMessageToRemotes(msg))
                    sendMessageAcrossRing(msg);
            }
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
                    if (log.isDebugEnabled())
                        log.debug("Add node failed message for node from failed nodes list: " + msg);

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

                while ((msg = pollPendingCustomeMessage()) != null) {
                    processCustomMessage(msg);

                    if (msg.verified())
                        msgHist.add(msg);
                }
            }
        }

        /**
         * @return Pending custom message.
         */
        @Nullable private TcpDiscoveryCustomEventMessage pollPendingCustomeMessage() {
            synchronized (mux) {
                return pendingCustomMsgs.poll();
            }
        }

        /**
         * @param msg Custom message.
         */
        private void notifyDiscoveryListener(TcpDiscoveryCustomEventMessage msg) {
            DiscoverySpiListener lsnr = spi.lsnr;

            TcpDiscoverySpiState spiState = spiStateCopy();

            Map<Long, Collection<ClusterNode>> hist;

            synchronized (mux) {
                hist = new TreeMap<>(topHist);
            }

            Collection<ClusterNode> snapshot = hist.get(msg.topologyVersion());

            if (lsnr != null && (spiState == CONNECTED || spiState == DISCONNECTING)) {
                TcpDiscoveryNode node = ring.node(msg.creatorNodeId());

                if (node != null) {
                    try {
                        DiscoverySpiCustomMessage msgObj = msg.message(spi.marshaller(),
                            U.resolveClassLoader(spi.ignite().configuration()));

                        lsnr.onDiscovery(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
                            msg.topologyVersion(),
                            node,
                            snapshot,
                            hist,
                            msgObj);

                        if (msgObj.isMutable())
                            msg.message(msgObj, U.marshal(spi.marshaller(), msgObj));
                    }
                    catch (Throwable e) {
                        U.error(log, "Failed to unmarshal discovery custom message.", e);
                    }
                }
            }
        }

        /**
         * Sends heartbeat message if needed.
         */
        private void sendHeartbeatMessage() {
            long elapsed = (lastTimeHbMsgSent + spi.hbFreq) - U.currentTimeMillis();

            if (elapsed > 0 || !isLocalNodeCoordinator())
                return;

            TcpDiscoveryHeartbeatMessage msg = new TcpDiscoveryHeartbeatMessage(getConfiguredNodeId());

            msg.verify(getLocalNodeId());

            msgWorker.addMessage(msg);

            lastTimeHbMsgSent = U.currentTimeMillis();
        }

        /**
         * Check the last time a heartbeat message received. If the time is bigger than {@code hbCheckTimeout} than
         * {@link TcpDiscoveryStatusCheckMessage} is sent across the ring.
         */
        private void checkHeartbeatsReceiving() {
            if (lastTimeStatusMsgSent < locNode.lastUpdateTime())
                lastTimeStatusMsgSent = locNode.lastUpdateTime();

            long updateTime = Math.max(lastTimeStatusMsgSent, lastRingMsgTime);

            long elapsed = (updateTime + hbCheckFreq) - U.currentTimeMillis();

            if (elapsed > 0)
                return;

            msgWorker.addMessage(new TcpDiscoveryStatusCheckMessage(locNode, null));

            lastTimeStatusMsgSent = U.currentTimeMillis();
        }

        /**
         * Check connection aliveness status.
         */
        private void checkConnection() {
            Boolean hasRemoteSrvNodes = null;

            if (spi.failureDetectionTimeoutEnabled() && !failureThresholdReached &&
                U.currentTimeMillis() - locNode.lastExchangeTime() >= connCheckThreshold &&
                spiStateCopy() == CONNECTED &&
                (hasRemoteSrvNodes = ring.hasRemoteServerNodes())) {

                log.info("Local node seems to be disconnected from topology (failure detection timeout " +
                    "is reached) [failureDetectionTimeout=" + spi.failureDetectionTimeout() +
                    ", connCheckFreq=" + connCheckFreq + ']');

                failureThresholdReached = true;

                // Reset sent time deliberately to force sending connection check message.
                lastTimeConnCheckMsgSent = 0;
            }

            long elapsed = (lastTimeConnCheckMsgSent + connCheckFreq) - U.currentTimeMillis();

            if (elapsed > 0)
                return;

            if (hasRemoteSrvNodes == null)
                hasRemoteSrvNodes = ring.hasRemoteServerNodes();

            if (hasRemoteSrvNodes) {
                sendMessageAcrossRing(new TcpDiscoveryConnectionCheckMessage(locNode));

                lastTimeConnCheckMsgSent = U.currentTimeMillis();
            }
        }
    }

    /**
     * Thread that accepts incoming TCP connections.
     * <p>
     * Tcp server will call provided closure when accepts incoming connection.
     * From that moment server is no more responsible for the socket.
     */
    private class TcpServer extends IgniteSpiThread {
        /** Socket TCP server listens to. */
        private ServerSocket srvrSock;

        /** Port to listen. */
        private int port;

        /**
         * Constructor.
         *
         * @throws IgniteSpiException In case of error.
         */
        TcpServer() throws IgniteSpiException {
            super(spi.ignite().name(), "tcp-disco-srvr", log);

            setPriority(spi.threadPri);

            int lastPort = spi.locPortRange == 0 ? spi.locPort : spi.locPort + spi.locPortRange - 1;

            for (port = spi.locPort; port <= lastPort; port++) {
                try {
                    if (spi.isSslEnabled())
                        srvrSock = spi.sslSrvSockFactory.createServerSocket(port, 0, spi.locHost);
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

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                while (!isInterrupted()) {
                    Socket sock = srvrSock.accept();

                    long tstamp = U.currentTimeMillis();

                    if (log.isDebugEnabled())
                        log.debug("Accepted incoming connection from addr: " + sock.getInetAddress());

                    SocketReader reader = new SocketReader(sock);

                    synchronized (mux) {
                        readers.add(reader);
                    }

                    reader.start();

                    spi.stats.onServerSocketInitialized(U.currentTimeMillis() - tstamp);
                }
            }
            catch (IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Failed to accept TCP connection.", e);

                onException("Failed to accept TCP connection.", e);

                if (!isInterrupted()) {
                    if (U.isMacInvalidArgumentError(e))
                        U.error(log, "Failed to accept TCP connection\n\t" + U.MAC_INVALID_ARG_MSG, e);
                    else
                        U.error(log, "Failed to accept TCP connection.", e);
                }
            }
            finally {
                U.closeQuiet(srvrSock);
            }
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            super.interrupt();

            U.close(srvrSock, log);
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
            super(spi.ignite().name(), "tcp-disco-sock-reader", log);

            this.sock = sock;

            setPriority(spi.threadPri);

            spi.stats.onSocketReaderCreated();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            UUID locNodeId = getConfiguredNodeId();

            ClientMessageWorker clientMsgWrk = null;

            try {
                InputStream in;

                try {
                    // Set socket options.
                    sock.setKeepAlive(true);
                    sock.setTcpNoDelay(true);

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
                                    "[rmtAddr=" + sock.getRemoteSocketAddress() +
                                    ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            LT.warn(log, "Failed to read magic header (too few bytes received) [rmtAddr=" +
                                sock.getRemoteSocketAddress() + ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            return;
                        }
                    }

                    if (!Arrays.equals(buf, U.IGNITE_HEADER)) {
                        if (log.isDebugEnabled())
                            log.debug("Unknown connection detected (is some other software connecting to " +
                                "this Ignite port?" +
                                (!spi.isSslEnabled() ? " missed SSL configuration?" : "" ) +
                                ") " +
                                "[rmtAddr=" + sock.getRemoteSocketAddress() +
                                ", locAddr=" + sock.getLocalSocketAddress() + ']');

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

                            TcpDiscoveryPingResponse res = new TcpDiscoveryPingResponse(locNodeId);

                            IgniteSpiOperationTimeoutHelper timeoutHelper =
                                new IgniteSpiOperationTimeoutHelper(spi);

                            if (req.clientNodeId() != null) {
                                ClientMessageWorker clientWorker = clientMsgWorkers.get(req.clientNodeId());

                                if (clientWorker != null)
                                    res.clientExists(clientWorker.ping(timeoutHelper));
                            }

                            spi.writeToSocket(sock, res, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Ignore ping request, node is stopping.");

                        return;
                    }

                    // Handshake.
                    TcpDiscoveryHandshakeRequest req = (TcpDiscoveryHandshakeRequest)msg;

                    UUID nodeId = req.creatorNodeId();

                    this.nodeId = nodeId;

                    TcpDiscoveryHandshakeResponse res =
                        new TcpDiscoveryHandshakeResponse(locNodeId, locNode.internalOrder());

                    if (req.client())
                        res.clientAck(true);

                    spi.writeToSocket(sock, res, spi.failureDetectionTimeoutEnabled() ?
                        spi.failureDetectionTimeout() : spi.getSocketTimeout());

                    // It can happen if a remote node is stopped and it has a loopback address in the list of addresses,
                    // the local node sends a handshake request message on the loopback address, so we get here.
                    if (locNodeId.equals(nodeId)) {
                        assert !req.client();

                        if (log.isDebugEnabled())
                            log.debug("Handshake request from local node: " + req);

                        return;
                    }

                    if (req.client()) {
                        ClientMessageWorker clientMsgWrk0 = new ClientMessageWorker(sock, nodeId);

                        while (true) {
                            ClientMessageWorker old = clientMsgWorkers.putIfAbsent(nodeId, clientMsgWrk0);

                            if (old == null)
                                break;

                            if (old.isInterrupted()) {
                                clientMsgWorkers.remove(nodeId, old);

                                continue;
                            }

                            old.join(500);

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

                    if (log.isDebugEnabled())
                        log.debug("Initialized connection with remote node [nodeId=" + nodeId +
                            ", client=" + req.client() + ']');

                    if (debugMode) {
                        debugLog(msg, "Initialized connection with remote node [nodeId=" + nodeId +
                            ", client=" + req.client() + ']');
                    }
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    if (X.hasCause(e, SSLException.class) && spi.isSslEnabled() && !spi.isNodeStopping0())
                        LT.warn(log, "Failed to initialize connection " +
                            "(missing SSL configuration on remote node?) " +
                            "[rmtAddr=" + sock.getInetAddress() + ']', true);
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
                        U.error(log, "Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    onException("Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    if (e.hasCause(SocketTimeoutException.class))
                        LT.warn(log, "Socket operation timed out on handshake " +
                            "(consider increasing 'networkTimeout' configuration property) " +
                            "[netTimeout=" + spi.netTimeout + ']');

                    else if (e.hasCause(ClassNotFoundException.class))
                        LT.warn(log, "Failed to read message due to ClassNotFoundException " +
                            "(make sure same versions of all classes are available on all nodes) " +
                            "[rmtAddr=" + sock.getRemoteSocketAddress() +
                            ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');

                        // Always report marshalling problems.
                    else if (e.hasCause(ObjectStreamException.class) ||
                        (!sock.isClosed() && !e.hasCause(IOException.class)))
                        LT.error(log, e, "Failed to initialize connection [sock=" + sock + ']');

                    return;
                }

                long sockTimeout = spi.failureDetectionTimeoutEnabled() ? spi.failureDetectionTimeout() :
                    spi.getSocketTimeout();

                while (!isInterrupted()) {
                    try {
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
                            if (clientMsgWrk != null) {
                                TcpDiscoverySpiState state = spiStateCopy();

                                if (state == CONNECTED) {
                                    spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                                    if (clientMsgWrk.getState() == State.NEW)
                                        clientMsgWrk.start();

                                    msgWorker.addMessage(msg);

                                    continue;
                                }
                                else {
                                    spi.writeToSocket(msg, sock, RES_CONTINUE_JOIN, sockTimeout);

                                    break;
                                }
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

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = AUTH_FAILED;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Auth failed message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

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
                        if (msg instanceof TcpDiscoveryPingResponse) {
                            assert msg.client() : msg;

                            ClientMessageWorker clientWorker = clientMsgWorkers.get(msg.creatorNodeId());

                            if (clientWorker != null)
                                clientWorker.pingResult(true);

                            continue;
                        }

                        TcpDiscoveryClientHeartbeatMessage heartbeatMsg = null;

                        if (msg instanceof TcpDiscoveryClientHeartbeatMessage)
                            heartbeatMsg = (TcpDiscoveryClientHeartbeatMessage)msg;
                        else
                            msgWorker.addMessage(msg);

                        // Send receipt back.
                        if (clientMsgWrk != null) {
                            TcpDiscoveryClientAckResponse ack = new TcpDiscoveryClientAckResponse(locNodeId, msg.id());

                            ack.verify(locNodeId);

                            clientMsgWrk.addMessage(ack);
                        }
                        else
                            spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                        if (heartbeatMsg != null)
                            processClientHeartbeatMessage(heartbeatMsg);
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
                }
            }
            finally {
                if (clientMsgWrk != null) {
                    if (log.isDebugEnabled())
                        log.debug("Client connection failed [sock=" + sock + ", locNodeId=" + locNodeId +
                            ", rmtNodeId=" + nodeId + ']');

                    clientMsgWorkers.remove(nodeId, clientMsgWrk);

                    U.interrupt(clientMsgWrk);
                }

                U.closeQuiet(sock);
            }
        }

        /**
         * Processes client heartbeat message.
         *
         * @param msg Heartbeat message.
         */
        private void processClientHeartbeatMessage(TcpDiscoveryClientHeartbeatMessage msg) {
            assert msg.client();

            ClientMessageWorker wrk = clientMsgWorkers.get(msg.creatorNodeId());

            if (wrk != null)
                wrk.metrics(msg.metrics());
            else if (log.isDebugEnabled())
                log.debug("Received heartbeat message from unknown client node: " + msg);
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
                spi.writeToSocket(msg, sock, RES_OK, sockTimeout);

                if (log.isDebugEnabled())
                    log.debug("Responded to join request message [msg=" + msg + ", res=" + RES_OK + ']');

                msg.responded(true);

                if (clientMsgWrk != null && clientMsgWrk.getState() == State.NEW) {
                    clientMsgWrk.clientVersion(U.productVersion(msg.node()));

                    clientMsgWrk.start();
                }

                msgWorker.addMessage(msg);

                return true;
            }
            else {
                spi.stats.onMessageProcessingStarted(msg);

                Integer res;

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

            spi.stats.onSocketReaderRemoved();
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

    /**
     */
    private class ClientMessageWorker extends MessageWorkerAdapter<T2<TcpDiscoveryAbstractMessage, byte[]>> {
        /** Node ID. */
        private final UUID clientNodeId;

        /** Socket. */
        private final Socket sock;

        /** Current client metrics. */
        private volatile ClusterMetrics metrics;

        /** */
        private final AtomicReference<GridFutureAdapter<Boolean>> pingFut = new AtomicReference<>();

        /** */
        private IgniteProductVersion clientVer;

        /**
         * @param sock Socket.
         * @param clientNodeId Node ID.
         */
        protected ClientMessageWorker(Socket sock, UUID clientNodeId) throws IOException {
            super("tcp-disco-client-message-worker", 2000);

            this.sock = sock;
            this.clientNodeId = clientNodeId;
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
            T2 t = new T2<>(msg, msgBytes);

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

                    if (clientVer != null &&
                        clientVer.compareTo(TcpDiscoveryClientAckResponse.CLIENT_ACK_SINCE_VERSION) >= 0) {
                        if (msgLog.isDebugEnabled())
                            msgLog.debug("Sending message ack to client [sock=" + sock + ", locNodeId="
                                + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');

                        spi.writeToSocket(sock, msg, msgBytes, spi.failureDetectionTimeoutEnabled() ?
                            spi.failureDetectionTimeout() : spi.getSocketTimeout());
                    }
                }
                else {
                    if (msgLog.isDebugEnabled())
                        msgLog.debug("Redirecting message to client [sock=" + sock + ", locNodeId="
                            + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');

                    assert topologyInitialized(msg) : msg;

                    spi.writeToSocket(sock, msg, msgBytes, spi.failureDetectionTimeoutEnabled() ?
                        spi.failureDetectionTimeout() : spi.getSocketTimeout());
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

                    U.interrupt(this);

                    U.closeQuiet(sock);
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
        @Override protected void cleanup() {
            super.cleanup();

            pingResult(false);

            U.closeQuiet(sock);
        }
    }

    /**
     * Base class for message workers.
     */
    protected abstract class MessageWorkerAdapter<T> extends IgniteSpiThread {
        /** Message queue. */
        protected final BlockingDeque<T> queue = new LinkedBlockingDeque<>();

        /** Backed interrupted flag. */
        private volatile boolean interrupted;

        /** Polling timeout. */
        private final long pollingTimeout;

        /**
         * @param name Thread name.
         * @param pollingTimeout Messages polling timeout.
         */
        protected MessageWorkerAdapter(String name, long pollingTimeout) {
            super(spi.ignite().name(), name, log);

            this.pollingTimeout = pollingTimeout;

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Message worker started [locNodeId=" + getConfiguredNodeId() + ']');

            while (!isInterrupted()) {
                T msg = queue.poll(pollingTimeout, TimeUnit.MILLISECONDS);

                if (msg == null)
                    noMessageLoop();
                else
                    processMessage(msg);
            }
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

        /**
         * @return Current queue size.
         */
        int queueSize() {
            return queue.size();
        }

        /**
         * @param msg Message.
         */
        protected abstract void processMessage(T msg);

        /**
         * Called when there is no message to process giving ability to perform other activity.
         */
        protected void noMessageLoop() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class GridPingFutureAdapter<R> extends GridFutureAdapter<R> {
        /** Socket. */
        private volatile Socket sock;

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
}
