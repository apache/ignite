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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.events.*;
import org.apache.ignite.internal.processors.security.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.messages.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;
import static org.apache.ignite.spi.IgnitePortProtocol.*;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.*;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.*;

/**
 *
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
class ServerImpl extends TcpDiscoveryImpl {
    /** */
    private final ThreadPoolExecutor utilityPool = new ThreadPoolExecutor(0, 1, 2000, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());

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

    /** Metrics sender. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private HeartbeatsSender hbsSnd;

    /** Status checker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private CheckStatusSender chkStatusSnd;

    /** IP finder cleaner. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private IpFinderCleaner ipFinderCleaner;

    /** Statistics printer thread. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private StatisticsPrinter statsPrinter;

    /** Failed nodes (but still in topology). */
    private Collection<TcpDiscoveryNode> failedNodes = new HashSet<>();

    /** Leaving nodes (but still in topology). */
    private Collection<TcpDiscoveryNode> leavingNodes = new HashSet<>();

    /** If non-shared IP finder is used this flag shows whether IP finder contains local address. */
    private boolean ipFinderHasLocAddr;

    /** Addresses that do not respond during join requests send (for resolving concurrent start). */
    private final Collection<SocketAddress> noResAddrs = new GridConcurrentHashSet<>();

    /** Addresses that incoming join requests send were send from (for resolving concurrent start). */
    private final Collection<SocketAddress> fromAddrs = new GridConcurrentHashSet<>();

    /** Response on join request from coordinator (in case of duplicate ID or auth failure). */
    private final GridTuple<TcpDiscoveryAbstractMessage> joinRes = F.t1();

    /** Mutex. */
    private final Object mux = new Object();

    /** Discovery state. */
    protected TcpDiscoverySpiState spiState = DISCONNECTED;

    /** Map with proceeding ping requests. */
    private final ConcurrentMap<InetSocketAddress, IgniteInternalFuture<IgniteBiTuple<UUID, Boolean>>> pingMap =
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
        return F.upcast(ring.visibleRemoteNodes());
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        synchronized (mux) {
            spiState = DISCONNECTED;
        }

        if (debugMode) {
            if (!log.isInfoEnabled())
                throw new IgniteSpiException("Info log level should be enabled for TCP discovery to work " +
                    "in debug mode.");

            debugLog = new ConcurrentLinkedDeque<>();

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
                    "GridTcpDiscoveryIpFinder.getRegisteredAddresses() configuration property " +
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

        hbsSnd = new HeartbeatsSender();
        hbsSnd.start();

        chkStatusSnd = new CheckStatusSender();
        chkStatusSnd.start();

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
                long threshold = U.currentTimeMillis() + spi.netTimeout;

                long timeout = spi.netTimeout;

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

        U.interrupt(hbsSnd);
        U.join(hbsSnd, log);

        U.interrupt(chkStatusSnd);
        U.join(chkStatusSnd, log);

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

        if (node == null || !node.visible())
            return false;

        boolean res = pingNode(node);

        if (!res && !node.isClient()) {
            LT.warn(log, null, "Failed to ping node (status check will be initiated): " + nodeId);

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

            if (node == null || !node.visible())
                return false;
        }

        for (InetSocketAddress addr : spi.getNodeAddresses(node, U.sameMacs(locNode, node))) {
            try {
                // ID returned by the node should be the same as ID of the parameter for ping to succeed.
                IgniteBiTuple<UUID, Boolean> t = pingNode(addr, clientNodeId);

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
     * @param clientNodeId Client node ID.
     * @return ID of the remote node and "client exists" flag if node alive.
     * @throws IgniteCheckedException If an error occurs.
     */
    private IgniteBiTuple<UUID, Boolean> pingNode(InetSocketAddress addr, @Nullable UUID clientNodeId)
        throws IgniteCheckedException {
        assert addr != null;

        UUID locNodeId = getLocalNodeId();

        if (F.contains(spi.locNodeAddrs, addr)) {
            if (clientNodeId == null)
                return F.t(getLocalNodeId(), false);

            ClientMessageWorker clientWorker = clientMsgWorkers.get(clientNodeId);

            if (clientWorker == null)
                return F.t(getLocalNodeId(), false);

            boolean clientPingRes;

            try {
                clientPingRes = clientWorker.ping();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedCheckedException(e);
            }

            return F.t(getLocalNodeId(), clientPingRes);
        }

        GridFutureAdapter<IgniteBiTuple<UUID, Boolean>> fut = new GridFutureAdapter<>();

        IgniteInternalFuture<IgniteBiTuple<UUID, Boolean>> oldFut = pingMap.putIfAbsent(addr, fut);

        if (oldFut != null)
            return oldFut.get();
        else {
            Collection<Throwable> errs = null;

            try {
                Socket sock = null;

                for (int i = 0; i < spi.reconCnt; i++) {
                    try {
                        if (addr.isUnresolved())
                            addr = new InetSocketAddress(InetAddress.getByName(addr.getHostName()), addr.getPort());

                        long tstamp = U.currentTimeMillis();

                        sock = spi.openSocket(addr);

                        spi.writeToSocket(sock, new TcpDiscoveryPingRequest(locNodeId, clientNodeId));

                        TcpDiscoveryPingResponse res = spi.readMessage(sock, null, spi.netTimeout);

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
                        if (errs == null)
                            errs = new ArrayList<>();

                        errs.add(e);
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

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        spiStop0(true);
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage evt) {
        try {
            msgWorker.addMessage(new TcpDiscoveryCustomEventMessage(getLocalNodeId(), evt, spi.marsh.marshal(evt)));
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

            msgWorker.addMessage(msg);
        }
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

        // Marshal credentials for backward compatibility and security.
        marshalCredentials(locNode);

        while (true) {
            if (!sendJoinRequestMessage()) {
                if (log.isDebugEnabled())
                    log.debug("Join request message has not been sent (local node is the first in the topology).");

                if (spi.nodeAuth != null) {
                    // Authenticate local node.
                    try {
                        SecurityContext subj = spi.nodeAuth.authenticateNode(locNode, locCred);

                        if (subj == null)
                            throw new IgniteSpiException("Authentication failed for local node: " + locNode.id());

                        Map<String, Object> attrs = new HashMap<>(locNode.attributes());

                        attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT, spi.marsh.marshal(subj));
                        attrs.remove(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

                        locNode.setAttributes(attrs);
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        throw new IgniteSpiException("Failed to authenticate local node (will shutdown local node).", e);
                    }
                }

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
                long threshold = U.currentTimeMillis() + spi.netTimeout;

                long timeout = spi.netTimeout;

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
                    LT.warn(log, null, "Node has not been connected to topology and will repeat join process. " +
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

                if (e != null && X.hasCause(e, ConnectException.class))
                    LT.warn(log, null, "Failed to connect to any address from IP finder " +
                        "(make sure IP finder addresses are correct and firewalls are disabled on all host machines): " +
                        addrs);

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

        long ackTimeout0 = spi.ackTimeout;

        int connectAttempts = 1;

        boolean joinReqSent = false;

        UUID locNodeId = getLocalNodeId();

        for (int i = 0; i < spi.reconCnt; i++) {
            // Need to set to false on each new iteration,
            // since remote node may leave in the middle of the first iteration.
            joinReqSent = false;

            boolean openSock = false;

            Socket sock = null;

            try {
                long tstamp = U.currentTimeMillis();

                sock = spi.openSocket(addr);

                openSock = true;

                // Handshake.
                spi.writeToSocket(sock, new TcpDiscoveryHandshakeRequest(locNodeId));

                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null, ackTimeout0);

                if (locNodeId.equals(res.creatorNodeId())) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake response from local node: " + res);

                    break;
                }

                spi.stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                // Send message.
                tstamp = U.currentTimeMillis();

                spi.writeToSocket(sock, msg);

                spi.stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                if (debugMode)
                    debugLog("Message has been sent directly to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + res.creatorNodeId() + ']');

                if (log.isDebugEnabled())
                    log.debug("Message has been sent directly to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + res.creatorNodeId() + ']');

                // Connection has been established, but
                // join request may not be unmarshalled on remote host.
                // E.g. due to class not found issue.
                joinReqSent = msg instanceof TcpDiscoveryJoinRequestMessage;

                return spi.readReceipt(sock, ackTimeout0);
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

                if (!openSock) {
                    // Reconnect for the second time, if connection is not established.
                    if (connectAttempts < 2) {
                        connectAttempts++;

                        continue;
                    }

                    break; // Don't retry if we can not establish connection.
                }

                if (e instanceof SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class)) {
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
     * @throws IgniteSpiException If marshalling failed.
     */
    private void marshalCredentials(TcpDiscoveryNode node) throws IgniteSpiException {
        try {
            // Use security-unsafe getter.
            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS,
                spi.marsh.marshal(attrs.get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS)));

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

            return spi.marsh.unmarshal(credBytes, null);
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

        if (lsnr != null && node.visible() && (spiState == CONNECTED || spiState == DISCONNECTING)) {
            if (log.isDebugEnabled())
                log.debug("Discovery notification [node=" + node + ", spiState=" + spiState +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');

            Collection<ClusterNode> top = F.upcast(ring.visibleNodes());

            Map<Long, Collection<ClusterNode>> hist = updateTopologyHistory(topVer, top);

            lsnr.onDiscovery(type, topVer, node, top, hist, null);
        }
        else if (log.isDebugEnabled())
            log.debug("Skipped discovery notification [node=" + node + ", spiState=" + spiState +
                ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');
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
            Collection<TcpDiscoveryNode> excluded = F.concat(false, failedNodes, leavingNodes);

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

            synchronized (mux) {
                failedNodesSize = failedNodes.size();
                leavingNodesSize = leavingNodes.size();
            }

            Runtime runtime = Runtime.getRuntime();

            TcpDiscoveryNode coord = resolveCoordinator();

            log.info("Discovery SPI statistics [statistics=" + spi.stats + ", spiState=" + spiStateCopy() +
                ", coord=" + coord +
                ", topSize=" + ring.allNodes().size() +
                ", leavingNodesSize=" + leavingNodesSize + ", failedNodesSize=" + failedNodesSize +
                ", msgWorker.queue.size=" + (msgWorker != null ? msgWorker.queueSize() : "N/A") +
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
    private void prepareNodeAddedMessage(TcpDiscoveryAbstractMessage msg, UUID destNodeId,
        @Nullable Collection<TcpDiscoveryAbstractMessage> msgs, @Nullable IgniteUuid discardMsgId) {
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
                nodeAddedMsg.messages(msgs, discardMsgId);

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
            nodeAddedMsg.messages(null, null);
        }
    }

    /** {@inheritDoc} */
    @Override void simulateNodeFailure() {
        U.warn(log, "Simulating node failure: " + getLocalNodeId());

        U.interrupt(tcpSrvr);
        U.join(tcpSrvr, log);

        U.interrupt(hbsSnd);
        U.join(hbsSnd, log);

        U.interrupt(chkStatusSnd);
        U.join(chkStatusSnd, log);

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
            next = ring.nextNode(failedNodes);
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
            b.append("    Check status sender: ").append(threadStatus(chkStatusSnd)).append(U.nl());
            b.append("    HB sender: ").append(threadStatus(hbsSnd)).append(U.nl());
            b.append("    IP finder cleaner: ").append(threadStatus(ipFinderCleaner)).append(U.nl());
            b.append("    Stats printer: ").append(threadStatus(statsPrinter)).append(U.nl());

            b.append(U.nl());

            b.append("Socket readers: ").append(U.nl());

            for (SocketReader rdr : readers)
                b.append("    ").append(rdr).append(U.nl());

            b.append(U.nl());

            b.append("In-memory log messages: ").append(U.nl());

            for (String msg : debugLog)
                b.append("    ").append(msg).append(U.nl());

            b.append(U.nl());

            b.append("Leaving nodes: ").append(U.nl());

            for (TcpDiscoveryNode node : leavingNodes)
                b.append("    ").append(node.id()).append(U.nl());

            b.append(U.nl());

            b.append("Failed nodes: ").append(U.nl());

            for (TcpDiscoveryNode node : failedNodes)
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
            !(msg instanceof TcpDiscoveryDiscardMessage);
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
        boolean dfltAllowMatch = !(locPerms.defaultAllowAll() ^ rmtPerms.defaultAllowAll());

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
     * Thread that sends heartbeats.
     */
    private class HeartbeatsSender extends IgniteSpiThread {
        /**
         * Constructor.
         */
        private HeartbeatsSender() {
            super(spi.ignite().name(), "tcp-disco-hb-sender", log);

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            while (!isLocalNodeCoordinator())
                Thread.sleep(1000);

            if (log.isDebugEnabled())
                log.debug("Heartbeats sender has been started.");

            UUID nodeId = getConfiguredNodeId();

            while (!isInterrupted()) {
                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping heartbeats sender (SPI is not connected to topology).");

                    return;
                }

                TcpDiscoveryHeartbeatMessage msg = new TcpDiscoveryHeartbeatMessage(nodeId);

                msg.verify(getLocalNodeId());

                msgWorker.addMessage(msg);

                Thread.sleep(spi.hbFreq);
            }
        }
    }

    /**
     * Thread that sends status check messages to next node if local node has not
     * been receiving heartbeats ({@link TcpDiscoveryHeartbeatMessage})
     * for {@link TcpDiscoverySpi#getMaxMissedHeartbeats()} *
     * {@link TcpDiscoverySpi#getHeartbeatFrequency()}.
     */
    private class CheckStatusSender extends IgniteSpiThread {
        /**
         * Constructor.
         */
        private CheckStatusSender() {
            super(spi.ignite().name(), "tcp-disco-status-check-sender", log);

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Status check sender has been started.");

            // Only 1 heartbeat missing is acceptable. Add 50 ms to avoid false alarm.
            long checkTimeout = (long)spi.maxMissedHbs * spi.hbFreq + 50;

            long lastSent = 0;

            while (!isInterrupted()) {
                // 1. Determine timeout.
                if (lastSent < locNode.lastUpdateTime())
                    lastSent = locNode.lastUpdateTime();

                long timeout = (lastSent + checkTimeout) - U.currentTimeMillis();

                if (timeout > 0)
                    Thread.sleep(timeout);

                // 2. Check if SPI is still connected.
                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping status check sender (SPI is not connected to topology).");

                    return;
                }

                // 3. Was there an update?
                if (locNode.lastUpdateTime() > lastSent || !ring.hasRemoteNodes()) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping status check send " +
                            "[locNodeLastUpdate=" + U.format(locNode.lastUpdateTime()) +
                            ", hasRmts=" + ring.hasRemoteNodes() + ']');

                    continue;
                }

                // 4. Send status check message.
                lastSent = U.currentTimeMillis();

                msgWorker.addMessage(new TcpDiscoveryStatusCheckMessage(locNode, null));
            }
        }
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
                                res = pingNode(addr, null).get1() != null;
                            }
                            catch (IgniteCheckedException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to ping node [addr=" + addr +
                                        ", err=" + e.getMessage() + ']');

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
     * Discovery messages history used for client reconnect.
     */
    private class EnsuredMessageHistory {
        /** */
        private static final int MAX = 1024;

        /** Pending messages. */
        private final ArrayDeque<TcpDiscoveryAbstractMessage> msgs = new ArrayDeque<>(MAX * 2);

        /**
         * @param msg Adds message.
         */
        void add(TcpDiscoveryAbstractMessage msg) {
            assert spi.ensured(msg) : msg;

            msgs.addLast(msg);

            while (msgs.size() > MAX)
                msgs.pollFirst();
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
                        if (node.id().equals(((TcpDiscoveryNodeAddedMessage) msg).node().id()))
                            res = new ArrayList<>(msgs.size());
                    }

                    if (res != null && msg.verified())
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
                    else if (msg.verified())
                        cp.add(prepare(msg, node.id()));
                }

                cp = !skip ? cp : null;

                if (log.isDebugEnabled()) {
                    if (cp == null)
                        log.debug("Failed to find messages history [node=" + node + ", lastMsgId" + lastMsgId + ']');
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
            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                prepareNodeAddedMessage(msg, destNodeId, null, null);

            return msg;
        }
    }

    /**
     * Pending messages container.
     */
    private static class PendingMessages {
        /** */
        private static final int MAX = 1024;

        /** Pending messages. */
        private final Queue<TcpDiscoveryAbstractMessage> msgs = new ArrayDeque<>(MAX * 2);

        /** Discarded message ID. */
        private IgniteUuid discardId;

        /**
         * Adds pending message and shrinks queue if it exceeds limit
         * (messages that were not discarded yet are never removed).
         *
         * @param msg Message to add.
         */
        void add(TcpDiscoveryAbstractMessage msg) {
            msgs.add(msg);

            while (msgs.size() > MAX) {
                TcpDiscoveryAbstractMessage polled = msgs.poll();

                assert polled != null;

                if (polled.id().equals(discardId))
                    break;
            }
        }

        /**
         * Resets pending messages.
         *
         * @param msgs Message.
         * @param discardId Discarded message ID.
         */
        void reset(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs, @Nullable IgniteUuid discardId) {
            this.msgs.clear();

            if (msgs != null)
                this.msgs.addAll(msgs);

            this.discardId = discardId;
        }

        /**
         * Clears pending messages.
         */
        void clear() {
            msgs.clear();

            discardId = null;
        }

        /**
         * Discards message with provided ID and all before it.
         *
         * @param id Discarded message ID.
         */
        void discard(IgniteUuid id) {
            discardId = id;
        }
    }

    /**
     * Message worker thread for messages processing.
     */
    private class RingMessageWorker extends MessageWorkerAdapter {
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

        /**
         */
        protected RingMessageWorker() {
            super("tcp-disco-msg-worker");
        }

        /**
         * @param msg Message to process.
         */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            if (debugMode)
                debugLog("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            spi.stats.onMessageProcessingStarted(msg);

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

            else if (msg instanceof TcpDiscoveryClientHeartbeatMessage)
                processClientHeartbeatMessage((TcpDiscoveryClientHeartbeatMessage)msg);

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

            if (spi.ensured(msg))
                msgHist.add(msg);

            spi.stats.onMessageProcessingFinished(msg);
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

            for (IgniteInClosure<TcpDiscoveryAbstractMessage> msgLsnr : spi.sendMsgLsnrs)
                msgLsnr.apply(msg);

            if (redirectToClients(msg)) {
                byte[] marshalledMsg = null;

                for (ClientMessageWorker clientMsgWorker : clientMsgWorkers.values()) {
                    // Send a clone to client to avoid ConcurrentModificationException
                    TcpDiscoveryAbstractMessage msgClone;

                    try {
                        if (marshalledMsg == null)
                            marshalledMsg = spi.marsh.marshal(msg);

                        msgClone = spi.marsh.unmarshal(marshalledMsg, null);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to marshal message: " + msg, e);

                        msgClone = msg;
                    }

                    clientMsgWorker.addMessage(msgClone);
                }
            }

            Collection<TcpDiscoveryNode> failedNodes;

            TcpDiscoverySpiState state;

            synchronized (mux) {
                failedNodes = U.arrayList(ServerImpl.this.failedNodes);

                state = spiState;
            }

            Collection<Throwable> errs = null;

            boolean sent = false;

            boolean searchNext = true;

            UUID locNodeId = getLocalNodeId();

            while (true) {
                if (searchNext) {
                    TcpDiscoveryNode newNext = ring.nextNode(failedNodes);

                    if (newNext == null) {
                        if (log.isDebugEnabled())
                            log.debug("No next node in topology.");

                        if (debugMode)
                            debugLog("No next node in topology.");

                        if (ring.hasRemoteNodes()) {
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
                            debugLog("New next node [newNext=" + newNext + ", formerNext=" + next +
                                ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

                        U.closeQuiet(sock);

                        sock = null;

                        next = newNext;
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Next node remains the same [nextId=" + next.id() +
                            ", nextOrder=" + next.internalOrder() + ']');
                }

                // Flag that shows whether next node exists and accepts incoming connections.
                boolean nextNodeExists = sock != null;

                final boolean sameHost = U.sameMacs(locNode, next);

                List<InetSocketAddress> locNodeAddrs = U.arrayList(locNode.socketAddresses());

                addr: for (InetSocketAddress addr : spi.getNodeAddresses(next, sameHost)) {
                    long ackTimeout0 = spi.ackTimeout;

                    if (locNodeAddrs.contains(addr)){
                        if (log.isDebugEnabled())
                            log.debug("Skip to send message to the local node (probably remote node has the same " +
                                "loopback address that local node): " + addr);

                        continue;
                    }

                    for (int i = 0; i < spi.reconCnt; i++) {
                        if (sock == null) {
                            nextNodeExists = false;

                            boolean success = false;

                            boolean openSock = false;

                            // Restore ring.
                            try {
                                long tstamp = U.currentTimeMillis();

                                sock = spi.openSocket(addr);

                                openSock = true;

                                // Handshake.
                                writeToSocket(sock, new TcpDiscoveryHandshakeRequest(locNodeId));

                                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null, ackTimeout0);

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
                                        debugLog("Failed to restore ring because next node ID received is not as " +
                                            "expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

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
                                                debugLog("Failed to restore ring because next node order received " +
                                                    "is not as expected [expected=" + next.internalOrder() +
                                                    ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                            break;
                                        }
                                    }

                                    if (log.isDebugEnabled())
                                        log.debug("Initialized connection with next node: " + next.id());

                                    if (debugMode)
                                        debugLog("Initialized connection with next node: " + next.id());

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

                                if (e instanceof SocketTimeoutException ||
                                    X.hasCause(e, SocketTimeoutException.class)) {
                                    ackTimeout0 *= 2;

                                    if (!checkAckTimeout(ackTimeout0))
                                        break;
                                }

                                continue;
                            }
                            finally {
                                if (!success) {
                                    U.closeQuiet(sock);

                                    sock = null;
                                }
                                else
                                    // Next node exists and accepts incoming messages.
                                    nextNodeExists = true;
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
                                        ", forceSndPending=" + forceSndPending + ']');

                                if (debugMode)
                                    debugLog("Pending messages will be sent [failure=" + failure +
                                        ", forceSndPending=" + forceSndPending + ']');

                                boolean skip = pendingMsgs.discardId != null;

                                for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs.msgs) {
                                    if (skip) {
                                        if (pendingMsg.id().equals(pendingMsgs.discardId))
                                            skip = false;

                                        continue;
                                    }

                                    long tstamp = U.currentTimeMillis();

                                    prepareNodeAddedMessage(pendingMsg, next.id(), pendingMsgs.msgs,
                                        pendingMsgs.discardId);

                                    try {
                                        writeToSocket(sock, pendingMsg);
                                    }
                                    finally {
                                        clearNodeAddedMessage(pendingMsg);
                                    }

                                    spi.stats.onMessageSent(pendingMsg, U.currentTimeMillis() - tstamp);

                                    int res = spi.readReceipt(sock, ackTimeout0);

                                    if (log.isDebugEnabled())
                                        log.debug("Pending message has been sent to next node [msg=" + msg.id() +
                                            ", pendingMsgId=" + pendingMsg + ", next=" + next.id() +
                                            ", res=" + res + ']');

                                    if (debugMode)
                                        debugLog("Pending message has been sent to next node [msg=" + msg.id() +
                                            ", pendingMsgId=" + pendingMsg + ", next=" + next.id() +
                                            ", res=" + res + ']');
                                }
                            }

                            prepareNodeAddedMessage(msg, next.id(), pendingMsgs.msgs, pendingMsgs.discardId);

                            try {
                                long tstamp = U.currentTimeMillis();

                                writeToSocket(sock, msg);

                                spi.stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                                int res = spi.readReceipt(sock, ackTimeout0);

                                if (log.isDebugEnabled())
                                    log.debug("Message has been sent to next node [msg=" + msg +
                                        ", next=" + next.id() +
                                        ", res=" + res + ']');

                                if (debugMode)
                                    debugLog("Message has been sent to next node [msg=" + msg +
                                        ", next=" + next.id() +
                                        ", res=" + res + ']');
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

                            if (e instanceof SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class)) {
                                ackTimeout0 *= 2;

                                if (!checkAckTimeout(ackTimeout0))
                                    break;
                            }
                        }
                        finally {
                            forceSndPending = false;

                            if (!sent) {
                                U.closeQuiet(sock);

                                sock = null;

                                if (log.isDebugEnabled())
                                    log.debug("Message has not been sent [next=" + next.id() + ", msg=" + msg +
                                        ", i=" + i + ']');
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
                            if (nextNodeExists && pingNode(next))
                                U.error(log, "Failed to send message to next node [msg=" + msg +
                                    ", next=" + next + ']', err);
                            else if (log.isDebugEnabled())
                                log.debug("Failed to send message to next node [msg=" + msg + ", next=" + next +
                                    ", errMsg=" + (err != null ? err.getMessage() : "N/A") + ']');
                        }
                    }

                    if (msg instanceof TcpDiscoveryStatusCheckMessage) {
                        TcpDiscoveryStatusCheckMessage msg0 = (TcpDiscoveryStatusCheckMessage)msg;

                        if (next.id().equals(msg0.failedNodeId())) {
                            next = null;

                            if (log.isDebugEnabled())
                                log.debug("Discarding status check since next node has indeed failed [next=" + next +
                                    ", msg=" + msg + ']');

                            // Discard status check message by exiting loop and handle failure.
                            break;
                        }
                    }

                    next = null;

                    searchNext = true;

                    errs = null;
                }
                else
                    break;
            }

            synchronized (mux) {
                failedNodes.removeAll(ServerImpl.this.failedNodes);
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
                    ServerImpl.this.failedNodes.addAll(failedNodes);
                }

                for (TcpDiscoveryNode n : failedNodes)
                    msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder()));

                LT.warn(log, null, "Local node has detected failed nodes and started cluster-wide procedure. " +
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

            for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs.msgs) {
                if (pendingMsg instanceof TcpDiscoveryNodeAddedMessage) {
                    TcpDiscoveryNodeAddedMessage addMsg = (TcpDiscoveryNodeAddedMessage)pendingMsg;

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
        private void processJoinRequestMessage(TcpDiscoveryJoinRequestMessage msg) {
            assert msg != null;

            TcpDiscoveryNode node = msg.node();

            UUID locNodeId = getLocalNodeId();

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

                    LT.warn(log, null, errMsg);

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
                        LT.warn(log, null, "Ignoring join request from node (duplicate ID) [node=" + node +
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
                            if (ring.hasRemoteNodes())
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
                            LT.warn(log, null,
                                "Authentication failed [nodeId=" + node.id() +
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
                                LT.warn(log, null,
                                    "Authentication subject is not Serializable [nodeId=" + node.id() +
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

                            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT, spi.marsh.marshal(subj));

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

                IgniteNodeValidationResult err = spi.getSpiContext().validateNode(node);

                if (err != null) {
                    boolean ping = node.id().equals(err.nodeId()) ? pingNode(node) : pingNode(err.nodeId());

                    if (!ping) {
                        if (log.isDebugEnabled())
                            log.debug("Conflicting node has already left, need to wait for event. " +
                                "Will ignore join request for now since it will be recent [req=" + msg +
                                ", err=" + err.message() + ']');

                        // Ignore join request.
                        return;
                    }

                    LT.warn(log, null, err.message());

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

                    // Ignore join request.
                    return;
                }

                String locMarsh = locNode.attribute(ATTR_MARSHALLER);
                String rmtMarsh = node.attribute(ATTR_MARSHALLER);

                if (!F.eq(locMarsh, rmtMarsh)) {
                    String errMsg = "Local node's marshaller differs from remote node's marshaller " +
                        "(to make sure all nodes in topology have identical marshaller, " +
                        "configure marshaller explicitly in configuration) " +
                        "[locMarshaller=" + locMarsh + ", rmtMarshaller=" + rmtMarsh +
                        ", locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(node) +
                        ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                    LT.warn(log, null, errMsg);

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
            }
            else if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
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
            UUID locNodeId = getLocalNodeId();

            boolean isLocNodeRouter = locNodeId.equals(msg.routerNodeId());

            if (!msg.verified()) {
                assert isLocNodeRouter;

                msg.verify(locNodeId);

                if (ring.hasRemoteNodes()) {
                    sendMessageAcrossRing(msg);

                    return;
                }
            }

            UUID nodeId = msg.creatorNodeId();

            TcpDiscoveryNode node = ring.node(nodeId);

            assert node == null || node.isClient();

            if (node != null) {
                assert node.isClient();

                node.clientRouterNodeId(msg.routerNodeId());
                node.aliveCheck(spi.maxMissedClientHbs);

                if (isLocalNodeCoordinator()) {
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

                        processNodeFailedMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                            node.id(), node.internalOrder()));
                    }
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
                if (ring.hasRemoteNodes())
                    sendMessageAcrossRing(msg);
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

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }
            else if (!locNodeId.equals(node.id()) && ring.node(node.id()) != null) {
                // Local node already has node from message in local topology.
                // Just pass it to coordinator via the ring.
                if (ring.hasRemoteNodes())
                    sendMessageAcrossRing(msg);

                if (log.isDebugEnabled())
                    log.debug("Local node already has node being added. Passing TcpDiscoveryNodeAddedMessage to " +
                                  "coordinator for final processing [ring=" + ring + ", node=" + node + ", locNode="
                                  + locNode + ", msg=" + msg + ']');

                if (debugMode)
                    debugLog("Local node already has node being added. Passing TcpDiscoveryNodeAddedMessage to " +
                                 "coordinator for final processing [ring=" + ring + ", node=" + node + ", locNode="
                                 + locNode + ", msg=" + msg + ']');

                return;
            }

            if (msg.verified() && !locNodeId.equals(node.id())) {
                if (node.internalOrder() <= ring.maxInternalOrder()) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    if (debugMode)
                        debugLog("Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    return;
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

                            SecurityContext coordSubj = spi.marsh.unmarshal(
                                node.<byte[]>attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT),
                                U.gridClassLoader());

                            if (!permissionsEqual(coordSubj.subject().permissions(), subj.subject().permissions())) {
                                // Node has not pass authentication.
                                LT.warn(log, null,
                                    "Authentication failed [nodeId=" + node.id() +
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
                        spi.onExchange(node.id(), node.id(), data, U.gridClassLoader());

                    msg.addDiscoveryData(locNodeId, spi.collectExchangeData(node.id()));
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

                            for (TcpDiscoveryNode n : top) {
                                // Make all preceding nodes and local node visible.
                                n.visible(true);
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

                            pendingMsgs.discard(msg.discardedMessageId());

                            // Clear data to minimize message size.
                            msg.messages(null, null);
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
                        spi.onExchange(node.id(), entry.getKey(), entry.getValue(), U.gridClassLoader());
                }
            }

            if (ring.hasRemoteNodes())
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

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

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

            if (msg.verified() && !locNodeId.equals(nodeId) && spiStateCopy() == CONNECTED && fireEvt) {
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
                        ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                notifyDiscovery(EVT_NODE_JOINED, topVer, node);

                try {
                    if (spi.ipFinder.isShared() && locNodeCoord)
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

            if (msg.verified() && locNodeId.equals(nodeId) && spiStateCopy() == CONNECTING) {
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

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
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
                            spi.ipFinder.unregisterAddresses(locNode.socketAddresses());
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

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified() && !locNodeId.equals(leavingNodeId)) {
                TcpDiscoveryNode leftNode = ring.removeNode(leavingNodeId);

                assert leftNode != null;

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
                        writeToSocket(sock, msg);

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

                spi.stats.onNodeLeft();

                notifyDiscovery(EVT_NODE_LEFT, topVer, leftNode);

                synchronized (mux) {
                    failedNodes.remove(leftNode);

                    leavingNodes.remove(leftNode);
                }
            }

            if (ring.hasRemoteNodes()) {
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

                    synchronized (mux) {
                        contains = failedNodes.contains(sndNode);
                    }

                    if (contains) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node failed message sent from node which is about to fail: " + msg);

                        return;
                    }
                }
            }

            UUID nodeId = msg.failedNodeId();
            long order = msg.order();

            TcpDiscoveryNode node = ring.node(nodeId);

            if (node != null && node.internalOrder() != order) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring node failed message since node internal order does not match " +
                        "[msg=" + msg + ", node=" + node + ']');

                return;
            }

            if (node != null) {
                synchronized (mux) {
                    failedNodes.add(node);
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

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified()) {
                node = ring.removeNode(nodeId);

                assert node != null;

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
                    failedNodes.remove(node);

                    leavingNodes.remove(node);

                    ClientMessageWorker worker = clientMsgWorkers.remove(node.id());

                    if (worker != null)
                        worker.interrupt();
                }

                if (msg.warning() != null && !msg.creatorNodeId().equals(getLocalNodeId())) {
                    ClusterNode creatorNode = ring.node(msg.creatorNodeId());

                    U.warn(log, "Received EVT_NODE_FAILED event with warning [" +
                        "nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : msg.creatorNodeId()) +
                        ", msg=" + msg.warning() + ']');
                }

                notifyDiscovery(EVT_NODE_FAILED, topVer, node);

                spi.stats.onNodeFailed();
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
            else {
                if (log.isDebugEnabled())
                    log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

                U.closeQuiet(sock);
            }
        }

        /**
         * Processes status check message.
         *
         * @param msg Status check message.
         */
        private void processStatusCheckMessage(TcpDiscoveryStatusCheckMessage msg) {
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

                        try {
                            trySendMessageDirectly(msg.creatorNode(), msg);

                            if (log.isDebugEnabled())
                                log.debug("Responded to status check message " +
                                    "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                        }
                        catch (IgniteSpiException e) {
                            if (e.hasCause(SocketException.class)) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Failed to respond to status check message (connection refused) " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                                }

                                onException("Failed to respond to status check message (connection refused) " +
                                    "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']', e);
                            }
                            else {
                                if (pingNode(msg.creatorNode())) {
                                    // Node exists and accepts incoming connections.
                                    U.error(log, "Failed to respond to status check message " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']', e);
                                }
                                else if (log.isDebugEnabled()) {
                                    log.debug("Failed to respond to status check message (did the node stop?) " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                                }
                            }
                        }
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

            if (ring.hasRemoteNodes())
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
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message that has made two passes: " + msg);

                return;
            }

            long tstamp = U.currentTimeMillis();

            if (spiStateCopy() == CONNECTED) {
                if (msg.hasMetrics()) {
                    for (Map.Entry<UUID, TcpDiscoveryHeartbeatMessage.MetricsSet> e : msg.metrics().entrySet()) {
                        UUID nodeId = e.getKey();

                        TcpDiscoveryHeartbeatMessage.MetricsSet metricsSet = e.getValue();

                        Map<Integer, CacheMetrics> cacheMetrics = msg.hasCacheMetrics() ?
                            msg.cacheMetrics().get(nodeId) : Collections.<Integer, CacheMetrics>emptyMap();

                        updateMetrics(nodeId, metricsSet.metrics(), cacheMetrics, tstamp);

                        for (T2<UUID, ClusterMetrics> t : metricsSet.clientMetrics())
                            updateMetrics(t.get1(), t.get2(), cacheMetrics, tstamp);
                    }
                }
            }

            if (ring.hasRemoteNodes()) {
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

                                if (aliveCheck <= 0 && isLocalNodeCoordinator() && !failedNodes.contains(clientNode))
                                    processNodeFailedMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                                        clientNode.id(), clientNode.internalOrder()));
                            }
                        }
                    }
                }

                if (ring.hasRemoteNodes())
                    sendMessageAcrossRing(msg);
            }
            else {
                locNode.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), locNode);
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
                pendingMsgs.discard(msgId);

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
                boolean sndNext;

                if (!msg.verified()) {
                    msg.verify(getLocalNodeId());
                    msg.topologyVersion(ring.topologyVersion());

                    notifyDiscoveryListener(msg);

                    sndNext = true;
                }
                else
                    sndNext = false;

                if (sndNext && ring.hasRemoteNodes())
                    sendMessageAcrossRing(msg);
                else {
                    spi.stats.onRingMessageReceived(msg);

                    DiscoverySpiCustomMessage msgObj = null;

                    try {
                        msgObj = msg.message(spi.marsh);
                    }
                    catch (Throwable e) {
                        U.error(log, "Failed to unmarshal discovery custom message.", e);
                    }

                    if (msgObj != null) {
                        DiscoverySpiCustomMessage nextMsg = msgObj.ackMessage();

                        if (nextMsg != null) {
                            try {
                                addMessage(new TcpDiscoveryCustomEventMessage(getLocalNodeId(), nextMsg,
                                    spi.marsh.marshal(nextMsg)));
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to marshal discovery custom message.", e);
                            }
                        }
                    }

                    addMessage(new TcpDiscoveryDiscardMessage(getLocalNodeId(), msg.id()));
                }
            }
            else {
                if (msg.verified())
                    notifyDiscoveryListener(msg);

                if (ring.hasRemoteNodes())
                    sendMessageAcrossRing(msg);
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
                        DiscoverySpiCustomMessage msgObj = msg.message(spi.marsh);

                        lsnr.onDiscovery(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
                            msg.topologyVersion(),
                            node,
                            snapshot,
                            hist,
                            msgObj);

                        if (msgObj.isMutable())
                            msg.message(msgObj, spi.marsh.marshal(msgObj));
                    }
                    catch (Throwable e) {
                        U.error(log, "Failed to unmarshal discovery custom message.", e);
                    }
                }
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

            for (port = spi.locPort; port < spi.locPort + spi.locPortRange; port++) {
                try {
                    srvrSock = new ServerSocket(port, 0, spi.locHost);

                    break;
                }
                catch (IOException e) {
                    if (port < spi.locPort + spi.locPortRange - 1) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to bind to local port (will try next port within range) " +
                                "[port=" + port + ", localHost=" + spi.locHost + ']');

                        onException("Failed to bind to local port. " +
                            "[port=" + port + ", localHost=" + spi.locHost + ']', e);
                    }
                    else {
                        throw new IgniteSpiException("Failed to bind TCP server socket (possibly all ports in range " +
                            "are in use) [firstPort=" + spi.locPort + ", lastPort=" + (spi.locPort + spi.locPortRange - 1) +
                            ", addr=" + spi.locHost + ']', e);
                    }
                }
            }

            if (log.isInfoEnabled())
                log.info("Successfully bound to TCP port [port=" + port + ", localHost=" + spi.locHost + ']');
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

                        reader.start();
                    }

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

                    in = new BufferedInputStream(sock.getInputStream());

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

                            LT.warn(log, null, "Failed to read magic header (too few bytes received) [rmtAddr=" +
                                sock.getRemoteSocketAddress() + ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            return;
                        }
                    }

                    if (!Arrays.equals(buf, U.IGNITE_HEADER)) {
                        if (log.isDebugEnabled())
                            log.debug("Unknown connection detected (is some other software connecting to " +
                                "this Ignite port?) " +
                                "[rmtAddr=" + sock.getRemoteSocketAddress() +
                                ", locAddr=" + sock.getLocalSocketAddress() + ']');

                        LT.warn(log, null, "Unknown connection detected (is some other software connecting to " +
                            "this Ignite port?) [rmtAddr=" + sock.getRemoteSocketAddress() +
                            ", locAddr=" + sock.getLocalSocketAddress() + ']');

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

                            if (req.clientNodeId() != null) {
                                ClientMessageWorker clientWorker = clientMsgWorkers.get(req.clientNodeId());

                                if (clientWorker != null)
                                    res.clientExists(clientWorker.ping());
                            }

                            spi.writeToSocket(sock, res);
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

                    spi.writeToSocket(sock, res);

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

                    if (debugMode)
                        debugLog("Initialized connection with remote node [nodeId=" + nodeId +
                            ", client=" + req.client() + ']');
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    if (X.hasCause(e, ObjectStreamException.class) || !sock.isClosed()) {
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
                        LT.warn(log, null, "Socket operation timed out on handshake " +
                            "(consider increasing 'networkTimeout' configuration property) " +
                            "[netTimeout=" + spi.netTimeout + ']');

                    else if (e.hasCause(ClassNotFoundException.class))
                        LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
                            "(make sure same versions of all classes are available on all nodes) " +
                            "[rmtAddr=" + sock.getRemoteSocketAddress() +
                            ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');

                        // Always report marshalling problems.
                    else if (e.hasCause(ObjectStreamException.class) ||
                        (!sock.isClosed() && !e.hasCause(IOException.class)))
                        LT.error(log, e, "Failed to initialize connection [sock=" + sock + ']');

                    return;
                }

                while (!isInterrupted()) {
                    try {
                        TcpDiscoveryAbstractMessage msg = spi.marsh.unmarshal(in, U.gridClassLoader());

                        msg.senderNodeId(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Message has been received: " + msg);

                        spi.stats.onMessageReceived(msg);

                        if (debugMode && recordable(msg))
                            debugLog("Message has been received: " + msg);

                        if (msg instanceof TcpDiscoveryJoinRequestMessage) {
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
                                    spi.writeToSocket(msg, sock, RES_OK);

                                    if (clientMsgWrk.getState() == State.NEW)
                                        clientMsgWrk.start();

                                    msgWorker.addMessage(msg);

                                    continue;
                                }
                                else {
                                    spi.writeToSocket(msg, sock, RES_CONTINUE_JOIN);

                                    break;
                                }
                            }
                        }
                        else if (msg instanceof TcpDiscoveryDuplicateIdMessage) {
                            // Send receipt back.
                            spi.writeToSocket(msg, sock, RES_OK);

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
                            spi.writeToSocket(msg, sock, RES_OK);

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
                            spi.writeToSocket(msg, sock, RES_OK);

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
                            spi.writeToSocket(msg, sock, RES_OK);

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

                        msgWorker.addMessage(msg);

                        // Send receipt back.
                        if (clientMsgWrk != null) {
                            TcpDiscoveryClientAckResponse ack = new TcpDiscoveryClientAckResponse(locNodeId, msg.id());

                            ack.verify(locNodeId);

                            clientMsgWrk.addMessage(ack);
                        }
                        else
                            spi.writeToSocket(msg, sock, RES_OK);
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
                            LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
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
         * @param nodeId Node ID.
         * @return {@code True} if node is in the ring and is not being removed from.
         */
        private boolean nodeAlive(UUID nodeId) {
            // Is node alive or about to be removed from the ring?
            TcpDiscoveryNode node = ring.node(nodeId);

            boolean nodeAlive = node != null && node.visible();

            if (nodeAlive) {
                synchronized (mux) {
                    nodeAlive = !F.transform(failedNodes, F.node2id()).contains(nodeId) &&
                        !F.transform(leavingNodes, F.node2id()).contains(nodeId);
                }
            }

            return nodeAlive;
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

            if (state == CONNECTED) {
                spi.writeToSocket(msg, sock, RES_OK);

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

                spi.writeToSocket(msg, sock, res);

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
    private class ClientMessageWorker extends MessageWorkerAdapter {
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
        protected ClientMessageWorker(Socket sock, UUID clientNodeId) {
            super("tcp-disco-client-message-worker");

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

        /** {@inheritDoc} */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            try {
                assert msg.verified() : msg;

                if (msg instanceof TcpDiscoveryClientAckResponse) {
                    if (clientVer == null) {
                        ClusterNode node = spi.getNode(clientNodeId);

                        if (node != null)
                            clientVer = IgniteUtils.productVersion(node);
                        else if (log.isDebugEnabled())
                            log.debug("Skip sending message ack to client, fail to get client node " +
                                "[sock=" + sock + ", locNodeId=" + getLocalNodeId() +
                                ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');
                    }

                    if (clientVer != null &&
                        clientVer.compareTo(TcpDiscoveryClientAckResponse.CLIENT_ACK_SINCE_VERSION) >= 0) {
                        if (log.isDebugEnabled())
                            log.debug("Sending message ack to client [sock=" + sock + ", locNodeId="
                                + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');

                        writeToSocket(sock, msg);
                    }
                }
                else {
                    try {
                        if (log.isDebugEnabled())
                            log.debug("Redirecting message to client [sock=" + sock + ", locNodeId="
                                + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']');

                        prepareNodeAddedMessage(msg, clientNodeId, null, null);

                        writeToSocket(sock, msg);
                    }
                    finally {
                        clearNodeAddedMessage(msg);
                    }
                }
            }
            catch (IgniteCheckedException | IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Client connection failed [sock=" + sock + ", locNodeId="
                        + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']', e);

                onException("Client connection failed [sock=" + sock + ", locNodeId="
                    + getLocalNodeId() + ", rmtNodeId=" + clientNodeId + ", msg=" + msg + ']', e);

                clientMsgWorkers.remove(clientNodeId, this);

                U.interrupt(this);

                U.closeQuiet(sock);
            }
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
         * @return Ping result.
         * @throws InterruptedException If interrupted.
         */
        public boolean ping() throws InterruptedException {
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
                return fut.get(spi.ackTimeout, TimeUnit.MILLISECONDS);
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
    protected abstract class MessageWorkerAdapter extends IgniteSpiThread {
        /** Pre-allocated output stream (100K). */
        private final GridByteArrayOutputStream bout = new GridByteArrayOutputStream(100 * 1024);

        /** Message queue. */
        private final BlockingDeque<TcpDiscoveryAbstractMessage> queue = new LinkedBlockingDeque<>();

        /** Backed interrupted flag. */
        private volatile boolean interrupted;

        /**
         * @param name Thread name.
         */
        protected MessageWorkerAdapter(String name) {
            super(spi.ignite().name(), name, log);

            setPriority(spi.threadPri);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Message worker started [locNodeId=" + getConfiguredNodeId() + ']');

            while (!isInterrupted()) {
                TcpDiscoveryAbstractMessage msg = queue.poll(2000, TimeUnit.MILLISECONDS);

                if (msg == null)
                    continue;

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
         * Adds message to queue.
         *
         * @param msg Message to add.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg) {
            if (msg.highPriority())
                queue.addFirst(msg);
            else
                queue.add(msg);

            if (log.isDebugEnabled())
                log.debug("Message has been added to queue: " + msg);
        }

        /**
         * @param msg Message.
         */
        protected abstract void processMessage(TcpDiscoveryAbstractMessage msg);

        /**
         * @param sock Socket.
         * @param msg Message.
         * @throws IOException If IO failed.
         * @throws IgniteCheckedException If marshalling failed.
         */
        protected final void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg)
            throws IOException, IgniteCheckedException {
            bout.reset();

            spi.writeToSocket(sock, msg, bout);
        }
    }
}
