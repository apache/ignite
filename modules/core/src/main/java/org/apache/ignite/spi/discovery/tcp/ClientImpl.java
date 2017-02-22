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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNodesRing;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.spi.discovery.tcp.ClientImpl.State.CONNECTED;
import static org.apache.ignite.spi.discovery.tcp.ClientImpl.State.DISCONNECTED;
import static org.apache.ignite.spi.discovery.tcp.ClientImpl.State.SEGMENTED;
import static org.apache.ignite.spi.discovery.tcp.ClientImpl.State.STARTING;
import static org.apache.ignite.spi.discovery.tcp.ClientImpl.State.STOPPED;

/**
 *
 */
class ClientImpl extends TcpDiscoveryImpl {
    /** */
    private static final Object JOIN_TIMEOUT = "JOIN_TIMEOUT";

    /** */
    private static final Object SPI_STOP = "SPI_STOP";

    /** */
    private static final Object SPI_RECONNECT_FAILED = "SPI_RECONNECT_FAILED";

    /** Remote nodes. */
    private final ConcurrentMap<UUID, TcpDiscoveryNode> rmtNodes = new ConcurrentHashMap8<>();

    /** Topology history. */
    private final NavigableMap<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

    /** Remote nodes. */
    private final ConcurrentMap<UUID, GridFutureAdapter<Boolean>> pingFuts = new ConcurrentHashMap8<>();

    /** Socket writer. */
    private SocketWriter sockWriter;

    /** */
    private SocketReader sockReader;

    /** */
    private volatile State state;

    /** Last message ID. */
    private volatile IgniteUuid lastMsgId;

    /** Current topology version. */
    private volatile long topVer;

    /** Join error. Contains error what occurs on join process. */
    private final AtomicReference<IgniteSpiException> joinErr = new AtomicReference<>();

    /** Joined latch. */
    private final CountDownLatch joinLatch = new CountDownLatch(1);

    /** Left latch. */
    private final CountDownLatch leaveLatch = new CountDownLatch(1);

    /** */
    private final Timer timer = new Timer("TcpDiscoverySpi.timer");

    /** */
    protected MessageWorker msgWorker;

    /** Force fail message for local node. */
    private TcpDiscoveryNodeFailedMessage forceFailMsg;

    /** */
    @GridToStringExclude
    private int joinCnt;

    /**
     * @param adapter Adapter.
     */
    ClientImpl(TcpDiscoverySpi adapter) {
        super(adapter);
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo(IgniteLogger log) {
        StringBuilder b = new StringBuilder(U.nl());

        b.append(">>>").append(U.nl());
        b.append(">>>").append("Dumping discovery SPI debug info.").append(U.nl());
        b.append(">>>").append(U.nl());

        b.append("Local node ID: ").append(getLocalNodeId()).append(U.nl()).append(U.nl());
        b.append("Local node: ").append(locNode).append(U.nl()).append(U.nl());

        b.append("Internal threads: ").append(U.nl());

        b.append("    Message worker: ").append(threadStatus(msgWorker)).append(U.nl());
        b.append("    Socket reader: ").append(threadStatus(sockReader)).append(U.nl());
        b.append("    Socket writer: ").append(threadStatus(sockWriter)).append(U.nl());

        b.append(U.nl());

        b.append("Nodes: ").append(U.nl());

        for (ClusterNode node : allVisibleNodes())
            b.append("    ").append(node.id()).append(U.nl());

        b.append(U.nl());

        b.append("Stats: ").append(spi.stats).append(U.nl());

        U.quietAndInfo(log, b.toString());
    }

    /** {@inheritDoc} */
    @Override public String getSpiState() {

        if (sockWriter.isOnline())
            return "connected";

        return "disconnected";
    }

    /** {@inheritDoc} */
    @Override public int getMessageWorkerQueueSize() {
        return msgWorker.queueSize();
    }

    /** {@inheritDoc} */
    @Override public UUID getCoordinator() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        spi.initLocalNode(
            0,
            true);

        locNode = spi.locNode;

        // Marshal credentials for backward compatibility and security.
        marshalCredentials(locNode);

        sockWriter = new SocketWriter();
        sockWriter.start();

        sockReader = new SocketReader();
        sockReader.start();

        if (spi.ipFinder.isShared())
            registerLocalNodeAddress();

        msgWorker = new MessageWorker();
        msgWorker.start();

        try {
            joinLatch.await();

            IgniteSpiException err = joinErr.get();

            if (err != null)
                throw err;
        }
        catch (InterruptedException e) {
            throw new IgniteSpiException("Thread has been interrupted.", e);
        }

        timer.schedule(
            new HeartbeatSender(),
            spi.hbFreq,
            spi.hbFreq);

        spi.printStartInfo();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (msgWorker != null && msgWorker.isAlive()) { // Should always be alive
            msgWorker.addMessage(SPI_STOP);

            try {
                if (!leaveLatch.await(spi.netTimeout, MILLISECONDS))
                    U.warn(log, "Failed to left node: timeout [nodeId=" + locNode + ']');
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        }

        for (GridFutureAdapter<Boolean> fut : pingFuts.values())
            fut.onDone(false);

        rmtNodes.clear();

        U.interrupt(msgWorker);
        U.interrupt(sockWriter);
        U.interrupt(sockReader);

        U.join(msgWorker, log);
        U.join(sockWriter, log);
        U.join(sockReader, log);

        timer.cancel();

        spi.printStopInfo();
    }

    /** {@inheritDoc} */
    @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return U.arrayList(rmtNodes.values(), TcpDiscoveryNodesRing.VISIBLE_NODES);
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        if (getLocalNodeId().equals(nodeId))
            return locNode;

        TcpDiscoveryNode node = rmtNodes.get(nodeId);

        return node != null && node.visible() ? node : null;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(@NotNull final UUID nodeId) {
        if (nodeId.equals(getLocalNodeId()))
            return true;

        TcpDiscoveryNode node = rmtNodes.get(nodeId);

        if (node == null || !node.visible())
            return false;

        GridFutureAdapter<Boolean> fut = pingFuts.get(nodeId);

        if (fut == null) {
            fut = new GridFutureAdapter<>();

            GridFutureAdapter<Boolean> oldFut = pingFuts.putIfAbsent(nodeId, fut);

            if (oldFut != null)
                fut = oldFut;
            else {
                State state = this.state;

                if (spi.getSpiContext().isStopping() || state == STOPPED || state == SEGMENTED) {
                    if (pingFuts.remove(nodeId, fut))
                        fut.onDone(false);

                    return false;
                }
                else if (state == DISCONNECTED) {
                    if (pingFuts.remove(nodeId, fut))
                        fut.onDone(new IgniteClientDisconnectedCheckedException(null,
                            "Failed to ping node, client node disconnected."));
                }
                else {
                    final GridFutureAdapter<Boolean> finalFut = fut;

                    timer.schedule(new TimerTask() {
                        @Override public void run() {
                            if (pingFuts.remove(nodeId, finalFut)) {
                                if (ClientImpl.this.state == DISCONNECTED)
                                    finalFut.onDone(new IgniteClientDisconnectedCheckedException(null,
                                        "Failed to ping node, client node disconnected."));
                                else
                                    finalFut.onDone(false);
                            }
                        }
                    }, spi.netTimeout);

                    sockWriter.sendMessage(new TcpDiscoveryClientPingRequest(getLocalNodeId(), nodeId));
                }
            }
        }

        try {
            return fut.get();
        }
        catch (IgniteInterruptedCheckedException ignored) {
            return false;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        U.interrupt(msgWorker);
        U.interrupt(sockWriter);
        U.interrupt(sockReader);

        U.join(msgWorker, log);
        U.join(sockWriter, log);
        U.join(sockReader, log);

        leaveLatch.countDown();
        joinLatch.countDown();

        spi.getSpiContext().deregisterPorts();

        Collection<ClusterNode> rmts = getRemoteNodes();

        // This is restart/disconnection and remote nodes are not empty.
        // We need to fire FAIL event for each.
        DiscoverySpiListener lsnr = spi.lsnr;

        if (lsnr != null) {
            for (ClusterNode n : rmts) {
                rmtNodes.remove(n.id());

                Collection<ClusterNode> top = updateTopologyHistory(topVer + 1, null);

                lsnr.onDiscovery(EVT_NODE_FAILED, topVer, n, top, new TreeMap<>(topHist), null);
            }
        }

        rmtNodes.clear();
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage evt) {
        State state = this.state;

        if (state == SEGMENTED)
            throw new IgniteException("Failed to send custom message: client is segmented.");

        if (state == DISCONNECTED)
            throw new IgniteClientDisconnectedException(null, "Failed to send custom message: client is disconnected.");

        try {
            sockWriter.sendMessage(new TcpDiscoveryCustomEventMessage(getLocalNodeId(), evt,
                U.marshal(spi.marshaller(), evt)));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal custom event: " + evt, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        TcpDiscoveryNode node = rmtNodes.get(nodeId);

        if (node != null) {
            TcpDiscoveryNodeFailedMessage msg = new TcpDiscoveryNodeFailedMessage(getLocalNodeId(),
                node.id(),
                node.internalOrder());

            msg.warning(warning);

            msg.force(true);

            msgWorker.addMessage(msg);
        }
    }

    /**
     * @param recon {@code True} if reconnects.
     * @param timeout Timeout.
     * @return Opened socket or {@code null} if timeout.
     * @throws InterruptedException If interrupted.
     * @throws IgniteSpiException If failed.
     * @see TcpDiscoverySpi#joinTimeout
     */
    @SuppressWarnings("BusyWait")
    @Nullable private T2<SocketStream, Boolean> joinTopology(boolean recon, long timeout)
        throws IgniteSpiException, InterruptedException {
        Collection<InetSocketAddress> addrs = null;

        long startTime = U.currentTimeMillis();

        while (true) {
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();

            while (addrs == null || addrs.isEmpty()) {
                addrs = spi.resolvedAddresses();

                if (!F.isEmpty(addrs)) {
                    if (log.isDebugEnabled())
                        log.debug("Resolved addresses from IP finder: " + addrs);
                }
                else {
                    if (timeout > 0 && (U.currentTimeMillis() - startTime) > timeout)
                        return null;

                    LT.warn(log, "IP finder returned empty addresses list. " +
                            "Please check IP finder configuration" +
                            (spi.ipFinder instanceof TcpDiscoveryMulticastIpFinder ?
                                " and make sure multicast works on your network. " : ". ") +
                            "Will retry every 2 secs.", true);

                    Thread.sleep(2000);
                }
            }

            Collection<InetSocketAddress> addrs0 = new ArrayList<>(addrs);

            Iterator<InetSocketAddress> it = addrs.iterator();

            boolean wait = false;

            while (it.hasNext()) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                InetSocketAddress addr = it.next();

                T3<SocketStream, Integer, Boolean> sockAndRes = sendJoinRequest(recon, addr);

                if (sockAndRes == null) {
                    it.remove();

                    continue;
                }

                assert sockAndRes.get1() != null && sockAndRes.get2() != null : sockAndRes;

                Socket sock = sockAndRes.get1().socket();

                if (log.isDebugEnabled())
                    log.debug("Received response to join request [addr=" + addr + ", res=" + sockAndRes.get2() + ']');

                switch (sockAndRes.get2()) {
                    case RES_OK:
                        return new T2<>(sockAndRes.get1(), sockAndRes.get3());

                    case RES_CONTINUE_JOIN:
                    case RES_WAIT:
                        wait = true;

                        U.closeQuiet(sock);

                        break;

                    default:
                        if (log.isDebugEnabled())
                            log.debug("Received unexpected response to join request: " + sockAndRes.get2());

                        U.closeQuiet(sock);
                }
            }

            if (wait) {
                if (timeout > 0 && (U.currentTimeMillis() - startTime) > timeout)
                    return null;

                if (log.isDebugEnabled())
                    log.debug("Will wait before retry join.");

                Thread.sleep(2000);
            }
            else if (addrs.isEmpty()) {
                if (timeout > 0 && (U.currentTimeMillis() - startTime) > timeout)
                    return null;

                LT.warn(log, "Failed to connect to any address from IP finder (will retry to join topology " +
                    "every 2 secs): " + toOrderedList(addrs0), true);

                Thread.sleep(2000);
            }
        }
    }

    /**
     * @param recon {@code True} if reconnects.
     * @param addr Address.
     * @return Socket, connect response and client acknowledge support flag.
     */
    @Nullable private T3<SocketStream, Integer, Boolean> sendJoinRequest(boolean recon, InetSocketAddress addr) {
        assert addr != null;

        if (log.isDebugEnabled())
            log.debug("Send join request [addr=" + addr + ", reconnect=" + recon +
                ", locNodeId=" + getLocalNodeId() + ']');

        Collection<Throwable> errs = null;

        long ackTimeout0 = spi.getAckTimeout();

        int reconCnt = 0;

        int connectAttempts = 1;

        UUID locNodeId = getLocalNodeId();

        IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(spi);

        while (true) {
            boolean openSock = false;

            Socket sock = null;

            try {
                long tstamp = U.currentTimeMillis();

                sock = spi.openSocket(addr, timeoutHelper);

                openSock = true;

                TcpDiscoveryHandshakeRequest req = new TcpDiscoveryHandshakeRequest(locNodeId);

                req.client(true);

                spi.writeToSocket(sock, req, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                TcpDiscoveryHandshakeResponse res = spi.readMessage(sock, null, ackTimeout0);

                UUID rmtNodeId = res.creatorNodeId();

                assert rmtNodeId != null;
                assert !getLocalNodeId().equals(rmtNodeId);

                spi.stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                locNode.clientRouterNodeId(rmtNodeId);

                tstamp = U.currentTimeMillis();

                TcpDiscoveryAbstractMessage msg;

                if (!recon) {
                    TcpDiscoveryNode node = locNode;

                    if (locNode.order() > 0)
                        node = locNode.clientReconnectNode();

                    msg = new TcpDiscoveryJoinRequestMessage(node, spi.collectExchangeData(getLocalNodeId()));
                }
                else
                    msg = new TcpDiscoveryClientReconnectMessage(getLocalNodeId(), rmtNodeId, lastMsgId);

                msg.client(true);

                spi.writeToSocket(sock, msg, timeoutHelper.nextTimeoutChunk(spi.getSocketTimeout()));

                spi.stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                if (log.isDebugEnabled())
                    log.debug("Message has been sent to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + rmtNodeId + ']');

                return new T3<>(new SocketStream(sock),
                    spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0)),
                    res.clientAck());
            }
            catch (IOException | IgniteCheckedException e) {
                U.closeQuiet(sock);

                if (log.isDebugEnabled())
                    log.error("Exception on joining: " + e.getMessage(), e);

                onException("Exception on joining: " + e.getMessage(), e);

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
        }

        if (log.isDebugEnabled())
            log.debug("Failed to join to address [addr=" + addr + ", recon=" + recon + ", errs=" + errs + ']');

        return null;
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
                U.marshal(spi.marshaller(), attrs.get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS)));

            node.setAttributes(attrs);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal node security credentials: " + node.id(), e);
        }
    }

    /**
     * @param topVer New topology version.
     * @param msg Discovery message.
     * @return Latest topology snapshot.
     */
    private Collection<ClusterNode> updateTopologyHistory(long topVer, @Nullable TcpDiscoveryAbstractMessage msg) {
        this.topVer = topVer;

        if (!topHist.isEmpty() && topVer <= topHist.lastKey()) {
            if (log.isDebugEnabled())
                log.debug("Skip topology update since topology already updated [msg=" + msg +
                    ", lastHistKey=" + topHist.lastKey() +
                    ", topVer=" + topVer +
                    ", locNode=" + locNode + ']');

            Collection<ClusterNode> top = topHist.get(topVer);

            assert top != null : "Failed to find topology history [msg=" + msg + ", hist=" + topHist + ']';

            return top;
        }

        NavigableSet<ClusterNode> allNodes = allVisibleNodes();

        if (!topHist.containsKey(topVer)) {
            assert topHist.isEmpty() || topHist.lastKey() == topVer - 1 :
                "lastVer=" + (topHist.isEmpty() ? null : topHist.lastKey()) +
                ", newVer=" + topVer +
                ", locNode=" + locNode +
                ", msg=" + msg;

            topHist.put(topVer, allNodes);

            if (topHist.size() > spi.topHistSize)
                topHist.pollFirstEntry();

            assert topHist.lastKey() == topVer;
            assert topHist.size() <= spi.topHistSize;
        }

        return allNodes;
    }

    /**
     * @return All nodes.
     */
    private NavigableSet<ClusterNode> allVisibleNodes() {
        NavigableSet<ClusterNode> allNodes = new TreeSet<>();

        for (TcpDiscoveryNode node : rmtNodes.values()) {
            if (node.visible())
                allNodes.add(node);
        }

        allNodes.add(locNode);

        return allNodes;
    }

    /** {@inheritDoc} */
    @Override void simulateNodeFailure() {
        U.warn(log, "Simulating client node failure: " + getLocalNodeId());

        U.interrupt(sockWriter);
        U.interrupt(msgWorker);

        U.join(sockWriter, log);
        U.join(
            msgWorker,
            log);
    }

    /** {@inheritDoc} */
    @Override public void brakeConnection() {
        SocketStream sockStream = msgWorker.currSock;

        if (sockStream != null)
            U.closeQuiet(sockStream.socket());
    }

    /** {@inheritDoc} */
    @Override protected IgniteSpiThread workerThread() {
        return msgWorker;
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    @SuppressWarnings("BusyWait")
    public void waitForClientMessagePrecessed() {
        Object last = msgWorker.queue.peekLast();

        while (last != null && msgWorker.isAlive() && msgWorker.queue.contains(last)) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * @param err Error.
     */
    private void joinError(IgniteSpiException err) {
        assert err != null;

        joinErr.compareAndSet(null, err);

        joinLatch.countDown();
    }

    /**
     * Heartbeat sender.
     */
    private class HeartbeatSender extends TimerTask {
        /** {@inheritDoc} */
        @Override public void run() {
            if (!spi.getSpiContext().isStopping() && sockWriter.isOnline()) {
                TcpDiscoveryClientHeartbeatMessage msg = new TcpDiscoveryClientHeartbeatMessage(getLocalNodeId(),
                    spi.metricsProvider.metrics());

                msg.client(true);

                sockWriter.sendMessage(msg);
            }
        }
    }

    /**
     * Socket reader.
     */
    private class SocketReader extends IgniteSpiThread {
        /** */
        private final Object mux = new Object();

        /** */
        private SocketStream sockStream;

        /** */
        private UUID rmtNodeId;

        /**
         */
        protected SocketReader() {
            super(spi.ignite().name(), "tcp-client-disco-sock-reader", log);
        }

        /**
         * @param sockStream Socket.
         * @param rmtNodeId Rmt node id.
         */
        public void setSocket(SocketStream sockStream, UUID rmtNodeId) {
            synchronized (mux) {
                this.sockStream = sockStream;

                this.rmtNodeId = rmtNodeId;

                mux.notifyAll();
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                SocketStream sockStream;
                UUID rmtNodeId;

                synchronized (mux) {
                    if (this.sockStream == null) {
                        mux.wait();

                        continue;
                    }

                    sockStream = this.sockStream;
                    rmtNodeId = this.rmtNodeId;
                }

                Socket sock = sockStream.socket();

                try {
                    InputStream in = sockStream.stream();

                    sock.setKeepAlive(true);
                    sock.setTcpNoDelay(true);

                    while (!isInterrupted()) {
                        TcpDiscoveryAbstractMessage msg;

                        try {
                            msg = U.unmarshal(spi.marshaller(), in, U.resolveClassLoader(spi.ignite().configuration()));
                        }
                        catch (IgniteCheckedException e) {
                            if (log.isDebugEnabled())
                                U.error(log, "Failed to read message [sock=" + sock + ", " +
                                    "locNodeId=" + getLocalNodeId() + ", rmtNodeId=" + rmtNodeId + ']', e);

                            IOException ioEx = X.cause(e, IOException.class);

                            if (ioEx != null)
                                throw ioEx;

                            ClassNotFoundException clsNotFoundEx = X.cause(e, ClassNotFoundException.class);

                            if (clsNotFoundEx != null)
                                LT.warn(log, "Failed to read message due to ClassNotFoundException " +
                                    "(make sure same versions of all classes are available on all nodes) " +
                                    "[rmtNodeId=" + rmtNodeId + ", err=" + clsNotFoundEx.getMessage() + ']');
                            else
                                LT.error(log, e, "Failed to read message [sock=" + sock + ", locNodeId=" +
                                    getLocalNodeId() + ", rmtNodeId=" + rmtNodeId + ']');

                            continue;
                        }

                        msg.senderNodeId(rmtNodeId);

                        DebugLogger debugLog = messageLogger(msg);

                        if (debugLog.isDebugEnabled())
                            debugLog.debug("Message has been received: " + msg);

                        spi.stats.onMessageReceived(msg);

                        boolean ack = msg instanceof TcpDiscoveryClientAckResponse;

                        if (!ack)
                            msgWorker.addMessage(msg);
                        else
                            sockWriter.ackReceived((TcpDiscoveryClientAckResponse)msg);
                    }
                }
                catch (IOException e) {
                    msgWorker.addMessage(new SocketClosedMessage(sockStream));

                    if (log.isDebugEnabled())
                        U.error(log, "Connection failed [sock=" + sock + ", locNodeId=" + getLocalNodeId() + ']', e);
                }
                finally {
                    U.closeQuiet(sock);

                    synchronized (mux) {
                        if (this.sockStream == sockStream) {
                            this.sockStream = null;
                            this.rmtNodeId = null;
                        }
                    }
                }
            }
        }
    }

    /**
     *
     */
    private class SocketWriter extends IgniteSpiThread {
        /** */
        private final Object mux = new Object();

        /** */
        private Socket sock;

        /** */
        private boolean clientAck;

        /** */
        private final Queue<TcpDiscoveryAbstractMessage> queue = new ArrayDeque<>();

        /** */
        private final long socketTimeout;

        /** */
        private TcpDiscoveryAbstractMessage unackedMsg;

        /**
         *
         */
        protected SocketWriter() {
            super(spi.ignite().name(), "tcp-client-disco-sock-writer", log);

            socketTimeout = spi.failureDetectionTimeoutEnabled() ? spi.failureDetectionTimeout() :
                spi.getSocketTimeout();
        }

        /**
         * @param msg Message.
         */
        private void sendMessage(TcpDiscoveryAbstractMessage msg) {
            synchronized (mux) {
                queue.add(msg);

                mux.notifyAll();
            }
        }

        /**
         * @param sock Socket.
         * @param clientAck {@code True} is server supports client message acknowlede.
         */
        private void setSocket(Socket sock, boolean clientAck) {
            synchronized (mux) {
                this.sock = sock;

                this.clientAck = clientAck;

                unackedMsg = null;

                mux.notifyAll();
            }
        }

        /**
         * @return {@code True} if connection is alive.
         */
        public boolean isOnline() {
            synchronized (mux) {
                return sock != null;
            }
        }

        /**
         * @param res Acknowledge response.
         */
        void ackReceived(TcpDiscoveryClientAckResponse res) {
            synchronized (mux) {
                if (unackedMsg != null) {
                    assert unackedMsg.id().equals(res.messageId()) : unackedMsg;

                    unackedMsg = null;
                }

                mux.notifyAll();
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            TcpDiscoveryAbstractMessage msg;

            while (!Thread.currentThread().isInterrupted()) {
                Socket sock;

                synchronized (mux) {
                    sock = this.sock;

                    if (sock == null) {
                        mux.wait();

                        continue;
                    }

                    msg = queue.poll();

                    if (msg == null) {
                        mux.wait();

                        continue;
                    }
                }

                for (IgniteInClosure<TcpDiscoveryAbstractMessage> msgLsnr : spi.sndMsgLsnrs)
                    msgLsnr.apply(msg);

                boolean ack = clientAck && !(msg instanceof TcpDiscoveryPingResponse);

                try {
                    if (ack) {
                        synchronized (mux) {
                            assert unackedMsg == null : "Unacked=" + unackedMsg + ", received=" + msg;

                            unackedMsg = msg;
                        }
                    }

                    spi.writeToSocket(
                        sock,
                        msg,
                        socketTimeout);

                    msg = null;

                    if (ack) {
                        long waitEnd = U.currentTimeMillis() + (spi.failureDetectionTimeoutEnabled() ?
                            spi.failureDetectionTimeout() : spi.getAckTimeout());

                        TcpDiscoveryAbstractMessage unacked;

                        synchronized (mux) {
                            while (unackedMsg != null && U.currentTimeMillis() < waitEnd)
                                mux.wait(waitEnd);

                            unacked = unackedMsg;

                            unackedMsg = null;
                        }

                        if (unacked != null) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to get acknowledge for message, will try to reconnect " +
                                "[msg=" + unacked +
                                (spi.failureDetectionTimeoutEnabled() ?
                                ", failureDetectionTimeout=" + spi.failureDetectionTimeout() :
                                ", timeout=" + spi.getAckTimeout()) + ']');

                            throw new IOException("Failed to get acknowledge for message: " + unacked);
                        }
                    }
                }
                catch (InterruptedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Client socket writer interrupted.");

                    return;
                }
                catch (Exception e) {
                    if (spi.getSpiContext().isStopping()) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send message, node is stopping [msg=" + msg + ", err=" + e + ']');
                    }
                    else
                        U.error(log, "Failed to send message: " + msg, e);

                    U.closeQuiet(sock);

                    synchronized (mux) {
                        if (sock == this.sock)
                            this.sock = null; // Connection has dead.
                    }
                }
            }
        }
    }

    /**
     *
     */
    private class Reconnector extends IgniteSpiThread {
        /** */
        private volatile SocketStream sockStream;

        /** */
        private boolean clientAck;

        /** */
        private boolean join;

        /**
         * @param join {@code True} if reconnects during join.
         */
        protected Reconnector(boolean join) {
            super(spi.ignite().name(), "tcp-client-disco-reconnector", log);

            this.join = join;
        }

        /**
         *
         */
        public void cancel() {
            interrupt();

            SocketStream sockStream = this.sockStream;

            if (sockStream != null)
                U.closeQuiet(sockStream.socket());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            boolean success = false;

            Exception err = null;

            long timeout = join ? spi.joinTimeout : spi.netTimeout;

            long startTime = U.currentTimeMillis();

            if (log.isDebugEnabled())
                log.debug("Started reconnect process [join=" + join + ", timeout=" + timeout + ']');

            try {
                while (true) {
                    T2<SocketStream, Boolean> joinRes = joinTopology(true, timeout);

                    if (joinRes == null) {
                        if (join) {
                            joinError(new IgniteSpiException("Join process timed out, connection failed and " +
                                "failed to reconnect (consider increasing 'joinTimeout' configuration property) " +
                                "[joinTimeout=" + spi.joinTimeout + ']'));
                        }
                        else
                            U.error(log, "Failed to reconnect to cluster (consider increasing 'networkTimeout'" +
                                " configuration property) [networkTimeout=" + spi.netTimeout + ']');

                        return;
                    }

                    sockStream = joinRes.get1();
                    clientAck = joinRes.get2();

                    Socket sock = sockStream.socket();

                    if (isInterrupted())
                        throw new InterruptedException();

                    int oldTimeout = 0;

                    try {
                        oldTimeout = sock.getSoTimeout();

                        sock.setSoTimeout((int)spi.netTimeout);

                        InputStream in = sockStream.stream();

                        sock.setKeepAlive(true);
                        sock.setTcpNoDelay(true);

                        List<TcpDiscoveryAbstractMessage> msgs = null;

                        while (!isInterrupted()) {
                            TcpDiscoveryAbstractMessage msg = U.unmarshal(spi.marshaller(), in,
                                U.resolveClassLoader(spi.ignite().configuration()));

                            if (msg instanceof TcpDiscoveryClientReconnectMessage) {
                                TcpDiscoveryClientReconnectMessage res = (TcpDiscoveryClientReconnectMessage)msg;

                                if (res.creatorNodeId().equals(getLocalNodeId())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Received reconnect response [success=" + res.success() +
                                            ", msg=" + msg + ']');

                                    if (res.success()) {
                                        msgWorker.addMessage(res);

                                        if (msgs != null) {
                                            for (TcpDiscoveryAbstractMessage msg0 : msgs)
                                                msgWorker.addMessage(msg0);
                                        }

                                        success = true;

                                        return;
                                    }
                                    else
                                        return;
                                }
                            }
                            else if (spi.ensured(msg)) {
                                if (msgs == null)
                                    msgs = new ArrayList<>();

                                msgs.add(msg);
                            }
                        }
                    }
                    catch (IOException | IgniteCheckedException e) {
                        U.closeQuiet(sock);

                        if (log.isDebugEnabled())
                            log.error("Reconnect error [join=" + join + ", timeout=" + timeout + ']', e);

                        if (timeout > 0 && (U.currentTimeMillis() - startTime) > timeout) {
                            String msg = join ? "Failed to connect to cluster (consider increasing 'joinTimeout' " +
                                "configuration  property) [joinTimeout=" + spi.joinTimeout + ", err=" + e + ']' :
                                "Failed to reconnect to cluster (consider increasing 'networkTimeout' " +
                                    "configuration  property) [networkTimeout=" + spi.netTimeout + ", err=" + e + ']';

                            U.warn(log, msg);

                            throw e;
                        }
                        else
                            U.warn(log, "Failed to reconnect to cluster (will retry): " + e);
                    }
                    finally {
                        if (success)
                            sock.setSoTimeout(oldTimeout);
                    }
                }
            }
            catch (IOException | IgniteCheckedException e) {
                err = e;

                success = false;

                U.error(log, "Failed to reconnect", e);
            }
            finally {
                if (!success) {
                    SocketStream sockStream = this.sockStream;

                    if (sockStream != null)
                        U.closeQuiet(sockStream.socket());

                    if (join)
                        joinError(new IgniteSpiException("Failed to connect to cluster, connection failed and failed " +
                            "to reconnect.", err));
                    else
                        msgWorker.addMessage(SPI_RECONNECT_FAILED);
                }
            }
        }
    }

    /**
     * Message worker.
     */
    protected class MessageWorker extends IgniteSpiThread {
        /** Message queue. */
        private final BlockingDeque<Object> queue = new LinkedBlockingDeque<>();

        /** */
        private SocketStream currSock;

        /** */
        private Reconnector reconnector;

        /** */
        private boolean nodeAdded;

        /**
         *
         */
        private MessageWorker() {
            super(spi.ignite().name(), "tcp-client-disco-msg-worker", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("InfiniteLoopStatement")
        @Override protected void body() throws InterruptedException {
            state = STARTING;

            spi.stats.onJoinStarted();

            try {
                tryJoin();

                while (true) {
                    Object msg = queue.take();

                    if (msg == JOIN_TIMEOUT) {
                        if (state == STARTING) {
                            joinError(new IgniteSpiException("Join process timed out, did not receive response for " +
                                "join request (consider increasing 'joinTimeout' configuration property) " +
                                "[joinTimeout=" + spi.joinTimeout + ", sock=" + currSock + ']'));

                            break;
                        }
                        else if (state == DISCONNECTED) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to reconnect, local node segmented " +
                                    "[joinTimeout=" + spi.joinTimeout + ']');

                            state = SEGMENTED;

                            notifyDiscovery(EVT_NODE_SEGMENTED, topVer, locNode, allVisibleNodes());
                        }
                    }
                    else if (msg == SPI_STOP) {
                        boolean connected = state == CONNECTED;

                        state = STOPPED;

                        assert spi.getSpiContext().isStopping();

                        if (connected && currSock != null) {
                            TcpDiscoveryAbstractMessage leftMsg = new TcpDiscoveryNodeLeftMessage(getLocalNodeId());

                            leftMsg.client(true);

                            sockWriter.sendMessage(leftMsg);
                        }
                        else
                            leaveLatch.countDown();
                    }
                    else if (msg instanceof TcpDiscoveryNodeFailedMessage &&
                        ((TcpDiscoveryNodeFailedMessage)msg).failedNodeId().equals(locNode.id())) {
                        TcpDiscoveryNodeFailedMessage msg0 = (TcpDiscoveryNodeFailedMessage)msg;

                        assert msg0.force() : msg0;

                        forceFailMsg = msg0;
                    }
                    else if (msg instanceof SocketClosedMessage) {
                        if (((SocketClosedMessage)msg).sock == currSock) {
                            currSock = null;

                            boolean join = joinLatch.getCount() > 0;

                            if (spi.getSpiContext().isStopping() || state == SEGMENTED) {
                                leaveLatch.countDown();

                                if (join) {
                                    joinError(new IgniteSpiException("Failed to connect to cluster: socket closed."));

                                    break;
                                }
                            }
                            else {
                                if (forceFailMsg != null) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Connection closed, local node received force fail message, " +
                                            "will not try to restore connection");
                                    }

                                    queue.addFirst(SPI_RECONNECT_FAILED);
                                }
                                else {
                                    if (log.isDebugEnabled())
                                        log.debug("Connection closed, will try to restore connection.");

                                    assert reconnector == null;

                                    final Reconnector reconnector = new Reconnector(join);
                                    this.reconnector = reconnector;
                                    reconnector.start();
                                }
                            }
                        }
                    }
                    else if (msg == SPI_RECONNECT_FAILED) {
                        if (reconnector != null) {
                            reconnector.cancel();
                            reconnector.join();

                            reconnector = null;
                        }
                        else
                            assert forceFailMsg != null;

                        if (spi.isClientReconnectDisabled()) {
                            if (state != SEGMENTED && state != STOPPED) {
                                if (forceFailMsg != null) {
                                    U.quietAndWarn(log, "Local node was dropped from cluster due to network problems " +
                                        "[nodeInitiatedFail=" + forceFailMsg.creatorNodeId() +
                                        ", msg=" + forceFailMsg.warning() + ']');
                                }

                                if (log.isDebugEnabled()) {
                                    log.debug("Failed to restore closed connection, reconnect disabled, " +
                                        "local node segmented [networkTimeout=" + spi.netTimeout + ']');
                                }

                                state = SEGMENTED;

                                notifyDiscovery(EVT_NODE_SEGMENTED, topVer, locNode, allVisibleNodes());
                            }
                        }
                        else {
                            if (state == STARTING || state == CONNECTED) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Failed to restore closed connection, will try to reconnect " +
                                        "[networkTimeout=" + spi.netTimeout +
                                        ", joinTimeout=" + spi.joinTimeout +
                                        ", failMsg=" + forceFailMsg + ']');
                                }

                                state = DISCONNECTED;

                                nodeAdded = false;

                                IgniteClientDisconnectedCheckedException err =
                                    new IgniteClientDisconnectedCheckedException(null, "Failed to ping node, " +
                                    "client node disconnected.");

                                for (Map.Entry<UUID, GridFutureAdapter<Boolean>> e : pingFuts.entrySet()) {
                                    GridFutureAdapter<Boolean> fut = e.getValue();

                                    if (pingFuts.remove(e.getKey(), fut))
                                        fut.onDone(err);
                                }

                                notifyDiscovery(EVT_CLIENT_NODE_DISCONNECTED, topVer, locNode, allVisibleNodes());
                            }

                            UUID newId = UUID.randomUUID();

                            if (forceFailMsg != null) {
                                long delay = IgniteSystemProperties.getLong(IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY,
                                    10_000);

                                if (delay > 0) {
                                    U.quietAndWarn(log, "Local node was dropped from cluster due to network problems, " +
                                        "will try to reconnect with new id after " + delay + "ms (reconnect delay " +
                                        "can be changed using IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY system " +
                                        "property) [" +
                                        "newId=" + newId +
                                        ", prevId=" + locNode.id() +
                                        ", locNode=" + locNode +
                                        ", nodeInitiatedFail=" + forceFailMsg.creatorNodeId() +
                                        ", msg=" + forceFailMsg.warning() + ']');

                                    Thread.sleep(delay);
                                }
                                else {
                                    U.quietAndWarn(log, "Local node was dropped from cluster due to network problems, " +
                                        "will try to reconnect with new id [" +
                                        "newId=" + newId +
                                        ", prevId=" + locNode.id() +
                                        ", locNode=" + locNode +
                                        ", nodeInitiatedFail=" + forceFailMsg.creatorNodeId() +
                                        ", msg=" + forceFailMsg.warning() + ']');
                                }

                                forceFailMsg = null;
                            }
                            else if (log.isInfoEnabled()) {
                                log.info("Client node disconnected from cluster, will try to reconnect with new id " +
                                    "[newId=" + newId + ", prevId=" + locNode.id() + ", locNode=" + locNode + ']');
                            }

                            locNode.onClientDisconnected(newId);

                            tryJoin();
                        }
                    }
                    else {
                        TcpDiscoveryAbstractMessage discoMsg = (TcpDiscoveryAbstractMessage)msg;

                        if (joining()) {
                            IgniteSpiException err = null;

                            if (discoMsg instanceof TcpDiscoveryDuplicateIdMessage)
                                err = spi.duplicateIdError((TcpDiscoveryDuplicateIdMessage)msg);
                            else if (discoMsg instanceof TcpDiscoveryAuthFailedMessage)
                                err = spi.authenticationFailedError((TcpDiscoveryAuthFailedMessage)msg);
                            else if (discoMsg instanceof TcpDiscoveryCheckFailedMessage)
                                err = spi.checkFailedError((TcpDiscoveryCheckFailedMessage)msg);

                            if (err != null) {
                                if (state == DISCONNECTED) {
                                    U.error(log, "Failed to reconnect, segment local node.", err);

                                    state = SEGMENTED;

                                    notifyDiscovery(EVT_NODE_SEGMENTED, topVer, locNode, allVisibleNodes());
                                }
                                else
                                    joinError(err);

                                break;
                            }
                        }

                        processDiscoveryMessage((TcpDiscoveryAbstractMessage)msg);
                    }
                }
            }
            finally {
                SocketStream currSock = this.currSock;

                if (currSock != null)
                    U.closeQuiet(currSock.socket());

                if (joinLatch.getCount() > 0)
                    joinError(new IgniteSpiException("Some error in join process.")); // This should not occur.

                if (reconnector != null) {
                    reconnector.cancel();

                    reconnector.join();
                }
            }
        }

        /**
         * @throws InterruptedException If interrupted.
         */
        private void tryJoin() throws InterruptedException {
            assert state == DISCONNECTED || state == STARTING : state;

            boolean join = state == STARTING;

            joinCnt++;

            T2<SocketStream, Boolean> joinRes = joinTopology(false, spi.joinTimeout);

            if (joinRes == null) {
                if (join)
                    joinError(new IgniteSpiException("Join process timed out."));
                else {
                    state = SEGMENTED;

                    notifyDiscovery(EVT_NODE_SEGMENTED, topVer, locNode, allVisibleNodes());
                }

                return;
            }

            currSock = joinRes.get1();

            sockWriter.setSocket(joinRes.get1().socket(), joinRes.get2());

            if (spi.joinTimeout > 0) {
                final int joinCnt0 = joinCnt;

                timer.schedule(new TimerTask() {
                    @Override public void run() {
                        if (joinCnt == joinCnt0 && joining())
                            queue.add(JOIN_TIMEOUT);
                    }
                }, spi.joinTimeout);
            }

            sockReader.setSocket(joinRes.get1(), locNode.clientRouterNodeId());
        }

        /**
         * @param msg Message.
         */
        protected void processDiscoveryMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;
            assert msg.verified() || msg.senderNodeId() == null;

            spi.stats.onMessageProcessingStarted(msg);

            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);
            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((TcpDiscoveryNodeAddFinishedMessage)msg);
            else if (msg instanceof TcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((TcpDiscoveryNodeLeftMessage)msg);
            else if (msg instanceof TcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((TcpDiscoveryNodeFailedMessage)msg);
            else if (msg instanceof TcpDiscoveryHeartbeatMessage)
                processHeartbeatMessage((TcpDiscoveryHeartbeatMessage)msg);
            else if (msg instanceof TcpDiscoveryClientReconnectMessage)
                processClientReconnectMessage((TcpDiscoveryClientReconnectMessage)msg);
            else if (msg instanceof TcpDiscoveryCustomEventMessage)
                processCustomMessage((TcpDiscoveryCustomEventMessage)msg);
            else if (msg instanceof TcpDiscoveryClientPingResponse)
                processClientPingResponse((TcpDiscoveryClientPingResponse)msg);
            else if (msg instanceof TcpDiscoveryPingRequest)
                processPingRequest();

            spi.stats.onMessageProcessingFinished(msg);

            if (spi.ensured(msg) && state == CONNECTED)
                lastMsgId = msg.id();
        }

        /**
         * @return {@code True} if client in process of join.
         */
        private boolean joining() {
            ClientImpl.State state = ClientImpl.this.state;

            return state == STARTING || state == DISCONNECTED;
        }

        /**
         * @return {@code True} if client disconnected.
         */
        private boolean disconnected() {
            return state == DISCONNECTED;
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
            if (spi.getSpiContext().isStopping())
                return;

            TcpDiscoveryNode node = msg.node();

            UUID newNodeId = node.id();

            if (getLocalNodeId().equals(newNodeId)) {
                if (joining()) {
                    Collection<TcpDiscoveryNode> top = msg.topology();

                    if (top != null) {
                        spi.gridStartTime = msg.gridStartTime();

                        if (disconnected())
                            rmtNodes.clear();

                        for (TcpDiscoveryNode n : top) {
                            if (n.order() > 0)
                                n.visible(true);

                            rmtNodes.put(n.id(), n);
                        }

                        topHist.clear();

                        nodeAdded = true;

                        if (msg.topologyHistory() != null)
                            topHist.putAll(msg.topologyHistory());
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node added message with empty topology: " + msg);
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node added message (this message has already been processed) " +
                        "[msg=" + msg + ", locNode=" + locNode + ']');
            }
            else {
                if (nodeAdded()) {
                    boolean topChanged = rmtNodes.putIfAbsent(newNodeId, node) == null;

                    if (topChanged) {
                        if (log.isDebugEnabled())
                            log.debug("Added new node to topology: " + node);

                        Map<Integer, byte[]> data = msg.newNodeDiscoveryData();

                        if (data != null)
                            spi.onExchange(newNodeId, newNodeId, data,
                                U.resolveClassLoader(spi.ignite().configuration()));
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Ignore topology message, local node not added to topology: " + msg);
                }
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddFinishedMessage(TcpDiscoveryNodeAddFinishedMessage msg) {
            if (spi.getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.nodeId())) {
                if (joining()) {
                    Map<UUID, Map<Integer, byte[]>> dataMap = msg.clientDiscoData();

                    if (dataMap != null) {
                        for (Map.Entry<UUID, Map<Integer, byte[]>> entry : dataMap.entrySet())
                            spi.onExchange(getLocalNodeId(), entry.getKey(), entry.getValue(),
                                U.resolveClassLoader(spi.ignite().configuration()));
                    }

                    locNode.setAttributes(msg.clientNodeAttributes());
                    locNode.visible(true);

                    long topVer = msg.topologyVersion();

                    locNode.order(topVer);

                    for (Iterator<Long> it = topHist.keySet().iterator(); it.hasNext();) {
                        if (it.next() >= topVer)
                            it.remove();
                    }

                    Collection<ClusterNode> nodes = updateTopologyHistory(topVer, msg);

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode, nodes);

                    boolean disconnected = disconnected();

                    state = CONNECTED;

                    if (disconnected) {
                        notifyDiscovery(EVT_CLIENT_NODE_RECONNECTED, topVer, locNode, nodes);

                        U.quietAndWarn(log, "Client node was reconnected after it was already considered " +
                            "failed by the server topology (this could happen after all servers restarted or due " +
                            "to a long network outage between the client and servers). All continuous queries and " +
                            "remote event listeners created by this client will be unsubscribed, consider " +
                            "listening to EVT_CLIENT_NODE_RECONNECTED event to restore them.");
                    }
                    else
                        spi.stats.onJoinFinished();

                    joinErr.set(null);

                    joinLatch.countDown();
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message (this message has already been processed) " +
                        "[msg=" + msg + ", locNode=" + locNode + ']');
            }
            else {
                if (nodeAdded()) {
                    TcpDiscoveryNode node = rmtNodes.get(msg.nodeId());

                    if (node == null) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node add finished message since node is not found [msg=" + msg + ']');

                        return;
                    }

                    boolean evt = false;

                    long topVer = msg.topologyVersion();

                    assert topVer > 0 : msg;

                    if (!node.visible()) {
                        node.order(topVer);
                        node.visible(true);

                        if (spi.locNodeVer.equals(node.version()))
                            node.version(spi.locNodeVer);

                        evt = true;
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Skip node join event, node already joined [msg=" + msg + ", node=" + node + ']');

                        assert node.order() == topVer : node;
                    }

                    Collection<ClusterNode> top = updateTopologyHistory(topVer, msg);

                    assert top != null && top.contains(node) : "Topology does not contain node [msg=" + msg +
                        ", node=" + node + ", top=" + top + ']';

                    if (state != CONNECTED) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node add finished message (join process is not finished): " + msg);

                        return;
                    }

                    if (evt) {
                        notifyDiscovery(EVT_NODE_JOINED, topVer, node, top);

                        spi.stats.onNodeJoined();
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Ignore topology message, local node not added to topology: " + msg);
                }
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeLeftMessage(TcpDiscoveryNodeLeftMessage msg) {
            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Received node left message for local node: " + msg);

                leaveLatch.countDown();
            }
            else {
                if (spi.getSpiContext().isStopping())
                    return;

                if (nodeAdded()) {
                    TcpDiscoveryNode node = rmtNodes.remove(msg.creatorNodeId());

                    if (node == null) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node left message since node is not found [msg=" + msg + ']');

                        return;
                    }

                    Collection<ClusterNode> top = updateTopologyHistory(msg.topologyVersion(), msg);

                    if (state != CONNECTED) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node left message (join process is not finished): " + msg);

                        return;
                    }

                    notifyDiscovery(EVT_NODE_LEFT, msg.topologyVersion(), node, top);

                    spi.stats.onNodeLeft();
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Ignore topology message, local node not added to topology: " + msg);
                }
            }
        }

        /**
         * @return {@code True} if received node added message for local node.
         */
        private boolean nodeAdded() {
            return nodeAdded;
        }

        /**
         * @param msg Message.
         */
        private void processNodeFailedMessage(TcpDiscoveryNodeFailedMessage msg) {
            if (spi.getSpiContext().isStopping()) {
                if (!getLocalNodeId().equals(msg.creatorNodeId()) && getLocalNodeId().equals(msg.failedNodeId())) {
                    if (leaveLatch.getCount() > 0) {
                        if (log.isDebugEnabled())
                            log.debug("Remote node fail this node while node is stopping [locNode=" + getLocalNodeId()
                                + ", rmtNode=" + msg.creatorNodeId() + ']');

                        leaveLatch.countDown();
                    }
                }

                return;
            }

            if (nodeAdded()) {
                if (!getLocalNodeId().equals(msg.creatorNodeId())) {
                    TcpDiscoveryNode node = rmtNodes.remove(msg.failedNodeId());

                    if (node == null) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node failed message since node is not found [msg=" + msg + ']');

                        return;
                    }

                    Collection<ClusterNode> top = updateTopologyHistory(msg.topologyVersion(), msg);

                    if (state != CONNECTED) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node failed message (join process is not finished): " + msg);

                        return;
                    }

                    if (msg.warning() != null) {
                        ClusterNode creatorNode = rmtNodes.get(msg.creatorNodeId());

                        U.warn(log, "Received EVT_NODE_FAILED event with warning [" +
                            "nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : msg.creatorNodeId()) +
                            ", msg=" + msg.warning() + ']');
                    }

                    notifyDiscovery(EVT_NODE_FAILED, msg.topologyVersion(), node, top);

                    spi.stats.onNodeFailed();
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Ignore topology message, local node not added to topology: " + msg);
            }
        }

        /**
         * @param msg Message.
         */
        private void processHeartbeatMessage(TcpDiscoveryHeartbeatMessage msg) {
            if (spi.getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                assert msg.senderNodeId() != null;

                if (log.isDebugEnabled())
                    log.debug("Received heartbeat response: " + msg);
            }
            else {
                long tstamp = U.currentTimeMillis();

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
        }

        /**
         * @param msg Message.
         */
        private void processClientReconnectMessage(TcpDiscoveryClientReconnectMessage msg) {
            if (spi.getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                if (reconnector != null) {
                    assert msg.success() : msg;

                    currSock = reconnector.sockStream;

                    sockWriter.setSocket(currSock.socket(), reconnector.clientAck);
                    sockReader.setSocket(currSock, locNode.clientRouterNodeId());

                    reconnector = null;

                    for (TcpDiscoveryAbstractMessage pendingMsg : msg.pendingMessages()) {
                        if (log.isDebugEnabled())
                            log.debug("Process pending message on reconnect [msg=" + pendingMsg + ']');

                        processDiscoveryMessage(pendingMsg);
                    }
                }
                else {
                    if (joinLatch.getCount() > 0) {
                        if (msg.success()) {
                            for (TcpDiscoveryAbstractMessage pendingMsg : msg.pendingMessages()) {
                                if (log.isDebugEnabled())
                                    log.debug("Process pending message on connect [msg=" + pendingMsg + ']');

                                processDiscoveryMessage(pendingMsg);
                            }

                            assert joinLatch.getCount() == 0 : msg;
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Discarding reconnect message, reconnect is completed: " + msg);
                }
            }
            else if (log.isDebugEnabled())
                log.debug("Discarding reconnect message for another client: " + msg);
        }

        /**
         * @param msg Message.
         */
        private void processCustomMessage(TcpDiscoveryCustomEventMessage msg) {
            if (state == CONNECTED) {
                DiscoverySpiListener lsnr = spi.lsnr;

                if (lsnr != null) {
                    UUID nodeId = msg.creatorNodeId();

                    TcpDiscoveryNode node = nodeId.equals(getLocalNodeId()) ? locNode : rmtNodes.get(nodeId);

                    if (node != null && node.visible()) {
                        try {
                            DiscoverySpiCustomMessage msgObj = msg.message(spi.marshaller(),
                                U.resolveClassLoader(spi.ignite().configuration()));

                            notifyDiscovery(EVT_DISCOVERY_CUSTOM_EVT, topVer, node, allVisibleNodes(), msgObj);
                        }
                        catch (Throwable e) {
                            U.error(log, "Failed to unmarshal discovery custom message.", e);
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Received metrics from unknown node: " + nodeId);
                }
            }
        }

        /**
         * @param msg Message.
         */
        private void processClientPingResponse(TcpDiscoveryClientPingResponse msg) {
            GridFutureAdapter<Boolean> fut = pingFuts.remove(msg.nodeToPing());

            if (fut != null)
                fut.onDone(msg.result());
        }

        /**
         * Router want to ping this client.
         */
        private void processPingRequest() {
            TcpDiscoveryPingResponse res = new TcpDiscoveryPingResponse(getLocalNodeId());

            res.client(true);

            sockWriter.sendMessage(res);
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
            boolean isLocDaemon = spi.locNode.isDaemon();

            assert nodeId != null;
            assert metrics != null;
            assert isLocDaemon || cacheMetrics != null;

            TcpDiscoveryNode node = nodeId.equals(getLocalNodeId()) ? locNode : rmtNodes.get(nodeId);

            if (node != null && node.visible()) {
                node.setMetrics(metrics);

                if (!isLocDaemon)
                    node.setCacheMetrics(cacheMetrics);

                node.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, topVer, node, allVisibleNodes());
            }
            else if (log.isDebugEnabled())
                log.debug("Received metrics from unknown node: " + nodeId);
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param top Topology snapshot.
         */
        private void notifyDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> top) {
            notifyDiscovery(type, topVer, node, top, null);
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param top Topology snapshot.
         * @param data Optional custom message data.
         */
        private void notifyDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> top,
            @Nullable DiscoverySpiCustomMessage data) {
            DiscoverySpiListener lsnr = spi.lsnr;

            DebugLogger log = type == EVT_NODE_METRICS_UPDATED ? traceLog : debugLog;

            if (lsnr != null) {
                if (log.isDebugEnabled())
                    log.debug("Discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                        ", topVer=" + topVer + ']');

                lsnr.onDiscovery(type, topVer, node, top, new TreeMap<>(topHist), data);
            }
            else if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                    ", topVer=" + topVer + ']');
        }

        /**
         * @param msg Message.
         */
        void addMessage(Object msg) {
            queue.add(msg);
        }

        /**
         * @return Queue size.
         */
        int queueSize() {
            return queue.size();
        }
    }

    /**
     *
     */
    private static class SocketClosedMessage {
        /** */
        private final SocketStream sock;

        /**
         * @param sock Socket.
         */
        private SocketClosedMessage(SocketStream sock) {
            this.sock = sock;
        }
    }

    /**
     *
     */
    private static class SocketStream {
        /** */
        private final Socket sock;

        /** */
        private final InputStream in;

        /**
         * @param sock Socket.
         * @throws IOException If failed to create stream.
         */
        public SocketStream(Socket sock) throws IOException {
            assert sock != null;

            this.sock = sock;

            this.in = new BufferedInputStream(sock.getInputStream());
        }

        /**
         * @return Socket.
         */
        Socket socket() {
            return sock;

        }

        /**
         * @return Socket input stream.
         */
        InputStream stream() {
            return in;
        }

        /** {@inheritDoc} */
        public String toString() {
            return sock.toString();
        }
    }

    /**
     *
     */
    enum State {
        /** */
        STARTING,

        /** */
        CONNECTED,

        /** */
        DISCONNECTED,

        /** */
        SEGMENTED,

        /** */
        STOPPED
    }
}
