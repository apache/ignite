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
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.spi.discovery.tcp.messages.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHeartbeatMessage.*;

/**
 * Client discovery SPI implementation that uses TCP/IP for node discovery.
 * <p>
 * This discovery SPI requires at least on server node configured with
 * {@link TcpDiscoverySpi}. It will try to connect to random IP taken from
 * {@link TcpDiscoveryIpFinder} which should point to one of these server
 * nodes and will maintain connection only with this node (will not enter the ring).
 * If this connection is broken, it will try to reconnect using addresses from
 * the same IP finder.
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiOrderSupport(true)
@DiscoverySpiHistorySupport(true)
public class TcpClientDiscoverySpi extends TcpDiscoverySpiAdapter implements TcpClientDiscoverySpiMBean {
    /** Default disconnect check interval. */
    public static final long DFLT_DISCONNECT_CHECK_INT = 2000;

    /** Remote nodes. */
    private final ConcurrentMap<UUID, TcpDiscoveryNode> rmtNodes = new ConcurrentHashMap8<>();

    /** Socket. */
    private volatile Socket sock;

    /** Socket reader. */
    private volatile SocketReader sockRdr;

    /** Heartbeat sender. */
    private volatile HeartbeatSender hbSender;

    /** Disconnect handler. */
    private volatile DisconnectHandler disconnectHnd;

    /** Last message ID. */
    private volatile IgniteUuid lastMsgId;

    /** Current topology version. */
    private volatile long topVer;

    /** Join error. */
    private IgniteSpiException joinErr;

    /** Whether reconnect failed. */
    private boolean reconFailed;

    /** Joined latch. */
    private CountDownLatch joinLatch;

    /** Left latch. */
    private volatile CountDownLatch leaveLatch;

    /** Disconnect check interval. */
    private long disconnectCheckInt = DFLT_DISCONNECT_CHECK_INT;

    /** {@inheritDoc} */
    @Override public long getDisconnectCheckInterval() {
        return disconnectCheckInt;
    }

    /**
     * Sets disconnect check interval.
     *
     * @param disconnectCheckInt Disconnect check interval.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setDisconnectCheckInterval(long disconnectCheckInt) {
        this.disconnectCheckInt = disconnectCheckInt;
    }

    /** {@inheritDoc} */
    @Override public long getSocketTimeout() {
        return sockTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getAckTimeout() {
        return ackTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getNetworkTimeout() {
        return netTimeout;
    }

    /** {@inheritDoc} */
    @Override public int getThreadPriority() {
        return threadPri;
    }

    /** {@inheritDoc} */
    @Override public long getHeartbeatFrequency() {
        return hbFreq;
    }

    /** {@inheritDoc} */
    @Override public String getIpFinderFormatted() {
        return ipFinder.toString();
    }

    /** {@inheritDoc} */
    @Override public int getMessageWorkerQueueSize() {
        SocketReader sockRdr0 = sockRdr;

        return sockRdr0 != null ? sockRdr0.msgWrk.queueSize() : 0;
    }

    /** {@inheritDoc} */
    @Override public long getNodesJoined() {
        return stats.joinedNodesCount();
    }

    /** {@inheritDoc} */
    @Override public long getNodesLeft() {
        return stats.leftNodesCount();
    }

    /** {@inheritDoc} */
    @Override public long getNodesFailed() {
        return stats.failedNodesCount();
    }

    /** {@inheritDoc} */
    @Override public long getAvgMessageProcessingTime() {
        return stats.avgMessageProcessingTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaxMessageProcessingTime() {
        return stats.maxMessageProcessingTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalReceivedMessages() {
        return stats.totalReceivedMessages();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Integer> getReceivedMessages() {
        return stats.receivedMessages();
    }

    /** {@inheritDoc} */
    @Override public int getTotalProcessedMessages() {
        return stats.totalProcessedMessages();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Integer> getProcessedMessages() {
        return stats.processedMessages();
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        startStopwatch();

        assertParameter(ipFinder != null, "ipFinder != null");
        assertParameter(netTimeout > 0, "networkTimeout > 0");
        assertParameter(sockTimeout > 0, "sockTimeout > 0");
        assertParameter(ackTimeout > 0, "ackTimeout > 0");
        assertParameter(hbFreq > 0, "heartbeatFreq > 0");
        assertParameter(threadPri > 0, "threadPri > 0");

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new IgniteSpiException("Unknown local address: " + locAddr, e);
        }

        if (log.isDebugEnabled()) {
            log.debug(configInfo("localHost", locHost.getHostAddress()));
            log.debug(configInfo("threadPri", threadPri));
            log.debug(configInfo("networkTimeout", netTimeout));
            log.debug(configInfo("sockTimeout", sockTimeout));
            log.debug(configInfo("ackTimeout", ackTimeout));
            log.debug(configInfo("ipFinder", ipFinder));
            log.debug(configInfo("heartbeatFreq", hbFreq));
        }

        // Warn on odd network timeout.
        if (netTimeout < 3000)
            U.warn(log, "Network timeout is too low (at least 3000 ms recommended): " + netTimeout);

        // Warn on odd heartbeat frequency.
        if (hbFreq < 2000)
            U.warn(log, "Heartbeat frequency is too high (at least 2000 ms recommended): " + hbFreq);

        registerMBean(gridName, this, TcpClientDiscoverySpiMBean.class);

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new IgniteSpiException("Unknown local address: " + locAddr, e);
        }

        if (ipFinder instanceof TcpDiscoveryMulticastIpFinder) {
            TcpDiscoveryMulticastIpFinder mcastIpFinder = ((TcpDiscoveryMulticastIpFinder)ipFinder);

            if (mcastIpFinder.getLocalAddress() == null)
                mcastIpFinder.setLocalAddress(locAddr);
        }

        IgniteBiTuple<Collection<String>, Collection<String>> addrs;

        try {
            addrs = U.resolveLocalAddresses(locHost);
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to resolve local host to set of external addresses: " + locHost, e);
        }

        locNode = new TcpDiscoveryNode(
            getLocalNodeId(),
            addrs.get1(),
            addrs.get2(),
            0,
            metricsProvider,
            locNodeVer);

        locNode.setAttributes(locNodeAttrs);
        locNode.local(true);

        sockTimeoutWorker = new SocketTimeoutWorker();
        sockTimeoutWorker.start();

        joinTopology(false);

        disconnectHnd = new DisconnectHandler();
        disconnectHnd.start();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        rmtNodes.clear();

        U.interrupt(disconnectHnd);
        U.join(disconnectHnd, log);

        U.interrupt(hbSender);
        U.join(hbSender, log);

        Socket sock0 = sock;

        sock = null;

        if (sock0 != null) {
            leaveLatch = new CountDownLatch(1);

            try {
                TcpDiscoveryNodeLeftMessage msg = new TcpDiscoveryNodeLeftMessage(getLocalNodeId());

                msg.client(true);

                writeToSocket(sock0, msg);

                if (!U.await(leaveLatch, netTimeout, MILLISECONDS)) {
                    if (log.isDebugEnabled())
                        log.debug("Did not receive node left message for local node (will stop anyway) [sock=" +
                            sock0 + ']');
                }
            }
            catch (IOException | IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Failed to send node left message (will stop anyway) [sock=" + sock0 + ']', e);
            }
            finally {
                U.closeQuiet(sock0);
            }
        }

        U.interrupt(sockRdr);
        U.join(sockRdr, log);

        U.interrupt(sockTimeoutWorker);
        U.join(sockTimeoutWorker, log);

        unregisterMBean();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        return Arrays.<Object>asList(ipFinder);
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return F.view(U.<TcpDiscoveryNode, ClusterNode>arrayList(rmtNodes.values(), new P1<TcpDiscoveryNode>() {
            @Override public boolean apply(TcpDiscoveryNode node) {
                return node.visible();
            }
        }));
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        if (getLocalNodeId().equals(nodeId))
            return locNode;

        TcpDiscoveryNode node = rmtNodes.get(nodeId);

        return node != null && node.visible() ? node : null;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        assert nodeId != null;

        if (nodeId.equals(getLocalNodeId()))
            return true;

        TcpDiscoveryNode node = rmtNodes.get(nodeId);

        return node != null && node.visible();
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(Serializable evt) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId) {
        ClusterNode node = rmtNodes.get(nodeId);

        if (node != null) {
            TcpDiscoveryNodeFailedMessage msg = new TcpDiscoveryNodeFailedMessage(getLocalNodeId(),
                node.id(), node.order());

            sockRdr.addMessage(msg);
        }
    }

    /**
     * @param recon Reconnect flag.
     * @return Whether joined successfully.
     * @throws IgniteSpiException In case of error.
     */
    private boolean joinTopology(boolean recon) throws IgniteSpiException {
        if (!recon)
            stats.onJoinStarted();

        Collection<InetSocketAddress> addrs = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                while (addrs == null || addrs.isEmpty()) {
                    addrs = resolvedAddresses();

                    if (!F.isEmpty(addrs)) {
                        if (log.isDebugEnabled())
                            log.debug("Resolved addresses from IP finder: " + addrs);
                    }
                    else {
                        U.warn(log, "No addresses registered in the IP finder (will retry in 2000ms): " + ipFinder);

                        U.sleep(2000);
                    }
                }

                Collection<InetSocketAddress> addrs0 = new ArrayList<>(addrs);

                Iterator<InetSocketAddress> it = addrs.iterator();

                while (it.hasNext() && !Thread.currentThread().isInterrupted()) {
                    InetSocketAddress addr = it.next();

                    Socket sock = null;

                    try {
                        long ts = U.currentTimeMillis();

                        IgniteBiTuple<Socket, UUID> t = initConnection(addr);

                        sock = t.get1();

                        UUID rmtNodeId = t.get2();

                        stats.onClientSocketInitialized(U.currentTimeMillis() - ts);

                        locNode.clientRouterNodeId(rmtNodeId);

                        TcpDiscoveryAbstractMessage msg = recon ?
                            new TcpDiscoveryClientReconnectMessage(getLocalNodeId(), rmtNodeId,
                                lastMsgId) :
                            new TcpDiscoveryJoinRequestMessage(locNode, null);

                        msg.client(true);

                        writeToSocket(sock, msg);

                        int res = readReceipt(sock, ackTimeout);

                        switch (res) {
                            case RES_OK:
                                this.sock = sock;

                                sockRdr = new SocketReader(rmtNodeId, new MessageWorker(recon));
                                sockRdr.start();

                                if (U.await(joinLatch, netTimeout, MILLISECONDS)) {
                                    IgniteSpiException joinErr0 = joinErr;

                                    if (joinErr0 != null)
                                        throw joinErr0;

                                    if (reconFailed) {
                                        if (log.isDebugEnabled())
                                            log.debug("Failed to reconnect, will try to rejoin [locNode=" +
                                                locNode + ']');

                                        U.closeQuiet(sock);

                                        U.interrupt(sockRdr);
                                        U.join(sockRdr, log);

                                        this.sock = null;

                                        return false;
                                    }

                                    if (log.isDebugEnabled())
                                        log.debug("Successfully connected to topology [sock=" + sock + ']');

                                    hbSender = new HeartbeatSender();
                                    hbSender.start();

                                    stats.onJoinFinished();

                                    return true;
                                }
                                else {
                                    U.warn(log, "Join process timed out (will try other address) [sock=" + sock +
                                        ", timeout=" + netTimeout + ']');

                                    U.closeQuiet(sock);

                                    U.interrupt(sockRdr);
                                    U.join(sockRdr, log);

                                    it.remove();

                                    break;
                                }

                            case RES_CONTINUE_JOIN:
                            case RES_WAIT:
                                U.closeQuiet(sock);

                                break;

                            default:
                                if (log.isDebugEnabled())
                                    log.debug("Received unexpected response to join request: " + res);

                                U.closeQuiet(sock);
                        }
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Joining thread was interrupted.");

                        return false;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Failed to establish connection with address: " + addr, e);

                        U.closeQuiet(sock);

                        it.remove();
                    }
                }

                if (addrs.isEmpty()) {
                    U.warn(log, "Failed to connect to any address from IP finder (will retry to join topology " +
                        "in 2000ms): " + addrs0);

                    U.sleep(2000);
                }
            }
            catch (IgniteInterruptedCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Joining thread was interrupted.");
            }
        }

        return false;
    }

    /**
     * @param addr Address.
     * @return Remote node ID.
     * @throws IOException In case of I/O error.
     * @throws IgniteCheckedException In case of other error.
     */
    private IgniteBiTuple<Socket, UUID> initConnection(InetSocketAddress addr) throws IOException, IgniteCheckedException {
        assert addr != null;

        joinLatch = new CountDownLatch(1);

        Socket sock = openSocket(addr);

        TcpDiscoveryHandshakeRequest req = new TcpDiscoveryHandshakeRequest(getLocalNodeId());

        req.client(true);

        writeToSocket(sock, req);

        TcpDiscoveryHandshakeResponse res = readMessage(sock, null, ackTimeout);

        UUID nodeId = res.creatorNodeId();

        assert nodeId != null;
        assert !getLocalNodeId().equals(nodeId);

        return F.t(sock, nodeId);
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    void simulateNodeFailure() {
        U.warn(log, "Simulating client node failure: " + getLocalNodeId());

        U.closeQuiet(sock);

        U.interrupt(disconnectHnd);
        U.join(disconnectHnd, log);

        U.interrupt(hbSender);
        U.join(hbSender, log);

        U.interrupt(sockRdr);
        U.join(sockRdr, log);

        U.interrupt(sockTimeoutWorker);
        U.join(sockTimeoutWorker, log);
    }

    /**
     * Disconnect handler.
     */
    private class DisconnectHandler extends IgniteSpiThread {
        /**
         */
        protected DisconnectHandler() {
            super(gridName, "tcp-client-disco-disconnect-hnd", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                try {
                    U.sleep(disconnectCheckInt);

                    if (sock == null) {
                        if (log.isDebugEnabled())
                            log.debug("Node is disconnected from topology, will try to reconnect.");

                        U.interrupt(hbSender);
                        U.join(hbSender, log);

                        U.interrupt(sockRdr);
                        U.join(sockRdr, log);

                        // If reconnection fails, try to rejoin.
                        if (!joinTopology(true)) {
                            rmtNodes.clear();

                            locNode.order(0);

                            joinTopology(false);

                            getSpiContext().recordEvent(new DiscoveryEvent(locNode,
                                "Client node reconnected: " + locNode,
                                EVT_CLIENT_NODE_RECONNECTED, locNode));
                        }
                    }
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Disconnect handler was interrupted.");

                    return;
                }
                catch (IgniteSpiException e) {
                    U.error(log, "Failed to reconnect to topology after failure.", e);
                }
            }
        }
    }

    /**
     * Heartbeat sender.
     */
    private class HeartbeatSender extends IgniteSpiThread {
        /**
         */
        protected HeartbeatSender() {
            super(gridName, "tcp-client-disco-heartbeat-sender", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            Socket sock0 = sock;

            if (sock0 == null) {
                if (log.isDebugEnabled())
                    log.debug("Failed to start heartbeat sender, node is already disconnected.");

                return;
            }

            try {
                while (!isInterrupted()) {
                    U.sleep(hbFreq);

                    TcpDiscoveryHeartbeatMessage msg = new TcpDiscoveryHeartbeatMessage(getLocalNodeId());

                    msg.client(true);

                    sockRdr.addMessage(msg);
                }
            }
            catch (IgniteInterruptedCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Heartbeat sender was interrupted.");
            }
        }
    }

    /**
     * Socket reader.
     */
    private class SocketReader extends IgniteSpiThread {
        /** Remote node ID. */
        private final UUID nodeId;

        /** Message worker. */
        private final MessageWorker msgWrk;

        /**
         * @param nodeId Node ID.
         * @param msgWrk Message worker.
         */
        protected SocketReader(UUID nodeId, MessageWorker msgWrk) {
            super(gridName, "tcp-client-disco-sock-reader", log);

            assert nodeId != null;
            assert msgWrk != null;

            this.nodeId = nodeId;
            this.msgWrk = msgWrk;
        }

        /** {@inheritDoc} */
        @Override public synchronized void start() {
            super.start();

            msgWrk.start();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            Socket sock0 = sock;

            if (sock0 == null) {
                if (log.isDebugEnabled())
                    log.debug("Failed to start socket reader, node is already disconnected.");

                return;
            }

            try {
                InputStream in = new BufferedInputStream(sock0.getInputStream());

                sock0.setKeepAlive(true);
                sock0.setTcpNoDelay(true);

                while (!isInterrupted()) {
                    try {
                        TcpDiscoveryAbstractMessage msg = marsh.unmarshal(in, U.gridClassLoader());

                        msg.senderNodeId(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Message has been received: " + msg);

                        stats.onMessageReceived(msg);

                        if (joinLatch.getCount() > 0) {
                            IgniteSpiException err = null;

                            if (msg instanceof TcpDiscoveryDuplicateIdMessage)
                                err = duplicateIdError((TcpDiscoveryDuplicateIdMessage)msg);
                            else if (msg instanceof TcpDiscoveryAuthFailedMessage)
                                err = authenticationFailedError((TcpDiscoveryAuthFailedMessage)msg);
                            else if (msg instanceof TcpDiscoveryCheckFailedMessage)
                                err = checkFailedError((TcpDiscoveryCheckFailedMessage)msg);

                            if (err != null) {
                                joinErr = err;

                                joinLatch.countDown();

                                return;
                            }
                        }

                        msgWrk.addMessage(msg);
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Failed to read message [sock=" + sock0 + ", " +
                                "locNodeId=" + getLocalNodeId() + ", rmtNodeId=" + nodeId + ']', e);

                        IOException ioEx = X.cause(e, IOException.class);

                        if (ioEx != null)
                            throw ioEx;

                        ClassNotFoundException clsNotFoundEx = X.cause(e, ClassNotFoundException.class);

                        if (clsNotFoundEx != null)
                            LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
                                "(make sure same versions of all classes are available on all nodes) " +
                                "[rmtNodeId=" + nodeId + ", err=" + clsNotFoundEx.getMessage() + ']');
                        else
                            LT.error(log, e, "Failed to read message [sock=" + sock0 + ", locNodeId=" +
                                getLocalNodeId() + ", rmtNodeId=" + nodeId + ']');
                    }
                }
            }
            catch (IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Connection failed [sock=" + sock0 + ", locNodeId=" +
                        getLocalNodeId() + ", rmtNodeId=" + nodeId + ']', e);
            }
            finally {
                U.closeQuiet(sock0);

                U.interrupt(msgWrk);

                try {
                    U.join(msgWrk);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                sock = null;
            }
        }

        /**
         * @param msg Message.
         */
        void addMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            msgWrk.addMessage(msg);
        }
    }

    /**
     * Message worker.
     */
    private class MessageWorker extends MessageWorkerAdapter {
        /** Topology history. */
        private final NavigableMap<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

        /** Indicates that reconnection is in progress. */
        private boolean recon;

        /** Indicates that pending messages are currently processed. */
        private boolean pending;

        /**
         * @param recon Whether reconnection is in progress.
         */
        protected MessageWorker(boolean recon) {
            super("tcp-client-disco-msg-worker");

            this.recon = recon;
        }

        /** {@inheritDoc} */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;
            assert msg.verified() || msg.senderNodeId() == null;

            stats.onMessageProcessingStarted(msg);

            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                processClientReconnectMessage((TcpDiscoveryClientReconnectMessage)msg);
            else {
                if (recon && !pending) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding message received during reconnection: " + msg);
                }
                else {
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

                    if (ensured(msg))
                        lastMsgId = msg.id();
                }
            }

            stats.onMessageProcessingFinished(msg);
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
            if (leaveLatch != null)
                return;

            TcpDiscoveryNode node = msg.node();

            UUID newNodeId = node.id();

            if (getLocalNodeId().equals(newNodeId)) {
                if (joinLatch.getCount() > 0) {
                    Collection<TcpDiscoveryNode> top = msg.topology();

                    if (top != null) {
                        gridStartTime = msg.gridStartTime();

                        for (TcpDiscoveryNode n : top) {
                            if (n.order() > 0)
                                n.visible(true);

                            rmtNodes.put(n.id(), n);
                        }

                        topHist.clear();

                        if (msg.topologyHistory() != null)
                            topHist.putAll(msg.topologyHistory());

                        Map<UUID, Map<Integer, byte[]>> dataMap = msg.oldNodesDiscoveryData();

                        if (dataMap != null) {
                            for (Map.Entry<UUID, Map<Integer, byte[]>> entry : dataMap.entrySet())
                                onExchange(newNodeId, entry.getKey(), entry.getValue(), null);
                        }

                        locNode.setAttributes(node.attributes());
                        locNode.visible(true);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Discarding node added message with empty topology: " + msg);
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node added message (this message has already been processed) " +
                        "[msg=" + msg + ", locNode=" + locNode + ']');
            }
            else {
                boolean topChanged = rmtNodes.putIfAbsent(newNodeId, node) == null;

                if (topChanged) {
                    if (log.isDebugEnabled())
                        log.debug("Added new node to topology: " + node);

                    Map<Integer, byte[]> data = msg.newNodeDiscoveryData();

                    if (data != null)
                        onExchange(newNodeId, newNodeId, data, null);
                }
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddFinishedMessage(TcpDiscoveryNodeAddFinishedMessage msg) {
            if (leaveLatch != null)
                return;

            if (getLocalNodeId().equals(msg.nodeId())) {
                if (joinLatch.getCount() > 0) {
                    long topVer = msg.topologyVersion();

                    locNode.order(topVer);

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode, updateTopologyHistory(topVer));

                    joinErr = null;

                    joinLatch.countDown();
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message (this message has already been processed) " +
                        "[msg=" + msg + ", locNode=" + locNode + ']');
            }
            else {
                TcpDiscoveryNode node = rmtNodes.get(msg.nodeId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node add finished message since node is not found [msg=" + msg + ']');

                    return;
                }

                long topVer = msg.topologyVersion();

                node.order(topVer);
                node.visible(true);

                if (locNodeVer.equals(node.version()))
                    node.version(locNodeVer);

                Collection<ClusterNode> top = updateTopologyHistory(topVer);

                if (!pending && joinLatch.getCount() > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node add finished message (join process is not finished): " + msg);

                    return;
                }

                notifyDiscovery(EVT_NODE_JOINED, topVer, node, top);

                stats.onNodeJoined();
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeLeftMessage(TcpDiscoveryNodeLeftMessage msg) {
            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Received node left message for local node: " + msg);

                CountDownLatch leaveLatch0 = leaveLatch;

                assert leaveLatch0 != null;

                leaveLatch0.countDown();
            }
            else {
                if (leaveLatch != null)
                    return;

                TcpDiscoveryNode node = rmtNodes.remove(msg.creatorNodeId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node left message since node is not found [msg=" + msg + ']');

                    return;
                }

                Collection<ClusterNode> top = updateTopologyHistory(msg.topologyVersion());

                if (!pending && joinLatch.getCount() > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node left message (join process is not finished): " + msg);

                    return;
                }

                notifyDiscovery(EVT_NODE_LEFT, msg.topologyVersion(), node, top);

                stats.onNodeLeft();
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeFailedMessage(TcpDiscoveryNodeFailedMessage msg) {
            if (leaveLatch != null)
                return;

            if (!getLocalNodeId().equals(msg.creatorNodeId())) {
                TcpDiscoveryNode node = rmtNodes.remove(msg.failedNodeId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message since node is not found [msg=" + msg + ']');

                    return;
                }

                Collection<ClusterNode> top = updateTopologyHistory(msg.topologyVersion());

                if (!pending && joinLatch.getCount() > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message (join process is not finished): " + msg);

                    return;
                }

                notifyDiscovery(EVT_NODE_FAILED, msg.topologyVersion(), node, top);

                stats.onNodeFailed();
            }
        }

        /**
         * @param msg Message.
         */
        private void processHeartbeatMessage(TcpDiscoveryHeartbeatMessage msg) {
            if (leaveLatch != null)
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                if (msg.senderNodeId() == null) {
                    Socket sock0 = sock;

                    if (sock0 != null) {
                        UUID nodeId = ignite.configuration().getNodeId();

                        msg.setMetrics(nodeId, metricsProvider.metrics());

                        msg.setCacheMetrics(nodeId, metricsProvider.cacheMetrics());

                        try {
                            writeToSocket(sock0, msg);

                            if (log.isDebugEnabled())
                                log.debug("Heartbeat message sent [sock=" + sock0 + ", msg=" + msg + ']');
                        }
                        catch (IOException | IgniteCheckedException e) {
                            if (log.isDebugEnabled())
                                U.error(log, "Failed to send heartbeat message [sock=" + sock0 +
                                    ", msg=" + msg + ']', e);

                            U.closeQuiet(sock0);

                            sock = null;

                            interrupt();
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Failed to send heartbeat message (node is disconnected): " + msg);
                }
                else if (log.isDebugEnabled())
                    log.debug("Received heartbeat response: " + msg);
            }
            else {
                long tstamp = U.currentTimeMillis();

                if (msg.hasMetrics()) {
                    for (Map.Entry<UUID, MetricsSet> e : msg.metrics().entrySet()) {
                        UUID nodeId = e.getKey();

                        MetricsSet metricsSet = e.getValue();

                        Map<Integer, CacheMetrics> cacheMetrics = msg.hasCacheMetrics() ?
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
            if (leaveLatch != null)
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                if (msg.success()) {
                    pending = true;

                    try {
                        for (TcpDiscoveryAbstractMessage pendingMsg : msg.pendingMessages())
                            processMessage(pendingMsg);
                    }
                    finally {
                        pending = false;
                    }

                    joinErr = null;
                    reconFailed = false;

                    joinLatch.countDown();
                }
                else {
                    joinErr = null;
                    reconFailed = true;

                    getSpiContext().recordEvent(new DiscoveryEvent(locNode,
                        "Client node disconnected: " + locNode,
                        EVT_CLIENT_NODE_DISCONNECTED, locNode));

                    joinLatch.countDown();
                }
            }
            else if (log.isDebugEnabled())
                log.debug("Discarding reconnect message for another client: " + msg);
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
            assert cacheMetrics != null;

            TcpDiscoveryNode node = nodeId.equals(getLocalNodeId()) ? locNode : rmtNodes.get(nodeId);

            if (node != null && node.visible()) {
                node.setMetrics(metrics);
                node.setCacheMetrics(cacheMetrics);

                node.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, topVer, node, allNodes());
            }
            else if (log.isDebugEnabled())
                log.debug("Received metrics from unknown node: " + nodeId);
        }

        /**
         * @param topVer New topology version.
         * @return Latest topology snapshot.
         */
        private Collection<ClusterNode> updateTopologyHistory(long topVer) {
            TcpClientDiscoverySpi.this.topVer = topVer;

            Collection<ClusterNode> allNodes = allNodes();

            if (!topHist.containsKey(topVer)) {
                assert topHist.isEmpty() || topHist.lastKey() == topVer - 1 :
                    "lastVer=" + topHist.lastKey() + ", newVer=" + topVer;

                topHist.put(topVer, allNodes);

                if (topHist.size() > topHistSize)
                    topHist.pollFirstEntry();

                assert topHist.lastKey() == topVer;
                assert topHist.size() <= topHistSize;
            }

            return allNodes;
        }

        /**
         * @return All nodes.
         */
        private Collection<ClusterNode> allNodes() {
            Collection<ClusterNode> allNodes = new TreeSet<>();

            for (TcpDiscoveryNode node : rmtNodes.values()) {
                if (node.visible())
                    allNodes.add(node);
            }

            allNodes.add(locNode);

            return allNodes;
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param top Topology snapshot.
         */
        private void notifyDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> top) {
            DiscoverySpiListener lsnr = TcpClientDiscoverySpi.this.lsnr;

            if (lsnr != null) {
                if (log.isDebugEnabled())
                    log.debug("Discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                        ", topVer=" + topVer + ']');

                lsnr.onDiscovery(type, topVer, node, top, new TreeMap<>(topHist), null);
            }
            else if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                    ", topVer=" + topVer + ']');
        }
    }
}
