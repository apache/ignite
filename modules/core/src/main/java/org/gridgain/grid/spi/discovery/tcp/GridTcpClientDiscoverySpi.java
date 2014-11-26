/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast.*;
import org.gridgain.grid.spi.discovery.tcp.messages.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * TODO
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
@GridSpiMultipleInstancesSupport(true)
@GridDiscoverySpiOrderSupport(true)
@GridDiscoverySpiHistorySupport(true)
public class GridTcpClientDiscoverySpi extends GridTcpDiscoverySpiAdapter implements GridTcpClientDiscoverySpiMBean {
    /** Default disconnect check interval. */
    public static final long DFLT_DISCONNECT_CHECK_INT = 2000;

    /** Remote nodes. */
    private final ConcurrentMap<UUID, GridTcpDiscoveryNode> rmtNodes = new ConcurrentHashMap8<>();

    /** Socket. */
    private volatile Socket sock;

    /** Socket reader. */
    private volatile SocketReader sockRdr;

    /** Heartbeat sender. */
    private volatile HeartbeatSender hbSender;

    /** Disconnect handler. */
    private volatile DisconnectHandler disconnectHnd;

    /** Join error. */
    private GridSpiException joinErr;

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
    @GridSpiConfiguration(optional = true)
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
    @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
        startStopwatch();

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new GridSpiException("Unknown local address: " + locAddr, e);
        }

        if (ipFinder instanceof GridTcpDiscoveryMulticastIpFinder) {
            GridTcpDiscoveryMulticastIpFinder mcastIpFinder = ((GridTcpDiscoveryMulticastIpFinder)ipFinder);

            if (mcastIpFinder.getLocalAddress() == null)
                mcastIpFinder.setLocalAddress(locAddr);
        }

        GridBiTuple<Collection<String>, Collection<String>> addrs;

        try {
            addrs = U.resolveLocalAddresses(locHost);
        }
        catch (IOException | GridException e) {
            throw new GridSpiException("Failed to resolve local host to set of external addresses: " + locHost, e);
        }

        locNode = new GridTcpDiscoveryNode(
            locNodeId,
            addrs.get1(),
            addrs.get2(),
            0,
            metricsProvider,
            locNodeVer,
            true);

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
    @Override public void spiStop() throws GridSpiException {
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
                GridTcpDiscoveryNodeLeftMessage msg = new GridTcpDiscoveryNodeLeftMessage(locNodeId);

                msg.client(true);

                writeToSocket(sock0, msg);

                if (!U.await(leaveLatch, netTimeout, MILLISECONDS)) {
                    if (log.isDebugEnabled())
                        log.debug("Did not receive node left message for local node (will stop anyway) [sock=" +
                            sock0 + ']');
                }
            }
            catch (IOException | GridException e) {
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

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        return Arrays.<Object>asList(ipFinder);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> getRemoteNodes() {
        return F.view(U.<GridTcpDiscoveryNode, GridNode>arrayList(rmtNodes.values(), new P1<GridTcpDiscoveryNode>() {
            @Override public boolean apply(GridTcpDiscoveryNode node) {
                return node.visible();
            }
        }));
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNode getNode(UUID nodeId) {
        if (locNodeId.equals(nodeId))
            return locNode;

        GridTcpDiscoveryNode node = rmtNodes.get(nodeId);

        return node != null && node.visible() ? node : null;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        assert nodeId != null;

        if (nodeId.equals(locNodeId))
            return true;

        GridTcpDiscoveryNode node = rmtNodes.get(nodeId);

        return node != null && node.visible();
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws GridSpiException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(GridDiscoverySpiNodeAuthenticator auth) {
        // No-op.
    }

    /**
     * @param recon Reconnect flag.
     * @throws GridSpiException In case of error.
     */
    private void joinTopology(boolean recon) throws GridSpiException {
        stats.onJoinStarted();

        GridTcpDiscoveryJoinRequestMessage req = new GridTcpDiscoveryJoinRequestMessage(locNode,
            exchange.collect(locNodeId));

        req.client(true);
        req.clientReconnect(recon);

        Collection<InetSocketAddress> addrs = null;
        List<InetSocketAddress> shuffledAddrs = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                while (shuffledAddrs == null || shuffledAddrs.isEmpty()) {
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

                Iterator<InetSocketAddress> it = shuffledAddrs.iterator();

                while (it.hasNext() && !Thread.currentThread().isInterrupted()) {
                    InetSocketAddress addr = it.next();

                    Socket sock = null;

                    try {
                        long ts = U.currentTimeMillis();

                        GridBiTuple<Socket, UUID> t = initConnection(addr);

                        sock = t.get1();

                        UUID rmtNodeId = t.get2();

                        stats.onClientSocketInitialized(U.currentTimeMillis() - ts);

                        locNode.clientRouterNodeId(rmtNodeId);

                        writeToSocket(sock, req);

                        int res = readReceipt(sock, ackTimeout);

                        switch (res) {
                            case RES_OK:
                                this.sock = sock;

                                sockRdr = new SocketReader(rmtNodeId, new MessageWorker());
                                sockRdr.start();

                                if (U.await(joinLatch, netTimeout, MILLISECONDS)) {
                                    GridSpiException joinErr0 = joinErr;

                                    if (joinErr0 != null)
                                        throw joinErr0;

                                    if (log.isDebugEnabled())
                                        log.debug("Successfully connected to topology [sock=" + sock + ']');

                                    hbSender = new HeartbeatSender();
                                    hbSender.start();

                                    stats.onJoinFinished();

                                    return;
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
                    catch (GridInterruptedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Joining thread was interrupted.");

                        return;
                    }
                    catch (IOException | GridException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Failed to establish connection with address: " + addr, e);

                        U.closeQuiet(sock);

                        it.remove();
                    }
                }

                if (shuffledAddrs.isEmpty()) {
                    U.warn(log, "Failed to connect to any address from IP finder (will retry to join topology " +
                        "in 2000ms): " + addrs);

                    U.sleep(2000);
                }
            }
            catch (GridInterruptedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Joining thread was interrupted.");
            }
        }
    }

    /**
     * @param addr Address.
     * @return Remote node ID.
     * @throws IOException In case of I/O error.
     * @throws GridException In case of other error.
     */
    private GridBiTuple<Socket, UUID> initConnection(InetSocketAddress addr) throws IOException, GridException {
        assert addr != null;

        joinLatch = new CountDownLatch(1);

        Socket sock = openSocket(addr);

        GridTcpDiscoveryHandshakeRequest req = new GridTcpDiscoveryHandshakeRequest(locNodeId);

        req.client(true);

        writeToSocket(sock, req);

        GridTcpDiscoveryHandshakeResponse res = readMessage(sock, null, ackTimeout);

        UUID nodeId = res.creatorNodeId();

        assert nodeId != null;
        assert !locNodeId.equals(nodeId);

        return F.t(sock, nodeId);
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    void simulateNodeFailure() {
        U.warn(log, "Simulating client node failure: " + locNodeId);

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
    private class DisconnectHandler extends GridSpiThread {
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

                        rmtNodes.clear();

                        locNode.order(0);

                        joinTopology(true);
                    }
                }
                catch (GridInterruptedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Disconnect handler was interrupted.");

                    return;
                }
                catch (GridSpiException e) {
                    U.error(log, "Failed to reconnect to topology after failure.", e);
                }
            }
        }
    }

    /**
     * Heartbeat sender.
     */
    private class HeartbeatSender extends GridSpiThread {
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

                    GridTcpDiscoveryHeartbeatMessage msg = new GridTcpDiscoveryHeartbeatMessage(locNodeId);

                    msg.client(true);

                    sockRdr.addMessage(msg);
                }
            }
            catch (GridInterruptedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Heartbeat sender was interrupted.");
            }
        }
    }

    /**
     * Socket reader.
     */
    private class SocketReader extends GridSpiThread {
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
                        GridTcpDiscoveryAbstractMessage msg = marsh.unmarshal(in, U.gridClassLoader());

                        msg.senderNodeId(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Message has been received: " + msg);

                        stats.onMessageReceived(msg);

                        GridSpiException err = null;

                        if (joinLatch.getCount() > 0) {
                            if (msg instanceof GridTcpDiscoveryDuplicateIdMessage)
                                err = duplicateIdError((GridTcpDiscoveryDuplicateIdMessage)msg);
                            else if (msg instanceof GridTcpDiscoveryAuthFailedMessage)
                                err = authenticationFailedError((GridTcpDiscoveryAuthFailedMessage)msg);
                            else if (msg instanceof GridTcpDiscoveryCheckFailedMessage)
                                err = checkFailedError((GridTcpDiscoveryCheckFailedMessage)msg);

                            if (err != null) {
                                joinErr = err;

                                joinLatch.countDown();

                                return;
                            }
                        }

                        msgWrk.addMessage(msg);
                    }
                    catch (GridException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Failed to read message [sock=" + sock0 + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']', e);

                        IOException ioEx = X.cause(e, IOException.class);

                        if (ioEx != null)
                            throw ioEx;

                        ClassNotFoundException clsNotFoundEx = X.cause(e, ClassNotFoundException.class);

                        if (clsNotFoundEx != null)
                            LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
                                "(make sure same versions of all classes are available on all nodes) " +
                                "[rmtNodeId=" + nodeId + ", err=" + clsNotFoundEx.getMessage() + ']');
                        else
                            LT.error(log, e, "Failed to read message [sock=" + sock0 + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']');
                    }
                }
            }
            catch (IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Connection failed [sock=" + sock0 + ", locNodeId=" + locNodeId +
                        ", rmtNodeId=" + nodeId + ']', e);
            }
            finally {
                U.closeQuiet(sock0);

                U.interrupt(msgWrk);

                try {
                    U.join(msgWrk);
                }
                catch (GridInterruptedException ignored) {
                    // No-op.
                }

                sock = null;
            }
        }

        /**
         * @param msg Message.
         */
        void addMessage(GridTcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            msgWrk.addMessage(msg);
        }
    }

    /**
     * Message worker.
     */
    private class MessageWorker extends MessageWorkerAdapter {
        /** Topology history. */
        private final NavigableMap<Long, Collection<GridNode>> topHist = new TreeMap<>();

        /** Current topology version. */
        private long topVer;

        /**
         */
        protected MessageWorker() {
            super("tcp-client-disco-msg-worker");
        }

        /** {@inheritDoc} */
        @Override protected void processMessage(GridTcpDiscoveryAbstractMessage msg) {
            assert msg != null;
            assert msg.verified() || msg.senderNodeId() == null;

            stats.onMessageProcessingStarted(msg);

            if (msg instanceof GridTcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((GridTcpDiscoveryNodeAddedMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((GridTcpDiscoveryNodeAddFinishedMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((GridTcpDiscoveryNodeLeftMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((GridTcpDiscoveryNodeFailedMessage)msg);
            else if (msg instanceof GridTcpDiscoveryHeartbeatMessage)
                processHeartbeatMessage((GridTcpDiscoveryHeartbeatMessage)msg);

            stats.onMessageProcessingFinished(msg);
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddedMessage(GridTcpDiscoveryNodeAddedMessage msg) {
            GridTcpDiscoveryNode node = msg.node();

            UUID newNodeId = node.id();

            if (locNodeId.equals(newNodeId)) {
                if (joinLatch.getCount() > 0) {
                    Collection<GridTcpDiscoveryNode> top = msg.topology();

                    if (top != null) {
                        for (GridTcpDiscoveryNode n : top) {
                            if (n.order() > 0)
                                n.visible(true);

                            rmtNodes.put(n.id(), n);
                        }

                        topHist.clear();

                        if (msg.topologyHistory() != null)
                            topHist.putAll(msg.topologyHistory());

                        Collection<List<Object>> dataList = msg.oldNodesDiscoveryData();

                        if (dataList != null) {
                            for (List<Object> discoData : dataList)
                                exchange.onExchange(discoData);
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

                    Socket sock0 = sock;

                    if (sock0 != null) {
                        try {
                            GridTcpDiscoveryNodeAddedClientResponse res =
                                new GridTcpDiscoveryNodeAddedClientResponse(locNodeId, msg.id(),
                                    exchange.collect(newNodeId));

                            res.client(true);

                            writeToSocket(sock0, res);
                        }
                        catch (IOException | GridException e) {
                            if (log.isDebugEnabled())
                                U.error(log, "Failed to send node added response [sock=" + sock0 + ']', e);

                            U.closeQuiet(sock0);

                            sock = null;

                            interrupt();
                        }
                    }

                    List<Object> data = msg.newNodeDiscoveryData();

                    if (data != null)
                        exchange.onExchange(data);
                }
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddFinishedMessage(GridTcpDiscoveryNodeAddFinishedMessage msg) {
            if (locNodeId.equals(msg.nodeId())) {
                if (joinLatch.getCount() > 0) {
                    long topVer = msg.topologyVersion();

                    locNode.order(topVer);

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode, updateTopology(topVer));

                    joinErr = null;

                    joinLatch.countDown();
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message (this message has already been processed) " +
                        "[msg=" + msg + ", locNode=" + locNode + ']');
            }
            else {
                GridTcpDiscoveryNode node = rmtNodes.get(msg.nodeId());

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

                Collection<GridNode> top = updateTopology(topVer);

                if (joinLatch.getCount() > 0) {
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
        private void processNodeLeftMessage(GridTcpDiscoveryNodeLeftMessage msg) {
            if (locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Received node left message for local node: " + msg);

                CountDownLatch leaveLatch0 = leaveLatch;

                assert leaveLatch0 != null;

                leaveLatch0.countDown();
            }
            else {
                GridTcpDiscoveryNode node = rmtNodes.remove(msg.creatorNodeId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node left message since node is not found [msg=" + msg + ']');

                    return;
                }

                if (joinLatch.getCount() > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node left message (join process is not finished): " + msg);

                    return;
                }

                notifyDiscovery(EVT_NODE_LEFT, msg.topologyVersion(), node, updateTopology(msg.topologyVersion()));

                stats.onNodeLeft();
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeFailedMessage(GridTcpDiscoveryNodeFailedMessage msg) {
            if (!locNodeId.equals(msg.creatorNodeId())) {
                GridTcpDiscoveryNode node = rmtNodes.remove(msg.failedNodeId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message since node is not found [msg=" + msg + ']');

                    return;
                }

                if (joinLatch.getCount() > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message (join process is not finished): " + msg);

                    return;
                }

                notifyDiscovery(EVT_NODE_FAILED, msg.topologyVersion(), node, updateTopology(msg.topologyVersion()));

                stats.onNodeFailed();
            }
        }

        /**
         * @param msg Message.
         */
        private void processHeartbeatMessage(GridTcpDiscoveryHeartbeatMessage msg) {
            if (locNodeId.equals(msg.creatorNodeId())) {
                if (msg.senderNodeId() == null) {
                    Socket sock0 = sock;

                    if (sock0 != null) {
                        msg.setMetrics(locNodeId, metricsProvider.getMetrics());

                        try {
                            writeToSocket(sock0, msg);

                            if (log.isDebugEnabled())
                                log.debug("Heartbeat message sent [sock=" + sock0 + ", msg=" + msg + ']');
                        }
                        catch (IOException | GridException e) {
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
                if (msg.hasMetrics()) {
                    for (Map.Entry<UUID, GridNodeMetrics> e : msg.metrics().entrySet()) {
                        UUID nodeId = e.getKey();

                        GridTcpDiscoveryNode node = nodeId.equals(locNodeId) ? locNode : rmtNodes.get(nodeId);

                        if (node != null) {
                            node.setMetrics(e.getValue());

                            node.lastUpdateTime(U.currentTimeMillis());

                            notifyDiscovery(EVT_NODE_METRICS_UPDATED, topVer, node, updateTopology(topVer));
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Received metrics from unknown node: " + nodeId);
                    }
                }
            }
        }

        /**
         * @param topVer New topology version.
         * @return Topology snapshot.
         */
        private Collection<GridNode> updateTopology(long topVer) {
            this.topVer = topVer;

            Collection<GridNode> allNodes = new ArrayList<>(rmtNodes.size() + 1);

            for (GridTcpDiscoveryNode node : rmtNodes.values()) {
                if (node.visible())
                    allNodes.add(node);
            }

            allNodes.add(locNode);

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
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param top Topology snapshot.
         */
        private void notifyDiscovery(int type, long topVer, GridNode node, Collection<GridNode> top) {
            GridDiscoverySpiListener lsnr = GridTcpClientDiscoverySpi.this.lsnr;

            if (lsnr != null) {
                if (log.isDebugEnabled())
                    log.debug("Discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                        ", topVer=" + topVer + ']');

                lsnr.onDiscovery(type, topVer, node, top, new TreeMap<>(topHist));
            }
            else if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                    ", topVer=" + topVer + ']');
        }
    }
}
