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
public class GridTcpClientDiscoverySpi extends GridTcpDiscoverySpiAdapter {
    /** Default disconnect check interval. */
    public static final long DFLT_DISCONNECT_CHECK_INT = 2000;

    /** Remote nodes. */
    private final ConcurrentMap<UUID, GridTcpDiscoveryNode> rmtNodes = new ConcurrentHashMap8<>();

    /** Socket. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile Socket sock;

    /** Socket reader. */
    private volatile SocketReader sockRdr;

    /** Disconnect handler. */
    private DisconnectHandler disconnectHnd;

    /** Joined latch. */
    private CountDownLatch joinLatch;

    /** Disconnect check interval. */
    private long disconnectCheckInt = DFLT_DISCONNECT_CHECK_INT;

    /**
     * Gets disconnect check interval.
     *
     * @return Disconnect check interval.
     */
    public long getDisconnectCheckInterval() {
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

        joinTopology();

        disconnectHnd = new DisconnectHandler();
        disconnectHnd.start();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        rmtNodes.clear();

        Socket sock0 = sock;

        if (sock0 != null) {
            try {
                GridTcpDiscoveryNodeLeftMessage msg = new GridTcpDiscoveryNodeLeftMessage(locNodeId);

                msg.client(true);

                writeToSocket(sock0, msg);
            }
            catch (IOException | GridException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Failed to send node left message (will stop anyway) [sock=" + sock0 + ']', e);
            }

            closeConnection();
        }

        U.interrupt(sockTimeoutWorker);
        U.join(sockTimeoutWorker, log);

        U.interrupt(disconnectHnd);
        U.join(disconnectHnd, log);

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        return Arrays.<Object>asList(ipFinder);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> getRemoteNodes() {
        return new ArrayList<GridNode>(rmtNodes.values());
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNode getNode(UUID nodeId) {
        if (locNodeId.equals(nodeId))
            return locNode;

        return rmtNodes.get(nodeId);
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return false; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws GridSpiException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void reconnect() throws GridSpiException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(GridDiscoverySpiNodeAuthenticator auth) {
        // No-op.
    }

    /**
     * @throws GridSpiException In case of error.
     */
    private void joinTopology() throws GridSpiException {
        stats.onJoinStarted();

        Collection<InetSocketAddress> addrs = resolvedAddresses();

        if (addrs == null || addrs.isEmpty())
            throw new GridSpiException("No addresses registered in the IP finder: " + ipFinder);

        List<InetSocketAddress> shuffled = new ArrayList<>(addrs);

        Collections.shuffle(shuffled);

        GridTcpDiscoveryJoinRequestMessage req = new GridTcpDiscoveryJoinRequestMessage(locNode,
            exchange.collect(locNodeId));

        req.client(true);

        while (true) {
            boolean retry = false;

            Iterator<InetSocketAddress> it = shuffled.iterator();

            while (it.hasNext()) {
                InetSocketAddress addr = it.next();

                try {
                    long ts = U.currentTimeMillis();

                    UUID rmtNodeId = initConnection(addr);

                    Socket sock0 = sock;

                    stats.onClientSocketInitialized(U.currentTimeMillis() - ts);

                    writeToSocket(sock0, req);

                    int res = readReceipt(sock0, ackTimeout);

                    switch (res) {
                        case RES_OK:
                            sockRdr = new SocketReader(sock0, rmtNodeId, new MessageWorker(sock0));
                            sockRdr.start();

                            if (U.await(joinLatch, netTimeout, MILLISECONDS)) {
                                if (log.isDebugEnabled())
                                    log.debug("Successfully connected to topology [sock=" + sock0 + ']');

                                stats.onJoinFinished();

                                return;
                            }
                            else {
                                throw new GridSpiException("Join process timed out [sock=" + sock0 +
                                    ", timeout=" + netTimeout + ']');
                            }

                        case RES_CONTINUE_JOIN:
                        case RES_WAIT:
                            closeConnection();

                            retry = true;

                            break;

                        default:
                            throw new GridSpiException("Unexpected response to join request: " + res);
                    }
                }
                catch (GridException | IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Failed to establish connection with address: " + addr, e);

                    closeConnection();

                    it.remove();
                }
            }

            if (!retry)
                break;
        }

        throw new GridSpiException("Failed to connect to any address from IP finder (make sure " +
            "IP finder addresses are correct, and operating system firewalls are disabled on all " +
            "host machines): " + addrs);
    }

    /**
     * @param addr Address.
     * @return Remote node ID.
     * @throws IOException In case of I/O error.
     * @throws GridException In case of other error.
     */
    private UUID initConnection(InetSocketAddress addr) throws IOException, GridException {
        assert sock == null;
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

        this.sock = sock;

        return nodeId;
    }

    /**
     * Closes connection.
     */
    private void closeConnection() {
        U.closeQuiet(sock);

        SocketReader sockRdr0 = sockRdr;

        U.interrupt(sockRdr0);
        U.join(sockRdr0, log);

        sock = null;
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    void simulateNodeFailure() {
        U.warn(log, "Simulating client node failure: " + locNodeId);

        closeConnection();

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
            try {
                while (!isInterrupted()) {
                    U.sleep(disconnectCheckInt);

                    if (sock == null) {
                        if (log.isDebugEnabled())
                            log.debug("Node disconnected from topology, will try to reconnect.");

                        rmtNodes.clear();

                        locNode.order(0);

                        joinTopology();
                    }
                }
            }
            catch (GridInterruptedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Disconnect handler was interrupted.");
            }
            catch (GridSpiException e) {
                U.error(log, "Failed to reconnect to topology after failure.", e);
            }
        }
    }

    /**
     * Socket reader.
     */
    private class SocketReader extends GridSpiThread {
        /** Socket. */
        private final Socket sock;

        /** Remote node ID. */
        private final UUID nodeId;

        /** Message worker. */
        private final MessageWorker msgWrk;

        /**
         * @param sock Socket.
         * @param nodeId Node ID.
         */
        protected SocketReader(Socket sock, UUID nodeId, MessageWorker msgWrk) {
            super(gridName, "tcp-client-disco-sock-reader", log);

            assert sock != null;
            assert nodeId != null;
            assert msgWrk != null;

            this.sock = sock;
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
            try {
                InputStream in = new BufferedInputStream(sock.getInputStream());

                sock.setKeepAlive(true);
                sock.setTcpNoDelay(true);

                while (!isInterrupted()) {
                    try {
                        GridTcpDiscoveryAbstractMessage msg = marsh.unmarshal(in, U.gridClassLoader());

                        msg.senderNodeId(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Message has been received: " + msg);

                        stats.onMessageReceived(msg);

                        msgWrk.addMessage(msg);
                    }
                    catch (GridException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Failed to read message [sock=" + sock + ", locNodeId=" + locNodeId +
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
                            LT.error(log, e, "Failed to read message [sock=" + sock + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']');
                    }
                }
            }
            catch (IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Connection failed [sock=" + sock + ", locNodeId=" + locNodeId +
                        ", rmtNodeId=" + nodeId + ']', e);
            }
            finally {
                closeConnection();

                U.interrupt(msgWrk);
                U.join(msgWrk, log);
            }
        }
    }

    /**
     * Message worker.
     */
    private class MessageWorker extends MessageWorkerAdapter {
        /**
         * @param sock Socket.
         */
        protected MessageWorker(Socket sock) {
            super("tcp-client-disco-msg-worker", sock);
        }

        /** {@inheritDoc} */
        @Override protected void processMessage(GridTcpDiscoveryAbstractMessage msg) {
            assert msg != null;
            assert msg.verified();

            stats.onMessageProcessingStarted(msg);

            if (msg instanceof GridTcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((GridTcpDiscoveryNodeAddedMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((GridTcpDiscoveryNodeAddFinishedMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((GridTcpDiscoveryNodeLeftMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((GridTcpDiscoveryNodeFailedMessage)msg);

            stats.onMessageProcessingFinished(msg);
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddedMessage(GridTcpDiscoveryNodeAddedMessage msg) {
            GridTcpDiscoveryNode node = msg.node();

            if (locNodeId.equals(node.id())) {
                if (joinLatch.getCount() > 0) {
                    Collection<GridTcpDiscoveryNode> top = msg.topology();

                    if (top != null) {
                        for (GridTcpDiscoveryNode n : top) {
                            n.visible(true);

                            rmtNodes.put(n.id(), n);
                        }

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
                boolean topChanged = rmtNodes.putIfAbsent(node.id(), node) == null;

                if (topChanged) {
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

                    joinLatch.countDown();

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode);
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

                if (locNodeVer.equals(node.version()))
                    node.version(locNodeVer);

                notifyDiscovery(EVT_NODE_JOINED, topVer, node);

                stats.onNodeJoined();
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeLeftMessage(GridTcpDiscoveryNodeLeftMessage msg) {
            assert !locNodeId.equals(msg.creatorNodeId());

            GridTcpDiscoveryNode node = rmtNodes.remove(msg.creatorNodeId());

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node add left message since node is not found [msg=" + msg + ']');

                return;
            }

            notifyDiscovery(EVT_NODE_LEFT, msg.topologyVersion(), node);

            stats.onNodeLeft();
        }

        /**
         * @param msg Message.
         */
        private void processNodeFailedMessage(GridTcpDiscoveryNodeFailedMessage msg) {
            assert !locNodeId.equals(msg.creatorNodeId());

            GridTcpDiscoveryNode node = rmtNodes.remove(msg.failedNodeId());

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node add failed message since node is not found [msg=" + msg + ']');

                return;
            }

            notifyDiscovery(EVT_NODE_FAILED, msg.topologyVersion(), node);

            stats.onNodeFailed();
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         */
        private void notifyDiscovery(int type, long topVer, GridNode node) {
            GridDiscoverySpiListener lsnr = GridTcpClientDiscoverySpi.this.lsnr;

            if (lsnr != null) {
                if (log.isDebugEnabled())
                    log.debug("Discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                        ", topVer=" + topVer + ']');

                Collection<GridNode> rmtNodes0 = new ArrayList<GridNode>(rmtNodes.values());

                lsnr.onDiscovery(type, topVer, node, F.concat(false, locNode, rmtNodes0), null);
            }
            else if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", type=" + U.gridEventName(type) +
                    ", topVer=" + topVer + ']');
        }
    }
}
