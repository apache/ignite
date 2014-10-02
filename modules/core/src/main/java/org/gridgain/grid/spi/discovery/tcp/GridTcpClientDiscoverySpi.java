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
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.spi.discovery.tcp.internal.GridTcpDiscoverySpiState.*;

/**
 * TODO
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
@GridSpiMultipleInstancesSupport(true)
@GridDiscoverySpiOrderSupport(true)
@GridDiscoverySpiHistorySupport(true)
public class GridTcpClientDiscoverySpi extends GridTcpDiscoverySpiAdapter {
    /** Mutex. */
    private final Object mux = new Object();

    /** Socket reader. */
    private SocketReader sockRdr;

    /** Message worker. */
    private MessageWorker msgWrk;

    /** */
    private volatile Map<UUID, GridNode> rmtNodes;

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

        stats.onJoinStarted();

        joinTopology();

        stats.onJoinFinished();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        closeConnection();

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
        Map<UUID, GridNode> rmtNodes0 = rmtNodes;

        return rmtNodes0 != null ? rmtNodes0.values() : Collections.<GridNode>emptyList();
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
        synchronized (mux) {
            assert spiState == DISCONNECTED;

            spiState = CONNECTING;
        }

        Collection<InetSocketAddress> addrs = resolvedAddresses();

        if (F.isEmpty(addrs))
            throw new GridSpiException("No addresses registered in the IP finder: " + ipFinder);

        List<InetSocketAddress> shuffled = new ArrayList<>(addrs);

        Collections.shuffle(shuffled);

        GridTcpDiscoveryJoinRequestMessage joinReq =
            new GridTcpDiscoveryJoinRequestMessage(locNode, exchange.collect(locNodeId), true);

        while (true) {
            boolean retry = false;

            Iterator<InetSocketAddress> it = shuffled.iterator();

            while (it.hasNext()) {
                InetSocketAddress addr = it.next();

                try {
                    long ts = U.currentTimeMillis();

                    Socket sock = openSocket(addr);

                    initConnection(sock);

                    stats.onClientSocketInitialized(U.currentTimeMillis() - ts);

                    writeToSocket(sock, joinReq);

                    int res = readReceipt(sock, ackTimeout);

                    switch (res) {
                        case RES_OK:
                            sockRdr.allowRead();

                            synchronized (mux) {
                                long timeout = netTimeout;
                                long threshold = U.currentTimeMillis() + timeout;

                                while (spiState == CONNECTING && timeout > 0) {
                                    try {
                                        mux.wait(timeout);

                                        timeout = threshold - U.currentTimeMillis();
                                    }
                                    catch (InterruptedException ignored) {
                                        Thread.currentThread().interrupt();

                                        throw new GridSpiException("Thread has been interrupted.");
                                    }
                                }

                                switch (spiState) {
                                    case CONNECTING:
                                        throw new GridSpiException("Join process timed out [sock=" + sock +
                                            ", timeout=" + netTimeout + ']');

                                    case CONNECTED:
                                        if (log.isDebugEnabled())
                                            log.debug("Successfully connected to topology [sock=" + sock + ']');

                                        return;

                                    // TODO: GG-9174 - Handle other states.
                                    default:
                                        throw new GridSpiException("Failed to join topology [spiState=" +
                                            spiState + ']');
                                }
                            }

                        case RES_CONTINUE_JOIN:
                        case RES_WAIT:
                            closeConnection();

                            retry = true;

                            break;

                        default:
                            if (log.isDebugEnabled())
                                log.debug("Unexpected response to join request: " + res);
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
     * @param sock Socket.
     * @throws IOException In case of I/O error.
     * @throws GridException In case of other error.
     */
    private void initConnection(Socket sock) throws IOException, GridException {
        assert sock != null;

        writeToSocket(sock, new GridTcpDiscoveryHandshakeRequest(locNodeId, true));

        GridTcpDiscoveryHandshakeResponse res = readMessage(sock, null, ackTimeout);

        UUID nodeId = res.creatorNodeId();

        assert !locNodeId.equals(nodeId);

        msgWrk = new MessageWorker(sock);
        msgWrk.start();

        sockRdr = new SocketReader(sock, nodeId);
        sockRdr.start();
    }

    /**
     * Closes connection.
     */
    private void closeConnection() {
        U.interrupt(sockRdr);
        U.join(sockRdr, log);

        U.interrupt(msgWrk);
        U.join(msgWrk, log);
    }

    /**
     * @return Spi state.
     */
    private GridTcpDiscoverySpiState spiState() {
        GridTcpDiscoverySpiState state;

        synchronized (mux) {
            state = spiState;
        }

        return state;
    }

    /**
     * Socket reader.
     */
    private class SocketReader extends GridSpiThread {
        /** Initialization latch. */
        private final CountDownLatch readLatch = new CountDownLatch(1);

        /** Socket. */
        private final Socket sock;

        /** Remote node ID. */
        private final UUID nodeId;

        /**
         * @param sock Socket.
         */
        protected SocketReader(Socket sock, UUID nodeId) {
            super(gridName, "tcp-client-disco-sock-reader", log);

            assert sock != null;
            assert nodeId != null;

            this.sock = sock;
            this.nodeId = nodeId;
        }

        /**
         */
        void allowRead() {
            readLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                U.await(readLatch);

                assert msgWrk != null;

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
                U.error(log, "Failed to initialize connection [sock=" + sock + ", locNodeId=" + locNodeId +
                    ", rmtNodeId=" + nodeId + ']', e);
            }
            catch (GridInterruptedException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Socket reader was interrupted.", e);
            }
            finally {
                U.closeQuiet(sock);
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
            stats.onMessageProcessingStarted(msg);

            if (msg instanceof GridTcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((GridTcpDiscoveryNodeAddedMessage)msg);
            else if (msg instanceof GridTcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((GridTcpDiscoveryNodeAddFinishedMessage)msg);

            stats.onMessageProcessingFinished(msg);
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddedMessage(GridTcpDiscoveryNodeAddedMessage msg) {
            GridTcpDiscoveryNode node = msg.node();

            if (msg.verified() && locNodeId.equals(node.id())) {
                Collection<List<Object>> dataList = null;

                GridTcpDiscoverySpiState state = spiState();

                if (state == CONNECTING) {
                    Collection<GridTcpDiscoveryNode> top = msg.topology();

                    if (!F.isEmpty(top)) {
                        Map<UUID, GridNode> top0 = new HashMap<>(top.size(), 1.0f);

                        for (GridTcpDiscoveryNode n : top) {
                            n.visible(true);

                            top0.put(n.id(), n);
                        }

                        rmtNodes = Collections.unmodifiableMap(top0);

                        locNode.setAttributes(node.attributes());
                        locNode.visible(true);

                        dataList = msg.oldNodesDiscoveryData();

                        msg.messages(null);
                        msg.topology(null);
                        msg.topologyHistory(null);
                        msg.clearDiscoveryData();
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Discarding node added message with empty topology: " + msg);
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node added message (this message has already been processed) " +
                        "[spiState=" + state + ", msg=" + msg + ", locNode=" + locNode + ']');

                if (dataList != null) {
                    for (List<Object> discoData : dataList)
                        exchange.onExchange(discoData);
                }
            }
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddFinishedMessage(GridTcpDiscoveryNodeAddFinishedMessage msg) {
            if (msg.verified() && locNodeId.equals(msg.nodeId())) {
                GridTcpDiscoverySpiState state = spiState();

                if (state == CONNECTING) {
                    long topVer = msg.topologyVersion();

                    locNode.order(topVer);

                    synchronized (mux) {
                        spiState = CONNECTED;

                        mux.notifyAll();
                    }

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode);
                }
                else if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message (this message has already been processed) " +
                        "[spiState=" + state + ", msg=" + msg + ", locNode=" + locNode + ']');
            }
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         */
        private void notifyDiscovery(int type, long topVer, GridNode node) {
            GridDiscoverySpiListener lsnr = GridTcpClientDiscoverySpi.this.lsnr;

            if (lsnr != null)
                lsnr.onDiscovery(type, topVer, node, F.concat(false, locNode, rmtNodes.values()), null);
            else if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", spiState=" + spiState() +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');
        }
    }
}
