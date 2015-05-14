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
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
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
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.*;
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

    /** */
    private static final Object JOIN_TIMEOUT = "JOIN_TIMEOUT";

    /** */
    private static final Object SPI_STOP = "SPI_STOP";

    /** */
    private static final Object SPI_RECONNECT_FAILED = "SPI_RECONNECT_FAILED";

    /** Remote nodes. */
    private final ConcurrentMap<UUID, TcpDiscoveryNode> rmtNodes = new ConcurrentHashMap8<>();

    /** Remote nodes. */
    private final ConcurrentMap<UUID, GridFutureAdapter<Boolean>> pingFuts = new ConcurrentHashMap8<>();

    /** Socket writer. */
    private SocketWriter sockWriter;

    /** */
    private SocketReader sockReader;

    /** */
    private boolean segmented;

    /** Last message ID. */
    private volatile IgniteUuid lastMsgId;

    /** Current topology version. */
    private volatile long topVer;

    /** Join error. Contains error what occurs on join process. */
    private IgniteSpiException joinErr;

    /** Joined latch. */
    private final CountDownLatch joinLatch = new CountDownLatch(1);

    /** Left latch. */
    private final CountDownLatch leaveLatch = new CountDownLatch(1);

    /** Disconnect check interval. */
    private long disconnectCheckInt = DFLT_DISCONNECT_CHECK_INT;

    /** */
    private final Timer timer = new Timer("TcpClientDiscoverySpi.timer");

    /** */
    private MessageWorker msgWorker;

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
        return msgWorker.queueSize();
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
        assertParameter(joinTimeout >= 0, "joinTimeout >= 0");

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

        sockWriter = new SocketWriter();
        sockWriter.start();

        sockReader = new SocketReader();
        sockReader.start();

        sockTimeoutWorker = new SocketTimeoutWorker();
        sockTimeoutWorker.start();

        msgWorker = new MessageWorker();
        msgWorker.start();

        try {
            joinLatch.await();

            if (joinErr != null)
                throw joinErr;
        }
        catch (InterruptedException e) {
            throw new IgniteSpiException("Thread has been interrupted.", e);
        }

        timer.schedule(new HeartbeatSender(), hbFreq, hbFreq);

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        timer.cancel();

        if (msgWorker != null && msgWorker.isAlive()) { // Should always be alive
            msgWorker.addMessage(SPI_STOP);

            try {
                if (!leaveLatch.await(netTimeout, MILLISECONDS))
                    U.warn(log, "Failed to left node: timeout [nodeId=" + locNode + ']');
            }
            catch (InterruptedException ignored) {

            }
        }

        for (GridFutureAdapter<Boolean> fut : pingFuts.values())
            fut.onDone(false);

        rmtNodes.clear();

        U.interrupt(sockTimeoutWorker);
        U.interrupt(msgWorker);
        U.interrupt(sockWriter);
        U.interrupt(sockReader);

        U.join(msgWorker, log);
        U.join(sockTimeoutWorker, log);
        U.join(sockWriter, log);
        U.join(sockReader, log);

        unregisterMBean();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        return F.<Object>asList(ipFinder);
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
                if (getSpiContext().isStopping()) {
                    if (pingFuts.remove(nodeId, fut))
                        fut.onDone(false);

                    return false;
                }

                final GridFutureAdapter<Boolean> finalFut = fut;

                timer.schedule(new TimerTask() {
                    @Override public void run() {
                        if (pingFuts.remove(nodeId, finalFut))
                            finalFut.onDone(false);
                    }
                }, netTimeout);

                sockWriter.sendMessage(new TcpDiscoveryClientPingRequest(getLocalNodeId(), nodeId));
            }
        }

        try {
            return fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException(e); // Should newer occur
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
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(Serializable evt) {
        try {
            sockWriter.sendMessage(new TcpDiscoveryCustomEventMessage(getLocalNodeId(), marsh.marshal(evt)));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal custom event: " + evt, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId) {
        ClusterNode node = rmtNodes.get(nodeId);

        if (node != null) {
            TcpDiscoveryNodeFailedMessage msg = new TcpDiscoveryNodeFailedMessage(getLocalNodeId(),
                node.id(), node.order());

            msgWorker.addMessage(msg);
        }
    }

    /**
     * @return Opened socket or {@code null} if timeout.
     * @see #joinTimeout
     */
    @SuppressWarnings("BusyWait")
    @Nullable private Socket joinTopology(boolean recon) throws IgniteSpiException, InterruptedException {
        Collection<InetSocketAddress> addrs = null;

        long startTime = U.currentTimeMillis();

        while (true) {
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();

            while (addrs == null || addrs.isEmpty()) {
                addrs = resolvedAddresses();

                if (!F.isEmpty(addrs)) {
                    if (log.isDebugEnabled())
                        log.debug("Resolved addresses from IP finder: " + addrs);
                }
                else {
                    U.warn(log, "No addresses registered in the IP finder (will retry in 2000ms): " + ipFinder);

                    if ((U.currentTimeMillis() - startTime) > joinTimeout)
                        return null;

                    Thread.sleep(2000);
                }
            }

            Collection<InetSocketAddress> addrs0 = new ArrayList<>(addrs);

            Iterator<InetSocketAddress> it = addrs.iterator();

            while (it.hasNext()) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

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
                            return sock;

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

                if ((U.currentTimeMillis() - startTime) > joinTimeout)
                    return null;

                Thread.sleep(2000);
            }
        }
    }

    /**
     * @param addr Address.
     * @return Remote node ID.
     * @throws IOException In case of I/O error.
     * @throws IgniteCheckedException In case of other error.
     */
    private IgniteBiTuple<Socket, UUID> initConnection(InetSocketAddress addr) throws IOException, IgniteCheckedException {
        assert addr != null;

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

        U.interrupt(sockWriter);
        U.interrupt(msgWorker);
        U.interrupt(sockTimeoutWorker);

        U.join(sockWriter, log);
        U.join(msgWorker, log);
        U.join(sockTimeoutWorker, log);

        timer.cancel();
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    public void brokeConnection() {
        U.closeQuiet(msgWorker.currSock);
    }

    /**
     * Heartbeat sender.
     */
    private class HeartbeatSender extends TimerTask {
        /** {@inheritDoc} */
        @Override public void run() {
            if (!getSpiContext().isStopping() && sockWriter.isOnline()) {
                TcpDiscoveryHeartbeatMessage msg = new TcpDiscoveryHeartbeatMessage(getLocalNodeId());

                UUID nodeId = ignite.configuration().getNodeId();

                msg.setMetrics(nodeId, metricsProvider.metrics());

                msg.setCacheMetrics(nodeId, metricsProvider.cacheMetrics());

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
        private Socket sock;

        /** */
        private UUID rmtNodeId;

        /**
         */
        protected SocketReader() {
            super(gridName, "tcp-client-disco-sock-reader", log);
        }

        /**
         * @param sock Socket.
         * @param rmtNodeId Rmt node id.
         */
        public void setSocket(Socket sock, UUID rmtNodeId) {
            synchronized (mux) {
                this.sock = sock;

                this.rmtNodeId = rmtNodeId;

                mux.notifyAll();
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                Socket sock;
                UUID rmtNodeId;

                synchronized (mux) {
                    if (this.sock == null) {
                        mux.wait();

                        continue;
                    }

                    sock = this.sock;
                    rmtNodeId = this.rmtNodeId;
                }

                try {
                    try {
                        InputStream in = new BufferedInputStream(sock.getInputStream());

                        sock.setKeepAlive(true);
                        sock.setTcpNoDelay(true);

                        while (!isInterrupted()) {
                            TcpDiscoveryAbstractMessage msg;

                            try {
                                msg = marsh.unmarshal(in, U.gridClassLoader());
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
                                    LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
                                        "(make sure same versions of all classes are available on all nodes) " +
                                        "[rmtNodeId=" + rmtNodeId + ", err=" + clsNotFoundEx.getMessage() + ']');
                                else
                                    LT.error(log, e, "Failed to read message [sock=" + sock + ", locNodeId=" +
                                        getLocalNodeId() + ", rmtNodeId=" + rmtNodeId + ']');

                                continue;
                            }

                            msg.senderNodeId(rmtNodeId);

                            if (log.isDebugEnabled())
                                log.debug("Message has been received: " + msg);

                            stats.onMessageReceived(msg);

                            if (ensured(msg))
                                lastMsgId = msg.id();

                            msgWorker.addMessage(msg);
                        }
                    }
                    catch (IOException e) {
                        msgWorker.addMessage(new SocketClosedMessage(sock));

                        if (log.isDebugEnabled())
                            U.error(log, "Connection failed [sock=" + sock + ", locNodeId=" + getLocalNodeId() + ']', e);
                    }
                }
                finally {
                    U.closeQuiet(sock);

                    synchronized (mux) {
                        if (this.sock == sock) {
                            this.sock = null;
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
        private final Queue<TcpDiscoveryAbstractMessage> queue = new ArrayDeque<>();

        /**
         *
         */
        protected SocketWriter() {
            super(gridName, "tcp-client-disco-sock-writer", log);
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
         */
        private void setSocket(Socket sock) {
            synchronized (mux) {
                this.sock = sock;

                mux.notifyAll();
            }
        }

        /**
         *
         */
        public boolean isOnline() {
            synchronized (mux) {
                return sock != null;
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            TcpDiscoveryAbstractMessage msg = null;

            while (!Thread.currentThread().isInterrupted()) {
                Socket sock;

                synchronized (mux) {
                    sock = this.sock;

                    if (sock == null) {
                        mux.wait();

                        continue;
                    }

                    if (msg == null)
                        msg = queue.poll();

                    if (msg == null) {
                        mux.wait();

                        continue;
                    }
                }

                try {
                    writeToSocket(sock, msg);

                    msg = null;
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Failed to send node left message (will stop anyway) [sock=" + sock + ']', e);

                    U.closeQuiet(sock);

                    synchronized (mux) {
                        if (sock == this.sock)
                            this.sock = null; // Connection has dead.
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send message: " + msg, e);

                    msg = null;
                }
            }
        }
    }

    /**
     *
     */
    private class Reconnector extends IgniteSpiThread {
        /** */
        private volatile Socket sock;

        /**
         *
         */
        protected Reconnector() {
            super(gridName, "tcp-client-disco-msg-worker", log);
        }

        /**
         *
         */
        public void cancel() {
            interrupt();

            U.closeQuiet(sock);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            assert !segmented;

            boolean success = false;

            try {
                sock = joinTopology(true);

                if (sock == null) {
                    U.error(log, "Failed to reconnect to cluster: timeout.");

                    return;
                }

                if (isInterrupted())
                    throw new InterruptedException();

                InputStream in = new BufferedInputStream(sock.getInputStream());

                sock.setKeepAlive(true);
                sock.setTcpNoDelay(true);

                // Wait for
                while (!isInterrupted()) {
                    TcpDiscoveryAbstractMessage msg = marsh.unmarshal(in, U.gridClassLoader());

                    if (msg instanceof TcpDiscoveryClientReconnectMessage) {
                        TcpDiscoveryClientReconnectMessage res = (TcpDiscoveryClientReconnectMessage)msg;

                        if (res.creatorNodeId().equals(getLocalNodeId())) {
                            if (res.success()) {
                                msgWorker.addMessage(res);

                                success = true;
                            }

                            break;
                        }
                    }

                }
            }
            catch (IOException | IgniteCheckedException e) {
                U.error(log, "Failed to reconnect", e);
            }
            finally {
                if (!success) {
                    U.closeQuiet(sock);

                    msgWorker.addMessage(SPI_RECONNECT_FAILED);
                }
            }
        }
    }

    /**
     * Message worker.
     */
    private class MessageWorker extends IgniteSpiThread {
        /** Topology history. */
        private final NavigableMap<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

        /** Message queue. */
        private final BlockingDeque<Object> queue = new LinkedBlockingDeque<>();

        /** */
        private Socket currSock;

        /** Indicates that pending messages are currently processed. */
        private boolean pending;

        /** */
        private Reconnector reconnector;

        /**
         *
         */
        private MessageWorker() {
            super(gridName, "tcp-client-disco-msg-worker", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("InfiniteLoopStatement")
        @Override protected void body() throws InterruptedException {
            stats.onJoinStarted();

            try {
                final Socket sock = joinTopology(false);

                if (sock == null) {
                    joinErr = new IgniteSpiException("Join process timed out");

                    joinLatch.countDown();

                    return;
                }

                currSock = sock;

                sockWriter.setSocket(sock);

                timer.schedule(new TimerTask() {
                    @Override public void run() {
                        if (joinLatch.getCount() > 0)
                           queue.add(JOIN_TIMEOUT);
                    }
                }, netTimeout);

                sockReader.setSocket(sock, locNode.clientRouterNodeId());

                while (true) {
                    Object msg = queue.take();

                    if (msg == JOIN_TIMEOUT) {
                        if (joinLatch.getCount() > 0) {
                            joinErr = new IgniteSpiException("Join process timed out [sock=" + sock +
                                ", timeout=" + netTimeout + ']');

                            joinLatch.countDown();
                        }
                    }
                    else if (msg == SPI_STOP) {
                        assert getSpiContext().isStopping();

                        if (currSock != null) {
                            TcpDiscoveryAbstractMessage leftMsg = new TcpDiscoveryNodeLeftMessage(getLocalNodeId());

                            leftMsg.client(true);

                            sockWriter.sendMessage(leftMsg);
                        }
                        else
                            leaveLatch.countDown();
                    }
                    else if (msg instanceof SocketClosedMessage) {
                        if (((SocketClosedMessage)msg).sock == currSock) {
                            currSock = null;

                            if (joinLatch.getCount() > 0) {
                                joinErr = new IgniteSpiException("Failed to connect to cluster: socket closed.");

                                joinLatch.countDown();
                            }
                            else {
                                if (getSpiContext().isStopping() || segmented)
                                    leaveLatch.countDown();
                                else {
                                    assert reconnector == null;

                                    final Reconnector reconnector = new Reconnector();
                                    this.reconnector = reconnector;
                                    reconnector.start();

                                    timer.schedule(new TimerTask() {
                                        @Override public void run() {
                                            reconnector.cancel();
                                        }
                                    }, netTimeout);
                                }
                            }
                        }
                    }
                    else if (msg == SPI_RECONNECT_FAILED) {
                        if (!segmented) {
                            segmented = true;

                            reconnector.cancel();
                            reconnector.join();

                            notifyDiscovery(EVT_NODE_SEGMENTED, topVer, locNode, allNodes());
                        }
                    }
                    else {
                        TcpDiscoveryAbstractMessage discoMsg = (TcpDiscoveryAbstractMessage)msg;

                        if (joinLatch.getCount() > 0) {
                            IgniteSpiException err = null;

                            if (discoMsg instanceof TcpDiscoveryDuplicateIdMessage)
                                err = duplicateIdError((TcpDiscoveryDuplicateIdMessage)msg);
                            else if (discoMsg instanceof TcpDiscoveryAuthFailedMessage)
                                err = authenticationFailedError((TcpDiscoveryAuthFailedMessage)msg);
                            else if (discoMsg instanceof TcpDiscoveryCheckFailedMessage)
                                err = checkFailedError((TcpDiscoveryCheckFailedMessage)msg);

                            if (err != null) {
                                joinErr = err;

                                joinLatch.countDown();

                                break;
                            }
                        }

                        processDiscoveryMessage((TcpDiscoveryAbstractMessage)msg);
                    }
                }
            }
            finally {
                U.closeQuiet(currSock);

                if (joinLatch.getCount() > 0) {
                    // This should not occurs.
                    joinErr = new IgniteSpiException("Some error occurs in joinig process");

                    joinLatch.countDown();
                }

                if (reconnector != null) {
                    reconnector.cancel();

                    reconnector.join();
                }
            }
        }

        /** {@inheritDoc} */
        protected void processDiscoveryMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;
            assert msg.verified() || msg.senderNodeId() == null;

            stats.onMessageProcessingStarted(msg);

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
                processPingRequest((TcpDiscoveryPingRequest)msg);

            stats.onMessageProcessingFinished(msg);
        }

        /**
         * @param msg Message.
         */
        private void processNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
            if (getSpiContext().isStopping())
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
            if (getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.nodeId())) {
                if (joinLatch.getCount() > 0) {
                    long topVer = msg.topologyVersion();

                    locNode.order(topVer);

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode, updateTopologyHistory(topVer));

                    joinErr = null;

                    joinLatch.countDown();

                    stats.onJoinFinished();
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

                leaveLatch.countDown();
            }
            else {
                if (getSpiContext().isStopping())
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
            if (getSpiContext().isStopping()) {
                if (!getLocalNodeId().equals(msg.creatorNodeId()) && getLocalNodeId().equals(msg.failedNodeId())) {
                    if (leaveLatch.getCount() > 0) {
                        log.debug("Remote node fail this node while node is stopping [locNode=" + getLocalNodeId()
                            + ", rmtNode=" + msg.creatorNodeId() + ']');

                        leaveLatch.countDown();
                    }
                }

                return;
            }

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
            if (getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                assert msg.senderNodeId() != null;

                if (log.isDebugEnabled())
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
            if (getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                assert msg.success();

                currSock = reconnector.sock;

                sockWriter.setSocket(currSock);
                sockReader.setSocket(currSock, locNode.clientRouterNodeId());

                reconnector = null;

                pending = true;

                try {
                    for (TcpDiscoveryAbstractMessage pendingMsg : msg.pendingMessages())
                        processDiscoveryMessage(pendingMsg);
                }
                finally {
                    pending = false;
                }
            }
            else if (log.isDebugEnabled())
                log.debug("Discarding reconnect message for another client: " + msg);
        }

        /**
         * @param msg Message.
         */
        private void processCustomMessage(TcpDiscoveryCustomEventMessage msg) {
            if (msg.verified()) {
                DiscoverySpiListener lsnr = TcpClientDiscoverySpi.this.lsnr;

                if (lsnr != null) {
                    UUID nodeId = msg.creatorNodeId();

                    TcpDiscoveryNode node = nodeId.equals(getLocalNodeId()) ? locNode : rmtNodes.get(nodeId);

                    if (node != null && node.visible()) {
                        try {
                            Serializable msgObj = marsh.unmarshal(msg.messageBytes(), U.gridClassLoader());

                            notifyDiscovery(EVT_DISCOVERY_CUSTOM_EVT, topVer, node, allNodes(), msgObj);
                        }
                        catch (IgniteCheckedException e) {
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
         *
         * @param msg Message.
         */
        private void processPingRequest(TcpDiscoveryPingRequest msg) {
            sockWriter.sendMessage(new TcpDiscoveryPingResponse(getLocalNodeId()));
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
            notifyDiscovery(type, topVer, node, top, null);
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param top Topology snapshot.
         */
        private void notifyDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> top,
            @Nullable Serializable data) {
            DiscoverySpiListener lsnr = TcpClientDiscoverySpi.this.lsnr;

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
        public void addMessage(Object msg) {
            queue.add(msg);
        }

        /**
         *
         */
        public int queueSize() {
            return queue.size();
        }
    }

    /**
     *
     */
    private static class SocketClosedMessage {
        /** */
        private final Socket sock;

        /**
         * @param sock Socket.
         */
        private SocketClosedMessage(Socket sock) {
            this.sock = sock;
        }
    }
}
