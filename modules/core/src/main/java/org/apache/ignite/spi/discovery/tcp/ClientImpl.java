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
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
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

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.*;
import static org.apache.ignite.spi.discovery.tcp.ClientImpl.State.*;

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
        spi.initLocalNode(0, true);

        locNode = spi.locNode;

        // Marshal credentials for backward compatibility and security.
        marshalCredentials(locNode);

        sockWriter = new SocketWriter();
        sockWriter.start();

        sockReader = new SocketReader();
        sockReader.start();

        msgWorker = new MessageWorker();
        msgWorker.start();

        if (spi.ipFinder.isShared())
            registerLocalNodeAddress();

        try {
            joinLatch.await();

            IgniteSpiException err = joinErr.get();

            if (err != null)
                throw err;
        }
        catch (InterruptedException e) {
            throw new IgniteSpiException("Thread has been interrupted.", e);
        }

        timer.schedule(new HeartbeatSender(), spi.hbFreq, spi.hbFreq);

        spi.printStartInfo();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        timer.cancel();

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
                spi.marsh.marshal(evt)));
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
    @Nullable private T2<Socket, Boolean> joinTopology(boolean recon, long timeout) throws IgniteSpiException, InterruptedException {
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

                    U.warn(log, "No addresses registered in the IP finder (will retry in 2000ms): " + spi.ipFinder);

                    Thread.sleep(2000);
                }
            }

            Collection<InetSocketAddress> addrs0 = new ArrayList<>(addrs);

            Iterator<InetSocketAddress> it = addrs.iterator();

            while (it.hasNext()) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                InetSocketAddress addr = it.next();

                T3<Socket, Integer, Boolean> sockAndRes = sendJoinRequest(recon, addr);

                if (sockAndRes == null) {
                    it.remove();

                    continue;
                }

                assert sockAndRes.get1() != null && sockAndRes.get2() != null : sockAndRes;

                Socket sock = sockAndRes.get1();

                switch (sockAndRes.get2()) {
                    case RES_OK:
                        return new T2<>(sock, sockAndRes.get3());

                    case RES_CONTINUE_JOIN:
                    case RES_WAIT:
                        U.closeQuiet(sock);

                        break;

                    default:
                        if (log.isDebugEnabled())
                            log.debug("Received unexpected response to join request: " + sockAndRes.get2());

                        U.closeQuiet(sock);
                }
            }

            if (addrs.isEmpty()) {
                if (timeout > 0 && (U.currentTimeMillis() - startTime) > timeout)
                    return null;

                U.warn(log, "Failed to connect to any address from IP finder (will retry to join topology " +
                    "in 2000ms): " + addrs0);

                Thread.sleep(2000);
            }
        }
    }

    /**
     * @param recon {@code True} if reconnects.
     * @param addr Address.
     * @return Socket, connect response and client acknowledge support flag.
     */
    @Nullable private T3<Socket, Integer, Boolean> sendJoinRequest(boolean recon, InetSocketAddress addr) {
        assert addr != null;

        if (log.isDebugEnabled())
            log.debug("Send join request [addr=" + addr + ", reconnect=" + recon +
                ", locNodeId=" + getLocalNodeId() + ']');

        Collection<Throwable> errs = null;

        long ackTimeout0 = spi.ackTimeout;

        int connectAttempts = 1;

        UUID locNodeId = getLocalNodeId();

        for (int i = 0; i < spi.reconCnt; i++) {
            boolean openSock = false;

            Socket sock = null;

            try {
                long tstamp = U.currentTimeMillis();

                sock = spi.openSocket(addr);

                openSock = true;

                TcpDiscoveryHandshakeRequest req = new TcpDiscoveryHandshakeRequest(locNodeId);

                req.client(true);

                spi.writeToSocket(sock, req);

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

                spi.writeToSocket(sock, msg);

                spi.stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                if (log.isDebugEnabled())
                    log.debug("Message has been sent to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + rmtNodeId + ']');

                return new T3<>(sock, spi.readReceipt(sock, ackTimeout0), res.clientAck());
            }
            catch (IOException | IgniteCheckedException e) {
                U.closeQuiet(sock);

                if (log.isDebugEnabled())
                    log.error("Exception on joining: " + e.getMessage(), e);

                onException("Exception on joining: " + e.getMessage(), e);

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
                spi.marsh.marshal(attrs.get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS)));

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

            assert top != null : msg;

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
        U.join(msgWorker, log);
    }

    /** {@inheritDoc} */
    @Override public void brakeConnection() {
        U.closeQuiet(msgWorker.currSock);
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
        private Socket sock;

        /** */
        private UUID rmtNodeId;

        /**
         */
        protected SocketReader() {
            super(spi.ignite().name(), "tcp-client-disco-sock-reader", log);
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
                    InputStream in = new BufferedInputStream(sock.getInputStream());

                    sock.setKeepAlive(true);
                    sock.setTcpNoDelay(true);

                    while (!isInterrupted()) {
                        TcpDiscoveryAbstractMessage msg;

                        try {
                            msg = spi.marsh.unmarshal(in, U.gridClassLoader());
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

                        spi.stats.onMessageReceived(msg);

                        boolean ack = msg instanceof TcpDiscoveryClientAckResponse;

                        if (!ack) {
                            if (spi.ensured(msg) && joinLatch.getCount() == 0L)
                                lastMsgId = msg.id();

                            msgWorker.addMessage(msg);
                        }
                        else
                            sockWriter.ackReceived((TcpDiscoveryClientAckResponse)msg);
                    }
                }
                catch (IOException e) {
                    msgWorker.addMessage(new SocketClosedMessage(sock));

                    if (log.isDebugEnabled())
                        U.error(log, "Connection failed [sock=" + sock + ", locNodeId=" + getLocalNodeId() + ']', e);
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
        private boolean clientAck;

        /** */
        private final Queue<TcpDiscoveryAbstractMessage> queue = new ArrayDeque<>();

        /** */
        private TcpDiscoveryAbstractMessage unackedMsg;

        /**
         *
         */
        protected SocketWriter() {
            super(spi.ignite().name(), "tcp-client-disco-sock-writer", log);
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

                for (IgniteInClosure<TcpDiscoveryAbstractMessage> msgLsnr : spi.sendMsgLsnrs)
                    msgLsnr.apply(msg);

                boolean ack = clientAck && !(msg instanceof TcpDiscoveryPingResponse);

                try {
                    if (ack) {
                        synchronized (mux) {
                            assert unackedMsg == null : unackedMsg;

                            unackedMsg = msg;
                        }
                    }

                    spi.writeToSocket(sock, msg);

                    msg = null;

                    if (ack) {
                        long waitEnd = U.currentTimeMillis() + spi.ackTimeout;

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
                                "[msg=" + unacked + ", timeout=" + spi.ackTimeout + ']');

                            throw new IOException("Failed to get acknowledge for message: " + unacked);
                        }
                    }
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Failed to send node left message (will stop anyway) " +
                            "[sock=" + sock + ", msg=" + msg + ']', e);

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

            U.closeQuiet(sock);
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
                    T2<Socket, Boolean> joinRes = joinTopology(true, timeout);

                    if (joinRes == null) {
                        if (join) {
                            joinError(new IgniteSpiException("Join process timed out, connection failed and " +
                                "failed to reconnect (consider increasing 'joinTimeout' configuration property) " +
                                "[networkTimeout=" + spi.joinTimeout + ", sock=" + sock + ']'));
                        }
                        else
                            U.error(log, "Failed to reconnect to cluster (consider increasing 'networkTimeout' " +
                                "configuration property) [networkTimeout=" + spi.netTimeout + ", sock=" + sock + ']');

                        return;
                    }

                    sock = joinRes.get1();
                    clientAck = joinRes.get2();

                    if (isInterrupted())
                        throw new InterruptedException();

                    int oldTimeout = 0;

                    try {
                        oldTimeout = sock.getSoTimeout();

                        sock.setSoTimeout((int)spi.netTimeout);

                        InputStream in = new BufferedInputStream(sock.getInputStream());

                        sock.setKeepAlive(true);
                        sock.setTcpNoDelay(true);

                        List<TcpDiscoveryAbstractMessage> msgs = null;

                        while (!isInterrupted()) {
                            TcpDiscoveryAbstractMessage msg = spi.marsh.unmarshal(in, U.gridClassLoader());

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

                U.error(log, "Failed to reconnect", e);
            }
            finally {
                if (!success) {
                    U.closeQuiet(sock);

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
        private Socket currSock;

        /** Indicates that pending messages are currently processed. */
        private boolean pending;

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
                        state = STOPPED;

                        assert spi.getSpiContext().isStopping();

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

                            boolean join = joinLatch.getCount() > 0;

                            if (spi.getSpiContext().isStopping() || state == SEGMENTED) {
                                leaveLatch.countDown();

                                if (join) {
                                    joinError(new IgniteSpiException("Failed to connect to cluster: socket closed."));

                                    break;
                                }
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
                    else if (msg == SPI_RECONNECT_FAILED) {
                        reconnector.cancel();
                        reconnector.join();

                        reconnector = null;

                        if (spi.isClientReconnectDisabled()) {
                            if (state != SEGMENTED && state != STOPPED) {
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
                                        "[networkTimeout=" + spi.netTimeout + ", joinTimeout=" + spi.joinTimeout + ']');
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

                            if (log.isInfoEnabled()) {
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
                U.closeQuiet(currSock);

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

            T2<Socket, Boolean> joinRes = joinTopology(false, spi.joinTimeout);

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

            sockWriter.setSocket(joinRes.get1(), joinRes.get2());

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
                    else if (log.isDebugEnabled())
                        log.debug("Discarding node added message with empty topology: " + msg);
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
                            spi.onExchange(newNodeId, newNodeId, data, null);
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Ignore topology message, local node not added to topology: " + msg);
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
                            spi.onExchange(getLocalNodeId(), entry.getKey(), entry.getValue(), null);
                    }

                    locNode.setAttributes(msg.clientNodeAttributes());
                    locNode.visible(true);

                    long topVer = msg.topologyVersion();

                    locNode.order(topVer);

                    Collection<ClusterNode> nodes = updateTopologyHistory(topVer, msg);

                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode, nodes);

                    boolean disconnected = disconnected();

                    state = CONNECTED;

                    if (disconnected)
                        notifyDiscovery(EVT_CLIENT_NODE_RECONNECTED, topVer, locNode, nodes);
                    else
                        spi.stats.onJoinFinished();

                    joinErr.set(null);;

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

                    if (!pending && joinLatch.getCount() > 0) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node add finished message (join process is not finished): " + msg);

                        return;
                    }

                    if (evt) {
                        notifyDiscovery(EVT_NODE_JOINED, topVer, node, top);

                        spi.stats.onNodeJoined();
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Ignore topology message, local node not added to topology: " + msg);
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

                    if (!pending && joinLatch.getCount() > 0) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node left message (join process is not finished): " + msg);

                        return;
                    }

                    notifyDiscovery(EVT_NODE_LEFT, msg.topologyVersion(), node, top);

                    spi.stats.onNodeLeft();
                }
                else if (log.isDebugEnabled())
                    log.debug("Ignore topology message, local node not added to topology: " + msg);
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

            if (!getLocalNodeId().equals(msg.creatorNodeId())) {
                TcpDiscoveryNode node = rmtNodes.remove(msg.failedNodeId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message since node is not found [msg=" + msg + ']');

                    return;
                }

                Collection<ClusterNode> top = updateTopologyHistory(msg.topologyVersion(), msg);

                if (!pending && joinLatch.getCount() > 0) {
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
            if (spi.getSpiContext().isStopping())
                return;

            if (getLocalNodeId().equals(msg.creatorNodeId())) {
                if (reconnector != null) {
                    assert msg.success() : msg;

                    currSock = reconnector.sock;

                    sockWriter.setSocket(currSock, reconnector.clientAck);
                    sockReader.setSocket(currSock, locNode.clientRouterNodeId());

                    reconnector = null;

                    pending = true;

                    try {
                        for (TcpDiscoveryAbstractMessage pendingMsg : msg.pendingMessages()) {
                            if (log.isDebugEnabled())
                                log.debug("Process pending message on reconnect [msg=" + pendingMsg + ']');

                            processDiscoveryMessage(pendingMsg);
                        }
                    }
                    finally {
                        pending = false;
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
            if (msg.verified() && state == CONNECTED) {
                DiscoverySpiListener lsnr = spi.lsnr;

                if (lsnr != null) {
                    UUID nodeId = msg.creatorNodeId();

                    TcpDiscoveryNode node = nodeId.equals(getLocalNodeId()) ? locNode : rmtNodes.get(nodeId);

                    if (node != null && node.visible()) {
                        try {
                            DiscoverySpiCustomMessage msgObj = msg.message(spi.marsh);

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
            assert nodeId != null;
            assert metrics != null;
            assert cacheMetrics != null;

            TcpDiscoveryNode node = nodeId.equals(getLocalNodeId()) ? locNode : rmtNodes.get(nodeId);

            if (node != null && node.visible()) {
                node.setMetrics(metrics);
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
         * @return Queue size.
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
