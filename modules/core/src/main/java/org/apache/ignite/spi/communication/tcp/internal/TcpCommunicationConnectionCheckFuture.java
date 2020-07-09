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

package org.apache.ignite.spi.communication.tcp.internal;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tcp Communication Connection Check Future.
 */
public class TcpCommunicationConnectionCheckFuture extends GridFutureAdapter<BitSet> implements IgniteSpiTimeoutObject, GridLocalEventListener {
    /** Session future. */
    public static final int SES_FUT_META = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private static final AtomicIntegerFieldUpdater<SingleAddressConnectFuture> connFutDoneUpdater =
        AtomicIntegerFieldUpdater.newUpdater(SingleAddressConnectFuture.class, "done");

    /** */
    private static final AtomicIntegerFieldUpdater<MultipleAddressesConnectFuture> connResCntUpdater =
        AtomicIntegerFieldUpdater.newUpdater(MultipleAddressesConnectFuture.class, "resCnt");

    /** */
    private final AtomicInteger resCntr = new AtomicInteger();

    /** */
    private final List<ClusterNode> nodes;

    /** */
    private volatile ConnectFuture[] futs;

    /** */
    private final GridNioServer nioSrvr;

    /** */
    private final TcpCommunicationSpi spi;

    /** */
    private final IgniteUuid timeoutObjId = IgniteUuid.randomUuid();

    /** */
    private final BitSet resBitSet;

    /** */
    private long endTime;

    /** */
    private final IgniteLogger log;

    /**
     * @param spi SPI instance.
     * @param log Logger.
     * @param nioSrvr NIO server.
     * @param nodes Nodes to check.
     */
    public TcpCommunicationConnectionCheckFuture(TcpCommunicationSpi spi,
        IgniteLogger log,
        GridNioServer nioSrvr,
        List<ClusterNode> nodes)
    {
        this.spi = spi;
        this.log = log;
        this.nioSrvr = nioSrvr;
        this.nodes = nodes;

        resBitSet = new BitSet(nodes.size());
    }

    /**
     * @param timeout Connect timeout.
     */
    public void init(long timeout) {
        ConnectFuture[] futs = new ConnectFuture[nodes.size()];

        UUID locId = spi.getSpiContext().localNode().id();

        for (int i = 0; i < nodes.size(); i++) {
            ClusterNode node = nodes.get(i);

            if (!node.id().equals(locId)) {
                if (spi.getSpiContext().node(node.id()) == null) {
                    receivedConnectionStatus(i, false);

                    continue;
                }

                Collection<InetSocketAddress> addrs;

                try {
                    addrs = spi.nodeAddresses(node, false);
                }
                catch (Exception e) {
                    U.error(log, "Failed to get node addresses: " + node, e);

                    receivedConnectionStatus(i, false);

                    continue;
                }

                if (addrs.size() == 1) {
                    SingleAddressConnectFuture fut = new SingleAddressConnectFuture(i);

                    fut.init(addrs.iterator().next(), node.consistentId(), node.id());

                    futs[i] = fut;
                }
                else {
                    MultipleAddressesConnectFuture fut = new MultipleAddressesConnectFuture(i);

                    fut.init(addrs, node.consistentId(), node.id());

                    futs[i] = fut;
                }
            }
            else
                receivedConnectionStatus(i, true);
        }

        this.futs = futs;

        spi.getSpiContext().addLocalEventListener(this, EVT_NODE_LEFT, EVT_NODE_FAILED);

        if (!isDone()) {
            endTime = System.currentTimeMillis() + timeout;

            spi.getSpiContext().addTimeoutObject(this);
        }
    }

    /**
     * @param idx Node index.
     * @param res Success flag.
     */
    private void receivedConnectionStatus(int idx, boolean res) {
        assert resCntr.get() < nodes.size();

        synchronized (resBitSet) {
            resBitSet.set(idx, res);
        }

        if (resCntr.incrementAndGet() == nodes.size())
            onDone(resBitSet);
    }

    /**
     * @param nodeIdx Node index.
     * @return Node ID.
     */
    private UUID nodeId(int nodeIdx) {
        return nodes.get(nodeIdx).id();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return timeoutObjId;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public void onEvent(Event evt) {
        if (isDone())
            return;

        assert evt instanceof DiscoveryEvent : evt;
        assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

        UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).id().equals(nodeId)) {
                ConnectFuture fut = futs[i];

                if (fut != null)
                    fut.onNodeFailed();

                return;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (isDone())
            return;

        ConnectFuture[] futs = this.futs;

        for (int i = 0; i < futs.length; i++) {
            ConnectFuture fut = futs[i];

            if (fut != null)
                fut.onTimeout();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable BitSet res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            spi.getSpiContext().removeTimeoutObject(this);

            spi.getSpiContext().removeLocalEventListener(this);

            return true;
        }

        return false;
    }

    /**
     *
     */
    private interface ConnectFuture {
        /**
         *
         */
        void onTimeout();

        /**
         *
         */
        void onNodeFailed();
    }

    /**
     *
     */
    private class SingleAddressConnectFuture implements TcpCommunicationNodeConnectionCheckFuture, ConnectFuture {
        /** */
        final int nodeIdx;

        /** */
        volatile int done;

        /** */
        Map<Integer, Object> sesMeta;

        /** */
        private SocketChannel ch;

        /**
         * @param nodeIdx Node index.
         */
        SingleAddressConnectFuture(int nodeIdx) {
            this.nodeIdx = nodeIdx;
        }

        /**
         * @param addr Node address.
         * @param consistentId Consistent if of the node.
         * @param rmtNodeId Id of node to open connection check session with.
         */
        public void init(InetSocketAddress addr, Object consistentId, UUID rmtNodeId) {
            boolean connect;

            try {
                ch = SocketChannel.open();

                ch.configureBlocking(false);

                ch.socket().setTcpNoDelay(true);
                ch.socket().setKeepAlive(false);

                connect = ch.connect(addr);
            }
            catch (Exception e) {
                finish(false);

                return;
            }

            if (!connect) {
                sesMeta = new GridLeanMap<>(3);

                // Set dummy key to identify connection-check outgoing connection.
                ConnectionKey connKey = new ConnectionKey(rmtNodeId, -1, -1, true);

                sesMeta.put(TcpCommunicationSpi.CONN_IDX_META, connKey);
                sesMeta.put(TcpCommunicationSpi.CONSISTENT_ID_META, consistentId);
                sesMeta.put(SES_FUT_META, this);

                nioSrvr.createSession(ch, sesMeta, true, new IgniteInClosure<IgniteInternalFuture<GridNioSession>>() {
                    @Override public void apply(IgniteInternalFuture<GridNioSession> fut) {
                        if (fut.error() != null)
                            finish(false);
                    }
                });
            }
        }

        /**
         *
         */
        @SuppressWarnings("unchecked")
        void cancel() {
            if (finish(false))
                nioSrvr.cancelConnect(ch, sesMeta);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            cancel();
        }

        /** {@inheritDoc} */
        @Override public void onConnected(UUID rmtNodeId) {
            finish(nodeId(nodeIdx).equals(rmtNodeId));
        }

        /** {@inheritDoc} */
        @Override public void onNodeFailed() {
            cancel();
        }

        /**
         * @param res Result.
         * @return {@code True} if result was set by this call.
         */
        public boolean finish(boolean res) {
            if (connFutDoneUpdater.compareAndSet(this, 0, 1)) {
                onStatusReceived(res);

                return true;
            }

            return false;
        }

        /**
         * @param res Result.
         */
        void onStatusReceived(boolean res) {
            receivedConnectionStatus(nodeIdx, res);
        }
    }

    /**
     *
     */
    private class MultipleAddressesConnectFuture implements ConnectFuture {
        /** */
        volatile int resCnt;

        /** */
        volatile SingleAddressConnectFuture[] futs;

        /** */
        final int nodeIdx;

        /**
         * @param nodeIdx Node index.
         */
        MultipleAddressesConnectFuture(int nodeIdx) {
            this.nodeIdx = nodeIdx;

        }

        /** {@inheritDoc} */
        @Override public void onNodeFailed() {
            SingleAddressConnectFuture[] futs = this.futs;

            for (int i = 0; i < futs.length; i++) {
                ConnectFuture fut = futs[i];

                if (fut != null)
                    fut.onNodeFailed();
            }
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            SingleAddressConnectFuture[] futs = this.futs;

            for (int i = 0; i < futs.length; i++) {
                ConnectFuture fut = futs[i];

                if (fut != null)
                    fut.onTimeout();
            }
        }

        /**
         * @param addrs Node addresses.
         * @param consistentId Consistent if of the node.
         * @param rmtNodeId Id of node to open connection check session with.
         */
        void init(Collection<InetSocketAddress> addrs, Object consistentId, UUID rmtNodeId) {
            SingleAddressConnectFuture[] futs = new SingleAddressConnectFuture[addrs.size()];

            for (int i = 0; i < addrs.size(); i++) {
                SingleAddressConnectFuture fut = new SingleAddressConnectFuture(nodeIdx) {
                    @Override void onStatusReceived(boolean res) {
                        receivedAddressStatus(res);
                    }
                };

                futs[i] = fut;
            }

            this.futs = futs;

            int idx = 0;

            for (InetSocketAddress addr : addrs) {
                futs[idx++].init(addr, consistentId, rmtNodeId);

                if (resCnt == Integer.MAX_VALUE)
                    return;
            }

            // Close race.
            if (done())
                cancelFutures();
        }

        /**
         * @return {@code True}
         */
        private boolean done() {
            int resCnt0 = resCnt;

            return resCnt0 == Integer.MAX_VALUE || resCnt0 == futs.length;
        }

        /**
         *
         */
        private void cancelFutures() {
            SingleAddressConnectFuture[] futs = this.futs;

            if (futs != null) {
                for (int i = 0; i < futs.length; i++) {
                    SingleAddressConnectFuture fut = futs[i];

                    fut.cancel();
                }
            }
        }

        /**
         * @param res Result.
         */
        void receivedAddressStatus(boolean res) {
            if (res) {
                for (;;) {
                    int resCnt0 = resCnt;

                    if (resCnt0 == Integer.MAX_VALUE)
                        return;

                    if (connResCntUpdater.compareAndSet(this, resCnt0, Integer.MAX_VALUE)) {
                        receivedConnectionStatus(nodeIdx, true);

                        cancelFutures(); // Cancel others connects if they are still in progress.

                        return;
                    }
                }
            }
            else {
                for (;;) {
                    int resCnt0 = resCnt;

                    if (resCnt0 == Integer.MAX_VALUE)
                        return;

                    int resCnt1 = resCnt0 + 1;

                    if (connResCntUpdater.compareAndSet(this, resCnt0, resCnt1)) {
                        if (resCnt1 == futs.length)
                            receivedConnectionStatus(nodeIdx, false);

                        return;
                    }
                }
            }
        }
    }
}
