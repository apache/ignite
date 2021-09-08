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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public abstract class IgniteClientReconnectAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final long RECONNECT_TIMEOUT = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestTcpDiscoverySpi disco = new TestTcpDiscoverySpi();

        disco.setIpFinder(sharedStaticIpFinder);
        disco.setJoinTimeout(2 * 60_000);
        disco.setSocketTimeout(1000);
        disco.setNetworkTimeout(2000);

        cfg.setDiscoverySpi(disco);

        BlockTcpCommunicationSpi commSpi = new BlockTcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);
        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @param latch Latch.
     * @throws Exception If failed.
     */
    protected void waitReconnectEvent(CountDownLatch latch) throws Exception {
        waitReconnectEvent(log, latch);
    }

    /**
     * @param log Logger.
     * @param latch Latch.
     * @throws Exception If failed.
     */
    protected static void waitReconnectEvent(IgniteLogger log, CountDownLatch latch) throws Exception {
        if (!latch.await(RECONNECT_TIMEOUT, MILLISECONDS)) {
            log.error("Failed to wait for reconnect event, will dump threads, latch count: " + latch.getCount());

            U.dumpThreads(log);

            fail("Failed to wait for disconnect/reconnect event.");
        }
    }

    /**
     * @return Number of server nodes started before tests.
     */
    protected abstract int serverCount();

    /**
     * @return Number of client nodes started before tests.
     */
    protected int clientCount() {
        return 0;
    }

    /**
     * @param ignite Node.
     * @return Discovery SPI.
     */
    protected static TestTcpDiscoverySpi spi(Ignite ignite) {
        return ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());
    }

    /**
     * @param ignite Node.
     * @return Discovery SPI.
     */
    protected static IgniteDiscoverySpi spi0(Ignite ignite) {
        return ((IgniteDiscoverySpi)ignite.configuration().getDiscoverySpi());
    }

    /**
     * @param ignite Node.
     * @return Communication SPI.
     */
    protected BlockTcpCommunicationSpi commSpi(Ignite ignite) {
        return ((BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        int srvs = serverCount();

        if (srvs > 0)
            startGrids(srvs);

        int clients = clientCount();

        if (clients > 0)
            startClientGridsMultiThreaded(srvs, clients);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @param client Client.
     * @return Server node client connected to.
     */
    protected Ignite clientRouter(Ignite client) {
        if (tcpDiscovery()) {
            TcpDiscoveryNode node = (TcpDiscoveryNode)client.cluster().localNode();

            assertTrue(node.isClient());
            assertNotNull(node.clientRouterNodeId());

            Ignite srv = G.ignite(node.clientRouterNodeId());

            assertNotNull(srv);

            return srv;
        }
        else {
            for (Ignite node : G.allGrids()) {
                if (!node.cluster().localNode().isClient())
                    return node;
            }

            fail();

            return null;
        }
    }

    /**
     * @param fut Future.
     * @throws Exception If failed.
     */
    protected void assertNotDone(IgniteInternalFuture<?> fut) throws Exception {
        assertNotNull(fut);

        if (fut.isDone())
            fail("Future completed with result: " + fut.get());
    }

    /**
     * Reconnect client node.
     *
     * @param client Client.
     * @param srv Server.
     * @param disconnectedC Closure which will be run when client node disconnected.
     * @throws Exception If failed.
     */
    protected void reconnectClientNode(Ignite client, Ignite srv, @Nullable Runnable disconnectedC)
        throws Exception {
        reconnectClientNodes(log, Collections.singletonList(client), srv, disconnectedC);
    }

    /**
     * Reconnect client node.
     *
     * @param log  Logger.
     * @param client Client.
     * @param srv Server.
     * @param disconnectedC Closure which will be run when client node disconnected.
     * @throws Exception If failed.
     */
    public static void reconnectClientNode(IgniteLogger log,
        Ignite client,
        Ignite srv,
        @Nullable Runnable disconnectedC)
        throws Exception {
        reconnectClientNodes(log, Collections.singletonList(client), srv, disconnectedC);
    }

    /**
     * Reconnect client node.
     *
     * @param log  Logger.
     * @param clients Clients.
     * @param srv Server.
     * @param disconnectedC Closure which will be run when client node disconnected.
     * @throws Exception If failed.
     */
    protected static void reconnectClientNodes(final IgniteLogger log,
        List<Ignite> clients, Ignite srv,
        @Nullable Runnable disconnectedC)
        throws Exception {
        final IgniteDiscoverySpi srvSpi = spi0(srv);

        final CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        final CountDownLatch reconnectLatch = new CountDownLatch(clients.size());

        log.info("Block reconnect.");

        List<DiscoverySpiTestListener> blockLsnrs = new ArrayList<>();

        for (Ignite client : clients) {
            DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

            lsnr.startBlockJoin();

            blockLsnrs.add(lsnr);

            spi0(client).setInternalListener(lsnr);
        }

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        };

        try {
            for (Ignite client : clients)
                client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

            for (Ignite client : clients)
                srvSpi.failNode(client.cluster().localNode().id(), null);

            waitReconnectEvent(log, disconnectLatch);

            if (disconnectedC != null)
                disconnectedC.run();

            log.info("Allow reconnect.");

            for (DiscoverySpiTestListener blockLsnr : blockLsnrs)
                blockLsnr.stopBlockJoin();

            waitReconnectEvent(log, reconnectLatch);

            for (Ignite client : clients)
                client.events().stopLocalListen(p);
        }
        finally {
            for (DiscoverySpiTestListener blockLsnr : blockLsnrs)
                blockLsnr.stopBlockJoin();
        }
    }

    /**
     * @param log Logger.
     * @param client Client node.
     * @param srvs Server nodes to stop.
     * @param srvStartC Closure starting server nodes.
     * @throws Exception If failed.
     * @return Restarted servers.
     */
    public static Collection<Ignite> reconnectServersRestart(final IgniteLogger log,
        Ignite client,
        Collection<Ignite> srvs,
        Callable<Collection<Ignite>> srvStartC)
        throws Exception {
        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        for (Ignite srv : srvs)
            srv.close();

        assertTrue(disconnectLatch.await(30_000, MILLISECONDS));

        Collection<Ignite> startedSrvs = srvStartC.call();

        assertTrue(reconnectLatch.await(10_000, MILLISECONDS));

        return startedSrvs;
    }

    /**
     * @param e Client disconnected exception.
     * @return Reconnect future.
     */
    protected IgniteFuture<?> check(CacheException e) {
        log.info("Expected exception: " + e);

        if (!(e.getCause() instanceof IgniteClientDisconnectedException))
            log.error("Unexpected cause: " + e.getCause(), e);

        assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof IgniteClientDisconnectedException);

        IgniteClientDisconnectedException e0 = (IgniteClientDisconnectedException)e.getCause();

        assertNotNull(e0.reconnectFuture());

        return e0.reconnectFuture();
    }

    /**
     * @param e Client disconnected exception.
     */
    protected void checkAndWait(CacheException e) {
        check(e).get();
    }

    /**
     * @param e Client disconnected exception.
     */
    protected void checkAndWait(IgniteClientDisconnectedException e) {
        log.info("Expected exception: " + e);

        assertNotNull(e.reconnectFuture());

        e.reconnectFuture().get();
    }

    /**
     *
     */
    public static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        volatile CountDownLatch writeLatch;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout)
            throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                CountDownLatch writeLatch0 = writeLatch;

                if (writeLatch0 != null) {
                    log.info("Block join request send: " + msg);

                    U.await(writeLatch0);
                }
            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    protected static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        volatile Class msgCls;

        /** */
        AtomicBoolean collectStart = new AtomicBoolean(false);

        /** */
        ConcurrentHashMap<String, ClusterNode> classes = new ConcurrentHashMap<>();

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Class msgCls0 = msgCls;

            if (collectStart.get() && msg instanceof GridIoMessage)
                classes.put(((GridIoMessage)msg).message().getClass().getName(), node);

            if (msgCls0 != null && msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message().getClass().equals(msgCls)) {
                log.info("Block message: " + msg);

                return;
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * @param clazz Class of messages which will be block.
         */
        public void blockMessage(Class clazz) {
            msgCls = clazz;
        }

        /**
         * Unlock all message.
         */
        public void unblockMessage() {
            msgCls = null;
        }

        /**
         * Start collect messages.
         */
        public void start() {
            collectStart.set(true);
        }

        /**
         * Print collected messages.
         */
        public void print() {
            for (String s : classes.keySet())
                log.error(s);
        }
    }
}
