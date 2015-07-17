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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.discovery.tcp.messages.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;

/**
 *
 */
public abstract class IgniteClientReconnectAbstractTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long RECONNECT_TIMEOUT = 10_000;

    /** */
    protected boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestTcpDiscoverySpi disco = new TestTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);
        disco.setJoinTimeout(2 * 60_000);
        disco.setSocketTimeout(1000);
        disco.setNetworkTimeout(2000);

        cfg.setDiscoverySpi(disco);

        BlockTpcCommunicationSpi commSpi = new BlockTpcCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        if (clientMode)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @param latch Latch.
     * @throws Exception If failed.
     */
    protected void waitReconnectEvent(CountDownLatch latch) throws Exception {
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
    protected TestTcpDiscoverySpi spi(Ignite ignite) {
        return ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());
    }

    /**
     * @param ignite Node.
     * @return Communication SPI.
     */
    protected BlockTpcCommunicationSpi commSpi(Ignite ignite) {
        return ((BlockTpcCommunicationSpi)ignite.configuration().getCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        int srvs = serverCount();

        if (srvs > 0)
            startGrids(srvs);

        int clients = clientCount();

        if (clients > 0) {
            clientMode = true;

            startGridsMultiThreaded(srvs, clients);

            clientMode = false;
        }
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
        TcpDiscoveryNode node = (TcpDiscoveryNode)client.cluster().localNode();

        assertTrue(node.isClient());
        assertNotNull(node.clientRouterNodeId());

        Ignite srv = G.ignite(node.clientRouterNodeId());

        assertNotNull(srv);

        return srv;
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
        final TestTcpDiscoverySpi clientSpi = spi(client);
        final TestTcpDiscoverySpi srvSpi = spi(srv);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        clientSpi.writeLatch = new CountDownLatch(1);

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        };

        client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(disconnectLatch);

        if (disconnectedC != null)
            disconnectedC.run();

        log.info("Allow reconnect.");

        clientSpi.writeLatch.countDown();

        waitReconnectEvent(reconnectLatch);

        client.events().stopLocalListen(p);
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
    protected static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        volatile CountDownLatch writeLatch;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg)
            throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                CountDownLatch writeLatch0 = writeLatch;

                if (writeLatch0 != null) {
                    log.info("Block join request send: " + msg);

                    U.await(writeLatch0);
                }
            }

            super.writeToSocket(sock, msg);
        }
    }

    /**
     *
     */
    protected static class BlockTpcCommunicationSpi extends TcpCommunicationSpi {
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
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            Class msgCls0 = msgCls;

            if (collectStart.get() && msg instanceof GridIoMessage)
                classes.put(((GridIoMessage)msg).message().getClass().getName(), node);

            if (msgCls0 != null && msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message().getClass().equals(msgCls)) {
                log.info("Block message: " + msg);

                return;
            }

            super.sendMessage(node, msg);
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
