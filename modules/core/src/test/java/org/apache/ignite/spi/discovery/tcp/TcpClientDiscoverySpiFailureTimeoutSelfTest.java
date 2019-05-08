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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;

/**
 * Client-based discovery SPI test with failure detection timeout enabled.
 */
public class TcpClientDiscoverySpiFailureTimeoutSelfTest extends TcpClientDiscoverySpiSelfTest {
    /** */
    private final static int FAILURE_AWAIT_TIME = 7_000;

    /** */
    private final static long FAILURE_THRESHOLD = 10_000;

    /** Failure detection timeout for nodes configuration. */
    private static long failureThreshold = FAILURE_THRESHOLD;

    /** */
    private static boolean useTestSpi;

    /** */
    private static boolean disableTopChangeRecovery;

    /** {@inheritDoc} */
    @Override protected boolean useFailureDetectionTimeout() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long clientFailureDetectionTimeout() {
        return clientFailureDetectionTimeout;
    }

    /** {@inheritDoc} */
    @Override protected long failureDetectionTimeout() {
        return failureThreshold;
    }

    /** {@inheritDoc} */
    @Override protected long awaitTime() {
        return failureDetectionTimeout() + FAILURE_AWAIT_TIME;
    }

    /** {@inheritDoc} */
    @Override protected long awaitClientTime() {
        return clientFailureDetectionTimeout() + FAILURE_AWAIT_TIME;
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoverySpi getDiscoverySpi() {
        TcpDiscoverySpi spi = useTestSpi ? new TestTcpDiscoverySpi2() : super.getDiscoverySpi();

        if (disableTopChangeRecovery)
            spi.setConnectionRecoveryTimeout(0);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        disableTopChangeRecovery = false;
    }

    /**
     * @throws Exception in case of error.
     */
    public void testFailureDetectionTimeoutEnabled() throws Exception {
        startServerNodes(1);
        startClientNodes(1);

        checkNodes(1, 1);

        assertTrue(((TcpDiscoverySpi)(G.ignite("server-0").configuration().getDiscoverySpi())).
            failureDetectionTimeoutEnabled());
        assertEquals(failureDetectionTimeout(),
            ((TcpDiscoverySpi)(G.ignite("server-0").configuration().getDiscoverySpi())).failureDetectionTimeout());

        assertTrue(((TcpDiscoverySpi)(G.ignite("client-0").configuration().getDiscoverySpi())).
            failureDetectionTimeoutEnabled());
        assertEquals(failureDetectionTimeout(),
            ((TcpDiscoverySpi)(G.ignite("client-0").configuration().getDiscoverySpi())).failureDetectionTimeout());
    }

    /**
     * @throws Exception in case of error.
     */
    public void testFailureTimeoutWorkabilityAvgTimeout() throws Exception {
        failureThreshold = 3000;

        try {
            checkFailureThresholdWorkability();
        }
        finally {
            failureThreshold = FAILURE_THRESHOLD;
        }
    }

    /**
     * @throws Exception in case of error.
     */
    public void testFailureTimeoutWorkabilitySmallTimeout() throws Exception {
        failureThreshold = 500;

        try {
            checkFailureThresholdWorkability();
        }
        finally {
            failureThreshold = FAILURE_THRESHOLD;
        }
    }

    /**
     * Test failure detection time between server and client if client fail with failure detection.
     *
     * @throws Exception in case of error.
     */
    public void testFailureTimeoutServerClient() throws Exception {
        failureThreshold = 3000;
        clientFailureDetectionTimeout = 2000;

        try {
            startServerNodes(1);

            startClientNodes(1);

            checkNodes(1, 1);

            Ignite srvNode = G.ignite("server-0");
            final TcpDiscoverySpi srvSpi = (TcpDiscoverySpi) srvNode.configuration().getDiscoverySpi();

            Ignite clientNode = G.ignite("client-0");
            final TcpDiscoverySpi clientSpi = (TcpDiscoverySpi)clientNode.configuration().getDiscoverySpi();

            long failureTime = U.currentTimeMillis();

            final long[] failureDetectTime = new long[1];
            final CountDownLatch latch = new CountDownLatch(1);

            clientSpi.simulateNodeFailure();

            srvNode.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    failureDetectTime[0] = U.currentTimeMillis();

                    latch.countDown();

                    return true;
                }
            }, EVT_NODE_FAILED);

            assertTrue("Can't get node failure event", latch.await(15000, TimeUnit.MILLISECONDS));

            long detectTime = failureDetectTime[0] - failureTime;

            assertTrue("Client node failure detected too fast: " + detectTime + "ms",
                detectTime > clientFailureDetectionTimeout - 200);
            assertTrue("Client node failure detected too slow:  " + detectTime + "ms",
                detectTime < clientFailureDetectionTimeout + 5000);
        }
        finally {
            failureThreshold = FAILURE_THRESHOLD;
        }
    }

    /**
     * Test failure detection time between servers with failure detection.
     *
     * @throws Exception in case of error.
     */
    public void testFailureTimeout3Server() throws Exception {
        failureThreshold = 1000;
        clientFailureDetectionTimeout = 10000;
        useTestSpi = true;
        disableTopChangeRecovery = true;

        try {
            startServerNodes(3);

            checkNodes(3, 0);

            Ignite srv0 = G.ignite("server-0");
            final TestTcpDiscoverySpi2 spi0 = (TestTcpDiscoverySpi2)srv0.configuration().getDiscoverySpi();

            final Ignite srv1 = G.ignite("server-1");
            final TestTcpDiscoverySpi2 spi1 = (TestTcpDiscoverySpi2)srv1.configuration().getDiscoverySpi();

            Ignite srv2 = G.ignite("server-2");
            final TestTcpDiscoverySpi2 spi2 = (TestTcpDiscoverySpi2)srv2.configuration().getDiscoverySpi();

            long failureTime = U.currentTimeMillis();

            final AtomicLong failureDetectTime = new AtomicLong();
            final CountDownLatch latch = new CountDownLatch(2);

            spi1.writeToSocketDelay = 2000;

            for (Ignite srv : new Ignite[]{srv0, srv2}) {
                srv.events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        DiscoveryEvent evt0 = (DiscoveryEvent)evt;

                        assertEquals(srv1.cluster().localNode().id(), evt0.eventNode().id());

                        failureDetectTime.compareAndSet(0, U.currentTimeMillis());

                        latch.countDown();

                        return true;
                    }
                }, EVT_NODE_FAILED);
            }

            assertTrue("Can't get node failure event", latch.await(15000, TimeUnit.MILLISECONDS));

            long detectTime = failureDetectTime.get() - failureTime;

            assertTrue("Server node failure detected too fast: " + detectTime + "ms",
                detectTime > failureThreshold - 100);
            assertTrue("Server node failure detected too slow:  " + detectTime + "ms",
                detectTime < clientFailureDetectionTimeout);
        }
        finally {
            failureThreshold = FAILURE_THRESHOLD;
            useTestSpi = false;
        }
    }

    /**
     * @throws Exception in case of error.
     */
    private void checkFailureThresholdWorkability() throws Exception {
        useTestSpi = true;

        TestTcpDiscoverySpi2 firstSpi = null;
        TestTcpDiscoverySpi2 secondSpi = null;

        try {
            startServerNodes(2);

            checkNodes(2, 0);

            firstSpi = (TestTcpDiscoverySpi2)(G.ignite("server-0").configuration().getDiscoverySpi());
            secondSpi = (TestTcpDiscoverySpi2)(G.ignite("server-1").configuration().getDiscoverySpi());

            assert firstSpi.err == null;

            secondSpi.readDelay = failureDetectionTimeout() + 5000;

            assertFalse(firstSpi.pingNode(secondSpi.getLocalNodeId()));

            Thread.sleep(failureDetectionTimeout());

            assertTrue(firstSpi.err != null && X.hasCause(firstSpi.err, SocketTimeoutException.class));

            firstSpi.reset();
            secondSpi.reset();

            assertTrue(firstSpi.pingNode(secondSpi.getLocalNodeId()));

            assertTrue(firstSpi.err == null);
        }
        finally {
            useTestSpi = false;

            if (firstSpi != null)
                firstSpi.reset();

            if (secondSpi != null)
                secondSpi.reset();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectOnCoordinatorRouterFail1() throws Exception {
        clientReconnectOnCoordinatorRouterFail(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectOnCoordinatorRouterFail2() throws Exception {
        clientReconnectOnCoordinatorRouterFail(2);
    }

    /**
     * Test tries to provoke scenario when client sends reconnect message before router failure detected.
     *
     * @param srvNodes Number of additional server nodes.
     * @throws Exception If failed.
     */
    public void clientReconnectOnCoordinatorRouterFail(int srvNodes) throws Exception {
        startServerNodes(1);

        Ignite srv = G.ignite("server-0");

        final TcpDiscoveryNode srvNode = (TcpDiscoveryNode)srv.cluster().localNode();

        final UUID srvNodeId = srvNode.id();

        clientIpFinder = new TcpDiscoveryVmIpFinder();

        clientIpFinder.setAddresses(
            Collections.singleton("localhost:" + srvNode.discoveryPort() + ".." + (srvNode.discoveryPort() + 1)));

        failureThreshold = 1000L;
        netTimeout = 1000L;

        startClientNodes(1); // Client should connect to coordinator.

        failureThreshold = 10_000L;
        netTimeout = 5000L;

        List<String> nodes = new ArrayList<>();

        for (int i = 0; i < srvNodes; i++) {
            Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

            nodes.add(g.name());

            srvNodeIds.add(g.cluster().localNode().id());
        }

        checkNodes(1 + srvNodes, 1);

        nodes.add("client-0");

        final CountDownLatch latch = new CountDownLatch(nodes.size());

        final AtomicBoolean err = new AtomicBoolean();

        for (String node : nodes) {
            G.ignite(node).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    DiscoveryEvent disoEvt = (DiscoveryEvent)evt;

                    if (disoEvt.eventNode().id().equals(srvNodeId)) {
                        info("Expected node failed event: " + ((DiscoveryEvent) evt).eventNode());

                        latch.countDown();
                    }
                    else {
                        log.info("Unexpected node failed event: " + evt);

                        err.set(true);
                    }

                    return true;
                }
            }, EVT_NODE_FAILED);
        }

        Thread.sleep(5000);

        Ignite client = G.ignite("client-0");

        UUID nodeId = client.cluster().localNode().id();

        log.info("Fail coordinator: " + srvNodeId);

        TestTcpDiscoverySpi srvSpi = (TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi();

        srvSpi.pauseAll(false);

        try {
            Thread.sleep(2000);
        }
        finally {
            srvSpi.simulateNodeFailure();
            srvSpi.resumeAll();
        }

        try {
            assertTrue(latch.await(failureThreshold + 3000, TimeUnit.MILLISECONDS));

            assertFalse("Unexpected event, see log for details.", err.get());
            assertEquals(nodeId, client.cluster().localNode().id());
        }
        finally {
            srvSpi.resumeAll();
        }
    }

    /**
     *
     */
    private static class TestTcpDiscoverySpi2 extends TcpDiscoverySpi {
        /** */
        private volatile long readDelay;

        private volatile long writeToSocketDelay;

        /** */
        private Exception err;

        /**  */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (writeToSocketDelay > 0) {
                try {
                    U.dumpStack(log, "Before sleep [msg=" + msg +
                        ", arrLen=" + (data != null ? data.length : "n/a") + ']');

                    Thread.sleep(writeToSocketDelay);
                }
                catch (InterruptedException e) {
                    // Nothing to do.
                }
            }

            if (sock.getSoTimeout() >= writeToSocketDelay)
                super.writeToSocket(sock, msg, data, timeout);
            else
                throw new SocketTimeoutException("Write to socket delay timeout exception.");
        }

        /**  */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (writeToSocketDelay > 0) {
                try {
                    U.dumpStack(log, "Before sleep [msg=" + msg + ']');

                    Thread.sleep(writeToSocketDelay);
                }
                catch (InterruptedException e) {
                    // Nothing to do.
                }
            }

            if (sock.getSoTimeout() >= writeToSocketDelay)
                super.writeToSocket(sock, out, msg, timeout);
            else
                throw new SocketTimeoutException("Write to socket delay timeout exception.");
        }

        /**  */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (writeToSocketDelay > 0) {
                try {
                    U.dumpStack(log, "Before sleep [msg=" + msg + ']');

                    Thread.sleep(writeToSocketDelay);
                }
                catch (InterruptedException e) {
                    // Nothing to do.
                }
            }

            if (sock.getSoTimeout() >= writeToSocketDelay)
                super.writeToSocket(sock, msg, timeout);
            else
                throw new SocketTimeoutException("Write to socket delay timeout exception.");
        }

        /**  */
        @Override protected void writeToSocket(
            TcpDiscoveryAbstractMessage msg,
            Socket sock,
            int res,
            long timeout
        ) throws IOException {
            if (writeToSocketDelay > 0) {
                try {
                    U.dumpStack(log, "Before sleep [msg=" + msg + ']');

                    Thread.sleep(writeToSocketDelay);
                }
                catch (InterruptedException e) {
                    // Nothing to do.
                }
            }

            if (sock.getSoTimeout() >= writeToSocketDelay)
                super.writeToSocket(msg, sock, res, timeout);
            else
                throw new SocketTimeoutException("Write to socket delay timeout exception.");
        }

        /** {@inheritDoc} */
        @Override protected <T> T readMessage(Socket sock, @Nullable InputStream in, long timeout)
            throws IOException, IgniteCheckedException {
            long currTimeout = getLocalNode().isClient() ?
                clientFailureDetectionTimeout() : failureDetectionTimeout();

            if (readDelay < currTimeout) {
                try {
                    return super.readMessage(sock, in, timeout);
                }
                catch (Exception e) {
                    err = e;

                    throw e;
                }
            }
            else {
                T msg = super.readMessage(sock, in, timeout);

                if (msg instanceof TcpDiscoveryPingRequest) {
                    try {
                        Thread.sleep(2000);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }

                    throw new SocketTimeoutException("Forced timeout");
                }

                return msg;
            }
        }

        /**
         * Resets testing state.
         */
        private void reset() {
            readDelay = 0;
            writeToSocketDelay = 0;
            err = null;
        }
    }
}
