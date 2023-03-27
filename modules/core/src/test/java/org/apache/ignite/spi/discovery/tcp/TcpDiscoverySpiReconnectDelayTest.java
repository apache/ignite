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
import java.io.OutputStream;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoveryImpl.RES_WAIT;

/**
 * Test for {@link TcpDiscoverySpi#setReconnectDelay(int)}.
 */
public class TcpDiscoverySpiReconnectDelayTest extends GridCommonAbstractTest {
    /** Time to wait for events. */
    private static final int EVT_TIMEOUT = 120000;

    /** Timeout for socket operations. */
    private static final int SOCK_AND_ACK_TIMEOUT = 500;

    static {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
    }

    //region Client joins after failNode()

    /** */
    @Test
    public void testClientJoinAfterFailureShortTimeout() throws Exception {
        checkClientJoinAfterNodeFailure(5, 500);
    }

    /** */
    @Test
    public void testClientJoinAfterFailureLongTimeout() throws Exception {
        checkClientJoinAfterNodeFailure(3, 5000);
    }

    /**
     * Check that client restores connection after the given time, with the expected number of messages sent
     * and expected time elapsed.
     *
     * @param numOfFailedRequests number of TcpDiscoveryJoinRequestMessage to be failed before succeed to connect.
     * @param reconnectDelay argument for {@link TcpDiscoverySpi#setReconnectDelay(int)}
     */
    private void checkClientJoinAfterNodeFailure(int numOfFailedRequests, int reconnectDelay) throws Exception {
        try (
            Ignite ignite1 = G.start(getConfiguration("server", false, reconnectDelay));
            Ignite ignite2 = G.start(getConfiguration("client", true, reconnectDelay))
        ) {
            // Check topology.
            assertEquals(1L, ignite1.cluster().localNode().order());
            assertEquals(2L, ignite2.cluster().localNode().order());
            assertEquals(2L, ignite2.cluster().topologyVersion());

            final CountDownLatch failLatch = new CountDownLatch(1);
            final CountDownLatch joinLatch = new CountDownLatch(1);
            final CountDownLatch reconnectLatch = new CountDownLatch(1);
            final CountDownLatch disconnectLatch = new CountDownLatch(1);

            ignite1.events().localListen(new IgnitePredicate<DiscoveryEvent>() {
                @Override public boolean apply(DiscoveryEvent evt) {
                    info("Node1 event: " + evt);

                    if (evt.type() == EVT_NODE_FAILED)
                        failLatch.countDown();
                    else if (evt.type() == EVT_NODE_JOINED)
                        joinLatch.countDown();

                    return true;
                }
            }, EVT_NODE_FAILED, EVT_NODE_JOINED);

            ignite2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    info("Node2 event: " + evt);

                    if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED)
                        disconnectLatch.countDown();
                    else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED)
                        reconnectLatch.countDown();

                    return true;
                }
            }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

            long startTime = System.currentTimeMillis();

            AtomicInteger failJoinReq = ((FailingTcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).failJoinReq;
            failJoinReq.set(numOfFailedRequests);
            ignite1.configuration().getDiscoverySpi().failNode(ignite2.cluster().localNode().id(), null);

            assertTrue(disconnectLatch.await(EVT_TIMEOUT, MILLISECONDS));
            assertTrue(failLatch.await(EVT_TIMEOUT, MILLISECONDS));
            assertTrue(reconnectLatch.await(EVT_TIMEOUT, MILLISECONDS));
            assertTrue(joinLatch.await(EVT_TIMEOUT, MILLISECONDS));

            long endTime = System.currentTimeMillis();

            // Check topology.
            assertEquals(1L, ignite1.cluster().localNode().order());
            assertEquals(4L, ignite2.cluster().localNode().order());
            assertEquals(4L, ignite2.cluster().topologyVersion());

            // Check connection time.
            // Total time should be at least the sum of all delays.
            long totalTime = endTime - startTime;
            long expTotalTime = numOfFailedRequests * reconnectDelay;
            assertTrue(totalTime >= expTotalTime);

            // Check number of messages.
            // If exactly numOfFailedRequests fail, counter will be at -1.
            // If unexpected additional requests are sent, counter will be <= -2.
            int cntr = failJoinReq.get();
            int numOfMessages = numOfFailedRequests - cntr;
            int expNumOfMessages = numOfFailedRequests + 1;
            assertEquals("Unexpected number of messages", expNumOfMessages, numOfMessages);
        }
    }

    //endregion

    //region Client joins after brakeConnection()

    /** */
    @Test
    public void testClientJoinAfterSocketClosedShortTimeout() throws Exception {
        checkClientJoinAfterSocketClosed(5, 500);
    }

    /** */
    @Test
    public void testClientJoinAfterSocketClosedLongTimeout() throws Exception {
        checkClientJoinAfterSocketClosed(3, 5000);
    }

    /**
     * Check that client restores connection after the given time, with the expected number of messages sent
     * and expected time elapsed.
     *
     * @param numOfFailedRequests number of TcpDiscoveryJoinRequestMessage to be failed before succeed to connect.
     * @param reconnectDelay argument for {@link TcpDiscoverySpi#setReconnectDelay(int)}
     */
    private void checkClientJoinAfterSocketClosed(int numOfFailedRequests, int reconnectDelay) throws Exception {
        try (
            Ignite ignite1 = G.start(getConfiguration("server", false, reconnectDelay));
            Ignite ignite2 = G.start(getConfiguration("client", true, reconnectDelay))
        ) {
            // Check topology.
            assertEquals(1L, ignite1.cluster().localNode().order());
            assertEquals(2L, ignite2.cluster().localNode().order());
            assertEquals(2L, ignite2.cluster().topologyVersion());

            long startTime = System.currentTimeMillis();

            AtomicInteger failCntr = ((FailingTcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).failReconReq;
            failCntr.set(numOfFailedRequests);
            ((TcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).brakeConnection();

            // Need to send a discovery message to a remote node to provoke reconnection.
            // remoteListen() is used because it is synchronous (e.g. send() is not).
            ignite2.message().remoteListen(null, new DummyListener());

            long endTime = System.currentTimeMillis();

            // Check topology.
            assertEquals(1L, ignite1.cluster().localNode().order());
            assertEquals(2L, ignite2.cluster().localNode().order());
            assertEquals(2L, ignite2.cluster().topologyVersion());

            // Check connection time.
            // Total time should be at least the sum of all delays.
            long totalTime = endTime - startTime;
            long expTotalTime = numOfFailedRequests * reconnectDelay;
            assertTrue(totalTime >= expTotalTime);

            // Check number of messages.
            // If exactly numOfFailedRequests fail, counter will be at -1.
            // If unexpected additional requests are sent, counter will be <= -2.
            int cntr = failCntr.get();
            int numOfMessages = numOfFailedRequests - cntr;
            int expNumOfMessages = numOfFailedRequests + 1;
            assertEquals("Unexpected number of messages", expNumOfMessages, numOfMessages);
        }
    }

    //endregion

    //region Client joins at start

    /** */
    @Test
    public void testClientJoinAtStartShortTimeout() throws Exception {
        checkClientJoinAtStart(5, 500);
    }

    /** */
    @Test
    public void testClientJoinAtStartLongTimeout() throws Exception {
        checkClientJoinAtStart(3, 5000);
    }

    /** */
    @Test
    public void testServerJoinAtStartShortTimeout() throws Exception {
        checkServerJoinAtStart(5, 500);
    }

    /** */
    @Test
    public void testServerJoinAtStartLongTimeout() throws Exception {
        checkServerJoinAtStart(3, 5000);
    }

    /**
     * Check that client restores connection after the given time, with the expected number of messages sent
     * and expected time elapsed.
     *  @param numOfFailedRequests number of TcpDiscoveryJoinRequestMessage to be failed before succeed to connect.
     * @param reconnectDelay argument for {@link TcpDiscoverySpi#setReconnectDelay(int)}
     */
    private void checkClientJoinAtStart(int numOfFailedRequests, int reconnectDelay) throws Exception {
        try (
            Ignite ignite1 = G.start(getConfiguration("server", false, reconnectDelay))
        ) {
            final CountDownLatch joinLatch = new CountDownLatch(1);

            ignite1.events().localListen(new IgnitePredicate<DiscoveryEvent>() {
                @Override public boolean apply(DiscoveryEvent evt) {
                    info("Node1 event: " + evt);

                    if (evt.type() == EVT_NODE_JOINED)
                        joinLatch.countDown();

                    return true;
                }
            }, EVT_NODE_JOINED);

            IgniteConfiguration ignite2Cfg = getConfiguration("client", true, reconnectDelay);
            final AtomicInteger failJoinReq = ((FailingTcpDiscoverySpi)ignite2Cfg.getDiscoverySpi()).failJoinReq;
            failJoinReq.set(numOfFailedRequests);

            final long startTime = System.currentTimeMillis();

            try (Ignite ignite2 = G.start(ignite2Cfg)) {
                assertTrue(joinLatch.await(EVT_TIMEOUT, MILLISECONDS));

                long endTime = System.currentTimeMillis();

                // Check topology.
                assertEquals(1L, ignite1.cluster().localNode().order());
                assertEquals(2L, ignite2.cluster().localNode().order());
                assertEquals(2L, ignite2.cluster().topologyVersion());

                // Check connection time.
                // Total time should be at least the sum of all delays.
                long totalTime = endTime - startTime;
                long expTotalTime = numOfFailedRequests * reconnectDelay;
                assertTrue(totalTime >= expTotalTime);

                // Check number of messages.
                // If exactly numOfFailedRequests fail, counter will be at -1.
                // If unexpected additional requests are sent, counter will be <= -2.
                int cntr = failJoinReq.get();
                int numOfMessages = numOfFailedRequests - cntr;
                int expNumOfMessages = numOfFailedRequests + 1;
                assertEquals("Unexpected number of messages", expNumOfMessages, numOfMessages);
            }
        }
    }

    /**
     * Check that client restores connection after the given time, with the expected number of messages sent
     * and expected time elapsed.
     *  @param numOfFailedRequests number of TcpDiscoveryJoinRequestMessage to be failed before succeed to connect.
     * @param reconnectDelay argument for {@link TcpDiscoverySpi#setReconnectDelay(int)}
     */
    private void checkServerJoinAtStart(int numOfFailedRequests, int reconnectDelay) throws Exception {
        try (
            Ignite ignite1 = G.start(getConfiguration("server", false, reconnectDelay))
        ) {
            final CountDownLatch joinLatch = new CountDownLatch(1);
            final AtomicInteger failJoinReqRes = ((FailingTcpDiscoverySpi)ignite1.configuration().getDiscoverySpi())
                .failJoinReqRes;
            failJoinReqRes.set(numOfFailedRequests);

            ignite1.events().localListen(new IgnitePredicate<DiscoveryEvent>() {
                @Override public boolean apply(DiscoveryEvent evt) {
                    info("Node1 event: " + evt);

                    if (evt.type() == EVT_NODE_JOINED)
                        joinLatch.countDown();

                    return true;
                }
            }, EVT_NODE_JOINED);

            final long startTime = System.currentTimeMillis();

            try (Ignite ignite2 = G.start(getConfiguration("server-2", false, reconnectDelay))) {
                assertTrue(joinLatch.await(EVT_TIMEOUT, MILLISECONDS));

                long endTime = System.currentTimeMillis();

                // Check topology.
                assertEquals(1L, ignite1.cluster().localNode().order());
                assertEquals(2L, ignite2.cluster().localNode().order());
                assertEquals(2L, ignite2.cluster().topologyVersion());

                // Check connection time.
                // Total time should be at least the sum of all delays.
                long totalTime = endTime - startTime;
                long expTotalTime = numOfFailedRequests * reconnectDelay;
                assertTrue(totalTime >= expTotalTime);

                // Check number of messages.
                // If exactly numOfFailedRequests fail, counter will be at -1.
                // If unexpected additional requests are sent, counter will be <= -2.
                int cntr = failJoinReqRes.get();
                int numOfMessages = numOfFailedRequests - cntr;
                int expNumOfMessages = numOfFailedRequests + 1;
                assertEquals("Unexpected number of messages", expNumOfMessages, numOfMessages);
            }
        }
    }

    //endregion

    //region Helpers

    /** */
    private IgniteConfiguration getConfiguration(String name, boolean isClient, int reconnectDelay) {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName(name)
            .setDiscoverySpi(new FailingTcpDiscoverySpi()
                .setIpFinder(LOCAL_IP_FINDER)
                .setReconnectDelay(reconnectDelay)
                // Allow reconnection to take long.
                .setNetworkTimeout(EVT_TIMEOUT)
                // Make sure reconnection attempts are short enough.
                // Each reconnection attempt is
                // 500ms for write (socketTimeout) + 500ms for read (ackTimeout)
                // tried only once.
                .setSocketTimeout(SOCK_AND_ACK_TIMEOUT)
                .setAckTimeout(SOCK_AND_ACK_TIMEOUT)
                .setReconnectCount(1))
            // Make sure that server doesn't kick reconnecting client out.
            .setClientFailureDetectionTimeout(EVT_TIMEOUT);

        if (isClient)
            cfg.setClientMode(true);
        return cfg;
    }

    /** Custom Discovery SPI allowing to fail sending of certain messages. */
    private static class FailingTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final AtomicInteger failJoinReq = new AtomicInteger();

        /** */
        private final AtomicInteger failJoinReqRes = new AtomicInteger();

        /** */
        private final AtomicInteger failReconReq = new AtomicInteger();

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {

            if (!onMessage(sock, msg))
                return;

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {

            if (msg instanceof TcpDiscoveryJoinRequestMessage && failJoinReqRes.getAndDecrement() > 0)
                res = RES_WAIT;

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         * @param sock Socket.
         * @param msg Message.
         * @return {@code False} if should not further process message.
         * @throws IOException If failed.
         */
        private boolean onMessage(Socket sock, TcpDiscoveryAbstractMessage msg) throws IOException {
            boolean fail = false;

            if (msg instanceof TcpDiscoveryJoinRequestMessage)
                fail = failJoinReq.getAndDecrement() > 0;
            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                fail = failReconReq.getAndDecrement() > 0;

            if (fail) {
                log.info("Close socket on message write [msg=" + msg + "]");

                sock.close();
            }

            return true;
        }
    }

    /** */
    private static class DummyListener implements IgniteBiPredicate<UUID, Object> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Object msg) {
            return true;
        }
    }

    //endregion
}
