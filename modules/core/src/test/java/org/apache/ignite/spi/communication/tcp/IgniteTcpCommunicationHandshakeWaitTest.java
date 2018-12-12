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

package org.apache.ignite.spi.communication.tcp;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Testing {@link TcpCommunicationSpi} that will send the wait handshake message on received connections until SPI
 * context initialized.
 */
public class IgniteTcpCommunicationHandshakeWaitTest extends GridCommonAbstractTest {
    /** */
    private static final long COMMUNICATION_TIMEOUT = 1000;

    /** */
    private static final long DISCOVERY_MESSAGE_DELAY = 500;

    /** */
    private static final int START_PORT = 55443;

    /** */
    private boolean customDiscoSpi;

    /** */
    private final AtomicBoolean slowNet = new AtomicBoolean();

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private boolean customCommSpi;

    /** */
    private volatile CountDownLatch commStartLatch;

    /** */
    private volatile int commSpiBoundedPort;

    /** */
    private volatile String commSpiSrvAddr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        if (customDiscoSpi) {
            TcpDiscoverySpi discoSpi = new SlowTcpDiscoverySpi();

            cfg.setDiscoverySpi(discoSpi);
        }

        TcpCommunicationSpi commSpi = customCommSpi ? new TestCommunicationSpi() : new TcpCommunicationSpi();

        commSpi.setConnectTimeout(COMMUNICATION_TIMEOUT);
        commSpi.setMaxConnectTimeout(COMMUNICATION_TIMEOUT);
        commSpi.setReconnectCount(1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * Test that joining node will send the wait handshake message on received connections until SPI context
     * initialized.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeOnNodeJoining() throws Exception {
        customDiscoSpi = true;

        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL, "true");

        IgniteEx ignite = startGrid("srv1");

        startGrid("srv2");

        slowNet.set(true);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            latch.await(2 * COMMUNICATION_TIMEOUT, TimeUnit.MILLISECONDS);

            Collection<ClusterNode> nodes = ignite.context().discovery().aliveServerNodes();

            assertEquals(3, nodes.size());

            return ignite.context().io().sendIoTest(new ArrayList<>(nodes), null, true).get();
        });

        startGrid("srv3");

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientConnectBeforeDiscoveryStarted() throws Exception {
        customCommSpi = true;

        GridNioServer<?> srvr = startServer();

        try {
            commStartLatch = new CountDownLatch(1);

            GridTestUtils.runAsync(() -> {
                startGrid(0);

                return true;
            });

            assertTrue(commStartLatch.await(5_000, TimeUnit.MILLISECONDS));

            SocketChannel ch = SocketChannel.open(new InetSocketAddress(commSpiSrvAddr, commSpiBoundedPort));

            GridNioSession ses = srvr.createSession(ch, null, false, null).get();

            boolean wait = GridTestUtils.waitForCondition(
                () -> ses.bytesReceived() == HandshakeWaitMessage.MESSAGE_FULL_SIZE, 1000);

            assertTrue("Handshake not started.", wait);
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * Starts custom server.
     *
     * @return Started server.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private GridNioServer<?> startServer() throws Exception {
        int srvPort = START_PORT;

        for (int i = 0; i < 10; i++) {
            try {
                GridNioServerListenerAdapter lsnr = new GridNioServerListenerAdapter() {
                    @Override public void onConnected(GridNioSession ses) {
                        // No-op.
                    }

                    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                        // No-op.
                    }

                    @Override public void onMessage(GridNioSession ses, Object msg) {
                        // No-op.
                    }
                };

                GridNioServer<?> srvr = GridNioServer.builder()
                    .address(U.getLocalHost())
                    .port(srvPort)
                    .listener(lsnr)
                    .logger(log)
                    .selectorCount(Runtime.getRuntime().availableProcessors())
                    .igniteInstanceName("nio-test-grid")
                    .filters().build();

                srvr.start();

                return srvr;
            }
            catch (IgniteCheckedException e) {
                if (i < 9 && e.hasCause(BindException.class)) {
                    log.error("Failed to start server, will try another port [err=" + e + ", port=" + srvPort + ']');

                    U.sleep(1000);

                    srvPort++;
                }
                else
                    throw e;
            }
        }

        fail("Failed to start server.");

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);
    }

    /** */
    private class SlowTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected boolean ensured(TcpDiscoveryAbstractMessage msg) {
            if (slowNet.get() && msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                try {
                    if (igniteInstanceName.contains("srv2") && msg.verified())
                        latch.countDown();

                    U.sleep(DISCOVERY_MESSAGE_DELAY);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteSpiException("Thread has been interrupted.", e);
                }
            }

            return super.ensured(msg);
        }
    }

    /** */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
            super.spiStart(igniteInstanceName);

            commSpiBoundedPort = boundPort();

            commSpiSrvAddr = getLocalAddress();

            commStartLatch.countDown();
        }
    }
}
