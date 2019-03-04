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

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Testing {@link TcpCommunicationSpi} that will send the wait handshake message on received connections until SPI
 * context initialized.
 */
public class IgniteTcpCommunicationConnectOnInitTest extends GridCommonAbstractTest {
    /** */
    private static final int START_PORT = 55443;

    /** */
    private volatile CountDownLatch commStartLatch;

    /** */
    private volatile CountDownLatch discoWriteLatch;

    /** */
    private volatile CountDownLatch discoStartLatch;

    /** */
    private volatile int commSpiBoundedPort;

    /** */
    private volatile String commSpiSrvAddr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());
        cfg.setDiscoverySpi(new TestDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder()
            .setAddresses(Collections.singleton("127.0.0.1:47500..47502"))));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientConnectBeforeDiscoveryStarted() throws Exception {
        GridNioServer<?> srvr = startServer();

        try {
            commStartLatch = new CountDownLatch(1);
            discoWriteLatch = new CountDownLatch(1);
            discoStartLatch = new CountDownLatch(1);

            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
                startGrid(0);

                return true;
            });

            assertTrue(discoStartLatch.await(5_000, TimeUnit.MILLISECONDS));
            assertTrue(commStartLatch.await(5_000, TimeUnit.MILLISECONDS));

            SocketChannel ch = SocketChannel.open(new InetSocketAddress(commSpiSrvAddr, commSpiBoundedPort));

            GridNioSession ses = srvr.createSession(ch, null, false, null).get();

            boolean wait = GridTestUtils.waitForCondition(
                () -> ses.bytesReceived() == HandshakeWaitMessage.MESSAGE_FULL_SIZE, 1000);

            assertTrue("Handshake not started.", wait);

            discoWriteLatch.countDown();

            fut.get();
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

    /**
     *
     */
    private class TestDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr, IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            awaitLatch();

            return super.openSocket(sockAddr, timeoutHelper);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr, IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            awaitLatch();

            return super.openSocket(sock, remAddr, timeoutHelper);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data, long timeout) throws IOException {
            awaitLatch();

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
            awaitLatch();

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(ClusterNode node, Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
            awaitLatch();

            super.writeToSocket(node, sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
            awaitLatch();

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res, long timeout) throws IOException {
            awaitLatch();

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         */
        private void awaitLatch() {
            try {
                discoStartLatch.countDown();
                discoWriteLatch.await();
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
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
