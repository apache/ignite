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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_ADDRS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_PORT;

/**
 * Tests that faulty client will be failed if connection can't be established.
 */
public class TcpCommunicationSpiFaultyClientTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Client mode. */
    private static boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(3000);
        cfg.setClientMode(clientMode);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        TcpDiscoverySpi discoSpi;

        if (clientMode && gridName.contains("3"))
            discoSpi = new FaultyClientDiscoverySpi();
        else
            discoSpi = clientMode ? new TcpDiscoverySpi() : new TestDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoServerOnHost() throws Exception {
        testFailClient(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotAcceptedConnection() throws Exception {
        testFailClient(new FakeServer());
    }

    /**
     * @param srv Server.
     */
    private void testFailClient(FakeServer srv) throws Exception {
        IgniteInternalFuture<Long> fut = null;

        try {
            if (srv != null)
                fut = GridTestUtils.runMultiThreadedAsync(srv, 1, "fake-server");

            clientMode = false;

            final Ignite ignite = startGrids(2);

            clientMode = true;

            startGrid(2);

            assertEquals(1, ignite.cluster().forClients().nodes().size());

            GridTestUtils.assertThrowsWithCause(new Callable<Ignite>() {
                @Override public Ignite call() throws Exception {
                    return startGrid(3);
                }
            }, IgniteClientDisconnectedCheckedException.class);

            CountDownLatch latch = ((TestDiscoverySpi)ignite.configuration().getDiscoverySpi()).latch;

            assertTrue(latch.await(3, TimeUnit.SECONDS));

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.cluster().forClients().nodes().size() == 1;
                }
            }, 5000);

            assertEquals(1, ignite.cluster().forClients().nodes().size());
        }
        finally {
            if (srv != null) {
                srv.stop();

                assert fut != null;

                fut.get();
            }

            stopAllGrids();
        }
    }

    /**
     * Server that emulates connection troubles.
     */
    private static class FakeServer implements Runnable {
        /** Server. */
        private final ServerSocket srv;

        /** Stop. */
        private volatile boolean stop;

        /**
         * Default constructor.
         */
        FakeServer() throws IOException {
            this.srv = new ServerSocket(47200, 50, InetAddress.getByName("127.0.0.1"));
        }

        /**
         *
         */
        public void stop() {
            stop = true;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (!stop) {
                    try {
                        U.sleep(10);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        // No-op.
                    }
                }
            }
            finally {
                U.closeQuiet(srv);
            }
        }
    }

    /**
     *
     */
    private static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** Latch. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeFailedMessage)
                latch.countDown();

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class FaultyClientDiscoverySpi extends TcpDiscoverySpi {
        @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
            super.setNodeAttributes(attrs, ver);

            attrs.put(createAttributeName(ATTR_ADDRS), Collections.singleton("127.0.0.1"));
            attrs.put(createAttributeName(ATTR_PORT), 47200);
        }

        private static String createAttributeName(String name) {
            return TcpCommunicationSpi.class.getSimpleName() + '.' + name;
        }
    }
}
