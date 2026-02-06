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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class MultiDataCenterSplitTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** */
    @Nullable private Supplier<TcpDiscoverySpi> discoSpiSupplier;

    /** */
    @Nullable private Long failureDetectionTimeout;

    /** */
    @Parameterized.Parameter()
    public int serversPerDc;

    /** */
    @Parameterized.Parameter(1)
    public boolean fullTimeoutFailure;

    /** */
    @Parameterized.Parameters(name = "serversPerDc={0}, fullTimeoutFailure={1}")
    public static Collection<Object[]> params() {
        return GridTestUtils.cartesianProduct(
            F.asList(2, 3, 4),
            F.asList(true, false)
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Setup DiscoverySPI.
        TcpDiscoverySpi discoSpi = discoSpiSupplier != null
            ? discoSpiSupplier.get()
            : (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setIpFinder(LOCAL_IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        // Disable unnesessary disco. messages.
        cfg.setMetricsUpdateFrequency(getTestTimeout() * 3);
        cfg.setClientFailureDetectionTimeout(cfg.getMetricsUpdateFrequency());

        assert ((TcpDiscoverySpi)cfg.getDiscoverySpi()).locPort == TcpDiscoverySpi.DFLT_PORT;

        if (failureDetectionTimeout != null)
            cfg.setFailureDetectionTimeout(failureDetectionTimeout);

        return cfg;
    }

    /** */
    @Test
    public void testConnectionRecoveryWithEntireDCFailure() throws Exception {
        // Fastens the tests.
        failureDetectionTimeout = 3000L;

        // Start DC0 with the test DiscoverySPI.
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
        discoSpiSupplier = () -> new TestTcpDiscoverySpi(TcpDiscoverySpi.DFLT_PORT + serversPerDc,
            TcpDiscoverySpi.DFLT_PORT + serversPerDc * 2 - 1, fullTimeoutFailure);

        startGridsMultiThreaded(serversPerDc);

        Ignite dc0CornerNode = G.allGrids().stream().filter(ig -> ig.cluster().localNode().order() == serversPerDc)
            .findFirst().get();
        assert dc0CornerNode != null;

        // Start DC1 with the default DiscoverySPI.
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
        discoSpiSupplier = null;

        startGridsMultiThreaded(serversPerDc, serversPerDc);

        for (ClusterNode n : grid(0).cluster().nodes()) {
            assertTrue(n.dataCenterId().equals(n.order() <= serversPerDc ? DC_ID_0 : DC_ID_1));

            // Simulate failure of DC1. Now, no node from DC0 can send a discovery message to DC1.
            if (n.dataCenterId().equals(DC_ID_0))
                discoSpi(G.ignite(n.id())).block = true;
        }

        Thread.sleep(25000);
    }

    /** */
    private static TestTcpDiscoverySpi discoSpi(Ignite node) {
        return (TestTcpDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    /** */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final int minPortToBlockMsg;

        /** */
        private final int maxPortToBlockMsg;

        /** */
        private final boolean fullTimeoutFailure;

        /** */
        private volatile boolean block;

        /** */
        private TestTcpDiscoverySpi(int minPortToBlockMsg, int maxPortToBlockMsg, boolean fullTimeoutFailure) {
            this.minPortToBlockMsg = minPortToBlockMsg;
            this.maxPortToBlockMsg = maxPortToBlockMsg;
            this.fullTimeoutFailure = fullTimeoutFailure;
        }

        /** {@inheritDoc} */
        @Override protected void writeMessage(TcpDiscoveryIoSession ses, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            blockMessage(ses.socket(), timeout);

            super.writeMessage(ses, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            blockMessage(sock, timeout);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** */
        private void blockMessage(Socket sock, long timeout) throws IOException {
            if (!block)
                return;

            int rmpPort = ((InetSocketAddress)sock.getRemoteSocketAddress()).getPort();

            if (rmpPort < minPortToBlockMsg || rmpPort > maxPortToBlockMsg)
                return;

            if (log.isDebugEnabled())
                log.debug("Simulation network delay of " + (fullTimeoutFailure ? timeout : 50) + "ms on " + sock);

            try {
                U.sleep(fullTimeoutFailure ? timeout : 50);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IOException("Network delay simulation interrupted.", e);
            }

            if (fullTimeoutFailure) {
                if (log.isDebugEnabled())
                    log.debug("Firing a timeout exception after " + timeout + "ms on " + sock);

                throw new SocketTimeoutException("Simulated timeout");
            }

            if (log.isDebugEnabled())
                log.debug("Firing a timeout exception after " + timeout + "ms on " + sock);

            throw new SocketTimeoutException("Simulated timeout");
        }
    }
}
