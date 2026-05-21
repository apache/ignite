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
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class MultiDataCenterSplitTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** */
    private static final String DC_ID_2 = "DC2";

    /** */
    private Supplier<TcpDiscoverySpi> discoSpiSupplier;

    /** Log listener. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** Datacenters number. */
    @Parameterized.Parameter()
    public int dcCnt;

    /** Nodes number per DC. */
    @Parameterized.Parameter(1)
    public int srvrsPerDc;

    /** The ping pool size. */
    @Parameterized.Parameter(2)
    public int pingPoolSize;

    /** If wait for full timeout before failure simulation. */
    @Parameterized.Parameter(3)
    public boolean fullTimeoutFailure;

    /** */
    @Parameterized.Parameters(name = "dcCnt={0}, serversPerDc={1}, pingPoolSize={2}, fullTimeoutFailure={3}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            F.asList(2, 3), // DCs cnt.
            F.asList(2, 3, 4), // Servers number per DC.
            F.asList(1, 2, TcpDiscoverySpi.DFLT_RMT_DC_PING_POOL_SIZE), // Ping pool size.
            F.asList(true, false) // Full-timeout failure (or fail quickly).
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        listeningLog.clearListeners();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Setup DiscoverySPI.
        TcpDiscoverySpi discoSpi = discoSpiSupplier.get();
        discoSpi.setIpFinder(LOCAL_IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        // Disable unnesessary discovery messages.
        cfg.setMetricsUpdateFrequency(getTestTimeout() * 3);
        cfg.setClientFailureDetectionTimeout(cfg.getMetricsUpdateFrequency());

        // To block nodes traffic we rely on exact ports.
        assert ((TcpDiscoverySpi)cfg.getDiscoverySpi()).locPort == DFLT_PORT;

        // Fastens the tests.
        cfg.setFailureDetectionTimeout(5000);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** */
    @Test
    public void testConnectionRecoveryWithEntireDCFailure() throws Exception {
        // Fastens the tests. Also reduces number of flaky tests. JVM/GC pauses can change or disrupt the supposed
        // connection recovery strategy. We should avoid to short timeouts.
        assumeTrue(pingPoolSize <= srvrsPerDc && srvrsPerDc / pingPoolSize <= 2);

        startDCs(dcCnt);

        // Ensure the ports order.
        for (int g = 0; g < srvrsPerDc * dcCnt; ++g)
            assertEquals(discoSpi(grid(g)).locNode.discoveryPort(), DFLT_PORT + g);

        // Register the log listeners.
        // There is 2 close-ring-to-local-DC scenarios: 1 - remote DC is completely pinged and doesn't answer enough
        // in some time before the connection recovery timeout and before corner node gets segmented; 2 - corner node
        // is able to traverse entire remote DC in the connection recovery timeout;
        LogListener logStartPing = LogListener.matches("Parallel ping of nodes in remote DCs is starting. " +
            "Number of nodes to ping: " + srvrsPerDc * (dcCnt - 1)).times(2).build();

        LogListener logSplit0 = LogListener.matches("No node of the following remote DCs responded. Considering DCs '"
            + DC_ID_1 + "' as unavailable").times(1).build();

        LogListener logSplit1 = LogListener.matches("No node of the following remote DCs responded. Considering DCs '"
            + DC_ID_0 + ", " + DC_ID_2 + "' as unavailable").times(1).build();

        LogListener logSplit2 = LogListener.matches("During the connection recovery, all remote DCs have been traversed, "
            + "none available.").build();

        listeningLog.registerAllListeners(logStartPing, logSplit0, logSplit1, logSplit2);

        if (log.isInfoEnabled())
            log.info("Splitting the datacenters...");

        // Check the DCs and break connections between them.
        for (ClusterNode n : grid(0).cluster().nodes())
            discoSpi(G.ignite(n.id())).block = true;

        long checkTimeout = grid(0).configuration().getFailureDetectionTimeout() * 3;

        checkDcSplited(DC_ID_1, null, checkTimeout);

        if (dcCnt == 2)
            checkDcSplited(DC_ID_0, null, checkTimeout);
        else {
            checkDcSplited(DC_ID_0, DC_ID_2, checkTimeout);
            checkDcSplited(DC_ID_2, DC_ID_0, checkTimeout);
        }

        if (log.isInfoEnabled())
            log.info("Waiting for the ping log...");

        // Now we check the logs.
        assertTrue(logStartPing.check(checkTimeout));

        CountDownLatch logLatch = new CountDownLatch(1);

        runAsync(() -> {
            if (logSplit0.check(checkTimeout))
                logLatch.countDown();
        });
        runAsync(() -> {
            if (logSplit1.check(checkTimeout))
                logLatch.countDown();
        });
        runAsync(() -> {
            if (logSplit2.check(checkTimeout))
                logLatch.countDown();
        });

        assertTrue(logLatch.await(checkTimeout, TimeUnit.MILLISECONDS));
    }

    /** */
    private void startDCs(int cnt) throws Exception {
        assert cnt == 2 || cnt == 3;

        if (cnt == 2) {
            // Start DC0. It misses connection to DC1.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1);

            startGrids(srvrsPerDc);

            // Start DC1. It misses connection to DC0.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT, DFLT_PORT + srvrsPerDc - 1);

            for (int g = srvrsPerDc; g < srvrsPerDc << 1; ++g)
                startGrid(g);
        }
        else {
            // Start DC0. It misses connection to DC1.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1);

            startGrids(srvrsPerDc);

            // Start DC1. It misses connection to DC0 and DC2.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT, DFLT_PORT + srvrsPerDc * 3 - 1,
                DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1);

            for (int g = srvrsPerDc; g < srvrsPerDc * 2; ++g)
                startGrid(g);

            // Start DC2. It misses connection to DC1.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_2);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1);

            for (int g = srvrsPerDc * 2; g < srvrsPerDc * 3; ++g)
                startGrid(g);

            for (int ig = srvrsPerDc; ig < srvrsPerDc * 2; ++ig)
                discoSpi(grid(ig)).block = true;
        }
    }

    /** Creates the test Discovery SPI. */
    private TcpDiscoverySpi testDiscovery(int portFrom, int portTo) {
        assert portTo >= portFrom;

        return new TestTcpDiscoverySpi(IntStream.range(portFrom, portTo + 1).boxed().collect(Collectors.toSet()),
            fullTimeoutFailure, pingPoolSize);
    }

    /** Creates the test Discovery SPI. */
    private TcpDiscoverySpi testDiscovery(int allPortsFrom, int allPortsTo, int workPortFrom, int workPortTo) {
        assert allPortsTo >= allPortsFrom;
        assert workPortFrom >= allPortsFrom;
        assert workPortTo <= allPortsTo;

        Set<Integer> failedPorts = IntStream.range(allPortsFrom, allPortsTo + 1).filter(p -> p < workPortFrom || p > workPortTo)
            .boxed().collect(Collectors.toSet());

        return new TestTcpDiscoverySpi(failedPorts, fullTimeoutFailure, pingPoolSize);
    }

    /** Check whether datacenter {@code dcId} is separated. If {@code otherAliveDc} is not {@code null}, these DCs are expected joined. */
    private void checkDcSplited(String dcId, @Nullable String otherAliveDc, long timeout) throws IgniteInterruptedCheckedException {
        assert !dcId.equals(otherAliveDc);

        if (log.isInfoEnabled())
            log.info("Awaiting for DC is splitted, DC id: " + dcId + '.');

        assertTrue(waitForCondition(() -> {
            for (Ignite grid : G.allGrids()) {
                if (!grid.cluster().localNode().dataCenterId().equals(dcId))
                    continue;

                if (grid.cluster().nodes().size() != srvrsPerDc * (otherAliveDc == null ? 1 : 2))
                    return false;

                int curDcCnt = 0;
                int otherAliveCnt = 0;

                for (ClusterNode n : grid.cluster().nodes()) {
                    if (n.dataCenterId().equals(dcId))
                        ++curDcCnt;

                    if (otherAliveDc != null && n.dataCenterId().equals(otherAliveDc))
                        ++otherAliveCnt;
                }

                if (curDcCnt != srvrsPerDc || (otherAliveDc != null && otherAliveCnt != srvrsPerDc))
                    return false;
            }

            return true;
        }, timeout, 500));
    }

    /** */
    private static TestTcpDiscoverySpi discoSpi(Ignite node) {
        return (TestTcpDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    /** */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final Collection<Integer> failedPorts;

        /** */
        private final int pingPoolSize;

        /** */
        private final boolean fullTimeoutFailure;

        /** */
        private volatile boolean block;

        /** */
        private TestTcpDiscoverySpi(
            Collection<Integer> failedPorts,
            boolean fullTimeoutFailure,
            int pingPoolSize
        ) {
            this.failedPorts = failedPorts;
            this.fullTimeoutFailure = fullTimeoutFailure;
            this.pingPoolSize = pingPoolSize;
        }

        /** {@inheritDoc} */
        @Override protected void initializeImpl() {
            if (impl != null)
                return;

            super.initializeImpl();

            // In theory, might be a ClientImpl.
            if (impl instanceof ServerImpl)
                impl = new ServerImpl(this, DFLT_UTLITY_POOL_SIZE, pingPoolSize);
        }

        /** {@inheritDoc} */
        @Override protected void writeMessage(TcpDiscoveryIoSession ses, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            tryToBlock(ses.socket(), null, timeout);

            super.writeMessage(ses, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, @Nullable TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException, IgniteCheckedException {
            tryToBlock(sock, data, timeout);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** */
        private void tryToBlock(
            Socket sock,
            @Nullable byte[] data,
            long timeout
        ) throws IOException {
            if (!block)
                return;

            int rmpPort = ((InetSocketAddress)sock.getRemoteSocketAddress()).getPort();

            if (!failedPorts.contains(rmpPort))
                return;

            if (data != null && Arrays.equals(U.IGNITE_HEADER, data))
                return;

            if (log.isDebugEnabled())
                log.debug("Simulation network delay of " + (fullTimeoutFailure ? timeout : 5) + "ms on " + sock);

            try {
                U.sleep(fullTimeoutFailure ? timeout : 5);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IOException("Network delay simulation interrupted.", e);
            }

            throw new SocketTimeoutException("Simulated timeout.");
        }
    }
}
