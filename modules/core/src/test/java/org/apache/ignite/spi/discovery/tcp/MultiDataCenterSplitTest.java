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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
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

    /** */
    @Parameterized.Parameter()
    public int srvrsPerDc;

    /** */
    @Parameterized.Parameter(1)
    public boolean fullTimeoutFailure;

    /** */
    @Parameterized.Parameter(2)
    public boolean rmtDcNodesRespond;

    /** */
    @Parameterized.Parameter(3)
    public int pingPoolSize;

    @Parameterized.Parameter(4)
    public int dcCnt;

    /** */
    @Parameterized.Parameters(name = "serversPerDc={0}, fullTimeoutFailure={1}, rmtDcNodesResponds={2}, pingPoolSize={3}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            F.asList(2, 3, 4), // Servers number per DC.
            F.asList(true, false), // Full-timeout failure (or fail quickly).
            F.asList(false, true), // Whether few nodes of the remote DC respond to the ping.
            F.asList(1, 2, TcpDiscoverySpi.DFLT_RMT_DC_PING_POOL_SIZE), // Ping pool size.
            F.asList(3) // Servers cnt.
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
        if (rmtDcNodesRespond)
            assumeTrue(srvrsPerDc > 2);

        // Fastens the tests. Also reduces number of flaky tests. JVM/GC pauses can change or disrupt the supposed
        // connection recovery strategy. We should avoid to short timeouts.
        assumeTrue(pingPoolSize <= srvrsPerDc && srvrsPerDc / pingPoolSize <= 2);

        startDCs(dcCnt);

        for (int g = 0; g < srvrsPerDc * dcCnt; ++g) {
            assertEquals(discoSpi(grid(g)).locNode.order(), g + 1);
            assertEquals(discoSpi(grid(g)).locNode.discoveryPort(), DFLT_PORT + g);
        }

        // Register the log listeners.
        // There is 2 close-ring-to-local-DC scenarios: 1 - remote DC is completely pinged and doesn't answer enough
        // in some time before the connection recovery timeout and before corner node gets segmented; 2 - corner node
        // is able to traverse entire remote DC in the connection recovery timeout;
        LogListener logLsnr0 = LogListener.matches("During the connection recovery, starting ping of the remote DCs. " +
            "Nodes number to ping: " + srvrsPerDc * (dcCnt - 1)).times(2).build();

        LogListener logLsnr10 = LogListener.matches("Half or less of the following remote DCs responded. " +
            "Considering DCs '" + DC_ID_1 + "' unavailable").times(1).build();

        LogListener logLsnr11;
        // The 'Responded nodes' log depends of successful pings.
        if (rmtDcNodesRespond) {
            logLsnr11 = LogListener.matches("During the connection recovery, nodes ping of DCs '" + DC_ID_1
                    + "' from current corner node has finished. Responded nodes: [").times(1).andMatches("Unavailable nodes: []")
                .times(0).build();
        }
        else {
            logLsnr11 = LogListener.matches("During the connection recovery, nodes ping of DCs '" + DC_ID_1
                + "' from current corner node has finished. Responded nodes: []").times(1).build();
        }

        // Corner node is able to traverse entire remote DC in the connection recovery timeout
        LogListener logLsnr2 = LogListener.matches("During the connection recovery, all the remote DCs have been traversed. " +
            "Failed to connect to any.").times(2).build();

        listeningLog.registerAllListeners(logLsnr0, logLsnr10, logLsnr11, logLsnr2);

        if (log.isInfoEnabled())
            log.info("Splitting the datacenters...");

        // Check the DCs and break connections between them.
        for (ClusterNode n : grid(0).cluster().nodes())
            discoSpi(G.ignite(n.id())).block = true;

        checkDcSplited(DC_ID_1, null);

        if (dcCnt == 2)
            checkDcSplited(DC_ID_0, null);
        else {
            checkDcSplited(DC_ID_0, DC_ID_2);
            checkDcSplited(DC_ID_2, DC_ID_0);
        }

        // The connection recovery should take <= failureDetectionTimeout * 2.
        long testTimeout = grid(0).configuration().getFailureDetectionTimeout() * 2;

        if (log.isInfoEnabled())
            log.info("Waiting for the ping log...");

        // Now we check the logs.
        assertTrue(logLsnr0.check(testTimeout));

        AtomicInteger logCntr = new AtomicInteger();

        runAsync(() -> {
            if (logLsnr10.check(testTimeout) && logLsnr11.check(testTimeout))
                logCntr.incrementAndGet();
        });
        runAsync(() -> {
            if (logLsnr2.check(testTimeout))
                logCntr.incrementAndGet();
        });

        if (log.isInfoEnabled())
            log.info("Waiting for the rest of the logs...");

        Thread.sleep(testTimeout);

        // Only one of the variants is expected.
        assertTrue(logCntr.get() == 1);
    }

    /** */
    private void startDCs(int cnt) throws Exception {
        assert cnt == 2 || cnt == 3;

        if (cnt == 2) {
            // Start DC0. It misses connection to DC1.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1, rmtDcNodesRespond);

            startGrids(srvrsPerDc);

            // Start DC1. It misses connection to DC0.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT, DFLT_PORT + srvrsPerDc - 1, rmtDcNodesRespond);

            for (int g = srvrsPerDc; g < srvrsPerDc << 1; ++g)
                startGrid(g);
        }
        else {
            // Start DC0. It misses connection to DC1.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1, rmtDcNodesRespond);

            startGrids(srvrsPerDc);

            // Start DC1. It misses connection to DC0 and DC2.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT, DFLT_PORT + srvrsPerDc * 3 - 1,
                DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1, rmtDcNodesRespond);

            for (int g = srvrsPerDc; g < srvrsPerDc * 2; ++g)
                startGrid(g);

            // Start DC2. It misses connection to DC1.
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_2);
            discoSpiSupplier = () -> testDiscovery(DFLT_PORT + srvrsPerDc, DFLT_PORT + srvrsPerDc * 2 - 1, rmtDcNodesRespond);

            for (int g = srvrsPerDc * 2; g < srvrsPerDc * 3; ++g)
                startGrid(g);

            for (int ig = srvrsPerDc; ig < srvrsPerDc * 2; ++ig)
                discoSpi(grid(ig)).block = true;
        }
    }

    /** */
    @Test
    public void testWith3DC() throws Exception {


        LogListener logLsnr0 = LogListener.matches("During the connection recovery, starting ping of the remote DCs. " +
            "Nodes number to ping: " + srvrsPerDc).times(2).build();
    }

    /** Creates the test Discovery SPI. */
    private TcpDiscoverySpi testDiscovery(int portFrom, int portTo, boolean someRemoteDcNodesRespond) {
        assert portTo >= portFrom;

        Set<Integer> portPingExceptions = pingPortExceptions(someRemoteDcNodesRespond, portFrom, portTo);

        return new TestTcpDiscoverySpi(IntStream.range(portFrom, portTo + 1).boxed().collect(Collectors.toSet()),
            fullTimeoutFailure, portPingExceptions, pingPoolSize);
    }

    /** Creates the test Discovery SPI. */
    private TcpDiscoverySpi testDiscovery(int allPortsFrom, int allPortsTo, int workPortFrom, int workPortTo, boolean someRemoteDcNodesRespond) {
        assert allPortsTo >= allPortsFrom;
        assert workPortFrom >= allPortsFrom;
        assert workPortTo <= allPortsTo;

        Set<Integer> failedPorts = IntStream.range(allPortsFrom, allPortsTo + 1).filter(p -> p < workPortFrom || p > workPortTo)
            .boxed().collect(Collectors.toSet());

        Set<Integer> portPingExceptions0 = pingPortExceptions(someRemoteDcNodesRespond, allPortsFrom, workPortFrom - 1);
        Set<Integer> portPingExceptions = pingPortExceptions(someRemoteDcNodesRespond, workPortTo + 1, allPortsTo);

        portPingExceptions.addAll(portPingExceptions0);

        return new TestTcpDiscoverySpi(failedPorts, fullTimeoutFailure, portPingExceptions, pingPoolSize);
    }

    /** */
    private Set<Integer> pingPortExceptions(boolean someRemoteDcNodesRespond, int portFrom, int portTo) {
        if (!someRemoteDcNodesRespond)
            return Collections.emptySet();

        List<Integer> list = IntStream.range(portFrom, portTo + 1).boxed().collect(Collectors.toList());

        Random rnd = new Random();

        while (list.size() > srvrsPerDc / 2)
            list.remove(rnd.nextInt(list.size()));

        return new HashSet<>(list);
    }

    /** Check whether datacenter {@code dcId} is separated. If {@code otherAliveDc} is not {@code null}, these DCs are expected joined. */
    private void checkDcSplited(String dcId, @Nullable String otherAliveDc) throws IgniteInterruptedCheckedException {
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
        }, getTestTimeout(), 500));
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
        private final Collection<Integer> portPingExceptions;

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
            Collection<Integer> portPingExceptions,
            int pingPoolSize
        ) {
            this.failedPorts = failedPorts;
            this.fullTimeoutFailure = fullTimeoutFailure;
            this.portPingExceptions = portPingExceptions;
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
            tryToBlock(ses.socket(), msg, null, timeout);

            super.writeMessage(ses, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, @Nullable TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException, IgniteCheckedException {
            tryToBlock(sock, msg, data, timeout);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** */
        private void tryToBlock(
            Socket sock,
            @Nullable TcpDiscoveryAbstractMessage msg,
            @Nullable byte[] data,
            long timeout
        ) throws IOException {
            if (!block)
                return;

            int rmpPort = ((InetSocketAddress)sock.getRemoteSocketAddress()).getPort();

            if (!failedPorts.contains(rmpPort))
                return;

            if (portPingExceptions.contains(rmpPort) && ((msg instanceof TcpDiscoveryPingRequest)
                || (data != null && Arrays.equals(U.IGNITE_HEADER, data))))
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
