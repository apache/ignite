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
    private Supplier<TcpDiscoverySpi> discoSpiSupplier;

    /** Log listener. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    @Parameterized.Parameter()
    public int serversPerDc;

    /** */
    @Parameterized.Parameter(1)
    public boolean fullTimeoutFailure;

    /** */
    @Parameterized.Parameter(2)
    public boolean someRemoteDcNodesRespond;

    /** */
    @Parameterized.Parameters(name = "serversPerDc={0}, fullTimeoutFailure={1}, someRemoteDcNodesRespond={2}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            F.asList(2, 3, 4), // Servers number per DC.
            F.asList(true, false), // Full-timeout failure (or fail quickly).
            F.asList( false, true) // Whether some nodes or remote DC respond to the ping.
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

        // Disable unnesessary disco. messages.
        cfg.setMetricsUpdateFrequency(getTestTimeout() * 3);
        cfg.setClientFailureDetectionTimeout(cfg.getMetricsUpdateFrequency());

        assert ((TcpDiscoverySpi)cfg.getDiscoverySpi()).locPort == TcpDiscoverySpi.DFLT_PORT;

        // Fastens the tests.
        cfg.setFailureDetectionTimeout(3000L);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** */
    @Test
    public void testConnectionRecoveryWithEntireDCFailure() throws Exception {
        if (someRemoteDcNodesRespond)
            assumeTrue(serversPerDc > 2);

        // Start DC0.
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
        discoSpiSupplier = () -> testDiscovery(TcpDiscoverySpi.DFLT_PORT + serversPerDc,
            TcpDiscoverySpi.DFLT_PORT + serversPerDc * 2 - 1, someRemoteDcNodesRespond);

        startGridsMultiThreaded(serversPerDc);

        // Start DC1.
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
        discoSpiSupplier = () -> testDiscovery(TcpDiscoverySpi.DFLT_PORT, TcpDiscoverySpi.DFLT_PORT + serversPerDc - 1,
            someRemoteDcNodesRespond);

        startGridsMultiThreaded(serversPerDc, serversPerDc);

        // Register the log listeners.
        LogListener logLsnr0 = LogListener.matches("During the connection recovery process, starting ping "
            + "of DC '" + DC_ID_1 + "'. Nodes number to ping: " + serversPerDc).times(1).build();
        LogListener logLsnr1 = LogListener.matches("Few nodes or only half of the remote DC has responded. "
            + "Considering DC '" + DC_ID_1 + "' is unavailable.").times(1).build();
        LogListener logLsnr2 = LogListener.matches("During the connection recovery process, entire remote DC '"
            + DC_ID_1 + "' has been traversed. Failed to connect to any node.").times(1).build();

        LogListener logLsnr3;

        if (someRemoteDcNodesRespond) {
            logLsnr3 = LogListener.matches("During the connection recovery process, nodes ping of DC '" + DC_ID_1
                    + "' from current corner node has finished. Responded nodes: [").times(1)
                .andMatches("Responded nodes: []").times(0)
                .build();
        }
        else {
            logLsnr3 = LogListener.matches("During the connection recovery process, nodes ping of DC '" + DC_ID_1
                + "' from current corner node has finished. Responded nodes: []").times(1).build();
        }

        listeningLog.registerAllListeners(logLsnr0, logLsnr1, logLsnr2, logLsnr3);

        // Check the DCs and break connections between them.
        for (ClusterNode n : grid(0).cluster().nodes()) {
            assertTrue(n.dataCenterId().equals(n.order() <= serversPerDc ? DC_ID_0 : DC_ID_1));

            discoSpi(G.ignite(n.id())).block = true;
        }

        // We expect 2 separated rings.
        checkDcSplit(0, serversPerDc, DC_ID_0);
        checkDcSplit(serversPerDc, serversPerDc * 2, DC_ID_1);

        // Check the logs.
        long failureDetectionTimeout = grid(0).configuration().getFailureDetectionTimeout();

        assertTrue(logLsnr0.check(failureDetectionTimeout * 2));

        AtomicInteger logCntr = new AtomicInteger();

        runAsync(() -> {
            if (logLsnr1.check(failureDetectionTimeout * 2) && logLsnr3.check(failureDetectionTimeout * 2))
                logCntr.incrementAndGet();
        });
        runAsync(() -> {
            if (logLsnr2.check(failureDetectionTimeout * 2))
                logCntr.incrementAndGet();
        });

        Thread.sleep(failureDetectionTimeout * 2);

        // Only one log variant is expected.
        assertTrue(logCntr.get() == 1);
    }

    /** Creates test blocking discovery. */
    private TcpDiscoverySpi testDiscovery(int portFrom, int portTo, boolean someRemoteDcNodesRespond) {
        Set<Integer> portPingExceptions = pingPortExceptions(someRemoteDcNodesRespond, portFrom, portTo);

        return new TestTcpDiscoverySpi(portFrom, portTo, fullTimeoutFailure, portPingExceptions);
    }

    /** */
    private Set<Integer> pingPortExceptions(boolean someRemoteDcNodesRespond, int portFrom, int portTo) {
        if (!someRemoteDcNodesRespond)
            return Collections.emptySet();

        List<Integer> list = IntStream.range(portFrom, portTo + 1).boxed().collect(Collectors.toList());

        Random rnd = new Random();

        while (list.size() > serversPerDc / 2)
            list.remove(rnd.nextInt(list.size()));

        return new HashSet<>(list);
    }

    /** */
    private void checkDcSplit(int nodeIdxFrom, int nodeIdxTo, String dcId) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            for (int i = nodeIdxFrom; i < nodeIdxTo; ++i) {
                if (!grid(i).cluster().localNode().dataCenterId().equals(dcId))
                    return false;

                int cnt = 0;

                for (ClusterNode n : grid(i).cluster().nodes()) {
                    if (!n.dataCenterId().equals(dcId))
                        return false;

                    ++cnt;
                }

                if (cnt != serversPerDc)
                    return false;
            }

            return true;
        }, getTestTimeout()));
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
        private final Collection<Integer> portPingExceptions;

        /** */
        private final boolean fullTimeoutFailure;

        /** */
        private volatile boolean block;

        /** */
        private TestTcpDiscoverySpi(
            int minPortToBlockMsg,
            int maxPortToBlockMsg,
            boolean fullTimeoutFailure,
            Collection<Integer> portPingExceptions
        ) {
            this.minPortToBlockMsg = minPortToBlockMsg;
            this.maxPortToBlockMsg = maxPortToBlockMsg;
            this.fullTimeoutFailure = fullTimeoutFailure;
            this.portPingExceptions = portPingExceptions;
        }

        /** {@inheritDoc} */
        @Override protected void writeMessage(TcpDiscoveryIoSession ses, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            tryToBlock(ses.socket(), msg, null, timeout);

            super.writeMessage(ses, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, @Nullable TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
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

            if (rmpPort < minPortToBlockMsg || rmpPort > maxPortToBlockMsg)
                return;

            if (portPingExceptions.contains(rmpPort)) {
                if (msg instanceof TcpDiscoveryPingRequest)
                    return;

                if (!F.isEmpty(data) && Arrays.equals(data, U.IGNITE_HEADER))
                    return;
            }

            if (log.isDebugEnabled())
                log.debug("Simulation network delay of " + (fullTimeoutFailure ? timeout : 50) + "ms on " + sock);

            try {
                U.sleep(fullTimeoutFailure ? timeout : 50);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IOException("Network delay simulation interrupted.", e);
            }

            throw new SocketTimeoutException("Simulated timeout.");
        }
    }
}
