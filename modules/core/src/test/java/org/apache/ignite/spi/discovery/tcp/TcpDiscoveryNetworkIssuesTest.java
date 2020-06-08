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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class TcpDiscoveryNetworkIssuesTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_0_PORT = 47500;

    /** */
    private static final int NODE_1_PORT = 47501;

    /** */
    private static final int NODE_2_PORT = 47502;

    /** */
    private static final int NODE_3_PORT = 47503;

    /** */
    private static final int NODE_4_PORT = 47504;

    /** */
    private static final int NODE_5_PORT = 47505;

    /** */
    private static final String NODE_0_NAME = "node00-" + NODE_0_PORT;

    /** */
    private static final String NODE_1_NAME = "node01-" + NODE_1_PORT;

    /** */
    private static final String NODE_2_NAME = "node02-" + NODE_2_PORT;

    /** */
    private static final String NODE_3_NAME = "node03-" + NODE_3_PORT;

    /** */
    private static final String NODE_4_NAME = "node04-" + NODE_4_PORT;

    /** */
    private static final String NODE_5_NAME = "node05-" + NODE_5_PORT;

    /** */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private TcpDiscoverySpi specialSpi;

    /** */
    private boolean usePortFromNodeName;

    /** */
    private int connectionRecoveryTimeout = -1;

    /** */
    private int failureDetectionTimeout = 2_000;

    /** */
    private int metricsUpdateFreq = 1_000;

    /** */
    private Long systemWorkerBlockedTimeout;

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = (specialSpi != null) ? specialSpi : new TcpDiscoverySpi();

        if (usePortFromNodeName)
            spi.setLocalPort(Integer.parseInt(igniteInstanceName.split("-")[1]));

        spi.setIpFinder(ipFinder);

        if (connectionRecoveryTimeout >= 0)
            spi.setConnectionRecoveryTimeout(connectionRecoveryTimeout);

        cfg.setFailureDetectionTimeout(failureDetectionTimeout);

        cfg.setMetricsUpdateFrequency(metricsUpdateFreq);

        cfg.setSystemWorkerBlockedTimeout(systemWorkerBlockedTimeout);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * Test scenario: some node (lets call it IllN) in the middle experience network issues: its previous cannot see it,
     * and the node cannot see two nodes in front of it.
     *
     * IllN is considered failed by othen nodes in topology but IllN manages to connect to topology and
     * sends StatusCheckMessage with non-empty failedNodes collection.
     *
     * Expected outcome: IllN eventually segments from topology, other healthy nodes work normally.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-11364">IGNITE-11364</a>
     * for more details about actual bug.
     */
    @Test
    public void testServerGetsSegmentedOnBecomeDangling() throws Exception {
        usePortFromNodeName = true;
        connectionRecoveryTimeout = 0;

        AtomicBoolean networkBroken = new AtomicBoolean(false);

        IgniteEx ig0 = startGrid(NODE_0_NAME);

        IgniteEx ig1 = startGrid(NODE_1_NAME);

        specialSpi = new TcpDiscoverySpi() {
            @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
                if (networkBroken.get() && sock.getPort() == NODE_3_PORT)
                    throw new SocketTimeoutException("Read timed out");

                return super.readReceipt(sock, timeout);
            }

            @Override protected Socket openSocket(InetSocketAddress sockAddr,
                IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
                if (networkBroken.get() && sockAddr.getPort() == NODE_4_PORT)
                    throw new SocketTimeoutException("connect timed out");

                return super.openSocket(sockAddr, timeoutHelper);
            }
        };

        Ignite ig2 = startGrid(NODE_2_NAME);

        AtomicBoolean illNodeSegmented = new AtomicBoolean(false);

        ig2.events().localListen((e) -> {
            illNodeSegmented.set(true);

            return false;
            }, EventType.EVT_NODE_SEGMENTED);

        specialSpi = null;

        startGrid(NODE_3_NAME);

        startGrid(NODE_4_NAME);

        startGrid(NODE_5_NAME);

        breakDiscoConnectionToNext(ig1);

        networkBroken.set(true);

        GridTestUtils.waitForCondition(illNodeSegmented::get, 10_000);

        assertTrue(illNodeSegmented.get());

        Map failedNodes = getFailedNodesCollection(ig0);

        assertTrue(String.format("Failed nodes is expected to be empty, but contains %s nodes.", failedNodes.size()),
            failedNodes.isEmpty());
    }

    /** Checks node get failed, segmented within failureDetectionTimeout + connectionRecoveryTimeout. */
    @Test
    public void testConnectionRecoveryTimeout() throws Exception {
        // Avoid useless warnings. We do block threads specially.
        systemWorkerBlockedTimeout = 5_000L;

        for (failureDetectionTimeout = 200; failureDetectionTimeout <= 200; failureDetectionTimeout += 200) {
            for (connectionRecoveryTimeout = 200; connectionRecoveryTimeout <= 200; connectionRecoveryTimeout += 200) {
                AtomicLong timer = new AtomicLong();

                startGrids(2);

                specialSpi = new TcpDiscoverySpi() {
                    /** {@inheritDoc} */
                    @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
                        if (timer.get() > 0)
                            simulateUnknowdDelay(timeout);

                        return super.readReceipt(sock, timeout);
                    }

                    /** {@inheritDoc} */
                    @Override protected Socket openSocket(InetSocketAddress sockAddr,
                        IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {

                        if (timer.get() > 0)
                            simulateUnknowdDelay(timeoutHelper.nextTimeoutChunk(getSocketTimeout()));

                        return super.openSocket(sockAddr, timeoutHelper);
                    }

                    /** {@inheritDoc} */
                    @Override protected void writeToSocket(Socket sock, OutputStream out,
                        TcpDiscoveryAbstractMessage msg,
                        long timeout) throws IOException, IgniteCheckedException {

                        if (timer.get() > 0) {
                            simulateUnknowdDelay(timeout);

                            super.writeToSocket(sock, out, msg, 0);
                        }
                        else
                            super.writeToSocket(sock, out, msg, timeout);
                    }

                    /** {@inheritDoc} */
                    @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
                        long timeout) throws IOException {
                        if (timer.get() > 0) {
                            simulateUnknowdDelay(timeout);

                            super.writeToSocket(msg, sock, res, 0);
                        }
                        else
                            super.writeToSocket(msg, sock, res, timeout);
                    }

                    /** {@inheritDoc} */
                    @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
                        long timeout) throws IOException {
                        if (timer.get() > 0) {
                            simulateUnknowdDelay(timeout);

                            super.writeToSocket(sock, msg, data, 0);
                        }
                        else
                            super.writeToSocket(sock, msg, data, timeout);
                    }

                    private void simulateUnknowdDelay(long delay) {
                        try {
                            Thread.sleep(delay);
                        }
                        catch (InterruptedException e) {
                            // No-op.
                        }
                    }
                };

                IgniteEx failedGrid = startGrid(2);

                failedGrid.events().localListen((e) -> {
                    if (e.node().isLocal()) {
                        timer.set(System.nanoTime() - timer.get());

                        log.error("Wrong node failed: " + e.node().order() + ". Expected: " +
                            failedGrid.localNode().order());
                    }
                    else
                        timer.set(-1);

                    synchronized (timer) {
                        timer.notifyAll();
                    }

                    return false;
                }, EventType.EVT_NODE_SEGMENTED, EventType.EVT_NODE_FAILED);

                specialSpi = null;

                startGrid(3);

                startGrid(4);

                synchronized (timer) {
                    timer.set(System.nanoTime());

                    timer.wait(getTestTimeout());
                }

                if (timer.get() < 0)
                    fail("Wrong node failed. See the log above.");
                else {
                    timer.set(U.nanosToMillis(timer.get()));

                    long expectedDelay = failureDetectionTimeout + connectionRecoveryTimeout;

                    // Give additional 100ms on GC, timer granulation and platform delays.
                    assertTrue("Too long delay of connection recovery failure: " + timer.get() + ". Expected: " +
                            expectedDelay + ". failureDetectionTimeout: " + failureDetectionTimeout +
                            ", connectionRecoveryTimeout: " + connectionRecoveryTimeout,
                        timer.get() < expectedDelay + 100);
                }

                stopAllGrids(true);
            }
        }
    }

    /**
     * @param ig Ignite instance to get failedNodes collection from.
     */
    private Map getFailedNodesCollection(IgniteEx ig) {
        GridDiscoveryManager disco = ig.context().discovery();

        Object spis = GridTestUtils.getFieldValue(disco, GridManagerAdapter.class, "spis");

        return GridTestUtils.getFieldValue(((Object[])spis)[0], "impl", "failedNodes");
    }

    /**
     * Breaks connectivity of passed server node to its next to simulate network failure.
     *
     * @param ig Ignite instance which connection to next node has to be broken.
     */
    private void breakDiscoConnectionToNext(IgniteEx ig) throws Exception {
        GridDiscoveryManager disco = ig.context().discovery();

        Object spis = GridTestUtils.getFieldValue(disco, GridManagerAdapter.class, "spis");

        OutputStream out = GridTestUtils.getFieldValue(((Object[])spis)[0], "impl", "msgWorker", "out");

        out.close();
    }
}
