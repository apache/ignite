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
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.GridNioServerWrapper;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.checkerframework.checker.units.qual.C;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

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
    private final GridConcurrentHashSet<Integer> segmentedNodes = new GridConcurrentHashSet<>();

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

        cfg.setDiscoverySpi(spi);

        cfg.setIncludeEventTypes(EVT_NODE_SEGMENTED);

        cfg.setSystemWorkerBlockedTimeout(10_000);

        cfg.setMetricsUpdateFrequency(getTestTimeout());
        cfg.setClientFailureDetectionTimeout(cfg.getMetricsUpdateFrequency());

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

        AtomicBoolean netBroken = new AtomicBoolean(false);

        IgniteEx ig0 = startGrid(NODE_0_NAME);

        IgniteEx ig1 = startGrid(NODE_1_NAME);

        specialSpi = new TcpDiscoverySpi() {
            @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
                if (netBroken.get() && sock.getPort() == NODE_3_PORT)
                    throw new SocketTimeoutException("Read timed out");

                return super.readReceipt(sock, timeout);
            }

            @Override protected Socket openSocket(InetSocketAddress sockAddr,
                IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
                if (netBroken.get() && sockAddr.getPort() == NODE_4_PORT)
                    throw new SocketTimeoutException("connect timed out");

                return super.openSocket(sockAddr, timeoutHelper);
            }
        };

        Ignite ig2 = startGrid(NODE_2_NAME);

        AtomicBoolean illNodeSegmented = new AtomicBoolean(false);

        ig2.events().localListen((e) -> {
            illNodeSegmented.set(true);

            return false;
        }, EVT_NODE_SEGMENTED);

        specialSpi = null;

        startGrid(NODE_3_NAME);

        startGrid(NODE_4_NAME);

        startGrid(NODE_5_NAME);

        breakDiscoConnectionToNext(ig1);

        netBroken.set(true);

        waitForCondition(illNodeSegmented::get, 10_000);

        assertTrue(illNodeSegmented.get());

        Map failedNodes = getFailedNodesCollection(ig0);

        assertTrue(String.format("Failed nodes is expected to be empty, but contains %s nodes.", failedNodes.size()),
            failedNodes.isEmpty());
    }

    /** */
    @Test
    public void testBackwardLocalHostCheck() throws Exception {
//        Collection<InetAddress> ip4addrs = F.flatCollections(F.viewReadOnly(Collections.list(NetworkInterface.getNetworkInterfaces()),
//            nf -> F.viewReadOnly(Collections.list(nf.getInetAddresses()), a -> a, a -> a instanceof Inet4Address)));
        failureDetectionTimeout = 3000;


        specialSpi = new TestDiscoverySpi(false);

        Ignite ig0 = startGrid(0);

        specialSpi = new TestDiscoverySpi(true);

        Ignite ig1 = startGrid(1);

        specialSpi = new TestDiscoverySpi(false);

        Ignite ig2 = startGrid(2);

        CountDownLatch handshakeToNode2 = new CountDownLatch(1);

        spi(ig0).onNextHandshake(hs->{
            spi(ig2).simulatePrevNodeAddress(new SocketAddress());

            handshakeToNode2.countDown();
        });

        // Blocking node 1.
        spi(ig1).block.set(true);
        spi(ig0).block.set(true);
        assertTrue(waitForCondition(()->!spi(ig1).blocked.isEmpty(), ig0.configuration().getFailureDetectionTimeout() * 2));
        assertTrue(waitForCondition(()->!spi(ig0).blocked.isEmpty(), ig0.configuration().getFailureDetectionTimeout() * 2));

        // Node 0 tries to connect to node 2 and asks if node 1 is alive.
        assertTrue(handshakeToNode2.await(getTestTimeout(), TimeUnit.MILLISECONDS));


        Thread.sleep(10_000);
    }

    /**
     * Ensures sequential failure of two nodes has no additional issues.
     */
    @Test
    public void testSequentialFailTwoNodes() throws Exception {
        simulateFailureOfTwoNodes(true);
    }

    /**
     * Ensures sequential failure of two nodes has no additional issues.
     */
    @Test
    public void testNotSequentialFailTwoNodes() throws Exception {
        simulateFailureOfTwoNodes(false);
    }

    /** */
    private void simulateFailureOfTwoNodes(boolean sequentionally) throws Exception {
        failureDetectionTimeout = 1000;

        int gridCnt = 7;

        startGrids(gridCnt);

        awaitPartitionMapExchange();

        final CountDownLatch failLatch = new CountDownLatch(2);

        for (int i = 0; i < gridCnt; i++) {
            ignite(i).events().localListen(evt -> {
                failLatch.countDown();

                return true;
            }, EVT_NODE_FAILED);

            int nodeIdx = i;

            ignite(i).events().localListen(evt -> {
                segmentedNodes.add(nodeIdx);

                return true;
            }, EVT_NODE_SEGMENTED);
        }

        Set<Integer> failedNodes = new HashSet<>();

        failedNodes.add(2);

        if (sequentionally)
            failedNodes.add(3);
        else
            failedNodes.add(4);

        failedNodes.forEach(idx -> processNetworkThreads(ignite(idx), Thread::suspend));

        try {
            failLatch.await(10, TimeUnit.SECONDS);
        }
        finally {
            failedNodes.forEach(idx -> processNetworkThreads(ignite(idx), Thread::resume));
        }

        for (int i = 0; i < gridCnt; i++) {
            if (!failedNodes.contains(i))
                assertFalse(segmentedNodes.contains(i));
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

    /**
     * Simulates network failure on certain node.
     */
    private void processNetworkThreads(Ignite ignite, Consumer<Thread> proc) {
        DiscoverySpi disco = ignite.configuration().getDiscoverySpi();

        ServerImpl serverImpl = U.field(disco, "impl");

        for (Thread thread : serverImpl.threads())
            proc.accept(thread);

        CommunicationSpi<?> comm = ignite.configuration().getCommunicationSpi();

        GridNioServerWrapper nioServerWrapper = U.field(comm, "nioSrvWrapper");

        for (GridWorker worker : nioServerWrapper.nio().workers())
            proc.accept(worker.runner());
    }

    /** */
    private static TestDiscoverySpi spi(Ignite ig) {
        return ((TestDiscoverySpi)ig.configuration().getDiscoverySpi());
    }

    /** */
    private static final class TestDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final List<TcpDiscoveryAbstractMessage> blocked = new CopyOnWriteArrayList<>();

        private final boolean blockAll;

        private final AtomicBoolean block = new AtomicBoolean();
        private final CountDownLatch blockLatch = new CountDownLatch(1);

        private final AtomicReference<Consumer<TcpDiscoveryHandshakeRequest>> handshekeConsumer = new AtomicReference();

        private TestDiscoverySpi(boolean blockAll) {
            this.blockAll = blockAll;
        }

        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            delay(timeout);

            super.writeToSocket(msg, sock, res, timeout);
        }

        private void delay(long timeout) {
            if (block.get()) {
                try {
                    blockLatch.await(timeout, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        }

        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            Consumer<TcpDiscoveryHandshakeRequest> cnsmr;

            if (msg instanceof TcpDiscoveryHandshakeRequest && (cnsmr = handshekeConsumer.getAndSet(null)) != null)
                cnsmr.accept((TcpDiscoveryHandshakeRequest)msg);

            delay(timeout);

            super.writeToSocket(sock, out, msg, timeout);
        }

        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            delay(timeout);

            super.writeToSocket(sock, msg, data, timeout);
        }

        private boolean doBlock(TcpDiscoveryAbstractMessage msg, Socket to) {
            if (block.get()) {
                blocked.add(msg);

                return true;
            }

            return false;
        }

        public void release() {
            block.set(false);
        }

        public void block() {
            block.set(true);
        }

        public void onNextHandshake(Consumer<TcpDiscoveryHandshakeRequest> cnsmr) {
            assert handshekeConsumer.get() == null;

            handshekeConsumer.set(cnsmr);
        }

        public void simulatePrevNodeAddress(SocketAddress address) {

        }
    }
}
