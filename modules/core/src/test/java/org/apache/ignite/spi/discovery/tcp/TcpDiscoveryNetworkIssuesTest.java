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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.GridNioServerWrapper;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
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
    private String localhost;

    /** */
    private IgniteLogger gridLog;

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

        cfg.setLocalHost(localhost);

        if (gridLog != null)
            cfg.setGridLogger(gridLog);

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

    /**
     * Tests backward ping of previous node if {@link TcpDiscoveryNode#socketAddresses()} contains same loopback address
     * as of local node. Assumes single localhost is set and single local address is resolved.
     */
    @Test
    public void testBackwardNodeCheckWithSameLoopbackSingleLocalAddress() throws Exception {
        doTestBackwardNodeCheckWithSameLoopback("127.0.0.1");
    }

    /**
     * Tests backward ping of previous node if {@link TcpDiscoveryNode#socketAddresses()} contains same loopback address
     * as of local node. Assumes {@link #getConfiguration(String)} localhost.
     */
    @Test
    public void testBackwardNodeCheckWithSameLoopbackSeveralLocalAddresses() throws Exception {
        doTestBackwardNodeCheckWithSameLoopback("0.0.0.0");
    }

    /**
     * Tests backward ping when the discovery threads of the malfunction node is simulated to hang at GC.
     * But the JVM is able to accept socket connections.
     */
    @Test
    public void testBackwardConnectionCheckWhenDiscoveryThreadsSuspended() throws Exception {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        gridLog = testLog;

        localhost = "127.0.0.1";

        failureDetectionTimeout = 3000;

        specialSpi = new TestDiscoverySpi();

        // This node suspects its next failed.
        Ignite doubtNode0 = startGrid(0);

        // Simulates frozen threads on node 1 but answering sockets. I.e. Socket#connect() works to node 1 but
        // reading anything with Socket#read() from it would fail with the timeout.
        specialSpi = new TestDiscoverySpi();

        // Node simulated 'frozen'. Can accept connections (socket accept) but won't write anything to a discovery socket.
        Ignite frozenNode1 = startGrid(1);

        UUID frozenNodeId = frozenNode1.cluster().localNode().id();

        specialSpi = new TestDiscoverySpi();

        setLoggerDebugLevel();

        // Node which does the backward connection check to its previous 'frozen'.
        Ignite pingingNode2 = startGrid(2);

        LogListener node1SegmentedLogLsnr = LogListener.matches("Local node SEGMENTED: TcpDiscoveryNode [id=" + frozenNode1).build();

        // Node1 must leave the cluster.
        LogListener backwardPingLogLsnr = LogListener.matches("Remote node requests topology change. Checking connection to " +
            "previous [TcpDiscoveryNode [id=" + frozenNodeId).build();

        testLog.registerListener(node1SegmentedLogLsnr);
        testLog.registerListener(backwardPingLogLsnr);

        // Result of the ping from node2 ot node1.
        AtomicReference<Boolean> backwardPingResult = new AtomicReference<>();

        // Request to establish new permanent cluster connection from doubting node0 to node2.
        testSpi(doubtNode0).hsRqLsnr.set((s, hsRq) -> {
            if (hsRq.changeTopology() && frozenNodeId.equals(hsRq.checkPreviousNodeId())) {
                // Continue simulation of node1 freeze at GC and processes no discovery messages.
                testSpi(frozenNode1).addrsToBlock = Collections.emptyList();
            }
        });

        // Response from node2 to node0 with negative check of frozen node1.
        testSpi(pingingNode2).hsRespLsnr.set((s, hsResp) -> {
            backwardPingResult.set(hsResp.previousNodeAlive());
        });

        // Begin simulation of node1 freeze at GC and processes no discovery messages and wait till
        // the discovery traffic node0->node1 stops.
        testSpi(doubtNode0).addrsToBlock = spi(frozenNode1).locNodeAddrs;
        assertTrue(waitForCondition(() -> testSpi(doubtNode0).blocked, getTestTimeout()));

        // Wait till the discovery traffic node1->node2 stops too.
        assertTrue(waitForCondition(() -> testSpi(frozenNode1).blocked, getTestTimeout()));

        // Wait till the backward connection check and ensure the result is negative (node1 confirmed failed).
        assertTrue(backwardPingLogLsnr.check(getTestTimeout()));
        assertTrue(waitForCondition(() -> backwardPingResult.get() != null, getTestTimeout()));

        assertFalse(backwardPingResult.get());

        assertTrue(backwardPingLogLsnr.check(getTestTimeout()));

        // Node0 and node2 must survive.
        assertTrue(waitForCondition(() -> doubtNode0.cluster().nodes().size() == 2
                && !doubtNode0.cluster().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet()).contains(frozenNodeId),
            getTestTimeout()));

        assertTrue(waitForCondition(() -> pingingNode2.cluster().nodes().size() == 2
                && !pingingNode2.cluster().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet()).contains(frozenNodeId),
            getTestTimeout()));
    }

    /**
     * Performs Tests backward node ping if {@link TcpDiscoveryNode#socketAddresses()} contains same loopback address as of local node.
     * Assumes several local address are resolved.
     */
    private void doTestBackwardNodeCheckWithSameLoopback(String localhost) throws Exception {
        this.localhost = localhost;

        specialSpi = new TestDiscoverySpi();

        Ignite node0 = startGrid(0);

        specialSpi = null;

        Ignite node1 = startGrid(1);

        specialSpi = new TestDiscoverySpi();

        Ignite node2 = startGrid(2);

        CountDownLatch handshakeToNode2 = new CountDownLatch(1);

        // Listener of handshake request from node0 to node2. Activates simulation of same localhost address of node1
        // for node2. Also, disabled network malfunction. The cluster must be restored.
        testSpi(node0).hsRqLsnr.set((socket, handshakeRequest) -> {
            // First, node0 tries to connect and send the handshake request to another address of faulty node1.
            if (testSpi(node2).locNodeAddrs.contains(new InetSocketAddress(socket.getInetAddress(), socket.getPort()))) {
                testSpi(node2).simulatedPrevNodeAddr.set(F.viewReadOnly(testSpi(node2).locNode.socketAddresses(),
                    a -> a, a -> a.getAddress().isLoopbackAddress()));

                testSpi(node0).hsRqLsnr.set(null);

                // Restore network. Node0 is now able to connect to node1 again.
                testSpi(node0).addrsToBlock = null;

                handshakeToNode2.countDown();
            }
        });

        AtomicReference<Boolean> node1AliveStatus = new AtomicReference<>();

        // Listener of handshake response from node2 to node1.
        testSpi(node2).hsRespLsnr.set(((socket1, response) -> {
            testSpi(node2).simulatedPrevNodeAddr.set(null);

            testSpi(node2).hsRespLsnr.set(null);

            node1AliveStatus.set(response.previousNodeAlive());
        }));

        // Simulate malfunction of connection node0 to mode1.
        testSpi(node0).addrsToBlock = spi(node1).locNodeAddrs;
        assertTrue(waitForCondition(() -> testSpi(node0).blocked, failureDetectionTimeout));

        // Wait until node0 tries to connect to node2 and asks if node1 is alive.
        assertTrue(handshakeToNode2.await((long)failureDetectionTimeout * (spi(node1).locNodeAddrs.size() + 1),
            TimeUnit.MILLISECONDS));

        assertTrue(waitForCondition(() -> node1AliveStatus.get() != null, failureDetectionTimeout));

        assertTrue(node1AliveStatus.get());

        // Wait a bit until node0 restore connection node1.
        U.sleep(failureDetectionTimeout / 2);

        // Node 1 must not be kicked.
        for (Ignite ig : G.allGrids())
            assertEquals(3, ig.cluster().nodes().size());
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
     * This test uses node failure by stopping service threads, which makes the node unresponsive and results in
     * failing connection to the server. Failures are simulated on the 1st node in the ring. In this case,
     * the 2nd node in the ring will trigger 'Backward Connection Check', which should result in failing attempt of connection.
     * This result is followed by the corresponding logs, indicating described failures. The test verifies the logs.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBackwardConnectionCheckFailedLogMessage() throws Exception {
        startGrid(0);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        LogListener lsnr0 = LogListener.matches("Failed to check connection to previous node").times(2).build();

        testLog.registerListener(lsnr0);

        gridLog = testLog;

        startGrid(1);

        startGrid(2);

        spi(grid(0)).simulateNodeFailure();

        assertTrue(lsnr0.check(getTestTimeout()));

        for (Ignite ig : Arrays.asList(grid(1), grid(2))) {
            waitForCondition(() -> ig.cluster().nodes().size() == 2, getTestTimeout());

            assertTrue(ig.cluster().nodes().stream().noneMatch(node -> node.order() == 1));
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
    private static TestDiscoverySpi testSpi(Ignite ig) {
        return ((TestDiscoverySpi)ig.configuration().getDiscoverySpi());
    }

    /** */
    private static TcpDiscoverySpi spi(Ignite ig) {
        return ((TcpDiscoverySpi)ig.configuration().getDiscoverySpi());
    }

    /** */
    private static final class TestDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private volatile Collection<InetSocketAddress> addrsToBlock;

        /** */
        private volatile boolean blocked;

        /** Handshake request listener. */
        private final AtomicReference<BiConsumer<Socket, TcpDiscoveryHandshakeRequest>> hsRqLsnr = new AtomicReference<>();

        /** Handshake response listener. */
        private final AtomicReference<BiConsumer<Socket, TcpDiscoveryHandshakeResponse>> hsRespLsnr = new AtomicReference<>();

        /** Additional simulated addresses of a previous node. */
        private final AtomicReference<Collection<InetSocketAddress>> simulatedPrevNodeAddr = new AtomicReference<>();

        /** {@inheritDoc} */
        @Override protected void initializeImpl() {
            if (impl != null)
                return;

            super.initializeImpl();

            // To make the test stable, we want a loopback paddress of the previous node responds first.
            // We don't need a concurrent ping execution.
            if (impl instanceof ServerImpl)
                impl = new ServerImpl(this, 1);
        }

        /** */
        private boolean dropMsg(Socket sock) {
            Collection<InetSocketAddress> addrsToBlock = this.addrsToBlock;

            if (addrsToBlock != null && (addrsToBlock.isEmpty() ||
                addrsToBlock.contains(new InetSocketAddress(sock.getInetAddress(), sock.getPort())))) {

                blocked = true;

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (dropMsg(sock))
                return;

            super.writeToSocket(msg, sock, res, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            BiConsumer<Socket, TcpDiscoveryHandshakeRequest> hsRqLsnr;
            BiConsumer<Socket, TcpDiscoveryHandshakeResponse> hsRespLsnr;

            if (msg instanceof TcpDiscoveryHandshakeRequest && (hsRqLsnr = this.hsRqLsnr.get()) != null)
                hsRqLsnr.accept(sock, (TcpDiscoveryHandshakeRequest)msg);

            if (msg instanceof TcpDiscoveryHandshakeResponse && (hsRespLsnr = this.hsRespLsnr.get()) != null)
                hsRespLsnr.accept(sock, (TcpDiscoveryHandshakeResponse)msg);

            if (dropMsg(sock))
                return;

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (dropMsg(sock))
                return;

            super.writeToSocket(sock, msg, data, timeout);
        }

        /**
         * Simulates same tcp discovery local host address for {@code node} as if it is auto-generated on different host.
         *
         * @see IgniteConfiguration#setLocalHost(String)
         * @see TcpDiscoverySpi#setLocalAddress(String)
         */
        @Override LinkedHashSet<InetSocketAddress> getEffectiveNodeAddresses(TcpDiscoveryNode node) {
            Collection<InetSocketAddress> testAddrs = simulatedPrevNodeAddr.getAndSet(null);

            if (testAddrs != null)
                node = new TestTcpDiscoveryNode(node, testAddrs);

            return super.getEffectiveNodeAddresses(node);
        }
    }

    /**
     * Simulates test node addresses.
     *
     * @see TcpDiscoveryNode#socketAddresses()
     */
    private static final class TestTcpDiscoveryNode extends TcpDiscoveryNode {
        /** */
        private final Collection<InetSocketAddress> testAddrs;

        /**
         * Creates test TCP discovery spi.
         *
         * @param node Original node.
         * @param simulatedAddrs Simulated addresses of {@code node}
         */
        public TestTcpDiscoveryNode(TcpDiscoveryNode node, Collection<InetSocketAddress> simulatedAddrs) {
            super(node);

            setAttributes(node.attributes());

            // We put test addresses first to make sure they are processed/requested before the real addresses.
            testAddrs = new ArrayList<>(simulatedAddrs);
            testAddrs.addAll(node.socketAddresses());
        }

        /** {@inheritDoc} */
        @Override public Collection<InetSocketAddress> socketAddresses() {
            return Collections.unmodifiableCollection(testAddrs);
        }
    }
}
