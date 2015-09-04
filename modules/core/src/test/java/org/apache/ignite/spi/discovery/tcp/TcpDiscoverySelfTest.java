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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.spi.IgnitePortProtocol.UDP;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class TcpDiscoverySelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private Map<String, TcpDiscoverySpi> discoMap = new HashMap<>();

    /** */
    private UUID nodeId;

    /**
     * @throws Exception If fails.
     */
    public TcpDiscoverySelfTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "deprecation"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = gridName.contains("testPingInterruptedOnNodeFailedFailingNode") ?
            new TestTcpDiscoverySpi() : new TcpDiscoverySpi();

        discoMap.put(gridName, spi);

        spi.setIpFinder(ipFinder);

        spi.setNetworkTimeout(2500);

        spi.setHeartbeatFrequency(1000);

        spi.setMaxMissedHeartbeats(3);

        spi.setIpFinderCleanFrequency(5000);

        spi.setJoinTimeout(5000);

        cfg.setDiscoverySpi(spi);

        cfg.setCacheConfiguration();

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setIncludeProperties();

        if (!gridName.contains("LoopbackProblemTest"))
            cfg.setLocalHost("127.0.0.1");

        if (gridName.contains("testFailureDetectionOnNodePing")) {
            spi.setReconnectCount(1); // To make test faster: on Windows 1 connect takes 1 second.
            spi.setHeartbeatFrequency(40000);
        }

        cfg.setConnectorConfiguration(null);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        if (gridName.contains("NonSharedIpFinder")) {
            TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

            finder.setAddresses(Arrays.asList("127.0.0.1:47501"));

            spi.setIpFinder(finder);
        }
        else if (gridName.contains("MulticastIpFinder")) {
            TcpDiscoveryMulticastIpFinder finder = new TcpDiscoveryMulticastIpFinder();

            finder.setAddressRequestAttempts(10);
            finder.setMulticastGroup(GridTestUtils.getNextMulticastGroup(getClass()));
            finder.setMulticastPort(GridTestUtils.getNextMulticastPort(getClass()));

            spi.setIpFinder(finder);

            // Loopback multicast discovery is not working on Mac OS
            // (possibly due to http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7122846).
            if (U.isMacOs())
                spi.setLocalAddress(F.first(U.allLocalIps()));
        }
        else if (gridName.contains("testPingInterruptedOnNodeFailedPingingNode"))
            cfg.setFailureDetectionTimeout(30_000);

        return cfg;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testSingleNodeStartStop() throws Exception {
        try {
            startGrid(1);
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testThreeNodesStartStop() throws Exception {
        try {
            IgniteEx ignite1 = startGrid(1);
            IgniteEx ignite2 = startGrid(2);
            IgniteEx ignite3 = startGrid(3);

            TcpDiscoverySpi spi1 = (TcpDiscoverySpi)ignite1.configuration().getDiscoverySpi();
            TcpDiscoverySpi spi2 = (TcpDiscoverySpi)ignite2.configuration().getDiscoverySpi();
            TcpDiscoverySpi spi3 = (TcpDiscoverySpi)ignite3.configuration().getDiscoverySpi();

            TcpDiscoveryNode node = (TcpDiscoveryNode)spi1.getNode(ignite2.localNode().id());

            assertNotNull(node);
            assertNotNull(node.lastSuccessfulAddress());

            LinkedHashSet<InetSocketAddress> addrs = spi1.getNodeAddresses(node);

            assertEquals(addrs.iterator().next(), node.lastSuccessfulAddress());

            assertTrue(spi1.pingNode(ignite3.localNode().id()));

            node = (TcpDiscoveryNode)spi1.getNode(ignite3.localNode().id());

            assertNotNull(node);
            assertNotNull(node.lastSuccessfulAddress());

            addrs = spi1.getNodeAddresses(node);
            assertEquals(addrs.iterator().next(), node.lastSuccessfulAddress());

            node = (TcpDiscoveryNode)spi2.getNode(ignite1.localNode().id());

            assertNotNull(node);
            assertNotNull(node.lastSuccessfulAddress());

            node = (TcpDiscoveryNode)spi2.getNode(ignite3.localNode().id());

            assertNotNull(node);
            assertNotNull(node.lastSuccessfulAddress());

            node = (TcpDiscoveryNode)spi3.getNode(ignite1.localNode().id());

            assertNotNull(node);
            assertNotNull(node.lastSuccessfulAddress());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any errors occur.
     */
    public void testNodeConnectMessageSize() throws Exception {
        try {
            Ignite g1 = startGrid(1);

            final AtomicInteger gridNameIdx = new AtomicInteger(1);

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    startGrid(gridNameIdx.incrementAndGet());

                    return null;
                }
            }, 4, "grid-starter");

            Collection<TcpDiscoveryNode> nodes = ((ServerImpl)discoMap.get(g1.name()).impl).ring().allNodes();

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            g1.configuration().getMarshaller().marshal(nodes, bos);

            info(">>> Approximate node connect message size [topSize=" + nodes.size() +
                ", msgSize=" + bos.size() / 1024.0 + "KB]");
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testPing() throws Exception {
        try {
            startGrid(1);
            startGrid(2);
            startGrid(3);

            info("Nodes were started");

            for (Map.Entry<String, TcpDiscoverySpi> e : discoMap.entrySet()) {
                DiscoverySpi spi = e.getValue();

                for (Ignite g : G.allGrids()) {
                    boolean res = spi.pingNode(g.cluster().localNode().id());

                    assert res : e.getKey() + " failed to ping " + g.cluster().localNode().id() + " of " + g.name();

                    info(e.getKey() + " pinged " + g.cluster().localNode().id() + " of " + g.name());
                }
            }

            info("All nodes pinged successfully.");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testFailureDetectionOnNodePing1() throws Exception {
        try {
            Ignite g1 = startGrid("testFailureDetectionOnNodePingCoordinator");
            startGrid("testFailureDetectionOnNodePing2");
            Ignite g3 = startGrid("testFailureDetectionOnNodePing3");

            testFailureDetectionOnNodePing(g1, g3);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testFailureDetectionOnNodePing2() throws Exception {
        try {
            startGrid("testFailureDetectionOnNodePingCoordinator");
            Ignite g2 = startGrid("testFailureDetectionOnNodePing2");
            Ignite g3 = startGrid("testFailureDetectionOnNodePing3");

            testFailureDetectionOnNodePing(g3, g2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testFailureDetectionOnNodePing3() throws Exception {
        try {
            Ignite g1 = startGrid("testFailureDetectionOnNodePingCoordinator");
            Ignite g2 = startGrid("testFailureDetectionOnNodePing2");
            startGrid("testFailureDetectionOnNodePing3");

            testFailureDetectionOnNodePing(g2, g1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    private void testFailureDetectionOnNodePing(Ignite pingingNode, Ignite failedNode) throws Exception {
        final CountDownLatch cnt = new CountDownLatch(1);

        pingingNode.events().localListen(
            new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    cnt.countDown();

                    return true;
                }
            },
            EventType.EVT_NODE_FAILED
        );

        info("Nodes were started");

        discoMap.get(failedNode.name()).simulateNodeFailure();

        TcpDiscoverySpi spi = discoMap.get(pingingNode.name());

        boolean res = spi.pingNode(failedNode.cluster().localNode().id());

        assertFalse("Ping is ok for node " + failedNode.cluster().localNode().id() + ", but had to fail.", res);

        // Heartbeat interval is 40 seconds, but we should detect node failure faster.
        assert cnt.await(7, SECONDS);
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testPingInterruptedOnNodeFailed() throws Exception {
        try {
            final Ignite pingingNode = startGrid("testPingInterruptedOnNodeFailedPingingNode");
            final Ignite failedNode = startGrid("testPingInterruptedOnNodeFailedFailingNode");
            startGrid("testPingInterruptedOnNodeFailedSimpleNode");

            ((TestTcpDiscoverySpi)failedNode.configuration().getDiscoverySpi()).ignorePingResponse = true;

            final CountDownLatch pingLatch = new CountDownLatch(1);

            final CountDownLatch eventLatch = new CountDownLatch(1);

            final AtomicBoolean pingRes = new AtomicBoolean(true);

            final AtomicBoolean failRes = new AtomicBoolean(false);

            long startTs = System.currentTimeMillis();

            pingingNode.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event event) {
                        if (((DiscoveryEvent)event).eventNode().id().equals(failedNode.cluster().localNode().id())) {
                            failRes.set(true);
                            eventLatch.countDown();
                        }

                        return true;
                    }
                },
                EventType.EVT_NODE_FAILED);

            IgniteInternalFuture<?> pingFut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        pingLatch.countDown();

                        pingRes.set(pingingNode.configuration().getDiscoverySpi().pingNode(
                            failedNode.cluster().localNode().id()));

                        return null;
                    }
                }, 1);

            IgniteInternalFuture<?> failingFut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        pingLatch.await();

                        Thread.sleep(3000);

                        ((TestTcpDiscoverySpi)failedNode.configuration().getDiscoverySpi()).simulateNodeFailure();

                        return null;
                    }
                }, 1);

            failingFut.get();
            pingFut.get();

            assertFalse(pingRes.get());

            assertTrue(System.currentTimeMillis() - startTs <
                pingingNode.configuration().getFailureDetectionTimeout() / 2);

            assertTrue(eventLatch.await(7, TimeUnit.SECONDS));
            assertTrue(failRes.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testPingInterruptedOnNodeLeft() throws Exception {
        try {
            final Ignite pingingNode = startGrid("testPingInterruptedOnNodeFailedPingingNode");
            final Ignite leftNode = startGrid("testPingInterruptedOnNodeFailedFailingNode");
            startGrid("testPingInterruptedOnNodeFailedSimpleNode");

            ((TestTcpDiscoverySpi)leftNode.configuration().getDiscoverySpi()).ignorePingResponse = true;

            final CountDownLatch pingLatch = new CountDownLatch(1);

            final AtomicBoolean pingRes = new AtomicBoolean(true);

            long startTs = System.currentTimeMillis();

            IgniteInternalFuture<?> pingFut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        pingLatch.countDown();

                        pingRes.set(pingingNode.configuration().getDiscoverySpi().pingNode(
                            leftNode.cluster().localNode().id()));

                        return null;
                    }
                }, 1);

            IgniteInternalFuture<?> stoppingFut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        pingLatch.await();

                        Thread.sleep(3000);

                        stopGrid("testPingInterruptedOnNodeFailedFailingNode");

                        return null;
                    }
                }, 1);

            stoppingFut.get();
            pingFut.get();

            assertFalse(pingRes.get());

            assertTrue(System.currentTimeMillis() - startTs <
                pingingNode.configuration().getFailureDetectionTimeout() / 2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private boolean ignorePingResponse;

        /** {@inheritDoc} */
        protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException,
            IgniteCheckedException {
            if (msg instanceof TcpDiscoveryPingResponse && ignorePingResponse)
                return;
            else
                super.writeToSocket(sock, msg, timeout);
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testNodeAdded() throws Exception {
        try {
            final Ignite g1 = startGrid(1);

            final CountDownLatch cnt = new CountDownLatch(2);

            g1.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Node joined: " + evt.message());

                        DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                        TcpDiscoveryNode node = ((TcpDiscoveryNode)discoMap.get(g1.name()).
                            getNode(discoEvt.eventNode().id()));

                        assert node != null && node.visible();

                        cnt.countDown();

                        return true;
                    }
                },
                EventType.EVT_NODE_JOINED
            );

            startGrid(2);
            startGrid(3);

            info("Nodes were started");

            assert cnt.await(1, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testOrdinaryNodeLeave() throws Exception {
        try {
            Ignite g1 = startGrid(1);
            startGrid(2);
            startGrid(3);

            final CountDownLatch cnt = new CountDownLatch(2);

            g1.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        cnt.countDown();

                        return true;
                    }
                },
                EVT_NODE_LEFT, EVT_NODE_FAILED);

            info("Nodes were started");

            stopGrid(3);
            stopGrid(2);

            boolean res = cnt.await(1, SECONDS);

            assert res;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testCoordinatorNodeLeave() throws Exception {
        try {
            startGrid(1);
            Ignite g2 = startGrid(2);

            final CountDownLatch cnt = new CountDownLatch(1);

            g2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    cnt.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);

            info("Nodes were started");

            stopGrid(1);

            assert cnt.await(1, SECONDS);

            // Start new grid, ensure that added to topology
            final CountDownLatch cnt2 = new CountDownLatch(1);

            g2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    cnt2.countDown();

                    return true;
                }
            }, EVT_NODE_JOINED);

            startGrid(3);

            assert cnt2.await(1, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testOrdinaryNodeFailure() throws Exception {
        try {
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);
            Ignite g3 = startGrid(3);

            final CountDownLatch cnt = new CountDownLatch(2);

            g1.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        cnt.countDown();

                        return true;
                    }
                },
                EventType.EVT_NODE_FAILED
            );

            info("Nodes were started");

            discoMap.get(g2.name()).simulateNodeFailure();
            discoMap.get(g3.name()).simulateNodeFailure();

            assert cnt.await(25, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testCoordinatorNodeFailure() throws Exception {
        try {
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            final CountDownLatch cnt = new CountDownLatch(1);

            g2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    cnt.countDown();

                    return true;
                }
            }, EventType.EVT_NODE_FAILED);

            info("Nodes were started");

            discoMap.get(g1.name()).simulateNodeFailure();

            assert cnt.await(20, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testMetricsSending() throws Exception {
        final AtomicBoolean stopping = new AtomicBoolean();

        try {
            final CountDownLatch latch1 = new CountDownLatch(1);

            final Ignite g1 = startGrid(1);

            IgnitePredicate<Event> lsnr1 = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    info(evt.message());

                    latch1.countDown();

                    return true;
                }
            };

            g1.events().localListen(lsnr1, EVT_NODE_METRICS_UPDATED);

            assert latch1.await(10, SECONDS);

            g1.events().stopLocalListen(lsnr1);

            final CountDownLatch latch1_1 = new CountDownLatch(1);
            final CountDownLatch latch1_2 = new CountDownLatch(1);
            final CountDownLatch latch2_1 = new CountDownLatch(1);
            final CountDownLatch latch2_2 = new CountDownLatch(1);

            final Ignite g2 = startGrid(2);

            g2.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        if (stopping.get())
                            return true;

                        info(evt.message());

                        UUID id = ((DiscoveryEvent) evt).eventNode().id();

                        if (id.equals(g1.cluster().localNode().id()))
                            latch2_1.countDown();
                        else if (id.equals(g2.cluster().localNode().id()))
                            latch2_2.countDown();
                        else
                            assert false : "Event fired for unknown node.";

                        return true;
                    }
                },
                EVT_NODE_METRICS_UPDATED
            );

            g1.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (stopping.get())
                        return true;

                    info(evt.message());

                    UUID id = ((DiscoveryEvent) evt).eventNode().id();

                    if (id.equals(g1.cluster().localNode().id()))
                        latch1_1.countDown();
                    else if (id.equals(g2.cluster().localNode().id()))
                        latch1_2.countDown();
                    else
                        assert false : "Event fired for unknown node.";

                    return true;
                }
            }, EVT_NODE_METRICS_UPDATED);

            assert latch1_1.await(10, SECONDS);
            assert latch1_2.await(10, SECONDS);
            assert latch2_1.await(10, SECONDS);
            assert latch2_2.await(10, SECONDS);
        }
        finally {
            stopping.set(true);

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testFailBeforeNodeAddedSent() throws Exception {
        try {
            Ignite g1 = startGrid(1);

            final CountDownLatch joinCnt = new CountDownLatch(2);
            final CountDownLatch failCnt = new CountDownLatch(1);

            g1.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_NODE_JOINED)
                        joinCnt.countDown();
                    else if (evt.type() == EVT_NODE_FAILED)
                        failCnt.countDown();
                    else
                        assert false : "Unexpected event type: " + evt;

                    return true;
                }
            }, EVT_NODE_JOINED, EVT_NODE_FAILED);

            final Ignite g = startGrid("FailBeforeNodeAddedSentSpi");

            discoMap.get(g.name()).addSendMessageListener(new IgniteInClosure<TcpDiscoveryAbstractMessage>() {
                @Override public void apply(TcpDiscoveryAbstractMessage msg) {
                    if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                        discoMap.get(g.name()).simulateNodeFailure();

                        throw new RuntimeException("Avoid message sending: " + msg.getClass());
                    }
                }
            });

            startGrid(3);

            assert joinCnt.await(10, SECONDS);
            assert failCnt.await(10, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testFailBeforeNodeLeftSent() throws Exception {
        try {
            startGrid(1);
            startGrid(2);

            final Ignite g = startGrid("FailBeforeNodeLeftSentSpi");

            discoMap.get(g.name()).addSendMessageListener(new IgniteInClosure<TcpDiscoveryAbstractMessage>() {
                @Override public void apply(TcpDiscoveryAbstractMessage msg) {
                    if (msg instanceof TcpDiscoveryNodeLeftMessage) {
                        discoMap.get(g.name()).simulateNodeFailure();

                        throw new RuntimeException("Avoid message sending: " + msg.getClass());
                    }
                }
            });

            Ignite g3 = startGrid(3);

            final CountDownLatch cnt = new CountDownLatch(1);

            g3.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    cnt.countDown();

                    return true;
                }
            }, EVT_NODE_FAILED);

            stopGrid(1);

            assert cnt.await(20, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testIpFinderCleaning() throws Exception {
        try {
            ipFinder.registerAddresses(Arrays.asList(new InetSocketAddress("host1", 1024),
                new InetSocketAddress("host2", 1024)));

            Ignite g1 = startGrid(1);

            long timeout = (long)(discoMap.get(g1.name()).getIpFinderCleanFrequency() * 1.5);

            Thread.sleep(timeout);

            assert ipFinder.getRegisteredAddresses().size() == 1 : "ipFinder=" + ipFinder.getRegisteredAddresses();

            // Check that missing addresses are returned back.
            ipFinder.unregisterAddresses(ipFinder.getRegisteredAddresses()); // Unregister valid address.

            ipFinder.registerAddresses(Arrays.asList(new InetSocketAddress("host1", 1024),
                new InetSocketAddress("host2", 1024)));

            Thread.sleep(timeout);

            assert ipFinder.getRegisteredAddresses().size() == 1 : "ipFinder=" + ipFinder.getRegisteredAddresses();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testNonSharedIpFinder() throws Exception {
        try {
            GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Thread.sleep(4000);

                    return startGrid("NonSharedIpFinder-2");
                }
            }, 1, "grid-starter");

            // This node should wait until any node "from ipFinder" appears, see log messages.
            Ignite g = startGrid("NonSharedIpFinder-1");

            assert g.cluster().localNode().order() == 2;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testMulticastIpFinder() throws Exception {
        try {
            for (int i = 0; i < 5; i++) {
                Ignite g = startGrid("MulticastIpFinder-" + i);

                assertEquals(i + 1, g.cluster().nodes().size());

                TcpDiscoverySpi spi = (TcpDiscoverySpi)g.configuration().getDiscoverySpi();

                TcpDiscoveryMulticastIpFinder ipFinder = (TcpDiscoveryMulticastIpFinder)spi.getIpFinder();

                boolean found = false;

                for (GridPortRecord rec : ((IgniteKernal) g).context().ports().records()) {
                    if ((rec.protocol() == UDP) && rec.port() == ipFinder.getMulticastPort()) {
                        found = true;

                        break;
                    }
                }

                assertTrue("GridTcpDiscoveryMulticastIpFinder should register port." , found);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testInvalidAddressIpFinder() throws Exception {
        ipFinder.setShared(false);

        ipFinder.setAddresses(Collections.singletonList("some-host"));

        try {
            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        startGrid(1);

                        return null;
                    }
                },
                IgniteCheckedException.class,
                null);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testJoinTimeout() throws Exception {
        try {
            // This start will fail as expected.
            Throwable t = GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid("NonSharedIpFinder-1");

                    return null;
                }
            }, IgniteCheckedException.class, null);

            assert X.hasCause(t, IgniteSpiException.class) : "Unexpected exception: " + t;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDirtyIpFinder() throws Exception {
        try {
            // Dirty IP finder
            for (int i = 47500; i < 47520; i++)
                ipFinder.registerAddresses(Arrays.asList(new InetSocketAddress("127.0.0.1", i),
                    new InetSocketAddress("unknown-host", i)));

            assert ipFinder.isShared();

            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testDuplicateId() throws Exception {
        try {
            // Random ID.
            startGrid(1);

            nodeId = UUID.randomUUID();

            startGrid(2);

            // Duplicate ID.
            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        // Exception will be thrown and output to log.
                        startGrid(3);

                        return null;
                    }
                },
                IgniteCheckedException.class,
                null);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testLoopbackProblemFirstNodeOnLoopback() throws Exception {
        // On Windows and Mac machines two nodes can reside on the same port
        // (if one node has localHost="127.0.0.1" and another has localHost="0.0.0.0").
        // So two nodes do not even discover each other.
        if (U.isWindows() || U.isMacOs())
            return;

        try {
            startGridNoOptimize(1);

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        // Exception will be thrown because we start node which does not use loopback address,
                        // but the first node does.
                        startGridNoOptimize("LoopbackProblemTest");

                        return null;
                    }
                },
                IgniteException.class,
                null);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testLoopbackProblemSecondNodeOnLoopback() throws Exception {
        if (U.isWindows() || U.isMacOs())
            return;

        try {
            startGridNoOptimize("LoopbackProblemTest");

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        // Exception will be thrown because we start node which uses loopback address,
                        // but the first node does not.
                        startGridNoOptimize(1);

                        return null;
                    }
                },
                IgniteException.class,
                null);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testGridStartTime() throws Exception {
        try {
            startGridsMultiThreaded(5);

            Long startTime = null;

            IgniteKernal firstGrid = null;

            Collection<IgniteKernal> grids = new ArrayList<>();

            for (int i = 0; i < 5 ; i++) {
                IgniteKernal grid = (IgniteKernal)grid(i);

                assertTrue(grid.context().discovery().gridStartTime() > 0);

                if (i > 0)
                    assertEquals(startTime, (Long)grid.context().discovery().gridStartTime());
                else
                    startTime = grid.context().discovery().gridStartTime();

                if (grid.localNode().order() == 1)
                    firstGrid = grid;
                else
                    grids.add(grid);
            }

            assertNotNull(firstGrid);

            stopGrid(firstGrid.name());

            for (IgniteKernal grid : grids)
                assertEquals(startTime, (Long)grid.context().discovery().gridStartTime());

            grids.add((IgniteKernal) startGrid(5));

            for (IgniteKernal grid : grids)
                assertEquals(startTime, (Long)grid.context().discovery().gridStartTime());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Starts new grid with given index. Method optimize is not invoked.
     *
     * @param idx Index of the grid to start.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    private Ignite startGridNoOptimize(int idx) throws Exception {
        return startGridNoOptimize(getTestGridName(idx));
    }

    /**
     * Starts new grid with given name. Method optimize is not invoked.
     *
     * @param gridName Grid name.
     * @return Started grid.
     * @throws Exception If failed.
     */
    private Ignite startGridNoOptimize(String gridName) throws Exception {
        return G.start(getConfiguration(gridName));
    }
}