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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.continuous.StartRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryStatistics;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;
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

    /** */
    private static ThreadLocal<TcpDiscoverySpi> nodeSpi = new ThreadLocal<>();

    /** */
    private GridStringLogger strLog;

    /** */
    private CacheConfiguration[] ccfgs;

    /** */
    private boolean client;

    /**
     * @throws Exception If fails.
     */
    public TcpDiscoverySelfTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        TcpDiscoverySpi spi = nodeSpi.get();

        if (spi == null) {
            spi = gridName.contains("testPingInterruptedOnNodeFailedFailingNode") ?
                new TestTcpDiscoverySpi() : new TcpDiscoverySpi();
        }
        else
            nodeSpi.set(null);

        discoMap.put(gridName, spi);

        spi.setIpFinder(ipFinder);

        spi.setNetworkTimeout(2500);

        spi.setHeartbeatFrequency(1000);

        spi.setMaxMissedHeartbeats(3);

        spi.setIpFinderCleanFrequency(5000);

        spi.setJoinTimeout(5000);

        cfg.setDiscoverySpi(spi);

        if (ccfgs != null)
            cfg.setCacheConfiguration(ccfgs);
        else
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

            finder.setAddressRequestAttempts(5);
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
        else if (gridName.contains("testNoRingMessageWorkerAbnormalFailureNormalNode"))
            cfg.setFailureDetectionTimeout(3_000);
        else if (gridName.contains("testNoRingMessageWorkerAbnormalFailureSegmentedNode")) {
            cfg.setFailureDetectionTimeout(6_000);

            cfg.setGridLogger(strLog = new GridStringLogger());
        }
        else if (gridName.contains("testNodeShutdownOnRingMessageWorkerFailureFailedNode"))
            cfg.setGridLogger(strLog = new GridStringLogger());

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        discoMap = null;

        super.afterTest();
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

        final UUID failedNodeId = failedNode.cluster().localNode().id();

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

        boolean res = spi.pingNode(failedNodeId);

        assertFalse("Ping is ok for node " + failedNodeId + ", but had to fail.", res);

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

            final UUID failedNodeId = failedNode.cluster().localNode().id();

            final CountDownLatch pingLatch = new CountDownLatch(1);

            final CountDownLatch eventLatch = new CountDownLatch(1);

            final AtomicBoolean pingRes = new AtomicBoolean(true);

            final AtomicBoolean failRes = new AtomicBoolean(false);

            long startTs = System.currentTimeMillis();

            pingingNode.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event event) {
                        if (((DiscoveryEvent)event).eventNode().id().equals(failedNodeId)) {
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
                            failedNodeId));

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
            ipFinder.registerAddresses(Arrays.asList(new InetSocketAddress("1.1.1.1", 1024),
                new InetSocketAddress("1.1.1.2", 1024)));

            Ignite g1 = startGrid(1);

            long failureDetectTimeout = g1.configuration().getFailureDetectionTimeout();

            long timeout = (long)(discoMap.get(g1.name()).getIpFinderCleanFrequency() * 1.5) + failureDetectTimeout;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ipFinder.getRegisteredAddresses().size() == 1;
                }
            }, timeout);

            if (ipFinder.getRegisteredAddresses().size() != 1) {
                log.error("Failed to wait for IP cleanup, will dump threads.");

                U.dumpThreads(log);
            }

            assert ipFinder.getRegisteredAddresses().size() == 1 : "ipFinder=" + ipFinder.getRegisteredAddresses();

            // Check that missing addresses are returned back.
            ipFinder.unregisterAddresses(ipFinder.getRegisteredAddresses()); // Unregister valid address.

            ipFinder.registerAddresses(Arrays.asList(new InetSocketAddress("1.1.1.1", 1024),
                new InetSocketAddress("1.1.1.2", 1024)));

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ipFinder.getRegisteredAddresses().size() == 1;
                }
            }, timeout);

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

                assertTrue("TcpDiscoveryMulticastIpFinder should register port." , found);
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
     * @throws Exception If any error occurs.
     */
    public void testJoinTimeoutForIpFinder() throws Exception {
        try {
            // This start will fail as expected.
            Throwable t = GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgniteConfiguration c = getConfiguration("test-grid");

                    TcpDiscoverySpi disco = (TcpDiscoverySpi)c.getDiscoverySpi();

                    disco.setJoinTimeout(3000);
                    disco.setIpFinder(
                        new TcpDiscoveryVmIpFinder(true) {
                            @Override public void initializeLocalAddresses(Collection<InetSocketAddress> addrs)
                                throws IgniteSpiException {
                                throw new IgniteSpiException("Test exception.");
                            }
                        });

                    startGrid("test-grid", c);

                    return null;
                }
            }, IgniteException.class, null);

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
        if (U.isWindows() || U.isMacOs() || U.isSolaris())
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

            grids.add((IgniteKernal)startGrid(5));

            for (IgniteKernal grid : grids)
                assertEquals(startTime, (Long)grid.context().discovery().gridStartTime());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testCustomEventRace1_1() throws Exception {
        try {
            customEventRace1(true, false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testCustomEventRace1_2() throws Exception {
        try {
            customEventRace1(false, false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testCustomEventRace1_3() throws Exception {
        try {
            customEventRace1(true, true);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cacheStartFrom1 If {code true} starts cache from node1.
     * @param stopCrd If {@code true} stops coordinator.
     * @throws Exception If failed
     */
    private void customEventRace1(final boolean cacheStartFrom1, boolean stopCrd) throws Exception {
        TestCustomEventRaceSpi spi0 = new TestCustomEventRaceSpi();

        nodeSpi.set(spi0);

        final Ignite ignite0 = startGrid(0);

        nodeSpi.set(new TestCustomEventRaceSpi());

        final Ignite ignite1 = startGrid(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        spi0.nodeAdded1 = latch1;
        spi0.nodeAdded2 = latch2;
        spi0.debug = true;

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start 2");

                nodeSpi.set(new TestCustomEventRaceSpi());

                startGrid(2);

                return null;
            }
        });

        latch1.await();

        final String CACHE_NAME = "cache";

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setName(CACHE_NAME);

                Ignite ignite = cacheStartFrom1 ? ignite1 : ignite0;

                ignite.createCache(ccfg);

                return null;
            }
        });

        if (stopCrd) {
            spi0.stop = true;

            latch2.countDown();

            ignite0.close();
        }
        else {
            U.sleep(500);

            latch2.countDown();
        }

        fut1.get();
        fut2.get();

        IgniteCache<Object, Object> cache = grid(2).cache(CACHE_NAME);

        assertNotNull(cache);

        cache.put(1, 1);

        assertEquals(1, cache.get(1));

        nodeSpi.set(new TestCustomEventRaceSpi());

        Ignite ignite = startGrid(3);

        cache = ignite.cache(CACHE_NAME);

        cache.put(2, 2);

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
    }

    /**
     * @throws Exception If failed
     */
    public void testCustomEventCoordinatorFailure1() throws Exception {
        try {
            customEventCoordinatorFailure(true);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testCustomEventCoordinatorFailure2() throws Exception {
        try {
            customEventCoordinatorFailure(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testNodeShutdownOnRingMessageWorkerFailure() throws Exception {
        try {
            final TestMessageWorkerFailureSpi1 spi0 = new TestMessageWorkerFailureSpi1(
                TestMessageWorkerFailureSpi1.EXCEPTION_MODE);

            nodeSpi.set(spi0);

            final Ignite ignite0 = startGrid("testNodeShutdownOnRingMessageWorkerFailureFailedNode");

            nodeSpi.set(new TcpDiscoverySpi());

            Ignite ignite1 = startGrid(1);

            final AtomicBoolean disconnected = new AtomicBoolean();

            final CountDownLatch latch = new CountDownLatch(1);

            final UUID failedNodeId = ignite0.cluster().localNode().id();

            ignite1.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EventType.EVT_NODE_FAILED &&
                        failedNodeId.equals(((DiscoveryEvent)evt).eventNode().id())) {
                        disconnected.set(true);

                        latch.countDown();
                    }

                    return false;
                }
            }, EventType.EVT_NODE_FAILED);

            spi0.stop = true;

            latch.await(15, TimeUnit.SECONDS);

            assertTrue(disconnected.get());

            String result = strLog.toString();

            assert result.contains("TcpDiscoverSpi's message worker thread failed abnormally") : result;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testNoRingMessageWorkerAbnormalFailureOnSegmentation() throws Exception {
        try {
            TestMessageWorkerFailureSpi1 spi1 = new TestMessageWorkerFailureSpi1(
                TestMessageWorkerFailureSpi1.SEGMENTATION_MODE);

            nodeSpi.set(spi1);

            Ignite ignite1 = startGrid("testNoRingMessageWorkerAbnormalFailureNormalNode");


            nodeSpi.set(new TcpDiscoverySpi());

            final Ignite ignite2 = startGrid("testNoRingMessageWorkerAbnormalFailureSegmentedNode");


            final AtomicBoolean disconnected = new AtomicBoolean();

            final AtomicBoolean segmented = new AtomicBoolean();

            final CountDownLatch disLatch = new CountDownLatch(1);

            final CountDownLatch segLatch = new CountDownLatch(1);

            final UUID failedNodeId = ignite2.cluster().localNode().id();

            ignite1.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EventType.EVT_NODE_FAILED &&
                        failedNodeId.equals(((DiscoveryEvent)evt).eventNode().id()))
                        disconnected.set(true);

                    disLatch.countDown();

                    return false;
                }
            }, EventType.EVT_NODE_FAILED);

            ignite2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (!failedNodeId.equals(((DiscoveryEvent)evt).eventNode().id()))
                        return true;

                    if (evt.type() == EventType.EVT_NODE_SEGMENTED) {
                        segmented.set(true);

                        segLatch.countDown();
                    }

                    return true;
                }
            }, EventType.EVT_NODE_SEGMENTED);


            spi1.stop = true;

            disLatch.await(15, TimeUnit.SECONDS);

            assertTrue(disconnected.get());


            spi1.stop = false;

            segLatch.await(15, TimeUnit.SECONDS);

            assertTrue(segmented.get());


            Thread.sleep(10_000);


            String result = strLog.toString();

            assert result.contains("Local node SEGMENTED") &&
                !result.contains("TcpDiscoverSpi's message worker thread failed abnormally") : result;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testNodeShutdownOnRingMessageWorkerStartNotFinished() throws Exception {
        try {
            Ignite ignite0 = startGrid(0);

            TestMessageWorkerFailureSpi2 spi0 = new TestMessageWorkerFailureSpi2();

            nodeSpi.set(spi0);

            try {
                startGrid(1);

                fail();
            }
            catch (Exception e) {
                log.error("Expected error: " + e, e);
            }

            Ignite ignite1 = startGrid(1);

            assertEquals(2, ignite1.cluster().nodes().size());
            assertEquals(4, ignite1.cluster().topologyVersion());

            assertEquals(2, ignite0.cluster().nodes().size());
            assertEquals(4, ignite0.cluster().topologyVersion());
        }
        finally {
            stopAllGrids();
        }
    }


    /**
     * @param twoNodes If {@code true} starts two nodes, otherwise three.
     * @throws Exception If failed
     */
    private void customEventCoordinatorFailure(boolean twoNodes) throws Exception {
        TestCustomEventCoordinatorFailureSpi spi0 = new TestCustomEventCoordinatorFailureSpi();

        nodeSpi.set(spi0);

        Ignite ignite0 = startGrid(0);

        nodeSpi.set(new TestCustomEventCoordinatorFailureSpi());

        Ignite ignite1 = startGrid(1);

        nodeSpi.set(new TestCustomEventCoordinatorFailureSpi());

        Ignite ignite2 = twoNodes ? null : startGrid(2);

        final Ignite createCacheNode = ignite2 != null ? ignite2 : ignite1;

        CountDownLatch latch = new CountDownLatch(1);

        spi0.latch = latch;

        final String CACHE_NAME = "test-cache";

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Create test cache");

                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setName(CACHE_NAME);

                createCacheNode.createCache(ccfg);

                return null;
            }
        }, "create-cache-thread");

        ((TcpCommunicationSpi)ignite0.configuration().getCommunicationSpi()).simulateNodeFailure();

        latch.await();

        ignite0.close();

        fut.get();

        IgniteCache<Object, Object> cache = grid(1).cache(CACHE_NAME);

        assertNotNull(cache);

        cache.put(1, 1);

        assertEquals(1, cache.get(1));

        log.info("Try start one more node.");

        nodeSpi.set(new TestCustomEventCoordinatorFailureSpi());

        Ignite ignite = startGrid(twoNodes ? 2 : 3);

        cache = ignite.cache(CACHE_NAME);

        assertNotNull(cache);

        cache.put(2, 2);

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
    }

    /**
     * Coordinator is added in failed list during node start.
     *
     * @throws Exception If failed.
     */
    public void testFailedNodes1() throws Exception {
        try {
            final int FAIL_ORDER = 3;

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            final Ignite ignite0 = startGrid(0);

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            startGrid(1);

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            Ignite ignite2 = startGrid(2);

            assertEquals(2, ignite2.cluster().nodes().size());

            waitNodeStop(ignite0.name());

            tryCreateCache(2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Coordinator is added in failed list, concurrent nodes start.
     *
     * @throws Exception If failed.
     */
    public void testFailedNodes2() throws Exception {
        try {
            final int FAIL_ORDER = 3;

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            Ignite ignite0 = startGrid(0);

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            startGrid(1);

            final AtomicInteger nodeIdx = new AtomicInteger(1);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = nodeIdx.incrementAndGet();

                    nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

                    startGrid(idx);

                    return null;
                }
            }, 3, "start-node");

            Ignite ignite2 = ignite(2);

            waitForRemoteNodes(ignite2, 3);

            waitNodeStop(ignite0.name());

            tryCreateCache(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Coordinator is added in failed list during node start, test with two nodes.
     *
     * @throws Exception If failed.
     */
    public void testFailedNodes3() throws Exception {
        try {
            nodeSpi.set(new TestFailedNodesSpi(-1));

            Ignite ignite0 = startGrid(0);

            nodeSpi.set(new TestFailedNodesSpi(2));

            Ignite ignite1 = startGrid(1);

            assertEquals(1, ignite1.cluster().nodes().size());

            waitNodeStop(ignite0.name());

            ignite1.getOrCreateCache(new CacheConfiguration<>()).put(1, 1);

            startGrid(2);

            assertEquals(2, ignite1.cluster().nodes().size());

            tryCreateCache(2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Coordinator is added in failed list during node start, but node detected failure dies before
     * sending {@link TcpDiscoveryNodeFailedMessage}.
     *
     * @throws Exception If failed.
     */
    public void testFailedNodes4() throws Exception {
        try {
            final int FAIL_ORDER = 3;

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            final Ignite ignite0 = startGrid(0);

            nodeSpi.set(new TestFailedNodesSpi(FAIL_ORDER));

            Ignite ignite1 = startGrid(1);

            TestFailedNodesSpi spi = new TestFailedNodesSpi(FAIL_ORDER);

            spi.stopBeforeSndFail = true;

            nodeSpi.set(spi);

            Ignite ignite2 = startGrid(2);

            waitNodeStop(ignite2.name());

            log.info("Try start new node.");

            Ignite ignite3 = startGrid(3);

            waitNodeStop(ignite0.name());

            assertEquals(2, ignite1.cluster().nodes().size());
            assertEquals(2, ignite3.cluster().nodes().size());

            tryCreateCache(2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Adds some node in failed list after join process finished.
     *
     * @throws Exception If failed.
     */
    public void testFailedNodes5() throws Exception {
        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int iter = 0; iter < 3; iter++) {
                final int NODES = iter == 0 ? 2 : rnd.nextInt(3, 6);

                for (int i = 0; i < NODES; i++) {
                    nodeSpi.set(new TestFailedNodesSpi(-1));

                    startGrid(i);
                }

                Map<Long, Ignite> nodes = new HashMap<>();

                for (int i = 0; i < NODES; i++) {
                    Ignite ignite = ignite(i);

                    nodes.put(ignite.cluster().localNode().order(), ignite);
                }

                Ignite ignite = ignite(rnd.nextInt(NODES));

                log.info("Iteration [iter=" + iter + ", nodes=" + NODES + ", failFrom=" + ignite.name() + ']');

                TestFailedNodesSpi spi = (TestFailedNodesSpi)ignite.configuration().getDiscoverySpi();

                spi.failSingleMsg = true;

                long order = ignite.cluster().localNode().order();

                long nextOrder = order == NODES ? 1 : order + 1;

                Ignite failingNode = nodes.get(nextOrder);

                assertNotNull(failingNode);

                waitNodeStop(failingNode.name());

                Ignite newNode = startGrid(NODES);

                assertEquals(NODES, newNode.cluster().nodes().size());

                tryCreateCache(NODES);

                stopAllGrids();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomEventAckNotSend() throws Exception {
        try {
            TestCustomerEventAckSpi spi0 = new TestCustomerEventAckSpi();

            nodeSpi.set(spi0);

            Ignite ignite0 = startGrid(0);

            nodeSpi.set(new TestCustomerEventAckSpi());

            Ignite ignite1 = startGrid(1);

            spi0.stopBeforeSndAck = true;

            ignite1.message().remoteListen("test", new DummyPredicate());

            waitNodeStop(ignite0.name());

            startGrid(2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiscoveryEventsDiscard() throws Exception {
        try {
            TestEventDiscardSpi spi = new TestEventDiscardSpi();

            nodeSpi.set(spi);

            Ignite ignite0 = startGrid(0);

            startGrid(1);

            ignite0.createCache(new CacheConfiguration<>()); // Send custom message.

            ignite0.destroyCache(null); // Send custom message.

            stopGrid(1);

            log.info("Start new node.");

            spi.checkDuplicates = true;

            startGrid(1);

            spi.checkDuplicates = false;

            assertFalse(spi.failed);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoExtraNodeFailedMessage() throws Exception {
        try {
            final int NODES = 10;

            startGridsMultiThreaded(NODES);

            int stopIdx = 5;

            Ignite failIgnite = ignite(stopIdx);

            ((TcpDiscoverySpi)failIgnite.configuration().getDiscoverySpi()).simulateNodeFailure();

            for (int i = 0; i < NODES; i++) {
                if (i != stopIdx) {
                    final Ignite ignite = ignite(i);

                    GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return ignite.cluster().topologyVersion() >= NODES + 1;
                        }
                    }, 10_000);

                    TcpDiscoverySpi spi = (TcpDiscoverySpi)ignite.configuration().getDiscoverySpi();

                    TcpDiscoveryStatistics stats = GridTestUtils.getFieldValue(spi, "stats");

                    Integer cnt = stats.sentMessages().get(TcpDiscoveryNodeFailedMessage.class.getSimpleName());

                    log.info("Count1: " + cnt);

                    assertTrue("Invalid message count: " + cnt, cnt == null || cnt <= 2);

                    cnt = stats.receivedMessages().get(TcpDiscoveryNodeFailedMessage.class.getSimpleName());

                    log.info("Count2: " + cnt);

                    assertTrue("Invalid message count: " + cnt, cnt == null || cnt <= 2);
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDuplicatedDiscoveryDataRemoved() throws Exception {
        try {
            TestDiscoveryDataDuplicateSpi.checkNodeAdded = false;
            TestDiscoveryDataDuplicateSpi.checkClientNodeAddFinished = false;
            TestDiscoveryDataDuplicateSpi.fail = false;

            ccfgs = new CacheConfiguration[5];

            for (int i = 0; i < ccfgs.length; i++) {
                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setName(i == 0 ? null : ("static-cache-" + i));

                ccfgs[i] = ccfg;
            }

            TestDiscoveryDataDuplicateSpi spi = new TestDiscoveryDataDuplicateSpi();

            nodeSpi.set(spi);

            startGrid(0);

            for (int i = 0; i < 5; i++) {
                nodeSpi.set(new TestDiscoveryDataDuplicateSpi());

                startGrid(i + 1);
            }

            client = true;

            Ignite clientNode = startGrid(6);

            assertTrue(clientNode.configuration().isClientMode());

            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName("c1");

            clientNode.createCache(ccfg);

            client = false;

            nodeSpi.set(new TestDiscoveryDataDuplicateSpi());

            startGrid(7);

            assertTrue(TestDiscoveryDataDuplicateSpi.checkNodeAdded);
            assertTrue(TestDiscoveryDataDuplicateSpi.checkClientNodeAddFinished);
            assertFalse(TestDiscoveryDataDuplicateSpi.fail);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param nodeName Node name.
     * @throws Exception If failed.
     */
    private void waitNodeStop(final String nodeName) throws Exception {
        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    Ignition.ignite(nodeName);

                    return false;
                }
                catch (IgniteIllegalStateException ignored) {
                    return true;
                }
            }
        }, 30_000);

        if (!wait)
            U.dumpThreads(log);

        assertTrue("Failed to wait for node stop.", wait);
    }

    /**
     * @param expNodes Expected nodes number.
     */
    private void tryCreateCache(int expNodes) {
        List<Ignite> allNodes = G.allGrids();

        assertEquals(expNodes, allNodes.size());

        int cntr = 0;

        for (Ignite ignite : allNodes) {
            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

            ccfg.setName("cache-" + cntr++);

            log.info("Try create cache [node=" + ignite.name() + ", cache=" + ccfg.getName() + ']');

            ignite.getOrCreateCache(ccfg).put(1, 1);
        }
    }

    /**
     *
     */
    static class DummyPredicate implements IgniteBiPredicate<UUID, Object> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Object o) {
            return true;
        }
    }

    /**
     *
     */
    private static class TestDiscoveryDataDuplicateSpi extends TcpDiscoverySpi {
        /** */
        static volatile boolean fail;

        /** */
        static volatile boolean checkNodeAdded;

        /** */
        static volatile boolean checkClientNodeAddFinished;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                Map<UUID, Map<Integer, byte[]>> discoData = ((TcpDiscoveryNodeAddedMessage)msg).oldNodesDiscoveryData();

                checkDiscoData(discoData, msg);
            }
            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                Map<UUID, Map<Integer, byte[]>> discoData = ((TcpDiscoveryNodeAddFinishedMessage)msg).clientDiscoData();

                checkDiscoData(discoData, msg);
            }

            super.writeToSocket(sock, out, msg, timeout);
        }

        /**
         * @param discoData Discovery data.
         * @param msg Message.
         */
        private void checkDiscoData(Map<UUID, Map<Integer, byte[]>> discoData, TcpDiscoveryAbstractMessage msg) {
            if (discoData != null && discoData.size() > 1) {
                int cnt = 0;

                for (Map.Entry<UUID, Map<Integer, byte[]>> e : discoData.entrySet()) {
                    Map<Integer, byte[]> map = e.getValue();

                    if (map.containsKey(GridComponent.DiscoveryDataExchangeType.CACHE_PROC.ordinal()))
                        cnt++;
                }

                if (cnt > 1) {
                    fail = true;

                    log.error("Expect cache data only from one node, but actually: " + cnt);
                }

                if (msg instanceof TcpDiscoveryNodeAddedMessage)
                    checkNodeAdded = true;
                else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                    checkClientNodeAddFinished = true;
            }
        }
    }


    /**
     *
     */
    private static class TestEventDiscardSpi extends TcpDiscoverySpi {
        /** */
        private ConcurrentHashSet<IgniteUuid> msgIds = new ConcurrentHashSet<>();

        /** */
        private volatile boolean checkDuplicates;

        /** */
        private volatile boolean failed;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            boolean add = msgIds.add(msg.id());

            if (checkDuplicates && !add && !(msg instanceof TcpDiscoveryHeartbeatMessage)) {
                log.error("Send duplicated message: " + msg);

                failed = true;
            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class TestCustomerEventAckSpi extends TcpDiscoverySpi {
        /** */
        private volatile boolean stopBeforeSndAck;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (stopBeforeSndAck) {
                if (msg instanceof TcpDiscoveryCustomEventMessage) {
                    try {
                        DiscoveryCustomMessage custMsg = GridTestUtils.getFieldValue(
                            ((TcpDiscoveryCustomEventMessage)msg).message(marshaller(), U.gridClassLoader()), "delegate");

                        if (custMsg instanceof StartRoutineAckDiscoveryMessage) {
                            log.info("Skip message send and stop node: " + msg);

                            sock.close();

                            GridTestUtils.runAsync(new Callable<Object>() {
                                @Override public Object call() throws Exception {
                                    ignite.close();

                                    return null;
                                }
                            }, "stop-node");

                            return;
                        }
                    }
                    catch (Throwable e) {
                        fail("Unexpected error: " + e);
                    }
                }
            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     * Simulate scenario when node detects node failure trying to send message, but node still alive.
     */
    private static class TestFailedNodesSpi extends TcpDiscoverySpi {
        /** */
        private AtomicBoolean failMsg = new AtomicBoolean();

        /** */
        private int failOrder;

        /** */
        private boolean stopBeforeSndFail;

        /** */
        private boolean stop;

        /** */
        private volatile boolean failSingleMsg;

        /**
         * @param failOrder Spi fails connection if local node order equals to this order.
         */
        TestFailedNodesSpi(int failOrder) {
            this.failOrder = failOrder;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (stop)
                return;

            if (failSingleMsg) {
                failSingleMsg = false;

                log.info("IO error on message send [locNode=" + locNode + ", msg=" + msg + ']');

                sock.close();

                throw new SocketTimeoutException();
            }

            if (locNode.internalOrder() == failOrder &&
                (msg instanceof TcpDiscoveryNodeAddedMessage) &&
                failMsg.compareAndSet(false, true)) {
                log.info("IO error on message send [locNode=" + locNode + ", msg=" + msg + ']');

                sock.close();

                throw new SocketTimeoutException();
            }

            if (stopBeforeSndFail &&
                locNode.internalOrder() == failOrder &&
                (msg instanceof TcpDiscoveryNodeFailedMessage)) {
                stop = true;

                log.info("Skip messages send and stop node [locNode=" + locNode + ", msg=" + msg + ']');

                sock.close();

                GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        ignite.close();

                        return null;
                    }
                }, "stop-node");

                return;
            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class TestCustomEventCoordinatorFailureSpi extends TcpDiscoverySpi {
        /** */
        private volatile CountDownLatch latch;

        /** */
        private boolean stop;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryCustomEventMessage && latch != null) {
                log.info("Stop node on custom event: " + msg);

                latch.countDown();

                stop = true;
            }

            if (stop)
                return;

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class TestCustomEventRaceSpi extends TcpDiscoverySpi {
        /** */
        private volatile CountDownLatch nodeAdded1;

        /** */
        private volatile CountDownLatch nodeAdded2;

        /** */
        private volatile boolean stop;

        /** */
        private boolean debug;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                if (nodeAdded1 != null) {
                    nodeAdded1.countDown();

                    if (debug)
                        log.info("--- Wait node added: " + msg);

                    U.await(nodeAdded2);

                    nodeAdded1 = null;
                    nodeAdded2 = null;
                }

                if (stop)
                    return;

                if (debug)
                    log.info("--- Send node added: " + msg);
            }

            if (debug && msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                log.info("--- Send node finished: " + msg);

            if (debug && msg instanceof TcpDiscoveryCustomEventMessage)
                log.info("--- Send custom event: " + msg);

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class TestMessageWorkerFailureSpi1 extends TcpDiscoverySpi {
        /** */
        private static int EXCEPTION_MODE = 0;

        /** */
        private static int SEGMENTATION_MODE = 1;

        /** */
        private final int failureMode;

        /** */
        private volatile boolean stop;

        /**
         * @param failureMode Failure mode to use during the test.
         */
        public TestMessageWorkerFailureSpi1(int failureMode) {
            this.failureMode = failureMode;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {

            if (stop) {
                if (failureMode == EXCEPTION_MODE)
                    throw new RuntimeException("Failing ring message worker explicitly");
                else {
                    try {
                        Thread.sleep(5_000);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }
                }

            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class TestMessageWorkerFailureSpi2 extends TcpDiscoverySpi {
        /** */
        private volatile boolean stop;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (stop)
                throw new RuntimeException("Failing ring message worker explicitly");

            super.writeToSocket(sock, out, msg, timeout);

            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                stop = true;
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
