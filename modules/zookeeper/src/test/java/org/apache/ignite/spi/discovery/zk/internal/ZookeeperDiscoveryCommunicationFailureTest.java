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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureContext;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.TestCacheNodeExcludingFilter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoveryCommunicationFailureTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoOpCommunicationFailureResolve_1() throws Exception {
        communicationFailureResolve_Simple(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoOpCommunicationErrorResolve_2() throws Exception {
        communicationFailureResolve_Simple(10);
    }

    /**
     * @param nodes Nodes number.
     * @throws Exception If failed.
     */
    private void communicationFailureResolve_Simple(int nodes) throws Exception {
        assert nodes > 1;

        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(nodes);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 3; i++) {
            info("Iteration: " + i);

            int idx1 = rnd.nextInt(nodes);

            int idx2;

            do {
                idx2 = rnd.nextInt(nodes);
            }
            while (idx1 == idx2);

            ZookeeperDiscoverySpi spi = spi(ignite(idx1));

            spi.resolveCommunicationFailure(ignite(idx2).cluster().localNode(), new Exception("test"));

            checkInternalStructuresCleanup();
        }
    }

    /**
     * Tests case when one node fails before sending communication status.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoOpCommunicationErrorResolve_3() throws Exception {
        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(3);

        sesTimeout = 10_000;

        testSockNio = true;
        sesTimeout = 5000;

        startGrid(3);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                ZookeeperDiscoverySpi spi = spi(ignite(0));

                spi.resolveCommunicationFailure(ignite(1).cluster().localNode(), new Exception("test"));

                return null;
            }
        });

        U.sleep(1000);

        ZkTestClientCnxnSocketNIO nio = ZkTestClientCnxnSocketNIO.forNode(ignite(3));

        nio.closeSocket(true);

        try {
            stopGrid(3);

            fut.get();
        }
        finally {
            nio.allowConnect();
        }

        waitForTopology(3);
    }

    /**
     * Tests case when Coordinator fails while resolve process is in progress.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoOpCommunicationErrorResolve_4() throws Exception {
        testCommSpi = true;

        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGrid(0);

        startGridsMultiThreaded(1, 3);

        ZkTestCommunicationSpi commSpi = ZkTestCommunicationSpi.testSpi(ignite(3));

        commSpi.pingLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                ZookeeperDiscoverySpi spi = spi(ignite(1));

                spi.resolveCommunicationFailure(ignite(2).cluster().localNode(), new Exception("test"));

                return null;
            }
        });

        U.sleep(1000);

        assertFalse(fut.isDone());

        stopGrid(0);

        commSpi.pingLatch.countDown();

        fut.get();

        waitForTopology(3);
    }

    /**
     * Tests that nodes join is delayed while resolve is in progress.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoOpCommunicationErrorResolve_5() throws Exception {
        testCommSpi = true;

        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGrid(0);

        startGridsMultiThreaded(1, 3);

        ZkTestCommunicationSpi commSpi = ZkTestCommunicationSpi.testSpi(ignite(3));

        commSpi.pingStartLatch = new CountDownLatch(1);
        commSpi.pingLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                ZookeeperDiscoverySpi spi = spi(ignite(1));

                spi.resolveCommunicationFailure(ignite(2).cluster().localNode(), new Exception("test"));

                return null;
            }
        });

        assertTrue(commSpi.pingStartLatch.await(10, SECONDS));

        try {
            assertFalse(fut.isDone());

            final AtomicInteger nodeIdx = new AtomicInteger(3);

            IgniteInternalFuture<?> startFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(nodeIdx.incrementAndGet());

                    return null;
                }
            }, 3, "start-node");

            U.sleep(1000);

            assertFalse(startFut.isDone());

            assertEquals(4, ignite(0).cluster().nodes().size());

            commSpi.pingLatch.countDown();

            startFut.get();
            fut.get();

            waitForTopology(7);
        }
        finally {
            commSpi.pingLatch.countDown();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillNode_1() throws Exception {
        communicationFailureResolve_KillNodes(2, Collections.singleton(2L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillNode_2() throws Exception {
        communicationFailureResolve_KillNodes(3, Collections.singleton(2L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillNode_3() throws Exception {
        communicationFailureResolve_KillNodes(10, Arrays.asList(2L, 4L, 6L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillCoordinator_1() throws Exception {
        communicationFailureResolve_KillNodes(2, Collections.singleton(1L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillCoordinator_2() throws Exception {
        communicationFailureResolve_KillNodes(3, Collections.singleton(1L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillCoordinator_3() throws Exception {
        communicationFailureResolve_KillNodes(10, Arrays.asList(1L, 4L, 6L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationErrorResolve_KillCoordinator_4() throws Exception {
        communicationFailureResolve_KillNodes(10, Arrays.asList(1L, 2L, 3L));
    }

    /**
     * @param startNodes Number of nodes to start.
     * @param killNodes Nodes to kill by resolve process.
     * @throws Exception If failed.
     */
    private void communicationFailureResolve_KillNodes(int startNodes, Collection<Long> killNodes) throws Exception {
        testCommSpi = true;

        commFailureRslvr = TestNodeKillCommunicationFailureResolver.factory(killNodes);

        startGrids(startNodes);

        ZkTestCommunicationSpi commSpi = ZkTestCommunicationSpi.testSpi(ignite(0));

        commSpi.checkRes = new BitSet(startNodes);

        ZookeeperDiscoverySpi spi = null;
        UUID killNodeId = null;

        for (Ignite node : G.allGrids()) {
            ZookeeperDiscoverySpi spi0 = spi(node);

            if (!killNodes.contains(node.cluster().localNode().order()))
                spi = spi0;
            else
                killNodeId = node.cluster().localNode().id();
        }

        assertNotNull(spi);
        assertNotNull(killNodeId);

        try {
            spi.resolveCommunicationFailure(spi.getNode(killNodeId), new Exception("test"));

            fail("Exception is not thrown");
        }
        catch (IgniteSpiException e) {
            assertTrue("Unexpected exception: " + e, e.getCause() instanceof ClusterTopologyCheckedException);
        }

        int expNodes = startNodes - killNodes.size();

        waitForTopology(expNodes);

        for (Ignite node : G.allGrids())
            assertFalse(killNodes.contains(node.cluster().localNode().order()));

        startGrid(startNodes);

        waitForTopology(expNodes + 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationFailureResolve_KillCoordinator_5() throws Exception {
        sesTimeout = 2000;

        testCommSpi = true;
        commFailureRslvr = KillCoordinatorCommunicationFailureResolver.FACTORY;

        startGrids(10);

        int crd = 0;

        int nodeIdx = 10;

        for (int i = 0; i < GridTestUtils.SF.applyLB(4, 2); i++) {
            info("Iteration: " + i);

            for (Ignite node : G.allGrids())
                ZkTestCommunicationSpi.testSpi(node).initCheckResult(10);

            UUID crdId = ignite(crd).cluster().localNode().id();

            ZookeeperDiscoverySpi spi = spi(ignite(crd + 1));

            try {
                spi.resolveCommunicationFailure(spi.getNode(crdId), new Exception("test"));

                fail("Exception is not thrown");
            }
            catch (IgniteSpiException e) {
                assertTrue("Unexpected exception: " + e, e.getCause() instanceof ClusterTopologyCheckedException);
            }

            waitForTopology(9);

            startGrid(nodeIdx++);

            waitForTopology(10);

            crd++;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10988")
    @Test
    public void testCommunicationFailureResolve_KillRandom() throws Exception {
        sesTimeout = 2000;

        testCommSpi = true;
        commFailureRslvr = KillRandomCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(10);

        startClientGridsMultiThreaded(10, 5);

        int nodesCnt = 15;

        waitForTopology(nodesCnt);

        int nodeIdx = 15;

        for (int i = 0; i < GridTestUtils.SF.applyLB(10, 2); i++) {
            info("Iteration: " + i);

            ZookeeperDiscoverySpi spi = null;

            for (Ignite node : G.allGrids()) {
                ZkTestCommunicationSpi.testSpi(node).initCheckResult(100);

                spi = spi(node);
            }

            assert spi != null;

            try {
                spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));
            }
            catch (IgniteSpiException ignore) {
                // No-op.
            }

            if (ThreadLocalRandom.current().nextBoolean())
                startClientGrid(nodeIdx++);
            else
                startGrid(nodeIdx++);

            nodesCnt = nodesCnt - KillRandomCommunicationFailureResolver.LAST_KILLED_NODES.size() + 1;

            waitForTopology(nodesCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCommunicationFailureResolver1() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        startGrids(3);

        ZkTestCommunicationSpi.testSpi(ignite(0)).initCheckResult(3, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(1)).initCheckResult(3, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(2)).initCheckResult(3, 2);

        UUID killedId = nodeId(2);

        assertNotNull(ignite(0).cluster().node(killedId));

        ZookeeperDiscoverySpi spi = spi(ignite(0));

        spi.resolveCommunicationFailure(spi.getNode(ignite(1).cluster().localNode().id()), new Exception("test"));

        waitForTopology(2);

        assertNull(ignite(0).cluster().node(killedId));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCommunicationFailureResolver2() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        startGrids(3);

        startClientGridsMultiThreaded(3, 2);

        ZkTestCommunicationSpi.testSpi(ignite(0)).initCheckResult(5, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(1)).initCheckResult(5, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(2)).initCheckResult(5, 2, 3, 4);
        ZkTestCommunicationSpi.testSpi(ignite(3)).initCheckResult(5, 2, 3, 4);
        ZkTestCommunicationSpi.testSpi(ignite(4)).initCheckResult(5, 2, 3, 4);

        ZookeeperDiscoverySpi spi = spi(ignite(0));

        spi.resolveCommunicationFailure(spi.getNode(ignite(1).cluster().localNode().id()), new Exception("test"));

        waitForTopology(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCommunicationFailureResolver3() throws Exception {
        defaultCommunicationFailureResolver_BreakCommunication(3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCommunicationFailureResolver4() throws Exception {
        defaultCommunicationFailureResolver_BreakCommunication(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCommunicationFailureResolver5() throws Exception {
        defaultCommunicationFailureResolver_BreakCommunication(10, 1, 3, 6);
    }

    /**
     * @param startNodes Initial nodes number.
     * @param breakNodes Node indices where communication server is closed.
     * @throws Exception If failed.
     */
    private void defaultCommunicationFailureResolver_BreakCommunication(int startNodes, final int...breakNodes) throws Exception {
        sesTimeout = 5000;

        startGridsMultiThreaded(startNodes);

        final CyclicBarrier b = new CyclicBarrier(breakNodes.length);

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer threadIdx) {
                try {
                    b.await();

                    int nodeIdx = breakNodes[threadIdx];

                    info("Close communication: " + nodeIdx);

                    ((TcpCommunicationSpi)ignite(nodeIdx).configuration().getCommunicationSpi()).simulateNodeFailure();
                }
                catch (Exception e) {
                    fail("Unexpected error: " + e);
                }
            }
        }, breakNodes.length, "break-communication");

        waitForTopology(startNodes - breakNodes.length);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationFailureResolve_CachesInfo1() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        final CacheInfoCommunicationFailureResolver rslvr = new CacheInfoCommunicationFailureResolver();

        commFailureRslvr = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return rslvr;
            }
        };

        startGrids(2);

        awaitPartitionMapExchange();

        Map<String, T3<Integer, Integer, Integer>> expCaches = new HashMap<>();

        expCaches.put(DEFAULT_CACHE_NAME, new T3<>(RendezvousAffinityFunction.DFLT_PARTITION_COUNT, 0, 1));

        checkResolverCachesInfo(ignite(0), expCaches);

        List<CacheConfiguration> caches = new ArrayList<>();

        CacheConfiguration c1 = new CacheConfiguration("c1");
        c1.setBackups(1);
        c1.setAffinity(new RendezvousAffinityFunction(false, 64));
        caches.add(c1);

        CacheConfiguration c2 = new CacheConfiguration("c2");
        c2.setBackups(2);
        c2.setAffinity(new RendezvousAffinityFunction(false, 128));
        caches.add(c2);

        CacheConfiguration c3 = new CacheConfiguration("c3");
        c3.setCacheMode(CacheMode.REPLICATED);
        c3.setAffinity(new RendezvousAffinityFunction(false, 256));
        caches.add(c3);

        ignite(0).createCaches(caches);

        expCaches.put("c1", new T3<>(64, 1, 2));
        expCaches.put("c2", new T3<>(128, 2, 2));
        expCaches.put("c3", new T3<>(256, 1, 2));

        checkResolverCachesInfo(ignite(0), expCaches);

        startGrid(2);
        startGrid(3);

        awaitPartitionMapExchange();

        expCaches.put("c2", new T3<>(128, 2, 3));
        expCaches.put("c3", new T3<>(256, 1, 4));

        checkResolverCachesInfo(ignite(0), expCaches);

        CacheConfiguration<Object, Object> c4 = new CacheConfiguration<>("c4");
        c4.setCacheMode(CacheMode.PARTITIONED);
        c4.setBackups(0);
        c4.setAffinity(new RendezvousAffinityFunction(false, 256));
        c4.setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0), getTestIgniteInstanceName(1)));

        ignite(2).createCache(c4);

        expCaches.put("c4", new T3<>(256, 0, 1));

        checkResolverCachesInfo(ignite(0), expCaches);

        stopGrid(0); // Stop current coordinator, check new coordinator will initialize required caches information.

        awaitPartitionMapExchange();

        expCaches.put("c3", new T3<>(256, 1, 3));

        checkResolverCachesInfo(ignite(1), expCaches);

        startGrid(0);

        expCaches.put("c3", new T3<>(256, 1, 4));

        checkResolverCachesInfo(ignite(1), expCaches);

        stopGrid(1);

        expCaches.put("c3", new T3<>(256, 1, 3));

        checkResolverCachesInfo(ignite(3), expCaches);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationFailureResolve_CachesInfo2() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        final CacheInfoCommunicationFailureResolver rslvr = new CacheInfoCommunicationFailureResolver();

        commFailureRslvr = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return rslvr;
            }
        };

        Ignite srv0 = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("c1");
        ccfg.setBackups(1);

        srv0.createCache(ccfg);

        // Block rebalance to make sure node0 will be the only owner.
        TestRecordingCommunicationSpi.spi(srv0).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage &&
                    ((GridDhtPartitionSupplyMessage) msg).groupId() == CU.cacheId("c1");
            }
        });

        startGrid(1);

        U.sleep(1000);

        ZookeeperDiscoverySpi spi = spi(srv0);

        rslvr.latch = new CountDownLatch(1);

        ZkTestCommunicationSpi.testSpi(srv0).initCheckResult(2, 0);

        spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));

        assertTrue(rslvr.latch.await(10, SECONDS));

        List<List<ClusterNode>> cacheOwners = rslvr.ownersMap.get("c1");

        ClusterNode node0 = srv0.cluster().localNode();

        for (int p = 0; p < RendezvousAffinityFunction.DFLT_PARTITION_COUNT; p++) {
            List<ClusterNode> owners = cacheOwners.get(p);

            assertEquals(1, owners.size());
            assertEquals(node0, owners.get(0));
        }

        TestRecordingCommunicationSpi.spi(srv0).stopBlock();

        awaitPartitionMapExchange();

        Map<String, T3<Integer, Integer, Integer>> expCaches = new HashMap<>();

        expCaches.put(DEFAULT_CACHE_NAME, new T3<>(RendezvousAffinityFunction.DFLT_PARTITION_COUNT, 0, 1));
        expCaches.put("c1", new T3<>(RendezvousAffinityFunction.DFLT_PARTITION_COUNT, 1, 2));

        checkResolverCachesInfo(srv0, expCaches);
    }

    /**
     * @param crd Coordinator node.
     * @param expCaches Expected caches info.
     * @throws Exception If failed.
     */
    private void checkResolverCachesInfo(Ignite crd, Map<String, T3<Integer, Integer, Integer>> expCaches)
        throws Exception
    {
        CacheInfoCommunicationFailureResolver rslvr =
            (CacheInfoCommunicationFailureResolver)crd.configuration().getCommunicationFailureResolver();

        assertNotNull(rslvr);

        ZookeeperDiscoverySpi spi = spi(crd);

        rslvr.latch = new CountDownLatch(1);

        ZkTestCommunicationSpi.testSpi(crd).initCheckResult(crd.cluster().nodes().size(), 0);

        spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));

        assertTrue(rslvr.latch.await(10, SECONDS));

        rslvr.checkCachesInfo(expCaches);

        rslvr.reset();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCommunicationFailureResolve_ConcurrentDiscoveyEvents() throws Exception {
        sesTimeout = 5000;

        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        final int INIT_NODES = 5;

        startGridsMultiThreaded(INIT_NODES);

        final CyclicBarrier b = new CyclicBarrier(4);

        GridCompoundFuture<?, ?> fut = new GridCompoundFuture<>();

        final AtomicBoolean stop = new AtomicBoolean();

        fut.add((IgniteInternalFuture)GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                b.await();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 10; i++) {
                    startGrid(i + INIT_NODES);

                    //noinspection BusyWait
                    Thread.sleep(rnd.nextLong(1000) + 10);

                    if (stop.get())
                        break;
                }

                return null;
            }
        }, "test-node-start"));

        fut.add((IgniteInternalFuture)GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                b.await();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    startGrid(100);

                    //noinspection BusyWait
                    Thread.sleep(rnd.nextLong(1000) + 10);

                    stopGrid(100);

                    //noinspection BusyWait
                    Thread.sleep(rnd.nextLong(1000) + 10);
                }

                return null;
            }
        }, "test-node-restart"));

        fut.add((IgniteInternalFuture)GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                b.await();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int idx = 0;

                while (!stop.get()) {
                    CacheConfiguration ccfg = new CacheConfiguration("c-" + idx++);
                    ccfg.setBackups(rnd.nextInt(5));

                    ignite(rnd.nextInt(INIT_NODES)).createCache(ccfg);

                    //noinspection BusyWait
                    Thread.sleep(rnd.nextLong(1000) + 10);

                    ignite(rnd.nextInt(INIT_NODES)).destroyCache(ccfg.getName());

                    //noinspection BusyWait
                    Thread.sleep(rnd.nextLong(1000) + 10);
                }

                return null;
            }
        }, "test-create-cache"));

        fut.add((IgniteInternalFuture)GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    b.await();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 5; i++) {
                        info("resolveCommunicationFailure: " + i);

                        ZookeeperDiscoverySpi spi = spi(ignite(rnd.nextInt(INIT_NODES)));

                        spi.resolveCommunicationFailure(ignite(rnd.nextInt(INIT_NODES)).cluster().localNode(),
                            new Exception("test"));
                    }

                    return null;
                }
                finally {
                    stop.set(true);
                }
            }
        }, 5, "test-resolve-failure"));

        fut.markInitialized();

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommunicationFailureResolve_ConcurrentMultinode() throws Exception {
        sesTimeout = 5000;

        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(5);

        startClientGridsMultiThreaded(5, 5);

        final int NODES = 10;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 5; i++) {
                    info("resolveCommunicationFailure: " + i);

                    ZookeeperDiscoverySpi spi = spi(ignite(rnd.nextInt(NODES)));

                    spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));
                }

                return null;
            }
        }, 30, "test-resolve-failure");
    }

    /** */
    private static class CacheInfoCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        Map<String, CacheConfiguration<?, ?>> caches;

        /** */
        Map<String, List<List<ClusterNode>>> affMap;

        /** */
        Map<String, List<List<ClusterNode>>> ownersMap;

        /** */
        volatile CountDownLatch latch;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            assert latch != null;
            assert latch.getCount() == 1L : latch.getCount();

            caches = ctx.startedCaches();

            log.info("Resolver called, started caches: " + caches.keySet());

            assertNotNull(caches);

            affMap = new HashMap<>();
            ownersMap = new HashMap<>();

            for (String cache : caches.keySet()) {
                affMap.put(cache, ctx.cacheAffinity(cache));
                ownersMap.put(cache, ctx.cachePartitionOwners(cache));
            }

            latch.countDown();
        }

        /**
         * @param expCaches Expected caches information (when late assignment doen and rebalance finished).
         */
        void checkCachesInfo(Map<String, T3<Integer, Integer, Integer>> expCaches) {
            assertNotNull(caches);
            assertNotNull(affMap);
            assertNotNull(ownersMap);

            for (Map.Entry<String, T3<Integer, Integer, Integer>> e : expCaches.entrySet()) {
                String cacheName = e.getKey();

                int parts = e.getValue().get1();
                int backups = e.getValue().get2();
                int expNodes = e.getValue().get3();

                assertTrue(cacheName, caches.containsKey(cacheName));

                CacheConfiguration ccfg = caches.get(cacheName);

                assertEquals(cacheName, ccfg.getName());

                if (ccfg.getCacheMode() == CacheMode.REPLICATED)
                    assertEquals(Integer.MAX_VALUE, ccfg.getBackups());
                else
                    assertEquals(backups, ccfg.getBackups());

                assertEquals(parts, ccfg.getAffinity().partitions());

                List<List<ClusterNode>> aff = affMap.get(cacheName);

                assertNotNull(cacheName, aff);
                assertEquals(parts, aff.size());

                List<List<ClusterNode>> owners = ownersMap.get(cacheName);

                assertNotNull(cacheName, owners);
                assertEquals(parts, owners.size());

                for (int i = 0; i < parts; i++) {
                    List<ClusterNode> partAff = aff.get(i);

                    assertEquals(cacheName, expNodes, partAff.size());

                    List<ClusterNode> partOwners = owners.get(i);

                    assertEquals(cacheName, expNodes, partOwners.size());

                    assertTrue(cacheName, partAff.containsAll(partOwners));
                    assertTrue(cacheName, partOwners.containsAll(partAff));
                }
            }
        }

        /** */
        void reset() {
            caches = null;
            affMap = null;
            ownersMap = null;
        }
    }

    /** */
    private static class NoOpCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        static final IgniteOutClosure<CommunicationFailureResolver> FACTORY
            = (IgniteOutClosure<CommunicationFailureResolver>)NoOpCommunicationFailureResolver::new;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            // No-op.
        }
    }

    /** */
    private static class KillCoordinatorCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        static final IgniteOutClosure<CommunicationFailureResolver> FACTORY
            = (IgniteOutClosure<CommunicationFailureResolver>)KillCoordinatorCommunicationFailureResolver::new;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            List<ClusterNode> nodes = ctx.topologySnapshot();

            ClusterNode node = nodes.get(0);

            log.info("Resolver kills node: " + node.id());

            ctx.killNode(node);
        }
    }

    /** */
    private static class KillRandomCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        static final IgniteOutClosure<CommunicationFailureResolver> FACTORY
            = (IgniteOutClosure<CommunicationFailureResolver>)KillRandomCommunicationFailureResolver::new;

        /** Last killed nodes. */
        static final Set<ClusterNode> LAST_KILLED_NODES = new HashSet<>();

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            LAST_KILLED_NODES.clear();

            List<ClusterNode> nodes = ctx.topologySnapshot();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int killNodes = rnd.nextInt(nodes.size() / 2);

            log.info("Resolver kills nodes [total=" + nodes.size() + ", kill=" + killNodes + ']');

            long srvCnt = nodes.stream().filter(node -> !node.isClient()).count();

            Set<Integer> idxs = new HashSet<>();

            while (idxs.size() < killNodes) {
                int idx = rnd.nextInt(nodes.size());

                if (!nodes.get(idx).isClient() && !idxs.contains(idx) && --srvCnt < 1)
                    continue;

                idxs.add(idx);
            }

            for (int idx : idxs) {
                ClusterNode node = nodes.get(idx);

                log.info("Resolver kills node: " + node.id());

                LAST_KILLED_NODES.add(node);

                ctx.killNode(node);
            }
        }
    }

    /** */
    private static class TestNodeKillCommunicationFailureResolver implements CommunicationFailureResolver {
        /**
         * @param killOrders Killed nodes order.
         * @return Factory.
         */
        static IgniteOutClosure<CommunicationFailureResolver> factory(final Collection<Long> killOrders) {
            return new IgniteOutClosure<CommunicationFailureResolver>() {
                @Override public CommunicationFailureResolver apply() {
                    return new TestNodeKillCommunicationFailureResolver(killOrders);
                }
            };
        }

        /** */
        final Collection<Long> killNodeOrders;

        /**
         * @param killNodeOrders Killed nodes order.
         */
        TestNodeKillCommunicationFailureResolver(Collection<Long> killNodeOrders) {
            this.killNodeOrders = killNodeOrders;
        }

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            List<ClusterNode> nodes = ctx.topologySnapshot();

            assertTrue(!nodes.isEmpty());

            for (ClusterNode node : nodes) {
                if (killNodeOrders.contains(node.order()))
                    ctx.killNode(node);
            }
        }
    }
}
