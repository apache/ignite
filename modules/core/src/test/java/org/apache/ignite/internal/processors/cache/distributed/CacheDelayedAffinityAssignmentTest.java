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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;

/**
 *
 */
public class CacheDelayedAffinityAssignmentTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private static final String CACHE_NAME1 = "aff_log_cache";

    /** */
    private IgniteClosure<String, CacheConfiguration> cacheC;

    /** */
    private IgnitePredicate<ClusterNode> cacheNodeFilter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg;

        if (cacheC != null)
            ccfg = cacheC.apply(gridName);
        else
            ccfg = cacheConfiguration();

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME1);
        ccfg.setNodeFilter(cacheNodeFilter);
        ccfg.setAffinity(affinityFunction());

        return ccfg;
    }

    /**
     * @return Affinity function.
     */
    protected AffinityFunction affinityFunction() {
        return new RendezvousAffinityFunction(false, 32);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Simple test, node join.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySequentialStart() throws Exception {
        startGrid(0);

        startGrid(1);

        checkAffinity(2, topVer(2, 0), false);

        checkAffinity(2, topVer(2, 1), true);

        startGrid(2);

        checkAffinity(3, topVer(3, 0), false);

        checkAffinity(3, topVer(3, 1), true);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinitySequentialStartNoCacheOnCoordinator() throws Exception {
        cacheC = new IgniteClosure<String, CacheConfiguration>() {
            @Override public CacheConfiguration apply(String gridName) {
                if (gridName.equals(getTestGridName(0)))
                    return null;

                return cacheConfiguration();
            }
        };

        cacheNodeFilter = new CachePredicate(F.asList(getTestGridName(1), getTestGridName(2)));

        testAffinitySequentialStart();

        assertNull(((IgniteKernal)ignite(0)).context().cache().internalCache(CACHE_NAME1));
    }

    /**
     * Simple test, node leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleNodeLeave() throws Exception {
        startGrid(0);

        startGrid(1);

        checkAffinity(2, topVer(2, 0), false);

        checkAffinity(2, topVer(2, 1), true);

        stopGrid(1);

        checkAffinity(1, topVer(3, 0), false);

        checkNoExchange(1, topVer(3, 1));

        awaitPartitionMapExchange();
    }

    /**
     * Simple test, client node joins/leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleClientNodeEvents1() throws Exception {
        affinitySimpleClientNodeEvents(1);
    }

    /**
     * Simple test, client node joins/leaves.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleClientNodeEvents2() throws Exception {
        affinitySimpleClientNodeEvents(3);
    }

    /**
     * Simple test, client node joins/leaves.
     *
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void affinitySimpleClientNodeEvents(int srvs) throws Exception {
        for (int i = 0; i < srvs; i++)
            startGrid(i);

        if (srvs == 1)
            checkAffinity(srvs, topVer(srvs, 0), true);
        else
            checkAffinity(srvs, topVer(srvs, 1), true);

        startClient(srvs);

        checkAffinity(srvs + 1, topVer(srvs + 1, 0), true);

        stopGrid(srvs);

        checkAffinity(srvs, topVer(srvs + 2, 0), true);
    }

    /**
     * Wait for rebalance, 2 nodes join.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentMultipleJoin1() throws Exception {
        delayAssignmentMultipleJoin(2);
    }

    /**
     * Wait for rebalance, 4 nodes join.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentMultipleJoin2() throws Exception {
        delayAssignmentMultipleJoin(4);
    }

    /**
     * @param joinCnt Number of joining nodes.
     * @throws Exception If failed.
     */
    private void delayAssignmentMultipleJoin(int joinCnt) throws Exception {
        Ignite ignite0 = startGrid(0);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        int majorVer = 1;

        for (int i = 0; i < joinCnt; i++) {
            startGrid(i + 1);

            majorVer++;

            checkAffinity(majorVer, topVer(majorVer, 0), false);
        }

        List<IgniteInternalFuture<?>> futs = affFutures(majorVer, topVer(majorVer, 1));

        U.sleep(1000);

        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());

        spi.stopBlock();

        checkAffinity(majorVer, topVer(majorVer, 1), true);

        for (IgniteInternalFuture<?> fut : futs)
            assertTrue(fut.isDone());

        awaitPartitionMapExchange();
    }

    /**
     * Wait for rebalance, client node joins.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentClientJoin() throws Exception {
        Ignite ignite0 = startGrid(0);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startGrid(1);

        startClient(2);

        checkAffinity(3, topVer(3, 0), false);

        spi.stopBlock();

        checkAffinity(3, topVer(3, 1), true);
    }

    /**
     * Wait for rebalance, client node leaves.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentClientLeave() throws Exception {
        Ignite ignite0 = startGrid(0);

        startClient(1);

        checkAffinity(2, topVer(2, 0), true);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startGrid(2);

        checkAffinity(3, topVer(3, 0), false);

        stopGrid(1);

        checkAffinity(2, topVer(4, 0), false);

        spi.stopBlock();

        checkAffinity(2, topVer(4, 1), true);
    }

    /**
     * Simple test, stop random node.
     *
     * @throws Exception If failed.
     */
    public void testAffinitySimpleStopRandomNode() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            final int NODES = 5;

            for (int i = 0 ; i < 5; i++)
                startGrid(i);

            int majorVer = NODES;

            checkAffinity(majorVer, topVer(majorVer, 1), true);

            Set<Integer> stopOrder = new HashSet<>();

            while (stopOrder.size() != NODES - 1)
                stopOrder.add(ThreadLocalRandom.current().nextInt(NODES));

            int nodes = NODES;

            for (Integer idx : stopOrder) {
                log.info("Stop node: " + idx);

                stopGrid(idx);

                majorVer++;

                checkAffinity(--nodes, topVer(majorVer, 0), false);

                awaitPartitionMapExchange();
            }

            stopAllGrids();
        }
    }

    /**
     * Wait for rebalance, coordinator leaves, 2 nodes.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCoordinatorLeave1() throws Exception {
        Ignite ignite0 = startGrid(0);

        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        blockSupplySend(spi, CACHE_NAME1);

        startGrid(1);

        stopGrid(0);

        checkAffinity(1, topVer(3, 0), true);

        checkNoExchange(1, topVer(3, 1));

        awaitPartitionMapExchange();
    }

    /**
     * Wait for rebalance, coordinator leaves, 3 nodes.
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentCoordinatorLeave2() throws Exception {
        Ignite ignite0 = startGrid(0);

        Ignite ignite1 = startGrid(1);

        checkAffinity(2, topVer(2, 1), true);

        TestRecordingCommunicationSpi spi0 =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();
        TestRecordingCommunicationSpi spi1 =
            (TestRecordingCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        blockSupplySend(spi0, CACHE_NAME1);
        blockSupplySend(spi1, CACHE_NAME1);

        startGrid(2);

        stopGrid(0);

        checkAffinity(2, topVer(4, 0), false);

        spi1.stopBlock();

        checkAffinity(2, topVer(4, 1), true);
    }

    /**
     * Coordinator leaves during node leave exchange.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeftExchangeCoordinatorLeave1() throws Exception {
        nodeLeftExchangeCoordinatorLeave(3);
    }

    /**
     * Coordinator leaves during node leave exchange.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeftExchangeCoordinatorLeave2() throws Exception {
        nodeLeftExchangeCoordinatorLeave(5);
    }

    /**
     * @param nodes Number of nodes.
     * @throws Exception If failed.
     */
    private void nodeLeftExchangeCoordinatorLeave(int nodes) throws Exception {
        assert nodes > 2 : nodes;

        for (int i = 0; i < nodes; i++)
            startGrid(i);

        Ignite ignite1 = grid(1);

        checkAffinity(nodes, topVer(nodes, 1), true);

        TestRecordingCommunicationSpi spi1 =
            (TestRecordingCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        // Prevent exchange finish while node0 is coordinator.
        spi1.blockMessages(GridDhtPartitionsSingleMessage.class, ignite(0).name());

        stopGrid(2); // New exchange started.

        stopGrid(0); // Stop coordinator while exchange in progress.

        checkAffinity(nodes - 2, topVer(nodes + 1, 0), false);

        checkAffinity(nodes - 2, topVer(nodes + 2, 0), true);

        awaitPartitionMapExchange();
    }

    /**
     * Wait for rebalance, send affinity change message, but affinity already changed (new node joined).
     *
     * @throws Exception If failed.
     */
    public void testDelayAssignmentAffinityChanged() throws Exception {
        Ignite ignite0 = startGrid(0);

        TestTcpDiscoverySpi discoSpi0 =
            (TestTcpDiscoverySpi)ignite0.configuration().getDiscoverySpi();
        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        startClient(1);

        checkAffinity(2, topVer(2, 0), true);

        discoSpi0.blockCustomEvent();

        startGrid(2);

        checkAffinity(3, topVer(3, 0), false);

        discoSpi0.waitCustomEvent();

        blockSupplySend(commSpi0, CACHE_NAME1);

        startGrid(3);

        discoSpi0.stopBlock();

        checkAffinity(4, topVer(4, 0), false);

        List<List<ClusterNode>> aff1 = affinity(ignite0, topVer(4, 0), CACHE_NAME1);

        checkAffinity(4, topVer(4, 1), false);

        List<List<ClusterNode>> aff2 = affinity(ignite0, topVer(4, 1), CACHE_NAME1);

        assertEquals(aff1, aff2);

        commSpi0.stopBlock();

        checkAffinity(4, topVer(4, 2), true);
    }

    /**
     * @param node Node.
     * @param topVer Topology version.
     * @param cache Cache name.
     * @return Affinity assignments.
     */
    private List<List<ClusterNode>> affinity(Ignite node, AffinityTopologyVersion topVer, String cache) {
        GridCacheContext cctx = ((IgniteKernal)node).context().cache().internalCache(cache).context();

        return cctx.affinity().assignments(topVer);
    }

    /**
     * @param spi SPI.
     * @param cacheName Cache name.
     */
    private void blockSupplySend(TestRecordingCommunicationSpi spi, final String cacheName) {
        spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
            @Override public boolean apply(GridIoMessage ioMsg) {
                if (!ioMsg.message().getClass().equals(GridDhtPartitionSupplyMessageV2.class))
                    return false;

                GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();

                return msg.cacheId() == CU.cacheId(cacheName);
            }
        });
    }

    /**
     * @param expNodes Expected nodes number.
     * @param topVer Topology version.
     * @return Affinity futures.
     */
    private List<IgniteInternalFuture<?>> affFutures(int expNodes, AffinityTopologyVersion topVer) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        List<IgniteInternalFuture<?>> futs = new ArrayList<>(nodes.size());

        for (Ignite node : nodes) {
            IgniteInternalFuture<?>
                fut = ((IgniteKernal)node).context().cache().context().exchange().affinityReadyFuture(topVer);

            futs.add(fut);
        }

        return futs;
    }

    /**
     * @param major Major version.
     * @param minor Minor version.
     * @return Topology version.
     */
    private static AffinityTopologyVersion topVer(int major, int minor) {
        return new AffinityTopologyVersion(major, minor);
    }

    private void checkAffinityApi(int expNodes) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        for (Ignite node : nodes) {
            Collection<String> cacheNames = node.cacheNames();

            for (String cacheName : cacheNames) {
                Affinity<Object> aff = node.affinity(cacheName);

                assertTrue(aff.partitions() > 0);

                for (int p = 0; p < aff.partitions(); p++) {
                    Collection<ClusterNode> partNodes = aff.mapPartitionToPrimaryAndBackups(p);

                }
            }
        }
    }

    /**
     * @param expNode Expected nodes number.
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void checkNoExchange(int expNode, AffinityTopologyVersion topVer) throws Exception {
        List<IgniteInternalFuture<?>> futs = affFutures(expNode, topVer);

        U.sleep(1000);

        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());
    }

    /**
     * @param expNodes Expected nodes number.
     * @param topVer Topology version.
     * @param expIdeal If {@code true} expect ideal affinity assignment.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkAffinity(int expNodes, AffinityTopologyVersion topVer, boolean expIdeal) throws Exception {
        List<Ignite> nodes = G.allGrids();

        Map<String, List<List<ClusterNode>>> aff = new HashMap<>();

        for (Ignite node : nodes) {
            log.info("Check node: " + node.name());

            IgniteKernal node0 = (IgniteKernal)node;

            for (GridCacheContext cctx : node0.context().cache().context().cacheContexts()) {
                List<List<ClusterNode>> aff1 = aff.get(cctx.name());
                List<List<ClusterNode>> aff2 = cctx.affinity().assignments(topVer);

                if (aff1 == null)
                    aff.put(cctx.name(), aff2);
                else
                    assertEquals(aff1, aff2);

                if (expIdeal) {
                    AffinityFunction func = cctx.config().getAffinity();

                    List<ClusterNode> affNodes = new ArrayList<>();

                    IgnitePredicate<ClusterNode> p = cctx.config().getNodeFilter();

                    for (ClusterNode n : node.cluster().nodes()) {
                        if (!CU.clientNode(n) && (p == null || p.apply(n)))
                            affNodes.add(n);
                    }

                    Collections.sort(affNodes, GridNodeOrderComparator.INSTANCE);

                    AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(
                        affNodes,
                        null,
                        null,
                        topVer,
                        cctx.config().getBackups());

                    List<List<ClusterNode>> ideal = func.assignPartitions(ctx);

                    assertEquals(ideal, aff2);
                }
            }

            IgniteInternalFuture<?> fut = node0.context().cache().context().exchange().affinityReadyFuture(topVer);

            if (fut != null)
                fut.get();

            assertEquals(expNodes, node.cluster().nodes().size());
        }

        assertEquals(expNodes, nodes.size());
    }

    /**
     *
     */
    static class CachePredicate implements IgnitePredicate<ClusterNode> {
        /** */
        private List<String> cacheNodes;

        /**
         * @param cacheNodes Cache nodes names.
         */
        public CachePredicate(List<String> cacheNodes) {
            this.cacheNodes = cacheNodes;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            String name = clusterNode.attribute(ATTR_GRID_NAME).toString();

            return cacheNodes.contains(name);
        }
    }

    /**
     * @param idx Node index.
     * @return Started node.
     * @throws Exception If failed.
     */
    private Ignite startClient(int idx) throws Exception {
        client = true;

        Ignite ignite = startGrid(idx);

        assertTrue(ignite.configuration().isClientMode());

        client = false;

        return ignite;
    }

    /**
     *
     */
    static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private boolean blockCustomEvt;

        /** */
        private final Object mux = new Object();

        /** */
        private List<DiscoverySpiCustomMessage> blockedMsgs = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
            synchronized (mux) {
                if (blockCustomEvt) {
                    DiscoveryCustomMessage msg0 = GridTestUtils.getFieldValue(msg, "delegate");

                    if (msg0 instanceof CacheAffinityChangeMessage) {
                        log.info("Block custom message: " + msg0);

                        blockedMsgs.add(msg);

                        mux.notifyAll();

                        return;
                    }
                }
            }

            super.sendCustomEvent(msg);
        }

        /**
         *
         */
        public void blockCustomEvent() {
            synchronized (mux) {
                assert blockedMsgs.isEmpty() : blockedMsgs;

                blockCustomEvt = true;
            }
        }

        /**
         *
         */
        public void waitCustomEvent() throws InterruptedException {
            synchronized (mux) {
                while (blockedMsgs.isEmpty())
                    mux.wait();
            }
        }

        /**
         *
         */
        public void stopBlock() {
            List<DiscoverySpiCustomMessage> msgs;

            synchronized (this) {
                msgs = new ArrayList<>(blockedMsgs);

                blockCustomEvt = false;

                blockedMsgs.clear();
            }

            for (DiscoverySpiCustomMessage msg : msgs) {
                log.info("Resend blocked message: " + msg);

                super.sendCustomEvent(msg);
            }
        }
    }
}
