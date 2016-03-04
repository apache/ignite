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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(CACHE_NAME1);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
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
    public void testAffinityMultipleJoin1() throws Exception {
        affinityAssignmentMultipleJoin(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityMultipleJoin2() throws Exception {
        affinityAssignmentMultipleJoin(4);
    }

    /**
     * @param joinCnt Number of joining nodes.
     * @throws Exception If failed.
     */
    private void affinityAssignmentMultipleJoin(int joinCnt) throws Exception {
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

        List<IgniteInternalFuture<?>> futs3_1 = affFutures(majorVer, topVer(majorVer, 1));

        U.sleep(1000);

        for (IgniteInternalFuture<?> fut : futs3_1)
            assertFalse(fut.isDone());

        spi.stopBlock();

        checkAffinity(majorVer, topVer(majorVer, 1), true);

        for (IgniteInternalFuture<?> fut : futs3_1)
            assertTrue(fut.isDone());

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityNodeLeave1() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testAffinityNodeLeave2() throws Exception {
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
     * @throws Exception If failed.
     */
    public void _testDelayedAffinityAssignment1() throws Exception {
        startGrid(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        //ccfg.setNodeFilter(new CachePredicate());
        ccfg.setAffinity(new FairAffinityFunction());

        ignite(0).createCache(ccfg);

        startGrid(1);

        IgniteCache cache = ignite(1).cache(null);

        for (int i = 0 ; i < 100; i++) {
            cache.put(1, 1);

            assertNotNull(cache.get(1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testDelayedAffinityAssignment2() throws Exception {
        startGrid(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setNodeFilter(new CachePredicate());

        ignite(0).createCache(ccfg);

        startGrid(1);

//        client = true;
//
//        Ignite client = startGrid(2);
//
//        IgniteCache cache = client.cache(null);
//
//        for (int i = 0; i < 100; i++) {
//            cache.put(1, 1);
//
//            assertNotNull(cache.get(1));
//        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testDelayedAffinityAssignment3() throws Exception {
        startGrid(0);

        ignite(0).createCache(new CacheConfiguration<>());

        startGrid(1);

        client = true;

        Ignite client = startGrid(2);

        IgniteCache cache = client.cache(null);

        for (int i = 0 ; i < 100; i++) {
            cache.put(1, 1);

            assertNotNull(cache.get(1));
        }


        //ignite(0).createCache(new CacheConfiguration<Object, Object>());
//        final CacheConfiguration ccfg = new CacheConfiguration();
//
//        ignite(0).createCache(ccfg);
//
//        for (int i = 0; i < NODES; i++) {
//            Ignite ignite = ignite(i);
//
//            TestRecordingCommunicationSpi spi =
//                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();
//
//            spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
//                @Override public boolean apply(GridIoMessage ioMsg) {
//                    if (!ioMsg.message().getClass().equals(GridDhtPartitionSupplyMessageV2.class))
//                        return false;
//
//                    GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();
//
//                    return msg.cacheId() == CU.cacheId(ccfg.getName());
//                }
//            });
//        }
//
//        startGrid(NODES);
//
//        Ignite ignite = ignite(0);
//
//        Affinity aff = ignite.affinity(ccfg.getName());
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
     * @param expNodes Expected nodes number.
     * @param topVer Topology version.
     * @param expIdeal If {@code true} expect ideal affinity assignment.
     */
    private void checkAffinity(int expNodes, AffinityTopologyVersion topVer, boolean expIdeal) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        Map<String, List<List<ClusterNode>>> aff = new HashMap<>();

        for (Ignite node : nodes) {
            assertEquals(expNodes, node.cluster().nodes().size());

            for (GridCacheContext cctx : ((IgniteKernal)node).context().cache().context().cacheContexts()) {
                List<List<ClusterNode>> aff1 = aff.get(cctx.name());
                List<List<ClusterNode>> aff2 = cctx.affinity().assignments(topVer);

                if (aff1 == null)
                    aff.put(cctx.name(), aff2);
                else
                    assertEquals(aff1, aff2);

                if (expIdeal) {
                    AffinityFunction func = cctx.config().getAffinity();

                    AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(
                        new ArrayList<>(node.cluster().nodes()),
                        null,
                        null,
                        topVer,
                        cctx.config().getBackups());

                    List<List<ClusterNode>> ideal = func.assignPartitions(ctx);

                    assertEquals(ideal, aff2);
                }
            }
        }
    }

    /**
     *
     */
    static class CachePredicate implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            String name = clusterNode.attribute(IgniteNodeAttributes.ATTR_GRID_NAME).toString();

            return !name.endsWith("0");
        }
    }
}
