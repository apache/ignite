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

package org.apache.ignite.cache;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test cases when rebalance processed and not cancelled during various exchange events.
 */
public class RebalanceCancellationTest extends GridCommonAbstractTest {
    /** Start cluster nodes. */
    public static final int NODES_CNT = 3;

    /** Count of backup partitions. */
    public static final int BACKUPS = 2;

    /** In memory data region name. */
    public static final String MEM_REGION = "mem-region";

    /** In memory cache name. */
    public static final String MEM_REGION_CACHE = DEFAULT_CACHE_NAME + "_mem";

    /** In memory dynamic cache name. */
    public static final String DYNAMIC_CACHE_NAME = DEFAULT_CACHE_NAME + "_dynamic";

    /** Node name suffex. Used for {@link CustomNodeFilter}. */
    public static final String FILTERED_NODE_SUFFIX = "_filtered";

    /** Persistence enabled. */
    public boolean persistenceEnabled;

    /** Add additional non-persistence data region. */
    public boolean addtiotionalMemRegion;

    /** Filter node. */
    public boolean filterNode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)))
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 15))
                    .setBackups(BACKUPS));

        if (addtiotionalMemRegion) {
            cfg.setCacheConfiguration(cfg.getCacheConfiguration()[0],
                new CacheConfiguration(MEM_REGION_CACHE)
                    .setDataRegionName(MEM_REGION)
                    .setBackups(BACKUPS))
                .getDataStorageConfiguration()
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setName(MEM_REGION));
        }

        if (filterNode) {
            for (CacheConfiguration ccfg : cfg.getCacheConfiguration())
                ccfg.setNodeFilter(new CustomNodeFilter());
        }

        return cfg;
    }

    /**
     * Custom node filter. It filters all node that name contains a {@link #FILTERED_NODE_SUFFIX}.
     */
    private static class CustomNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.consistentId().toString().contains(FILTERED_NODE_SUFFIX);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * Non baseline node leaves cluster with only persistent caches during rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceNoneBltNodeLeftOnOnlyPersistenceCluster() throws Exception {
        testRebalanceNoneBltNode(true, false, false);
    }

    /**
     * Non baseline node leaves cluster with only memory caches during rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceNoneBltNodeLeftOnOnlyInMemoryCluster() throws Exception {
        testRebalanceNoneBltNode(false, false, false);
    }

    /**
     * Non baseline node leaves cluster with persistent and memory caches during rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceNoneBltNodeLeftOnMixedCluster() throws Exception {
        testRebalanceNoneBltNode(true, true, false);
    }

    /**
     * Non baseline node fails in cluster with only persistent caches during rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceNoneBltNodeFailedOnOnlyPersistenceCluster() throws Exception {
        testRebalanceNoneBltNode(true, false, true);
    }

    /**
     * Non baseline node fails in cluster with only memory caches during rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceNoneBltNodeFailedOnOnlyInMemoryCluster() throws Exception {
        testRebalanceNoneBltNode(false, false, true);
    }

    /**
     * Non baseline node fails in cluster with persistent and memory caches during rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceNoneBltNodeFailedOnMixedCluster() throws Exception {
        testRebalanceNoneBltNode(true, true, true);
    }

    /**
     * Filtered node leaves cluster with persistent region.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceFilteredNodeOnOnlyPersistenceCluster() throws Exception {
        testRebalanceFilteredNode(true, false);
    }

    /**
     * Filtered node leaves cluster with memory region.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceFilteredNodeOnOnlyInMemoryCluster() throws Exception {
        testRebalanceFilteredNode(false, false);
    }

    /**
     * Filtered node leaves cluster with persistent and memory regions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceFilteredNodeOnMixedCluster() throws Exception {
        testRebalanceFilteredNode(true, true);
    }

    /**
     * Cache stops/starts several times on persistent cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceDynamicCacheOnOnlyPersistenceCluster() throws Exception {
        testRebalanceDynamicCache(true, false);
    }

    /**
     * Cache stop/start several times on memory cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceDynamicCacheOnOnlyInMemoryCluster() throws Exception {
        testRebalanceDynamicCache(false, false);
    }

    /**
     * Cache stop/start several times on cluster with persistent and memory regions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceDynamicCacheOnMixedCluster() throws Exception {
        testRebalanceDynamicCache(true, true);
    }

    /** */
    @Test
    public void testComplexCompatibilityInMemory() throws Exception {
        persistenceEnabled = false;
        addtiotionalMemRegion = false;

        IgniteEx crd = startGrid(0);

        IgnitePredicate<ClusterNode> filter0 =
            n -> Stream.of("0", "1").anyMatch(k -> n.consistentId().toString().endsWith(k));
        IgnitePredicate<ClusterNode> filter1 =
            n -> Stream.of("0", "1", "2").anyMatch(k -> n.consistentId().toString().endsWith(k));
        IgnitePredicate<ClusterNode> filter2 =
            n -> Stream.of("0", "1", "3").anyMatch(k -> n.consistentId().toString().endsWith(k));

        final String cache0Name = "cache0";
        IgniteCache<Object, Object> cache0 =
            crd.getOrCreateCache(new CacheConfiguration<>(cache0Name).setBackups(1).setNodeFilter(filter0));

        for (int i = 0; i < 1024; i++)
            cache0.put(i, i);

        // Trigger rebalancing.
        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi2.blockMessages(TestRecordingCommunicationSpi.blockDemandMessageForGroup(CU.cacheId(cache0Name)));

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(cfg1);

                return null;
            }
        });

        spi2.waitForBlocked();

        // Compatible event.
        startGrid(2);

        // Compatible event.
        startGrid(3);

        awaitPartitionMapExchange(false, false, null, false, new HashSet<>(Arrays.asList("cache1", "cache2")));

        // Compatible event.
        final String cache1Name = "cache1";
        IgniteCache<Object, Object> cache1 =
            crd.getOrCreateCache(new CacheConfiguration<>(cache1Name).setBackups(1).setNodeFilter(filter1));

        // Compatible event.
        final String cache2Name = "cache2";
        IgniteCache<Object, Object> cache2 =
            crd.getOrCreateCache(new CacheConfiguration<>(cache2Name).setBackups(1).setNodeFilter(filter2));

        for (int i = 0; i < 1024; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        // Compatible event.
        stopGrid(2);

        // Compatible event.
        stopGrid(3);

        spi2.stopBlock();

        awaitPartitionMapExchange();
    }

    /**
     * Trigger rebalance when dynamic caches stop/start.
     *
     * @param persistence Persistent flag.
     * @param addtiotionalRegion Use additional (non default) region.
     * @throws Exception If failed.
     */
    public void testRebalanceDynamicCache(boolean persistence, boolean addtiotionalRegion) throws Exception {
        persistenceEnabled = persistence;
        addtiotionalMemRegion = addtiotionalRegion;

        IgniteEx ignite0 = startGrids(NODES_CNT);

        ignite0.cluster().active(true);

        grid(1).close();

        for (String cache : ignite0.cacheNames())
            loadData(ignite0, cache);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi1 = startNodeWithBlockingRebalance(getTestIgniteInstanceName(1));

        commSpi1.waitForBlocked();

        IgniteInternalFuture<Boolean>[] futs = getAllRebalanceFutures(ignite0);

        int previousCaches = ignite0.cacheNames().size();

        for (int i = 0; i < 3; i++) {
            ignite0.createCache(DYNAMIC_CACHE_NAME);

            assertEquals(previousCaches + 1, ignite0.cacheNames().size());

            ignite0.destroyCache(DYNAMIC_CACHE_NAME);

            assertEquals(previousCaches, ignite0.cacheNames().size());
        }

        for (IgniteInternalFuture<Boolean> fut : futs)
            assertFalse(futInfoString(fut), fut.isDone());

        commSpi1.stopBlock();

        awaitPartitionMapExchange();

        for (IgniteInternalFuture<Boolean> fut : futs)
            assertTrue(futInfoString(fut), fut.isDone() && fut.get());
    }

    /**
     * Trigger rebalance when non-blt node left topology.
     *
     * @param persistence Persistent flag.
     * @param addtiotionalRegion Use additional (non default) region.
     * @param fail If true node forcibly falling.
     * @throws Exception If failed.
     */
    public void testRebalanceNoneBltNode(boolean persistence, boolean addtiotionalRegion,
        boolean fail) throws Exception {
        persistenceEnabled = persistence;
        addtiotionalMemRegion = addtiotionalRegion;

        IgniteEx ignite0 = startGrids(NODES_CNT);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        IgniteEx newNode = startGrid(NODES_CNT);

        grid(1).close();

        for (String cache : ignite0.cacheNames())
            loadData(ignite0, cache);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi1 = startNodeWithBlockingRebalance(getTestIgniteInstanceName(1));

        commSpi1.waitForBlocked();

        IgniteInternalFuture<Boolean>[] futs = getAllRebalanceFutures(ignite0);

        for (int i = 0; i < 3; i++) {
            if (fail) {
                ignite0.configuration().getDiscoverySpi().failNode(newNode.localNode().id(), "Fail node by test.");

                newNode.close();
            }
            else
                newNode.close();

            checkTopology(NODES_CNT);

            newNode = startGrid(NODES_CNT);

            checkTopology(NODES_CNT + 1);
        }

        for (IgniteInternalFuture<Boolean> fut : futs) {
            CacheGroupContext grp = U.field(fut, "grp");

            if (CU.isPersistentCache(grp.config(), ignite0.configuration().getDataStorageConfiguration()))
                assertFalse(futInfoString(fut), fut.isDone());
        }

        commSpi1.stopBlock();

        awaitPartitionMapExchange();

        for (IgniteInternalFuture<Boolean> fut : futs) {
            CacheGroupContext grp = U.field(fut, "grp");

            if (CU.isPersistentCache(grp.config(), ignite0.configuration().getDataStorageConfiguration()))
                assertTrue(futInfoString(fut), fut.isDone() && fut.get());
        }
    }

    /**
     * Trigger rebalance when filtered node left topology.
     *
     * @param persistence Persistent flag.
     * @param addtiotionalRegion Use additional (non default) region.
     * @throws Exception If failed.
     */
    public void testRebalanceFilteredNode(boolean persistence, boolean addtiotionalRegion) throws Exception {
        persistenceEnabled = persistence;
        addtiotionalMemRegion = addtiotionalRegion;
        filterNode = true;

        IgniteEx ignite0 = startGrids(NODES_CNT);
        IgniteEx filteredNode = startGrid(getTestIgniteInstanceName(NODES_CNT) + FILTERED_NODE_SUFFIX);

        ignite0.cluster().active(true);

        grid(1).close();

        for (String cache : ignite0.cacheNames())
            loadData(ignite0, cache);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi1 = startNodeWithBlockingRebalance(getTestIgniteInstanceName(1));

        commSpi1.waitForBlocked();

        IgniteInternalFuture<Boolean>[] futs = getAllRebalanceFutures(ignite0);

        for (int k = 0; k < 3; k++) {
            filteredNode.close();

            checkTopology(NODES_CNT);

            filteredNode = startGrid(getTestIgniteInstanceName(NODES_CNT) + FILTERED_NODE_SUFFIX);
        }

        for (IgniteInternalFuture<Boolean> fut : futs)
            assertFalse(futInfoString(fut), fut.isDone());

        commSpi1.stopBlock();

        awaitPartitionMapExchange();

        for (IgniteInternalFuture<Boolean> fut : futs)
            assertTrue(futInfoString(fut), fut.isDone() && fut.get());
    }

    /**
     * Finds all existed rebalance future by all cache for Ignite's instance specified.
     *
     * @param ignite Ignite.
     * @return Array of rebelance futures.
     */
    private IgniteInternalFuture<Boolean>[] getAllRebalanceFutures(IgniteEx ignite) {
        IgniteInternalFuture<Boolean>[] futs = new IgniteInternalFuture[ignite.cacheNames().size()];

        int i = 0;

        for (String cache : ignite.cacheNames()) {
            futs[i] = grid(1).context().cache()
                .cacheGroup(CU.cacheId(cache)).preloader().rebalanceFuture();

            assertFalse(futInfoString(futs[i]), futs[i].isDone());

            i++;
        }
        return futs;
    }

    /**
     * Prepares string representation of rebalance future.
     *
     * @param rebalanceFuture Rebalance future.
     * @return Information string about passed future.
     */
    private String futInfoString(IgniteInternalFuture<Boolean> rebalanceFuture) {
        return "Fut: " + rebalanceFuture
            + " is done: " + rebalanceFuture.isDone()
            + " result: " + (rebalanceFuture.isDone() ? rebalanceFuture.result() : "None");
    }

    /**
     * Loades several data entries to cache specified.
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void loadData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++)
                streamer.addData(i, System.nanoTime());
        }
    }

    /**
     * Starts node with name <code>name</code> and blocks demand message for custom caches.
     *
     * @param name Node instance name.
     * @return Test communication SPI.
     * @throws Exception If failed.
     */
    private TestRecordingCommunicationSpi startNodeWithBlockingRebalance(String name) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(name));

        TestRecordingCommunicationSpi communicationSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        communicationSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMessage = (GridDhtPartitionDemandMessage)msg;

                if (CU.cacheId(DEFAULT_CACHE_NAME) != demandMessage.groupId()
                    && CU.cacheId(MEM_REGION_CACHE) != demandMessage.groupId())
                    return false;

                info("Message was caught: " + msg.getClass().getSimpleName()
                    + " rebalanceId = " + U.field(demandMessage, "rebalanceId")
                    + " to: " + node.consistentId()
                    + " by cache id: " + demandMessage.groupId());

                return true;
            }

            return false;
        });

        startGrid(cfg);

        return communicationSpi;
    }
}
