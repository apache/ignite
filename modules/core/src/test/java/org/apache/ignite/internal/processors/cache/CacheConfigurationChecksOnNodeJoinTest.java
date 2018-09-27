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
package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;

/**
 *
 */
public class CacheConfigurationChecksOnNodeJoinTest extends GridCommonAbstractTest {
    /** Number records in cache. */
    private static final int NUMBER_RECORDS = 30;

    /** Nodes count. */
    private static final int NODES_COUNT = 4;

    /** Cluster node to number. */
    private static final Map<ClusterNode, Integer> nodeToNumber = new HashMap<>();

    /** Find's first node in cluster. */
    private static final ActivateNodeFinder FIRST_NODE = nodes ->
        nodeToNumber.entrySet().stream().min(comparingInt(Map.Entry::getValue)).get().getKey();

    /** Find's last node in cluster. */
    private static final ActivateNodeFinder LAST_NODE = nodes ->
        nodeToNumber.entrySet().stream().max(comparingInt(Map.Entry::getValue)).get().getKey();

    /** Find's node with min order. */
    private static final ActivateNodeFinder COORDINATOR_NODE =
        nodes -> nodes.stream().min(comparingLong(ClusterNode::order)).get();

    /** Fins's node with max order. */
    private static final ActivateNodeFinder NON_COORDINATOR_NODE =
        nodes -> nodes.stream().max(comparingLong(ClusterNode::order)).get();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration drCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        nodeToNumber.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        nodeToNumber.clear();
    }

    /**
     * Checks, that lost node will get cache configuration updates (one cache was started) on startup.
     *
     * @throws Exception if failed.
     */
    public void testStartNodeAfterCacheStarted() throws Exception {
        restoreClusterAfterCacheCreate(NODES_COUNT, false, LAST_NODE);
    }

    /**
     * Checks, that lost node will get cache configuration updates (one cache was destroyed) on startup.
     *
     * @throws Exception if failed.
     */
    public void testStartNodeAfterCacheDestroy() throws Exception {
        restoreClusterAfterCacheDestroy(NODES_COUNT, false, LAST_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromCoordinator() throws Exception {
        restoreClusterAfterCacheDestroy(NODES_COUNT, true, COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromNonCoordinator() throws Exception {
        restoreClusterAfterCacheDestroy(NODES_COUNT, true, NON_COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromFirstNode() throws Exception {
        restoreClusterAfterCacheDestroy(NODES_COUNT, true, FIRST_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromLastNode() throws Exception {
        restoreClusterAfterCacheDestroy(NODES_COUNT, true, LAST_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromCoordinator() throws Exception {
        restoreClusterAfterCacheCreate(NODES_COUNT, true, COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromNonCoordinator() throws Exception {
        restoreClusterAfterCacheCreate(NODES_COUNT, true, NON_COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromFirstNode() throws Exception {
        restoreClusterAfterCacheCreate(NODES_COUNT, true, FIRST_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromLastNode() throws Exception {
        restoreClusterAfterCacheCreate(NODES_COUNT, true, LAST_NODE);
    }

    private void restoreClusterAfterCacheCreate(
        int nodesCnt,
        boolean fullStop,
        ActivateNodeFinder finder
    ) throws Exception {
        assert nodesCnt > 1;

        startGrids(nodesCnt);

        grid(finder.getActivateNode(grid(0).cluster().nodes())).cluster().active(true);

        stopSecondHalfNodes();

        CacheConfiguration<Long, Long> cacheCfg =
            new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups((nodesCnt + 1) / 2);

        IgniteCache<Long, Long> cache0 = grid(0).getOrCreateCache(cacheCfg);

        populateData(cache0);

        ClusterNode nodeActivator;

        log.error("IGNITE-8717 stage!");

        if (fullStop) {
            stopAllGrids();

            startGrids(nodesCnt);

            nodeActivator = finder.getActivateNode(grid(0).cluster().nodes());

            grid(nodeActivator).cluster().active(true);
        }
        else {
            startSecondHalfNodes(nodesCnt);

            nodeActivator = finder.getActivateNode(grid(0).cluster().nodes());
        }

        awaitPartitionMapExchange();

        IgniteCache<Long, Long> cache = grid(nodeActivator).cache(DEFAULT_CACHE_NAME);

        checkDataPresent(cache);
    }

    private void restoreClusterAfterCacheDestroy(
        int nodesCnt,
        boolean fullStop,
        ActivateNodeFinder finder
    ) throws Exception {
        assert nodesCnt >= 2;

        startGrids(nodesCnt);

        grid(finder.getActivateNode(grid(0).cluster().nodes())).cluster().active(true);

        CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups(0);

        IgniteCache<Long, Long> cache0 = grid(0).getOrCreateCache(cacheCfg);

        populateData(cache0);

        stopSecondHalfNodes();

        cache0.destroy();

        log.error("IGNITE-8717 stage!");

        if (fullStop) {
            stopAllGrids();

            startGrids(nodesCnt);

            grid(finder.getActivateNode(grid(0).cluster().nodes())).cluster().active(true);
        }
        else
            startSecondHalfNodes(nodesCnt);

        awaitPartitionMapExchange();
    }

    private void startSecondHalfNodes(int clusterSize) throws Exception {
        for (int i = clusterSize / 2; i < clusterSize; i++)
            startGrid(i);
    }

    private void stopSecondHalfNodes() {
        Collection<ClusterNode> nodes = grid(0).cluster().nodes();

        for (int i = nodes.size() / 2; i < nodes.size(); i++) {
            IgniteEx igniteEx = grid(getTestIgniteInstanceName(i));

            nodeToNumber.remove(igniteEx.localNode());

            igniteEx.close();
        }
    }

    private void populateData(IgniteCache<Long, Long> cache) {
        for (int i = 0; i < NUMBER_RECORDS; i++)
            cache.put(1L << i, 1L << i);
    }

    private void checkDataPresent(IgniteCache<Long, Long> cache) {
        for (int i = 0; i < NUMBER_RECORDS; i++)
            assertTrue(cache.containsKey(1L << i));
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx igniteEx = super.startGrid(idx);

        nodeToNumber.put(igniteEx.localNode(), idx);

        return igniteEx;
    }

    /** {@inheritDoc} */
    @Override protected void stopGrid(@Nullable String igniteInstanceName, boolean cancel, boolean awaitTop) {
        nodeToNumber.remove(grid(igniteInstanceName).localNode());

        super.stopGrid(igniteInstanceName, cancel, awaitTop);
    }

    private interface ActivateNodeFinder {
        ClusterNode getActivateNode(Collection<ClusterNode> nodes);
    }
}