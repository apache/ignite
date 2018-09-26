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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheConfigurationChecksOnNodeJoinTest extends GridCommonAbstractTest {
    /** Number records in cache. */
    private static final int NUMBER_RECORDS = 30;

    /** Nodes count. */
    private static final int NODES_COUNT = 4;

    /** */
    private static final ActivateNodeFinder FIRST_NODE = nodes -> nodes.stream().limit(1).findAny().get();

    /** */
    private static final ActivateNodeFinder LAST_NODE = nodes -> nodes.stream().skip(nodes.size() - 1).findAny().get();

    /** */
    private static final ActivateNodeFinder COORDINATOR_NODE =
        nodes -> nodes.stream().min(Comparator.comparingLong(ClusterNode::order)).get();

    /** */
    private static final ActivateNodeFinder NON_COORDINATOR_NODE =
        nodes -> nodes.stream().max(Comparator.comparingLong(ClusterNode::order)).get();

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
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks, that lost node will get cache configuration updates (one cache was started) on startup.
     *
     * @throws Exception if failed.
     */
    public void testStartNodeAfterCacheStarted() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().active(true);

        stopGrid(1);

        CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups(1);

        IgniteCache<Long, Long> cache0 = ignite0.getOrCreateCache(cacheCfg);

        populateData(cache0);

        ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Long, Long> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < NUMBER_RECORDS; i++)
            assertTrue(cache1.containsKey(1L << i));
    }

    /**
     * Checks, that lost node will get cache configuration updates (one cache was destroyed) on startup.
     *
     * @throws Exception if failed.
     */
    public void testStartNodeAfterCacheDestroy() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups(0);

        ignite1.cluster().active(true);

        IgniteCache<Long, Long> cache0 = ignite0.getOrCreateCache(cacheCfg);

        populateData(cache0);

        IgniteCache<Long, Long> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        checkDataPresent(cache1);

        stopGrid(1);

        cache0.destroy();

        // Starting grid with stored DEFAULT_CACHE_NAME configuration after DEFAULT_CACHE_NAME cache was destroyed.
        startGrid(1);

        awaitPartitionMapExchange();
    }

    public void testFullRestartAfterCacheDestroyActivateFromCoordinator() throws Exception {
        fullRestartClusterAfterCacheDestoroy(NODES_COUNT, COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromNonCoordinator() throws Exception {
        fullRestartClusterAfterCacheDestoroy(NODES_COUNT, NON_COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromFirstNode() throws Exception {
        fullRestartClusterAfterCacheDestoroy(NODES_COUNT, FIRST_NODE);
    }

    public void testFullRestartAfterCacheDestroyActivateFromLastNode() throws Exception {
        fullRestartClusterAfterCacheDestoroy(NODES_COUNT, LAST_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromCoordinator() throws Exception {
        fullRestartClusterAfterCacheCreate(NODES_COUNT, COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromNonCoordinator() throws Exception {
        fullRestartClusterAfterCacheCreate(NODES_COUNT, NON_COORDINATOR_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromFirstNode() throws Exception {
        fullRestartClusterAfterCacheCreate(NODES_COUNT, FIRST_NODE);
    }

    public void testFullRestartAfterCacheCreateActivateFromLastNode() throws Exception {
        fullRestartClusterAfterCacheCreate(NODES_COUNT, LAST_NODE);
    }

    private void fullRestartClusterAfterCacheCreate(int nodesCnt, ActivateNodeFinder finder) throws Exception {
        assert nodesCnt > 1;

        Map<ClusterNode, Ignite> nodes = start(nodesCnt);

        grid(0).cluster().active(true);

        stopSecondHalfNodes(nodes);

        CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups((nodesCnt + 1) / 2);

        IgniteCache<Long, Long> cache0 = grid(0).getOrCreateCache(cacheCfg);

        populateData(cache0);

        stopAllGrids();

        startGrids(nodesCnt);

        ClusterNode nodeActivator = finder.getActivateNode(grid(0).cluster().nodes());

        grid(nodeActivator).cluster().active(true);

        IgniteCache<Long, Long> cache = grid(nodeActivator).cache(DEFAULT_CACHE_NAME);

        checkDataPresent(cache);
    }

    private void fullRestartClusterAfterCacheDestoroy(int nodesCnt, ActivateNodeFinder finder) throws Exception {
        assert nodesCnt > 1;

        Map<ClusterNode, Ignite> nodes = start(nodesCnt);

        grid(0).cluster().active(true);

        CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups(0);

        IgniteCache<Long, Long> cache0 = grid(0).getOrCreateCache(cacheCfg);

        populateData(cache0);

        stopSecondHalfNodes(nodes);

        cache0.destroy();

        stopAllGrids();

        startGrids(nodesCnt);

        grid(finder.getActivateNode(grid(0).cluster().nodes())).cluster().active(true);
    }

    private void stopSecondHalfNodes(Map<ClusterNode, Ignite> nodesMap) {
        Collection<ClusterNode> nodes = grid(0).cluster().nodes();

        List<ClusterNode> nodesForStop = nodes.stream().skip(nodes.size() / 2).collect(Collectors.toList());

        for (ClusterNode node : nodesForStop)
            nodesMap.get(node).close();
    }

    private void populateData(IgniteCache<Long, Long> cache) {
        for (int i = 0; i < NUMBER_RECORDS; i++)
            cache.put(1L << i, 1L << i);
    }

    private void checkDataPresent(IgniteCache<Long, Long> cache) {
        for (int i = 0; i < NUMBER_RECORDS; i++)
            assertTrue(cache.containsKey(1L << i));
    }

    private Map<ClusterNode, Ignite> start(int cnt) throws Exception {
        assert cnt > 0;

        Map<ClusterNode, Ignite> map = new HashMap<>(cnt);

        for (int i = 0; i < cnt; i++) {
            IgniteEx ignite = startGrid(i);

            map.put(ignite.localNode(), ignite);
        }

        return map;
    }

    private interface ActivateNodeFinder {
        ClusterNode getActivateNode(Collection<ClusterNode> nodes);
    }

}