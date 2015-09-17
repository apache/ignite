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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Affinity Processor")
public abstract class GridAffinityProcessorAbstractSelfTest extends GridCommonAbstractTest {
    /** Number of grids started for tests. Should not be less than 2. */
    private static final int NODES_CNT = 3;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Flag to start grid with cache. */
    private boolean withCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setForceServerMode(true);
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        if (withCache) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName(CACHE_NAME);
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);
            cacheCfg.setAffinity(affinityFunction());

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * Creates affinity function for test.
     *
     * @return Affinity function.
     */
    protected abstract AffinityFunction affinityFunction();

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT >= 1;

        withCache = false;

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i);

        withCache = true;

        for (int i = NODES_CNT; i < 2 * NODES_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test affinity functions caching and clean up.
     *
     * @throws Exception In case of any exception.
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    public void testAffinityProcessor() throws Exception {
        Random rnd = new Random();

        final IgniteKernal grid1 = (IgniteKernal)grid(rnd.nextInt(NODES_CNT)); // With cache.
        IgniteKernal grid2 = (IgniteKernal)grid(NODES_CNT + rnd.nextInt(NODES_CNT)); // Without cache.

        assertEquals(NODES_CNT * 2, grid1.cluster().nodes().size());
        assertEquals(NODES_CNT * 2, grid2.cluster().nodes().size());

        IgniteCache<Integer, Integer> cache = grid2.cache(CACHE_NAME);

        assertNotNull(cache);

        GridAffinityProcessor affPrc1 = grid1.context().affinity();
        GridAffinityProcessor affPrc2 = grid2.context().affinity();

        // Create keys collection.
        Collection<Integer> keys = new ArrayList<>(1000);

        for (int i = 0; i < 1000; i++)
            keys.add(i);

        //
        // Validate affinity functions collection updated on first call.
        //

        Map<ClusterNode, Collection<Integer>> node1Map = affPrc1.mapKeysToNodes(CACHE_NAME, keys);
        Map<ClusterNode, Collection<Integer>> node2Map = affPrc2.mapKeysToNodes(CACHE_NAME, keys);
        Map<ClusterNode, Collection<Integer>> cacheMap = affinity(cache).mapKeysToNodes(keys);

        assertEquals(cacheMap.size(), node1Map.size());
        assertEquals(cacheMap.size(), node2Map.size());

        for (Map.Entry<ClusterNode, Collection<Integer>> entry : cacheMap.entrySet()) {
            ClusterNode node = entry.getKey();

            Collection<Integer> mappedKeys = entry.getValue();

            Collection<Integer> mapped1 = node1Map.get(node);
            Collection<Integer> mapped2 = node2Map.get(node);

            assertTrue(mappedKeys.containsAll(mapped1) && mapped1.containsAll(mappedKeys));
            assertTrue(mappedKeys.containsAll(mapped2) && mapped2.containsAll(mappedKeys));
        }
    }

    /**
     * Test performance of affinity processor.
     *
     * @throws Exception In case of any exception.
     */
    public void testPerformance() throws Exception {
        IgniteKernal grid = (IgniteKernal)grid(0);
        GridAffinityProcessor aff = grid.context().affinity();

        int keysSize = 1000000;

        Collection<Integer> keys = new ArrayList<>(keysSize);

        for (int i = 0; i < keysSize; i++)
            keys.add(i);

        long start = System.currentTimeMillis();

        int iterations = 10000000;

        for (int i = 0; i < iterations; i++)
            aff.mapKeyToNode(null, keys);

        long diff = System.currentTimeMillis() - start;

        info(">>> Map " + keysSize + " keys to " + grid.cluster().nodes().size() +
            " nodes " + iterations + " times in " + diff + "ms.");

        assertTrue(diff < 25000);
    }
}