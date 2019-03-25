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
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for cluster wide cache metrics.
 */
public class CacheMetricsForClusterGroupSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Cache 1 name. */
    private static final String CACHE1 = "cache1";

    /** Cache 2 name. */
    private static final String CACHE2 = "cache2";

    /** Entry count cache 1. */
    private static final int ENTRY_CNT_CACHE1 = 1000;

    /** Entry count cache 2. */
    private static final int ENTRY_CNT_CACHE2 = 500;

    /** Cache 1. */
    private IgniteCache<Integer, Integer> cache1;

    /** Cache 2. */
    private IgniteCache<Integer, Integer> cache2;

    /** Daemon grid. */
    private boolean daemon;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.METRICS);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDaemon(daemon);

        return cfg;
    }

    /**
     * Test cluster group metrics in case of statistics enabled.
     */
    @Test
    public void testMetricsStatisticsEnabled() throws Exception {
        startGrids();

        try {
            createCaches(true);

            populateCacheData(cache1, ENTRY_CNT_CACHE1);
            populateCacheData(cache2, ENTRY_CNT_CACHE2);

            readCacheData(cache1, ENTRY_CNT_CACHE1);
            readCacheData(cache2, ENTRY_CNT_CACHE2);

            awaitMetricsUpdate(1);

            Collection<ClusterNode> nodes = grid(0).cluster().forRemotes().nodes();

            for (ClusterNode node : nodes) {
                Map<Integer, CacheMetrics> metrics = ((IgniteClusterNode)node).cacheMetrics();
                assertNotNull(metrics);
                assertFalse(metrics.isEmpty());
            }

            assertMetrics(cache1, true);
            assertMetrics(cache2, true);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test cluster group metrics in case of statistics disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetricsStatisticsDisabled() throws Exception {
        startGrids();

        try {
            createCaches(false);

            populateCacheData(cache1, ENTRY_CNT_CACHE1);
            populateCacheData(cache2, ENTRY_CNT_CACHE2);

            readCacheData(cache1, ENTRY_CNT_CACHE1);
            readCacheData(cache2, ENTRY_CNT_CACHE2);

            awaitMetricsUpdate(1);

            Collection<ClusterNode> nodes = grid(0).cluster().forRemotes().nodes();

            for (ClusterNode node : nodes) {
                Map<Integer, CacheMetrics> metrics = ((IgniteClusterNode)node).cacheMetrics();
                assertNotNull(metrics);
                assertTrue(metrics.isEmpty());
            }

            assertMetrics(cache1, false);
            assertMetrics(cache2, false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests that only local metrics are updating if discovery updates disabled.
     */
    @Test
    public void testMetricsDiscoveryUpdatesDisabled() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DISCOVERY_DISABLE_CACHE_METRICS_UPDATE, "true");

        try {
            startGrids();

            try {
                createCaches(true);

                populateCacheData(cache1, ENTRY_CNT_CACHE1);
                populateCacheData(cache2, ENTRY_CNT_CACHE2);

                readCacheData(cache1, ENTRY_CNT_CACHE1);
                readCacheData(cache2, ENTRY_CNT_CACHE2);

                awaitMetricsUpdate(1);

                Collection<ClusterNode> nodes = grid(0).cluster().forRemotes().nodes();

                for (ClusterNode node : nodes) {
                    Map<Integer, CacheMetrics> metrics = ((IgniteClusterNode)node).cacheMetrics();
                    assertNotNull(metrics);
                    assertTrue(metrics.isEmpty());
                }

                assertOnlyLocalMetricsUpdating(CACHE1);
                assertOnlyLocalMetricsUpdating(CACHE2);
            }
            finally {
                stopAllGrids();
            }
        }
        finally {
            System.setProperty(IgniteSystemProperties.IGNITE_DISCOVERY_DISABLE_CACHE_METRICS_UPDATE, "false");
        }
    }

    /**
     * Start grids.
     */
    private void startGrids() throws Exception {
        startGrids(GRID_CNT);

        daemon = true;

        startGrid(GRID_CNT);
    }

    /**
     * @param statisticsEnabled Statistics enabled.
     */
    private void createCaches(boolean statisticsEnabled) {
        CacheConfiguration ccfg1 = defaultCacheConfiguration();
        ccfg1.setName(CACHE1);
        ccfg1.setStatisticsEnabled(statisticsEnabled);

        CacheConfiguration ccfg2 = defaultCacheConfiguration();
        ccfg2.setName(CACHE2);
        ccfg2.setStatisticsEnabled(statisticsEnabled);

        cache1 = grid(0).getOrCreateCache(ccfg1);
        cache2 = grid(0).getOrCreateCache(ccfg2);
    }

    /**
     * Closes caches.
     */
    private void destroyCaches() {
        cache1.destroy();
        cache2.destroy();
    }

    /**
     * @param cache Cache.
     * @param cnt Count.
     */
    private void populateCacheData(IgniteCache<Integer, Integer> cache, int cnt) {
        for (int i = 0; i < cnt; i++)
            cache.put(i, i);
    }

    /**
     * @param cache Cache.
     * @param cnt Count.
     */
    private void readCacheData(IgniteCache<Integer, Integer> cache, int cnt) {
        for (int i = 0; i < cnt; i++)
            grid(i % GRID_CNT).cache(cache.getName()).get(i);
    }

    /**
     * @param cache Cache.
     */
    private void assertMetrics(IgniteCache<Integer, Integer> cache, boolean expectNonZero) {
        CacheMetrics[] ms = new CacheMetrics[GRID_CNT];

        for (int i = 0; i < GRID_CNT; i++) {
            CacheMetrics metrics = cache.metrics(grid(i).cluster().forCacheNodes(cache.getName()));

            for (int j = 0; j < GRID_CNT; j++)
                ms[j] = grid(j).cache(cache.getName()).localMetrics();

            // Static metrics
            for (int j = 0; j < GRID_CNT; j++)
                assertEquals(metrics.name(), ms[j].name());

            // Dynamic metrics
            long sumGets = sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                @Override public Long apply(CacheMetrics input) {
                    return input.getCacheGets();
                }
            }, expectNonZero);

            assertEquals(metrics.getCacheGets(), sumGets);
            assertEquals(cache.mxBean().getCacheGets(), sumGets);

            long sumPuts = sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                @Override public Long apply(CacheMetrics input) {
                    return input.getCachePuts();
                }
            }, expectNonZero);

            assertEquals(metrics.getCachePuts(), sumPuts);
            assertEquals(cache.mxBean().getCachePuts(), sumPuts);

            long sumHits = sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                @Override public Long apply(CacheMetrics input) {
                    return input.getCacheHits();
                }
            }, expectNonZero);

            assertEquals(metrics.getCacheHits(), sumHits);
            assertEquals(cache.mxBean().getCacheHits(), sumHits);

            if (expectNonZero) {
                long sumHeapEntries = sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                    @Override public Long apply(CacheMetrics input) {
                        return input.getHeapEntriesCount();
                    }
                    // Currently non-zero even when statistics is off
                }, true);

                assertEquals(metrics.getHeapEntriesCount(), sumHeapEntries);
                assertEquals(cache.mxBean().getHeapEntriesCount(), sumHeapEntries);
            }

        }
    }

    /**
     * Asserts that only local metrics are updating.
     *
     * @param cacheName Cache name.
     */
    private void assertOnlyLocalMetricsUpdating(String cacheName) {
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache cache = grid(i).cache(cacheName);

            CacheMetrics clusterMetrics = cache.metrics(grid(i).cluster().forCacheNodes(cacheName));
            CacheMetrics locMetrics = cache.localMetrics();

            assertEquals(clusterMetrics.name(), locMetrics.name());

            assertEquals(0L, clusterMetrics.getCacheGets());
            assertEquals(0L, cache.mxBean().getCacheGets());
            assertEquals(locMetrics.getCacheGets(), cache.localMxBean().getCacheGets());

            assertEquals(0L, clusterMetrics.getCachePuts());
            assertEquals(0L, cache.mxBean().getCachePuts());
            assertEquals(locMetrics.getCachePuts(), cache.localMxBean().getCachePuts());

            assertEquals(0L, clusterMetrics.getCacheHits());
            assertEquals(0L, cache.mxBean().getCacheHits());
            assertEquals(locMetrics.getCacheHits(), cache.localMxBean().getCacheHits());
        }
    }

    /**
     * @param ms Milliseconds.
     * @param f Function.
     * @param expectNonZero Check if each value is non-zero.
     */
    private long sum(CacheMetrics[] ms, IgniteClosure<CacheMetrics, Long> f, boolean expectNonZero) {
        long res = 0;

        for (int i = 0; i < GRID_CNT; i++) {
            long val = f.apply(ms[i]);

            if (expectNonZero)
                assertTrue(val > 0);

            res += val;
        }

        return res;
    }
}
