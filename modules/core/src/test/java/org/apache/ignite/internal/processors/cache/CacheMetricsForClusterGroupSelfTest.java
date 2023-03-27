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
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.METRICS);

        super.beforeTestsStarted();
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

            checkRemoteMetrics(false);

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

            checkRemoteMetrics(true);

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
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DISCOVERY_DISABLE_CACHE_METRICS_UPDATE, value = "true")
    public void testMetricsDiscoveryUpdatesDisabled() throws Exception {
        startGrids();

        try {
            createCaches(true);

            populateCacheData(cache1, ENTRY_CNT_CACHE1);
            populateCacheData(cache2, ENTRY_CNT_CACHE2);

            readCacheData(cache1, ENTRY_CNT_CACHE1);
            readCacheData(cache2, ENTRY_CNT_CACHE2);

            checkRemoteMetrics(true);

            assertOnlyLocalMetricsUpdating(CACHE1);
            assertOnlyLocalMetricsUpdating(CACHE2);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void checkRemoteMetrics(boolean expectedEmpty) throws InterruptedException {
        // Wait for two subsequent metrics update events, to be sure we have cought the last metric state, but
        // not some intermediate state with pending updates.
        awaitMetricsUpdate(2);

        Collection<ClusterNode> nodes = grid(0).cluster().forRemotes().nodes();

        for (ClusterNode node : nodes) {
            Map<Integer, CacheMetrics> metrics = ((IgniteClusterNode)node).cacheMetrics();
            assertNotNull(metrics);
            assertEquals(expectedEmpty, metrics.isEmpty());
        }
    }

    /**
     * Start grids.
     */
    private void startGrids() throws Exception {
        startGrids(GRID_CNT);

        startClientGrid(GRID_CNT);
    }

    /**
     * @param statisticsEnabled Statistics enabled.
     */
    private void createCaches(boolean statisticsEnabled) {
        CacheConfiguration<Integer, Integer> ccfg1 = defaultCacheConfiguration();
        ccfg1.setName(CACHE1);
        ccfg1.setStatisticsEnabled(statisticsEnabled);

        CacheConfiguration<Integer, Integer> ccfg2 = defaultCacheConfiguration();
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
            long sumGets = sum(ms, CacheMetrics::getCacheGets, expectNonZero);

            assertEquals(metrics.getCacheGets(), sumGets);

            long sumPuts = sum(ms, CacheMetrics::getCachePuts, expectNonZero);

            assertEquals(metrics.getCachePuts(), sumPuts);

            long sumHits = sum(ms, CacheMetrics::getCacheHits, expectNonZero);

            assertEquals(metrics.getCacheHits(), sumHits);

            if (expectNonZero) {
                // Currently non-zero even when statistics is off.
                long sumHeapEntries = sum(ms, CacheMetrics::getHeapEntriesCount, true);

                assertEquals(metrics.getHeapEntriesCount(), sumHeapEntries);
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
            IgniteCache<Integer, Integer> cache = grid(i).cache(cacheName);

            CacheMetrics clusterMetrics = cache.metrics(grid(i).cluster().forCacheNodes(cacheName));
            CacheMetrics locMetrics = cache.localMetrics();

            assertEquals(clusterMetrics.name(), locMetrics.name());

            assertEquals(0L, clusterMetrics.getCacheGets());
            assertNotEquals(0L, locMetrics.getCacheGets());

            assertEquals(0L, clusterMetrics.getCachePuts());
            assertNotEquals(0L, locMetrics.getCachePuts());

            assertEquals(0L, clusterMetrics.getCacheHits());
            assertNotEquals(0L, locMetrics.getCacheHits());
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
