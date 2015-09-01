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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDaemon(daemon);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);

        daemon = true;

        startGrid(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test cluster group metrics in case of statistics enabled.
     */
    public void testMetricsStatisticsEnabled() throws Exception {
        createCaches(true);

        populateCacheData(cache1, ENTRY_CNT_CACHE1);
        populateCacheData(cache2, ENTRY_CNT_CACHE2);

        readCacheData(cache1, ENTRY_CNT_CACHE1);
        readCacheData(cache2, ENTRY_CNT_CACHE2);

        awaitMetricsUpdate();

        Collection<ClusterNode> nodes = grid(0).cluster().forRemotes().nodes();

        for (ClusterNode node : nodes) {
            Map<Integer, CacheMetrics> metrics = ((TcpDiscoveryNode) node).cacheMetrics();
            assertNotNull(metrics);
            assertFalse(metrics.isEmpty());
        }

        assertMetrics(cache1);
        assertMetrics(cache2);

        destroyCaches();
    }

    /**
     * Test cluster group metrics in case of statistics disabled.
     */
    public void testMetricsStatisticsDisabled() throws Exception {
        createCaches(false);

        populateCacheData(cache1, ENTRY_CNT_CACHE1);
        populateCacheData(cache2, ENTRY_CNT_CACHE2);

        readCacheData(cache1, ENTRY_CNT_CACHE1);
        readCacheData(cache2, ENTRY_CNT_CACHE2);

        awaitMetricsUpdate();

        Collection<ClusterNode> nodes = grid(0).cluster().forRemotes().nodes();

        for (ClusterNode node : nodes) {
            Map<Integer, CacheMetrics> metrics = ((TcpDiscoveryNode) node).cacheMetrics();
            assertNotNull(metrics);
            assertTrue(metrics.isEmpty());
        }

        assertMetrics(cache1);
        assertMetrics(cache2);

        destroyCaches();
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
     * Wait for {@link EventType#EVT_NODE_METRICS_UPDATED} event will be receieved.
     */
    private void awaitMetricsUpdate() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch((GRID_CNT + 1) * 2);

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event ignore) {
                latch.countDown();

                return true;
            }
        };

        for (int i = 0; i < GRID_CNT; i++)
            grid(i).events().localListen(lsnr, EVT_NODE_METRICS_UPDATED);

        latch.await();
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
            cache.get(i);
    }

    /**
     * @param cache Cache.
     */
    private void assertMetrics(IgniteCache<Integer, Integer> cache) {
        CacheMetrics[] ms = new CacheMetrics[GRID_CNT];

        for (int i = 0; i < GRID_CNT; i++) {
            CacheMetrics metrics = cache.metrics(grid(i).cluster().forCacheNodes(cache.getName()));

            for (int j = 0; j < GRID_CNT; j++)
                ms[j] = grid(j).cache(cache.getName()).metrics();

            // Static metrics
            for (int j = 0; j < GRID_CNT; j++)
                assertEquals(metrics.name(), ms[j].name());

            // Dynamic metrics
            assertEquals(metrics.getCacheGets(), sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                @Override public Long apply(CacheMetrics input) {
                    return input.getCacheGets();
                }
            }));

            assertEquals(metrics.getCachePuts(), sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                @Override public Long apply(CacheMetrics input) {
                    return input.getCachePuts();
                }
            }));

            assertEquals(metrics.getCacheHits(), sum(ms, new IgniteClosure<CacheMetrics, Long>() {
                @Override public Long apply(CacheMetrics input) {
                    return input.getCacheHits();
                }
            }));
        }
    }

    /**
     * @param ms Milliseconds.
     * @param f Function.
     */
    private long sum(CacheMetrics[] ms, IgniteClosure<CacheMetrics, Long> f) {
        long res = 0;

        for (int i = 0; i < GRID_CNT; i++)
            res += f.apply(ms[i]);

        return res;
    }
}