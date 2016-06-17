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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMetricsAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for cache query metrics.
 */
public abstract class CacheAbstractQueryMetricsSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    protected int gridCnt;

    /** Cache mode. */
    protected CacheMode cacheMode;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCnt);

        IgniteCache<String, Integer> cacheA = grid(0).cache("A");

        for (int i = 0; i < 100; i++)
            cacheA.put(String.valueOf(i), i);

        IgniteCache<String, Integer> cacheB = grid(0).cache("B");

        for (int i = 0; i < 100; i++)
            cacheB.put(String.valueOf(i), i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration<String, Integer> cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName("A");
        cacheCfg1.setCacheMode(cacheMode);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg1.setIndexedTypes(String.class, Integer.class);
        cacheCfg1.setStatisticsEnabled(true);

        CacheConfiguration<String, Integer> cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName("B");
        cacheCfg2.setCacheMode(cacheMode);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg2.setIndexedTypes(String.class, Integer.class);
        cacheCfg2.setStatisticsEnabled(true);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        return cfg;
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsQueryMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from Integer");

        testQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from Integer");
        qry.setPageSize(10);

        testQueryNotFullyFetchedMetrics(cache, qry, false);
    }

    /**
     * Test metrics for failed SQL queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsQueryFailedMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from UNKNOWN");

        testQueryFailedMetrics(cache, qry);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testScanQueryMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        ScanQuery<String, Integer> qry = new ScanQuery<>();

        testQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testScanQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        ScanQuery<String, Integer> qry = new ScanQuery<>();
        qry.setPageSize(10);

        testQueryNotFullyFetchedMetrics(cache, qry, true);
    }

    /**
     * Test metrics for failed Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testScanQueryFailedMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        ScanQuery<String, Integer> qry = new ScanQuery<>(Integer.MAX_VALUE);

        testQueryFailedMetrics(cache, qry);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlCrossCacheQueryMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Integer");

        testQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlCrossCacheQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Integer");
        qry.setPageSize(10);

        testQueryNotFullyFetchedMetrics(cache, qry, false);
    }

    /**
     * Test metrics for failed SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlCrossCacheQueryFailedMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"G\".Integer");

        testQueryFailedMetrics(cache, qry);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void testQueryMetrics(IgniteCache<String, Integer> cache, Query qry) {
        cache.query(qry).getAll();

        GridCacheQueryMetricsAdapter m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        info("Metrics: " + m);

        assertEquals(1, m.executions());
        assertEquals(1, m.completedExecutions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        info("Metrics: " + m);

        assertEquals(2, m.executions());
        assertEquals(2, m.completedExecutions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @param waitingForCompletion Waiting for query completion.
     */
    private void testQueryNotFullyFetchedMetrics(IgniteCache<String, Integer> cache, Query qry,
        boolean waitingForCompletion) throws IgniteInterruptedCheckedException {
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingForCompletion(cache, 1);

        GridCacheQueryMetricsAdapter m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        info("Metrics: " + m);

        assertEquals(1, m.executions());
        assertEquals(1, m.completedExecutions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);

        // Execute again with the same parameters.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingForCompletion(cache, 2);

        m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        info("Metrics: " + m);

        assertEquals(2, m.executions());
        assertEquals(2, m.completedExecutions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void testQueryFailedMetrics(IgniteCache<String, Integer> cache, Query qry) {
        try {
            cache.query(qry).getAll();
        }
        catch (Exception e) {
            // No-op.
        }

        GridCacheQueryMetricsAdapter m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        info("Metrics: " + m);

        assertEquals(1, m.executions());
        assertEquals(0, m.completedExecutions());
        assertEquals(1, m.fails());
        assertTrue(m.averageTime() == 0);
        assertTrue(m.maximumTime() == 0);
        assertTrue(m.minimumTime() == 0);

        // Execute again with the same parameters.
        try {
            cache.query(qry).getAll();
        }
        catch (Exception e) {
            // No-op.
        }

        m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        info("Metrics: " + m);

        assertEquals(2, m.executions());
        assertEquals(0, m.completedExecutions());
        assertEquals(2, m.fails());
        assertTrue(m.averageTime() == 0);
        assertTrue(m.maximumTime() == 0);
        assertTrue(m.minimumTime() == 0);
    }

    /**
     * @param cache Cache.
     * @param exp Expected.
     */
    private static void waitingForCompletion(final IgniteCache<String, Integer> cache,
        final int exp) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheQueryMetricsAdapter m = (GridCacheQueryMetricsAdapter)cache.queryMetrics();
                return m.completedExecutions() == exp;
            }
        }, 5000);
    }
}
