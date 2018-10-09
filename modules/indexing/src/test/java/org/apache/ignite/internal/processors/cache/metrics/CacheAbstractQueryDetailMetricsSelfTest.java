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

package org.apache.ignite.internal.processors.cache.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for cache query details metrics.
 */
public abstract class CacheAbstractQueryDetailMetricsSelfTest extends GridCommonAbstractTest {
    /**  */
    static final int QRY_DETAIL_METRICS_SIZE = 3;

    /** */
    public static final int CACHED_VALUES = 100;

    /** Grid count. */
    protected int gridCnt;

    /** Cache mode. */
    protected CacheMode cacheMode;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCnt);

        IgniteCache<Integer, String> cacheA = grid(0).cache("A");
        IgniteCache<Integer, Long> cacheB = grid(0).cache("B");

        for (int i = 0; i < CACHED_VALUES; i++) {
            cacheA.put(i, String.valueOf(i));
            cacheB.put(i, Integer.valueOf(i).longValue());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, String> configureCache(String cacheName) {
        CacheConfiguration<Integer, String> ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(Integer.class, String.class);
        ccfg.setStatisticsEnabled(true);
        ccfg.setQueryDetailMetricsSize(QRY_DETAIL_METRICS_SIZE);

        return ccfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, String> configureCache2(String cacheName) {
        CacheConfiguration<Integer, String> ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(Integer.class, Long.class);
        ccfg.setStatisticsEnabled(true);
        ccfg.setQueryDetailMetricsSize(QRY_DETAIL_METRICS_SIZE);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(configureCache("A"), configureCache2("B"));

        return cfg;
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");
        IgniteCache<Integer, String> cache2 = grid(0).context().cache().jcache("B");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Long");

        checkDeferredQueryMetrics(cache, cache2, qry, CACHED_VALUES);
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");
        IgniteCache<Integer, String> cache2 = grid(0).context().cache().jcache("B");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Long");

        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, cache2, false, qry);
    }

    /**
     * Test metrics eviction.
     *
     * @throws Exception In case of error.
     */
    public void testQueryMetricsEviction() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        // Execute several DIFFERENT queries with guaranteed DIFFERENT time of execution.
        cache.query(new SqlFieldsQuery("select * from String")).getAll();
        Thread.sleep(100);

        cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
        Thread.sleep(100);

        cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
        Thread.sleep(100);

        cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
        Thread.sleep(100);

        cache.query(new ScanQuery()).getAll();
        Thread.sleep(100);

        cache.query(new SqlQuery("String", "from String")).getAll();

        waitingFor(cache, "size", QRY_DETAIL_METRICS_SIZE);

        for (int i = 0; i < QRY_DETAIL_METRICS_SIZE; i++)
            checkMetrics(cache, QRY_DETAIL_METRICS_SIZE, i, 1, 1, 0, false);

        // Check that collected metrics contains correct items: metrics for last 3 queries.
        Collection<? extends QueryDetailMetrics> metrics = cache.queryDetailMetrics();

        String lastMetrics = "";

        for (QueryDetailMetrics m : metrics)
            lastMetrics += m.queryType() + " " + m.query() + ";";

        assertTrue(lastMetrics.contains("SQL_FIELDS select * from String limit 2;"));
        assertTrue(lastMetrics.contains("SCAN A;"));
        assertTrue(lastMetrics.contains("SQL from String;"));

        cache = grid(0).context().cache().jcache("B");

        cache.query(new SqlFieldsQuery("select * from Long")).getAll();
        cache.query(new SqlFieldsQuery("select count(*) from Long")).getAll();
        cache.query(new SqlFieldsQuery("select * from Long limit 1")).getAll();
        cache.query(new SqlFieldsQuery("select * from Long limit 2")).getAll();
        cache.query(new ScanQuery()).getAll();
        cache.query(new SqlQuery("Long", "from Long")).getAll();

        waitingFor(cache, "size", QRY_DETAIL_METRICS_SIZE);

        for (int i = 0; i < QRY_DETAIL_METRICS_SIZE; i++)
            checkMetrics(cache, QRY_DETAIL_METRICS_SIZE, i, 1, 1, 0, false);

        if (gridCnt > 1) {
            cache = grid(1).context().cache().jcache("A");

            cache.query(new SqlFieldsQuery("select * from String")).getAll();
            cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
            cache.query(new ScanQuery()).getAll();
            cache.query(new SqlQuery("String", "from String")).getAll();

            waitingFor(cache, "size", QRY_DETAIL_METRICS_SIZE);

            for (int i = 0; i < QRY_DETAIL_METRICS_SIZE; i++)
                checkMetrics(cache, QRY_DETAIL_METRICS_SIZE, i, 1, 1, 0, false);

            cache = grid(1).context().cache().jcache("B");

            cache.query(new SqlFieldsQuery("select * from Long")).getAll();
            cache.query(new SqlFieldsQuery("select count(*) from Long")).getAll();
            cache.query(new SqlFieldsQuery("select * from Long limit 1")).getAll();
            cache.query(new SqlFieldsQuery("select * from Long limit 2")).getAll();
            cache.query(new ScanQuery()).getAll();
            cache.query(new SqlQuery(Long.class, "from Long")).getAll();

            waitingFor(cache, "size", QRY_DETAIL_METRICS_SIZE);

            for (int i = 0; i < QRY_DETAIL_METRICS_SIZE; i++)
                checkMetrics(cache, QRY_DETAIL_METRICS_SIZE, i, 1, 1, 0, false);
        }
    }

    /** */
    public void testInsertMetrics() throws Exception {
        IgniteCache<Integer, Long> cache = grid(0).context().cache().jcache("B");

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "INSERT INTO LONG (_key, _val) values (-101,-101),(-102,-102),(-103,-103)");

        checkQueryMetrics(cache, qry, 1);
    }

    /** */
    public void testDeleteMetrics() throws Exception {
        IgniteCache<Integer, Long> cache = grid(0).context().cache().jcache("B");

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM LONG Where _val='5'");

        checkQueryMetrics(cache, qry, 1);
    }

    /** */
    private static class Worker extends Thread {
        /** */
        private final IgniteCache cache;

        /** */
        private final Query qry;

        /** */
        Worker(IgniteCache cache, Query qry) {
            this.cache = cache;
            this.qry = qry;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            cache.query(qry).getAll();
        }
    }

    /**
     * Test metrics if queries executed from several threads.
     *
     * @throws Exception In case of error.
     */
    public void testQueryMetricsMultithreaded() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        Collection<Worker> workers = new ArrayList<>();

        int repeat = 10;

        for (int k = 0; k < repeat; k++) {
            // Execute as match queries as history size to avoid eviction.
            for (int i = 1; i <= QRY_DETAIL_METRICS_SIZE; i++)
                workers.add(new Worker(cache, new SqlFieldsQuery("select * from String limit " + i)));
        }

        for (Worker worker : workers)
            worker.start();

        for (Worker worker : workers)
            worker.join();

        for (int i = 0; i < QRY_DETAIL_METRICS_SIZE; i++)
            checkMetrics(cache, QRY_DETAIL_METRICS_SIZE, i, repeat, repeat, 0, false);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlQuery<Integer, String> qry = new SqlQuery<>("String", "from String");

        checkDoubleQueryMetrics(cache, qry, CACHED_VALUES);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");
        IgniteCache<Integer, String> cache2 = grid(0).context().cache().jcache("B");

        SqlQuery<Integer, String> qry = new SqlQuery<>(Long.class, "Select * from \"B\".Long");

        qry.setPageSize(10);

        //todo: Enable after fix of IGNITE-9771
        //checkQueryNotFullyFetchedMetrics(cache, cache2, true, qry);
    }

    /**
     * Test metrics for failed Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlQueryFailedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlQuery<Integer, String> qry = new SqlQuery<>("Long", "from Long");

        checkQueryFailedMetrics(cache, qry);
    }

    /**
     * Test metrics for Sql queries.
     *
     * @throws Exception In case of error.
     */
    public void testTextQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        TextQuery qry = new TextQuery<>("String", "1");

        checkDoubleQueryMetrics(cache, qry, 1);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsCrossCacheQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");
        IgniteCache<Integer, Long> cache2 = grid(0).context().cache().jcache("B");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Long");

        checkDeferredQueryMetrics(cache, cache2, qry, CACHED_VALUES);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsCrossCacheQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");
        IgniteCache<Integer, String> cache2 = grid(0).context().cache().jcache("B");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Long");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, cache2, false, qry);
    }

    /**
     * Check metrics.
     *
     * @param cache Cache to check metrics.
     * @param sz Expected size of metrics.
     * @param idx Index of metrics to check.
     * @param execs Expected number of executions.
     * @param completions Expected number of completions.
     * @param failures Expected number of failures.
     * @param first {@code true} if metrics checked for first query only.
     */
    private void checkMetrics(IgniteCache<?, ?> cache,
        int sz,
        int idx,
        int execs,
        int completions,
        int failures,
        boolean first) {
        Collection<? extends QueryDetailMetrics> metrics = cache.queryDetailMetrics();

        assertNotNull(metrics);
        assertEquals(sz, metrics.size());

        if (sz > 0) {
            QueryDetailMetrics m = new ArrayList<>(metrics).get(idx);

            info("Metrics: " + m);

            assertEquals("Executions", execs, m.executions());
            assertEquals("Completions", completions, m.completions());
            assertEquals("Failures", failures, m.failures());
            assertTrue(m.averageTime() >= 0);
            assertTrue(m.maximumTime() >= 0);
            assertTrue(m.minimumTime() >= 0);

            if (first)
                assertTrue("On first execution minTime == maxTime", m.minimumTime() == m.maximumTime());
        }
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkDoubleQueryMetrics(IgniteCache<?, ?> cache, Query qry, int expResSize) {
        // Execute query.
        checkQueryMetrics(cache, qry, expResSize, 1);

        // Execute again with the same parameters.
        checkQueryMetrics(cache, qry, expResSize, 2);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkQueryMetrics(IgniteCache<?, ?> cache, Query qry, int expResSize) {
        checkQueryMetrics(cache, qry, expResSize, 1);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkQueryMetrics(IgniteCache<?, ?> cache, Query qry, int expResSize, int number) {
        // Execute query.
        QueryCursor<?> cur = cache.query(qry);

        List<?> res1 = cur.getAll();

        assertEquals(expResSize, res1.size());

        checkMetrics(cache, 1, 0, number, number, 0, number<=1);
    }

    /**
     * @param cache Cache.
     * @param cache2 Cache.
     * @param qry Query.
     */
    private void checkDeferredQueryMetrics(IgniteCache<Integer, String> cache, IgniteCache cache2, Query qry, int expResSize) {
        // Execute query.
        QueryCursor<?> cur = cache.query(qry);

        checkMetrics(cache, 0, 0, 0, 0, 0, true);

        checkMetrics(cache2, 0, 0, 0, 0, 0, true);

        List<?> res1 = cur.getAll();

        assertEquals(expResSize, res1.size());

        checkMetrics(cache, 0, 0, 0, 0, 0, true);

        checkMetrics(cache2, 1, 0, 1, 1, 0, true);

        // Execute again with the same parameters.
        List<?> res2 = cache.query(qry).getAll();

        assertEquals(expResSize, res2.size());

        checkMetrics(cache2, 1, 0, 2, 2, 0, false);
    }

    /**
     * @param cache Cache.
     * @param cache2
     * @param waitingForCompletion Waiting for query completion.
     * @param qry Query.
     */
    private void checkQueryNotFullyFetchedMetrics(IgniteCache<?, ?> cache,
        IgniteCache<?,?> cache2, boolean waitingForCompletion, Query qry) throws IgniteInterruptedCheckedException {
        // Execute query.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingFor(cache, "completions", 1);

        checkMetrics(cache2, 1, 0, 1, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingFor(cache, "completions", 2);

        checkMetrics(cache2, 1, 0, 2, 2, 0, false);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkQueryFailedMetrics(IgniteCache<Integer, String> cache, Query qry) {
        try {
            // Execute invalid query.
            cache.query(qry).getAll();
        }
        catch (Exception ignored) {
            // No-op.
        }

        checkMetrics(cache, 1, 0, 1, 0, 1, true);

        try {
            // Execute invalid query again with the same parameters.
            cache.query(qry).getAll();
        }
        catch (Exception ignored) {
            // No-op.
        }

        checkMetrics(cache, 1, 0, 2, 0, 2, true);
    }

    /**
     * @param cache Cache.
     * @param cond Condition to check.
     * @param exp Expected value.
     */
    private static void waitingFor(final IgniteCache<?, ?> cache,
        final String cond, final int exp) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Collection<? extends QueryDetailMetrics> metrics = cache.queryDetailMetrics();

                switch (cond) {
                    case "size":
                        return metrics.size() == exp;

                    case "completions":
                        int completions = 0;

                        for (QueryDetailMetrics m : metrics)
                            completions += m.completions();

                        return completions == exp;

                    default:
                        return true;
                }
            }
        }, 5000);
    }
}
