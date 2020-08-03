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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for cache query details metrics.
 */
public abstract class CacheAbstractQueryDetailMetricsSelfTest extends GridCommonAbstractTest {
    /**  */
    private static final int QRY_DETAIL_METRICS_SIZE = 3;

    /** Grid count. */
    protected int gridCnt;

    /** Cache mode. */
    protected CacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCnt);

        IgniteCache<Integer, String> cacheA = grid(0).cache("A");
        IgniteCache<Integer, String> cacheB = grid(0).cache("B");

        for (int i = 0; i < 100; i++) {
            cacheA.put(i, String.valueOf(i));
            cacheB.put(i, String.valueOf(i));
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(configureCache("A"), configureCache("B"));

        return cfg;
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from String");

        checkQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from String");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, qry, false);
    }

    /**
     * Test metrics for failed SQL queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsQueryFailedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from UNKNOWN");

        checkQueryFailedMetrics(cache, qry);
    }

    /**
     * Test metrics eviction.
     *
     * @throws Exception In case of error.
     */
    @Test
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
        assertTrue(lastMetrics.contains("SELECT \"A\".\"STRING\"._KEY, \"A\".\"STRING\"._VAL from String;"));

        cache = grid(0).context().cache().jcache("B");

        cache.query(new SqlFieldsQuery("select * from String")).getAll();
        cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
        cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
        cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
        cache.query(new ScanQuery()).getAll();
        cache.query(new SqlQuery("String", "from String")).getAll();

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

            cache.query(new SqlFieldsQuery("select * from String")).getAll();
            cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
            cache.query(new ScanQuery()).getAll();
            cache.query(new SqlQuery("String", "from String")).getAll();

            waitingFor(cache, "size", QRY_DETAIL_METRICS_SIZE);

            for (int i = 0; i < QRY_DETAIL_METRICS_SIZE; i++)
                checkMetrics(cache, QRY_DETAIL_METRICS_SIZE, i, 1, 1, 0, false);
        }
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
    @Test
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
    @Test
    public void testScanQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        ScanQuery<Integer, String> qry = new ScanQuery<>();

        checkQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testScanQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        ScanQuery<Integer, String> qry = new ScanQuery<>();
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, qry, true);
    }

    /**
     * Test metrics for failed Scan queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testScanQueryFailedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        ScanQuery<Integer, String> qry = new ScanQuery<>(Integer.MAX_VALUE);

        checkQueryFailedMetrics(cache, qry);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlQuery<Integer, String> qry = new SqlQuery<>("String", "from String");

        checkQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlQuery<Integer, String> qry = new SqlQuery<>("String", "from String");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, qry, true);
    }

    /**
     * Test metrics for Sql queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        TextQuery qry = new TextQuery<>("String", "1");

        checkQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for Sql queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        TextQuery qry = new TextQuery<>("String", "1");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, qry, true);
    }

    /**
     * Test metrics for failed Scan queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryFailedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        TextQuery qry = new TextQuery<>("Unknown", "zzz");

        checkQueryFailedMetrics(cache, qry);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsCrossCacheQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".String");

        checkQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsCrossCacheQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".String");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(cache, qry, false);
    }

    /**
     * Test metrics for failed SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsCrossCacheQueryFailedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"G\".String");

        checkQueryFailedMetrics(cache, qry);
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
    private void checkMetrics(IgniteCache<Integer, String> cache, int sz, int idx, int execs,
        int completions, int failures, boolean first) {
        Collection<? extends QueryDetailMetrics> metrics = cache.queryDetailMetrics();

        assertNotNull(metrics);
        assertEquals(sz, metrics.size());

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

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkQueryMetrics(IgniteCache<Integer, String> cache, Query qry) {
        // Execute query.
        cache.query(qry).getAll();

        checkMetrics(cache, 1, 0, 1, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        checkMetrics(cache, 1, 0, 2, 2, 0, false);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @param waitingForCompletion Waiting for query completion.
     */
    private void checkQueryNotFullyFetchedMetrics(IgniteCache<Integer, String> cache, Query qry,
        boolean waitingForCompletion) throws IgniteInterruptedCheckedException {
        // Execute query.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingFor(cache, "completions", 1);

        checkMetrics(cache, 1, 0, 1, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingFor(cache, "completions", 2);

        checkMetrics(cache, 1, 0, 2, 2, 0, false);
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
    private static void waitingFor(final IgniteCache<Integer, String> cache,
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
