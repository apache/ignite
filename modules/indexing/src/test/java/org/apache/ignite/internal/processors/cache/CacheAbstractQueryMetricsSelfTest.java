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
import org.apache.ignite.cache.query.QueryMetrics;
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
 * Tests for cache query metrics.
 */
public abstract class CacheAbstractQueryMetricsSelfTest extends GridCommonAbstractTest {
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, String> cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName("A");
        cacheCfg1.setCacheMode(cacheMode);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg1.setIndexedTypes(Integer.class, String.class);
        cacheCfg1.setStatisticsEnabled(true);

        CacheConfiguration<Integer, String> cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName("B");
        cacheCfg2.setCacheMode(cacheMode);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg2.setIndexedTypes(Integer.class, String.class);
        cacheCfg2.setStatisticsEnabled(true);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

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
     * Test metrics for Sql queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlQueryMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlQuery qry = new SqlQuery<>("String", "from String");

        checkQueryMetrics(cache, qry);
    }

    /**
     * Test metrics for Sql queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlQueryNotFullyFetchedMetrics() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).context().cache().jcache("A");

        SqlQuery qry = new SqlQuery<>("String", "from String");
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

        Collection<CacheAbstractQueryMetricsSelfTest.Worker> workers = new ArrayList<>();

        int repeat = 100;

        for (int i = 0; i < repeat; i++) {
            workers.add(new CacheAbstractQueryMetricsSelfTest.Worker(cache, new SqlFieldsQuery("select * from String limit " + i)));
            workers.add(new CacheAbstractQueryMetricsSelfTest.Worker(cache, new SqlQuery("String", "from String")));
            workers.add(new CacheAbstractQueryMetricsSelfTest.Worker(cache, new ScanQuery()));
            workers.add(new CacheAbstractQueryMetricsSelfTest.Worker(cache, new TextQuery("String", "1")));
        }

        for (CacheAbstractQueryMetricsSelfTest.Worker worker : workers)
            worker.start();

        for (CacheAbstractQueryMetricsSelfTest.Worker worker : workers)
            worker.join();

        checkMetrics(cache, repeat * 4, repeat * 4, 0, false);
    }

    /**
     * Check metrics.
     *
     * @param cache Cache to check metrics.
     * @param execs Expected number of executions.
     * @param completions Expected number of completions.
     * @param failures Expected number of failures.
     * @param first {@code true} if metrics checked for first query only.
     */
    private void checkMetrics(IgniteCache<Integer, String> cache, int execs, int completions, int failures, boolean first) {
        QueryMetrics m = cache.queryMetrics();

        assertNotNull(m);

        info("Metrics: " + m);

        assertEquals("Executions", execs, m.executions());
        assertEquals("Completions", completions, m.executions() - m.fails());
        assertEquals("Failures", failures, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);

        if (first)
            assertEquals("On first execution minTime == maxTime", m.minimumTime(), m.maximumTime());
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkQueryMetrics(IgniteCache<Integer, String> cache, Query qry) {
        cache.query(qry).getAll();

        checkMetrics(cache, 1, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        checkMetrics(cache, 2, 2, 0, false);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @param waitingForCompletion Waiting for query completion.
     */
    private void checkQueryNotFullyFetchedMetrics(IgniteCache<Integer, String> cache, Query qry,
        boolean waitingForCompletion) throws IgniteInterruptedCheckedException {
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingForCompletion(cache, 1);

        checkMetrics(cache, 1, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingForCompletion(cache, 2);

        checkMetrics(cache, 2, 2, 0, false);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkQueryFailedMetrics(IgniteCache<Integer, String> cache, Query qry) {
        try {
            cache.query(qry).getAll();
        }
        catch (Exception ignored) {
            // No-op.
        }

        checkMetrics(cache, 1, 0, 1, true);

        // Execute again with the same parameters.
        try {
            cache.query(qry).getAll();
        }
        catch (Exception ignored) {
            // No-op.
        }

        checkMetrics(cache, 2, 0, 2, true);
    }

    /**
     * @param cache Cache.
     * @param exp Expected.
     */
    private static void waitingForCompletion(final IgniteCache<Integer, String> cache,
        final int exp) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                QueryMetrics m = cache.queryMetrics();
                return m.executions() - m.fails() == exp;
            }
        }, 5000);
    }
}
