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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for cache query details metrics.
 */
@RunWith(JUnit4.class)
public class QueryHistoryMetricsSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    private static final int QUERY_HISTORY_SIZE = 3;

    /** Grid count. */
    private int gridCnt = 2;

    /**
     *
     */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(Integer.class, String.class);
        ccfg.setSqlFunctionClasses(Functions.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(configureCache("A"), configureCache("B"));

        cfg.setQueryHistoryStatisticsSize(3);

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

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from String where fail()=1");

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

        cache.query(new SqlFieldsQuery("select * from String")).getAll();

        cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();

        cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();

        cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();

        cache.query(new SqlQuery("String", "from String")).getAll();

        waitingFor("size", QUERY_HISTORY_SIZE);

        for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
            checkMetrics(QUERY_HISTORY_SIZE, i, 1, 0, false);

        // Check that collected metrics contains correct items: metrics for last N queries.
        Collection<QueryHistoryMetrics> metrics = grid(0).context().query().queryHistory();

        String lastMetrics = "";

        for (QueryHistoryMetrics m : metrics)
            lastMetrics += m.query() + ";";

        assertTrue(lastMetrics.contains("select * from String limit 2;"));
        assertTrue(lastMetrics.contains("SELECT \"A\".\"STRING\"._KEY, \"A\".\"STRING\"._VAL from String;"));

        cache = grid(0).context().cache().jcache("B");

        cache.query(new SqlFieldsQuery("select * from String")).getAll();
        cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
        cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
        cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
        cache.query(new SqlQuery("String", "from String")).getAll();

        waitingFor("size", QUERY_HISTORY_SIZE);

        for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
            checkMetrics(QUERY_HISTORY_SIZE, i, 1, 0, false);

        if (gridCnt > 1) {
            cache = grid(1).context().cache().jcache("A");

            cache.query(new SqlFieldsQuery("select * from String")).getAll();
            cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
            cache.query(new SqlQuery("String", "from String")).getAll();

            waitingFor("size", QUERY_HISTORY_SIZE);

            for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
                checkMetrics(QUERY_HISTORY_SIZE, i, 1, 0, false);

            cache = grid(1).context().cache().jcache("B");

            cache.query(new SqlFieldsQuery("select * from String")).getAll();
            cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();
            cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();
            cache.query(new SqlQuery("String", "from String")).getAll();

            waitingFor("size", QUERY_HISTORY_SIZE);

            for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
                checkMetrics(QUERY_HISTORY_SIZE, i, 1, 0, false);
        }
    }

    /**
     *
     */
    private static class Worker extends Thread {
        /**
         *
         */
        private final IgniteCache cache;

        /**
         *
         */
        private final Query qry;

        /**
         *
         */
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
            for (int i = 1; i <= QUERY_HISTORY_SIZE; i++)
                workers.add(new Worker(cache, new SqlFieldsQuery("select * from String limit " + i)));
        }

        for (Worker worker : workers)
            worker.start();

        for (Worker worker : workers)
            worker.join();

        for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
            checkMetrics(QUERY_HISTORY_SIZE, i, repeat, 0, false);
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

        checkNoQueryMetrics(cache, qry);
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

        checkNoQueryMetrics(cache, qry);
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
     * Check metrics.
     *
     * @param sz Expected size of metrics.
     * @param idx Index of metrics to check.
     * @param execs Expected number of executions.
     * @param failures Expected number of failures.
     * @param first {@code true} if metrics checked for first query only.
     */
    private void checkMetrics(int sz, int idx, int execs, int failures,
        boolean first) {

        Collection<QueryHistoryMetrics> metrics = grid(0).context().query().queryHistory();

        metrics.forEach(System.out::println);
        if (sz == 0)
            return;

        assertNotNull(metrics);
        assertEquals(sz, metrics.size());

        QueryHistoryMetrics m = new ArrayList<>(metrics).get(idx);

        info("Metrics: " + m);

        assertEquals("Executions", execs, m.executions());
        assertEquals("Failures", failures, m.failures());
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

        checkMetrics(1, 0, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        checkMetrics(1, 0, 2, 0, false);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     */
    private void checkNoQueryMetrics(IgniteCache<Integer, String> cache, Query qry) {
        // Execute query.
        cache.query(qry).getAll();

        checkMetrics(0, 0, 0, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        checkMetrics(0, 0, 0, 0, false);
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
            waitingFor("executions", 1);

        checkMetrics(0, 0, 0, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).iterator().next();

        if (waitingForCompletion)
            waitingFor("executions", 2);

        checkMetrics(0, 0, 0, 0, false);
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
            ignored.printStackTrace();
            // No-op.
        }

        checkMetrics(1, 0, 1, 1, true);

        try {
            // Execute invalid query again with the same parameters.
            cache.query(qry).getAll();
        }
        catch (Exception ignored) {
            // No-op.
        }

        checkMetrics(1, 0, 2, 2, true);
    }

    /**
     * @param cond Condition to check.
     * @param exp Expected value.
     */
    private void waitingFor(final String cond, final int exp) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Collection<QueryHistoryMetrics> metrics = grid(0).context().query().queryHistory();

                switch (cond) {
                    case "size":
                        return metrics.size() == exp;

                    case "executions":
                        int executions = 0;

                        for (QueryHistoryMetrics m : metrics)
                            executions += m.executions();

                        return executions == exp;

                    default:
                        return true;
                }
            }
        }, 5000);
    }

    /**
     *
     */
    public static class Functions {
        /**
         *
         */
        @QuerySqlFunction
        public static int fail() {
            throw new IgniteSQLException("SQL function fail for test purpuses");
        }
    }
}
