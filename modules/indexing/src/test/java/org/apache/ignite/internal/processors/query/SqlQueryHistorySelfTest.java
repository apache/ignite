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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Check query history metrics from server node.
 */
public class SqlQueryHistorySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int QUERY_HISTORY_SIZE = 3;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startTestGrid();

        IgniteCache<Integer, String> cacheA = grid(0).cache("A");
        IgniteCache<Integer, String> cacheB = grid(0).cache("B");

        for (int i = 0; i < 100; i++) {
            cacheA.put(i, String.valueOf(i));
            cacheB.put(i, String.valueOf(i));
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ((IgniteH2Indexing)queryNode().context().query().getIndexing()).runningQueryManager()
            .resetQueryHistoryMetrics();
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

        cfg.setSqlConfiguration(new SqlConfiguration().setSqlQueryHistorySize(QUERY_HISTORY_SIZE));

        return cfg;
    }

    /**
     * Test metrics for JDBC.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testJdbcSelectQueryHistory() throws Exception {
        String qry = "select * from A.String";
        checkQueryMetrics(qry);
    }

    /**
     * Test metrics for JDBC in case not fully resultset is not fully read.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testJdbcSelectNotFullyFetchedQueryHistory() throws Exception {
        String qry = "select * from A.String";

        try (Connection conn = GridTestUtils.connect(queryNode(), null); Statement stmt = conn.createStatement()) {
            stmt.setFetchSize(1);

            ResultSet rs = stmt.executeQuery(qry);

            assertTrue(rs.next());

            checkMetrics(0, 0, 0, 0, true);
        }
    }

    /**
     * Test metrics for failed SQL queries.
     */
    @Test
    public void testJdbcQueryHistoryFailed() {
        try (Connection conn = GridTestUtils.connect(queryNode(), null); Statement stmt = conn.createStatement()) {
            stmt.executeQuery("select * from A.String where A.fail()=1");

            fail("Query should be failed.");
        }
        catch (Exception ignore) {
            //No-Op
        }

        checkMetrics(1, 0, 1, 1, true);
    }

    /**
     * Test metrics for JDBC in case of DDL and DML
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testJdbcQueryHistoryForDmlAndDdl() throws Exception {
        List<String> cmds = Arrays.asList(
            "create table TST(id int PRIMARY KEY, name varchar)",
            "insert into TST(id) values(1)",
            "commit"
        );

        try (Connection conn = GridTestUtils.connect(queryNode(), null); Statement stmt = conn.createStatement()) {
            for (String cmd : cmds)
                stmt.execute(cmd);
        }

        checkSeriesCommand(cmds);
    }

    /**
     * @param cmds List of SQL commands.
     * @throws IgniteInterruptedCheckedException In case of failure.
     */
    private void checkSeriesCommand(List<String> cmds) throws IgniteInterruptedCheckedException {
        waitingFor("size", QUERY_HISTORY_SIZE);

        for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
            checkMetrics(QUERY_HISTORY_SIZE, i, 1, 0, false);

        // Check that collected metrics contains correct items: metrics for last N queries.

        Collection<QueryHistory> metrics = ((IgniteH2Indexing)queryNode().context().query().getIndexing())
            .runningQueryManager().queryHistoryMetrics().values();

        assertEquals(QUERY_HISTORY_SIZE, metrics.size());

        Set<String> qries = metrics.stream().map(QueryHistory::query).collect(Collectors.toSet());

        for (int i = 0; i < cmds.size(); i++)
            assertTrue(qries.contains(cmds.get(QUERY_HISTORY_SIZE - 1 - i)));
    }

    /**
     * Test metrics for SQL fields queries.
     */
    @Test
    public void testSqlFieldsQueryHistory() {
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from String");

        checkQueryMetrics(qry);
    }

    /**
     * Test metrics for SQL fields queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsQueryHistoryNotFullyFetched() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from String");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(qry, false);
    }

    /**
     * Test metrics for failed SQL queries.
     */
    @Test
    public void testSqlFieldsQueryHistoryFailed() {
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from String where fail()=1");

        checkQueryFailedMetrics(qry);
    }

    /**
     * Test metrics eviction.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testQueryHistoryForDmlAndDdl() throws Exception {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

        List<String> cmds = Arrays.asList(
            "create table TST(id int PRIMARY KEY, name varchar)",
            "insert into TST(id) values(1)",
            "commit"
        );

        cmds.forEach((cmd) ->
            cache.query(new SqlFieldsQuery(cmd)).getAll()
        );

        checkSeriesCommand(cmds);
    }

    /**
     * Test metrics eviction.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testQueryHistoryEviction() throws Exception {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

        cache.query(new SqlFieldsQuery("select * from String")).getAll();

        cache.query(new SqlFieldsQuery("select count(*) from String")).getAll();

        cache.query(new SqlFieldsQuery("select * from String limit 1")).getAll();

        cache.query(new SqlFieldsQuery("select * from String limit 2")).getAll();

        cache.query(new SqlQuery<>("String", "from String")).getAll();

        waitingFor("size", QUERY_HISTORY_SIZE);

        for (int i = 0; i < QUERY_HISTORY_SIZE; i++)
            checkMetrics(QUERY_HISTORY_SIZE, i, 1, 0, false);

        // Check that collected metrics contains correct items: metrics for last N queries.
        Collection<QueryHistory> metrics = ((IgniteH2Indexing)queryNode().context().query().getIndexing())
            .runningQueryManager().queryHistoryMetrics().values();

        assertEquals(QUERY_HISTORY_SIZE, metrics.size());

        Set<String> qries = metrics.stream().map(QueryHistory::query).collect(Collectors.toSet());

        assertTrue(qries.contains("SELECT \"A\".\"STRING\"._KEY, \"A\".\"STRING\"._VAL from String"));
        assertTrue(qries.contains("select * from String limit 2"));
        assertTrue(qries.contains("select * from String limit 1"));
    }

    /**
     * Test metrics if queries executed from several threads.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testQueryHistoryMultithreaded() throws Exception {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

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
     */
    @Test
    public void testScanQueryHistory() {
        ScanQuery<Integer, String> qry = new ScanQuery<>();

        checkNoQueryMetrics(qry);
    }

    /**
     * Test metrics for Scan queries.
     */
    @Test
    public void testSqlQueryHistory() {
        SqlQuery<Integer, String> qry = new SqlQuery<>("String", "from String");

        checkQueryMetrics(qry);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlQueryHistoryNotFullyFetched() throws Exception {
        SqlQuery<Integer, String> qry = new SqlQuery<>("String", "from String");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(qry, true);
    }

    /**
     * Test metrics for Sql queries.
     */
    @Test
    public void testTextQueryMetrics() {
        TextQuery qry = new TextQuery<>("String", "1");

        checkNoQueryMetrics(qry);
    }

    /**
     * Test metrics for Sql queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryHistoryNotFullyFetched() throws Exception {
        TextQuery qry = new TextQuery<>("String", "1");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(qry, true);
    }

    /**
     * Test metrics for SQL cross cache queries.
     */
    @Test
    public void testSqlFieldsCrossCacheQueryHistory() {
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".String");

        checkQueryMetrics(qry);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testSqlFieldsQueryHistoryCrossCacheQueryNotFullyFetched() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".String");
        qry.setPageSize(10);

        checkQueryNotFullyFetchedMetrics(qry, false);
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

        Collection<QueryHistory> metrics = ((IgniteH2Indexing)queryNode().context().query().getIndexing())
            .runningQueryManager().queryHistoryMetrics().values();

        assertNotNull(metrics);
        assertEquals(sz, metrics.size());

        if (sz == 0)
            return;

        QueryHistory m = new ArrayList<>(metrics).get(idx);

        info("Metrics: " + m);

        assertEquals("Executions", execs, m.executions());
        assertEquals("Failures", failures, m.failures());
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);
        assertTrue(m.lastStartTime() > 0 && m.lastStartTime() <= System.currentTimeMillis());

        if (first)
            assertEquals("On first execution minTime == maxTime", m.minimumTime(), m.maximumTime());
    }

    /**
     * @param qry Query.
     * @throws SQLException In case of failure.
     */
    private void checkQueryMetrics(String qry) throws SQLException {

        runJdbcQuery(qry);

        checkMetrics(1, 0, 1, 0, true);

        // Execute again with the same parameters.
        runJdbcQuery(qry);

        checkMetrics(1, 0, 2, 0, false);
    }

    /**
     * @param qry SQL query.
     * @throws SQLException In case of failure.
     */
    private void runJdbcQuery(String qry) throws SQLException {
        try (Connection conn = GridTestUtils.connect(queryNode(), null); Statement stmt = conn.createStatement()) {
            stmt.execute(qry);
        }
    }

    /**
     * @param qry Query.
     */
    private void checkQueryMetrics(Query qry) {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

        // Execute query.
        cache.query(qry).getAll();

        checkMetrics(1, 0, 1, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        checkMetrics(1, 0, 2, 0, false);
    }

    /**
     * @param qry Query.
     */
    private void checkNoQueryMetrics(Query qry) {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

        // Execute query.
        cache.query(qry).getAll();

        checkMetrics(0, 0, 0, 0, true);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        checkMetrics(0, 0, 0, 0, false);
    }

    /**
     * @param qry Query.
     * @param waitingForCompletion Waiting for query completion.
     */
    private void checkQueryNotFullyFetchedMetrics(Query qry, boolean waitingForCompletion)
        throws IgniteInterruptedCheckedException {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

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
     * @param qry Query.
     */
    private void checkQueryFailedMetrics(Query qry) {
        IgniteCache<Integer, String> cache = queryNode().context().cache().jcache("A");

        try {
            // Execute invalid query.
            cache.query(qry).getAll();
        }
        catch (Exception ignored) {
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

        checkMetrics(1, 0, 2, 2, false);
    }

    /**
     * @param cond Condition to check.
     * @param exp Expected value.
     */
    private void waitingFor(final String cond, final int exp) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(() -> {
            Collection<QueryHistory> metrics = ((IgniteH2Indexing)queryNode().context().query().getIndexing())
                .runningQueryManager().queryHistoryMetrics().values();

            switch (cond) {
                case "size":
                    return metrics.size() == exp;

                case "executions":
                    int executions = 0;

                    for (QueryHistory m : metrics)
                        executions += m.executions();

                    return executions == exp;

                default:
                    return true;
            }
        }, 2000);
    }

    /**
     * @return Ignite instance for quering.
     */
    protected IgniteEx queryNode() {
        IgniteEx node = grid(0);

        assertFalse(node.context().clientNode());

        return node;
    }

    /**
     * @throws Exception In case of failure.
     */
    protected void startTestGrid() throws Exception {
        startGrids(2);
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
}
