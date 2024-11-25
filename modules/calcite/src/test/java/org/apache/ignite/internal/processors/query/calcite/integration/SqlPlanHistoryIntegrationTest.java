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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngineConfigurationEx;
import org.apache.ignite.internal.processors.query.running.SqlPlan;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.systemview.view.SqlPlanHistoryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_PLAN_HIST_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeFalse;

/** Tests for SQL plan history. */
@RunWith(Parameterized.class)
public class SqlPlanHistoryIntegrationTest extends GridCommonAbstractTest {
    /** SQL plan history size. */
    private static final int PLAN_HISTORY_SIZE = 10;

    /** SQL plan history size excess. */
    private static final int PLAN_HISTORY_EXCESS = 2;

    /** Simple SQL query. */
    private static final String SQL = "SELECT * FROM A.String";

    /** Failed SQL query. */
    private static final String SQL_FAILED = "select * from A.String where A.fail()=1";

    /** Cross-cache SQL query. */
    private static final String SQL_CROSS_CACHE = "SELECT * FROM B.String";

    /** Failed cross-cache SQL query. */
    private static final String SQL_CROSS_CACHE_FAILED = "select * from B.String where B.fail()=1";

    /** SQL query with reduce phase. */
    private static final String SQL_WITH_REDUCE_PHASE = "select o.name n1, p.name n2 from \"pers\".Person p, " +
        "\"org\".Organization o where p.orgId=o._key and o._key=101" +
        " union select o.name n1, p.name n2 from \"pers\".Person p, \"org\".Organization o" +
        " where p.orgId=o._key and o._key=102";

    /** List of simple DML commands and the simple queries flag. */
    private final IgniteBiTuple<List<String>, Boolean> dmlCmds = new IgniteBiTuple<>(
        Arrays.asList(
            "insert into A.String (_key, _val) values(101, '101')",
            "update A.String set _val='111' where _key=101",
            "delete from A.String where _key=101"
        ), true
    );

    /** List of DML commands with joins and the simple queries flag. */
    private final IgniteBiTuple<List<String>, Boolean> dmlCmdsWithJoins = new IgniteBiTuple<>(
        Arrays.asList(
            "insert into A.String (_key, _val) select o._key, p.name " +
                "from \"pers\".Person p, \"org\".Organization o where p.orgId=o._key",
            "update A.String set _val = 'updated' where _key in " +
                "(select o._key from \"pers\".Person p, \"org\".Organization o where p.orgId=o._key)",
            "delete from A.String where _key in (select orgId from \"pers\".Person)"
        ), false
    );

    /** Successful SqlFieldsQuery. */
    private final SqlFieldsQuery sqlFieldsQry = new SqlFieldsQuery(SQL);

    /** Failed SqlFieldsQuery. */
    private final SqlFieldsQuery sqlFieldsQryFailed = new SqlFieldsQuery(SQL_FAILED);

    /** Successful cross-cache SqlFieldsQuery. */
    private final SqlFieldsQuery sqlFieldsQryCrossCache = new SqlFieldsQuery(SQL_CROSS_CACHE);

    /** Failed cross-cache SqlFieldsQuery. */
    private final SqlFieldsQuery sqlFieldsQryCrossCacheFailed = new SqlFieldsQuery(SQL_CROSS_CACHE_FAILED);

    /** Succesful SqlFieldsQuery with reduce phase. */
    private final SqlFieldsQuery sqlFieldsQryWithReducePhase = new SqlFieldsQuery(SQL_WITH_REDUCE_PHASE)
        .setDistributedJoins(true);

    /** Failed SqlFieldsQueries with reduce phase. */
    private final SqlFieldsQuery[] sqlFieldsQryWithReducePhaseFailed = F.asArray(
        new SqlFieldsQuery(SQL_WITH_REDUCE_PHASE.replace("o._key=101", "o._key=fail()"))
            .setDistributedJoins(true),
        new SqlFieldsQuery(SQL_WITH_REDUCE_PHASE.replace("o._key=102", "o._key=fail()"))
            .setDistributedJoins(true));

    /** Successful SqlQuery. */
    private final SqlQuery sqlQry = new SqlQuery<>("String", "from String");

    /** Failed SqlQuery. */
    private final SqlQuery sqlQryFailed = new SqlQuery<>("String", "from String where fail()=1");

    /** ScanQuery. */
    private final ScanQuery<Integer, String> scanQry = new ScanQuery<>();

    /** TextQuery. */
    private final TextQuery<Integer, String> textQry = new TextQuery<>("String", "2");

    /** SQL engine. */
    @Parameterized.Parameter
    public String sqlEngine;

    /** Client mode flag. */
    @Parameterized.Parameter(1)
    public boolean isClient;

    /** Local query flag. */
    @Parameterized.Parameter(2)
    public boolean loc;

    /** Fully-fetched query flag. */
    @Parameterized.Parameter(3)
    public boolean isFullyFetched;

    /** */
    @Parameterized.Parameters(name = "sqlEngine={0}, isClient={1} loc={2}, isFullyFetched={3}")
    public static Collection<Object[]> params() {
        return Arrays.stream(new Object[][]{
            {CalciteQueryEngineConfiguration.ENGINE_NAME},
            {IndexingQueryEngineConfiguration.ENGINE_NAME}
        }).flatMap(sqlEngine -> Arrays.stream(sqlEngine[0].equals(IndexingQueryEngineConfiguration.ENGINE_NAME) ?
                new Boolean[]{false} : new Boolean[]{true, false})
                .flatMap(isClient -> Arrays.stream(isClient ? new Boolean[]{false} : new Boolean[]{true, false})
                    .flatMap(loc -> Arrays.stream(new Boolean[]{true, false})
                        .map(isFullyFetched -> new Object[]{sqlEngine[0], isClient, loc, isFullyFetched})))
        ).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryEngineConfigurationEx engCfg = configureSqlEngine();

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setSqlPlanHistorySize(PLAN_HISTORY_SIZE)
            .setQueryEnginesConfiguration(engCfg)
        );

        return cfg.setCacheConfiguration(
            configureCache("A", Integer.class, String.class),
            configureCache("B", Integer.class, String.class),
            configureCache("pers", Integer.class, Person.class),
            configureCache("org", Integer.class, Organization.class)
        );
    }

    /** */
    protected QueryEngineConfigurationEx configureSqlEngine() {
        if (sqlEngine.equals(CalciteQueryEngineConfiguration.ENGINE_NAME))
            return new CalciteQueryEngineConfiguration();
        else
            return new IndexingQueryEngineConfiguration();
    }

    /**
     * @param name Cache name.
     * @param idxTypes Index types.
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> configureCache(String name, Class<?>... idxTypes) {
        return new CacheConfiguration<>()
            .setName(name)
            .setIndexedTypes(idxTypes)
            .setSqlFunctionClasses(Functions.class);
    }

    /**
     * @return Ignite node where queries are executed.
     */
    protected IgniteEx queryNode() {
        IgniteEx node = isClient ? grid(1) : grid(0);

        if (isClient)
            assertEquals(isClient, node.context().clientNode());

        return node;
    }

    /**
     * Starts and initiates Ignite instance.
     *
     * @throws Exception In case of failure.
     */
    protected void startTestGrid() throws Exception {
        startGrid(0);

        if (isClient)
            startClientGrid(1);

        IgniteCache<Integer, String> cacheA = queryNode().cache("A");
        IgniteCache<Integer, String> cacheB = queryNode().cache("B");

        for (int i = 0; i < 100; i++) {
            cacheA.put(i, String.valueOf(i));
            cacheB.put(i, String.valueOf(i));
        }

        IgniteCache<Integer, Person> cachePers = queryNode().cache("pers");
        IgniteCache<Integer, Organization> cacheOrg = queryNode().cache("org");

        cacheOrg.put(101, new Organization("o1"));
        cacheOrg.put(102, new Organization("o2"));
        cachePers.put(103, new Person(101, "p1"));
        cachePers.put(104, new Person(102, "p2"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Checks successful JDBC queries. */
    @Test
    public void testJdbcQuery() throws Exception {
        for (int i = 0; i < 2; i++) {
            jdbcQuery(SQL);

            checkSqlPlanHistory(1);
        }
    }

    /** Checks failed JDBC queries. */
    @Test
    public void testJdbcQueryFailed() throws Exception {
        try {
            jdbcQuery(SQL_FAILED);
        }
        catch (Exception e) {
            if (e instanceof AssumptionViolatedException)
                throw e;
        }

        checkSqlPlanHistory(1);
    }

    /** Checks successful SqlFieldsQuery. */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        runSuccessfulQuery(sqlFieldsQry);
    }

    /** Checks failed SqlFieldsQuery. */
    @Test
    public void testSqlFieldsQueryFailed() throws Exception {
        runFailedQuery(sqlFieldsQryFailed);
    }

    /** Checks successful cross-cache SqlFieldsQuery. */
    @Test
    public void testSqlFieldsCrossCacheQuery() throws Exception {
        runSuccessfulQuery(sqlFieldsQryCrossCache);
    }

    /** Checks failed cross-cache SqlFieldsQuery. */
    @Test
    public void testSqlFieldsCrossCacheQueryFailed() throws Exception {
        runFailedQuery(sqlFieldsQryCrossCacheFailed);
    }

    /**
     * Checks successful SqlFieldsQuery with reduce phase. There should be 3 entries in SQL plan history on the
     * reduce node (2 SELECT subqueries and 1 UNION/MERGE subquery) and 2 entries on the map node (2 SELECT subqueries).
     */
    @Test
    public void testSqlFieldsQueryWithReducePhase() throws Exception {
        runQueryWithReducePhase(() -> {
            try {
                startGridsMultiThreaded(1, 2);

                awaitPartitionMapExchange();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            cacheQuery(sqlFieldsQryWithReducePhase, "pers");

            checkSqlPlanHistory(3);

            for (int i = 1; i <= 2; i++) {
                Map<SqlPlan, Long> sqlPlansOnMapNode = grid(i).context().query().runningQueryManager()
                    .planHistoryTracker().sqlPlanHistory();

                assertNotNull(sqlPlansOnMapNode);

                checkMetrics(2, sqlPlansOnMapNode);
            }
        });
    }

    /**
     * Checks failed SqlFieldsQuery with reduce phase. If the fisrt subquery fails, there should be only one entry in
     * SQL plan history. If the fisrt subquery is successfully executed but the second one fails, the history should
     * contain two entries.
     */
    @Test
    public void testSqlFieldsQueryWithReducePhaseFailed() throws Exception {
        runQueryWithReducePhase(() -> {
            for (int i = 0; i < sqlFieldsQryWithReducePhaseFailed.length; i++) {
                try {
                    cacheQuery(sqlFieldsQryWithReducePhaseFailed[i], "pers");
                }
                catch (Exception ignore) {
                    // No-op.
                }

                checkSqlPlanHistory( i + 1);

                queryNode().context().query().runningQueryManager().resetPlanHistoryMetrics();
            }
        });
    }

    /** Checks successful SqlQuery. */
    @Test
    public void testSqlQuery() throws Exception {
        runSuccessfulQuery(sqlQry);
    }

    /** Checks failed SqlQuery. */
    @Test
    public void testSqlQueryFailed() throws Exception {
        runFailedQuery(sqlQryFailed);
    }

    /** Checks ScanQuery. */
    @Test
    public void testScanQuery() throws Exception {
        runQueryWithoutPlan(scanQry);
    }

    /** Checks TextQuery. */
    @Test
    public void testTextQuery() throws Exception {
        runQueryWithoutPlan(textQry);
    }

    /** Checks DML commands executed via JDBC. */
    @Test
    public void testJdbcDml() throws Exception {
        runJdbcDml(dmlCmds);
    }

    /** Checks DML commands with joins executed via JDBC. */
    @Test
    public void testJdbcDmlWithJoins() throws Exception {
        runJdbcDml(dmlCmdsWithJoins);
    }

    /** Checks DML commands executed via SqlFieldsQuery. */
    @Test
    public void testSqlFieldsDml() throws Exception {
        runSqlFieldsDml(dmlCmds);
    }

    /** Checks DML commands with joins executed via SqlFieldsQuery. */
    @Test
    public void testSqlFieldsDmlWithJoins() throws Exception {
        runSqlFieldsDml(dmlCmdsWithJoins);
    }

    /** Checks that older plan entries are evicted when maximum history size is reached. */
    @Test
    public void testPlanHistoryEviction() throws Exception {
        startTestGrid();

        IgniteCache<Integer, String> cache = queryNode().cache("A");

        for (int i = 1; i <= (PLAN_HISTORY_SIZE + PLAN_HISTORY_EXCESS); i++) {
            cache.put(100 + i, "STR" + String.format("%02d", i));

            SqlFieldsQuery qry = new SqlFieldsQuery(SQL + " where _val='STR" + String.format("%02d", i) + "'");

            cacheQuery(qry.setLocal(loc), "A");
        }

        assertTrue(waitForCondition(() -> getSqlPlanHistory().size() == PLAN_HISTORY_SIZE, 1000));

        Set<String> qrys = getSqlPlanHistory().keySet().stream().map(SqlPlan::query).collect(Collectors.toSet());

        for (int i = 1; i <= PLAN_HISTORY_EXCESS; i++) {
            int finI = i;

            assertFalse(qrys.stream().anyMatch(str -> str.contains("STR" + String.format("%02d", finI))));
        }

        assertTrue(qrys.stream().anyMatch(str -> str.contains("STR" + String.format("%02d", PLAN_HISTORY_EXCESS + 1))));
    }

    /**
     * Checks that older SQL plan history entries are replaced with newer ones with the same parameters (except for
     * the beginning time).
     */
    @Test
    public void testEntryReplacement() throws Exception {
        assumeFalse("With the H2 engine, scan counts can be added to SQL plans for local queries ",
            loc && sqlEngine == IndexingQueryEngineConfiguration.ENGINE_NAME);

        startTestGrid();

        long[] timeStamps = new long[2];

        for (int i = 0; i < 2; i++) {
            cacheQuery(sqlFieldsQry.setLocal(loc), "A");

            checkSqlPlanHistory(1);

            timeStamps[i] = F.first(getSqlPlanHistory().values());

            if (i == 0) {
                long ts0 = U.currentTimeMillis();

                assertTrue(waitForCondition(() -> (U.currentTimeMillis() != ts0), 1000));
            }
        }

        assertTrue(timeStamps[1] > timeStamps[0]);
    }

    /** Checks that SQL plan history remains empty if history size is set to zero. */
    @Test
    public void testEmptyPlanHistory() throws Exception {
        startTestGrid();

        queryNode().context().query().runningQueryManager().planHistoryTracker().setHistorySize(0);

        cacheQuery(sqlFieldsQry.setLocal(loc), "A");

        assertTrue(getSqlPlanHistory().isEmpty());
    }

    /** Checks that new entries are present in the plan history after history size is reset. */
    @Test
    public void testResetPlanHistorySize() throws Exception {
        checkReset(() -> queryNode().context().query().runningQueryManager().planHistoryTracker().setHistorySize(5));
    }

    /** Checks that new entries are present in the plan history after PlanHistoryTracker is reset. */
    @Test
    public void testResetPlanHistoryMetrics() throws Exception {
        checkReset(() -> queryNode().context().query().runningQueryManager().resetPlanHistoryMetrics());
    }

    /**
     * @param qry Query.
     */
    public void runSuccessfulQuery(Query qry) throws Exception {
        startTestGrid();

        for (int i = 0; i < 2; i++) {
            cacheQuery(qry.setLocal(loc), "A");

            checkSqlPlanHistory(1);
        }
    }

    /**
     * @param qry Query.
     */
    public void runFailedQuery(Query qry) throws Exception {
        startTestGrid();

        try {
            cacheQuery(qry.setLocal(loc), "A");
        }
        catch (Exception ignore) {
            //No-Op
        }

        checkSqlPlanHistory(1);
    }

    /**
     * @param qry Query.
     */
    public void runQueryWithoutPlan(Query qry) throws Exception {
        startTestGrid();

        cacheQuery(qry.setLocal(loc), "A");

        checkSqlPlanHistory(0);
    }

    /**
     * @param task Test task to execute.
     */
    public void runQueryWithReducePhase(Runnable task) throws Exception {
        assumeFalse("Map/reduce queries are only applicable to H2 engine",
            sqlEngine != IndexingQueryEngineConfiguration.ENGINE_NAME);

        assumeFalse("Only distributed queries have map and reduce phases", loc);

        startTestGrid();

        task.run();
    }

    /**
     * @param qry Query.
     */
    private void jdbcQuery(String qry) throws Exception {
        assumeFalse("There is no 'local query' parameter for JDBC queries", loc);

        if (Ignition.allGrids().isEmpty())
            startTestGrid();

        try (
            Connection conn = GridTestUtils.connect(queryNode(), null);
            Statement stmt = conn.createStatement()
        ) {
            if (!isFullyFetched)
                stmt.setFetchSize(1);

            ResultSet rs = stmt.executeQuery(qry);

            assertTrue(rs.next());
        }
    }

    /**
     * @param qry Query.
     * @param cacheName Cache name.
     */
    public void cacheQuery(Query qry, String cacheName) {
        IgniteCache<Integer, String> cache = queryNode().getOrCreateCache(cacheName);

        if (isFullyFetched)
            assertFalse(cache.query(qry).getAll().isEmpty());
        else {
            qry.setPageSize(1);

            assertNotNull(cache.query(qry).iterator().next());
        }
    }

    /**
     * @param qrysInfo DML commands info (queries, simple query flag).
     */
    public void runJdbcDml(IgniteBiTuple<List<String>, Boolean> qrysInfo) throws Exception {
        assumeFalse("There is no 'local query' parameter for JDBC queries", loc);

        executeDml(qrysInfo, (cmds) -> {
            try (
                Connection conn = GridTestUtils.connect(queryNode(), null);
                Statement stmt = conn.createStatement()
            ) {
                for (String cmd : cmds)
                    stmt.execute(cmd);
            }
            catch (SQLException e) {
                new RuntimeException(e);
            }
        });
    }

    /**
     * @param qrysInfo DML commands info (queries, simple query flag).
     */
    public void runSqlFieldsDml(IgniteBiTuple<List<String>, Boolean> qrysInfo) throws Exception {
        executeDml(qrysInfo, (cmds) -> {
            IgniteCache<Integer, String> cache = queryNode().getOrCreateCache("A");

            cmds.forEach(cmd -> cache.query(new SqlFieldsQuery(cmd).setLocal(loc)));
        });
    }

    /**
     * @param qrysInfo DML commands info (queries, simple query flag).
     * @param task Task to be executed.
     */
    public void executeDml(IgniteBiTuple<List<String>, Boolean> qrysInfo, Consumer<List<String>> task) throws Exception {
        assumeFalse("There is no lazy mode for DML operations", !isFullyFetched);

        startTestGrid();

        List<String> cmds = qrysInfo.get1();

        task.accept(cmds);

        checkSqlPlanHistoryDml(cmds.size(), qrysInfo.get2());
    }

    /** Returns current SQL plan history on the query node. */
    public Map<SqlPlan, Long> getSqlPlanHistory() {
        SystemView<SqlPlanHistoryView> views = queryNode().context().systemView().view(SQL_PLAN_HIST_VIEW);

        Map<SqlPlan, Long> res = new LinkedHashMap<>();

        views.forEach(entry -> res.put(entry.sqlPlan().getKey(), entry.sqlPlan().getValue()));

        return res;
    }

    /**
     * Checks SQL plan history for select queries.
     *
     * @param size Number of SQL plan entries expected to be in the history.
     */
    public void checkSqlPlanHistory(int size) {
        Map<SqlPlan, Long> sqlPlans = getSqlPlanHistory();

        assertNotNull(sqlPlans);

        checkMetrics(size, sqlPlans);
    }

    /**
     * Checks SQL plan history for DML operations.
     *
     * @param size Number of SQL plan entries expected to be in the history.
     * @param isSimpleQry Simple query flag.
     */
    public void checkSqlPlanHistoryDml(int size, boolean isSimpleQry) {
        Map<SqlPlan, Long> sqlPlans = getSqlPlanHistory();

        assertNotNull(sqlPlans);

        if (sqlEngine == IndexingQueryEngineConfiguration.ENGINE_NAME) {
            String check;

            if (isSimpleQry)
                check = "no SELECT queries have been executed.";
            else
                check = "the following " + (loc ? "local " : "") + "query has been executed:";

            sqlPlans = sqlPlans.entrySet().stream()
                .filter(e -> e.getKey().plan().contains(check))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        checkMetrics(size, sqlPlans);
    }

    /**
     * Checks metrics of provided SQL plan history entries.
     *
     * @param size Number of SQL plan entries expected to be in the history.
     * @param sqlPlans Sql plans recorded in the history.
     */
    public void checkMetrics(int size, Map<SqlPlan, Long> sqlPlans) {
        if (size == 1 && sqlPlans.size() == 2) {
            List<Map.Entry<SqlPlan, Long>> sortedPlans = new ArrayList<>(sqlPlans.entrySet()).stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .collect(Collectors.toList());

            String plan1 = sortedPlans.get(0).getKey().plan();
            String plan2 = sortedPlans.get(1).getKey().plan();

            assertTrue(plan2.contains(plan1) && plan2.contains("/* scanCount"));
        }
        else
            assertTrue(size == sqlPlans.size());

        if (size == 0)
            return;

        for (Map.Entry<SqlPlan, Long> plan : sqlPlans.entrySet()) {
            assertEquals(loc, plan.getKey().local());
            assertEquals(sqlEngine, plan.getKey().engine());

            assertNotNull(plan.getKey().plan());
            assertNotNull(plan.getKey().query());
            assertNotNull(plan.getKey().schema());

            assertTrue(plan.getValue() > 0);
        }
    }

    /**
     * Compares entries in the plan history before and after the reset event.
     *
     * @param reset Reset event.
     */
    public void checkReset(Runnable reset) throws Exception {
        startTestGrid();

        String[] qryText = new String[2];

        for (int i = 0; i < 2; i++) {
            try {
                cacheQuery(new SqlFieldsQuery(SQL + " where A.fail()=" + i).setLocal(loc), "A");
            }
            catch (Exception ignore) {
                //No-Op
            }

            qryText[i] = getSqlPlanHistory().keySet().stream().findFirst().map(SqlPlan::query).orElse("");

            if (i == 0)
                reset.run();
        }

        assertNotEquals(qryText[0], qryText[1]);
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        int orgId;

        /** */
        @QuerySqlField(index = true)
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }

    /** */
    private static class Organization {
        /** */
        @QuerySqlField
        String name;

        /**
         * @param name Organization name.
         */
        public Organization(String name) {
            this.name = name;
        }
    }

    /** */
    public static class Functions {
        /** */
        @QuerySqlFunction
        public static int fail() {
            throw new IgniteSQLException("SQL function fail for test purpuses");
        }
    }
}
