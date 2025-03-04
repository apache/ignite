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
import java.util.List;
import java.util.Optional;
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

import static org.apache.ignite.configuration.SqlConfiguration.DFLT_SQL_PLAN_HISTORY_SIZE_CALCITE;
import static org.apache.ignite.configuration.SqlConfiguration.DFLT_SQL_PLAN_HISTORY_SIZE_H2;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_PLAN_HIST_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** Tests for SQL plan history. */
@RunWith(Parameterized.class)
public class SqlPlanHistoryIntegrationTest extends GridCommonAbstractTest {
    /** SQL plan history size excess. */
    private static final int PLAN_HISTORY_EXCESS = 2;

    /** Simple SQL query. */
    private static final String SQL = "select * from A.String";

    /** Failed SQL query. */
    private static final String SQL_FAILED = "select * from A.String where A.fail()=1";

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

    /** Flag indicating whether SQL is configured by using {@link IgniteConfiguration#setSqlConfiguration(SqlConfiguration)}. */
    private boolean isSqlConfigured = true;

    /** Flag indicating whether the SQL engine is configured. */
    private boolean isSqlEngineConfigured = true;

    /**
     * Flag indicating whether a custom SQL plan history size is explicitly set. If {@code false}, the default SQL plan
     * history size will be used.
     */
    private boolean isPlanHistorySizeSet = true;

    /** SQL plan history size. */
    private int planHistorySize = 10;

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

    /**
     * @return Test parameters.
     */
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

        if (isSqlConfigured) {
            SqlConfiguration sqlCfg = new SqlConfiguration();

            if (isSqlEngineConfigured) {
                sqlCfg.setQueryEnginesConfiguration(configureSqlEngine());

                if (isPlanHistorySizeSet)
                    sqlCfg.setSqlPlanHistorySize(planHistorySize);
            }

            cfg.setSqlConfiguration(sqlCfg);
        }

        return cfg.setCacheConfiguration(
            configureCache("A", Integer.class, String.class),
            configureCache("B", Integer.class, String.class),
            configureCache("pers", Integer.class, Person.class),
            configureCache("org", Integer.class, Organization.class)
        );
    }

    /**
     * @return SQL engine configuration.
     */
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

        assertEquals(isClient, node.context().clientNode());

        return node;
    }

    /** Starts Ignite instance, initiates and populates caches. */
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
        runSuccessfulQuery(new SqlFieldsQuery(SQL));
    }

    /** Checks failed SqlFieldsQuery. */
    @Test
    public void testSqlFieldsQueryFailed() throws Exception {
        runFailedQuery(new SqlFieldsQuery(SQL_FAILED));
    }

    /** Checks successful cross-cache SqlFieldsQuery. */
    @Test
    public void testSqlFieldsCrossCacheQuery() throws Exception {
        runSuccessfulQuery(new SqlFieldsQuery(SQL.replace("A.", "B.")));
    }

    /** Checks failed cross-cache SqlFieldsQuery. */
    @Test
    public void testSqlFieldsCrossCacheQueryFailed() throws Exception {
        runFailedQuery(new SqlFieldsQuery(SQL_FAILED.replace("A.", "B.")));
    }

    /**
     * Checks successful SqlFieldsQuery with reduce phase. There should be 3 entries in SQL plan history on the
     * reduce node (2 SELECT subqueries and 1 UNION/MERGE subquery) and 2 entries on the map node (2 SELECT subqueries).
     */
    @Test
    public void testSqlFieldsQueryWithReducePhase() throws Exception {
        assumeTrue("Map/reduce queries are only applicable to H2 engine",
            sqlEngine == IndexingQueryEngineConfiguration.ENGINE_NAME);

        assumeFalse("Only distributed queries have map and reduce phases", loc);

        startTestGrid();

        try {
            startGridsMultiThreaded(1, 2);

            awaitPartitionMapExchange();

            cacheQuery(new SqlFieldsQuery(SQL_WITH_REDUCE_PHASE).setDistributedJoins(true), "pers");

            checkSqlPlanHistory(3);

            for (int i = 1; i <= 2; i++) {
                List<SqlPlanHistoryView> sqlPlansOnMapNode = getSqlPlanHistory(grid(i));

                assertTrue(waitForCondition(() -> sqlPlansOnMapNode.size() == 2, 1000));

                checkMetrics(sqlPlansOnMapNode);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Checks successful SqlQuery. */
    @Test
    public void testSqlQuery() throws Exception {
        runSuccessfulQuery(new SqlQuery<>("String", "from String"));
    }

    /** Checks failed SqlQuery. */
    @Test
    public void testSqlQueryFailed() throws Exception {
        runFailedQuery(new SqlQuery<>("String", "from String where fail()=1"));
    }

    /** Checks ScanQuery. */
    @Test
    public void testScanQuery() throws Exception {
        runQueryWithoutPlan(new ScanQuery<>());
    }

    /** Checks TextQuery. */
    @Test
    public void testTextQuery() throws Exception {
        runQueryWithoutPlan(new TextQuery<>("String", "2"));
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

        for (int i = 1; i <= (planHistorySize + PLAN_HISTORY_EXCESS); i++) {
            cache.put(100 + i, "STR" + String.format("%02d", i));

            SqlFieldsQuery qry = new SqlFieldsQuery(SQL + " where _val='STR" + String.format("%02d", i) + "'");

            cacheQuery(qry, "A");
        }

        assertTrue(waitForCondition(() -> getSqlPlanHistory().stream().map(SqlPlanHistoryView::sql).anyMatch(str ->
            str.contains("STR" + String.format("%02d", planHistorySize + PLAN_HISTORY_EXCESS))), 1000));

        Set<String> qrys = getSqlPlanHistory().stream().map(SqlPlanHistoryView::sql).collect(Collectors.toSet());

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
        startTestGrid();

        long firstTs;

        cacheQuery(new SqlFieldsQuery(SQL), "A");

        assertTrue(waitForCondition(() -> !getSqlPlanHistory().isEmpty(), 1000));

        firstTs = F.first(getSqlPlanHistory()).lastStartTime().getTime();

        long curTs = U.currentTimeMillis();

        assertTrue(waitForCondition(() -> (U.currentTimeMillis() != curTs), 1000));

        cacheQuery(new SqlFieldsQuery(SQL), "A");

        assertTrue(waitForCondition(() -> F.first(getSqlPlanHistory()).lastStartTime().getTime() > firstTs, 1000));
    }

    /** Checks that SQL plan history stays empty if the grid is started with a zero history size. */
    @Test
    public void testEmptyPlanHistoryInit() throws Exception {
        checkEmptyHistory(() -> planHistorySize = 0, false);
    }

    /** Checks that SQL plan history stays empty if history size is set to zero after the grid has been started. */
    @Test
    public void testEmptyPlanHistorySet() throws Exception {
        checkEmptyHistory(() ->
            queryNode().context().query().runningQueryManager().planHistoryTracker().setHistorySize(0), true);
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
     * Checks that the H2 default SQL plan history size is applied when SQL is not explicitly configured. In such cases,
     * H2 is used as the default engine.
     */
    @Test
    public void testNoSqlConfiguration() throws Exception {
        isSqlConfigured = false;

        checkDefaultSettings((histSize) ->
            assertEquals(Optional.of(DFLT_SQL_PLAN_HISTORY_SIZE_H2).get(), histSize), false);
    }

    /**
     * Checks that the H2 default SQL plan history size is applied when the SQL engine is not configured. In such cases,
     * H2 is used as the default engine.
     */
    @Test
    public void testNoSqlEngineConfiguration() throws Exception {
        isSqlEngineConfigured = false;

        checkDefaultSettings((histSize) ->
            assertEquals(Optional.of(DFLT_SQL_PLAN_HISTORY_SIZE_H2).get(), histSize), false);
    }

    /** Checks that the default SQL plan history size is applied when the history size is not explicitly set. */
    @Test
    public void testDefaultHistorySize() throws Exception {
        isPlanHistorySizeSet = false;

        checkDefaultSettings((histSize) -> {
            if (sqlEngine.equals(CalciteQueryEngineConfiguration.ENGINE_NAME))
                assertEquals(Optional.of(DFLT_SQL_PLAN_HISTORY_SIZE_CALCITE).get(), histSize);
            else if (sqlEngine.equals(IndexingQueryEngineConfiguration.ENGINE_NAME))
                assertEquals(Optional.of(DFLT_SQL_PLAN_HISTORY_SIZE_H2).get(), histSize);
        }, true);
    }

    /**
     * Checks that there is no 'scanCount' suffix in H2 local query plans even if identical queries are not
     * executed one after another (when there are other queries executed between them).
     */
    @Test
    public void testNoScanCountSuffix() throws Exception {
        assumeTrue("ScanCount suffix can only be present in H2 local query plans",
            sqlEngine == IndexingQueryEngineConfiguration.ENGINE_NAME && loc);

        startTestGrid();

        final int iterations = 5;

        for (int i = 0; i < iterations; i++) {
            cacheQuery(new SqlFieldsQuery(SQL), "A");

            cacheQuery(new SqlFieldsQuery(SQL.replace("A", "B")), "A");

            cacheQuery(new SqlFieldsQuery(SQL + " where _key > 10"), "A");

            if (i != (iterations - 1)) {
                long startTs = U.currentTimeMillis();

                while (U.currentTimeMillis() - startTs <= 250)
                    doSleep(50L);
            }
        }

        checkSqlPlanHistory(3);
    }

    /**
     * @param qry Query.
     */
    public void runSuccessfulQuery(Query qry) throws Exception {
        startTestGrid();

        for (int i = 0; i < 2; i++) {
            cacheQuery(qry, "A");

            checkSqlPlanHistory(1);
        }
    }

    /**
     * @param qry Query.
     */
    public void runFailedQuery(Query qry) throws Exception {
        startTestGrid();

        try {
            cacheQuery(qry, "A");
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

        cacheQuery(qry, "A");

        assertTrue(getSqlPlanHistory().isEmpty());
    }

    /**
     * @param qryText Query text.
     */
    private void jdbcQuery(String qryText) throws Exception {
        assumeFalse("There is no 'local query' parameter for JDBC queries", loc);

        if (Ignition.allGrids().isEmpty())
            startTestGrid();

        try (
            Connection conn = GridTestUtils.connect(queryNode(), null);
            Statement stmt = conn.createStatement()
        ) {
            if (!isFullyFetched)
                stmt.setFetchSize(1);

            ResultSet rs = stmt.executeQuery(qryText);

            assertTrue(rs.next());
        }
    }

    /**
     * @param qry Query.
     * @param cacheName Cache name.
     */
    public void cacheQuery(Query qry, String cacheName) {
        IgniteCache<Integer, String> cache = queryNode().getOrCreateCache(cacheName);

        qry.setLocal(loc);

        if (isFullyFetched)
            assertFalse(cache.query(qry).getAll().isEmpty());
        else {
            qry.setPageSize(1);

            assertNotNull(cache.query(qry).iterator().next());
        }
    }

    /**
     * @param qrysInfo DML commands info (command text, simple query flag).
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
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param qrysInfo DML commands info (command text, simple query flag).
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
        assumeTrue("There is no lazy mode for DML operations", isFullyFetched);

        startTestGrid();

        List<String> cmds = qrysInfo.get1();

        task.accept(cmds);

        checkSqlPlanHistoryDml(cmds.size(), qrysInfo.get2());
    }

    /**
     * @return current SQL plan history on the query node.
     */
    public List<SqlPlanHistoryView> getSqlPlanHistory() {
        return getSqlPlanHistory(queryNode());
    }

    /**
     * @param node Ignite node.
     * @return current SQL plan history on a given node.
     */
    public List<SqlPlanHistoryView> getSqlPlanHistory(IgniteEx node) {
        List<SqlPlanHistoryView> res = new ArrayList<>();

        SystemView<SqlPlanHistoryView> plans = node.context().systemView().view(SQL_PLAN_HIST_VIEW);

        plans.forEach(res::add);

        return res;
    }

    /**
     * Checks SQL plan history for select queries.
     *
     * @param size Number of SQL plan entries expected to be in the history.
     */
    public void checkSqlPlanHistory(int size) throws Exception {
        assertTrue(waitForCondition(() -> getSqlPlanHistory().size() == size, 1000));

        checkMetrics(getSqlPlanHistory());
    }

    /**
     * Checks SQL plan history for DML operations.
     *
     * @param size Number of SQL plan entries expected to be in the history.
     * @param isSimpleQry Simple query flag.
     */
    public void checkSqlPlanHistoryDml(int size, boolean isSimpleQry) {
        List<SqlPlanHistoryView> sqlPlans = getSqlPlanHistory();

        assertNotNull(sqlPlans);

        if (sqlEngine == IndexingQueryEngineConfiguration.ENGINE_NAME) {
            String check;

            if (isSimpleQry)
                check = "no SELECT queries have been executed.";
            else
                check = "the following " + (loc ? "local " : "") + "query has been executed:";

            sqlPlans = sqlPlans.stream().filter(e -> e.plan().contains(check)).collect(Collectors.toList());
        }

        assertTrue(sqlPlans.size() == size);

        checkMetrics(sqlPlans);
    }

    /**
     * Checks metrics of provided SQL plan history entries.
     *
     * @param sqlPlans Sql plans recorded in the history.
     */
    public void checkMetrics(List<SqlPlanHistoryView> sqlPlans) {
        for (SqlPlanHistoryView plan : sqlPlans) {
            assertEquals(loc, plan.local());
            assertEquals(sqlEngine, plan.engine());

            assertNotNull(plan.plan());
            assertNotNull(plan.sql());
            assertNotNull(plan.schemaName());

            assertTrue(plan.lastStartTime().getTime() > 0);

            if (loc)
                assertFalse(plan.plan().contains("/* scanCount"));
        }
    }

    /**
     * Compares entries in the plan history before and after the reset event.
     *
     * @param reset Reset event.
     */
    public void checkReset(Runnable reset) throws Exception {
        startTestGrid();

        IgniteCache<Integer, String> cache = queryNode().cache("A");

        String[] qryText = new String[2];

        for (int i = 1; i <= 2; i++) {
            cache.put(100 + i, "STR" + i);

            cacheQuery(new SqlFieldsQuery(SQL + " where _val='STR" + i + "'"), "A");

            checkSqlPlanHistory(1);

            qryText[i - 1] = F.first(getSqlPlanHistory()).sql();

            if (i == 1)
                reset.run();
        }

        assertNotEquals(qryText[0], qryText[1]);
    }

    /**
     * @param setup Setup test task.
     * @param startGridFirst Flag indicating whether to start the grid before the setup.
     */
    public void checkEmptyHistory(Runnable setup, boolean startGridFirst) throws Exception {
        if (startGridFirst)
            startTestGrid();

        setup.run();

        if (!startGridFirst)
            startTestGrid();

        cacheQuery(new SqlFieldsQuery(SQL), "A");

        assertTrue(getSqlPlanHistory().isEmpty());
    }

    /**
     * @param check SQL plan history size check task.
     * @param isBothEnginesChecked Flag indicating whether to the test for both SQL engines.
     */
    public void checkDefaultSettings(Consumer<Integer> check, boolean isBothEnginesChecked) throws Exception {
        if (!isBothEnginesChecked)
            assumeFalse(sqlEngine == CalciteQueryEngineConfiguration.ENGINE_NAME);

        assumeFalse(isClient || loc || isFullyFetched);

        startTestGrid();

        int histSize = queryNode().configuration().getSqlConfiguration().getSqlPlanHistorySize();

        check.accept(histSize);
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
