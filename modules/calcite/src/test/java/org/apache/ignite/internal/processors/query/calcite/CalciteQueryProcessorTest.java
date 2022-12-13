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

package org.apache.ignite.internal.processors.query.calcite;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.awaitReservationsRelease;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "false")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx client;

    /** Log listener. */
    private ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private static LogListener lsnr = LogListener.matches(s ->
        s.contains("Execution is cancelled") ||
        s.contains("NullPointer") ||
        s.contains("AssertionError")).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        listeningLog.registerListener(lsnr);

        return super.getConfiguration(igniteInstanceName).setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(5);

        client = startClientGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws InterruptedException {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);

            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * Test verifies that replicated cache with specified cache group
     * could be properly mapped on server nodes.
     */
    @Test
    public void queryOverReplCacheGroup() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("ID", Integer.class.getName());
        fields.put("VAL", String.class.getName());

        IgniteCache<Integer, String> cache = client.getOrCreateCache(new CacheConfiguration<Integer, String>()
            .setGroupName("SOME_GROUP")
            .setName("TBL")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, String.class).setTableName("TBL")
                .setKeyFieldName("ID")
                .setFields(fields)
                .setValueFieldName("VAL")
            ))
            .setCacheMode(CacheMode.REPLICATED)
        );

        cache.put(1, "1");
        cache.put(2, "2");

        assertQuery(client, "select val from tbl order by id")
            .returns("1")
            .returns("2")
            .check();

        assertQuery(client, "select id, val from tbl")
            .returns(1, "1")
            .returns(2, "2")
            .check();
    }


    /** */
    @Test
    public void test0() throws IgniteInterruptedCheckedException {
        execute(client, "CREATE TABLE integers(ID INT primary key, i INTEGER)");
        execute(client, "INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (4, NULL)");

        assertQuery(client, "SELECT SUM(i1.i) FROM integers i1").returns(6L).check();
        assertQuery(client, "SELECT (SELECT SUM(i1.i)) FROM integers i1").returns(6L).check();
    }


    /** */
    @Test
    public void test1() throws IgniteInterruptedCheckedException {
        execute(client, "CREATE TABLE strings(id int primary key, a VARCHAR, b BIGINT)");
        execute(client, "INSERT INTO STRINGS VALUES (1, 'abc', 1)");

        assertQuery(client, "SELECT LEFT('asd', ?)").withParams(1L).returns("a").check();
    }

    /** Tests varchar min\max aggregates. */
    @Test
    public void testVarCharMinMax() throws IgniteInterruptedCheckedException {
        execute(client, "CREATE TABLE TEST(val VARCHAR primary key, val1 integer);");
        execute(client, "INSERT INTO test VALUES ('б', 1), ('бб', 2), ('щ', 3), ('щщ', 4), ('Б', 4), ('ББ', 4), ('Я', 4);");
        List<List<?>> rows = sql("SELECT MAX(val), MIN(val) FROM TEST");

        assertEquals(1, rows.size());
        assertEquals(Arrays.asList("щщ", "Б"), F.first(rows));
    }

    /** */
    @Test
    public void testCountWithJoin() throws Exception {
        IgniteCache<Integer, RISK> RISK = client.getOrCreateCache(new CacheConfiguration<Integer, RISK>()
            .setName("RISK")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, RISK.class).setTableName("RISK")
                .setKeyFields(new HashSet<>(Arrays.asList("TRADEID", "TRADEVER")))))
            .setBackups(1)
        );

        IgniteCache<Integer, TRADE> TRADE = client.getOrCreateCache(new CacheConfiguration<Integer, TRADE>()
            .setName("TRADE")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, TRADE.class).setTableName("TRADE")
                .setKeyFields(new HashSet<>(Arrays.asList("TRADEID", "TRADEVER")))))
            .setBackups(1)
        );

        IgniteCache<Integer, BATCH> BATCH = client.getOrCreateCache(new CacheConfiguration<Integer, BATCH>()
            .setName("BATCH")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, BATCH.class).setTableName("BATCH")))
            .setCacheMode(CacheMode.REPLICATED)
        );

        int numRiskRows = 65_000;

        Map<Integer, RISK> mRisk = new HashMap<>(numRiskRows);

        for (int i = 0; i < numRiskRows; i++)
            mRisk.put(i, new RISK(i));

        RISK.putAll(mRisk);

        Map<Integer, TRADE> mTrade = new HashMap<>(200);

        for (int i = 0; i < 200; i++)
            mTrade.put(i, new TRADE(i));

        TRADE.putAll(mTrade);

        for (int i = 0; i < 80; i++)
            BATCH.put(i, new BATCH(i));

        awaitPartitionMapExchange(true, true, null);

        List<String> joinConverters = Arrays.asList("CorrelatedNestedLoopJoin", "MergeJoinConverter", "NestedLoopJoinConverter");

        // CorrelatedNestedLoopJoin skipped intentionally since it takes too long to finish
        // the query with only CNLJ
        for (int i = 1; i < joinConverters.size(); i++) {
            String currJoin = joinConverters.get(i);

            log.info("Verifying " + currJoin);

            String disableRuleArgs = joinConverters.stream()
                .filter(rn -> !rn.equals(currJoin))
                .collect(Collectors.joining("', '", "'", "'"));

            String sql = "SELECT /*+ DISABLE_RULE(" + disableRuleArgs + ") */ count(*)" +
                " FROM RISK R," +
                " TRADE T," +
                " BATCH B " +
                "WHERE R.BATCHKEY = B.BATCHKEY " +
                "AND R.TRADEID = T.TRADEID " +
                "AND R.TRADEVER = T.TRADEVER " +
                "AND T.BOOK = 'BOOK' " +
                "AND B.LS = TRUE";

            // loop for test execution.
            for (int j = 0; j < 10; j++) {
                List<List<?>> res = sql(sql);

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).size());
                assertEquals(40L, res.get(0).get(0));

                awaitReservationsRelease("RISK");
                awaitReservationsRelease("TRADE");
                awaitReservationsRelease("BATCH");

                assertFalse(lsnr.check());
            }
        }

        listeningLog.clearListeners();
    }

    /**
     * Checks bang equal is allowed and works.
     */
    @Test
    public void testBangEqual() throws Exception {
        IgniteCache<Integer, Developer> developer = grid(1).createCache(new CacheConfiguration<Integer, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setBackups(2)
        );

        developer.put(1, new Developer("Name1", 1));
        developer.put(10, new Developer("Name10", 10));
        developer.put(100, new Developer("Name100", 100));

        awaitPartitionMapExchange(true, true, null);

        assertEquals(2, sql("SELECT * FROM Developer WHERE projectId != ?", false, 1).size());
    }

    /**
     * Checks all grids execution contexts are closed or registered (Out|In)boxes are present.
     */
    private void checkContextCancelled() throws IgniteInterruptedCheckedException {
        for (Ignite instance : G.allGrids()) {
            QueryEngine engineCli = Commons.lookupComponent(((IgniteEx)instance).context(), QueryEngine.class);

            MailboxRegistryImpl mailReg = GridTestUtils.getFieldValue(engineCli, CalciteQueryProcessor.class, "mailboxRegistry");

            Map<Object, Inbox<?>> remotes = GridTestUtils.getFieldValue(mailReg, MailboxRegistryImpl.class, "remotes");

            Map<Object, Outbox<?>> locals = GridTestUtils.getFieldValue(mailReg, MailboxRegistryImpl.class, "locals");

            waitForCondition(() -> remotes.isEmpty() || remotes.values().stream().allMatch(s -> s.context().isCancelled()), 5_000);

            waitForCondition(() -> locals.isEmpty() || locals.values().stream().allMatch(s -> s.context().isCancelled()), 5_000);
        }
    }

    /** */
    public static class RISK {
        /** */
        @QuerySqlField
        public Integer batchKey;

        /** */
        @QuerySqlField
        public Integer tradeId;

        /** */
        @QuerySqlField
        public Integer tradeVer;

        /** */
        public RISK(Integer in) {
            batchKey = in;
            tradeId = in;
            tradeVer = in;
        }
    }

    /** */
    public static class TRADE {
        /** */
        @QuerySqlField
        public Integer tradeId;

        /** */
        @QuerySqlField
        public Integer tradeVer;

        /** */
        @QuerySqlField
        public String book;

        /** */
        public TRADE(Integer in) {
            tradeId = in;
            tradeVer = in;
            book = (in & 1) != 0 ? "BOOK" : "";
        }
    }

    /** */
    public static class BATCH {
        /** */
        @QuerySqlField
        public Integer batchKey;

        /** */
        @QuerySqlField
        public Boolean ls;

        /** */
        public BATCH(Integer in) {
            batchKey = in;
            ls = (in & 1) != 0;
        }
    }

    /** */
    @Test
    public void unionAll() throws Exception {
        IgniteCache<Integer, Employer> employer1 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER1")))
            .setBackups(1)
        );

        IgniteCache<Integer, Employer> employer2 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER2")))
            .setBackups(2)
        );

        IgniteCache<Integer, Employer> employer3 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer3")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER3")))
            .setBackups(3)
        );

        employer1.put(1, new Employer("Igor", 10d));
        employer2.put(1, new Employer("Roman", 15d));
        employer3.put(1, new Employer("Nikolay", 20d));
        employer1.put(2, new Employer("Igor", 10d));
        employer2.put(2, new Employer("Roman", 15d));
        employer3.put(3, new Employer("Nikolay", 20d));

        awaitPartitionMapExchange(true, true, null);

        List<List<?>> rows = sql("SELECT * FROM employer1 " +
            "UNION ALL " +
            "SELECT * FROM employer2 " +
            "UNION ALL " +
            "SELECT * FROM employer3 ");

        assertEquals(6, rows.size());
    }

    /** */
    @Test
    public void union() throws Exception {
        IgniteCache<Integer, Employer> employer1 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER1")))
            .setBackups(1)
        );

        IgniteCache<Integer, Employer> employer2 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER2")))
            .setBackups(2)
        );

        IgniteCache<Integer, Employer> employer3 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer3")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER3")))
            .setBackups(3)
        );

        employer1.put(1, new Employer("Igor", 10d));
        employer2.put(1, new Employer("Roman", 15d));
        employer3.put(1, new Employer("Nikolay", 20d));
        employer1.put(2, new Employer("Igor", 10d));
        employer2.put(2, new Employer("Roman", 15d));
        employer3.put(3, new Employer("Nikolay", 20d));

        awaitPartitionMapExchange(true, true, null);

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "SELECT * FROM employer1 " +
                "UNION " +
                "SELECT * FROM employer2 " +
                "UNION " +
                "SELECT * FROM employer3 ");

        assertEquals(1, query.size());

        List<List<?>> rows = query.get(0).getAll();
        assertEquals(3, rows.size());
    }

    /** */
    private void populateTables() throws InterruptedException {
        IgniteCache<Integer, Employer> orders = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("orders")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("orders")))
            .setBackups(2)
        );

        IgniteCache<Integer, Employer> account = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("account")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("account")))
            .setBackups(1)
        );

        orders.put(1, new Employer("Igor", 10d));
        orders.put(2, new Employer("Igor", 11d));
        orders.put(3, new Employer("Igor", 12d));
        orders.put(4, new Employer("Igor1", 13d));
        orders.put(5, new Employer("Igor1", 13d));
        orders.put(6, new Employer("Igor1", 13d));
        orders.put(7, new Employer("Roman", 14d));

        account.put(1, new Employer("Roman", 10d));
        account.put(2, new Employer("Roman", 11d));
        account.put(3, new Employer("Roman", 12d));
        account.put(4, new Employer("Roman", 13d));
        account.put(5, new Employer("Igor1", 13d));
        account.put(6, new Employer("Igor1", 13d));

        /*
        select * from orders;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Igor  |   10  |
        |  2 | Igor  |   11  |
        |  3 | Igor  |   12  |
        |  4 | Igor1 |   13  |
        |  5 | Igor1 |   13  |
        |  6 | Igor1 |   13  |
        |  7 | Roman |   14  |
        +----+-------+-------+

        select * from account;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Roman |   10  |
        |  2 | Roman |   11  |
        |  3 | Roman |   12  |
        |  4 | Roman |   13  |
        |  5 | Igor1 |   13  |
        |  6 | Igor1 |   13  |
        +----+-------+-------+
         */

        awaitPartitionMapExchange(true, true, null);
    }

    /** */
    @Test
    public void testOrderingByColumnOutsideSelectList() throws InterruptedException {
        populateTables();

        assertQuery(client, "select salary from account order by _key desc")
            .returns(13d)
            .returns(13d)
            .returns(13d)
            .returns(12d)
            .returns(11d)
            .returns(10d)
            .check();

        assertQuery(client, "select name, sum(salary) from account group by name order by count(salary)")
            .returns("Roman", 46d)
            .returns("Igor1", 26d)
            .check();
    }

    /** */
    @Test
    public void testEqConditionWithDistinctSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders WHERE salary = (SELECT DISTINCT(salary) from Account WHERE name='Igor1')");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testEqConditionWithAggregateSubqueryMax() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders WHERE salary = (SELECT MAX(salary) from Account WHERE name='Roman')");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testEqConditionWithAggregateSubqueryMin() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders WHERE salary = (SELECT MIN(salary) from Account WHERE name='Roman')");

        assertEquals(1, rows.size());
    }

    /** */
    @Test
    public void testInConditionWithSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders WHERE name IN (SELECT name from Account)");

        assertEquals(4, rows.size());
    }

    /** */
    @Test
    public void testDistinctQueryWithInConditionWithSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT distinct(name) FROM Orders o WHERE name IN (" +
                "   SELECT name" +
                "   FROM Account)");

        assertEquals(2, rows.size());
    }


    /** */
    @Test
    public void testUnion() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders UNION SELECT name from Account");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testUnionWithDistinct() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT distinct(name) FROM Orders UNION SELECT name from Account");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testNotInConditionWithSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders WHERE name NOT IN (SELECT name from Account)");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testExistsConditionWithSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders o WHERE EXISTS (" +
                "   SELECT 1" +
                "   FROM Account a" +
                "   WHERE o.name = a.name)");

        assertEquals(4, rows.size());
    }

    /** */
    @Test
    public void testNotExistsConditionWithSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT name FROM Orders o WHERE NOT EXISTS (" +
                "   SELECT 1" +
                "   FROM Account a" +
                "   WHERE o.name = a.name)");

        assertEquals(3, rows.size());

        rows = sql(
            "SELECT name FROM Orders o WHERE NOT EXISTS (" +
                "   SELECT name" +
                "   FROM Account a" +
                "   WHERE o.name = a.name)");

        assertEquals(3, rows.size());

        rows = sql(
            "SELECT distinct(name) FROM Orders o WHERE NOT EXISTS (" +
                "   SELECT name" +
                "   FROM Account a" +
                "   WHERE o.name = a.name)");

        assertEquals(1, rows.size());
    }

    /**
     * Execute SQL statement on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    protected List<List<?>> execute(IgniteEx node, String sql, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args), true).getAll();
    }

    /** */
    @Test
    public void aggregate() throws Exception {
        IgniteCache<Integer, Employer> employer = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Employer.class)
            .setBackups(2)
        );

        employer.putAll(ImmutableMap.of(
            0, new Employer("Igor", 10d),
            1, new Employer("Roman", 15d),
            2, new Employer("Nikolay", 20d)
        ));

        awaitPartitionMapExchange(true, true, null);

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "SELECT * FROM employer WHERE employer.salary = (SELECT AVG(employer.salary) FROM employer)");

        assertEquals(1, query.size());

        List<List<?>> rows = query.get(0).getAll();
        assertEquals(1, rows.size());
        assertEquals(Arrays.asList("Roman", 15d), F.first(rows));
    }

    /** */
    @Test
    public void aggregateNested() throws Exception {
        String cacheName = "employer";

        IgniteCache<Integer, Employer> employer = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName(cacheName)
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Employer.class)
            .setBackups(2)
        );

        awaitPartitionMapExchange(true, true, null);

        List<Integer> keysNode0 = primaryKeys(grid(0).cache(cacheName), 2);
        List<Integer> keysNode1 = primaryKeys(grid(1).cache(cacheName), 1);

        employer.putAll(ImmutableMap.of(
            keysNode0.get(0), new Employer("Igor", 10d),
            keysNode0.get(1), new Employer("Roman", 20d),
            keysNode1.get(0), new Employer("Nikolay", 30d)
        ));

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> qry = engine.query(null, "PUBLIC",
            "SELECT avg(salary) FROM " +
                "(SELECT avg(salary) as salary FROM employer UNION ALL SELECT salary FROM employer)");

        assertEquals(1, qry.size());

        List<List<?>> rows = qry.get(0).getAll();
        assertEquals(1, rows.size());
        assertEquals(20d, F.first(F.first(rows)));
    }

    /** */
    @Test
    public void query() throws Exception {
        IgniteCache<Integer, Developer> developer = grid(1).createCache(new CacheConfiguration<Integer, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setBackups(2)
        );

        IgniteCache<Integer, Project> project = grid(1).createCache(new CacheConfiguration<Integer, Project>()
            .setName("project")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Project.class)
            .setBackups(2)
        );

        project.put(0, new Project("Ignite"));
        project.put(1, new Project("Calcite"));

        developer.put(0, new Developer("Igor", 1));
        developer.put(1, new Developer("Roman", 0));

        awaitPartitionMapExchange(true, true, null);

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = ?", 0);

        assertEquals(1, query.size());

        assertEqualsCollections(Arrays.asList("Igor", 1, "Calcite"), F.first(query.get(0).getAll()));
    }

    /** */
    @Test
    public void query2() {
        IgniteCache<Integer, Developer> developer = grid(1).getOrCreateCache(new CacheConfiguration<Integer, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setCacheMode(CacheMode.REPLICATED)
        );

        IgniteCache<Integer, Project> project = grid(1).getOrCreateCache(new CacheConfiguration<Integer, Project>()
            .setName("project")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Project.class)
            .setBackups(2)
        );

        project.put(0, new Project("Ignite"));
        project.put(1, new Project("Calcite"));

        developer.put(0, new Developer("Igor", 1));
        developer.put(1, new Developer("Roman", 0));

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = ?", 0);

        assertEquals(1, query.size());

        assertEqualsCollections(Arrays.asList("Igor", 1, "Calcite"), F.first(query.get(0).getAll()));
    }

    /** */
    @Test
    public void queryMultiStatement() throws Exception {
        IgniteCache<Integer, Developer> developer = grid(1).getOrCreateCache(new CacheConfiguration<Integer, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setBackups(2)
        );

        IgniteCache<Integer, Project> project = grid(1).getOrCreateCache(new CacheConfiguration<Integer, Project>()
            .setName("project")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Project.class)
            .setBackups(2)
        );

        project.putAll(ImmutableMap.of(
            0, new Project("Ignite"),
            1, new Project("Calcite")
        ));

        developer.putAll(ImmutableMap.of(
            0, new Developer("Igor", 1),
            1, new Developer("Roman", 0)
        ));

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "" +
                "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = ?;" +
                "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = 10;" +
                "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = ?", 0, 1);

        assertEquals(3, query.size());

        assertEqualsCollections(Arrays.asList("Igor", 1, "Calcite"), F.first(query.get(0).getAll()));
        assertEquals(0, query.get(1).getAll().size());
        assertEqualsCollections(Arrays.asList("Roman", 0, "Ignite"), F.first(query.get(2).getAll()));
    }

    /**
     * Test verifies that table has a distribution function over valid keys.
     */
    @Test
    public void testTableDistributionKeysForComplexKeyObject() {
        {
            /** */
            class MyKey {
                int id1;

                int id2;
            }

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("ID1", Integer.class.getName());
            fields.put("ID2", Integer.class.getName());
            fields.put("VAL", String.class.getName());

            client.getOrCreateCache(new CacheConfiguration<MyKey, String>("test_cache_1")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(F.asList(
                    new QueryEntity(MyKey.class.getName(), String.class.getName())
                        .setTableName("MY_TBL_1")
                        .setFields(fields)
                        .setKeyFields(ImmutableSet.of("ID1", "ID2"))
                        .setValueFieldName("VAL")
                ))
                .setBackups(2)
            );
        }

        {
            client.getOrCreateCache(new CacheConfiguration<Key, String>("test_cache_2")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(F.asList(
                    new QueryEntity(Key.class, String.class)
                        .setTableName("MY_TBL_2")
                ))
                .setBackups(2)
            );
        }

        CalciteQueryProcessor qryProc = Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);

        Map<String, IgniteSchema> schemas = GridTestUtils.getFieldValue(qryProc, "schemaHolder", "igniteSchemas");

        IgniteSchema pub = schemas.get("PUBLIC");

        Map<String, IgniteTable> tblMap = GridTestUtils.getFieldValue(pub, "tblMap");

        assertEquals(ImmutableIntList.of(2, 3), tblMap.get("MY_TBL_1").descriptor().distribution().getKeys());
        assertEquals(ImmutableIntList.of(3), tblMap.get("MY_TBL_2").descriptor().distribution().getKeys());
    }

    /** */
    @Test
    public void testSequentialInserts() throws Exception {
        sql("CREATE TABLE t(x INTEGER)", true);

        for (int i = 0; i < 10_000; i++)
            sql("INSERT INTO t VALUES (?)", true, i);

        assertEquals(10_000L, sql("SELECT count(*) FROM t").get(0).get(0));
    }

    /**
     * Verifies that table modification events are passed to a calcite schema modification listener.
     */
    @Test
    public void testIgniteSchemaAwaresAlterTableCommand() {
        String selectAllQry = "select * from test_tbl";

        execute(client, "drop table if exists test_tbl");
        execute(client, "create table test_tbl(id int primary key, val varchar)");

        CalciteQueryProcessor qryProc = Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);

        {
            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL")));
        }

        {
            execute(client, "alter table test_tbl add column new_col int");

            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL", "NEW_COL")));
        }

        {
            try {
                execute(client, "alter table test_tbl add column new_col int");
            }
            catch (Exception ignored) {
                // it's expected because column with such name already exists
            }

            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL", "NEW_COL")));
        }

        {
            execute(client, "alter table test_tbl add column if not exists new_col int");

            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL", "NEW_COL")));
        }

        {
            execute(client, "alter table test_tbl drop column new_col");

            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL")));
        }

        {
            try {
                execute(client, "alter table test_tbl drop column new_col");
            }
            catch (Exception ignored) {
                // it's expected since the column was already removed
            }

            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL")));
        }

        {
            execute(client, "alter table test_tbl drop column if exists new_col");

            List<String> names = deriveColumnNamesFromCursor(
                qryProc.query(null, "PUBLIC", selectAllQry).get(0)
            );

            assertThat(names, equalTo(F.asList("ID", "VAL")));
        }
    }

    /**
     * Verifies infix cast operator.
     */
    @Test
    public void testInfixTypeCast() throws Exception {
        execute(client, "drop table if exists test_tbl");
        execute(client, "create table test_tbl(id int primary key, val varchar)");

        // Await for PME, see details here: https://issues.apache.org/jira/browse/IGNITE-14974
        awaitPartitionMapExchange();

        QueryEngine engineSrv = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        FieldsQueryCursor<List<?>> cur = engineSrv.query(null, "PUBLIC",
            "select id, id::tinyint as tid, id::smallint as sid, id::varchar as vid, id::interval hour, " +
                "id::interval year from test_tbl").get(0);

        assertThat(cur, CoreMatchers.instanceOf(QueryCursorEx.class));

        QueryCursorEx<?> qCur = (QueryCursorEx<?>)cur;

        assertThat(qCur.fieldsMeta().get(0).fieldTypeName(), equalTo(Integer.class.getName()));
        assertThat(qCur.fieldsMeta().get(1).fieldTypeName(), equalTo(Byte.class.getName()));
        assertThat(qCur.fieldsMeta().get(2).fieldTypeName(), equalTo(Short.class.getName()));
        assertThat(qCur.fieldsMeta().get(3).fieldTypeName(), equalTo(String.class.getName()));
        assertThat(qCur.fieldsMeta().get(4).fieldTypeName(), equalTo(Duration.class.getName()));
        assertThat(qCur.fieldsMeta().get(5).fieldTypeName(), equalTo(Period.class.getName()));
    }

    /** Quantified predicates test. */
    @Test
    public void quantifiedCompTest() throws InterruptedException {
        populateTables();

        assertQuery(client, "select salary from account where salary > SOME (10, 11) ORDER BY salary")
            .returns(11d)
            .returns(12d)
            .returns(13d)
            .returns(13d)
            .returns(13d)
            .check();

        assertQuery(client, "select salary from account where salary < SOME (12, 12) ORDER BY salary")
            .returns(10d)
            .returns(11d)
            .check();

        assertQuery(client, "select salary from account where salary < ANY (11, 12) ORDER BY salary")
            .returns(10d)
            .returns(11d)
            .check();

        assertQuery(client, "select salary from account where salary > ANY (12, 13) ORDER BY salary")
            .returns(13d)
            .returns(13d)
            .returns(13d)
            .check();

        assertQuery(client, "select salary from account where salary <> ALL (12, 13) ORDER BY salary")
            .returns(10d)
            .returns(11d)
            .check();
    }

    /**
     * Test verifies that 1) proper indexes will be chosen for queries with
     * different kinds of ordering, and 2) result set returned will be
     * sorted as expected.
     *
     * @throws IgniteInterruptedCheckedException If failed.
     */
    @Test
    public void testSelectWithOrdering() throws IgniteInterruptedCheckedException {
        sql( "drop table if exists test_tbl", true);

        sql( "create table test_tbl (c1 int)", true);

        sql( "insert into test_tbl values (1), (2), (3), (null)", true);

        sql( "create index idx_asc on test_tbl (c1)", true);
        sql( "create index idx_desc on test_tbl (c1 desc)", true);

        assertQuery(client, "select c1 from test_tbl ORDER BY c1")
            .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_ASC"))
            .matches(not(containsSubPlan("IgniteSort")))
            .ordered()
            .returns(new Object[]{null})
            .returns(1)
            .returns(2)
            .returns(3)
            .check();

        assertQuery(client, "select c1 from test_tbl ORDER BY c1 asc nulls first")
            .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_ASC"))
            .matches(not(containsSubPlan("IgniteSort")))
            .ordered()
            .returns(new Object[]{null})
            .returns(1)
            .returns(2)
            .returns(3)
            .check();

        assertQuery(client, "select c1 from test_tbl ORDER BY c1 asc nulls last")
            .matches(containsSubPlan("IgniteSort"))
            .ordered()
            .returns(1)
            .returns(2)
            .returns(3)
            .returns(new Object[]{null})
            .check();

        assertQuery(client, "select c1 from test_tbl ORDER BY c1 desc")
            .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_DESC"))
            .matches(not(containsSubPlan("IgniteSort")))
            .ordered()
            .returns(3)
            .returns(2)
            .returns(1)
            .returns(new Object[]{null})
            .check();

        assertQuery(client, "select c1 from test_tbl ORDER BY c1 desc nulls first")
            .matches(containsSubPlan("IgniteSort"))
            .ordered()
            .returns(new Object[]{null})
            .returns(3)
            .returns(2)
            .returns(1)
            .check();

        assertQuery(client, "select c1 from test_tbl ORDER BY c1 desc nulls last")
            .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_DESC"))
            .matches(not(containsSubPlan("IgniteSort")))
            .ordered()
            .returns(3)
            .returns(2)
            .returns(1)
            .returns(new Object[]{null})
            .check();
    }

    /** */
    private static List<String> deriveColumnNamesFromCursor(FieldsQueryCursor cursor) {
        List<String> names = new ArrayList<>(cursor.getColumnsCount());

        assertNotNull(cursor.getAll());

        for (int i = 0; i < cursor.getColumnsCount(); i++)
            names.add(cursor.getFieldName(i));

        return names;
    }

    /** for test purpose only. */
    public void testThroughput() {
        IgniteCache<Integer, Developer> developer = client.getOrCreateCache(new CacheConfiguration<Integer, Developer>()
            .setCacheMode(CacheMode.REPLICATED)
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setBackups(2)
        );

        int numIterations = 1000;

        int prId = -1;

        for (int i = 0; i < 5000; i++) {
            if (i % 1000 == 0)
                prId++;

            developer.put(i, new Developer("Name" + i, prId));
        }

        QueryEngine engine = Commons.lookupComponent(client.context(), QueryEngine.class);

        // warmup
        for (int i = 0; i < numIterations; i++) {
            List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC", "select * from DEVELOPER");
            query.get(0).getAll();
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC", "select * from DEVELOPER");
            query.get(0).getAll();
        }
        System.out.println("Calcite duration = " + (System.currentTimeMillis() - start));

        // warmup
        for (int i = 0; i < numIterations; i++) {
            List<FieldsQueryCursor<List<?>>> query = client.context().query().querySqlFields(
                new SqlFieldsQuery("select * from DEVELOPER").setSchema("PUBLIC"), false, false);
            query.get(0).getAll();
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            List<FieldsQueryCursor<List<?>>> query = client.context().query().querySqlFields(
                new SqlFieldsQuery("select * from DEVELOPER").setSchema("PUBLIC"), false, false);
            query.get(0).getAll();
        }
        System.out.println("H2 duration = " + (System.currentTimeMillis() - start));
    }

    /** */
    private List<List<?>> sql(String sql) throws IgniteInterruptedCheckedException {
        return sql(sql, false);
    }

    /** */
    private List<List<?>> sql(String sql, boolean noCheck, Object...args) throws IgniteInterruptedCheckedException {
        QueryEngine engineSrv = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        assertTrue(client.configuration().isClientMode());

        QueryEngine engineCli = Commons.lookupComponent(client.context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursorsCli = engineCli.query(null, "PUBLIC", sql, args);

        List<List<?>> allSrv;

        if (!noCheck) {
            List<FieldsQueryCursor<List<?>>> cursorsSrv = engineSrv.query(null, "PUBLIC", sql, args);

            try (QueryCursor srvCursor = cursorsSrv.get(0); QueryCursor cliCursor = cursorsCli.get(0)) {
                allSrv = srvCursor.getAll();

                assertEquals(allSrv.size(), cliCursor.getAll().size());

                checkContextCancelled();
            }
        }
        else {
            try (QueryCursor cliCursor = cursorsCli.get(0)) {
                allSrv = cliCursor.getAll();
            }
        }

        return allSrv;
    }

    /** */
    public static class Key {
        /** */
        @QuerySqlField
        public int id;

        /** */
        @QuerySqlField
        @AffinityKeyMapped
        public int affinityKey;

        /** */
        public Key(int id, int affinityKey) {
            this.id = id;
            this.affinityKey = affinityKey;
        }
    }

    /** */
    public static class Employer {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField
        public Double salary;

        /** */
        public Employer(String name, Double salary) {
            this.name = name;
            this.salary = salary;
        }
    }

    /** */
    public static class Developer {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField
        public Integer projectId;

        /** */
        public Developer(String name, Integer projectId) {
            this.name = name;
            this.projectId = projectId;
        }

        /** */
        public Developer(String name) {
            this.name = name;
        }
    }

    /** */
    public static class Project {
        /** */
        @QuerySqlField
        public String name;

        /** */
        public Project(String name) {
            this.name = name;
        }
    }

    /** */
    private QueryChecker assertQuery(IgniteEx ignite, String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(ignite.context(), QueryEngine.class);
            }
        };
    }
}
