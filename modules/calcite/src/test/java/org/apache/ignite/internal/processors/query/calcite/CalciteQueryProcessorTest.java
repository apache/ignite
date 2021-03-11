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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "false")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(5);

        client = startClientGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);

            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }
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

        assertQuery(client, "select id, val from tbl")
            .returns(1, "1")
            .returns(2, "2")
            .check();
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

        Map<Integer, RISK> mRisk = new HashMap<>(65000);

        for (int i = 0; i < 65000; i++)
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
            }
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
        |  1 | igor  |   10  |
        |  2 | igor  |   11  |
        |  3 | igor  |   12  |
        |  4 | igor1 |   13  |
        |  5 | igor1 |   13  |
        |  6 | igor1 |   13  |
        |  7 | roman |   14  |
        +----+-------+-------+

        select * from account;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Roman |   10  |
        |  2 | Roman |   11  |
        |  3 | Roman |   12  |
        |  4 | Roman |   13  |
        |  5 | igor1 |   13  |
        |  6 | igor1 |   13  |
        +----+-------+-------+
         */

        awaitPartitionMapExchange(true, true, null);
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
    protected List<List<?>> execute(IgniteEx node, String sql) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC"), true).getAll();
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
    public void query2() throws Exception {
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
                "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = ?", 0,1);

        assertEquals(2, query.size());

        assertEqualsCollections(Arrays.asList("Igor", 1, "Calcite"), F.first(query.get(0).getAll()));
        assertEqualsCollections(Arrays.asList("Roman", 0, "Ignite"), F.first(query.get(1).getAll()));
    }

    /** */
    @Test
    public void testInsertPrimitiveKey() throws Exception {
        IgniteCache<Integer, Developer> developer = grid(1).getOrCreateCache(new CacheConfiguration<Integer, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setBackups(2)
        );

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC", "INSERT INTO DEVELOPER VALUES (?, ?, ?)", 0, "Igor", 1);

        assertEquals(1, query.size());

        List<List<?>> rows = query.get(0).getAll();

        assertEquals(1, rows.size());

        List<?> row = rows.get(0);

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select _key, * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(Arrays.asList(0, "Igor", 1), row);
    }

    /** */
    @Test
    public void testInsertUpdateDeleteNonPrimitiveKey() throws Exception {
        IgniteCache<Key, Developer> developer = client.getOrCreateCache(new CacheConfiguration<Key, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Key.class, Developer.class)
            .setBackups(2)
        );

        awaitPartitionMapExchange(true, true, null);

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC", "INSERT INTO DEVELOPER VALUES (?, ?, ?, ?)", 0, 0, "Igor", 1);

        assertEquals(1, query.size());

        List<?> row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "Igor", 1), row);

        query = engine.query(null, "PUBLIC", "UPDATE DEVELOPER d SET name = 'Roman' WHERE id = ?", 0);

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "Roman", 1), row);

        query = engine.query(null, "PUBLIC", "DELETE FROM DEVELOPER WHERE id = ?", 0);

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNull(row);
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
            List<FieldsQueryCursor<List<?>>> query = client.context().query().querySqlFields(new SqlFieldsQuery("select * from DEVELOPER").setSchema("PUBLIC"), false, false);
            query.get(0).getAll();
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            List<FieldsQueryCursor<List<?>>> query = client.context().query().querySqlFields(new SqlFieldsQuery("select * from DEVELOPER").setSchema("PUBLIC"), false, false);
            query.get(0).getAll();
        }
        System.out.println("H2 duration = " + (System.currentTimeMillis() - start));
    }

    /** */
    private List<List<?>> sql(String sql) {
        QueryEngine engineSrv = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        assertTrue(client.configuration().isClientMode());

        QueryEngine engineCli = Commons.lookupComponent(client.context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursorsSrv = engineSrv.query(null, "PUBLIC", sql);

        List<FieldsQueryCursor<List<?>>> cursorsCli = engineCli.query(null, "PUBLIC", sql);

        List<List<?>> allSrv;

        try (QueryCursor srvCursor = cursorsSrv.get(0); QueryCursor cliCursor = cursorsCli.get(0)) {
            allSrv = srvCursor.getAll();

            assertEquals(allSrv.size(), cliCursor.getAll().size());
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
