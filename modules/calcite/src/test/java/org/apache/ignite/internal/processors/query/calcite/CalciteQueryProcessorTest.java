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
import java.util.List;
import com.google.common.collect.ImmutableMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.FieldsQueryCursor;
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
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "false")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(5);

        ignite = startClientGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);

            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }
    }

    /** */
    @Test
    public void unionAll() throws Exception {
        IgniteCache<Integer, Employer> employer1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER1")))
            .setBackups(1)
        );

        IgniteCache<Integer, Employer> employer2 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER2")))
            .setBackups(2)
        );

        IgniteCache<Integer, Employer> employer3 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
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
                "UNION ALL " +
                "SELECT * FROM employer2 " +
                "UNION ALL " +
                "SELECT * FROM employer3 ");

        assertEquals(1, query.size());

        List<List<?>> rows = query.get(0).getAll();
        assertEquals(6, rows.size());
    }

    /** */
    @Test
    public void union() throws Exception {
        IgniteCache<Integer, Employer> employer1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER1")))
            .setBackups(1)
        );

        IgniteCache<Integer, Employer> employer2 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("employer2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("EMPLOYER2")))
            .setBackups(2)
        );

        IgniteCache<Integer, Employer> employer3 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
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
        IgniteCache<Integer, Employer> orders = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("orders")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("orders")))
            .setBackups(2)
        );

        IgniteCache<Integer, Employer> account = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13727")
    public void testUnionWithDistinct() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
            "SELECT distinct(name) FROM Orders UNION SELECT name from Account");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13721")
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13729")
    public void testNotExistsConditionWithSubquery() throws Exception {
        populateTables();

        List<List<?>> rows = sql(
                "EXPLAIN PLAN FOR SELECT name FROM Orders o WHERE NOT EXISTS (" +
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

    /** */
    @Test
    public void aggregate() throws Exception {
        IgniteCache<Integer, Employer> employer = ignite.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
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
        IgniteCache<Key, Developer> developer = ignite.getOrCreateCache(new CacheConfiguration<Key, Developer>()
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
        IgniteCache<Integer, Developer> developer = ignite.getOrCreateCache(new CacheConfiguration<Integer, Developer>()
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

        QueryEngine engine = Commons.lookupComponent(ignite.context(), QueryEngine.class);

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
            List<FieldsQueryCursor<List<?>>> query = ignite.context().query().querySqlFields(new SqlFieldsQuery("select * from DEVELOPER").setSchema("PUBLIC"), false, false);
            query.get(0).getAll();
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            List<FieldsQueryCursor<List<?>>> query = ignite.context().query().querySqlFields(new SqlFieldsQuery("select * from DEVELOPER").setSchema("PUBLIC"), false, false);
            query.get(0).getAll();
        }
        System.out.println("H2 duration = " + (System.currentTimeMillis() - start));
    }

    /** */
    private List<List<?>> sql(String sql) {
        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors = engine.query(null, "PUBLIC", sql);

        return cursors.get(0).getAll();
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
}
