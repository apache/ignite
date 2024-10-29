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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class AggregatesIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Test
    public void testMinMaxWithTable() {
        String[] indexes = new String[] {
            "val0",
            "val0 desc",
            "val0, val1",
            "val0 desc, val1 desc",
            "val0 asc, val1 desc",
            "val0 desc, val1 asc"
        };

        for (String idx : indexes) {
            for (int backups = -1; backups < 3; ++backups) {
                executeSql("create table tbl(id integer primary key, val0 integer, val1 float, val2 varchar) " +
                    "with template=" + (backups < 0 ? "replicated" : "partitioned,backups=" + backups) + "," + atomicity());

                executeSql("create index test_idx on tbl(" + idx + ")");

                fillTestTbl();

                assertQuery("select min(val0) from tbl").returns(1).check();
                assertQuery("select min(val1) from tbl").returns(10.0f).check();
                assertQuery("select max(val0) from tbl").returns(5).check();
                assertQuery("select max(val1) from tbl").returns(50.0f).check();

                clearTransaction();

                executeSql("drop table tbl");
            }
        }
    }

    /** */
    private void fillTestTbl() {
        executeSql("insert into tbl values(-1, null, null, 'value_-1')");
        executeSql("insert into tbl values(1, 2, 20.0, 'value_1')");
        executeSql("insert into tbl values(2, 3, 10.0, 'value_2')");
        executeSql("insert into tbl values(3, null, 30.0, null)");
        executeSql("insert into tbl values(4, 4, 30.0, 'value_4')");
        executeSql("insert into tbl values(5, 5, 50.0, 'value_5')");
        executeSql("insert into tbl values(6, 1, null, 'value_6')");
        executeSql("insert into tbl values(7, null, 20.0, 'value_7')");
        executeSql("insert into tbl values(8, null, null, null)");
    }

    /** */
    @Test
    public void testMinMaxWithEntity() {
        for (int b = -1; b < 3; ++b) {
            createAndPopulateIndexedTable(b, b < 0 ? CacheMode.REPLICATED : CacheMode.PARTITIONED);

            assertQuery("select min(salary) from person").returns(1.0).check();
            assertQuery("select min(descVal) from person").returns(1.0).check();
            assertQuery("select max(salary) from person").returns(15.0).check();
            assertQuery("select max(descVal) from person").returns(15.0).check();

            clearTransaction();

            client.destroyCache(TABLE_NAME);
        }
    }

    /** */
    @Test
    public void testCountWithBackupsAndCacheModes() {
        for (int b = 0; b < 2; ++b) {
            createAndPopulateIndexedTable(b, CacheMode.PARTITIONED);

            assertQuery("select count(*) from person").returns(7L).check();

            clearTransaction();

            client.destroyCache(TABLE_NAME);
        }

        createAndPopulateIndexedTable(0, CacheMode.REPLICATED);

        assertQuery("select count(*) from person").returns(7L).check();
    }

    /** */
    @Test
    public void testCountIndexedField() {
        // Check count with two columns index.
        sql("CREATE TABLE tbl (a INT, b INT, c INT) WITH " + atomicity());
        sql("CREATE INDEX idx_a ON tbl(a, c)");
        sql("CREATE INDEX idx_b ON tbl(b DESC, c)");

        createAndPopulateIndexedTable(1, CacheMode.PARTITIONED);

        assertQuery("select count(salary) from person").returns(4L).check();
        assertQuery("select count(descVal) from person").returns(4L).check();
        assertQuery("select count(salary + 1) from person").returns(4L).check();
        assertQuery("select count(distinct descVal) from person").returns(3L).check();
        assertQuery("select count(salary) from person where salary >= 5").returns(2L).check();
        assertQuery("select count(salary) filter (where salary >= 5) from person").returns(2L).check();
        assertQuery("select count(salary), descVal from person group by descVal")
            .returns(1L, 1d)
            .returns(1L, 9d)
            .returns(1L, 15d)
            .returns(1L, null)
            .check();

        for (int i = 0; i < 100; i++) {
            sql("INSERT INTO tbl VALUES (null, null, ?)", i % 2 == 0 ? i : null);
            sql("INSERT INTO tbl VALUES (?, ?, ?)", i, i, i % 2 == 0 ? null : i);
        }

        assertQuery("SELECT COUNT(a) FROM tbl").returns(100L).check();
        assertQuery("SELECT COUNT(b) FROM tbl").returns(100L).check();
    }

    /**
     * Tests grouping result by an alias and an ordinal value.
     *
     * @see SqlConformance#isGroupByAlias()
     * @see SqlConformance#isGroupByOrdinal()
     */
    @Test
    public void testGroupingByAlias() {
        executeSql("CREATE TABLE t1(id INT, val_int INT, val_char VARCHAR, PRIMARY KEY(id)) WITH " + atomicity());

        for (int i = 0; i < 10; i++)
            executeSql("INSERT INTO t1 VALUES (?, ?, ?)", i, i % 3, "val" + i % 3);

        assertQuery("SELECT val_char as ALS, count(val_int) FROM t1 GROUP BY ALS")
            .returns("val0", 4L)
            .returns("val1", 3L)
            .returns("val2", 3L)
            .check();

        assertQuery("SELECT val_char, count(val_int) FROM t1 GROUP BY 1")
            .returns("val0", 4L)
            .returns("val1", 3L)
            .returns("val2", 3L)
            .check();
    }

    /** */
    @Test
    public void testCountOfNonNumericField() {
        createAndPopulateTable();

        assertQuery("select count(name) from person").returns(4L).check();
        assertQuery("select count(*) from person").returns(5L).check();
        assertQuery("select count(1) from person").returns(5L).check();
        assertQuery("select count(null) from person").returns(0L).check();

        assertQuery("select count(DISTINCT name) from person").returns(3L).check();
        assertQuery("select count(DISTINCT 1) from person").returns(1L).check();

        assertQuery("select count(*) from person where salary < 0").returns(0L).check();
        assertQuery("select count(*) from person where salary < 0 and salary > 0").returns(0L).check();

        assertQuery("select count(case when name like 'R%' then 1 else null end) from person").returns(2L).check();
        assertQuery("select count(case when name not like 'I%' then 1 else null end) from person").returns(2L).check();

        assertQuery("select count(name) from person where salary > 10").returns(1L).check();
        assertQuery("select count(*) from person where salary > 10").returns(2L).check();
        assertQuery("select count(1) from person where salary > 10").returns(2L).check();
        assertQuery("select count(*) from person where name is not null").returns(4L).check();

        assertQuery("select count(name) filter (where salary > 10) from person").returns(1L).check();
        assertQuery("select count(*) filter (where salary > 10) from person").returns(2L).check();
        assertQuery("select count(1) filter (where salary > 10) from person").returns(2L).check();

        assertQuery("select salary, count(name) from person group by salary order by salary")
            .returns(10d, 3L)
            .returns(15d, 1L)
            .check();

        assertQuery("select salary, count(*) from person group by salary order by salary")
            .returns(10d, 3L)
            .returns(15d, 2L)
            .check();

        assertQuery("select salary, count(1) from person group by salary order by salary")
            .returns(10d, 3L)
            .returns(15d, 2L)
            .check();

        assertQuery("select salary, count(1), sum(1) from person group by salary order by salary")
            .returns(10d, 3L, 3L)
            .returns(15d, 2L, 2L)
            .check();

        assertQuery("select salary, name, count(1), sum(salary) from person group by salary, name order by salary")
            .returns(10d, "Igor", 1L, 10d)
            .returns(10d, "Roma", 2L, 20d)
            .returns(15d, "Ilya", 1L, 15d)
            .returns(15d, null, 1L, 15d)
            .check();

        assertQuery("select salary, count(name) from person group by salary having salary < 10 order by salary")
            .check();

        assertQuery("select count(_key), _key from person group by _key")
            .returns(1L, 0)
            .returns(1L, 1)
            .returns(1L, 2)
            .returns(1L, 3)
            .returns(1L, 4)
            .check();

        assertQuery("select count(name), name from person group by name")
            .returns(1L, "Igor")
            .returns(1L, "Ilya")
            .returns(2L, "Roma")
            .returns(0L, null)
            .check();

        assertQuery("select avg(salary) from person")
            .returns(12.0)
            .check();
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testMultipleRowsFromSingleAggr() {
        createAndPopulateTable();

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT (SELECT name FROM person)").check(),
            IllegalArgumentException.class);

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT t._key, (SELECT x FROM " +
                "TABLE(system_range(1, 5))) FROM person t").check(), IllegalArgumentException.class);

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT t._key, (SELECT x FROM " +
                "TABLE(system_range(t._key, t._key + 1))) FROM person t").check(), IllegalArgumentException.class);

        assertQuery("SELECT t._key, (SELECT x FROM TABLE(system_range(t._key, t._key))) FROM person t").check();

        // Check exception on reduce phase.
        String cacheName = "person";

        IgniteCache<Integer, Employer> person = client.cache(cacheName);

        clearTransaction();

        person.clear();

        for (int gridIdx = 0; gridIdx < nodeCount(); gridIdx++)
            put(client, person, primaryKey(grid(gridIdx).cache(cacheName)), new Employer(gridIdx == 0 ? "Emp" : null, 0.0d));

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT (SELECT name FROM person)").check(),
            IllegalArgumentException.class);

        assertQuery("SELECT (SELECT name FROM person WHERE name is not null)").returns("Emp").check();
    }

    /** */
    @Test
    public void testAnyValAggr() {
        createAndPopulateTable();

        List<List<?>> res = executeSql("select any_value(name) from person");

        assertEquals(1, res.size());

        Object val = res.get(0).get(0);

        assertTrue("Unexpected value: " + val, "Igor".equals(val) || "Roma".equals(val) || "Ilya".equals(val));

        // Test with grouping.
        res = executeSql("select any_value(name), salary from person group by salary order by salary");

        assertEquals(2, res.size());

        val = res.get(0).get(0);

        assertTrue("Unexpected value: " + val, "Igor".equals(val) || "Roma".equals(val));

        val = res.get(1).get(0);

        assertEquals("Ilya", val);
    }

    /** */
    @Test
    public void testColocatedAggregate() {
        executeSql("CREATE TABLE t1(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) " +
            "WITH AFFINITY_KEY=val1," + atomicity());

        executeSql("CREATE TABLE t2(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) " +
            "WITH AFFINITY_KEY=val1," + atomicity());

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO t1 VALUES (?, ?, ?, ?)", i, "val" + i, "val" + i % 2, "val" + i);

        executeSql("INSERT INTO t2 VALUES (0, 'val0', 'val0', 'val0'), (1, 'val1', 'val1', 'val1')");

        String sql = "SELECT val1, count(val2) FROM t1 GROUP BY val1";

        assertQuery(sql)
            .matches(QueryChecker.matches(".*Exchange.*Colocated.*Aggregate.*"))
            .returns("val0", 50L)
            .returns("val1", 50L)
            .check();

        sql = "SELECT t2.val1, agg.cnt " +
            "FROM t2 JOIN (SELECT val1, COUNT(val2) AS cnt FROM t1 GROUP BY val1) AS agg ON t2.val1 = agg.val1";

        assertQuery(sql)
            .matches(QueryChecker.matches(".*Exchange.*Join.*Colocated.*Aggregate.*"))
            .returns("val0", 50L)
            .returns("val1", 50L)
            .check();
    }

    /** */
    @Test
    public void testEverySomeAggregate() {
        executeSql("CREATE TABLE t(c1 INT, c2 INT) WITH " + atomicity());
        executeSql("INSERT INTO t VALUES (null, 0)");
        executeSql("INSERT INTO t VALUES (0, null)");
        executeSql("INSERT INTO t VALUES (null, null)");
        executeSql("INSERT INTO t VALUES (0, 1)");
        executeSql("INSERT INTO t VALUES (1, 1)");
        executeSql("INSERT INTO t VALUES (1, 2)");
        executeSql("INSERT INTO t VALUES (2, 2)");

        assertQuery("SELECT EVERY(c1 < c2) FROM t").returns(false).check();
        assertQuery("SELECT SOME(c1 < c2) FROM t").returns(true).check();
        assertQuery("SELECT EVERY(c1 <= c2) FROM t").returns(true).check();
        assertQuery("SELECT SOME(c1 > c2) FROM t").returns(false).check();
    }

    /** */
    @Test
    public void testCountIndexedFieldSegmented() {
        client.getOrCreateCache(cacheConfiguration()
            .setName("cache_seg")
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setQueryEntities(Collections.singleton(
                new QueryEntity()
                    .setKeyType(Integer.class.getName())
                    .setValueType("tbl_seg")
                    .setTableName("TBL_SEG")
                    .addQueryField("a", Integer.class.getName(), null)
                    .addQueryField("b", Integer.class.getName(), null)
                    .setIndexes(F.asList(new QueryIndex("a", true), new QueryIndex("b", false)))))
            .setQueryParallelism(5));

        for (int i = 0; i < 100; i++)
            sql("INSERT INTO tbl_seg (_key, a, b) VALUES (?, ?, ?)", i, i % 2 == 0 ? i : null, i % 2 == 0 ? null : i);

        assertQuery("SELECT COUNT(a) FROM tbl_seg").returns(50L).check();
        assertQuery("SELECT COUNT(b) FROM tbl_seg").returns(50L).check();
    }

    /** */
    protected void createAndPopulateIndexedTable(int backups, CacheMode cacheMode) {
        IgniteCache<Integer, IndexedEmployer> person = client.getOrCreateCache(this.<Integer, IndexedEmployer>cacheConfiguration()
            .setName(TABLE_NAME)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, IndexedEmployer.class).setTableName(TABLE_NAME)))
            .setCacheMode(cacheMode)
            .setBackups(backups)
        );

        int idx = 0;

        put(client, person, idx++, new IndexedEmployer("Igor", 5d, 9d));
        put(client, person, idx++, new IndexedEmployer(null, 3d, null));
        put(client, person, idx++, new IndexedEmployer("Ilya", 1d, 1d));
        put(client, person, idx++, new IndexedEmployer("Roma", null, 9d));
        put(client, person, idx++, new IndexedEmployer(null, null, null));
        put(client, person, idx++, new IndexedEmployer("Oleg", 15d, 15d));
        put(client, person, idx, new IndexedEmployer("Maya", null, null));
    }

    /** */
    public static class IndexedEmployer {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField(index = true)
        public Double salary;

        /** */
        @QuerySqlField(index = true, descending = true)
        public Double descVal;

        /** */
        public IndexedEmployer(String name, Double salary, Double descVal) {
            this.name = name;
            this.salary = salary;
            this.descVal = descVal;
        }
    }
}
