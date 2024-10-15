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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Integration test for set op (EXCEPT, INTERSECT).
 */
public class SetOpIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    private void createTables() {
        client.getOrCreateCache(this.<Integer, Employer>cacheConfiguration()
            .setName("emp1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp1")))
            .setBackups(2)
        );

        client.getOrCreateCache(this.<Integer, Employer>cacheConfiguration()
            .setName("emp2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp2")))
            .setBackups(1)
        );
    }

    /** */
    private void populateEmp1(IgniteCache<Integer, Employer> emp1) {
        put(client, emp1, 1, new Employer("Igor", 10d));
        put(client, emp1, 2, new Employer("Igor", 11d));
        put(client, emp1, 3, new Employer("Igor", 12d));
        put(client, emp1, 4, new Employer("Igor1", 13d));
        put(client, emp1, 5, new Employer("Igor1", 13d));
        put(client, emp1, 6, new Employer("Igor1", 13d));
        put(client, emp1, 7, new Employer("Roman", 14d));
    }

    /** */
    private void populateEmp2(IgniteCache<Integer, Employer> emp2) {
        put(client, emp2, 1, new Employer("Roman", 10d));
        put(client, emp2, 2, new Employer("Roman", 11d));
        put(client, emp2, 3, new Employer("Roman", 12d));
        put(client, emp2, 4, new Employer("Roman", 13d));
        put(client, emp2, 5, new Employer("Igor1", 13d));
        put(client, emp2, 6, new Employer("Igor1", 13d));
    }

    /** */
    private void createCacheCopyAsReplicated(String cacheName) throws InterruptedException {
        IgniteCache<Object, Object> cache = client.cache(cacheName);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<Object, Object>(
            cache.getConfiguration(CacheConfiguration.class));

        ccfg.setName(cacheName + "Replicated");
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.getQueryEntities().forEach(qe -> qe.setTableName(qe.getTableName() + "_repl"));

        client.getOrCreateCache(ccfg);

        awaitPartitionMapExchange(true, true, null);
    }

    /** */
    @Test
    public void testExcept() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1 EXCEPT SELECT name FROM emp2");

        assertEquals(1, rows.size());
        assertEquals("Igor", rows.get(0).get(0));
    }

    /** */
    @Test
    public void testExceptFromEmpty() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");
        createCacheCopyAsReplicated("emp2");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));
        populateEmp2(client.cache("emp2Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1 WHERE salary < 0 EXCEPT SELECT name FROM emp2");

        assertEquals(0, rows.size());

        rows = executeSql("SELECT name FROM emp1_repl WHERE salary < 0 EXCEPT SELECT name FROM emp2_repl");

        assertEquals(0, rows.size());
    }

    /** */
    @Test
    public void testExceptSeveralColumns() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows = executeSql("SELECT name, salary FROM emp1 EXCEPT SELECT name, salary FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, F.size(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testExceptAll() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1 EXCEPT ALL SELECT name FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, F.size(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testExceptNested() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows =
            executeSql("SELECT name FROM emp1 EXCEPT (SELECT name FROM emp1 EXCEPT SELECT name FROM emp2)");

        assertEquals(2, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Roman")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testExceptReplicatedWithPartitioned() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1_repl EXCEPT ALL SELECT name FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, F.size(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testExceptReplicated() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");
        createCacheCopyAsReplicated("emp2");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));
        populateEmp2(client.cache("emp2Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1_repl EXCEPT ALL SELECT name FROM emp2_repl");

        assertEquals(4, rows.size());
        assertEquals(3, F.size(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testExceptMerge() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1_repl EXCEPT ALL SELECT name FROM emp2 EXCEPT ALL " +
            "SELECT name FROM emp1 WHERE salary < 11");

        assertEquals(3, rows.size());
        assertEquals(2, F.size(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testSetOpBigBatch() throws Exception {
        client.getOrCreateCache(this.<Integer, Integer>cacheConfiguration()
            .setName("cache1")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Integer.class).setTableName("table1")))
            .setBackups(2)
        );

        client.getOrCreateCache(this.<Integer, Integer>cacheConfiguration()
            .setName("cache2")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Integer.class).setTableName("table2")))
            .setBackups(1)
        );

        createCacheCopyAsReplicated("cache1");
        createCacheCopyAsReplicated("cache2");

        try (IgniteDataStreamer<Integer, Integer> ds1 = client.dataStreamer("cache1");
             IgniteDataStreamer<Integer, Integer> ds2 = client.dataStreamer("cache2");
             IgniteDataStreamer<Integer, Integer> ds3 = client.dataStreamer("cache1Replicated");
             IgniteDataStreamer<Integer, Integer> ds4 = client.dataStreamer("cache2Replicated")
        ) {
            int key = 0;

            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < ((i == 0) ? 1 : (1 << (i * 4 - 1))); j++) {
                    // Cache1 keys count: 1 of "0", 8 of "1", 128 of "2", 2048 of "3", 32768 of "4".
                    ds1.addData(key++, i);
                    ds3.addData(key++, i);

                    // Cache2 keys count: 1 of "5", 128 of "3", 32768 of "1".
                    if ((i & 1) == 0) {
                        ds2.addData(key++, 5 - i);
                        ds4.addData(key++, 5 - i);
                    }
                }
            }
        }

        awaitPartitionMapExchange(true, true, null);

        List<List<?>> rows;

        // Check 2 partitioned caches.
        rows = executeSql("SELECT _val FROM \"cache1\".table1 EXCEPT SELECT _val FROM \"cache2\".table2");

        assertEquals(3, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(0)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(2)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(4)));

        rows = executeSql("SELECT _val FROM \"cache1\".table1 EXCEPT ALL SELECT _val FROM \"cache2\".table2");

        assertEquals(34817, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(0)));
        assertEquals(128, F.size(rows, r -> r.get(0).equals(2)));
        assertEquals(1920, F.size(rows, r -> r.get(0).equals(3)));
        assertEquals(32768, F.size(rows, r -> r.get(0).equals(4)));

        rows = executeSql("SELECT _val FROM \"cache1\".table1 INTERSECT SELECT _val FROM \"cache2\".table2");

        assertEquals(2, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(1)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(3)));

        rows = executeSql("SELECT _val FROM \"cache1\".table1 INTERSECT ALL SELECT _val FROM \"cache2\".table2");

        assertEquals(136, rows.size());
        assertEquals(8, F.size(rows, r -> r.get(0).equals(1)));
        assertEquals(128, F.size(rows, r -> r.get(0).equals(3)));

        // Check 1 replicated and 1 partitioned caches.
        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl EXCEPT SELECT _val " +
            "FROM \"cache2\".table2");

        assertEquals(3, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(0)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(2)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(4)));

        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl EXCEPT ALL SELECT _val " +
            "FROM \"cache2\".table2");

        assertEquals(34817, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(0)));
        assertEquals(128, F.size(rows, r -> r.get(0).equals(2)));
        assertEquals(1920, F.size(rows, r -> r.get(0).equals(3)));
        assertEquals(32768, F.size(rows, r -> r.get(0).equals(4)));

        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl INTERSECT SELECT _val " +
            "FROM \"cache2\".table2");

        assertEquals(2, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(1)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(3)));

        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl INTERSECT ALL SELECT _val " +
            "FROM \"cache2\".table2");

        assertEquals(136, rows.size());
        assertEquals(8, F.size(rows, r -> r.get(0).equals(1)));
        assertEquals(128, F.size(rows, r -> r.get(0).equals(3)));

        // Check 2 replicated caches.
        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl EXCEPT SELECT _val " +
            "FROM \"cache2Replicated\".table2_repl");

        assertEquals(3, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(0)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(2)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(4)));

        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl EXCEPT ALL SELECT _val " +
            "FROM \"cache2Replicated\".table2_repl");

        assertEquals(34817, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(0)));
        assertEquals(128, F.size(rows, r -> r.get(0).equals(2)));
        assertEquals(1920, F.size(rows, r -> r.get(0).equals(3)));
        assertEquals(32768, F.size(rows, r -> r.get(0).equals(4)));

        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl INTERSECT SELECT _val " +
            "FROM \"cache2Replicated\".table2_repl");

        assertEquals(2, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals(1)));
        assertEquals(1, F.size(rows, r -> r.get(0).equals(3)));

        rows = executeSql("SELECT _val FROM \"cache1Replicated\".table1_repl INTERSECT ALL SELECT _val " +
            "FROM \"cache2Replicated\".table2_repl");

        assertEquals(136, rows.size());
        assertEquals(8, F.size(rows, r -> r.get(0).equals(1)));
        assertEquals(128, F.size(rows, r -> r.get(0).equals(3)));
    }

    /** */
    @Test
    public void testIntersect() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1 INTERSECT SELECT name FROM emp2");

        assertEquals(2, rows.size());
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testInstersectAll() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1 INTERSECT ALL SELECT name FROM emp2");

        assertEquals(3, rows.size());
        assertEquals(2, F.size(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testIntersectEmpty() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");
        createCacheCopyAsReplicated("emp2");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        populateEmp1(client.cache("emp1Replicated"));
        populateEmp2(client.cache("emp2Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1 WHERE salary < 0 INTERSECT SELECT name FROM emp2");

        assertEquals(0, rows.size());

        rows = executeSql("SELECT name FROM emp1_repl WHERE salary < 0 INTERSECT SELECT name FROM emp2_repl");

        assertEquals(0, rows.size());
    }

    /** */
    @Test
    public void testIntersectMerge() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1_repl INTERSECT ALL SELECT name FROM emp2 INTERSECT ALL " +
            "SELECT name FROM emp1 WHERE salary < 14");

        assertEquals(2, rows.size());
        assertEquals(2, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testIntersectReplicated() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");
        createCacheCopyAsReplicated("emp2");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));
        populateEmp2(client.cache("emp2Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1_repl INTERSECT ALL SELECT name FROM emp2_repl");

        assertEquals(3, rows.size());
        assertEquals(2, F.size(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testIntersectReplicatedWithPartitioned() throws Exception {
        createTables();
        createCacheCopyAsReplicated("emp1");

        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));
        populateEmp1(client.cache("emp1Replicated"));

        List<List<?>> rows = executeSql("SELECT name FROM emp1_repl INTERSECT ALL SELECT name FROM emp2");

        assertEquals(3, rows.size());
        assertEquals(2, F.size(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, F.size(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testIntersectSeveralColumns() throws Exception {
        createTables();
        populateEmp1(client.cache("emp1"));
        populateEmp2(client.cache("emp2"));

        List<List<?>> rows = executeSql("SELECT name, salary FROM emp1 INTERSECT ALL SELECT name, salary FROM emp2");

        assertEquals(2, rows.size());
        assertEquals(2, F.size(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testSetOpColocated() {
        executeSql("CREATE TABLE emp(empid INTEGER, deptid INTEGER, name VARCHAR, PRIMARY KEY(empid, deptid)) " +
            "WITH AFFINITY_KEY=deptid," + atomicity());
        executeSql("CREATE TABLE dept(deptid INTEGER, name VARCHAR, PRIMARY KEY(deptid)) WITH " + atomicity());

        executeSql("INSERT INTO emp VALUES (0, 0, 'test0'), (1, 0, 'test1'), (2, 1, 'test2')");
        executeSql("INSERT INTO dept VALUES (0, 'test0'), (1, 'test1'), (2, 'test2')");

        assertQuery("SELECT deptid, name FROM emp EXCEPT SELECT deptid, name FROM dept")
            .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedMinus.*"))
            .returns(0, "test1")
            .returns(1, "test2")
            .check();

        assertQuery("SELECT deptid, name FROM dept EXCEPT SELECT deptid, name FROM emp")
            .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedMinus.*"))
            .returns(1, "test1")
            .returns(2, "test2")
            .check();

        assertQuery("SELECT deptid FROM dept EXCEPT SELECT deptid FROM emp")
            .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedMinus.*"))
            .returns(2)
            .check();

        assertQuery("SELECT deptid FROM dept INTERSECT SELECT deptid FROM emp")
            .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedIntersect.*"))
            .returns(0)
            .returns(1)
            .check();
    }

    /**
     * Test that set op node can be rewinded.
     */
    @Test
    public void testSetOpRewindability() {
        executeSql("CREATE TABLE test(i INTEGER) WITH " + atomicity());
        executeSql("INSERT INTO test VALUES (1), (2)");

        assertQuery("SELECT (SELECT i FROM test EXCEPT SELECT test.i) FROM test")
            .returns(1)
            .returns(2)
            .check();
    }

    /** */
    @Test
    public void testNumbersCastInUnion() throws Exception {
        doTestNumbersCastInSetOp("UNION", 10, 20, 30, 33, 40, 44, 50, null);

        doTestNumbersCastInSetOp("UNION ALL", 10, 20, 20, 30, 30, 33, 40, 44, 50, 50, 50, 50, null, null);
    }

    /** */
    @Test
    public void testNumbersCastInIntersect() throws Exception {
        doTestNumbersCastInSetOp("INTERSECT", 20, 50, null);

        doTestNumbersCastInSetOp("INTERSECT ALL", 20, 50, 50, null);
    }

    /** */
    @Test
    public void testNumbersCastInExcept() throws Exception {
        doTestNumbersCastInSetOp("EXCEPT", 30, 40);

        doTestNumbersCastInSetOp("EXCEPT ALL", 30, 30, 40);
    }

    /**
     * Tests 'SELECT TBL1.val SetOp TBL2.val' where TBL1 has `INT val` and TBL2 has 'val' of different numeric type.
     *  TBL1: 30, 20, 30, 40, 50, 50, null
     *  TBL2: 10, 20, 33, 44, 50, 50, null
     *
     * @param op Operation like 'UNION' or 'INTERSECT'
     * @param expected Expected result as integers.
     */
    private void doTestNumbersCastInSetOp(String op, Integer... expected) throws InterruptedException {
        List<String> types = F.asList("TINYINT", "SMALLINT", "INTEGER", "REAL", "FLOAT", "BIGINT", "DOUBLE", "DECIMAL");

        sql("CREATE TABLE t0(id INT PRIMARY KEY, val INTEGER) WITH \"affinity_key=id," + atomicity() + "\"");

        try {
            for (String tblOpts : Arrays.asList("", "template=replicated", " affinity_key=aff")) {
                for (String t2 : types) {
                    sql("CREATE TABLE t1(id INT, aff INT, val " + t2 + ", PRIMARY KEY(id, aff)) " +
                        "WITH " + tblOpts + (tblOpts.isEmpty() ? "" : ",") + atomicity());

                    sql("INSERT INTO t0 VALUES (1, 30), (2, 20), (3, 30), (4, 40), (5, 50), (6, 50), (7, null)");

                    sql("INSERT INTO t1 VALUES (1, 1, 10), (2, 1, 20), (3, 1, 33), (4, 2, 44), (5, 2, 50), " +
                        "(6, 3, 50), (7, 3, null)");

                    List<List<?>> res = sql("SELECT val from t0 " + op + " select val from t1 ORDER BY 1 NULLS LAST");

                    clearTransaction();

                    sql("DROP TABLE t1");

                    assertEquals(expected.length, res.size());

                    for (int i = 0; i < expected.length; ++i) {
                        assertEquals(1, res.get(i).size());

                        assertEquals(expected[i], res.get(i).get(0) == null ? null : ((Number)res.get(i).get(0)).intValue());
                    }

                    sql("DELETE FROM t0");

                    clearTransaction();
                }
            }
        }
        finally {
            clearTransaction();

            sql("DROP TABLE t0");

            awaitPartitionMapExchange();
        }
    }
}
