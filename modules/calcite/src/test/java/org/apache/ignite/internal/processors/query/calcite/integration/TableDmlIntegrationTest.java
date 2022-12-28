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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class TableDmlIntegrationTest extends AbstractBasicIntegrationTest {
    /**
     * Test verifies that already inserted by the current query data
     * is not processed by this query again.
     */
    @Test
    public void testInsertAsSelect() {
        executeSql("CREATE TABLE test (epoch_cur int, epoch_copied int)");
        executeSql("INSERT INTO test VALUES (0, 0)");

        final String insertAsSelectSql = "INSERT INTO test SELECT ?, epoch_cur FROM test";

        for (int i = 1; i < 16; i++) {
            executeSql(insertAsSelectSql, i);

            List<List<?>> rows = executeSql("SELECT * FROM test WHERE epoch_copied = ?", i);

            assertEquals("Unexpected rows for epoch " + i, 0, rows.size());
        }
    }

    /**
     * Test verifies that cuncurrent updates does not affect (in terms of its size)
     * a result set provided for insertion.
     */
    @Test
    public void testInsertAsSelectWithConcurrentDataModification() throws IgniteCheckedException {
        executeSql("CREATE TABLE test (id int primary key, val int) with cache_name=\"test\", value_type=\"my_type\"");
        IgniteCache<Integer, Object> cache = grid(0).cache("test").withKeepBinary();

        BinaryObjectBuilder builder = grid(0).binary().builder("my_type");

        for (int i = 0; i < 128; i++)
            cache.put(i, builder.setField("val", i).build());

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get())
                cache.put(ThreadLocalRandom.current().nextInt(128), builder.setField("val", 0).build());
        });

        for (int i = 8; i < 18; i++) {
            int off = 1 << (i - 1);

            executeSql("INSERT INTO test SELECT id + ?::INT, val FROM test", off);

            long cnt = (Long)executeSql("SELECT count(*) FROM test").get(0).get(0);

            assertEquals("Unexpected rows count", 1 << i, cnt);
        }

        stop.set(true);
        fut.get(getTestTimeout());
    }

    /**
     * Ensure that update node updates each row only once.
     */
    @Test
    public void testUpdate() {
        executeSql("CREATE TABLE test (val integer)");

        client.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE INDEX test_val_idx ON test (val)").setSchema("PUBLIC"), false).getAll();

        for (int i = 1; i <= 4096; i++)
            executeSql("INSERT INTO test VALUES (?)", i);

        final String updateSql = "UPDATE test SET val = val * 10";

        int mul = 1;
        for (int i = 1; i < 5; i++) {
            mul *= 10;
            executeSql(updateSql);
        }

        final int fMul = mul;

        List<List<?>> rows = executeSql("SELECT val FROM test ORDER BY val");

        List<Integer> vals = rows.stream().map(r -> (Integer)r.get(0)).collect(Collectors.toList());

        for (int rowNum = 1; rowNum <= rows.size(); rowNum++) {
            assertEquals(
                "Unexpected results: " + S.compact(vals, i -> i + fMul),
                rowNum * fMul,
                rows.get(rowNum - 1).get(0)
            );
        }
    }

    /** */
    @Test
    public void testInsertPrimitiveKey() {
        grid(1).getOrCreateCache(new CacheConfiguration<Integer, CalciteQueryProcessorTest.Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, CalciteQueryProcessorTest.Developer.class)
            .setBackups(2)
        );

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "INSERT INTO DEVELOPER(_key, name, projectId) VALUES (?, ?, ?)", 0, "Igor", 1);

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
        client.getOrCreateCache(new CacheConfiguration<CalciteQueryProcessorTest.Key, CalciteQueryProcessorTest.Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(CalciteQueryProcessorTest.Key.class, CalciteQueryProcessorTest.Developer.class)
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

        query = engine.query(null, "PUBLIC", "UPDATE DEVELOPER d SET name = name || 'Roman' WHERE id = ?", 0);

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "IgorRoman", 1), row);

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

    /**
     * Test insert/update/delete rows of a table with two or more fields in the primary key.
     */
    @Test
    public void testInsertUpdateDeleteComplexKey() {
        executeSql("CREATE TABLE t(id INT, val VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val))");
        executeSql("INSERT INTO t(id, val, val2) VALUES (1, 'a', 'b')");

        assertQuery("SELECT * FROM t").returns(1, "a", "b").check();

        executeSql("UPDATE t SET val2 = 'c' WHERE id = 1");

        // Can't update the part of the key field.
        assertThrows("UPDATE t SET val = 'c' WHERE id = 1", IgniteSQLException.class, "Cannot update field \"VAL\".");

        assertQuery("SELECT * FROM t").returns(1, "a", "c").check();

        executeSql("DELETE FROM t WHERE id = 1");

        assertQuery("SELECT COUNT(*) FROM t").returns(0L).check();
    }

    /**
     * Test full MERGE command.
     */
    @Test
    public void testMerge() {
        executeSql("CREATE TABLE test1 (a int, b varchar, c varchar)");
        executeSql("INSERT INTO test1 VALUES (0, 'a', '0')");
        executeSql("INSERT INTO test1 VALUES (1, 'b', '1')");

        executeSql("CREATE TABLE test2 (a int, b varchar)");
        executeSql("INSERT INTO test2 VALUES (0, '0')");
        executeSql("INSERT INTO test2 VALUES (2, '2')");

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
            "WHEN MATCHED THEN UPDATE SET a = src.a, b = src.b " +
            "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)";

        assertQuery(sql).matches(QueryChecker.containsSubPlan("IgniteTableSpool")).check();

        assertQuery("SELECT * FROM test2")
            .returns(0, "a")
            .returns(1, "b")
            .returns(2, "2")
            .check();
    }

    /**
     * Test MERGE with UPDATE clause only.
     */
    @Test
    public void testMergeWhenMatched() {
        executeSql("CREATE TABLE test1 (a int, b varchar, c varchar)");
        executeSql("INSERT INTO test1 VALUES (0, 'a', '0')");
        executeSql("INSERT INTO test1 VALUES (1, 'b', '1')");

        executeSql("CREATE TABLE test2 (a int, b varchar)");
        executeSql("INSERT INTO test2 VALUES (0, '0')");
        executeSql("INSERT INTO test2 VALUES (2, '2')");

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
            "WHEN MATCHED THEN UPDATE SET a = src.a, b = src.b";

        assertQuery(sql).matches(QueryChecker.containsSubPlan("IgniteTableSpool")).check();

        assertQuery("SELECT * FROM test2")
            .returns(0, "a")
            .returns(2, "2")
            .check();
    }

    /**
     * Test MERGE with INSERT clause only.
     */
    @Test
    public void testMergeWhenNotMatched() {
        executeSql("CREATE TABLE test1 (a int, b varchar, c varchar)");
        executeSql("INSERT INTO test1 VALUES (0, 'a', '0')");
        executeSql("INSERT INTO test1 VALUES (1, 'b', '1')");

        executeSql("CREATE TABLE test2 (a int, b varchar)");
        executeSql("INSERT INTO test2 VALUES (0, '0')");
        executeSql("INSERT INTO test2 VALUES (2, '2')");

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
            "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)";

        assertQuery(sql).matches(QueryChecker.containsSubPlan("IgniteTableSpool")).check();

        assertQuery("SELECT * FROM test2")
            .returns(0, "0")
            .returns(1, "b")
            .returns(2, "2")
            .check();
    }

    /**
     * Test MERGE table with itself.
     */
    @Test
    public void testMergeTableWithItself() {
        executeSql("CREATE TABLE test1 (a int, b int, c varchar)");
        executeSql("INSERT INTO test1 VALUES (0, 0, '0')");

        String sql = "MERGE INTO test1 dst USING test1 src ON dst.a = src.a + 1 " +
            "WHEN MATCHED THEN UPDATE SET b = dst.b + 1 " +
            "WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (src.a + 1, 1, src.a)";

        for (int i = 0; i < 5; i++)
            executeSql(sql);

        assertQuery("SELECT * FROM test1")
            .returns(0, 0, "0")
            .returns(1, 5, "0")
            .returns(2, 4, "1")
            .returns(3, 3, "2")
            .returns(4, 2, "3")
            .returns(5, 1, "4")
            .check();
    }

    /**
     * Test MERGE operator with large batch.
     */
    @Test
    public void testMergeBatch() {
        executeSql("CREATE TABLE test1 (a int)");

        executeSql("INSERT INTO test1 SELECT x FROM TABLE(SYSTEM_RANGE(0, 9999))");

        executeSql("CREATE TABLE test2 (a int, b int)");

        executeSql("INSERT INTO test2 SELECT x, 0 FROM TABLE(SYSTEM_RANGE(-5000, 4999))");

        executeSql("MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
            "WHEN MATCHED THEN UPDATE SET b = 1 " +
            "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, 2)");

        assertQuery("SELECT count(*) FROM test2 WHERE b = 0").returns(5_000L).check();
        assertQuery("SELECT count(*) FROM test2 WHERE b = 1").returns(5_000L).check();
        assertQuery("SELECT count(*) FROM test2 WHERE b = 2").returns(5_000L).check();
    }

    /**
     * Test MERGE operator with aliases.
     */
    @Test
    public void testMergeAliases() {
        executeSql("CREATE TABLE test1 (a int, b int, c varchar)");
        executeSql("INSERT INTO test1 VALUES (0, 0, '0')");

        executeSql("CREATE TABLE test2 (a int, d int, e varchar)");

        // Without aliases, column 'A' in insert statement is not ambiguous.
        executeSql("MERGE INTO test2 USING test1 ON c = e " +
            "WHEN MATCHED THEN UPDATE SET d = b + 1" +
            "WHEN NOT MATCHED THEN INSERT (a, d, e) VALUES (a, b, c)");

        assertQuery("SELECT * FROM test2").returns(0, 0, "0").check();

        // Target table alias duplicate source table name.
        assertThrows("MERGE INTO test2 test1 USING test1 ON c = e " +
            "WHEN MATCHED THEN UPDATE SET d = b + 1", IgniteSQLException.class, "Duplicate relation name");

        // Source table alias duplicate target table name.
        assertThrows("MERGE INTO test2 USING test1 test2 ON c = e " +
            "WHEN MATCHED THEN UPDATE SET d = b + 1", IgniteSQLException.class, "Duplicate relation name");

        // Without aliases, reference columns by table name.
        executeSql("MERGE INTO test2 USING test1 ON test1.a = test2.a " +
            "WHEN MATCHED THEN UPDATE SET a = test1.a + 1");

        assertQuery("SELECT * FROM test2").returns(1, 0, "0").check();

        // Ambiguous column name in condition.
        assertThrows("MERGE INTO test2 USING test1 ON a = test1.a " +
            "WHEN MATCHED THEN UPDATE SET a = test1.a + 1", IgniteSQLException.class, "Column 'A' is ambiguous");

        // Ambiguous column name in update statement.
        assertThrows("MERGE INTO test2 USING test1 ON c = e " +
            "WHEN MATCHED THEN UPDATE SET a = a + 1", IgniteSQLException.class, "Column 'A' is ambiguous");

        // With aliases, reference columns by table alias.
        executeSql("MERGE INTO test2 test1 USING test1 test2 ON test1.d = test2.b " +
            "WHEN MATCHED THEN UPDATE SET a = test1.a + 1 " +
            "WHEN NOT MATCHED THEN INSERT (a, d, e) VALUES (test2.a, test2.b, test2.c)");

        assertQuery("SELECT * FROM test2").returns(2, 0, "0").check();
    }

    /**
     * Test MERGE operator with keys conflicts.
     */
    @Test
    public void testMergeKeysConflict() {
        executeSql("CREATE TABLE test1 (a int, b int)");
        executeSql("INSERT INTO test1 VALUES (0, 0)");
        executeSql("INSERT INTO test1 VALUES (1, 1)");

        executeSql("CREATE TABLE test2 (a int primary key, b int)");

        assertThrows("MERGE INTO test2 USING test1 ON test1.a = test2.a " +
            "WHEN MATCHED THEN UPDATE SET b = test1.b + 1 " +
            "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (0, b)", IgniteSQLException.class,
            "Failed to MERGE some keys due to keys conflict");
    }

    /**
     * Ensure that DML operations fails with proper errors on non-existent table
     */
    @Test
    public void testFailureOnNonExistentTable() {
        assertThrows("INSERT INTO NON_EXISTENT_TABLE(ID, NAME) VALUES (1, 'Name')",
            IgniteSQLException.class,
            "Failed to validate query. From line 1, column 13 to line 1, column 30: " +
                "Object 'NON_EXISTENT_TABLE' not found");

        assertThrows("UPDATE NON_EXISTENT_TABLE SET NAME ='NAME' WHERE ID = 1",
            IgniteSQLException.class,
            "Failed to validate query. From line 1, column 1 to line 1, column 55: " +
                "Object 'NON_EXISTENT_TABLE' not found");

        assertThrows("DELETE FROM NON_EXISTENT_TABLE WHERE ID = 1",
            IgniteSQLException.class,
            "Failed to validate query. From line 1, column 13 to line 1, column 30: " +
                "Object 'NON_EXISTENT_TABLE' not found");

        executeSql("CREATE TABLE PERSON(ID INT, PRIMARY KEY(id), NAME VARCHAR)");

        assertThrows("" +
                "MERGE INTO PERSON DST USING NON_EXISTENT_TABLE SRC ON DST.ID = SRC.ID" +
                "    WHEN MATCHED THEN UPDATE SET NAME = SRC.NAME" +
                "    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME)",
            IgniteSQLException.class,
            "Failed to validate query. From line 1, column 29 to line 1, column 46: " +
                "Object 'NON_EXISTENT_TABLE' not found");

        assertThrows("" +
                "MERGE INTO NON_EXISTENT_TABLE DST USING PERSON SRC ON DST.ID = SRC.ID" +
                "    WHEN MATCHED THEN UPDATE SET NAME = SRC.NAME" +
                "    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME)",
            IgniteSQLException.class,
            "Failed to validate query. From line 1, column 74 to line 1, column 117: " +
                "Object 'NON_EXISTENT_TABLE' not found");
    }

    /** */
    @Test
    public void testInsertDefaultValue() {
        checkDefaultValue("BOOLEAN", "TRUE", Boolean.TRUE);
        checkDefaultValue("BOOLEAN NOT NULL", "TRUE", Boolean.TRUE);
        checkDefaultValue("BIGINT", "10", 10L);
        checkDefaultValue("INTEGER", "10", 10);
        checkDefaultValue("SMALLINT", "10", (short)10);
        checkDefaultValue("TINYINT", "10", (byte)10);
        checkDefaultValue("DOUBLE", "10.01", 10.01d);
        checkDefaultValue("FLOAT", "10.01", 10.01f);
        checkDefaultValue("DECIMAL(4, 2)", "10.01", new BigDecimal("10.01"));
        checkDefaultValue("CHAR(2)", "'10'", "10");
        checkDefaultValue("VARCHAR", "'10'", "10");
        checkDefaultValue("VARCHAR NOT NULL", "'10'", "10");
        checkDefaultValue("VARCHAR(2)", "'10'", "10");
        checkDefaultValue("INTERVAL DAYS TO SECONDS", "INTERVAL '10' DAYS", Duration.ofDays(10));
        checkDefaultValue("INTERVAL YEARS TO MONTHS", "INTERVAL '10' MONTHS", Period.ofMonths(10));
        checkDefaultValue("INTERVAL MONTHS", "INTERVAL '10' YEARS", Period.ofYears(10));
        checkDefaultValue("DATE", "DATE '2021-01-01'", Date.valueOf("2021-01-01"));
        checkDefaultValue("TIME", "TIME '01:01:01'", Time.valueOf("01:01:01"));
        checkDefaultValue("TIMESTAMP", "TIMESTAMP '2021-01-01 01:01:01'", Timestamp.valueOf("2021-01-01 01:01:01"));
        checkDefaultValue("BINARY(3)", "x'010203'", new byte[] {1, 2, 3});
        checkDefaultValue("VARBINARY", "x'010203'", new byte[] {1, 2, 3});

        UUID uuid = UUID.randomUUID();
        checkDefaultValue("UUID", '\'' + uuid.toString() + '\'', uuid);

        checkWrongDefault("VARCHAR", "10");
        checkWrongDefault("INT", "'10'");
        checkWrongDefault("INT", "TRUE");
        checkWrongDefault("DATE", "10");
        checkWrongDefault("DATE", "TIME '01:01:01'");
        checkWrongDefault("TIME", "TIMESTAMP '2021-01-01 01:01:01'");
        checkWrongDefault("BOOLEAN", "1");
        checkWrongDefault("INTERVAL DAYS", "INTERVAL '10' MONTHS");
        checkWrongDefault("INTERVAL MONTHS", "INTERVAL '10' DAYS");
        checkWrongDefault("VARBINARY", "'10'");
        checkWrongDefault("VARBINARY", "10");
        checkWrongDefault("UUID", "FALSE");
    }

    /** */
    private void checkDefaultValue(String sqlType, String sqlVal, Object expectedVal) {
        try {
            executeSql("CREATE TABLE test (dummy INT, val " + sqlType + " DEFAULT " + sqlVal + ")");
            executeSql("INSERT INTO test (dummy) VALUES (0)");

            checkQueryResult("SELECT val FROM test", expectedVal);

            executeSql("DELETE FROM test");
            executeSql("INSERT INTO test (dummy, val) VALUES (0, DEFAULT)");

            checkQueryResult("SELECT val FROM test", expectedVal);
        }
        finally {
            executeSql("DROP TABLE IF EXISTS test");
        }
    }

    /** */
    private void checkQueryResult(String sql, Object expectedVal) {
        if (expectedVal.getClass().isArray()) {
            List<List<?>> res = executeSql(sql);

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());
            assertTrue("Expected: " + Arrays.deepToString(new Object[] {expectedVal}) + ", actual: " +
                Arrays.deepToString(new Object[] {res.get(0).get(0)}), Objects.deepEquals(expectedVal, res.get(0).get(0)));
        }
        else
            assertQuery(sql).returns(expectedVal).check();
    }

    /** */
    private void checkWrongDefault(String sqlType, String sqlVal) {
        try {
            assertThrows("CREATE TABLE test (val " + sqlType + " DEFAULT " + sqlVal + ")",
                IgniteSQLException.class, "Cannot convert literal");
        }
        finally {
            executeSql("DROP TABLE IF EXISTS test");
        }
    }
}
