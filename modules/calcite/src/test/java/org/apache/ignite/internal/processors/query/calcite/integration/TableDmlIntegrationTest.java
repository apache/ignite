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
import java.util.stream.Stream;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class TableDmlIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /**
     * Test verifies that already inserted by the current query data
     * is not processed by this query again.
     */
    @Test
    public void testInsertAsSelect() {
        executeSql("CREATE TABLE test (epoch_cur int, epoch_copied int) WITH " + atomicity());
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
        executeSql("CREATE TABLE test (id int primary key, val int) " +
            "with cache_name=\"test\", value_type=\"my_type\", " + atomicity());
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
        executeSql("CREATE TABLE test (val integer) with " + atomicity());

        executeSql("CREATE INDEX test_val_idx ON test (val)");

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
        grid(1).getOrCreateCache(this.<Integer, CalciteQueryProcessorTest.Developer>cacheConfiguration()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, CalciteQueryProcessorTest.Developer.class)
            .setBackups(2)
        );

        List<List<?>> rows = executeSql("INSERT INTO DEVELOPER(_key, name, projectId) VALUES (?, ?, ?)", 0, "Igor", 1);

        assertEquals(1, rows.size());

        List<?> row = rows.get(0);

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        rows = executeSql("select _key, * from DEVELOPER");

        assertEquals(1, rows.size());

        row = F.first(rows);

        assertNotNull(row);

        assertEqualsCollections(Arrays.asList(0, "Igor", 1), row);
    }

    /** */
    @Test
    public void testInsertUpdateDeleteNonPrimitiveKey() throws Exception {
        client.getOrCreateCache(this.<CalciteQueryProcessorTest.Key, CalciteQueryProcessorTest.Developer>cacheConfiguration()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(CalciteQueryProcessorTest.Key.class, CalciteQueryProcessorTest.Developer.class)
            .setBackups(2)
        );

        awaitPartitionMapExchange(true, true, null);

        List<?> row = F.first(executeSql("INSERT INTO DEVELOPER VALUES (?, ?, ?, ?)", 0, 0, "Igor", 1));

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        row = F.first(executeSql("select * from DEVELOPER"));

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "Igor", 1), row);

        row = F.first(executeSql("UPDATE DEVELOPER d SET name = name || 'Roman' WHERE id = ?", 0));

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        row = F.first(executeSql("select * from DEVELOPER"));

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "IgorRoman", 1), row);

        row = F.first(executeSql("DELETE FROM DEVELOPER WHERE id = ?", 0));

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        row = F.first(executeSql("select * from DEVELOPER"));

        assertNull(row);
    }

    /**
     * Test insert/update/delete rows of a table with two or more fields in the primary key.
     */
    @Test
    public void testInsertUpdateDeleteComplexKey() {
        executeSql("CREATE TABLE t(id INT, val VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val)) WITH " + atomicity());
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
        executeSql("CREATE TABLE test1 (a int, b varchar, c varchar) WITH " + atomicity());
        executeSql("CREATE TABLE test2 (a int, b varchar) WITH " + atomicity());

        executeSql("INSERT INTO test1 VALUES (0, 'a', '0')");
        executeSql("INSERT INTO test1 VALUES (1, 'b', '1')");

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
        executeSql("CREATE TABLE test1 (a int, b varchar, c varchar) WITH " + atomicity());
        executeSql("CREATE TABLE test2 (a int, b varchar) WITH " + atomicity());

        executeSql("INSERT INTO test1 VALUES (0, 'a', '0')");
        executeSql("INSERT INTO test1 VALUES (1, 'b', '1')");

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
        executeSql("CREATE TABLE test1 (a int, b varchar, c varchar) WITH " + atomicity());
        executeSql("CREATE TABLE test2 (a int, b varchar) WITH " + atomicity());

        executeSql("INSERT INTO test1 VALUES (0, 'a', '0')");
        executeSql("INSERT INTO test1 VALUES (1, 'b', '1')");

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
        executeSql("CREATE TABLE test1 (a int, b int, c varchar) WITH " + atomicity());
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
        executeSql("CREATE TABLE test1 (a int) WITH " + atomicity());
        executeSql("CREATE TABLE test2 (a int, b int) WITH " + atomicity());

        executeSql("INSERT INTO test1 SELECT x FROM TABLE(SYSTEM_RANGE(0, 9999))");

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
        executeSql("CREATE TABLE test1 (a int, b int, c varchar) WITH " + atomicity());
        executeSql("CREATE TABLE test2 (a int, d int, e varchar) WITH " + atomicity());

        executeSql("INSERT INTO test1 VALUES (0, 0, '0')");

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
        executeSql("CREATE TABLE test1 (a int, b int) WITH " + atomicity());
        executeSql("CREATE TABLE test2 (a int primary key, b int) WITH " + atomicity());

        executeSql("INSERT INTO test1 VALUES (0, 0)");
        executeSql("INSERT INTO test1 VALUES (1, 1)");

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
            "Object 'NON_EXISTENT_TABLE' not found");

        assertThrows("UPDATE NON_EXISTENT_TABLE SET NAME ='NAME' WHERE ID = 1",
            IgniteSQLException.class,
            "Object 'NON_EXISTENT_TABLE' not found");

        assertThrows("DELETE FROM NON_EXISTENT_TABLE WHERE ID = 1",
            IgniteSQLException.class,
            "Object 'NON_EXISTENT_TABLE' not found");

        executeSql("CREATE TABLE PERSON(ID INT, PRIMARY KEY(id), NAME VARCHAR) WITH " + atomicity());

        assertThrows("" +
                "MERGE INTO PERSON DST USING NON_EXISTENT_TABLE SRC ON DST.ID = SRC.ID" +
                "    WHEN MATCHED THEN UPDATE SET NAME = SRC.NAME" +
                "    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME)",
            IgniteSQLException.class,
            "Object 'NON_EXISTENT_TABLE' not found");

        assertThrows("" +
                "MERGE INTO NON_EXISTENT_TABLE DST USING PERSON SRC ON DST.ID = SRC.ID" +
                "    WHEN MATCHED THEN UPDATE SET NAME = SRC.NAME" +
                "    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME)",
            IgniteSQLException.class,
            "Object 'NON_EXISTENT_TABLE' not found");
    }

    /** */
    @Test
    public void testInsertMultipleDefaults() {
        Stream.of(true, false).forEach(withPk -> {
            try {
                sql("CREATE TABLE integers(i INTEGER " + (withPk ? "PRIMARY KEY" : "") +
                        " , col1 INTEGER DEFAULT 200, col2 INTEGER DEFAULT 300) WITH " + atomicity());

                sql("INSERT INTO integers (i) VALUES (0)");
                sql("INSERT INTO integers VALUES (1, DEFAULT, DEFAULT)");
                sql("INSERT INTO integers(i, col2) VALUES (2, DEFAULT), (3, 4), (4, DEFAULT)");
                sql("INSERT INTO integers VALUES (5, DEFAULT, DEFAULT)");
                sql("INSERT INTO integers VALUES (6, 4, DEFAULT)");
                sql("INSERT INTO integers VALUES (7, 5, 5)");
                sql("INSERT INTO integers(col1, i) VALUES (DEFAULT, 8)");
                sql("INSERT INTO integers(i, col1) VALUES (9, DEFAULT)");

                assertQuery("SELECT i, col1, col2 FROM integers ORDER BY i")
                        .returns(0, 200, 300)
                        .returns(1, 200, 300)
                        .returns(2, 200, 300)
                        .returns(3, 200, 4)
                        .returns(4, 200, 300)
                        .returns(5, 200, 300)
                        .returns(6, 4, 300)
                        .returns(7, 5, 5)
                        .returns(8, 200, 300)
                        .returns(9, 200, 300)
                        .check();
            }
            finally {
                clearTransaction();

                sql("DROP TABLE IF EXISTS integers");
            }
        });
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

    /**
     * Test checks the impossibility of inserting duplicate keys.
     */
    @Test
    public void testInsertDuplicateKey() {
        executeSql("CREATE TABLE test (a int primary key, b int) WITH " + atomicity());

        executeSql("INSERT INTO test VALUES (0, 0)");
        executeSql("INSERT INTO test VALUES (1, 1)");

        assertThrows("INSERT INTO test VALUES (1, 2)", IgniteSQLException.class,
                "Failed to INSERT some keys because they are already in cache");
    }

    /** */
    @Test
    public void testInsertValueOverflow() {
        List<List<Object>> args = F.asList(
            F.asList(SqlTypeName.BIGINT.getName(), Long.MAX_VALUE, Long.MIN_VALUE),
            F.asList(SqlTypeName.INTEGER.getName(), (long)Integer.MAX_VALUE, (long)Integer.MIN_VALUE),
            F.asList(SqlTypeName.SMALLINT.getName(), (long)Short.MAX_VALUE, (long)Short.MIN_VALUE),
            F.asList(SqlTypeName.TINYINT.getName(), (long)Byte.MAX_VALUE, (long)Byte.MIN_VALUE)
        );

        for (List<Object> arg : args) {
            try {
                String type = (String)arg.get(0);
                long max = (Long)arg.get(1);
                long min = (Long)arg.get(2);

                sql(String.format("CREATE TABLE TEST_SOURCE (ID INT PRIMARY KEY, VAL %s) WITH " + atomicity(), type));
                sql(String.format("CREATE TABLE TEST_DEST (ID INT PRIMARY KEY, VAL %s) WITH " + atomicity(), type));

                sql("INSERT INTO TEST_SOURCE VALUES (1, 1)");
                sql(String.format("INSERT INTO TEST_SOURCE VALUES (2, %d)", max));
                sql("INSERT INTO TEST_SOURCE VALUES (3, -1)");
                sql(String.format("INSERT INTO TEST_SOURCE VALUES (4, %d)", min));

                BigDecimal moreThanMax = new BigDecimal(max).add(BigDecimal.ONE);

                assertThrows(String.format("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, %s)", moreThanMax.toString()),
                    IgniteSQLException.class, type + " overflow");
                assertThrows(String.format("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, %d + 1)", max),
                    IgniteSQLException.class, type + " overflow");
                assertThrows(String.format("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, %d - 1)", min),
                    IgniteSQLException.class, type + " overflow");
                assertThrows(String.format("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, %d + (SELECT 1))", max),
                    IgniteSQLException.class, type + " overflow");
                assertThrows(String.format("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, %d + (SELECT -1))", min),
                    IgniteSQLException.class, type + " overflow");
                assertThrows("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, (SELECT SUM(VAL) FROM TEST_SOURCE WHERE VAL > 0))",
                    IgniteSQLException.class, type + " overflow");
                assertThrows("INSERT INTO TEST_DEST (ID, VAL) VALUES (1, (SELECT SUM(VAL) FROM TEST_SOURCE WHERE VAL < 0))",
                    IgniteSQLException.class, type + " overflow");
            }
            finally {
                clearTransaction();

                sql("DROP TABLE TEST_SOURCE");
                sql("DROP TABLE TEST_DEST");
            }
        }
    }

    /** */
    private void checkDefaultValue(String sqlType, String sqlVal, Object expectedVal) {
        try {
            executeSql("CREATE TABLE test (dummy INT, val " + sqlType + " DEFAULT " + sqlVal + ") WITH " + atomicity());
            executeSql("INSERT INTO test (dummy) VALUES (0)");

            checkQueryResult("SELECT val FROM test", expectedVal);

            executeSql("DELETE FROM test");
            executeSql("INSERT INTO test (dummy, val) VALUES (0, DEFAULT)");

            checkQueryResult("SELECT val FROM test", expectedVal);
        }
        finally {
            clearTransaction();

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
            assertThrows("CREATE TABLE test (val " + sqlType + " DEFAULT " + sqlVal + ") WITH " + atomicity(),
                IgniteSQLException.class, "Cannot convert literal");
        }
        finally {
            executeSql("DROP TABLE IF EXISTS test");
        }
    }
}
