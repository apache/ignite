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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.runtime.CalciteException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test SQL data types.
 */
public class DataTypesTest extends AbstractBasicIntegrationTest {
    /** Tests Other type. */
    @Test
    public void testOtherType() {
        try {
            executeSql("CREATE TABLE t(id INT, oth OTHER)");

            assertThrows("CREATE TABLE t2(id INT, oth OTHER DEFAULT 'str')", IgniteSQLException.class,
                "Type 'OTHER' doesn't support default value.");

            executeSql("INSERT INTO t VALUES (1, 'str')");
            executeSql("INSERT INTO t VALUES (2, 22)");
            executeSql("INSERT INTO t VALUES (3, CAST('33.5' AS FLOAT))");
            executeSql("INSERT INTO t VALUES (4, CAST('44.5' AS DOUBLE))");
            executeSql("INSERT INTO t VALUES (5, CAST('fd10556e-fc27-4a99-b5e4-89b8344cb3ce' as UUID))");
            executeSql("INSERT INTO t VALUES (6, NULL)");
            executeSql("INSERT INTO t VALUES (7, NULL)");

            assertQuery("SELECT oth FROM t order by id")
                .ordered()
                .returns("str")
                .returns(22)
                .returns(33.5f)
                .returns(44.5d)
                .returns(UUID.fromString("fd10556e-fc27-4a99-b5e4-89b8344cb3ce"))
                .returns(new Object[] {null})
                .returns(new Object[] {null})
                .check();

            assertThrows("SELECT MIN(oth) FROM t", UnsupportedOperationException.class,
                "MIN() is not supported for type 'OTHER'.");

            assertThrows("SELECT MAX(oth) FROM t", UnsupportedOperationException.class,
                "MAX() is not supported for type 'OTHER'.");

            assertThrows("SELECT AVG(oth) from t", UnsupportedOperationException.class,
                "AVG() is not supported for type 'OTHER'.");

            assertThrows("SELECT oth from t WHERE oth > 0", CalciteException.class,
                "Invalid types for comparison");

            assertQuery("SELECT oth FROM t WHERE oth=22")
                .returns(22)
                .check();

            assertQuery("SELECT oth FROM t WHERE oth is NULL")
                .returns(new Object[] {null})
                .returns(new Object[] {null})
                .check();

            executeSql("DELETE FROM t WHERE id IN (2, 3, 4, 5)");

            assertQuery("SELECT oth FROM t WHERE oth is not NULL")
                .returns("str")
                .check();

            assertQuery("SELECT oth FROM t WHERE oth!=22")
                .returns("str")
                .check();
        }
        finally {
            executeSql("DROP TABLE IF EXISTS t");
        }
    }

    /** Tests UUID without index. */
    @Test
    public void testUuidWithoutIndex() {
        testUuid(false);
    }

    /** Tests UUID with the index. */
    @Test
    public void testUuidWithIndex() {
        testUuid(true);
    }

    /** Tests UUID type. */
    private void testUuid(boolean indexed) {
        try {
            executeSql("CREATE TABLE t(id INT, name VARCHAR(255), uid UUID, primary key (id))");

            if (indexed)
                executeSql("CREATE INDEX uuid_idx ON t (uid);");

            UUID uuid1 = UUID.randomUUID();
            UUID uuid2 = UUID.randomUUID();

            UUID max = uuid1.compareTo(uuid2) > 0 ? uuid1 : uuid2;
            UUID min = max == uuid1 ? uuid2 : uuid1;

            executeSql("INSERT INTO t VALUES (1, 'fd10556e-fc27-4a99-b5e4-89b8344cb3ce', '" + uuid1 + "')");
            // Name == UUID
            executeSql("INSERT INTO t VALUES (2, '" + uuid2 + "', '" + uuid2 + "')");
            executeSql("INSERT INTO t VALUES (3, NULL, NULL)");

            assertQuery("SELECT * FROM t")
                .returns(1, "fd10556e-fc27-4a99-b5e4-89b8344cb3ce", uuid1)
                .returns(2, uuid2.toString(), uuid2)
                .returns(3, null, null)
                .check();

            assertQuery("SELECT uid FROM t WHERE uid < '" + max + "'")
                .returns(min)
                .check();

            assertQuery("SELECT uid FROM t WHERE '" + max + "' > uid")
                .returns(min)
                .check();

            assertQuery("SELECT * FROM t WHERE name = uid")
                .returns(2, uuid2.toString(), uuid2)
                .check();

            assertQuery("SELECT * FROM t WHERE name != uid")
                .returns(1, "fd10556e-fc27-4a99-b5e4-89b8344cb3ce", uuid1)
                .check();

            assertQuery("SELECT count(*), uid FROM t group by uid order by uid")
                .returns(1L, min)
                .returns(1L, max)
                .returns(1L, null)
                .check();

            assertQuery("SELECT count(*), uid FROM t group by uid having uid = " +
                "'" + uuid1 + "' order by uid")
                .returns(1L, uuid1)
                .check();

            assertQuery("SELECT t1.* from t t1, (select * from t) t2 where t1.uid = t2.name")
                .returns(2, uuid2.toString(), uuid2)
                .check();

            //TODO: https://issues.apache.org/jira/browse/IGNITE-16693 Incorrect processing nulls values in merge join.
            assertQuery("SELECT /*+ DISABLE_RULE('MergeJoinConverter') */ t1.* from t t1, (select * from t) t2 " +
                "where t1.uid = t2.uid")
                .returns(1, "fd10556e-fc27-4a99-b5e4-89b8344cb3ce", uuid1)
                .returns(2, uuid2.toString(), uuid2)
                .check();

            assertQuery("SELECT max(uid) from t")
                .returns(max)
                .check();

            assertQuery("SELECT min(uid) from t")
                .returns(min)
                .check();

            assertQuery("SELECT uid from t where uid is not NULL order by uid")
                .returns(min)
                .returns(max)
                .check();

            assertQuery("SELECT uid from t where uid is NULL order by uid")
                .returns(new Object[] {null})
                .check();

            assertThrows("SELECT avg(uid) from t", UnsupportedOperationException.class,
                "AVG() is not supported for type 'UUID'.");

            assertThrows("SELECT sum(uid) from t", UnsupportedOperationException.class,
                "SUM() is not supported for type 'UUID'.");
        }
        finally {
            executeSql("DROP TABLE if exists tbl");
        }
    }

    /**
     * Tests numeric types mapping on Java types.
     */
    @Test
    public void testNumericRanges() {
        try {
            executeSql("CREATE TABLE tbl(tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT)");

            executeSql("INSERT INTO tbl VALUES (" + Byte.MAX_VALUE + ", " + Short.MAX_VALUE + ", " +
                Integer.MAX_VALUE + ", " + Long.MAX_VALUE + ')');

            assertQuery("SELECT tiny FROM tbl").returns(Byte.MAX_VALUE).check();
            assertQuery("SELECT small FROM tbl").returns(Short.MAX_VALUE).check();
            assertQuery("SELECT i FROM tbl").returns(Integer.MAX_VALUE).check();
            assertQuery("SELECT big FROM tbl").returns(Long.MAX_VALUE).check();

            executeSql("DELETE from tbl");

            executeSql("INSERT INTO tbl VALUES (" + Byte.MIN_VALUE + ", " + Short.MIN_VALUE + ", " +
                Integer.MIN_VALUE + ", " + Long.MIN_VALUE + ')');

            assertQuery("SELECT tiny FROM tbl").returns(Byte.MIN_VALUE).check();
            assertQuery("SELECT small FROM tbl").returns(Short.MIN_VALUE).check();
            assertQuery("SELECT i FROM tbl").returns(Integer.MIN_VALUE).check();
            assertQuery("SELECT big FROM tbl").returns(Long.MIN_VALUE).check();
        }
        finally {
            executeSql("DROP TABLE if exists tbl");
        }
    }

    /**
     * Tests numeric type convertation on equals.
     */
    @Test
    public void testNumericConvertationOnEquals() {
        try {
            executeSql("CREATE TABLE tbl(tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT)");

            executeSql("INSERT INTO tbl VALUES (1, 2, 3, 4), (5, 5, 5, 5)");

            assertQuery("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.small)").returns((byte)5).check();
            assertQuery("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.tiny)").returns((short)5).check();

            assertQuery("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.i)").returns((byte)5).check();
            assertQuery("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.tiny)").returns(5).check();

            assertQuery("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.big)").returns((byte)5).check();
            assertQuery("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.tiny)").returns(5L).check();

            assertQuery("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.i)").returns((short)5).check();
            assertQuery("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.small)").returns(5).check();

            assertQuery("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.big)").returns((short)5).check();
            assertQuery("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.small)").returns(5L).check();

            assertQuery("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.big)").returns(5).check();
            assertQuery("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.i)").returns(5L).check();
        }
        finally {
            executeSql("DROP TABLE if exists tbl");
        }
    }

    /** */
    @Test
    public void testUnicodeStrings() {
        client.getOrCreateCache(new CacheConfiguration<Integer, String>()
            .setName("string_cache")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, String.class).setTableName("string_table")))
        );

        String[] values = new String[] {"Кирилл", "Müller", "我是谁", "ASCII"};

        int key = 0;

        // Insert as inlined values.
        for (String val : values)
            executeSql("INSERT INTO string_table (_key, _val) VALUES (" + key++ + ", '" + val + "')");

        List<List<?>> rows = executeSql("SELECT _val FROM string_table");

        assertEquals(ImmutableSet.copyOf(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        executeSql("DELETE FROM string_table");

        // Insert as parameters.
        for (String val : values)
            executeSql("INSERT INTO string_table (_key, _val) VALUES (?, ?)", key++, val);

        rows = executeSql("SELECT _val FROM string_table");

        assertEquals(ImmutableSet.copyOf(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        rows = executeSql("SELECT substring(_val, 1, 2) FROM string_table");

        assertEquals(ImmutableSet.of("Ки", "Mü", "我是", "AS"),
            rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        for (String val : values) {
            rows = executeSql("SELECT char_length(_val) FROM string_table WHERE _val = ?", val);

            assertEquals(1, rows.size());
            assertEquals(val.length(), rows.get(0).get(0));
        }
    }

    /** */
    @Test
    public void testBinarySql() {
        try {
            executeSql("CREATE TABLE tbl(b BINARY(3), v VARBINARY)");

            byte[] val = new byte[] {1, 2, 3};

            // From parameters to internal, from internal to store, from store to internal and from internal to user.
            executeSql("INSERT INTO tbl VALUES (?, ?)", val, val);

            List<List<?>> res = executeSql("SELECT b, v FROM tbl");

            assertEquals(1, res.size());
            assertEquals(2, res.get(0).size());
            assertTrue(Objects.deepEquals(val, res.get(0).get(0)));
            assertTrue(Objects.deepEquals(val, res.get(0).get(1)));

            executeSql("DELETE FROM tbl");

            // From literal to internal, from internal to store, from store to internal and from internal to user.
            executeSql("INSERT INTO tbl VALUES (x'1A2B3C', x'AABBCC')");

            res = executeSql("SELECT b, v FROM tbl");

            assertEquals(1, res.size());
            assertEquals(2, res.get(0).size());
            assertTrue(Objects.deepEquals(new byte[] {0x1A, 0x2B, 0x3C}, res.get(0).get(0)));
            assertTrue(Objects.deepEquals(new byte[] {(byte)0xAA, (byte)0xBB, (byte)0xCC}, res.get(0).get(1)));
        }
        finally {
            executeSql("DROP TABLE IF EXISTS tbl");
        }
    }

    /** Cache API - SQL API cross check. */
    @Test
    public void testBinaryCache() {
        try {
            IgniteCache<Integer, byte[]> cache = client.getOrCreateCache(new CacheConfiguration<Integer, byte[]>()
                .setName("binary_cache")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(F.asList(new QueryEntity(Integer.class, byte[].class).setTableName("binary_table")))
            );

            byte[] val = new byte[] {1, 2, 3};

            // From cache to internal, from internal to user.
            cache.put(1, val);

            List<List<?>> res = executeSql("SELECT _val FROM binary_table");
            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());
            assertTrue(Objects.deepEquals(val, res.get(0).get(0)));

            // From literal to internal, from internal to cache.
            executeSql("INSERT INTO binary_table (_KEY, _VAL) VALUES (2, x'010203')");
            byte[] resVal = cache.get(2);
            assertTrue(Objects.deepEquals(val, resVal));
        }
        finally {
            client.destroyCache("binary_cache");
        }
    }

    /** */
    @Test
    public void testBinaryAggregation() {
        try {
            executeSql("CREATE TABLE tbl(b varbinary)");
            executeSql("INSERT INTO tbl VALUES (NULL)");
            executeSql("INSERT INTO tbl VALUES (x'010203')");
            executeSql("INSERT INTO tbl VALUES (x'040506')");
            List<List<?>> res = executeSql("SELECT MIN(b), MAX(b) FROM tbl");

            assertEquals(1, res.size());
            assertEquals(2, res.get(0).size());
            assertTrue(Objects.deepEquals(new byte[] {1, 2, 3}, res.get(0).get(0)));
            assertTrue(Objects.deepEquals(new byte[] {4, 5, 6}, res.get(0).get(1)));
        }
        finally {
            executeSql("DROP TABLE IF EXISTS tbl");
        }
    }

    /** */
    @Test
    public void testBinaryConcat() {
        try {
            executeSql("CREATE TABLE tbl(b varbinary)");
            executeSql("INSERT INTO tbl VALUES (x'010203')");
            List<List<?>> res = executeSql("SELECT b || x'040506' FROM tbl");

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());
            assertTrue(Objects.deepEquals(new byte[] {1, 2, 3, 4, 5, 6}, res.get(0).get(0)));
        }
        finally {
            executeSql("DROP TABLE IF EXISTS tbl");
        }
    }

    /** */
    @Test
    public void testDecimalScale() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, val1 DECIMAL(5, 3), val2 DECIMAL(3), val3 DECIMAL)");

        // Check literals scale.
        sql("INSERT INTO t values (0, 0, 0, 0)");
        sql("INSERT INTO t values (1.1, 1.1, 1.1, 1.1)");
        sql("INSERT INTO t values (2.123, 2.123, 2.123, 2.123)");
        sql("INSERT INTO t values (3.123456, 3.123456, 3.123456, 3.123456)");

        // Check dynamic parameters scale.
        List<Number> params = F.asList(4, 5L, 6f, 7.25f, 8d, 9.03125d, new BigDecimal("10"),
            new BigDecimal("11.1"), new BigDecimal("12.123456"));

        for (Object val : params)
            sql("INSERT INTO t values (?, ?, ?, ?)", val, val, val, val);

        assertQuery("SELECT * FROM t")
            .returns(0, new BigDecimal("0.000"), new BigDecimal("0"), new BigDecimal("0"))
            .returns(1, new BigDecimal("1.100"), new BigDecimal("1"), new BigDecimal("1.1"))
            .returns(2, new BigDecimal("2.123"), new BigDecimal("2"), new BigDecimal("2.123"))
            .returns(3, new BigDecimal("3.123"), new BigDecimal("3"), new BigDecimal("3.123456"))
            .returns(4, new BigDecimal("4.000"), new BigDecimal("4"), new BigDecimal("4"))
            .returns(5, new BigDecimal("5.000"), new BigDecimal("5"), new BigDecimal("5"))
            .returns(6, new BigDecimal("6.000"), new BigDecimal("6"), new BigDecimal("6"))
            .returns(7, new BigDecimal("7.250"), new BigDecimal("7"), new BigDecimal("7.25"))
            .returns(8, new BigDecimal("8.000"), new BigDecimal("8"), new BigDecimal("8"))
            .returns(9, new BigDecimal("9.031"), new BigDecimal("9"), new BigDecimal("9.03125"))
            .returns(10, new BigDecimal("10.000"), new BigDecimal("10"), new BigDecimal("10"))
            .returns(11, new BigDecimal("11.100"), new BigDecimal("11"), new BigDecimal("11.1"))
            .returns(12, new BigDecimal("12.123"), new BigDecimal("12"), new BigDecimal("12.123456"))
            .check();
    }

    /** */
    @Test
    public void testNumericConversion() {
        sql("CREATE TABLE t (v1 TINYINT, v2 SMALLINT, v3 INT, v4 BIGINT, v5 DECIMAL, v6 FLOAT, v7 DOUBLE)");

        List<Number> params = F.asList((byte)1, (short)2, 3, 4L, BigDecimal.valueOf(5), 6f, 7d);

        for (Object val : params)
            sql("INSERT INTO t values (?, ?, ?, ?, ?, ?, ?)", val, val, val, val, val, val, val);

        assertQuery("SELECT * FROM t")
            .returns((byte)1, (short)1, 1, 1L, BigDecimal.valueOf(1), 1f, 1d)
            .returns((byte)2, (short)2, 2, 2L, BigDecimal.valueOf(2), 2f, 2d)
            .returns((byte)3, (short)3, 3, 3L, BigDecimal.valueOf(3), 3f, 3d)
            .returns((byte)4, (short)4, 4, 4L, BigDecimal.valueOf(4), 4f, 4d)
            .returns((byte)5, (short)5, 5, 5L, BigDecimal.valueOf(5), 5f, 5d)
            .returns((byte)6, (short)6, 6, 6L, BigDecimal.valueOf(6), 6f, 6d)
            .returns((byte)7, (short)7, 7, 7L, BigDecimal.valueOf(7), 7f, 7d)
            .check();
    }
}
