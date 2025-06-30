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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.SupplierX;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.junit.Assume.assumeTrue;

/**
 * Test SQL data types.
 */
public class DataTypesTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Test
    public void testRoundingOfNumerics() {
        doTestCoercionOfNumerics(numericsToRound(), false, false);
    }

    /** */
    @Test
    public void testRoundingOfNumericsPrecasted() {
        doTestCoercionOfNumerics(numericsToRound(), false, true);
    }

    /** */
    @Test
    public void testRoundingOfDynamicNumerics() {
        doTestCoercionOfNumerics(numericsToRound(), true, false);
    }

    /** */
    @Test
    public void testRoundingOfDynamicNumericsPrecasted() {
        doTestCoercionOfNumerics(numericsToRound(), true, true);
    }

    /** @return input type, input value, target type, expected result. */
    private static List<List<Object>> numericsToRound() {
        List<List<Object>> lst = new ArrayList<>(50);

        Exception overflowErr = new ArithmeticException("overflow");

        lst.add(F.asList("DECIMAL(5,4)", BigDecimal.valueOf(1.4999d), "DECIMAL(1)", new BigDecimal(1)));
        lst.add(F.asList("DECIMAL(5,4)", BigDecimal.valueOf(-1.4999d), "DECIMAL(1)", new BigDecimal(-1)));
        lst.add(F.asList("DECIMAL(2,1)", BigDecimal.valueOf(1.5d), "DECIMAL(1)", new BigDecimal(2)));
        lst.add(F.asList("DECIMAL(2,1)", BigDecimal.valueOf(-1.5d), "DECIMAL(1)", new BigDecimal(-2)));
        lst.add(F.asList("DECIMAL(5,4)", BigDecimal.valueOf(1.4999d), "DECIMAL(2,1)", BigDecimal.valueOf(1.5)));
        lst.add(F.asList("DECIMAL(5,4)", BigDecimal.valueOf(-1.4999d), "DECIMAL(2,1)", BigDecimal.valueOf(-1.5)));

        lst.add(F.asList("DECIMAL(20,1)", new BigDecimal("-9223372036854775808.4"), "BIGINT", -9223372036854775808L));
        lst.add(F.asList("DECIMAL(20,1)", new BigDecimal("-9223372036854775808.5"), "BIGINT", overflowErr));
        lst.add(F.asList("DECIMAL(20,1)", new BigDecimal("9223372036854775807.4"), "BIGINT", 9223372036854775807L));
        lst.add(F.asList("DECIMAL(20,1)", new BigDecimal("9223372036854775807.5"), "BIGINT", overflowErr));

        lst.add(F.asList("DOUBLE", -2147483648.4d, "INT", -2147483648));
        lst.add(F.asList("DOUBLE", -2147483648.5d, "INT", overflowErr));
        lst.add(F.asList("DOUBLE", 2147483647.4d, "INT", 2147483647));
        lst.add(F.asList("DOUBLE", 2147483647.5d, "INT", overflowErr));

        for (String numTypeName : F.asList("DOUBLE", "FLOAT")) {
            lst.add(F.asList(numTypeName, floatingVal(1.4999f, numTypeName), "DECIMAL(1)", new BigDecimal(1)));
            lst.add(F.asList(numTypeName, floatingVal(-1.4999f, numTypeName), "DECIMAL(1)", new BigDecimal(-1)));
            lst.add(F.asList(numTypeName, floatingVal(1.5f, numTypeName), "DECIMAL(1)", new BigDecimal(2)));
            lst.add(F.asList(numTypeName, floatingVal(-1.5f, numTypeName), "DECIMAL(1)", new BigDecimal(-2)));

            lst.add(F.asList(numTypeName, floatingVal(1.4999f, numTypeName), "BIGINT", 1L));
            lst.add(F.asList(numTypeName, floatingVal(-1.4999f, numTypeName), "BIGINT", -1L));
            lst.add(F.asList(numTypeName, floatingVal(1.5f, numTypeName), "BIGINT", 2L));
            lst.add(F.asList(numTypeName, floatingVal(-1.5f, numTypeName), "BIGINT", -2L));

            lst.add(F.asList(numTypeName, floatingVal(1.4999f, numTypeName), "INT", 1));
            lst.add(F.asList(numTypeName, floatingVal(-1.4999f, numTypeName), "INT", -1));
            lst.add(F.asList(numTypeName, floatingVal(1.5f, numTypeName), "INT", 2));
            lst.add(F.asList(numTypeName, floatingVal(-1.5f, numTypeName), "INT", -2));

            lst.add(F.asList(numTypeName, floatingVal(1.4999f, numTypeName), "SMALLINT", (short)1));
            lst.add(F.asList(numTypeName, floatingVal(-1.4999f, numTypeName), "SMALLINT", (short)-1));
            lst.add(F.asList(numTypeName, floatingVal(1.5f, numTypeName), "SMALLINT", (short)2));
            lst.add(F.asList(numTypeName, floatingVal(-1.5f, numTypeName), "SMALLINT", (short)-2));
            lst.add(F.asList(numTypeName, floatingVal(32767.4f, numTypeName), "SMALLINT", (short)32767));
            lst.add(F.asList(numTypeName, floatingVal(32767.5f, numTypeName), "SMALLINT", overflowErr));
            lst.add(F.asList(numTypeName, floatingVal(-32768.4f, numTypeName), "SMALLINT", (short)-32768));
            lst.add(F.asList(numTypeName, floatingVal(-32768.5f, numTypeName), "SMALLINT", overflowErr));

            lst.add(F.asList(numTypeName, floatingVal(1.4999f, numTypeName), "TINYINT", (byte)1));
            lst.add(F.asList(numTypeName, floatingVal(-1.4999f, numTypeName), "TINYINT", (byte)-1));
            lst.add(F.asList(numTypeName, floatingVal(1.5f, numTypeName), "TINYINT", (byte)2));
            lst.add(F.asList(numTypeName, floatingVal(-1.5f, numTypeName), "TINYINT", (byte)-2));
            lst.add(F.asList(numTypeName, floatingVal(127.4f, numTypeName), "TINYINT", (byte)127));
            lst.add(F.asList(numTypeName, floatingVal(127.5f, numTypeName), "TINYINT", overflowErr));
            lst.add(F.asList(numTypeName, floatingVal(-128.4f, numTypeName), "TINYINT", (byte)-128));
            lst.add(F.asList(numTypeName, floatingVal(-128.5f, numTypeName), "TINYINT", overflowErr));
        }

        return lst;
    }

    /** */
    private static Number floatingVal(float v, String typeName) {
        if ("DOUBLE".equalsIgnoreCase(typeName))
            return (double)v;

        return v;
    }

    /** Tests Other type. */
    @Test
    public void testOtherType() {
        executeSql("CREATE TABLE t(id INT, oth OTHER) WITH " + atomicity());

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
            .returns(new Object[]{null})
            .returns(new Object[]{null})
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
            .returns(new Object[]{null})
            .returns(new Object[]{null})
            .check();

        executeSql("DELETE FROM t WHERE id IN (2, 3, 4, 5)");

        assertQuery("SELECT oth FROM t WHERE oth is not NULL")
            .returns("str")
            .check();

        assertQuery("SELECT oth FROM t WHERE oth!=22")
            .returns("str")
            .check();
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
        executeSql("CREATE TABLE t(id INT, name VARCHAR(255), uid UUID, primary key (id)) WITH " + atomicity());

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

        assertQuery("SELECT t1.* from t t1, (select * from t) t2 " +
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
            .returns(new Object[]{null})
            .check();

        assertThrows("SELECT avg(uid) from t", SqlValidatorException.class,
            "Cannot apply 'AVG' to arguments of type");

        assertThrows("SELECT sum(uid) from t", SqlValidatorException.class,
            "Cannot apply 'SUM' to arguments of type");

        assertThrows("SELECT bitand(uid, 1) from t", SqlValidatorException.class,
            "Cannot apply 'BITAND' to arguments of type");
        assertThrows("SELECT bitor(uid, 1) from t", SqlValidatorException.class,
            "Cannot apply 'BITOR' to arguments of type");
        assertThrows("SELECT bitxor(uid, 1) from t", SqlValidatorException.class,
            "Cannot apply 'BITXOR' to arguments of type");
    }

    /**
     * Tests numeric types mapping on Java types.
     */
    @Test
    public void testNumericRanges() {
        executeSql("CREATE TABLE tbl(tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT) WITH " + atomicity());

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

    /**
     * Tests numeric type convertation on equals.
     */
    @Test
    public void testNumericConvertationOnEquals() {
        executeSql("CREATE TABLE tbl(tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT) WITH " + atomicity());

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

    /** */
    @Test
    public void testUnicodeStrings() {
        client.getOrCreateCache(this.<Integer, String>cacheConfiguration()
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
        executeSql("CREATE TABLE tbl(b BINARY(3), v VARBINARY) WITH " + atomicity());

        byte[] val = new byte[]{1, 2, 3};

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
        assertTrue(Objects.deepEquals(new byte[]{0x1A, 0x2B, 0x3C}, res.get(0).get(0)));
        assertTrue(Objects.deepEquals(new byte[]{(byte)0xAA, (byte)0xBB, (byte)0xCC}, res.get(0).get(1)));
    }

    /** */
    @Test
    public void testUnsupportedTypes() {
        assertThrows("CREATE TABLE test (val TIME WITH TIME ZONE)", IgniteException.class,
            "'TIME WITH TIME ZONE' is not supported.");
        assertThrows("CREATE TABLE test (val TIMESTAMP WITH TIME ZONE)", IgniteException.class,
            "'TIMESTAMP WITH TIME ZONE' is not supported.");
        assertThrows("CREATE TABLE test (val TIME WITH LOCAL TIME ZONE)", IgniteException.class,
            "'TIME WITH LOCAL TIME ZONE' is not supported.");
        assertThrows("CREATE TABLE test (val TIMESTAMP WITH LOCAL TIME ZONE)", IgniteException.class,
            "'TIMESTAMP WITH LOCAL TIME ZONE' is not supported.");

        assertThrows("SELECT CAST (1 as TIME WITH TIME ZONE)", IgniteException.class,
            "'TIME WITH TIME ZONE' is not supported.");
        assertThrows("SELECT CAST (1 as TIMESTAMP WITH TIME ZONE)", IgniteException.class,
            "'TIMESTAMP WITH TIME ZONE' is not supported.");
        assertThrows("SELECT CAST (1 as TIME WITH LOCAL TIME ZONE)", IgniteException.class,
            "'TIME WITH LOCAL TIME ZONE' is not supported.");
        assertThrows("SELECT CAST (1 as TIMESTAMP WITH LOCAL TIME ZONE)", IgniteException.class,
            "'TIMESTAMP WITH LOCAL TIME ZONE' is not supported.");
    }

    /** Cache API - SQL API cross check. */
    @Test
    public void testBinaryCache() {
        IgniteCache<Integer, byte[]> cache = client.getOrCreateCache(this.<Integer, byte[]>cacheConfiguration()
            .setName("binary_cache")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, byte[].class).setTableName("binary_table")))
        );

        byte[] val = new byte[]{1, 2, 3};

        // From cache to internal, from internal to user.
        put(client, cache, 1, val);

        List<List<?>> res = executeSql("SELECT _val FROM binary_table");
        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertTrue(Objects.deepEquals(val, res.get(0).get(0)));

        // From literal to internal, from internal to cache.
        executeSql("INSERT INTO binary_table (_KEY, _VAL) VALUES (2, x'010203')");

        SupplierX<Object> check = () -> {
            byte[] resVal = cache.get(2);

            assertTrue(Objects.deepEquals(val, resVal));

            return null;
        };

        if (sqlTxMode == SqlTransactionMode.NONE)
            check.get();
        else
            txAction(client, check);
    }

    /** */
    @Test
    public void testBinaryAggregation() {
        executeSql("CREATE TABLE tbl(b varbinary) WITH " + atomicity());
        executeSql("INSERT INTO tbl VALUES (NULL)");
        executeSql("INSERT INTO tbl VALUES (x'010203')");
        executeSql("INSERT INTO tbl VALUES (x'040506')");
        List<List<?>> res = executeSql("SELECT MIN(b), MAX(b) FROM tbl");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
        assertTrue(Objects.deepEquals(new byte[]{1, 2, 3}, res.get(0).get(0)));
        assertTrue(Objects.deepEquals(new byte[]{4, 5, 6}, res.get(0).get(1)));
    }

    /** */
    @Test
    public void testBinaryConcat() {
        executeSql("CREATE TABLE tbl(b varbinary) WITH " + atomicity());
        executeSql("INSERT INTO tbl VALUES (x'010203')");
        List<List<?>> res = executeSql("SELECT b || x'040506' FROM tbl");

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertTrue(Objects.deepEquals(new byte[]{1, 2, 3, 4, 5, 6}, res.get(0).get(0)));
    }

    /** */
    @Test
    public void testDecimalScale() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, val1 DECIMAL(5, 3), val2 DECIMAL(3), val3 DECIMAL) WITH " + atomicity());

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
            .returns(1, new BigDecimal("1.100"), new BigDecimal("1"), new BigDecimal("1"))
            .returns(2, new BigDecimal("2.123"), new BigDecimal("2"), new BigDecimal("2"))
            .returns(3, new BigDecimal("3.123"), new BigDecimal("3"), new BigDecimal("3"))
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
    public void testIsNotDistinctFromTypeConversion() {
        SqlTypeName[] numerics = new SqlTypeName[] {SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER,
            SqlTypeName.BIGINT, SqlTypeName.DECIMAL, SqlTypeName.FLOAT, SqlTypeName.DOUBLE};

        for (SqlTypeName type : numerics) {
            String t = type.getName();

            sql("CREATE TABLE t1(key1 INTEGER, i1idx INTEGER, i1 INTEGER, chr1 VARCHAR, PRIMARY KEY(key1)) WITH " + atomicity());
            sql("CREATE INDEX t1_idx ON t1(i1idx)");

            sql("CREATE TABLE t2(key2 " + t + ", i2idx " + t + ", i2 " + t + ", i3 INTEGER, chr2 VARCHAR, PRIMARY KEY(key2)) " +
                "WITH " + atomicity());
            sql("CREATE INDEX t2_idx ON t2(i2idx)");

            sql("INSERT INTO t1 VALUES (1, 1, null, '1'), (2, 2, 2, '22'), (3, 33, 3, null), (4, null, 4, '4')");
            sql("INSERT INTO t2 VALUES (0, 0, 0, null, '0'), (11, null, 1, 1, '1'), (2, 2, 2, 2, '22'), (3, 3, null, 3, null)");

            for (HintDefinition hint : F.asList(HintDefinition.MERGE_JOIN, HintDefinition.NL_JOIN, HintDefinition.CNL_JOIN)) {
                String h = "/*+ " + hint.name() + " */ ";

                // Primary keys, indexed.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON key2 IS NOT DISTINCT FROM key1")
                    .returns(2, 2)
                    .returns(3, 3)
                    .check();

                // Indexed and not indexed.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2idx IS NOT DISTINCT FROM i1")
                    .returns(1, 1)
                    .returns(2, 2)
                    .returns(3, 3)
                    .check();

                // Both not indexed.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2 IS NOT DISTINCT FROM i1")
                    .returns(1, 3)
                    .returns(2, 2)
                    .check();

                // Indexed and casted.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2idx IS NOT DISTINCT FROM CAST(chr1 as INTEGER)")
                    .returns(3, 1)
                    .check();

                // Not indexed and casted.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2 IS NOT DISTINCT FROM CAST(chr1 as INTEGER)")
                    .returns(1, 1)
                    .returns(3, 3)
                    .check();

                // @see MergeJoinConverterRule#matchesJoin(RelOptRuleCall)
                if (hint == HintDefinition.MERGE_JOIN)
                    continue;

                // Primary keys, indexed.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON key2 IS DISTINCT FROM key1 and key1<2")
                    .returns(1, 1)
                    .returns(1, 2)
                    .returns(1, 3)
                    .returns(1, null)
                    .check();

                // Indexed and not indexed.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2idx IS NOT DISTINCT FROM i1")
                    .returns(1, 1)
                    .returns(2, 2)
                    .returns(3, 3)
                    .check();

                // Both not indexed.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2 IS DISTINCT FROM i1 and key1<3")
                    .returns(1, 1)
                    .returns(1, 2)
                    .returns(1, null)
                    .returns(2, 1)
                    .returns(2, 3)
                    .returns(2, null)
                    .check();

                // Indexed and casted.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2idx IS DISTINCT FROM CAST(chr1 as INTEGER) and key1<2")
                    .returns(1, null)
                    .returns(1, 2)
                    .returns(1, 3)
                    .returns(1, 1)
                    .check();

                // Not indexed and casted.
                assertQuery("SELECT " + h + "key1, i3 FROM t1 JOIN t2 ON i2 IS DISTINCT FROM CAST(chr1 as INTEGER) and key1<2")
                    .returns(1, null)
                    .returns(1, 2)
                    .returns(1, 3)
                    .check();
            }

            clearTransaction();

            sql("DROP TABLE t1");
            sql("DROP TABLE t2");

            clearTransaction();
        }
    }

    /** */
    @Test
    public void testNumericConversion() {
        sql("CREATE TABLE t (v1 TINYINT, v2 SMALLINT, v3 INT, v4 BIGINT, v5 DECIMAL, v6 FLOAT, v7 DOUBLE) WITH " + atomicity());

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

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-25749")
    @Test
    public void testCharLiteralsInUnion() {
        assumeTrue(sqlTxMode == SqlTransactionMode.NONE);

        assertQuery("SELECT * FROM (SELECT 'word' i UNION ALL SELECT 'w' i) t1 WHERE i='w'")
            .returns("w")
            .check();
    }

    /** */
    @Test
    public void testCoercionOfVarcharLiterals() {
        assumeNoTransactions();

        doTestCoercionOfVarchars(false);
    }

    /** */
    @Test
    public void testCoercionOfVarcharDynamicParameters() {
        assumeNoTransactions();

        doTestCoercionOfVarchars(true);
    }

    /** */
    private void doTestCoercionOfVarchars(boolean dynamics) {
        for (List<String> params : varcharsToCoerce()) {
            String val = params.get(0);
            String type = params.get(1);
            String result = params.get(2);

            if (dynamics)
                assertQuery(String.format("SELECT CAST(? AS %s)", type)).withParams(val).returns(result).check();
            else
                assertQuery(String.format("SELECT CAST('%s' AS %s)", val, type)).returns(result).check();
        }
    }

    /** */
    private static List<List<String>> varcharsToCoerce() {
        return F.asList(
            F.asList("abcde", "VARCHAR(3)", "abc"),
            F.asList("abcde", "VARCHAR(5)", "abcde"),
            F.asList("abcde", "VARCHAR(6)", "abcde"),
            F.asList("abcde", "VARCHAR", "abcde"),
            F.asList("abcde", "CHAR", "a"),
            F.asList("abcde", "CHAR(3)", "abc")
        );
    }

    /** */
    @Test
    public void testCoercionOfNumericLiterals() {
        doTestCoercionOfNumerics(numericsToCast(), false, false);
    }

    /** */
    @Test
    public void testCoercionOfNumericLiteralsPrecasted() {
        doTestCoercionOfNumerics(numericsToCast(), false, true);
    }

    /** */
    @Test
    public void testCoercionOfNumericDynamicParameters() {
        doTestCoercionOfNumerics(numericsToCast(), true, false);
    }

    /** */
    @Test
    public void testCoercionOfNumericDynamicParametersPrecasted() {
        doTestCoercionOfNumerics(numericsToCast(), true, true);
    }

    /** */
    private void doTestCoercionOfNumerics(List<List<Object>> testSuite, boolean dynamic, boolean precasted) {
        assumeNoTransactions();

        for (List<Object> params : testSuite) {
            assert params.size() == 4 : "Wrong params lenght: " + params.size();

            String inputType = params.get(0).toString();
            Object inputVal = params.get(1);
            String targetType = params.get(2).toString();
            Object expectedRes = params.get(3);

            log.info("Params: inputType=" + inputType + ", inputValue=" + inputVal + ", targetType=" + targetType
                + ", expectedResult=" + expectedRes);

            if (dynamic) {
                String qry = precasted
                    ? String.format("SELECT CAST(?::%s AS %s)", inputType, targetType)
                    : String.format("SELECT CAST(? AS %s)", targetType);

                if (expectedRes instanceof Exception)
                    assertThrows(qry, (Class<? extends Exception>)expectedRes.getClass(), ((Throwable)expectedRes).getMessage(), inputVal);
                else
                    assertQuery(qry).withParams(inputVal).returns(expectedRes).check();
            }
            else {
                String qry = precasted
                    ? String.format("SELECT CAST(%s::%s AS %s)", asLiteral(inputVal, inputType), inputType, targetType)
                    : String.format("SELECT CAST(%s AS %s)", asLiteral(inputVal, inputType), targetType);

                if (expectedRes instanceof Exception)
                    assertThrows(qry, (Class<? extends Exception>)expectedRes.getClass(), ((Throwable)expectedRes).getMessage());
                else
                    assertQuery(qry).returns(expectedRes).check();
            }
        }
    }

    /** */
    private static String asLiteral(Object val, String type) {
        return "VARCHAR".equalsIgnoreCase(type) ? String.format("'%s'", val) : String.valueOf(val);
    }

    /** @return input type, input value, target type, expected result. */
    private static List<List<Object>> numericsToCast() {
        Exception overflowErr = new IllegalArgumentException(IgniteSqlFunctions.NUMERIC_OVERFLOW_ERROR);
        Exception numFormatErr = new NumberFormatException("is neither a decimal digit number");

        //noinspection RedundantTypeArguments (explicit type arguments speedup compilation and analysis time)
        return F.<List<Object>>asList(
            // String
            F.asList("VARCHAR", "100", "DECIMAL(3)", new BigDecimal("100")),
            F.asList("VARCHAR", "100", "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("VARCHAR", "100", "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("VARCHAR", "100.12", "DECIMAL(5, 1)", new BigDecimal("100.1")),
            F.asList("VARCHAR", "100.16", "DECIMAL(5, 1)", new BigDecimal("100.2")),
            F.asList("VARCHAR", "-100.16", "DECIMAL(5, 1)", new BigDecimal("-100.2")),
            F.asList("VARCHAR", "lame", "DECIMAL(5, 1)", numFormatErr),
            F.asList("VARCHAR", "12345", "DECIMAL(5, 1)", overflowErr),
            F.asList("VARCHAR", "1234", "DECIMAL(5, 1)", new BigDecimal("1234.0")),
            F.asList("VARCHAR", "100.12", "DECIMAL(1, 0)", overflowErr),
            F.asList("VARCHAR", "100", "DECIMAL(2, 0)", overflowErr),

            // Numeric
            F.asList("DECIMAL(1, 1)", "0.1", "DECIMAL(1, 1)", new BigDecimal("0.1")),
            F.asList("DECIMAL(3)", "100", "DECIMAL(3)", new BigDecimal("100")),
            F.asList("DECIMAL(5, 2)", "100.16", "DECIMAL(4, 1)", new BigDecimal("100.2")),
            F.asList("DECIMAL(5, 2)", "-100.16", "DECIMAL(4, 1)", new BigDecimal("-100.2")),
            F.asList("DECIMAL(5, 2)", "100.16", "DECIMAL(5, 2)", new BigDecimal("100.16")),
            F.asList("DECIMAL(5, 2)", "-100.16", "DECIMAL(5, 2)", new BigDecimal("-100.16")),
            F.asList("DECIMAL(3)", "100", "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("DECIMAL(3)", "100", "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("DECIMAL(3)", "100", "DECIMAL(2, 0)", overflowErr),
            F.asList("DECIMAL(1, 1)", "0.1", "DECIMAL(2, 2)", new BigDecimal("0.10")),
            F.asList("DECIMAL(4, 2)", "10.12", "DECIMAL(2, 1)", overflowErr),
            F.asList("DECIMAL(2, 2)", "0.12", "DECIMAL(1, 2)", overflowErr),
            F.asList("DECIMAL(1, 1)", "0.1", "DECIMAL(1, 1)", new BigDecimal("0.1")),

            // Tinyint
            F.asList("TINYINT", (byte)100, "DECIMAL(3)", new BigDecimal("100")),
            F.asList("TINYINT", (byte)100, "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("TINYINT", (byte)100, "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("TINYINT", (byte)100, "DECIMAL(2, 0)", overflowErr),

            // Smallint
            F.asList("SMALLINT", (short)100, "DECIMAL(3)", new BigDecimal("100")),
            F.asList("SMALLINT", (short)100, "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("SMALLINT", (short)100, "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("SMALLINT", (short)100, "DECIMAL(2, 0)", overflowErr),

            // Integer
            F.asList("INTEGER", 100, "DECIMAL(3)", new BigDecimal("100")),
            F.asList("INTEGER", 100, "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("INTEGER", 100, "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("INTEGER", 100, "DECIMAL(2, 0)", overflowErr),

            // Bigint
            F.asList("BIGINT", 100L, "DECIMAL(3)", new BigDecimal("100")),
            F.asList("BIGINT", 100L, "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("BIGINT", 100L, "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("BIGINT", 100L, "DECIMAL(2, 0)", overflowErr),

            // Real
            F.asList("REAL", 100.0f, "DECIMAL(3)", new BigDecimal("100")),
            F.asList("REAL", 100.0f, "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("REAL", 100.0f, "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("REAL", 100.0f, "DECIMAL(2, 0)", overflowErr),
            F.asList("REAL", 0.1f, "DECIMAL(1, 1)", new BigDecimal("0.1")),
            F.asList("REAL", 0.1f, "DECIMAL(2, 2)", new BigDecimal("0.10")),
            F.asList("REAL", 10.12f, "DECIMAL(2, 1)", overflowErr),
            F.asList("REAL", 0.12f, "DECIMAL(1, 2)", overflowErr),

            // Double
            F.asList("DOUBLE", 100.0d, "DECIMAL(3)", new BigDecimal("100")),
            F.asList("DOUBLE", 100.0d, "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("DOUBLE", 100.0d, "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("DOUBLE", 100.0d, "DECIMAL(2, 0)", overflowErr),
            F.asList("DOUBLE", 0.1d, "DECIMAL(1, 1)", new BigDecimal("0.1")),
            F.asList("DOUBLE", 0.1d, "DECIMAL(2, 2)", new BigDecimal("0.10")),
            F.asList("DOUBLE", 10.12d, "DECIMAL(2, 1)", overflowErr),
            F.asList("DOUBLE", 0.12d, "DECIMAL(1, 2)", overflowErr),

            // Decimal
            F.asList("DECIMAL(1, 1)", new BigDecimal("0.1"), "DECIMAL(1, 1)", new BigDecimal("0.1")),
            F.asList("DECIMAL(3)", new BigDecimal("100"), "DECIMAL(3)", new BigDecimal("100")),
            F.asList("DECIMAL(3)", new BigDecimal("100"), "DECIMAL(3, 0)", new BigDecimal("100")),
            F.asList("DECIMAL(3)", new BigDecimal("100"), "DECIMAL(4, 1)", new BigDecimal("100.0")),
            F.asList("DECIMAL(3)", new BigDecimal("100"), "DECIMAL(2, 0)", overflowErr),
            F.asList("DECIMAL(1, 1)", new BigDecimal("0.1"), "DECIMAL(2, 2)", new BigDecimal("0.10")),
            F.asList("DECIMAL(4, 2)", new BigDecimal("10.12"), "DECIMAL(2, 1)", overflowErr),
            F.asList("DECIMAL(2, 2)", new BigDecimal("0.12"), "DECIMAL(1, 2)", overflowErr),
            F.asList("DECIMAL(1, 1)", new BigDecimal("0.1"), "DECIMAL(1, 1)", new BigDecimal("0.1"))
        );
    }

    /** */
    @Test
    public void testFunctionArgsToNumericImplicitConversion() {
        assumeNoTransactions();

        assertQuery("select decode(?, 0, 0, 1, 1.0)").withParams(0).returns(new BigDecimal("0.0")).check();
        assertQuery("select decode(?, 0, 0, 1, 1.0)").withParams(1).returns(new BigDecimal("1.0")).check();
        assertQuery("select decode(?, 0, 0, 1, 1.000)").withParams(0).returns(new BigDecimal("0.000")).check();
        assertQuery("select decode(?, 0, 0, 1, 1.000)").withParams(1).returns(new BigDecimal("1.000")).check();
        assertQuery("select decode(?, 0, 0.0, 1, 1.000)").withParams(0).returns(new BigDecimal("0.000")).check();
        assertQuery("select decode(?, 0, 0.000, 1, 1.0)").withParams(1).returns(new BigDecimal("1.000")).check();

        // With callRewrite==true function COALESCE is rewritten to CASE and CoalesceImplementor can't be checked.
        FrameworkConfig frameworkCfg = Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
            .sqlValidatorConfig(FRAMEWORK_CONFIG.getSqlValidatorConfig().withCallRewrite(false))
            .build();

        assertQuery("select coalesce(?, 1.000)").withParams(0).withFrameworkConfig(frameworkCfg)
            .returns(new BigDecimal("0.000")).check();
    }

    /** */
    @Test
    public void testArithmeticOverflow() {
        // BIGINT
        assertThrows("select CAST(9223372036854775807.5 + 1 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("select 9223372036854775807 + 1", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("select 9223372036854775807 * 2", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("select -9223372036854775808 - 1", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("select -(-9223372036854775807 - 1)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("select -CAST(-9223372036854775808 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("select -(?)", IgniteSQLException.class, "BIGINT overflow", -9223372036854775808L);
        assertThrows("select -9223372036854775808/-1", IgniteSQLException.class, "BIGINT overflow");

        // INTEGER
        assertThrows("select CAST(CAST(3000000000.0 + 1 AS DOUBLE) AS INTEGER)",
            IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select CAST(9223372036854775807.5 + 9223372036854775807.5 AS INTEGER)",
            IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select CAST(2147483647.5 + 1 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select 2147483647 + 1", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select 2147483647 * 2", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select -2147483648 - 1", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select -(-2147483647 - 1)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select -CAST(-2147483648 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("select -(?)", IgniteSQLException.class, "INTEGER overflow", -2147483648);
        assertThrows("select -2147483648/-1", IgniteSQLException.class, "INTEGER overflow");

        // SMALLINT
        assertThrows("select CAST(CAST(90000.0 + 1 AS FLOAT) AS SMALLINT)",
            IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select CAST(9223372036854775807.5 + 9223372036854775807.5 AS SMALLINT)",
            IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select 32000::smallint + 1000::smallint", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select 17000::smallint * 2::smallint", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select -32000::smallint - 1000::smallint", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select -(-32767::smallint - 1::smallint)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select -CAST(-32768 AS smallint)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("select -CAST(? AS smallint)", IgniteSQLException.class, "SMALLINT overflow", -32768);
        assertThrows("select -32768::smallint/-1::smallint", IgniteSQLException.class, "SMALLINT overflow");

        // TINYINT
        assertThrows("select CAST(CAST(200.0 + 1 AS FLOAT) AS TINYINT)",
            IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select CAST(9223372036854775807.5 + 9223372036854775807.5 AS TINYINT)",
            IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select 2::tinyint + 127::tinyint", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select 2::tinyint * 127::tinyint", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select -2::tinyint - 127::tinyint", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select -(-127::tinyint - 1::tinyint)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select -CAST(-128 AS tinyint)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("select -CAST(? AS tinyint)", IgniteSQLException.class, "TINYINT overflow", -128);
        assertThrows("select -128::tinyint/-1::tinyint", IgniteSQLException.class, "TINYINT overflow");
    }

    /** */
    @Test
    public void testCastDecimalOverflows() {
        assumeNoTransactions();

        // BIGINT
        assertQuery("SELECT CAST(9223372036854775807.1 AS BIGINT)").returns(9223372036854775807L).check();
        assertQuery("SELECT CAST(9223372036854775807.4 AS BIGINT)").returns(9223372036854775807L).check();
        assertQuery("SELECT CAST(9223372036854775806.9 AS BIGINT)").returns(9223372036854775807L).check();
        assertThrows("SELECT CAST(9223372036854775807.5 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST(9223372036854775807.9 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertQuery("SELECT CAST(9223372036854775807.9 - 1 AS BIGINT)").returns(9223372036854775807L).check();
        assertThrows("SELECT CAST(9223372036854775808 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST(9223372036854775808.1 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST(-9223372036854775809 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST(-9223372036854775809.1 AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertQuery("SELECT CAST(-9223372036854775808.1 AS BIGINT)").returns(-9223372036854775808L).check();
        assertQuery("SELECT CAST(-9223372036854775807.9 AS BIGINT)").returns(-9223372036854775808L).check();
        assertQuery("SELECT CAST(-9223372036854775808.4 AS BIGINT)").returns(-9223372036854775808L).check();
        assertQuery("SELECT CAST(-9223372036854775808.9 + 1 AS BIGINT)").returns(-9223372036854775808L).check();
        assertQuery("SELECT CAST('9223372036854775807.1' AS BIGINT)").returns(9223372036854775807L).check();
        assertQuery("SELECT CAST('9223372036854775807.4' AS BIGINT)").returns(9223372036854775807L).check();
        assertThrows("SELECT CAST('9223372036854775807.5' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST('9223372036854775807.9' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST('9223372036854775808' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST('9223372036854775808.1' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST('-9223372036854775809' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertThrows("SELECT CAST('-9223372036854775809.1' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");
        assertQuery("SELECT CAST('-9223372036854775808.1' AS BIGINT)").returns(-9223372036854775808L).check();
        assertThrows("SELECT CAST('-9223372036854775808.9' AS BIGINT)", IgniteSQLException.class, "BIGINT overflow");

        // INTEGER
        assertQuery("SELECT CAST(2147483647.4 AS INTEGER)").returns(2147483647).check();
        assertThrows("SELECT CAST(2147483647.5 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(2147483647.9 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(2147483648.5 - 1 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(2147483648.9 - 1 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertQuery("SELECT CAST(2147483648.4 - 1 AS INTEGER)").returns(2147483647).check();
        assertQuery("SELECT CAST(2147483647.9 - 1 AS INTEGER)").returns(2147483647).check();
        assertThrows("SELECT CAST(2147483648 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(2147483648.1 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(-2147483649 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(-2147483649.1 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertQuery("SELECT CAST(-2147483648.1 AS INTEGER)").returns(-2147483648).check();
        assertQuery("SELECT CAST(-2147483648.4 AS INTEGER)").returns(-2147483648).check();
        assertThrows("SELECT CAST(-2147483648.5 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST(-2147483648.9 AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertQuery("SELECT CAST('2147483647.1' AS INTEGER)").returns(2147483647).check();
        assertQuery("SELECT CAST('2147483647.4' AS INTEGER)").returns(2147483647).check();
        assertThrows("SELECT CAST('2147483647.5' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('2147483647.9' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('2147483648' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('2147483648.1' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('2147483648.4' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('2147483648.5' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('-2147483649' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('-2147483649.4' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertQuery("SELECT CAST('-2147483648.1' AS INTEGER)").returns(-2147483648).check();
        assertQuery("SELECT CAST('-2147483648.4' AS INTEGER)").returns(-2147483648).check();
        assertThrows("SELECT CAST('-2147483648.5' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT CAST('-2147483648.9' AS INTEGER)", IgniteSQLException.class, "INTEGER overflow");

        // SMALLINT
        assertQuery("SELECT CAST(32767.1 AS SMALLINT)").returns((short)32767).check();
        assertQuery("SELECT CAST(32767.4 AS SMALLINT)").returns((short)32767).check();
        assertThrows("SELECT CAST(32767.5 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(32767.9 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertQuery("SELECT CAST(32767.9 - 1 AS SMALLINT)").returns((short)32767).check();
        assertQuery("SELECT CAST(32768.4 - 1 AS SMALLINT)").returns((short)32767).check();
        assertThrows("SELECT CAST(32768.5 - 1 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(32768.9 - 1 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(32768 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(32768.1 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(-32769 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(-32769.1 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertQuery("SELECT CAST(-32768.1 AS SMALLINT)").returns((short)-32768).check();
        assertQuery("SELECT CAST(-32768.4 AS SMALLINT)").returns((short)-32768).check();
        assertThrows("SELECT CAST(-32768.5 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST(-32768.9 AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertQuery("SELECT CAST('32767.1' AS SMALLINT)").returns((short)32767).check();
        assertQuery("SELECT CAST('32767.4' AS SMALLINT)").returns((short)32767).check();
        assertThrows("SELECT CAST('32767.9' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST('32768' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST('32768.1' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST('-32769' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST('-32769.1' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertQuery("SELECT CAST('-32768.1' AS SMALLINT)").returns((short)-32768).check();
        assertQuery("SELECT CAST('-32768.4' AS SMALLINT)").returns((short)-32768).check();
        assertThrows("SELECT CAST('-32768.5' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");
        assertThrows("SELECT CAST('-32768.9' AS SMALLINT)", IgniteSQLException.class, "SMALLINT overflow");

        // TINYINT
        assertQuery("SELECT CAST(127.1 AS TINYINT)").returns((byte)127).check();
        assertQuery("SELECT CAST(127.4 AS TINYINT)").returns((byte)127).check();
        assertThrows("SELECT CAST(127.5 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(127.9 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertQuery("SELECT CAST(127.9 - 1 AS TINYINT)").returns((byte)127).check();
        assertQuery("SELECT CAST(128.4 - 1 AS TINYINT)").returns((byte)127).check();
        assertThrows("SELECT CAST(128.9 - 1 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(128.5 - 1 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(128 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(128.1 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(-129 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(-129.1 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertQuery("SELECT CAST(-128.1 AS TINYINT)").returns((byte)-128).check();
        assertQuery("SELECT CAST(-128.4 AS TINYINT)").returns((byte)-128).check();
        assertThrows("SELECT CAST(-128.5 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST(-128.9 AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertQuery("SELECT CAST('127.1' AS TINYINT)").returns((byte)127).check();
        assertQuery("SELECT CAST('127.4' AS TINYINT)").returns((byte)127).check();
        assertThrows("SELECT CAST('127.5' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST('127.9' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST('128' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST('128.1' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST('-129' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST('-129.1' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertQuery("SELECT CAST('-128.1' AS TINYINT)").returns((byte)-128).check();
        assertQuery("SELECT CAST('-128.4' AS TINYINT)").returns((byte)-128).check();
        assertThrows("SELECT CAST('-128.5' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
        assertThrows("SELECT CAST('-128.9' AS TINYINT)", IgniteSQLException.class, "TINYINT overflow");
    }

    /** */
    private void assumeNoTransactions() {
        assumeTrue("Test use queries that doesn't touch any data. Skip for tx modes", sqlTxMode == SqlTransactionMode.NONE);
    }
}
