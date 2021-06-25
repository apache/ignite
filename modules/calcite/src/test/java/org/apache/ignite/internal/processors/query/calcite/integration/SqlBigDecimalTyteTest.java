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
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class SqlBigDecimalTyteTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_NODE_NAME = "client";

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        client = startClientGrid(CLIENT_NODE_NAME);
    }

    /** */
    @Before
    public void init() {
        client = grid(CLIENT_NODE_NAME);
    }

    /** */
    @After
    public void cleanUp() {
        client.destroyCaches(client.cacheNames());
    }

    @Test
    public void test10() {
        final String insertAsSelectSql = "SELECT CAST(0.01 as DECIMAL) * 10";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        assertEquals(new BigDecimal("0.10"), rows.get(0).get(0));
    }

    @Test
    public void test1() {
        final String insertAsSelectSql = "SELECT " +
            "CAST(0.01 as DECIMAL) * 10," +
            " CAST(0.01 as DECIMAL(10,1)) * 10," +
            " CAST(0.01 as DECIMAL(10,3)) * 10";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        assertEquals(new BigDecimal("0.10"), rows.get(0).get(0));
        assertEquals(new BigDecimal("0.0"), rows.get(0).get(1));
        assertEquals(new BigDecimal("0.100"), rows.get(0).get(2));
    }

    @Test
    public void test2() {
        executeSql("CREATE TABLE test (val real)");
        executeSql("INSERT INTO test VALUES (0.01)");

        final String selectSql = "SELECT " +
            "CAST(val as DECIMAL) * 10," +
            " CAST(val as DECIMAL(10,1)) * 10," +
            " CAST(val as DECIMAL(10,3)) * 10 FROM test";

        List<List<?>> rows = executeSql(selectSql);

        System.out.println(rows.get(0));
        assertEquals(new BigDecimal("0.10"), rows.get(0).get(0));
        assertEquals(new BigDecimal("0.0"), rows.get(0).get(1));
        assertEquals(new BigDecimal("0.100"), rows.get(0).get(2));
    }

    @Test
    public void test21() {
        executeSql("CREATE TABLE test (val int)");
        executeSql("INSERT INTO test VALUES (10)");

        final String selectSql = "SELECT CAST(val as DECIMAL)," +
            " CAST(val as DECIMAL(10,1)) * 0.1," +
            " CAST(val as DECIMAL(10,2)) * 0.1," +
            " CAST(val as DECIMAL(10,3)) * 0.1," +
            " CAST(val as DECIMAL(10,4)) * 0.1 FROM test";

        List<List<?>> rows = executeSql(selectSql);

        System.out.println(rows.get(0));
        assertEquals(new BigDecimal("10.0"), rows.get(0).get(0));
        assertEquals(new BigDecimal("1.00"), rows.get(0).get(1));
        assertEquals(new BigDecimal("1.000"), rows.get(0).get(2));
        assertEquals(new BigDecimal("1.0000"), rows.get(0).get(3));
        assertEquals(new BigDecimal("1.00000"), rows.get(0).get(4));
    }

    @Test
    public void test22() {
        executeSql("CREATE TABLE test (val real)");
        executeSql("INSERT INTO test VALUES (0.01)");

        final String selectSql = "SELECT " +
            "CAST(val as DECIMAL) * 10," +
            " CAST(val as DECIMAL(10,1)) * 10," +
            " CAST(val as DECIMAL(10,3)) * 10 FROM test";

        List<List<?>> rows = executeSql(selectSql);

        System.out.println(rows.get(0));
        assertEquals(new BigDecimal("0.10"), rows.get(0).get(0));
        assertEquals(new BigDecimal("0.0"), rows.get(0).get(1));
        assertEquals(new BigDecimal("0.100"), rows.get(0).get(2));
    }
    @Test
    public void test23() {
        executeSql("CREATE TABLE test (val real)");
        executeSql("INSERT INTO test VALUES (0.01)");

        final String selectSql = "SELECT 10::INTEGER::DECIMAL(10,3)";

        List<List<?>> rows = executeSql(selectSql);

        System.out.println(rows.get(0));
        assertEquals(new BigDecimal("10.000"), rows.get(0).get(0));
    }

    @Test
    public void test3() {
        final String insertAsSelectSql = "SELECT 127::DECIMAL(3,0)::FLOAT, -7::DECIMAL(9,1)::FLOAT, 27::DECIMAL(18,1)::FLOAT, 33::DECIMAL(38,1)::FLOAT";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals(127.0f, rows.get(0).get(0));
        assertEquals(-7.0f, rows.get(0).get(1));
        assertEquals(27.0f, rows.get(0).get(2));
        assertEquals(33.0f, rows.get(0).get(3));
    }

    @Test
    public void test4() {
        final String insertAsSelectSql = "SELECT -17014118346046923173168730371588410572::DECIMAL(38,0)::FLOAT";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals(-1.7014119E37f, rows.get(0).get(0));
    }

    @Test
    public void test5() {
        final String insertAsSelectSql = "SELECT 100::FLOAT";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals(100.0f, rows.get(0).get(0));
    }

    @Test
    public void test51() {
        executeSql("CREATE TABLE test (val real)");
        executeSql("INSERT INTO test VALUES (100)");

        final String insertAsSelectSql = "SELECT val::FLOAT FROM test";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows);

        assertEquals(100.0f, rows.get(0).get(0));
    }

    @Test
    public void test6() {
        final String insertAsSelectSql = "SELECT '0.1'::DECIMAL(4,1)::VARCHAR";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals("0.1", rows.get(0).get(0));
    }

    @Test
    public void test62() {
        final String insertAsSelectSql = "SELECT '100.100'::DECIMAL(10,0)::VARCHAR";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals("100", rows.get(0).get(0));
    }

    @Test
    public void test621() {
        final String insertAsSelectSql = "SELECT '100.100'::DECIMAL::VARCHAR";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals("100.100", rows.get(0).get(0));
    }

    @Test
    public void test63() {
        final String insertAsSelectSql = "SELECT '100.100'::DECIMAL(10)::VARCHAR";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals("100", rows.get(0).get(0));
    }

    @Test
    public void test61() {
        executeSql("CREATE TABLE test (val varchar)");
        executeSql("INSERT INTO test VALUES ('0.1')");

        final String insertAsSelectSql = "SELECT val::DECIMAL(4,1)::VARCHAR FROM test";

        List<List<?>> rows = executeSql(insertAsSelectSql);

        System.out.println(rows.get(0));

        assertEquals("0.1", rows.get(0).get(0));
    }

    @Test
    public void test7() {
        final String insertAsSelectSql = "SELECT ROUND('100.3908147521'::DECIMAL(18,10), 20)::VARCHAR";

        List<List<?>> rows = executeSql(insertAsSelectSql);
        System.out.println(rows);

        assertEquals("100.39081475210000000000", rows.get(0).get(0));
    }

    @Test
    public void test8() {
        executeSql("CREATE TABLE decimals(d DECIMAL(3, 2));");
        executeSql("INSERT INTO decimals VALUES ('0.1'), ('0.2');");
        final String insertAsSelectSql = "SELECT d + '0.1'::DECIMAL, d + 10000 FROM decimals ORDER BY d ASC;";

        List<List<?>> rows = executeSql(insertAsSelectSql);
        System.out.println(rows);

        assertEquals(new BigDecimal("0.2"), rows.get(0).get(0));
        assertEquals(new BigDecimal("10000.1"), rows.get(0).get(1));
        assertEquals(new BigDecimal("0.3"), rows.get(1).get(0));
        assertEquals(new BigDecimal("10000.2"), rows.get(1).get(1));
    }

    @Test
    public void test9() {
        executeSql("CREATE TABLE decimals(d VARCHAR);");
        executeSql("INSERT INTO decimals VALUES ('0.1'), ('0.2');");
        executeSql("INSERT INTO decimals VALUES ('0.11'), ('0.21');");

        final String insertAsSelectSql = "DELETE FROM decimals WHERE d <> d::DECIMAL(9,1)::VARCHAR;";

        System.out.println(executeSql("EXPLAIN PLAN FOR " + insertAsSelectSql));
        executeSql(insertAsSelectSql);
        List<List<?>> rows = executeSql("SELECT * FROM decimals");
        System.out.println(rows);
    }

    @Test
    public void test90() {
        executeSql("CREATE TABLE decimals(d real);");
        executeSql("INSERT INTO decimals VALUES (0.1), (0.2);");
        executeSql("INSERT INTO decimals VALUES (0.11), (0.21);");

        final String insertAsSelectSql = "DELETE FROM decimals WHERE d <> d::DECIMAL(9,1);";

        List<List<?>> rows = executeSql(insertAsSelectSql);
        System.out.println(rows);

        assertEquals(4L, rows.get(0).get(0));
    }

    @Test
    public void test901() {
        executeSql("CREATE TABLE decimals(d DECIMAL);");
        executeSql("INSERT INTO decimals VALUES ('0.1'), ('0.2');");
        executeSql("INSERT INTO decimals VALUES ('0.11'), ('0.21');");

        final String insertAsSelectSql = "DELETE FROM decimals WHERE d <> d::DECIMAL(9,1);";
        System.out.println(executeSql("EXPLAIN PLAN FOR " + insertAsSelectSql));
        List<List<?>> rows = executeSql(insertAsSelectSql);
        System.out.println(rows);

        assertEquals(2L, rows.get(0).get(0));
    }

    @Test
    public void test91() {
        executeSql("CREATE TABLE decimals(d DECIMAL(3,2));");
        executeSql("INSERT INTO decimals VALUES ('0.1'), ('0.2');");
        executeSql("INSERT INTO decimals VALUES ('0.11'), ('0.21');");

        final String insertAsSelectSql = "SELECT * FROM decimals WHERE d = '0.1'::DECIMAL(9,1)";

        List<List<?>> rows = executeSql(insertAsSelectSql);
        System.out.println(rows);

        assertEquals(new BigDecimal("0.1"), rows.get(0).get(0));
    }

    @Test
    public void test11() {
        final String insertAsSelectSql = "select '0'::DECIMAL(38,10)::VARCHAR";

        List<List<?>> rows = executeSql(insertAsSelectSql);
        System.out.println(rows);

        assertEquals("0.0000000000", rows.get(0).get(0));
    }

    /** */
    private List<List<?>> executeSql(String sql, Object... args) {
        List<FieldsQueryCursor<List<?>>> cur = queryProcessor().query(null, "PUBLIC", sql, args);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    private CalciteQueryProcessor queryProcessor() {
        return Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
    }
}
