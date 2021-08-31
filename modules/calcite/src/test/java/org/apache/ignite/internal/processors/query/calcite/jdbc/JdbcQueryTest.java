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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "true")
public class JdbcQueryTest extends GridCommonAbstractTest {
    /** URL. */
    private final String url = "jdbc:ignite:thin://127.0.0.1?useExperimentalQueryEngine=true";

    /** Nodes count. */
    private final int nodesCnt = 3;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(nodesCnt);
        conn = DriverManager.getConnection(url);
        conn.setSchema("PUBLIC");
        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();

        stopAllGrids();
    }

    /**
     * Checks bang equal is allowed and works.
     */
    @Test
    public void testBangEqual() throws Exception {
        stmt.execute("CREATE TABLE Person(id INT, salary INT, name VARCHAR, PRIMARY KEY(id))");

        stmt.executeUpdate("INSERT INTO Person VALUES (1, 1, 'test1')");
        stmt.executeUpdate("INSERT INTO Person VALUES (10, 10, 'test10')");
        stmt.executeUpdate("INSERT INTO Person VALUES (100, 100, 'test100')");

        try (ResultSet rs = stmt.executeQuery("SELECT * FROM Person WHERE id != 1")) {
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testSimpleQuery() throws Exception {
        stmt.execute("CREATE TABLE Person(\"id\" INT, PRIMARY KEY(\"id\"), \"name\" VARCHAR)");

        grid(0).context().cache().context().exchange().affinityReadyFuture(
            new AffinityTopologyVersion(3, 2)).get(10_000, TimeUnit.MILLISECONDS);

        stmt.executeUpdate("INSERT INTO Person VALUES (10, 'Name')");
        try (ResultSet rs = stmt.executeQuery("select p.*, (1+1) as synthetic from Person p")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals("Name", rs.getString(2));
            assertEquals(2, rs.getInt(3));
        }

        stmt.execute("alter table Person add column age int");

        stmt.execute("drop table Person");

        stmt.close();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testMultilineQuery() throws Exception {
        String multiLineQuery = "CREATE TABLE test (val0 int primary key, val1 varchar);" +
            "INSERT INTO test(val0, val1) VALUES (0, 'test0');" +
            "ALTER TABLE test ADD COLUMN val2 int;" +
            "INSERT INTO test(val0, val1, val2) VALUES(1, 'test1', 10);" +
            "ALTER TABLE test DROP COLUMN val2;";
        stmt.execute(multiLineQuery);

        try (ResultSet rs = stmt.executeQuery("select * from test order by val0")) {
            int i;
            for (i = 0; rs.next(); i++) {
                assertEquals(i, rs.getInt(1));
                assertEquals("test" + i, rs.getString(2));
            }
            assertEquals(2, i);
        }

        stmt.execute("drop table test");
        stmt.close();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testQueryColumnTypes() throws Exception {
        stmt.execute("CREATE TABLE t1 (id INT NOT NULL, " +
            "bool_col BOOLEAN, " +
            "tinyint_col TINYINT, " +
            "smallint_col SMALLINT, " +
            "int_col INT, " +
            "bigint_col BIGINT, " +
            "varchar_col VARCHAR, " +
            "char_col CHAR, " +
            "float_col FLOAT, " +
            "double_col DOUBLE, " +
            "time_col TIME, " +
            "timestamp_col_14 TIMESTAMP(14), " +
            "timestamp_col_10 TIMESTAMP(10), " +
            "timestamp_col_def TIMESTAMP, " +
            "date_col DATE, " +
            "PRIMARY KEY (id));");

        grid(0).context().cache().context().exchange().affinityReadyFuture(
            new AffinityTopologyVersion(3, 2)).get(10_000, TimeUnit.MILLISECONDS);

        stmt.executeUpdate("INSERT INTO t1 (id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
            "varchar_col, char_col, float_col, double_col, time_col, timestamp_col_14, timestamp_col_10, timestamp_col_def, date_col) " +
            "VALUES (1, null, null, null, null, null, null, null, null, null, null, null, null, null, null);");

        try (ResultSet rs = stmt.executeQuery("SELECT * FROM t1;")) {
            assertTrue(rs.next());

            ResultSetMetaData md = rs.getMetaData();

            // Columns start from 1.
            assertEquals(Types.INTEGER, md.getColumnType(1));
            assertEquals(Types.BOOLEAN, md.getColumnType(2));
            assertEquals(Types.TINYINT, md.getColumnType(3));
            assertEquals(Types.SMALLINT, md.getColumnType(4));
            assertEquals(Types.INTEGER, md.getColumnType(5));
            assertEquals(Types.BIGINT, md.getColumnType(6));
            assertEquals(Types.VARCHAR, md.getColumnType(7));
            assertEquals(Types.VARCHAR, md.getColumnType(8));
            assertEquals(Types.FLOAT, md.getColumnType(9));
            assertEquals(Types.DOUBLE, md.getColumnType(10));
            assertEquals(Types.TIME, md.getColumnType(11));
            assertEquals(Types.TIMESTAMP, md.getColumnType(12));
            assertEquals(Types.TIMESTAMP, md.getColumnType(13));
            assertEquals(Types.TIMESTAMP, md.getColumnType(14));
            assertEquals(Types.DATE, md.getColumnType(15));
        }

        stmt.execute("DROP TABLE t1");

        stmt.close();
    }
}
