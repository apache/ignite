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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.jdbc2.JdbcBinaryBuffer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "true")
public class JdbcQueryTest extends GridCommonAbstractTest {
    /** URL. */
    private final String url = "jdbc:ignite:thin://127.0.0.1?queryEngine=" + CalciteQueryEngineConfiguration.ENGINE_NAME;

    /** Nodes count. */
    private final int nodesCnt = 3;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(nodesCnt);

        connect(url);

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** */
    private void connect(String url) throws Exception {
        if (stmt != null)
            stmt.close();

        if (conn != null)
            conn.close();

        conn = DriverManager.getConnection(url);
        conn.setSchema("PUBLIC");

        stmt = conn.createStatement();
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
     * @throws SQLException If failed.
     */
    @Test
    public void testOtherType() throws Exception {
        List<Object> values = new ArrayList<>();

        values.add("str");
        values.add(11);
        values.add(2);
        values.add(5);
        values.add(7);
        values.add(101.1);
        values.add(202.2d);
        values.add(new byte[] {1, 2, 3});
        values.add(UUID.randomUUID());
        values.add(new ObjectToStore(1, "noname", 22.2));

        Map<String, Object> map = new HashMap<>();
        map.put("a", "bb");
        map.put("vvv", "zzz");
        map.put("111", "222");
        map.put("lst", Stream.of("abc", 1, null, 20.f).collect(Collectors.toSet()));
        values.add(map);

        List<Object> lst = new ArrayList<>();
        lst.add(1);
        lst.add(2.2f);
        lst.add(3.3d);
        lst.add("str");
        lst.add(map);
        values.add(lst);

        stmt.execute("CREATE TABLE tbl(id INT, oth OTHER, primary key(id))");

        try (PreparedStatement ps = conn.prepareStatement("insert into tbl values(?, ?)")) {
            int idx = 1;

            for (Object obj : values) {
                ps.setInt(1, idx++);

                ps.setObject(2, obj);

                assertEquals(1, ps.executeUpdate());
            }
        }

        try (ResultSet rs = stmt.executeQuery("select * from tbl order by id")) {
            for (Object obj : values) {
                assertTrue(rs.next());

                if (F.isArray(obj))
                    F.compareArrays(obj, ((JdbcBinaryBuffer)rs.getObject(2)).getBytes());
                else
                    assertEquals(obj, rs.getObject(2));
            }
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

    /** Test batched execution of statement. */
    @Test
    public void testBatch() throws Exception {
        stmt.execute("CREATE TABLE Person(\"id\" INT, PRIMARY KEY(\"id\"), \"name\" VARCHAR)");

        for (int i = 0; i < 10; ++i)
            stmt.addBatch(String.format("INSERT INTO Person VALUES (%d, 'Name')", i));

        stmt.executeBatch();

        try (ResultSet rs = stmt.executeQuery("select * from Person p order by \"id\" asc")) {
            for (int i = 0; rs.next(); ++i) {
                assertEquals(i, rs.getInt(1));
                assertEquals("Name", rs.getString(2));
            }
        }
    }

    /** Test enforced join order parameter. */
    @Test
    public void testEnforcedJoinOrder() throws Exception {
        stmt.execute("CREATE TABLE Person1(\"ID\" INT, PRIMARY KEY(\"ID\"), \"NAME\" VARCHAR) WITH template=REPLICATED");
        stmt.execute("CREATE TABLE Person2(\"ID\" INT, PRIMARY KEY(\"ID\"), \"NAME\" VARCHAR) WITH template=REPLICATED");

        for (int i = 0; i < 3; ++i)
            stmt.execute(String.format("INSERT INTO Person1 VALUES (%d, 'Name')", i));

        for (int i = 0; i < 100; ++i)
            stmt.addBatch(String.format("INSERT INTO Person2 VALUES (%d, 'Name')", i));

        stmt.executeBatch();

        String scan1 = "Scan(table=[[PUBLIC, PERSON1]]";
        String scan2 = "Scan(table=[[PUBLIC, PERSON2]]";

        connect(url + "&enforceJoinOrder=true");

        try (ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR SELECT p2.Name from Person1 p1 LEFT JOIN Person2 " +
            "p2 on p2.NAME=p1.NAME")) {
            assertTrue(rs.next());

            String plan = rs.getString(1);

            // Joins as in the query.
            assertTrue(plan.indexOf(scan1) < plan.indexOf(scan2));

            // Join type is not changed.
            assertTrue(plan.contains("joinType=[left]"));
        }

        try (ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR SELECT /*+ NO_NL_JOIN */ " +
            "p2.Name from Person2 p2 RIGHT JOIN Person1 p1 on p2.NAME=p1.NAME")) {
            assertTrue(rs.next());

            String plan = rs.getString(1);

            // Joins as in the query.
            assertTrue(plan.indexOf(scan1) > plan.indexOf(scan2));

            // Join type is not changed.
            assertTrue(plan.contains("joinType=[right]"));
        }
    }

    /** Test batched execution of prepared statement. */
    @Test
    public void testBatchPrepared() throws Exception {
        stmt.execute("CREATE TABLE Person(\"id\" INT, PRIMARY KEY(\"id\"), \"name\" VARCHAR)");

        try (PreparedStatement stmt0 = conn.prepareStatement("INSERT INTO Person VALUES (?, ?);" +
            "INSERT INTO Person VALUES (?, ?)")) {
            stmt0.setInt(1, 0);
            stmt0.setString(2, "Name");
            stmt0.setInt(2, 1);
            stmt0.setString(4, "Name");
            stmt0.addBatch();

            GridTestUtils.assertThrows(log, stmt0::executeBatch, BatchUpdateException.class,
                "Multiline statements are not supported in batched query");
        }

        try (PreparedStatement stmt0 = conn.prepareStatement("SELECT * FROM Person WHERE id = ?")) {
            stmt0.setInt(1, 0);
            stmt0.addBatch();

            GridTestUtils.assertThrows(log, stmt0::executeBatch, BatchUpdateException.class,
                "Unexpected operation kind for batched query [kind=SELECT]");
        }

        int[] ret;
        try (PreparedStatement stmt0 = conn.prepareStatement("INSERT INTO Person VALUES (?, ?), (?, ?)")) {
            for (int i = 0; i < 1000; i += 2) {
                stmt0.setInt(1, i);
                stmt0.setString(2, "Name");
                stmt0.setInt(3, i + 1);
                stmt0.setString(4, "Name");
                stmt0.addBatch();
            }

            ret = stmt0.executeBatch();
        }

        try (ResultSet rs = stmt.executeQuery("select * from Person p order by \"id\" asc")) {
            int i = 0;
            for (; rs.next(); ++i) {
                assertEquals(i, rs.getInt(1));
                assertEquals("Name", rs.getString(2));
            }
            assertEquals(ret.length * 2, i);
            assertTrue(Arrays.stream(ret).anyMatch(k -> k == 2));
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testMultilineQuery() throws Exception {
        String multiLineQry = "CREATE TABLE test (val0 int primary key, val1 varchar);" +
            "INSERT INTO test(val0, val1) VALUES (0, 'test0');" +
            "ALTER TABLE test ADD COLUMN val2 int;" +
            "INSERT INTO test(val0, val1, val2) VALUES(1, 'test1', 10);" +
            "ALTER TABLE test DROP COLUMN val2;";
        stmt.execute(multiLineQry);

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
            "interval_ym_col INTERVAL YEAR TO MONTH, " +
            "interval_dt_col INTERVAL DAY TO SECONDS, " +
            "uuid_col UUID, " +
            "other_col OTHER, " +
            "binary_col BINARY, " +
            "PRIMARY KEY (id));");

        grid(0).context().cache().context().exchange().affinityReadyFuture(
            new AffinityTopologyVersion(3, 2)).get(10_000, TimeUnit.MILLISECONDS);

        stmt.executeUpdate("INSERT INTO t1 (id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
            "varchar_col, char_col, float_col, double_col, time_col, timestamp_col_14, timestamp_col_10, " +
            "timestamp_col_def, date_col, interval_ym_col, interval_dt_col, uuid_col, other_col, binary_col) " +
            "VALUES (1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, " +
            "null, null, null, null);");

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
            // Custom java types Period and Duration for intervals.
            assertEquals(Types.OTHER, md.getColumnType(16));
            assertEquals(Types.OTHER, md.getColumnType(17));

            assertEquals(Types.OTHER, md.getColumnType(18));
            assertEquals(UUID.class.getName(), md.getColumnClassName(18));

            assertEquals(Types.OTHER, md.getColumnType(19));
            assertEquals(Object.class.getName(), md.getColumnClassName(19));
        }

        stmt.execute("DROP TABLE t1");

        stmt.close();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testParametersMetadata() throws Exception {
        stmt.execute("CREATE TABLE Person(id BIGINT, PRIMARY KEY(id), name VARCHAR, amount DECIMAL(10,2))");

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO Person VALUES (?, ?, ?)")) {
            ParameterMetaData meta = stmt.getParameterMetaData();

            assertEquals(3, meta.getParameterCount(), 3);

            assertEquals(Long.class.getName(), meta.getParameterClassName(1));
            assertEquals(String.class.getName(), meta.getParameterClassName(2));
            assertEquals(BigDecimal.class.getName(), meta.getParameterClassName(3));

            assertEquals(JDBCType.valueOf(Types.BIGINT).getName(), meta.getParameterTypeName(1));
            assertEquals(JDBCType.valueOf(Types.VARCHAR).getName(), meta.getParameterTypeName(2));
            assertEquals(JDBCType.valueOf(Types.DECIMAL).getName(), meta.getParameterTypeName(3));

            assertEquals(Types.BIGINT, meta.getParameterType(1));
            assertEquals(Types.VARCHAR, meta.getParameterType(2));
            assertEquals(Types.DECIMAL, meta.getParameterType(3));

            assertEquals(19, meta.getPrecision(1));
            assertEquals(-1, meta.getPrecision(2));
            assertEquals(10, meta.getPrecision(3));

            assertEquals(0, meta.getScale(1));
            assertEquals(Integer.MIN_VALUE, meta.getScale(2));
            assertEquals(2, meta.getScale(3));

            assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(1));
            assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(2));
            assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(3));

            assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(1));
            assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(2));
            assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(3));

            assertTrue(meta.isSigned(1));
            assertTrue(meta.isSigned(2));
            assertTrue(meta.isSigned(3));
        }
    }

    /** Some object to store. */
    private static class ObjectToStore implements Serializable {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        private double val;

        /** */
        public ObjectToStore(int id, String name, double val) {
            this.id = id;
            this.name = name;
            this.val = val;
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectToStore store = (ObjectToStore)o;

            return id == store.id && val == store.val && Objects.equals(name, store.name);
        }

        /** */
        @Override public int hashCode() {
            return Objects.hash(id, name, val);
        }
    }
}
