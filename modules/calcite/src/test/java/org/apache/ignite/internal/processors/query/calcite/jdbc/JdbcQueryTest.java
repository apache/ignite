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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
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
    private final String url = "jdbc:ignite:thin://127.0.0.1";

    /** Nodes count. */
    private final int nodesCnt = 2;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration("cache-partitioned*")
                .setCacheMode(CacheMode.PARTITIONED)
                .setStatisticsEnabled(true)
                .setQueryParallelism(8)
                .setGroupName("group1")
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
        );

        long dsSize = 4L * 1024L * 1024L * 1024L;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalSegments(20)
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(false).setMetricsEnabled(true)
                    .setInitialSize(dsSize).setMaxSize(dsSize)
            )
        );

        cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
            new IndexingQueryEngineConfiguration().setDefault(true),
            new CalciteQueryEngineConfiguration().setDefault(false)
        ));

        return cfg;
    }

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

    /** {@inheritDoc} */
    @Test
    public void testSlowCalcite() throws Exception {
        long records = 1_000;
//        long records = 50_000;
//        long records = 100_000;
//        long records = 200_000;
//        long records = 500_000;
//        long records = 1_000_000;
//        long records = 2_000_000;

        ddl();
        fillDb(records, 100);

        long t;
        ResultSet rs;

        for (int i = 0; i < 10; ++i) {
//            t = System.nanoTime();
//            rs = stmt.executeQuery("select /*+ QUERY_ENGINE('h2')*/ count(*) from PI_COM_DAY");
//            t = System.nanoTime() -t;
//
//            assert rs.next();
//            assert rs.getLong(1) == records;
//
//            log.error("TEST | H2 timing: " + TimeUnit.NANOSECONDS.toMillis(t));

            t = System.nanoTime();
            rs = stmt.executeQuery("select /*+ QUERY_ENGINE('calcite')*/ count(KIND) from PI_COM_DAY");
            t = System.nanoTime() -t;

            assert rs.next();
            assert rs.getLong(1) == records;

            log.error("TEST | Calcite timing: " + TimeUnit.NANOSECONDS.toMillis(t));
        }
    }

    private void ddl() throws SQLException {
        stmt.execute("CREATE TABLE PI_COM_DAY (\n" +
            "    ITEM_ID VARCHAR(30) NOT NULL ,\n" +
            "    KIND VARCHAR(1) DEFAULT '',\n" +
            "    PRIMARY KEY (ITEM_ID)) WITH \"template=cache-partitioned,CACHE_NAME=PI_COM_DAY\";"
        );

        stmt.execute("CREATE INDEX IDX_PI_COM_DAY_ITEM_DATE ON PI_COM_DAY(KIND);");
    }

    private void fillDb(long recordNum, int batchSize) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO PI_COM_DAY(ITEM_ID) values (?)")){
            conn.setAutoCommit(false);

            for (long i = 0, batch = 0; i < recordNum; ++i) {
                ps.setString(1, "ITEM_ID_" + i);

                ps.addBatch();

                if(++batch == batchSize) {
                    ps.executeBatch();
                    batch = 0;
                    conn.commit();
                }
            }

            ps.executeBatch();
            conn.commit();
        }
        finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    @Override protected long getTestTimeout() {
        return 30 * 60 * 1000;
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
                    F.compareArrays(obj, rs.getObject(2));
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
