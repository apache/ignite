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

package org.apache.ignite.jdbc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Prepared statement test.
 */
public class JdbcPreparedStatementSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite://127.0.0.1/";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private PreparedStatement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            Integer.class, TestObject.class
        );

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        IgniteCache<Integer, TestObject> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assert cache != null;

        TestObject o = new TestObject(1);

        o.boolVal = true;
        o.byteVal = 1;
        o.shortVal = 1;
        o.intVal = 1;
        o.longVal = 1L;
        o.floatVal = 1.0f;
        o.doubleVal = 1.0d;
        o.bigVal = new BigDecimal(1);
        o.strVal = "str";
        o.arrVal = new byte[] {1};
        o.dateVal = new Date(1);
        o.timeVal = new Time(1);
        o.tsVal = new Timestamp(1);
        o.urlVal = new URL("http://abc.com/");

        cache.put(1, o);
        cache.put(2, new TestObject(2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        assert conn != null;
        assert !conn.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null) {
            stmt.close();

            assert stmt.isClosed();
        }

        if (conn != null) {
            conn.close();

            assert conn.isClosed();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableUsage() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where id = ?");

        stmt.setInt(1, 1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assertEquals(1, rs.getInt(1));

            cnt++;
        }

        assertEquals(1, cnt);

        cnt = 0;

        rs = stmt.executeQuery();

        while (rs.next()) {
            if (cnt == 0)
                assertEquals(1, rs.getInt(1));

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoolean() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where boolVal is not distinct from ?");

        stmt.setBoolean(1, true);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, BOOLEAN);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testByte() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where byteVal is not distinct from ?");

        stmt.setByte(1, (byte)1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, TINYINT);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testShort() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where shortVal is not distinct from ?");

        stmt.setShort(1, (short)1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, SMALLINT);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInteger() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where intVal is not distinct from ?");

        stmt.setInt(1, 1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, INTEGER);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLong() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where longVal is not distinct from ?");

        stmt.setLong(1, 1L);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, BIGINT);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloat() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where floatVal is not distinct from ?");

        stmt.setFloat(1, 1.0f);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, FLOAT);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDouble() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where doubleVal is not distinct from ?");

        stmt.setDouble(1, 1.0d);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, DOUBLE);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBigDecimal() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where bigVal is not distinct from ?");

        stmt.setBigDecimal(1, new BigDecimal(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, OTHER);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testString() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where strVal is not distinct from ?");

        stmt.setString(1, "str");

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, VARCHAR);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testArray() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where arrVal is not distinct from ?");

        stmt.setBytes(1, new byte[] {1});

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, BINARY);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDate() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where dateVal is not distinct from ?");

        stmt.setObject(1, new Date(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, DATE);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTime() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where timeVal is not distinct from ?");

        stmt.setTime(1, new Time(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, TIME);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimestamp() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where tsVal is not distinct from ?");

        stmt.setTimestamp(1, new Timestamp(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, TIMESTAMP);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUrl() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where urlVal is not distinct from ?");

        stmt.setURL(1, new URL("http://abc.com/"));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, DATALINK);

        rs = stmt.executeQuery();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * Test object.
     */
    private static class TestObject implements Serializable {
        /** */
        @QuerySqlField(index = false)
        private final int id;

        /** */
        @QuerySqlField
        private Boolean boolVal;

        /** */
        @QuerySqlField
        private Byte byteVal;

        /** */
        @QuerySqlField
        private Short shortVal;

        /** */
        @QuerySqlField
        private Integer intVal;

        /** */
        @QuerySqlField
        private Long longVal;

        /** */
        @QuerySqlField
        private Float floatVal;

        /** */
        @QuerySqlField
        private Double doubleVal;

        /** */
        @QuerySqlField
        private BigDecimal bigVal;

        /** */
        @QuerySqlField
        private String strVal;

        /** */
        @QuerySqlField
        private byte[] arrVal;

        /** */
        @QuerySqlField
        private Date dateVal;

        /** */
        @QuerySqlField
        private Time timeVal;

        /** */
        @QuerySqlField
        private Timestamp tsVal;

        /** */
        @QuerySqlField
        private URL urlVal;

        /**
         * @param id ID.
         */
        private TestObject(int id) {
            this.id = id;
        }
    }
}