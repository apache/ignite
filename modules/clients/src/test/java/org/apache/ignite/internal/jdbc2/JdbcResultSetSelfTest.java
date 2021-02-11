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

package org.apache.ignite.internal.jdbc2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.jdbc.JdbcResultSetSelfTest.assertEqualsToStringRepresentation;

/**
 * Result set test.
 */
@SuppressWarnings("FloatingPointEquality")
public class JdbcResultSetSelfTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "cache=default@modules/clients/src/test/config/jdbc-config.xml";

    /** SQL query. */
    private static final String SQL =
        "select id, boolVal, byteVal, shortVal, intVal, longVal, floatVal, " +
            "doubleVal, bigVal, strVal, arrVal, dateVal, timeVal, tsVal, urlVal, f1, f2, f3, _val, " +
            "boolVal2, boolVal3, boolVal4 " +
            "from TestObject where id = 1";

    /** Statement. */
    private Statement stmt;

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

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        IgniteCache<Integer, TestObject> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assert cache != null;

        TestObject o = createObjectWithData(1);

        cache.put(1, o);
        cache.put(2, new TestObject(2));
        cache.put(3, new TestObject(3));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stmt = DriverManager.getConnection(BASE_URL).createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null) {
            stmt.getConnection().close();
            stmt.close();

            assert stmt.isClosed();
        }
    }

    /**
     * @param id ID.
     * @return Object.
     * @throws MalformedURLException If URL in incorrect.
     */
    @SuppressWarnings("deprecation")
    private TestObject createObjectWithData(int id) throws MalformedURLException {
        TestObject o = new TestObject(id);

        o.boolVal = true;
        o.boolVal2 = true;
        o.boolVal3 = true;
        o.boolVal4 = true;
        o.byteVal = 1;
        o.shortVal = 1;
        o.intVal = 1;
        o.longVal = 1L;
        o.floatVal = 1.0f;
        o.doubleVal = 1.0d;
        o.bigVal = new BigDecimal(1);
        o.strVal = "1";
        o.arrVal = new byte[] {1};
        o.dateVal = new Date(1, 1, 1);
        o.timeVal = new Time(1, 1, 1);
        o.tsVal = new Timestamp(1);
        o.urlVal = new URL("http://abc.com/");

        return o;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getBoolean("boolVal");
                assert rs.getBoolean(2);
                assert rs.getByte(2) == 1;
                assert rs.getInt(2) == 1;
                assert rs.getShort(2) == 1;
                assert rs.getLong(2) == 1;
                assert rs.getDouble(2) == 1.0;
                assert rs.getFloat(2) == 1.0f;
                assert rs.getBigDecimal(2).equals(new BigDecimal(1));
                assert rs.getString(2).equals("true");

                assert rs.getObject(2, Boolean.class);
                assert rs.getObject(2, Byte.class) == 1;
                assert rs.getObject(2, Short.class) == 1;
                assert rs.getObject(2, Integer.class) == 1;
                assert rs.getObject(2, Long.class) == 1;
                assert rs.getObject(2, Float.class) == 1.f;
                assert rs.getObject(2, Double.class) == 1;
                assert rs.getObject(2, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(2, String.class).equals("true");
            }

            cnt++;
        }

        assert cnt == 1;

        ResultSet rs0 = stmt.executeQuery("select 1");

        assert rs0.next();
        assert rs0.getBoolean(1);

        rs0 = stmt.executeQuery("select 0");

        assert rs0.next();
        assert !rs0.getBoolean(1);

        rs0 = stmt.executeQuery("select '1'");

        assert rs0.next();
        assert rs0.getBoolean(1);

        rs0 = stmt.executeQuery("select '0'");

        assert rs0.next();
        assert !rs0.getBoolean(1);

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ResultSet rs0 = stmt.executeQuery("select ''");

                assert rs0.next();
                assert rs0.getBoolean(1);

                return null;
            }
        }, SQLException.class, "Cannot convert to boolean: ");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ResultSet rs0 = stmt.executeQuery("select 'qwe'");

                assert rs0.next();
                assert rs0.getBoolean(1);

                return null;
            }
        }, SQLException.class, "Cannot convert to boolean: qwe");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean2() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getBoolean("boolVal2");
                assert rs.getBoolean(20);
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean3() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getBoolean("boolVal3");
                assert rs.getBoolean(21);
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean4() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getBoolean("boolVal4");
                assert rs.getBoolean(22);
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByte() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getByte("byteVal") == 1;
                assert rs.getByte(3) == 1;

                assert rs.getBoolean(3);
                assert rs.getByte(3) == 1;
                assert rs.getInt(3) == 1;
                assert rs.getShort(3) == 1;
                assert rs.getLong(3) == 1;
                assert rs.getDouble(3) == 1.0;
                assert rs.getFloat(3) == 1.0f;
                assert rs.getBigDecimal(3).equals(new BigDecimal(1));
                assert rs.getString(3).equals("1");

                assert rs.getObject(3, Boolean.class);
                assert rs.getObject(3, Byte.class) == 1;
                assert rs.getObject(3, Short.class) == 1;
                assert rs.getObject(3, Integer.class) == 1;
                assert rs.getObject(3, Long.class) == 1;
                assert rs.getObject(3, Float.class) == 1.f;
                assert rs.getObject(3, Double.class) == 1;
                assert rs.getObject(3, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(3, String.class).equals("1");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShort() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getShort("shortVal") == 1;
                assert rs.getShort(4) == 1;

                assert rs.getBoolean(4);
                assert rs.getByte(4) == 1;
                assert rs.getShort(4) == 1;
                assert rs.getInt(4) == 1;
                assert rs.getLong(4) == 1;
                assert rs.getDouble(4) == 1.0;
                assert rs.getFloat(4) == 1.0f;
                assert rs.getBigDecimal(4).equals(new BigDecimal(1));
                assert rs.getString(4).equals("1");

                assert rs.getObject(4, Boolean.class);
                assert rs.getObject(4, Byte.class) == 1;
                assert rs.getObject(4, Short.class) == 1;
                assert rs.getObject(4, Integer.class) == 1;
                assert rs.getObject(4, Long.class) == 1;
                assert rs.getObject(4, Float.class) == 1.f;
                assert rs.getObject(4, Double.class) == 1;
                assert rs.getObject(4, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(4, String.class).equals("1");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInteger() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getInt("intVal") == 1;
                assert rs.getInt(5) == 1;

                assert rs.getBoolean(5);
                assert rs.getByte(5) == 1;
                assert rs.getShort(5) == 1;
                assert rs.getInt(5) == 1;
                assert rs.getLong(5) == 1;
                assert rs.getDouble(5) == 1.0;
                assert rs.getFloat(5) == 1.0f;
                assert rs.getBigDecimal(5).equals(new BigDecimal(1));
                assert rs.getString(5).equals("1");

                assert rs.getObject(5, Boolean.class);
                assert rs.getObject(5, Byte.class) == 1;
                assert rs.getObject(5, Short.class) == 1;
                assert rs.getObject(5, Integer.class) == 1;
                assert rs.getObject(5, Long.class) == 1;
                assert rs.getObject(5, Float.class) == 1.f;
                assert rs.getObject(5, Double.class) == 1;
                assert rs.getObject(5, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(5, String.class).equals("1");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLong() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getLong("longVal") == 1;
                assert rs.getLong(6) == 1;

                assert rs.getBoolean(6);
                assert rs.getByte(6) == 1;
                assert rs.getShort(6) == 1;
                assert rs.getInt(6) == 1;
                assert rs.getLong(6) == 1;
                assert rs.getDouble(6) == 1.0;
                assert rs.getFloat(6) == 1.0f;
                assert rs.getBigDecimal(6).equals(new BigDecimal(1));
                assert rs.getString(6).equals("1");

                assert rs.getObject(6, Boolean.class);
                assert rs.getObject(6, Byte.class) == 1;
                assert rs.getObject(6, Short.class) == 1;
                assert rs.getObject(6, Integer.class) == 1;
                assert rs.getObject(6, Long.class) == 1;
                assert rs.getObject(6, Float.class) == 1.f;
                assert rs.getObject(6, Double.class) == 1;
                assert rs.getObject(6, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(6, String.class).equals("1");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloat() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getFloat("floatVal") == 1.0;
                assert rs.getFloat(7) == 1.0;

                assert rs.getBoolean(7);
                assert rs.getByte(7) == 1;
                assert rs.getShort(7) == 1;
                assert rs.getInt(7) == 1;
                assert rs.getLong(7) == 1;
                assert rs.getDouble(7) == 1.0;
                assert rs.getFloat(7) == 1.0f;
                assert rs.getBigDecimal(7).equals(new BigDecimal(1));
                assert rs.getString(7).equals("1.0");

                assert rs.getObject(7, Boolean.class);
                assert rs.getObject(7, Byte.class) == 1;
                assert rs.getObject(7, Short.class) == 1;
                assert rs.getObject(7, Integer.class) == 1;
                assert rs.getObject(7, Long.class) == 1;
                assert rs.getObject(7, Float.class) == 1.f;
                assert rs.getObject(7, Double.class) == 1;
                assert rs.getObject(7, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(7, String.class).equals("1.0");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDouble() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getDouble("doubleVal") == 1.0;
                assert rs.getDouble(8) == 1.0;

                assert rs.getBoolean(8);
                assert rs.getByte(8) == 1;
                assert rs.getShort(8) == 1;
                assert rs.getInt(8) == 1;
                assert rs.getLong(8) == 1;
                assert rs.getDouble(8) == 1.0;
                assert rs.getFloat(8) == 1.0f;
                assert rs.getBigDecimal(8).equals(new BigDecimal(1));
                assert rs.getString(8).equals("1.0");

                assert rs.getObject(8, Boolean.class);
                assert rs.getObject(8, Byte.class) == 1;
                assert rs.getObject(8, Short.class) == 1;
                assert rs.getObject(8, Integer.class) == 1;
                assert rs.getObject(8, Long.class) == 1;
                assert rs.getObject(8, Float.class) == 1.f;
                assert rs.getObject(8, Double.class) == 1;
                assert rs.getObject(8, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(8, String.class).equals("1.0");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBigDecimal() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getBigDecimal("bigVal").intValue() == 1;
                assert rs.getBigDecimal(9).intValue() == 1;

                assert rs.getBoolean(9);
                assert rs.getByte(9) == 1;
                assert rs.getShort(9) == 1;
                assert rs.getInt(9) == 1;
                assert rs.getLong(9) == 1;
                assert rs.getDouble(9) == 1.0;
                assert rs.getFloat(9) == 1.0f;
                assert rs.getBigDecimal(9).equals(new BigDecimal(1));
                assert rs.getString(9).equals("1");

                assert rs.getObject(9, Boolean.class);
                assert rs.getObject(9, Byte.class) == 1;
                assert rs.getObject(9, Short.class) == 1;
                assert rs.getObject(9, Integer.class) == 1;
                assert rs.getObject(9, Long.class) == 1;
                assert rs.getObject(9, Float.class) == 1.f;
                assert rs.getObject(9, Double.class) == 1;
                assert rs.getObject(9, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(9, String.class).equals("1");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBigDecimalScale() throws Exception {
        assert "0.12".equals(convertStringToBigDecimalViaJdbc("0.1234", 2).toString());
        assert "1.001".equals(convertStringToBigDecimalViaJdbc("1.0005", 3).toString());
        assert "1E+3".equals(convertStringToBigDecimalViaJdbc("1205.5", -3).toString());
        assert "1.3E+4".equals(convertStringToBigDecimalViaJdbc("12505.5", -3).toString());
    }

    /**
     * @param strDec String representation of a decimal value.
     * @param scale Scale.
     * @return BigDecimal object.
     * @throws SQLException On error.
     */
    private BigDecimal convertStringToBigDecimalViaJdbc(String strDec, int scale) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select '" + strDec + "'")) {
            assert rs.next();

            return rs.getBigDecimal(1, scale);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testString() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert "1".equals(rs.getString("strVal"));

                assert rs.getBoolean(10);
                assert rs.getByte(10) == 1;
                assert rs.getShort(10) == 1;
                assert rs.getInt(10) == 1;
                assert rs.getLong(10) == 1;
                assert rs.getDouble(10) == 1.0;
                assert rs.getFloat(10) == 1.0f;
                assert rs.getBigDecimal(10).equals(new BigDecimal("1"));
                assert rs.getString(10).equals("1");

                assert rs.getObject(10, Boolean.class);
                assert rs.getObject(10, Byte.class) == 1;
                assert rs.getObject(10, Short.class) == 1;
                assert rs.getObject(10, Integer.class) == 1;
                assert rs.getObject(10, Long.class) == 1;
                assert rs.getObject(10, Float.class) == 1.f;
                assert rs.getObject(10, Double.class) == 1;
                assert rs.getObject(10, BigDecimal.class).equals(new BigDecimal(1));
                assert rs.getObject(10, String.class).equals("1");
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testArray() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert Arrays.equals(rs.getBytes("arrVal"), new byte[] {1});
                assert Arrays.equals(rs.getBytes(11), new byte[] {1});
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testDate() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getDate("dateVal").equals(new Date(1, 1, 1));

                assert rs.getDate(12).equals(new Date(1, 1, 1));
                assert rs.getTime(12).equals(new Time(new Date(1, 1, 1).getTime()));
                assert rs.getTimestamp(12).equals(new Timestamp(new Date(1, 1, 1).getTime()));

                assert rs.getObject(12, Date.class).equals(new Date(1, 1, 1));
                assert rs.getObject(12, Time.class).equals(new Time(new Date(1, 1, 1).getTime()));
                assert rs.getObject(12, Timestamp.class).equals(new Timestamp(new Date(1, 1, 1).getTime()));
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testTime() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getTime("timeVal").equals(new Time(1, 1, 1));

                assert rs.getDate(13).equals(new Date(new Time(1, 1, 1).getTime()));
                assert rs.getTime(13).equals(new Time(1, 1, 1));
                assert rs.getTimestamp(13).equals(new Timestamp(new Time(1, 1, 1).getTime()));

                assert rs.getObject(13, Date.class).equals(new Date(new Time(1, 1, 1).getTime()));
                assert rs.getObject(13, Time.class).equals(new Time(1, 1, 1));
                assert rs.getObject(13, Timestamp.class).equals(new Timestamp(new Time(1, 1, 1).getTime()));
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTimestamp() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getTimestamp("tsVal").getTime() == 1;

                assert rs.getDate(14).equals(new Date(new Timestamp(1).getTime()));
                assert rs.getTime(14).equals(new Time(new Timestamp(1).getTime()));
                assert rs.getTimestamp(14).equals(new Timestamp(1));

                assert rs.getObject(14, Date.class).equals(new Date(new Timestamp(1).getTime()));
                assert rs.getObject(14, Time.class).equals(new Time(new Timestamp(1).getTime()));
                assert rs.getObject(14, Timestamp.class).equals(new Timestamp(1));
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUrl() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assertTrue("http://abc.com/".equals(rs.getURL("urlVal").toString()));
                assertTrue("http://abc.com/".equals(rs.getURL(15).toString()));
            }

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObject() throws Exception {
        final Ignite ignite = ignite(0);
        final boolean binaryMarshaller = ignite.configuration().getMarshaller() instanceof BinaryMarshaller;
        final IgniteBinary binary = binaryMarshaller ? ignite.binary() : null;
        ResultSet rs = stmt.executeQuery(SQL);

        TestObjectField f1 = new TestObjectField(100, "AAAA");
        TestObjectField f2 = new TestObjectField(500, "BBBB");

        TestObject o = createObjectWithData(1);

        assertTrue(rs.next());

        assertEqualsToStringRepresentation(f1, binary, rs.getObject("f1"));
        assertEqualsToStringRepresentation(f1, binary, rs.getObject(16));

        assertEqualsToStringRepresentation(f2, binary, rs.getObject("f2"));
        assertEqualsToStringRepresentation(f2, binary, rs.getObject(17));

        assertNull(rs.getObject("f3"));
        assertTrue(rs.wasNull());
        assertNull(rs.getObject(18));
        assertTrue(rs.wasNull());

        assertEqualsToStringRepresentation(o, binary, rs.getObject("_val"));
        assertEqualsToStringRepresentation(o, binary, rs.getObject(19));

        assertFalse(rs.next());
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNavigation() throws Exception {
        ResultSet rs = stmt.executeQuery("select * from TestObject where id > 0");

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(0, rs.getRow());

        assertTrue(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(1, rs.getRow());

        assertTrue(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(2, rs.getRow());

        assertTrue(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertTrue(rs.isLast());
        assertEquals(3, rs.getRow());

        assertFalse(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(0, rs.getRow());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFetchSize() throws Exception {
        stmt.setFetchSize(1);

        ResultSet rs = stmt.executeQuery("select * from TestObject where id > 0");

        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());

        stmt.setFetchSize(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNewQueryTaskFetchSize() throws Exception {
        stmt.setFetchSize(1);

        boolean res = stmt.execute("select * from TestObject where id > 0");

        assertTrue(res);

        ResultSet rs = stmt.getResultSet();

        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());

        stmt.setFetchSize(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFindColumn() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assertNotNull(rs);
        assertTrue(rs.next());

        assert rs.findColumn("id") == 1;

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    rs.findColumn("wrong");

                    return null;
                }
            },
            SQLException.class,
            "Column not found: wrong"
        );
    }

    /**
     * Test object.
     */
    private static class BaseTestObject implements Serializable {
        /** */
        @QuerySqlField(index = false)
        protected Boolean boolVal2;

        /** */
        @QuerySqlField(index = false)
        protected boolean boolVal3;

        /** */
        @QuerySqlField(index = false)
        protected boolean boolVal4;

        /** */
        public boolean isBoolVal4() {
            return boolVal4;
        }
    }

    /**
     * Test object.
     */
    private static class TestObject extends BaseTestObject {
        /** */
        @QuerySqlField
        private final int id;

        /** */
        @QuerySqlField(index = false)
        private Boolean boolVal;

        /** */
        @QuerySqlField(index = false)
        private Byte byteVal;

        /** */
        @QuerySqlField(index = false)
        private Short shortVal;

        /** */
        @QuerySqlField(index = false)
        private Integer intVal;

        /** */
        @QuerySqlField(index = false)
        private Long longVal;

        /** */
        @QuerySqlField(index = false)
        private Float floatVal;

        /** */
        @QuerySqlField(index = false)
        private Double doubleVal;

        /** */
        @QuerySqlField(index = false)
        private BigDecimal bigVal;

        /** */
        @QuerySqlField(index = false)
        private String strVal;

        /** */
        @QuerySqlField(index = false)
        private byte[] arrVal;

        /** */
        @QuerySqlField(index = false)
        private Date dateVal;

        /** */
        @QuerySqlField(index = false)
        private Time timeVal;

        /** */
        @QuerySqlField(index = false)
        private Timestamp tsVal;

        /** */
        @QuerySqlField(index = false)
        private URL urlVal;

        /** */
        @QuerySqlField(index = false)
        private TestObjectField f1 = new TestObjectField(100, "AAAA");

        /** */
        @QuerySqlField(index = false)
        private TestObjectField f2 = new TestObjectField(500, "BBBB");

        /** */
        @QuerySqlField(index = false)
        private TestObjectField f3;

        /**
         * @param id ID.
         */
        private TestObject(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestObject.class, this);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BigDecimalEquals", "EqualsHashCodeCalledOnUrl", "RedundantIfStatement"})
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestObject that = (TestObject)o;

            if (id != that.id) return false;
            if (!Arrays.equals(arrVal, that.arrVal)) return false;
            if (bigVal != null ? !bigVal.equals(that.bigVal) : that.bigVal != null) return false;
            if (boolVal != null ? !boolVal.equals(that.boolVal) : that.boolVal != null) return false;
            if (byteVal != null ? !byteVal.equals(that.byteVal) : that.byteVal != null) return false;
            if (dateVal != null ? !dateVal.equals(that.dateVal) : that.dateVal != null) return false;
            if (doubleVal != null ? !doubleVal.equals(that.doubleVal) : that.doubleVal != null) return false;
            if (f1 != null ? !f1.equals(that.f1) : that.f1 != null) return false;
            if (f2 != null ? !f2.equals(that.f2) : that.f2 != null) return false;
            if (f3 != null ? !f3.equals(that.f3) : that.f3 != null) return false;
            if (floatVal != null ? !floatVal.equals(that.floatVal) : that.floatVal != null) return false;
            if (intVal != null ? !intVal.equals(that.intVal) : that.intVal != null) return false;
            if (longVal != null ? !longVal.equals(that.longVal) : that.longVal != null) return false;
            if (shortVal != null ? !shortVal.equals(that.shortVal) : that.shortVal != null) return false;
            if (strVal != null ? !strVal.equals(that.strVal) : that.strVal != null) return false;
            if (timeVal != null ? !timeVal.equals(that.timeVal) : that.timeVal != null) return false;
            if (tsVal != null ? !tsVal.equals(that.tsVal) : that.tsVal != null) return false;
            if (urlVal != null ? !urlVal.equals(that.urlVal) : that.urlVal != null) return false;

            return true;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("EqualsHashCodeCalledOnUrl")
        @Override public int hashCode() {
            int res = id;

            res = 31 * res + (boolVal != null ? boolVal.hashCode() : 0);
            res = 31 * res + (byteVal != null ? byteVal.hashCode() : 0);
            res = 31 * res + (shortVal != null ? shortVal.hashCode() : 0);
            res = 31 * res + (intVal != null ? intVal.hashCode() : 0);
            res = 31 * res + (longVal != null ? longVal.hashCode() : 0);
            res = 31 * res + (floatVal != null ? floatVal.hashCode() : 0);
            res = 31 * res + (doubleVal != null ? doubleVal.hashCode() : 0);
            res = 31 * res + (bigVal != null ? bigVal.hashCode() : 0);
            res = 31 * res + (strVal != null ? strVal.hashCode() : 0);
            res = 31 * res + (arrVal != null ? Arrays.hashCode(arrVal) : 0);
            res = 31 * res + (dateVal != null ? dateVal.hashCode() : 0);
            res = 31 * res + (timeVal != null ? timeVal.hashCode() : 0);
            res = 31 * res + (tsVal != null ? tsVal.hashCode() : 0);
            res = 31 * res + (urlVal != null ? urlVal.hashCode() : 0);
            res = 31 * res + (f1 != null ? f1.hashCode() : 0);
            res = 31 * res + (f2 != null ? f2.hashCode() : 0);
            res = 31 * res + (f3 != null ? f3.hashCode() : 0);

            return res;
        }
    }

    /**
     * Test object field.
     */
    @SuppressWarnings("PackageVisibleField")
    private static class TestObjectField implements Serializable {
        /** */
        @GridToStringInclude final int a;

        /** */
        @GridToStringInclude final String b;

        /**
         * @param a A.
         * @param b B.
         */
        private TestObjectField(int a, String b) {
            this.a = a;
            this.b = b;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestObjectField that = (TestObjectField)o;

            return a == that.a && !(b != null ? !b.equals(that.b) : that.b != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = a;

            res = 31 * res + (b != null ? b.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestObjectField.class, this);
        }
    }
}
