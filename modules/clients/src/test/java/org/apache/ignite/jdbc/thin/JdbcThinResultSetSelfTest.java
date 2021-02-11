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

package org.apache.ignite.jdbc.thin;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.RunnableX;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Result set test.
 */
@SuppressWarnings({"FloatingPointEquality", "ThrowableNotThrown", "AssertWithSideEffects"})
public class JdbcThinResultSetSelfTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** SQL query. */
    private static final String SQL =
        "select id, boolVal, byteVal, shortVal, intVal, longVal, floatVal, " +
            "doubleVal, bigVal, strVal, arrVal, dateVal, timeVal, tsVal, objVal " +
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

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);

        IgniteCache<Integer, TestObject> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assert cache != null;

        TestObject o = createObjectWithData(1);

        cache.put(1, o);
        cache.put(2, new TestObject(2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = DriverManager.getConnection(URL);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        stmt = conn.createStatement();

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

        assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ResultSet rs0 = stmt.executeQuery("select ''");

                assert rs0.next();
                assert rs0.getBoolean(1);

                return null;
            }
        }, SQLException.class, "Cannot convert to boolean: ");

        assertThrowsAnyCause(log, new Callable<Void>() {
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
    public void testByte() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assert rs.getByte("byteVal") == 1;

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
    public void testObject() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        TestObjectField exp = new TestObjectField(100, "AAAA");

        while (rs.next()) {
            if (cnt == 0) {
                Assert.assertEquals("Result by column label mismatch", exp, rs.getObject("objVal"));

                Assert.assertEquals("Result by column index mismatch", exp, rs.getObject(15));

                Assert.assertEquals("Result by column index with general cast mismatch",
                    exp, rs.getObject(15, Object.class));

                Assert.assertEquals("Result by column index with precise cast mismatch",
                    exp, rs.getObject(15, TestObjectField.class));
            }

            cnt++;
        }

        Assert.assertEquals("Result count mismatch", 1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNavigation() throws Exception {
        ResultSet rs = stmt.executeQuery("select id from TestObject where id > 0");

        assert rs.isBeforeFirst();
        assert !rs.isAfterLast();
        assert !rs.isFirst();
        assert !rs.isLast();
        assert rs.getRow() == 0;

        assert rs.next();

        assert !rs.isBeforeFirst();
        assert !rs.isAfterLast();
        assert rs.isFirst();
        assert !rs.isLast();
        assert rs.getRow() == 1;

        assert rs.next();

        assert !rs.isBeforeFirst();
        assert !rs.isAfterLast();
        assert !rs.isFirst();
        assert rs.isLast();
        assert rs.getRow() == 2;

        assert !rs.next();

        assert !rs.isBeforeFirst();
        assert rs.isAfterLast();
        assert !rs.isFirst();
        assert !rs.isLast();
        assert rs.getRow() == 0;

        rs = stmt.executeQuery("select id from TestObject where id < 0");

        assert !rs.isBeforeFirst();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFindColumn() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assert rs != null;
        assert rs.next();

        assert rs.findColumn("id") == 1;

        assertThrows(
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
     * @throws Exception If failed.
     */
    @Test
    public void testNotSupportedTypes() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assert rs.next();

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getArray(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getArray("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getAsciiStream(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getAsciiStream("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBinaryStream(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBinaryStream("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBlob(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBlob("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getClob(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getClob("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getCharacterStream(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getCharacterStream("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getNCharacterStream(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getNCharacterStream("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getNClob(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getNClob("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getRef(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getRef("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getRowId(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getRowId("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getSQLXML(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getSQLXML("id");
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateNotSupported() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assert rs.next();

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBoolean(1, true);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBoolean("id", true);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateByte(1, (byte)0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateByte("id", (byte)0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateShort(1, (short)0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateShort("id", (short)0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateInt(1, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateInt("id", 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateLong(1, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateLong("id", 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateFloat(1, (float)0.0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateFloat("id", (float)0.0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateDouble(1, 0.0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateDouble("id", 0.0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateString(1, "");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateString("id", "");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateTime(1, new Time(0));
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateTime("id", new Time(0));
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateDate(1, new Date(0));
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateDate("id", new Date(0));
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateTimestamp(1, new Timestamp(0));
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateTimestamp("id", new Timestamp(0));
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBytes(1, new byte[]{});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBytes("id", new byte[]{});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateArray(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateArray("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBlob(1, (Blob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBlob(1, (InputStream)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBlob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBlob("id", (Blob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBlob("id", (InputStream)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBlob("id", null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateClob(1, (Clob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateClob(1, (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateClob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateClob("id", (Clob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateClob("id", (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateClob("id", null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNClob(1, (NClob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNClob(1, (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNClob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNClob("id", (NClob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNClob("id", (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNClob("id", null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateAsciiStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateAsciiStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateAsciiStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateAsciiStream("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateAsciiStream("id", null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateAsciiStream("id", null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateCharacterStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateCharacterStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateCharacterStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateCharacterStream("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateCharacterStream("id", null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateCharacterStream("id", null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNCharacterStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNCharacterStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNCharacterStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNCharacterStream("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNCharacterStream("id", null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNCharacterStream("id", null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateRef(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateRef("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateRowId(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateRowId("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNString(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNString("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateSQLXML(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateSQLXML("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateObject(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateObject(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateObject("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateObject("id", null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBigDecimal(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateBigDecimal("id", null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNull(1);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateNull("id");
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.cancelRowUpdates();
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.updateRow();
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.deleteRow();
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.insertRow();
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.moveToInsertRow();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnClosedResultSet() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        rs.close();

        // Must do nothing on closed result set
        rs.close();

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBoolean(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBoolean("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getByte(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getByte("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getShort(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getShort("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getInt(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getInt("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getLong(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getLong("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getFloat(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getFloat("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getDouble(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getDouble("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getString(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getString("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBytes(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getBytes("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getDate(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getDate(1, new GregorianCalendar());
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getDate("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getDate("id", new GregorianCalendar());
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTime(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTime(1, new GregorianCalendar());
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTime("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTime("id", new GregorianCalendar());
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTimestamp(1);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTimestamp(1, new GregorianCalendar());
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTimestamp("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getTimestamp("id", new GregorianCalendar());
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getObject("objVal");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getObject("objVal", TestObjectField.class);
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.wasNull();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getMetaData();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.next();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.last();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.afterLast();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.beforeFirst();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.first();
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.findColumn("id");
            }
        });

        checkResultSetClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                rs.getRow();
            }
        });
    }

    /**
     * Test object.
     */
    private static class TestObject implements Serializable {
        /** */
        @QuerySqlField
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

        /** */
        @QuerySqlField
        private TestObjectField objVal = new TestObjectField(100, "AAAA");

        /** */
        @QuerySqlField
        private TestObjectField f2 = new TestObjectField(500, "BBBB");

        /** */
        @QuerySqlField
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
            if (objVal != null ? !objVal.equals(that.objVal) : that.objVal != null) return false;
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
            res = 31 * res + (objVal != null ? objVal.hashCode() : 0);
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
        final int a;

        /** */
        final String b;

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
