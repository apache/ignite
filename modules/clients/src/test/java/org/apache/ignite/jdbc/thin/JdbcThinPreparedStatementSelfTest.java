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
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcThinFeature;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Assert;
import org.junit.Test;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

/**
 * Prepared statement test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinPreparedStatementSelfTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** SQL query. */
    private static final String SQL_PART =
        "select id, boolVal, byteVal, shortVal, intVal, longVal, floatVal, " +
            "doubleVal, bigVal, strVal, arrVal, dateVal, timeVal, tsVal, objVal " +
            "from TestObject ";

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

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

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
        o.objVal = new TestObjectField(100, "AAAA");

        cache.put(1, o);
        cache.put(2, new TestObject(2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = createConnection(false);

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
     * Create new JDBC connection to the grid.
     *
     * @param keepBinary Whether to keep bin object in binary format.
     * @return New connection.
     */
    private Connection createConnection(boolean keepBinary) throws SQLException {
        String url = keepBinary ? URL + "?keepBinary=true" : URL;

        Connection conn = DriverManager.getConnection(url);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        return conn;
    }

    /**
     * Create new JDBC connection to the grid.
     *
     * @param disabledFeatues Features that should be disabled.
     * @return New connection.
     */
    private Connection createConnection(JdbcThinFeature... disabledFeatues) throws SQLException {
        String url = URL + "?disabledFeatures=" + Arrays.stream(disabledFeatues)
            .map(JdbcThinFeature::name)
            .collect(Collectors.joining(","));

        Connection conn = DriverManager.getConnection(url);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        return conn;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableUsage() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where id = ?");

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
     * Ensure binary object's meta is properly synchronized between connections
     *      - start grid
     *      - from one connection create and fill table such one of the columns was user's object
     *      - from another connection execute query with filter by this object
     *      - verify that result is not empty and returned object is the same as expected
     *
     * @throws SQLException In case of any sql error.
     */
    @Test
    public void testObjectDifferentConnections() throws SQLException {
        final TestObjectField exp = new TestObjectField(42, "BBBB");

        conn.createStatement().execute("CREATE TABLE test(id INT PRIMARY KEY, objVal OTHER)");

        stmt = conn.prepareStatement("INSERT INTO test(id, objVal) VALUES (?, ?)");

        stmt.setInt(1, exp.a);
        stmt.setObject(2, exp);

        stmt.execute();

        try (Connection anotherConn = createConnection(false);
             PreparedStatement stmt = anotherConn.prepareStatement("SELECT id, objVal FROM test WHERE id = ?")
        ) {
            stmt.setInt(1, exp.a);

            int cnt = 0;

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                if (cnt == 0) {
                    Assert.assertTrue("Result's value type mismatch",
                        rs.getObject("objVal") instanceof TestObjectField);

                    Assert.assertEquals("Result's value mismatch", exp, rs.getObject("objVal", TestObjectField.class));
                }

                cnt++;
            }

            Assert.assertEquals("There should be exactly 1 result", 1, cnt);
        }
    }

    /**
     * Ensure custom objects can be retrieved as {@link BinaryObject}
     * if keepBinary flag is set to {@code true} on connection
     *      - start grid and create and fill table such one of the columns was user's object
     *      - from another connection with keepBinary flag set to {@code true}
     *      execute query with filter by this object
     *      - verify that result is not empty and returned object is the {@link BinaryObject}
     *
     * @throws SQLException In case of any sql error.
     */
    @Test
    public void testObjectConnectionWithKeepBinaryFlag() throws SQLException {
        try (Connection anotherConn = createConnection(true)) {
            stmt = anotherConn.prepareStatement(SQL_PART + " where objVal is not distinct from ?");

            stmt.setObject(1, new TestObjectField(100, "AAAA"));

            int cnt = 0;

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                if (cnt == 0) {
                    Assert.assertEquals("Result's id mismatch", 1, rs.getInt("id"));

                    Assert.assertTrue(rs.getObject("objVal") instanceof BinaryObject);

                    Assert.assertEquals("Result's value mismatch", Integer.valueOf(100),
                        rs.getObject("objVal", BinaryObject.class).field("a"));
                }

                cnt++;
            }

            Assert.assertEquals("There should be exactly 1 result", 1, cnt);
        }
    }

    /**
     * Ensure custom objects can be retrieved through JdbcThinConnection
     *      - start grid and create and fill table such one of the columns was user's object
     *      - execute query with filter by this object (use both real object and null for param value)
     *      - verify that result is not empty and returned object is the same as expected
     *
     * @throws Exception If failed.
     */
    @Test
    public void testObject() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where objVal is not distinct from ?");

        stmt.setObject(1, new TestObjectField(100, "AAAA"));

        int cnt = 0;

        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            if (cnt == 0) {
                Assert.assertEquals("Result's id mismatch", 1, rs.getInt("id"));

                Assert.assertTrue("Result's value type mismatch",
                    rs.getObject("objVal") instanceof TestObjectField);

                Assert.assertEquals("Result's value mismatch", 100,
                    rs.getObject("objVal", TestObjectField.class).a);
            }

            cnt++;
        }

        Assert.assertEquals("There should be exactly 1 result", 1, cnt);

        stmt.setNull(1, Types.JAVA_OBJECT);

        stmt.execute();

        cnt = 0;

        rs = stmt.getResultSet();

        while (rs.next()) {
            if (cnt == 0) {
                Assert.assertEquals("Result's id mismatch", 2, rs.getInt("id"));

                Assert.assertNull("Result's value should be null", rs.getObject("objVal"));
            }

            cnt++;
        }

        Assert.assertEquals("There should be exactly 1 result", 1, cnt);
    }

    /**
     * Ensure custom object support could be disabled via disabledFeatures connection property
     *      - start grid and create and fill table such one of the columns was user's object
     *      - from another connection with disabledFeatures set to {@link JdbcThinFeature#CUSTOM_OBJECT}
     *      execute query with filter by this object
     *      - verify that exception is thrown when you try to set custom object as statement param
     * @throws SQLException
     */
    @Test
    public void testCustomObjectSupportCanBeDisabled() throws SQLException {
        try (Connection conn = createConnection(JdbcThinFeature.CUSTOM_OBJECT);
            PreparedStatement stmt = conn.prepareStatement(SQL_PART + " where objVal is not distinct from ?")
        ) {
            Throwable t = GridTestUtils.assertThrowsWithCause(
                new RunnableX() {
                    @Override public void runx() throws Exception {
                        stmt.setObject(1, new TestObjectField(100, "AAAA"));
                    }
                },
                SQLException.class
            );

            Assert.assertThat(t.getMessage(), is(containsString("Custom objects are not supported")));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryExecuteException() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where boolVal is not distinct from ?");

        stmt.setBoolean(1, true);

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeQuery("select 1");

                return null;
            }
        }, SQLException.class, "The method 'executeQuery(String)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.execute("select 1");

                return null;
            }
        }, SQLException.class, "The method 'execute(String)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.execute("select 1", Statement.NO_GENERATED_KEYS);

                return null;
            }
        }, SQLException.class, "The method 'execute(String)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeUpdate("select 1", Statement.NO_GENERATED_KEYS);

                return null;
            }
        }, SQLException.class, "The method 'executeUpdate(String, int)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeUpdate("select 1", new int[] {1});

                return null;
            }
        }, SQLException.class, "The method 'executeUpdate(String, int[])' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeUpdate("select 1 as a", new String[]{"a"});

                return null;
            }
        }, SQLException.class, "The method 'executeUpdate(String, String[])' is called on PreparedStatement instance.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where boolVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testByte() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where byteVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testShort() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where shortVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testInteger() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where intVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testLong() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where longVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testFloat() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where floatVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testDouble() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where doubleVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testBigDecimal() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where bigVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testString() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where strVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testArray() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where arrVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testDate() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where dateVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testTime() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where timeVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testTimestamp() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where tsVal is not distinct from ?");

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

        stmt.execute();

        rs = stmt.getResultSet();

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
    @Test
    public void testClearParameter() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where boolVal is not distinct from ?");

        stmt.setString(1, "");
        stmt.setLong(2, 1L);
        stmt.setInt(5, 1);

        stmt.clearParameters();

        stmt.setBoolean(1, true);

        ResultSet rs = stmt.executeQuery();

        boolean hasNext = rs.next();

        assert hasNext;

        assert rs.getInt("id") == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotSupportedTypes() throws Exception {
        stmt = conn.prepareStatement("");

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setArray(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setAsciiStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setAsciiStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setAsciiStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setBinaryStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setBinaryStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setBinaryStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setBlob(1, (Blob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setBlob(1, (InputStream)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setBlob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCharacterStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCharacterStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCharacterStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setClob(1, (Clob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setClob(1, (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setClob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNCharacterStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNCharacterStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNClob(1, (NClob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNClob(1, (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNClob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setRowId(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setRef(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setSQLXML(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setURL(1, new URL("http://test"));
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
        private TestObjectField objVal;

        /**
         * @param id ID.
         */
        private TestObject(int id) {
            this.id = id;
        }
    }

    /**
     * Dummy object represents object field of {@link TestObject TestObject}.
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
