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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CLOB;
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
import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertArrayEquals;

/**
 * Prepared statement test.
 */
public class JdbcPreparedStatementSelfTest extends GridCommonAbstractTest {
    /** Default URL params. */
    private static final String URL_DEFAULT_PARAM = "cache=default@modules/clients/src/test/config/jdbc-config.xml";

    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + URL_DEFAULT_PARAM;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private PreparedStatement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

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
        o.blobVal = new byte[] {1};
        o.clobVal = "large str";
        o.dateVal = new Date(1);
        o.timeVal = new Time(1);
        o.tsVal = new Timestamp(1);
        o.urlVal = new URL("http://abc.com/");

        cache.put(1, o);
        cache.put(2, new TestObject(2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(BASE_URL);

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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testBinaryStreamKnownLength() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where arrVal is not distinct from ?");

        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[]{1});

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, -1L);
            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, (long)(Integer.MAX_VALUE - 8 + 1));
            return null;
        }, SQLFeatureNotSupportedException.class, null);

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, -1);
            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, Integer.MAX_VALUE - 8 + 1);
            return null;
        }, SQLFeatureNotSupportedException.class, null);

        stmt.setBinaryStream(1, stream, 1);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stream.reset();
        stmt.setBinaryStream(1, stream, 1L);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stream.reset();
        stmt.setBinaryStream(1, stream, 10L);
        assertThrows(null, () -> stmt.executeQuery(), SQLException.class, null);

        stmt.setBinaryStream(1, null, 0);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());

        stmt.setBinaryStream(1, null, 0L);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryStreamUnknownLength() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where arrVal is not distinct from ?");

        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[]{1});

        stmt.setBinaryStream(1, stream);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stmt.setBinaryStream(1, null);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryStreamUnknownLengthOnDiskMaterialized() throws Exception {
        String url = CFG_URL_PREFIX + "maxInMemoryLobSize=5:" + URL_DEFAULT_PARAM;

        Connection conn = DriverManager.getConnection(url);

        String sql = "insert into TestObject(_key, id, blobVal) values (?, ?, ?)";

        byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        Set<String> existingTempStreamFiles = getTempStreamFiles();
        Set<String> newTempStreamFiles;

        try (conn) {
            PreparedStatement stmtToBeLeftUnclosed = conn.prepareStatement(sql);
            stmtToBeLeftUnclosed.setInt(1, 3);
            stmtToBeLeftUnclosed.setInt(2, 3);
            stmtToBeLeftUnclosed.setBinaryStream(3, new ByteArrayInputStream(bytes));

            PreparedStatement stmtToBeClosed = conn.prepareStatement(sql);
            stmtToBeClosed.setInt(1, 4);
            stmtToBeClosed.setInt(2, 4);
            stmtToBeClosed.setBinaryStream(3, new ByteArrayInputStream(bytes));

            PreparedStatement stmtToBeNotExecutedButClosed = conn.prepareStatement(sql);
            stmtToBeNotExecutedButClosed.setInt(1, 5);
            stmtToBeNotExecutedButClosed.setInt(2, 5);
            stmtToBeNotExecutedButClosed.setBinaryStream(3, new ByteArrayInputStream(bytes));

            newTempStreamFiles = getTempStreamFiles();
            newTempStreamFiles.removeAll(existingTempStreamFiles);
            assertEquals(3, newTempStreamFiles.size());

            assertEquals(1, stmtToBeLeftUnclosed.executeUpdate());
            assertEquals(1, stmtToBeClosed.executeUpdate());

            // Abandon the statements without closing it.
            stmtToBeLeftUnclosed = null;

            stmtToBeNotExecutedButClosed.close();
            stmtToBeClosed.close();

            stmt = conn.prepareStatement("select * from TestObject where id = ?");

            stmt.setInt(1, 3);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertArrayEquals(bytes, rs.getBytes("blobVal"));
            assertFalse(rs.next());

            stmt.setInt(1, 4);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertArrayEquals(bytes, rs.getBytes("blobVal"));
            assertFalse(rs.next());
        }
        finally {
            grid(0).cache(DEFAULT_CACHE_NAME).remove(3);
            grid(0).cache(DEFAULT_CACHE_NAME).remove(4);
        }

        // Invoke gc to force phantom references detection.
        System.gc();

        // Make sure the phantom reference to stream wrapper created inside
        // the stmtToBeLeftUnclosed statemnt was detected and the corresponding
        // temp file is removed by java.lang.ref.Cleaner thread.
        assertTrue(GridTestUtils.waitForCondition(() -> !getTempStreamFiles().containsAll(newTempStreamFiles), 3_000, 10));
    }

    /** */
    private Set<String> getTempStreamFiles() {
        Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"));

        try (Stream<Path> entries = Files.list(tmpDir)) {
            return entries
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(e -> e.startsWith("ignite-jdbc-temp-data"))
                    .collect(Collectors.toSet());
        }
        catch (NotDirectoryException e) {
            throw new AssertionError(e);
        }
        catch (IOException e) {
            return Collections.emptySet();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlob() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where blobVal is not distinct from ?");

        Blob blob = conn.createBlob();
        blob.setBytes(1, new byte[] {1});
        stmt.setBlob(1, blob);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        Blob blob2 = conn.createBlob();
        stmt.setBlob(1, blob2);
        blob2.setBytes(1, new byte[] {1});
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        Blob blob3 = conn.createBlob();
        stmt.setBlob(1, blob3);
        try (OutputStream out = blob3.setBinaryStream(1)) {
            out.write(new byte[] {1});
        }
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        Blob blob4 = conn.createBlob();
        stmt.setBlob(1, blob4);
        try (OutputStream out = blob4.setBinaryStream(1)) {
            out.write(1);
        }
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stmt.setNull(1, BLOB);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobInputStream() throws Exception {
        final int BLOB_SIZE = 123;

        byte[] bytes = new byte[BLOB_SIZE + 100];
        new Random().nextBytes(bytes);

        try {
            try (PreparedStatement stmt = conn.prepareStatement("insert into TestObject(_key, id, blobVal) values (?, ?, ?)")) {
                ByteArrayInputStream stream1 = new ByteArrayInputStream(bytes);

                stmt.setInt(1, 3);
                stmt.setInt(2, 3);
                stmt.setBlob(3, stream1, BLOB_SIZE);

                assertEquals(1, stmt.executeUpdate());
            }

            try (PreparedStatement stmt = conn.prepareStatement("select * from TestObject where blobVal is not distinct from ?")) {
                ByteArrayInputStream stream2 = new ByteArrayInputStream(bytes);

                stmt.setBlob(1, stream2, BLOB_SIZE);

                ResultSet rs = stmt.executeQuery();

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertFalse(rs.next());
            }
        }
        finally {
            grid(0).cache(DEFAULT_CACHE_NAME).remove(3);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobOnDiskMaterialized() throws Exception {
        String url = CFG_URL_PREFIX + "maxInMemoryLobSize=5:" + URL_DEFAULT_PARAM;

        Connection conn = DriverManager.getConnection(url);

        byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};

        Blob blob = null;

        try(conn) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into TestObject(_key, id, blobVal) values (?, ?, ?)")) {
                blob = conn.createBlob();

                try (OutputStream outputStream = blob.setBinaryStream(1)) {
                    outputStream.write(0);
                    outputStream.write(bytes);
                    outputStream.write(10);
                    outputStream.write(bytes, 2, 2);
                }

                stmt.setInt(1, 3);
                stmt.setInt(2, 3);
                stmt.setBlob(3, blob);

                assertEquals(1, stmt.executeUpdate());
            }
            finally {
                if (blob != null)
                    blob.free();
            }

            try (PreparedStatement stmt = conn.prepareStatement("select * from TestObject where blobVal is not distinct from ?")) {
                stmt.setBlob(1, new ByteArrayInputStream(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 4}));

                ResultSet rs = stmt.executeQuery();

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertFalse(rs.next());
            }
        }
        finally {
            grid(0).cache(DEFAULT_CACHE_NAME).remove(3);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClob() throws Exception {
        stmt = conn.prepareStatement("select * from TestObject where clobVal is not distinct from ?");

        Clob clob = conn.createClob();

        clob.setString(1, "large str");

        stmt.setClob(1, clob);

        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stmt.setNull(1, CLOB);

        rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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
    @Test
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
        private byte[] blobVal;

        /** */
        @QuerySqlField
        private String clobVal;

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
