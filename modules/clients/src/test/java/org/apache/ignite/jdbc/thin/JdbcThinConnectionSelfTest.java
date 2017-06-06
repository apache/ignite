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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;

/**
 * Connection test.
 */
public class JdbcThinConnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"EmptyTryBlock", "unused"})
    public void testDefaults() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
            // No-op.
        }
    }

    /**
     * Test invalid endpoint.
     *
     * @throws Exception If failed.
     */
    public void testInvalidEndpoint() throws Exception {
        assertInvalid("jdbc:ignite:thin://", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://:10000", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://     :10000", "Host name is empty");

        assertInvalid("jdbc:ignite:thin://127.0.0.1:-1", "Invalid port");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:0", "Invalid port");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:100000", "Invalid port");
    }

    /**
     * Test invalid socket buffer sizes.
     *
     * @throws Exception If failed.
     */
    public void testSocketBuffers() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1?socketSendBuffer=-1",
            "Property cannot be negative [name=" + JdbcThinUtils.PARAM_SOCK_SND_BUF);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?socketReceiveBuffer=-1",
            "Property cannot be negative [name=" + JdbcThinUtils.PARAM_SOCK_RCV_BUF);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertEquals(0, io(conn).socketSendBuffer());
            assertEquals(0, io(conn).socketReceiveBuffer());
        }

        // Note that SO_* options are hints, so we check that value is equals to either what we set or to default.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?socketSendBuffer=1024")) {
            assertEquals(1024, io(conn).socketSendBuffer());
            assertEquals(0, io(conn).socketReceiveBuffer());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?socketReceiveBuffer=1024")) {
            assertEquals(0, io(conn).socketSendBuffer());
            assertEquals(1024, io(conn).socketReceiveBuffer());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?" +
            "socketSendBuffer=1024&socketReceiveBuffer=2048")) {
            assertEquals(1024, io(conn).socketSendBuffer());
            assertEquals(2048, io(conn).socketReceiveBuffer());
        }
    }

    /**
     * Test SQL hints.
     *
     * @throws Exception If failed.
     */
    public void testSqlHints() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertFalse(io(conn).distributedJoins());
            assertFalse(io(conn).enforceJoinOrder());
            assertFalse(io(conn).collocated());
            assertFalse(io(conn).replicatedOnly());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?distributedJoins=true")) {
            assertTrue(io(conn).distributedJoins());
            assertFalse(io(conn).enforceJoinOrder());
            assertFalse(io(conn).collocated());
            assertFalse(io(conn).replicatedOnly());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?enforceJoinOrder=true")) {
            assertFalse(io(conn).distributedJoins());
            assertTrue(io(conn).enforceJoinOrder());
            assertFalse(io(conn).collocated());
            assertFalse(io(conn).replicatedOnly());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?collocated=true")) {
            assertFalse(io(conn).distributedJoins());
            assertFalse(io(conn).enforceJoinOrder());
            assertTrue(io(conn).collocated());
            assertFalse(io(conn).replicatedOnly());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?replicatedOnly=true")) {
            assertFalse(io(conn).distributedJoins());
            assertFalse(io(conn).enforceJoinOrder());
            assertFalse(io(conn).collocated());
            assertTrue(io(conn).replicatedOnly());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?distributedJoins=true&" +
                "enforceJoinOrder=true&collocated=true&replicatedOnly=true")) {
            assertTrue(io(conn).distributedJoins());
            assertTrue(io(conn).enforceJoinOrder());
            assertTrue(io(conn).collocated());
            assertTrue(io(conn).replicatedOnly());
        }
    }

    /**
     * Test TCP no delay property handling.
     *
     * @throws Exception If failed.
     */
    public void testTcpNoDelay() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=0",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=1",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=false1",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=true1",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertTrue(io(conn).tcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=true")) {
            assertTrue(io(conn).tcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=True")) {
            assertTrue(io(conn).tcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=false")) {
            assertFalse(io(conn).tcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=False")) {
            assertFalse(io(conn).tcpNoDelay());
        }
    }

    /**
     * Get client socket for connection.
     *
     * @param conn Connection.
     * @return Socket.
     * @throws Exception If failed.
     */
    private static JdbcThinTcpIo io(Connection conn) throws Exception {
        JdbcThinConnection conn0 = conn.unwrap(JdbcThinConnection.class);

        return conn0.io();
    }

    /**
     * Assert that provided URL is invalid.
     *
     * @param url URL.
     * @param errMsg Error message.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertInvalid(final String url, String errMsg) {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(url);

                return null;
            }
        }, SQLException.class, errMsg);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testClose() throws Exception {
        final Connection conn;

        try (Connection conn0 = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            conn = conn0;

            assert conn != null;
            assert !conn.isClosed();
        }

        assert conn.isClosed();

        assert !conn.isValid(2): "Connection must be closed";

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                conn.isValid(-2);

                return null;
            }
        }, SQLException.class, "Invalid timeout");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            try (Statement stmt = conn.createStatement()) {
                assertNotNull(stmt);

                stmt.close();

                conn.close();

                // Exception when called on closed connection
                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return conn.createStatement();
                        }
                    },
                    SQLException.class,
                    "Connection is closed"
                );
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStatement2() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Unsupported result set type
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid result set type"
            );

            // Unsupported concurrency type
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid concurrency"
            );

            // Accepted parameters
            try (Statement stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)) {
                assertNotNull(stmt);

                stmt.close();
            }

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Unsupported result set type
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStatement(TYPE_SCROLL_INSENSITIVE,
                            CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid result set type"
            );

            // Unsupported concurrency type
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStatement(TYPE_FORWARD_ONLY,
                            CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid concurrency"
            );

            // Accepted parameters
            try (Statement stmt = conn.createStatement(TYPE_FORWARD_ONLY,
                CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)) {
                assertNotNull(stmt);

                assertEquals(HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
            }

            try (Statement stmt = conn.createStatement(TYPE_FORWARD_ONLY,
                CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)) {
                assertNotNull(stmt);

                assertEquals(CLOSE_CURSORS_AT_COMMIT, stmt.getResultSetHoldability());
            }

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStatement(TYPE_FORWARD_ONLY,
                            CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // null query text
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(null);
                    }
                },
                SQLException.class,
                "Invalid arguments"
            );

            final String sqlText = "select * from test where param = ?";

            try (PreparedStatement prepared = conn.prepareStatement(sqlText)) {
                assertNotNull(prepared);
            }

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // null query text
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(null, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                SQLException.class,
                "Invalid arguments"
            );

            final String sqlText = "select * from test where param = ?";

            // Unsupported result set type
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid result set type"
            );

            // Unsupported concurrency
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid concurrency"
            );

            try (PreparedStatement prepared = conn.prepareStatement(sqlText,
                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)) {
                assertNotNull(prepared);
            }

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatement4() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // null query text
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(null, TYPE_FORWARD_ONLY,
                            CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLException.class,
                "Invalid arguments"
            );

            final String sqlText = "select * from test where param = ?";

            // Unsupported result set type
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, TYPE_SCROLL_INSENSITIVE,
                            CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid result set type"
            );

            // Unsupported concurrency
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY,
                            CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Invalid concurrency"
            );

            try (PreparedStatement prepared = conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY,
                CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)) {
                assertNotNull(prepared);

                assertEquals(HOLD_CURSORS_OVER_COMMIT, prepared.getResultSetHoldability());
            }

            try (PreparedStatement prepared = conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY,
                CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)) {
                assertNotNull(prepared);

                assertEquals(CLOSE_CURSORS_AT_COMMIT, prepared.getResultSetHoldability());
            }

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY,
                            CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatementAutoGeneratedKeysUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            final String sqlText = "insert into test (val) values (?)";

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, RETURN_GENERATED_KEYS);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, NO_GENERATED_KEYS);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, new int[] {1});
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, new String[] {"ID"});
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareCallUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            final String sqlText = "exec test()";

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareCall(sqlText);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Callable functions are not supported."
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareCall(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Callable functions are not supported."
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareCall(sqlText, TYPE_FORWARD_ONLY,
                            CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Callable functions are not supported."
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNativeSql() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // null query text
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.nativeSQL(null);
                    }
                },
                SQLException.class,
                "Invalid arguments"
            );

            final String sqlText = "select * from test";

            assertEquals(sqlText, conn.nativeSQL(sqlText));

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.nativeSQL(sqlText);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            assertTrue(conn.getAutoCommit());

            conn.setAutoCommit(false);

            assertFalse(conn.getAutoCommit());

            conn.setAutoCommit(true);

            assertTrue(conn.getAutoCommit());

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(true);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getAutoCommit();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.commit();

                        return null;
                    }
                },
                SQLException.class,
                "Auto commit mode"
            );

            conn.setAutoCommit(false);

            conn.commit();

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.commit();

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback();

                        return null;
                    }
                },
                SQLException.class,
                "Auto commit mode"
            );

            conn.setAutoCommit(false);

            conn.rollback();

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback();

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertNotNull(meta);

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getMetaData();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetReadOnly() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            assertFalse(conn.isReadOnly());

            conn.setReadOnly(true);

            assertTrue(conn.isReadOnly());

            conn.setReadOnly(false);

            assertFalse(conn.isReadOnly());

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setReadOnly(true);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.isReadOnly();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            assertNull(conn.getCatalog());

            final String catalog = "catalog";

            conn.setCatalog(catalog);

            assertEquals(catalog, conn.getCatalog());

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setCatalog(catalog);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getCatalog();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Invalid parameter value
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setTransactionIsolation(-1);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid parameter"
            );

            // default level
            assertEquals(TRANSACTION_NONE, conn.getTransactionIsolation());

            int[] levels = {
                TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,
                TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE};

            for (int level : levels) {
                conn.setTransactionIsolation(level);
                assertEquals(level, conn.getTransactionIsolation());
            }

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getTransactionIsolation();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearGetWarnings() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            SQLWarning warn = conn.getWarnings();

            assertNull(warn);

            //TODO: need a way to trigger warning

            conn.clearWarnings();

            warn = conn.getWarnings();

            assertNull(warn);

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getWarnings();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.clearWarnings();

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetTypeMap() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getTypeMap();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Types mapping is not supported"
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setTypeMap(new HashMap<String, Class<?>>());

                        return null;
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Types mapping is not supported"
            );

            conn.close();

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getTypeMap();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setTypeMap(new HashMap<String, Class<?>>());

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            //TODO: is it at all possible to pass something other than java.util.Map to setTypeMap()?
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetHoldability() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // default value
            assertEquals(conn.getMetaData().getResultSetHoldability(), conn.getHoldability());

            assertEquals(HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

            conn.setHoldability(CLOSE_CURSORS_AT_COMMIT);

            assertEquals(CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            // Invalid constant
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setHoldability(-1);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid result set holdability value"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getHoldability();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setHoldability(HOLD_CURSORS_OVER_COMMIT);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSavepoint();

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint();

                        return null;
                    }
                },
                SQLException.class,
                "Auto-commit mode"
            );

            conn.setAutoCommit(false);

            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint();

                        return null;
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Savepoints are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint();

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetSavepointName() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid argument"
            );

            final String name = "savepoint";

            conn.setSavepoint();

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(name);

                        return null;
                    }
                },
                SQLException.class,
                "Auto-commit mode"
            );

            conn.setAutoCommit(false);

            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(name);

                        return null;
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Savepoints are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(name);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackSavePoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid argument"
            );

            final Savepoint savepoint = getFakeSavepoint();

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(savepoint);

                        return null;
                    }
                },
                SQLException.class,
                "Auto-commit mode"
            );

            conn.setAutoCommit(false);

            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(savepoint);

                        return null;
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Savepoints are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(savepoint);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleaseSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.releaseSavepoint(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid argument"
            );

            final Savepoint savepoint = getFakeSavepoint();

            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.releaseSavepoint(savepoint);

                        return null;
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Savepoints are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.releaseSavepoint(savepoint);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createClob();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createClob();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateBlob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createBlob();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createBlob();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateNClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createNClob();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createNClob();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateSQLXML() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createSQLXML();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createSQLXML();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetClientInfoPair() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            assertNull(conn.getWarnings());

            conn.setClientInfo(name, val);

            assertNotNull(conn.getWarnings());

            assertNull(conn.getClientInfo(val));

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getClientInfo(name);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setClientInfo(name, val);

                        return null;
                    }
                },
                SQLClientInfoException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetClientInfoProperties() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            final Properties props = new Properties();
            props.setProperty(name, val);

            conn.setClientInfo(props);

            Properties propsResult = conn.getClientInfo();

            assertNotNull(propsResult);

            assertTrue(propsResult.isEmpty());

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getClientInfo();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setClientInfo(props);

                        return null;
                    }
                },
                SQLClientInfoException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateArrayOf() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            final String typeName = "varchar";

            final String[] elements = new String[] {"apple", "pear"};

            // Invalid typename
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.createArrayOf(null, null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid type name"
            );

            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createArrayOf(typeName, elements);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createArrayOf(typeName, elements);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStruct() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // Invalid typename
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStruct(null, null);
                    }
                },
                SQLException.class,
                "Invalid type name"
            );

            final String typeName = "employee";

            final Object[] attrs = new Object[] {100, "Tom"};

            // Unsupported
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStruct(typeName, attrs);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStruct(typeName, attrs);
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            assertEquals("PUBLIC", conn.getSchema());

            // Invalid schema
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSchema(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid schema value"
            );

            final String schema = "test";

            conn.setSchema(schema);

            assertEquals(schema, conn.getSchema());

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSchema(schema);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getSchema();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAbort() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            //Invalid executor
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.abort(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid executor value"
            );

            final Executor executor = Executors.newFixedThreadPool(1);

            conn.abort(executor);

            assertTrue(conn.isClosed());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetNetworkTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            // default
            assertEquals(0, conn.getNetworkTimeout());

            final Executor executor = Executors.newFixedThreadPool(1);

            final int timeout = 1000;

            //Invalid executor
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setNetworkTimeout(null, timeout);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid executor value"
            );

            //Invalid timeout
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setNetworkTimeout(executor, -1);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid timeout value"
            );

            conn.setNetworkTimeout(executor, timeout);

            assertEquals(timeout, conn.getNetworkTimeout());

            conn.close();

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getNetworkTimeout();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setNetworkTimeout(executor, timeout);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed"
            );
        }
    }

    // TODO: Methods to throw SQLException when database access fails (network conn lost / cluster stop)
    // createStatement
    // prepareStatement
    // nativeSql
    // getAutoCommit/setAutoCommit
    // commit/rollback
    // getMetaData
    // setReadOnly/isReadOnly
    // setCatalog/getCatalog
    // setTransactionIsolationLevel/getTransactionIsolationLevel
    // getWarnings/clearWarnings
    // getTypeMap/setTypeMap
    // getHoldability/setHoldability
    // setSavepoint
    // releaseSavepoint
    // createClob/createBlob/createNClob/createSQLXML
    // setClientInfo/getClientInfo
    // createArrayOf
    // createStruct
    // setSchema/getSchema
    // abort
    // setNetworTimeout/getNetworkTimeout
    //
    // TODO: methods disallowed during distributed transactions
    // setAutoCommit
    // commit/rollback
    // setSavePoint
    //
    // TODO: methods disallowed during transaction
    // setReadOnly
    //

    private Savepoint getFakeSavepoint() {
        return new Savepoint() {
            @Override public int getSavepointId() throws SQLException {
                return 100;
            }

            @Override public String getSavepointName() throws SQLException {
                return "savepoint";
            }
        };
    }
}