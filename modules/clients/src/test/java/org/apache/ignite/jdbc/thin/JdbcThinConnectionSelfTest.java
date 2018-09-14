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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.jdbc.thin.ConnectionProperties;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Client key store path. */
    private static final String CLI_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/client.jks";

    /** Server key store path. */
    private static final String SRV_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/server.jks";

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setGridLogger(new GridStringLogger());

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
     */
    public void testInvalidEndpoint() {
        assertInvalid("jdbc:ignite:thin://", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://:10000", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://     :10000", "Host name is empty");

        assertInvalid("jdbc:ignite:thin://127.0.0.1:-1", "port range contains invalid port -1");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:0", "port range contains invalid port 0");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:100000",
            "port range contains invalid port 100000");
    }

    /**
     * Test invalid socket buffer sizes.
     *
     * @throws Exception If failed.
     */
    public void testSocketBuffers() throws Exception {
        final int dfltDufSize = 64 * 1024;

        assertInvalid("jdbc:ignite:thin://127.0.0.1?socketSendBuffer=-1",
            "Property cannot be lower than 0 [name=socketSendBuffer, value=-1]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1?socketReceiveBuffer=-1",
            "Property cannot be lower than 0 [name=socketReceiveBuffer, value=-1]");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertEquals(dfltDufSize, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(dfltDufSize, io(conn).connectionProperties().getSocketReceiveBuffer());
        }

        // Note that SO_* options are hints, so we check that value is equals to either what we set or to default.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?socketSendBuffer=1024")) {
            assertEquals(1024, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(dfltDufSize, io(conn).connectionProperties().getSocketReceiveBuffer());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?socketReceiveBuffer=1024")) {
            assertEquals(dfltDufSize, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(1024, io(conn).connectionProperties().getSocketReceiveBuffer());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?" +
            "socketSendBuffer=1024&socketReceiveBuffer=2048")) {
            assertEquals(1024, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(2048, io(conn).connectionProperties().getSocketReceiveBuffer());
        }
    }

    /**
     * Test invalid socket buffer sizes with semicolon.
     *
     * @throws Exception If failed.
     */
    public void testSocketBuffersSemicolon() throws Exception {
        final int dfltDufSize = 64 * 1024;

        assertInvalid("jdbc:ignite:thin://127.0.0.1;socketSendBuffer=-1",
            "Property cannot be lower than 0 [name=socketSendBuffer, value=-1]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1;socketReceiveBuffer=-1",
            "Property cannot be lower than 0 [name=socketReceiveBuffer, value=-1]");

        // Note that SO_* options are hints, so we check that value is equals to either what we set or to default.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;socketSendBuffer=1024")) {
            assertEquals(1024, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(dfltDufSize, io(conn).connectionProperties().getSocketReceiveBuffer());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;socketReceiveBuffer=1024")) {
            assertEquals(dfltDufSize, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(1024, io(conn).connectionProperties().getSocketReceiveBuffer());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;" +
            "socketSendBuffer=1024;socketReceiveBuffer=2048")) {
            assertEquals(1024, io(conn).connectionProperties().getSocketSendBuffer());
            assertEquals(2048, io(conn).connectionProperties().getSocketReceiveBuffer());
        }
    }

    /**
     * Test SQL hints.
     *
     * @throws Exception If failed.
     */
    public void testSqlHints() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertHints(conn, false, false, false, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?distributedJoins=true")) {
            assertHints(conn, true, false, false, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?enforceJoinOrder=true")) {
            assertHints(conn, false, true, false, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?collocated=true")) {
            assertHints(conn, false, false, true, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?replicatedOnly=true")) {
            assertHints(conn, false, false, false, true, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?lazy=true")) {
            assertHints(conn, false, false, false, false, true, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?skipReducerOnUpdate=true")) {
            assertHints(conn, false, false, false, false, false, true);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?distributedJoins=true&" +
            "enforceJoinOrder=true&collocated=true&replicatedOnly=true&lazy=true&skipReducerOnUpdate=true")) {
            assertHints(conn, true, true, true, true, true, true);
        }
    }

    /**
     * Test SQL hints with semicolon.
     *
     * @throws Exception If failed.
     */
    public void testSqlHintsSemicolon() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;distributedJoins=true")) {
            assertHints(conn, true, false, false, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;enforceJoinOrder=true")) {
            assertHints(conn, false, true, false, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;collocated=true")) {
            assertHints(conn, false, false, true, false, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;replicatedOnly=true")) {
            assertHints(conn, false, false, false, true, false, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;lazy=true")) {
            assertHints(conn, false, false, false, false, true, false);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;skipReducerOnUpdate=true")) {
            assertHints(conn, false, false, false, false, false, true);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;distributedJoins=true;" +
            "enforceJoinOrder=true;collocated=true;replicatedOnly=true;lazy=true;skipReducerOnUpdate=true")) {
            assertHints(conn, true, true, true, true, true, true);
        }
    }

    /**
     * Assert hints.
     *
     * @param conn Connection.
     * @param distributedJoins Distributed joins.
     * @param enforceJoinOrder Enforce join order.
     * @param collocated Co-located.
     * @param replicatedOnly Replicated only.
     * @param lazy Lazy.
     * @param skipReducerOnUpdate Skip reducer on update.
     * @throws Exception If failed.
     */
    private void assertHints(Connection conn, boolean distributedJoins, boolean enforceJoinOrder, boolean collocated,
        boolean replicatedOnly, boolean lazy, boolean skipReducerOnUpdate)throws Exception {
        assertEquals(distributedJoins, io(conn).connectionProperties().isDistributedJoins());
        assertEquals(enforceJoinOrder, io(conn).connectionProperties().isEnforceJoinOrder());
        assertEquals(collocated, io(conn).connectionProperties().isCollocated());
        assertEquals(replicatedOnly, io(conn).connectionProperties().isReplicatedOnly());
        assertEquals(lazy, io(conn).connectionProperties().isLazy());
        assertEquals(skipReducerOnUpdate, io(conn).connectionProperties().isSkipReducerOnUpdate());
    }

    /**
     * Test TCP no delay property handling.
     *
     * @throws Exception If failed.
     */
    public void testTcpNoDelay() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=0",
            "Invalid property value. [name=tcpNoDelay, val=0, choices=[true, false]]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=1",
            "Invalid property value. [name=tcpNoDelay, val=1, choices=[true, false]]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=false1",
            "Invalid property value. [name=tcpNoDelay, val=false1, choices=[true, false]]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=true1",
            "Invalid property value. [name=tcpNoDelay, val=true1, choices=[true, false]]");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertTrue(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=true")) {
            assertTrue(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=True")) {
            assertTrue(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=false")) {
            assertFalse(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=False")) {
            assertFalse(io(conn).connectionProperties().isTcpNoDelay());
        }
    }

    /**
     * Test TCP no delay property handling with semicolon.
     *
     * @throws Exception If failed.
     */
    public void testTcpNoDelaySemicolon() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=0",
            "Invalid property value. [name=tcpNoDelay, val=0, choices=[true, false]]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=1",
            "Invalid property value. [name=tcpNoDelay, val=1, choices=[true, false]]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=false1",
            "Invalid property value. [name=tcpNoDelay, val=false1, choices=[true, false]]");

        assertInvalid("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=true1",
            "Invalid property value. [name=tcpNoDelay, val=true1, choices=[true, false]]");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=true")) {
            assertTrue(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=True")) {
            assertTrue(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=false")) {
            assertFalse(io(conn).connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;tcpNoDelay=False")) {
            assertFalse(io(conn).connectionProperties().isTcpNoDelay());
        }
    }

    /**
     * Test autoCloseServerCursor property handling.
     *
     * @throws Exception If failed.
     */
    public void testAutoCloseServerCursorProperty() throws Exception {
        String url = "jdbc:ignite:thin://127.0.0.1?autoCloseServerCursor";

        String err = "Invalid property value. [name=autoCloseServerCursor";

        assertInvalid(url + "=0", err);
        assertInvalid(url + "=1", err);
        assertInvalid(url + "=false1", err);
        assertInvalid(url + "=true1", err);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertFalse(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=true")) {
            assertTrue(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=True")) {
            assertTrue(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=false")) {
            assertFalse(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=False")) {
            assertFalse(io(conn).connectionProperties().isAutoCloseServerCursor());
        }
    }

    /**
     * Test autoCloseServerCursor property handling with semicolon.
     *
     * @throws Exception If failed.
     */
    public void testAutoCloseServerCursorPropertySemicolon() throws Exception {
        String url = "jdbc:ignite:thin://127.0.0.1;autoCloseServerCursor";

        String err = "Invalid property value. [name=autoCloseServerCursor";

        assertInvalid(url + "=0", err);
        assertInvalid(url + "=1", err);
        assertInvalid(url + "=false1", err);
        assertInvalid(url + "=true1", err);

        try (Connection conn = DriverManager.getConnection(url + "=true")) {
            assertTrue(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=True")) {
            assertTrue(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=false")) {
            assertFalse(io(conn).connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=False")) {
            assertFalse(io(conn).connectionProperties().isAutoCloseServerCursor());
        }
    }

    /**
     * Test schema property in URL.
     *
     * @throws Exception If failed.
     */
    public void testSchema() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1/qwe/qwe",
            "Invalid URL format (only schema name is allowed in URL path parameter 'host:port[/schemaName]')" );

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/public")) {
            assertEquals("Invalid schema", "PUBLIC", conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/\"" + DEFAULT_CACHE_NAME + '"')) {
            assertEquals("Invalid schema", DEFAULT_CACHE_NAME, conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/_not_exist_schema_")) {
            assertEquals("Invalid schema", "_NOT_EXIST_SCHEMA_", conn.getSchema());
        }
    }

    /**
     * Test schema property in URL with semicolon.
     *
     * @throws Exception If failed.
     */
    public void testSchemaSemicolon() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;schema=public")) {
            assertEquals("Invalid schema", "PUBLIC", conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;schema=\"" + DEFAULT_CACHE_NAME + '"')) {
            assertEquals("Invalid schema", DEFAULT_CACHE_NAME, conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1;schema=_not_exist_schema_")) {
            assertEquals("Invalid schema", "_NOT_EXIST_SCHEMA_", conn.getSchema());
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

        return GridTestUtils.getFieldValue(conn0, JdbcThinConnection.class, "cliIo");
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

        assert !conn.isValid(2) : "Connection must be closed";

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
        try (Connection conn = DriverManager.getConnection(URL)) {
            try (Statement stmt = conn.createStatement()) {
                assertNotNull(stmt);

                stmt.close();

                conn.close();

                // Exception when called on closed connection
                checkConnectionClosed(new RunnableX() {
                    @Override public void run() throws Exception {
                        conn.createStatement();
                    }
                });
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStatement2() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            int [] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int [] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assert type == TYPE_FORWARD_ONLY;
                        assert concur == CONCUR_READ_ONLY;

                        try (Statement stmt = conn.createStatement(type, concur)) {
                            assertNotNull(stmt);

                            assertEquals(type, stmt.getResultSetType());
                            assertEquals(concur, stmt.getResultSetConcurrency());
                        }

                        continue;
                    }

                    GridTestUtils.assertThrows(log,
                        new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                return conn.createStatement(type, concur);
                            }
                        },
                        SQLFeatureNotSupportedException.class,
                        null
                    );
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.createStatement(TYPE_FORWARD_ONLY,
                        CONCUR_READ_ONLY);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            int [] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int [] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int [] rsHoldabilities = new int[]
                {HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    for (final int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assert type == TYPE_FORWARD_ONLY;
                            assert concur == CONCUR_READ_ONLY;

                            try (Statement stmt = conn.createStatement(type, concur, holdabililty)) {
                                assertNotNull(stmt);

                                assertEquals(type, stmt.getResultSetType());
                                assertEquals(concur, stmt.getResultSetConcurrency());
                                assertEquals(holdabililty, stmt.getResultSetHoldability());
                            }

                            continue;
                        }

                        GridTestUtils.assertThrows(log,
                            new Callable<Object>() {
                                @Override public Object call() throws Exception {
                                    return conn.createStatement(type, concur, holdabililty);
                                }
                            },
                            SQLFeatureNotSupportedException.class,
                            null
                        );
                    }
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.createStatement(TYPE_FORWARD_ONLY,
                        CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // null query text
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(null);
                    }
                },
                SQLException.class,
                "SQL string cannot be null"
            );

            final String sqlText = "select * from test where param = ?";

            try (PreparedStatement prepared = conn.prepareStatement(sqlText)) {
                assertNotNull(prepared);
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.prepareStatement(sqlText);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "select * from test where param = ?";

            int [] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int [] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assert type == TYPE_FORWARD_ONLY;
                        assert concur == CONCUR_READ_ONLY;

                        // null query text
                        GridTestUtils.assertThrows(log,
                            new Callable<Object>() {
                                @Override public Object call() throws Exception {
                                    return conn.prepareStatement(null, type, concur);
                                }
                            },
                            SQLException.class,
                            "SQL string cannot be null"
                        );

                        continue;
                    }

                    GridTestUtils.assertThrows(log,
                        new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                return conn.prepareStatement(sqlText, type, concur);
                            }
                        },
                        SQLFeatureNotSupportedException.class,
                        null
                    );
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                }
            });

            conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatement4() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "select * from test where param = ?";

            int [] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int [] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int [] rsHoldabilities = new int[]
                {HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    for (final int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assert type == TYPE_FORWARD_ONLY;
                            assert concur == CONCUR_READ_ONLY;

                            // null query text
                            GridTestUtils.assertThrows(log,
                                new Callable<Object>() {
                                    @Override public Object call() throws Exception {
                                        return conn.prepareStatement(null, type, concur, holdabililty);
                                    }
                                },
                                SQLException.class,
                                "SQL string cannot be null"
                            );

                            continue;
                        }

                        GridTestUtils.assertThrows(log,
                            new Callable<Object>() {
                                @Override public Object call() throws Exception {
                                    return conn.prepareStatement(sqlText, type, concur, holdabililty);
                                }
                            },
                            SQLFeatureNotSupportedException.class,
                            null
                        );
                    }
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                }
            });

            conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareStatementAutoGeneratedKeysUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
            // null query text
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.nativeSQL(null);
                    }
                },
                SQLException.class,
                "SQL string cannot be null"
            );

            final String sqlText = "select * from test";

            assertEquals(sqlText, conn.nativeSQL(sqlText));

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.nativeSQL(sqlText);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.getAutoCommit());

            // Cannot disable autocommit when MVCC is disabled.
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(false);

                        return null;
                    }
                },
                SQLException.class,
                "MVCC must be enabled in order to invoke transactional operation: COMMIT"
            );

            assertTrue(conn.getAutoCommit());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setAutoCommit(true);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.commit();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be committed explicitly in auto-commit mode"
            );

            // Cannot disable autocommit when MVCC is disabled.
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(false);

                        return null;
                    }
                },
                SQLException.class,
                "MVCC must be enabled in order to invoke transactional operation: COMMIT"
            );

            assertTrue(conn.getAutoCommit());

            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.commit();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be committed explicitly in auto-commit mode."
            );

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.commit();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be rolled back explicitly in auto-commit mode."
            );

            // Cannot disable autocommit when MVCC is disabled.
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(false);

                        return null;
                    }
                },
                SQLException.class,
                "MVCC must be enabled in order to invoke transactional operation: COMMIT"
            );

            assertTrue(conn.getAutoCommit());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.rollback();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertNotNull(meta);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getMetaData();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetReadOnly() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setReadOnly(true);
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.isReadOnly();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsCatalogsInDataManipulation();

            assertNull(conn.getCatalog());

            conn.setCatalog("catalog");

            assertEquals(null, conn.getCatalog());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setCatalog("");
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getCatalog();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid parameter value
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @SuppressWarnings("MagicConstant")
                    @Override public Object call() throws Exception {
                        conn.setTransactionIsolation(-1);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid transaction isolation level"
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

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getTransactionIsolation();
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearGetWarnings() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            SQLWarning warn = conn.getWarnings();

            assertNull(warn);

            conn.clearWarnings();

            warn = conn.getWarnings();

            assertNull(warn);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getWarnings();
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.clearWarnings();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetTypeMap() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetHoldability() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint();

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be set in auto-commit mode"
            );

            // Cannot disable autocommit when MVCC is disabled.
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(false);

                        return null;
                    }
                },
                SQLException.class,
                "MVCC must be enabled in order to invoke transactional operation: COMMIT"
            );

            assertTrue(conn.getAutoCommit());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSavepoint();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetSavepointName() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(null);

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint name cannot be null"
            );

            final String name = "savepoint";

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(name);

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be set in auto-commit mode"
            );

            // Cannot disable autocommit when MVCC is disabled.
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(false);

                        return null;
                    }
                },
                SQLException.class,
                "MVCC must be enabled in order to invoke transactional operation: COMMIT"
            );

            assertTrue(conn.getAutoCommit());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSavepoint(name);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackSavePoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid savepoint"
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

            // Cannot disable autocommit when MVCC is disabled.
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setAutoCommit(false);

                        return null;
                    }
                },
                SQLException.class,
                "MVCC must be enabled in order to invoke transactional operation: COMMIT"
            );

            assertTrue(conn.getAutoCommit());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.rollback(savepoint);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleaseSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.releaseSavepoint(null);

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be null"
            );

            final Savepoint savepoint = getFakeSavepoint();

            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.releaseSavepoint(savepoint);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.releaseSavepoint(savepoint);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
//        fail("https://issues.apache.org/jira/browse/IGNITE-5425");

        try (Connection conn = DriverManager.getConnection(URL)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            assertNull(conn.getWarnings());

            conn.setClientInfo(name, val);

            assertNull(conn.getClientInfo(val));

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getClientInfo(name);
                }
            });

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setClientInfo(name, val);

                        return null;
                    }
                }, SQLClientInfoException.class, "Connection is closed");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetClientInfoProperties() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            final Properties props = new Properties();
            props.setProperty(name, val);

            conn.setClientInfo(props);

            Properties propsResult = conn.getClientInfo();

            assertNotNull(propsResult);

            assertTrue(propsResult.isEmpty());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getClientInfo();
                }
            });

            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setClientInfo(props);

                        return null;
                    }
                }, SQLClientInfoException.class, "Connection is closed");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateArrayOf() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
                "Type name cannot be null"
            );

            // Unsupported

            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.createArrayOf(typeName, elements);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.createArrayOf(typeName, elements);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateStruct() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid typename
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createStruct(null, null);
                    }
                },
                SQLException.class,
                "Type name cannot be null"
            );

            final String typeName = "employee";

            final Object[] attrs = new Object[] {100, "Tom"};

            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.createStruct(typeName, attrs);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.createStruct(typeName, attrs);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertEquals("PUBLIC", conn.getSchema());

            final String schema = "test";

            conn.setSchema(schema);

            assertEquals(schema.toUpperCase(), conn.getSchema());

            conn.setSchema('"' + schema + '"');

            assertEquals(schema, conn.getSchema());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSchema(schema);
                }
            });

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getSchema();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAbort() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            //Invalid executor
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.abort(null);

                        return null;
                    }
                },
                SQLException.class,
                "Executor cannot be null"
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
                "Executor cannot be null"
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
                "Network timeout cannot be negative"
            );

            conn.setNetworkTimeout(executor, timeout);

            assertEquals(timeout, conn.getNetworkTimeout());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.getNetworkTimeout();
                }
            });

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setNetworkTimeout(executor, timeout);
                }
            });
        }
    }

    /**
     * Test that attempting to supply invalid nested TX mode to driver fails on the client.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testInvalidNestedTxMode() {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                DriverManager.getConnection(URL + "/?nestedTransactionsMode=invalid");

                return null;
            }
        }, SQLException.class, "Invalid nested transactions handling mode");
    }

    /**
     * Test that attempting to send unexpected name of nested TX mode to server on handshake yields an error.
     * We have to do this without explicit {@link Connection} as long as there's no other way to bypass validation and
     * supply a malformed {@link ConnectionProperties} to {@link JdbcThinTcpIo}.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "ThrowFromFinallyBlock"})
    public void testInvalidNestedTxModeOnServerSide() throws SQLException, NoSuchMethodException,
        IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.setAddresses(new HostAndPortRange[]{new HostAndPortRange("127.0.0.1", DFLT_PORT, DFLT_PORT)});

        connProps.nestedTxMode("invalid");

        Constructor ctor = JdbcThinTcpIo.class.getDeclaredConstructor(ConnectionProperties.class);

        boolean acc = ctor.isAccessible();

        ctor.setAccessible(true);

        final JdbcThinTcpIo io = (JdbcThinTcpIo)ctor.newInstance(connProps);

        try {
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    io.start();

                    return null;
                }
            }, SQLException.class, "err=Invalid nested transactions handling mode: invalid");
        }
        finally {
            io.close();

            ctor.setAccessible(acc);
        }
    }

    /**
     */
    public void testSslClientAndPlainServer()  {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + SRV_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");

                return null;
            }
        }, SQLException.class, "Failed to SSL connect to server");
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadingException() throws Exception {
        int threadCnt = 10;

        final boolean end[] = new boolean[] {false};

        final SQLException exs [] = new SQLException[threadCnt];

        final AtomicInteger exCnt = new AtomicInteger(0);

        try (final Connection conn = DriverManager.getConnection(URL)) {
            final IgniteInternalFuture f = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        conn.createStatement();

                        while (!end[0])
                            conn.createStatement().execute("SELECT 1");

                        conn.createStatement().execute("SELECT 1");
                    }
                    catch (SQLException e) {
                        end[0] = true;
                        exs[exCnt.getAndIncrement()] = e;
                    }
                    catch (Exception e) {
                        e.printStackTrace(System.err);

                        fail("Unexpected exception (see details above): " + e.getMessage());
                    }
                }
            }, threadCnt, "run-query");

            f.get();

            boolean exceptionFound = false;

            for (SQLException e : exs) {
                if (e != null && e.getMessage().contains("Concurrent access to JDBC connection is not allowed"))
                    exceptionFound = true;
            }

            assertTrue("Concurrent access to JDBC connection is not allowed", exceptionFound);
        }
    }

    /**
     * @return Savepoint.
     */
    private Savepoint getFakeSavepoint() {
        return new Savepoint() {
            @Override public int getSavepointId() throws SQLException {
                return 100;
            }

            @Override public String getSavepointName() {
                return "savepoint";
            }
        };
    }
}