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

import java.net.InetSocketAddress;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryNoopMetadataHandler;
import org.apache.ignite.internal.jdbc.thin.ConnectionProperties;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.GridStringLogger;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

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
import static org.apache.ignite.internal.processors.odbc.SqlStateCode.TRANSACTION_STATE_EXCEPTION;
import static org.apache.ignite.testframework.GridTestUtils.RunnableX;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** Client key store path. */
    private static final String CLI_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/client.jks";

    /** Server key store path. */
    private static final String SRV_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/server.jks";

    /** Localhost. */
    private static final String LOCALHOST = "127.0.0.1";

    /** URL. */
    private String url = partitionAwareness ?
        "jdbc:ignite:thin://127.0.0.1:10800..10802" :
        "jdbc:ignite:thin://127.0.0.1";

    /** URL with partition awareness property. */
    private String urlWithPartitionAwarenessProp = url + "?partitionAwareness=" + partitionAwareness;

    /** URL with partition awareness property and semicolon as delimiter. */
    private String urlWithPartitionAwarenessPropSemicolon = url + ";partitionAwareness=" + partitionAwareness;

    /** Nodes count. */
    private int nodesCnt = partitionAwareness ? 4 : 2;

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

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

        startGridsMultiThreaded(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"EmptyTryBlock", "unused"})
    @Test
    public void testDefaults() throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection(url + "/")) {
            // No-op.
        }
    }

    /**
     * Test invalid endpoint.
     */
    @Test
    public void testInvalidEndpoint() {
        assertInvalid("jdbc:ignite:thin://", "Address is empty");
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
    @Test
    public void testSocketBuffers() throws Exception {
        final int dfltDufSize = 64 * 1024;

        assertInvalid(urlWithPartitionAwarenessProp + "&socketSendBuffer=-1",
            "Property cannot be lower than 0 [name=socketSendBuffer, value=-1]");

        assertInvalid(urlWithPartitionAwarenessProp + "&socketReceiveBuffer=-1",
            "Property cannot be lower than 0 [name=socketReceiveBuffer, value=-1]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(dfltDufSize, io.connectionProperties().getSocketSendBuffer());
                assertEquals(dfltDufSize, io.connectionProperties().getSocketReceiveBuffer());
            }
        }

        // Note that SO_* options are hints, so we check that value is equals to either what we set or to default.
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&socketSendBuffer=1024")) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(1024, io.connectionProperties().getSocketSendBuffer());
                assertEquals(dfltDufSize, io.connectionProperties().getSocketReceiveBuffer());
            }
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&socketReceiveBuffer=1024")) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(dfltDufSize, io.connectionProperties().getSocketSendBuffer());
                assertEquals(1024, io.connectionProperties().getSocketReceiveBuffer());
            }
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&" +
            "socketSendBuffer=1024&socketReceiveBuffer=2048")) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(1024, io.connectionProperties().getSocketSendBuffer());
                assertEquals(2048, io.connectionProperties().getSocketReceiveBuffer());
            }
        }
    }

    /**
     * Test invalid socket buffer sizes with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSocketBuffersSemicolon() throws Exception {
        final int dfltDufSize = 64 * 1024;

        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";socketSendBuffer=-1",
            "Property cannot be lower than 0 [name=socketSendBuffer, value=-1]");

        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";socketReceiveBuffer=-1",
            "Property cannot be lower than 0 [name=socketReceiveBuffer, value=-1]");

        // Note that SO_* options are hints, so we check that value is equals to either what we set or to default.
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";socketSendBuffer=1024")) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(1024, io.connectionProperties().getSocketSendBuffer());
                assertEquals(dfltDufSize, io.connectionProperties().getSocketReceiveBuffer());
            }
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";socketReceiveBuffer=1024")) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(dfltDufSize, io.connectionProperties().getSocketSendBuffer());
                assertEquals(1024, io.connectionProperties().getSocketReceiveBuffer());
            }
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";" +
            "socketSendBuffer=1024;socketReceiveBuffer=2048")) {
            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(1024, io.connectionProperties().getSocketSendBuffer());
                assertEquals(2048, io.connectionProperties().getSocketReceiveBuffer());
            }
        }
    }

    /**
     * Test update batch size property.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateBatchSize() throws Exception {
        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";updateBatchSize=-1",
            "Property cannot be lower than 1 [name=updateBatchSize, value=-1]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon)) {
            for (JdbcThinTcpIo io: ios(conn))
                assertNull(io.connectionProperties().getUpdateBatchSize());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon
            + ";updateBatchSize=1024")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertEquals(1024, (int)io.connectionProperties().getUpdateBatchSize());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp +
            "&updateBatchSize=1024")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertEquals(1024, (int)io.connectionProperties().getUpdateBatchSize());
        }
    }

    /**
     * Test partition awareness Sql cache size property.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionAwarenessSqlCacheSizeProperty() throws Exception {
        assertInvalid(urlWithPartitionAwarenessProp + "&partitionAwarenessSQLCacheSize=0",
            "Property cannot be lower than 1 [name=partitionAwarenessSQLCacheSize, value=0]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            for (JdbcThinTcpIo io : ios(conn))
                assertEquals(1_000, io.connectionProperties().getPartitionAwarenessSqlCacheSize());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp +
            "&partitionAwarenessSQLCacheSize=100")) {
            for (JdbcThinTcpIo io : ios(conn))
                assertEquals(100, io.connectionProperties().getPartitionAwarenessSqlCacheSize());
        }
    }

    /**
     * Test partition awareness Sql cache size property with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionAwarenessSqlCacheSizePropertySemicolon() throws Exception {
        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";partitionAwarenessSQLCacheSize=0",
            "Property cannot be lower than 1 [name=partitionAwarenessSQLCacheSize, value=0]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon)) {
            for (JdbcThinTcpIo io : ios(conn))
                assertEquals(1_000, io.connectionProperties().getPartitionAwarenessSqlCacheSize());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon +
            ";partitionAwarenessSQLCacheSize=100")) {
            for (JdbcThinTcpIo io : ios(conn))
                assertEquals(100, io.connectionProperties().getPartitionAwarenessSqlCacheSize());
        }
    }

    /**
     * Test partition awareness partition distributions cache size property.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionAwarenessPartitionDistributionsCacheSizeProperty() throws Exception {
        assertInvalid(urlWithPartitionAwarenessProp + "&partitionAwarenessPartitionDistributionsCacheSize=0",
            "Property cannot be lower than 1 [name=partitionAwarenessPartitionDistributionsCacheSize, value=0]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            for (JdbcThinTcpIo io : ios(conn)) {
                assertEquals(1_000,
                    io.connectionProperties().getPartitionAwarenessPartitionDistributionsCacheSize());
            }
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp +
            "&partitionAwarenessPartitionDistributionsCacheSize=100")) {
            for (JdbcThinTcpIo io : ios(conn))
                assertEquals(100, io.connectionProperties().getPartitionAwarenessPartitionDistributionsCacheSize());
        }
    }

    /**
     * Test partition awareness partition distributions cache size property with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionAwarenessPartitionDistributionsCacheSizePropertySemicolon() throws Exception {
        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";partitionAwarenessPartitionDistributionsCacheSize=0",
            "Property cannot be lower than 1 [name=partitionAwarenessPartitionDistributionsCacheSize, value=0]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon)) {
            for (JdbcThinTcpIo io : ios(conn)) {
                assertEquals(1_000,
                    io.connectionProperties().getPartitionAwarenessPartitionDistributionsCacheSize());
            }
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon +
            ";partitionAwarenessPartitionDistributionsCacheSize=100")) {
            for (JdbcThinTcpIo io : ios(conn))
                assertEquals(100, io.connectionProperties().getPartitionAwarenessPartitionDistributionsCacheSize());
        }
    }

    /**
     * Test SQL hints.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlHints() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assertHints(conn, false, false, false, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&distributedJoins=true")) {
            assertHints(conn, true, false, false, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&enforceJoinOrder=true")) {
            assertHints(conn, false, true, false, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&collocated=true")) {
            assertHints(conn, false, false, true, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&replicatedOnly=true")) {
            assertHints(conn, false, false, false, true, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&lazy=true")) {
            assertHints(conn, false, false, false, false, true,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&skipReducerOnUpdate=true")) {
            assertHints(conn, false, false, false, false, false,
                true, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&distributedJoins=true&" +
            "enforceJoinOrder=true&collocated=true&replicatedOnly=true&lazy=true&skipReducerOnUpdate=true")) {
            assertHints(conn, true, true, true, true, true,
                true, partitionAwareness);
        }
    }

    /**
     * Test SQL hints with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlHintsSemicolon() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";distributedJoins=true")) {
            assertHints(conn, true, false, false, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";enforceJoinOrder=true")) {
            assertHints(conn, false, true, false, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";collocated=true")) {
            assertHints(conn, false, false, true, false, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";replicatedOnly=true")) {
            assertHints(conn, false, false, false, true, false,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";lazy=true")) {
            assertHints(conn, false, false, false, false, true,
                false, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";skipReducerOnUpdate=true")) {
            assertHints(conn, false, false, false, false, false,
                true, partitionAwareness);
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";distributedJoins=true;" +
            "enforceJoinOrder=true;collocated=true;replicatedOnly=true;lazy=true;skipReducerOnUpdate=true")) {
            assertHints(conn, true, true, true, true, true,
                true, partitionAwareness);
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
        boolean replicatedOnly, boolean lazy, boolean skipReducerOnUpdate, boolean partitionAwarenessEnabled)throws Exception {
        for (JdbcThinTcpIo io: ios(conn)) {
            assertEquals(distributedJoins, io.connectionProperties().isDistributedJoins());
            assertEquals(enforceJoinOrder, io.connectionProperties().isEnforceJoinOrder());
            assertEquals(collocated, io.connectionProperties().isCollocated());
            assertEquals(replicatedOnly, io.connectionProperties().isReplicatedOnly());
            assertEquals(lazy, io.connectionProperties().isLazy());
            assertEquals(skipReducerOnUpdate, io.connectionProperties().isSkipReducerOnUpdate());
            assertEquals(partitionAwarenessEnabled, io.connectionProperties().isPartitionAwareness());
        }
    }

    /**
     * Test TCP no delay property handling.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTcpNoDelay() throws Exception {
        assertInvalid(urlWithPartitionAwarenessProp + "&tcpNoDelay=0",
            "Invalid property value. [name=tcpNoDelay, val=0, choices=[true, false]]");

        assertInvalid(urlWithPartitionAwarenessProp + "&tcpNoDelay=1",
            "Invalid property value. [name=tcpNoDelay, val=1, choices=[true, false]]");

        assertInvalid(urlWithPartitionAwarenessProp + "&tcpNoDelay=false1",
            "Invalid property value. [name=tcpNoDelay, val=false1, choices=[true, false]]");

        assertInvalid(urlWithPartitionAwarenessProp + "&tcpNoDelay=true1",
            "Invalid property value. [name=tcpNoDelay, val=true1, choices=[true, false]]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&tcpNoDelay=true")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&tcpNoDelay=True")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&tcpNoDelay=false")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp + "&tcpNoDelay=False")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isTcpNoDelay());
        }
    }

    /**
     * Test TCP no delay property handling with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTcpNoDelaySemicolon() throws Exception {
        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=0",
            "Invalid property value. [name=tcpNoDelay, val=0, choices=[true, false]]");

        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=1",
            "Invalid property value. [name=tcpNoDelay, val=1, choices=[true, false]]");

        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=false1",
            "Invalid property value. [name=tcpNoDelay, val=false1, choices=[true, false]]");

        assertInvalid(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=true1",
            "Invalid property value. [name=tcpNoDelay, val=true1, choices=[true, false]]");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=true")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=True")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=false")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";tcpNoDelay=False")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isTcpNoDelay());
        }
    }

    /**
     * Test autoCloseServerCursor property handling.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAutoCloseServerCursorProperty() throws Exception {
        String url = urlWithPartitionAwarenessProp + "&autoCloseServerCursor";

        String err = "Invalid property value. [name=autoCloseServerCursor";

        assertInvalid(url + "=0", err);
        assertInvalid(url + "=1", err);
        assertInvalid(url + "=false1", err);
        assertInvalid(url + "=true1", err);

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=true")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=True")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=false")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=False")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isAutoCloseServerCursor());
        }
    }

    /**
     * Test autoCloseServerCursor property handling with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAutoCloseServerCursorPropertySemicolon() throws Exception {
        String url = urlWithPartitionAwarenessPropSemicolon + ";autoCloseServerCursor";

        String err = "Invalid property value. [name=autoCloseServerCursor";

        assertInvalid(url + "=0", err);
        assertInvalid(url + "=1", err);
        assertInvalid(url + "=false1", err);
        assertInvalid(url + "=true1", err);

        try (Connection conn = DriverManager.getConnection(url + "=true")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=True")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=false")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isAutoCloseServerCursor());
        }

        try (Connection conn = DriverManager.getConnection(url + "=False")) {
            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isAutoCloseServerCursor());
        }
    }

    /**
     * Test schema property in URL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchema() throws Exception {
        assertInvalid(url + "/qwe/qwe",
            "Invalid URL format (only schema name is allowed in URL path parameter 'host:port[/schemaName]')" );

        try (Connection conn = DriverManager.getConnection(url + "/public")) {
            assertEquals("Invalid schema", "PUBLIC", conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection(url + "/\"" + DEFAULT_CACHE_NAME + '"')) {
            assertEquals("Invalid schema", DEFAULT_CACHE_NAME, conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection(url + "/_not_exist_schema_")) {
            assertEquals("Invalid schema", "_NOT_EXIST_SCHEMA_", conn.getSchema());
        }
    }

    /**
     * Test schema property in URL with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchemaSemicolon() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";schema=public")) {
            assertEquals("Invalid schema", "PUBLIC", conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";schema=\"" + DEFAULT_CACHE_NAME + '"')) {
            assertEquals("Invalid schema", DEFAULT_CACHE_NAME, conn.getSchema());
        }

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessPropSemicolon + ";schema=_not_exist_schema_")) {
            assertEquals("Invalid schema", "_NOT_EXIST_SCHEMA_", conn.getSchema());
        }
    }

    /**
     * Get client endpoints for connection.
     *
     * @param conn Connection.
     * @return Collection of endpoints.
     * @throws Exception If failed.
     */
    private static Collection<JdbcThinTcpIo> ios(Connection conn) throws Exception {
        JdbcThinConnection conn0 = conn.unwrap(JdbcThinConnection.class);

        Collection<JdbcThinTcpIo> ios = partitionAwareness ? ((Map<UUID, JdbcThinTcpIo>)
            getFieldValue(conn0, JdbcThinConnection.class, "ios")).values() :
            Collections.singleton(getFieldValue(conn0, JdbcThinConnection.class, "singleIo"));

        assert !ios.isEmpty();

        return ios;
    }

    /**
     * Assert that provided URL is invalid.
     *
     * @param url URL.
     * @param errMsg Error message.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertInvalid(final String url, String errMsg) {
        assertThrowsAnyCause(log, new Callable<Void>() {
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
    @Test
    public void testClose() throws Exception {
        final Connection conn;

        try (Connection conn0 = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            conn = conn0;

            assert conn != null;
            assert !conn.isClosed();
        }

        assert conn.isClosed();

        assert !conn.isValid(2) : "Connection must be closed";

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                conn.isValid(-2);

                return null;
            }
        }, SQLException.class, "Invalid timeout");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            try (Statement stmt = conn.createStatement()) {
                assertNotNull(stmt);

                stmt.close();

                conn.close();

                // Exception when called on closed connection
                checkConnectionClosed(new RunnableX() {
                    @Override public void runx() throws Exception {
                        conn.createStatement();
                    }
                });
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStatement2() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
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

                    assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.createStatement(TYPE_FORWARD_ONLY,
                        CONCUR_READ_ONLY);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int[] rsHoldabilities = new int[]
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

                        assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.createStatement(TYPE_FORWARD_ONLY,
                        CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // null query text
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.prepareStatement(sqlText);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final String sqlText = "select * from test where param = ?";

            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assert type == TYPE_FORWARD_ONLY;
                        assert concur == CONCUR_READ_ONLY;

                        // null query text
                        assertThrows(log,
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

                    assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                }
            });

            conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatement4() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final String sqlText = "select * from test where param = ?";

            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int[] rsHoldabilities = new int[]
                {HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    for (final int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assert type == TYPE_FORWARD_ONLY;
                            assert concur == CONCUR_READ_ONLY;

                            // null query text
                            assertThrows(log,
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

                        assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
                }
            });

            conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatementAutoGeneratedKeysUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final String sqlText = "insert into test (val) values (?)";

            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, RETURN_GENERATED_KEYS);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );

            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, NO_GENERATED_KEYS);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );

            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareStatement(sqlText, new int[] {1});
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Auto generated keys are not supported."
            );

            assertThrows(log,
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
    @Test
    public void testPrepareCallUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final String sqlText = "exec test()";

            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareCall(sqlText);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Callable functions are not supported."
            );

            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.prepareCall(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Callable functions are not supported."
            );

            assertThrows(log,
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
    @Test
    public void testNativeSql() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // null query text
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.nativeSQL(sqlText);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            boolean ac0 = conn.getAutoCommit();

            conn.setAutoCommit(!ac0);
            // assert no exception

            conn.setAutoCommit(ac0);
            // assert no exception

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setAutoCommit(ac0);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Should not be called in auto-commit mode
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.commit();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be committed explicitly in auto-commit mode"
            );

            assertTrue(conn.getAutoCommit());

            // Should not be called in auto-commit mode
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.commit();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Should not be called in auto-commit mode
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be rolled back explicitly in auto-commit mode."
            );

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.rollback();
                }
            });
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBeginFailsWhenMvccIsDisabled() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            conn.createStatement().execute("BEGIN");

            fail("Exception is expected");
        }
        catch (SQLException e) {
            assertEquals(TRANSACTION_STATE_EXCEPTION, e.getSQLState());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCommitIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("COMMIT");

            conn.commit();
        }
        // assert no exception
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRollbackIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            conn.setAutoCommit(false);

            conn.createStatement().execute("ROLLBACK");

            conn.rollback();
        }
        // assert no exception
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertNotNull(meta);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.getMetaData();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetReadOnly() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setReadOnly(true);
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.isReadOnly();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assert !conn.getMetaData().supportsCatalogsInDataManipulation();

            assertNull(conn.getCatalog());

            conn.setCatalog("catalog");

            assertEquals(null, conn.getCatalog());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setCatalog("");
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.getCatalog();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Invalid parameter value
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.getTransactionIsolation();
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearGetWarnings() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            SQLWarning warn = conn.getWarnings();

            assertNull(warn);

            conn.clearWarnings();

            warn = conn.getWarnings();

            assertNull(warn);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.getWarnings();
                }
            });

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.clearWarnings();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetTypeMap() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getTypeMap();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "Types mapping is not supported"
            );

            assertThrows(log,
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
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getTypeMap();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            // Exception when called on closed connection
            assertThrows(log,
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
    @Test
    public void testGetSetHoldability() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // default value
            assertEquals(conn.getMetaData().getResultSetHoldability(), conn.getHoldability());

            assertEquals(HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

            conn.setHoldability(CLOSE_CURSORS_AT_COMMIT);

            assertEquals(CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            // Invalid constant
            assertThrows(log,
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

            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.getHoldability();
                    }
                },
                SQLException.class,
                "Connection is closed"
            );

            assertThrows(log,
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
    @Test
    public void testSetSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Disallowed in auto-commit mode
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint();

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be set in auto-commit mode"
            );

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setSavepoint();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetSavepointName() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            assertThrows(log,
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
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(name);

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be set in auto-commit mode"
            );

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setSavepoint(name);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackSavePoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            assertThrows(log,
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
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(savepoint);

                        return null;
                    }
                },
                SQLException.class,
                "Auto-commit mode"
            );

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.rollback(savepoint);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisabledFeatures() throws Exception {
        assertInvalid(url + "?disabledFeatures=unknownFeature",
            "Unknown feature: unknownFeature");

        try (Connection conn = DriverManager.getConnection(url + "?disabledFeatures=reserved")) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.releaseSavepoint(savepoint);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.releaseSavepoint(savepoint);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Unsupported
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createClob();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(log,
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
    @Test
    public void testCreateBlob() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Unsupported
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createBlob();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(log,
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
    @Test
    public void testCreateNClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Unsupported
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createNClob();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(log,
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
    @Test
    public void testCreateSQLXML() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Unsupported
            assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return conn.createSQLXML();
                    }
                },
                SQLFeatureNotSupportedException.class,
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(log,
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
    @Test
    public void testGetSetClientInfoPair() throws Exception {
//        fail("https://issues.apache.org/jira/browse/IGNITE-5425");

        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            assertNull(conn.getWarnings());

            conn.setClientInfo(name, val);

            assertNull(conn.getClientInfo(val));

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.getClientInfo(name);
                }
            });

            assertThrows(log,
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
    @Test
    public void testGetSetClientInfoProperties() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
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
                @Override public void runx() throws Exception {
                    conn.getClientInfo();
                }
            });

            assertThrows(log,
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
    @Test
    public void testCreateArrayOf() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final String typeName = "varchar";

            final String[] elements = new String[] {"apple", "pear"};

            // Invalid typename
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.createArrayOf(typeName, elements);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.createArrayOf(typeName, elements);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStruct() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // Invalid typename
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.createStruct(typeName, attrs);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.createStruct(typeName, attrs);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            assertEquals("PUBLIC", conn.getSchema());

            final String schema = "test";

            conn.setSchema(schema);

            assertEquals(schema.toUpperCase(), conn.getSchema());

            conn.setSchema('"' + schema + '"');

            assertEquals(schema, conn.getSchema());

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setSchema(schema);
                }
            });

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.getSchema();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAbort() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            //Invalid executor
            assertThrows(log,
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
    @Test
    public void testGetSetNetworkTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            // default
            assertEquals(0, conn.getNetworkTimeout());

            final Executor executor = Executors.newFixedThreadPool(1);

            final int timeout = 1000;

            //Invalid timeout
            assertThrows(log,
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
                @Override public void runx() throws Exception {
                    conn.getNetworkTimeout();
                }
            });

            checkConnectionClosed(new RunnableX() {
                @Override public void runx() throws Exception {
                    conn.setNetworkTimeout(executor, timeout);
                }
            });
        }
    }

    /**
     * Test that attempting to supply invalid nested TX mode to driver fails on the client.
     */
    @Test
    public void testInvalidNestedTxMode() {
        assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                DriverManager.getConnection(urlWithPartitionAwarenessProp + "&nestedTransactionsMode=invalid");

                return null;
            }
        }, SQLException.class, "Invalid nested transactions handling mode");
    }

    /**
     * Test that attempting to send unexpected name of nested TX mode to server on handshake yields an error.
     * We have to do this without explicit {@link Connection} as long as there's no other way to bypass validation and
     * supply a malformed {@link ConnectionProperties} to {@link JdbcThinTcpIo}.
     */
    @Test
    public void testInvalidNestedTxModeOnServerSide() {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.setAddresses(new HostAndPortRange[] {new HostAndPortRange(LOCALHOST, DFLT_PORT, DFLT_PORT)});

        connProps.nestedTxMode("invalid");

        connProps.setPartitionAwareness(partitionAwareness);

        assertThrows(null, new Callable<Object>() {
            @SuppressWarnings("ResultOfObjectAllocationIgnored")
            @Override public Object call() throws Exception {
                new JdbcThinTcpIo(connProps, new InetSocketAddress(LOCALHOST, DFLT_PORT), getBinaryContext(), 0);

                return null;
            }
        }, SQLException.class, "err=Invalid nested transactions handling mode: invalid");
    }

    /**
     */
    @Test
    public void testSslClientAndPlainServer() {
        Throwable e = assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                DriverManager.getConnection(urlWithPartitionAwarenessProp + "&sslMode=require" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + SRV_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");

                return null;
            }
        }, SQLException.class, partitionAwareness ? "Failed to connect to server" : "Failed to SSL connect to server");

        if (partitionAwareness) {
            for (Throwable t: e.getSuppressed()) {
                assertEquals(SQLException.class, t.getClass());
                assertTrue(t.getMessage().contains("Failed to SSL connect to server"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadingException() throws Exception {
        int threadCnt = 10;

        final boolean end[] = new boolean[] {false};

        final SQLException exs[] = new SQLException[threadCnt];

        final AtomicInteger exCnt = new AtomicInteger(0);

        try (final Connection conn = DriverManager.getConnection(urlWithPartitionAwarenessProp)) {
            final IgniteInternalFuture f = runMultiThreadedAsync(new Runnable() {
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

    /**
     * Returns stub for marshaller context.
     *
     * @return Marshaller context.
     */
    private MarshallerContext getFakeMarshallerCtx() {
        return new MarshallerContext() {
            @Override public boolean registerClassName(byte platformId, int typeId,
                String clsName) throws IgniteCheckedException {
                return false;
            }

            @Override public boolean registerClassNameLocally(byte platformId, int typeId,
                String clsName) throws IgniteCheckedException {
                return false;
            }

            @Override public Class getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException {
                return null;
            }

            @Override public String getClassName(byte platformId,
                int typeId) throws ClassNotFoundException, IgniteCheckedException {
                return null;
            }

            @Override public boolean isSystemType(String typeName) {
                return false;
            }

            @Override public IgnitePredicate<String> classNameFilter() {
                return null;
            }

            @Override public JdkMarshaller jdkMarshaller() {
                return null;
            }
        };
    }

    /**
     * Returns new binary context.
     *
     * @return New binary context.
     */
    private BinaryContext getBinaryContext() {
        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(getFakeMarshallerCtx());

        BinaryContext ctx = new BinaryContext(BinaryNoopMetadataHandler.instance(),
            new IgniteConfiguration(), new NullLogger());

        ctx.configure(marsh);
        ctx.registerUserTypesSchema();

        return ctx;
    }
}
