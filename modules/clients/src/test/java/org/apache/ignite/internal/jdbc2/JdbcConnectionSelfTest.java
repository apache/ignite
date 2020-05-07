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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Connection test.
 */
public class JdbcConnectionSelfTest extends GridCommonAbstractTest {
    /** Custom cache name. */
    private static final String CUSTOM_CACHE_NAME = "custom-cache";

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Daemon node flag. */
    private boolean daemon;

    /**
     * @return Config URL to use in test.
     */
    protected String configURL() {
        return "modules/clients/src/test/config/jdbc-config.xml";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME), cacheConfiguration(CUSTOM_CACHE_NAME));

        cfg.setDaemon(daemon);

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
        startGridsMultiThreaded(GRID_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaults() throws Exception {
        String url = CFG_URL_PREFIX + configURL();

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
            assertTrue(((JdbcConnection)conn).ignite().configuration().isClientMode());
        }

        try (Connection conn = DriverManager.getConnection(url + '/')) {
            assertNotNull(conn);
            assertTrue(((JdbcConnection)conn).ignite().configuration().isClientMode());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeId() throws Exception {
        String url = CFG_URL_PREFIX + "nodeId=" + grid(0).localNode().id() + '@' + configURL();

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }

        url = CFG_URL_PREFIX + "cache=" + CUSTOM_CACHE_NAME + ":nodeId=" + grid(0).localNode().id() + '@' + configURL();

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongNodeId() throws Exception {
        UUID wrongId = UUID.randomUUID();

        final String url = CFG_URL_PREFIX + "nodeId=" + wrongId + '@' + configURL();

        GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Connection conn = DriverManager.getConnection(url)) {
                            return conn;
                        }
                    }
                },
                SQLException.class,
                "Failed to establish connection with node (is it a server node?): " + wrongId
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeId() throws Exception {
        IgniteEx client = startClientGrid();

        UUID clientId = client.localNode().id();

        final String url = CFG_URL_PREFIX + "nodeId=" + clientId + '@' + configURL();

        GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Connection conn = DriverManager.getConnection(url)) {
                            return conn;
                        }
                    }
                },
                SQLException.class,
                "Failed to establish connection with node (is it a server node?): " + clientId
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDaemonNodeId() throws Exception {
        daemon = true;

        IgniteEx daemon = startGrid(GRID_CNT);

        UUID daemonId = daemon.localNode().id();

        final String url = CFG_URL_PREFIX + "nodeId=" + daemonId + '@' + configURL();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Connection conn = DriverManager.getConnection(url)) {
                        return conn;
                    }
                }
            },
            SQLException.class,
            "Failed to establish connection with node (is it a server node?): " + daemonId
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomCache() throws Exception {
        String url = CFG_URL_PREFIX + "cache=" + CUSTOM_CACHE_NAME + '@' + configURL();

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongCache() throws Exception {
        final String url = CFG_URL_PREFIX + "cache=wrongCacheName@" + configURL();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Connection conn = DriverManager.getConnection(url)) {
                        return conn;
                    }
                }
            },
            SQLException.class,
            "Client is invalid. Probably cache name is wrong."
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClose() throws Exception {
        String url = CFG_URL_PREFIX + configURL();

        try (final Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());

            conn.close();

            assertTrue(conn.isClosed());

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.isValid(2);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed."
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxAllowedCommit() throws Exception {
        String url = CFG_URL_PREFIX + "transactionsAllowed=true@" + configURL();

        try (final Connection conn = DriverManager.getConnection(url)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());

            conn.setAutoCommit(false);

            conn.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxAllowedRollback() throws Exception {
        String url = CFG_URL_PREFIX + "transactionsAllowed=true@" + configURL();

        try (final Connection conn = DriverManager.getConnection(url)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());

            conn.setAutoCommit(false);

            conn.rollback();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlHints() throws Exception {
        try (final Connection conn = DriverManager.getConnection(CFG_URL_PREFIX + "enforceJoinOrder=true@"
            + configURL())) {
            assertTrue(((JdbcConnection)conn).isEnforceJoinOrder());
            assertFalse(((JdbcConnection)conn).isDistributedJoins());
            assertFalse(((JdbcConnection)conn).isCollocatedQuery());
            assertFalse(((JdbcConnection)conn).isLazy());
            assertFalse(((JdbcConnection)conn).skipReducerOnUpdate());
        }

        try (final Connection conn = DriverManager.getConnection(CFG_URL_PREFIX + "distributedJoins=true@"
            + configURL())) {
            assertFalse(((JdbcConnection)conn).isEnforceJoinOrder());
            assertTrue(((JdbcConnection)conn).isDistributedJoins());
            assertFalse(((JdbcConnection)conn).isCollocatedQuery());
            assertFalse(((JdbcConnection)conn).isLazy());
            assertFalse(((JdbcConnection)conn).skipReducerOnUpdate());
        }

        try (final Connection conn = DriverManager.getConnection(CFG_URL_PREFIX + "collocated=true@"
            + configURL())) {
            assertFalse(((JdbcConnection)conn).isEnforceJoinOrder());
            assertFalse(((JdbcConnection)conn).isDistributedJoins());
            assertTrue(((JdbcConnection)conn).isCollocatedQuery());
            assertFalse(((JdbcConnection)conn).isLazy());
            assertFalse(((JdbcConnection)conn).skipReducerOnUpdate());
        }

        try (final Connection conn = DriverManager.getConnection(CFG_URL_PREFIX + "lazy=true@" + configURL())) {
            assertFalse(((JdbcConnection)conn).isEnforceJoinOrder());
            assertFalse(((JdbcConnection)conn).isDistributedJoins());
            assertFalse(((JdbcConnection)conn).isCollocatedQuery());
            assertTrue(((JdbcConnection)conn).isLazy());
            assertFalse(((JdbcConnection)conn).skipReducerOnUpdate());
        }
        try (final Connection conn = DriverManager.getConnection(CFG_URL_PREFIX + "skipReducerOnUpdate=true@"
            + configURL())) {
            assertFalse(((JdbcConnection)conn).isEnforceJoinOrder());
            assertFalse(((JdbcConnection)conn).isDistributedJoins());
            assertFalse(((JdbcConnection)conn).isCollocatedQuery());
            assertFalse(((JdbcConnection)conn).isLazy());
            assertTrue(((JdbcConnection)conn).skipReducerOnUpdate());
        }
    }
}
