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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.Executor;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Jdbc Thin Connection timeout tests.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionTimeoutSelfTest extends JdbcThinAbstractSelfTest {

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Server thread pull size. */
    private static final int SERVER_THREAD_POOL_SIZE = 4;

    /** Nodes count. */
    private static final byte NODES_COUNT = 3;

    /** Max table rows. */
    private static final int MAX_ROWS = 10000;

    /** Executor stub */
    private static final Executor EXECUTOR_STUB = (Runnable command) -> {};

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setSqlFunctionClasses(JdbcThinConnectionTimeoutSelfTest.class);
        cache.setIndexedTypes(Integer.class, Integer.class);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().setThreadPoolSize(SERVER_THREAD_POOL_SIZE));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES_COUNT);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);
    }

    /**
     * Called before execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @Before
    public void before() throws Exception {
        conn = DriverManager.getConnection(URL);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /**
     * Called after execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @After
    public void after() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }

    /**
     *
     */
    @Test
    public void testSettingNegativeConnectionTimeout() {
        GridTestUtils.assertThrows(log,
            () -> {
                conn.setNetworkTimeout(EXECUTOR_STUB, -1);
                return null;
            },
            SQLException.class, "Network timeout cannot be negative.");
    }

    /**
     *
     */
    @Test
    public void testNegativeConnectionTimeout() {
        GridTestUtils.assertThrows(log,
            () -> {
                try (final Connection conn = DriverManager.getConnection(URL + "?connectionTimeout=-1")) {
                    return null;
                }
            },
            SQLException.class, "Property cannot be lower than 0 [name=connectionTimeout, value=-1]");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionTimeoutRetrieval() throws Exception {
        try (final Connection conn = DriverManager.getConnection(URL + "?connectionTimeout=1000")) {
            assertEquals(1000, conn.getNetworkTimeout());

            conn.setNetworkTimeout(EXECUTOR_STUB, 2000);

            assertEquals(2000, conn.getNetworkTimeout());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionTimeout() throws Exception {
        conn.setNetworkTimeout(EXECUTOR_STUB, 1000);

        GridTestUtils.assertThrows(log,
            () -> {
                stmt.execute("select sleep_func(2000)");
                return null;
            },
            SQLException.class, "Connection timed out.");

        GridTestUtils.assertThrows(log,
            () -> {
                stmt.execute("select 1");
                return null;
            },
            SQLException.class, "Statement is closed.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUrlConnectionTimeoutProperty() throws Exception {
        try (final Connection conn = DriverManager.getConnection(URL + "?connectionTimeout=1000")) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            try (final Statement stmt = conn.createStatement()) {
                GridTestUtils.assertThrows(log,
                    () -> {
                        stmt.execute("select sleep_func(2000)");

                        return null;
                    },
                    SQLException.class, "Connection timed out.");

                GridTestUtils.assertThrows(log,
                    () -> {
                        stmt.execute("select 1");

                        return null;
                    },
                    SQLException.class, "Statement is closed.");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryTimeoutOccursBeforeConnectionTimeout() throws Exception {
        conn.setNetworkTimeout(EXECUTOR_STUB, 10_000);

        stmt.setQueryTimeout(1);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select sleep_func(3) from Integer;");

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");

        stmt.execute("select 1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUrlQueryTimeoutProperty() throws Exception {
        try (final Connection conn = DriverManager.getConnection(URL + "?connectionTimeout=10000&queryTimeout=1")) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            final Statement stmt = conn.createStatement();

            GridTestUtils.assertThrows(log, () -> {
                stmt.executeQuery("select sleep_func(3) from Integer;");

                return null;
            }, SQLTimeoutException.class, "The query was cancelled while executing.");

            stmt.execute("select 1");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionTimeoutUpdate() throws Exception {
        try (final Connection conn = DriverManager.getConnection(URL +
            "?connectionTimeout=5000")) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            final Statement stmt = conn.createStatement();

            stmt.execute("select sleep_func(1000)");

            conn.setNetworkTimeout(EXECUTOR_STUB, 500);

            GridTestUtils.assertThrows(log, () -> {
                stmt.execute("select sleep_func(1000)");

                return null;
            }, SQLException.class, "Connection timed out.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelingTimedOutStatement() throws Exception {
        conn.setNetworkTimeout(EXECUTOR_STUB, 1);

        GridTestUtils.runAsync(
            () -> {
                try {
                    Thread.sleep(1000);

                    GridTestUtils.assertThrows(log,
                        () -> {
                            stmt.cancel();

                            return null;
                        },
                        SQLException.class, "Statement is closed.");
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    fail("Unexpected exception");
                }
            });

        GridTestUtils.runAsync(() -> {
            try {
                GridTestUtils.assertThrows(log,
                    () -> {
                        stmt.execute("select sleep_func(1000)");
                        return null;
                    },
                    SQLException.class, "Connection timed out.");
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        });
    }

    /**
     * @param v amount of milliseconds to sleep
     * @return amount of milliseconds to sleep
     */
    @SuppressWarnings("unused")
    @QuerySqlFunction
    public static int sleep_func(int v) {
        try {
            Thread.sleep(v);
        }
        catch (InterruptedException ignored) {
            // No-op
        }
        return v;
    }
}
