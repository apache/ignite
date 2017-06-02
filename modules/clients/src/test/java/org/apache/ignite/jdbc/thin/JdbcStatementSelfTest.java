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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Statement test
 */
public class JdbcStatementSelfTest extends GridCommonAbstractTest {

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL prefix. */
    private static final String URL_PREFIX = "jdbc:ignite:thin://";

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setOdbcConfiguration(new OdbcConfiguration());

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

        cfg.setSqlFunctionClasses(getClass());

        cfg.setIndexedTypes(Integer.class, Test.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        try {
            Driver drv = DriverManager.getDriver("jdbc:ignite://");

            if (drv != null)
                DriverManager.deregisterDriver(drv);
        } catch (SQLException ignored) {
            // No-op.
        }

        startGridsMultiThreaded(2);

        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        fillCache();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQuery() throws Exception {
        final String sqlText = "select * from test";

        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSchema(DEFAULT_CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sqlText)) {
                    assertNotNull(rs);

                    assertTrue(rs.next());

                    assertEquals(1, rs.getInt(1));
                }

                stmt.close();

                // Call on a closed statement
                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return stmt.executeQuery(sqlText);
                        }
                    },
                    SQLException.class,
                    "Statement is closed"
                );
            }
        }
        //TODO: cannot be called on PreparedStatement
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQueryTimeout() throws Exception {
        final String sqlText = "select sleep_func(5)";

        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSchema(DEFAULT_CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(1);

                // Timeout
                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return stmt.executeQuery(sqlText);
                        }
                    },
                    SQLTimeoutException.class,
                    "Timeout"
                );
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQueryMultipleResultSets() throws Exception {
        final String sqlText = "select * from test; select * from test";

        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSchema(DEFAULT_CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return stmt.executeQuery(sqlText);
                        }
                    },
                    SQLException.class,
                    "Multiple result sets"
                );
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdate() throws Exception {
        final String sqlText = "update test set val=1 where _key=1";

        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSchema(DEFAULT_CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                assertEquals(1, stmt.executeUpdate(sqlText));

                stmt.close();

                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return stmt.executeUpdate(sqlText);
                        }
                    },
                    SQLException.class,
                    "Closed statement"
                );
            }
        }

        // TODO: Cannot be called on PreparedStatement
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdateProducesResultSet() throws Exception {
        final String sqlText = "select * from test";

        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSchema(DEFAULT_CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return stmt.executeUpdate(sqlText);
                        }
                    },
                    SQLException.class,
                    "Produces result set"
                );
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdateTimeout() throws Exception {
        final String sqlText = "update test set val=1 where _key=sleep_func(5)";

        try (Connection conn = DriverManager.getConnection(URL_PREFIX + HOST)) {
            conn.setSchema(DEFAULT_CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(1);

                // Timeout
                GridTestUtils.assertThrows(log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return stmt.executeUpdate(sqlText);
                        }
                    },
                    SQLTimeoutException.class,
                    "Timeout"
                );
            }
        }
    }

    /** */
    private void fillCache() {
        grid(0).cache(DEFAULT_CACHE_NAME).put(1, new Test(1));
    }

    /** */
    public static class Test {
        @QuerySqlField
        private int val;

        /** */
        public Test(int val) {
            this.val = val;
        }
    }

    /**
     *
     * @param v seconds to sleep
     * @return passed value
     */
    @SuppressWarnings("unused")
    @QuerySqlFunction
    public static int sleep_func(int v) {
        try {
            Thread.sleep(v * 1000);
        }
        catch (InterruptedException ignored) {
            // No-op
        }
        return v;
    }
}
