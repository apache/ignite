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

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
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
 * Statement timeout test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinStatementTimeoutSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Server thread pull size. */
    private static final int SERVER_THREAD_POOL_SIZE = 4;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setSqlFunctionClasses(TestSQLFunctions.class);
        cache.setIndexedTypes(Integer.class, Integer.class, Long.class, Long.class, String.class,
            JdbcThinAbstractDmlStatementSelfTest.Person.class);

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

        startGridsMultiThreaded(3);

        for (int i = 0; i < 10000; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        for (int i = 0; i < 10000; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put((long)i, (long)i);
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
     * Trying to set negative timeout. <code>SQLException</> with message "Invalid timeout value." is expected.
     */
    @Test
    public void testSettingNegativeQueryTimeout() {
        GridTestUtils.assertThrows(log, () -> {
            stmt.setQueryTimeout(-1);

            return null;
        }, SQLException.class, "Invalid timeout value.");
    }

    /**
     * Trying to set negative timeout. <code>SQLException</> with message "Invalid timeout value." is expected.
     * @throws SQLException If failed.
     */
    @Test
    public void testNegativeQueryTimeout() throws SQLException {
        GridTestUtils.assertThrows(log, () -> {
            try (final Connection conn = DriverManager.getConnection(URL + "?queryTimeout=-1")) {
                try (final Statement stmt = conn.createStatement()) {
                    return null;
                }
            }
        }, SQLException.class, "Property cannot be lower than 0 [name=queryTimeout, value=-1]");
    }

    /**
     * Trying to set zero timeout. Zero timeout means no timeout, so no exception is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSettingZeroQueryTimeout() throws Exception {
        stmt.setQueryTimeout(0);

        stmt.executeQuery("select sleep_func(1000);");
    }

    /**
     * Trying to set zero timeout. Zero timeout means no timeout, so no exception is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testZeroQueryTimeout() throws Exception {
        try (final Connection conn = DriverManager.getConnection(URL + "?queryTimeout=0")) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            try (final Statement stmt = conn.createStatement()) {
                stmt.executeQuery("select sleep_func(1000);");
            }
        }
    }

    /**
     * Setting timeout that is greater than query execution time. <code>SQLTimeoutException</code> is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryTimeout() throws Exception {
        stmt.setQueryTimeout(2);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select sleep_func(10) from Integer;");

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryTimeoutRetrival() throws Exception {
        try (final Connection conn = DriverManager.getConnection(URL + "?queryTimeout=5")) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            try (final Statement stmt = conn.createStatement()) {
                stmt.executeQuery("select sleep_func(2);");
            }

            try (final Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(1);

                GridTestUtils.assertThrows(log, () -> {
                    // This takes 10_000 ms.
                    stmt.executeQuery("select sleep_func(2) from Integer;");

                    return null;
                }, SQLTimeoutException.class, "The query was cancelled while executing.");
            }
        }
    }

    /**
     * Setting timeout that is greater than query execution time. Running same query multiple times.
     * <code>SQLTimeoutException</code> is expected in all cases.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testQueryTimeoutRepeatable() throws Exception {
        stmt.setQueryTimeout(2);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select sleep_func(5) from Integer;");

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select sleep_func(5) from Integer;");

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");

        stmt.executeQuery("select sleep_func(50)");
    }

    /**
     * Setting timeout that is greater than file uploading execution time.
     * <code>SQLTimeoutException</code> is expected.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFileUploadingTimeout() throws Exception {

        File file = File.createTempFile("bulkload", "csv");

        FileWriter writer = new FileWriter(file);

        for (int i = 1; i <= 1_000_000; i++)
            writer.write(String.format("%d,%d,\"FirstName%d MiddleName%d\",LastName%d", i, i, i, i, i));

        writer.close();

        stmt.setQueryTimeout(1);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeUpdate(
                "copy from '" + file.getAbsolutePath() + "' into Person" +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");
    }

    /**
     * Setting timeout that is greater than batch query execution time.
     * <code>SQLTimeoutException</code> is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatchQuery() throws Exception {
        stmt.setQueryTimeout(1);

        GridTestUtils.assertThrows(log, () -> {
            stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
            stmt.addBatch("update Long set _val = _val + 1 where _key > sleep_func (10)");

            stmt.executeBatch();

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");
    }

    /**
     * Setting timeout that is greater than multiple statements query execution time.
     * <code>SQLTimeoutException</code> is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleStatementsQuery() throws Exception {
        stmt.setQueryTimeout(1);

        GridTestUtils.assertThrows(log, () -> {
            stmt.execute(
                "update Long set _val = _val + 1 where _key > sleep_func (10);"
                    + "update Long set _val = _val + 1 where _key > sleep_func (10);"
                    + "update Long set _val = _val + 1 where _key > sleep_func (10);"
                    + "update Long set _val = _val + 1 where _key > sleep_func (10);"
                    + "select _val, sleep_func(10) as s from Integer limit 10");

            return null;
        }, SQLTimeoutException.class, "The query was cancelled while executing.");
    }

    /**
     * Setting timeout that is greater than update query execution time.
     * <code>SQLTimeoutException</code> is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteUpdateTimeout() throws Exception {
        stmt.setQueryTimeout(1);

        GridTestUtils.assertThrows(log, () ->
                stmt.executeUpdate("update Integer set _val=1 where _key > sleep_func(10)"),
            SQLTimeoutException.class, "The query was cancelled while executing.");
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
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
}
