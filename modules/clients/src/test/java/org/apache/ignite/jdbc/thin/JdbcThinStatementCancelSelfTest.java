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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Statement cancel test.
 */
@SuppressWarnings({"ThrowableNotThrown", "AssertWithSideEffects"})
@RunWith(JUnit4.class)
public class JdbcThinStatementCancelSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** A CSV file with one record. */
    private static final String BULKLOAD_20_000_LINE_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath("/modules/clients/src/test/resources/bulkload20_000.csv")).
            getAbsolutePath();

    /** Max table rows. */
    private static final int MAX_ROWS = 100;

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

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().
            setThreadPoolSize(SERVER_THREAD_POOL_SIZE));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put((long)i, (long)i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }

    /**
     * Test canceling statement without query.
     */
    @Test
    public void testCancelingStmtWithoutQuery() {
        try {
            stmt.cancel();
        }
        catch (Exception e) {
            log.error("Unexpected exception.", e);

            fail("Unexpected exception");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResultSetRetrievalInCanceledStatement() throws Exception {
        stmt.execute("SELECT 1; SELECT 2; SELECT 3;");

        assertNotNull(stmt.getResultSet());

        stmt.cancel();

        GridTestUtils.assertThrows(log, () -> {
            stmt.getResultSet();

            return null;
        }, SQLException.class, "The query was cancelled while executing.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelClosedStmt() throws Exception {
        stmt.close();

        GridTestUtils.assertThrows(log, () -> {
            stmt.cancel();

            return null;
        }, SQLException.class, "Statement is closed.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResultSetNextAfterCanceling() throws Exception {
        stmt.setFetchSize(10);

        ResultSet rs = stmt.executeQuery("select * from Integer");

        assert rs.next();

        stmt.cancel();

        GridTestUtils.assertThrows(log, () -> {
            rs.next();

            return null;
        }, SQLException.class, "The query was cancelled while executing.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAnotherStmt() throws Exception {
        stmt.setFetchSize(10);

        ResultSet rs = stmt.executeQuery("select * from Integer");

        assert rs.next();

        stmt.cancel();

        ResultSet rs2 = stmt.executeQuery("select * from Integer order by _val");

        assert rs2.next() : "The other cursor mustn't be closed";
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAnotherStmtResultSet() throws Exception {
        try (Statement anotherStmt = conn.createStatement()) {
            ResultSet rs1 = stmt.executeQuery("select * from Integer WHERE _key % 2 = 0");

            ResultSet rs2 = anotherStmt.executeQuery("select * from Integer  WHERE _key % 2 <> 0");

            stmt.cancel();

            GridTestUtils.assertThrows(log, () -> {
                rs1.next();

                return null;
            }, SQLException.class, "The query was cancelled while executing.");

            assert rs2.next() : "The other cursor mustn't be closed";
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCancelLongRunningQueryBasedOnJoins() {
        GridTestUtils.runAsync(() -> {
            try {
                TestSQLFunctions.cancelLatch.await();

                stmt.cancel();

                TestSQLFunctions.reqLatch.countDown();
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        });

//        long startTime = System.currentTimeMillis();
//        try {
//            stmt.executeQuery("SELECT * FROM Integer I1 join Integer I2 join Integer I3 join Integer I4 join Integer I5 join Integer I6 join Integer I7 join Integer I8 WHERE awaitLatch() = 0");
//        }
//        catch (SQLException e) {
//            System.out.println(">>>>>>>>");
//        }
//        System.out.println("!!!!!!!!!!!!!" + (System.currentTimeMillis() - startTime));

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("SELECT * FROM Integer WHERE awaitLatch() = 0");
//            stmt.executeQuery("SELECT * FROM Integer I1 join Integer I2 join Integer I3 join Integer I4 WHERE awaitLatch() = 0");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCancelMultipleStatementsQuery() throws Exception {
        try (Statement anotherStatment = conn.createStatement()){
            anotherStatment.setFetchSize(1);
            // Open the second cursor
            ResultSet rs = anotherStatment.executeQuery("select * from Integer");

            assert rs.next();

            GridTestUtils.runAsync(() -> {
                try {
                    Thread.sleep(500);
                    stmt.cancel();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);
                    fail("Unexpected exception");
                }
            });

            IgniteInternalFuture<Object> res = GridTestUtils.runAsync(() -> {
                GridTestUtils.assertThrows(log, () -> {
                    // Execute long running query
                    stmt.execute(
                        "select 100 from Integer I1 join Integer I2 join Integer I3 join Integer I4;"
                            + "select 100 from Integer I1 join Integer I2 join Integer I3 join Integer I4;"
                            + "select 100 from Integer I1 join Integer I2 join Integer I3 join Integer I4;"
                            + "select 100 from Integer I1 join Integer I2 join Integer I3 join Integer I4;");
                    return null;
                }, SQLException.class, "The query was cancelled while executing");
            });

            res.get(1500, TimeUnit.MILLISECONDS);

            assert rs.next() : "The other cursor mustn't be closed";
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelBatchQuery() throws Exception {
        try (Statement stmt2 = conn.createStatement()) {
            stmt2.setFetchSize(1);

            // Open the second cursor
            ResultSet rs = stmt2.executeQuery("SELECT * from Integer");

            assert rs.next();

            GridTestUtils.runAsync(() -> {
                try {
                    Thread.sleep(1000);
                    stmt.cancel();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);
                    fail("Unexpected exception");
                }
            });

            GridTestUtils.assertThrows(log, () -> {
                // Execute long running query
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.executeBatch();
                return null;
            }, java.sql.SQLException.class, "The query was cancelled while executing");

            assert rs.next() : "The other cursor mustn't be closed";
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCancelAgainstFullServerThreadPool() throws Exception {
        List<Statement> statements = Collections.synchronizedList(new ArrayList<>());
        List<Connection> connections = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++) {
            Connection yaConn = DriverManager.getConnection(URL);

            yaConn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            connections.add(yaConn);

            Statement yaStmt = yaConn.createStatement();

            statements.add(yaStmt);
        }

        try {
            GridTestUtils.runAsync(() -> {
                try {
                    Thread.sleep(500);

                    for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++)
                        statements.get(i).cancel();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    fail("Unexpected exception");
                }
            });

            IgniteInternalFuture<Object> res = null;
            for (int i = 0; i < SERVER_THREAD_POOL_SIZE - 1; i++) {
                final int statementIdx = i;
                res = GridTestUtils.runAsync(() -> {
                    GridTestUtils.assertThrows(log, () -> {
                        statements.get(statementIdx).executeQuery("select 100 from Integer I1 join Integer I2" +
                            " join Integer I3 join Integer I4 join Integer I5;");

                        return null;
                    }, SQLException.class, "The query was cancelled while executing.");
                });
            }

            res.get(2, TimeUnit.SECONDS);
        }
        finally {
            for (Statement statement : statements)
                statement.close();

            for (Connection connection : connections)
                connection.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCancelFetchAgainstFullServerThreadPool() throws Exception {
        stmt.setFetchSize(1);

        ResultSet rs = stmt.executeQuery("SELECT * from Integer");

        rs.next();

        List<Statement> statements = Collections.synchronizedList(new ArrayList<>());
        List<Connection> connections = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++) {
            Connection yaConn = DriverManager.getConnection(URL);

            yaConn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            connections.add(yaConn);

            Statement yaStmt = yaConn.createStatement();

            statements.add(yaStmt);
        }

        try {
            for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++) {
                final int statementIdx = i;
                GridTestUtils.runAsync(() -> {
                    GridTestUtils.assertThrows(log, () -> {
                        statements.get(statementIdx).executeQuery("select 100 from Integer I1 join Integer I2" +
                            " join Integer I3 join Integer I4 join Integer I5;");

                        return null;
                    }, SQLException.class, "The query was cancelled while executing.");
                });
            }

            IgniteInternalFuture<Object> res = GridTestUtils.runAsync(() -> {
                GridTestUtils.assertThrows(log, () -> {
                    rs.next();

                    return null;
                }, SQLException.class, "The query was cancelled while executing.");
            });

            Thread.sleep(100);

            stmt.cancel();

            for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++)
                statements.get(i).cancel();

            res.get(2, TimeUnit.SECONDS);
        }
        finally {
            for (Statement statement : statements)
                statement.close();

            for (Connection connection : connections)
                connection.close();
        }
    }

    /**
     * Test cancelling long running file upload.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCancellingLongRunningFileUpload() {
        GridTestUtils.runAsync(() -> {
            try {
                Thread.sleep(200);

                stmt.cancel();
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        });

        GridTestUtils.runAsync(() -> {
            GridTestUtils.assertThrows(log, () -> {
                stmt.executeUpdate(
                    "copy from '" + BULKLOAD_20_000_LINE_CSV_FILE + "' into Person" +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

                return null;
            }, SQLException.class, "The query was cancelled while executing.");
        });
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /** Request latch. */
        static CountDownLatch reqLatch = new CountDownLatch(1);

        /** Cancel latch. */
        static CountDownLatch cancelLatch = new CountDownLatch(1);

        /**
         * Await cyclic barrier twice, first time to wait for enter method, second time to wait for collecting running
         * queries.
         */
        @QuerySqlFunction
        public static long awaitLatch() {
            try {
                cancelLatch.countDown();
                reqLatch.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }

        /**
         *
         * @param v amount of milliseconds to sleep
         * @return amount of milliseconds to sleep
         */
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