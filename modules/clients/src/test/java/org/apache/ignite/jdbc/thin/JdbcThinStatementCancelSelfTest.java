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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Statement cancel test.
 */
@SuppressWarnings({"ThrowableNotThrown", "AssertWithSideEffects"})
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
    private static final int MAX_ROWS = 10000;

    /** Server thread pull size. */
    private static final int SERVER_THREAD_POOL_SIZE = 4;

    /** Cancellation processing timeout. */
    public static final int TIMEOUT = 5000;

    /** Nodes count. */
    private static final byte NODES_COUNT = 3;

    /** Timeout for checking async result. */
    public static final int CHECK_RESULT_TIMEOUT = 1_000;

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

        startGridsMultiThreaded(NODES_COUNT);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put((long)i, (long)i);
    }

    /**
     * Called before execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @Before
    public void before() throws Exception {
        TestSQLFunctions.init();

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
     * Trying to cancel stament without query. In given case cancel is noop, so no exception expected.
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
     * Trying to retrieve result set of a canceled query.
     * SQLException with message "The query was cancelled while executing." expected.
     *
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
     * Trying to cancel already cancelled query.
     * No exceptions exceped.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelCanceledQuery() throws Exception {
        stmt.execute("SELECT 1;");

        assertNotNull(stmt.getResultSet());

        stmt.cancel();

        stmt.cancel();

        GridTestUtils.assertThrows(log, () -> {
            stmt.getResultSet();

            return null;
        }, SQLException.class, "The query was cancelled while executing.");
    }

    /**
     * Trying to cancel closed query.
     * SQLException with message "Statement is closed." expected.
     *
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
     * Trying to call <code>resultSet.next()</code> on a canceled query.
     * SQLException with message "The query was cancelled while executing." expected.
     *
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
     * Ensure that it's possible to execute new query on cancelled statement.
     *
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
     * Ensure that stament cancel doesn't effect another statement workflow, created by the same connection.
     *
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
     * Trying to cancel long running query. No exceptions expected.
     * In order to guarantee correct concurrent processing of query itself and it's cancellation request
     * two latches and some other stuff is used.
     * For more details see <code>TestSQLFunctions#awaitLatchCancelled()</code>
     * and <code>JdbcThinStatementCancelSelfTest#cancel(java.sql.Statement)</code>.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelQuery() throws Exception {
        IgniteInternalFuture cancelRes = cancel(stmt);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select * from Integer where _key in " +
                "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledInCaseOfCancellation()");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Trying close canceling query. No exceptions expected.
     * In order to guarantee correct concurrent processing of query itself and it's cancellation request
     * two latches and some other stuff is used.
     * For more details see <code>TestSQLFunctions#awaitLatchCancelled()</code>
     * and <code>JdbcThinStatementCancelSelfTest#cancel(java.sql.Statement)</code>.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCloseCancelingQuery() throws Exception {
        IgniteInternalFuture res = GridTestUtils.runAsync(() -> {
            try {
                TestSQLFunctions.cancelLatch.await();

                long cancelCntrBeforeCancel = ClientListenerProcessor.CANCEL_COUNTER.get();

                stmt.cancel();

                try {
                    GridTestUtils.waitForCondition(
                        () -> ClientListenerProcessor.CANCEL_COUNTER.get() == cancelCntrBeforeCancel + 1, TIMEOUT);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                assertEquals(cancelCntrBeforeCancel + 1, ClientListenerProcessor.CANCEL_COUNTER.get());

                // Nothing expected here, cause query was already marked as canceled.
                stmt.close();

                TestSQLFunctions.reqLatch.countDown();
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        });

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select * from Integer where _key in " +
                "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledInCaseOfCancellation()");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        res.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Trying to cancel long running multiple statments query. No exceptions expected.
     * In order to guarantee correct concurrent processing of query itself and it's cancellation request
     * two latches and some other stuff is used.
     * For more details see <code>TestSQLFunctions#awaitLatchCancelled()</code>
     * and <code>JdbcThinStatementCancelSelfTest#cancel(java.sql.Statement)</code>.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelMultipleStatementsQuery() throws Exception {
        try (Statement anotherStatment = conn.createStatement()) {
            anotherStatment.setFetchSize(1);

            ResultSet rs = anotherStatment.executeQuery("select * from Integer");

            assert rs.next();

            IgniteInternalFuture cancelRes = cancel(stmt);

            GridTestUtils.assertThrows(log, () -> {
                // Executes multiple long running query
                stmt.execute(
                    "select 100 from Integer;"
                        + "select _key from Integer where awaitLatchCancelled() = 0;"
                        + "select 100 from Integer I1 join Integer I2;"
                        + "select * from Integer where shouldNotBeCalledInCaseOfCancellation()");
                return null;
            }, SQLException.class, "The query was cancelled while executing");

            assert rs.next() : "The other cursor mustn't be closed";

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);
        }
    }

    /**
     * Trying to cancel long running batch query. No exceptions expected.
     * In order to guarantee correct concurrent processing of query itself and it's cancellation request
     * two latches and some other stuff is used.
     * For more details see <code>TestSQLFunctions#awaitLatchCancelled()</code>
     * and <code>JdbcThinStatementCancelSelfTest#cancel(java.sql.Statement)</code>.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelBatchQuery() throws Exception {
        try (Statement stmt2 = conn.createStatement()) {
            stmt2.setFetchSize(1);

            ResultSet rs = stmt2.executeQuery("SELECT * from Integer");

            assert rs.next();

            IgniteInternalFuture cancelRes = cancel(stmt);

            GridTestUtils.assertThrows(log, () -> {
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30) OR _key < " + MAX_ROWS);
                stmt.addBatch("update Long set _val = _val + 1 where awaitLatchCancelled() = 0");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30) OR _key < " + MAX_ROWS);
                stmt.addBatch("update Long set _val = _val + 1 where shouldNotBeCalledInCaseOfCancellation()");

                stmt.executeBatch();
                return null;
            }, java.sql.SQLException.class, "The query was cancelled while executing");

            assert rs.next() : "The other cursor mustn't be closed";

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);
        }
    }

    /**
     * Trying to cancel long running query in situation that there's no worker for cancel query,
     * cause server thread pool is full. No exceptions expected.
     * In order to guarantee correct concurrent processing of query itself and it's cancellation request
     * thress latches and some other stuff is used.
     * For more details see <code>TestSQLFunctions#awaitLatchCancelled()</code>,
     * <code>TestSQLFunctions#awaitQuerySuspensionLatch()</code>
     * and <code>JdbcThinStatementCancelSelfTest#cancel(java.sql.Statement)</code>.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAgainstFullServerThreadPool() throws Exception {
        List<Statement> statements = Collections.synchronizedList(new ArrayList<>());
        List<Connection> connections = Collections.synchronizedList(new ArrayList<>());

        // Prepares connections and statemens in order to use them for filling thread pool with pseuso-infine quries.
        for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++) {
            Connection yaConn = DriverManager.getConnection(URL);

            yaConn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            connections.add(yaConn);

            Statement yaStmt = yaConn.createStatement();

            statements.add(yaStmt);
        }

        try {
            IgniteInternalFuture cancelRes = cancel(statements.get(SERVER_THREAD_POOL_SIZE - 1));

            // Completely fills server thread pool.
            IgniteInternalFuture<Long> fillPoolRes = fillServerThreadPool(statements, SERVER_THREAD_POOL_SIZE - 1);

            GridTestUtils.assertThrows(log, () -> {
                statements.get(SERVER_THREAD_POOL_SIZE - 1).executeQuery(
                    "select * from Integer where _key in " +
                        "(select _key from Integer where awaitLatchCancelled() = 0) and" +
                        " shouldNotBeCalledInCaseOfCancellation()");

                return null;
            }, SQLException.class, "The query was cancelled while executing.");

            // Releases queries in thread pool.
            TestSQLFunctions.suspendQryLatch.countDown();

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);

            // Ensures that there were no exceptions within async thread pool filling process.
            fillPoolRes.get(CHECK_RESULT_TIMEOUT);
        }
        finally {
            for (Statement statement : statements)
                statement.close();

            for (Connection connection : connections)
                connection.close();
        }
    }

    /**
     * Trying to cancel fetch query in situation that there's no worker for cancel query,
     * cause server thread pool is full. No exceptions expected.
     * In order to guarantee correct concurrent processing of query itself and it's cancellation request
     * thress latches and some other stuff is used.
     * For more details see <code>TestSQLFunctions#awaitLatchCancelled()</code>,
     * <code>TestSQLFunctions#awaitQuerySuspensionLatch()</code>
     * and <code>JdbcThinStatementCancelSelfTest#cancel(java.sql.Statement)</code>.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelFetchAgainstFullServerThreadPool() throws Exception {
        stmt.setFetchSize(1);

        ResultSet rs = stmt.executeQuery("SELECT * from Integer");

        rs.next();

        List<Statement> statements = Collections.synchronizedList(new ArrayList<>());
        List<Connection> connections = Collections.synchronizedList(new ArrayList<>());

        // Prepares connections and statemens in order to use them for filling thread pool with pseuso-infine quries.
        for (int i = 0; i < SERVER_THREAD_POOL_SIZE; i++) {
            Connection yaConn = DriverManager.getConnection(URL);

            yaConn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            connections.add(yaConn);

            Statement yaStmt = yaConn.createStatement();

            statements.add(yaStmt);
        }

        try {
            // Completely fills server thread pool.
            IgniteInternalFuture<Long> fillPoolRes = fillServerThreadPool(statements,
                SERVER_THREAD_POOL_SIZE - 1);

            IgniteInternalFuture fetchRes = GridTestUtils.runAsync(() -> {
                GridTestUtils.assertThrows(log, () -> {
                    rs.next();

                    return null;
                }, SQLException.class, "The query was cancelled while executing.");
            });

            stmt.cancel();

            // Ensures that there were no exceptions within async data fetching process.
            fetchRes.get(CHECK_RESULT_TIMEOUT);

            // Releases queries in thread pool.
            TestSQLFunctions.suspendQryLatch.countDown();

            // Ensure that there were no exceptions within async thread pool filling process.
            fillPoolRes.get(CHECK_RESULT_TIMEOUT);
        }
        finally {
            for (Statement statement : statements)
                statement.close();

            for (Connection connection : connections)
                connection.close();
        }
    }

    /**
     * Trying to cancel long running file upload. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancellingLongRunningFileUpload() throws Exception {
        IgniteInternalFuture cancelRes = GridTestUtils.runAsync(() -> {
            try {
                Thread.sleep(200);

                stmt.cancel();
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        });

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeUpdate(
                "copy from '" + BULKLOAD_20_000_LINE_CSV_FILE + "' into Person" +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");

        // Ensure that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Cancels current query, actual cancel will wait <code>cancelLatch</code> to be releaseds.
     *
     * @return <code>IgniteInternalFuture</code> to check whether exception was thrown.
     */
    private IgniteInternalFuture cancel(Statement stmt) {
        return GridTestUtils.runAsync(() -> {
            try {
                TestSQLFunctions.cancelLatch.await();

                long cancelCntrBeforeCancel = ClientListenerProcessor.CANCEL_COUNTER.get();

                stmt.cancel();

                try {
                    GridTestUtils.waitForCondition(
                        () -> ClientListenerProcessor.CANCEL_COUNTER.get() == cancelCntrBeforeCancel + 1, TIMEOUT);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                assertEquals(cancelCntrBeforeCancel + 1, ClientListenerProcessor.CANCEL_COUNTER.get());

                TestSQLFunctions.reqLatch.countDown();
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        });
    }

    /**
     * Fills Server Thread Pool with <code>qryCnt</code> queries. Given queries will wait for
     * <code>suspendQryLatch</code> to be released.
     *
     * @param statements Statements.
     * @param qryCnt Number of queries to execute.
     * @return <code>IgniteInternalFuture</code> in order to check whether exception was thrown or not.
     */
    private IgniteInternalFuture<Long> fillServerThreadPool(List<Statement> statements, int qryCnt) {
        AtomicInteger idx = new AtomicInteger(0);

        return GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                statements.get(idx.getAndIncrement()).executeQuery(
                    "select * from Integer where awaitQuerySuspensionLatch();");
            }
            catch (SQLException e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception");
            }
        }, qryCnt, "ThreadName");
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /** Request latch. */
        static CountDownLatch reqLatch;

        /** Cancel latch. */
        static CountDownLatch cancelLatch;

        /** Suspend query latch. */
        static CountDownLatch suspendQryLatch;

        /**
         * Recreate latches.
         */
        static void init() {
            reqLatch = new CountDownLatch(1);

            cancelLatch = new CountDownLatch(1);

            suspendQryLatch = new CountDownLatch(1);
        }

        /**
         * Releases cancelLatch that leeds to sending cancel Query and waits until cancel Query is fully processed.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long awaitLatchCancelled() {
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
         * Waits latch release.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long awaitQuerySuspensionLatch() {
            try {
                suspendQryLatch.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }

        /**
         * If called fails with corresponding message.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long shouldNotBeCalledInCaseOfCancellation() {
            fail("Query wasn't actually cancelled.");

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
