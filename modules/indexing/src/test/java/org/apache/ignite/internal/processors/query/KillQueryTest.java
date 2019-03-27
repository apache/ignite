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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.junit.Assert;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Test KILL QUERY requested from the same node where quere was executed.
 */
@SuppressWarnings({"ThrowableNotThrown", "AssertWithSideEffects"})
@RunWith(JUnit4.class)
public class KillQueryTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** A CSV file with one record. */
    private static final String BULKLOAD_20_000_LINE_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath("/modules/clients/src/test/resources/bulkload20_000.csv")).
            getAbsolutePath();

    /** Max table rows. */
    private static final int MAX_ROWS = 10000;

    /** Cancellation processing timeout. */
    public static final int TIMEOUT = 5000;

    /** Nodes count. */
    protected static final byte NODES_COUNT = 3;

    /** Timeout for checking async result. */
    public static final int CHECK_RESULT_TIMEOUT = 1_000;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** Ignite. */
    protected IgniteEx ignite;

    /** Ignite instance for kill request. */
    private IgniteEx igniteForKillRequest;

    /** Node configration conter. */
    private static int cntr;

    /** Table count. */
    private static AtomicInteger tblCnt = new AtomicInteger();

    /** Barrier. */
    private static volatile CyclicBarrier barrier;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cache = GridAbstractTest.defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setSqlFunctionClasses(TestSQLFunctions.class);
        cache.setIndexedTypes(Integer.class, Integer.class, Long.class, Long.class, String.class, Person.class);

        cfg.setCacheConfiguration(cache);

        if (++cntr == NODES_COUNT)
            cfg.setClientMode(true);

        cfg.setDiscoverySpi(new TcpDiscoverySpi() {

            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                if (CustomMessageWrapper.class.isAssignableFrom(msg.getClass())) {
                    DiscoveryCustomMessage delegate = ((CustomMessageWrapper)msg).delegate();

                    if (DynamicCacheChangeBatch.class.isAssignableFrom(delegate.getClass())) {
                        try {
                            awaitTimeout();
                        }
                        catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }

                    }
                    else
                        if (SchemaProposeDiscoveryMessage.class.isAssignableFrom(delegate.getClass())) {
                        try {
                            awaitTimeout();
                        }
                        catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }

                super.sendCustomEvent(msg);
            }
        }.setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cntr = 0;

        startGrids(NODES_COUNT);

        for (int i = 0; i < MAX_ROWS; ++i) {
            grid(0).cache(GridAbstractTest.DEFAULT_CACHE_NAME).put(i, i);

            grid(0).cache(GridAbstractTest.DEFAULT_CACHE_NAME).put((long)i, (long)i);
        }
    }

    /**
     * @return Ignite node which will be used to execute kill query.
     */
    protected IgniteEx getKillRequestNode() {
        return grid(0);
    }

    /**
     * Called before execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @Before
    public void before() throws Exception {
        TestSQLFunctions.init();

        newBarrier(1);

        tblCnt.incrementAndGet();

        conn = GridTestUtils.connect(grid(0), null);

        conn.setSchema('"' + GridAbstractTest.DEFAULT_CACHE_NAME + '"');

        stmt = conn.createStatement();

        ignite = grid(0);

        igniteForKillRequest = getKillRequestNode();
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

        assertTrue(ignite.context().query().runningQueries(-1).isEmpty());
    }

    /**
     * Trying to cancel COPY FROM command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testBulkLoadCancellationUnsupported() throws Exception {
        testCreateTableCancellationUnsupported(false);
    }

    /**
     * Trying to async cancel COPY FROM command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testAsyncBulkLoadCancellationUnsupported() throws Exception {
        testCreateTableCancellationUnsupported(true);
    }

    /**
     * Trying to cancel COPY FROM command.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    public void testBulkLoadCancellationUnsupported(boolean async) throws Exception {
        String path = Objects.requireNonNull(resolveIgnitePath("/modules/clients/src/test/resources/bulkload1.csv"))
            .getAbsolutePath();

        String sqlPrepare = "CREATE TABLE " + currentTestTableName() +
            "(id integer primary key, age integer, firstName varchar, lastname varchar)";
        String sqlCmd = "COPY FROM '" + path + "'" +
            " INTO " + currentTestTableName() +
            " (_key, age, firstName, lastName)" +
            " format csv charset 'ascii'";

        testCancellationUnsupported(
            Arrays.asList(sqlPrepare),
            sqlCmd,
            async);
    }

    /**
     * Trying to cancel CREATE TABLE command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testCreateTableCancellationUnsupported() throws Exception {
        testCreateTableCancellationUnsupported(false);
    }

    /**
     * Trying to async cancel CREATE TABLE command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testAsyncCreateTableCancellationUnsupported() throws Exception {
        testCreateTableCancellationUnsupported(true);
    }

    /**
     * Trying to cancel CREATE TABLE command.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    public void testCreateTableCancellationUnsupported(boolean async) throws Exception {
        testCancellationUnsupported(Collections.<String>emptyList(),
            "CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)",
            async);
    }

    /**
     * Trying to cancel ALTER TABLE command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testAlterTableCancellationUnsupported() throws Exception {
        testAlterTableCancellationUnsupported(false);
    }

    /**
     * Trying to async cancel ALTER TABLE command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testAsyncAlterTableCancellationUnsupported() throws Exception {
        testAlterTableCancellationUnsupported(true);
    }

    /**
     * Trying to cancel ALTER TABLE command.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    private void testAlterTableCancellationUnsupported(boolean async) throws Exception {
        testCancellationUnsupported(Arrays.asList("CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)"),
            "ALTER TABLE " + currentTestTableName() + " ADD COLUMN COL VARCHAR",
            async);
    }

    /**
     * Trying to cancel CREATE INDEX command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testCreateIndexCancellationUnsupported() throws Exception {
        testCreateIndexCancellationUnsupported(false);
    }

    /**
     * Trying to async cancel CREATE INDEX command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testAsyncCreateIndexCancellationUnsupported() throws Exception {
        testCreateIndexCancellationUnsupported(true);
    }

    /**
     * Trying to cancel CREATE INDEX command.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    private void testCreateIndexCancellationUnsupported(boolean async) throws Exception {
        testCancellationUnsupported(Arrays.asList("CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)"),
            "CREATE INDEX " + currentTestTableName() + "_IDX ON " + currentTestTableName() + "(name, id)",
            async);
    }


    /**
     * Trying to cancel DROP INDEX command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testDropIndexCancellationUnsupported() throws Exception {
        testDropIndexCancellationUnsupported(false);
    }

    /**
     * Trying to async cancel DROP INDEX command.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testAsyncDropIndexCancellationUnsupported() throws Exception {
        testDropIndexCancellationUnsupported(true);
    }

    /**
     * Trying to cancel DROP INDEX command.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    private void testDropIndexCancellationUnsupported(boolean async) throws Exception {
        testCancellationUnsupported(
            Arrays.asList("CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)",
            "CREATE INDEX " + currentTestTableName() + "_IDX ON " + currentTestTableName() + "(name, id)"),
            "DROP INDEX " + currentTestTableName() + "_IDX",
            async);
    }

    /**
     * Get test table name unique for per tests, but stable within one test run.
     *
     * @return Generated test table name unique for per tests, but stable within one test run.
     */
    private String currentTestTableName() {
        return "TST_TABLE_" + tblCnt.get();
    }

    /**
     * Check that trying cancellation execution of {@code sqlCmd} can't be cancelled due to it's not supported.
     *
     * @param prepareSteps Preparation SQLs before start test.
     * @param sqlCmd Command which can't be cancelled
     * @param async Execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    private void testCancellationUnsupported(List<String> prepareSteps, String sqlCmd, boolean async) throws Exception {
        for (String sql : prepareSteps) {
            try {
                stmt.execute(sql);
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }

        newBarrier(2);

        IgniteInternalFuture cancelRes = cancelQueryWithBarrier(sqlCmd, "Query doesn't support cancellation", async);

        stmt.execute(sqlCmd);

        cancelRes.get(TIMEOUT);
    }

    /**
     * Trying to cancel unexist query.
     */
    @Test
    public void testKillUnknownQry() {
        testKillUnknownQry(false);
    }

    /**
     * Trying to async cancel unexist query.
     */
    @Test
    public void testAsyncKillUnknownQry() {
        testKillUnknownQry(true);
    }

    /**
     * Trying to cancel unexist query.
     *
     * @param async execute cancellation in ASYNC mode.
     */
    private void testKillUnknownQry(boolean async) {
        UUID nodeId = ignite.localNode().id();

        GridTestUtils.assertThrows(log, () -> {
            igniteForKillRequest.cache(DEFAULT_CACHE_NAME)
                .query(createKillQuery(nodeId, Long.MAX_VALUE, async));

            return null;
        }, CacheException.class, "Query with provided ID doesn't exist [nodeId=" + nodeId);
    }

    /**
     * Trying to cancel query on unexist node.
     */
    @Test
    public void testKillQryUnknownNode() {
        testKillQryUnknownNode(false);
    }

    /**
     * Trying to async cancel query on unexist node.
     */
    @Test
    public void testAsyncKillQryUnknownNode() {
        testKillQryUnknownNode(true);
    }

    /**
     * Trying to cancel query on unexist node.
     *
     * @param async execute cancellation in ASYNC mode.
     */
    private void testKillQryUnknownNode(boolean async) {
        GridTestUtils.assertThrows(log, () -> {
            igniteForKillRequest.cache(DEFAULT_CACHE_NAME)
                .query(createKillQuery(UUID.randomUUID(), Long.MAX_VALUE, async));

            return null;
        }, CacheException.class, "Failed to cancel query, node is not alive");
    }

    /**
     * Trying to kill already killed query. No exceptions expected.
     */
    @Test
    public void testKillAlreadyKilledQuery() {
        testKillAlreadyKilledQuery(false);
    }

    /**
     * Trying to kill already killed query. No exceptions expected.
     */
    @Test
    public void testAsyncKillAlreadyKilledQuery() {
        testKillAlreadyKilledQuery(true);
    }

    /**
     * Trying to kill already killed query. No exceptions expected.
     *
     * @param async execute cancellation in ASYNC mode.
     */
    private void testKillAlreadyKilledQuery(boolean async) {
        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select * from Integer"));

        List<GridRunningQueryInfo> runningQueries = (List<GridRunningQueryInfo>)ignite.context().query().runningQueries(-1);

        runningQueries.forEach(r -> System.out.println(r.query()));

        GridRunningQueryInfo runQryInfo = runningQueries.get(0);

        SqlFieldsQuery killQry = createKillQuery(runQryInfo.globalQueryId(), async);

        IgniteCache<Object, Object> reqCache = igniteForKillRequest.cache(DEFAULT_CACHE_NAME);

        reqCache.query(killQry);

        reqCache.query(killQry);

        cur.close();
    }

    /**
     * @param nodeId Node id.
     * @param qryId Node query id.
     */
    private SqlFieldsQuery createKillQuery(UUID nodeId, long qryId, boolean async) {
        return createKillQuery(nodeId + "_" + qryId, async);
    }

    /**
     * @param globalQryId Global query id.
     */
    private SqlFieldsQuery createKillQuery(String globalQryId, boolean async) {
        return new SqlFieldsQuery("KILL QUERY" + (async ? " ASYNC" : "") + " '" + globalQryId + "'");
    }

    /**
     * Trying to cancel long running query. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelQuery() throws Exception {
        testCancelQuery(false);
    }

    /**
     * Trying to async cancel long running query. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAsyncCancelQuery() throws Exception {
        testCancelQuery(true);
    }

    /**
     * Trying to cancel long running query. No exceptions expected.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception If failed.
     */
    private void testCancelQuery(boolean async) throws Exception {
        IgniteInternalFuture cancelRes = cancel(1, async);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select * from Integer where _key in " +
                "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledInCaseOfCancellation()");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Trying to cancel long running multiple statments query. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKillMultipleStatementsQuery() throws Exception {
        testKillMultipleStatementsQuery(false);
    }

    /**
     * Trying to async cancel long running multiple statments query. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAsyncKillMultipleStatementsQuery() throws Exception {
        testKillMultipleStatementsQuery(true);
    }

    /**
     * Trying to async cancel long running multiple statments query. No exceptions expected.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception If failed.
     */
    private void testKillMultipleStatementsQuery(boolean async) throws Exception {
        try (Statement anotherStatment = conn.createStatement()) {
            anotherStatment.setFetchSize(1);

            ResultSet rs = anotherStatment.executeQuery("select * from Integer");

            assert rs.next();

            IgniteInternalFuture cancelRes = cancel(3, async);

            GridTestUtils.assertThrows(log, () -> {
                // Executes multiple long running query
                stmt.execute(
                    "select 100 from Integer;"
                        + "select _key from Integer where awaitLatchCancelled() = 0;");
                return null;
            }, SQLException.class, "The query was cancelled while executing");

            assert rs.next() : "The other cursor mustn't be closed";

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);
        }
    }

    /**
     * Trying to cancel long running batch query. No exceptions expected.     *
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelBatchQuery() throws Exception {
        testCancelBatchQuery(false);
    }

    /**
     * Trying to async cancel long running batch query. No exceptions expected.     *
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAsyncCancelBatchQuery() throws Exception {
        testCancelBatchQuery(true);
    }

    /**
     * Trying to cancel long running batch query. No exceptions expected.     *
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception If failed.
     */
    private void testCancelBatchQuery(boolean async) throws Exception {
        try (Statement stmt2 = conn.createStatement()) {
            stmt2.setFetchSize(1);

            ResultSet rs = stmt2.executeQuery("SELECT * from Integer");

            Assert.assertTrue(rs.next());

            IgniteInternalFuture cancelRes = cancel(2, async);

            GridTestUtils.assertThrows(log, () -> {
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where awaitLatchCancelled() = 0");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where shouldNotBeCalledInCaseOfCancellation()");

                stmt.executeBatch();
                return null;
            }, SQLException.class, "The query was cancelled while executing");

            Assert.assertTrue("The other cursor mustn't be closed", rs.next());

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);
        }
    }

    /**
     * Cancels current query which wait on barrier.
     *
     * @param qry Query which need to try cancel.
     * @param expErrMsg Exoected error message during cancellation.
     * @param async Execute cancellation in ASYNC mode.
     * @return <code>IgniteInternalFuture</code> to check whether exception was thrown.
     */
    private IgniteInternalFuture cancelQueryWithBarrier(String qry, String expErrMsg, boolean async) {
        return GridTestUtils.runAsync(() -> {
            try {
                List<GridRunningQueryInfo> runningQueries = new ArrayList<>();

                GridTestUtils.waitForCondition(() -> {
                    List<GridRunningQueryInfo> r = (List<GridRunningQueryInfo>)ignite.context().query()
                        .runningQueries(-1);

                    runningQueries.addAll(r.stream().filter(q -> q.query().equals(qry)).collect(Collectors.toList()));

                    return !runningQueries.isEmpty();
                }, TIMEOUT);

                assertFalse(runningQueries.isEmpty());

                for (GridRunningQueryInfo runningQuery : runningQueries) {
                    GridTestUtils.assertThrowsAnyCause(log,
                        () -> igniteForKillRequest.cache(DEFAULT_CACHE_NAME).query(createKillQuery(runningQuery.globalQueryId(), async)),
                        CacheException.class, expErrMsg);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                Assert.fail("Unexpected exception");
            }
            finally {
                try {
                    awaitTimeout();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    Assert.fail("Unexpected exception");
                }
            }
        });
    }

    /**
     * Cancels current query, actual cancel will wait <code>cancelLatch</code> to be releaseds.
     *
     * @return <code>IgniteInternalFuture</code> to check whether exception was thrown.
     */
    private IgniteInternalFuture cancel(int expQryNum, boolean async) {
        return GridTestUtils.runAsync(() -> {
            try {
                TestSQLFunctions.cancelLatch.await();

                List<GridRunningQueryInfo> runningQueries = (List<GridRunningQueryInfo>)ignite.context().query().runningQueries(-1);

                List<IgniteInternalFuture> res = new ArrayList<>();

                for (GridRunningQueryInfo runningQuery : runningQueries) {
                    res.add(GridTestUtils.runAsync(() -> {
                            igniteForKillRequest.cache(DEFAULT_CACHE_NAME).query(createKillQuery(runningQuery.globalQueryId(), async));
                        }
                    ));
                }

                doSleep(500);

                assertEquals(expQryNum, runningQueries.size());

                TestSQLFunctions.reqLatch.countDown();

                for (IgniteInternalFuture fut : res) {
                    fut.get(TIMEOUT);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                Assert.fail("Unexpected exception");
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

                Assert.fail("Unexpected exception");
            }
        }, qryCnt, "ThreadName");
    }

    /**
     * Create and set new CyclicBarrier for the function.
     *
     * @param parties the number of threads that must invoke await method before the barrier is tripped
     */
    private static void newBarrier(int parties) {
        barrier = new CyclicBarrier(parties);
    }

    /**
     * @throws InterruptedException In case of failure.
     * @throws TimeoutException In case of failure.
     * @throws BrokenBarrierException In case of failure.
     */
    private static void awaitTimeout() throws InterruptedException, TimeoutException, BrokenBarrierException {
        barrier.await(TIMEOUT, TimeUnit.MILLISECONDS);
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

    /**
     * Person.
     */
    static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField
        private final String firstName;

        /** Last name. */
        @QuerySqlField
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            if (id != person.id)
                return false;
            if (age != person.age)
                return false;
            if (firstName != null ? !firstName.equals(person.firstName) : person.firstName != null)
                return false;
            return lastName != null ? lastName.equals(person.lastName) : person.lastName == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            result = 31 * result + age;
            return result;
        }
    }
}
