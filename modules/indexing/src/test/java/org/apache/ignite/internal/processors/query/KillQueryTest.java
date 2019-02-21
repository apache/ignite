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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
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

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        if (++cntr == NODES_COUNT)
            cfg.setClientMode(true);

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
        igniteForKillRequest.cache(DEFAULT_CACHE_NAME)
            .query(createKillQuery(UUID.randomUUID(), Long.MAX_VALUE, async));
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
     * Trying to cancel long running file upload. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11176")
    @Test
    public void testCancellingLongRunningFileUpload() throws Exception {
        testCancellingLongRunningFileUpload(false);
    }

    /**
     * Trying to async cancel long running file upload. No exceptions expected.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11176")
    @Test
    public void testAsyncCancellingLongRunningFileUpload() throws Exception {
        testCancellingLongRunningFileUpload(true);
    }

    /**
     * Trying to cancel long running file upload. No exceptions expected.
     *
     * @param async execute cancellation in ASYNC mode.
     * @throws Exception If failed.
     */
    private void testCancellingLongRunningFileUpload(boolean async) throws Exception {
        IgniteInternalFuture cancelRes = GridTestUtils.runAsync(() -> {
            try {
                Thread.sleep(200);

                GridRunningQueryInfo runQry = ignite.context().query().runningQueries(-1).iterator().next();

                igniteForKillRequest.cache(DEFAULT_CACHE_NAME).query(createKillQuery(runQry.globalQueryId(), async));
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                Assert.fail("Unexpected exception");
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
