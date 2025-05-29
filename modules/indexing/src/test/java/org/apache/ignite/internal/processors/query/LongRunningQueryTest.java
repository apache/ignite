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

package org.apache.ignite.internal.processors.query;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.H2QueryInfo;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker;
import org.apache.ignite.internal.processors.query.running.TrackableQuery;
import org.apache.ignite.internal.processors.query.running.TrackableQueryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static java.lang.Thread.currentThread;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_EXEC_MSG;
import static org.h2.engine.Constants.DEFAULT_PAGE_SIZE;

/**
 * Tests for log print for long-running query.
 */
public class LongRunningQueryTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 1000;

    /** Number of keys to be queries in lazy queries. */
    private static final int LAZY_QRYS_KEY_CNT = 5;

    /** Long query warning timeout. */
    private static final int LONG_QUERY_WARNING_TIMEOUT = 1000;

    /** External wait time. */
    private static final int EXT_WAIT_TIME = 2000;

    /** Insert. */
    private static final String INSERT_SQL = "insert into test (_key, _val) values (1001, wait_func())";

    /** Insert with a subquery. */
    private static final String INSERT_WITH_SUBQUERY_SQL = "insert into test (_key, _val) select p._key, p.orgId from " +
        "\"pers\".Person p where p._key < wait_func()";

    /** Update. */
    private static final String UPDATE_SQL = "update test set _val = wait_func() where _key = 1";

    /** Update with a subquery. */
    private static final String UPDATE_WITH_SUBQUERY_SQL = "update test set _val = 111 where _key in " +
        "(select p._key from \"pers\".Person p where p._key < wait_func())";

    /** Delete. */
    private static final String DELETE_SQL = "delete from test where _key = wait_func()";

    /** Delete with a subquery. */
    private static final String DELETE_WITH_SUBQUERY_SQL = "delete from test where _key in " +
        "(select p._key from \"pers\".Person p where p._key < wait_func())";

    /** Log listener for long DMLs. */
    private static LogListener lsnrDml;

    /** Multi-node test rule. */
    @Rule
    public final MultiNodeTestRule multiNodeTestRule = new MultiNodeTestRule();

    /** Annotation for the {@link MultiNodeTestRule}. */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface MultiNodeTest {}

    /** Page size. */
    private int pageSize = DEFAULT_PAGE_SIZE;

    /** Local query mode. */
    private boolean local;

    /** Lazy query mode. */
    private boolean lazy;

    /** Merge table usage flag. */
    private boolean withMergeTable;

    /** Distributed joins flag. */
    private boolean distributedJoins;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setSqlConfiguration(new SqlConfiguration().setLongQueryWarningTimeout(LONG_QUERY_WARNING_TIMEOUT));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite = startGrids(multiNodeTestRule.isMultiNode ? 3 : 1);

        IgniteCache c = grid(0).createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10))
            .setSqlFunctionClasses(TestSQLFunctions.class));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);

        IgniteCache c2 = grid(0).createCache(cacheConfig("pers", Integer.class, Person.class));

        c2.put(1001, new Person(1, "p1"));

        lsnrDml = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(s -> s.contains("type=DML"))
            .build();

        testLog().registerListener(lsnrDml);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        checkQryInfoCount(0);

        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param name Name.
     * @param idxTypes Index types.
     */
    @SuppressWarnings("unchecked")
    private static CacheConfiguration cacheConfig(String name, Class<?>... idxTypes) {
        return new CacheConfiguration()
            .setName(name)
            .setIndexedTypes(idxTypes)
            .setSqlFunctionClasses(TestSQLFunctions.class);
    }

    /**
     *
     */
    @Test
    public void testLongDistributed() {
        local = false;
        lazy = false;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongLocal() {
        local = true;
        lazy = false;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongDistributedLazy() {
        local = false;
        lazy = true;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongDistributedLazyWithMergeTable() {
        local = false;
        lazy = true;

        withMergeTable = true;

        try {
            checkLongRunning();
        }
        finally {
            withMergeTable = false;
        }
    }

    /**
     *
     */
    @Test
    public void testLongLocalLazy() {
        local = true;
        lazy = true;

        checkLongRunning();
        checkFastQueries();
    }

    /** */
    @Test
    public void testLongRunningInsert() {
        local = false;

        runDml(INSERT_SQL);
    }

    /** */
    @Test
    public void testLongRunningInsertWithSubquery() {
        local = false;

        runDml(INSERT_WITH_SUBQUERY_SQL);
    }

    /** */
    @Test
    public void testLongRunningInsertLocal() {
        local = true;

        runDml(INSERT_SQL);
    }

    /** */
    @Test
    public void testLongRunningInsertWithSubqueryLocal() {
        local = true;

        runDml(INSERT_WITH_SUBQUERY_SQL);
    }

    /** */
    @Test
    public void testLongRunningUpdate() {
        local = false;

        runDml(UPDATE_SQL);
    }

    /** */
    @Test
    public void testLongRunningUpdateWithSubquery() {
        local = false;

        runDml(UPDATE_WITH_SUBQUERY_SQL);
    }

    /** */
    @Test
    public void testLongRunningUpdateLocal() {
        local = true;

        runDml(UPDATE_SQL);
    }

    /** */
    @Test
    public void testLongRunningUpdateWithSubqueryLocal() {
        local = true;

        runDml(UPDATE_WITH_SUBQUERY_SQL);
    }

    /** */
    @Test
    public void testLongRunningDelete() {
        local = false;

        runDml(DELETE_SQL);
    }

    /** */
    @Test
    public void testLongRunningDeleteWithSubquery() {
        local = false;

        runDml(DELETE_WITH_SUBQUERY_SQL);
    }

    /** */
    @Test
    public void testLongRunningDeleteLocal() {
        local = true;

        runDml(DELETE_SQL);
    }

    /** */
    @Test
    public void testLongRunningDeleteWithSubqueryLocal() {
        local = true;

        runDml(DELETE_WITH_SUBQUERY_SQL);
    }

    /**
     * Test checks that no long-running queries warnings are printed in case of external waits during
     * the execution of distributed queries.
     */
    @Test
    public void testDistributedLazyWithExternalWait() {
        local = false;
        lazy = true;

        checkLazyWithExternalWait();
    }

    /**
     * Test checks that no long-running queries warnings are printed in case of external waits during
     * the execution of local queries.
     */
    @Test
    public void testlocalLazyWithExternalWait() {
        local = true;
        lazy = true;

        checkLazyWithExternalWait();
    }

    /**
     * Test checks the correctness of thread name when displaying errors
     * about long queries.
     */
    @Test
    public void testCorrectThreadName() {
        GridWorker checkWorker = GridTestUtils.getFieldValue(heavyQueriesTracker(), "checkWorker");

        LogListener logLsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(logStr -> currentThread().getName().startsWith(checkWorker.name()))
            .build();

        testLog().registerListener(logLsnr);

        sqlCheckLongRunning();

        assertTrue(logLsnr.check());
    }

    /**
     *
     */
    @Test
    public void testBigResultSetLocal() throws Exception {
        local = true;
        lazy = true;

        checkBigResultSet();
    }

    /**
     *
     */
    @Test
    public void testBigResultDistributed() throws Exception {
        local = false;
        lazy = true;

        checkBigResultSet();
    }

    /**
     * Verifies that while a query is not fully fetched, its {@link H2QueryInfo} is kept in {@link HeavyQueriesTracker}
     * on all cluster nodes and its {@link H2QueryInfo#isSuspended()} returns {@code true}. Then, once the query is fully
     * fetched, its {@link H2QueryInfo} is removed from {@link HeavyQueriesTracker}.
     */
    @Test
    @MultiNodeTest
    public void testEmptyHeavyQueriesTrackerWithFullyFetchedIterator() {
        Iterator<?> it = queryCursor(false).iterator();

        checkQryInfoCount(gridCount());

        H2QueryInfo qry = (H2QueryInfo)heavyQueriesTracker().getQueries().iterator().next();

        assertTrue(qry.isSuspended());

        it.forEachRemaining(x -> {});
    }

    /**
     * Verifies that when the cursor of a not fully fetched query is closed, its {@link H2QueryInfo} is removed from
     * {@link HeavyQueriesTracker} on all cluster nodes.
     */
    @Test
    @MultiNodeTest
    public void testEmptyHeavyQueriesTrackerWithClosedCursor() {
        FieldsQueryCursor<List<?>> cursor = queryCursor(false);

        cursor.iterator().next();

        checkQryInfoCount(gridCount());

        H2QueryInfo qryInfo = (H2QueryInfo)heavyQueriesTracker().getQueries().iterator().next();

        assertTrue(qryInfo.isSuspended());

        cursor.close();
    }

    /**
     * Verifies that when a not fully fetched query is cancelled, its {@link H2QueryInfo} is removed from
     * {@link HeavyQueriesTracker} on all cluster nodes.
     */
    @Test
    @MultiNodeTest
    public void testEmptyHeavyQueriesTrackerWithCancelledQuery() {
        cancelQueries(runNotFullyFetchedQuery(false));
    }

    /**
     * Verifies that when a local not fully fetched query is cancelled, its {@link H2QueryInfo} is removed from
     * {@link HeavyQueriesTracker} on all cluster nodes.
     */
    @Test
    @MultiNodeTest
    public void testEmptyHeavyQueriesTrackerWithCancelledLocalQuery() {
        long qryId = runNotFullyFetchedQuery(true);

        ((IgniteEx)ignite).context().query().cancelLocalQueries(Set.of(qryId));
    }

    /**
     * Verifies that when there are multiple not fully fetched queries, and they are cancelled separately, corresponding
     * {@link H2QueryInfo} instances are removed from {@link HeavyQueriesTracker} on all cluster nodes.
     * */
    @Test
    @MultiNodeTest
    public void testEmptyHeavyQueriesTrackerWithMultipleCancelledQueries() {
        int qryCnt = 4;

        for (int i = 0; i < qryCnt; i++)
            runNotFullyFetchedQuery(false);

        for (int i = 0; i < gridCount(); ++i)
            assertEquals(qryCnt, heavyQueriesTracker(i).getQueries().size());

        List<H2QueryInfo> qrysList = heavyQueriesTracker(0).getQueries().stream().map(q -> (H2QueryInfo)q).collect(Collectors.toList());

        cancelQueries(qrysList.get(0).queryId(), qrysList.get(1).queryId());

        for (int i = 0; i < gridCount(); ++i) {
            Set<TrackableQuery> qrys = heavyQueriesTracker(i).getQueries();

            assertEquals(2, qrys.size());

            assertFalse(qrys.stream().anyMatch(qryInfo -> {
                long id = ((TrackableQueryImpl)qryInfo).queryId();

                return id == qrysList.get(0).queryId() || id == qrysList.get(1).queryId();
            }));
        }

        cancelQueries(qrysList.get(2).queryId(), qrysList.get(3).queryId());
    }

    /**
     * Do several fast queries.
     * Log messages must not contain info about long query.
     */
    private void checkFastQueries() {
        ListeningTestLogger testLog = testLog();

        LogListener lsnr = LogListener
            .matches(Pattern.compile(LONG_QUERY_EXEC_MSG))
            .build();

        testLog.registerListener(lsnr);

        // Several fast queries.
        for (int i = 0; i < 10; ++i)
            sql("test", "SELECT * FROM test").getAll();

        assertFalse(lsnr.check());
    }

    /**
     * Do long-running query canceled by timeout and check log output.
     * Log messages must contain info about long query.
     */
    private void checkLongRunning() {
        ListeningTestLogger testLog = testLog();

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .build();

        testLog.registerListener(lsnr);

        sqlCheckLongRunning();

        assertTrue(lsnr.check());
    }

    /**
     */
    private void checkBigResultSet() throws Exception {
        ListeningTestLogger testLog = testLog();

        LogListener lsnr = LogListener
            .matches("Query produced big result set")
            .build();

        testLog.registerListener(lsnr);

        try (FieldsQueryCursor cur = sql("test", "SELECT T0.id FROM test AS T0, test AS T1")) {
            Iterator it = cur.iterator();

            while (it.hasNext())
                it.next();
        }

        assertTrue(lsnr.check(1_000));
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     */
    private void sqlCheckLongRunning(String sql, Object... args) {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql("test", sql, args).getAll(), QueryCancelledException.class, "");
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     */
    private void sqlCheckLongRunningLazy(String sql, Object... args) {
        pageSize = 1;

        try {
            assertEquals(LAZY_QRYS_KEY_CNT, sql("test", sql, args).getAll().size());
        }
        finally {
            pageSize = DEFAULT_PAGE_SIZE;
        }
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     */
    private void sqlCheckLongRunningLazyWithMergeTable(String sql, Object... args) {
        distributedJoins = true;

        try {
            CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);
            CacheConfiguration ccfg2 = cacheConfig("org", Integer.class, Organization.class);

            IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);
            IgniteCache<Integer, Organization> cache2 = ignite.getOrCreateCache(ccfg2);

            cache2.put(1, new Organization("o1"));
            cache2.put(2, new Organization("o2"));
            cache1.put(3, new Person(1, "p1"));
            cache1.put(4, new Person(2, "p2"));
            cache1.put(5, new Person(3, "p3"));

            assertFalse(sql("pers", sql, args).getAll().isEmpty());
        }
        finally {
            distributedJoins = false;
        }
    }

    /**
     * Execute long-running sql with a check for errors.
     */
    private void sqlCheckLongRunning() {
        if (lazy && withMergeTable) {
            String select = "select o.name n1, p.name n2 from Person p, \"org\".Organization o" +
                " where p.orgId = o._key and o._key=1 and o._key < sleep_func(?, ?)" +
                " union select o.name n1, p.name n2 from Person p, \"org\".Organization o" +
                " where p.orgId = o._key and o._key=2";

            sqlCheckLongRunningLazyWithMergeTable(select, 2000, LAZY_QRYS_KEY_CNT);
        }
        else if (lazy && !withMergeTable)
            sqlCheckLongRunningLazy("SELECT * FROM test WHERE _key < sleep_func(?, ?)", 2000, LAZY_QRYS_KEY_CNT);
        else
            sqlCheckLongRunning("SELECT T0.id FROM test AS T0, test AS T1, test AS T2 where T0.id > ?", 0);
    }

    /**
     * @param cacheName Cache name.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String cacheName, String sql, Object... args) {
        return ignite.cache(cacheName).query(new SqlFieldsQuery(sql)
            .setTimeout(10, TimeUnit.SECONDS)
            .setLocal(local)
            .setLazy(lazy)
            .setPageSize(pageSize)
            .setDistributedJoins(distributedJoins)
            .setArgs(args));
    }

    /** */
    public void checkLazyWithExternalWait() {
        pageSize = 1;

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .build();

        testLog().registerListener(lsnr);

        Iterator<List<?>> it = sql("test", "select * from test").iterator();

        try {
            it.next();

            long sleepStartTs = U.currentTimeMillis();

            while (U.currentTimeMillis() - sleepStartTs <= EXT_WAIT_TIME)
                doSleep(100L);

            it.next();

            H2QueryInfo qry = (H2QueryInfo)heavyQueriesTracker().getQueries().iterator().next();

            assertTrue(qry.extWait() >= EXT_WAIT_TIME);

            assertFalse(lsnr.check());
        }
        finally {
            pageSize = DEFAULT_PAGE_SIZE;

            it.forEachRemaining(x -> {});
        }
    }

    /**
     * @param dml Dml command.
     */
    public void runDml(String dml) {
        lazy = false;

        long start = U.currentTimeMillis();

        sql("test", dml);

        assertTrue((U.currentTimeMillis() - start) > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnrDml.check());
    }

    /**
     * @param loc Flag indicating if the query is local.
     * @return Query id.
     */
    public long runNotFullyFetchedQuery(boolean loc) {
        queryCursor(loc).iterator().next();

        checkQryInfoCount(loc ? 1 : gridCount());

        H2QueryInfo qryInfo = (H2QueryInfo)heavyQueriesTracker().getQueries().iterator().next();

        assertTrue(qryInfo.isSuspended());

        return qryInfo.queryId();
    }

    /**
     * @param loc Flag indicating if the query is local.
     * @return Query cursor.
     */
    public FieldsQueryCursor<List<?>> queryCursor(boolean loc) {
        return ignite.cache("test").query(new SqlFieldsQuery("select * from test").setLocal(loc).setPageSize(1));
    }

    /**
     * @param qryIds Query ids.
     */
    public void cancelQueries(long... qryIds) {
        for (long id : qryIds)
            ((IgniteEx)ignite).context().query().cancelQuery(id, ignite.cluster().node().id(), false);
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /**
         * @param sleep amount of milliseconds to sleep
         * @param val value to be returned by the function
         */
        @SuppressWarnings("unused")
        @QuerySqlFunction
        public static int sleep_func(int sleep, int val) {
            try {
                Thread.sleep(sleep);
            }
            catch (InterruptedException ignored) {
                // No-op
            }
            return val;
        }

        /** */
        @SuppressWarnings("unused")
        @QuerySqlFunction
        public static int wait_func() {
            try {
                GridTestUtils.waitForCondition(() -> lsnrDml.check(), 10_000);
            }
            catch (IgniteInterruptedCheckedException ignored) {
                // No-op
            }

            return 1;
        }
    }

    /**
     * Setup and return test log.
     *
     * @return Test logger.
     */
    private ListeningTestLogger testLog() {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)grid(0).context().query().getIndexing()).heavyQueriesTracker(),
            "log", testLog);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)grid(0).context().query().getIndexing()).mapQueryExecutor(),
            "log", testLog);

        GridTestUtils.setFieldValue(grid(0).context().query().getIndexing(), "log", testLog);

        return testLog;
    }

    /**
     * Getting {@link HeavyQueriesTracker} from the node.
     *
     * @return Heavy queries tracker.
     */
    private HeavyQueriesTracker heavyQueriesTracker() {
        return heavyQueriesTracker(0);
    }

    /** */
    private HeavyQueriesTracker heavyQueriesTracker(int idx) {
        return ((IgniteH2Indexing)grid(idx).context().query().getIndexing()).heavyQueriesTracker();
    }

    /** */
    private void checkQryInfoCount(int exp) {
        int res = 0;

        for (int i = 0; i < gridCount(); i++) {
            if (!heavyQueriesTracker(i).getQueries().isEmpty())
                res++;
        }

        assertEquals(exp, res);
    }

    /** */
    public int gridCount() {
        return Ignition.allGrids().size();
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        int orgId;

        /** */
        @QuerySqlField(index = true)
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }

    /** */
    private static class Organization {
        /** */
        @QuerySqlField
        String name;

        /**
         * @param name Organization name.
         */
        public Organization(String name) {
            this.name = name;
        }
    }

    /** Test rule that allows marking a test as multi-node via the {@link MultiNodeTest} annotation. */
    private static class MultiNodeTestRule extends TestWatcher {
        /** */
        private boolean isMultiNode;

        /** {@inheritDoc} */
        @Override protected void starting(Description description) {
            isMultiNode = description.getAnnotation(MultiNodeTest.class) != null;
        }

        /** */
        public boolean isMultiNode() {
            return isMultiNode;
        }
    }
}
