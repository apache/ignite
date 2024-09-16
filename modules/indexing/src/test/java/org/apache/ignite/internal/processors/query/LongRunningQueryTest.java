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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.H2QueryInfo;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_EXEC_MSG;
import static org.h2.engine.Constants.DEFAULT_PAGE_SIZE;

/**
 * Tests for log print for long-running query.
 */
public class LongRunningQueryTest extends AbstractIndexingCommonTest {
    /** Keys count. */
//    private static final int KEY_CNT = 1000;
    private static final int KEY_CNT = 5;

    private static final int LONG_QUERY_WARNING_TIMEOUT = 1000;

    String update = "update A.String set _val = 'failed' where A.fail()=1";

    String delete = "delete from A.String where A.fail()=1";

    /** External wait time. */
    private static final int EXT_WAIT_TIME = 2000;

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
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration();

        return cfg.setSqlConfiguration(new SqlConfiguration().setLongQueryWarningTimeout(LONG_QUERY_WARNING_TIMEOUT));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite = startGrid();

        IgniteCache c = grid().createCache(new CacheConfiguration<Long, Long>()
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
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
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

    @Test
    public void testLongRunningInsert() {
        local = false;
        lazy = false;

        String insert = "insert into test (_key, _val) values (5, sleep_func1())";
//        String insert = "insert into test (_key, _val) values (5, 5)";

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(s -> !s.contains("plan=SELECT"))
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        sql("test", insert);

        long time = System.currentTimeMillis() - start;

        assertTrue(time > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningInsertWithSelect() {
        local = false;
        lazy = false;

        CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);

        IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);

        cache1.put(6, new Person(1, "p1"));
        cache1.put(7, new Person(2, "p2"));
        cache1.put(8, new Person(3, "p3"));


        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(s -> !s.contains("plan=SELECT"))
            .build();

        testLog().registerListener(lsnr);

        String insert = "insert into test (_key, _val) select p._key, p.orgId from \"pers\".Person p where p._key < sleep_func(?)";

        sql("test", insert, 2000);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningInsertLocal() {
        local = true;
        lazy = false;

        String insert = "insert into test (_key, _val) values (5, sleep_func1())";

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        sql("test", insert);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningInsertWithSelectLocal() {
        local = true;
        lazy = false;

        CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);

        IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);

        cache1.put(6, new Person(1, "p1"));
        cache1.put(7, new Person(2, "p2"));
        cache1.put(8, new Person(3, "p3"));


        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(s -> !s.contains("plan=SELECT"))
            .build();

        testLog().registerListener(lsnr);

        String insert = "insert into test (_key, _val) select p._key, p.orgId from \"pers\".Person p where p._key < sleep_func(?)";

        sql("test", insert, 2000);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningUpdate() {
        local = false;
        lazy = false;

//        for (int i = 6; i < 1_000_000; ++i)
//            sql("test", "insert into test (_key, _val) values (?,?)", i, i);

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .build();

        testLog().registerListener(lsnr);

        String update = "update test set _val = sleep_func1()";

        long start = System.currentTimeMillis();

        sql("test", update);

//        for (int i = 6; i < 1_000_000; ++i)
//            sql("test", "update test set _val = " + (i + 1) + " where _key = " + i);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningUpdateWithSelect() {
        local = false;
        lazy = false;

        CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);

        IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);

        cache1.put(6, new Person(1, "p1"));
        cache1.put(7, new Person(2, "p2"));
        cache1.put(8, new Person(3, "p3"));

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches("plan=SELECT")
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        String update = "update test set _val = 111 where _key in " +
            "(select p._key from \"pers\".Person p where p._key < sleep_func(?))";

        sql("test", update, 2000);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningUpdateLocal() {
        local = true;
        lazy = false;

        for (int i = 6; i < 1_000_000; ++i)
            sql("test", "insert into test (_key, _val) values (?,?)", i, i);

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        for (int i = 6; i < 1_000_000; ++i)
            sql("test", "update test set _val = " + (i + 1) + " where _key = " + i);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningUpdateWithSelectLocal() {
        local = true;
        lazy = false;

        CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);

        IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);

        cache1.put(6, new Person(1, "p1"));
        cache1.put(7, new Person(2, "p2"));
        cache1.put(8, new Person(3, "p3"));

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches("plan=SELECT")
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        String update = "update test set _val = 111 where _key in " +
            "(select p._key from \"pers\".Person p where p._key < sleep_func(?))";

        sql("test", update, 2000);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningDelete() {
        local = false;
        lazy = false;

        for (int i = 6; i < 1_000_000; ++i)
            sql("test", "insert into test (_key, _val) values (?,?)", i, i);

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
//            .andMatches("plan=SELECT")
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

//        String delete = "delete from test where _key between 6 and 1000000";
        String delete = "delete from test";

        sql("test", delete);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());

        //Печатаются предупреждения типа "Query produced big result set"
    }

    @Test
    public void testLongRunningDeleteWithSelect() {
        local = false;
        lazy = false;

        CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);

        IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);

        cache1.put(6, new Person(1, "p1"));
        cache1.put(7, new Person(2, "p2"));
        cache1.put(8, new Person(3, "p3"));


        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
//            .andMatches("plan=SELECT")
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        String delete = "delete from test where _key in " +
            "(select p._key from \"pers\".Person p where p._key < sleep_func(?))";

        sql("test", delete, 2000);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
    }

    @Test
    public void testLongRunningDeleteLocal() {
        local = true;
        lazy = false;

        for (int i = 6; i < 1_000_000; ++i)
            sql("test", "insert into test (_key, _val) values (?,?)", i, i);

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
//            .andMatches("plan=SELECT")
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

//        String delete = "delete from test where _key between 6 and 1000000";
        String delete = "delete from test";

        sql("test", delete);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());

        //Печатаются предупреждения типа "Query produced big result set"
    }

    @Test
    public void testLongRunningDeleteWithSelectLocal() {
        local = true;
        lazy = false;

        CacheConfiguration ccfg1 = cacheConfig("pers", Integer.class, Person.class);

        IgniteCache<Integer, Person> cache1 = ignite.getOrCreateCache(ccfg1);

        cache1.put(6, new Person(1, "p1"));
        cache1.put(7, new Person(2, "p2"));
        cache1.put(8, new Person(3, "p3"));


        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches("plan=SELECT")
            .build();

        testLog().registerListener(lsnr);

        long start = System.currentTimeMillis();

        String delete = "delete from test where _key in " +
            "(select p._key from \"pers\".Person p where p._key < sleep_func(?))";

        sql("test", delete, 2000);

        assertTrue(System.currentTimeMillis() - start > LONG_QUERY_WARNING_TIMEOUT);

        assertTrue(lsnr.check());
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
            assertFalse(sql("test", sql, args).iterator().next().isEmpty());
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
                " where p.orgId = o._key and o._key=1 and o._key < sleep_func(?)" +
                " union select o.name n1, p.name n2 from Person p, \"org\".Organization o" +
                " where p.orgId = o._key and o._key=2";

            sqlCheckLongRunningLazyWithMergeTable(select, 2000);
        }
        else if (lazy && !withMergeTable)
            sqlCheckLongRunningLazy("SELECT * FROM test WHERE _key < sleep_func(?)", 2000);
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

        try {
            Iterator<List<?>> it = sql("test", "select * from test").iterator();

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
        }
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

        @QuerySqlFunction
        public static int sleep_func1() {
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException ignored) {
                // No-op
            }

            return 2;
        }
    }

    /**
     * Setup and return test log.
     *
     * @return Test logger.
     */
    private ListeningTestLogger testLog() {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)grid().context().query().getIndexing()).heavyQueriesTracker(),
            "log", testLog);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)grid().context().query().getIndexing()).mapQueryExecutor(),
            "log", testLog);

        GridTestUtils.setFieldValue(grid().context().query().getIndexing(), "log", testLog);

        return testLog;
    }

    /**
     * Getting {@link HeavyQueriesTracker} from the node.
     *
     * @return Heavy queries tracker.
     */
    private HeavyQueriesTracker heavyQueriesTracker() {
        return ((IgniteH2Indexing)grid().context().query().getIndexing()).heavyQueriesTracker();
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
}
