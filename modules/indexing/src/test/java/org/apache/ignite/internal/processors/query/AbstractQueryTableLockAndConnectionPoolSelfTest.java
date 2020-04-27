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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.ConnectionManager;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for query execution check cases for correct table lock/unlock.
 */
public abstract class AbstractQueryTableLockAndConnectionPoolSelfTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 500;

    /** Base query argument. */
    private static final int BASE_QRY_ARG = 50;

    /** Size for small pages. */
    private static final int PAGE_SIZE_SMALL = 12;

    /** Test duration. */
    private static final long TEST_DUR = GridTestUtils.SF.applyLB(10_000, 3_000);

    /** Run query local . */
    private static boolean local = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test local query execution.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNode() throws Exception {
        checkSingleNode(1);
    }

    /**
     * Test local query execution.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeWithParallelism() throws Exception {
        checkSingleNode(4);
    }

    /**
     * Test query execution with multiple topology nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodes() throws Exception {
        checkMultipleNodes(1);
    }

    /**
     * Test query execution with multiple topology nodes with query parallelism.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesWithParallelism() throws Exception {
        checkMultipleNodes(4);
    }

    /**
     * Test DDL operation on table with high load queries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeTablesLockQueryAndDDLMultithreaded() throws Exception {
        final Ignite srv = startGrid(0);

        populateBaseQueryData(srv, 1);

        checkTablesLockQueryAndDDLMultithreaded(srv);

        checkTablesLockQueryAndDropColumnMultithreaded(srv);
    }

    /**
     * Test DDL operation on table with high load local queries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeTablesLockQueryLocalAndDDLMultithreaded() throws Exception {
        local = true;

        try {
            final Ignite srv = startGrid(0);

            populateBaseQueryData(srv, 1);

            checkTablesLockQueryAndDDLMultithreaded(srv);

            checkTablesLockQueryAndDropColumnMultithreaded(srv);
        }
        finally {
            local = false;
        }
    }

    /**
     * Test DDL operation on table with high load queries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeWithParallelismTablesLockQueryAndDDLMultithreaded() throws Exception {
        final Ignite srv = startGrid(0);

        populateBaseQueryData(srv, 4);

        checkTablesLockQueryAndDDLMultithreaded(srv);

        checkTablesLockQueryAndDropColumnMultithreaded(srv);
    }

    /**
     * Test DDL operation on table with high load queries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesWithTablesLockQueryAndDDLMultithreaded() throws Exception {
        Ignite srv0 = startGrid(0);
        Ignite srv1 = startGrid(1);
        startGrid(2);

        Ignite cli;

        try {
            Ignition.setClientMode(true);

            cli = startGrid(3);
        }
        finally {
            Ignition.setClientMode(false);
        }

        populateBaseQueryData(srv0, 1);

        checkTablesLockQueryAndDDLMultithreaded(srv0);
        checkTablesLockQueryAndDDLMultithreaded(srv1);
        checkTablesLockQueryAndDDLMultithreaded(cli);

        checkTablesLockQueryAndDropColumnMultithreaded(srv0);
        checkTablesLockQueryAndDropColumnMultithreaded(srv1);
        // TODO: +++ DDL DROP COLUMN CacheContext == null on CLI
        // checkTablesLockQueryAndDropColumnMultithreaded(cli);
    }

    /**
     * Test DDL operation on table with high load queries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodesWithParallelismTablesLockQueryAndDDLMultithreaded() throws Exception {
        Ignite srv0 = startGrid(0);
        Ignite srv1 = startGrid(1);
        startGrid(2);

        Ignite cli;

        try {
            Ignition.setClientMode(true);

            cli = startGrid(3);
        }
        finally {
            Ignition.setClientMode(false);
        }

        populateBaseQueryData(srv0, 4);

        checkTablesLockQueryAndDDLMultithreaded(srv0);
        checkTablesLockQueryAndDDLMultithreaded(srv1);
        checkTablesLockQueryAndDDLMultithreaded(cli);

        checkTablesLockQueryAndDropColumnMultithreaded(srv0);
        checkTablesLockQueryAndDropColumnMultithreaded(srv1);
        // TODO: +++ DDL DROP COLUMN CacheContext == null on CLI
        // checkTablesLockQueryAndDropColumnMultithreaded(cli);
    }

    /**
     * Test release reserved partition after query complete (results is bigger than one page).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleasePartitionReservationSeveralPagesResults() throws Exception {
        checkReleasePartitionReservation(PAGE_SIZE_SMALL);
    }

    /**
     * Test release reserved partition after query complete (results is placed on one page).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleasePartitionReservationOnePageResults() throws Exception {
        checkReleasePartitionReservation(KEY_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFetchFromRemovedTable() throws Exception {
        Ignite srv = startGrid(0);

        execute(srv, "CREATE TABLE TEST (id int primary key, val int)");

        for (int i = 0; i < 10; ++i)
            execute(srv, "INSERT INTO TEST VALUES (" + i + ", " + i + ")");

        FieldsQueryCursor<List<?>> cur = execute(srv, new SqlFieldsQuery("SELECT * from TEST").setPageSize(1));

        Iterator<List<?>> it = cur.iterator();

        it.next();

        execute(srv, "DROP TABLE TEST");

        try {
            while (it.hasNext())
                it.next();

            if (lazy())
                fail("Retry exception must be thrown");
        }
        catch (Exception e) {
            if (!lazy()) {
                log.error("In lazy=false mode the query must be finished successfully", e);

                fail("In lazy=false mode the query must be finished successfully");
            }
            else
                assertNotNull(X.cause(e, QueryRetryException.class));
        }
    }

    /**
     * @param node Ignite node to execute query.
     * @throws Exception If failed.
     */
    private void checkTablesLockQueryAndDDLMultithreaded(final Ignite node) throws Exception {
        final AtomicBoolean end = new AtomicBoolean(false);

        final int qryThreads = 10;

        // Do many concurrent queries.
        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                while (!end.get()) {
                    try {
                        FieldsQueryCursor<List<?>> cursor = execute(node, new SqlFieldsQueryEx(
                            "SELECT pers.id, pers.name " +
                            "FROM (SELECT DISTINCT p.id, p.name " +
                            "FROM \"pers\".PERSON as p) as pers " +
                            "JOIN \"pers\".PERSON p on p.id = pers.id " +
                            "JOIN (SELECT t.persId as persId, SUM(t.time) totalTime " +
                            "FROM \"persTask\".PersonTask as t GROUP BY t.persId) as task ON task.persId = pers.id", true)
                            .setLazy(lazy())
                            .setLocal(local)
                            .setPageSize(PAGE_SIZE_SMALL));

                        cursor.getAll();
                    }
                    catch (Exception e) {
                        if (X.cause(e, QueryRetryException.class) == null) {
                            log.error("Unexpected exception", e);

                            fail("Unexpected exception. " + e);
                        }
                        else if (!lazy()) {
                            log.error("Unexpected exception", e);

                            fail("Unexpected QueryRetryException.");
                        }
                    }
                }
            }
        }, qryThreads, "usr-qry");

        long tEnd = U.currentTimeMillis() + TEST_DUR;

        while (U.currentTimeMillis() < tEnd) {
            execute(node, new SqlFieldsQuery("CREATE INDEX \"pers\".PERSON_NAME ON \"pers\".Person (name asc)")).getAll();
            execute(node, new SqlFieldsQuery("DROP INDEX \"pers\".PERSON_NAME")).getAll();
        }

        // Test is OK in case DDL operations is passed on hi load queries pressure.
        end.set(true);
        fut.get();

        checkConnectionLeaks(Ignition.allGrids().size());
    }

    /**
     * @param node Ignite node to execute query.
     * @throws Exception If failed.
     */
    private void checkTablesLockQueryAndDropColumnMultithreaded(final Ignite node) throws Exception {
        final AtomicBoolean end = new AtomicBoolean(false);

        final int qryThreads = 10;

        // Do many concurrent queries.
        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                while (!end.get()) {
                    try {
                        FieldsQueryCursor<List<?>> cursor = execute(node, new SqlFieldsQuery(
                            "SELECT pers.id, pers.name FROM \"pers\".PERSON")
                            .setLazy(lazy())
                            .setPageSize(PAGE_SIZE_SMALL));

                        cursor.getAll();
                    }
                    catch (Exception e) {
                        if (e.getMessage().contains("Failed to parse query. Column \"PERS.ID\" not found")) {
                            // Swallow exception when column is dropped.
                        }
                        else if (X.cause(e, QueryRetryException.class) == null) {
                            log.error("Unexpected exception", e);

                            fail("Unexpected exception. " + e);
                        }
                        else if (!lazy()) {
                            log.error("Unexpected exception", e);

                            fail("Unexpected QueryRetryException.");
                        }
                    }
                }
            }
        }, qryThreads, "usr-qry");

        long tEnd = U.currentTimeMillis() + TEST_DUR;

        while (U.currentTimeMillis() < tEnd) {
            execute(node, new SqlFieldsQuery("ALTER TABLE \"pers\".Person DROP COLUMN name")).getAll();
            execute(node, new SqlFieldsQuery("ALTER TABLE \"pers\".Person ADD  COLUMN name varchar")).getAll();
        }

        // Test is OK in case DDL operations is passed on hi load queries pressure.
        end.set(true);
        fut.get();

        checkConnectionLeaks(Ignition.allGrids().size());
    }

    /**
     * Test release reserved partition after query complete.
     * In case partitions not released the `awaitPartitionMapExchange` fails by timeout.
     *
     * @param pageSize Results page size.
     * @throws Exception If failed.
     */
    public void checkReleasePartitionReservation(int pageSize) throws Exception {
        Ignite srv1 = startGrid(1);
        startGrid(2);

        populateBaseQueryData(srv1, 1);

        FieldsQueryCursor<List<?>> cursor = execute(srv1, query(0).setPageSize(pageSize));

        cursor.getAll();

        startGrid(3);

        awaitPartitionMapExchange();
    }

    /**
     * Check local query execution.
     *
     * @param parallelism Query parallelism.
     * @throws Exception If failed.
     */
    public void checkSingleNode(int parallelism) throws Exception {
        Ignite srv = startGrid(0);

        populateBaseQueryData(srv, parallelism);

        checkBaseOperations(srv);
    }

    /**
     * Check query execution with multiple topology nodes.
     *
     * @param parallelism Query parallelism.
     * @throws Exception If failed.
     */
    public void checkMultipleNodes(int parallelism) throws Exception {
        Ignite srv1 = startGrid(0);
        Ignite srv2 = startGrid(1);

        Ignite cli;

        try {
            Ignition.setClientMode(true);

            cli = startGrid(2);
        }
        finally {
            Ignition.setClientMode(false);
        }

        populateBaseQueryData(cli, parallelism);

        checkBaseOperations(srv1);
        checkBaseOperations(srv2);
        checkBaseOperations(cli);

        // Test originating node leave.
        FieldsQueryCursor<List<?>> cursor = execute(cli, baseQuery().setPageSize(PAGE_SIZE_SMALL));

        Iterator<List<?>> iter = cursor.iterator();

        for (int i = 0; i < 30; i++)
            iter.next();

        stopGrid(2);

        // Test server node leave with active worker.
        FieldsQueryCursor<List<?>> cursor2 = execute(srv1, baseQuery().setPageSize(PAGE_SIZE_SMALL));

        try {
            Iterator<List<?>> iter2 = cursor2.iterator();

            for (int i = 0; i < 30; i++)
                iter2.next();

            stopGrid(1);
        }
        finally {
            cursor2.close();
        }
    }

    /**
     * Check base operations.
     *
     * @param node Node.
     * @throws Exception If failed.
     */
    private void checkBaseOperations(Ignite node) throws Exception {
        checkQuerySplitToSeveralMapQueries(node);

        // Get full data.
        {
            List<List<?>> rows = execute(node, baseQuery()).getAll();

            assertBaseQueryResults(rows);
        }

        // Check QueryRetryException is thrown
        {
            List<List<?>> rows = new ArrayList<>();

            FieldsQueryCursor<List<?>> cursor = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL));

            Iterator<List<?>> it = cursor.iterator();

            for (int i = 0; i < 10; ++i)
                rows.add(it.next());

            execute(node, new SqlFieldsQuery("CREATE INDEX \"pers\".PERSON_NAME ON \"pers\".Person (name asc)")).getAll();
            execute(node, new SqlFieldsQuery("DROP INDEX \"pers\".PERSON_NAME")).getAll();

            try {
                while (it.hasNext())
                    rows.add(it.next());

                if (lazy())
                    fail("Retry exception must be thrown");
            }
            catch (Exception e) {
                if (!lazy() || X.cause(e, QueryRetryException.class) == null) {
                    log.error("Invalid exception: ", e);

                    fail("QueryRetryException is expected");
                }
            }
        }

        // Get data in several pages.
        {
            List<List<?>> rows = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL)).getAll();

            assertBaseQueryResults(rows);
        }

        // Test full iteration.
        {
            List<List<?>> rows = new ArrayList<>();

            FieldsQueryCursor<List<?>> cursor = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL));

            for (List<?> row : cursor)
                rows.add(row);

            cursor.close();

            assertBaseQueryResults(rows);
        }

        // Test partial iteration with cursor close.
        try (FieldsQueryCursor<List<?>> partialCursor = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL))) {
            Iterator<List<?>> iter = partialCursor.iterator();

            for (int i = 0; i < 30; i++)
                iter.next();
        }

        // Test execution of multiple queries at a time.
        List<Iterator<List<?>>> iters = new ArrayList<>();

        for (int i = 0; i < 200; i++)
            iters.add(execute(node, randomizedQuery().setPageSize(PAGE_SIZE_SMALL)).iterator());

        while (!iters.isEmpty()) {
            Iterator<Iterator<List<?>>> iterIter = iters.iterator();

            while (iterIter.hasNext()) {
                Iterator<List<?>> iter = iterIter.next();

                int i = 0;

                while (iter.hasNext() && i < 20) {
                    iter.next();

                    i++;
                }

                if (!iter.hasNext())
                    iterIter.remove();
            }
        }

        checkConnectionLeaks(Ignition.allGrids().size());

        checkHoldQuery(node);

        checkShortQuery(node);
    }

    /**
     * @param node Ignite node.
     * @throws Exception If failed.
     */
    public void checkHoldQuery(Ignite node) throws Exception {
        ArrayList rows = new ArrayList<>();

        Iterator<List<?>> it0 = execute(node, query(BASE_QRY_ARG).setPageSize(PAGE_SIZE_SMALL)).iterator();
        rows.add(it0.next());

        // Do many concurrent queries to Test full iteration.
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 5; ++i) {
                    FieldsQueryCursor<List<?>> cursor = execute(node, query(KEY_CNT - PAGE_SIZE_SMALL + 1)
                        .setPageSize(PAGE_SIZE_SMALL));

                    cursor.getAll();
                }
            }
        }, 5, "test-qry");

        // Do the same query in the same thread.
        {
            FieldsQueryCursor<List<?>> cursor = execute(node, query(BASE_QRY_ARG)
                .setPageSize(PAGE_SIZE_SMALL));

            cursor.getAll();
        }

        while (it0.hasNext())
            rows.add(it0.next());

        assertBaseQueryResults(rows);
    }

    /**
     * @param node Ignite node.
     * @throws Exception If failed.
     */
    public void checkShortQuery(Ignite node) throws Exception {
        ArrayList rows = new ArrayList<>();

        FieldsQueryCursor<List<?>> cursor0 = execute(node, query(KEY_CNT - PAGE_SIZE_SMALL + 1).setPageSize(PAGE_SIZE_SMALL));

        Iterator<List<?>> it = cursor0.iterator();

        while (it.hasNext())
            rows.add(it.next());

        assertQueryResults(rows, KEY_CNT - PAGE_SIZE_SMALL + 1);
    }

    /**
     * @param node Ignite node.
     * @throws Exception If failed.
     */
    public void checkQuerySplitToSeveralMapQueries(Ignite node) throws Exception {
        ArrayList rows = new ArrayList<>();

        FieldsQueryCursor<List<?>> cursor0 = execute(node, new SqlFieldsQuery(
            "SELECT pers.id, pers.name " +
            "FROM (SELECT DISTINCT p.id, p.name " +
                "FROM \"pers\".PERSON as p) as pers " +
            "JOIN \"pers\".PERSON p on p.id = pers.id " +
            "JOIN (SELECT t.persId as persId, SUM(t.time) totalTime " +
                "FROM \"persTask\".PersonTask as t GROUP BY t.persId) as task ON task.persId = pers.id")
            .setPageSize(PAGE_SIZE_SMALL));

        Iterator<List<?>> it = cursor0.iterator();

        while (it.hasNext())
            rows.add(it.next());

        assertQueryResults(rows, 0);
    }

    /**
     * Populate base query data.
     *
     * @param node Node.
     * @param parallelism Query parallelism.
     */
    private static void populateBaseQueryData(Ignite node, int parallelism) {
        node.createCache(cacheConfiguration(parallelism, "pers", Person.class));
        node.createCache(cacheConfiguration(parallelism, "persTask", PersonTask.class));

        IgniteCache<Long, Object> pers = cache(node, "pers");

        for (long i = 0; i < KEY_CNT; i++)
            pers.put(i, new Person(i));

        IgniteCache<Long, Object> comp = cache(node, "persTask");

        for (long i = 0; i < KEY_CNT; i++)
            comp.put(i, new PersonTask(i));
    }

    /**
     * @return Query with randomized argument.
     */
    private static SqlFieldsQuery randomizedQuery() {
        return query(ThreadLocalRandom.current().nextInt(KEY_CNT / 2));
    }

    /**
     * @return Base query.
     */
    private static SqlFieldsQuery baseQuery() {
        return query(BASE_QRY_ARG);
    }

    /**
     * @param parallelism Query parallelism.
     * @param name Cache name.
     * @param valClass Value class.
     * @return Default cache configuration.
     */
    private static CacheConfiguration<Long, Person> cacheConfiguration(int parallelism, String name, Class valClass) {
        return new CacheConfiguration<Long, Person>()
            .setName(name)
            .setIndexedTypes(Long.class, valClass)
            .setQueryParallelism(parallelism)
            .setAffinity(new RendezvousAffinityFunction(false, 10));
    }

    /**
     * Default query.
     *
     * @param arg Argument.
     * @return Query.
     */
    private static SqlFieldsQuery query(long arg) {
        return new SqlFieldsQuery(
            "SELECT id, name FROM \"pers\".Person WHERE id >= " + arg);
    }

    /**
     * Assert base query results.
     *
     * @param rows Result rows.
     */
    private static void assertBaseQueryResults(List<List<?>> rows) {
        assertQueryResults(rows, BASE_QRY_ARG);
    }

    /**
     * Assert base query results.
     *
     * @param rows Result rows.
     * @param resSize Result size.
     */
    private static void assertQueryResults(List<List<?>> rows, int resSize) {
        assertEquals(KEY_CNT - resSize, rows.size());

        for (List<?> row : rows) {
            Long id = (Long)row.get(0);
            String name = (String)row.get(1);

            assertTrue(id >= resSize);
            assertEquals(nameForId(id), name);
        }
    }

    /**
     * Get cache for node.
     *
     * @param node Ignite node to get cache.
     * @param name Cache name.
     * @return Cache.
     */
    private static IgniteCache<Long, Object> cache(Ignite node, String name) {
        return node.cache(name);
    }

    /**
     * Execute query on the given cache.
     *
     * @param node Node.
     * @param sql Query.
     * @return Cursor.
     */
    private FieldsQueryCursor<List<?>> execute(Ignite node, String sql) {
        return ((IgniteEx)node).context().query().querySqlFields(new SqlFieldsQuery(sql).setLazy(lazy()), false);
    }

    /**
     * Execute query on the given cache.
     *
     * @param node Node.
     * @param qry Query.
     * @return Cursor.
     */
    private FieldsQueryCursor<List<?>> execute(Ignite node, SqlFieldsQuery qry) {
        return ((IgniteEx)node).context().query().querySqlFields(qry.setLazy(lazy()), false);
    }

    /**
     * @return Lazy mode.
     */
    protected abstract boolean lazy();

    /**
     * Get name for ID.
     *
     * @param id ID.
     * @return Name.
     */
    private static String nameForId(long id) {
        return "name-" + id;
    }

    /**
     * @param nodeCnt Count of nodes.
     * @throws Exception On error.
     */
    private void checkConnectionLeaks(int nodeCnt) throws Exception {
        boolean notLeak = GridTestUtils.waitForCondition(() -> {
            for (int i = 0; i < nodeCnt; i++) {
                if (!usedConnections(i).isEmpty())
                    return false;
            }

            return true;
        }, 5000);

        if (!notLeak) {
            for (int i = 0; i < nodeCnt; i++) {
                Set<H2PooledConnection> usedConns = usedConnections(i);

                if (!usedConnections(i).isEmpty())
                    log.error("Not closed connections: " + usedConns);
            }

            fail("H2 JDBC connections leak detected. See the log above.");
        }
    }

    /**
     * @param i Node index.
     * @return Set of used connections.
     */
    private Set<H2PooledConnection> usedConnections(int i) {
        ConnectionManager connMgr = ((IgniteH2Indexing)grid(i).context().query().getIndexing()).connections();

        return GridTestUtils.getFieldValue(connMgr, "usedConns");
    }

    /**
     * Person class.
     */
    private static class Person {
        /** ID. */
        @QuerySqlField(index = true)
        private long id;

        /** Name. */
        @QuerySqlField
        private String name;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        public Person(long id) {
            this.id = id;
            this.name = nameForId(id);
        }

        /**
         * @return ID.
         */
        public long id() {
            return id;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }
    }

    /**
     * Company class.
     */
    private static class PersonTask {
        /** ID. */
        @QuerySqlField(index = true)
        private long id;

        @QuerySqlField(index = true)
        private long persId;

        /** Name. */
        @QuerySqlField
        private long time;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        public PersonTask(long id) {
            this.id = id;
            this.persId = id;
            this.time = id;
        }

        /**
         * @return ID.
         */
        public long id() {
            return id;
        }

        /**
         * @return Name.
         */
        public long time() {
            return time;
        }
    }
}
