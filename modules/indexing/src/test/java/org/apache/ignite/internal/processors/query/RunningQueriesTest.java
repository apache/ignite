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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Tests for running queries.
 */
public class RunningQueriesTest extends GridCommonAbstractTest {

    /** Timeout in sec. */
    private static final long TIMEOUT_IN_SEC = 5;

    /** Timeout in sec. */
    private static final long TIMEOUT_IN_MS = TIMEOUT_IN_SEC * 1000;

    /** Barrier. */
    static CyclicBarrier barrier;

    /** Ignite. */
    private static IgniteEx ignite;

    /** Node count. */
    private static final int NODE_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridQueryProcessor.idxCls = BlockingIndexing.class;

        ignite = (IgniteEx)startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite.destroyCache(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
        );

        cache.put(100000, 0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        Assert.assertEquals(0, barrier.getNumberWaiting());

        assertNoRunningQueries();
    }

    /**
     *
     */
    public void testQueriesOriginalText() throws Exception {
        newBarrier(3);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<List<List<?>>> fut1 = GridTestUtils.runAsync(() -> cache.query(new SqlFieldsQuery(
            "SELECT * FROM /* comment */ Integer WHERE 1 = 1")).getAll());

        IgniteInternalFuture<List<Cache.Entry<Integer, Integer>>> fut2 = GridTestUtils.runAsync(() -> cache.query(
            new SqlQuery<Integer, Integer>(Integer.class, "FROM /* comment */ Integer WHERE 1 = 1"))
            .getAll());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> barrier.getNumberWaiting() == 2, TIMEOUT_IN_MS));

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(2, runningQueries.size());

        for (GridRunningQueryInfo info : runningQueries)
            assertTrue("Failed to find comment in query: " + info.query(), info.query().contains("/* comment */"));

        assertNoRunningQueries(ignite);

        barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        fut1.get(TIMEOUT_IN_MS);

        fut2.get(TIMEOUT_IN_MS);
    }

    /**
     *
     */
    public void testQueryDML() throws Exception {
        newBarrier(2);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM /* comment */ Integer");

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
            GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(1, runningQueries.size());

        assertNoRunningQueries(ignite);

        runningQueries.forEach((info) -> Assert.assertEquals(qry.getSql(), info.query()));

        IgniteInternalFuture<Integer> fut1 = GridTestUtils.runAsync(() -> barrier.await());

        barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        fut1.get(TIMEOUT_IN_MS);

        fut.get(TIMEOUT_IN_MS);
    }

    /**
     *
     */
    public void testQueryDDL() throws Exception {
        newBarrier(2);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("CREATE TABLE tst(id long PRIMARY KEY, cnt integer)");

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
            GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(1, runningQueries.size());

        assertNoRunningQueries(ignite);

        runningQueries.forEach((info) -> Assert.assertEquals(qry.getSql(), info.query()));

        barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        fut.get(TIMEOUT_IN_MS);

    }

    public void testJdbcBatchDML() throws Exception {
        newBarrier(2);

        try (Connection conn = GridTestUtils.connect(ignite, null); Statement stmt = conn.createStatement()) {
            conn.setSchema("\"default\"");

            final int BATCH_SIZE = 10;

            for (int i = 0; i < BATCH_SIZE; i++)
                stmt.addBatch("insert into Integer (_key, _val) values (" + i + "," + i + ")");

            IgniteInternalFuture<int[]> fut = GridTestUtils.runAsync(() -> stmt.executeBatch());

            for (int i = 0; i < BATCH_SIZE; i++) {
                Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
                    GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));

                Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

                assertEquals(1, runningQueries.size());

                barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
            }

            fut.get(TIMEOUT_IN_MS);
        }
    }

    public void testMultiStatement() throws Exception {
        newBarrier(2);

        String sql =
            "create table test(ID int primary key, NAME varchar(20)); " +
                "insert into test (ID, NAME) values (1, 'name_1');" +
                "insert into test (ID, NAME) values (2, 'name_2'), (3, 'name_3');" +
                "SELECT * FROM test";

        try (Connection conn = GridTestUtils.connect(ignite, null); Statement stmt = conn.createStatement()) {
            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> stmt.execute(sql));

            Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
                GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));

            Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

            assertEquals(4, runningQueries.size());

            barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

            fut.get(TIMEOUT_IN_MS);
        }
    }

    public void testJdbcStreamBatchUpdate() throws Exception {
        try (Connection conn = GridTestUtils.connect(ignite, null); Statement stmt = conn.createStatement()) {
            conn.setSchema("\"default\"");

            newBarrier(1);

            final int BATCH_SIZE = 10;

            stmt.executeUpdate("SET STREAMING ON BATCH_SIZE " + BATCH_SIZE);

            newBarrier(2);

            for (int i = 0; i < BATCH_SIZE; i++)
                stmt.addBatch("insert into Integer (_key, _val) values (" + i + "," + i + ")");

            for (int i = 0; i < BATCH_SIZE; i++) {
                Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
                    GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));

                barrier.await(TIMEOUT_IN_MS, TimeUnit.SECONDS);

                Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
                    GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));

                Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

                assertEquals(1, runningQueries.size());

                barrier.await(TIMEOUT_IN_MS, TimeUnit.SECONDS);
            }
        }

    }

    /**
     * Check all nodes except passed as parameter on no any running queries.
     *
     * @param excludeNodes Nodes shich will be excluded from check.
     */
    private void assertNoRunningQueries(IgniteEx... excludeNodes) {
        Set<UUID> excludeIds = Stream.of(excludeNodes).map((ignite) -> ignite.localNode().id()).collect(Collectors.toSet());

        for (Ignite g : G.allGrids()) {
            IgniteEx node = (IgniteEx)g;

            if (!excludeIds.contains(node.localNode().id())) {
                Collection<GridRunningQueryInfo> runningQueries = node.context().query().runningQueries(-1);

                Assert.assertEquals(0, runningQueries.size());
            }
        }
    }

    /**
     * Create and set new CyclicBarrier for the function.
     *
     * @param parties the number of threads that must invoke await method before the barrier is tripped
     */
    static void newBarrier(int parties) {
        barrier = new CyclicBarrier(parties);
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        @Override public void checkStatementStreamable(PreparedStatement nativeStmt) {
            super.checkStatementStreamable(nativeStmt);

            try {
                barrier.await();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(String schemaName, SqlFieldsQuery qry,
            @Nullable SqlClientContext cliCtx, boolean keepBinary, boolean failOnMultipleStmts,
            MvccQueryTracker tracker,
            GridQueryCancel cancel, boolean clientReq) {

            List<FieldsQueryCursor<List<?>>> res = super.querySqlFields(schemaName, qry, cliCtx, keepBinary, failOnMultipleStmts, tracker, cancel,
                clientReq);
            try {
                barrier.await();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }

            return res;
        }
    }
}
