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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Tests for running queries.
 */
public class RunningQueriesTest extends AbstractIndexingCommonTest {
    /** Timeout in sec. */
    private static final long TIMEOUT_IN_SEC = 5;

    /** Timeout in sec. */
    private static final long TIMEOUT_IN_MS = TIMEOUT_IN_SEC * 1000;

    /** Barrier. */
    private static volatile CyclicBarrier barrier;

    /** Ignite. */
    private static IgniteEx ignite;

    /** Node count. */
    private static final int NODE_CNT = 2;

    /** Restarts the grid if if the last test failed. */
    @Rule public final TestWatcher restarter = new TestWatcher() {
        /** {@inheritDoc} */
        @Override protected void failed(Throwable e, Description lastTest) {
            try {
                log().error("Last test failed [name=" + lastTest.getMethodName() +
                    ", reason=" + e.getMessage() + "]. Restarting the grid.");

                // Release the indexing.
                if (barrier != null)
                    barrier.reset();

                stopAllGrids();

                beforeTestsStarted();

                log().error("Grid restarted.");
            }
            catch (Exception restartFailure) {
                throw new RuntimeException("Failed to recover after test failure [test=" + lastTest.getMethodName() +
                    ", reason=" + e.getMessage() + "]. Subsequent test results of this test class are incorrect.",
                    restartFailure);
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridQueryProcessor.idxCls = BlockingIndexing.class;

        ignite = (IgniteEx)startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        newBarrier(1);

        ignite.destroyCache(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
            .setIndexedTypes(Integer.class,Integer.class)
        );

        cache.put(100000, 0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        Assert.assertEquals(0, barrier.getNumberWaiting());

        assertNoRunningQueries();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi() {

            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                if (CustomMessageWrapper.class.isAssignableFrom(msg.getClass())) {
                    DiscoveryCustomMessage delegate = ((CustomMessageWrapper)msg).delegate();

                    if (DynamicCacheChangeBatch.class.isAssignableFrom(delegate.getClass())) {
                        ((DynamicCacheChangeBatch)delegate).requests().stream()
                            .filter((c) -> !c.cacheName().equalsIgnoreCase("default"))
                            .findAny()
                            .ifPresent((c) -> {
                                try {
                                    awaitTimeout();
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                    }
                    else if (SchemaProposeDiscoveryMessage.class.isAssignableFrom(delegate.getClass())) {
                        try {
                            awaitTimeout();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                super.sendCustomEvent(msg);
            }
        });

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                if (GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                    Message gridMsg = ((GridIoMessage)msg).message();

                    if (GridNearAtomicSingleUpdateFilterRequest.class.isAssignableFrom(gridMsg.getClass())
                        || GridNearAtomicFullUpdateRequest.class.isAssignableFrom(gridMsg.getClass())
                    ) {
                        try {
                            awaitTimeout();
                        }
                        catch (Exception ignore) {
                        }
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /**
     * Check cleanup running queries on node stop.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testCloseRunningQueriesOnNodeStop() throws Exception {
        IgniteEx ign = startGrid(super.getConfiguration("TST"));

        IgniteCache<Integer, Integer> cache = ign.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("TST")
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
        );

        for (int i = 0; i < 10000; i++)
            cache.put(i, i);

        cache.query(new SqlFieldsQuery("SELECT * FROM Integer order by _key"));

        Assert.assertEquals("Should be one running query",
            1,
            ign.context().query().runningQueries(-1).size());

        ign.close();

        Assert.assertEquals(0, ign.context().query().runningQueries(-1).size());
    }

    /**
     * Check auto cleanup running queries on fully read iterator.
     */
    @SuppressWarnings("CodeBlock2Expr")
    @Test
    public void testAutoCloseQueryAfterIteratorIsExhausted() {
        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        FieldsQueryCursor<List<?>> query = cache.query(new SqlFieldsQuery("SELECT * FROM Integer order by _key"));

        query.iterator().forEachRemaining((e) -> {
            Assert.assertEquals("Should be one running query",
                1,
                ignite.context().query().runningQueries(-1).size());
        });

        assertNoRunningQueries();
    }

    /**
     * Check cluster wide query id generation.
     */
    @Test
    public void testClusterWideQueryIdGeneration() {
        newBarrier(1);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            FieldsQueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("SELECT * FROM Integer WHERE 1 = 1"));

            Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

            assertEquals(1, runningQueries.size());

            GridRunningQueryInfo r = runningQueries.iterator().next();

            assertEquals(ignite.context().localNodeId() + "_" + r.id(), r.globalQueryId());

            cursor.close();
        }
    }

    /**
     * Check tracking running queries for Select.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testQueries() throws Exception {
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

        awaitTimeout();

        fut1.get(TIMEOUT_IN_MS);

        fut2.get(TIMEOUT_IN_MS);
    }

    /**
     * Check tracking running queries for DELETE.
     *
     * @throws Exception Exception in case of failure.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11510")
    @Test
    public void testQueryDmlDelete() throws Exception {
        testQueryDML("DELETE FROM /* comment */ Integer");
    }

    /**
     * Check tracking running queries for INSERT.
     *
     * @throws Exception Exception in case of failure.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11510")
    @Test
    public void testQueryDmlInsert() throws Exception {
        testQueryDML("INSERT INTO Integer(_key, _val) VALUES(1,1)");
    }

    /**
     * Check tracking running queries for UPDATE.
     *
     * @throws Exception Exception in case of failure.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11510")
    @Test
    public void testQueryDmlUpdate() throws Exception {
        testQueryDML("UPDATE Integer set _val = 1 where 1=1");
    }

    /**
     * Check tracking running queries for DML.
     *
     * @param dmlQry DML query.
     * @throws Exception Exception in case of failure.
     */
    public void testQueryDML(String dmlQry) throws Exception {
        newBarrier(2);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(dmlQry);

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        assertWaitingOnBarrier();

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(1, runningQueries.size());

        assertNoRunningQueries(ignite);

        runningQueries.forEach((info) -> Assert.assertEquals(qry.getSql(), info.query()));

        IgniteInternalFuture<Integer> fut1 = GridTestUtils.runAsync(() -> barrier.await());

        awaitTimeout();

        fut1.get(TIMEOUT_IN_MS);

        fut.get(TIMEOUT_IN_MS);
    }

    /**
     * Check tracking running queries for DROP INDEX.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testQueryDdlDropIndex() throws Exception {
        newBarrier(1);

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE tst_idx_drop(id long PRIMARY KEY, cnt integer)"));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX tst_idx_drop_idx ON tst_idx_drop(cnt)"));

        testQueryDDL("DROP INDEX tst_idx_drop_idx");
    }

    /**
     * Check tracking running queries for CREATE INDEX.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testQueryDdlCreateIndex() throws Exception {
        newBarrier(1);

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE tst_idx_create(id long PRIMARY KEY, cnt integer)"));

        testQueryDDL("CREATE INDEX tst_idx_create_idx ON tst_idx_create(cnt)");
    }

    /**
     * Check tracking running queries for DROP TABLE.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testQueryDdlDropTable() throws Exception {
        newBarrier(1);

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE tst_drop(id long PRIMARY KEY, cnt integer)"));

        testQueryDDL("DROP TABLE tst_drop");
    }

    /**
     * Check tracking running queries for CREATE TABLE.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testQueryDdlCreateTable() throws Exception {
        testQueryDDL("CREATE TABLE tst_create(id long PRIMARY KEY, cnt integer)");
    }

    /**
     * Check tracking running queries for DDL.
     *
     * @throws Exception Exception in case of failure.
     */
    public void testQueryDDL(String sql) throws Exception {
        newBarrier(2);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        assertWaitingOnBarrier();

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(1, runningQueries.size());

        assertNoRunningQueries(ignite);

        runningQueries.forEach((info) -> Assert.assertEquals(qry.getSql(), info.query()));

        awaitTimeout();

        awaitTimeout();

        fut.get(TIMEOUT_IN_MS);
    }

    /**
     * Check tracking running queries for batches.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testJdbcBatchDML() throws Exception {
        newBarrier(2);

        try (Connection conn = GridTestUtils.connect(ignite, null); Statement stmt = conn.createStatement()) {
            conn.setSchema("\"default\"");

            final int BATCH_SIZE = 10;

            int key = 0;

            for (int i = 0; i < BATCH_SIZE; i++) {
                while (ignite.affinity(DEFAULT_CACHE_NAME).isPrimary(ignite.localNode(), key))
                    key++;

                stmt.addBatch("insert into Integer (_key, _val) values (" + key + "," + key + ")");

                key++;
            }

            IgniteInternalFuture<int[]> fut = GridTestUtils.runAsync(stmt::executeBatch);

            for (int i = 0; i < BATCH_SIZE; i++) {
                assertWaitingOnBarrier();

                Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

                assertEquals(1, runningQueries.size());

                awaitTimeout();

                assertWaitingOnBarrier();

                awaitTimeout();
            }

            fut.get(TIMEOUT_IN_MS);
        }
    }

    /**
     * Check tracking running queries for multi-statements.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testMultiStatement() throws Exception {
        newBarrier(2);

        int key = 0;

        int[] notAffinityKey = new int[2];

        for (int i = 0; i < notAffinityKey.length; i++) {
            while (ignite.affinity(DEFAULT_CACHE_NAME).isPrimary(ignite.localNode(), key))
                key++;

            notAffinityKey[i] = key;

            key++;
        }

        String[] queries = {
            "create table test(ID int primary key, NAME varchar(20))",
            "insert into test (ID, NAME) values (" + notAffinityKey[0] + ", 'name')",
            "insert into test (ID, NAME) values (" + notAffinityKey[1] + ", 'name')",
            "SELECT * FROM test"
        };

        String sql = String.join(";", queries);

        try (Connection conn = GridTestUtils.connect(ignite, null); Statement stmt = conn.createStatement()) {
            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> stmt.execute(sql));

            for (String query : queries) {
                assertWaitingOnBarrier();

                List<GridRunningQueryInfo> runningQueries = (List<GridRunningQueryInfo>)ignite.context().query()
                    .runningQueries(-1);

                assertEquals(1, runningQueries.size());

                assertEquals(query, runningQueries.get(0).query());

                awaitTimeout();
            }

            fut.get(TIMEOUT_IN_MS);
        }
    }

    /**
     * Check tracking running queries for stream COPY command.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testCopyCommand() throws Exception {
        try (Connection conn = GridTestUtils.connect(ignite, null); Statement stmt = conn.createStatement()) {
            conn.setSchema("\"default\"");

            newBarrier(1);

            stmt.execute("CREATE TABLE Person(id integer primary key, age integer, firstName varchar, lastname varchar)");

            String path = Objects.requireNonNull(resolveIgnitePath("/modules/clients/src/test/resources/bulkload1.csv"))
                .getAbsolutePath();

            newBarrier(2);

            String sql = "copy from '" + path + "'" +
                " into Person" +
                " (_key, age, firstName, lastName)" +
                " format csv charset 'ascii'";

            IgniteInternalFuture<Integer> fut = GridTestUtils.runAsync(() -> stmt.executeUpdate(sql));

            assertWaitingOnBarrier();

            List<GridRunningQueryInfo> runningQueries = (List<GridRunningQueryInfo>)ignite.context().query().runningQueries(-1);

            assertEquals(1, runningQueries.size());

            assertEquals(sql, runningQueries.get(0).query());

            awaitTimeout();

            fut.get(TIMEOUT_IN_MS);
        }
    }

    /**
     * Assert that on barrier waiting one thread.
     *
     * @throws IgniteInterruptedCheckedException In case of failure.
     */
    private void assertWaitingOnBarrier() throws IgniteInterruptedCheckedException {
        Assert.assertTrue("Still waiting " + barrier.getNumberWaiting() + " parties",
            GridTestUtils.waitForCondition(() -> barrier.getNumberWaiting() == 1, TIMEOUT_IN_MS));
    }

    /**
     * Check all nodes except passed as parameter on no any running queries.
     *
     * @param excludeNodes Nodes which will be excluded from check.
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
    private static void newBarrier(int parties) {
        barrier = new CyclicBarrier(parties);
    }

    /**
     * @throws InterruptedException In case of failure.
     * @throws TimeoutException In case of failure.
     * @throws BrokenBarrierException In case of failure.
     */
    private static void awaitTimeout() throws InterruptedException, TimeoutException, BrokenBarrierException {
        barrier.await(TIMEOUT_IN_MS, TimeUnit.SECONDS);
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(
            String schemaName,
            SqlFieldsQuery qry,
            @Nullable SqlClientContext cliCtx,
            boolean keepBinary,
            boolean failOnMultipleStmts,
            GridQueryCancel cancel
        ) {
            List<FieldsQueryCursor<List<?>>> res = super.querySqlFields(
                schemaName,
                qry,
                cliCtx,
                keepBinary,
                failOnMultipleStmts,
                cancel
            );

            try {
                awaitTimeout();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }

            return res;
        }
    }
}
