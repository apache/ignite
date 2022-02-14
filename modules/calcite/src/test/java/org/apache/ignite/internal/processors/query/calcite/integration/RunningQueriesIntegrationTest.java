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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.QueryState;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_EXPERIMENTAL_SQL_ENGINE_PLANNER_TIMEOUT;

/**
 *
 */
@WithSystemProperty(key = IGNITE_EXPERIMENTAL_SQL_ENGINE_PLANNER_TIMEOUT, value = "5000")
public class RunningQueriesIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final long PLANNER_TIMEOUT = getLong(IGNITE_EXPERIMENTAL_SQL_ENGINE_PLANNER_TIMEOUT, 0);

    /** */
    private static IgniteEx srv;

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = grid(0);
    }

    /**
     * Execute query with a lot of JOINs to produce very long planning phase.
     * Cancel query on planning phase and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtPlanningPhase() throws IgniteCheckedException {
        QueryEngine engine = queryProcessor(client);
        int cnt = 9;

        for (int i = 0; i < cnt; i++)
            sql("CREATE TABLE test_tbl" + i + " (id int, val varchar)");

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "test_tbl" + i + " p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> !engine.runningQueries().isEmpty() || fut.isDone(), TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = engine.runningQueries();

        assertEquals("Running: " + running, 1, running.size());

        RunningQuery qry = F.first(running);

        assertSame(qry, engine.runningQuery(qry.id()));

        // Waits for planning.
        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> qry.state() == QueryState.PLANNING, TIMEOUT_IN_MS));

        qry.cancel();

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(0), IgniteSQLException.class, "The query was cancelled while planning");
    }

    /**
     * Execute query with a latch on excution phase.
     * Cancel query on execution phase and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtExecutionPhase() throws Exception {
        CalciteQueryProcessor cliEngine = queryProcessor(client);
        CalciteQueryProcessor srvEngine = queryProcessor(srv);

        sql("CREATE TABLE person (id int, val varchar)");
        sql("INSERT INTO person (id, val) VALUES (?, ?)", 0, "val0");

        IgniteCacheTable oldTbl = (IgniteCacheTable)srvEngine.schemaHolder().schema("PUBLIC").getTable("PERSON");

        CountDownLatch scanLatch = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean();

        IgniteCacheTable newTbl = new CacheTableImpl(srv.context(), oldTbl.descriptor()) {
            @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup grp,
                Predicate<Row> filter,
                Function<Row, Row> rowTransformer,
                @Nullable ImmutableBitSet usedColumns
            ) {
                return new Iterable<Row>() {
                    @NotNull @Override public Iterator<Row> iterator() {
                        scanLatch.countDown();

                        return new Iterator<Row>() {
                            @Override public boolean hasNext() {
                                // Produce rows until stopped.
                                return !stop.get();
                            }

                            @Override public Row next() {
                                if (stop.get())
                                    throw new NoSuchElementException();

                                return execCtx.rowHandler().factory().create();
                            }
                        };
                    }
                };
            }
        };

        srvEngine.schemaHolder().schema("PUBLIC").add("PERSON", newTbl);

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql("SELECT * FROM person"));

        try {
            scanLatch.await(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

            // Check state on server.
            assertEquals(1, srvEngine.runningQueries().size());
            assertEquals(QueryState.EXECUTING, F.first(srvEngine.runningQueries()).state());

            // Check state on client.
            assertEquals(1, cliEngine.runningQueries().size());
            RunningQuery qry = F.first(cliEngine.runningQueries());
            assertEquals(QueryState.EXECUTING, qry.state());

            qry.cancel();

            Assert.assertTrue(GridTestUtils.waitForCondition(
                () -> srvEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

            Assert.assertTrue(GridTestUtils.waitForCondition(
                () -> cliEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));
        }
        finally {
            stop.set(true);
        }

        GridTestUtils.assertThrowsAnyCause(log,
            () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

    /**
     * Execute query with a lot of JOINs to produce very long excution phase.
     * Cancel query on execution phase on remote node (no query originator node)
     * and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelByRemoteFragment() throws IgniteCheckedException {
        QueryEngine clientEngine = queryProcessor(client);
        QueryEngine serverEngine = queryProcessor(srv);
        int cnt = 6;

        sql("CREATE TABLE t (id int, val varchar)");

        String data = IntStream.range(0, 10000).mapToObj((i) -> "(" + i + ",'" + i + "')").collect(joining(", "));
        String insertSql = "INSERT INTO t (id, val) VALUES " + data;

        sql(insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "t t" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                Collection<? extends RunningQuery> queries = clientEngine.runningQueries();

                return !queries.isEmpty() && F.first(queries).state() == QueryState.EXECUTING;
            },
            TIMEOUT_IN_MS));

        Assert.assertTrue(GridTestUtils.waitForCondition(() -> !serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = serverEngine.runningQueries();
        RunningQuery qry = F.first(running);

        assertSame(qry, serverEngine.runningQuery(qry.id()));

        qry.cancel();

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> clientEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

    /** */
    @Test
    public void testLongPlanningTimeout() throws Exception {
        sql("CREATE TABLE T1(A INT, B INT)");
        sql("CREATE TABLE T2(A INT, C INT)");
        sql("CREATE TABLE T3(B INT, C INT)");
        sql("CREATE TABLE T4(A INT, C INT)");

        sql("INSERT INTO T1(A, B) VALUES (1, 1)");
        sql("INSERT INTO T2(A, C) VALUES (1, 2)");
        sql("INSERT INTO T3(B, C) VALUES (1, 2)");
        sql("INSERT INTO T4(A, C) VALUES (1, 2)");

        String longJoinQry = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A JOIN T3 ON T3.B = T1.B AND T3.C = T2.C JOIN T4" +
            " ON T4.C = T3.C AND T4.A = T1.A";

        AtomicReference<List<List<?>>> res = new AtomicReference<>();
        GridTestUtils.assertTimeout(3 * PLANNER_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
            res.set(sql(longJoinQry));
        });

        assertNotNull(res.get());
        assertFalse(res.get().isEmpty());
    }
}
