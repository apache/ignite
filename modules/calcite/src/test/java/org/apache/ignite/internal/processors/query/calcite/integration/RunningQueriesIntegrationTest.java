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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.query.DistributedSqlConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.Query;
import org.apache.ignite.internal.processors.query.calcite.QueryState;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_PLANNER_TIMEOUT;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_VIEW;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 *
 */
@WithSystemProperty(key = IGNITE_CALCITE_PLANNER_TIMEOUT, value = "2000")
public class RunningQueriesIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final long PLANNER_TIMEOUT = getLong(IGNITE_CALCITE_PLANNER_TIMEOUT, 0);

    /** */
    private static IgniteEx srv;

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = grid(0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /**
     * Execute query with a lot of JOINs to produce very long planning phase.
     * Cancel query on planning phase and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtPlanningPhase() throws IgniteCheckedException {
        MetricRegistryImpl mreg = client.context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg.reset();

        CalciteQueryProcessor engine = queryProcessor(client);

        // Calcite takes time to reorder joins. We disable some default joins reorder rules by the joins number to preveng
        // long planning. But even with a small joins count there is significant time to optimize joins.
        // We use several joins here as a long planning.
        int cnt = Math.min(PlannerHelper.MAX_JOINS_TO_COMMUTE, PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS);

        for (int i = 0; i < cnt; i++)
            sql("CREATE TABLE test_tbl" + i + " (id int, val varchar)");

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "test_tbl" + i + " p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        assertTrue(GridTestUtils.waitForCondition(
            () -> !engine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Collection<? extends Query<?>> running = engine.runningQueries();

        assertEquals("Running: " + running, 1, running.size());
        assertFalse(fut.isDone());

        Query<?> qry = F.first(running);

        assertSame(qry, engine.runningQuery(qry.id()));

        // Waits for planning.
        assertTrue(GridTestUtils.waitForCondition(
            () -> qry.state() == QueryState.PLANNING, TIMEOUT_IN_MS));

        qry.cancel();

        assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(0), IgniteSQLException.class, "The query was cancelled while planning");

        assertEquals(1, ((LongMetric)mreg.findMetric("canceled")).value());
    }

    /**
     * Execute query with a latch on excution phase.
     * Cancel query on execution phase and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtExecutionPhase() throws Exception {
        MetricRegistryImpl mreg = client.context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg.reset();

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
            Query<?> qry = F.first(cliEngine.runningQueries());
            assertEquals(QueryState.EXECUTING, qry.state());

            qry.cancel();

            assertTrue(GridTestUtils.waitForCondition(
                () -> srvEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

            assertTrue(GridTestUtils.waitForCondition(
                () -> cliEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));
        }
        finally {
            stop.set(true);
        }

        GridTestUtils.assertThrowsAnyCause(log,
            () -> fut.get(100), IgniteSQLException.class, "The query was cancelled");

        assertEquals(1, ((LongMetric)mreg.findMetric("canceled")).value());
    }

    /**
     * Execute query with a lot of JOINs to produce very long excution phase.
     * Cancel query on execution phase on remote node (no query originator node)
     * and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelByRemoteFragment() throws IgniteCheckedException {
        MetricRegistryImpl mreg = client.context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg.reset();

        CalciteQueryProcessor clientEngine = queryProcessor(client);
        CalciteQueryProcessor serverEngine = queryProcessor(srv);
        int cnt = 6;

        sql("CREATE TABLE t (id int, val varchar)");

        String data = IntStream.range(0, 10000).mapToObj((i) -> "(" + i + ",'" + i + "')").collect(joining(", "));
        String insertSql = "INSERT INTO t (id, val) VALUES " + data;

        sql(insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "t t" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        assertTrue(GridTestUtils.waitForCondition(
            () -> {
                Collection<? extends Query<?>> queries = clientEngine.runningQueries();

                return !queries.isEmpty() && F.first(queries).state() == QueryState.EXECUTING;
            },
            TIMEOUT_IN_MS));

        assertTrue(GridTestUtils.waitForCondition(() -> !serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Collection<? extends Query<?>> running = serverEngine.runningQueries();
        Query<?> qry = F.first(running);

        assertSame(qry, serverEngine.runningQuery(qry.id()));

        qry.cancel();

        assertTrue(GridTestUtils.waitForCondition(
            () -> clientEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        assertTrue(GridTestUtils.waitForCondition(
            () -> serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled");

        assertEquals(1, ((LongMetric)mreg.findMetric("canceled")).value());
    }

    /** */
    @Test
    public void testLongPlanningTimeout() throws IgniteCheckedException {
        Stream.of("T1", "T2").forEach(tblName -> {
            sql(String.format("CREATE TABLE %s(A INT, B INT)", tblName));

            IgniteTable tbl = (IgniteTable)queryProcessor(client).schemaHolder().schema("PUBLIC").getTable(tblName);

            tbl.addIndex(new DelegatingIgniteIndex(tbl.getIndex(QueryUtils.PRIMARY_KEY_INDEX)) {
                @Override public RelCollation collation() {
                    doSleep(300);

                    return delegate.collation();
                }
            });

            sql(String.format("INSERT INTO %s(A, B) VALUES (1, 1)", tblName));
        });

        String longJoinQry = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A";

        try {
            AtomicReference<List<List<?>>> res = new AtomicReference<>();
            GridTestUtils.assertTimeout(3 * PLANNER_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
                res.set(sql(longJoinQry));
            });

            assertNotNull(res.get());
            assertFalse(res.get().isEmpty());
        }
        catch (Exception e) {
            assertTrue("Unexpected exception: " + e, X.cause(e, QueryCancelledException.class) != null);
        }

        DistributedSqlConfiguration distrCfg = queryProcessor(client).distributedConfiguration();

        try {
            distrCfg.defaultQueryTimeout((int)PLANNER_TIMEOUT / 3).get();

            GridTestUtils.assertTimeout(PLANNER_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
                sql(longJoinQry + " AND 1=1"); // Modify SQL to skip cached plan.
            });
        }
        catch (Exception e) {
            assertTrue("Unexpected exception: " + e, X.cause(e, QueryCancelledException.class) != null);
        }
        finally {
            distrCfg.defaultQueryTimeout(DistributedSqlConfiguration.DFLT_QRY_TIMEOUT).get();
        }
    }

    /**
     * Test propagation of query initiator ID.
     */
    @Test
    public void testQueryInitiator() throws IgniteCheckedException {
        CalciteQueryProcessor engine = queryProcessor(client);

        sql("CREATE TABLE t(id int, val varchar) WITH cache_name=\"cache\"");

        IgniteCacheTable tbl = (IgniteCacheTable)engine.schemaHolder().schema("PUBLIC").getTable("T");

        CountDownLatch latch = new CountDownLatch(1);

        tbl.addIndex(new DelegatingIgniteIndex(tbl.getIndex(QueryUtils.PRIMARY_KEY_INDEX)) {
            @Override public RelCollation collation() {
                try {
                    latch.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }

                return delegate.collation();
            }
        });

        String initiatorId = "initiator";

        GridTestUtils.runAsync(() -> client.cache("cache").query(new SqlFieldsQuery("SELECT * FROM t")
            .setQueryInitiatorId(initiatorId)).getAll());

        try {
            SystemView<SqlQueryView> view = client.context().systemView().view(SQL_QRY_VIEW);

            assertTrue(GridTestUtils.waitForCondition(() -> !F.isEmpty(view), 1_000));

            assertFalse(F.isEmpty(engine.runningQueries()));

            assertEquals(1, F.size(view.iterator(), v -> initiatorId.equals(v.initiatorId())));
        }
        finally {
            latch.countDown();
        }

        assertTrue(GridTestUtils.waitForCondition(() -> F.isEmpty(engine.runningQueries()), PLANNER_TIMEOUT * 2));
    }

    /** */
    @Test
    public void testErrorOnRemoteFragment() throws Exception {
        MetricRegistryImpl mreg = client.context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg.reset();

        CalciteQueryProcessor clientEngine = queryProcessor(client);
        CalciteQueryProcessor srvEngine = queryProcessor(srv);

        sql("CREATE TABLE t(id int, val varchar)");

        IgniteCacheTable oldTbl = (IgniteCacheTable)srvEngine.schemaHolder().schema("PUBLIC").getTable("T");

        CountDownLatch initLatch = new CountDownLatch(1);

        IgniteCacheTable newTbl = new CacheTableImpl(srv.context(), oldTbl.descriptor()) {
            @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup grp,
                @Nullable ImmutableBitSet usedColumns
            ) {
                initLatch.countDown();

                throw new IllegalStateException("Init error");
            }
        };

        srvEngine.schemaHolder().schema("PUBLIC").add("T", newTbl);

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql("SELECT * FROM t"));

        initLatch.await(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

        assertTrue(GridTestUtils.waitForCondition(
            () -> clientEngine.runningQueries().isEmpty() && srvEngine.runningQueries().isEmpty(),
            TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IllegalStateException.class, "Init error");

        assertEquals(0, ((LongMetric)mreg.findMetric("canceled")).value());
        assertEquals(1, ((LongMetric)mreg.findMetric("failed")).value());
    }
}
