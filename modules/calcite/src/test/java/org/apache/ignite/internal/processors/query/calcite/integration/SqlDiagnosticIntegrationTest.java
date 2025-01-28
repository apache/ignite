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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.SqlQueryExecutionEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.Query;
import org.apache.ignite.internal.processors.query.calcite.QueryRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.task.AbstractQueryTaskExecutor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_SQL_QUERY_EXECUTION;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.withSecurityContextOnAllNodes;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.cleanPerformanceStatisticsDir;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.startCollectStatistics;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.stopCollectStatisticsAndRead;
import static org.apache.ignite.internal.processors.query.QueryParserMetricsHolder.QUERY_PARSER_METRIC_GROUP_NAME;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.BIG_RESULT_SET_MSG;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_ERROR_MSG;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_EXEC_MSG;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_FINISHED_MSG;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 * Test SQL diagnostic tools.
 */
public class SqlDiagnosticIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String jdbcUrl = "jdbc:ignite:thin://127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT;

    /** */
    private static final long LONG_QRY_TIMEOUT = 1_000L;

    /** */
    private static final int BIG_RESULT_SET_THRESHOLD = 10_000;

    /** */
    private ListeningTestLogger log;

    /** */
    private SecurityContext secCtxDflt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setAuthenticationEnabled(true)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration())
                .setLongQueryWarningTimeout(LONG_QRY_TIMEOUT))
            .setIncludeEventTypes(EVT_SQL_QUERY_EXECUTION, EVT_CACHE_QUERY_EXECUTED, EVT_CACHE_QUERY_OBJECT_READ)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        log = new ListeningTestLogger(log());

        startGrids(nodeCount());

        client = startClientGrid();

        client.cluster().state(ClusterState.ACTIVE);

        secCtxDflt = authenticate(grid(0), DFAULT_USER_NAME, "ignite");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPerformanceStatisticsDir();
    }

    /** */
    @Override protected int nodeCount() {
        return 2;
    }

    /** */
    @Test
    public void testParserMetrics() {
        MetricRegistryImpl mreg0 = grid(0).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        MetricRegistryImpl mreg1 = grid(1).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        mreg0.reset();
        mreg1.reset();

        LongMetric hits0 = mreg0.findMetric("hits");
        LongMetric hits1 = mreg1.findMetric("hits");
        LongMetric misses0 = mreg0.findMetric("misses");
        LongMetric misses1 = mreg1.findMetric("misses");

        // Parse and plan on client.
        sql("CREATE TABLE test_parse(a INT)");

        assertEquals(0, hits0.value());
        assertEquals(0, hits1.value());
        assertEquals(0, misses0.value());
        assertEquals(0, misses1.value());

        for (int i = 0; i < 10; i++)
            sql(grid(0), "INSERT INTO test_parse VALUES (?)", i);

        assertEquals(9, hits0.value());
        assertEquals(0, hits1.value());
        assertEquals(1, misses0.value());
        assertEquals(0, misses1.value());

        for (int i = 0; i < 10; i++)
            sql(grid(1), "SELECT * FROM test_parse WHERE a = ?", i);

        assertEquals(9, hits0.value());
        assertEquals(9, hits1.value());
        assertEquals(1, misses0.value());
        assertEquals(1, misses1.value());
    }

    /** */
    @Test
    public void testBatchParserMetrics() throws Exception {
        withSecurityContextOnAllNodes(secCtxDflt);

        MetricRegistryImpl mreg0 = grid(0).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        MetricRegistryImpl mreg1 = grid(1).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        mreg0.reset();
        mreg1.reset();

        LongMetric hits0 = mreg0.findMetric("hits");
        LongMetric hits1 = mreg1.findMetric("hits");
        LongMetric misses0 = mreg0.findMetric("misses");
        LongMetric misses1 = mreg1.findMetric("misses");

        sql("CREATE TABLE test_batch(a INT)");

        assertEquals(0, hits0.value());
        assertEquals(0, hits1.value());
        assertEquals(0, misses0.value());
        assertEquals(0, misses1.value());

        try (Connection conn = DriverManager.getConnection(jdbcUrl, DFAULT_USER_NAME, "ignite")) {
            conn.setSchema("PUBLIC");

            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < 10; i++)
                    stmt.addBatch(String.format("INSERT INTO test_batch VALUES (%d)", i));

                stmt.executeBatch();

                assertEquals(0, hits0.value());
                assertEquals(0, hits1.value());
                assertEquals(10, misses0.value());
                assertEquals(0, misses1.value());
            }

            String sql = "INSERT INTO test_batch VALUES (?)";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 10; i < 20; i++) {
                    stmt.setInt(1, i);
                    stmt.addBatch();
                }

                stmt.executeBatch();

                assertEquals(0, hits0.value());
                assertEquals(0, hits1.value());
                assertEquals(11, misses0.value()); // Only one increment per batch.
                assertEquals(0, misses1.value());
            }

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 20; i < 30; i++) {
                    stmt.setInt(1, i);
                    stmt.addBatch();
                }

                stmt.executeBatch();

                assertEquals(1, hits0.value()); // Only one increment per batch.
                assertEquals(0, hits1.value());
                assertEquals(11, misses0.value());
                assertEquals(0, misses1.value());
            }
        }
    }

    /** */
    @Test
    public void testUserQueriesMetrics() throws Exception {
        sql(grid(0), "CREATE TABLE test_metric (a INT)");

        MetricRegistryImpl mreg0 = grid(0).context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        MetricRegistryImpl mreg1 = grid(1).context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg0.reset();
        mreg1.reset();

        AtomicInteger qryCnt = new AtomicInteger();
        grid(0).context().query().runningQueryManager().registerQueryFinishedListener(q -> qryCnt.incrementAndGet());

        sql(grid(0), "INSERT INTO test_metric VALUES (?)", 0);
        sql(grid(0), "SELECT * FROM test_metric WHERE a = ?", 0);

        try {
            sql(grid(0), "SELECT * FROM test_fail");

            fail();
        }
        catch (IgniteSQLException ignored) {
            // Expected.
        }

        FieldsQueryCursor<?> cur = grid(0).getOrCreateCache("test_metric")
            .query(new SqlFieldsQuery("SELECT * FROM table(system_range(1, 10000))"));

        assertTrue(cur.iterator().hasNext());

        cur.close();

        // Query unregistering is async process, wait for it before metrics check.
        assertTrue(GridTestUtils.waitForCondition(() -> qryCnt.get() == 4, 1_000L));

        assertEquals(2, ((LongMetric)mreg0.findMetric("success")).value());
        assertEquals(2, ((LongMetric)mreg0.findMetric("failed")).value()); // 1 error + 1 cancelled.
        assertEquals(1, ((LongMetric)mreg0.findMetric("canceled")).value());

        assertEquals(0, ((LongMetric)mreg1.findMetric("success")).value());
        assertEquals(0, ((LongMetric)mreg1.findMetric("failed")).value());
        assertEquals(0, ((LongMetric)mreg1.findMetric("canceled")).value());
    }

    /** */
    @Test
    public void testThreadPoolMetrics() {
        String regName = metricName(PoolProcessor.THREAD_POOLS, AbstractQueryTaskExecutor.THREAD_POOL_NAME);
        MetricRegistry mreg = client.context().metric().registry(regName);

        LongMetric tasksCnt = mreg.findMetric("CompletedTaskCount");

        tasksCnt.reset();

        assertEquals(0, tasksCnt.value());

        sql("SELECT 'test'");

        assertTrue(tasksCnt.value() > 0);
    }

    /** */
    @Test
    public void testPerformanceStatistics() throws Exception {
        cleanPerformanceStatisticsDir();
        startCollectStatistics();

        long startTime = U.currentTimeMillis();

        AtomicInteger finishQryCnt = new AtomicInteger();
        grid(0).context().query().runningQueryManager().registerQueryFinishedListener(q -> finishQryCnt.incrementAndGet());

        sql(grid(0), "SELECT * FROM table(system_range(1, 1000))");
        sql(grid(0), "CREATE TABLE test_perf_stat (a INT)");
        sql(grid(0), "INSERT INTO test_perf_stat VALUES (0), (1), (2), (3), (4)");
        sql(grid(0), "SELECT * FROM test_perf_stat WHERE a > 0");

        assertTrue(GridTestUtils.waitForCondition(() -> finishQryCnt.get() == 4, 1_000L));

        // Only the last query should trigger queryReads event.
        // The first query uses generated data and doesn't require any page reads.
        // The second query is DDL and doesn't perform any page reads as well.
        // The third query performs scan for static values and insert data into cache. We are able to analyze only
        // ScanNode page reads, since table/index scans are local and executed in current thread. ModifyNode uses
        // distributed `invoke` operation, which can be executed by other threads or on other nodes. It's hard to
        // obtain correct value of page reads for these types of operations, so, currently we just ignore page reads
        // performed by ModifyNode.
        // The fourth query is a table scan and should perform page reads on all data nodes.

        AtomicInteger qryCnt = new AtomicInteger();
        AtomicLong rowsScanned = new AtomicLong();
        Iterator<String> sqlIt = F.asList("SELECT", "CREATE", "INSERT", "SELECT").iterator();
        Set<UUID> dataNodesIds = new HashSet<>(F.asList(grid(0).localNode().id(), grid(1).localNode().id()));
        Set<UUID> readsNodes = new HashSet<>(dataNodesIds);
        Set<Long> readsQueries = new HashSet<>();
        Map<Long, Long> rowsFetchedPerQry = new HashMap<>();
        AtomicLong firstQryId = new AtomicLong(-1);
        AtomicLong lastQryId = new AtomicLong();

        stopCollectStatisticsAndRead(new AbstractPerformanceStatisticsTest.TestHandler() {
            @Override public void query(
                UUID nodeId,
                GridCacheQueryType type,
                String text,
                long id,
                long qryStartTime,
                long duration,
                boolean success
            ) {
                qryCnt.incrementAndGet();

                assertEquals(grid(0).localNode().id(), nodeId);
                assertEquals(SQL_FIELDS, type);
                assertTrue(text.startsWith(sqlIt.next()));
                assertTrue(qryStartTime >= startTime);
                assertTrue(duration >= 0);
                assertTrue(success);

                firstQryId.compareAndSet(-1, id);
                lastQryId.set(id);
            }

            @Override public void queryReads(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                long logicalReads,
                long physicalReads
            ) {
                readsQueries.add(id);
                assertTrue(dataNodesIds.contains(nodeId));
                readsNodes.remove(nodeId);

                assertEquals(grid(0).localNode().id(), qryNodeId);
                assertEquals(SQL_FIELDS, type);
                assertTrue(logicalReads > 0);
            }

            @Override public void queryRows(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                String action,
                long rows
            ) {
                assertEquals(grid(0).localNode().id(), qryNodeId);
                assertEquals(SQL_FIELDS, type);

                if (action.toLowerCase().contains("test_perf_stat")) {
                    assertTrue(dataNodesIds.contains(nodeId));
                    rowsScanned.addAndGet(rows);
                }
                else if ("Fetched".equals(action)) {
                    assertEquals(grid(0).localNode().id(), nodeId);
                    assertNull(rowsFetchedPerQry.put(id, rows));
                }
            }
        });

        assertEquals(4, qryCnt.get());
        assertTrue("Query reads expected on nodes: " + readsNodes, readsNodes.isEmpty());
        assertEquals(Collections.singleton(lastQryId.get()), readsQueries);
        assertEquals((Long)1000L, rowsFetchedPerQry.get(firstQryId.get()));
        assertEquals((Long)4L, rowsFetchedPerQry.get(lastQryId.get()));
        assertEquals(5L, rowsScanned.get());
    }

    /** */
    @Test
    public void testPerformanceStatisticsEnableAfterQuery() throws Exception {
        cleanPerformanceStatisticsDir();

        String qry = "SELECT * FROM table(system_range(1, 1000))";

        sql(grid(0), qry);

        startCollectStatistics();

        AtomicInteger finishQryCnt = new AtomicInteger();
        grid(0).context().query().runningQueryManager().registerQueryFinishedListener(q -> finishQryCnt.incrementAndGet());

        sql(grid(0), qry);

        assertTrue(GridTestUtils.waitForCondition(() -> finishQryCnt.get() == 1, 1_000L));

        AtomicInteger qryCnt = new AtomicInteger();
        AtomicBoolean hasPlan = new AtomicBoolean();

        stopCollectStatisticsAndRead(new AbstractPerformanceStatisticsTest.TestHandler() {
            @Override public void query(
                UUID nodeId,
                GridCacheQueryType type,
                String text,
                long id,
                long qryStartTime,
                long duration,
                boolean success
            ) {
                qryCnt.incrementAndGet();

                assertEquals(grid(0).localNode().id(), nodeId);
                assertEquals(SQL_FIELDS, type);
                assertTrue(success);
            }

            @Override public void queryProperty(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                String name,
                String val
            ) {
                if ("Query plan".equals(name)) {
                    assertFalse(F.isEmpty(val));
                    hasPlan.set(true);
                }
            }
        });

        assertEquals(1, qryCnt.get());
        assertTrue(hasPlan.get());
    }

    /** */
    @Test
    public void testPerformanceStatisticsNestedScan() throws Exception {
        sql(grid(0), "CREATE TABLE test_perf_stat_nested (a INT) WITH template=REPLICATED");
        sql(grid(0), "INSERT INTO test_perf_stat_nested VALUES (0), (1), (2), (3), (4)");

        cleanPerformanceStatisticsDir();
        startCollectStatistics();

        AtomicInteger finishQryCnt = new AtomicInteger();
        grid(0).context().query().runningQueryManager().registerQueryFinishedListener(q -> finishQryCnt.incrementAndGet());

        sql(grid(0), "SELECT * FROM test_perf_stat_nested UNION ALL SELECT * FROM test_perf_stat_nested");

        assertTrue(GridTestUtils.waitForCondition(() -> finishQryCnt.get() == 1, 1_000L));

        AtomicInteger qryCnt = new AtomicInteger();
        AtomicInteger readsCnt = new AtomicInteger();
        AtomicLong rowsCnt = new AtomicLong();

        stopCollectStatisticsAndRead(new AbstractPerformanceStatisticsTest.TestHandler() {
            @Override public void query(
                UUID nodeId,
                GridCacheQueryType type,
                String text,
                long id,
                long qryStartTime,
                long duration,
                boolean success
            ) {
                qryCnt.incrementAndGet();
                assertTrue(success);
            }

            @Override public void queryReads(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                long logicalReads,
                long physicalReads
            ) {
                readsCnt.incrementAndGet();
                assertTrue(logicalReads > 0);
            }

            @Override public void queryRows(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                String action,
                long rows
            ) {
                if ("Fetched".equals(action))
                    rowsCnt.addAndGet(rows);
            }
        });

        assertEquals(1, qryCnt.get());
        // The second scan is executed inside the first scan processNextBatch() method,
        // after the first scan invoke downstream().end(), so here we have only one read record.
        assertEquals(1, readsCnt.get());
        assertEquals(10, rowsCnt.get());
    }

    /** */
    @Test
    public void testSqlEvents() {
        sql("CREATE TABLE test_event (a INT) WITH cache_name=\"test_event\"");

        AtomicIntegerArray evtsSqlExec = new AtomicIntegerArray(nodeCount());
        AtomicIntegerArray evtsQryExec = new AtomicIntegerArray(nodeCount());
        AtomicIntegerArray evtsQryRead = new AtomicIntegerArray(nodeCount());
        for (int i = 0; i < nodeCount(); i++) {
            int n = i;
            grid(i).events().localListen(e -> {
                evtsSqlExec.incrementAndGet(n);

                assertTrue(e instanceof SqlQueryExecutionEvent);
                assertTrue(((SqlQueryExecutionEvent)e).text().toLowerCase().contains("test_event"));

                return true;
            }, EVT_SQL_QUERY_EXECUTION);

            grid(i).events().localListen(e -> {
                evtsQryExec.incrementAndGet(n);

                assertTrue(e instanceof CacheQueryExecutedEvent);
                assertEquals("test_event", ((CacheQueryExecutedEvent<?, ?>)e).cacheName());
                assertTrue(((CacheQueryExecutedEvent<?, ?>)e).clause().toLowerCase().contains("test_event"));
                assertEquals(SQL_FIELDS.name(), ((CacheQueryExecutedEvent<?, ?>)e).queryType());
                assertEquals(3, ((CacheQueryExecutedEvent<?, ?>)e).arguments().length);
                assertNull(((CacheQueryExecutedEvent<?, ?>)e).scanQueryFilter());
                assertNull(((CacheQueryExecutedEvent<?, ?>)e).continuousQueryFilter());

                return true;
            }, EVT_CACHE_QUERY_EXECUTED);

            grid(i).events().localListen(e -> {
                evtsQryRead.incrementAndGet(n);

                assertTrue(e instanceof CacheQueryReadEvent);
                assertEquals(SQL_FIELDS.name(), ((CacheQueryReadEvent<?, ?>)e).queryType());
                assertTrue(((CacheQueryReadEvent<?, ?>)e).clause().toLowerCase().contains("test_event"));
                assertNotNull(((CacheQueryReadEvent<?, ?>)e).row());

                return true;
            }, EVT_CACHE_QUERY_OBJECT_READ);
        }

        grid(0).cache("test_event").query(new SqlFieldsQuery("INSERT INTO test_event VALUES (?), (?), (?)")
                .setArgs(0, 1, 2)).getAll();

        grid(0).cache("test_event").query(new SqlFieldsQuery("SELECT * FROM test_event WHERE a IN (?, ?, ?)")
                .setArgs(0, 1, 3)).getAll();

        assertEquals(2, evtsSqlExec.get(0));
        assertEquals(0, evtsSqlExec.get(1));
        assertEquals(2, evtsQryExec.get(0));
        assertEquals(0, evtsQryExec.get(1));
        // 1 event fired by insert (number of rows inserted) + 2 events (1 per row selected) fired by the second query.
        assertEquals(3, evtsQryRead.get(0));
        assertEquals(0, evtsQryRead.get(1));
    }

    /** */
    @Test
    public void testSensitiveInformationHiding() throws Exception {
        withSecurityContextOnAllNodes(secCtxDflt);

        cleanPerformanceStatisticsDir();
        startCollectStatistics();

        client.getOrCreateCache(new CacheConfiguration<Integer, Integer>("func_cache")
            .setSqlFunctionClasses(FunctionsLibrary.class)
            .setSqlSchema("PUBLIC")
        );

        QueryUtils.INCLUDE_SENSITIVE = false;

        try {
            // Check the same query twice, the first time - with parsing and planning,
            // the second time from the parsers cache.
            for (int i = 0; i < 2; i++) {
                FunctionsLibrary.latch = new CountDownLatch(1);

                IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                    List<List<?>> res = sql(grid(0), "SELECT * FROM (VALUES('sensitive')) t(v) " +
                        "WHERE v = 'sensitive' and waitLatch(1000)");

                    assertEquals(1, res.size());
                    assertEquals(1, res.get(0).size());
                    assertEquals("sensitive", res.get(0).get(0));
                });

                try {
                    QueryRegistry qreg = queryProcessor(grid(0)).queryRegistry();
                    assertTrue(GridTestUtils.waitForCondition(() -> qreg.runningQueries().size() == 1, 1000L));
                    Query<?> qry = F.first(qreg.runningQueries());
                    assertFalse(qry.toString().contains("sensitive"));
                }
                finally {
                    FunctionsLibrary.latch.countDown();
                }

                fut.get();
            }

            // Test bounds hiding in index scans.
            sql(grid(0), "CREATE TABLE test_sens (id int, val varchar)");
            sql(grid(0), "CREATE INDEX test_sens_idx ON test_sens(val) INLINE_SIZE 10");
            sql(grid(0), "INSERT INTO test_sens (id, val) VALUES (0, 'sensitive0'), (1, 'sensitive1'), " +
                "(2, 'sensitive2'), (3, 'sensitive3'), (4, 'sensitive4'), (5, 'sensitive5'), (6, 'sensitive6')");
            sql(grid(0), "SELECT * FROM test_sens WHERE val IN ('sensitive0', 'sensitive1')");
            sql(grid(0), "SELECT * FROM test_sens WHERE val BETWEEN 'sensitive1' AND 'sensitive3'");
            sql(grid(0), "SELECT * FROM test_sens WHERE val = 'sensitive4'");

            // Test CREATE AS SELECT rewrite.
            sql(grid(0), "CREATE TABLE test_sens1 (val) WITH CACHE_NAME=\"test_sens1\" AS SELECT 'sensitive' AS val");

            // Test CREATE/ALTER USER commands rewrite.
            sql(grid(0), "CREATE USER test WITH PASSWORD 'sensitive'");
            sql(grid(0), "ALTER USER test WITH PASSWORD 'sensitive'");

            // Test JOIN.
            sql(grid(0),
                "SELECT * FROM test_sens t1 JOIN test_sens t2 ON t1.id = t2.id WHERE t1.val like 'sensitive%'");

            AtomicInteger qryCnt = new AtomicInteger();
            AtomicInteger planCnt = new AtomicInteger();

            stopCollectStatisticsAndRead(new AbstractPerformanceStatisticsTest.TestHandler() {
                @Override public void query(
                    UUID nodeId,
                    GridCacheQueryType type,
                    String text,
                    long id,
                    long qryStartTime,
                    long duration,
                    boolean success
                ) {
                    qryCnt.incrementAndGet();
                    assertFalse(text, text.contains("sensitive"));
                }

                @Override public void queryProperty(
                    UUID nodeId,
                    GridCacheQueryType type,
                    UUID qryNodeId,
                    long id,
                    String name,
                    String val
                ) {
                    if ("Query plan".equals(name)) {
                        planCnt.incrementAndGet();
                        assertFalse(val, val.contains("sensitive"));
                    }
                }
            });

            assertEquals(13, qryCnt.get()); // CREATE AS SELECT counts as two queries.
            assertEquals(8, planCnt.get()); // DDL queries don't produce plans, except CREATE AS SELECT.
        }
        finally {
            QueryUtils.INCLUDE_SENSITIVE = true;
        }
    }

    /** */
    @Test
    public void testLongRunningQueries() throws Exception {
        client.getOrCreateCache(new CacheConfiguration<Integer, Integer>("func_cache")
            .setSqlFunctionClasses(FunctionsLibrary.class)
            .setSqlSchema("PUBLIC")
        );

        LogListener logLsnr0 = LogListener.matches(LONG_QUERY_EXEC_MSG).build();

        log.registerListener(logLsnr0);

        FunctionsLibrary.latch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> sql(grid(0), "SELECT waitLatch(10000)"));

        doSleep(LONG_QRY_TIMEOUT * 3);

        assertTrue(logLsnr0.check());

        LogListener logLsnr1 = LogListener.matches(LONG_QUERY_FINISHED_MSG).build();

        log.registerListener(logLsnr1);

        FunctionsLibrary.latch.countDown();

        fut.get();

        assertTrue(logLsnr1.check(1000L));

        FunctionsLibrary.latch = new CountDownLatch(1);

        fut = GridTestUtils.runAsync(() -> sql(grid(0), "SELECT waitLatch(2000)"));

        LogListener logLsnr2 = LogListener.matches(LONG_QUERY_ERROR_MSG).build();

        log.registerListener(logLsnr2);

        doSleep(LONG_QRY_TIMEOUT * 2);

        try {
            fut.get();
        }
        catch (Exception ignore) {
            // Expected.
        }

        assertTrue(logLsnr2.check(1000L));
    }

    /** */
    @Test
    public void testBigResultSet() throws Exception {
        grid(0).context().query().runningQueryManager().heavyQueriesTracker()
            .setResultSetSizeThreshold(BIG_RESULT_SET_THRESHOLD);

        int rowCnt = BIG_RESULT_SET_THRESHOLD * 5 + 1;

        LogListener logLsnr0 = LogListener.matches(BIG_RESULT_SET_MSG).build();
        LogListener logLsnr1 = LogListener.matches("fetched=" + BIG_RESULT_SET_THRESHOLD).build();
        LogListener logLsnr2 = LogListener.matches("fetched=" + rowCnt).build();

        log.registerListener(logLsnr0);
        log.registerListener(logLsnr1);
        log.registerListener(logLsnr2);

        sql(grid(0), "SELECT * FROM TABLE(SYSTEM_RANGE(1, ?))", rowCnt);

        assertTrue(logLsnr0.check(1000L));
        assertTrue(logLsnr1.check(1000L));
        assertTrue(logLsnr2.check(1000L));
    }

    /** */
    public static class FunctionsLibrary {
        /** */
        static volatile CountDownLatch latch;

        /** */
        @QuerySqlFunction
        public static boolean waitLatch(long time) {
            try {
                if (!latch.await(time, TimeUnit.MILLISECONDS))
                    throw new RuntimeException();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return true;
        }
    }
}
