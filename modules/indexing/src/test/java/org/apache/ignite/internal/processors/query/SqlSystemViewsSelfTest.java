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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for ignite SQL system views.
 */
public class SqlSystemViewsSelfTest extends GridCommonAbstractTest {
    /** Metrics check attempts. */
    private static final int METRICS_CHECK_ATTEMPTS = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object ... args) {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object ... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     */
    private void assertSqlError(final String sql) {
        Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        assertEquals(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlE.statusCode());
    }
    
    /**
     * Test system views modifications.
     */
    public void testModifications() throws Exception {
        startGrid(getConfiguration());

        assertSqlError("DROP TABLE IGNITE.NODES");

        assertSqlError("TRUNCATE TABLE IGNITE.NODES");

        assertSqlError("ALTER TABLE IGNITE.NODES RENAME TO IGNITE.N");

        assertSqlError("ALTER TABLE IGNITE.NODES ADD COLUMN C VARCHAR");

        assertSqlError("ALTER TABLE IGNITE.NODES DROP COLUMN ID");

        assertSqlError("ALTER TABLE IGNITE.NODES RENAME COLUMN ID TO C");

        assertSqlError("CREATE INDEX IDX ON IGNITE.NODES(ID)");

        assertSqlError("INSERT INTO IGNITE.NODES (ID) VALUES ('-')");

        assertSqlError("UPDATE IGNITE.NODES SET ID = '-'");

        assertSqlError("DELETE IGNITE.NODES");
    }

    /**
     * Test different query modes.
     */
    public void testQueryModes() throws Exception {
        Ignite ignite = startGrid(0);
        startGrid(1);

        UUID nodeId = ignite.cluster().localNode().id();

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        String sql = "SELECT ID FROM IGNITE.NODES WHERE NODE_ORDER = 1";

        SqlFieldsQuery qry;

        qry = new SqlFieldsQuery(sql).setDistributedJoins(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setReplicatedOnly(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setLocal(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));
    }

    /**
     * Test that we can't use cache tables and system views in the same query.
     */
    public void testCacheToViewJoin() throws Exception {
        Ignite ignite = startGrid();

        ignite.createCache(new CacheConfiguration<>().setName(DEFAULT_CACHE_NAME).setQueryEntities(
            Collections.singleton(new QueryEntity(Integer.class.getName(), String.class.getName()))));

        assertSqlError("SELECT * FROM \"" + DEFAULT_CACHE_NAME + "\".String JOIN IGNITE.NODES ON 1=1");
    }

    /**
     * @param rowData Row data.
     * @param colTypes Column types.
     */
    private void assertColumnTypes(List<?> rowData, Class<?> ... colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            if (rowData.get(i) != null)
                assertEquals("Column " + i + " type", colTypes[i], rowData.get(i).getClass());
        }
    }

    /**
     * Test nodes system view.
     *
     * @throws Exception If failed.
     */
    public void testNodesViews() throws Exception {
        Ignite igniteSrv = startGrid(getTestIgniteInstanceName(), getConfiguration().setMetricsUpdateFrequency(500L));

        Ignite igniteCli = startGrid(getTestIgniteInstanceName(1), getConfiguration().setMetricsUpdateFrequency(500L)
            .setClientMode(true));

        startGrid(getTestIgniteInstanceName(2), getConfiguration().setMetricsUpdateFrequency(500L).setDaemon(true));

        UUID nodeId0 = igniteSrv.cluster().localNode().id();

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT ID, CONSISTENT_ID, VERSION, IS_CLIENT, IS_DAEMON, " +
                "NODE_ORDER, ADDRESSES, HOSTNAMES FROM IGNITE.NODES");

        assertColumnTypes(resAll.get(0), UUID.class, String.class, String.class, Boolean.class, Boolean.class,
            Integer.class, String.class, String.class);

        assertEquals(3, resAll.size());

        List<List<?>> resSrv = execSql(
            "SELECT ID, NODE_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = FALSE AND IS_DAEMON = FALSE"
        );

        assertEquals(1, resSrv.size());

        assertEquals(nodeId0, resSrv.get(0).get(0));

        assertEquals(1, resSrv.get(0).get(1));

        List<List<?>> resCli = execSql(
            "SELECT ID, NODE_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = TRUE");

        assertEquals(1, resCli.size());

        assertEquals(nodeId(1), resCli.get(0).get(0));

        assertEquals(2, resCli.get(0).get(1));

        List<List<?>> resDaemon = execSql(
            "SELECT ID, NODE_ORDER FROM IGNITE.NODES WHERE IS_DAEMON = TRUE");

        assertEquals(1, resDaemon.size());

        assertEquals(nodeId(2), resDaemon.get(0).get(0));

        assertEquals(3, resDaemon.get(0).get(1));

        // Check index on ID column.
        assertEquals(0, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = '-'").size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            nodeId0).size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            nodeId(2)).size());

        // Check index on ID column with disjunction.
        assertEquals(3, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ? " +
            "OR node_order=1 OR node_order=2 OR node_order=3", nodeId0).size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.NODES").get(0).get(0));

        // Check joins
        assertEquals(nodeId0, execSql("SELECT N1.ID FROM IGNITE.NODES N1 JOIN " +
            "IGNITE.NODES N2 ON N1.NODE_ORDER = N2.NODE_ORDER JOIN IGNITE.NODES N3 ON N2.ID = N3.ID " +
            "WHERE N3.NODE_ORDER = 1")
            .get(0).get(0));

        // Check sub-query
        assertEquals(nodeId0, execSql("SELECT N1.ID FROM IGNITE.NODES N1 " +
            "WHERE NOT EXISTS (SELECT 1 FROM IGNITE.NODES N2 WHERE N2.ID = N1.ID AND N2.NODE_ORDER <> 1)")
            .get(0).get(0));

        // Check node attributes view
        String cliAttrName = IgniteNodeAttributes.ATTR_CLIENT_MODE;

        assertColumnTypes(execSql("SELECT NODE_ID, NAME, VALUE FROM IGNITE.NODE_ATTRIBUTES").get(0),
            UUID.class, String.class, String.class);

        assertEquals(1,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NAME = ? AND VALUE = 'true'",
                cliAttrName).size());

        assertEquals(3,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NAME = ?", cliAttrName).size());

        assertEquals(1,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = ? AND VALUE = 'true'",
                nodeId(1), cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = '-' AND NAME = ?",
                cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = '-'",
                nodeId(1)).size());

        // Check node metrics view.
        String sqlAllMetrics = "SELECT NODE_ID, LAST_UPDATE_TIME, " +
            "MAX_ACTIVE_JOBS, CUR_ACTIVE_JOBS, AVG_ACTIVE_JOBS, " +
            "MAX_WAITING_JOBS, CUR_WAITING_JOBS, AVG_WAITING_JOBS, " +
            "MAX_REJECTED_JOBS, CUR_REJECTED_JOBS, AVG_REJECTED_JOBS, TOTAL_REJECTED_JOBS, " +
            "MAX_CANCELED_JOBS, CUR_CANCELED_JOBS, AVG_CANCELED_JOBS, TOTAL_CANCELED_JOBS, " +
            "MAX_JOBS_WAIT_TIME, CUR_JOBS_WAIT_TIME, AVG_JOBS_WAIT_TIME, " +
            "MAX_JOBS_EXECUTE_TIME, CUR_JOBS_EXECUTE_TIME, AVG_JOBS_EXECUTE_TIME, TOTAL_JOBS_EXECUTE_TIME, " +
            "TOTAL_EXECUTED_JOBS, TOTAL_EXECUTED_TASKS, " +
            "TOTAL_BUSY_TIME, TOTAL_IDLE_TIME, CUR_IDLE_TIME, BUSY_TIME_PERCENTAGE, IDLE_TIME_PERCENTAGE, " +
            "TOTAL_CPU, CUR_CPU_LOAD, AVG_CPU_LOAD, CUR_GC_CPU_LOAD, " +
            "HEAP_MEMORY_INIT, HEAP_MEMORY_USED, HEAP_MEMORY_COMMITED, HEAP_MEMORY_MAX, HEAP_MEMORY_TOTAL, " +
            "NONHEAP_MEMORY_INIT, NONHEAP_MEMORY_USED, NONHEAP_MEMORY_COMMITED, NONHEAP_MEMORY_MAX, NONHEAP_MEMORY_TOTAL, " +
            "UPTIME, JVM_START_TIME, NODE_START_TIME, LAST_DATA_VERSION, " +
            "CUR_THREAD_COUNT, MAX_THREAD_COUNT, TOTAL_THREAD_COUNT, CUR_DAEMON_THREAD_COUNT, " +
            "SENT_MESSAGES_COUNT, SENT_BYTES_COUNT, RECEIVED_MESSAGES_COUNT, RECEIVED_BYTES_COUNT, " +
            "OUTBOUND_MESSAGES_QUEUE FROM IGNITE.NODE_METRICS";

        List<List<?>> resMetrics = execSql(sqlAllMetrics);

        assertColumnTypes(resMetrics.get(0), UUID.class, Timestamp.class,
            Integer.class, Integer.class, Float.class, // Active jobs.
            Integer.class, Integer.class, Float.class, // Waiting jobs.
            Integer.class, Integer.class, Float.class, Integer.class, // Rejected jobs.
            Integer.class, Integer.class, Float.class, Integer.class, // Canceled jobs.
            Time.class, Time.class, Time.class, // Jobs wait time.
            Time.class, Time.class, Time.class, Time.class, // Jobs execute time.
            Integer.class, Integer.class, // Executed jobs/task.
            Time.class, Time.class, Time.class, Float.class, Float.class, // Busy/idle time.
            Integer.class, Double.class, Double.class, Double.class, // CPU.
            Long.class, Long.class, Long.class, Long.class, Long.class, // Heap memory.
            Long.class, Long.class, Long.class, Long.class, Long.class, // Nonheap memory.
            Time.class, Timestamp.class, Timestamp.class, Long.class, // Uptime.
            Integer.class, Integer.class, Long.class, Integer.class, // Threads.
            Integer.class, Long.class, Integer.class, Long.class, // Sent/received messages.
            Integer.class); // Outbound message queue.

        assertEquals(3, resAll.size());

        // Check join with nodes.
        assertEquals(3, execSql("SELECT NM.LAST_UPDATE_TIME FROM IGNITE.NODES N " +
            "JOIN IGNITE.NODE_METRICS NM ON N.ID = NM.NODE_ID").size());

        // Check index on NODE_ID column.
        assertEquals(1, execSql("SELECT LAST_UPDATE_TIME FROM IGNITE.NODE_METRICS WHERE NODE_ID = ?",
            nodeId(1)).size());

        // Check malformed value for indexed column.
        assertEquals(0, execSql("SELECT LAST_UPDATE_TIME FROM IGNITE.NODE_METRICS WHERE NODE_ID = ?",
            "-").size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.NODE_METRICS").get(0).get(0));

        // Check metric values.

        // Broadcast jobs to server and client nodes to get non zero metric values.
        for (int i = 0; i < 100; i++) {
            IgniteFuture<Void > fut = igniteSrv.compute(igniteSrv.cluster().forNodeId(nodeId0, nodeId(1)))
                .broadcastAsync(
                    new IgniteRunnable() {
                        @Override public void run() {
                            Random rnd = new Random();

                            try {
                                doSleep(rnd.nextInt(100));
                            }
                            catch (Throwable ignore) {
                                // No-op.
                            }
                        }
                    });

            if (i % 10 == 0)
                fut.cancel();
        }

        doSleep(igniteSrv.configuration().getMetricsUpdateFrequency() * 3L);

        for (Ignite grid : G.allGrids()) {
            UUID nodeId = grid.cluster().localNode().id();

            // Metrics for node must be collected from another node to avoid race and get consistent metrics snapshot.
            Ignite ignite = F.eq(nodeId, nodeId0) ? igniteCli : igniteSrv;

            for (int i = 0; i < METRICS_CHECK_ATTEMPTS; i++) {
                ClusterMetrics metrics = ignite.cluster().node(nodeId).metrics();

                assertTrue(metrics instanceof ClusterMetricsSnapshot);

                resMetrics = execSql(ignite, sqlAllMetrics + " WHERE NODE_ID = ?", nodeId);

                log.info("Check metrics for node " + grid.name() + ", attempt " + (i + 1));

                if (metrics.getLastUpdateTime() == ((Timestamp)resMetrics.get(0).get(1)).getTime()) {
                    assertEquals(metrics.getMaximumActiveJobs(), resMetrics.get(0).get(2));
                    assertEquals(metrics.getCurrentActiveJobs(), resMetrics.get(0).get(3));
                    assertEquals(metrics.getAverageActiveJobs(), resMetrics.get(0).get(4));
                    assertEquals(metrics.getMaximumWaitingJobs(), resMetrics.get(0).get(5));
                    assertEquals(metrics.getCurrentWaitingJobs(), resMetrics.get(0).get(6));
                    assertEquals(metrics.getAverageWaitingJobs(), resMetrics.get(0).get(7));
                    assertEquals(metrics.getMaximumRejectedJobs(), resMetrics.get(0).get(8));
                    assertEquals(metrics.getCurrentRejectedJobs(), resMetrics.get(0).get(9));
                    assertEquals(metrics.getAverageRejectedJobs(), resMetrics.get(0).get(10));
                    assertEquals(metrics.getTotalRejectedJobs(), resMetrics.get(0).get(11));
                    assertEquals(metrics.getMaximumCancelledJobs(), resMetrics.get(0).get(12));
                    assertEquals(metrics.getCurrentCancelledJobs(), resMetrics.get(0).get(13));
                    assertEquals(metrics.getAverageCancelledJobs(), resMetrics.get(0).get(14));
                    assertEquals(metrics.getTotalCancelledJobs(), resMetrics.get(0).get(15));
                    assertEquals(metrics.getMaximumJobWaitTime(), convertToMilliseconds(resMetrics.get(0).get(16)));
                    assertEquals(metrics.getCurrentJobWaitTime(), convertToMilliseconds(resMetrics.get(0).get(17)));
                    assertEquals((long)metrics.getAverageJobWaitTime(), convertToMilliseconds(resMetrics.get(0).get(18)));
                    assertEquals(metrics.getMaximumJobExecuteTime(), convertToMilliseconds(resMetrics.get(0).get(19)));
                    assertEquals(metrics.getCurrentJobExecuteTime(), convertToMilliseconds(resMetrics.get(0).get(20)));
                    assertEquals((long)metrics.getAverageJobExecuteTime(), convertToMilliseconds(resMetrics.get(0).get(21)));
                    assertEquals(metrics.getTotalJobsExecutionTime(), convertToMilliseconds(resMetrics.get(0).get(22)));
                    assertEquals(metrics.getTotalExecutedJobs(), resMetrics.get(0).get(23));
                    assertEquals(metrics.getTotalExecutedTasks(), resMetrics.get(0).get(24));
                    assertEquals(metrics.getTotalBusyTime(), convertToMilliseconds(resMetrics.get(0).get(25)));
                    assertEquals(metrics.getTotalIdleTime(), convertToMilliseconds(resMetrics.get(0).get(26)));
                    assertEquals(metrics.getCurrentIdleTime(), convertToMilliseconds(resMetrics.get(0).get(27)));
                    assertEquals(metrics.getBusyTimePercentage(), resMetrics.get(0).get(28));
                    assertEquals(metrics.getIdleTimePercentage(), resMetrics.get(0).get(29));
                    assertEquals(metrics.getTotalCpus(), resMetrics.get(0).get(30));
                    assertEquals(metrics.getCurrentCpuLoad(), resMetrics.get(0).get(31));
                    assertEquals(metrics.getAverageCpuLoad(), resMetrics.get(0).get(32));
                    assertEquals(metrics.getCurrentGcCpuLoad(), resMetrics.get(0).get(33));
                    assertEquals(metrics.getHeapMemoryInitialized(), resMetrics.get(0).get(34));
                    assertEquals(metrics.getHeapMemoryUsed(), resMetrics.get(0).get(35));
                    assertEquals(metrics.getHeapMemoryCommitted(), resMetrics.get(0).get(36));
                    assertEquals(metrics.getHeapMemoryMaximum(), resMetrics.get(0).get(37));
                    assertEquals(metrics.getHeapMemoryTotal(), resMetrics.get(0).get(38));
                    assertEquals(metrics.getNonHeapMemoryInitialized(), resMetrics.get(0).get(39));
                    assertEquals(metrics.getNonHeapMemoryUsed(), resMetrics.get(0).get(40));
                    assertEquals(metrics.getNonHeapMemoryCommitted(), resMetrics.get(0).get(41));
                    assertEquals(metrics.getNonHeapMemoryMaximum(), resMetrics.get(0).get(42));
                    assertEquals(metrics.getNonHeapMemoryTotal(), resMetrics.get(0).get(43));
                    assertEquals(metrics.getUpTime(), convertToMilliseconds(resMetrics.get(0).get(44)));
                    assertEquals(metrics.getStartTime(), ((Timestamp)resMetrics.get(0).get(45)).getTime());
                    assertEquals(metrics.getNodeStartTime(), ((Timestamp)resMetrics.get(0).get(46)).getTime());
                    assertEquals(metrics.getLastDataVersion(), resMetrics.get(0).get(47));
                    assertEquals(metrics.getCurrentThreadCount(), resMetrics.get(0).get(48));
                    assertEquals(metrics.getMaximumThreadCount(), resMetrics.get(0).get(49));
                    assertEquals(metrics.getTotalStartedThreadCount(), resMetrics.get(0).get(50));
                    assertEquals(metrics.getCurrentDaemonThreadCount(), resMetrics.get(0).get(51));
                    assertEquals(metrics.getSentMessagesCount(), resMetrics.get(0).get(52));
                    assertEquals(metrics.getSentBytesCount(), resMetrics.get(0).get(53));
                    assertEquals(metrics.getReceivedMessagesCount(), resMetrics.get(0).get(54));
                    assertEquals(metrics.getReceivedBytesCount(), resMetrics.get(0).get(55));
                    assertEquals(metrics.getOutboundMessagesQueueSize(), resMetrics.get(0).get(56));

                    break;
                }
                else {
                    log.info("Metrics was updated in background, will retry check");

                    if (i == METRICS_CHECK_ATTEMPTS - 1)
                        fail("Failed to check metrics, attempts limit reached (" + METRICS_CHECK_ATTEMPTS + ')');
                }
            }
        }
    }

    /**
     * Test baseline topology system view.
     */
    public void testBaselineViews() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrid(getTestIgniteInstanceName(), getPdsConfiguration("node0"));
        startGrid(getTestIgniteInstanceName(1), getPdsConfiguration("node1"));

        ignite.cluster().active(true);

        List<List<?>> res = execSql("SELECT CONSISTENT_ID, ONLINE FROM IGNITE.BASELINE_NODES ORDER BY CONSISTENT_ID");

        assertColumnTypes(res.get(0), String.class, Boolean.class);

        assertEquals(2, res.size());

        assertEquals("node0", res.get(0).get(0));
        assertEquals("node1", res.get(1).get(0));

        assertEquals(true, res.get(0).get(1));
        assertEquals(true, res.get(1).get(1));

        stopGrid(getTestIgniteInstanceName(1));

        res = execSql("SELECT CONSISTENT_ID FROM IGNITE.BASELINE_NODES WHERE ONLINE = false");

        assertEquals(1, res.size());

        assertEquals("node1", res.get(0).get(0));

        Ignite ignite2 = startGrid(getTestIgniteInstanceName(2), getPdsConfiguration("node2"));

        assertEquals(2, execSql(ignite2, "SELECT CONSISTENT_ID FROM IGNITE.BASELINE_NODES").size());

        res = execSql("SELECT CONSISTENT_ID FROM IGNITE.NODES N WHERE NOT EXISTS (SELECT 1 FROM " +
            "IGNITE.BASELINE_NODES B WHERE B.CONSISTENT_ID = N.CONSISTENT_ID)");

        assertEquals(1, res.size());

        assertEquals("node2", res.get(0).get(0));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return super.getConfiguration().setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME));
    }

    /**
     * Gets ignite configuration with persistence enabled.
     */
    private IgniteConfiguration getPdsConfiguration(String consistentId) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024L * 1024L).setPersistenceEnabled(true))
        );

        cfg.setConsistentId(consistentId);

        return cfg;
    }

    /**
     * Convert Time to milliseconds.
     *
     * Note: Returned Time values from SQL it's milliseconds since January 1, 1970, 00:00:00 GMT. To get right interval
     * in milliseconds this value must be adjusted to current time zone.
     *
     * @param sqlTime Time value returned from SQL.
     */
    private long convertToMilliseconds(Object sqlTime) {
        Time time0 = (Time)sqlTime;

        return time0.getTime() + TimeZone.getDefault().getOffset(time0.getTime());
    }
}
