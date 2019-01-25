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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewTables;
import org.apache.ignite.internal.util.lang.GridNodePredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Arrays.asList;

/**
 * Tests for ignite SQL system views.
 */
@RunWith(JUnit4.class)
public class SqlSystemViewsSelfTest extends AbstractIndexingCommonTest {
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
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
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
    private List<List<?>> execSql(String sql, Object... args) {
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
    @Test
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
    @Test
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
    @Test
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
    private void assertColumnTypes(List<?> rowData, Class<?>... colTypes) {
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
    @Test
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
            IgniteFuture<Void> fut = igniteSrv.compute(igniteSrv.cluster().forNodeId(nodeId0, nodeId(1)))
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
    @Test
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
     * Test IO statistics SQL system views for cache groups.
     *
     * @throws Exception
     */
    @Test
    public void testIoStatisticsViews() throws Exception {
        Ignite ignite = startGrid(getTestIgniteInstanceName(), getPdsConfiguration("node0"));

        ignite.cluster().active(true);

        execSql("CREATE TABLE TST(id INTEGER PRIMARY KEY, name VARCHAR, age integer)");

        for (int i = 0; i < 500; i++)
            execSql("INSERT INTO DEFAULT.TST(id, name, age) VALUES (" + i + ",'name-" + i + "'," + i + 1 + ")");

        String sql1 = "SELECT GROUP_ID, GROUP_NAME, PHYSICAL_READS, LOGICAL_READS FROM IGNITE.CACHE_GROUPS_IO";

        List<List<?>> res1 = execSql(sql1);

        Map<?, ?> map = res1.stream().collect(Collectors.toMap(k -> k.get(1), v -> v.get(3)));

        assertEquals(2, map.size());

        assertTrue(map.containsKey("SQL_default_TST"));

        assertTrue((Long)map.get("SQL_default_TST") > 0);

        assertTrue(map.containsKey(DEFAULT_CACHE_NAME));

        sql1 = "SELECT GROUP_ID, GROUP_NAME, PHYSICAL_READS, LOGICAL_READS FROM IGNITE.CACHE_GROUPS_IO WHERE " +
            "GROUP_NAME='SQL_default_TST'";

        assertEquals(1, execSql(sql1).size());
    }

    /**
     * Simple test for {@link SqlSystemViewTables}
     */
    @Test
    public void testTablesView() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        GridCacheProcessor cacheProc = ignite.context().cache();

        execSql("CREATE TABLE CACHE_SQL (ID INT PRIMARY KEY, MY_VAL VARCHAR) WITH " +
            "\"cache_name=cache_sql,template=partitioned,atomicity=atomic,wrap_value=true,value_type=random_name\"");

        execSql("CREATE TABLE PUBLIC.DFLT_CACHE (ID1 INT, ID2 INT, MY_VAL VARCHAR, PRIMARY KEY (ID1, ID2)) WITH"
            + "\"affinity_key=ID2,wrap_value=false,key_type=random_name\"");

        int cacheSqlId = cacheProc.cacheDescriptor("cache_sql").cacheId();
        int ddlTabId = cacheProc.cacheDescriptor("SQL_PUBLIC_DFLT_CACHE").cacheId();

        List<List<?>> cacheSqlInfos = execSql("SELECT * FROM IGNITE.TABLES WHERE TABLE_NAME = 'CACHE_SQL'");

        List<?> expRow = asList(
            "DEFAULT",           // SCHEMA_NAME
            "CACHE_SQL",         // TABLE_NAME
            "cache_sql",         // CACHE_NAME
            cacheSqlId,          // CACHE_ID
            null,                // AFFINITY_KEY_COLUMN
            "ID",                // KEY_ALIAS
            null,                // VALUE_ALIAS
            "java.lang.Integer", // KEY_TYPE_NAME
            "random_name"        // VALUE_TYPE_NAME

        );

        assertEquals("Returned incorrect info. ", expRow, cacheSqlInfos.get(0));

        // no more rows are expected.
        assertEquals("Expected to return only one row", 1, cacheSqlInfos.size());

        List<List<?>> allInfos = execSql("SELECT * FROM IGNITE.TABLES");

        List<?> allExpRows = asList(
            expRow,
            asList(
                "PUBLIC",                // SCHEMA_NAME
                "DFLT_CACHE",            // TABLE_NAME
                "SQL_PUBLIC_DFLT_CACHE", // CACHE_NAME
                ddlTabId,                // CACHE_ID
                "ID2",                   // AFFINITY_KEY_COLUMN
                null,                    // KEY_ALIAS
                "MY_VAL",                // VALUE_ALIAS
                "random_name",           // KEY_TYPE_NAME
                "java.lang.String"       // VALUE_TYPE_NAME
            )
        );

        if (!F.eqNotOrdered(allExpRows, allInfos))
            fail("Returned incorrect rows [expected=" + allExpRows + ", actual=" + allInfos + "].");

        // Filter by cache name:
        assertEquals(
            Collections.singletonList(asList("DFLT_CACHE", "SQL_PUBLIC_DFLT_CACHE")),
            execSql("SELECT TABLE_NAME, CACHE_NAME " +
                "FROM IGNITE.TABLES " +
                "WHERE CACHE_NAME LIKE 'SQL\\_PUBLIC\\_%'"));

        assertEquals(
            Collections.singletonList(asList("CACHE_SQL", "cache_sql")),
            execSql("SELECT TABLE_NAME, CACHE_NAME " +
                "FROM IGNITE.TABLES " +
                "WHERE CACHE_NAME NOT LIKE 'SQL\\_PUBLIC\\_%'"));

        // Join with CACHES view.
        assertEquals(
            asList(
                asList("DFLT_CACHE", "SQL_PUBLIC_DFLT_CACHE", "SQL_PUBLIC_DFLT_CACHE"),
                asList("CACHE_SQL", "cache_sql", "cache_sql")),
            execSql("SELECT TABLE_NAME, TAB.CACHE_NAME, C.NAME " +
                "FROM IGNITE.TABLES AS TAB JOIN IGNITE.CACHES AS C " +
                "ON TAB.CACHE_ID = C.CACHE_ID " +
                "ORDER BY C.NAME")
        );
    }

    /**
     * Verify that if we drop or create table, TABLES system view reflects these changes.
     */
    @Test
    public void testTablesDropAndCreate() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        final String selectTabNameCacheName = "SELECT TABLE_NAME, CACHE_NAME FROM IGNITE.TABLES ORDER BY TABLE_NAME";

        assertTrue("Initially no tables expected", execSql(selectTabNameCacheName).isEmpty());

        execSql("CREATE TABLE PUBLIC.TAB1 (ID INT PRIMARY KEY, VAL VARCHAR)");

        assertEquals(
            asList(asList("TAB1", "SQL_PUBLIC_TAB1")),
            execSql(selectTabNameCacheName));

        execSql("CREATE TABLE PUBLIC.TAB2 (ID LONG PRIMARY KEY, VAL_STR VARCHAR) WITH \"cache_name=cache2\"");
        execSql("CREATE TABLE PUBLIC.TAB3 (ID LONG PRIMARY KEY, VAL_INT INT) WITH \"cache_name=cache3\" ");

        assertEquals(
            asList(
                asList("TAB1", "SQL_PUBLIC_TAB1"),
                asList("TAB2", "cache2"),
                asList("TAB3", "cache3")
            ),
            execSql(selectTabNameCacheName));

        execSql("DROP TABLE PUBLIC.TAB2");

        assertEquals(
            asList(
                asList("TAB1", "SQL_PUBLIC_TAB1"),
                asList("TAB3", "cache3")
            ),
            execSql(selectTabNameCacheName));

        execSql("DROP TABLE PUBLIC.TAB3");

        assertEquals(
            asList(asList("TAB1", "SQL_PUBLIC_TAB1")),
            execSql(selectTabNameCacheName));

        execSql("DROP TABLE PUBLIC.TAB1");

        assertTrue("All tables should be dropped", execSql(selectTabNameCacheName).isEmpty());
    }



    /**
     * Dummy implementation of the mapper. Required to test "AFFINITY_KEY_COLUMN".
     */
    static class ConstantMapper implements AffinityKeyMapper {
        /** Serial version uid. */
        private static final long serialVersionUID = 7018626316531791556L;

        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            //NO-op
        }
    }

    /**
     * Check affinity column if custom affinity mapper is specified.
     */
    @Test
    public void testTablesNullAffinityKey() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        AffinityKeyMapper fakeMapper = new ConstantMapper();

        ignite.getOrCreateCache(defaultCacheConfiguration().setName("NO_KEY_FIELDS_CACHE").setAffinityMapper(fakeMapper)
            .setQueryEntities(Collections.singleton(
                // A cache with  no key fields
                new QueryEntity(Object.class.getName(), "Object2")
                    .addQueryField("name", String.class.getName(), null)
                    .addQueryField("salary", Integer.class.getName(), null)
                    .setTableName("NO_KEY_TABLE")
            )));

        List<List<String>> expected = Collections.singletonList(asList("NO_KEY_TABLE", null));

        assertEquals(expected,
            execSql("SELECT TABLE_NAME, AFFINITY_KEY_COLUMN " +
                "FROM IGNITE.TABLES " +
                "WHERE CACHE_NAME = 'NO_KEY_FIELDS_CACHE'"));

        assertEquals(expected,
            execSql("SELECT TABLE_NAME, AFFINITY_KEY_COLUMN " +
                "FROM IGNITE.TABLES " +
                "WHERE AFFINITY_KEY_COLUMN IS NULL"));
    }

    /**
     * Special test for key/val name and type. Covers most used cases
     */
    @Test
    public void testTablesViewKeyVal() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        {
            ignite.getOrCreateCache(defaultCacheConfiguration().setName("NO_ALIAS_NON_SQL_KEY")
                .setQueryEntities(Collections.singleton(
                    // A cache with  no key fields
                    new QueryEntity(Object.class.getName(), "Object2")
                        .addQueryField("name", String.class.getName(), null)
                        .addQueryField("salary", Integer.class.getName(), null)
                        .setTableName("NO_ALIAS_NON_SQL_KEY")
                )));

            List<?> keyValAliases = execSql("SELECT KEY_ALIAS, VALUE_ALIAS FROM IGNITE.TABLES " +
                "WHERE TABLE_NAME = 'NO_ALIAS_NON_SQL_KEY'").get(0);

            assertEquals(asList(null, null), keyValAliases);
        }

        {
            execSql("CREATE TABLE PUBLIC.SIMPLE_KEY_SIMPLE_VAL (ID INT PRIMARY KEY, NAME VARCHAR) WITH \"wrap_value=false\"");

            List<?> keyValAliases = execSql("SELECT KEY_ALIAS, VALUE_ALIAS FROM IGNITE.TABLES " +
                "WHERE TABLE_NAME = 'SIMPLE_KEY_SIMPLE_VAL'").get(0);

            assertEquals(asList("ID", "NAME"), keyValAliases);

        }

        {
            execSql("CREATE TABLE PUBLIC.COMPLEX_KEY_COMPLEX_VAL " +
                "(ID1 INT, " +
                "ID2 INT, " +
                "VAL1 VARCHAR, " +
                "VAL2 VARCHAR, " +
                "PRIMARY KEY(ID1, ID2))");

            List<?> keyValAliases = execSql("SELECT KEY_ALIAS, VALUE_ALIAS FROM IGNITE.TABLES " +
                "WHERE TABLE_NAME = 'COMPLEX_KEY_COMPLEX_VAL'").get(0);

            assertEquals(asList(null, null), keyValAliases);
        }
    }

    /**
     * Test caches system views.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testCachesViews() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setName("def").setPersistenceEnabled(true))
            .setDataRegionConfigurations(new DataRegionConfiguration().setName("dr1"),
                new DataRegionConfiguration().setName("dr2"));

        IgniteEx ignite0 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg));

        Ignite ignite1 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg).setIgniteInstanceName("node1"));

        ignite0.cluster().active(true);

        Ignite ignite2 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg).setIgniteInstanceName("node2"));

        Ignite ignite3 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg).setIgniteInstanceName("node3")
            .setClientMode(true));

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_atomic_part")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setGroupName("cache_grp")
            .setNodeFilter(new TestNodeFilter(ignite0.cluster().localNode()))
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_atomic_repl")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("dr1")
            .setTopologyValidator(new TestTopologyValidator())
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_tx_part")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setGroupName("cache_grp")
            .setNodeFilter(new TestNodeFilter(ignite0.cluster().localNode()))
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_tx_repl")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("dr2")
            .setEvictionFilter(new TestEvictionFilter())
            .setEvictionPolicyFactory(new TestEvictionPolicyFactory())
            .setOnheapCacheEnabled(true)
        );

        execSql("CREATE TABLE cache_sql (ID INT PRIMARY KEY, VAL VARCHAR) WITH " +
            "\"cache_name=cache_sql,template=partitioned,atomicity=atomic\"");

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT NAME, CACHE_ID, CACHE_TYPE, GROUP_ID, GROUP_NAME, " +
                "CACHE_MODE, ATOMICITY_MODE, IS_ONHEAP_CACHE_ENABLED, IS_COPY_ON_READ, IS_LOAD_PREVIOUS_VALUE, " +
                "IS_READ_FROM_BACKUP, PARTITION_LOSS_POLICY, NODE_FILTER, TOPOLOGY_VALIDATOR, IS_EAGER_TTL, " +
                "WRITE_SYNCHRONIZATION_MODE, IS_INVALIDATE, IS_EVENTS_DISABLED, IS_STATISTICS_ENABLED, " +
                "IS_MANAGEMENT_ENABLED, BACKUPS, AFFINITY, AFFINITY_MAPPER, " +
                "REBALANCE_MODE, REBALANCE_BATCH_SIZE, REBALANCE_TIMEOUT, REBALANCE_DELAY, REBALANCE_THROTTLE, " +
                "REBALANCE_BATCHES_PREFETCH_COUNT, REBALANCE_ORDER, " +
                "EVICTION_FILTER, EVICTION_POLICY_FACTORY, " +
                "IS_NEAR_CACHE_ENABLED, NEAR_CACHE_EVICTION_POLICY_FACTORY, NEAR_CACHE_START_SIZE, " +
                "DEFAULT_LOCK_TIMEOUT, CACHE_INTERCEPTOR, CACHE_STORE_FACTORY, " +
                "IS_STORE_KEEP_BINARY, IS_READ_THROUGH, IS_WRITE_THROUGH, " +
                "IS_WRITE_BEHIND_ENABLED, WRITE_BEHIND_COALESCING, WRITE_BEHIND_FLUSH_SIZE, " +
                "WRITE_BEHIND_FLUSH_FREQUENCY, WRITE_BEHIND_FLUSH_THREAD_COUNT, WRITE_BEHIND_FLUSH_BATCH_SIZE, " +
                "MAX_CONCURRENT_ASYNC_OPERATIONS, CACHE_LOADER_FACTORY, CACHE_WRITER_FACTORY, EXPIRY_POLICY_FACTORY, " +
                "IS_SQL_ESCAPE_ALL, SQL_SCHEMA, SQL_INDEX_MAX_INLINE_SIZE, IS_SQL_ONHEAP_CACHE_ENABLED, " +
                "SQL_ONHEAP_CACHE_MAX_SIZE, QUERY_DETAILS_METRICS_SIZE, QUERY_PARALLELISM, MAX_QUERY_ITERATORS_COUNT, " +
                "DATA_REGION_NAME FROM IGNITE.CACHES");

        assertColumnTypes(resAll.get(0),
            String.class, Integer.class, String.class, Integer.class, String.class,
            String.class, String.class, Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, String.class, String.class, String.class, Boolean.class,
            String.class, Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, Integer.class, String.class, String.class,
            String.class, Integer.class, Long.class, Long.class, Long.class, // Rebalance.
            Long.class, Integer.class,
            String.class, String.class, // Eviction.
            Boolean.class, String.class, Integer.class, // Near cache.
            Long.class, String.class, String.class,
            Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, Boolean.class, Integer.class, // Write-behind.
            Long.class, Integer.class, Integer.class,
            Integer.class, String.class, String.class, String.class,
            Boolean.class, String.class, Integer.class, Boolean.class, // SQL.
            Integer.class, Integer.class, Integer.class, Integer.class,
            String.class);

        assertEquals("cache_tx_part", execSql("SELECT NAME FROM IGNITE.CACHES WHERE " +
            "CACHE_MODE = 'PARTITIONED' AND ATOMICITY_MODE = 'TRANSACTIONAL' AND NAME like 'cache%'").get(0).get(0));

        assertEquals("cache_atomic_repl", execSql("SELECT NAME FROM IGNITE.CACHES WHERE " +
            "CACHE_MODE = 'REPLICATED' AND ATOMICITY_MODE = 'ATOMIC' AND NAME like 'cache%'").get(0).get(0));

        assertEquals(2L, execSql("SELECT COUNT(*) FROM IGNITE.CACHES WHERE GROUP_NAME = 'cache_grp'")
            .get(0).get(0));

        assertEquals("cache_atomic_repl", execSql("SELECT NAME FROM IGNITE.CACHES " +
            "WHERE DATA_REGION_NAME = 'dr1'").get(0).get(0));

        assertEquals("cache_tx_repl", execSql("SELECT NAME FROM IGNITE.CACHES " +
            "WHERE DATA_REGION_NAME = 'dr2'").get(0).get(0));

        assertEquals("PARTITIONED", execSql("SELECT CACHE_MODE FROM IGNITE.CACHES " +
            "WHERE NAME = 'cache_atomic_part'").get(0).get(0));

        assertEquals("USER", execSql("SELECT CACHE_TYPE FROM IGNITE.CACHES WHERE NAME = 'cache_sql'")
            .get(0).get(0));

        assertEquals(0L, execSql("SELECT COUNT(*) FROM IGNITE.CACHES WHERE NAME = 'no_such_cache'").get(0)
            .get(0));

        assertEquals(0L, execSql("SELECT COUNT(*) FROM IGNITE.CACHES WHERE NAME = 1").get(0).get(0));

        assertEquals("TestNodeFilter", execSql("SELECT NODE_FILTER FROM IGNITE.CACHES WHERE NAME = " +
            "'cache_atomic_part'").get(0).get(0));

        assertEquals("TestEvictionFilter", execSql("SELECT EVICTION_FILTER FROM IGNITE.CACHES " +
            "WHERE NAME = 'cache_tx_repl'").get(0).get(0));

        assertEquals("TestEvictionPolicyFactory", execSql("SELECT EVICTION_POLICY_FACTORY " +
            "FROM IGNITE.CACHES WHERE NAME = 'cache_tx_repl'").get(0).get(0));

        assertEquals("TestTopologyValidator", execSql("SELECT TOPOLOGY_VALIDATOR FROM IGNITE.CACHES " +
            "WHERE NAME = 'cache_atomic_repl'").get(0).get(0));

        // Check quick count.
        assertEquals(execSql("SELECT COUNT(*) FROM IGNITE.CACHES").get(0).get(0),
            execSql("SELECT COUNT(*) FROM IGNITE.CACHES WHERE CACHE_ID <> CACHE_ID + 1").get(0).get(0));

        // Check that caches are the same on BLT, BLT filtered by node filter, non BLT and client nodes.
        assertEquals(5L, execSql("SELECT COUNT(*) FROM IGNITE.CACHES WHERE NAME like 'cache%'").get(0)
            .get(0));

        assertEquals(5L, execSql(ignite1, "SELECT COUNT(*) FROM IGNITE.CACHES WHERE NAME like 'cache%'")
            .get(0).get(0));

        assertEquals(5L, execSql(ignite2, "SELECT COUNT(*) FROM IGNITE.CACHES WHERE NAME like 'cache%'")
            .get(0).get(0));

        assertEquals(5L, execSql(ignite3, "SELECT COUNT(*) FROM IGNITE.CACHES WHERE NAME like 'cache%'")
            .get(0).get(0));

        // Check cache groups.
        resAll = execSql("SELECT ID, GROUP_NAME, IS_SHARED, CACHE_COUNT, " +
            "CACHE_MODE, ATOMICITY_MODE, AFFINITY, PARTITIONS_COUNT, " +
            "NODE_FILTER, DATA_REGION_NAME, TOPOLOGY_VALIDATOR, PARTITION_LOSS_POLICY, " +
            "REBALANCE_MODE, REBALANCE_DELAY, REBALANCE_ORDER, BACKUPS " +
            "FROM IGNITE.CACHE_GROUPS");

        assertColumnTypes(resAll.get(0),
            Integer.class, String.class, Boolean.class, Integer.class,
            String.class, String.class, String.class, Integer.class,
            String.class, String.class, String.class, String.class,
            String.class, Long.class, Integer.class, Integer.class);

        assertEquals(2, execSql("SELECT CACHE_COUNT FROM IGNITE.CACHE_GROUPS " +
            "WHERE GROUP_NAME = 'cache_grp'").get(0).get(0));

        assertEquals("cache_grp", execSql("SELECT GROUP_NAME FROM IGNITE.CACHE_GROUPS " +
            "WHERE IS_SHARED = true AND GROUP_NAME like 'cache%'").get(0).get(0));

        // Check index on ID column.
        assertEquals("cache_tx_repl", execSql("SELECT GROUP_NAME FROM IGNITE.CACHE_GROUPS " +
            "WHERE ID = ?", ignite0.cachex("cache_tx_repl").context().groupId()).get(0).get(0));

        assertEquals(0, execSql("SELECT ID FROM IGNITE.CACHE_GROUPS WHERE ID = 0").size());

        // Check join by indexed column.
        assertEquals("cache_tx_repl", execSql("SELECT CG.GROUP_NAME FROM IGNITE.CACHES C JOIN " +
            "IGNITE.CACHE_GROUPS CG ON C.GROUP_ID = CG.ID WHERE C.NAME = 'cache_tx_repl'").get(0).get(0));

        // Check join by non-indexed column.
        assertEquals("cache_grp", execSql("SELECT CG.GROUP_NAME FROM IGNITE.CACHES C JOIN " +
            "IGNITE.CACHE_GROUPS CG ON C.GROUP_NAME = CG.GROUP_NAME WHERE C.NAME = 'cache_tx_part'").get(0).get(0));

        // Check configuration equality for cache and cache group views.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.CACHES C JOIN IGNITE.CACHE_GROUPS CG " +
            "ON C.NAME = CG.GROUP_NAME WHERE C.NAME like 'cache%' " +
            "AND C.CACHE_MODE = CG.CACHE_MODE " +
            "AND C.ATOMICITY_MODE = CG.ATOMICITY_MODE " +
            "AND COALESCE(C.AFFINITY, '-') = COALESCE(CG.AFFINITY, '-') " +
            "AND COALESCE(C.NODE_FILTER, '-') = COALESCE(CG.NODE_FILTER, '-') " +
            "AND COALESCE(C.DATA_REGION_NAME, '-') = COALESCE(CG.DATA_REGION_NAME, '-') " +
            "AND COALESCE(C.TOPOLOGY_VALIDATOR, '-') = COALESCE(CG.TOPOLOGY_VALIDATOR, '-') " +
            "AND C.PARTITION_LOSS_POLICY = CG.PARTITION_LOSS_POLICY " +
            "AND C.REBALANCE_MODE = CG.REBALANCE_MODE " +
            "AND C.REBALANCE_DELAY = CG.REBALANCE_DELAY " +
            "AND C.REBALANCE_ORDER = CG.REBALANCE_ORDER " +
            "AND C.BACKUPS = CG.BACKUPS").get(0).get(0));

        // Check quick count.
        assertEquals(execSql("SELECT COUNT(*) FROM IGNITE.CACHE_GROUPS").get(0).get(0),
            execSql("SELECT COUNT(*) FROM IGNITE.CACHE_GROUPS WHERE ID <> ID + 1").get(0).get(0));

        // Check that cache groups are the same on different nodes.
        assertEquals(4L, execSql("SELECT COUNT(*) FROM IGNITE.CACHE_GROUPS " +
            "WHERE GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(4L, execSql(ignite1, "SELECT COUNT(*) FROM IGNITE.CACHE_GROUPS " +
            "WHERE GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(4L, execSql(ignite2, "SELECT COUNT(*) FROM IGNITE.CACHE_GROUPS " +
            "WHERE GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(4L, execSql(ignite3, "SELECT COUNT(*) FROM IGNITE.CACHE_GROUPS " +
            "WHERE GROUP_NAME like 'cache%'").get(0).get(0));
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

    /**
     *
     */
    private static class TestNodeFilter extends GridNodePredicate {
        /**
         * @param node Node.
         */
        public TestNodeFilter(ClusterNode node) {
            super(node);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestNodeFilter";
        }
    }

    /**
     *
     */
    private static class TestEvictionFilter implements EvictionFilter<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean evictAllowed(Cache.Entry<Object, Object> entry) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestEvictionFilter";
        }
    }

    /**
     *
     */
    private static class TestEvictionPolicyFactory implements Factory<EvictionPolicy<Object, Object>> {
        /** {@inheritDoc} */
        @Override public EvictionPolicy<Object, Object> create() {
            return new EvictionPolicy<Object, Object>() {
                @Override public void onEntryAccessed(boolean rmv, EvictableEntry<Object, Object> entry) {
                    // No-op.
                }
            };
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestEvictionPolicyFactory";
        }
    }

    /**
     *
     */
    private static class TestTopologyValidator implements TopologyValidator {
        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestTopologyValidator";
        }
    }
}
