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

package org.apache.ignite.util;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectAllTypes;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectEnum;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg;
import org.apache.ignite.internal.metric.SystemViewSelfTest.TestPredicate;
import org.apache.ignite.internal.metric.SystemViewSelfTest.TestRunnable;
import org.apache.ignite.internal.metric.SystemViewSelfTest.TestTransformer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest.TestAffinityFunction;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeVisitor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.regex.Pattern.quote;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommand.COLUMN_SEPARATOR;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.metric.SystemViewSelfTest.TEST_PREDICATE;
import static org.apache.ignite.internal.metric.SystemViewSelfTest.TEST_TRANSFORMER;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.PART_STATES_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.BINARY_METADATA_VIEW;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.DATA_REGION_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl.DISTRIBUTED_METASTORE_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_SCHEMA_VIEW;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_TBLS_VIEW;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_TBL_COLS_VIEW;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_VIEWS_VIEW;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_VIEW_COLS_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;
import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** Tests output of {@link CommandList#SYSTEM_VIEW} command. */
public class SystemViewCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Command line argument for printing content of a system view. */
    private static final String CMD_SYS_VIEW = SYSTEM_VIEW.text();

    /** Latch that is used to unblock all compute jobs. */
    private static final CountDownLatch COMPUTE_JOB_UNBLOCK_LATCH = new CountDownLatch(1);

    /** Latch that indicates number of compute jobs to be blocked.*/
    private static final CountDownLatch COMPUTE_JOB_BLOCK_LATCH = new CountDownLatch(5);

    /** Name of the test data region. */
    private static final String DATA_REGION_NAME = "in-memory";

    /** Test node with 0 index. */
    private IgniteEx ignite0;

    /** Test node with 1 index. */
    private IgniteEx ignite1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDataRegionConfigurations(new DataRegionConfiguration()
                .setName(DATA_REGION_NAME)
                .setMaxSize(100L * 1024 * 1024))
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();

        autoConfirmation = false;

        ignite0 = ignite(0);
        ignite1 = ignite(1);
    }

    /** Tests command error output in case of mandatory system view name is omitted. */
    @Test
    public void testSystemViewNameMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW),
            "The name of the system view for which its content should be printed is expected.");
    }

    /** Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is omitted. */
    @Test
    public void testNodeIdMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName()),
            "ID of the node from which system view content should be obtained is expected.");
    }

    /** Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is invalid.*/
    @Test
    public void testInvalidNodeIdFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName(), "invalid_node_id"),
            "Failed to parse " + NODE_ID.argName() +
                " command argument. String representation of \"java.util.UUID\" is exepected." +
                " For example: 123e4567-e89b-42d3-a456-556642440000"
        );
    }

    /** Tests command error output in case multiple system view names are specified. */
    @Test
    public void testMultipleSystemViewNamesFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, SVCS_VIEW, CACHE_GRP_PAGE_LIST_VIEW),
            "Multiple system view names are not supported.");
    }

    /**
     * Tests command error output in case {@link SystemViewCommandArg#NODE_ID} argument value refers to nonexistent
     * node.
     */
    @Test
    public void testNonExistentNodeIdFailure() {
        String incorrectNodeId = UUID.randomUUID().toString();

        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, "--node-id", incorrectNodeId, CACHES_VIEW),
            "Failed to perform operation.\nNode with id=" + incorrectNodeId + " not found");
    }

    /** Tests command output in case nonexistent system view names is specified. */
    @Test
    public void testNonExistentSystemView() {
        assertContains(log, executeCommand(EXIT_CODE_OK, CMD_SYS_VIEW, "non_existent_system_view"),
            "No system view with specified name was found [name=non_existent_system_view]");
    }

    /** */
    @Test
    public void testCachesView() {
        Set<String> cacheNames = new HashSet<>(asList("cache-1", "cache-2"));

        cacheNames.forEach(cacheName -> ignite0.createCache(cacheName));

        List<List<String>> cachesView = systemView(ignite0, CACHES_VIEW);

        assertEquals(ignite0.context().cache().cacheDescriptors().size(), cachesView.size());

        cachesView.forEach(row -> cacheNames.remove(row.get(0)));

        assertTrue(cacheNames.isEmpty());
    }

    /** */
    @Test
    public void testCacheGroupsView() {
        Set<String> grpNames = new HashSet<>(asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite0.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        List<List<String>> cacheGrpsView = systemView(ignite0, CACHE_GRPS_VIEW);

        assertEquals(ignite0.context().cache().cacheGroupDescriptors().size(), cacheGrpsView.size());

        cacheGrpsView.forEach(row -> grpNames.remove(row.get(0)));

        assertTrue(grpNames.toString(), grpNames.isEmpty());
    }

    /** */
    @Test
    public void testComputeBroadcast() throws Exception {
        long tasksCnt = COMPUTE_JOB_BLOCK_LATCH.getCount();

        try {
            for (int i = 0; i < tasksCnt; i++) {
                ignite0.compute().broadcastAsync(() -> {
                    try {
                        COMPUTE_JOB_BLOCK_LATCH.countDown();
                        COMPUTE_JOB_UNBLOCK_LATCH.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            COMPUTE_JOB_BLOCK_LATCH.await();

            List<List<String>> tasksView = systemView(ignite0, TASKS_VIEW).stream()
                .filter(row -> !VisorSystemViewTask.class.getName().equals(row.get(3)))
                .collect(Collectors.toList());

            assertEquals(tasksCnt, tasksView.size());

            tasksView.forEach(row -> {
                assertEquals("false", row.get(10)); //internal
                assertEquals("null", row.get(6)); // affinityCacheName
                assertEquals("-1", row.get(5)); // affinityPartitionId
                assertTrue(row.get(4).startsWith(getClass().getName())); // taskClassName
                assertTrue(row.get(3).startsWith(getClass().getName())); // taskName
                assertEquals(ignite0.localNode().id().toString(), row.get(2)); // taskNodeId
                assertEquals("0", row.get(12)); // userVersion
            });
        }
        finally {
            COMPUTE_JOB_UNBLOCK_LATCH.countDown();
        }
    }

    /** */
    @Test
    public void testServices() {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName("service");
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new DummyService());

        ignite0.services().deploy(srvcCfg);

        List<List<String>> srvsView = systemView(ignite0, SVCS_VIEW);

        assertEquals(1, srvsView.size());

        List<String> sysView = srvsView.get(0);

        assertEquals(srvcCfg.getName(), sysView.get(1)); // name
        assertEquals(DummyService.class.getName(), sysView.get(2)); // serviceClass
        assertEquals(Integer.toString(srvcCfg.getMaxPerNodeCount()), sysView.get(6)); // maxPerNodeCount
    }

    /** */
    @Test
    public void testClientsConnections() throws Exception {
        String host = ignite0.configuration().getClientConnectorConfiguration().getHost();

        if (host == null)
            host = ignite0.configuration().getLocalHost();

        int port = ignite0.configuration().getClientConnectorConfiguration().getPort();

        try (
            IgniteClient ignored1 = Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port));
            Connection ignored2 = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())
        ) {
            assertEquals(2, systemView(ignite0, CLI_CONN_VIEW).size());
        }
    }

    /** */
    @Test
    public void testTransactions() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite0.createCache(
            new CacheConfiguration<Integer, Integer>("test-cache").setAtomicityMode(TRANSACTIONAL));

        assertTrue(systemView(ignite0, TXS_MON_LIST).isEmpty());

        CountDownLatch txBlockLatch = new CountDownLatch(10);
        CountDownLatch txUnblockLatch = new CountDownLatch(1);

        try {
            AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                try (
                    Transaction ignored = ignite0.transactions().withLabel("test")
                        .txStart(PESSIMISTIC, REPEATABLE_READ)
                ) {
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                    txBlockLatch.countDown();
                    txUnblockLatch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, 5, "sys-view-cmd-tx-test-1");

            GridTestUtils.runMultiThreadedAsync(() -> {
                try (Transaction ignored = ignite0.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                    txBlockLatch.countDown();
                    txUnblockLatch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, 5, "sys-view-cmd-tx-test-2");

            txBlockLatch.await(getTestTimeout(), MILLISECONDS);

            List<List<String>> txsView = systemView(ignite0, TXS_MON_LIST);

            assertEquals(10, txsView.size());
        }
        finally {
            txUnblockLatch.countDown();
        }

        assertTrue(waitForCondition(() -> systemView(ignite0, TXS_MON_LIST).isEmpty(), getTestTimeout()));
    }

    /** */
    @Test
    public void testSchemas() throws Exception {
        try (
            IgniteEx g = startGrid(getConfiguration(getTestIgniteInstanceName(3))
                .setSqlConfiguration(new SqlConfiguration().setSqlSchemas("MY_SCHEMA", "ANOTHER_SCHEMA")))
        ) {
            Set<String> schemaNames = new HashSet<>();

            HashSet<String> expSchemas = new HashSet<>(asList("MY_SCHEMA", "ANOTHER_SCHEMA", "SYS", "PUBLIC"));

            List<List<String>> sqlSchemaView = systemView(g, SQL_SCHEMA_VIEW);

            sqlSchemaView.forEach(row -> schemaNames.add(row.get(0))); // name

            assertEquals(schemaNames, expSchemas);
        }
    }

    /** */
    @Test
    public void testViews() {
        Set<String> expViewNames = new HashSet<>(asList(
            "METRICS",
            "VIEWS",
            "SERVICES",
            "CACHE_GROUPS",
            "CACHES",
            "TASKS",
            "JOBS",
            "SQL_QUERIES_HISTORY",
            "NODES",
            "SCHEMAS",
            "NODE_METRICS",
            "BASELINE_NODES",
            "BASELINE_NODE_ATTRIBUTES",
            "INDEXES",
            "LOCAL_CACHE_GROUPS_IO",
            "SQL_QUERIES",
            "SCAN_QUERIES",
            "SNAPSHOT",
            "NODE_ATTRIBUTES",
            "TABLES",
            "CLIENT_CONNECTIONS",
            "TABLE_COLUMNS",
            "VIEW_COLUMNS",
            "TRANSACTIONS",
            "CONTINUOUS_QUERIES",
            "STRIPED_THREADPOOL_QUEUE",
            "DATASTREAM_THREADPOOL_QUEUE",
            "DATA_REGION_PAGE_LISTS",
            "CACHE_GROUP_PAGE_LISTS",
            "PARTITION_STATES",
            "BINARY_METADATA",
            "METASTORAGE",
            "DISTRIBUTED_METASTORAGE",
            "STATISTICS_CONFIGURATION",
            "STATISTICS_PARTITION_DATA",
            "STATISTICS_LOCAL_DATA",
            "DS_ATOMICLONGS",
            "DS_ATOMICREFERENCES",
            "DS_ATOMICSTAMPED",
            "DS_ATOMICSEQUENCES",
            "DS_COUNTDOWNLATCHES",
            "DS_REENTRANTLOCKS",
            "DS_SETS",
            "DS_SEMAPHORES",
            "DS_QUEUES"
        ));

        Set<String> viewNames = new HashSet<>();

        List<List<String>> sqlViewsView = systemView(ignite0, SQL_VIEWS_VIEW);

        sqlViewsView.forEach(row -> viewNames.add(row.get(0))); // name

        assertEquals(expViewNames, viewNames);
    }

    /** */
    @Test
    public void testTable() {
        assertTrue(systemView(ignite0, SQL_TBLS_VIEW).isEmpty());

        executeSql(ignite0, "CREATE TABLE T1(ID LONG PRIMARY KEY, NAME VARCHAR)");

        List<List<String>> sqlTablesView = systemView(ignite0, SQL_TBLS_VIEW);

        assertEquals(1, sqlTablesView.size());

        List<String> row = sqlTablesView.get(0);

        assertEquals(Integer.toString(cacheId("SQL_PUBLIC_T1")), row.get(0)); // CACHE_GROUP_ID
        assertEquals("SQL_PUBLIC_T1", row.get(1)); // CACHE_GROUP_NAME
        assertEquals(Integer.toString(cacheId("SQL_PUBLIC_T1")), row.get(2)); // CACHE_ID
        assertEquals("SQL_PUBLIC_T1", row.get(3)); // CACHE_NAME
        assertEquals(DFLT_SCHEMA, row.get(4)); // SCHEMA_NAME
        assertEquals("T1", row.get(5)); // TABLE_NAME
        assertEquals("null", row.get(6)); // AFFINITY_KEY_COLUMN
        assertEquals("ID", row.get(7)); // KEY_ALIAS
        assertEquals("null", row.get(8)); // VALUE_ALIAS
        assertEquals("java.lang.Long", row.get(9)); // KEY_TYPE_NAME
        assertFalse("null".equals(row.get(10))); // VALUE_TYPE_NAME

        executeSql(ignite0, "CREATE TABLE T2(ID LONG PRIMARY KEY, NAME VARCHAR)");

        assertEquals(2, systemView(ignite0, SQL_TBLS_VIEW).size());

        executeSql(ignite0, "DROP TABLE T1");
        executeSql(ignite0, "DROP TABLE T2");

        assertTrue(systemView(ignite0, SQL_TBLS_VIEW).isEmpty());
    }

    /** */
    @Test
    public void testTableColumns() {
        assertTrue(systemView(ignite0, SQL_TBL_COLS_VIEW).isEmpty());

        executeSql(ignite0, "CREATE TABLE T1(ID LONG PRIMARY KEY, NAME VARCHAR(40))");

        Set<?> actCols = systemView(ignite0, SQL_TBL_COLS_VIEW).stream()
            .map(row -> row.get(0)) // columnName
            .collect(Collectors.toSet());

        assertEquals(new HashSet<>(asList("ID", "NAME", "_KEY", "_VAL")), actCols);

        executeSql(ignite0, "CREATE TABLE T2(ID LONG PRIMARY KEY, NAME VARCHAR(50))");

        Set<List<String>> expSqlTableColumnsView = new HashSet<>(asList(
            asList("ID", "T1", "PUBLIC", "false", "false", "null", "true", "true", "-1", "-1", Long.class.getName()),
            asList("NAME", "T1", "PUBLIC", "false", "false", "null", "true", "false", "40", "-1", String.class.getName()),
            asList("_KEY", "T1", "PUBLIC", "true", "false", "null", "false", "true", "-1", "-1", "null"),
            asList("_VAL", "T1", "PUBLIC", "false", "false", "null", "true", "false", "-1", "-1", "null"),
            asList("ID", "T2", "PUBLIC", "false", "false", "null", "true", "true", "-1", "-1", Long.class.getName()),
            asList("NAME", "T2", "PUBLIC", "false", "false", "null", "true", "false", "50", "-1", String.class.getName()),
            asList("_KEY", "T2", "PUBLIC", "true", "false", "null", "false", "true", "-1", "-1", "null"),
            asList("_VAL", "T2", "PUBLIC", "false", "false", "null", "true", "false", "-1", "-1", "null")
        ));

        Set<List<String>> sqlTableColumnsView = new HashSet<>(systemView(ignite0, SQL_TBL_COLS_VIEW));

        assertEquals(expSqlTableColumnsView, sqlTableColumnsView);

        executeSql(ignite0, "DROP TABLE T1");
        executeSql(ignite0, "DROP TABLE T2");

        assertTrue(systemView(ignite0, SQL_TBL_COLS_VIEW).isEmpty());
    }

    /** */
    @Test
    public void testViewColumns() {
        Set<List<String>> expsqlViewColumnsView = new HashSet<>(asList(
            asList("CONNECTION_ID", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", "true", "19", "0", Long.class.getName()),
            asList("LOCAL_ADDRESS", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", "true", Integer.toString(MAX_VALUE), "0",
                String.class.getName()),
            asList("REMOTE_ADDRESS", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", "true", Integer.toString(MAX_VALUE), "0",
                String.class.getName()),
            asList("TYPE", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", "true", Integer.toString(MAX_VALUE), "0",
                String.class.getName()),
            asList("USER", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", "true", Integer.toString(MAX_VALUE), "0",
                String.class.getName()),
            asList("VERSION", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", "true", Integer.toString(MAX_VALUE), "0",
                String.class.getName())
        ));

        Set<List<String>> sqlViewColumnsView = systemView(ignite0, SQL_VIEW_COLS_VIEW).stream()
            .filter(row -> "CLIENT_CONNECTIONS".equals(row.get(1))) // viewName
            .collect(Collectors.toSet());

        assertEquals(expsqlViewColumnsView, sqlViewColumnsView);
    }

    /** */
    @Test
    public void testContinuousQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite0.createCache("test-cache");

        assertTrue(systemView(ignite0, CQ_SYS_VIEW).isEmpty());
        assertTrue(systemView(ignite1, CQ_SYS_VIEW).isEmpty());

        try (QueryCursor<?> ignored = cache.query(new ContinuousQuery<>()
            .setInitialQuery(new ScanQuery<>())
            .setPageSize(100)
            .setTimeInterval(1000)
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilterFactory(() -> evt -> true)
        )) {
            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            checkContinuouQueryView(ignite0, true);
            checkContinuouQueryView(ignite1, false);
        }

        assertTrue(systemView(ignite0, CQ_SYS_VIEW).isEmpty());
        assertTrue(waitForCondition(() -> systemView(ignite1, CQ_SYS_VIEW).isEmpty(), getTestTimeout()));
    }

    /** */
    private void checkContinuouQueryView(IgniteEx node, boolean loc) {
        List<List<String>> cqSysView = systemView(node, CQ_SYS_VIEW);

        assertEquals(1, cqSysView.size());

        List<String> row = cqSysView.get(0);

        assertEquals("test-cache", row.get(0)); //cacheName
        assertEquals("100", row.get(7)); // bufferSize
        assertEquals("1000", row.get(9)); // interval
        assertEquals(ignite0.localNode().id().toString(), row.get(14)); // nodeId

        if (loc)
            assertTrue(row.get(1).startsWith(getClass().getName())); // localListener
        else
            assertEquals("null", row.get(1)); // localListener

        assertTrue(row.get(2).startsWith(getClass().getName())); // remoteFilter
        assertEquals("null", row.get(4)); // localTransformedListener
        assertEquals("null", row.get(3)); // remoteTransformer
    }

    /** */
    @Test
    public void testLocalScanQuery() throws Exception {
        IgniteCache<Integer, Integer> cache1 = ignite0.createCache(
            new CacheConfiguration<Integer, Integer>("cache1").setGroupName("group1"));

        int part = ignite0.affinity("cache1").primaryPartitions(ignite0.localNode())[0];

        List<Integer> partKeys = partitionKeys(cache1, part, 11, 0);

        partKeys.forEach(key -> cache1.put(key, key));

        assertTrue(systemView(ignite0, SCAN_QRY_SYS_VIEW).isEmpty());

        try (
            QueryCursor<Integer> qryRes1 = cache1.query(
                new ScanQuery<Integer, Integer>()
                    .setFilter(new TestPredicate())
                    .setLocal(true)
                    .setPartition(part)
                    .setPageSize(10),
                new TestTransformer())
        ) {
            assertTrue(qryRes1.iterator().hasNext());

            assertTrue(waitForCondition(() -> !systemView(ignite0, SCAN_QRY_SYS_VIEW).isEmpty(), getTestTimeout()));

            List<String> view = systemView(ignite0, SCAN_QRY_SYS_VIEW).get(0);

            assertEquals(ignite0.localNode().id().toString(), view.get(0)); // originNodeId
            assertEquals("0", view.get(1)); // queryId
            assertEquals("cache1", view.get(2)); // cacheName
            assertEquals(Integer.toString(cacheId("cache1")), view.get(3)); // cacheId
            assertEquals(Integer.toString(cacheGroupId("cache1", "group1")), view.get(4)); // cacheGroupId
            assertEquals("group1", view.get(5)); // cacheGroupName
            assertTrue(Long.parseLong(view.get(6)) <= System.currentTimeMillis()); // startTime
            assertTrue(Long.parseLong(view.get(7)) >= 0); // duration
            assertFalse(Boolean.parseBoolean(view.get(8))); // canceled
            assertEquals(TEST_PREDICATE, view.get(9)); // filter
            assertTrue(Boolean.parseBoolean(view.get(11))); // local
            assertEquals(part, Integer.parseInt(view.get(13))); // partition
            assertEquals(toStringSafe(ignite0.context().discovery().topologyVersionEx()), view.get(16)); // topology
            assertEquals(TEST_TRANSFORMER, view.get(17)); // transformer
            assertFalse(Boolean.parseBoolean(view.get(10))); // keepBinary
            assertEquals("null", view.get(14)); // subjectId
            assertEquals("null", view.get(15)); // taskName
        }

        assertTrue(waitForCondition(() -> systemView(ignite0, SCAN_QRY_SYS_VIEW).isEmpty(), getTestTimeout()));
    }

    /** */
    @Test
    public void testScanQuery() throws Exception {
        try (
            IgniteEx client1 = startClientGrid("client-1");
            IgniteEx client2 = startClientGrid("client-2")
        ) {
            IgniteCache<Integer, Integer> cache1 = client1.createCache(
                new CacheConfiguration<Integer, Integer>("cache1").setGroupName("group1"));

            IgniteCache<Integer, Integer> cache2 = client2.createCache("cache2");

            for (int i = 0; i < 100; i++) {
                cache1.put(i, i);
                cache2.put(i, i);
            }

            assertTrue(systemView(ignite0, SCAN_QRY_SYS_VIEW).isEmpty());
            assertTrue(systemView(ignite1, SCAN_QRY_SYS_VIEW).isEmpty());

            try (
                QueryCursor<Integer> qryRes1 = cache1.query(
                    new ScanQuery<Integer, Integer>()
                        .setFilter(new TestPredicate())
                        .setPageSize(10),
                    new TestTransformer());

                QueryCursor<?> qryRes2 = cache2.withKeepBinary().query(new ScanQuery<>().setPageSize(20))
            ) {
                assertTrue(qryRes1.iterator().hasNext());
                assertTrue(qryRes2.iterator().hasNext());

                checkScanQueryView(client1, client2, ignite0);
                checkScanQueryView(client1, client2, ignite1);
            }

            assertTrue(waitForCondition(() ->
                systemView(ignite0, SCAN_QRY_SYS_VIEW).isEmpty() && systemView(ignite1, SCAN_QRY_SYS_VIEW).isEmpty(),
                getTestTimeout()));
        }
    }

    /** */
    private void checkScanQueryView(IgniteEx client1, IgniteEx client2, IgniteEx node) throws Exception {
        assertTrue(waitForCondition(() -> systemView(ignite0, SCAN_QRY_SYS_VIEW).size() > 1, getTestTimeout()));

        Consumer<List<String>> cache1checker = view -> {
            assertEquals(client1.localNode().id().toString(), view.get(0));
            assertTrue(Long.parseLong(view.get(1)) != 0);
            assertEquals("cache1", view.get(2));
            assertEquals(Integer.toString(cacheId("cache1")), view.get(3));
            assertEquals(Integer.toString(cacheGroupId("cache1", "group1")), view.get(4));
            assertEquals("group1", view.get(5));
            assertTrue(Long.parseLong(view.get(6)) <= System.currentTimeMillis());
            assertTrue(Long.parseLong(view.get(7)) >= 0);
            assertFalse(Boolean.parseBoolean(view.get(8)));
            assertEquals(TEST_PREDICATE, view.get(9));
            assertFalse(Boolean.parseBoolean(view.get(11)));
            assertEquals("-1", view.get(13));
            assertEquals(toStringSafe(client1.context().discovery().topologyVersionEx()), view.get(16));
            assertEquals(TEST_TRANSFORMER, view.get(17));
            assertFalse(Boolean.parseBoolean(view.get(10)));
            assertEquals("null", view.get(14));
            assertEquals("null", view.get(15));
            assertEquals("10", view.get(12));
        };

        Consumer<List<String>> cache2checker = view -> {
            assertEquals(client2.localNode().id().toString(), view.get(0));
            assertTrue(Long.parseLong(view.get(1)) != 0);
            assertEquals("cache2", view.get(2));
            assertEquals(Integer.toString(cacheId("cache2")), view.get(3));
            assertEquals(Integer.toString(cacheGroupId("cache2", null)), view.get(4));
            assertEquals("cache2", view.get(5));
            assertTrue(Long.parseLong(view.get(6)) <= System.currentTimeMillis());
            assertTrue(Long.parseLong(view.get(7)) >= 0);
            assertFalse(Boolean.parseBoolean(view.get(8)));
            assertEquals("null", view.get(9));
            assertFalse(Boolean.parseBoolean(view.get(11)));
            assertEquals("-1", view.get(13));
            assertEquals(toStringSafe(client2.context().discovery().topologyVersionEx()), view.get(16));
            assertEquals("null", view.get(17));
            assertTrue(Boolean.parseBoolean(view.get(10)));
            assertEquals("null", view.get(14));
            assertEquals("null", view.get(15));
            assertEquals("20", view.get(12));
        };

        boolean found1 = false;
        boolean found2 = false;

        for (List<String> row : systemView(node, SCAN_QRY_SYS_VIEW)) {
            if ("cache2".equals(row.get(2))) {
                cache2checker.accept(row);
                found1 = true;
            }
            else {
                cache1checker.accept(row);
                found2 = true;
            }
        }

        assertTrue(found1 && found2);
    }

    /** */
    @Test
    public void testStripedExecutor() throws Exception {
        checkStripeExecutorView(ignite0.context().pools().getStripedExecutorService(),
            SYS_POOL_QUEUE_VIEW,
            "sys");
    }

    /** */
    @Test
    public void testStreamerExecutor() throws Exception {
        checkStripeExecutorView(ignite0.context().pools().getDataStreamerExecutorService(),
            STREAM_POOL_QUEUE_VIEW,
            "data-streamer");
    }

    /**
     * Checks striped executor system view.
     *
     * @param execSvc Striped executor.
     * @param view System view name.
     * @param poolName Executor name.
     */
    private void checkStripeExecutorView(StripedExecutor execSvc, String view, String poolName) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        execSvc.execute(0, new TestRunnable(latch, 0));
        execSvc.execute(0, new TestRunnable(latch, 1));
        execSvc.execute(1, new TestRunnable(latch, 2));
        execSvc.execute(1, new TestRunnable(latch, 3));

        try {
            assertTrue(waitForCondition(() -> systemView(ignite0, view).size() == 2, getTestTimeout()));

            List<List<String>> stripedQueue = systemView(ignite0, view);

            List<String> row0 = stripedQueue.get(0);

            assertEquals("0", row0.get(0));
            assertEquals(TestRunnable.class.getSimpleName() + '1', row0.get(1));
            assertEquals(poolName + "-stripe-0", row0.get(2));
            assertEquals(TestRunnable.class.getName(), row0.get(3));

            List<String> row1 = stripedQueue.get(1);

            assertEquals("1", row1.get(0));
            assertEquals(TestRunnable.class.getSimpleName() + '3', row1.get(1));
            assertEquals(poolName + "-stripe-1", row1.get(2));
            assertEquals(TestRunnable.class.getName(), row1.get(3));
        }
        finally {
            latch.countDown();
        }
    }

    /** */
    @Test
    public void testPagesList() throws Exception {
        String cacheName = "cacheFL";

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(new CacheConfiguration<Integer, byte[]>()
            .setName(cacheName)
            .setAffinity(new RendezvousAffinityFunction()
                .setPartitions(1)));

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite0.context().cache().context()
            .database();

        int pageSize = dbMgr.pageSize();

        try {
            dbMgr.enableCheckpoints(false).get();

            int key = 0;

            // Fill up different free-list buckets.
            for (int j = 0; j < pageSize / 2; j++)
                cache.put(key++, new byte[j + 1]);

            // Put some pages to one bucket to overflow pages cache.
            for (int j = 0; j < 1000; j++)
                cache.put(key++, new byte[pageSize / 2]);

            List<List<String>> cacheGrpView = systemView(ignite0, CACHE_GRP_PAGE_LIST_VIEW);

            List<List<String>> dataRegionView = systemView(ignite0, DATA_REGION_PAGE_LIST_VIEW);

            String cacheId = Integer.toString(cacheId(cacheName));

            // Test filtering by 3 columns.
            assertFalse(cacheGrpView.stream().noneMatch(row ->
                cacheId.equals(row.get(0)) &&
                "0".equals(row.get(1)) &&
                "0".equals(row.get(3))));

            // Test filtering with invalid cache group id.
            assertTrue(cacheGrpView.stream().noneMatch(row -> "-1".equals(row.get(0))));

            // Test filtering with invalid partition id.
            assertTrue(cacheGrpView.stream().noneMatch(row -> "-1".equals(row.get(1))));

            // Test filtering with invalid bucket number.
            assertTrue(cacheGrpView.stream().noneMatch(row -> "-1".equals(row.get(3))));

            assertFalse(cacheGrpView.stream().noneMatch(row ->
                Integer.parseInt(row.get(4)) > 0 && cacheId.equals(row.get(0))));

            assertFalse(cacheGrpView.stream().noneMatch(row ->
                Integer.parseInt(row.get(5)) > 0 && cacheId.equals(row.get(0))));

            assertFalse(cacheGrpView.stream().noneMatch(row ->
                Integer.parseInt(row.get(6)) > 0 && cacheId.equals(row.get(0))));

            assertFalse(dataRegionView.stream().noneMatch(row -> row.get(0).startsWith(DATA_REGION_NAME)));

            assertTrue(dataRegionView.stream().noneMatch(row ->
                row.get(0).startsWith(DATA_REGION_NAME) && Integer.parseInt(row.get(4)) > 0));
        }
        finally {
            dbMgr.enableCheckpoints(true).get();
        }

        ignite0.cluster().state(INACTIVE);

        ignite0.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cacheInMemory = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>()
                .setName("cacheFLInMemory")
                .setDataRegionName(DATA_REGION_NAME));

        cacheInMemory.put(0, 0);

        // After activation/deactivation new view for data region pages lists should be created, check that new view
        // correctly reflects changes in free-lists.
        assertTrue(systemView(ignite0, DATA_REGION_PAGE_LIST_VIEW).stream().noneMatch(row ->
            row.get(0).startsWith(DATA_REGION_NAME) && Integer.parseInt(row.get(4)) > 0));
    }

    /** */
    @Test
    public void testPartitionStates() throws Exception {
        String nodeName0 = getTestIgniteInstanceName(0);
        String nodeName1 = getTestIgniteInstanceName(1);
        String nodeName2 = getTestIgniteInstanceName(2);

        IgniteCache<Integer, Integer> cache1 = ignite0.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("cache1")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new TestAffinityFunction(new String[][] {{nodeName0, nodeName1}, {nodeName1, nodeName2},
                {nodeName2, nodeName0}})));

        IgniteCache<Integer, Integer> cache2 = ignite0.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("cache2")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new TestAffinityFunction(new String[][] {{nodeName0, nodeName1, nodeName2}, {nodeName1}})));

        for (int i = 0; i < 100; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        try (IgniteEx ignite2 = startGrid(nodeName2)) {
            ignite2.rebalanceEnabled(false);

            ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

            String nodeId0 = ignite0.cluster().localNode().id().toString();
            String nodeId1 = ignite(1).cluster().localNode().id().toString();
            String nodeId2 = ignite2.cluster().localNode().id().toString();

            String cacheGrpId1 = Integer.toString(ignite0.cachex("cache1").context().groupId());
            String cacheGrpId2 = Integer.toString(ignite0.cachex("cache2").context().groupId());

            String owningState = GridDhtPartitionState.OWNING.name();
            String movingState = GridDhtPartitionState.MOVING.name();

            for (int i = 0; i < 3; i++) {
                List<List<String>> partStatesView = systemView(ignite(i), PART_STATES_VIEW);

                // Check partitions for cache1.
                checkPartitionStatesView(partStatesView, owningState, true, cacheGrpId1, nodeId0, 0);
                checkPartitionStatesView(partStatesView, owningState, false, cacheGrpId1, nodeId1, 0);
                checkPartitionStatesView(partStatesView, owningState, true, cacheGrpId1, nodeId1, 1);
                checkPartitionStatesView(partStatesView, movingState, false, cacheGrpId1, nodeId2, 1);
                checkPartitionStatesView(partStatesView, owningState, true, cacheGrpId1, nodeId0, 2);
                checkPartitionStatesView(partStatesView, movingState, false, cacheGrpId1, nodeId2, 2);

                checkPartitionStatesView(partStatesView, owningState, true, cacheGrpId2, nodeId0, 0);
                checkPartitionStatesView(partStatesView, owningState, false, cacheGrpId2, nodeId1, 0);
                checkPartitionStatesView(partStatesView, movingState, false, cacheGrpId2, nodeId2, 0);
                checkPartitionStatesView(partStatesView, owningState, true, cacheGrpId2, nodeId1, 1);
            }

            AffinityTopologyVersion topVer = ignite0.context().discovery().topologyVersionEx();

            ignite2.rebalanceEnabled(true);

            // Wait until rebalance complete.
            assertTrue(waitForCondition(() ->
                ignite0.context().discovery().topologyVersionEx().compareTo(topVer) > 0, getTestTimeout()));

            for (int i = 0; i < 3; i++) {
                List<List<String>> rows = systemView(ignite(i), PART_STATES_VIEW);

                assertEquals(10, rows.stream().filter(row ->
                    (cacheGrpId1.equals(row.get(0)) || cacheGrpId2.equals(row.get(0))) && owningState.equals(row.get(3))).count());

                assertTrue(rows.stream().noneMatch(row ->
                    (cacheGrpId1.equals(row.get(0)) || cacheGrpId2.equals(row.get(0))) && movingState.equals(row.get(3))));
            }

            // Check that assignment is now changed to ideal.
            for (int i = 0; i < 3; i++) {
                List<List<String>> rows = systemView(ignite(i), PART_STATES_VIEW);

                checkPartitionStatesView(rows, owningState, false, cacheGrpId1, nodeId0, 2);
                checkPartitionStatesView(rows, owningState, true, cacheGrpId1, nodeId2, 2);
            }
        }
        finally {
            ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());
        }
    }

    /**
     * Checks that partitions stated view contains row with specified parameters.
     *
     * @param partStatesView Partition states view rows.
     * @param state Partition state.
     * @param isPrimary Primary falg.
     * @param cacheGrpId Cache group id.
     * @param nodeId Node id.
     * @param partitionId partition id.
     */
    private void checkPartitionStatesView(
        List<List<String>> partStatesView,
        String state,
        boolean isPrimary,
        String cacheGrpId,
        String nodeId,
        int partitionId
    ) {
        assertEquals(1, partStatesView.stream().filter(row ->
            cacheGrpId.equals(row.get(0)) &&
                nodeId.equals(row.get(1)) &&
                Integer.toString(partitionId).equals(row.get(2)) &&
                state.equals(row.get(3)) &&
                Boolean.toString(isPrimary).equals(row.get(4))
        ).count());
    }

    /** */
    @Test
    public void testBinaryMeta() {
        IgniteCache<Integer, TestObjectAllTypes> c1 = ignite0.createCache("test-cache");
        IgniteCache<Integer, TestObjectEnum> c2 = ignite0.createCache("test-enum-cache");

        executeSql(ignite0, "CREATE TABLE T1(ID LONG PRIMARY KEY, NAME VARCHAR(40), ACCOUNT BIGINT)");
        executeSql(ignite0, "INSERT INTO T1(ID, NAME, ACCOUNT) VALUES(1, 'test', 1)");

        c1.put(1, new TestObjectAllTypes());
        c2.put(1, TestObjectEnum.A);

        List<List<String>> binaryMetaView = systemView(ignite0, BINARY_METADATA_VIEW);

        assertEquals(3, binaryMetaView.size());

        for (List<String> row : binaryMetaView) {
            if (Objects.equals(TestObjectEnum.class.getName(), row.get(1))) {
                assertTrue(Boolean.parseBoolean(row.get(6)));

                assertEquals("0", row.get(3));
            }
            else if (Objects.equals(TestObjectAllTypes.class.getName(), row.get(1))) {
                assertFalse(Boolean.parseBoolean(row.get(6)));

                Field[] fields = TestObjectAllTypes.class.getDeclaredFields();

                assertEquals(Integer.toString(fields.length), row.get(3));

                for (Field field : fields)
                    assertTrue(row.get(4).contains(field.getName()));
            }
            else {
                assertFalse(Boolean.parseBoolean(row.get(6)));

                assertEquals("2", row.get(3));

                assertTrue(row.get(4).contains("NAME"));
                assertTrue(row.get(4).contains("ACCOUNT"));
            }
        }
    }

    /** */
    @Test
    public void testMetastorage() throws Exception {
        IgniteCacheDatabaseSharedManager db = ignite0.context().cache().context().database();

        String name = "test-key";
        String val = "test-value";

        db.checkpointReadLock();

        try {
            db.metaStorage().write(name, val);
        }
        finally {
            db.checkpointReadUnlock();
        }

        assertEquals(1, systemView(ignite0, METASTORE_VIEW).stream()
            .filter(row -> name.equals(row.get(0)) && val.equals(row.get(1)))
            .count());
    }

    /** */
    @Test
    public void testDistributedMetastorage() throws Exception {
        DistributedMetaStorage dms = ignite0.context().distributedMetastorage();

        String name = "test-distributed-key";
        String val = "test-distributed-value";

        dms.write(name, val);

        assertEquals(1, systemView(ignite0, DISTRIBUTED_METASTORE_VIEW).stream()
            .filter(row -> name.equals(row.get(0)) && val.equals(row.get(1)))
            .count());

        assertTrue(waitForCondition(() -> systemView(ignite1, DISTRIBUTED_METASTORE_VIEW).stream()
                .filter(row -> name.equals(row.get(0)) && val.equals(row.get(1)))
                .count() == 1,
            getTestTimeout()));
    }

    /** */
    @Test
    public void testSnapshotView() throws Exception {
        int srvCnt = ignite0.cluster().forServers().nodes().size();

        String snap0 = "testSnapshot0";

        ignite0.snapshot().createSnapshot(snap0).get();

        assertEquals(srvCnt, systemView(ignite0, SNAPSHOT_SYS_VIEW).size());
    }

    /**
     * Execute query on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    private List<List<?>> executeSql(Ignite node, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        return queryProcessor(node).querySqlFields(qry, true).getAll();
    }

    /**
     * Gets system view content via control utility from specified node. Here we also check if attributes names
     * returned by the command match the real ones. And that both "SQL" and "Java" command names styles are supported.
     *
     * @param node The node to obtain system view from.
     * @param sysViewName Name of the system view which content is required.
     * @return Content of the requested system view.
     */
    private List<List<String>> systemView(IgniteEx node, String sysViewName) {
        List<String> attrNames = new ArrayList<>();

        SystemView<?> sysView = node.context().systemView().view(sysViewName);

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                attrNames.add(name);
            }
        });

        String nodeId = node.context().discovery().localNode().id().toString();

        List<List<String>> rows = parseSystemViewCommandOutput(
            executeCommand(EXIT_CODE_OK, CMD_SYS_VIEW, toSqlName(sysViewName), NODE_ID.argName(), nodeId));

        assertEquals(attrNames, rows.get(0));

        rows = parseSystemViewCommandOutput(
            executeCommand(EXIT_CODE_OK, CMD_SYS_VIEW, toSqlName(sysViewName).toLowerCase(), NODE_ID.argName(), nodeId));

        assertEquals(attrNames, rows.get(0));

        rows = parseSystemViewCommandOutput(
            executeCommand(EXIT_CODE_OK, CMD_SYS_VIEW, sysViewName, NODE_ID.argName(), nodeId));

        assertEquals(attrNames, rows.remove(0));

        int attrsCnt = sysView.walker().count();

        rows.forEach(row -> assertEquals(attrsCnt, row.size()));

        return rows;
    }

    /**
     * Obtains system view values for each row from command output.
     *
     * @param out Command output to parse.
     * @return System view values.
     */
    private List<List<String>> parseSystemViewCommandOutput(String out) {
        String outStart = "--------------------------------------------------------------------------------";

        String outEnd = "Command [" + SYSTEM_VIEW.toCommandName() + "] finished with code: " + EXIT_CODE_OK;

        List<String> rows = Arrays.asList(out.substring(
            out.indexOf(outStart) + outStart.length() + 1,
            out.indexOf(outEnd) - 1
        ).split(U.nl()));

        return rows.stream().map(row ->
            Arrays.stream(row.split(quote(COLUMN_SEPARATOR)))
                .map(String::trim)
                .filter(str -> !str.isEmpty())
                .collect(Collectors.toList()))
            .collect(Collectors.toList());
    }

    /**
     * Executes command and checks its exit code.
     *
     * @param expExitCode Expected exit code.
     * @param args Command lines arguments.
     * @return Result of command execution.
     */
    private String executeCommand(int expExitCode, String... args) {
        int res = execute(args);

        assertEquals(expExitCode, res);

        return testOut.toString();
    }
}
