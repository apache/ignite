/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.InlineIndexTreeFactory;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.NoopRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineRecommender;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.idxTreeFactory;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests case when long index deletion operation happens.
 */
@WithSystemProperty(key = IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT, value = "5000")
public class LongDestroyDurableBackgroundTaskTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_COUNT = 2;

    /** Number of node that can be restarted during test, if test scenario requires it. */
    private static final int RESTARTED_NODE_NUM = 0;

    /** Number of node that is always alive during tests. */
    private static final int ALWAYS_ALIVE_NODE_NUM = 1;

    /** We imitate long index destroy in these tests, so this is delay for each page to destroy. */
    private static final long TIME_FOR_EACH_INDEX_PAGE_TO_DESTROY = 300;

    /** Index name. */
    private static final String IDX_NAME = "T_IDX";

    /** */
    private final LogListener blockedSysCriticalThreadLsnr =
        LogListener.matches("Blocked system-critical thread has been detected. " +
            "This can lead to cluster-wide undefined behaviour [workerName=db-checkpoint-thread").build();

    /** Latch that waits for execution of durable background task. */
    private CountDownLatch pendingDelLatch;

    /** Latch that waits for indexes rebuilding. */
    private CountDownLatch idxsRebuildLatch;

    /** */
    private final LogListener pendingDelFinishedLsnr = new CallbackExecutorLogListener(
        ".*?Execution of durable background task completed.*",
        () -> pendingDelLatch.countDown()
    );

    /** */
    private final LogListener idxsRebuildFinishedLsnr = new CallbackExecutorLogListener(
        "Indexes rebuilding completed for all caches.",
        () -> idxsRebuildLatch.countDown()
    );

    /** */
    private final LogListener taskLifecycleLsnr =
        new MessageOrderLogListener(
            ".*?Executing durable background task: drop-sql-index-SQL_PUBLIC_T-" + IDX_NAME + "-.*",
            ".*?Could not execute durable background task: drop-sql-index-SQL_PUBLIC_T-" + IDX_NAME + "-.*",
            ".*?Executing durable background task: drop-sql-index-SQL_PUBLIC_T-" + IDX_NAME + "-.*",
            ".*?Execution of durable background task completed: drop-sql-index-SQL_PUBLIC_T-" + IDX_NAME + "-.*"
        );

    /**
     * When it is set to true during index deletion, node with number {@link #RESTARTED_NODE_NUM} fails to complete
     * deletion.
     */
    private final AtomicBoolean blockDestroy = new AtomicBoolean(false);

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(
        log(),
        blockedSysCriticalThreadLsnr,
        pendingDelFinishedLsnr,
        idxsRebuildFinishedLsnr,
        taskLifecycleLsnr
    );

    /** */
    private DurableBackgroundTaskTestListener durableBackgroundTaskTestLsnr;

    /** Original {@link DurableBackgroundCleanupIndexTreeTaskV2#idxTreeFactory}. */
    private InlineIndexTreeFactory originalFactory;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setInitialSize(10 * 1024L * 1024L)
                            .setMaxSize(50 * 1024L * 1024L)
                    )
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration()
                            .setName("dr1")
                            .setPersistenceEnabled(false)
                    )
                    .setCheckpointFrequency(Long.MAX_VALUE / 2)
            )
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setBackups(1)
                    .setSqlSchema(DFLT_SCHEMA),
                new CacheConfiguration<Integer, Integer>("TEST")
                    .setSqlSchema(DFLT_SCHEMA)
                    .setBackups(1)
                    .setDataRegionName("dr1")
            )
            .setGridLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        blockedSysCriticalThreadLsnr.reset();

        pendingDelLatch = new CountDownLatch(1);
        idxsRebuildLatch = new CountDownLatch(1);

        originalFactory = idxTreeFactory;
        idxTreeFactory = new InlineIndexTreeFactoryEx();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        blockedSysCriticalThreadLsnr.reset();

        stopAllGrids();

        cleanPersistenceDir();

        durableBackgroundTaskTestLsnr = null;

        idxTreeFactory = originalFactory;
        originalFactory = null;

        super.afterTest();
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. Node can restart without fully deleted index tree.
     *
     * @param restart Whether do the restart of one node.
     * @param rebalance Whether add to topology one more node while the index is being deleted.
     * @param multicolumn Is index multicolumn.
     * @param checkWhenOneNodeStopped Whether try to check index and try to recreate it while one node with pending
     * task is stopped.
     * @param dropIdxWhenOneNodeStopped Whether drop index on alive nodes while one node with pending
     * task is stopped.
     * @throws Exception If failed.
     */
    private void testLongIndexDeletion(
        boolean restart,
        boolean rebalance,
        boolean multicolumn,
        boolean checkWhenOneNodeStopped,
        boolean dropIdxWhenOneNodeStopped
    ) throws Exception {
        // If not restart, then assume that index is always dropped.
        boolean dropIdxWhenOneNodeStopped0 = !restart || dropIdxWhenOneNodeStopped;

        int nodeCnt = NODES_COUNT;

        Ignite ignite = prepareAndPopulateCluster(nodeCnt, multicolumn, false);

        Ignite aliveNode = grid(ALWAYS_ALIVE_NODE_NUM);

        IgniteCache<Integer, Integer> cacheOnAliveNode = aliveNode.cache(DEFAULT_CACHE_NAME);

        if (rebalance) {
            startGrid(nodeCnt);

            nodeCnt++;

            Collection<ClusterNode> blt = IntStream.range(0, nodeCnt)
                .mapToObj(i -> grid(i).localNode())
                .collect(toList());

            ignite.cluster().setBaselineTopology(blt);
        }

        if (restart) {
            blockDestroy.set(true);

            stopGrid(RESTARTED_NODE_NUM, true);

            awaitPartitionMapExchange();

            checkSelectAndPlan(cacheOnAliveNode, false);

            if (checkWhenOneNodeStopped) {
                createIndex(cacheOnAliveNode, multicolumn);

                checkSelectAndPlan(cacheOnAliveNode, true);

                if (dropIdxWhenOneNodeStopped0)
                    query(cacheOnAliveNode, "drop index " + IDX_NAME);

                forceCheckpoint(aliveNode);

                aliveNode.cluster().state(ClusterState.INACTIVE);
            }

            ignite = startGrid(RESTARTED_NODE_NUM);

            awaitLatch(pendingDelLatch, "Test timed out: failed to await for durable background task completion.");

            awaitPartitionMapExchange();

            if (checkWhenOneNodeStopped) {
                ignite.cluster().state(ClusterState.ACTIVE);

                // If index was dropped, we need to wait it's rebuild on restarted node.
                if (!dropIdxWhenOneNodeStopped0)
                    awaitLatch(idxsRebuildLatch, "Failed to wait for indexes rebuilding.");
            }

            checkSelectAndPlan(cacheOnAliveNode, !dropIdxWhenOneNodeStopped0);
        }
        else
            awaitLatch(pendingDelLatch, "Test timed out: failed to await for durable background task completion.");

        IgniteCache<Integer, Integer> cache = grid(RESTARTED_NODE_NUM).cache(DEFAULT_CACHE_NAME);

        checkSelectAndPlan(cache, !dropIdxWhenOneNodeStopped0);
        checkSelectAndPlan(cacheOnAliveNode, !dropIdxWhenOneNodeStopped0);

        // Trying to recreate index if it was dropped.
        if (dropIdxWhenOneNodeStopped0)
            createIndex(cache, multicolumn);

        checkSelectAndPlan(cache, true);
        checkSelectAndPlan(cacheOnAliveNode, true);

        forceCheckpoint();

        validateIndexes(ignite);

        assertFalse(blockedSysCriticalThreadLsnr.check());
    }

    /**
     * Awaits for latch for 60 seconds and fails, if latch was not counted down.
     *
     * @param latch Latch.
     * @param failMsg Failure message.
     * @throws InterruptedException If waiting failed.
     */
    private void awaitLatch(CountDownLatch latch, String failMsg) throws InterruptedException {
        if (!latch.await(60, TimeUnit.SECONDS))
            fail(failMsg);
    }

    /**
     * Validates indexes on {@link #RESTARTED_NODE_NUM} and {@link #ALWAYS_ALIVE_NODE_NUM}.
     *
     * @param ignite Ignite instance.
     */
    private void validateIndexes(Ignite ignite) {
        Set<UUID> nodeIds = new HashSet<>();

        nodeIds.add(grid(RESTARTED_NODE_NUM).cluster().localNode().id());
        nodeIds.add(grid(ALWAYS_ALIVE_NODE_NUM).cluster().localNode().id());

        log.info("Doing indexes validation.");

        VisorValidateIndexesTaskArg taskArg =
            new VisorValidateIndexesTaskArg(Collections.singleton("SQL_PUBLIC_T"), nodeIds, 0, 1, true, true);

        VisorValidateIndexesTaskResult taskRes =
            ignite.compute().execute(VisorValidateIndexesTask.class.getName(), new VisorTaskArgument<>(nodeIds, taskArg, false));

        if (!taskRes.exceptions().isEmpty()) {
            for (Map.Entry<UUID, Exception> e : taskRes.exceptions().entrySet())
                log.error("Exception while validation indexes on node id=" + e.getKey().toString(), e.getValue());
        }

        for (Map.Entry<UUID, VisorValidateIndexesJobResult> nodeEntry : taskRes.results().entrySet()) {
            if (nodeEntry.getValue().hasIssues()) {
                log.error("Validate indexes issues had been found on node id=" + nodeEntry.getKey().toString());

                log.error("Integrity check failures: " + nodeEntry.getValue().integrityCheckFailures().size());

                nodeEntry.getValue().integrityCheckFailures().forEach(f -> log.error(f.toString()));

                logIssuesFromMap("Partition results", nodeEntry.getValue().partitionResult());

                logIssuesFromMap("Index validation issues", nodeEntry.getValue().indexResult());
            }
        }

        assertTrue(taskRes.exceptions().isEmpty());

        for (VisorValidateIndexesJobResult res : taskRes.results().values())
            assertFalse(res.hasIssues());
    }

    /**
     * Logs index validation issues.
     *
     * @param caption Caption of log messages.
     * @param map Map containing issues.
     */
    private void logIssuesFromMap(String caption, Map<?, ValidateIndexesPartitionResult> map) {
        List<String> partResIssues = new LinkedList<>();

        map.forEach((k, v) -> v.issues().forEach(vi -> partResIssues.add(k.toString() + ": " + vi.toString())));

        log.error(caption + ": " + partResIssues.size());

        partResIssues.forEach(r -> log.error(r));
    }

    /**
     * Checks that select from table "t" is successful and correctness of index usage.
     * Table should be already created.
     *
     * @param cache Cache.
     * @param idxShouldExist Should index exist or not.
     */
    private void checkSelectAndPlan(IgniteCache<Integer, Integer> cache, boolean idxShouldExist) {
        // Ensure that index is not used after it was dropped.
        String plan = query(cache, "explain select id, p from t where p = 0")
            .get(0).get(0).toString();

        assertEquals(plan, idxShouldExist, plan.toUpperCase().contains(IDX_NAME));

        // Trying to do a select.
        String val = query(cache, "select p from t where p = 100").get(0).get(0).toString();

        assertEquals("100", val);
    }

    /**
     * Creates index on table "t", which should be already created.
     *
     * @param cache Cache.
     * @param multicolumn Whether index is multicolumn.
     */
    private void createIndex(IgniteCache<Integer, Integer> cache, boolean multicolumn) {
        query(cache, "create index " + IDX_NAME + " on t (p" + (multicolumn ? ", f)" : ")"));
    }

    /**
     * Does single query.
     *
     * @param cache Cache.
     * @param qry Query.
     * @return Query result.
     */
    private List<List<?>> query(IgniteCache<Integer, Integer> cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /**
     * Does parametrized query.
     *
     * @param cache Cache.
     * @param qry Query.
     * @param args Arguments.
     * @return Query result.
     */
    private List<List<?>> query(IgniteCache<Integer, Integer> cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /**
     * Argument list for batch insert.
     *
     * @param batchSize Batch size.
     * @param argCnt Arguments count.
     * @return List of batch arguments.
     */
    private List<Object[]> batchInsertArgs(int batchSize, int argCnt) {
        List<Object[]> batchArgs = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            Object[] args = new Object[argCnt];

            for (int j = 0; j < argCnt; j++)
                args[j] = i;

            batchArgs.add(args);
        }

        return batchArgs;
    }

    /**
     * Batch query.
     *
     * @param ignite Ignite instance.
     * @param qry SQL query.
     * @param batchArgs Batch arguments.
     * @throws Exception If failed.
     */
    private void batchQuery(Ignite ignite, String qry, List<Object[]> batchArgs) throws Exception {
        String host = ignite.configuration().getLocalHost();

        int port = ignite.configuration().getClientConnectorConfiguration().getPort();

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
            try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {
                PreparedStatement statement = conn.prepareStatement(qry);

                for (Object[] args : batchArgs) {
                    for (int i = 1; i <= args.length; i++)
                        statement.setObject(i, args[i - 1]);

                    statement.addBatch();

                    statement.clearParameters();
                }

                statement.executeBatch();
            }
        }
    }

    /**
     * Starts cluster and populates with data.
     *
     * @param nodeCnt Nodes count.
     * @param multicolumn Is index multicolumn.
     * @return Ignite instance.
     * @throws Exception If failed.
     */
    private IgniteEx prepareAndPopulateCluster(int nodeCnt, boolean multicolumn, boolean createLsnr) throws Exception {
        IgniteEx ignite = startGrids(nodeCnt);

        if (createLsnr) {
            GridCacheSharedContext ctx = ignite.context().cache().context();

            durableBackgroundTaskTestLsnr = new DurableBackgroundTaskTestListener(ctx.database().metaStorage());

            ((GridCacheDatabaseSharedManager)ctx.cache().context().database())
                    .addCheckpointListener(durableBackgroundTaskTestLsnr);
        }

        ignite.cluster().state(ACTIVE);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        query(cache, "create table t (id integer primary key, p integer, f integer) with \"BACKUPS=1\"");

        createIndex(cache, multicolumn);

        batchQuery(ignite, "insert into t (id, p, f) values (?, ?, ?)", batchInsertArgs(5_000, 3));

        forceCheckpoint();

        checkSelectAndPlan(cache, true);

        final IgniteCache<Integer, Integer> finalCache = cache;

        new Thread(() -> finalCache.query(new SqlFieldsQuery("drop index " + IDX_NAME)).getAll()).start();

        // Waiting for some modified pages
        doSleep(500);

        // Now checkpoint will happen during index deletion before it completes.
        forceCheckpoint();

        return ignite;
    }

    /**
     * Test case when cluster deactivation happens with no-persistence cache. Index tree deletion task should not be
     * started after stopping cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDeactivationShouldPassWithoutErrors() throws Exception {
        IgniteEx ignite = startGrids(NODES_COUNT);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache("TEST");

        query(cache, "create table TEST (id integer primary key, p integer, f integer) with " +
            "\"DATA_REGION=dr1\"");

        query(cache, "create index TEST_IDX on TEST (p)");

        for (int i = 0; i < 5_000; i++)
            query(cache, "insert into TEST (id, p, f) values (?, ?, ?)", i, i, i);

        LogListener lsnr = LogListener.matches("Could not execute durable background task").build();
        LogListener lsnr2 = LogListener.matches("Executing durable background task").build();
        LogListener lsnr3 = LogListener.matches("Execution of durable background task completed").build();

        testLog.registerAllListeners(lsnr, lsnr2, lsnr3);

        ignite.cluster().state(ClusterState.INACTIVE);

        doSleep(1_000);

        assertFalse(lsnr.check());
        assertFalse(lsnr2.check());
        assertFalse(lsnr3.check());

        testLog.unregisterListener(lsnr);
        testLog.unregisterListener(lsnr2);
        testLog.unregisterListener(lsnr3);

        for (int i = 0; i < NODES_COUNT; i++)
            grid(i);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionSimple() throws Exception {
        testLongIndexDeletion(false, false, false, false, true);
    }

    /**
     * Tests case when long multicolumn index deletion operation happens. Checkpoint should run in the middle
     * of index deletion operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongMulticolumnIndexDeletion() throws Exception {
        testLongIndexDeletion(false, false, true, false, true);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After checkpoint node should restart without fully deleted index tree.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionWithRestart() throws Exception {
        testLongIndexDeletion(true, false, false, false, true);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After deletion start one more node should be included in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionWithRebalance() throws Exception {
        testLongIndexDeletion(false, true, false, false, true);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After checkpoint node should restart without fully deleted index tree. While node is stopped,
     * we should check index and try to recreate it and do not drop again.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionCheckWhenOneNodeStopped() throws Exception {
        testLongIndexDeletion(true, false, false, true, false);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After checkpoint node should restart without fully deleted index tree. While node is stopped,
     * we should check index and try to recreate it and then drop again.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionCheckWhenOneNodeStoppedAndDropIndex() throws Exception {
        testLongIndexDeletion(true, false, false, true, true);
    }

    /**
     * Tests that local task lifecycle was correct.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyTaskLifecycle() throws Exception {
        taskLifecycleLsnr.reset();

        IgniteEx ignite = prepareAndPopulateCluster(1, false, false);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        checkSelectAndPlan(cache, false);

        ignite.cluster().state(INACTIVE);

        ignite.cluster().state(ACTIVE);

        ignite.cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

        blockDestroy.set(true);

        stopGrid(RESTARTED_NODE_NUM);

        blockDestroy.set(false);

        startGrid(RESTARTED_NODE_NUM);

        awaitLatch(pendingDelLatch, "Test timed out: failed to await for durable background task completion.");

        assertTrue(taskLifecycleLsnr.check());
    }

    /**
     * Tests that index is correctly deleted when corresponding SQL table is deleted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveIndexesOnTableDrop() throws Exception {
        IgniteEx ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        query(cache, "create table t1 (id integer primary key, p integer, f integer) " +
            "with \"BACKUPS=1, CACHE_GROUP=grp_test_table\"");

        query(cache, "create table t2 (id integer primary key, p integer, f integer) " +
            "with \"BACKUPS=1, CACHE_GROUP=grp_test_table\"");

        query(cache, "create index t2_idx on t2 (p)");

        batchQuery(ignite, "insert into t2 (id, p, f) values (?, ?, ?)", batchInsertArgs(5_000, 3));

        forceCheckpoint();

        CountDownLatch inxDelInAsyncTaskLatch = new CountDownLatch(1);

        LogListener lsnr = new CallbackExecutorLogListener(
            ".*?Execution of durable background task completed: drop-sql-index-SQL_PUBLIC_T2-T2_IDX-.*",
            inxDelInAsyncTaskLatch::countDown
        );

        testLog.registerListener(lsnr);

        ignite.destroyCache("SQL_PUBLIC_T2");

        awaitLatch(
            inxDelInAsyncTaskLatch,
            "Failed to await for index deletion in async task " +
                "(either index failed to delete in 1 minute or async task not started)"
        );
    }

    /**
     * Tests that completed task removed from metastorage in the ending of next checkpoint.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexDeletionTaskRemovedAfterCheckpointFinished() throws Exception {
        prepareAndPopulateCluster(1, false, true);

        assertFalse(durableBackgroundTaskTestLsnr.check());

        awaitLatch(pendingDelLatch, "Test timed out: failed to await for durable background task completion.");

        forceCheckpoint();

        assertTrue(durableBackgroundTaskTestLsnr.check());
    }

    /**
     * Checking the converting of the old problem into the new one.
     */
    @Test
    public void testConvertOldTaskToNew() {
        String grpName = "grpTest";
        String cacheName = "cacheTest";
        String treeName = "treeTest";
        String idxName = "idxTest";

        List<Long> pages = F.asList(100L);

        DurableBackgroundCleanupIndexTreeTask oldTask = new DurableBackgroundCleanupIndexTreeTask(
            pages,
            emptyList(),
            grpName,
            cacheName,
            new IndexName(cacheName, "schemaTest", "tableTest", idxName),
            treeName
        );

        DurableBackgroundTask convertedTask = oldTask.convertAfterRestoreIfNeeded();
        assertTrue(convertedTask instanceof DurableBackgroundCleanupIndexTreeTaskV2);

        assertEquals(grpName, getFieldValue(convertedTask, "grpName"));
        assertEquals(cacheName, getFieldValue(convertedTask, "cacheName"));
        assertEquals(treeName, getFieldValue(convertedTask, "oldTreeName"));
        assertNotNull(getFieldValue(convertedTask, "newTreeName"));
        assertEquals(idxName, getFieldValue(convertedTask, "idxName"));
        assertEquals(pages.size(), (int)getFieldValue(convertedTask, "segments"));
    }

    /** */
    private class InlineIndexTreeTest extends InlineIndexTree {
        /** */
        public InlineIndexTreeTest(
            SortedIndexDefinition def,
            CacheGroupContext grpCtx,
            String treeName,
            IgniteCacheOffheapManager offheap,
            ReuseList reuseList,
            PageMemory pageMemory,
            PageIoResolver pageIoResolver,
            long metaPageId,
            boolean initNew,
            int configuredInlineSize,
            int maxInlineSize,
            IndexKeyTypeSettings keyTypeSettings,
            IndexRowCache rowCache,
            IoStatisticsHolder stats,
            InlineIndexRowHandlerFactory rowHndFactory,
            InlineRecommender recommender
        ) throws IgniteCheckedException {
            super(
                def,
                grpCtx,
                treeName,
                offheap,
                reuseList,
                pageMemory,
                pageIoResolver,
                metaPageId,
                initNew,
                configuredInlineSize,
                maxInlineSize,
                keyTypeSettings,
                rowCache,
                stats,
                rowHndFactory,
                recommender
            );
        }

        /** {@inheritDoc} */
        @Override protected long destroyDownPages(
            LongListReuseBag bag,
            long pageId,
            int lvl,
            IgniteInClosure<IndexRow> c,
            AtomicLong lockHoldStartTime,
            long lockMaxTime,
            Deque<GridTuple3<Long, Long, Long>> lockedPages
        ) throws IgniteCheckedException {
            doSleep(TIME_FOR_EACH_INDEX_PAGE_TO_DESTROY);

            if (Thread.currentThread() instanceof IgniteThread) {
                IgniteThread thread = (IgniteThread)Thread.currentThread();

                if (thread.getIgniteInstanceName().endsWith(String.valueOf(RESTARTED_NODE_NUM))
                    && blockDestroy.compareAndSet(true, false))
                    throw new RuntimeException("Aborting destroy (test).");
            }

            return super.destroyDownPages(bag, pageId, lvl, c, lockHoldStartTime, lockMaxTime, lockedPages);
        }
    }

    /**
     *
     */
    private class DurableBackgroundTaskTestListener implements CheckpointListener {
        /**
         * Prefix for metastorage keys for durable background tasks.
         */
        private static final String STORE_DURABLE_BACKGROUND_TASK_PREFIX = "durable-background-task-";

        /**
         * Metastorage.
         */
        private volatile ReadOnlyMetastorage metastorage;

        /**
         * Task keys in metastorage.
         */
        private List<String> savedTasks = new ArrayList<>();

        /** */
        public DurableBackgroundTaskTestListener(ReadWriteMetastorage metastorage) {
            this.metastorage = metastorage;
        }

        /**
         * Checks that saved tasks from before checkpoint begin step removed from metastorage.
         * Сall after the end of the checkpoint.
         *
         * @return true if check is successful.
         */
        public boolean check() throws IgniteCheckedException {
            if (savedTasks.isEmpty())
                return false;

            for (String taskKey : savedTasks) {
                DurableBackgroundTask task = (DurableBackgroundTask)metastorage.read(taskKey);

                if (task != null)
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
            savedTasks.clear();

            metastorage.iterate(STORE_DURABLE_BACKGROUND_TASK_PREFIX,
                (key, val) -> savedTasks.add(key),
                true);
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            /* No op. */
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            /* No op. */
        }
    }

    /**
     * Extension {@link InlineIndexTreeFactory} for test.
     */
    private class InlineIndexTreeFactoryEx extends InlineIndexTreeFactory {
        /** {@inheritDoc} */
        @Override protected InlineIndexTree create(
            CacheGroupContext grpCtx,
            RootPage rootPage,
            String treeName
        ) throws IgniteCheckedException {
            return new InlineIndexTreeTest(
                null,
                grpCtx,
                treeName,
                grpCtx.offheap(),
                grpCtx.offheap().reuseListForIndex(treeName),
                grpCtx.dataRegion().pageMemory(),
                PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
                rootPage.pageId().pageId(),
                false,
                0,
                0,
                new IndexKeyTypeSettings(),
                null,
                null,
                new NoopRowHandlerFactory(),
                null
            );
        }
    }
}
