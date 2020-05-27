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
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelper;
import org.apache.ignite.internal.util.lang.GridTuple3;
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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT;

/**
 * Tests case when long index deletion operation happens.
 */
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
        LogListener.matches("Blocked system-critical thread has been detected").build();

    /** Latch that waits for execution of durable background task. */
    private CountDownLatch pendingDelLatch;

    /** Latch that waits for indexes rebuilding. */
    private CountDownLatch idxsRebuildLatch;

    /** */
    private final LogListener pendingDelFinishedLsnr =
        new CallbackExecutorLogListener(".*?Execution of durable background task completed.*", () -> pendingDelLatch.countDown());

    /** */
    private final LogListener idxsRebuildFinishedLsnr =
        new CallbackExecutorLogListener("Indexes rebuilding completed for all caches.", () -> idxsRebuildLatch.countDown());

    /** */
    private final LogListener taskLifecycleListener =
        new MessageOrderLogListener(
            ".*?Executing durable background task: DROP_SQL_INDEX-PUBLIC." + IDX_NAME + "-.*",
            ".*?Could not execute durable background task: DROP_SQL_INDEX-PUBLIC." + IDX_NAME + "-.*",
            ".*?Executing durable background task: DROP_SQL_INDEX-PUBLIC." + IDX_NAME + "-.*",
            ".*?Execution of durable background task completed: DROP_SQL_INDEX-PUBLIC." + IDX_NAME + "-.*"
        );

    /**
     * When it is set to true during index deletion, node with number {@link #RESTARTED_NODE_NUM} fails to complete
     * deletion.
     */
    private final AtomicBoolean blockDestroy = new AtomicBoolean(false);

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(
        false,
        log(),
        blockedSysCriticalThreadLsnr,
        pendingDelFinishedLsnr,
        idxsRebuildFinishedLsnr,
        taskLifecycleListener
    );

    /** */
    private H2TreeIndex.H2TreeFactory regularH2TreeFactory;

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
                    .setCheckpointFrequency(Integer.MAX_VALUE)
            )
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setBackups(1)
                    .setSqlSchema("PUBLIC"),
                new CacheConfiguration<Integer, Integer>("TEST")
                    .setSqlSchema("PUBLIC")
                    .setBackups(1)
                    .setDataRegionName("dr1")
            )
            .setGridLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        withSystemProperty(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT, "5000");

        cleanPersistenceDir();

        regularH2TreeFactory = H2TreeIndex.h2TreeFactory;

        H2TreeIndex.h2TreeFactory = (
            cctx,
            name,
            tblName,
            table,
            cacheName,
            idxName,
            reuseList,
            grpId,
            grpName,
            pageMem,
            wal,
            globalRmvId,
            rowStore,
            metaPageId,
            initNew,
            cols,
            inlineIdxs,
            inlineSize,
            pk,
            affinityKey,
            rowCache,
            failureProcessor,
            log
        ) -> new H2TreeTest(
            cctx,
            name,
            tblName,
            cacheName,
            idxName,
            reuseList,
            grpId,
            grpName,
            pageMem,
            wal,
            globalRmvId,
            rowStore,
            metaPageId,
            initNew,
            cols,
            inlineIdxs,
            inlineSize,
            pk,
            affinityKey,
            rowCache,
            failureProcessor,
            log
        ) {
            @Override public int compareValues(Value v1, Value v2) {
                return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);
            }
        };

        blockedSysCriticalThreadLsnr.reset();

        pendingDelLatch = new CountDownLatch(1);
        idxsRebuildLatch = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        blockedSysCriticalThreadLsnr.reset();

        H2TreeIndex.h2TreeFactory = regularH2TreeFactory;

        stopAllGrids();

        cleanPersistenceDir();

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

        Ignite ignite = prepareAndPopulateCluster(nodeCnt, multicolumn);

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

                aliveNode.cluster().active(false);
            }

            ignite = startGrid(RESTARTED_NODE_NUM);

            awaitLatch(pendingDelLatch, "Test timed out: failed to await for durable background task completion.");

            awaitPartitionMapExchange();

            if (checkWhenOneNodeStopped) {
                ignite.cluster().active(true);

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
        Set<UUID> nodeIds = new HashSet<UUID>() {{
            add(grid(RESTARTED_NODE_NUM).cluster().localNode().id());
            add(grid(ALWAYS_ALIVE_NODE_NUM).cluster().localNode().id());
        }};

        log.info("Doing indexes validation.");

        VisorValidateIndexesTaskArg taskArg =
            new VisorValidateIndexesTaskArg(Collections.singleton("SQL_PUBLIC_T"), nodeIds, 0, 1, true);

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
     * Starts cluster and populates with data.
     *
     * @param nodeCnt Nodes count.
     * @param multicolumn Is index multicolumn.
     * @return Ignite instance.
     * @throws Exception If failed.
     */
    private IgniteEx prepareAndPopulateCluster(int nodeCnt, boolean multicolumn) throws Exception {
        IgniteEx ignite = startGrids(nodeCnt);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        query(cache, "create table t (id integer primary key, p integer, f integer, p integer) with \"BACKUPS=1\"");

        createIndex(cache, multicolumn);

        for (int i = 0; i < 5_000; i++)
            query(cache, "insert into t (id, p, f) values (?, ?, ?)", i, i, i);

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
    public void testClusterDeactivationShouldPassWithoutErrors() throws Exception {
        IgniteEx ignite = startGrids(NODES_COUNT);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache("TEST");

        query(cache, "create table TEST (id integer primary key, p integer, f integer, p integer) with " +
            "\"DATA_REGION=dr1\"");

        query(cache, "create index TEST_IDX on TEST (p)");

        for (int i = 0; i < 5_000; i++)
            query(cache, "insert into TEST (id, p, f) values (?, ?, ?)", i, i, i);

        LogListener lsnr = LogListener.matches("Could not execute durable background task").build();
        LogListener lsnr2 = LogListener.matches("Executing durable background task").build();
        LogListener lsnr3 = LogListener.matches("Execution of durable background task completed").build();

        testLog.registerListener(lsnr);
        testLog.registerListener(lsnr2);
        testLog.registerListener(lsnr3);

        ignite.cluster().active(false);

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
    public void testLongIndexDeletionSimple() throws Exception {
        testLongIndexDeletion(false, false, false, false, true);
    }

    /**
     * Tests case when long multicolumn index deletion operation happens. Checkpoint should run in the middle
     * of index deletion operation.
     *
     * @throws Exception If failed.
     */
    public void testLongMulticolumnIndexDeletion() throws Exception {
        testLongIndexDeletion(false, false, true, false, true);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After checkpoint node should restart without fully deleted index tree.
     *
     * @throws Exception If failed.
     */
    public void testLongIndexDeletionWithRestart() throws Exception {
        testLongIndexDeletion(true, false, false, false, true);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After deletion start one more node should be included in topology.
     *
     * @throws Exception If failed.
     */
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
    public void testLongIndexDeletionCheckWhenOneNodeStoppedAndDropIndex() throws Exception {
        testLongIndexDeletion(true, false, false, true, true);
    }

    /**
     * Tests that local task lifecycle was correct.
     *
     * @throws Exception If failed.
     */
    public void testDestroyTaskLifecycle() throws Exception {
        taskLifecycleListener.reset();

        IgniteEx ignite = prepareAndPopulateCluster(1, false);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        checkSelectAndPlan(cache, false);

        ignite.cluster().active(false);

        ignite.cluster().active(true);

        ignite.cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

        blockDestroy.set(true);

        stopGrid(RESTARTED_NODE_NUM);

        blockDestroy.set(false);

        startGrid(RESTARTED_NODE_NUM);

        awaitLatch(pendingDelLatch, "Test timed out: failed to await for durable background task completion.");

        assertTrue(taskLifecycleListener.check());
    }

    /**
     *
     */
    private abstract class H2TreeTest extends H2Tree {
        /**
         * Constructor.
         *
         * @param cctx Cache context.
         * @param name Tree name.
         * @param tblName Table name.
         * @param cacheName Cache name.
         * @param idxName Name of index.
         * @param reuseList Reuse list.
         * @param grpId Cache group ID.
         * @param grpName Cache group name.
         * @param pageMem Page memory.
         * @param wal Write ahead log manager.
         * @param globalRmvId Global remove id.
         * @param metaPageId Meta page ID.
         * @param initNew Initialize new index.
         * @param cols Cols.
         * @param inlineIdxs Inline indexes.
         * @param maxCalculatedInlineSize Maximum calculated inline size.
         * @param pk {@code true} for primary key.
         * @param affinityKey {@code true} for affinity key.
         * @param rowCache Row cache.
         * @param failureProcessor if the tree is corrupted.
         * @param log Logger.
         * @throws IgniteCheckedException If failed.
         */
        public H2TreeTest(
            GridCacheContext cctx,
            String name,
            String tblName,
            String cacheName,
            String idxName,
            ReuseList reuseList,
            int grpId,
            String grpName,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            AtomicLong globalRmvId,
            H2RowFactory rowStore,
            long metaPageId,
            boolean initNew,
            IndexColumn[] cols,
            List<InlineIndexHelper> inlineIdxs,
            int maxCalculatedInlineSize,
            boolean pk,
            boolean affinityKey,
            @Nullable H2RowCache rowCache,
            @Nullable FailureProcessor failureProcessor,
            IgniteLogger log
        ) throws IgniteCheckedException {
            super(
                cctx,
                name,
                tblName,
                cacheName,
                idxName,
                reuseList,
                grpId,
                grpName,
                pageMem,
                wal,
                globalRmvId,
                rowStore,
                metaPageId,
                initNew,
                cols,
                inlineIdxs,
                maxCalculatedInlineSize,
                pk,
                affinityKey,
                rowCache,
                failureProcessor,
                log
            );
        }

        /** {@inheritDoc} */
        @Override protected long destroyDownPages(
            DestroyBag bag,
            long pageId,
            int lvl,
            IgniteInClosure<SearchRow> c,
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
}
