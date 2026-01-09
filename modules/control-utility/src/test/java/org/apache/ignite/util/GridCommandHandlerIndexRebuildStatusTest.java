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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.management.cache.IndexRebuildStatusInfoContainer;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillThreeFieldsEntryCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.simpleIndexEntity;

/**
 * Test for --cache index_status command. Uses single cluster per suite.
 */
public class GridCommandHandlerIndexRebuildStatusTest extends GridCommandHandlerAbstractTest {
    /** Number of indexes that are being rebuilt. */
    private static AtomicInteger idxRebuildsStartedNum = new AtomicInteger();

    /** Is set to {@code True} when command was completed. */
    private static AtomicBoolean statusRequestingFinished = new AtomicBoolean();

    /** Is set to {@code True} if cluster should be restored before next test. */
    private static boolean clusterRestartRequired;

    /** Grids number. */
    public static final int GRIDS_NUM = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startupTestCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        shutdownTestCluster();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (clusterRestartRequired) {
            shutdownTestCluster();

            startupTestCluster();

            clusterRestartRequired = false;
        }

        idxRebuildsStartedNum.set(0);
        statusRequestingFinished.set(false);
        BlockingSchemaIndexCacheVisitorClosure.rowIndexListener = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        boolean allIndexesRebuilt = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 0, 30_000);

        super.afterTest();

        if (!allIndexesRebuilt) {
            clusterRestartRequired = true;

            fail("Failed to wait for index rebuild");
        }
    }

    /** */
    private void startupTestCluster() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrids(GRIDS_NUM);

        ignite.cluster().state(ClusterState.ACTIVE);

        createAndFillCache(ignite, "cache1", "group2");
        createAndFillCache(ignite, "cache2", "group1");

        createAndFillThreeFieldsEntryCache(ignite, "cache_no_group", null, Arrays.asList(simpleIndexEntity()));
    }

    /** */
    private void shutdownTestCluster() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Basic check.
     */
    @Test
    public void testCommandOutput() throws Exception {
        injectTestSystemOut();
        idxRebuildsStartedNum.set(0);

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        NodeFileTree ft1 = grid(GRIDS_NUM - 1).context().pdsFolderResolver().fileTree();
        NodeFileTree ft2 = grid(GRIDS_NUM - 2).context().pdsFolderResolver().fileTree();

        stopGrid(GRIDS_NUM - 1);
        stopGrid(GRIDS_NUM - 2);

        deleteIndexBin(ft1);
        deleteIndexBin(ft2);

        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;
        startGrid(GRIDS_NUM - 1);

        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;
        startGrid(GRIDS_NUM - 2);

        grid(0).cache("cache1").enableStatistics(true);

        boolean allRebuildsStarted = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 6, 30_000);
        assertTrue("Failed to wait for all indexes to start being rebuilt", allRebuildsStarted);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "indexes_rebuild_status"));

        checkResult(hnd, 1, 2);

        statusRequestingFinished.set(true);

        CountDownLatch idxProgressBlockedLatch = new CountDownLatch(1);
        CountDownLatch idxProgressUnblockedLatch = new CountDownLatch(1);

        BlockingSchemaIndexCacheVisitorClosure.rowIndexListener = () -> {
            if (isIndexRebuildInProgress("cache1")) {
                try {
                    idxProgressBlockedLatch.countDown();

                    idxProgressUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }
        };

        assertTrue(idxProgressBlockedLatch.await(getTestTimeout(), MILLISECONDS));

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "indexes_rebuild_status"));

        checkRebuildInProgressOutputFor("cache1");

        idxProgressUnblockedLatch.countDown();
    }

    /**
     * Check --node-id option.
     */
    @Test
    public void testNodeIdOption() throws Exception {
        injectTestSystemOut();
        idxRebuildsStartedNum.set(0);

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        NodeFileTree ft1 = grid(GRIDS_NUM - 1).context().pdsFolderResolver().fileTree();
        NodeFileTree ft2 = grid(GRIDS_NUM - 2).context().pdsFolderResolver().fileTree();

        stopGrid(GRIDS_NUM - 1);
        stopGrid(GRIDS_NUM - 2);

        deleteIndexBin(ft1);
        deleteIndexBin(ft2);

        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;
        IgniteEx ignite1 = startGrid(GRIDS_NUM - 1);

        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;
        startGrid(GRIDS_NUM - 2);

        final UUID id1 = ignite1.localNode().id();

        boolean allRebuildsStarted = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 6, 30_000);
        assertTrue("Failed to wait for all indexes to start being rebuilt", allRebuildsStarted);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "indexes_rebuild_status", "--node-id", id1.toString()));

        statusRequestingFinished.set(true);

        checkResult(hnd, 2);
    }

    /**
     * Checks command output when no indexes are being rebuilt.
     */
    @Test
    public void testEmptyResult() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_rebuild_status"));

        String output = testOut.toString();

        assertContains(log, output, "There are no caches that have index rebuilding in progress.");
    }

    /**
     * Checks that info about all {@code nodeIds} and only about them is present
     * in {@code handler} last operation result and in {@code testOut}.
     *
     * @param handler CommandHandler used to run command.
     * @param nodeIdxs Indexes of node to check.
     */
    private void checkResult(TestCommandHandler handler, int... nodeIdxs) {
        String output = testOut.toString();

        Map<UUID, Set<IndexRebuildStatusInfoContainer>> cmdResult = handler.getLastOperationResult();

        assertNotNull(cmdResult);
        assertEquals("Unexpected number of nodes in result", nodeIdxs.length, cmdResult.size());

        for (int nodeIdx : nodeIdxs) {
            Set<IndexRebuildStatusInfoContainer> cacheInfos = cmdResult.get(grid(nodeIdx).localNode().id());

            assertNotNull(cacheInfos);
            assertEquals("Unexpected number of cacheInfos in result", 3, cacheInfos.size());

            checkRebuildStartOutput(output, nodeIdx, "group1", "cache2");
            checkRebuildStartOutput(output, nodeIdx, "group2", "cache1");
            checkRebuildStartOutput(output, nodeIdx, "no_group", "cache_no_group");
        }
    }

    /** */
    private void checkRebuildStartOutput(String output, int nodeIdx, String grpName, String cacheName) {
        IgniteEx ignite = grid(nodeIdx);

        int locPartsCnt = ignite.context().cache().cache(cacheName).context().topology().localPartitionsNumber();

        assertContains(
            log,
            output,
            "node_id=" + ignite.localNode().id() +
                ", groupName=" + grpName +
                ", cacheName=" + cacheName +
                ", indexBuildPartitionsLeftCount=" + locPartsCnt +
                ", totalPartitionsCount=" + locPartsCnt +
                ", progress=0%");
    }

    /** */
    private void checkRebuildInProgressOutputFor(String cacheName) throws Exception {
        Matcher matcher = Pattern.compile(
            "cacheName=" + cacheName + ", indexBuildPartitionsLeftCount=(\\d+), totalPartitionsCount=(\\d+), progress=(\\d+)%"
        ).matcher(testOut.toString());

        List<Integer> rebuildProgressStatuses = new ArrayList<>();
        List<Integer> idxBuildPartitionsLeftCounts = new ArrayList<>();

        while (matcher.find()) {
            idxBuildPartitionsLeftCounts.add(Integer.parseInt(matcher.group(1)));

            rebuildProgressStatuses.add(Integer.parseInt(matcher.group(3)));
        }

        assertTrue(rebuildProgressStatuses.stream().anyMatch(progress -> progress > 0));

        int cacheTotalRebuildingPartsCnt = idxBuildPartitionsLeftCounts.stream().mapToInt(Integer::intValue).sum();

        assertTrue(waitForCondition(
            () -> grid(0).cache(cacheName).metrics().getIndexBuildPartitionsLeftCount() == cacheTotalRebuildingPartsCnt,
            getTestTimeout())
        );
    }

    /** */
    private boolean isIndexRebuildInProgress(String cacheName) {
        for (Ignite ignite : Ignition.allGrids()) {
            GridCacheContext<Object, Object> cctx = ((IgniteEx)ignite).context().cache().cache(cacheName).context();

            long parts = cctx.cache().metrics0().getIndexBuildPartitionsLeftCount();

            if (parts > 0 && parts < cctx.topology().partitions() / 2)
                return true;
        }

        return false;
    }

    /**
     * Indexing that blocks index rebuild until status request is completed.
     */
    private static class BlockingIndexesRebuildTask extends IndexesRebuildTask {
        /** {@inheritDoc} */
        @Override protected void startRebuild(GridCacheContext cctx, GridFutureAdapter<Void> fut,
            SchemaIndexCacheVisitorClosure clo, IndexRebuildCancelToken cancel) {
            idxRebuildsStartedNum.incrementAndGet();

            fut.listen(() -> idxRebuildsStartedNum.decrementAndGet());

            super.startRebuild(cctx, fut, new BlockingSchemaIndexCacheVisitorClosure(clo), cancel);
        }
    }

    /** */
    private static class BlockingSchemaIndexCacheVisitorClosure implements SchemaIndexCacheVisitorClosure {
        /** */
        private SchemaIndexCacheVisitorClosure original;

        /** */
        public static Runnable rowIndexListener;

        /**
         * @param original Original.
         */
        BlockingSchemaIndexCacheVisitorClosure(SchemaIndexCacheVisitorClosure original) {
            this.original = original;
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
            try {
                GridTestUtils.waitForCondition(() -> statusRequestingFinished.get(), 60_000);
            }
            catch (IgniteInterruptedCheckedException e) {
                log.error("Waiting for indexes rebuild was interrupted", e);

                statusRequestingFinished.set(true);
            }

            if (rowIndexListener != null)
                rowIndexListener.run();

            original.apply(row);
        }
    }
}
