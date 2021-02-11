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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusInfoContainer;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;
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

        ignite.cluster().active(true);

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

        final CommandHandler handler = new CommandHandler(createTestLogger());

        stopGrid(GRIDS_NUM - 1);
        stopGrid(GRIDS_NUM - 2);

        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 1));
        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 2));

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        IgniteEx ignite1 = startGrid(GRIDS_NUM - 1);

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        IgniteEx ignite2 = startGrid(GRIDS_NUM - 2);

        final UUID id1 = ignite1.localNode().id();
        final UUID id2 = ignite2.localNode().id();

        boolean allRebuildsStarted = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 6, 30_000);
        assertTrue("Failed to wait for all indexes to start being rebuilt", allRebuildsStarted);

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_rebuild_status"));

        statusRequestingFinished.set(true);

        checkResult(handler, id1, id2);
    }

    /**
     * Check --node-id option.
     */
    @Test
    public void testNodeIdOption() throws Exception {
        injectTestSystemOut();
        idxRebuildsStartedNum.set(0);

        final CommandHandler handler = new CommandHandler(createTestLogger());

        stopGrid(GRIDS_NUM - 1);
        stopGrid(GRIDS_NUM - 2);

        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 1));
        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 2));

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        IgniteEx ignite1 = startGrid(GRIDS_NUM - 1);

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        startGrid(GRIDS_NUM - 2);

        final UUID id1 = ignite1.localNode().id();

        boolean allRebuildsStarted = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 6, 30_000);
        assertTrue("Failed to wait for all indexes to start being rebuilt", allRebuildsStarted);

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_rebuild_status", "--node-id", id1.toString()));

        statusRequestingFinished.set(true);

        checkResult(handler, id1);
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
     * @param nodeIds Ids to check.
     */
    private void checkResult(CommandHandler handler, UUID... nodeIds) {
        String output = testOut.toString();

        Map<UUID, Set<IndexRebuildStatusInfoContainer>> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);
        assertEquals("Unexpected number of nodes in result", nodeIds.length, cmdResult.size());

        for (UUID nodeId: nodeIds) {
            Set<IndexRebuildStatusInfoContainer> cacheInfos = cmdResult.get(nodeId);
            assertNotNull(cacheInfos);
            assertEquals("Unexpected number of cacheInfos in result", 3, cacheInfos.size());

            final String nodeStr = "node_id=" + nodeId + ", groupName=group1, cacheName=cache2\n" +
                "node_id=" + nodeId + ", groupName=group2, cacheName=cache1\n" +
                "node_id=" + nodeId + ", groupName=no_group, cacheName=cache_no_group";

            assertContains(log, output, nodeStr);
        }
    }

    /**
     * Indexing that blocks index rebuild until status request is completed.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(
            GridCacheContext cctx,
            SchemaIndexCacheVisitorClosure clo,
            GridFutureAdapter<Void> rebuildIdxFut)
        {
            idxRebuildsStartedNum.incrementAndGet();

            rebuildIdxFut.listen((CI1<IgniteInternalFuture<?>>)f -> idxRebuildsStartedNum.decrementAndGet());

            super.rebuildIndexesFromHash0(cctx, new BlockingSchemaIndexCacheVisitorClosure(clo), rebuildIdxFut);
        }
    }

    /** */
    private static class BlockingSchemaIndexCacheVisitorClosure implements SchemaIndexCacheVisitorClosure {
        /** */
        private SchemaIndexCacheVisitorClosure original;

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

            original.apply(row);
        }
    }
}
