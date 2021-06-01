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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheFuture;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Person;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.String.valueOf;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakSqlIndex;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.complexIndexEntity;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillThreeFieldsEntryCache;

/**
 * Test for --cache indexes_force_rebuild command. Uses single cluster per suite.
 */
public class GridCommandHandlerIndexForceRebuildTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String CACHE_NAME_1_1 = "cache_1_1";

    /** */
    private static final String CACHE_NAME_1_2 = "cache_1_2";

    /** */
    private static final String CACHE_NAME_2_1 = "cache_2_1";

    /** */
    private static final String CACHE_NAME_NO_GRP = "cache_no_group";

    /** */
    private static final String CACHE_NAME_NON_EXISTING = "non_existing_cache";

    /** */
    private static final String GRP_NAME_1 = "group_1";

    /** */
    private static final String GRP_NAME_2 = "group_2";

    /** */
    private static final String GRP_NAME_NON_EXISTING = "non_existing_group";

    /** */
    private static final int GRIDS_NUM = 3;

    /** */
    private static final int LAST_NODE_NUM = GRIDS_NUM - 1;

    /**
     * Map for blocking index rebuilds in a {@link BlockingIndexesRebuildTask}.
     * To stop blocking, need to delete the entry.
     * Mapping: cache name -> future start blocking rebuilding indexes.
     */
    private static final Map<String, GridFutureAdapter<Void>> blockRebuildIdx = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startupTestCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        blockRebuildIdx.clear();
    }

    /** */
    private void startupTestCluster() throws Exception {
        for (int i = 0; i < GRIDS_NUM; i++ ) {
            IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;
            startGrid(i);
        }

        IgniteEx ignite = grid(0);

        ignite.cluster().active(true);

        createAndFillCache(ignite, CACHE_NAME_1_1, GRP_NAME_1);
        createAndFillCache(ignite, CACHE_NAME_1_2, GRP_NAME_1);
        createAndFillCache(ignite, CACHE_NAME_2_1, GRP_NAME_2);

        createAndFillThreeFieldsEntryCache(ignite, CACHE_NAME_NO_GRP, null, Collections.singletonList(complexIndexEntity()));
    }

    /**
     * Checks error messages when trying to rebuild indexes for
     * non-existent cache of group.
     */
    @Test
    public void testEmptyResult() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--cache-names", CACHE_NAME_NON_EXISTING));

        String cacheNamesOutputStr = testOut.toString();

        assertTrue(cacheNamesOutputStr.contains("WARNING: Indexes rebuild was not started for any cache. Check command input."));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--group-names", GRP_NAME_NON_EXISTING));

        String grpNamesOutputStr = testOut.toString();

        assertTrue(grpNamesOutputStr.contains("WARNING: Indexes rebuild was not started for any cache. Check command input."));
    }

    /**
     * Checks that index on 2 fields is rebuilt correctly.
     */
    @Test
    public void testComplexIndexRebuild() throws IgniteInterruptedCheckedException {
        injectTestSystemOut();

        LogListener lsnr = installRebuildCheckListener(grid(LAST_NODE_NUM), CACHE_NAME_NO_GRP);

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--cache-names", CACHE_NAME_NO_GRP));

        assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));

        assertTrue(lsnr.check());

        removeLogListener(grid(LAST_NODE_NUM), lsnr);
    }

    /**
     * Checks --node-id and --cache-names options,
     * correctness of utility output and the fact that indexes were actually rebuilt.
     */
    @Test
    public void testCacheNamesArg() throws Exception {
        blockRebuildIdx.put(CACHE_NAME_2_1, new GridFutureAdapter<>());

        injectTestSystemOut();

        LogListener[] cache1Listeners = new LogListener[GRIDS_NUM];
        LogListener[] cache2Listeners = new LogListener[GRIDS_NUM];

        try {
            triggerIndexRebuild(LAST_NODE_NUM, Collections.singletonList(CACHE_NAME_2_1));

            for (int i = 0; i < GRIDS_NUM; i++) {
                cache1Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_1_1);
                cache2Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_1_2);
            }

            assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
                "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
                "--cache-names", CACHE_NAME_1_1 + "," + CACHE_NAME_2_1 + "," + CACHE_NAME_NON_EXISTING));

            blockRebuildIdx.remove(CACHE_NAME_2_1);

            waitForIndexesRebuild(grid(LAST_NODE_NUM));

            String outputStr = testOut.toString();

            validateOutputCacheNamesNotFound(outputStr, CACHE_NAME_NON_EXISTING);

            validateOutputIndicesRebuildingInProgress(outputStr, F.asMap(GRP_NAME_2, F.asList(CACHE_NAME_2_1)));

            validateOutputIndicesRebuildWasStarted(outputStr, F.asMap(GRP_NAME_1, F.asList(CACHE_NAME_1_1)));

            assertEquals("Unexpected number of lines in output.", 19, outputStr.split("\n").length);

            // Index rebuild must be triggered only for cache1_1 and only on node3.
            assertFalse(cache1Listeners[0].check());
            assertFalse(cache1Listeners[1].check());
            assertTrue(cache1Listeners[LAST_NODE_NUM].check());

            for (LogListener cache2Lsnr: cache2Listeners)
                assertFalse(cache2Lsnr.check());
        }
        finally {
            blockRebuildIdx.remove(CACHE_NAME_2_1);

            for (int i = 0; i < GRIDS_NUM; i++) {
                removeLogListener(grid(i), cache1Listeners[i]);
                removeLogListener(grid(i), cache2Listeners[i]);
            }

            assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));
        }
    }

    /**
     * Checks --node-id and --group-names options,
     * correctness of utility output and the fact that indexes were actually rebuilt.
     */
    @Test
    public void testGroupNamesArg() throws Exception {
        blockRebuildIdx.put(CACHE_NAME_1_2, new GridFutureAdapter<>());

        injectTestSystemOut();

        LogListener[] cache1Listeners = new LogListener[GRIDS_NUM];
        LogListener[] cache2Listeners = new LogListener[GRIDS_NUM];

        try {
            triggerIndexRebuild(LAST_NODE_NUM, Collections.singletonList(CACHE_NAME_1_2));

            for (int i = 0; i < GRIDS_NUM; i++) {
                cache1Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_1_1);
                cache2Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_NO_GRP);
            }

            assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
                "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
                "--group-names", GRP_NAME_1 + "," + GRP_NAME_2 + "," + GRP_NAME_NON_EXISTING));

            blockRebuildIdx.remove(CACHE_NAME_1_2);

            waitForIndexesRebuild(grid(LAST_NODE_NUM));

            String outputStr = testOut.toString();

            validateOutputCacheGroupsNotFound(outputStr, GRP_NAME_NON_EXISTING);

            validateOutputIndicesRebuildingInProgress(outputStr, F.asMap(GRP_NAME_1, F.asList(CACHE_NAME_1_2)));

            validateOutputIndicesRebuildWasStarted(
                outputStr,
                F.asMap(
                    GRP_NAME_1, F.asList(CACHE_NAME_1_1),
                    GRP_NAME_2, F.asList(CACHE_NAME_2_1)
                )
            );

            assertEquals("Unexpected number of lines in outputStr.", 20, outputStr.split("\n").length);

            assertFalse(cache1Listeners[0].check());
            assertFalse(cache1Listeners[1].check());
            assertTrue(cache1Listeners[LAST_NODE_NUM].check());

            for (LogListener cache2Lsnr: cache2Listeners)
                assertFalse(cache2Lsnr.check());
        }
        finally {
            blockRebuildIdx.remove(CACHE_NAME_1_2);

            for (int i = 0; i < GRIDS_NUM; i++) {
                removeLogListener(grid(i), cache1Listeners[i]);
                removeLogListener(grid(i), cache2Listeners[i]);
            }

            assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));
        }
    }

    /**
     * Checks illegal parameter after indexes_force_rebuild.
     */
    @Test
    public void testIllegalArgument() {
        int code = execute("--cache", "indexes_force_rebuild", "--illegal_parameter");
        assertEquals(1, code);
    }

    /**
     * Checks client node id as an agrument. Command shoul
     *
     * @throws Exception If failed to start client node.
     */
    @Test
    public void testClientNodeConnection() throws Exception {
        IgniteEx client = startGrid("client");

        try {
            assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--cache", "indexes_force_rebuild",
                "--node-id", client.localNode().id().toString(),
                "--group-names", GRP_NAME_1));
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * Checks that 2 commands launch trigger async index rebuild.
     */
    @Test
    public void testAsyncIndexesRebuild() throws IgniteInterruptedCheckedException {
        blockRebuildIdx.put(CACHE_NAME_1_1, new GridFutureAdapter<>());
        blockRebuildIdx.put(CACHE_NAME_1_2, new GridFutureAdapter<>());

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(0).localNode().id().toString(),
            "--cache-names", CACHE_NAME_1_1));

        assertTrue("Failed to wait for index rebuild start for first cache.",
            GridTestUtils.waitForCondition(() -> getActiveRebuildCaches(grid(0)).size() == 1, 10_000));

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(0).localNode().id().toString(),
            "--cache-names", CACHE_NAME_1_2));

        assertTrue("Failed to wait for index rebuild start for second cache.",
            GridTestUtils.waitForCondition(() -> getActiveRebuildCaches(grid(0)).size() == 2, 10_000));

        blockRebuildIdx.clear();

        assertTrue("Failed to wait for final index rebuild.", waitForIndexesRebuild(grid(0)));
    }

    /**
     * Checks how index force rebuild command behaves when caches are under load.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexRebuildUnderLoad() throws Exception {
        IgniteEx n = grid(0);

        AtomicBoolean stopLoad = new AtomicBoolean(false);

        String cacheName1 = "tmpCache1";
        String cacheName2 = "tmpCache2";

        List<String> caches = F.asList(cacheName1, cacheName2);

        try {
            for (String c : caches)
                createAndFillCache(n, c, "tmpGrp");

            int cacheSize = n.cache(cacheName1).size();

            for (String c : caches)
                blockRebuildIdx.put(c, new GridFutureAdapter<>());

            assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
                "--node-id", n.localNode().id().toString(),
                "--cache-names", cacheName1 + "," + cacheName2));

            IgniteInternalFuture<?> putCacheFut = runAsync(() -> {
                ThreadLocalRandom r = ThreadLocalRandom.current();

                while (!stopLoad.get())
                    n.cache(cacheName1).put(r.nextInt(), new Person(r.nextInt(), valueOf(r.nextLong())));
            });

            assertTrue(waitForCondition(() -> n.cache(cacheName1).size() > cacheSize, getTestTimeout()));

            for (String c : caches) {
                IgniteInternalFuture<?> rebIdxFut = n.context().query().indexRebuildFuture(CU.cacheId(c));
                assertNotNull(rebIdxFut);
                assertFalse(rebIdxFut.isDone());

                blockRebuildIdx.get(c).get(getTestTimeout());
            }

            IgniteInternalFuture<Boolean> destroyCacheFut = n.context().cache()
                .dynamicDestroyCache(cacheName2, false, true, false, null);

            SchemaIndexCacheFuture intlRebIdxFut = schemaIndexCacheFuture(n, CU.cacheId(cacheName2));
            assertNotNull(intlRebIdxFut);

            assertTrue(waitForCondition(intlRebIdxFut.cancelToken()::isCancelled, getTestTimeout()));

            stopLoad.set(true);

            blockRebuildIdx.clear();

            waitForIndexesRebuild(n);

            intlRebIdxFut.get(getTestTimeout());
            destroyCacheFut.get(getTestTimeout());
            putCacheFut.get(getTestTimeout());

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", cacheName1));

            assertContains(log, testOut.toString(), "no issues found.");
        }
        finally {
            stopLoad.set(true);

            blockRebuildIdx.clear();

            n.destroyCache(cacheName1);
            n.destroyCache(cacheName2);
        }
    }

    /**
     * Checks that corrupted index is successfully rebuilt by the command.
     */
    @Test
    public void testCorruptedIndexRebuild() throws Exception {
        IgniteEx ignite = grid(0);

        final String cacheName = "tmpCache";
        final String grpName = "tmpGrp";

        try {
            createAndFillCache(ignite, cacheName, grpName);

            breakSqlIndex(ignite.cachex(cacheName), 1, null);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", "--check-sizes"));

            assertContains(log, testOut.toString(), "issues found (listed above)");

            testOut.reset();

            assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
                "--node-id", ignite.localNode().id().toString(),
                "--cache-names", cacheName));

            assertTrue(waitForIndexesRebuild(ignite));

            forceCheckpoint(ignite);

            assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", cacheName));

            assertContains(log, testOut.toString(), "no issues found.");
        }
        finally {
            ignite.destroyCache(cacheName);
        }
    }

    /**
     * Checking that a sequence of forced rebuild of indexes is possible
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequentialForceRebuildIndexes() throws Exception {
        IgniteEx grid = grid(0);

        injectTestSystemOut();

        String outputStr;

        forceRebuildIndices(F.asList(CACHE_NAME_1_1), grid);

        outputStr = testOut.toString();

        validateOutputIndicesRebuildWasStarted(outputStr, F.asMap(GRP_NAME_1, F.asList(CACHE_NAME_1_1)));

        assertFalse(outputStr.contains("WARNING: These caches have indexes rebuilding in progress:"));

        forceRebuildIndices(F.asList(CACHE_NAME_1_1), grid);

        validateOutputIndicesRebuildWasStarted(outputStr, F.asMap(GRP_NAME_1, F.asList(CACHE_NAME_1_1)));

        assertFalse(outputStr.contains("WARNING: These caches have indexes rebuilding in progress:"));
    }

    /**
     * Validates control.sh output when caches by name not found.
     *
     * @param outputStr CLI {@code control.sh} utility output.
     * @param cacheNames Cache names to print.
     */
    private void validateOutputCacheNamesNotFound(String outputStr, String... cacheNames) {
        assertContains(
            log,
            outputStr,
            "WARNING: These caches were not found:" + U.nl() + makeStringListWithIndent(cacheNames)
        );
    }

    /**
     * Validates control.sh output when caches by group not found.
     *
     * @param outputStr CLI {@code control.sh} utility output.
     * @param cacheGrps Cache groups to print.
     */
    private void validateOutputCacheGroupsNotFound(String outputStr, String... cacheGrps) {
        assertContains(
            log,
            outputStr,
            "WARNING: These cache groups were not found:" + U.nl() + makeStringListWithIndent(cacheGrps)
        );
    }

    /**
     * Makes new-line List with indent.
     * @param strings List of strings.
     * @return Formated text.
     */
    private String makeStringListWithIndent(String... strings) {
        return INDENT + String.join(U.nl() + INDENT, strings);
    }

    /**
     * Makes formatted text for given caches.
     *
     * @param cacheGroputToNames Cache groups mapping to non-existing cache names.
     * @return Text for CLI print output for given caches.
     */
    private String makeStringListForCacheGroupsAndNames(Map<String, List<String>> cacheGroputToNames) {
        SB sb = new SB();

        for (Map.Entry<String, List<String>> entry : cacheGroputToNames.entrySet()) {
            String cacheGrp = entry.getKey();

            for (String cacheName : entry.getValue())
                sb.a(INDENT).a("groupName=").a(cacheGrp).a(", cacheName=").a(cacheName).a(U.nl());
        }

        return sb.toString();
    }

    /**
     * Validates control.sh output when some indices rebuilt in progress.
     *
     * @param outputStr CLI {@code control.sh} utility output.
     * @param cacheGroputToNames Cache groups mapping to non-existing cache names.
     */
    private void validateOutputIndicesRebuildingInProgress(String outputStr, Map<String, List<String>> cacheGroputToNames) {
        String caches = makeStringListForCacheGroupsAndNames(cacheGroputToNames);

        assertContains(
            log,
            outputStr,
            "WARNING: These caches have indexes rebuilding in progress:" + U.nl() + caches
        );
    }

    /**
     * Validates control.sh output when indices started to rebuild.
     *
     * @param outputStr CLI {@code control.sh} utility output.
     * @param cacheGroputToNames Cache groups mapping to non-existing cache names.
     */
    private void validateOutputIndicesRebuildWasStarted(String outputStr, Map<String, List<String>> cacheGroputToNames) {
        String caches = makeStringListForCacheGroupsAndNames(cacheGroputToNames);

        assertContains(
            log,
            outputStr,
            "Indexes rebuild was started for these caches:" + U.nl() + caches
        );
    }

    /**
     * Triggers indexes rebuild for ALL caches on grid node with index {@code igniteIdx}.
     *
     * @param igniteIdx Node index.
     * @param excludedCacheNames Collection of cache names for which
     *  end of index rebuilding is not awaited.
     * @throws Exception if failed.
     */
    private void triggerIndexRebuild(int igniteIdx, Collection<String> excludedCacheNames) throws Exception {
        stopGrid(igniteIdx);

        GridTestUtils.deleteIndexBin(getTestIgniteInstanceName(2));

        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;
        final IgniteEx ignite = startGrid(igniteIdx);

        resetBaselineTopology();
        awaitPartitionMapExchange();
        waitForIndexesRebuild(ignite, 30_000, excludedCacheNames);
    }

    /** */
    private boolean waitForIndexesRebuild(IgniteEx ignite) throws IgniteInterruptedCheckedException {
        return waitForIndexesRebuild(ignite, 30_000, Collections.emptySet());
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout timeout
     * @param excludedCacheNames Collection of cache names for which
     *  end of index rebuilding is not awaited.
     * @return {@code True} if index rebuild was completed before {@code timeout} was reached.
     * @throws IgniteInterruptedCheckedException if failed.
     */
    private boolean waitForIndexesRebuild(IgniteEx ignite, long timeout, Collection<String> excludedCacheNames)
        throws IgniteInterruptedCheckedException
    {
        return GridTestUtils.waitForCondition(
            () -> ignite.context().cache().publicCaches()
                .stream()
                .filter(c -> !excludedCacheNames.contains(c.getName()))
                .allMatch(c -> c.indexReadyFuture().isDone()),
            timeout);
    }

    /**
     * @param ignite Node from which caches will be collected.
     * @return {@code Set} of ignite caches that have index rebuild in process.
     */
    private Set<IgniteCacheProxy<?, ?>> getActiveRebuildCaches(IgniteEx ignite) {
        return ignite.context().cache().publicCaches()
            .stream()
            .filter(c -> !c.indexReadyFuture().isDone())
            .collect(Collectors.toSet());
    }

    /**
     * @param ignite IgniteEx instance.
     * @param cacheName Name of checked cache.
     * @return newly installed LogListener.
     */
    private LogListener installRebuildCheckListener(IgniteEx ignite, String cacheName) {
        final MessageOrderLogListener lsnr = new MessageOrderLogListener(
            new MessageOrderLogListener.MessageGroup(true)
                .add("Started indexes rebuilding for cache \\[name=" + cacheName + ".*")
                .add("Finished indexes rebuilding for cache \\[name=" + cacheName + ".*")
        );

        ListeningTestLogger impl = GridTestUtils.getFieldValue(ignite.log(), "impl");
        assertNotNull(impl);

        impl.registerListener(lsnr);

        return lsnr;
    }

    /** */
    private void removeLogListener(IgniteEx ignite, LogListener lsnr) {
        ListeningTestLogger impl = GridTestUtils.getFieldValue(ignite.log(), "impl");
        assertNotNull(impl);

        impl.unregisterListener(lsnr);
    }

    /**
     * Indexing that blocks index rebuild until status request is completed.
     */
    private static class BlockingIndexesRebuildTask extends IndexesRebuildTask {
        /** {@inheritDoc} */
        @Override protected void startRebuild(GridCacheContext cctx, GridFutureAdapter<Void> fut,
            SchemaIndexCacheVisitorClosure clo, SchemaIndexOperationCancellationToken cancel) {
            super.startRebuild(cctx, new BlockingRebuildIdxFuture(fut, cctx), clo, cancel);
        }
    }

    /**
     * Modified rebuild indexes future which is blocked right before finishing for specific caches.
     */
    private static class BlockingRebuildIdxFuture extends GridFutureAdapter<Void> {
        /** */
        private final GridFutureAdapter<Void> original;

        /** */
        private final GridCacheContext cctx;

        /** */
        BlockingRebuildIdxFuture(GridFutureAdapter<Void> original, GridCacheContext cctx) {
            this.original = original;
            this.cctx = cctx;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            try {
                GridFutureAdapter<Void> fut = blockRebuildIdx.get(cctx.name());

                if (fut != null) {
                    fut.onDone();

                    assertTrue("Failed to wait for indexes rebuild unblocking",
                        GridTestUtils.waitForCondition(() -> !blockRebuildIdx.containsKey(cctx.name()), 60_000));
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                fail("Waiting for indexes rebuild unblocking was interrupted");
            }

            return original.onDone(res, err);
        }
    }

    /**
     * Getting internal index rebuild future for cache.
     *
     * @param n Node.
     * @param cacheId Cache id.
     * @return Internal index rebuild future.
     */
    @Nullable private SchemaIndexCacheFuture schemaIndexCacheFuture(IgniteEx n, int cacheId) {
        IndexesRebuildTask idxRebuild = n.context().indexProcessor().idxRebuild();

        Map<Integer, SchemaIndexCacheFuture> idxRebuildFuts = getFieldValue(idxRebuild, "idxRebuildFuts");

        return idxRebuildFuts.get(cacheId);
    }

    /**
     * Force rebuilds indices for chosen caches, and waits until rebuild process is complete.
     *
     * @param cacheNames Cache names need indices to rebuild.
     * @param grid Ignite node.
     * @throws Exception If failed.
     */
    private void forceRebuildIndices(Iterable<String> cacheNames, IgniteEx grid) throws Exception {
        String cacheNamesArg = String.join(",", cacheNames);

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--cache", "indexes_force_rebuild",
                "--node-id", grid.localNode().id().toString(),
                "--cache-names", cacheNamesArg
            )
        );

        waitForIndexesRebuild(grid, getTestTimeout(), Collections.emptyList());
    }
}
