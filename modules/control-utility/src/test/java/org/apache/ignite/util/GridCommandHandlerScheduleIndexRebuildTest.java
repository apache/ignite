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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexTarget;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.parseMaintenanceTaskParameters;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.util.IgniteUtils.max;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakSqlIndex;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.complexIndexEntity;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillThreeFieldsEntryCache;

/**
 * Tests for --cache schedule_indexes_rebuild command. Uses single cluster per suite.
 */
public class GridCommandHandlerScheduleIndexRebuildTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String INDEX_REBUILD_MNTC_TASK = "indexRebuildMaintenanceTask";

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
    private static final String GROUP_NAME_NON_EXISTING = "non_existing_group";

    /** */
    private static final String GRP_NAME_1 = "group_1";

    /** */
    private static final String GRP_NAME_2 = "group_2";

    /** */
    private static final int GRIDS_NUM = 3;

    /** */
    private static final int LAST_NODE_NUM = GRIDS_NUM - 1;

    /** */
    private static final int REBUILD_TIMEOUT = 30_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(new ListeningTestLogger(log));

        cfg.setBuildIndexThreadPoolSize(max(2, cfg.getBuildIndexThreadPoolSize()));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

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

        G.allGrids().forEach(ignite -> assertFalse(((IgniteEx)ignite).context().maintenanceRegistry().isMaintenanceMode()));
    }

    /** */
    private void startupTestCluster() throws Exception {
        for (int i = 0; i < GRIDS_NUM; i++)
            startGrid(i);

        IgniteEx ignite = grid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        createAndFillCache(ignite, CACHE_NAME_1_1, GRP_NAME_1);
        createAndFillCache(ignite, CACHE_NAME_1_2, GRP_NAME_1);
        createAndFillCache(ignite, CACHE_NAME_2_1, GRP_NAME_2);

        createAndFillThreeFieldsEntryCache(ignite, CACHE_NAME_NO_GRP, null, Collections.singletonList(complexIndexEntity()));

        // Flush indexes rebuild status (it happens only on checkpoint).
        forceCheckpoint();
    }

    /**
     * Checks error messages when trying to rebuild indexes for non-existent cache, when trying
     * to rebuild non-existent indexes or when not specifying any indexes inside the square brackets.
     */
    @Test
    public void testErrors() {
        injectTestSystemOut();

        IgniteEx lastNode = grid(LAST_NODE_NUM);

        // Tests non-existing cache name.
        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", lastNode.localNode().id().toString(),
            "--cache-names", CACHE_NAME_NON_EXISTING));

        String notExistingCacheOutputStr = testOut.toString();

        assertTrue(notExistingCacheOutputStr.contains("WARNING: Indexes rebuild was not scheduled for any cache. Check command input."));
        assertTrue(notExistingCacheOutputStr.contains(
            "WARNING: These caches were not found:" + System.lineSeparator()
            + INDENT + CACHE_NAME_NON_EXISTING
        ));

        // Test non-existing cache group name.
        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", lastNode.localNode().id().toString(),
            "--group-names", GROUP_NAME_NON_EXISTING));

        String notExistingGroupOutputStr = testOut.toString();

        assertTrue(notExistingGroupOutputStr.contains("WARNING: Indexes rebuild was not scheduled for any cache. Check command input."));
        assertTrue(notExistingGroupOutputStr.contains(
            "WARNING: These cache groups were not found:" + System.lineSeparator()
            + INDENT + GROUP_NAME_NON_EXISTING
        ));

        testOut.reset();

        // Tests non-existing index name.
        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--cache-names", CACHE_NAME_1_1 + "[non-existing-index]"));

        String notExistingIndexOutputStr = testOut.toString();

        assertTrue(notExistingIndexOutputStr.contains("WARNING: Indexes rebuild was not scheduled for any cache. Check command input."));

        assertTrue(notExistingIndexOutputStr.contains(
            "WARNING: These indexes were not found:" + System.lineSeparator()
            + INDENT + CACHE_NAME_1_1 + ":" + System.lineSeparator()
            + INDENT + INDENT + "non-existing-index")
        );
    }

    /**
     * Checks that index is rebuilt correctly.
     */
    @Test
    public void testRebuild() throws Exception {
        IgniteEx node = grid(LAST_NODE_NUM);

        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", node.localNode().id().toString(),
            "--cache-names", CACHE_NAME_NO_GRP));

        checkIndexesRebuildScheduled(node, singletonMap(CU.cacheId(CACHE_NAME_NO_GRP), indexes(node, CACHE_NAME_NO_GRP)));

        node.close();

        node = startGrid(LAST_NODE_NUM);

        assertTrue(node.context().maintenanceRegistry().isMaintenanceMode());

        assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));

        node.close();

        node = startGrid(LAST_NODE_NUM);

        assertFalse(node.context().maintenanceRegistry().isMaintenanceMode());

        checkIndexes(CACHE_NAME_NO_GRP);
    }

    /**
     * Checks that corrupted index is successfully rebuilt by the command.
     */
    @Test
    public void testCorruptedIndexRebuildCache() throws Exception {
        testCorruptedIndexRebuild(false, true);
    }

    /**
     * Checks that corrupted index is successfully rebuilt by the command.
     */
    @Test
    public void testCorruptedIndexRebuildCacheWithGroup() throws Exception {
        testCorruptedIndexRebuild(true, true);
    }

    /**
     * Checks that corrupted index is successfully rebuilt by the command.
     */
    @Test
    public void testCorruptedIndexRebuildCacheOnAllNodes() throws Exception {
        testCorruptedIndexRebuild(false, false);
    }

    /**
     * Checks that corrupted index is successfully rebuilt by the command.
     */
    @Test
    public void testCorruptedIndexRebuildCacheWithGroupOnAllNodes() throws Exception {
        testCorruptedIndexRebuild(true, false);
    }

    /**
     * Checks that corrupted index is successfully rebuilt by the command.
     *
     * @param withCacheGroup If {@code true} creates a cache with a cache group.
     * @param specifyNodeId If {@code true} then execute rebuild only on one node.
     */
    private void testCorruptedIndexRebuild(boolean withCacheGroup, boolean specifyNodeId) throws Exception {
        IgniteEx firstNode = grid(0);

        String cacheName = "tmpCache";

        try {
            createAndFillCache(firstNode, cacheName, withCacheGroup ? "tmpGrp" : null);

            breakSqlIndex(firstNode.cachex(cacheName), 1, null);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", "--check-sizes"));

            assertContains(log, testOut.toString(), "issues found (listed above)");

            testOut.reset();

            List<String> args = new ArrayList<>();
            args.add("--cache");
            args.add("schedule_indexes_rebuild");
            if (specifyNodeId) {
                args.add("--node-id");
                args.add(firstNode.localNode().id().toString());
            }
            args.add("--cache-names");
            args.add(cacheName);

            assertEquals(EXIT_CODE_OK, execute(args.toArray(new String[0])));

            int nodeCount = specifyNodeId ? 1 : GRIDS_NUM;

            for (int i = 0; i < nodeCount; i++) {
                IgniteEx grid = grid(i);

                checkIndexesRebuildScheduled(grid, singletonMap(CU.cacheId(cacheName), indexes(grid, cacheName)));

                grid.close();

                grid = startGrid(i);

                assertTrue(grid.context().maintenanceRegistry().isMaintenanceMode());

                assertTrue(waitForIndexesRebuild(grid));

                grid.close();

                startGrid(i);
            }

            checkIndexes(cacheName);
        }
        finally {
            grid(0).destroyCache(cacheName);
        }
    }

    /**
     * Checks that command can be executed multiple times and all specified indexes will be rebuilt.
     */
    @Test
    public void testConsecutiveCommandInvocations() throws Exception {
        IgniteEx ignite = grid(0);

        breakAndCheckBroken(ignite, CACHE_NAME_1_1);
        breakAndCheckBroken(ignite, CACHE_NAME_1_2);
        breakAndCheckBroken(ignite, CACHE_NAME_2_1);
        breakAndCheckBroken(ignite, CACHE_NAME_NO_GRP);

        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", ignite.localNode().id().toString(),
            "--cache-names", CACHE_NAME_1_1 + "," + CACHE_NAME_1_2));

        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", ignite.localNode().id().toString(),
            "--cache-names", CACHE_NAME_2_1 + "," + CACHE_NAME_NO_GRP));

        Map<Integer, Set<String>> cacheToIndexes = new HashMap<>();
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_1_1), indexes(ignite, CACHE_NAME_1_1));
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_1_2), indexes(ignite, CACHE_NAME_1_2));
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_2_1), indexes(ignite, CACHE_NAME_2_1));
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_NO_GRP), indexes(ignite, CACHE_NAME_NO_GRP));

        checkIndexesRebuildScheduled(ignite, cacheToIndexes);

        ignite.close();

        ignite = startGrid(0);

        assertTrue(waitForIndexesRebuild(ignite));

        checkIndexes(CACHE_NAME_1_1);
        checkIndexes(CACHE_NAME_1_2);
        checkIndexes(CACHE_NAME_2_1);
        checkIndexes(CACHE_NAME_NO_GRP);

        ignite.close();

        startGrid(0);

        checkIndexes(CACHE_NAME_1_1);
        checkIndexes(CACHE_NAME_1_2);
        checkIndexes(CACHE_NAME_2_1);
        checkIndexes(CACHE_NAME_NO_GRP);
    }

    /**
     * Checks that specific indexes can be passed to the schedule rebuild command.
     */
    @Test
    public void testSpecificIndexes() throws Exception {
        IgniteEx ignite = grid(0);

        assertEquals(EXIT_CODE_OK, execute("--cache", "schedule_indexes_rebuild",
            "--node-id", ignite.localNode().id().toString(),
            "--cache-names", CACHE_NAME_1_1 + "[_key_PK]," + CACHE_NAME_1_2 + "[PERSON_ORGID_ASC_IDX]"));

        Map<Integer, Set<String>> cacheToIndexes = new HashMap<>();
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_1_1), Collections.singleton("_key_PK"));
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_1_2), Collections.singleton("PERSON_ORGID_ASC_IDX"));

        checkIndexesRebuildScheduled(ignite, cacheToIndexes);

        ignite.close();

        ignite = startGrid(0);

        assertTrue(waitForIndexesRebuild(ignite));

        checkIndexes(CACHE_NAME_1_1);
        checkIndexes(CACHE_NAME_1_2);

        ignite.close();

        startGrid(0);

        checkIndexes(CACHE_NAME_1_1);
        checkIndexes(CACHE_NAME_1_2);
    }

    /**
     * Checks that cache groups can be passed to the schedule rebuild command.
     */
    @Test
    public void testCacheGroupParameter() throws Exception {
        testCacheGroupsParameter(false);
    }

    /**
     * Checks that cache groups can be passed to the schedule rebuild command along with cache names parameter.
     */
    @Test
    public void testCacheGroupParameterWithCacheNames() throws Exception {
        testCacheGroupsParameter(true);
    }

    /**
     * Checks that cache groups can be passed to the schedule rebuild command
     * along with cache names parameter if {@code withCacheNames} is {@code true}.
     *
     * @param withCacheNames Pass --cache-names parameter along with --group-names.
     * @throws Exception If failed.
     */
    private void testCacheGroupsParameter(boolean withCacheNames) throws Exception {
        IgniteEx ignite = grid(0);

        List<String> cmd = new ArrayList<>();

        cmd.add("--cache");
        cmd.add("schedule_indexes_rebuild");
        cmd.add("--node-id");
        cmd.add(ignite.localNode().id().toString());
        cmd.add("--group-names");
        cmd.add(GRP_NAME_1);

        if (withCacheNames) {
            cmd.add("--cache-names");
            cmd.add(CACHE_NAME_2_1 + "[PERSON_ORGID_ASC_IDX]");
        }

        assertEquals(EXIT_CODE_OK, execute(cmd));

        HashSet<String> allIndexes = new HashSet<>(Arrays.asList("_key_PK", "PERSON_ORGID_ASC_IDX", "PERSON_NAME_ASC_IDX"));

        Map<Integer, Set<String>> cacheToIndexes = new HashMap<>();
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_1_1), allIndexes);
        cacheToIndexes.put(CU.cacheId(CACHE_NAME_1_2), allIndexes);

        if (withCacheNames)
            cacheToIndexes.put(CU.cacheId(CACHE_NAME_2_1), Collections.singleton("PERSON_ORGID_ASC_IDX"));

        checkIndexesRebuildScheduled(ignite, cacheToIndexes);

        ignite.close();

        ignite = startGrid(0);

        assertTrue(waitForIndexesRebuild(ignite));

        checkIndexes(CACHE_NAME_1_1);
        checkIndexes(CACHE_NAME_1_2);

        if (withCacheNames)
            checkIndexes(CACHE_NAME_2_1);

        ignite.close();

        startGrid(0);

        checkIndexes(CACHE_NAME_1_1);
        checkIndexes(CACHE_NAME_1_2);

        if (withCacheNames)
            checkIndexes(CACHE_NAME_2_1);
    }

    /**
     * Breaks sql index and checks that it is broken.
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void breakAndCheckBroken(IgniteEx ignite, String cacheName) throws Exception {
        injectTestSystemOut();

        breakSqlIndex(ignite.cachex(cacheName), 1, null);

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", cacheName));

        assertContains(log, testOut.toString(), "issues found (listed above)");

        testOut.reset();
    }

    /**
     * Checks that indexes are valid.
     *
     * @param cacheName Cache name.
     */
    private void checkIndexes(String cacheName) {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", cacheName));

        assertContains(log, testOut.toString(), "no issues found.");

        testOut.reset();
    }

    /**
     * Waits for the rebuild of the indexes.
     *
     * @param ignite Ignite instance.
     * @return {@code True} if index rebuild was completed before {@code timeout} was reached.
     * @throws IgniteInterruptedCheckedException if failed.
     */
    private boolean waitForIndexesRebuild(IgniteEx ignite) throws IgniteInterruptedCheckedException {
        return GridTestUtils.waitForCondition(
            () -> ignite.context().cache().publicCaches()
                .stream()
                .allMatch(c -> c.indexReadyFuture().isDone()),
            REBUILD_TIMEOUT);
    }

    /**
     * Checks that given indexes are scheduled for the rebuild.
     *
     * @param node Node.
     * @param cacheToIndexes Map of caches to indexes.
     */
    private void checkIndexesRebuildScheduled(IgniteEx node, Map<Integer, Set<String>> cacheToIndexes) {
        MaintenanceTask maintenanceTask = node.context().maintenanceRegistry().requestedTask(INDEX_REBUILD_MNTC_TASK);

        assertNotNull(maintenanceTask);

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(maintenanceTask.parameters());

        Map<Integer, Set<String>> result = targets.stream().collect(groupingBy(
            MaintenanceRebuildIndexTarget::cacheId,
            mapping(MaintenanceRebuildIndexTarget::idxName, toSet())
        ));

        assertEqualsMaps(cacheToIndexes, result);
    }

    /**
     * Returns indexes' names of the given cache.
     *
     * @param node Node.
     * @param cache Cache name.
     * @return Indexes of the cache.
     */
    private Set<String> indexes(IgniteEx node, String cache) {
        return node.context().indexProcessor().treeIndexes(cache, false).stream()
            .map(Index::name)
            .collect(Collectors.toSet());
    }
}
