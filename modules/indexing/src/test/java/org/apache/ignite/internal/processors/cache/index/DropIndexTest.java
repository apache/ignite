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

package org.apache.ignite.internal.processors.cache.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.InlineIndexTreeFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState;
import org.apache.ignite.internal.util.function.ThrowableFunction;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.destroyIndexTrees;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.findIndexRootPages;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.idxTreeFactory;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.toRootPages;
import static org.apache.ignite.testframework.GridTestUtils.cacheContext;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Class for testing index drop.
 */
@WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "1000000")
public class DropIndexTest extends AbstractRebuildIndexTest {
    /** Original {@link DurableBackgroundCleanupIndexTreeTask#idxTreeFactory}. */
    private InlineIndexTreeFactory originalTaskIdxTreeFactory;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        originalTaskIdxTreeFactory = idxTreeFactory;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        idxTreeFactory = originalTaskIdxTreeFactory;
    }

    /** {@inheritDoc} */
    @Override protected void populate(IgniteCache<Integer, Person> cache, int cnt) {
        String prefix = IntStream.range(0, 1_000).mapToObj(i -> "name").collect(joining("_")) + "_";

        for (int i = 0; i < cnt; i++)
            cache.put(i, new Person(i, prefix + i));
    }

    /**
     * Checking {@link DurableBackgroundCleanupIndexTreeTask#destroyIndexTrees}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyIndexTrees() throws Exception {
        checkDestroyIndexTrees(true, 3);
    }

    /**
     * Check that the {@link DurableBackgroundCleanupIndexTreeTask} will not
     * be executed if the cache group and root pages are not found.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTaskNotExecuteIfAbsentCacheGroupOrRootPages() throws Exception {
        IgniteEx n = startGrid(0);

        String fake = UUID.randomUUID().toString();

        GridCacheContext<Integer, Person> cctx = cacheContext(n.cache(DEFAULT_CACHE_NAME));

        List<DurableBackgroundCleanupIndexTreeTask> tasks = F.asList(
            new DurableBackgroundCleanupIndexTreeTask(fake, fake, fake, fake, fake, 10, null),
            new DurableBackgroundCleanupIndexTreeTask(cctx.group().name(), cctx.name(), fake, fake, fake, 10, null)
        );

        for (DurableBackgroundCleanupIndexTreeTask task : tasks) {
            DurableBackgroundTaskResult<Long> res = task.executeAsync(n.context()).get(0);

            assertTrue(res.completed());
            assertNull(res.error());
            assertNull(res.result());
        }
    }

    /**
     * Checking that the {@link DurableBackgroundCleanupIndexTreeTask} will work correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorrectTaskExecute() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        GridCacheContext<Integer, Person> cctx = cacheContext(cache);

        Index idx = index(n, cache, idxName);

        SortedIndexDefinition idxDef = indexDefinition(idx);
        InlineIndexTree[] trees = segments(idx);

        Map<Integer, RootPage> rootPages = toRootPages(trees);

        for (int i = 0; i < trees.length; i++) {
            InlineIndexTree tree = trees[i];

            assertEquals(new FullPageId(tree.getMetaPageId(), tree.groupId()), rootPages.get(i).pageId());
        }

        String oldTreeName = idxDef.treeName();
        String newTreeName = UUID.randomUUID().toString();
        int segments = idxDef.segments();

        assertFalse(findIndexRootPages(cctx.group(), cctx.name(), oldTreeName, segments).isEmpty());
        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), newTreeName, segments).isEmpty());

        DurableBackgroundCleanupIndexTreeTask task = new DurableBackgroundCleanupIndexTreeTask(
            cctx.group().name(),
            cctx.name(),
            idxName,
            oldTreeName,
            newTreeName,
            segments,
            trees
        );

        assertTrue(task.name().startsWith(taskNamePrefix(cctx.name(), idxName)));
        assertTrue(getFieldValue(task, "needToRen"));

        GridFutureAdapter<Void> startFut = new GridFutureAdapter<>();
        GridFutureAdapter<Void> endFut = new GridFutureAdapter<>();

        idxTreeFactory = taskIndexTreeFactoryEx(startFut, endFut);

        IgniteInternalFuture<DurableBackgroundTaskResult<Long>> taskFut = task.executeAsync(n.context());

        startFut.get(getTestTimeout());

        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), oldTreeName, segments).isEmpty());
        assertFalse(findIndexRootPages(cctx.group(), cctx.name(), newTreeName, segments).isEmpty());

        endFut.onDone();

        DurableBackgroundTaskResult<Long> res = taskFut.get(getTestTimeout());

        assertTrue(res.completed());
        assertNull(res.error());
        assertTrue(res.result() >= 3);

        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), oldTreeName, segments).isEmpty());
        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), newTreeName, segments).isEmpty());

        assertFalse(getFieldValue(task, "needToRen"));
    }

    /**
     * Checking that the {@link DurableBackgroundCleanupIndexTreeTask} will
     * run when the index drop is called.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdx() throws Exception {
        checkExecuteTask(true, 3L);
    }

    /**
     * Checking that when the node is restarted, the
     * {@link DurableBackgroundCleanupIndexTreeTask} will finish correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdxAfterRestart() throws Exception {
        checkExecuteTaskAfterRestart(true, 3L, n -> {
            // Disable auto activation.
            n.cluster().baselineAutoAdjustEnabled(false);
            stopGrid(0);

            n = startGrid(0, cfg -> {
                cfg.setClusterStateOnStart(INACTIVE);
            });

            return n;
        });
    }

    /**
     * Checking that when the node is re-activated, the
     * {@link DurableBackgroundCleanupIndexTreeTask} will finish correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdxAfterReActivated() throws Exception {
        checkExecuteTaskAfterRestart(true, 3L, n -> {
            n.cluster().state(INACTIVE);

            return n;
        });
    }

    /**
     * Checking that the {@link DurableBackgroundCleanupIndexTreeTask} will
     * work correctly for in-memory cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdxForInMemory() throws Exception {
        checkExecuteTask(false, 2L);
    }

    /**
     * Checking that when the node is re-activated, the
     * {@link DurableBackgroundCleanupIndexTreeTask} will finish correctly for in-memory cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdxAfterReActivatedForInMemory() throws Exception {
        checkExecuteTaskAfterRestart(false, null, n -> {
            n.cluster().state(INACTIVE);

            return n;
        });
    }

    /**
     * Checking {@link DurableBackgroundCleanupIndexTreeTask#destroyIndexTrees} for in-memory cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyIndexTreesForInMemory() throws Exception {
        checkDestroyIndexTrees(false, 2);
    }

    /**
     * Checks that {@link DurableBackgroundCleanupIndexTreeTask} will not be
     * added when the cluster is deactivated for in-memory caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDonotAddTaskOnDeactivateForInMemory() throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(false);
        });

        n.cluster().state(ACTIVE);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        n.cluster().state(INACTIVE);

        assertTrue(tasks(n).isEmpty());
    }

    /**
     * Getting index trees.
     *
     * @param idx Index.
     * @return Index trees.
     */
    private InlineIndexTree[] segments(Index idx) {
        return getFieldValue(idx, "segments");
    }

    /**
     * Getting task state.
     *
     * @param n Node.
     * @param taskNamePrefix Task name prefix;
     * @return Task state.
     */
    @Nullable private DurableBackgroundTaskState<?> taskState(IgniteEx n, String taskNamePrefix) {
        return tasks(n).entrySet().stream()
            .filter(e -> e.getKey().startsWith(taskNamePrefix)).map(Map.Entry::getValue).findAny().orElse(null);
    }

    /**
     * Getting {@code DurableBackgroundTasksProcessor#tasks}.
     *
     * @return Tasks.
     */
    private Map<String, DurableBackgroundTaskState<?>> tasks(IgniteEx n) {
        return getFieldValue(n.context().durableBackgroundTask(), "tasks");
    }

    /**
     * Getting {@link DurableBackgroundCleanupIndexTreeTask} name prefix.
     *
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @return Task prefix;
     */
    private String taskNamePrefix(String cacheName, String idxName) {
        return "drop-sql-index-" + cacheName + "-" + idxName + "-";
    }

    /**
     * Creating an extension for {@link InlineIndexTreeFactory}.
     *
     * @param startFut Future to indicate that the tree for the tak has begun to be created.
     * @param endFut Future to wait for the continuation of the tree creation for the task.
     * @return Extending the {@link InlineIndexTreeFactory}.
     */
    private InlineIndexTreeFactory taskIndexTreeFactoryEx(
        GridFutureAdapter<Void> startFut,
        GridFutureAdapter<Void> endFut
    ) {
        return new InlineIndexTreeFactory() {
            /** {@inheritDoc} */
            @Override protected InlineIndexTree create(
                CacheGroupContext grpCtx,
                RootPage rootPage,
                String treeName
            ) throws IgniteCheckedException {
                startFut.onDone();

                endFut.get(getTestTimeout());

                return super.create(grpCtx, rootPage, treeName);
            }
        };
    }

    /**
     * Drop of an index for the cache of{@link Person}.
     * SQL: DROP INDEX " + idxName
     *
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index creation future.
     */
    private List<List<?>> dropIdx(IgniteCache<Integer, Person> cache, String idxName) {
        return cache.query(new SqlFieldsQuery("DROP INDEX " + idxName)).getAll();
    }

    /**
     * Check that the {@link DurableBackgroundCleanupIndexTreeTask} will be completed successfully.
     *
     * @param persistent Persistent default data region.
     * @param expRes Expected result should not be less than which.
     * @throws Exception If failed.
     */
    private void checkExecuteTask(
        boolean persistent,
        long expRes
    ) throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(persistent);
        });

        n.cluster().state(ACTIVE);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        GridFutureAdapter<Void> startFut = new GridFutureAdapter<>();
        GridFutureAdapter<Void> endFut = new GridFutureAdapter<>();

        idxTreeFactory = taskIndexTreeFactoryEx(startFut, endFut);

        IgniteInternalFuture<List<List<?>>> dropIdxFut = runAsync(() -> dropIdx(cache, idxName));

        startFut.get(getTestTimeout());

        GridFutureAdapter<?> taskFut = taskState(n, taskNamePrefix(DEFAULT_CACHE_NAME, idxName)).outFuture();
        assertFalse(taskFut.isDone());

        endFut.onDone();

        assertTrue((Long)taskFut.get(getTestTimeout()) >= expRes);

        dropIdxFut.get(getTestTimeout());
    }

    /**
     * Check that after restart / reactivation of the node,
     * the {@link DurableBackgroundCleanupIndexTreeTask} will be completed successfully.
     *
     * @param persistent Persistent default data region.
     * @param expRes Expected result should not be less than which, or {@code null} if there should be no result.
     * @param restartFun Node restart/reactivation function.
     * @throws Exception If failed.
     */
    private void checkExecuteTaskAfterRestart(
        boolean persistent,
        @Nullable Long expRes,
        ThrowableFunction<IgniteEx, IgniteEx, Exception> restartFun
    ) throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(persistent);
        });

        n.cluster().state(ACTIVE);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        GridFutureAdapter<Void> startFut = new GridFutureAdapter<>();
        GridFutureAdapter<Void> endFut = new GridFutureAdapter<>();

        idxTreeFactory = taskIndexTreeFactoryEx(startFut, endFut);

        endFut.onDone(new IgniteCheckedException("Stop drop idx"));

        // Removing the index will succeed, but the trees not.
        dropIdx(cache, idxName);

        String taskNamePrefix = taskNamePrefix(cacheContext(cache).name(), idxName);
        assertFalse(taskState(n, taskNamePrefix).outFuture().isDone());

        n = restartFun.apply(n);

        idxTreeFactory = originalTaskIdxTreeFactory;

        GridFutureAdapter<?> taskFut = taskState(n, taskNamePrefix).outFuture();
        assertFalse(taskFut.isDone());

        n.cluster().state(ACTIVE);

        if (expRes == null)
            assertNull(taskFut.get(getTestTimeout()));
        else
            assertTrue((Long)taskFut.get(getTestTimeout()) >= expRes);
    }

    /**
     * Checking {@link DurableBackgroundCleanupIndexTreeTask#destroyIndexTrees}.
     *
     * @param persistent Persistent default data region.
     * @param expRes Expected result should not be less than which.
     * @throws Exception If failed.
     */
    private void checkDestroyIndexTrees(
        boolean persistent,
        long expRes
    ) throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(persistent);
        });

        n.cluster().state(ACTIVE);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        GridCacheContext<Integer, Person> cctx = cacheContext(cache);

        Index idx = index(n, cache, idxName);

        SortedIndexDefinition idxDef = indexDefinition(idx);

        String treeName = idxDef.treeName();
        int segments = idxDef.segments();

        Map<Integer, RootPage> rootPages = new HashMap<>();

        if (persistent)
            rootPages.putAll(findIndexRootPages(cctx.group(), cctx.name(), treeName, segments));
        else
            rootPages.putAll(toRootPages(segments(idx)));

        assertFalse(rootPages.isEmpty());

        // Emulating worker cancellation, let's make sure it doesn't cause problems.
        Thread.currentThread().interrupt();

        long pageCnt = 0;

        for (Map.Entry<Integer, RootPage> e : rootPages.entrySet())
            pageCnt += destroyIndexTrees(cctx.group(), e.getValue(), cctx.name(), treeName, e.getKey());

        assertTrue(pageCnt >= expRes);
        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), treeName, segments).isEmpty());
    }
}
