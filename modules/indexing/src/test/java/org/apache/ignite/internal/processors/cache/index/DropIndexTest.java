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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.InlineIndexTreeFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.IDX_TREE_FACTORY;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.destroyIndexTrees;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.findIndexRootPages;
import static org.apache.ignite.testframework.GridTestUtils.cacheContext;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Class for testing index drop.
 */
@WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "1000000")
public class DropIndexTest extends AbstractRebuildIndexTest {
    /** Original index tree factory. */
    private InlineIndexTreeFactory originalIdxTreeFactory;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        originalIdxTreeFactory = IDX_TREE_FACTORY;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IDX_TREE_FACTORY = originalIdxTreeFactory;
    }

    /** {@inheritDoc} */
    @Override protected void populate(IgniteCache<Integer, Person> cache, int cnt) {
        String prefix = IntStream.range(0, 1_000).mapToObj(i -> "name").collect(joining("_")) + "_";

        for (int i = 0; i < cnt; i++)
            cache.put(i, new Person(i, prefix + i));
    }

    /**
     * Checking {@link DurableBackgroundCleanupIndexTreeTaskV2#destroyIndexTrees}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyIndexTrees() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        GridCacheContext<Integer, Person> cctx = cacheContext(cache);
        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        String treeName = idxDef.treeName();
        int segments = idxDef.segments();

        Map<Integer, RootPage> rootPages = findIndexRootPages(cctx.group(), cctx.name(), treeName, segments);
        assertFalse(rootPages.isEmpty());

        assertTrue(destroyIndexTrees(cctx.group(), rootPages, cctx.name(), treeName, idxName) >= 3);
        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), treeName, segments).isEmpty());
    }

    /**
     * Check that the {@link DurableBackgroundCleanupIndexTreeTaskV2} will not
     * be executed if the cache group and root pages are not found.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTaskNotExecuteIfAbsentCacheGroupOrRootPages() throws Exception {
        IgniteEx n = startGrid(0);

        String fake = IntStream.generate(() -> ThreadLocalRandom.current().nextInt())
            .filter(i -> n.context().cache().cacheGroup(i) == null)
            .mapToObj(String::valueOf)
            .findAny().get();

        GridCacheContext<Integer, Person> cctx = cacheContext(n.cache(DEFAULT_CACHE_NAME));

        List<DurableBackgroundCleanupIndexTreeTaskV2> tasks = F.asList(
            new DurableBackgroundCleanupIndexTreeTaskV2(fake, fake, fake, fake, fake, 10),
            new DurableBackgroundCleanupIndexTreeTaskV2(cctx.group().name(), cctx.name(), fake, fake, fake, 10)
        );

        for (DurableBackgroundCleanupIndexTreeTaskV2 task : tasks) {
            DurableBackgroundTaskResult<Long> res = task.executeAsync(n.context()).get(0);

            assertTrue(res.completed());
            assertNull(res.error());
            assertNull(res.result());
        }
    }

    /**
     * Checking that the {@link DurableBackgroundCleanupIndexTreeTaskV2} will work correctly.
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
        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        String oldTreeName = idxDef.treeName();
        String newTreeName = UUID.randomUUID().toString();
        int segments = idxDef.segments();

        assertFalse(findIndexRootPages(cctx.group(), cctx.name(), oldTreeName, segments).isEmpty());
        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), newTreeName, segments).isEmpty());

        DurableBackgroundCleanupIndexTreeTaskV2 task = new DurableBackgroundCleanupIndexTreeTaskV2(
            cctx.group().name(),
            cctx.name(),
            idxName,
            oldTreeName,
            newTreeName,
            segments
        );

        assertTrue(task.name().startsWith(taskNamePrefix(cctx.name(), idxName)));
        assertTrue(getFieldValue(task, "needToRen"));

        GridFutureAdapter<Void> startFut = new GridFutureAdapter<>();
        GridFutureAdapter<Void> endFut = new GridFutureAdapter<>();

        IDX_TREE_FACTORY = indexTreeFactoryEx(startFut, endFut, false);

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
     * Checking that the {@link DurableBackgroundCleanupIndexTreeTaskV2} will
     * run when the index drop is called.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdx() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        GridFutureAdapter<Void> startFut = new GridFutureAdapter<>();
        GridFutureAdapter<Void> endFut = new GridFutureAdapter<>();

        IDX_TREE_FACTORY = indexTreeFactoryEx(startFut, endFut, false);

        IgniteInternalFuture<List<List<?>>> dropIdxFut = runAsync(() -> dropIdx(cache, idxName));

        startFut.get(getTestTimeout());

        assertFalse(dropIdxFut.isDone());

        endFut.onDone();

        dropIdxFut.get(getTestTimeout());
    }

    /**
     * Checking that when the node is restarted, the
     * {@link DurableBackgroundCleanupIndexTreeTaskV2} will finish correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskOnDropIdxAfterRestart() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        IDX_TREE_FACTORY = indexTreeFactoryEx(null, null, true);

        // Removing the index will succeed, but the trees not.
        dropIdx(cache, idxName);

        String taskNamePrefix = taskNamePrefix(cacheContext(cache).name(), idxName);
        assertFalse(taskState(n, taskNamePrefix).outFuture().isDone());

        // Disable auto activation.
        n.cluster().baselineAutoAdjustEnabled(false);
        stopGrid(0);

        n = startGrid(0, cfg -> {
            cfg.setClusterStateOnStart(INACTIVE);
        });

        IDX_TREE_FACTORY = originalIdxTreeFactory;

        GridFutureAdapter<?> taskFut = taskState(n, taskNamePrefix).outFuture();
        assertFalse(taskFut.isDone());

        n.cluster().state(ACTIVE);

        assertTrue((Long)taskFut.get(getTestTimeout()) >= 3);
    }

    /**
     * Getting task state.
     *
     * @param n Node.
     * @param taskNamePrefix Task name prefix;
     * @return Task state.
     */
    @Nullable private DurableBackgroundTaskState<?> taskState(IgniteEx n, String taskNamePrefix) {
        Map<String, DurableBackgroundTaskState<?>> tasks = getFieldValue(n.context().durableBackgroundTask(), "tasks");

        return tasks.entrySet().stream()
            .filter(e -> e.getKey().startsWith(taskNamePrefix)).map(Map.Entry::getValue).findAny().orElse(null);
    }

    /**
     * Getting {@link DurableBackgroundCleanupIndexTreeTaskV2} name prefix.
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
     * @param throwEx Throw an exception.
     * @return Extending the {@link InlineIndexTreeFactory}.
     */
    private InlineIndexTreeFactory indexTreeFactoryEx(
        @Nullable GridFutureAdapter<Void> startFut,
        @Nullable GridFutureAdapter<Void> endFut,
        boolean throwEx
    ) {
        return new InlineIndexTreeFactory() {
            /** {@inheritDoc} */
            @Override protected InlineIndexTree create(
                CacheGroupContext grpCtx,
                RootPage rootPage,
                String treeName
            ) throws IgniteCheckedException {
                if (throwEx)
                    throw new IgniteCheckedException("From test");
                else {
                    assertNotNull(startFut);
                    assertNotNull(endFut);

                    startFut.onDone();

                    endFut.get(getTestTimeout());
                }

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
}
