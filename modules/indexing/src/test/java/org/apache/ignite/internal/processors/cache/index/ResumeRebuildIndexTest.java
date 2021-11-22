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

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.BreakBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.StopBuildIndexConsumer;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage;
import org.apache.ignite.internal.util.function.ThrowableFunction;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.prepareBeforeNodeStart;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage.KEY_PREFIX;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.deleteCacheGrpDir;

/**
 * Class for testing rebuilding index resumes.
 */
public class ResumeRebuildIndexTest extends AbstractRebuildIndexTest {
    /**
     * Checking normal flow for {@link IndexBuildStatusStorage}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNormalFlowIndexRebuildStateStorage() throws Exception {
        prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 1_000);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());

        dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

        assertTrue(forceRebuildIndexes(n, cacheCtx).isEmpty());
        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, cacheCtx.cacheId());

        assertFalse(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut.get(getTestTimeout());

        assertEquals(1_000, stopRebuildIdxConsumer.visitCnt.get());
        assertTrue(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));

        dbMgr(n).enableCheckpoints(true).get(getTestTimeout());
        forceCheckpoint();

        assertNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));
    }

    /**
     * Checking the flow in case of an error for {@link IndexBuildStatusStorage}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testErrorFlowIndexRebuildStateStorage() throws Exception {
        prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 1_000);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        BreakBuildIndexConsumer breakRebuildIdxConsumer = addBreakRebuildIndexConsumer(n, cacheCtx.name(), 10);

        assertTrue(forceRebuildIndexes(n, cacheCtx).isEmpty());
        IgniteInternalFuture<?> idxRebFut0 = indexRebuildFuture(n, cacheCtx.cacheId());

        assertFalse(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        breakRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        breakRebuildIdxConsumer.finishBuildIdxFut.onDone();

        assertThrows(log, () -> idxRebFut0.get(getTestTimeout()), Throwable.class, null);
        assertTrue(breakRebuildIdxConsumer.visitCnt.get() < 1_000);

        forceCheckpoint();

        assertFalse(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());
        dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

        assertTrue(forceRebuildIndexes(n, cacheCtx).isEmpty());
        IgniteInternalFuture<?> idxRebFut1 = indexRebuildFuture(n, cacheCtx.cacheId());

        assertFalse(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut1.get(getTestTimeout());

        assertEquals(1_000, stopRebuildIdxConsumer.visitCnt.get());
        assertTrue(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));

        dbMgr(n).enableCheckpoints(true).get(getTestTimeout());
        forceCheckpoint();

        assertNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));
    }

    /**
     * Checking the flow in case of an restart node for {@link IndexBuildStatusStorage}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartNodeFlowIndexRebuildStateStorage() throws Exception {
        prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 1_000);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        BreakBuildIndexConsumer breakRebuildIdxConsumer = addBreakRebuildIndexConsumer(n, cacheCtx.name(), 10);

        assertTrue(forceRebuildIndexes(n, cacheCtx).isEmpty());
        IgniteInternalFuture<?> idxRebFut0 = indexRebuildFuture(n, cacheCtx.cacheId());

        breakRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        breakRebuildIdxConsumer.finishBuildIdxFut.onDone();

        assertThrows(log, () -> idxRebFut0.get(getTestTimeout()), Throwable.class, null);

        forceCheckpoint();

        assertFalse(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        stopAllGrids();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());

        prepareBeforeNodeStart();
        n = startGrid(0);

        assertFalse(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        IgniteInternalFuture<?> idxRebFut1 = indexRebuildFuture(n, cacheCtx.cacheId());

        dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        assertEquals(1_000, stopRebuildIdxConsumer.visitCnt.get());
        assertTrue(indexBuildStatusStorage(n).rebuildCompleted(cacheCtx.name()));
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));

        dbMgr(n).enableCheckpoints(true).get(getTestTimeout());
        forceCheckpoint();

        assertNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheCtx.name())));
    }

    /**
     * Checks that rebuilding indexes will be automatically started after
     * restarting the node due to the fact that the previous one did not
     * complete successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeRestart() throws Exception {
        checkRestartRebuildIndexes(1, n -> {
            stopAllGrids();

            prepareBeforeNodeStart();

            return startGrid(0);
        });
    }

    /**
     * Checks that rebuilding indexes will be automatically started after
     * reactivation the node due to the fact that the previous one did not
     * complete successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeReactivation() throws Exception {
        checkRestartRebuildIndexes(1, n -> {
            n.cluster().state(INACTIVE);

            n.cluster().state(ACTIVE);

            return n;
        });
    }

    /**
     * Checks that rebuilding indexes will be automatically started after
     * restarting the node due to the fact that the previous one did not
     * complete successfully. Two-node cluster, only one node will be restarted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodeRestart() throws Exception {
        checkRestartRebuildIndexes(2, n -> {
            String nodeName = n.name();

            stopGrid(nodeName);

            prepareBeforeNodeStart();

            return startGrid(nodeName);
        });
    }

    /**
     * Checks that rebuilding indexes will be automatically started after
     * reactivation the node due to the fact that the previous one did not
     * complete successfully. Two-node cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodeReactivation() throws Exception {
        checkRestartRebuildIndexes(2, n -> {
            n.cluster().state(INACTIVE);

            n.cluster().state(ACTIVE);

            return n;
        });
    }

    /**
     * Checks that when the caches are destroyed,
     * the index rebuild states will also be deleted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteIndexRebuildStateOnDestroyCache() throws Exception {
        IgniteEx n0 = startGrid(getTestIgniteInstanceName(0));

        prepareBeforeNodeStart();
        IgniteEx n1 = startGrid(getTestIgniteInstanceName(1));

        n0.cluster().state(ACTIVE);
        awaitPartitionMapExchange();

        for (int i = 0; i < 4; i++) {
            String cacheName = DEFAULT_CACHE_NAME + i;

            String grpName = i == 1 || i == 2 ? DEFAULT_CACHE_NAME + "_G" : null;

            populate(n1.getOrCreateCache(cacheCfg(cacheName, grpName)), 10_000);

            BreakBuildIndexConsumer breakRebuildIdxConsumer = addBreakRebuildIndexConsumer(n1, cacheName, 10);

            IgniteInternalCache<?, ?> cachex = n1.cachex(cacheName);

            int cacheSize = cachex.size();

            assertTrue(forceRebuildIndexes(n1, cachex.context()).isEmpty());

            IgniteInternalFuture<?> rebIdxFut = indexRebuildFuture(n1, cachex.context().cacheId());

            breakRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
            breakRebuildIdxConsumer.finishBuildIdxFut.onDone();

            assertThrows(log, () -> rebIdxFut.get(getTestTimeout()), Throwable.class, null);
            assertTrue(breakRebuildIdxConsumer.visitCnt.get() < cacheSize);
        }

        n1.destroyCache(DEFAULT_CACHE_NAME + 0);
        assertTrue(indexBuildStatusStorage(n1).rebuildCompleted(DEFAULT_CACHE_NAME + 0));

        n1.destroyCache(DEFAULT_CACHE_NAME + 1);
        assertTrue(indexBuildStatusStorage(n1).rebuildCompleted(DEFAULT_CACHE_NAME + 1));

        assertFalse(indexBuildStatusStorage(n1).rebuildCompleted(DEFAULT_CACHE_NAME + 2));
        assertFalse(indexBuildStatusStorage(n1).rebuildCompleted(DEFAULT_CACHE_NAME + 3));

        forceCheckpoint(n1);

        ConcurrentMap<String, IndexBuildStatusHolder> states = statuses(n1);

        assertFalse(states.containsKey(DEFAULT_CACHE_NAME + 0));
        assertFalse(states.containsKey(DEFAULT_CACHE_NAME + 1));

        assertTrue(states.containsKey(DEFAULT_CACHE_NAME + 2));
        assertTrue(states.containsKey(DEFAULT_CACHE_NAME + 3));

        stopGrid(1);
        awaitPartitionMapExchange();

        n0.destroyCache(DEFAULT_CACHE_NAME + 2);
        n0.destroyCache(DEFAULT_CACHE_NAME + 3);

        deleteCacheGrpDir(
            n1.name(),
            (dir, name) -> name.contains(DEFAULT_CACHE_NAME + 3) || name.contains(DEFAULT_CACHE_NAME + "_G")
        );

        n1 = startGrid(getTestIgniteInstanceName(1));

        assertTrue(indexBuildStatusStorage(n1).rebuildCompleted(DEFAULT_CACHE_NAME + 2));
        assertTrue(indexBuildStatusStorage(n1).rebuildCompleted(DEFAULT_CACHE_NAME + 3));

        forceCheckpoint(n1);

        states = statuses(n1);

        assertFalse(states.containsKey(DEFAULT_CACHE_NAME + 2));
        assertFalse(states.containsKey(DEFAULT_CACHE_NAME + 3));
    }

    /**
     * Check that for node the index rebuilding will be restarted
     * automatically after executing the function on the node.
     *
     * @param nodeCnt Node count.
     * @param function Function for node.
     * @throws Exception If failed.
     */
    private void checkRestartRebuildIndexes(
        int nodeCnt,
        ThrowableFunction<IgniteEx, IgniteEx, Exception> function
    ) throws Exception {
        assertTrue(nodeCnt > 0);

        for (int i = 0; i < nodeCnt - 1; i++)
            startGrid(getTestIgniteInstanceName(i + 1));

        prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 10_000);

        populate(n.getOrCreateCache(cacheCfg(DEFAULT_CACHE_NAME + 0, null)), 10_000);

        if (nodeCnt > 1)
            awaitPartitionMapExchange();

        IgniteInternalCache<?, ?> cachex0 = n.cachex(DEFAULT_CACHE_NAME);
        IgniteInternalCache<?, ?> cachex1 = n.cachex(DEFAULT_CACHE_NAME + 0);

        int cacheSize0 = cachex0.size();
        int cacheSize1 = cachex1.size();
        assertTrue(String.valueOf(cacheSize0), cacheSize0 >= 1_000);
        assertTrue(String.valueOf(cacheSize1), cacheSize1 >= 1_000);

        GridCacheContext<?, ?> cacheCtx0 = cachex0.context();
        GridCacheContext<?, ?> cacheCtx1 = cachex1.context();

        BreakBuildIndexConsumer breakRebuildIdxConsumer = addBreakRebuildIndexConsumer(n, cacheCtx0.name(), 10);

        StopBuildIndexConsumer stopRebuildIdxConsumer0 = addStopRebuildIndexConsumer(n, cacheCtx1.name());

        assertTrue(forceRebuildIndexes(n, cacheCtx0, cacheCtx1).isEmpty());

        IgniteInternalFuture<?> rebIdxFut0 = indexRebuildFuture(n, cacheCtx0.cacheId());
        IgniteInternalFuture<?> rebIdxFut1 = indexRebuildFuture(n, cacheCtx1.cacheId());

        breakRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        breakRebuildIdxConsumer.finishBuildIdxFut.onDone();

        stopRebuildIdxConsumer0.startBuildIdxFut.get(getTestTimeout());
        stopRebuildIdxConsumer0.finishBuildIdxFut.onDone();

        assertThrows(log, () -> rebIdxFut0.get(getTestTimeout()), Throwable.class, null);
        assertTrue(breakRebuildIdxConsumer.visitCnt.get() < cacheSize0);

        rebIdxFut1.get(getTestTimeout());
        assertEquals(cacheSize1, stopRebuildIdxConsumer0.visitCnt.get());

        StopBuildIndexConsumer stopRebuildIdxConsumer1 = addStopRebuildIndexConsumer(n, cacheCtx0.name());
        stopRebuildIdxConsumer0.resetFutures();

        forceCheckpoint();

        n = function.apply(n);

        IgniteInternalFuture<?> rebIdxFut01 = indexRebuildFuture(n, cacheCtx0.cacheId());
        IgniteInternalFuture<?> rebIdxFut11 = indexRebuildFuture(n, cacheCtx1.cacheId());

        stopRebuildIdxConsumer1.startBuildIdxFut.get(getTestTimeout());
        stopRebuildIdxConsumer1.finishBuildIdxFut.onDone();

        assertThrows(
            log,
            () -> stopRebuildIdxConsumer0.startBuildIdxFut.get(1_000),
            IgniteFutureTimeoutCheckedException.class,
            null
        );
        stopRebuildIdxConsumer0.finishBuildIdxFut.onDone();

        rebIdxFut01.get(getTestTimeout());
        assertEquals(cacheSize0, stopRebuildIdxConsumer1.visitCnt.get());

        assertNull(rebIdxFut11);
        assertEquals(cacheSize1, stopRebuildIdxConsumer0.visitCnt.get());
    }
}
