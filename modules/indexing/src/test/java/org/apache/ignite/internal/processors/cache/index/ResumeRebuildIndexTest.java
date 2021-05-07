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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.BreakRebuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.StopRebuildIndexConsumer;
import org.apache.ignite.internal.util.function.ThrowableFunction;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.prepareBeforeNodeStart;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing rebuilding index resumes.
 */
public class ResumeRebuildIndexTest extends AbstractRebuildIndexTest {
    /**
     * Checks that rebuilding indexes will be automatically started after
     * restarting the node due to the fact that the previous one did not
     * complete successfully. One node cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeRestart() throws Exception {
        checkSingleNode(n -> {
            stopAllGrids();

            prepareBeforeNodeStart();

            return startGrid(0);
        });
    }

    /**
     * Checks that rebuilding indexes will be automatically started after
     * reactivation the node due to the fact that the previous one did not
     * complete successfully. One node cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeReactivation() throws Exception {
        checkSingleNode(n -> {
            n.cluster().state(INACTIVE);

            n.cluster().state(ACTIVE);

            return n;
        });
    }

    /**
     * Check that for one node the index rebuilding will be restarted
     * automatically after executing the function on the node.
     *
     * @param function Function for node.
     * @throws Exception If failed.
     */
    private void checkSingleNode(ThrowableFunction<IgniteEx, IgniteEx, Exception> function) throws Exception {
        prepareBeforeNodeStart();

        IgniteEx n = prepareCluster(10_000);

        GridCacheContext<Object, Object> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        BreakRebuildIndexConsumer breakRebuildIdxConsumer =
            addBreakRebuildIndexConsumer(n, cacheCtx.name(), (c, row) -> c.visitCnt.get() >= 10);

        assertTrue(forceRebuildIndexes(n, cacheCtx).isEmpty());

        IgniteInternalFuture<?> rebIdxFut0 = indexRebuildFuture(n, cacheCtx.cacheId());

        breakRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());
        breakRebuildIdxConsumer.finishRebuildIdxFut.onDone();

        assertThrows(log, () -> rebIdxFut0.get(getTestTimeout()), Throwable.class, null);
        assertTrue(breakRebuildIdxConsumer.visitCnt.get() < 10_000);

        StopRebuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());

        n = function.apply(n);

        IgniteInternalFuture<?> rebIdxFut1 = indexRebuildFuture(n, cacheCtx.cacheId());

        stopRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());
        stopRebuildIdxConsumer.finishRebuildIdxFut.onDone();

        rebIdxFut1.get(getTestTimeout());
        assertEquals(10_000, stopRebuildIdxConsumer.visitCnt.get());
    }
}
