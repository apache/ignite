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

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.BreakRebuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.StopRebuildIndexConsumer;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.prepareBeforeNodeStart;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing rebuilding index resumes.
 */
public class ResumeRebuildIndexTest extends AbstractRebuildIndexTest {
    @Test
    public void test1() throws Exception {
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

        boolean restart = false;

        if (restart) {
            stopAllGrids();

            prepareBeforeNodeStart();
            n = startGrid(0);
        }
        else {
            n.cluster().state(ClusterState.INACTIVE);
            n.cluster().state(ClusterState.ACTIVE);
        }

        IgniteInternalFuture<?> rebIdxFut1 = indexRebuildFuture(n, cacheCtx.cacheId());

        stopRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());
        stopRebuildIdxConsumer.finishRebuildIdxFut.onDone();

        rebIdxFut1.get(getTestTimeout());
        assertEquals(10_000, stopRebuildIdxConsumer.visitCnt.get());
    }
}
