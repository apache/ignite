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
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.prepareBeforeNodeStart;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing rebuilding index resumes.
 */
public class ResumeRebuildIndexTest extends AbstractRebuildIndexTest {
    @Test
    public void test0() throws Exception {
        IgniteEx n = prepareCluster(100);

        GridCacheContext<Object, Object> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopAllGridsWithDeleteIndexBin();

        BreakRebuildIndexConsumer breakRebuildIdxConsumer = addBreakRebuildIndexConsumer(
            n,
            cacheCtx.name(),
            (c, r) -> c.visitCnt.get() >= 10
        );

        prepareBeforeNodeStart();

        n = startGrid(0);

        breakRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);

        breakRebuildIdxConsumer.finishRebuildIdxFut.onDone();
        assertThrows(log, () -> idxRebFut0.get(getTestTimeout()), Throwable.class, null);

        assertTrue(cacheMetrics0(n, DEFAULT_CACHE_NAME).getIndexRebuildKeysProcessed() < 100);
        assertTrue(breakRebuildIdxConsumer.visitCnt.get() < 100);

        stopAllGrids();

        StopRebuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());

        prepareBeforeNodeStart();

        n = startGrid(0);

        stopRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.finishRebuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
    }
}
