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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.StopBuildIndexConsumer;
import org.apache.ignite.internal.processors.query.aware.IndexRebuildFutureStorage;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.prepareBeforeNodeStart;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing forced rebuilding of indexes.
 */
public class ForceRebuildIndexTest extends AbstractRebuildIndexTest {
    /**
     * Checking that a forced rebuild of indexes is possible only after the previous one has finished.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequentialForceRebuildIndexes() throws Exception {
        prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 100);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());

        // The forced rebuild has begun - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        assertFalse(idxRebFut0.isDone());

        // There will be no forced rebuilding since the previous one has not ended - they will be rejected.
        assertEqualsCollections(F.asList(cacheCtx), forceRebuildIndexes(n, cacheCtx));
        assertTrue(idxRebFut0 == indexRebuildFuture(n, cacheCtx.cacheId()));

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut0.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());

        stopRebuildIdxConsumer.resetFutures();

        // Forced rebuilding is possible again as the past is over - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(200, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Checking that a forced index rebuild can only be performed after an index rebuild after an exchange.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testForceRebuildIndexesAfterExchange() throws Exception {
        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 100);

        stopAllGridsWithDeleteIndexBin();

        prepareBeforeNodeStart();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, DEFAULT_CACHE_NAME);

        n = startGrid(0);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), true);

        // There will be no forced rebuilding of indexes since it has not ended after the exchange - they will be rejected.
        assertEqualsCollections(F.asList(cacheCtx), forceRebuildIndexes(n, cacheCtx));
        assertTrue(idxRebFut0 == indexRebuildFuture(n, cacheCtx.cacheId()));
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), true);

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut0.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);

        stopRebuildIdxConsumer.resetFutures();

        // A forced index rebuild will be triggered because it has ended after the exchange - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);
        assertEquals(200, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Checking that sequential index rebuilds on exchanges will not intersection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequentialRebuildIndexesOnExchange() throws Exception {
        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 100);

        stopAllGridsWithDeleteIndexBin();

        prepareBeforeNodeStart();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, DEFAULT_CACHE_NAME);

        n = startGrid(0);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut = checkStartRebuildIndexes(n, cacheCtx);

        // To initiate an exchange.
        n.getOrCreateCache(DEFAULT_CACHE_NAME + "_1");

        assertTrue(idxRebFut == indexRebuildFuture(n, cacheCtx.cacheId()));

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Checking the contents of the cache in {@code GridQueryProcessor#idxRebuildOnExchange}.
     * Allows to check if the cache will be marked, that the rebuild for it should be after the exchange.
     *
     * @param n Node.
     * @param cacheId Cache id.
     * @param expContains Whether a cache is expected.
     */
    private void checkRebuildAfterExchange(IgniteEx n, int cacheId, boolean expContains) {
        IndexRebuildFutureStorage idxRebuildAware = getFieldValue(n.context().query(), "idxRebuildFutStorage");

        GridDhtPartitionsExchangeFuture exhFut = n.context().cache().context().exchange().lastTopologyFuture();

        assertEquals(expContains, idxRebuildAware.rebuildIndexesOnExchange(cacheId, exhFut.initialVersion()));
    }
}
