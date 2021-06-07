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

import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.StopBuildIndexConsumer;
import org.apache.ignite.internal.processors.query.IndexRebuildAware;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.addCacheRowConsumer;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.nodeName;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing forced rebuilding of indexes.
 */
public class ForceRebuildIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Person.class)
            );
    }

    /**
     * Checking that a forced rebuild of indexes is possible only after the previous one has finished.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequentialForceRebuildIndexes() throws Exception {
        IndexProcessor.idxRebuildCls = IndexesRebuildTaskEx.class;

        IgniteEx n = prepareCluster(100);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        StopBuildIndexConsumer stopRebuildIdxConsumer = new StopBuildIndexConsumer(getTestTimeout());
        addCacheRowConsumer(nodeName(n), cacheCtx.name(), stopRebuildIdxConsumer);

        // The forced rebuild has begun - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.startFut.get(getTestTimeout());
        assertFalse(idxRebFut0.isDone());

        // There will be no forced rebuilding since the previous one has not ended - they will be rejected.
        assertEqualsCollections(F.asList(cacheCtx), forceRebuildIndexes(n, cacheCtx));
        assertTrue(idxRebFut0 == indexRebuildFuture(n, cacheCtx.cacheId()));

        stopRebuildIdxConsumer.finishFut.onDone();

        idxRebFut0.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());

        stopRebuildIdxConsumer.resetFutures();

        // Forced rebuilding is possible again as the past is over - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.startFut.get(getTestTimeout());
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishFut.onDone();
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
        IgniteEx n = prepareCluster(100);

        stopAllGrids();
        deleteIndexBin(n.context().igniteInstanceName());

        IndexProcessor.idxRebuildCls = IndexesRebuildTaskEx.class;

        StopBuildIndexConsumer stopRebuildIdxConsumer = new StopBuildIndexConsumer(getTestTimeout());
        addCacheRowConsumer(nodeName(n), DEFAULT_CACHE_NAME, stopRebuildIdxConsumer);

        n = startGrid(0);
        n.cluster().state(ACTIVE);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopRebuildIdxConsumer.startFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), true);

        // There will be no forced rebuilding of indexes since it has not ended after the exchange - they will be rejected.
        assertEqualsCollections(F.asList(cacheCtx), forceRebuildIndexes(n, cacheCtx));
        assertTrue(idxRebFut0 == indexRebuildFuture(n, cacheCtx.cacheId()));
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), true);

        stopRebuildIdxConsumer.finishFut.onDone();

        idxRebFut0.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);

        stopRebuildIdxConsumer.resetFutures();

        // A forced index rebuild will be triggered because it has ended after the exchange - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);

        stopRebuildIdxConsumer.startFut.get(getTestTimeout());
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishFut.onDone();
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
        IgniteEx n = prepareCluster(100);

        stopAllGrids();
        deleteIndexBin(n.context().igniteInstanceName());

        IndexProcessor.idxRebuildCls = IndexesRebuildTaskEx.class;

        StopBuildIndexConsumer stopRebuildIdxConsumer = new StopBuildIndexConsumer(getTestTimeout());
        addCacheRowConsumer(nodeName(n), DEFAULT_CACHE_NAME, stopRebuildIdxConsumer);

        n = startGrid(0);
        n.cluster().state(ACTIVE);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopRebuildIdxConsumer.startFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut = checkStartRebuildIndexes(n, cacheCtx);

        // To initiate an exchange.
        n.getOrCreateCache(DEFAULT_CACHE_NAME + "_1");

        assertTrue(idxRebFut == indexRebuildFuture(n, cacheCtx.cacheId()));

        stopRebuildIdxConsumer.finishFut.onDone();

        idxRebFut.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Prepare cluster for test.
     *
     * @param keys Key count.
     * @return Coordinator.
     * @throws Exception If failed.
     */
    private IgniteEx prepareCluster(int keys) throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);

        for (int i = 0; i < keys; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new Person(i, "name_" + i));

        return n;
    }

    /**
     * Checking metrics rebuilding indexes of cache.
     *
     * @param n                          Node.
     * @param cacheName                  Cache name.
     * @param expIdxRebuildInProgress    The expected status of rebuilding indexes.
     * @param expIdxRebuildKeysProcessed The expected number of keys processed during index rebuilding.
     */
    private void checkCacheMetrics0(
        IgniteEx n,
        String cacheName,
        boolean expIdxRebuildInProgress,
        long expIdxRebuildKeysProcessed
    ) {
        CacheMetricsImpl metrics0 = cacheMetrics0(n, cacheName);
        assertNotNull(metrics0);

        assertEquals(expIdxRebuildInProgress, metrics0.isIndexRebuildInProgress());
        assertEquals(expIdxRebuildKeysProcessed, metrics0.getIndexRebuildKeysProcessed());
    }

    /**
     * Checking that rebuilding indexes for the cache has started.
     *
     * @param n Node.
     * @param cacheCtx Cache context.
     * @return Rebuild index future.
     */
    private IgniteInternalFuture<?> checkStartRebuildIndexes(IgniteEx n, GridCacheContext<?, ?> cacheCtx) {
        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, cacheCtx.cacheId());

        assertNotNull(idxRebFut);
        assertFalse(idxRebFut.isDone());

        checkCacheMetrics0(n, cacheCtx.name(), true, 0);

        return idxRebFut;
    }

    /**
     * Checking that the rebuild of indexes for the cache has completed.
     *
     * @param n Node.
     * @param cacheCtx Cache context.
     * @param expKeys The expected number of keys processed during index rebuilding
     */
    private void checkFinishRebuildIndexes(IgniteEx n, GridCacheContext<?, ?> cacheCtx, int expKeys) {
        assertNull(indexRebuildFuture(n, cacheCtx.cacheId()));

        checkCacheMetrics0(n, cacheCtx.name(), false, expKeys);
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
        IndexRebuildAware idxRebuildAware = getFieldValue(n.context().query(), "idxRebuildAware");

        GridDhtPartitionsExchangeFuture exhFut = n.context().cache().context().exchange().lastTopologyFuture();

        assertEquals(expContains, idxRebuildAware.rebuildIndexesOnExchange(cacheId, exhFut.initialVersion()));
    }
}
