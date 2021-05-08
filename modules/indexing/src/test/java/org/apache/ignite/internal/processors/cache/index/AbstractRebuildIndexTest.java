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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.BreakRebuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.StopRebuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.IgniteThrowableBiPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.addCacheRowConsumer;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.nodeName;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;

/**
 * Base class for testing index rebuilds.
 */
public abstract class AbstractRebuildIndexTest extends GridCommonAbstractTest {
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
            ).setCacheConfiguration(cacheCfg(DEFAULT_CACHE_NAME, null));
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx n = super.startGrid(idx);

        n.cluster().state(ACTIVE);

        return n;
    }

    /**
     * Prepare cluster for test.
     *
     * @param keys Key count.
     * @return Coordinator.
     * @throws Exception If failed.
     */
    protected IgniteEx prepareCluster(int keys) throws Exception {
        IgniteEx n = startGrid(0);

        if (n.cluster().state() != ACTIVE)
            n.cluster().state(ACTIVE);

        populate(n.cache(DEFAULT_CACHE_NAME), keys);

        return n;
    }

    /**
     * Registering a {@link StopRebuildIndexConsumer} for cache.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @return New instance of {@link StopRebuildIndexConsumer}.
     */
    protected StopRebuildIndexConsumer addStopRebuildIndexConsumer(IgniteEx n, String cacheName) {
        StopRebuildIndexConsumer stopRebuildIdxConsumer = new StopRebuildIndexConsumer(getTestTimeout());

        addCacheRowConsumer(nodeName(n), cacheName, stopRebuildIdxConsumer);

        return stopRebuildIdxConsumer;
    }

    /**
     * Registering a {@link BreakRebuildIndexConsumer} for cache.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @param brakePred Predicate for throwing an {@link IgniteCheckedException}.
     * @return New instance of {@link BreakRebuildIndexConsumer}.
     */
    protected BreakRebuildIndexConsumer addBreakRebuildIndexConsumer(
        IgniteEx n,
        String cacheName,
        IgniteThrowableBiPredicate<BreakRebuildIndexConsumer, CacheDataRow> brakePred
    ) {
        BreakRebuildIndexConsumer breakRebuildIdxConsumer = new BreakRebuildIndexConsumer(getTestTimeout(), brakePred);

        addCacheRowConsumer(nodeName(n), cacheName, breakRebuildIdxConsumer);

        return breakRebuildIdxConsumer;
    }

    /**
     * Checking that rebuilding indexes for the cache has started.
     *
     * @param n Node.
     * @param cacheCtx Cache context.
     * @return Rebuild index future.
     */
    protected IgniteInternalFuture<?> checkStartRebuildIndexes(IgniteEx n, GridCacheContext<?, ?> cacheCtx) {
        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, cacheCtx.cacheId());

        assertNotNull(idxRebFut);
        assertFalse(idxRebFut.isDone());

        checkCacheMetrics0(n, cacheCtx.name(), true, 0L);

        return idxRebFut;
    }

    /**
     * Checking metrics rebuilding indexes of cache.
     *
     * @param n                          Node.
     * @param cacheName                  Cache name.
     * @param expIdxRebuildInProgress    The expected status of rebuilding indexes.
     * @param expIdxRebuildKeysProcessed The expected number of keys processed during index rebuilding.
     */
    protected void checkCacheMetrics0(
        IgniteEx n,
        String cacheName,
        boolean expIdxRebuildInProgress,
        @Nullable Long expIdxRebuildKeysProcessed
    ) {
        CacheMetricsImpl metrics0 = cacheMetrics0(n, cacheName);
        assertNotNull(metrics0);

        assertEquals(expIdxRebuildInProgress, metrics0.isIndexRebuildInProgress());

        if (expIdxRebuildKeysProcessed != null)
            assertEquals(expIdxRebuildKeysProcessed.longValue(), metrics0.getIndexRebuildKeysProcessed());
    }

    /**
     * Checking that the rebuild of indexes for the cache has completed.
     *
     * @param n Node.
     * @param cacheCtx Cache context.
     * @param expKeys The expected number of keys processed during index rebuilding
     */
    protected void checkFinishRebuildIndexes(IgniteEx n, GridCacheContext<?, ?> cacheCtx, int expKeys) {
        assertNull(indexRebuildFuture(n, cacheCtx.cacheId()));

        checkCacheMetrics0(n, cacheCtx.name(), false, (long)expKeys);
    }

    /**
     * Stopping all nodes and deleting their index.bin.
     *
     * @throws Exception If failed.
     */
    protected void stopAllGridsWithDeleteIndexBin() throws Exception {
        List<String> igniteInstanceNames = G.allGrids().stream().map(Ignite::name).collect(toList());

        stopAllGrids();

        for (String n : igniteInstanceNames)
            deleteIndexBin(n);
    }

    /**
     * Create cache configuration with index: {@link Integer} -> {@link Person}.
     *
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @return New instance of the cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheCfg(String cacheName, @Nullable String grpName) {
        CacheConfiguration<K, V> cacheCfg = new CacheConfiguration<>(cacheName);

        return cacheCfg.setGroupName(grpName).setIndexedTypes(Integer.class, Person.class);
    }

    /**
     * Populate cache with {@link Person} sequentially.
     *
     * @param cache Cache.
     * @param cnt Entry count.
     */
    protected void populate(IgniteCache<Integer, Person> cache, int cnt) {
        for (int i = 0; i < cnt; i++)
            cache.put(i, new Person(i, "name_" + i));
    }
}
