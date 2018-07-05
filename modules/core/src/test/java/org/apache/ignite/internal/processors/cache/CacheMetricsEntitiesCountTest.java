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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * This test checks that entries count metrics, calculated by method
 * {@link org.apache.ignite.internal.processors.cache.CacheMetricsImpl#getEntriesStat()} (which uses one iteration
 * over local partitions to get all set of metrics), have the same values as metrics, calculated by individual methods
 * (which use iteration over local partition per each method call).
 */
public class CacheMetricsEntitiesCountTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Cache prefix. */
    private static final String CACHE_PREFIX = "CACHE";

    /** Entities cnt. */
    private static final int ENTITIES_CNT = 100;

    /** Onheap peek modes. */
    private static final CachePeekMode[] ONHEAP_PEEK_MODES = new CachePeekMode[] {
        CachePeekMode.ONHEAP, CachePeekMode.PRIMARY, CachePeekMode.BACKUP, CachePeekMode.NEAR};

    /** Cache count. */
    private static final int CACHE_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg0 = new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 0)
            .setCacheMode(CacheMode.LOCAL);

        CacheConfiguration<?, ?> ccfg1 = new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration<?, ?> ccfg2 = new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);

        CacheConfiguration<?, ?> ccfg3 = new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 3)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setNearConfiguration(new NearCacheConfiguration<>());

        cfg.setCacheConfiguration(ccfg0, ccfg1, ccfg2, ccfg3);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /**
     * Test entities count, calculated by different implementations.
     */
    public void testEnitiesCount() throws Exception {
        awaitPartitionMapExchange();

        for (int igniteIdx = 0; igniteIdx < GRID_CNT; igniteIdx++)
            for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++)
                fillCache(igniteIdx, cacheIdx);

        for (int igniteIdx = 0; igniteIdx < GRID_CNT; igniteIdx++)
            for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++)
                checkCache(igniteIdx, cacheIdx);
    }

    /**
     * @param igniteIdx Ignite index.
     * @param cacheIdx Cache index.
     */
    private void fillCache(int igniteIdx, int cacheIdx) {
        log.info("Filling cache, igniteIdx=" + igniteIdx + ", cacheIdx=" + cacheIdx);

        IgniteCache cache = grid(igniteIdx).cache(CACHE_PREFIX + cacheIdx);

        for (int i = 0; i < ENTITIES_CNT; i++)
            cache.put("key" + igniteIdx + "-" + i, i);
    }

    /**
     * @param igniteIdx Ignite index.
     * @param cacheIdx Cache index.
     */
    private void checkCache(int igniteIdx, int cacheIdx) throws IgniteCheckedException {
        IgniteInternalCache internalCache = grid(igniteIdx).cachex(CACHE_PREFIX + cacheIdx);

        GridCacheContext cctx = internalCache.context();

        GridCacheAdapter cache = cctx.cache();

        CacheMetricsImpl metrics = cache.metrics0();

        CacheMetricsImpl.EntriesStatMetrics entriesStatMetrics = metrics.getEntriesStat();

        long offHeapEntriesCnt = cache.offHeapEntriesCount();

        long offHeapPrimaryEntriesCnt = cctx.offheap().cacheEntriesCount(cctx.cacheId(),
            true,
            false,
            cctx.affinity().affinityTopologyVersion());

        long offHeapBackupEntriesCnt = cctx.offheap().cacheEntriesCount(cctx.cacheId(),
            false,
            true,
            cctx.affinity().affinityTopologyVersion());

        long heapEntriesCnt = cache.localSizeLong(ONHEAP_PEEK_MODES);

        int size = cache.size();

        int keySize = size;

        boolean isEmpty = cache.isEmpty();

        String cacheInfo = "igniteIdx=" + igniteIdx + ", cacheIdx=" + cacheIdx + " ";

        log.info("Checking cache,  " + cacheInfo);

        assertEquals(cacheInfo + " offHeapEntriesCnt", offHeapEntriesCnt,
            entriesStatMetrics.offHeapEntriesCount());
        assertEquals(cacheInfo + " offHeapBackupEntriesCnt", offHeapBackupEntriesCnt,
            entriesStatMetrics.offHeapBackupEntriesCount());
        assertEquals(cacheInfo + " offHeapPrimaryEntriesCnt", offHeapPrimaryEntriesCnt,
            entriesStatMetrics.offHeapPrimaryEntriesCount());
        assertEquals(cacheInfo + " heapEntriesCnt", heapEntriesCnt, entriesStatMetrics.heapEntriesCount());
        assertEquals(cacheInfo + " size", size, entriesStatMetrics.size());
        assertEquals(cacheInfo + " keySize", keySize, entriesStatMetrics.keySize());
        assertEquals(cacheInfo + " isEmpty", isEmpty, entriesStatMetrics.isEmpty());
    }
}
