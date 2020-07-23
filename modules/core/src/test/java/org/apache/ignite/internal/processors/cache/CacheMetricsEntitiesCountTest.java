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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

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
    private static int cacheCnt = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        Collection<CacheConfiguration> ccfgs = new ArrayList<>(4);

        ccfgs.add(new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 0)
            .setStatisticsEnabled(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ccfgs.add(new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 1)
            .setStatisticsEnabled(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ccfgs.add(new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 2)
            .setStatisticsEnabled(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setNearConfiguration(new NearCacheConfiguration<>())
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ccfgs.add(new CacheConfiguration<>()
            .setName(CACHE_PREFIX + 3)
            .setStatisticsEnabled(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setOnheapCacheEnabled(true)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        if (!MvccFeatureChecker.forcedMvcc() || MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.LOCAL_CACHE)) {
            ccfgs.add(new CacheConfiguration<>()
                .setName(CACHE_PREFIX + 4)
                .setStatisticsEnabled(true)
                .setCacheMode(CacheMode.LOCAL)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
        }

        cacheCnt = ccfgs.size();

        cfg.setCacheConfiguration(U.toArray(ccfgs, new CacheConfiguration[cacheCnt]));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /**
     * Test entities count, calculated by different implementations.
     */
    @Test
    public void testEnitiesCount() throws Exception {
        awaitPartitionMapExchange();

        for (int igniteIdx = 0; igniteIdx < GRID_CNT; igniteIdx++)
            for (int cacheIdx = 0; cacheIdx < cacheCnt; cacheIdx++)
                fillCache(igniteIdx, cacheIdx);

        awaitMetricsUpdate(1);

        int cacheSize = GRID_CNT * ENTITIES_CNT;

        // CacheMode == REPLICATED.
        checkCacheClusterMetrics(0,
            cacheSize,
            cacheSize * GRID_CNT,
            cacheSize,
            cacheSize * (GRID_CNT - 1),
            0);

        // CacheMode == PARTITIONED, Backups == 1.
        checkCacheClusterMetrics(1,
            cacheSize,
            cacheSize * 2,
            cacheSize,
            cacheSize,
            0);

        // CacheMode == PARTITIONED, Backups == 1, NearCache.
        checkCacheClusterMetrics(2,
            cacheSize,
            cacheSize * 2,
            cacheSize,
            cacheSize,
            216 /* TODO */);

        // CacheMode == PARTITIONED, Backups == 1, OnheapCache.
        checkCacheClusterMetrics(3,
            cacheSize,
            cacheSize * 2,
            cacheSize,
            cacheSize,
            cacheSize * 2);

        // CacheMode == LOCAL
        if (cacheCnt == 5)
            checkCacheClusterMetrics(4,
                cacheSize,
                cacheSize,
                cacheSize,
                0,
                0);

        for (int igniteIdx = 0; igniteIdx < GRID_CNT; igniteIdx++)
            for (int cacheIdx = 0; cacheIdx < cacheCnt; cacheIdx++)
                checkCacheLocalMetrics(igniteIdx, cacheIdx);
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
    private void checkCacheLocalMetrics(int igniteIdx, int cacheIdx) throws IgniteCheckedException {
        IgniteInternalCache internalCache = grid(igniteIdx).cachex(CACHE_PREFIX + cacheIdx);

        GridCacheContext cctx = internalCache.context();

        GridCacheAdapter cache = cctx.cache();

        CacheMetricsImpl metrics = cache.metrics0();

        long offHeapEntriesCount = cache.offHeapEntriesCount();

        long offHeapPrimaryEntriesCount = cctx.offheap().cacheEntriesCount(cctx.cacheId(),
            true,
            false,
            cctx.affinity().affinityTopologyVersion());

        long offHeapBackupEntriesCount = cctx.offheap().cacheEntriesCount(cctx.cacheId(),
            false,
            true,
            cctx.affinity().affinityTopologyVersion());

        long heapEntriesCount = cache.localSizeLong(ONHEAP_PEEK_MODES);

        long cacheSize = cache.localSizeLong(new CachePeekMode[]{CachePeekMode.PRIMARY});
        int size = cache.localSize(new CachePeekMode[]{CachePeekMode.PRIMARY});

        boolean isEmpty = cache.isEmpty();

        String cacheInfo = "igniteIdx=" + igniteIdx + ", cacheIdx=" + cacheIdx + " ";

        log.info("Checking cache,  " + cacheInfo);

        assertEquals(cacheInfo + " offHeapEntriesCount",
            offHeapEntriesCount, metrics.getOffHeapEntriesCount());
        assertEquals(cacheInfo + " offHeapBackupEntriesCount",
            offHeapBackupEntriesCount, metrics.getOffHeapBackupEntriesCount());
        assertEquals(cacheInfo + " offHeapPrimaryEntriesCount",
            offHeapPrimaryEntriesCount, metrics.getOffHeapPrimaryEntriesCount());
        assertEquals(cacheInfo + " heapEntriesCount", heapEntriesCount, metrics.getHeapEntriesCount());
        assertEquals(cacheInfo + " size", size, metrics.getSize());
        assertEquals(cacheInfo + " keySize", size, metrics.getKeySize());
        assertEquals(cacheInfo + " cacheSize", cacheSize, metrics.getCacheSize());
        assertEquals(cacheInfo + " isEmpty", isEmpty, metrics.isEmpty());

        MetricRegistry mreg = cctx.kernalContext().metric().registry(cacheMetricsRegistryName(cctx.name(),
            cache.isNear()));

        assertNotNull(mreg);

        assertEquals(offHeapEntriesCount, ((LongMetric)mreg.findMetric("OffHeapEntriesCount")).value());
        assertEquals(offHeapBackupEntriesCount, ((LongMetric)mreg.findMetric("OffHeapBackupEntriesCount")).value());
        assertEquals(offHeapPrimaryEntriesCount, ((LongMetric)mreg.findMetric("OffHeapPrimaryEntriesCount")).value());
        assertEquals(heapEntriesCount, ((LongMetric)mreg.findMetric("HeapEntriesCount")).value());
        assertEquals(cacheSize, ((LongMetric)mreg.findMetric("CacheSize")).value());
    }

    /**
     * @param cacheIdx Cache index.
     */
    private void checkCacheClusterMetrics(int cacheIdx,
        long cacheSize,
        long offHeapEntriesCnt,
        long offHeapPrimaryEntriesCnt,
        long offHeapBackupEntriesCnt,
        long heapEntriesCnt
    ) {
        long cacheSizeSum = 0;
        long offHeapEntriesCntSum = 0;
        long offHeapPrimaryEntriesCntSum = 0;
        long offHeapBackupEntriesCntSum = 0;
        long heapEntriesCntSum = 0;
        boolean isEmptySum = true;

        for (int igniteIdx = 0; igniteIdx < GRID_CNT; igniteIdx++) {
            IgniteCache cache = grid(igniteIdx).cache(CACHE_PREFIX + cacheIdx);

            CacheMetrics metrics = cache.metrics();

            String cacheInfo = "igniteIdx=" + igniteIdx + ", cacheIdx=" + cacheIdx + " ";

            assertEquals(cacheInfo + " CacheSize", cacheSize, metrics.getCacheSize());
            assertEquals(cacheInfo + " offHeapEntriesCnt", offHeapEntriesCnt,
                metrics.getOffHeapEntriesCount());
            assertEquals(cacheInfo + " offHeapBackupEntriesCnt", offHeapBackupEntriesCnt,
                metrics.getOffHeapBackupEntriesCount());
            assertEquals(cacheInfo + " offHeapPrimaryEntriesCnt", offHeapPrimaryEntriesCnt,
                metrics.getOffHeapPrimaryEntriesCount());

            if (!MvccFeatureChecker.forcedMvcc()) // Onheap cache is not supported in Mvcc mode.
                assertEquals(cacheInfo + " heapEntriesCnt", heapEntriesCnt, metrics.getHeapEntriesCount());

            assertEquals(cacheInfo + " size", cacheSize, metrics.getSize());
            assertEquals(cacheInfo + " keySize", cacheSize, metrics.getKeySize());
            assertEquals(cacheInfo + " isEmpty", cacheSize == 0, metrics.isEmpty());

            metrics = cache.localMetrics();

            cacheSizeSum += metrics.getCacheSize();
            offHeapEntriesCntSum += metrics.getOffHeapEntriesCount();
            offHeapPrimaryEntriesCntSum += metrics.getOffHeapPrimaryEntriesCount();
            offHeapBackupEntriesCntSum += metrics.getOffHeapBackupEntriesCount();
            heapEntriesCntSum += metrics.getHeapEntriesCount();
            isEmptySum = isEmptySum && metrics.isEmpty();
        }

        String cacheInfo = "cacheIdx=" + cacheIdx + " check sum";

        assertEquals(cacheInfo + " CacheSize", cacheSize, cacheSizeSum);
        assertEquals(cacheInfo + " offHeapEntriesCnt", offHeapEntriesCnt, offHeapEntriesCntSum);
        assertEquals(cacheInfo + " offHeapBackupEntriesCnt", offHeapBackupEntriesCnt, offHeapBackupEntriesCntSum);
        assertEquals(cacheInfo + " offHeapPrimaryEntriesCnt", offHeapPrimaryEntriesCnt, offHeapPrimaryEntriesCntSum);

        if (!MvccFeatureChecker.forcedMvcc()) // Onheap cache is not supported in Mvcc mode.
            assertEquals(cacheInfo + " heapEntriesCnt", heapEntriesCnt, heapEntriesCntSum);

        assertEquals(cacheInfo + " isEmpty", cacheSize == 0, isEmptySum);
    }
}
