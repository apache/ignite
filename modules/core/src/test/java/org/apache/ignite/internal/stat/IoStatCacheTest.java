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
 *
 */

package org.apache.ignite.internal.stat;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

/**
 * Tests for cache IO statistics for inmemory mode.
 */
public class IoStatCacheTest extends GridCommonAbstractTest {
    /** */
    private final static String ATOMIC_CACHE_NAME = "ATOMIC_CACHE";

    /** */
    private final static String MVCC_CACHE_NAME = "MVCC_CACHE";

    /** */
    private final static String TRANSACTIONAL_CACHE_NAME = "TRANSACTIONAL_CACHE";

    /** */
    private static final int RECORD_COUNT = 200;

    /** */
    private static IgniteEx ignite;

    /**
     * @return {@code true} in case persistence mode enabled.
     */
    protected boolean persist() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("consistentId");

        final CacheConfiguration atomicCacheCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(ATOMIC_CACHE_NAME);

        final CacheConfiguration mvccCacheCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            .setName(MVCC_CACHE_NAME);

        final CacheConfiguration transactionalCacheCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setName(TRANSACTIONAL_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(persist()))
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.NONE);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(transactionalCacheCfg, atomicCacheCfg, mvccCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = (IgniteEx)startGrid();

        if (persist())
            ignite.active(true);

        //Need to initialize partition memory page.
        String WARM_UP_KEY = "WARM_UP";

        ignite.cache(TRANSACTIONAL_CACHE_NAME).put(WARM_UP_KEY, WARM_UP_KEY);

        ignite.cache(TRANSACTIONAL_CACHE_NAME).remove(WARM_UP_KEY);

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite.context().ioStats().resetStats();
    }

    /**
     * Test statistics for TRANSACTIONAL cache.
     */
    public void testTransactonalCache() {
        cacheTest(TRANSACTIONAL_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 2);
    }

    /**
     * Test statistics for MVCC cache.
     */
    public void testMvccCache() {
        cacheTest(MVCC_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 2);
    }

    /**
     * Test statistics for ATOMIC cache.
     */
    public void testAtomicCache() {
        cacheTest(ATOMIC_CACHE_NAME, RECORD_COUNT, RECORD_COUNT);
    }

    /**
     * Test statistics for three caches in the same time.
     */
    public void testForThreeCaches() {
        prepareData(ATOMIC_CACHE_NAME, RECORD_COUNT);

        prepareData(TRANSACTIONAL_CACHE_NAME, RECORD_COUNT);

        prepareData(MVCC_CACHE_NAME, RECORD_COUNT);

        GridIoStatManager ioStatMgr = ignite.context().ioStats();

        Set<String> statisticCacheNames = ioStatMgr.subTypesLogicalReads(StatType.CACHE);

        Assert.assertEquals(3, statisticCacheNames.size());

        Map<AggregatePageType, AtomicLong> aggregateGlobal = ioStatMgr.aggregate(ioStatMgr.logicalReadsGlobal());

        statisticCacheNames.forEach((cacheName) -> {
            Map<PageType, Long> cacheStat = ioStatMgr.logicalReads(StatType.CACHE, cacheName);

            Map<AggregatePageType, AtomicLong> cacheStatAggregate = ioStatMgr.aggregate(cacheStat);

            checkAggregatedStatIsNotEmpty(cacheStatAggregate);

            for (Map.Entry<AggregatePageType, AtomicLong> globalEntry : aggregateGlobal.entrySet()) {
                Assert.assertTrue(globalEntry.getValue().get() > cacheStatAggregate.get(globalEntry.getKey()).get());
            }
        });

    }

    /**
     * @param cacheName Name of cache.
     * @param rowCnt Number of row nee to put into cache.
     * @param idxScanCnt How many index scan expected.
     */
    private void cacheTest(String cacheName, int rowCnt, int idxScanCnt) {
        prepareData(cacheName, rowCnt);

        GridIoStatManager ioStatMgr = ignite.context().ioStats();

        Set<String> statisticCacheNames = ioStatMgr.subTypesLogicalReads(StatType.CACHE);

        Assert.assertEquals(1, statisticCacheNames.size());

        Assert.assertTrue(statisticCacheNames.contains(cacheName));

        Map<PageType, Long> cacheStat = ioStatMgr.logicalReads(StatType.CACHE, cacheName);

        Map<AggregatePageType, AtomicLong> cacheStatAggregate = ioStatMgr.aggregate(cacheStat);

        Map<AggregatePageType, AtomicLong> aggregateGlobal = ioStatMgr.aggregate(ioStatMgr.logicalReadsGlobal());

        checkAggregatedStatIsNotEmpty(cacheStatAggregate);

        Assert.assertEquals(idxScanCnt, cacheStatAggregate.get(AggregatePageType.INDEX).intValue());

        Assert.assertEquals(aggregateGlobal.get(AggregatePageType.INDEX).get(), cacheStatAggregate.get(AggregatePageType.INDEX).get());

        Assert.assertEquals(aggregateGlobal.get(AggregatePageType.DATA).get(), cacheStatAggregate.get(AggregatePageType.DATA).get());
    }

    /**
     * @param aggregatedStat Aggregated IO statistics.
     */
    private void checkAggregatedStatIsNotEmpty(Map<AggregatePageType, AtomicLong> aggregatedStat) {
        Stream.of(AggregatePageType.INDEX, AggregatePageType.DATA).forEach(type -> {
                long val = aggregatedStat.get(type).get();

                Assert.assertTrue("Statistics shouldn't be empty for " + type, val > 0);
            }
        );
    }

    /**
     * Fill cache.
     *
     * @param cacheName Name of cache to populate.
     * @param cnt Number of entries to put.
     */
    private void prepareData(String cacheName, int cnt) {
        IgniteCache cache = ignite.cache(cacheName);

        for (int i = 0; i < cnt; i++)
            cache.put("KEY-" + i, "VAL-" + i);
    }

}
