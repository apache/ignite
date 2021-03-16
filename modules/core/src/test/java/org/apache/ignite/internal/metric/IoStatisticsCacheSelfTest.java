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

package org.apache.ignite.internal.metric;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.google.common.collect.Sets;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMXBeanImplSelfTest.resetAllIoMetrics;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.metric.IoStatisticsType.HASH_INDEX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Tests for cache IO statistics for inmemory mode.
 */
public class IoStatisticsCacheSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final String ATOMIC_CACHE_NAME = "ATOMIC_CACHE";

    /** */
    protected static final String MVCC_CACHE_NAME = "MVCC_CACHE";

    /** */
    protected static final String TRANSACTIONAL_CACHE_NAME = "TRANSACTIONAL_CACHE";

    /** */
    protected static final String CACHE1_IN_GROUP_NAME = "CACHE1_GROUP";

    /** */
    protected static final String CACHE2_IN_GROUP_NAME = "CACHE2_GROUP";

    /** */
    protected static final String CACHE_GROUP_NAME = "CACHE_GROUP_NAME";

    /** */
    protected static final Set<String> ALL_CACHE_GROUP_NAMES = Sets.newHashSet(
        ATOMIC_CACHE_NAME, MVCC_CACHE_NAME, TRANSACTIONAL_CACHE_NAME, CACHE_GROUP_NAME);

    /** */
    protected static final int RECORD_COUNT = 100;

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
            .setName(TRANSACTIONAL_CACHE_NAME);

        final CacheConfiguration atomic1CacheGrpCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(CACHE1_IN_GROUP_NAME)
            .setGroupName(CACHE_GROUP_NAME);

        final CacheConfiguration atomic2CacheGrpCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(CACHE2_IN_GROUP_NAME)
            .setGroupName(CACHE_GROUP_NAME);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(persist()))
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.NONE);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(transactionalCacheCfg, atomicCacheCfg, mvccCacheCfg,
            atomic1CacheGrpCfg, atomic2CacheGrpCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = (IgniteEx)startGrid();

        if (persist())
            ignite.active(true);
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

        resetAllIoMetrics(ignite);
    }

    /**
     * Test statistics for TRANSACTIONAL cache.
     */
    @Test
    public void testTransactonalCache() throws Exception {
        cacheTest(TRANSACTIONAL_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 3, RECORD_COUNT * 2);
    }

    /**
     * Test statistics for MVCC cache.
     */
    @Test
    public void testMvccCache() throws Exception {
        cacheTest(MVCC_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 6, RECORD_COUNT * 3);
    }

    /**
     * Test statistics for ATOMIC cache.
     */
    @Test
    public void testAtomicCache() throws Exception {
        cacheTest(ATOMIC_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 2, RECORD_COUNT);
    }

    /**
     * Test statistics for three caches in the same time.
     */
    @Test
    public void testForThreeCaches() throws Exception {
        prepareData(RECORD_COUNT, ATOMIC_CACHE_NAME, TRANSACTIONAL_CACHE_NAME, MVCC_CACHE_NAME);

        GridMetricManager mmgr = ignite.context().metric();

        Set<String> statisticCacheNames = deriveStatisticNames(CACHE_GROUP);

        assertEquals(ALL_CACHE_GROUP_NAMES, statisticCacheNames);

        Stream.of(ATOMIC_CACHE_NAME, TRANSACTIONAL_CACHE_NAME, MVCC_CACHE_NAME).forEach((cacheName) -> {
            long logicalReads = logicalReads(mmgr, CACHE_GROUP, cacheName);

            assertTrue(logicalReads > RECORD_COUNT);
        });
    }

    /**
     * Test statistics for two caches in the same cache group.
     */
    @Test
    public void testCacheGroupCaches() throws Exception {
        prepareData(RECORD_COUNT, CACHE1_IN_GROUP_NAME, CACHE2_IN_GROUP_NAME);

        GridMetricManager mmgr = ignite.context().metric();

        Set<String> statisticCacheNames = deriveStatisticNames(CACHE_GROUP);

        assertEquals(ALL_CACHE_GROUP_NAMES, statisticCacheNames);

        long logicalReads = logicalReads(mmgr, CACHE_GROUP, CACHE_GROUP_NAME);

        assertEquals(RECORD_COUNT * 4, logicalReads);
    }

    /**
     * @param cacheName Name of cache.
     * @param rowCnt Number of row need to put into cache.
     * @param dataPageReads How many data page reads operation expected.
     * @param idxPageReadsCnt How many index page reads scan expected.
     */
    protected void cacheTest(String cacheName, int rowCnt, int dataPageReads, int idxPageReadsCnt) throws Exception {
        prepareData(rowCnt, cacheName);

        GridMetricManager mmgr = ignite.context().metric();

        Set<String> statisticCacheNames = deriveStatisticNames(CACHE_GROUP);

        assertEquals(ALL_CACHE_GROUP_NAMES, statisticCacheNames);

        assertTrue(statisticCacheNames.contains(cacheName));

        long logicalReadsCache = logicalReads(mmgr, CACHE_GROUP, cacheName);

        assertEquals(dataPageReads, logicalReadsCache);

        long logicalReadsIdx = logicalReads(mmgr, HASH_INDEX, metricName(cacheName, HASH_PK_IDX_NAME));

        assertEquals(idxPageReadsCnt, logicalReadsIdx);

    }

    /**
     * Extract all tracked names for given statistics type.
     *
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatisticNames(IoStatisticsType statType) {
        assert statType != null;

        GridMetricManager mmgr = ignite.context().metric();

        Stream<ReadOnlyMetricRegistry> grpsStream = StreamSupport.stream(mmgr.spliterator(), false)
                .filter(grp -> grp.name().startsWith(statType.metricGroupName()));

        return grpsStream.flatMap(grp -> StreamSupport.stream(grp.spliterator(), false))
            .filter(m -> m.name().endsWith("name"))
            .map(Metric::getAsString)
            .collect(Collectors.toSet());
    }

    /**
     * Warm up and fill cache.
     *
     * @param cacheNames Names of caches to populate.
     * @param cnt Number of entries to put.
     */
    private void prepareData(int cnt, String... cacheNames) throws Exception {
        //Need to initialize partition and data memory pages
        for (String cacheName : cacheNames) {

            IgniteCache cache = ignite.cache(cacheName);

            for (int i = 0; i < cnt; i++) {
                cache.put(i, i);
                cache.put(i, i); //Second invocation required to warm up MVCC cache to fill old versions chains.
            }
        }

        resetAllIoMetrics(ignite);

        for (String cacheName : cacheNames) {

            IgniteCache cache = ignite.cache(cacheName);

            for (int i = 0; i < cnt; i++)
                cache.put(i, i);
        }
    }

    /**
     * @param mmgr Metric manager.
     * @param type Staticstics type.
     * @param id Metric registry id.
     * @return Logical reads count.
     */
    public static long logicalReads(GridMetricManager mmgr, IoStatisticsType type, String id) {
        MetricRegistry mreg = mmgr.registry(metricName(type.metricGroupName(), id));

        if (type == CACHE_GROUP)
            return mreg.<LongMetric>findMetric(LOGICAL_READS).value();
        else {
            long leaf = mreg.<LongMetric>findMetric(LOGICAL_READS_LEAF).value();
            long inner = mreg.<LongMetric>findMetric(LOGICAL_READS_INNER).value();

            return leaf + inner;
        }
    }
}
