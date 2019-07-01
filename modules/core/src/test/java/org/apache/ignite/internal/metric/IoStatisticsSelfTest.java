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

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsCacheSelfTest.logicalReads;
import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.PHYSICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMXBeanImplSelfTest.resetAllIoMetrics;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.metric.IoStatisticsType.HASH_INDEX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Tests for IO statistic manager.
 */
public class IoStatisticsSelfTest extends GridCommonAbstractTest {

    /** */
    private static final int RECORD_COUNT = 5000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test existing zero statistics for not touched caches.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testEmptyIOStat() throws Exception {
        IgniteEx ign = prepareIgnite(true);

        MetricRegistry mreg = ign.context().metric().registry();

        checkEmptyStat(mreg.withPrefix(CACHE_GROUP.metricGroupName()),
            DEFAULT_CACHE_NAME,
            null,
            CACHE_GROUP);

        checkEmptyStat(mreg.withPrefix(HASH_INDEX.metricGroupName()),
            DEFAULT_CACHE_NAME,
            HASH_PK_IDX_NAME,
            HASH_INDEX);
    }

    /**
     * @param stat Statistics map.
     * @param name Name of statistics.
     * @param subName Subname of statistics.
     */
    private void checkEmptyStat(MetricRegistry mset, String name, String subName, IoStatisticsType type) {
        MetricRegistry cacheIoStatHolder;

        if (subName == null)
            cacheIoStatHolder = mset.withPrefix(name);
        else
            cacheIoStatHolder = mset.withPrefix(name, subName);

        assertNotNull(cacheIoStatHolder);

        if (type == CACHE_GROUP) {
            assertEquals(5, mset.getMetrics().size());

            assertEquals(0, ((LongMetric)cacheIoStatHolder.findMetric(LOGICAL_READS)).longValue());

            assertEquals(0, ((LongMetric)cacheIoStatHolder.findMetric(PHYSICAL_READS)).longValue());
        }
        else {
            assertEquals(7, mset.getMetrics().size());

            assertEquals(0, ((LongMetric)cacheIoStatHolder.findMetric(LOGICAL_READS_LEAF)).longValue());

            assertEquals(0, ((LongMetric)cacheIoStatHolder.findMetric(LOGICAL_READS_INNER)).longValue());

            assertEquals(0, ((LongMetric)cacheIoStatHolder.findMetric(PHYSICAL_READS_LEAF)).longValue());

            assertEquals(0, ((LongMetric)cacheIoStatHolder.findMetric(PHYSICAL_READS_INNER)).longValue());
        }
    }

    /**
     * Test LOCAL_NODE statistics tracking for persistent cache.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testNotPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(false);
    }

    /**
     * Test LOCAL_NODE statistics tracking for not persistent cache.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(true);
    }

    /**
     * Check LOCAL_NODE statistics tracking.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @throws Exception In case of failure.
     */
    private void ioStatGlobalPageTrackTest(boolean isPersistent) throws Exception {
        IgniteEx grid = prepareData(isPersistent);

        MetricRegistry mreg = grid.context().metric().registry();

        long physicalReadsCnt = physicalReads(mreg, CACHE_GROUP, DEFAULT_CACHE_NAME, null);

        if (isPersistent)
            Assert.assertTrue(physicalReadsCnt > 0);
        else
            Assert.assertEquals(0, physicalReadsCnt);

        Long logicalReads = logicalReads(mreg, HASH_INDEX, metricName(DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME));

        Assert.assertNotNull(logicalReads);

        Assert.assertEquals(RECORD_COUNT, logicalReads.longValue());
    }

    /**
     * Prepare Ignite instance and fill cache.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @return Ignite instance.
     * @throws Exception In case of failure.
     */
    @NotNull private IgniteEx prepareData(boolean isPersistent) throws Exception {
        IgniteEx grid = prepareIgnite(isPersistent);

        IgniteCache cache = grid.getOrCreateCache(DEFAULT_CACHE_NAME);

        resetAllIoMetrics(grid);

        for (int i = 0; i < RECORD_COUNT; i++)
            cache.put("KEY-" + i, "VAL-" + i);

        return grid;
    }

    /**
     * Create Ignite configuration.
     *
     * @param isPersist {@code true} in case persistence should be enable.
     * @return Ignite configuration.
     * @throws Exception In case of failure.
     */
    private IgniteConfiguration getConfiguration(boolean isPersist) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        if (isPersist) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(30L * 1024 * 1024)
                        .setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     * Start Ignite grid and create cache.
     *
     * @param isPersist {@code true} in case Ignate should use persistente storage.
     * @return Started Ignite instance.
     * @throws Exception In case of failure.
     */
    private IgniteEx prepareIgnite(boolean isPersist) throws Exception {
        IgniteEx ignite = startGrid(getConfiguration(isPersist));

        ignite.cluster().active(true);

        ignite.createCache(new CacheConfiguration<String, String>(DEFAULT_CACHE_NAME));

        return ignite;
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Number of physical reads since last reset statistics.
     */
    public Long physicalReads(MetricRegistry mreg, IoStatisticsType statType, String name, String subName) {
        String fullName = subName == null ? name : metricName(name, subName);

        MetricRegistry mset = mreg.withPrefix(statType.metricGroupName(), fullName);

        if (mset == null)
            return null;

        Map<String, Long> res;

        switch (statType) {
            case CACHE_GROUP:
                return ((LongMetric)mset.findMetric(PHYSICAL_READS)).value();

            case HASH_INDEX:
            case SORTED_INDEX:
                long leaf = ((LongMetric)mset.findMetric(PHYSICAL_READS_LEAF)).value();
                long inner = ((LongMetric)mset.findMetric(PHYSICAL_READS_INNER)).value();

                return leaf + inner;

            default:
                return null;
        }
    }
}
