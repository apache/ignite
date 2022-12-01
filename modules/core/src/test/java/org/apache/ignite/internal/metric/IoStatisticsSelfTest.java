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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.Iterators;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

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
import static org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsSelfTest.TIMEOUT;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for IO statistic manager.
 */
public class IoStatisticsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicBoolean LOAD = new AtomicBoolean(true);

    /** */
    private static final AtomicLong LOAD_COUNTER = new AtomicLong();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        LOAD.set(true);

        LOAD_COUNTER.set(0);
    }

    /**
     * Test existing zero statistics for not touched caches.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testEmptyIOStat() throws Exception {
        IgniteEx ign = prepareIgnite(true);

        GridMetricManager mmgr = ign.context().metric();

        checkEmptyStat(mmgr.registry(metricName(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME)), CACHE_GROUP);

        checkEmptyStat(mmgr.registry(metricName(HASH_INDEX.metricGroupName(), DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME)),
            HASH_INDEX);
    }

    /**
     * @param mreg Metric registry.
     */
    private void checkEmptyStat(MetricRegistry mreg, IoStatisticsType type) {
        assertNotNull(mreg);

        if (type == CACHE_GROUP) {
            assertEquals(5, Iterators.size(mreg.iterator()));

            assertEquals(0, mreg.<LongMetric>findMetric(LOGICAL_READS).value());

            assertEquals(0, mreg.<LongMetric>findMetric(PHYSICAL_READS).value());
        }
        else {
            assertEquals(7, Iterators.size(mreg.iterator()));

            assertEquals(0, mreg.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());

            assertEquals(0, mreg.<LongMetric>findMetric(LOGICAL_READS_INNER).value());

            assertEquals(0, mreg.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());

            assertEquals(0, mreg.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
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

        GridMetricManager mmgr = grid.context().metric();

        try {
            if (isPersistent)
                assertTrue(waitForCondition(() -> physicalReads(mmgr) > 0, TIMEOUT));
            else {
                assertTrue(waitForCondition(() -> logicalReads(mmgr) > 0, TIMEOUT));
                assertEquals(0, physicalReads(mmgr));
            }
        }
        finally {
            LOAD.set(false);
        }

        assertTrue(waitForCondition(() -> logicalReads(mmgr) == LOAD_COUNTER.get(), TIMEOUT));
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

        IgniteCache<Long, Long> cache = grid.getOrCreateCache(DEFAULT_CACHE_NAME);

        resetAllIoMetrics(grid);

        new Thread(() -> {
            while (LOAD.get()) {
                long i = LOAD_COUNTER.incrementAndGet();

                cache.put(i, i);
            }
        }).start();

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

        ignite.createCache(new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME));

        return ignite;
    }

    /**
     * @param mmgr Metric manager.
     * @return Number of physical reads since last reset statistics.
     */
    private long physicalReads(GridMetricManager mmgr) {
        return physicalReads(mmgr, CACHE_GROUP, DEFAULT_CACHE_NAME, null);
    }

    /**
     * @param mmgr Metric manager.
     * @return Logical reads count.
     */
    private long logicalReads(GridMetricManager mmgr) {
        return IoStatisticsCacheSelfTest.logicalReads(mmgr, HASH_INDEX, metricName(DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME));
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Number of physical reads since last reset statistics.
     */
    private long physicalReads(GridMetricManager mmgr, IoStatisticsType statType, String name, String subName) {
        String fullName = subName == null ? name : metricName(name, subName);

        MetricRegistry mreg = mmgr.registry(metricName(statType.metricGroupName(), fullName));

        assertNotNull(mreg);

        if (statType == CACHE_GROUP)
            return mreg.<LongMetric>findMetric(PHYSICAL_READS).value();
        else {
            long leaf = mreg.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value();
            long inner = mreg.<LongMetric>findMetric(PHYSICAL_READS_INNER).value();

            return leaf + inner;
        }
    }
}
