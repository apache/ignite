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
package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl.DATAREGION_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Tests metrics on a cluster activation.
 */
@RunWith(Parameterized.class)
public class MetricsClusterActivationTest extends GridCommonAbstractTest {
    /** */
    public static final int ENTRY_CNT = 50;

    /** */
    public static final int BACKUPS = 2;

    /** Persistence enabled flag. */
    @Parameterized.Parameter
    public boolean isPersistenceEnabled;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "isPersistenceEnabled={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(isPersistenceEnabled)
                    .setMetricsEnabled(true))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (isPersistenceEnabled)
            cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        if (isPersistenceEnabled)
            cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testReActivate() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(true)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(BACKUPS));

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.put(i, i);

        // Pause rebalance to check instant partitions states.
        grid(0).rebalanceEnabled(false);
        grid(1).rebalanceEnabled(false);
        stopGrid(2);

        checkMetrics(true);

        for (int i = 0; i < 3; i++) {
            ignite.cluster().state(ClusterState.INACTIVE);

            checkMetrics(false);

            ignite.cluster().state(ClusterState.ACTIVE);

            checkMetrics(isPersistenceEnabled);
        }
    }

    /** Checks metrics. */
    private void checkMetrics(boolean expEntries) throws IgniteCheckedException {
        for (IgniteEx ignite : F.transform(G.allGrids(), ignite -> (IgniteEx)ignite)) {
            checkDataRegionMetrics(ignite);

            checkCacheGroupsMetrics(ignite);

            checkCacheMetrics(ignite, expEntries);
        }
    }

    /** Checks data region metrics. */
    private void checkDataRegionMetrics(IgniteEx ignite) throws IgniteCheckedException {
        DataRegion region = ignite.context().cache().context().database().dataRegion(DFLT_DATA_REG_DEFAULT_NAME);

        MetricRegistry mreg = ignite.context().metric().registry(metricName(DATAREGION_METRICS_PREFIX,
            DFLT_DATA_REG_DEFAULT_NAME));

        if (!ignite.cluster().state().active()) {
            assertEquals(0, F.size(mreg.iterator()));

            return;
        }

        long offHeapSize = mreg.<LongMetric>findMetric("OffHeapSize").value();
        long initSize = mreg.<LongMetric>findMetric("InitialSize").value();
        long maxSize = mreg.<LongMetric>findMetric("MaxSize").value();

        assertTrue(offHeapSize > 0);
        assertTrue(offHeapSize <= region.config().getMaxSize());
        assertEquals(region.config().getInitialSize(), initSize);
        assertEquals(region.config().getMaxSize(), maxSize);
    }

    /** Checks cache groups metrics. */
    private void checkCacheGroupsMetrics(IgniteEx ignite) {
        MetricRegistry mreg = ignite.context().metric().registry(metricName(CACHE_GROUP_METRICS_PREFIX,
            DEFAULT_CACHE_NAME));

        if (!ignite.cluster().state().active()) {
            assertEquals(0, F.size(mreg.iterator()));

            return;
        }

        int minimumNumberOfPartitionCopies = mreg.<IntMetric>findMetric("MinimumNumberOfPartitionCopies").value();
        int maximumNumberOfPartitionCopies = mreg.<IntMetric>findMetric("MaximumNumberOfPartitionCopies").value();
        Map<Integer, List<String>> owningPartitionsAllocationMap = mreg.
            <ObjectMetric<Map<Integer, List<String>>>>findMetric("OwningPartitionsAllocationMap").value();
        Map<Integer, List<String>> movingPartitionsAllocationMap = mreg.
            <ObjectMetric<Map<Integer, List<String>>>>findMetric("MovingPartitionsAllocationMap").value();

        assertEquals(BACKUPS, minimumNumberOfPartitionCopies);
        assertEquals(BACKUPS, maximumNumberOfPartitionCopies);
        assertFalse(owningPartitionsAllocationMap.isEmpty());
        assertFalse(movingPartitionsAllocationMap.isEmpty());
    }

    /** Checks cache metrics. */
    private void checkCacheMetrics(IgniteEx ignite, boolean expEntries) {
        MetricRegistry mreg = ignite.context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        if (!ignite.cluster().state().active()) {
            assertEquals(0, F.size(mreg.iterator()));

            return;
        }

        long offHeapEntriesCnt = mreg.<LongMetric>findMetric("OffHeapEntriesCount").value();
        long offHeapPrimaryEntriesCnt = mreg.<LongMetric>findMetric("OffHeapPrimaryEntriesCount").value();
        long offHeapBackupEntriesCnt = mreg.<LongMetric>findMetric("OffHeapBackupEntriesCount").value();

        if (expEntries) {
            assertEquals(ENTRY_CNT, offHeapEntriesCnt);
            assertTrue(offHeapPrimaryEntriesCnt > 0);
            assertTrue(offHeapBackupEntriesCnt > 0);
        }
        else {
            assertEquals(0, offHeapEntriesCnt);
            assertEquals(0, offHeapPrimaryEntriesCnt);
            assertEquals(0, offHeapBackupEntriesCnt);
        }
    }
}
