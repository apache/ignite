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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.impl.PeriodicHistogramMetricImpl;
import org.apache.ignite.internal.util.GridTestClockTimer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.PagesTimestampHistogramView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.PAGE_TS_HISTOGRAM_VIEW;

/** Tests for {@link SystemView} for page timestamps. */
public class SystemViewPageTimestampsTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testPagesTimestampHistogram() throws Exception {
        int keysCnt = 50_000;

        AtomicLong curTime = new AtomicLong(System.currentTimeMillis());

        GridTestClockTimer.timeSupplier(curTime::get);
        GridTestClockTimer.update();

        String regionName = "default";

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setMaxSize(50L * 1024 * 1024)
                .setPersistenceEnabled(true)
                .setName(regionName)
                .setMetricsEnabled(true)
        );

        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>("test-pages-ts1")
                .setAffinity(new RendezvousAffinityFunction(false, 10));

            CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>("test-pages-ts2")
                .setAffinity(new RendezvousAffinityFunction(false, 10));

            IgniteCache<Object, Object> cache1 = ignite.createCache(ccfg1);

            long ts1 = curTime.get();

            for (int i = 0; i < 1000; i++)
                cache1.put(i, i);

            long ts2 = curTime.addAndGet(PeriodicHistogramMetricImpl.DFLT_BUCKETS_INTERVAL);
            GridTestClockTimer.update();

            for (int i = 1000; i < 2000; i++)
                cache1.put(i, i);

            SystemView<PagesTimestampHistogramView> pagesTsHistogram =
                ignite.context().systemView().view(PAGE_TS_HISTOGRAM_VIEW);

            assertNotNull(pagesTsHistogram);

            long totalCnt = 0;

            for (PagesTimestampHistogramView view : pagesTsHistogram) {
                if (regionName.equals(view.dataRegionName())) {
                    if ((ts1 >= view.intervalStart().getTime() && ts1 <= view.intervalEnd().getTime()) ||
                        (ts2 >= view.intervalStart().getTime() && ts2 <= view.intervalEnd().getTime())) {
                        assertTrue("Unexpected pages count: " + view.pagesCount(), view.pagesCount() > 0);

                        totalCnt += view.pagesCount();
                    }
                    else
                        assertEquals(0, view.pagesCount());
                }
            }

            assertTrue(totalCnt > 0);
            assertEquals(ignite.dataRegionMetrics(regionName).getPhysicalMemoryPages(), totalCnt);

            assertEquals(2, F.size(F.iterator(pagesTsHistogram, v -> v, true, v -> v.pagesCount() > 0)));

            // Check histogram after replacement.
            long ts3 = curTime.addAndGet(PeriodicHistogramMetricImpl.DFLT_BUCKETS_INTERVAL);
            GridTestClockTimer.update();

            ignite.createCache(ccfg2);

            try (IgniteDataStreamer<Integer, Object> streamer = ignite.dataStreamer("test-pages-ts2")) {
                for (int i = 0; i < keysCnt; i++)
                    streamer.addData(i, new byte[1000]);
            }

            assertEquals(ignite.dataRegionMetrics(regionName).getPhysicalMemoryPages(),
                F.sumInt(F.iterator(pagesTsHistogram, v -> (int)v.pagesCount(), true)));

            assertFalse(F.isEmpty(F.iterator(pagesTsHistogram, v -> v, true, v -> v.pagesCount() > 0 &&
                v.intervalStart().getTime() <= ts3 && ts3 <= v.intervalEnd().getTime())));

            // Check histogram after cache destroy and remove of outdated pages.
            long ts4 = curTime.addAndGet(PeriodicHistogramMetricImpl.DFLT_BUCKETS_INTERVAL);
            GridTestClockTimer.update();

            ignite.destroyCache("test-pages-ts2");

            IgniteCache<Object, Object> cache2 = ignite.createCache(ccfg2);

            for (int i = 0; i < keysCnt; i++) {
                cache1.put(i, i);
                cache2.put(i, new byte[1000]);
            }

            assertEquals(ignite.dataRegionMetrics(regionName).getPhysicalMemoryPages(),
                F.sumInt(F.iterator(pagesTsHistogram, v -> (int)v.pagesCount(), true)));

            assertFalse(F.isEmpty(F.iterator(pagesTsHistogram, v -> v, true, v -> v.pagesCount() > 0 &&
                v.intervalStart().getTime() <= ts4 && ts4 <= v.intervalEnd().getTime())));
        }
        finally {
            GridTestClockTimer.timeSupplier(GridTestClockTimer.DFLT_TIME_SUPPLIER);
        }
    }

    /** */
    @Test
    public void testPagesTimestampHistogramAfterPartitionEviction() throws Exception {
        int keysCnt = 50_000;

        String regionName = "default";

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setMaxSize(50L * 1024 * 1024)
                .setPersistenceEnabled(true)
                .setName(regionName)
                .setMetricsEnabled(true)
        );

        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>("test-pages-ts")
                .setBackups(1).setAffinity(new RendezvousAffinityFunction(false, 10)));

            try (IgniteDataStreamer<Integer, Object> streamer = ignite.dataStreamer("test-pages-ts")) {
                for (int i = 0; i < keysCnt; i++)
                    streamer.addData(i, new byte[1000]);
            }

            startGrid(getConfiguration(getTestIgniteInstanceName(1)).setDataStorageConfiguration(dsCfg));
            startGrid(getConfiguration(getTestIgniteInstanceName(2)).setDataStorageConfiguration(dsCfg));

            resetBaselineTopology();

            awaitPartitionMapExchange(true, true, null);

            // Force checkpoint to invalidate evicted partitions.
            forceCheckpoint(ignite);

            // Check histogram after partition eviction.
            SystemView<PagesTimestampHistogramView> pagesTsHistogram =
                ignite.context().systemView().view(PAGE_TS_HISTOGRAM_VIEW);

            assertEquals(ignite.dataRegionMetrics(regionName).getPhysicalMemoryPages(),
                F.sumInt(F.iterator(pagesTsHistogram, v -> (int)v.pagesCount(), true)));

            stopGrid(2);

            resetBaselineTopology();

            // Wait until rebalance complete.
            assertTrue(GridTestUtils.waitForCondition(() -> ignite.context().discovery().topologyVersionEx()
                .minorTopologyVersion() >= 2, 5_000L));

            // Check histogram after rebalance.
            assertEquals(ignite.dataRegionMetrics(regionName).getPhysicalMemoryPages(),
                F.sumInt(F.iterator(pagesTsHistogram, v -> (int)v.pagesCount(), true)));

            stopGrid(1);

            resetBaselineTopology();

            // Allocate some pages after eviction.
            for (int i = 0; i < 10_000; i++)
                cache.put(i + keysCnt, new byte[1024]);

            // Acquire some outdated pages.
            for (int i = 0; i < keysCnt + 10_000; i++)
                assertNotNull(cache.get(i));

            // Check histogram after replacement of outdated pages.
            assertEquals(ignite.dataRegionMetrics(regionName).getPhysicalMemoryPages(),
                F.sumInt(F.iterator(pagesTsHistogram, v -> (int)v.pagesCount(), true)));
        }
        finally {
            stopAllGrids();
        }
    }
}
