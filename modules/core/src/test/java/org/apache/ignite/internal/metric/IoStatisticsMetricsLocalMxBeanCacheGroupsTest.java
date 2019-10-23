/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metric;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.StreamSupport;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderQuery.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderQuery.PHYSICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.metric.IoStatisticsType.HASH_INDEX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Test of local node IO statistics MX bean.
 */
@RunWith(Parameterized.class)
public class IoStatisticsMetricsLocalMxBeanCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_1_NAME = "cache1";

    /** */
    public static final String CACHE_2_NAME = "cache2";

    /** */
    public static final String CACHE_GROUP_NAME = "cacheGroup";

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicity;

    /** */
    @Parameterized.Parameter(1)
    public boolean persistent;

    /** */
    @Parameterized.Parameters(name = "Cache Atomicity = {0}, persistent = {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, true},
            new Object[] {CacheAtomicityMode.ATOMIC, true},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, false},
            new Object[] {CacheAtomicityMode.ATOMIC, false}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(256 * 1024L * 1024L).setName("default"));

        dsCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setPersistenceEnabled(true).setMaxSize(256 * 1024 * 1024).setName("persistent"));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /**
     * Simple test JMX bean for indexes IO stats.
     */
    @Test
    public void testExistingCachesMetrics() {
        IgniteEx ignite = ignite(0);

        final CacheConfiguration cCfg1 = new CacheConfiguration()
            .setName(CACHE_1_NAME)
            .setDataRegionName(persistent ? "persistent" : "default")
            .setGroupName(CACHE_GROUP_NAME)
            .setAtomicityMode(atomicity)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        final CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName(CACHE_2_NAME)
            .setDataRegionName(persistent ? "persistent" : "default")
            .setGroupName(CACHE_GROUP_NAME)
            .setAtomicityMode(atomicity)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        try {
            ignite(0).getOrCreateCaches(Arrays.asList(cCfg1, cCfg2));

            MetricRegistry pk = ignite.context().metric()
                .registry(metricName(HASH_INDEX.metricGroupName(), CACHE_GROUP_NAME, HASH_PK_IDX_NAME));
            MetricRegistry grp = ignite.context().metric()
                .registry(metricName(CACHE_GROUP.metricGroupName(), CACHE_GROUP_NAME));

            resetAllIoMetrics(ignite);

            // Check that in initial state all metrics are zero.
            assertEquals(0, pk.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
            assertEquals(0, pk.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
            assertEquals(0, pk.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
            assertEquals(0, pk.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
            assertEquals(0, grp.<LongMetric>findMetric(LOGICAL_READS).value());
            assertEquals(0, grp.<LongMetric>findMetric(PHYSICAL_READS).value());

            int cnt = 180;

            populateCaches(0, cnt);

            resetAllIoMetrics(ignite);

            readCaches(0, cnt);

            // 1 of the reads got resolved from the inner page.
            // Each data page is touched twice - one during index traversal and second
            assertEquals(2 * cnt - 1, pk.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
            assertEquals(0, pk.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
            assertEquals(2 * cnt, pk.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
            assertEquals(0, pk.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
            assertEquals(4 * cnt, grp.<LongMetric>findMetric(LOGICAL_READS).value());
            assertEquals(0, grp.<LongMetric>findMetric(PHYSICAL_READS).value());

            if (persistent) {
                // Force physical reads
                ignite.cluster().active(false);
                ignite.cluster().active(true);

                resetAllIoMetrics(ignite);

                assertEquals(0, pk.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
                assertEquals(0, pk.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
                assertEquals(0, pk.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
                assertEquals(0, pk.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
                assertEquals(0, grp.<LongMetric>findMetric(LOGICAL_READS).value());
                assertEquals(0, grp.<LongMetric>findMetric(PHYSICAL_READS).value());

                readCaches(0, cnt);

                // We had a split, so now we have 2 leaf pages and 1 inner page read from disk.
                // For sure should overflow 2 data pages.
                assertEquals(2 * cnt - 1, pk.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
                assertEquals(2, pk.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
                assertEquals(2 * cnt, pk.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
                assertEquals(1, pk.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
                assertEquals(4 * cnt, grp.<LongMetric>findMetric(LOGICAL_READS).value());

                long physReads = grp.<LongMetric>findMetric(PHYSICAL_READS).value();
                assertTrue(physReads > 2);

                // Check that metrics are further increasing for logical reads and stay the same for physical reads.
                readCaches(0, cnt);

                assertEquals(2 * (2 * cnt - 1), pk.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
                assertEquals(2, pk.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
                assertEquals(2 * 2 * cnt, pk.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
                assertEquals(1, pk.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
                assertEquals(2 * 4 * cnt, grp.<LongMetric>findMetric(LOGICAL_READS).value());
                assertEquals(physReads, grp.<LongMetric>findMetric(PHYSICAL_READS).value());
            }
        }
        finally {
            ignite(0).destroyCache(CACHE_1_NAME);
            ignite(0).destroyCache(CACHE_2_NAME);
        }
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void populateCaches(int start, int cnt) {
        for (int i = start; i < cnt; i++) {
            ignite(0).cache(CACHE_1_NAME).put(i, i);

            ignite(0).cache(CACHE_2_NAME).put(i, i);
        }
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void readCaches(int start, int cnt) {
        for (int i = start; i < cnt; i++) {
            ignite(0).cache(CACHE_1_NAME).get(i);

            ignite(0).cache(CACHE_2_NAME).get(i);
        }
    }

    /**
     * Resets all io statistics.
     *
     * @param ignite Ignite.
     */
    public static void resetAllIoMetrics(IgniteEx ignite) {
        GridMetricManager mmgr = ignite.context().metric();

        StreamSupport.stream(mmgr.spliterator(), false)
            .map(MetricRegistry::name)
            .filter(name -> {
                for (IoStatisticsType type : IoStatisticsType.values()) {
                    if (name.startsWith(type.metricGroupName()))
                        return true;
                }

                return false;
            })
            .forEach(grpName -> resetMetric(ignite, grpName));

    }

    /**
     * Resets all metrics for a given prefix.
     *
     * @param grpName Group name to reset metrics.
     */
    public static void resetMetric(IgniteEx ignite, String grpName) {
        try {
            ObjectName mbeanName = U.makeMBeanName(ignite.name(), "Kernal",
                IgniteKernal.class.getSimpleName());

            MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

            if (!mbeanSrv.isRegistered(mbeanName))
                fail("MBean is not registered: " + mbeanName.getCanonicalName());

            IgniteMXBean bean = MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IgniteMXBean.class, false);

            bean.resetMetrics(grpName);
        } catch (MalformedObjectNameException e) {
            throw new IgniteException(e);
        }
    }
}
