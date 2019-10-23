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

package org.apache.ignite.internal.stat;

import java.lang.management.ManagementFactory;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IoStatisticsMetricsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;

/**
 * Test of local node IO statistics MX bean.
 */
@RunWith(Parameterized.class)
public class IoStatisticsMetricsLocalMXBeanImplSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_1_NAME = "cache1";

    /** */
    public static final String CACHE_2_NAME = "cache2";

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicity1;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicity2;

    /** */
    @Parameterized.Parameters(name = "Cache 1 = {0}, Cache 2 = {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CacheAtomicityMode.TRANSACTIONAL},
            new Object[] {CacheAtomicityMode.ATOMIC, CacheAtomicityMode.ATOMIC},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CacheAtomicityMode.ATOMIC},
            new Object[] {CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL}
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
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testExistingCachesMetrics() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        final CacheConfiguration cCfg1 = new CacheConfiguration()
            .setName(CACHE_1_NAME)
            .setDataRegionName("default")
            .setAtomicityMode(atomicity1)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        final CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName(CACHE_2_NAME)
            .setDataRegionName("persistent")
            .setAtomicityMode(atomicity2)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        try {
            ignite(0).getOrCreateCaches(Arrays.asList(cCfg1, cCfg2));

            IoStatisticsManager ioStatMgr = ignite(0).context().ioStats();

            Assert.assertEquals(ioStatMgr.startTime().toEpochSecond(), bean.getStartTime());

            Assert.assertEquals(ioStatMgr.startTime().format(DateTimeFormatter.ISO_DATE_TIME), bean.getStartTimeLocal());

            bean.reset();

            Assert.assertEquals(ioStatMgr.startTime().toEpochSecond(), bean.getStartTime());

            Assert.assertEquals(ioStatMgr.startTime().format(DateTimeFormatter.ISO_DATE_TIME), bean.getStartTimeLocal());

            // Check that in initial state all metrics are zero.
            assertEquals(0, (long)bean.getIndexLeafLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLeafPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getCacheGroupLogicalReads(CACHE_1_NAME));
            assertEquals(0, (long)bean.getCacheGroupPhysicalReads(CACHE_1_NAME));

            assertEquals(0, (long)bean.getIndexLeafLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLeafPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getCacheGroupLogicalReads(CACHE_2_NAME));
            assertEquals(0, (long)bean.getCacheGroupPhysicalReads(CACHE_2_NAME));

            int cnt = 500;

            populateCaches(0, cnt);

            bean.reset();

            readCaches(0, cnt);

            // 1 of the reads will get resolved from the inner page.
            int off = 1;

            assertEquals(cnt - off, (long)bean.getIndexLeafLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexLeafPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(cnt, (long)bean.getIndexInnerLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexInnerPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt - off, (long)bean.getIndexLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            // Each data page is touched twice - one during index traversal and second
            assertEquals(2 * cnt, (long)bean.getCacheGroupLogicalReads(CACHE_1_NAME));
            assertEquals(0,   (long)bean.getCacheGroupPhysicalReads(CACHE_1_NAME));

            assertEquals(cnt - off, (long)bean.getIndexLeafLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexLeafPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(cnt, (long)bean.getIndexInnerLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexInnerPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt - off, (long)bean.getIndexLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt, (long)bean.getCacheGroupLogicalReads(CACHE_2_NAME));
            assertEquals(0,   (long)bean.getCacheGroupPhysicalReads(CACHE_2_NAME));

            Assert.assertEquals("HASH_INDEX cache1.HASH_PK [LOGICAL_READS_LEAF=" + (cnt - off) +
                    ", LOGICAL_READS_INNER=" + cnt + ", " +
                "PHYSICAL_READS_INNER=0, PHYSICAL_READS_LEAF=0]",
                bean.getIndexStatistics(CACHE_1_NAME, HASH_PK_IDX_NAME));

            Assert.assertEquals("HASH_INDEX cache2.HASH_PK [LOGICAL_READS_LEAF=" + (cnt - off) +
                    ", LOGICAL_READS_INNER=" + cnt + ", " +
                "PHYSICAL_READS_INNER=0, PHYSICAL_READS_LEAF=0]",
                bean.getIndexStatistics(CACHE_2_NAME, HASH_PK_IDX_NAME));

            // Check that logical reads keep growing.
            readCaches(0, cnt);

            assertEquals(2 * (cnt - off), (long)bean.getIndexLeafLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexLeafPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt, (long)bean.getIndexInnerLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexInnerPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * (2 * cnt - off), (long)bean.getIndexLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * (2 * cnt), (long)bean.getCacheGroupLogicalReads(CACHE_1_NAME));
            assertEquals(0,   (long)bean.getCacheGroupPhysicalReads(CACHE_1_NAME));

            assertEquals(2 * (cnt - off), (long)bean.getIndexLeafLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexLeafPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt, (long)bean.getIndexInnerLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexInnerPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * (2 * cnt - off), (long)bean.getIndexLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0,   (long)bean.getIndexPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * (2 * cnt), (long)bean.getCacheGroupLogicalReads(CACHE_2_NAME));
            assertEquals(0,   (long)bean.getCacheGroupPhysicalReads(CACHE_2_NAME));

            // Force physical reads
            ignite(0).cluster().active(false);
            ignite(0).cluster().active(true);

            bean.reset();

            assertEquals(0, (long)bean.getIndexLeafLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLeafPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLogicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexPhysicalReads(CACHE_1_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getCacheGroupLogicalReads(CACHE_1_NAME));
            assertEquals(0, (long)bean.getCacheGroupPhysicalReads(CACHE_1_NAME));

            assertEquals(0, (long)bean.getIndexLeafLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLeafPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexInnerPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getIndexPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(0, (long)bean.getCacheGroupLogicalReads(CACHE_2_NAME));
            assertEquals(0, (long)bean.getCacheGroupPhysicalReads(CACHE_2_NAME));

            readCaches(0, cnt);

            assertEquals(cnt - off, (long)bean.getIndexLeafLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            // We had a split, so now we have 2 leaf pages...
            assertEquals(2, (long)bean.getIndexLeafPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(cnt, (long)bean.getIndexInnerLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            // And 1 inner page
            assertEquals(1, (long)bean.getIndexInnerPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt - off, (long)bean.getIndexLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(3, (long)bean.getIndexPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt, (long)bean.getCacheGroupLogicalReads(CACHE_2_NAME));

            Long physReads = bean.getCacheGroupPhysicalReads(CACHE_2_NAME);
            // For sure should overflow 2 data pages.
            assertTrue(physReads > 2);

            // Check that metrics keep growing. Logical reads will increase, physycal reads will be the same.
            readCaches(0, cnt);

            assertEquals(2 * (cnt - off), (long)bean.getIndexLeafLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            // We had a split, so now we have 2 leaf pages...
            assertEquals(2,   (long)bean.getIndexLeafPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * cnt, (long)bean.getIndexInnerLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            // And 1 inner page
            assertEquals(1,   (long)bean.getIndexInnerPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * (2 * cnt - off), (long)bean.getIndexLogicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(3,   (long)bean.getIndexPhysicalReads(CACHE_2_NAME, HASH_PK_IDX_NAME));
            assertEquals(2 * (2 * cnt), (long)bean.getCacheGroupLogicalReads(CACHE_2_NAME));
            assertEquals(physReads, bean.getCacheGroupPhysicalReads(CACHE_2_NAME));
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
     * @return IO statistics MX bean for node with given index.
     * @throws Exception In case of failure.
     */
    private IoStatisticsMetricsMXBean ioStatMXBean() throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(0), "IOMetrics",
            IoStatisticsMetricsLocalMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IoStatisticsMetricsMXBean.class, false);
    }
}
