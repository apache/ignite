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

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import junit.framework.Assert;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IoStatisticsMetricsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.stat.IoStatisticsManager.HASH_PK_INDEX_NAME;

/**
 * Test of local node IO statistics MX bean.
 */
public class IoStatisticsMetricsLocalMXBeanImplSelfTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        final CacheConfiguration cCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Simple test JMX bean for indexes IO stats.
     *
     * @throws Exception In case of failure.
     */
    public void testIndexBasic() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        bean.resetStatistics();

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        int cnt = 100;

        populateCache(cnt);

        Set<String> idxHashNames = bean.getStatHashIndexesNames(DEFAULT_CACHE_NAME);

        Assert.assertEquals(1, idxHashNames.size());

        Assert.assertTrue(idxHashNames.contains(HASH_PK_INDEX_NAME));

        long idxLeafCnt = bean.getIndexLeafLogicalReadsStatistics(DEFAULT_CACHE_NAME, HASH_PK_INDEX_NAME);

        Assert.assertEquals(cnt, idxLeafCnt);

        Long aggregatedIdxLogicalRads = bean.getIndexLogicalReadsStatistics(DEFAULT_CACHE_NAME, HASH_PK_INDEX_NAME);

        Assert.assertNotNull(aggregatedIdxLogicalRads);

        Assert.assertEquals(aggregatedIdxLogicalRads.longValue(), idxLeafCnt);

        String formatted = bean.getIndexStatisticsFormatted(DEFAULT_CACHE_NAME, HASH_PK_INDEX_NAME);

        Assert.assertEquals("HASH_INDEX default.PK [LOGICAL_READS_LEAF=100, LOGICAL_READS_INNER=0, " +
            "PHYSICAL_READS_INNER=0, PHYSICAL_READS_LEAF=0]", formatted);

        String unexistedStats = bean.getIndexStatisticsFormatted("unknownCache", "unknownIdx");

        Assert.assertEquals("SORTED_INDEX unknownCache.unknownIdx []", unexistedStats);
    }

    /**
     * Simple test JMX bean for caches IO stats.
     *
     * @throws Exception In case of failure.
     */
    public void testCacheBasic() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        bean.resetStatistics();

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        int cnt = 100;

        warmUpMemmory(bean, cnt);

        populateCache(cnt);

        Set<String> cacheNames = bean.getStatCachesNames();

        Assert.assertEquals(1, cacheNames.size());

        Assert.assertTrue(cacheNames.contains(DEFAULT_CACHE_NAME));

        Long cacheLogicalReadsCnt = bean.getCacheLogicalReadsStatistics(DEFAULT_CACHE_NAME);

        Assert.assertNotNull(cacheLogicalReadsCnt);

        Assert.assertEquals(cnt, cacheLogicalReadsCnt.longValue());

        String formatted = bean.getCacheStatisticsFormatted(DEFAULT_CACHE_NAME);

        Assert.assertEquals("CACHE_GROUP default [LOGICAL_READS=100, PHYSICAL_READS=0]", formatted);

        String unexistedStats = bean.getCacheStatisticsFormatted("unknownCache");

        Assert.assertEquals("CACHE_GROUP unknownCache []", unexistedStats);
    }

    /**
     * Warm up memmory to allocate partitions cache pages related to inserting keys.
     *
     * @param bean JMX bean.
     * @param cnt Number of inserting elements.
     */
    private void warmUpMemmory(IoStatisticsMetricsMXBean bean, int cnt) {
        populateCache(cnt);

        clearCache(cnt);

        bean.resetStatistics();
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void populateCache(int cnt) {
        for (int i = 0; i < cnt; i++)
            ignite.cache(DEFAULT_CACHE_NAME).put(i, i);
    }

    /**
     * @param cnt Number of removing elements.
     */
    private void clearCache(int cnt) {
        for (int i = 0; i < cnt; i++)
            ignite.cache(DEFAULT_CACHE_NAME).remove(i);
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
