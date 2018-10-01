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
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import junit.framework.Assert;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IoStatMetricsMXBean;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IoStatMetricsMXBeanImplTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        final CacheConfiguration cCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /**
     * Check that JMX bean exposed and works.
     *
     * @throws Exception in case of failure.
     */
    public void testBasic() throws Exception {
        IgniteEx ignite = startGrid(0);

        IoStatMetricsMXBean bean = ioStatMXBean(0);

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        checkAllTypesIOStatisticsPresent(bean);

        bean.resetStatistics();

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        checkAggregatedStatIsEmpty(bean.getAggregatedLogicalReads());
        checkAggregatedStatIsEmpty(bean.getAggregatedPhysicalReads());
        checkAggregatedStatIsEmpty(bean.getAggregatedPhysicalWrites());

        for (int i = 0; i < 100; i++)
            ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

        checkAggregatedStatIsNotEmpty(bean.getAggregatedLogicalReads());

    }

    /**
     * @param aggregatedStat Aggregated IO statistics.
     */
    private void checkAggregatedStatIsNotEmpty(Map<String, Long> aggregatedStat) {
        long aggregatedReadIdx = aggregatedStat.get(AggregatePageType.INDEX.name());

        Assert.assertTrue(aggregatedReadIdx > 0);

        long aggregatedReadOther = aggregatedStat.get(AggregatePageType.OTHER.name());

        Assert.assertTrue(aggregatedReadOther > 0);

        long aggregatedReadData = aggregatedStat.get(AggregatePageType.DATA.name());

        Assert.assertTrue(aggregatedReadData > 0);
    }

    /**
     * @param aggregatedStat Aggregated IO statistics.
     */
    private void checkAggregatedStatIsEmpty(Map<String, Long> aggregatedStat) {
        long aggregatedReadIdx = aggregatedStat.get(AggregatePageType.INDEX.name());

        Assert.assertEquals(0, aggregatedReadIdx);

        long aggregatedReadOther = aggregatedStat.get(AggregatePageType.OTHER.name());

        Assert.assertEquals(0, aggregatedReadOther);

        long aggregatedReadData = aggregatedStat.get(AggregatePageType.DATA.name());

        Assert.assertEquals(0, aggregatedReadData);
    }

    /**
     * @param bean IO statistics MX bean.
     */
    private void checkAllTypesIOStatisticsPresent(IoStatMetricsMXBean bean) {
        Map<String, Long> logicalReads = bean.getLogicalReads();

        int pageTypesCnt = PageType.values().length;

        assertEquals(pageTypesCnt, logicalReads.size());

        Map<String, Long> physicalReads = bean.getPhysicalReads();

        assertEquals(pageTypesCnt, physicalReads.size());

        Map<String, Long> physicalWrites = bean.getPhysicalWrites();

        assertEquals(pageTypesCnt, physicalWrites.size());

        int aggPageTypesCnt = AggregatePageType.values().length;

        Map<String, Long> aggReads = bean.getAggregatedLogicalReads();

        assertEquals(aggPageTypesCnt, aggReads.size());
    }

    /**
     * @param igniteIdx Ignite node index.
     * @return IO statistics MX bean for node with given index.
     * @throws Exception In case of failure.
     */
    private IoStatMetricsMXBean ioStatMXBean(int igniteIdx) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteIdx), "IOMetrics",
            IoStatMetricsLocalMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IoStatMetricsMXBean.class, false);
    }
}
