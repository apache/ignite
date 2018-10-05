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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.stat.AggregatePageType.DATA;
import static org.apache.ignite.internal.stat.AggregatePageType.INDEX;
import static org.apache.ignite.internal.stat.PageType.T_DATA;
import static org.apache.ignite.internal.stat.PageType.T_DATA_REF_LEAF;

/**
 * Test of IO statistics MX bean.
 */
public class IoStatMetricsMXBeanImplTest extends GridCommonAbstractTest {

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

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Check that JMX bean exposed and works.
     *
     * @throws Exception In case of failure.
     */
    public void testBasic() throws Exception {
        IoStatMetricsMXBean bean = ioStatMXBean(0);

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        bean.resetStatistics();

        Assert.assertNotNull(bean.getStartGatheringStatistics());

        checkAggregatedStatIsEmpty(bean.getAggregatedLogicalReadsGlobal());
        checkAggregatedStatIsEmpty(bean.getAggregatedPhysicalReadsGlobal());
        checkAggregatedStatIsEmpty(bean.getAggregatedPhysicalWritesGlobal());

        populateCache(ignite, 100);

        checkAggregatedStatIsNotEmpty(bean.getAggregatedLogicalReadsGlobal());
    }

    /**
     * Check JMX universal statistics methods.
     *
     * @throws Exception In case of failure.
     */
    public void testUniversalStatisticMethods() throws Exception {
        IoStatMetricsMXBean bean = ioStatMXBean(0);

        int cnt = 300;

        bean.resetStatistics();

        populateCache(ignite, cnt);

        Map<String, Long> globalAggregated = bean.getLogicalReadStatistics(StatType.GLOBAL.name(), null, true);

        checkAggregatedStatIsNotEmpty(globalAggregated);

        Assert.assertEquals(Long.valueOf(cnt), globalAggregated.get(INDEX.name()));

        Map<String, Long> globalPlain = bean.getLogicalReadStatistics(StatType.GLOBAL.name(), null, false);

        Assert.assertEquals(globalAggregated.get(DATA.name()), globalPlain.get(T_DATA.name()));

        Assert.assertEquals(globalAggregated.get(INDEX.name()), globalPlain.get(T_DATA_REF_LEAF.name()));
    }

    /**
     * @param ignite Ignite instance.
     * @param cnt Number of inserting elements.
     */
    private void populateCache(IgniteEx ignite, int cnt) {
        for (int i = 0; i < cnt; i++)
            ignite.cache(DEFAULT_CACHE_NAME).put(i, i);
    }

    /**
     * @param aggregatedStat Aggregated IO statistics.
     */
    private void checkAggregatedStatIsNotEmpty(Map<String, Long> aggregatedStat) {
        for (AggregatePageType type : AggregatePageType.values()) {
            long val = aggregatedStat.get(type.name());

            Assert.assertTrue(val > 0);
        }
    }

    /**
     * @param aggregatedStat Aggregated IO statistics.
     */
    private void checkAggregatedStatIsEmpty(Map<String, Long> aggregatedStat) {
        for (AggregatePageType type : AggregatePageType.values()) {
            long val = aggregatedStat.get(type.name());

            Assert.assertEquals(0, val);
        }
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
