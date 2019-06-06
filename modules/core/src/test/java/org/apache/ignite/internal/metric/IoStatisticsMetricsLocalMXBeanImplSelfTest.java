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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderQuery.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderQuery.PHYSICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.metric.IoStatisticsType.HASH_INDEX;

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

    /**
     * Simple test JMX bean for indexes IO stats.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testIndexBasic() throws Exception {
        resetMetric(ignite, HASH_INDEX.metricGroupName());

        int cnt = 100;

        populateCache(cnt);

        MetricRegistry mreg = ignite.context().metric().registry()
            .withPrefix(HASH_INDEX.metricGroupName(), DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME);

        long idxLeafLogicalCnt = ((LongMetric)mreg.findMetric(LOGICAL_READS_LEAF)).value();

        assertEquals(cnt, idxLeafLogicalCnt);

        long idxLeafPhysicalCnt = ((LongMetric)mreg.findMetric(PHYSICAL_READS_LEAF)).value();

        assertEquals(0, idxLeafPhysicalCnt);

        long idxInnerLogicalCnt = ((LongMetric)mreg.findMetric(LOGICAL_READS_INNER)).value();

        assertEquals(0, idxInnerLogicalCnt);

        long idxInnerPhysicalCnt = ((LongMetric)mreg.findMetric(PHYSICAL_READS_INNER)).value();

        assertEquals(0, idxInnerPhysicalCnt);
    }

    /**
     * Simple test JMX bean for caches IO stats.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testCacheBasic() throws Exception {
        int cnt = 100;

        populateCache(cnt);

        clearCache(cnt);

        resetMetric(ignite, CACHE_GROUP.metricGroupName());

        populateCache(cnt);

        MetricRegistry mreg = ignite.context().metric().registry()
            .withPrefix(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME);

        long cacheLogicalReadsCnt = ((LongMetric)mreg.findMetric(LOGICAL_READS)).value();

        assertEquals(cnt, cacheLogicalReadsCnt);

        long cachePhysicalReadsCnt = ((LongMetric)mreg.findMetric(PHYSICAL_READS)).value();

        assertEquals(0, cachePhysicalReadsCnt);
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
     * Resets all io statistics.
     *
     * @param ignite Ignite.
     */
    public static void resetAllIoMetrics(IgniteEx ignite) throws MalformedObjectNameException {
        for (IoStatisticsType type : IoStatisticsType.values())
            resetMetric(ignite, type.metricGroupName());
    }

    /**
     * Resets all metrics for a given prefix.
     *
     * @param prefix Prefix to reset metrics.
     */
    public static void resetMetric(IgniteEx ignite, String prefix) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(ignite.name(), "Kernal",
            IgniteKernal.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        IgniteMXBean bean = MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IgniteMXBean.class, false);

        bean.resetMetrics(prefix);
    }
}
