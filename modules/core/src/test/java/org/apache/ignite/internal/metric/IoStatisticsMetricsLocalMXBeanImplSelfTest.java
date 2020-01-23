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

import java.util.stream.StreamSupport;
import javax.management.MalformedObjectNameException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
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
import static org.apache.ignite.internal.metric.MetricsConfigurationTest.metricsBean;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

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
        resetMetric(ignite, metricName(HASH_INDEX.metricGroupName(), DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME));

        int cnt = 100;

        populateCache(cnt);

        MetricRegistry mreg = ignite.context().metric()
            .registry(metricName(HASH_INDEX.metricGroupName(), DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME));

        long idxLeafLogicalCnt = mreg.<LongMetric>findMetric(LOGICAL_READS_LEAF).value();

        assertEquals(cnt, idxLeafLogicalCnt);

        long idxLeafPhysicalCnt = mreg.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value();

        assertEquals(0, idxLeafPhysicalCnt);

        long idxInnerLogicalCnt = mreg.<LongMetric>findMetric(LOGICAL_READS_INNER).value();

        assertEquals(0, idxInnerLogicalCnt);

        long idxInnerPhysicalCnt = mreg.<LongMetric>findMetric(PHYSICAL_READS_INNER).value();

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

        resetMetric(ignite, metricName(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME));

        populateCache(cnt);

        MetricRegistry mreg = ignite.context().metric()
            .registry(metricName(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME));

        long cacheLogicalReadsCnt = mreg.<LongMetric>findMetric(LOGICAL_READS).value();

        assertEquals(cnt, cacheLogicalReadsCnt);

        long cachePhysicalReadsCnt = mreg.<LongMetric>findMetric(PHYSICAL_READS).value();

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
        GridMetricManager mmgr = ignite.context().metric();

        StreamSupport.stream(mmgr.spliterator(), false)
            .map(ReadOnlyMetricRegistry::name)
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
        metricsBean(ignite).resetMetrics(grpName);
    }
}
