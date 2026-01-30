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
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.INSERTED_BYTES;
import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.REMOVED_BYTES;
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
    @Override protected void beforeTest() throws Exception {
        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
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

        assertEquals(cnt - 1, cacheLogicalReadsCnt); // 1 is for reuse bucket stripe.

        long cachePhysicalReadsCnt = mreg.<LongMetric>findMetric(PHYSICAL_READS).value();

        assertEquals(0, cachePhysicalReadsCnt);
    }

    /** */
    @Test
    public void testInsertedDeletedBytes() {
        int cnt = 100;

        MetricRegistry mreg = ignite.context().metric()
            .registry(metricName(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME));

        LongMetric insertedBytes = mreg.findMetric(INSERTED_BYTES);
        LongMetric removedBytes = mreg.findMetric(REMOVED_BYTES);

        assertEquals(0, insertedBytes.value());

        populateCache(cnt);

        int minEntrySize = 20; // Size of key, size of val, entry headers, data page payload headers, etc.
        int maxEntrySize = 100;

        assertTrue(insertedBytes.value() > cnt * minEntrySize);
        assertTrue(insertedBytes.value() < cnt * maxEntrySize);

        assertEquals(0, removedBytes.value());

        clearCache(cnt);

        assertEquals(insertedBytes.value(), removedBytes.value());
    }

    /** */
    @Test
    public void testInsertedDeletedBytesOnRebalance() throws Exception {
        int cnt = 100;

        MetricRegistry mreg0 = ignite.context().metric()
            .registry(metricName(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME));

        LongMetric insertedBytes0 = mreg0.findMetric(INSERTED_BYTES);
        LongMetric removedBytes0 = mreg0.findMetric(REMOVED_BYTES);

        populateCache(cnt);

        assertNotSame(0, insertedBytes0.value());
        assertEquals(0, removedBytes0.value());

        IgniteEx ignite1 = startGrid(1);

        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);

        MetricRegistry mreg1 = ignite1.context().metric()
            .registry(metricName(CACHE_GROUP.metricGroupName(), DEFAULT_CACHE_NAME));

        LongMetric insertedBytes1 = mreg1.findMetric(INSERTED_BYTES);
        LongMetric removedBytes1 = mreg1.findMetric(REMOVED_BYTES);

        assertNotSame(0, removedBytes0.value());
        assertNotSame(0, insertedBytes1.value());
        assertEquals(0, removedBytes1.value());

        clearCache(cnt);

        assertNotSame(0, removedBytes1.value());

        assertTrue(GridTestUtils.waitForCondition(() -> insertedBytes0.value() == removedBytes0.value(), 1_000L));
        assertTrue(GridTestUtils.waitForCondition(() -> insertedBytes1.value() == removedBytes1.value(), 1_000L));
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
