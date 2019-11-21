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

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_SYSTEM_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

/** Tests metrics configuration. */
public class MetricsConfigurationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.startsWith("persistent")) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration();

            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
            dsCfg.setWalMode(FSYNC);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /** Tests configuration of {@link HitRateMetric}. */
    @Test
    public void testHitRateConfiguration() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            IgniteMXBean bean = (IgniteMXBean)g;

            //Empty name.
            assertThrowsWithCause(
                () -> bean.configureHitRateMetric(null, 1),
                NullPointerException.class);

            //Wrong rateTimeInterval value.
            assertThrowsWithCause(
                () -> bean.configureHitRateMetric("io.dataregion.default.AllocationRate", 0),
                IllegalArgumentException.class);

            assertThrowsWithCause(
                () -> bean.configureHitRateMetric("io.dataregion.default.AllocationRate", -1),
                IllegalArgumentException.class);

            bean.configureHitRateMetric("io.dataregion.default.AllocationRate", 5000);

            HitRateMetric allocationRate = g.context().metric().registry(metricName("io.dataregion.default"))
                .findMetric("AllocationRate");

            assertEquals(5000, allocationRate.rateTimeInterval());
        }
    }

    /** Tests configuration of {@link HistogramMetric}. */
    @Test
    public void testHistogramConfiguration() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            IgniteMXBean bean = (IgniteMXBean)g;

            long[] bounds = new long[] {50, 100};

            //Empty name.
            assertThrowsWithCause(() -> bean.configureHistogramMetric(null, bounds), NullPointerException.class);

            //Wrong bounds value.
            assertThrowsWithCause(
                () -> bean.configureHistogramMetric(metricName(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM), null),
                NullPointerException.class);

            assertThrowsWithCause(
                () -> bean.configureHistogramMetric(metricName(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM), new long[0]),
                IllegalArgumentException.class);

            bean.configureHistogramMetric(metricName(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM), bounds);

            HistogramMetric systemTime = g.context().metric().registry(TX_METRICS)
                .findMetric(METRIC_SYSTEM_TIME_HISTOGRAM);

            assertArrayEquals(bounds, systemTime.bounds());
        }
    }

    /** Tests metric configuration applied on all nodes. */
    @Test
    public void testConfigurationSeveralNodes() throws Exception {
        try (IgniteEx g0 = startGrid(0); IgniteEx g1 = startGrid(1)) {
            long[] bounds = new long[] {50, 100};

            assertNotEquals(bounds.length, g0.context().metric().registry(TX_METRICS)
                .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds().length);

            assertNotEquals(bounds.length, g1.context().metric().registry(TX_METRICS)
                .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds().length);

            ((IgniteMXBean)g0).configureHistogramMetric(metricName(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM), bounds);

            assertArrayEquals(bounds, g0.context().metric().registry(TX_METRICS)
                .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds());

            assertArrayEquals(bounds, g1.context().metric().registry(TX_METRICS)
                .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds());
        }
    }

    /** Tests metric configuration applied on all nodes. */
    @Test
    public void testNodeRestart() throws Exception {
        IgniteEx g0 = startGrid("persistent-0");
        IgniteEx g1 = startGrid("persistent-1");

        g0.cluster().active(true);

        long[] bounds = new long[] {50, 100};

        assertNotEquals(bounds.length, g0.context().metric().registry(TX_METRICS)
            .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds().length);

        assertNotEquals(bounds.length, g1.context().metric().registry(TX_METRICS)
            .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds().length);

        ((IgniteMXBean)g0).configureHistogramMetric(metricName(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM), bounds);

        assertArrayEquals(bounds, g0.context().metric().registry(TX_METRICS)
            .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds());

        assertArrayEquals(bounds, g1.context().metric().registry(TX_METRICS)
            .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds());

        g0.close();
        g1.close();

        g0 = startGrid("persistent-0");
        g1 = startGrid("persistent-1");

        g0.cluster().active(true);

        assertArrayEquals(bounds, g0.context().metric().registry(TX_METRICS)
            .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds());

        assertArrayEquals(bounds, g1.context().metric().registry(TX_METRICS)
            .<HistogramMetric>findMetric(METRIC_SYSTEM_TIME_HISTOGRAM).bounds());
    }

}
