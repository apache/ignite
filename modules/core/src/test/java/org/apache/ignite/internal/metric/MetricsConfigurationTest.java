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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_SYSTEM_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_TOTAL_USER_TIME;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests metrics configuration. */
public class MetricsConfigurationTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx g;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        g = startGrid(0);
    }

    /** Tests configuration of {@link HitRateMetric}. */
    @Test
    public void testHitRateConfiguration() throws Exception {
        IgniteMXBean bean = (IgniteMXBean)g;

        //Unknown registry.
        assertThrowsWithCause(
            () -> bean.configureHitRateMetric("unknownreg", "Puts", 1),
            IgniteException.class);

        //Uknown metric.
        assertThrowsWithCause(
            () -> bean.configureHitRateMetric(metricName("io.dataregion.default"), "UnknonwnMetric", 1),
            IgniteException.class);

        //Wrong metric type.
        assertThrowsWithCause(
            () -> bean.configureHitRateMetric(metricName("io.dataregion.default"), "TotalAllocatedPages", 222),
            IgniteException.class);

        //Wrong rateTimeInterval value.
        assertThrowsWithCause(
            () -> bean.configureHitRateMetric(metricName("io.dataregion.default"), "AllocationRate", 0),
            IllegalArgumentException.class);

        //Wrong rateTimeInterval value.
        assertThrowsWithCause(
            () -> bean.configureHitRateMetric(metricName("io.dataregion.default"), "AllocationRate", -1),
            IllegalArgumentException.class);

        bean.configureHitRateMetric(metricName("io.dataregion.default"), "AllocationRate", 5000);

        HitRateMetric allocationRate = ((IgniteEx)g).context().metric().registry(metricName("io.dataregion.default"))
            .findMetric("AllocationRate");

        assertEquals(5000, allocationRate.rateTimeInterval());
    }

    /** Tests configuration of {@link HistogramMetric}. */
    @Test
    public void testHistogramConfiguration() throws Exception {
        IgniteMXBean bean = (IgniteMXBean)g;

        long[] bounds = new long[] {50, 100};

        //Unknown registry.
        assertThrowsWithCause(
            () -> bean.configureHistogramMetric("unknownreg", "Puts", bounds),
            IgniteException.class);

        //Unknown metric.
        assertThrowsWithCause(
            () -> bean.configureHistogramMetric(TX_METRICS, "UnknonwnMetric", bounds),
            IgniteException.class);

        //Wrong metric type.
        assertThrowsWithCause(
            () -> bean.configureHistogramMetric(TX_METRICS, METRIC_TOTAL_USER_TIME, bounds),
            IgniteException.class);

        //Wrong bounds value.
        assertThrowsWithCause(
            () -> bean.configureHistogramMetric(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM, null),
            NullPointerException.class);

        assertThrowsWithCause(
            () -> bean.configureHistogramMetric(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM, new long[0]),
            IllegalArgumentException.class);

        bean.configureHistogramMetric(TX_METRICS, METRIC_SYSTEM_TIME_HISTOGRAM, bounds);

        HistogramMetric systemTime = ((IgniteEx)g).context().metric().registry(TX_METRICS)
            .findMetric(METRIC_SYSTEM_TIME_HISTOGRAM);

        assertEquals(bounds, systemTime.bounds());
    }
}
