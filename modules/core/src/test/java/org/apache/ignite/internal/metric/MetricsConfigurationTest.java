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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/** */
public class MetricsConfigurationTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testHitRateMetric() throws Exception {
        HitRateMetric hitRateMetric = new HitRateMetric("test", null, 1000, 3, false);

        assertThrowsWithCause(() -> hitRateMetric.configure(null), IgniteException.class);

        assertThrowsWithCause(() -> hitRateMetric.configure(""), IgniteException.class);

        assertThrowsWithCause(() -> hitRateMetric.configure("1000"), IgniteException.class);

        assertThrowsWithCause(() -> hitRateMetric.configure("1000, 1"), IgniteException.class);

        assertThrowsWithCause(() -> hitRateMetric.configure("-1000, 1"), IgniteException.class);

        assertThrowsWithCause(() -> hitRateMetric.configure("-1000, 1xxx"), IgniteException.class);

        assertThrowsWithCause(() -> hitRateMetric.configure("xxx"), IgniteException.class);

        hitRateMetric.configure("5000, 42");

        checkHitRate(hitRateMetric, 5000, 42);
    }

    /** */
    private void checkHitRate(HitRateMetric hitRateMetric, long expRateTimeInterval, int expSize) {
        Object hitRateMetricImpl = getFieldValue(hitRateMetric, "cntr");

        long rateTimeInterval = getFieldValue(hitRateMetricImpl, "rateTimeInterval");

        int size = getFieldValue(hitRateMetricImpl, "size");

        assertEquals(expRateTimeInterval, rateTimeInterval);
        assertEquals(expSize, size);
    }

    /** */
    @Test
    public void testHistogramConfiguration() throws Exception {
        HistogramMetric histogramMetric = new HistogramMetric("test", null, new long[] {0, 50, 100, 250}, false);

        assertThrowsWithCause(() -> histogramMetric.configure(null), IgniteException.class);

        assertThrowsWithCause(() -> histogramMetric.configure(""), IgniteException.class);

        assertThrowsWithCause(() -> histogramMetric.configure("44,43"), IgniteException.class);

        assertThrowsWithCause(() -> histogramMetric.configure("-1000"), IgniteException.class);

        assertThrowsWithCause(() -> histogramMetric.configure("1,2,xx"), IgniteException.class);

        assertThrowsWithCause(() -> histogramMetric.configure("xx"), IgniteException.class);

        histogramMetric.configure("42,43 ,44, 45");

        Object histogramHolder = getFieldValue(histogramMetric, "holder");

        long[] bounds = getFieldValue(histogramHolder, "bounds");

        assertTrue(Arrays.equals(new long[] {42, 43, 44, 45}, bounds));
    }

    /** */
    @Test
    public void testJMXConfiguration() throws Exception {
        Ignite g = startGrid();

        IgniteMXBean bean = (IgniteMXBean)g;

        //Expected exception on LongMetricImpl configuration
        assertThrowsWithCause(
            () -> bean.configureMetric(metricName("io.dataregion.default"), "TotalAllocatedPages", "222"),
            IgniteException.class);

        assertThrowsWithCause(() -> bean.configureMetric("unknownreg", "Puts", "xxx"), IgniteException.class);

        assertThrowsWithCause(() -> bean.configureMetric(metricName("io.dataregion.default"), "UnknonwnMetric", "xxx"),
            IgniteException.class);

        assertThrowsWithCause(() -> bean.configureMetric(metricName("io.dataregion.default"), "AllocationRate", "1000"),
            IgniteException.class);

        assertThrowsWithCause(() -> bean.configureMetric(metricName("io.dataregion.default"), "AllocationRate", "xxx"),
            IgniteException.class);

        bean.configureMetric(metricName("io.dataregion.default"), "AllocationRate", "5000,42");

        Metric allocationRate = ((IgniteEx)g).context().metric().registry(metricName("io.dataregion.default"))
            .findMetric("AllocationRate");

        checkHitRate((HitRateMetric)allocationRate, 5000, 42);
    }
}
