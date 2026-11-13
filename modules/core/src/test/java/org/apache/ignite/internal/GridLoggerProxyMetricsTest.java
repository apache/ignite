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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.spi.metric.LongMetric;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests {@link GridLoggerProxy} metrics. */
public class GridLoggerProxyMetricsTest {
    /** */
    @Test
    public void testLogMessagesMetrics() {
        GridLoggerProxy log = logger(true);

        log.trace("trace");
        log.trace("marker", "trace");
        log.debug("debug");
        log.debug("marker", "debug");
        log.info("info");
        log.info("marker", "info");
        log.getLogger(getClass()).info("info from category logger");
        log.warning("warning");
        log.warning("warning", new Exception());
        log.warning("marker", "warning", null);
        log.error("error");
        log.error("error", new Exception());
        log.error("marker", "error", null);

        MetricRegistryImpl mreg = new MetricRegistryImpl(GridLoggerProxy.LOG_COUNT_METRICS, null, null, null, null);

        log.registerMetrics(metricManager(mreg));

        assertEquals(2, metricValue(mreg, "trace"));
        assertEquals(2, metricValue(mreg, "debug"));
        assertEquals(3, metricValue(mreg, "info"));
        assertEquals(3, metricValue(mreg, "warning"));
        assertEquals(3, metricValue(mreg, "error"));
    }

    /** */
    @Test
    public void testDisabledLevelsAreNotCounted() {
        GridLoggerProxy log = logger(false);

        log.trace("trace");
        log.trace("marker", "trace");
        log.debug("debug");
        log.debug("marker", "debug");
        log.info("info");
        log.info("marker", "info");
        log.warning("warning");
        log.error("error");

        MetricRegistryImpl mreg = new MetricRegistryImpl(GridLoggerProxy.LOG_COUNT_METRICS, null, null, null, null);

        log.registerMetrics(metricManager(mreg));

        assertEquals(0, metricValue(mreg, "trace"));
        assertEquals(0, metricValue(mreg, "debug"));
        assertEquals(0, metricValue(mreg, "info"));
        assertEquals(1, metricValue(mreg, "warning"));
        assertEquals(1, metricValue(mreg, "error"));
    }

    /** Messages counted before registration remain visible and updates continue after registration. */
    @Test
    public void testUpdatesAfterRegistration() {
        GridLoggerProxy log = logger(true);

        log.info("before registration");

        MetricRegistryImpl mreg = new MetricRegistryImpl(GridLoggerProxy.LOG_COUNT_METRICS, null, null, null, null);

        log.registerMetrics(metricManager(mreg));

        LongMetric info = mreg.findMetric("info");

        assertEquals(1, info.value());

        log.info("after registration");

        assertEquals(2, info.value());
    }

    /** Categories share their root's counters, while independent roots have separate counters. */
    @Test
    public void testSharedCategoriesAndIndependentRoots() {
        GridLoggerProxy first = logger(true);
        GridLoggerProxy second = logger(true);

        IgniteLogger category = first.getLogger("first category");
        IgniteLogger nestedCategory = category.getLogger("nested category");
        IgniteLogger anotherCategory = first.getLogger("another category");

        MetricRegistryImpl firstReg = new MetricRegistryImpl(GridLoggerProxy.LOG_COUNT_METRICS, null, null, null, null);
        MetricRegistryImpl secondReg = new MetricRegistryImpl(GridLoggerProxy.LOG_COUNT_METRICS, null, null, null, null);

        ((GridLoggerProxy)nestedCategory).registerMetrics(metricManager(firstReg));
        second.registerMetrics(metricManager(secondReg));

        first.info("root");
        category.info("category");
        nestedCategory.info("nested category");
        anotherCategory.info("another category");

        assertEquals(4, metricValue(firstReg, "info"));
        assertEquals(0, metricValue(secondReg, "info"));

        second.info("independent root");

        assertEquals(4, metricValue(firstReg, "info"));
        assertEquals(1, metricValue(secondReg, "info"));
    }

    /**
     * @param enabled Whether TRACE, DEBUG and INFO are enabled.
     * @return Logger with a backend whose levels are controlled by the test.
     */
    private GridLoggerProxy logger(boolean enabled) {
        IgniteLogger impl = mock(IgniteLogger.class);

        when(impl.isTraceEnabled()).thenReturn(enabled);
        when(impl.isDebugEnabled()).thenReturn(enabled);
        when(impl.isInfoEnabled()).thenReturn(enabled);
        when(impl.getLogger(any())).thenReturn(impl);

        return new GridLoggerProxy(impl, null, null, "node-id");
    }

    /**
     * @param mreg Metrics registry.
     * @return Metric manager returning the registry for log counters.
     */
    private GridMetricManager metricManager(MetricRegistryImpl mreg) {
        GridMetricManager metricMgr = mock(GridMetricManager.class);

        when(metricMgr.registry(GridLoggerProxy.LOG_COUNT_METRICS)).thenReturn(mreg);

        return metricMgr;
    }

    /**
     * @param mreg Metrics registry.
     * @param name Metric name.
     * @return Metric value.
     */
    private long metricValue(MetricRegistryImpl mreg, String name) {
        return mreg.<LongMetric>findMetric(name).value();
    }
}
