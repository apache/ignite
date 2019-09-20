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

import java.util.Optional;
import java.util.Set;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.spi.metric.jmx.MetricRegistryMBean.searchHistogram;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class JmxMetricExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        JmxMetricExporterSpi jmxSpi = new JmxMetricExporterSpi();

        jmxSpi.setExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(jmxSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testSysJmxMetrics() throws Exception {
        DynamicMBean sysMBean = metricRegistry(ignite.name(), null, SYS_METRICS);

        Set<String> res = stream(sysMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.contains(CPU_LOAD));
        assertTrue(res.contains(GC_CPU_LOAD));
        assertTrue(res.contains(metricName("memory", "heap", "init")));
        assertTrue(res.contains(metricName("memory", "heap", "used")));
        assertTrue(res.contains(metricName("memory", "nonheap", "committed")));
        assertTrue(res.contains(metricName("memory", "nonheap", "max")));

        Optional<MBeanAttributeInfo> cpuLoad = stream(sysMBean.getMBeanInfo().getAttributes())
            .filter(a -> a.getName().equals(CPU_LOAD))
            .findFirst();

        assertTrue(cpuLoad.isPresent());
        assertEquals(CPU_LOAD_DESCRIPTION, cpuLoad.get().getDescription());

        Optional<MBeanAttributeInfo> gcCpuLoad = stream(sysMBean.getMBeanInfo().getAttributes())
            .filter(a -> a.getName().equals(GC_CPU_LOAD))
            .findFirst();

        assertTrue(gcCpuLoad.isPresent());
        assertEquals(GC_CPU_LOAD_DESCRIPTION, gcCpuLoad.get().getDescription());
    }

    /** */
    @Test
    public void testDataRegionJmxMetrics() throws Exception {
        DynamicMBean dataRegionMBean = metricRegistry(ignite.name(), "io", "dataregion.default");

        Set<String> res = stream(dataRegionMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.containsAll(EXPECTED_ATTRIBUTES));

        for (String metricName : res)
            assertNotNull(metricName, dataRegionMBean.getAttribute(metricName));
    }

    /** */
    @Test
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite);

        assertThrowsWithCause(new RunnableX() {
            @Override public void runx() throws Exception {
                metricRegistry(ignite.name(), "filtered", "metric");
            }
        }, IgniteException.class);

        DynamicMBean bean1 = metricRegistry(ignite.name(), "other", "prefix");

        assertEquals(42L, bean1.getAttribute("test"));
        assertEquals(43L, bean1.getAttribute("test2"));

        DynamicMBean bean2 = metricRegistry(ignite.name(), "other", "prefix2");

        assertEquals(44L, bean2.getAttribute("test3"));
    }

    /** */
    @Test
    public void testHistogramSearchByName() throws Exception {
        MetricRegistry mreg = new MetricRegistry("test", null);

        createTestHistogram(mreg);

        assertEquals(Long.valueOf(1), searchHistogram("histogram_0_50", mreg));
        assertEquals(Long.valueOf(2), searchHistogram("histogram_50_500", mreg));
        assertEquals(Long.valueOf(3), searchHistogram("histogram_500_inf", mreg));

        assertNull(searchHistogram("unknown", mreg));
        assertNull(searchHistogram("unknown_0", mreg));
        assertNull(searchHistogram("unknown_0_50", mreg));
        assertNull(searchHistogram("unknown_test", mreg));
        assertNull(searchHistogram("unknown_test_test", mreg));
        assertNull(searchHistogram("unknown_0_inf", mreg));

        assertNull(searchHistogram("histogram", mreg));
        assertNull(searchHistogram("histogram_0", mreg));
        assertNull(searchHistogram("histogram_0_100", mreg));
        assertNull(searchHistogram("histogram_0_inf", mreg));
        assertNull(searchHistogram("histogram_0_500", mreg));
    }

    /** */
    @Test
    public void testHistogramExport() throws Exception {
        MetricRegistry mreg = ignite.context().metric().registry("histogramTest");

        createTestHistogram(mreg);

        DynamicMBean bean = metricRegistry(ignite.name(), null, "histogramTest");

        MBeanAttributeInfo[] attrs = bean.getMBeanInfo().getAttributes();

        assertEquals(3, attrs.length);

        assertEquals(1L, bean.getAttribute("histogram_0_50"));
        assertEquals(2L, bean.getAttribute("histogram_50_500"));
        assertEquals(3L, bean.getAttribute("histogram_500_inf"));
    }

    /** */
    private void createTestHistogram(MetricRegistry mreg) {
        long[] bounds = new long[] {50, 500};

        HistogramMetric histogram = mreg.histogram("histogram", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);
    }
}
