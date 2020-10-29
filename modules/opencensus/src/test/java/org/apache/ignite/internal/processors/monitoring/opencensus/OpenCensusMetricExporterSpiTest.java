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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Pattern;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.AbstractExporterSpiTest;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.sources.AbstractMetricSource;
import org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi;
import org.junit.Test;

import static org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi.CONSISTENT_ID_TAG;
import static org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi.INSTANCE_NAME_TAG;
import static org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi.NODE_ID_TAG;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class OpenCensusMetricExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    public static final String HOST = "localhost";

    /** */
    public static final int PORT = 8888;

    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        OpenCensusMetricExporterSpi ocSpi = new OpenCensusMetricExporterSpi();

        ocSpi.setExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));
        ocSpi.setPeriod(EXPORT_TIMEOUT);
        ocSpi.setSendConsistentId(true);
        ocSpi.setSendInstanceName(true);
        ocSpi.setSendNodeId(true);

        cfg.setMetricExporterSpi(ocSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        PrometheusStatsCollector.createAndRegister();

        HTTPServer srv = new HTTPServer(HOST, PORT, true);

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testDataRegionOcMetrics() throws Exception {
        boolean res = waitForCondition(() -> {
            try {
                String httpMetrics = metricsFromHttp();

                for (String expAttr : EXPECTED_ATTRIBUTES) {
                    if (!httpMetrics.contains(expAttr))
                        return false;
                }

                if (!httpMetrics.contains(INSTANCE_NAME_TAG.getName() + "=\"" + ignite.name() + '\"'))
                    return false;

                String consistentId = CONSISTENT_ID_TAG.getName() + "=\"" + ignite.localNode().consistentId() + '\"';
                if (!httpMetrics.contains(consistentId))
                    return false;

                if (!httpMetrics.contains(NODE_ID_TAG.getName() + "=\"" + ignite.localNode().id() + '\"'))
                    return false;

                return true;
            }
            catch (IOException e) {
                return false;
            }
        }, EXPORT_TIMEOUT * 10);

        assertTrue("Metrics should be exported via http", res);
    }

    /** */
    @Test
    public void testHistogram() throws Exception {
        String registryName = "test_registry";

        String[] expValuesPtrn = new String[] {
            "test_registry_test_histogram_0_10.* 1",
            "test_registry_test_histogram_10_100.* 2",
            "test_registry_test_histogram_100_inf.* 3"};

        long[] testValues = new long[] {5, 50, 50, 500, 500, 500};

        HistMetricSource src = new HistMetricSource(registryName, ignite.context());

        ignite.context().metric().registerSource(src);
        ignite.context().metric().enableMetrics(src);

        for (long value : testValues)
            src.histogramMetric().value(value);

        assertTrue("Histogram metrics should be exported via http", checkHttpMetrics(expValuesPtrn));

        src.bounds = new long[] {50};

        src.histogramMetric().reset(src.bounds);

        for (long value : testValues)
            src.histogramMetric().value(value);

        expValuesPtrn = new String[] {
            "test_registry_test_histogram_0_50.* 3",
            "test_registry_test_histogram_50_inf.* 3"};

        assertTrue("Updated histogram metrics should be exported via http", checkHttpMetrics(expValuesPtrn));
    }

    /**
     * @param patterns Patterns to find.
     * @return {@code True} if given patterns present in metrics from http.
     * @throws Exception If failed.
     */
    private boolean checkHttpMetrics(String[] patterns) throws Exception {
        return waitForCondition(() -> {
            try {
                String httpMetrics = metricsFromHttp();

                assertFalse("Filtered prefix shouldn't export.",
                    httpMetrics.contains(FILTERED_PREFIX.replaceAll("\\.", "_")));

                for (String exp : patterns) {
                    if (!Pattern.compile(exp).matcher(httpMetrics).find())
                        return false;
                }

                return true;
            }
            catch (IOException e) {
                return false;
            }
        }, EXPORT_TIMEOUT * 10);
    }

    /** */
    public String metricsFromHttp() throws IOException {
        URL url = new URL("http://" + HOST + ':' + PORT);

        URLConnection con = url.openConnection();

        try (InputStream in = con.getInputStream()) {
            return IOUtils.toString(in, con.getContentEncoding());
        }
    }

    /** */
    private static class HistMetricSource extends AbstractMetricSource {
        /** Histogram name. */
        String histogramName = "test_histogram";

        /** Histogram description. */
        String histogramDesc = "Test histogram description.";

        /** Bounds. */
        long[] bounds = new long[] {10, 100};

        /** Metric. */
        HistogramMetricImpl metric;

        /**
         * Base constructor for all metric source implemnetations.
         *
         * @param name Metric source name.
         * @param ctx Kernal context.
         */
        protected HistMetricSource(String name, GridKernalContext ctx) {
            super(name, ctx);
        }

        /** */
        public HistogramMetricImpl histogramMetric() {
            return metric;
        }

        /** {@inheritDoc} */
        @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
            metric = bldr.histogram(histogramName, bounds, histogramDesc);
        }

        /** {@inheritDoc} */
        @Override protected Holder createHolder() {
            return null;
        }
    }
}
