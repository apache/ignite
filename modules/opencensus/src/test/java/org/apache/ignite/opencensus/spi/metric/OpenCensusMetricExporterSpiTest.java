/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.opencensus.spi.metric;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.AbstractExporterSpiTest;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.junit.Test;

import static org.apache.ignite.opencensus.spi.metric.OpenCensusMetricExporterSpi.CONSISTENT_ID_TAG;
import static org.apache.ignite.opencensus.spi.metric.OpenCensusMetricExporterSpi.INSTANCE_NAME_TAG;
import static org.apache.ignite.opencensus.spi.metric.OpenCensusMetricExporterSpi.NODE_ID_TAG;
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

        HTTPServer server = new HTTPServer(HOST, PORT, true);

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
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite);

        Set<String> expectedMetrics = new GridConcurrentHashSet<>(Arrays.asList(
            "other_prefix_test\\{.*\\} 42",
            "other_prefix_test2\\{.*\\} 43",
            "other_prefix2_test3\\{.*\\} 44"
        ));

        boolean res = waitForCondition(() -> {
            try {
                String httpMetrics = metricsFromHttp();

                assertFalse("Filtered prefix shouldn't export.",
                    httpMetrics.contains(FILTERED_PREFIX.replaceAll("\\.", "_")));

                for (String expMetric : expectedMetrics) {
                    if (!Pattern.compile(expMetric).matcher(httpMetrics).find())
                        return false;
                }

                return true;
            }
            catch (IOException e) {
                return false;
            }
        }, EXPORT_TIMEOUT * 10);

        assertTrue("Additional metrics should be exported via http", res);
    }

    /** */
    public String metricsFromHttp() throws IOException {
        URL url = new URL("http://" + HOST + ':' + PORT);

        URLConnection con = url.openConnection();

        try (InputStream in = con.getInputStream()) {
            return IOUtils.toString(in, con.getContentEncoding());
        }
    }
}
