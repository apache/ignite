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

package org.apache.ignite.examples.opencensus;

import java.awt.Desktop;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi;

/**
 * This example demonstrates usage of the `ignite-opencensus` integration module.
 */
public class OpenCensusMetricsExporterExample {
    /** Host name. */
    private static final String HOST = "localhost";

    /** Port. */
    private static final int PORT = 8080;

    /** URL for the metrics. */
    private static final String METRICS_URL = "http://" + HOST + ":" + PORT;

    /** Export period. */
    private static final long PERIOD = 1_000L;

    public static void main(String[] args) throws Exception {
        // Setting up prometheus stats collector.
        PrometheusStatsCollector.createAndRegister();

        // Setting up HTTP server that would serve http://localhost:8080 requests.
        HTTPServer srv = new HTTPServer(HOST, PORT, true);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Setting up OpenCensus exporter.
        OpenCensusMetricExporterSpi openCensusMetricExporterSpi = new OpenCensusMetricExporterSpi();

        // Metrics written to the collector each 1 second.
        openCensusMetricExporterSpi.setPeriod(PERIOD);

        cfg.setMetricExporterSpi(openCensusMetricExporterSpi);

        try (Ignite ignite = Ignition.start(cfg)) {
            // Creating cache.
            IgniteCache<Integer, Integer> cache = ignite.createCache("my-cache");

            // Putting some data to the cache.
            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            // Sleeping for 2 sec to make sure data exported to the prometheus.
            Thread.sleep(2 * PERIOD);

            // If desktop supported opens up page with the metrics.
            if (Desktop.isDesktopSupported()) {
                Desktop desktop = Desktop.getDesktop();

                try {
                    desktop.browse(new URI(METRICS_URL));

                    Thread.sleep(2 * PERIOD);
                }
                catch (IOException | URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                // In case desktop disabled printing URL content.
                URLConnection conn = new URL(METRICS_URL).openConnection();

                try (InputStream in = conn.getInputStream()) {
                    String content = IOUtils.toString(in, conn.getContentEncoding());

                    System.out.println(content);
                }
            }
        }
    }
}
