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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.log.LogExporterSpi;
import org.junit.jupiter.api.Test;

public class ConfiguringMetrics {

    @Test
    void cacheMetrics() {
        // tag::cache-metrics[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        CacheConfiguration cacheCfg = new CacheConfiguration("test-cache");

        // Enable statistics for the cache.
        cacheCfg.setStatisticsEnabled(true);

        cfg.setCacheConfiguration(cacheCfg);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        // end::cache-metrics[]

        ignite.close();
    }

    @Test
    void dataStorageMetrics() {

        // tag::data-storage-metrics[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.setMetricsEnabled(true);

        // Apply the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        // end::data-storage-metrics[]
        ignite.close();
    }

    @Test
    void dataRegionMetrics() {

        // tag::data-region-metrics[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
        defaultRegion.setMetricsEnabled(true);

        storageCfg.setDefaultDataRegionConfiguration(defaultRegion);

        // Create a new data region.
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("myDataRegion");

        // Enable metrics for this region.
        regionCfg.setMetricsEnabled(true);

        // Set the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);

        // Other properties

        // Apply the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        // end::data-region-metrics[]
        ignite.close();
    }

    @Test
    void newMetrics() {

        //tag::new-metric-framework[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Change metric exporter. The default is JmxMetricExporterSpi.
        cfg.setMetricExporterSpi(new LogExporterSpi());

        Ignite ignite = Ignition.start(cfg);
        //end::new-metric-framework[]

        ignite.close();
    }
    
    @Test
    void jmxExporter() {

        //tag::metrics-filter[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Create configured JMX metrics exporter.
        JmxMetricExporterSpi jmxExporter = new JmxMetricExporterSpi();

        //export cache metrics only
        jmxExporter.setExportFilter(mreg -> mreg.name().startsWith("cache."));

        cfg.setMetricExporterSpi(jmxExporter);
        //end::metrics-filter[]

        Ignition.start(cfg).close();
    }

    @Test
    void logExporter() {

        //tag::log-exporter[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        LogExporterSpi logExporter = new LogExporterSpi();
        logExporter.setPeriod(600_000);

        //export cache metrics only
        logExporter.setExportFilter(mreg -> mreg.name().startsWith("cache."));

        cfg.setMetricExporterSpi(logExporter);

        Ignite ignite = Ignition.start(cfg);
        //end::log-exporter[]
        ignite.close();
    }
}
