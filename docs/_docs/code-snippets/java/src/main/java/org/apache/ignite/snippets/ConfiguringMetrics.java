package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
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
}
