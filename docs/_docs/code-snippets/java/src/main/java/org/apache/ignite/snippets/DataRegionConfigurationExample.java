package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class DataRegionConfigurationExample {

    public static void main(String[] args) {

        //tag::ignite-config[]
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        //tag::default[]

        DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
        defaultRegion.setName("Default_Region");
        defaultRegion.setInitialSize(100 * 1024 * 1024);

        storageCfg.setDefaultDataRegionConfiguration(defaultRegion);
        //end::default[]
        //tag::data-regions[]
        // 40MB memory region with eviction enabled.
        DataRegionConfiguration regionWithEviction = new DataRegionConfiguration();
        regionWithEviction.setName("40MB_Region_Eviction");
        regionWithEviction.setInitialSize(20 * 1024 * 1024);
        regionWithEviction.setMaxSize(40 * 1024 * 1024);
        regionWithEviction.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);

        storageCfg.setDataRegionConfigurations(regionWithEviction);
        //end::data-regions[]

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDataStorageConfiguration(storageCfg);
        //tag::caches[]

        CacheConfiguration cache1 = new CacheConfiguration("SampleCache");
        //this cache will be hosted in the "40MB_Region_Eviction" data region
        cache1.setDataRegionName("40MB_Region_Eviction");

        cfg.setCacheConfiguration(cache1);
        //end::caches[]

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::ignite-config[]

        ignite.close();
    }
}
