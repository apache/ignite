package org.apache.ignite.snippets;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class Swap {

    public void configureSwap() {
        //tag::swap[]
        // Node configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Durable Memory configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Creating a new data region.
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("500MB_Region");

        // Setting initial RAM size.
        regionCfg.setInitialSize(100L * 1024 * 1024);

        // Setting region max size equal to physical RAM size(5 GB)
        regionCfg.setMaxSize(5L * 1024 * 1024 * 1024);
                
        // Enable swap space.
        regionCfg.setSwapPath("/path/to/some/directory");
                
        // Setting the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);
                
        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);
        //end::swap[]
    }
}
