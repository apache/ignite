package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class DiskCompression {

    @Test
    void configuration() {
        //tag::configuration[]
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        //set the page size to 2 types of the disk page size
        dsCfg.setPageSize(4096 * 2);

        //enable persistence for the default data region
        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");
        //enable disk page compression for this cache
        cacheCfg.setDiskPageCompression(DiskPageCompression.LZ4);
        //optionally set the compression level
        cacheCfg.setDiskPageCompressionLevel(10);

        cfg.setCacheConfiguration(cacheCfg);

        Ignite ignite = Ignition.start(cfg);
        //end::configuration[]
        
        ignite.close();
    }
}
