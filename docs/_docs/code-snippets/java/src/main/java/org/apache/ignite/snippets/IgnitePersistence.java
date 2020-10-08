package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class IgnitePersistence {

    @Test
    void disablingWal() {

        //tag::wal[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);

        ignite.cluster().state(ClusterState.ACTIVE);

        String cacheName = "myCache";

        ignite.getOrCreateCache(cacheName);

        ignite.cluster().disableWal(cacheName);

        //load data
        ignite.cluster().enableWal(cacheName);

        //end::wal[]
        ignite.close();
    }

    @Test
    public static void changeWalSegmentSize() {
        // tag::segment-size[] 
        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        storageCfg.setWalSegmentSize(128 * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        // end::segment-size[]

        ignite.close();
    }

    @Test
    public static void cfgExample() {
        //tag::cfg[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        //data storage configuration
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        //tag::storage-path[]
        storageCfg.setStoragePath("/opt/storage");
        //end::storage-path[]

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        //end::cfg[]
        ignite.close();
    }

    @Test
    void walRecordsCompression() {
        //tag::wal-records-compression[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        //WAL page compression parameters
        dsCfg.setWalPageCompression(DiskPageCompression.LZ4);
        dsCfg.setWalPageCompressionLevel(8);

        cfg.setDataStorageConfiguration(dsCfg);
        Ignite ignite = Ignition.start(cfg);
        //end::wal-records-compression[]

        ignite.close();
    }

}
