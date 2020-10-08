package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class PersistenceTuning {

    void pageSize() {

        // tag::page-size[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Durable memory configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        
        // Changing the page size to 8 KB.
        storageCfg.setPageSize(8192);

        cfg.setDataStorageConfiguration(storageCfg);
        // end::page-size[]
    }

    void separateWal() {
        // tag::separate-wal[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Configuring Native Persistence.
        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        // Sets a path to the root directory where data and indexes are to be persisted.
        // It's assumed the directory is on a separated SSD.
        storeCfg.setStoragePath("/ssd/storage");

        // Sets a path to the directory where WAL is stored.
        // It's assumed the directory is on a separated HDD.
        storeCfg.setWalPath("/wal");

        // Sets a path to the directory where WAL archive is stored.
        // The directory is on the same HDD as the WAL.
        storeCfg.setWalArchivePath("/wal/archive");

        cfg.setDataStorageConfiguration(storeCfg);

        // Starting the node.
        Ignite ignite = Ignition.start(cfg);

        // end::separate-wal[]

        ignite.close();
    }

    void writesThrottling() {
        // tag::throttling[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Configuring Native Persistence.
        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        // Enabling the writes throttling.
        storeCfg.setWriteThrottlingEnabled(true);

        cfg.setDataStorageConfiguration(storeCfg);
        // Starting the node.
        Ignite ignite = Ignition.start(cfg);
        // end::throttling[]

        ignite.close();
    }

    void checkpointingBufferSize() {
        // tag::checkpointing-buffer-size[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Configuring Native Persistence.
        DataStorageConfiguration storeCfg = new DataStorageConfiguration();
        
        // Enabling the writes throttling.
        storeCfg.setWriteThrottlingEnabled(true);

        // Increasing the buffer size to 1 GB.
        storeCfg.getDefaultDataRegionConfiguration().setCheckpointPageBufferSize(1024L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storeCfg);

        // Starting the node.
        Ignite ignite = Ignition.start(cfg);
        // end::checkpointing-buffer-size[]
        ignite.close();
    }

}
