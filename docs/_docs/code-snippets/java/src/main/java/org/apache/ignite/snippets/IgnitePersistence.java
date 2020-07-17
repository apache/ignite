package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgnitePersistence {

    public static void disablingWal() {

        //tag::wal[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(storageCfg);

        Ignite ignite = Ignition.start(cfg);
        
        ignite.cluster().active(true);
        
        String cacheName = "myCache";
        
        ignite.getOrCreateCache(cacheName);

        ignite.cluster().disableWal(cacheName);

        //load data
        ignite.cluster().enableWal(cacheName);

        //end::wal[]
        ignite.close();
    }
    
    public static void changeWalSegmentSize()  {
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
    
    public static void main(String[] args) {
        IgnitePersistence ip = new IgnitePersistence();
        
        ip.disablingWal();
        ip.changeWalSegmentSize();
        ip.cfgExample();
    }

}
