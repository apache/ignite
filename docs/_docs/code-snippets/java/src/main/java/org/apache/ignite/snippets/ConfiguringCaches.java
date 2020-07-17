package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class ConfiguringCaches {

    public static void main(String[] args) {
        configurationExample();
        cacheTemplateExample();
    }

    public static void configurationExample() {
        // tag::cfg[]
        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(2);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);

        // Start a node.
        Ignition.start(cfg);
        // end::cfg[]
        Ignition.ignite().close();
    }

    public static void cacheTemplateExample() {
        // tag::template[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        try (Ignite ignite = Ignition.start(igniteCfg)) {
            CacheConfiguration cacheCfg = new CacheConfiguration("myCacheTemplate");

            cacheCfg.setBackups(2);
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);

            // Register the cache template 
            ignite.addCacheConfiguration(cacheCfg);
        }
        // end::template[]
    }

    static void backupsSync() {
        // tag::synchronization-mode[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        // Start the node.
        Ignition.start(cfg);
        // end::synchronization-mode[]
    }

    static void configuringBackups() {
        // tag::backups[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);

        // end::backups[]
    }

}
