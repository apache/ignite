package org.apache.ignite.snippets;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class DataPartitioning {

    @Test
     void configurationExample() {
        // tag::cfg[]
        // Defining cluster configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();
        
        // Defining Person cache configuration.
        CacheConfiguration<Integer, Person> personCfg = new CacheConfiguration<Integer, Person>("Person");

        personCfg.setBackups(1);

        // Group the cache belongs to.
        personCfg.setGroupName("group1");

        // Defining Organization cache configuration.
        CacheConfiguration orgCfg = new CacheConfiguration("Organization");

        orgCfg.setBackups(1);

        // Group the cache belongs to.
        orgCfg.setGroupName("group1");

        cfg.setCacheConfiguration(personCfg, orgCfg);

        // Starting the node.
        Ignition.start(cfg);
        // end::cfg[]
        Ignition.ignite().close();
    }

    @Test
    void partitionLossPolicy() {
        //tag::partition-loss-policy[]

        CacheConfiguration<Integer, Person> personCfg = new CacheConfiguration<Integer, Person>("Person");
        
        personCfg.setPartitionLossPolicy(PartitionLossPolicy.IGNORE);

        //end::partition-loss-policy[]
    }
}
