package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class RebalancingConfiguration {

    public static void main(String[] args) {
        RebalancingConfiguration rc = new RebalancingConfiguration();

        rc.configure();
    }

    void configure() {
        //tag::ignite-config[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        //tag::pool-size[]

        cfg.setRebalanceThreadPoolSize(4);
        //end::pool-size[]

        CacheConfiguration cacheCfg = new CacheConfiguration("mycache");
        //tag::mode[]

        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        //end::mode[]
        //tag::throttling[]

        cfg.setRebalanceBatchSize(2 * 1024 * 1024);
        cfg.setRebalanceThrottle(100);

        //end::throttling[]
        cfg.setCacheConfiguration(cacheCfg);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        //end::ignite-config[]

        ignite.close();
    }

}
