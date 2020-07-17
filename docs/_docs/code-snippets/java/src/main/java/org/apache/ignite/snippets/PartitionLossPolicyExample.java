package org.apache.ignite.snippets;

import java.util.Arrays;

import javax.cache.CacheException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.jupiter.api.Test;

public class PartitionLossPolicyExample {
    @Test
    void configure() {

        //tag::cfg[]
        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");

        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        //end::cfg[]
    }

    @Test
    void events() {
        //tag::events[]
        Ignite ignite = Ignition.start();

        IgnitePredicate<Event> locLsnr = evt -> {
            CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent) evt;

            int lostPart = cacheEvt.partition();

            ClusterNode node = cacheEvt.discoveryNode();

            System.out.println(lostPart);

            return true; // Continue listening.
        };

        ignite.events().localListen(locLsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        //end::events[]

        ignite.close();

    }

    @Test
    void reset() {
        Ignite ignite = Ignition.start();
        //tag::reset[]
        ignite.resetLostPartitions(Arrays.asList("myCache"));
        //end::reset[]
        ignite.close();
    }

    void getLostPartitions(Ignite ignite) {
        //tag::lost-partitions[]
        IgniteCache<Integer, String> cache = ignite.cache("myCache");

        cache.lostPartitions();

        //end::lost-partitions[]
    }

    @Test
    void exception() {

        try (Ignite ignite = Ignition.start()) {
            ignite.getOrCreateCache("myCache");

            //tag::exception[]
            IgniteCache<Integer, Integer> cache = ignite.cache("myCache");

            try {
                Integer value = cache.get(3);
                System.out.println(value);
            } catch (CacheException e) {
                if (e.getCause() instanceof CacheInvalidStateException) {
                    System.out.println(e.getCause().getMessage());
                } else {
                    e.printStackTrace();
                }
            }
            //end::exception[]
        }
    }
}
