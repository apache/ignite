/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
