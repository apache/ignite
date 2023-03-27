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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 */
public class GridCachePartitionedSupplyEventsSelfTest extends GridCommonAbstractTest {
    /** Default cache name with cache events disabled. */
    private static final String DEFAULT_CACHE_NAME_EVTS_DISABLED = DEFAULT_CACHE_NAME + "EvtsDisabled";

    /** */
    public static final int NODES = 7;

    /** */
    public static final int PARTS = 8192;

    /** */
    private final ConcurrentHashMap<UUID, Integer> nodesToPartsSupplied = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = cacheConfiguration();

        CacheConfiguration<?, ?> ccfgEvtsDisabled = new CacheConfiguration<>(ccfg);

        ccfgEvtsDisabled.setName(DEFAULT_CACHE_NAME_EVTS_DISABLED);
        ccfgEvtsDisabled.setEventsDisabled(true);

        cfg.setCacheConfiguration(ccfg, ccfgEvtsDisabled);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_PART_SUPPLIED, EventType.EVT_CACHE_REBALANCE_PART_MISSED);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap();

        lsnrs.put(new IgnitePredicate<CacheRebalancingEvent>() {
                @Override public boolean apply(CacheRebalancingEvent evt) {
                    nodesToPartsSupplied.compute(evt.node().id(), (k, v) -> (v == null) ? 1 : (v + 1));

                    assertEquals(DEFAULT_CACHE_NAME, evt.cacheName());

                    return true;
                }
            }, new int[]{EventType.EVT_CACHE_REBALANCE_PART_SUPPLIED});

        lsnrs.put(new IgnitePredicate<CacheRebalancingEvent>() {
                @Override public boolean apply(CacheRebalancingEvent evt) {
                    fail("Should not miss any partitions!");

                    assertEquals(DEFAULT_CACHE_NAME, evt.cacheName());

                    return true;
                }
            }, new int[]{EventType.EVT_CACHE_REBALANCE_PART_MISSED});

        cfg.setLocalEventListeners(lsnrs);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
        cacheCfg.setBackups(3);
        return cacheCfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSupplyEvents() throws Exception {
        Ignite g = startGrid("g0");

        IgniteCache c1 = g.cache(DEFAULT_CACHE_NAME);
        IgniteCache c0 = g.cache(DEFAULT_CACHE_NAME_EVTS_DISABLED);

        for (int k = 0; k < PARTS * 2; k++) {
            c1.put(k, k);
            c0.put(k, k);
        }

        for (int n = 1; n <= NODES; n++) {
            assertTrue(nodesToPartsSupplied.isEmpty());

            startGrid("g" + n);

            awaitPartitionMapExchange();

            int max = 0;
            int min = PARTS;
            int total = 0;

            for (int supplied : nodesToPartsSupplied.values()) {
                assertTrue(supplied > 0);

                max = Math.max(max, supplied);
                min = Math.min(min, supplied);

                total += supplied;
            }

            log.info("After start-up of node " + n + " each node supplied " + min + "-" + max + " partitions, total of " + total);

            assertEquals(n, nodesToPartsSupplied.size());

            assertTrue(max > 0);
            assertTrue(total > 0);

            // Until we have node 5, data is replicated
            assertTrue(total == PARTS != n >= 4);

            // Quality of distribution of supplied partitions
            assertTrue(max - min < PARTS >> 4);

            nodesToPartsSupplied.clear();
        }
    }
}
