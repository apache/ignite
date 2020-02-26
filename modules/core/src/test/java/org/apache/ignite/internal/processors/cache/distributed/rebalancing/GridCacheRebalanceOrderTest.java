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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;

/**
 * Tests that caches with rebalance mode equals to SYNC are rebalanced in the first place.
 */
public class GridCacheRebalanceOrderTest extends GridCommonAbstractTest {
    /** Rebalance timeout. */
    private static final long REBALANCE_TIMEOUT = 5_000;

    /** Flag indicates that local listener should be used to track rebalance events. */
    private boolean trackRebalanceEvts;

    /** Caches rebalance events. */
    private final List<CacheRebalancingEvent> evtsList = Collections.synchronizedList(new ArrayList<>());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (trackRebalanceEvts) {
            Map<IgnitePredicate<? extends Event>, int[]> listeners = new HashMap<>();

            listeners.put(new IgnitePredicate<CacheRebalancingEvent>() {
                @Override public boolean apply(CacheRebalancingEvent evt) {
                    evtsList.add(evt);

                    return true;
                }
            }, new int[] {EVT_CACHE_REBALANCE_STOPPED});

            cfg.setLocalEventListeners(listeners);

            cfg.setIncludeEventTypes(EventType.EVTS_ALL);
        }

        return cfg;
    }

    /**
     * Stops all nodes after test.
     */
    @After
    public void testCleanup() {
        stopAllGrids();
    }

    /**
     * Tests that caches with rebalance mode equals to SYNC are rebalanced in the first place.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceOrderBasedOnCacheRebalanceMode() throws Exception {
        Ignite g = startGrid(0);

        // Fix the expected order of rebalance.
        List<IgniteBiTuple<Integer, CacheRebalanceMode>> order = new ArrayList<>();
        order.add(new T2<>(0, SYNC));
        order.add(new T2<>(0, SYNC));
        order.add(new T2<>(0, ASYNC));
        order.add(new T2<>(0, ASYNC));
        order.add(new T2<>(1, SYNC));
        order.add(new T2<>(1, SYNC));
        order.add(new T2<>(1, ASYNC));
        order.add(new T2<>(1, ASYNC));

        // Prepare caches with different rebalance mode and order.
        List<IgniteCache<Integer, Integer>> caches = new ArrayList<>();
        for (int i = order.size() - 1; i >= 0; i--) {
            int rebalanceOrder = order.get(i).get1();

            CacheRebalanceMode rebalanceMode = order.get(i).get2();

            caches.add(g.getOrCreateCache(getCacheConfiguration(
                "cache-" + i + "-order-" + rebalanceOrder + "-mode-" + rebalanceMode,
                rebalanceOrder,
                rebalanceMode)));
        }

        // Fill values.
        for (IgniteCache<Integer, Integer> c : caches)
            c.put(12, 21);

        trackRebalanceEvts = true;

        Ignite g1 = startGrid(1);

        // Wait for all rebalance futures.
        for (IgniteCache<Integer, Integer> c : caches)
            grid(1).context().cache().internalCache(c.getName()).preloader().syncFuture().get(REBALANCE_TIMEOUT);

        // Check that all events were fired.
        assertEquals("Expected rebalance events were not triggered.", order.size(), evtsList.size());

        // Check rebelance order.
        for (int i = 0; i < order.size(); ++i) {
            int expOrder = order.get(i).get1();

            CacheRebalanceMode expMode = order.get(i).get2();

            CacheRebalancingEvent actualEvt = evtsList.get(i);

            CacheConfiguration<Integer, Integer> actualCfg = g1.cache(actualEvt.cacheName())
                .getConfiguration(CacheConfiguration.class);

            assertEquals(
                "Unexpected rebalance order [cacheName=" + actualEvt.cacheName() + ']',
                expOrder,
                actualCfg.getRebalanceOrder());

            assertEquals(
                "Unexpected cache rebalance mode [cacheName=" + actualEvt.cacheName() + ']',
                expMode,
                actualCfg.getRebalanceMode());
        }
    }

    /**
     * Creates a new cache configuration with the given parameters.
     *
     * @param cacheName Cache name.
     * @param rebalanceOrder Rebalance order.
     * @param rebalanceMode Rebalance mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> getCacheConfiguration(
        String cacheName,
        int rebalanceOrder,
        CacheRebalanceMode rebalanceMode
    ) {
        return new CacheConfiguration<Integer, Integer>(cacheName)
            .setRebalanceOrder(rebalanceOrder)
            .setRebalanceMode(rebalanceMode)
            .setBackups(1);
    }
}
