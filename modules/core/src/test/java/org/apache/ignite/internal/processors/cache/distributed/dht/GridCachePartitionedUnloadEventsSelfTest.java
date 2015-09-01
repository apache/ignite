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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED;

/**
 */
public class GridCachePartitionedUnloadEventsSelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        cacheCfg.setBackups(0);
        return cacheCfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testUnloadEvents() throws Exception {
        final Ignite g1 = startGrid("g1");

        Collection<Integer> allKeys = new ArrayList<>(100);

        IgniteCache<Integer, String> cache = g1.cache(null);

        for (int i = 0; i < 100; i++) {
            cache.put(i, "val");
            allKeys.add(i);
        }

        Ignite g2 = startGrid("g2");

        Map<ClusterNode, Collection<Object>> keysMap = g1.affinity(null).mapKeysToNodes(allKeys);
        Collection<Object> g2Keys = keysMap.get(g2.cluster().localNode());

        assertNotNull(g2Keys);
        assertFalse("There are no keys assigned to g2", g2Keys.isEmpty());

        Thread.sleep(5000);

        Collection<Event> objEvts =
            g1.events().localQuery(F.<Event>alwaysTrue(), EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        checkObjectUnloadEvents(objEvts, g1, g2Keys);

        Collection <Event> partEvts =
            g1.events().localQuery(F.<Event>alwaysTrue(), EVT_CACHE_REBALANCE_PART_UNLOADED);

        checkPartitionUnloadEvents(partEvts, g1, dht(g2.cache(null)).topology().localPartitions());
    }

    /**
     * @param evts Events.
     * @param g Grid.
     * @param keys Keys.
     */
    private void checkObjectUnloadEvents(Collection<Event> evts, Ignite g, Collection<?> keys) {
        assertEquals(keys.size(), evts.size());

        for (Event evt : evts) {
            CacheEvent cacheEvt = ((CacheEvent)evt);

            assertEquals(EVT_CACHE_REBALANCE_OBJECT_UNLOADED, cacheEvt.type());
            assertEquals(g.cache(null).getName(), cacheEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), cacheEvt.node().id());
            assertEquals(g.cluster().localNode().id(), cacheEvt.eventNode().id());
            assertTrue("Unexpected key: " + cacheEvt.key(), keys.contains(cacheEvt.key()));
        }
    }

    /**
     * @param evts Events.
     * @param g Grid.
     * @param parts Parts.
     */
    private void checkPartitionUnloadEvents(Collection<Event> evts, Ignite g,
        Collection<GridDhtLocalPartition> parts) {
        assertEquals(parts.size(), evts.size());

        for (Event evt : evts) {
            CacheRebalancingEvent unloadEvt = (CacheRebalancingEvent)evt;

            final int part = unloadEvt.partition();

            assertNotNull("Unexpected partition: " + part, F.find(parts, null,
                new IgnitePredicate<GridDhtLocalPartition>() {
                    @Override
                    public boolean apply(GridDhtLocalPartition e) {
                        return e.id() == part;
                    }
                }));

            assertEquals(g.cache(null).getName(), unloadEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), unloadEvt.node().id());
        }
    }
}