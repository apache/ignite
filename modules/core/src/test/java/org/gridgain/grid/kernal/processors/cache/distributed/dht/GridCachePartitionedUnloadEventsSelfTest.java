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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

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
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 10));
        cacheCfg.setBackups(0);
        return cacheCfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testUnloadEvents() throws Exception {
        final Ignite g1 = startGrid("g1");

        Collection<Integer> allKeys = new ArrayList<>(100);

        GridCache<Integer, String> cache = g1.cache(null);

        for (int i = 0; i < 100; i++) {
            cache.put(i, "val");
            allKeys.add(i);
        }

        Ignite g2 = startGrid("g2");

        Map<ClusterNode, Collection<Object>> keysMap = g1.cache(null).affinity().mapKeysToNodes(allKeys);
        Collection<Object> g2Keys = keysMap.get(g2.cluster().localNode());

        assertNotNull(g2Keys);
        assertFalse("There are no keys assigned to g2", g2Keys.isEmpty());

        Thread.sleep(5000);

        Collection<IgniteEvent> objEvts =
            g1.events().localQuery(F.<IgniteEvent>alwaysTrue(), EVT_CACHE_PRELOAD_OBJECT_UNLOADED);

        checkObjectUnloadEvents(objEvts, g1, g2Keys);

        Collection <IgniteEvent> partEvts =
            g1.events().localQuery(F.<IgniteEvent>alwaysTrue(), EVT_CACHE_PRELOAD_PART_UNLOADED);

        checkPartitionUnloadEvents(partEvts, g1, dht(g2.cache(null)).topology().localPartitions());
    }

    /**
     * @param evts Events.
     * @param g Grid.
     * @param keys Keys.
     */
    private void checkObjectUnloadEvents(Collection<IgniteEvent> evts, Ignite g, Collection<?> keys) {
        assertEquals(keys.size(), evts.size());

        for (IgniteEvent evt : evts) {
            IgniteCacheEvent cacheEvt = ((IgniteCacheEvent)evt);

            assertEquals(EVT_CACHE_PRELOAD_OBJECT_UNLOADED, cacheEvt.type());
            assertEquals(g.cache(null).name(), cacheEvt.cacheName());
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
    private void checkPartitionUnloadEvents(Collection<IgniteEvent> evts, Ignite g,
        Collection<GridDhtLocalPartition<Object, Object>> parts) {
        assertEquals(parts.size(), evts.size());

        for (IgniteEvent evt : evts) {
            IgniteCachePreloadingEvent unloadEvt = (IgniteCachePreloadingEvent)evt;

            final int part = unloadEvt.partition();

            assertNotNull("Unexpected partition: " + part, F.find(parts, null,
                new IgnitePredicate<GridDhtLocalPartition<Object, Object>>() {
                    @Override
                    public boolean apply(GridDhtLocalPartition<Object, Object> e) {
                        return e.id() == part;
                    }
                }));

            assertEquals(g.cache(null).name(), unloadEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), unloadEvt.node().id());
        }
    }
}
