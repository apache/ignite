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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;

/**
 *
 */
public abstract class GridCachePreloadEventsAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        MemoryEventStorageSpi evtStorageSpi = new MemoryEventStorageSpi();

        evtStorageSpi.setExpireCount(50_000);

        cfg.setEventStorageSpi(evtStorageSpi);

        return cfg;
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode getCacheMode();

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(getCacheMode());
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());

        if (getCacheMode() == PARTITIONED)
            cacheCfg.setBackups(1);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testPreloadEvents() throws Exception {
        Ignite g1 = startGrid("g1");

        IgniteCache<Integer, String> cache = g1.cache(null);

        cache.put(1, "val1");
        cache.put(2, "val2");
        cache.put(3, "val3");

        Ignite g2 = startGrid("g2");

        Collection<Event> evts = g2.events().localQuery(F.<Event>alwaysTrue(), EVT_CACHE_REBALANCE_OBJECT_LOADED);

        checkPreloadEvents(evts, g2, U.toIntList(new int[]{1, 2, 3}));
    }

    /**
     * @param evts Events.
     * @param g Grid.
     * @param keys Keys.
     */
    protected void checkPreloadEvents(Collection<Event> evts, Ignite g, Collection<? extends Object> keys) {
        assertEquals(keys.size(), evts.size());

        for (Event evt : evts) {
            CacheEvent cacheEvt = (CacheEvent)evt;
            assertEquals(EVT_CACHE_REBALANCE_OBJECT_LOADED, cacheEvt.type());
            assertEquals(g.cache(null).getName(), cacheEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), cacheEvt.node().id());
            assertEquals(g.cluster().localNode().id(), cacheEvt.eventNode().id());
            assertTrue(cacheEvt.hasNewValue());
            assertNotNull(cacheEvt.newValue());
            assertTrue("Unexpected key: " + cacheEvt.key(), keys.contains(cacheEvt.key()));
        }
    }
}