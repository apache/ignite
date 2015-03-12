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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.eventstorage.memory.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.events.EventType.*;

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
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

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

        IgniteCache<Integer, String> cache = g1.jcache(null);

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
            assertEquals(g.jcache(null).getName(), cacheEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), cacheEvt.node().id());
            assertEquals(g.cluster().localNode().id(), cacheEvt.eventNode().id());
            assertTrue(cacheEvt.hasNewValue());
            assertNotNull(cacheEvt.newValue());
            assertTrue("Unexpected key: " + cacheEvt.key(), keys.contains(cacheEvt.key()));
        }
    }
}
