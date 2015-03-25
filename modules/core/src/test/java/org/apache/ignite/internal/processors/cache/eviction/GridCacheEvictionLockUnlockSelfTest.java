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

package org.apache.ignite.internal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;

/**
 *
 */
public class GridCacheEvictionLockUnlockSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Evict latch. */
    private static CountDownLatch evictLatch;

    /** Evict counter. */
    private static final AtomicInteger evictCnt = new AtomicInteger();

    /** Touch counter. */
    private static final AtomicInteger touchCnt = new AtomicInteger();

    /** Cache mode. */
    private CacheMode mode;

    /** Number of grids to start. */
    private int gridCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictionPolicy(new EvictionPolicy());
        cc.setAtomicityMode(TRANSACTIONAL);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        nearCfg.setNearEvictionPolicy(new EvictionPolicy());
        cc.setNearConfiguration(nearCfg);

        if (mode == PARTITIONED)
            cc.setBackups(1);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        c.setDiscoverySpi(discoSpi);

        return c;
    }

    /** @throws Exception If failed. */
    public void testLocal() throws Exception {
        mode = LOCAL;
        gridCnt = 1;

        doTest();
    }

    /** @throws Exception If failed. */
    public void testReplicated() throws Exception {
        mode = REPLICATED;
        gridCnt = 3;

        doTest();
    }

    /** @throws Exception If failed. */
    public void testPartitioned() throws Exception {
        mode = PARTITIONED;
        gridCnt = 3;

        doTest();
    }

    /** @throws Exception If failed. */
    private void doTest() throws Exception {
        try {
            startGridsMultiThreaded(gridCnt);

            for (int i = 0; i < gridCnt; i++)
                grid(i).events().localListen(new EvictListener(), EVT_CACHE_ENTRY_EVICTED);

            for (int i = 0; i < gridCnt; i++) {
                reset();

                IgniteCache<Object, Object> cache = jcache(i);

                Lock lock = cache.lock("key");

                lock.lock();
                lock.unlock();

                assertTrue(evictLatch.await(3, SECONDS));

                assertEquals(gridCnt, evictCnt.get());
                assertEquals(gridCnt, touchCnt.get());

                for (int j = 0; j < gridCnt; j++)
                    assertFalse(jcache(j).containsKey("key"));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    private void reset() throws Exception {
        evictLatch = new CountDownLatch(gridCnt);

        evictCnt.set(0);
        touchCnt.set(0);
    }

    /** Eviction event listener. */
    private static class EvictListener implements IgnitePredicate<Event> {
        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            assert evt.type() == EVT_CACHE_ENTRY_EVICTED;

            evictCnt.incrementAndGet();

            evictLatch.countDown();

            return true;
        }
    }

    /** Eviction policy. */
    private static class EvictionPolicy implements org.apache.ignite.cache.eviction.EvictionPolicy<Object, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry<Object, Object> entry) {
            touchCnt.incrementAndGet();

            entry.evict();
        }
    }
}
