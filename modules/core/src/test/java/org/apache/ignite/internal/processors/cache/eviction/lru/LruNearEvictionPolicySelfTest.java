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

package org.apache.ignite.internal.processors.cache.eviction.lru;

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * LRU near eviction tests (GG-8884).
 */
public class LruNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Cache atomicity mode specified by test. */
    private CacheAtomicityMode atomicityMode;

    /** Memory mode. */
    private CacheMemoryMode memMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = new CacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(atomicityMode);
        cc.setMemoryMode(memMode);
        cc.setWriteSynchronizationMode(PRIMARY_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setStartSize(100);
        cc.setBackups(0);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(EVICTION_MAX_SIZE);

        nearCfg.setNearEvictionPolicy(plc);
        cc.setNearConfiguration(nearCfg);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        memMode = ONHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffHeapNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        memMode = CacheMemoryMode.OFFHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        memMode = ONHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalOffHeapNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        memMode = CacheMemoryMode.OFFHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startGridsMultiThreaded(GRID_COUNT);

        try {
            Random rand = new Random(0);

            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            try (IgniteDataStreamer<Integer, String> ldr = grid(0).dataStreamer(null)) {
                for (int i = 0; i < cnt; i++)
                    ldr.addData(i, Integer.toString(i));
            }

            for (int i = 0; i < GRID_COUNT; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                IgniteCache<Integer, String> cache = grid(rand.nextInt(GRID_COUNT)).cache(null);

                assertTrue(cache.get(i).equals(Integer.toString(i)));
            }

            for (int i = 0; i < GRID_COUNT; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}