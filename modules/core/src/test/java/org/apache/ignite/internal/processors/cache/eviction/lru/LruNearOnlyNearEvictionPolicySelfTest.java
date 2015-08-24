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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * LRU near eviction tests for NEAR_ONLY distribution mode (GG-8884).
 */
public class LruNearOnlyNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Node count. */
    private int cnt;

    /** Caching mode specified by test. */
    private CacheMode cacheMode;

    /** Cache atomicity mode specified by test. */
    private CacheAtomicityMode atomicityMode;

    /** Memory mode. */
    private CacheMemoryMode memMode;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cnt = 0;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        if (cnt == 0)
            c.setClientMode(true);
        else {
            CacheConfiguration cc = new CacheConfiguration();

            cc.setCacheMode(cacheMode);
            cc.setAtomicityMode(atomicityMode);
            cc.setMemoryMode(memMode);
            cc.setWriteSynchronizationMode(PRIMARY_SYNC);
            cc.setRebalanceMode(SYNC);
            cc.setStartSize(100);
            cc.setBackups(0);

            c.setCacheConfiguration(cc);
        }

        c.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder).setForceServerMode(true));

        cnt++;

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = PARTITIONED;
        memMode = ONHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedAtomicOffHeapNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = PARTITIONED;
        memMode = OFFHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = PARTITIONED;
        memMode = ONHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTransactionalOffHeapNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = PARTITIONED;
        memMode = OFFHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = REPLICATED;
        memMode = ONHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomicOffHeapNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = REPLICATED;
        memMode = OFFHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = REPLICATED;
        memMode = ONHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTransactionalOffHeapNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = REPLICATED;
        memMode = OFFHEAP_TIERED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startGrids(GRID_COUNT);

        try {
            NearCacheConfiguration nearCfg = new NearCacheConfiguration();

            LruEvictionPolicy plc = new LruEvictionPolicy();
            plc.setMaxSize(EVICTION_MAX_SIZE);

            nearCfg.setNearEvictionPolicy(plc);

            grid(0).createNearCache(null, nearCfg);

            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            try (IgniteDataStreamer<Integer, String> ldr = grid(1).dataStreamer(null)) {
                for (int i = 0; i < cnt; i++)
                    ldr.addData(i, Integer.toString(i));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                IgniteCache<Integer, String> cache = grid(0).cache(null);

                assertTrue(cache.get(i).equals(Integer.toString(i)));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}
