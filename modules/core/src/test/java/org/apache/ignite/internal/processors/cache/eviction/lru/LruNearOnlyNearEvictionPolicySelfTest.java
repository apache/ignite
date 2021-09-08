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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * LRU near eviction tests for NEAR_ONLY distribution mode (GG-8884).
 */
public class LruNearOnlyNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Caching mode specified by test. */
    private CacheMode cacheMode;

    /** Cache atomicity mode specified by test. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != 0) {
            CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

            cc.setCacheMode(cacheMode);
            cc.setAtomicityMode(atomicityMode);
            cc.setWriteSynchronizationMode(PRIMARY_SYNC);
            cc.setRebalanceMode(SYNC);
            cc.setBackups(0);

            c.setCacheConfiguration(cc);
        }

        ((TcpDiscoverySpi)c.getDiscoverySpi()).setForceServerMode(true);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testPartitionedMvccTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testReplicatedMvccTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startClientGrid(0);
        startGridsMultiThreaded(1, GRID_COUNT - 1);

        try {
            NearCacheConfiguration nearCfg = new NearCacheConfiguration();

            LruEvictionPolicy plc = new LruEvictionPolicy();
            plc.setMaxSize(EVICTION_MAX_SIZE);

            nearCfg.setNearEvictionPolicy(plc);

            grid(0).createNearCache(DEFAULT_CACHE_NAME, nearCfg);

            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            try (IgniteDataStreamer<Integer, String> ldr = grid(1).dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = 0; i < cnt; i++)
                    ldr.addData(i, Integer.toString(i));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

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
