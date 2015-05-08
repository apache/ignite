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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Checks that CacheProjection.reload() operations are performed correctly.
 */
public class GridCacheReloadSelfTest extends GridCommonAbstractTest {
    /** Maximum allowed number of cache entries. */
    public static final int MAX_CACHE_ENTRIES = 500;

    /** Number of entries to load from store. */
    public static final int N_ENTRIES = 5000;

    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Near enabled flag. */
    private boolean nearEnabled = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cacheMode = null;
        nearEnabled = true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy(MAX_CACHE_ENTRIES));
        cacheCfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);

        final CacheStore store = new CacheStoreAdapter<Integer, Integer>() {
            @Override public Integer load(Integer key) {
                return key;
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends Integer> e) {
                //No-op.
            }

            @Override public void delete(Object key) {
                //No-op.
            }
        };

        cacheCfg.setCacheStoreFactory(singletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        if (cacheMode == PARTITIONED)
            cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Checks that eviction works with reload() on local cache.
     *
     * @throws Exception If error occurs.
     */
    public void testReloadEvictionLocalCache() throws Exception {
        cacheMode = CacheMode.LOCAL;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on partitioned cache
     * with near enabled.
     *
     * @throws Exception If error occurs.
     */
    //TODO: Active when ticket GG-3926 will be ready.
    public void _testReloadEvictionPartitionedCacheNearEnabled() throws Exception {
        cacheMode = PARTITIONED;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on partitioned cache
     * with near disabled.
     *
     * @throws Exception If error occurs.
     */
    public void testReloadEvictionPartitionedCacheNearDisabled() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on replicated cache.
     *
     * @throws Exception If error occurs.
     */
    public void testReloadEvictionReplicatedCache() throws Exception {
        cacheMode = CacheMode.REPLICATED;

        doTest();
    }

    /**
     * Actual test logic.
     *
     * @throws Exception If error occurs.
     */
    private void doTest() throws Exception {
        Ignite ignite = startGrid();

        try {
            IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < N_ENTRIES; i++)
                load(cache, i, true);

            assertEquals(MAX_CACHE_ENTRIES, cache.size());
        }
        finally {
            stopGrid();
        }
    }
}
