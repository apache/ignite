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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test {@link GridCache#globalClearAll()} operation in multinode environment with nodes
 * having caches with different names.
 */
public class GridCacheGlobalClearAllSelfTest extends GridCommonAbstractTest {
    /** Grid nodes count. */
    private static final int GRID_CNT = 3;

    /** Amount of keys stored in the default cache. */
    private static final int KEY_CNT = 20;

    /** Amount of keys stored in cache other than default. */
    private static final int KEY_CNT_OTHER = 10;

    /** Default cache name. */
    private static final String CACHE_NAME = "cache_name";

    /** Cache name which differs from the default one. */
    private static final String CACHE_NAME_OTHER = "cache_name_other";

    /** VM IP finder for TCP discovery SPI. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name which will be passed to grid configuration. */
    private GridCacheMode cacheMode = PARTITIONED;

    /** Cache mode which will be passed to grid configuration. */
    private String cacheName = CACHE_NAME;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setDistributionMode(NEAR_PARTITIONED);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Start GRID_CNT nodes. All nodes except the last one will have one cache with particular name, while the last
     * one will have one cache of the same type, but with different name.
     *
     * @throws Exception In case of exception.
     */
    private void startNodes() throws Exception {
        cacheName = CACHE_NAME;

        for (int i = 0; i < GRID_CNT - 1; i++)
            startGrid(i);

        cacheName = CACHE_NAME_OTHER;

        startGrid(GRID_CNT - 1);
    }

    /**
     * Test for partitioned cache.
     *
     * @throws Exception In case of exception.
     */
    public void testGlobalClearAllPartitioned() throws Exception {
        cacheMode = PARTITIONED;

        startNodes();

        performTest();
    }

    /**
     * Test for replicated cache.
     *
     * @throws Exception In case of exception.
     */
    public void testGlobalClearAllReplicated() throws Exception {
        cacheMode = REPLICATED;

        startNodes();

        performTest();
    }

    /**
     * Ensure that globalClearAll() clears correct cache and is only executed on nodes with the cache excluding
     * master-node where it is executed locally.
     *
     * @throws Exception If failed.
     */
    public void performTest() throws Exception {
        // Put values into normal replicated cache.
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).cache(CACHE_NAME).put(i, "val" + i);

        // Put values into a cache with another name.
        for (int i = 0; i < KEY_CNT_OTHER; i++)
            grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).put(i, "val" + i);

        // Check cache sizes.
        for (int i = 0; i < GRID_CNT - 1; i++) {
            GridCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

            assertEquals("Key set [i=" + i + ", keys=" + cache.keySet() + ']', KEY_CNT, cache.size());
        }

        assert grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).size() == KEY_CNT_OTHER;

        // Perform clear.
        grid(0).cache(CACHE_NAME).globalClearAll();

        // Expect caches with the given name to be clear on all nodes.
        for (int i = 0; i < GRID_CNT - 1; i++)
            assert grid(i).cache(CACHE_NAME).isEmpty();

        // ... but cache with another name should remain untouched.
        assert grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).size() == KEY_CNT_OTHER;
    }
}
