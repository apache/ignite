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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.swapspace.GridSwapSpaceManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check swap is cleaned after cache destroy.
 */
public class GridCacheSwapCleanupTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "swapCleanupCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * Creates cache configuration.
     *
     * @return Cache configuration.
     * */
    private CacheConfiguration createCacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setEvictionPolicy(new LruEvictionPolicy(10));
        ccfg.setSwapEnabled(true);

        return ccfg;
    }

    /**
     * Checks swap is cleaned after cache destroy.
     *
     * @throws Exception If failed.
     * */
    public void testSwapCleanupAfterCacheDestroy() throws Exception {
        try (Ignite g = startGrid()) {
            for (int iter = 0; iter < 3; iter++) {
                IgniteCache cache = g.getOrCreateCache(createCacheConfiguration());

                for (int i = 0; i < 20; i++) {
                    assertNull(cache.get(i));

                    cache.put(i, i);
                }

                String spaceName = CU.swapSpaceName(internalCache(cache).context());

                GridSwapSpaceManager swapSpaceMgr = ((IgniteEx)g).context().swap();

                assertEquals(10, swapSpaceMgr.swapKeys(spaceName));

                g.destroyCache(cache.getName());

                assertEquals(0, swapSpaceMgr.swapKeys(spaceName));
                assertEquals(0, swapSpaceMgr.swapSize(spaceName));
            }
        }
    }
}
