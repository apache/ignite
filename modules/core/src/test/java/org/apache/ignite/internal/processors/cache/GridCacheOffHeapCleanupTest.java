/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.noop.NoopSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check offheap allocations are freed after cache destroy.
 */
public class GridCacheOffHeapCleanupTest extends GridCommonAbstractTest {
    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Memory mode. */
    private CacheMemoryMode memoryMode;

    /** Eviction policy. */
    private EvictionPolicy evictionPolicy;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setSwapSpaceSpi(new NoopSwapSpaceSpi());

        return cfg;
    }

    /**
     * Creates cache configuration.
     *
     * @return cache configuration.
     * */
    private CacheConfiguration<Integer, String> createCacheConfiguration() {
        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setMemoryMode(memoryMode);
        cacheCfg.setEvictionPolicy(evictionPolicy);
        cacheCfg.setNearConfiguration(null);

        return cacheCfg;
    }

    /**
     * Check offheap resources are freed after cache destroy.
     *
     * @throws Exception If failed.
     */
    private void checkCleanupOffheapAfterCacheDestroy() throws Exception {
        final String defaultCacheSpaceName = "gg-swap-cache-dflt";

        try (Ignite g = startGrid(0)) {
            checkOffheapAllocated(defaultCacheSpaceName, false);

            IgniteCache<Integer, String> cache = g.getOrCreateCache(createCacheConfiguration());

            cache.put(1, "value_1");
            cache.put(2, "value_2");

            checkOffheapAllocated(defaultCacheSpaceName, true);

            g.destroyCache(null);
            checkOffheapAllocated(defaultCacheSpaceName, false);
        }
    }

    /**
     * Check is offheap allocated for given space name using internal API.
     *
     * @param spaceName Space name.
     * @param allocated true, if we expected that offheap is allocated; false, otherwise.
     * @throws Exception If failed.
     * */
    private void checkOffheapAllocated(String spaceName, boolean allocated) throws Exception {
        long offheapSize = grid(0).context().offheap().allocatedSize(spaceName);

        if (allocated != (offheapSize >= 0))
            assertEquals("Unexpected offheap allocated size for space " + spaceName, offheapSize);
    }


    /**
     * Check offheap resources are freed after cache destroy - ONHEAP_TIERED memory mode
     *
     * @throws Exception If failed.
     */
    public void testCleanupOffheapAfterCacheDestroyOnheapTiered() throws Exception {
        memoryMode = CacheMemoryMode.ONHEAP_TIERED;

        FifoEvictionPolicy evictionPolicy0 = new FifoEvictionPolicy();
        evictionPolicy0.setMaxSize(1);

        evictionPolicy = evictionPolicy0;

        checkCleanupOffheapAfterCacheDestroy();
    }

    /**
     * Check offheap resources are freed after cache destroy - OFFHEAP_TIERED memory mode
     *
     * @throws Exception If failed.
     */
    public void testCleanupOffheapAfterCacheDestroyOffheapTiered() throws Exception {
        memoryMode = CacheMemoryMode.OFFHEAP_TIERED;
        evictionPolicy = null;

        checkCleanupOffheapAfterCacheDestroy();
    }

    /**
     * Check offheap resources are freed after cache destroy - OFFHEAP_VALUES memory mode
     *
     * @throws Exception If failed.
     */
    public void testCleanupOffheapAfterCacheDestroyOffheapValues() throws Exception {
        memoryMode = CacheMemoryMode.OFFHEAP_VALUES;
        evictionPolicy = null;

        try (Ignite g = startGrid(0)) {
            IgniteCache<Integer, String> cache = g.getOrCreateCache(createCacheConfiguration());

            cache.put(1, "value_1");
            cache.put(2, "value_2");

            GridCacheContext context =  GridTestUtils.cacheContext(cache);
            GridUnsafeMemory unsafeMemory = context.unsafeMemory();

            g.destroyCache(null);

            if (unsafeMemory != null)
                assertEquals("Unsafe memory not freed", 0, unsafeMemory.allocatedSize());
        }
    }
}
