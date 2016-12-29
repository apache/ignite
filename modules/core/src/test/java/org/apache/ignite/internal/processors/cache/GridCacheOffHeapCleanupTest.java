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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;

/**
 * Check offheap allocations are freed after cache destroy.
 */
public class GridCacheOffHeapCleanupTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "testCache";

    /** Memory mode. */
    private CacheMemoryMode memoryMode;

    /** Eviction policy. */
    private EvictionPolicy evictionPlc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /**
     * Checks offheap resources are freed after cache destroy - ONHEAP_TIERED memory mode
     *
     * @throws Exception If failed.
     */
    public void testCleanupOffheapAfterCacheDestroyOnheapTiered() throws Exception {
        memoryMode = ONHEAP_TIERED;

        FifoEvictionPolicy evictionPlc0 = new FifoEvictionPolicy();
        evictionPlc0.setMaxSize(1);

        evictionPlc = evictionPlc0;

        checkCleanupOffheapAfterCacheDestroy();
    }

    /**
     * Checks offheap resources are freed after cache destroy - OFFHEAP_TIERED memory mode
     *
     * @throws Exception If failed.
     */
    public void testCleanupOffheapAfterCacheDestroyOffheapTiered() throws Exception {
        memoryMode = OFFHEAP_TIERED;
        evictionPlc = null;

        checkCleanupOffheapAfterCacheDestroy();
    }

    /**
     * TODO: IGNITE-2714.
     *
     * Checks offheap resources are freed after cache destroy - OFFHEAP_VALUES memory mode
     *
     * @throws Exception If failed.
     */
    public void _testCleanupOffheapAfterCacheDestroyOffheapValues() throws Exception {
        memoryMode = OFFHEAP_VALUES;
        evictionPlc = null;

        try (Ignite g = startGrid(0)) {
            IgniteCache<Integer, String> cache = g.getOrCreateCache(createCacheConfiguration());

            cache.put(1, "value_1");
            cache.put(2, "value_2");

            GridCacheContext ctx =  GridTestUtils.cacheContext(cache);
            GridUnsafeMemory unsafeMemory = ctx.unsafeMemory();

            g.destroyCache(null);

            if (unsafeMemory != null)
                assertEquals("Unsafe memory not freed", 0, unsafeMemory.allocatedSize());
        }
    }

    /**
     * Creates cache configuration.
     *
     * @return cache configuration.
     * */
    private CacheConfiguration<Integer, String> createCacheConfiguration() {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE_NAME);
        ccfg.setOffHeapMaxMemory(0);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setEvictionPolicy(evictionPlc);

        return ccfg;
    }

    /**
     * Check offheap resources are freed after cache destroy.
     *
     * @throws Exception If failed.
     */
    private void checkCleanupOffheapAfterCacheDestroy() throws Exception {
        final String spaceName = "gg-swap-cache-" + CACHE_NAME;

        try (Ignite g = startGrid(0)) {
            checkOffheapAllocated(spaceName, false);

            IgniteCache<Integer, String> cache = g.getOrCreateCache(createCacheConfiguration());

            cache.put(1, "value_1");
            cache.put(2, "value_2");

            checkOffheapAllocated(spaceName, true);

            g.destroyCache(cache.getName());

            checkOffheapAllocated(spaceName, false);
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

        assertEquals("Unexpected offheap allocated size", allocated, (offheapSize >= 0));
    }
}
