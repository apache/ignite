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
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 * Test for cache swap preloading.
 */
public class GridCacheSwapPreloadSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    private static final int ENTRY_CNT = 15000;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private CacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setEvictSynchronized(false);
        cacheCfg.setEvictNearSynchronized(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        if (cacheMode == PARTITIONED)
            cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testSwapReplicated() throws Exception {
        cacheMode = REPLICATED;

        checkSwap();
    }

    /** @throws Exception If failed. */
    public void testSwapPartitioned() throws Exception {
        cacheMode = PARTITIONED;

        checkSwap();
    }

    /** @throws Exception If failed. */
    private void checkSwap() throws Exception {
        try {
            startGrid(0);

            IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

            Set<Integer> keys = new HashSet<>();

            // Populate.
            for (int i = 0; i < ENTRY_CNT; i++) {
                keys.add(i);

                cache.put(i, i);
            }

            info("Put finished.");

            // Evict all.
            cache.localEvict(keys);

            info("Evict finished.");

            for (int i = 0; i < ENTRY_CNT; i++)
                assertNull(cache.localPeek(i, CachePeekMode.ONHEAP));

            assert cache.localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP, CachePeekMode.NEAR,
                CachePeekMode.ONHEAP) == 0;

            startGrid(1);

            int size = grid(1).jcache(null).localSize();

            info("New node cache size: " + size);

            assertEquals(ENTRY_CNT, size);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * TODO: GG-4804 Swap preloading test failed with NotNull assertion, TODO: GG-4804 while key should have been found
     * either in swap or in cache
     *
     * @throws Exception If failed.
     */
    public void _testSwapReplicatedMultithreaded() throws Exception {
        cacheMode = REPLICATED;

        checkSwapMultithreaded();
    }

    /** @throws Exception If failed. */
    public void testSwapPartitionedMultithreaded() throws Exception {
        cacheMode = PARTITIONED;

        checkSwapMultithreaded();
    }

    /** @throws Exception If failed. */
    private void checkSwapMultithreaded() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        IgniteInternalFuture<?> fut = null;

        try {
            startGrid(0);

            final GridCache<Integer, Integer> cache = ((IgniteKernal)grid(0)).cache(null);

            assertNotNull(cache);

            // Populate.
            for (int i = 0; i < ENTRY_CNT; i++)
                cache.put(i, i);

            cache.evictAll();

            fut = multithreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    while (!done.get()) {
                        int key = rnd.nextInt(ENTRY_CNT);

                        Integer i = cache.get(key);

                        assertNotNull(i);
                        assertEquals(Integer.valueOf(key), i);

                        boolean b = cache.evict(rnd.nextInt(ENTRY_CNT));

                        assert b;
                    }

                    return null;
                }
            }, 10);

            startGrid(1);

            done.set(true);

            int size = grid(1).jcache(null).localSize();

            info("New node cache size: " + size);

            if (size != ENTRY_CNT) {
                Set<Integer> keySet = new TreeSet<>();

                int next = 0;

                for (IgniteCache.Entry<Integer, Integer> e : grid(1).<Integer, Integer>jcache(null).localEntries())
                    keySet.add(e.getKey());

                for (Integer i : keySet) {
                    while (next < i)
                        info("Missing key: " + next++);

                    next++;
                }
            }

            assertEquals(ENTRY_CNT, size);
        }
        finally {
            done.set(true);

            try {
                if (fut != null)
                    fut.get();
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
