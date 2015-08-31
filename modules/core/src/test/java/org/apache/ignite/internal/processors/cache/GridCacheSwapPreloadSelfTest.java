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

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for cache swap preloading.
 */
public class GridCacheSwapPreloadSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    private static final int ENTRY_CNT = 15_000;

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

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setEvictSynchronized(false);
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

            IgniteCache<Integer, Integer> cache = grid(0).cache(null);

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

            int size = grid(1).cache(null).localSize(CachePeekMode.ALL);

            info("New node cache size: " + size);

            assertEquals(ENTRY_CNT, size);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwapReplicatedMultithreaded() throws Exception {
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
        fail("https://issues.apache.org/jira/browse/IGNITE-614");

        final AtomicBoolean done = new AtomicBoolean();
        IgniteInternalFuture<?> fut = null;

        try {
            startGrid(0);

            final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            assertNotNull(cache);

            // Populate.
            for (int i = 0; i < ENTRY_CNT; i++)
                cache.put(i, i);

            Set<Integer> keys = new HashSet<>();

            for (Cache.Entry<Integer, Integer> entry : cache.localEntries())
                keys.add(entry.getKey());

            cache.localEvict(keys);

            fut = multithreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    while (!done.get()) {
                        int key = rnd.nextInt(ENTRY_CNT);

                        Integer i = cache.get(key);

                        assertNotNull(i);
                        assertEquals(Integer.valueOf(key), i);

                        cache.localEvict(Collections.singleton(rnd.nextInt(ENTRY_CNT)));
                    }

                    return null;
                }
            }, 10);

            startGrid(1);

            done.set(true);

            fut.get();

            fut = null;

            int size = grid(1).cache(null).localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP,
                CachePeekMode.NEAR, CachePeekMode.ONHEAP);

            info("New node cache size: " + size);

            if (size != ENTRY_CNT) {
                Set<Integer> keySet = new TreeSet<>();

                int next = 0;

                for (IgniteCache.Entry<Integer, Integer> e : grid(1).<Integer, Integer>cache(null).localEntries())
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