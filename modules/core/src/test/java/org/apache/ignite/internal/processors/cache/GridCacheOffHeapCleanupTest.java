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
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.PE;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;

/**
 * Check offheap allocations are freed after cache destroy.
 */
public class GridCacheOffHeapCleanupTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Name of cache. */
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

        checkCleanupOffheapAfterCacheDestroy();;
    }

    /**
     * Checks cache are correctly destroyed under load - ONHEAP_TIERED memory mode
     *
     * @throws Exception If failed
     * */
    public void testCacheDestroyUnderLoadOnheapTiered() throws Exception {
        memoryMode = ONHEAP_TIERED;

        FifoEvictionPolicy evictionPlc0 = new FifoEvictionPolicy();
        evictionPlc0.setMaxSize(1);

        evictionPlc = evictionPlc0;

        checkCacheDestroyUnderLoad();
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
        checkCacheDestroyUnderLoad();
    }

    /**
     * Checks cache are correctly destroyed under load - OFFHEAP_TIERED memory mode
     *
     * @throws Exception If failed
     * */
    public void testCacheDestroyUnderLoadOffheapTiered() throws Exception {
        memoryMode = OFFHEAP_TIERED;
        evictionPlc = null;

        checkCacheDestroyUnderLoad();
    }

    /**
     * Checks offheap resources are freed after cache destroy - OFFHEAP_VALUES memory mode
     *
     * @throws Exception If failed.
     */
    public void testCleanupOffheapAfterCacheDestroyOffheapValues() throws Exception {
        memoryMode = OFFHEAP_VALUES;
        evictionPlc = null;

        try {
            Ignite g = startGrids(2);

            IgniteCache<Integer, String> cache = g.getOrCreateCache(createCacheConfiguration());

            cache.put(1, "value_1");
            cache.put(2, "value_22");

            GridUnsafeMemory nearCacheMem = internalCache(cache).context().unsafeMemory();
            GridUnsafeMemory dhtCacheMem = dht(cache).context().unsafeMemory();

            g.destroyCache(cache.getName());

            if (nearCacheMem != null)
                assertEquals("Unsafe memory not freed", 0, nearCacheMem.allocatedSize());
            if (dhtCacheMem != null)
                assertEquals("Unsafe memory not freed", 0, dhtCacheMem.allocatedSize());
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Checks cache are correctly destroyed under load - OFFHEAP_VALUES memory mode
     *
     * @throws Exception If failed
     * */
    public void testCacheDestroyUnderLoadOffheapValues() throws Exception {
        memoryMode = OFFHEAP_VALUES;
        evictionPlc = null;

        checkCacheDestroyUnderLoad();
    }

    /**
     * Destroying cache when many thread are still reading. Expects that
     * reader threads will get "Cache has been stopped" exception and wont
     * get invalid value (that may be caused by incorrect offHeap memory
     * deallocation).
     *
     * @throws Exception If failed.
     * */
    private void checkCacheDestroyUnderLoad() throws Exception {
        try (Ignite g = startGrid(0)) {
            final AtomicBoolean fail = new AtomicBoolean(false);
            final IgniteCache<Integer, String> cache = g.getOrCreateCache(createCacheConfiguration());

            final int totalCnt = 100_000;
            final String[] values = new String[totalCnt];
            for (int i = 0; i < totalCnt; i++) {
                values[i] = "value-" + i;
                cache.put(i, values[i]);
            }

            final int threadCnt = 20;
            final CountDownLatch startLatch = new CountDownLatch(threadCnt);
            final CountDownLatch stoppedLatch = new CountDownLatch(threadCnt);
            final AtomicInteger threadNum = new AtomicInteger(0);

            GridTestUtils.runMultiThreadedAsync(new Runnable() {
                    @Override
                    public void run() {
                        final int num = threadNum.getAndIncrement();
                        final int start = num * totalCnt / threadCnt;
                        final int stop = (num + 1) * totalCnt / threadCnt;
                        try {
                            GridTestUtils.assertThrows(log, new Callable<Object>() {
                                @Override
                                public Object call() throws Exception {
                                    startLatch.countDown();
                                    int i = start;
                                    while (true) {
                                        if (i == stop)
                                            i = start;
                                        String value = cache.get(i);
                                        assertEquals("Unexpected cache value", values[i], value);
                                        i++;
                                    }
                                }
                            }, IllegalStateException.class, "Cache has been stopped");
                        } catch (Exception e) {
                            error("Unexpected exception in in reader thread", e);
                            fail.set(true);
                        } finally {
                            stoppedLatch.countDown();
                        }
                    }
                },
                threadCnt, "reader");

            startLatch.await();

            Thread.sleep(300);

            g.destroyCache(cache.getName());

            stoppedLatch.await();

            assertTrue("Unexpected termination of reader thread. Please see log for details", !fail.get());
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

        ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, String>());

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
