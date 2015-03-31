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
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.configuration.DeploymentMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.GridCachePeekMode.*;

/**
 * Test for cache swap.
 */
public class GridCacheOffHeapSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    private static final int ENTRY_CNT = 1000;

    /** Swap count. */
    private final AtomicInteger swapCnt = new AtomicInteger();

    /** Unswap count. */
    private final AtomicInteger unswapCnt = new AtomicInteger();

    /** Saved versions. */
    private final Map<Integer, Object> versions = new HashMap<>();

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** PeerClassLoadingLocalClassPathExclude enable. */
    private boolean excluded;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setOffHeapMaxMemory(1024L * 1024L * 1024L);
        cacheCfg.setIndexedTypes(
            Integer.class, CacheValue.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setMarshaller(new OptimizedMarshaller(false));
        cfg.setDeploymentMode(SHARED);

        if (excluded)
            cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheOffHeapSelfTest.class.getName(),
                CacheValue.class.getName());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        versions.clear();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testOffHeapDeployment() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);

            excluded = true;

            Ignite ignite2 = startGrid(2);

            GridCache<Integer, Object> cache1 = ((IgniteKernal)ignite1).getCache(null);
            GridCache<Integer, Object> cache2 = ((IgniteKernal)ignite2).getCache(null);

            Object v1 = new CacheValue(1);

            cache1.put(1, v1);

            info("Stored value in cache1 [v=" + v1 + ", ldr=" + v1.getClass().getClassLoader() + ']');

            Object v2 = cache2.get(1);

            assert v2 != null;

            info("Read value from cache2 [v=" + v2 + ", ldr=" + v2.getClass().getClassLoader() + ']');

            assert v2 != null;
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");

            SwapListener lsnr = new SwapListener();

            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_TO_OFFHEAP, EVT_CACHE_OBJECT_FROM_OFFHEAP);

            cache2.evictAll();

            assert lsnr.awaitSwap();

            assert cache2.get(1) != null;

            assert lsnr.awaitUnswap();

            ignite2.events().stopLocalListen(lsnr);

            lsnr = new SwapListener();

            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_TO_OFFHEAP, EVT_CACHE_OBJECT_FROM_OFFHEAP);

            cache2.evictAll();

            assert lsnr.awaitSwap();

            stopGrid(1);

            boolean success = false;

            for (int i = 0; i < 6; i++) {
                success = cache2.get(1) == null;

                if (success)
                    break;
                else if (i < 2) {
                    info("Sleeping to wait for cache clear.");

                    Thread.sleep(500);
                }
            }

            assert success;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Cache.
     * @param timeout Timeout.
     * @return {@code True} if success.
     * @throws InterruptedException If thread was interrupted.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean waitCacheEmpty(CacheProjection<Integer,Object> cache, long timeout)
        throws InterruptedException {
        assert cache != null;
        assert timeout >= 0;

        long end = System.currentTimeMillis() + timeout;

        while (end - System.currentTimeMillis() >= 0) {
            if (cache.isEmpty())
                return true;

            Thread.sleep(500);
        }

        return cache.isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeap() throws Exception {
        try {
            startGrids(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt != null;

                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_TO_OFFHEAP:
                            swapCnt.incrementAndGet();

                            break;
                        case EVT_CACHE_OBJECT_FROM_OFFHEAP:
                            unswapCnt.incrementAndGet();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_TO_OFFHEAP, EVT_CACHE_OBJECT_FROM_OFFHEAP);

            GridCache<Integer, CacheValue> cache = ((IgniteKernal)grid(0)).getCache(null);

            populate(cache);
            evictAll(cache);

            // Check iterator.
            Iterator<Map.Entry<Integer, CacheValue>> it = cache.offHeapIterator();

            int cnt = 0;

            while (it.hasNext()) {
                Map.Entry<Integer, CacheValue> e = it.next();

                assertEquals(e.getKey().intValue(), e.getValue().value());

                cnt++;
            }

            assertEquals(ENTRY_CNT, cnt);

            query(cache, 0, 200);        // Query swapped entries.
            unswap(cache, 200, 400);     // Check 'promote' method.
            unswapAll(cache, 400, 600);  // Check 'promoteAll' method.
            get(cache, 600, 800);        // Check 'get' method.
            peek(cache, 800, ENTRY_CNT); // Check 'peek' method in 'SWAP' mode.

            // Check that all entries were unswapped.
            for (int i = 0; i < ENTRY_CNT; i++) {
                CacheValue val = cache.peek(i);

                assert val != null;
                assert val.value() == i;
            }

            // Query unswapped entries.
            Collection<Map.Entry<Integer, CacheValue>> res = cache.queries().
                createSqlQuery(CacheValue.class, "val >= ? and val < ?").
                execute(0, ENTRY_CNT).
                get();

            assert res.size() == ENTRY_CNT;

            for (Map.Entry<Integer, CacheValue> entry : res) {
                assert entry != null;
                assert entry.getKey() != null;
                assert entry.getValue() != null;
                assert entry.getKey() == entry.getValue().value();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapIterator() throws Exception {
        try {
            startGrids(1);

            grid(0);

            GridCache<Integer, Integer> cache = ((IgniteKernal)grid(0)).getCache(null);

            for (int i = 0; i < 100; i++) {
                info("Putting: " + i);

                cache.put(i, i);

                assert cache.evict(i);
            }

            Iterator<Map.Entry<Integer, Integer>> iter = cache.offHeapIterator();

            assert iter != null;

            int i = 0;

            while (iter.hasNext()) {
                Map.Entry<Integer, Integer> e = iter.next();

                Integer key = e.getKey();

                info("Key: " + key);

                i++;

                iter.remove();

                assertNull(cache.get(key));
            }

            assertEquals(100, i);

            assert cache.isEmpty();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Populates cache.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void populate(GridCache<Integer, CacheValue> cache) throws Exception {
        resetCounters();

        for (int i = 0; i < ENTRY_CNT; i++) {
            cache.put(i, new CacheValue(i));

            CacheValue val = cache.peek(i);

            assert val != null;
            assert val.value() == i;

            GridCacheEntryEx entry = dht(cache).peekEx(i);

            assert entry != null;

            versions.put(i, entry.version());
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;
    }

    /**
     * Evicts all entries in cache.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void evictAll(GridCache<Integer, CacheValue> cache) throws Exception {
        resetCounters();

        assertEquals(ENTRY_CNT, cache.size());
        assertEquals(0, cache.offHeapEntriesCount());

        for (int i = 0; i < ENTRY_CNT; i++) {
            cache.evict(i);

            assertEquals(ENTRY_CNT - i - 1, cache.size());
            assertEquals(i + 1, cache.offHeapEntriesCount());
        }
        // cache.evictAll();

        assertEquals(0, cache.size());
        assertEquals(ENTRY_CNT, cache.offHeapEntriesCount());

        for (int i = 0; i < ENTRY_CNT; i++)
            assertNull(cache.peek(i));

        assertEquals(ENTRY_CNT, swapCnt.get());
        assertEquals(0, unswapCnt.get());
    }

    /**
     * Runs SQL query and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void query(GridCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        Collection<Map.Entry<Integer, CacheValue>> res = cache.queries().
            createSqlQuery(CacheValue.class, "val >= ? and val < ?").
            execute(lowerBound, upperBound).
            get();

        assertEquals(res.size(), upperBound - lowerBound);

        for (Map.Entry<Integer, CacheValue> entry : res) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getValue() != null;
            assert entry.getKey() == entry.getValue().value();
        }

        assertEquals(0, swapCnt.get());
        assertEquals(0, unswapCnt.get());

        checkEntries(cache, lowerBound, upperBound);

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);
    }

    /**
     * Unswaps entries and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void unswap(GridCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        assertEquals(0, swapCnt.get());
        assertEquals(0, unswapCnt.get());

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            CacheValue val = cache.promote(i);

            assertNotNull(val);
            assertEquals(i, val.value());

            assertEquals(i - lowerBound + 1, unswapCnt.get());
        }

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);

        checkEntries(cache, lowerBound, upperBound);

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);
    }

    /**
     * Unswaps entries and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void unswapAll(GridCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        Collection<Integer> keys = new HashSet<>();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            keys.add(i);
        }

        cache.promoteAll(keys);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Unswaps entries via {@code get} method and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void get(GridCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            CacheValue val = cache.get(i);

            assert val != null;
            assert val.value() == i;
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Peeks entries in {@code SWAP} mode and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void peek(GridCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            CacheValue val = cache.peek(i, F.asList(SWAP));

            assert val != null;
            assert val.value() == i;
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Resets event counters.
     */
    private void resetCounters() {
        swapCnt.set(0);
        unswapCnt.set(0);
    }

    /**
     * Checks that entries in cache are correct after being unswapped.
     * If entry is still swapped, it will be unswapped in this method.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void checkEntries(GridCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        for (int i = lowerBound; i < upperBound; i++) {
            cache.promote(i);

            GridCacheEntryEx entry = dht(cache).entryEx(i);

            assert entry != null;
            assert entry.key() != null;

            CacheValue val = CU.value(entry.rawGet(), entry.context(), false);

            assertNotNull("Value null for key: " + i, val);
            assertEquals(entry.key().value(entry.context().cacheObjectContext(), false), (Integer)val.value());

            assertEquals(entry.version(), versions.get(i));
        }
    }

    /**
     *
     */
    private static class CacheValue {
        /** Value. */
        @QuerySqlField
        private final int val;

        /**
         * @param val Value.
         */
        private CacheValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheValue.class, this);
        }
    }

    /**
     *
     */
    private class SwapListener implements IgnitePredicate<Event> {
        /** */
        private final CountDownLatch swapLatch = new CountDownLatch(1);

        /** */
        private final CountDownLatch unswapLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            assert evt != null;

            info("Received event: " + evt);

            switch (evt.type()) {
                case EVT_CACHE_OBJECT_TO_OFFHEAP:
                    swapLatch.countDown();

                    break;
                case EVT_CACHE_OBJECT_FROM_OFFHEAP:
                    unswapLatch.countDown();

                    break;
            }

            return true;
        }

        /**
         * @return {@code True} if await succeeded.
         * @throws InterruptedException If interrupted.
         */
        boolean awaitSwap() throws InterruptedException {
            return swapLatch.await(5000, MILLISECONDS);
        }

        /**
         * @return {@code True} if await succeeded.
         * @throws InterruptedException If interrupted.
         */
        boolean awaitUnswap() throws InterruptedException {
            return unswapLatch.await(5000, MILLISECONDS);
        }
    }
}
