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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;

/**
 * Tests off heap storage when both offheaped and swapped entries exists.
 */
public class GridCacheOffHeapAndSwapSelfTest extends GridCommonAbstractTest {
    /** Entry count. This count should result in 20KB memory. */
    private static final int ENTRY_CNT = 1000;

    /** This amount of memory gives 256 stored entries on 32 JVM. */
    private static final long OFFHEAP_MEM = 10L * 1024L;

    /** Offheap store count. */
    private final AtomicInteger offheapedCnt = new AtomicInteger();

    /** Offheap load count. */
    private final AtomicInteger onheapedCnt = new AtomicInteger();

    /** Swap count. */
    private final AtomicInteger swappedCnt = new AtomicInteger();

    /** Unswap count. */
    private final AtomicInteger unswapedCnt = new AtomicInteger();

    /** Lower bound for tested key range. */
    private long from;

    /** Upper bound for tested key range. */
    private long to;

    /** Saved versions. */
    private final Map<Long, Object> versions = new HashMap<>();

    /** Listener on swap events. Updates counters. */
    private final IgnitePredicate<IgniteEvent> swapLsnr = new IgnitePredicate<IgniteEvent>() {
        @Override public boolean apply(IgniteEvent evt) {
            assert evt != null;

            switch (evt.type()) {
                case EVT_CACHE_OBJECT_TO_OFFHEAP:
                    offheapedCnt.incrementAndGet();

                    break;
                case EVT_CACHE_OBJECT_FROM_OFFHEAP:
                    onheapedCnt.incrementAndGet();

                    break;

                case EVT_CACHE_OBJECT_SWAPPED:
                    swappedCnt.incrementAndGet();

                    break;

                case EVT_CACHE_OBJECT_UNSWAPPED:
                    unswapedCnt.incrementAndGet();

                    break;
            }

            return true;
        }
    };

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setOffHeapMaxMemory(OFFHEAP_MEM);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setEvictNearSynchronized(true);
        cacheCfg.setEvictSynchronizedKeyBufferSize(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cacheCfg.setEvictionPolicy(null);

        GridCacheQueryConfiguration qcfg = new GridCacheQueryConfiguration();

        qcfg.setIndexPrimitiveKey(true);
        qcfg.setIndexPrimitiveValue(true);

        cacheCfg.setQueryConfiguration(qcfg);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDeploymentMode(SHARED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);

        grid(0).events().localListen(swapLsnr,
            EVT_CACHE_OBJECT_TO_OFFHEAP, EVT_CACHE_OBJECT_FROM_OFFHEAP,
            EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        versions.clear();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid(0).events().localListen(swapLsnr,
            EVT_CACHE_OBJECT_TO_OFFHEAP, EVT_CACHE_OBJECT_FROM_OFFHEAP,
            EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache(null).clearAll();
    }

    /** Resets event counters. */
    private void resetCounters() {
        offheapedCnt.set(0);
        onheapedCnt.set(0);
        swappedCnt.set(0);
        unswapedCnt.set(0);
    }

    /**
     * Populates cache with entries and evicts them partially to offheap partially to swap.
     *
     * @return Cache to use in tests.
     * @throws Exception If failed.
     */
    private GridCache<Long, Long> populate() throws Exception {
        GridCache<Long, Long> cache = grid(0).cache(null);

        assertEquals(0, cache.size());
        assertEquals(0, cache.offHeapEntriesCount());

        assert offheapedCnt.get() == 0;
        assert onheapedCnt.get() == 0;
        assert swappedCnt.get() == 0;
        assert unswapedCnt.get() == 0;

        for (long i = 0; i < ENTRY_CNT; i++) {
            info("putting: " + i);

            cache.put(i, i);

            Long val = cache.peek(i);

            assert val != null;
            assert val == i;

            GridCacheEntry<Long, Long> entry = cache.entry(i);

            assert entry != null;

            versions.put(i, entry.version());
        }

        assertEquals(0, offheapedCnt.get());
        assertEquals(0, onheapedCnt.get());
        assertEquals(0, swappedCnt.get());
        assertEquals(0, unswapedCnt.get());

        assertEquals(ENTRY_CNT, cache.size());
        assertEquals(0, cache.offHeapEntriesCount());

        for (long i = 0; i < ENTRY_CNT; i++) {
            cache.evict(i);

            assertEquals(ENTRY_CNT - i - 1, cache.size());
        }

        // Ensure that part of entries located in off-heap memory and part is swapped.
        assertEquals(0, cache.size());
        assertTrue(cache.offHeapEntriesCount() > 0);
        assertTrue(cache.offHeapEntriesCount() < ENTRY_CNT);

        // Setting test window to catch near half of both offheaped and swapped entries.
        from = cache.offHeapEntriesCount() / 2;
        to = (ENTRY_CNT + cache.offHeapEntriesCount()) / 2;

        for (long i = 0; i < ENTRY_CNT; i++)
            assertNull(cache.peek(i));

        assertEquals(ENTRY_CNT, offheapedCnt.get());
        assertEquals(0, onheapedCnt.get());
        assertTrue(swappedCnt.get() > 0);
        assertEquals(0, unswapedCnt.get());

        resetCounters();

        return cache;
    }

    /**
     * Checks that entries in cache are correct after being unswapped. If entry is still swapped, it will be unswapped
     * in this method.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void checkEntries(GridCacheProjection<Long, Long> cache) throws Exception {
        for (long i = from; i < to; i++) {
            GridCacheEntry<Long, Long> entry = cache.entry(i);

            assert entry != null;
            assert entry.getKey() != null;

            Long val = entry.getValue();

            assertNotNull("Value null for key: " + i, val);
            assertEquals(entry.getKey(), val);
            assertEquals(entry.version(), versions.get(i));
        }

        assertEquals(0, swappedCnt.get());
        assertEquals(0, offheapedCnt.get());
    }

    /** @throws Exception If failed. */
    public void testPartitionIterators() throws Exception {
        populate();

        GridCacheAdapter<Long, Object> cacheAdapter = ((GridKernal)grid(0)).internalCache();
        GridNearCacheAdapter<Long, Object> cache = (GridNearCacheAdapter<Long, Object>)cacheAdapter;

        Map<Integer, Collection<Long>> grouped = new HashMap<>();

        for (long i = 0; i < ENTRY_CNT; i++) {
            // Avoid entry creation.
            int part = cache.affinity().partition(i);

            Collection<Long> list = grouped.get(part);

            if (list == null) {
                list = new LinkedList<>();

                grouped.put(part, list);
            }

            list.add(i);
        }

        // Now check that partition iterators contain all values.
        for (Map.Entry<Integer, Collection<Long>> entry : grouped.entrySet()) {
            int part = entry.getKey();
            Collection<Long> vals = entry.getValue();

            GridCacheContext<Long, Object> ctx = cache.dht().context();

            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<Object>>> it = ctx.swap().iterator(part, true);

            assert it != null || vals.isEmpty();

            if (it != null) {
                while (it.hasNext()) {
                    Map.Entry<byte[], GridCacheSwapEntry<Object>> swapEntry = it.next();

                    Long key = ctx.marshaller().unmarshal(swapEntry.getKey(), ctx.deploy().globalLoader());

                    assertTrue(vals.contains(key));

                    vals.remove(key);
                }
            }
        }

        info(String.valueOf(grouped));

        for (Map.Entry<Integer, Collection<Long>> entry : grouped.entrySet()) {
            assertTrue("Got skipped keys in partition iterator [partId=" + entry.getKey() +
                ", keys=" + entry.getValue(), F.isEmpty(entry.getValue()));
        }
    }

    /**
     * Tests offheap and swap iterators.
     *
     * @throws Exception If failed.
     */
    public void testIterators() throws Exception {
        GridCache<Long, Long> cache = populate();

        int cnt = 0;

        Iterator<Map.Entry<Long, Long>> ohIt = cache.offHeapIterator();

        while (ohIt.hasNext()) {
            Map.Entry<Long, Long> e = ohIt.next();

            assertEquals(e.getKey(), e.getValue());

            cnt++;
        }

        int cnt0 = cnt;

        assertTrue(cnt > 0);

        Iterator<Map.Entry<Long, Long>> sIt = cache.swapIterator();

        while (sIt.hasNext()) {
            Map.Entry<Long, Long> e = sIt.next();

            assertEquals(e.getKey(), e.getValue());

            cnt++;
        }

        assertTrue(cnt > cnt0);
        assertEquals(ENTRY_CNT, cnt);
    }

    /**
     * Tests SQL queries over evicted entries.
     *
     * @throws Exception If failed.
     */
    public void testSql() throws Exception {
        GridCache<Long, Long> cache = populate();

        Collection<Map.Entry<Long, Long>> res = cache.queries().
            createSqlQuery(Long.class, "_val >= ? and _val < ?").
            execute(from, to).
            get();

        assertEquals(to - from, res.size());

        for (Map.Entry<Long, Long> entry : res) {
            assertNotNull(entry);
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());
            assert entry.getKey().equals(entry.getValue());
        }

        assertEquals(0, offheapedCnt.get());
        assertEquals(0, onheapedCnt.get());

        checkEntries(cache);

        assertEquals(0, offheapedCnt.get());
        assertEquals(to - from, onheapedCnt.get() + unswapedCnt.get());
    }

    /**
     * Tests {@link GridCacheProjection#promote(Object)} behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    public void testUnswap() throws Exception {
        GridCache<Long, Long> cache = populate();

        for (long i = from; i < to; i++) {
            Long val = cache.promote(i);

            assertNotNull(val);
            assertEquals(i, val.longValue());

            assertEquals(i - from + 1, unswapedCnt.get() + onheapedCnt.get());
        }

        assertEquals(0, swappedCnt.get());

        checkEntries(cache);

        assertEquals(0, swappedCnt.get());
        assertEquals(0, offheapedCnt.get());
        assertEquals(to - from, unswapedCnt.get() + onheapedCnt.get());
    }

    /**
     * Tests.
     *
     * @throws Exception If failed.
     */
    public void testUnswapAll() throws Exception {
        GridCache<Long, Long> cache = populate();

        Collection<Long> keys = new HashSet<>();

        for (long i = from; i < to; i++)
            keys.add(i);

        cache.promoteAll(keys);

        assertEquals(0, swappedCnt.get());
        assertEquals(to - from, unswapedCnt.get() + onheapedCnt.get());

        checkEntries(cache);

        assertEquals(to - from, unswapedCnt.get() + onheapedCnt.get());
    }

    /**
     * Tests behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        GridCache<Long, Long> cache = populate();

        for (long i = from; i < to; i++) {
            Long val = cache.get(i);

            assertNotNull(val);
            assertEquals(i, val.longValue());
        }

        assertEquals(0, swappedCnt.get());
        assertEquals(0, offheapedCnt.get());
        assertEquals(to - from, unswapedCnt.get() + onheapedCnt.get());

        checkEntries(cache);

        assertEquals(to - from, unswapedCnt.get() + onheapedCnt.get());
    }

    /**
     * Tests {@link GridCacheProjection#peek(Object)} behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    public void testPeek() throws Exception {
        GridCache<Long, Long> cache = populate();

        for (long i = from; i < to; i++) {
            assertNull(cache.peek(i));

            Long val = cache.peek(i, F.asList(SWAP));

            assertNotNull(val);
            assertEquals(i, val.longValue());
        }

        assert swappedCnt.get() == 0;
        assert unswapedCnt.get() == 0;
        assert offheapedCnt.get() == 0;
        assert onheapedCnt.get() == 0;

        checkEntries(cache);
    }

    /**
     * Tests weak iterators cleanup after garbage collections.
     *
     * @throws Exception If failed.
     */
    public void testIteratorsCleanup() throws Exception {
        final GridCache<Long, Long> cache = populate();

        IgniteFuture<?> offHeapFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Iterator<Map.Entry<Long, Long>> ohIt = cache.offHeapIterator();

                    int cnt = 0;

                    while (ohIt.hasNext()) {
                        Map.Entry<Long, Long> e = ohIt.next();

                        assertEquals(e.getKey(), e.getValue());

                        cnt++;
                    }

                    assertEquals(cache.offHeapEntriesCount(), cnt);
                }
                catch (IgniteCheckedException ignored) {
                    fail();
                }
            }
        }, 20);

        IgniteFuture<?> swapFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Iterator<Map.Entry<Long, Long>> ohIt = cache.swapIterator();

                    int cnt = 0;

                    while (ohIt.hasNext()) {
                        Map.Entry<Long, Long> e = ohIt.next();

                        assertEquals(e.getKey(), e.getValue());

                        cnt++;
                    }

                    assertEquals(ENTRY_CNT - cache.offHeapEntriesCount(), cnt);
                }
                catch (IgniteCheckedException ignored) {
                    fail();
                }
            }
        }, 20);

        offHeapFut.get();
        swapFut.get();

        System.gc();

        // Runs iterator queue cleanup in GridCacheSwapManager.read method.
        cache.get(1L + ENTRY_CNT);

        assertEquals(0, ((GridKernal)grid(0)).internalCache().context().swap().iteratorSetSize());
    }
}
