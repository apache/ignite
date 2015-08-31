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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_FROM_OFFHEAP;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_SWAPPED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TO_OFFHEAP;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNSWAPPED;

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
    private IgnitePredicate<Event> swapLsnr;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Creates a SwapSpaceSpi.
     * @return the Spi
     */
    protected SwapSpaceSpi spi() {
        return new FileSwapSpaceSpi();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        cfg.setSwapSpaceSpi(spi());

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setOffHeapMaxMemory(OFFHEAP_MEM);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setEvictSynchronizedKeyBufferSize(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setIndexedTypes(
            Long.class, Long.class
        );
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());

        cacheCfg.setEvictionPolicy(null);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDeploymentMode(SHARED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        versions.clear();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        swapLsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
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

        grid(0).events().localListen(swapLsnr,
            EVT_CACHE_OBJECT_TO_OFFHEAP, EVT_CACHE_OBJECT_FROM_OFFHEAP,
            EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).events().stopLocalListen(swapLsnr);

        grid(0).cache(null).removeAll();
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
    private IgniteCache<Long, Long> populate() throws Exception {
        IgniteCache<Long, Long> cache = grid(0).cache(null);

        assertEquals(0, cache.size());
        assertEquals(0, cache.localSize(CachePeekMode.OFFHEAP));

        assert offheapedCnt.get() == 0;
        assert onheapedCnt.get() == 0;
        assert swappedCnt.get() == 0;
        assert unswapedCnt.get() == 0;

        for (long i = 0; i < ENTRY_CNT; i++) {
            info("putting: " + i);

            cache.put(i, i);

            Long val = cache.localPeek(i);

            assert val != null;
            assert val == i;

            GridCacheEntryEx entry = dht(cache).peekEx(i);

            assert entry != null;

            versions.put(i, entry.version());
        }

        assertEquals(0, offheapedCnt.get());
        assertEquals(0, onheapedCnt.get());
        assertEquals(0, swappedCnt.get());
        assertEquals(0, unswapedCnt.get());

        assertEquals(ENTRY_CNT, cache.size());
        assertEquals(0, cache.localSize(CachePeekMode.OFFHEAP));

        for (long i = 0; i < ENTRY_CNT; i++) {
            cache.localEvict(Collections.singleton(i));

            assertEquals(ENTRY_CNT - i - 1, cache.localSize(CachePeekMode.ONHEAP));
        }

        // Ensure that part of entries located in off-heap memory and part is swapped.
        assertEquals(0, cache.localSize(CachePeekMode.ONHEAP));
        assertTrue(cache.localSize(CachePeekMode.OFFHEAP) > 0);
        assertTrue(cache.localSize(CachePeekMode.OFFHEAP) < ENTRY_CNT);

        // Setting test window to catch near half of both offheaped and swapped entries.
        from = cache.localSize(CachePeekMode.OFFHEAP) / 2;
        to = (ENTRY_CNT + cache.localSize(CachePeekMode.OFFHEAP)) / 2;

        for (long i = 0; i < ENTRY_CNT; i++)
            assertNull(cache.localPeek(i, CachePeekMode.ONHEAP));

        assertEquals(ENTRY_CNT, offheapedCnt.get());
        assertEquals(0, onheapedCnt.get());
        assertTrue(swappedCnt.get() > 0);
        assertEquals(0, unswapedCnt.get());

        resetCounters();

        return grid(0).cache(null);
    }

    /**
     * Checks that entries in cache are correct after being unswapped. If entry is still swapped, it will be unswapped
     * in this method.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void checkEntries(IgniteCache<Long, Long> cache) throws Exception {
        for (long i = from; i < to; i++) {
            cache.localPromote(Collections.singleton(i));

            GridCacheEntryEx entry = dht(cache).entryEx(i);

            assert entry != null;
            assert entry.key() != null;

            Long val = entry.rawGet().value(entry.context().cacheObjectContext(), false);

            assertNotNull("Value null for key: " + i, val);
            assertEquals(entry.key().value(entry.context().cacheObjectContext(), false), val);
            assertEquals(entry.version(), versions.get(i));
        }

        assertEquals(0, swappedCnt.get());
        assertEquals(0, offheapedCnt.get());
    }

    /** @throws Exception If failed. */
    public void testPartitionIterators() throws Exception {
        populate();

        GridCacheAdapter<Long, Object> cacheAdapter = ((IgniteKernal)grid(0)).internalCache();
        GridNearCacheAdapter<Long, Object> cache = (GridNearCacheAdapter<Long, Object>)cacheAdapter;

        Map<Integer, Collection<Long>> grouped = new HashMap<>();

        for (long i = 0; i < ENTRY_CNT; i++) {
            // Avoid entry creation.
            int part = grid(0).affinity(null).partition(i);

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

            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> it = ctx.swap().iterator(part);

            assert it != null || vals.isEmpty();

            if (it != null) {
                while (it.hasNext()) {
                    Map.Entry<byte[], GridCacheSwapEntry> swapEntry = it.next();

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
        IgniteCache<Long, Long> cache = populate();

        int cnt = 0;


        for (Cache.Entry<Long, Long> e : cache.localEntries(CachePeekMode.OFFHEAP)) {
            assertEquals(e.getKey(), e.getValue());

            cnt++;
        }

        int cnt0 = cnt;

        assertTrue(cnt > 0);

        for (Cache.Entry<Long, Long> e : cache.localEntries(CachePeekMode.SWAP)) {
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
        IgniteCache<Long, Long> cache = populate();

        Collection<Cache.Entry<Long, Long>> res = cache.query(
            new SqlQuery<Long, Long>(Long.class, "_val >= ? and _val < ?").
            setArgs(from, to)).
            getAll();

        assertEquals(to - from, res.size());

        for (Cache.Entry<Long, Long> entry : res) {
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
     * Tests {@link IgniteCache#localPromote(java.util.Set)} behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    public void testUnswap() throws Exception {
        IgniteCache<Long, Long> cache = populate();

        for (long i = from; i < to; i++) {
            cache.localPromote(Collections.singleton(i));

            Long val = cache.localPeek(i);

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
        IgniteCache<Long, Long> cache = populate();

        Set<Long> keys = new HashSet<>();

        for (long i = from; i < to; i++)
            keys.add(i);

        cache.localPromote(keys);

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
        IgniteCache<Long, Long> cache = populate();

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
     * Tests {@link IgniteCache#localPeek(Object, CachePeekMode...)} behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    public void testPeek() throws Exception {
        IgniteCache<Long, Long> cache = populate();

        for (long i = from; i < to; i++) {
            assertNull(cache.localPeek(i, CachePeekMode.ONHEAP));

            Long val = cache.localPeek(i, CachePeekMode.SWAP);

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
        final IgniteCache<Long, Long> cache = populate();

        IgniteInternalFuture<?> offHeapFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int cnt = 0;

                for (Cache.Entry<Long, Long> e : cache.localEntries(CachePeekMode.OFFHEAP)) {
                    assertEquals(e.getKey(), e.getValue());

                    cnt++;
                }

                assertEquals(cache.localSize(CachePeekMode.OFFHEAP), cnt);

            }
        }, 20);

        IgniteInternalFuture<?> swapFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int cnt = 0;

                for (Cache.Entry<Long, Long> e : cache.localEntries(CachePeekMode.SWAP)) {
                    assertEquals(e.getKey(), e.getValue());

                    cnt++;
                }

                assertEquals(ENTRY_CNT - cache.localSize(CachePeekMode.OFFHEAP), cnt);
            }
        }, 20);

        offHeapFut.get();
        swapFut.get();

        System.gc();

        // Runs iterator queue cleanup in GridCacheSwapManager.read method.
        cache.get(1L + ENTRY_CNT);

        assertEquals(0, ((IgniteKernal)grid(0)).internalCache().context().swap().iteratorSetSize());
    }
}