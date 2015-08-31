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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Multi-threaded tests for cache queries.
 */
@SuppressWarnings("StatementWithEmptyBody")
public class IgniteCacheQueryMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** Number of test grids (nodes). Should not be less than 2. */
    private static final int GRID_CNT = 3;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static AtomicInteger idxSwapCnt = new AtomicInteger();

    /** */
    private static AtomicInteger idxUnswapCnt = new AtomicInteger();

    /** */
    private static final long DURATION = 30 * 1000;

    /** Don't start grid by default. */
    public IgniteCacheQueryMultiThreadedSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        cfg.setCacheConfiguration(cacheConfiguration());

        GridQueryProcessor.idxCls = FakeIndexing.class;

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setBackups(1);

        LruEvictionPolicy plc = null;

        if (evictsEnabled()) {
            plc = new LruEvictionPolicy();
            plc.setMaxSize(100);
        }

        cacheCfg.setEvictionPolicy(plc);

        cacheCfg.setSqlOnheapRowCacheSize(128);
        cacheCfg.setIndexedTypes(
            Integer.class, Integer.class,
            Integer.class, TestValue.class,
            Integer.class, String.class,
            Integer.class, Long.class,
            Integer.class, Object.class
        );

        if (offheapEnabled())
            cacheCfg.setOffHeapMaxMemory(evictsEnabled() ? 1000 : 0); // Small offheap for evictions.

        return cacheCfg;
    }

    /**
     *
     */
    private static class FakeIndexing extends IgniteH2Indexing {
        @Override public void onSwap(@Nullable String spaceName, CacheObject key) throws IgniteCheckedException {
            super.onSwap(spaceName, key);

            idxSwapCnt.incrementAndGet();
        }

        @Override public void onUnswap(@Nullable String spaceName, CacheObject key, CacheObject val)
        throws IgniteCheckedException {
            super.onUnswap(spaceName, key, val);

            idxUnswapCnt.incrementAndGet();
        }
    }

    /** @return {@code true} If offheap enabled. */
    protected boolean offheapEnabled() {
        return false;
    }

    /** @return {@code true} If evictions enabled. */
    protected boolean evictsEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Clean up all caches.
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Object, Object> c = grid(i).cache(null);

            assertEquals(0, c.size());
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assert GRID_CNT >= 2 : "Constant GRID_CNT must be greater than or equal to 2.";

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        if (evictsEnabled()) {
            assertTrue(idxSwapCnt.get() > 0);
            assertTrue(idxUnswapCnt.get() > 0);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Clean up all caches.
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Object, Object> c = grid(i).cache(null);

            c.removeAll();

            // Fix for tests where mapping was removed at primary node
            // but was not removed at others.
            // removeAll() removes mapping only when it presents at a primary node.
            // To remove all mappings used force remove by key.
            if (c.size() > 0) {
                for (Cache.Entry<Object, Object> e : c.localEntries())
                    c.remove(e.getKey());
            }

            U.sleep(5000);

            assertEquals("Swap keys: " + c.size(CachePeekMode.SWAP), 0, c.size(CachePeekMode.SWAP));
            assertEquals(0, c.size(CachePeekMode.OFFHEAP));
            assertEquals(0, c.size(CachePeekMode.PRIMARY));
            assertEquals(0, c.size());
        }
    }

    /** {@inheritDoc} */
    @Override protected void info(String msg) {
        if (TEST_INFO)
            super.info(msg);
    }

    /**
     * @param entries Entries.
     * @param g Grid.
     * @return Affinity nodes.
     */
    private Set<UUID> affinityNodes(Iterable<Cache.Entry<Integer, Integer>> entries, Ignite g) {
        Set<UUID> nodes = new HashSet<>();

        for (Cache.Entry<Integer, Integer> entry : entries)
            nodes.add(g.affinity(null).mapKeyToPrimaryAndBackups(entry.getKey()).iterator().next().id());

        return nodes;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedSwapUnswapString() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 2000;
        final int valCnt = 10000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, String> c = g.cache(null);
        final IgniteCache<Integer, Long> cl = g.cache(null);

        if (c.getConfiguration(CacheConfiguration.class).getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
            return;

        assertEquals(0, g.cache(null).localSize());
        assertEquals(0, c.query(new SqlQuery(String.class, "1 = 1")).getAll().size());
        assertEquals(0, cl.query(new SqlQuery(Long.class, "1 = 1")).getAll().size());

        Random rnd = new Random();

        for (int i = 0; i < keyCnt; i += 1 + rnd.nextInt(3)) {
            c.put(i, String.valueOf(rnd.nextInt(valCnt)));

            if (evictsEnabled() && rnd.nextBoolean())
                c.localEvict(Arrays.asList(i));
        }

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                Random rnd = new Random();

                while (!done.get()) {
                    switch (rnd.nextInt(5)) {
                        case 0:
                            c.put(rnd.nextInt(keyCnt), String.valueOf(rnd.nextInt(valCnt)));

                            break;
                        case 1:
                            if (evictsEnabled())
                                c.localEvict(Arrays.asList(rnd.nextInt(keyCnt)));

                            break;
                        case 2:
                            c.remove(rnd.nextInt(keyCnt));

                            break;
                        case 3:
                            c.get(rnd.nextInt(keyCnt));

                            break;
                        case 4:
                            int from = rnd.nextInt(valCnt);

                            c.query(new SqlQuery(String.class, "_val between ? and ?").setArgs(
                                    String.valueOf(from), String.valueOf(from + 250))).getAll();
                    }
                }
            }
        }, threadCnt);

        Thread.sleep(DURATION);

        done.set(true);

        fut.get();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedSwapUnswapLong() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 2000;
        final int valCnt = 10000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, Long> c = g.cache(null);
        final IgniteCache<Integer, String> c1 = g.cache(null);

        if (c.getConfiguration(CacheConfiguration.class).getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
            return;

        assertEquals(0, g.cache(null).localSize());
        assertEquals(0, c1.query(new SqlQuery(String.class, "1 = 1")).getAll().size());
        assertEquals(0, c.query(new SqlQuery(Long.class, "1 = 1")).getAll().size());

        Random rnd = new Random();

        for (int i = 0; i < keyCnt; i += 1 + rnd.nextInt(3)) {
            c.put(i, (long)rnd.nextInt(valCnt));

            if (evictsEnabled() && rnd.nextBoolean())
                c.localEvict(Arrays.asList(i));
        }

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                Random rnd = new Random();

                while (!done.get()) {
                    int key = rnd.nextInt(keyCnt);

                    switch (rnd.nextInt(5)) {
                        case 0:
                            c.put(key, (long)rnd.nextInt(valCnt));

                            break;
                        case 1:
                            if (evictsEnabled())
                                c.localEvict(Arrays.asList(key));

                            break;
                        case 2:
                            c.remove(key);

                            break;
                        case 3:
                            c.get(key);

                            break;
                        case 4:
                            int from = rnd.nextInt(valCnt);

                            c.query(new SqlQuery(Long.class, "_val between ? and ?").setArgs(from, from + 250)).getAll();
                    }
                }
            }
        }, threadCnt);

        Thread.sleep(DURATION);

        done.set(true);

        fut.get();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedSwapUnswapLongString() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 2000;
        final int valCnt = 10000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, Object> c = g.cache(null);

        if (c.getConfiguration(CacheConfiguration.class).getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
            return;

        assertEquals(0, g.cache(null).size());
        assertEquals(0, c.query(new SqlQuery(Object.class, "1 = 1")).getAll().size());

        Random rnd = new Random();

        for (int i = 0; i < keyCnt; i += 1 + rnd.nextInt(3)) {
            c.put(i, rnd.nextBoolean() ? (long)rnd.nextInt(valCnt) : String.valueOf(rnd.nextInt(valCnt)));

            if (evictsEnabled() && rnd.nextBoolean())
                c.localEvict(Arrays.asList(i));
        }

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                Random rnd = new Random();

                while (!done.get()) {
                    int key = rnd.nextInt(keyCnt);

                    switch (rnd.nextInt(5)) {
                        case 0:
                            c.put(key, rnd.nextBoolean() ? (long)rnd.nextInt(valCnt) :
                                String.valueOf(rnd.nextInt(valCnt)));

                            break;
                        case 1:
                            if (evictsEnabled())
                                c.localEvict(Arrays.asList(key));

                            break;
                        case 2:
                            c.remove(key);

                            break;
                        case 3:
                            c.get(key);

                            break;
                        case 4:
                            int from = rnd.nextInt(valCnt);

                            c.query(new SqlQuery(Object.class, "_val between ? and ?").setArgs(from, from + 250))
                                .getAll();
                    }
                }
            }
        }, threadCnt);

        Thread.sleep(DURATION);

        done.set(true);

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedSwapUnswapObject() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 4000;
        final int valCnt = 10000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, TestValue> c = g.cache(null);

        if (c.getConfiguration(CacheConfiguration.class).getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
            return;

        assertEquals(0, g.cache(null).localSize());
        assertEquals(0, c.query(new SqlQuery(TestValue.class, "1 = 1")).getAll().size());

        Random rnd = new Random();

        for (int i = 0; i < keyCnt; i += 1 + rnd.nextInt(3)) {
            c.put(i, new TestValue(rnd.nextInt(valCnt)));

            if (evictsEnabled() && rnd.nextBoolean())
                c.localEvict(Arrays.asList(i));
        }

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                Random rnd = new Random();

                while (!done.get()) {
                    int key = rnd.nextInt(keyCnt);

                    switch (rnd.nextInt(5)) {
                        case 0:
                            c.put(key, new TestValue(rnd.nextInt(valCnt)));

                            break;
                        case 1:
                            if (evictsEnabled())
                                c.localEvict(Arrays.asList(key));

                            break;
                        case 2:
                            c.remove(key);

                            break;
                        case 3:
                            c.get(key);

                            break;
                        case 4:
                            int from = rnd.nextInt(valCnt);

                            c.query(new SqlQuery(TestValue.class, "TestValue.val between ? and ?")
                                .setArgs(from, from + 250)).getAll();
                    }
                }
            }
        }, threadCnt);

        Thread.sleep(DURATION);

        done.set(true);

        fut.get();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedSameQuery() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 10;
        final int logMod = 5000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, Integer> c = g.cache(null);

        for (int i = 0; i < keyCnt; i++) {
            c.put(i, i);

            c.localEvict(Arrays.asList(i));
        }

        final AtomicInteger cnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    int iter = 0;

                    while (!done.get() && !Thread.currentThread().isInterrupted()) {
                        iter++;

                        Collection<Cache.Entry<Integer, Integer>> entries =
                            c.query(new SqlQuery(Integer.class, "_val >= 0")).getAll();

                        assert entries != null;

                        assertEquals("Query results [entries=" + entries + ", aff=" + affinityNodes(entries, g) +
                            ", iteration=" + iter + ']', keyCnt, entries.size());

                        if (cnt.incrementAndGet() % logMod == 0) {
                            GridCacheQueryManager<Object, Object> qryMgr =
                                ((IgniteKernal)g).internalCache().context().queries();

                            assert qryMgr != null;

                            qryMgr.printMemoryStats();
                        }
                    }
                }
            }, threadCnt);

        Thread.sleep(DURATION);

        info("Finishing test...");

        done.set(true);

        fut.get();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedNewQueries() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 10;
        final int logMod = 5000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, Integer> c = g.cache(null);

        for (int i = 0; i < keyCnt; i++) {
            c.put(i, i);

            c.localEvict(Arrays.asList(i));
        }

        final AtomicInteger cnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                int iter = 0;

                while (!done.get() && !Thread.currentThread().isInterrupted()) {
                    iter++;

                    Collection<Cache.Entry<Integer, Integer>> entries =
                        c.query(new SqlQuery(Integer.class, "_val >= 0")).getAll();

                    assert entries != null;

                    assertEquals("Entries count is not as expected on iteration: " + iter, keyCnt, entries.size());

                    if (cnt.incrementAndGet() % logMod == 0) {
                        GridCacheQueryManager<Object, Object> qryMgr =
                            ((IgniteKernal)g).internalCache().context().queries();

                        assert qryMgr != null;

                        qryMgr.printMemoryStats();
                    }
                }
            }
        }, threadCnt);

        Thread.sleep(DURATION);

        done.set(true);

        fut.get();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedScanQuery() throws Exception {
        int threadCnt = 50;
        final int keyCnt = 500;
        final int logMod = 5000;

        final Ignite g = grid(0);

        // Put test values into cache.
        final IgniteCache<Integer, Integer> c = g.cache(null);

        for (int i = 0; i < keyCnt; i++)
            c.put(i, i);

        final AtomicInteger cnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    int iter = 0;

                    while (!done.get() && !Thread.currentThread().isInterrupted()) {
                        iter++;

                        // Scan query.
                        Collection<Cache.Entry<Integer, Integer>> entries =
                            c.query(new ScanQuery<Integer, Integer>()).getAll();

                        assert entries != null;

                        assertEquals("Entries count is not as expected on iteration: " + iter, keyCnt, entries.size());

                        if (cnt.incrementAndGet() % logMod == 0) {
                            GridCacheQueryManager<Object, Object> qryMgr =
                                ((IgniteKernal)g).internalCache().context().queries();

                            assert qryMgr != null;

                            qryMgr.printMemoryStats();
                        }
                    }
                }
            }, threadCnt);

        Thread.sleep(DURATION);

        done.set(true);

        fut.get();
    }

    /**
     * Test value.
     */
    private static class TestValue implements Serializable {
        /** Value. */
        @QuerySqlField(index = true)
        private int val;

        /**
         * @param val Value.
         */
        private TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }
    }
}