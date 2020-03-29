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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Continuous queries counter tests.
 */
public abstract class CacheContinuousQueryCounterAbstractTest extends GridCommonAbstractTest
    implements Serializable {
    /** */
    protected static final String CACHE_NAME = "test_cache";

    /** Latch timeout. */
    protected static final long LATCH_TIMEOUT = 5000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(peerClassLoadingEnabled());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @NotNull private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setRebalanceMode(ASYNC);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheStoreFactory(new StoreFactory());
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        return cacheCfg;
    }

    /**
     * @return Peer class loading enabled flag.
     */
    protected boolean peerClassLoadingEnabled() {
        return true;
    }

    /**
     * @return Distribution.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    if (grid(i).cluster().nodes().size() != gridCount())
                        return false;
                }

                return true;
            }
        }, 3000);

        for (int i = 0; i < gridCount(); i++)
            grid(i).destroyCache(CACHE_NAME);

        for (int i = 0; i < gridCount(); i++)
            grid(i).getOrCreateCache(cacheConfiguration());
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Grids count.
     */
    protected abstract int gridCount();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAllEntries() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<T2<Integer, Long>>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<T2<Integer, Long>> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(new T2<>(e.getValue(), e
                            .unwrap(CacheQueryEntryEvent.class).getPartitionUpdateCounter()));
                    }

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            cache.put(1, 1);
            cache.put(2, 2);
            cache.put(3, 3);

            cache.remove(2);

            cache.put(1, 10);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(3, map.size());

            List<T2<Integer, Long>> vals = map.get(1);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(1, (int)vals.get(0).get1());
            assertEquals(1L, (long)vals.get(0).get2());
            assertEquals(10, (int)vals.get(1).get1());
            assertEquals(2L, (long)vals.get(1).get2());

            vals = map.get(2);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(2, (int)vals.get(0).get1());
            assertEquals(1L, (long)vals.get(0).get2());
            assertEquals(2, (int)vals.get(1).get1());

            vals = map.get(3);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(3, (int)vals.get(0).get1());
            assertEquals(1L, (long)vals.get(0).get2());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTwoQueryListener() throws Exception {
        if (cacheMode() == LOCAL)
            return;

        final IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE_NAME);
        final IgniteCache<Integer, Integer> cache1 = grid(1).cache(CACHE_NAME);

        final AtomicInteger cntr = new AtomicInteger(0);
        final AtomicInteger cntr1 = new AtomicInteger(0);

        final ContinuousQuery<Integer, Integer> qry1 = new ContinuousQuery<>();
        final ContinuousQuery<Integer, Integer> qry2 = new ContinuousQuery<>();

        final Map<Integer, List<T2<Integer, Long>>> map1 = new HashMap<>();
        final Map<Integer, List<T2<Integer, Long>>> map2 = new HashMap<>();

        qry1.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    cntr.incrementAndGet();

                    synchronized (map1) {
                        List<T2<Integer, Long>> vals = map1.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map1.put(e.getKey(), vals);
                        }

                        vals.add(new T2<>(e.getValue(),
                            e.unwrap(CacheQueryEntryEvent.class).getPartitionUpdateCounter()));
                    }
                }
            }
        });

        qry2.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    cntr1.incrementAndGet();

                    synchronized (map2) {
                        List<T2<Integer, Long>> vals = map2.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map2.put(e.getKey(), vals);
                        }

                        vals.add(new T2<>(e.getValue(),
                            e.unwrap(CacheQueryEntryEvent.class).getPartitionUpdateCounter()));
                    }
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> query2 = cache1.query(qry2);
            QueryCursor<Cache.Entry<Integer, Integer>> query1 = cache.query(qry1)) {
            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Object, Object> cache0 = grid(i).cache(CACHE_NAME);

                cache0.put(1, 1);
                cache0.put(2, 2);
                cache0.put(3, 3);

                cache0.remove(1);
                cache0.remove(2);
                cache0.remove(3);

                final int iter = i + 1;

                assert GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        return iter * 6 /* count operation */ * 2 /* count continues queries*/
                            == (cntr.get() + cntr1.get());
                    }
                }, 5000L);

                checkEvents(map1, i);

                map1.clear();

                checkEvents(map2, i);

                map2.clear();
            }
        }
    }

    /**
     * @param evnts Events.
     * @param iter Iteration.
     */
    private void checkEvents(Map<Integer, List<T2<Integer, Long>>> evnts, long iter) {
        List<T2<Integer, Long>> val = evnts.get(1);

        assertEquals(val.size(), 2);

        // Check put 1
        assertEquals(iter * 2 + 1, (long)val.get(0).get2());
        assertEquals(1L, (long)val.get(0).get1());

        // Check remove 1
        assertEquals(1L, (long)val.get(1).get1());
        assertEquals(iter * 2 + 2, (long)val.get(1).get2());

        val = evnts.get(2);

        assertEquals(val.size(), 2);

        // Check put 2
        assertEquals(iter * 2 + 1, (long)val.get(0).get2());
        assertEquals(2L, (long)val.get(0).get1());

        // Check remove 2
        assertEquals(2L, (long)val.get(1).get1());
        assertEquals(iter * 2 + 2, (long)val.get(1).get2());

        val = evnts.get(3);

        assertEquals(val.size(), 2);

        // Check put 3
        assertEquals(iter * 2 + 1, (long)val.get(0).get2());
        assertEquals(3L, (long)val.get(0).get1());

        // Check remove 3
        assertEquals(3L, (long)val.get(1).get1());
        assertEquals(iter * 2 + 2, (long)val.get(1).get2());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE_NAME);

        final int keyCnt = SF.applyLB(300, 50);

        final int updateKey = 1;

        for (int i = 0; i < keyCnt; i++)
            cache.put(updateKey, i);

        for (int i = 0; i < SF.applyLB(10, 4); i++) {
            if (i % 2 == 0) {
                final AtomicInteger cntr = new AtomicInteger(0);

                ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

                final List<T2<Integer, Long>> vals = new ArrayList<>();

                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
                    @Override public void onUpdated(
                        Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                        for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                            synchronized (vals) {
                                cntr.incrementAndGet();

                                vals.add(new T2<>(e.getValue(),
                                    e.unwrap(CacheQueryEntryEvent.class).getPartitionUpdateCounter()));
                            }
                        }
                    }
                });

                try (QueryCursor<Cache.Entry<Integer, Integer>> ignore = cache.query(qry)) {
                    for (int key = 0; key < keyCnt; key++)
                        cache.put(updateKey, cache.get(updateKey) + 1);

                    assert GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return cntr.get() == keyCnt;
                        }
                    }, 2000L);

                    synchronized (vals) {
                        for (T2<Integer, Long> val : vals)
                            assertEquals((long)val.get1() + 1, (long)val.get2());
                    }
                }
            }
            else {
                for (int key = 0; key < keyCnt; key++)
                    cache.put(updateKey, cache.get(updateKey) + 1);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesByFilter() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<T2<Integer, Long>>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(8);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<T2<Integer, Long>> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(new T2<>(e.getValue(),
                            e.unwrap(CacheQueryEntryEvent.class).getPartitionUpdateCounter()));
                    }

                    latch.countDown();
                }
            }
        });

        qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Integer,Integer>() {
            @Override public boolean evaluate(CacheEntryEvent<? extends Integer,? extends Integer> evt) {
                return evt.getValue() % 2 == 0;
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            cache.put(1, 1);
            cache.put(1, 2);
            cache.put(1, 3);
            cache.put(1, 4);

            cache.put(2, 1);
            cache.put(2, 2);
            cache.put(2, 3);
            cache.put(2, 4);

            cache.remove(1);
            cache.remove(2);

            cache.put(1, 10);
            cache.put(2, 40);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            List<T2<Integer, Long>> vals = map.get(1);

            assertNotNull(vals);
            assertEquals(4, vals.size());

            assertEquals((int)vals.get(0).get1(), 2);
            assertEquals((long)vals.get(0).get1(), (long)vals.get(0).get2());

            assertEquals((int)vals.get(1).get1(), 4);
            assertEquals((long)vals.get(1).get1(), (long)vals.get(1).get2());

            assertEquals(4, (long)vals.get(2).get1());
            assertEquals(5, (long)vals.get(2).get2());

            assertEquals((int)vals.get(3).get1(), 10);
            assertEquals(6, (long)vals.get(3).get2());

            vals = map.get(2);

            assertNotNull(vals);
            assertEquals(4, vals.size());

            assertEquals((int)vals.get(0).get1(), 2);
            assertEquals((long)vals.get(0).get1(), (long)vals.get(0).get2());

            assertEquals((int)vals.get(1).get1(), 4);
            assertEquals((long)vals.get(1).get1(), (long)vals.get(1).get2());

            assertEquals(4, (long)vals.get(2).get1());
            assertEquals(5, (long)vals.get(2).get2());

            assertEquals((int)vals.get(3).get1(), 40);
            assertEquals(6, (long)vals.get(3).get2());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCache() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, T2<Integer, Long>> map = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(10);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    map.put(e.getKey(), new T2<>(e.getValue(),
                        e.unwrap(CacheQueryEntryEvent.class).getPartitionUpdateCounter()));

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            cache.loadCache(null, 0);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : "Count: " + latch.getCount();

            assertEquals(10, map.size());

            for (int i = 0; i < 10; i++) {
                assertEquals(i, (int)map.get(i).get1());
                assertEquals((long)1, (long)map.get(i).get2());
            }
        }
    }

    /**
     *
     */
    private static class StoreFactory implements Factory<CacheStore> {
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     * Store.
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (int i = 0; i < 10; i++)
                clo.apply(i, i);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
