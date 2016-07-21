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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.query.CacheQueryType.CONTINUOUS;

/**
 * Continuous queries tests.
 */
public abstract class GridCacheContinuousQueryAbstractSelfTest extends GridCommonAbstractTest implements Serializable {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Latch timeout. */
    protected static final long LATCH_TIMEOUT = 5000;

    /** */
    private static final String NO_CACHE_GRID_NAME = "noCacheGrid";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(peerClassLoadingEnabled());

        if (!gridName.equals(NO_CACHE_GRID_NAME)) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(cacheMode());
            cacheCfg.setAtomicityMode(atomicityMode());
            cacheCfg.setNearConfiguration(nearConfiguration());
            cacheCfg.setRebalanceMode(ASYNC);
            cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            cacheCfg.setCacheStoreFactory(new StoreFactory());
            cacheCfg.setReadThrough(true);
            cacheCfg.setWriteThrough(true);
            cacheCfg.setLoadPreviousValue(true);
            cacheCfg.setMemoryMode(memoryMode());

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            cfg.setClientMode(true);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /**
     * @return Cache memory mode.
     */
    protected CacheMemoryMode memoryMode() {
        return ONHEAP_TIERED;
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
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
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
            assertEquals(gridCount(), grid(i).cluster().nodes().size());

        for (int i = 0; i < gridCount(); i++) {
            for (int j = 0; j < 5; j++) {
                try {
                    IgniteCache<Object, Object> cache = grid(i).cache(null);

                    for (Cache.Entry<Object, Object> entry : cache.localEntries(new CachePeekMode[] {CachePeekMode.ALL}))
                        cache.remove(entry.getKey());

                    break;
                }
                catch (IgniteException e) {
                    if (j == 4)
                        throw new Exception("Failed to clear cache for grid: " + i, e);

                    U.warn(log, "Failed to clear cache for grid (will retry in 500 ms) [gridIdx=" + i +
                        ", err=" + e.getMessage() + ']');

                    U.sleep(500);
                }
            }
        }

        for (int i = 0; i < gridCount(); i++) {
            GridContinuousProcessor proc = grid(i).context().continuous();

            assertEquals(String.valueOf(i), 2, ((Map)U.field(proc, "locInfos")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "rmtInfos")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "startFuts")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "stopFuts")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "bufCheckThreads")).size());

            CacheContinuousQueryManager mgr = grid(i).context().cache().internalCache().context().continuousQueries();

            assertEquals(0, ((Map)U.field(mgr, "lsnrs")).size());
        }
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
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testIllegalArguments() throws Exception {
        final ContinuousQuery<Object, Object> q = new ContinuousQuery<>();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.setPageSize(-1);

                    return null;
                }
            },
            IllegalArgumentException.class,
            null
        );

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.setPageSize(0);

                    return null;
                }
            }, IllegalArgumentException.class, null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.setTimeInterval(-1);

                    return null;
                }
            },
            IllegalArgumentException.class,
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllEntries() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
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

            List<Integer> vals = map.get(1);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(1, (int)vals.get(0));
            assertEquals(10, (int)vals.get(1));

            vals = map.get(2);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(2, (int)vals.get(0));
            assertNull(vals.get(1));

            vals = map.get(3);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(3, (int)vals.get(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilterException() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                // No-op.
            }
        });

        qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Integer, Integer>() {
            @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
                throw new RuntimeException("Test error.");
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            for (int i = 0; i < 100; i++)
                cache.put(i, i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoQueryListener() throws Exception {
        if (cacheMode() == LOCAL)
            return;

        IgniteCache<Integer, Integer> cache = grid(0).cache(null);
        IgniteCache<Integer, Integer> cache1 = grid(1).cache(null);

        final AtomicInteger cntr = new AtomicInteger(0);
        final AtomicInteger cntr1 = new AtomicInteger(0);

        ContinuousQuery<Integer, Integer> qry1 = new ContinuousQuery<>();
        ContinuousQuery<Integer, Integer> qry2 = new ContinuousQuery<>();

        qry1.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> ignore : evts)
                    cntr.incrementAndGet();
            }
        });

        qry2.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> ignore : evts)
                    cntr1.incrementAndGet();
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> qryCur2 = cache1.query(qry2);
             QueryCursor<Cache.Entry<Integer, Integer>> qryCur1 = cache.query(qry1)) {
            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Object, Object> cache0 = grid(i).cache(null);

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
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartQuery() throws Exception {
        if (cacheMode() == LOCAL)
            return;

        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        final int parts = grid(0).affinity(null).partitions();

        final int keyCnt = parts * 2;

        for (int i = 0; i < parts / 2; i++)
            cache.put(i, i);

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                final AtomicInteger cntr = new AtomicInteger(0);

                ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
                    @Override public void onUpdated(
                        Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                        for (CacheEntryEvent<? extends Integer, ? extends Integer> ignore : evts)
                            cntr.incrementAndGet();
                    }
                });

                QueryCursor<Cache.Entry<Integer, Integer>> qryCur = cache.query(qry);

                for (int key = 0; key < keyCnt; key++)
                    cache.put(key, key);

                try {
                    assert GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return cntr.get() == keyCnt;
                        }
                    }, 2000L);
                }
                finally {
                    qryCur.close();
                }
            }
            else {
                for (int key = 0; key < keyCnt; key++)
                    cache.put(key, key);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesByFilter() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(4);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }
            }
        });

        qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Integer,Integer>() {
            @Override public boolean evaluate(CacheEntryEvent<? extends Integer,? extends Integer> evt) {
                return evt.getKey() > 2;
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            cache.put(1, 1);
            cache.put(2, 2);
            cache.put(3, 3);
            cache.put(4, 4);

            cache.remove(2);
            cache.remove(3);

            cache.put(1, 10);
            cache.put(4, 40);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            List<Integer> vals = map.get(3);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(3, (int)vals.get(0));
            assertNull(vals.get(1));

            vals = map.get(4);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(4, (int)vals.get(0));
            assertEquals(40, (int)vals.get(1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalNodeOnly() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        if (grid(0).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode() != PARTITIONED)
            return;

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer,Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer,? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer,? extends Integer> e : evts) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry.setLocal(true))) {
            int locKey = -1;
            int rmtKey = -1;

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).cluster().mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(grid(0).localNode()))
                    locKey = key;
                else
                    rmtKey = key;

                key++;

                if (locKey >= 0 && rmtKey >= 0)
                    break;
            }

            cache.put(locKey, 1);
            cache.put(rmtKey, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(1, map.size());

            List<Integer> vals = map.get(locKey);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(1, (int)vals.get(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBuffering() throws Exception {
        if (grid(0).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode() != PARTITIONED)
            return;

        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }
            }
        });

        qry.setPageSize(5);

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            ClusterNode node = F.first(grid(0).cluster().forRemotes().nodes());

            Collection<Integer> keys = new HashSet<>();

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).cluster().mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(node))
                    keys.add(key);

                key++;

                if (keys.size() == 6)
                    break;
            }

            Iterator<Integer> it = keys.iterator();

            for (int i = 0; i < 4; i++)
                cache.put(it.next(), 0);

            assert !latch.await(2, SECONDS);

            for (int i = 0; i < 2; i++)
                cache.put(it.next(), 0);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(5, map.size());

            it = keys.iterator();

            for (int i = 0; i < 5; i++) {
                Integer k = it.next();

                List<Integer> vals = map.get(k);

                assertNotNull(vals);
                assertEquals(1, vals.size());
                assertEquals(0, (int)vals.get(0));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimeInterval() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() != PARTITIONED)
            return;

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }
            }
        });

        qry.setPageSize(10);
        qry.setTimeInterval(3000);

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            ClusterNode node = F.first(grid(0).cluster().forRemotes().nodes());

            Collection<Integer> keys = new HashSet<>();

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).cluster().mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(node))
                    keys.add(key);

                key++;

                if (keys.size() == 5)
                    break;
            }

            for (Integer k : keys)
                cache.put(k, 0);

            assert !latch.await(2, SECONDS);
            assert latch.await(1000 + LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(5, map.size());

            Iterator<Integer> it = keys.iterator();

            for (int i = 0; i < 5; i++) {
                Integer k = it.next();

                List<Integer> vals = map.get(k);

                assertNotNull(vals);
                assertEquals(1, vals.size());
                assertEquals(0, (int)vals.get(0));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInitialQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new P2<Integer, Integer>() {
            @Override public boolean apply(Integer k, Integer v) {
                return k >= 5;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                assert false;
            }
        });

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(qry)) {
            List<Cache.Entry<Integer, Integer>> res = cur.getAll();

            Collections.sort(res, new Comparator<Cache.Entry<Integer, Integer>>() {
                @Override public int compare(Cache.Entry<Integer, Integer> e1, Cache.Entry<Integer, Integer> e2) {
                    return e1.getKey().compareTo(e2.getKey());
                }
            });

            assertEquals(5, res.size());

            int exp = 5;

            for (Cache.Entry<Integer, Integer> e : res) {
                assertEquals(exp, e.getKey().intValue());
                assertEquals(exp, e.getValue().intValue());

                exp++;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInitialQueryAndUpdates() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new P2<Integer, Integer>() {
            @Override public boolean apply(Integer k, Integer v) {
                return k >= 5;
            }
        }));

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }
            }
        });

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(qry)) {
            List<Cache.Entry<Integer, Integer>> res = cur.getAll();

            Collections.sort(res, new Comparator<Cache.Entry<Integer, Integer>>() {
                @Override public int compare(Cache.Entry<Integer, Integer> e1, Cache.Entry<Integer, Integer> e2) {
                    return e1.getKey().compareTo(e2.getKey());
                }
            });

            assertEquals(5, res.size());

            int exp = 5;

            for (Cache.Entry<Integer, Integer> e : res) {
                assertEquals(exp, e.getKey().intValue());
                assertEquals(exp, e.getValue().intValue());

                exp++;
            }

            cache.put(10, 10);
            cache.put(11, 11);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : latch.getCount();

            assertEquals(2, map.size());

            for (int i = 11; i < 12; i++)
                assertEquals(i, (int)map.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            cache.loadCache(null, 0);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : "Count: " + latch.getCount();

            assertEquals(10, map.size());

            for (int i = 0; i < 10; i++)
                assertEquals(i, (int)map.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInternalKey() throws Exception {
        if (atomicityMode() == ATOMIC)
            return;

        IgniteCache<Object, Object> cache = grid(0).cache(null);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        final Map<Object, Object> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                for (CacheEntryEvent<?, ?> e : evts) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Object, Object>> ignored = cache.query(qry)) {
            cache.put(new GridCacheInternalKeyImpl("test"), 1);

            cache.put(1, 1);
            cache.put(2, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            assertEquals(1, (int)map.get(1));
            assertEquals(2, (int)map.get(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("TryFinallyCanBeTryWithResources")
    public void testNodeJoinWithoutCache() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final CountDownLatch latch = new CountDownLatch(1);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                latch.countDown();
            }
        });

        QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(qry);

        try {
            try (Ignite ignite = startGrid(NO_CACHE_GRID_NAME)) {
                log.info("Started node without cache: " + ignite);
            }

            cache.put(1, 1);

            assertTrue(latch.await(5000, MILLISECONDS));
        }
        finally {
            cur.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvents() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(50);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof CacheQueryReadEvent;

                CacheQueryReadEvent qe = (CacheQueryReadEvent)evt;

                assertEquals(CONTINUOUS.name(), qe.queryType());
                assertNull(qe.cacheName());

                assertEquals(grid(0).localNode().id(), qe.subjectId());

                assertNull(qe.className());
                assertNull(qe.clause());
                assertNull(qe.scanQueryFilter());
                assertNotNull(qe.continuousQueryFilter());
                assertNull(qe.arguments());

                cnt.incrementAndGet();
                latch.countDown();

                return true;
            }
        };

        IgnitePredicate<Event> execLsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof CacheQueryExecutedEvent;

                CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                assertEquals(CONTINUOUS.name(), qe.queryType());
                assertNull(qe.cacheName());

                assertEquals(grid(0).localNode().id(), qe.subjectId());

                assertNull(qe.className());
                assertNull(qe.clause());
                assertNull(qe.scanQueryFilter());
                assertNotNull(qe.continuousQueryFilter());
                assertNull(qe.arguments());

                execLatch.countDown();

                return true;
            }
        };

        try {
            for (int i = 0; i < gridCount(); i++) {
                grid(i).events().localListen(lsnr, EVT_CACHE_QUERY_OBJECT_READ);
                grid(i).events().localListen(execLsnr, EVT_CACHE_QUERY_EXECUTED);
            }

            IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

            qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                    // No-op.
                }
            });

            qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Integer, Integer>() {
                @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
                    return evt.getValue() >= 50;
                }
            });

            try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
                for (int i = 0; i < 100; i++)
                    cache.put(i, i);

                assert latch.await(LATCH_TIMEOUT, MILLISECONDS);
                assert execLatch.await(LATCH_TIMEOUT, MILLISECONDS);

                assertEquals(50, cnt.get());
            }
        }
        finally {
            for (int i = 0; i < gridCount(); i++) {
                grid(i).events().stopLocalListen(lsnr, EVT_CACHE_QUERY_OBJECT_READ);
                grid(i).events().stopLocalListen(execLsnr, EVT_CACHE_QUERY_EXECUTED);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpired() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(null).
            withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, 1000)));

        final Map<Object, Object> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setIncludeExpired(true);

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                for (CacheEntryEvent<?, ?> e : evts) {
                    if (e.getEventType() == EventType.EXPIRED) {
                        assertNull(e.getValue());

                        map.put(e.getKey(), e.getOldValue());

                        latch.countDown();
                    }
                }
            }
        });

        try (QueryCursor<Cache.Entry<Object, Object>> ignored = cache.query(qry)) {
            cache.put(1, 1);
            cache.put(2, 2);

            // Wait for expiration.
            Thread.sleep(2000);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            assertEquals(1, (int)map.get(1));
            assertEquals(2, (int)map.get(2));
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
        @Override public void write(javax.cache.Cache.Entry<?, ?> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
