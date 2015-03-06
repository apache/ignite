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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.event.*;
import javax.cache.integration.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.query.CacheQueryType.*;

/**
 * Continuous queries tests.
 */
public abstract class GridCacheContinuousQueryAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Latch timeout. */
    protected static final long LATCH_TIMEOUT = 5000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(peerClassLoadingEnabled());

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setDistributionMode(distributionMode());
        cacheCfg.setRebalanceMode(ASYNC);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new TestStore()));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);
        cacheCfg.setQueryIndexEnabled(false);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new OptimizedMarshaller(false));

        return cfg;
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
    protected CacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
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
                    ((IgniteKernal)grid(i)).cache(null).removeAll();

                    break;
                }
                catch (CachePartialUpdateCheckedException e) {
                    if (j == 4)
                        throw new Exception("Failed to clear cache for grid: " + i, e);

                    U.warn(log, "Failed to clear cache for grid (will retry in 500 ms) [gridIdx=" + i +
                        ", err=" + e.getMessage() + ']');

                    U.sleep(500);
                }
            }
        }

        for (int i = 0; i < gridCount(); i++)
            assertEquals("Cache is not empty: " + ((IgniteKernal)grid(i)).cache(null).entrySet(), 0,
                ((IgniteKernal)grid(i)).cache(null).size());


        for (int i = 0; i < gridCount(); i++) {
            GridContinuousProcessor proc = ((IgniteKernal)grid(i)).context().continuous();

            assertEquals(String.valueOf(i), 2, ((Map)U.field(proc, "locInfos")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "rmtInfos")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "startFuts")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "waitForStartAck")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "stopFuts")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "waitForStopAck")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "pending")).size());

            CacheContinuousQueryManager mgr =
                ((IgniteKernal)grid(i)).context().cache().internalCache().context().continuousQueries();

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
        final ContinuousQuery<Object, Object> q = Query.continuous();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.setBufferSize(-1);

                    return null;
                }
            },
            IllegalArgumentException.class,
            null
        );

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.setBufferSize(0);

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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

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
    public void testEntriesByFilter() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

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

        qry.setRemoteFilter(new CacheEntryEventFilter<Integer, Integer>() {
            @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        if (((IgniteKernal)grid(0)).cache(null).configuration().getCacheMode() != PARTITIONED)
            return;

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(1);

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

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.localQuery(qry)) {
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
        if (((IgniteKernal)grid(0)).cache(null).configuration().getCacheMode() != PARTITIONED)
            return;

        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

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

        qry.setBufferSize(5);

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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() != PARTITIONED)
            return;

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

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

        qry.setBufferSize(10);
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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

        qry.setInitialQuery(Query.scan(new P2<Integer, Integer>() {
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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

        qry.setInitialQuery(Query.scan(new P2<Integer, Integer>() {
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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

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

        IgniteCache<Object, Object> cache = grid(0).jcache(null);

        ContinuousQuery<Object, Object> qry = Query.continuous();

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
    public void testNodeJoin() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        ContinuousQuery<Integer, Integer> qry = Query.continuous();

        final Collection<CacheEntryEvent<? extends Integer, ? extends Integer>> all = new ConcurrentLinkedDeque8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                int size = 0;

                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                    all.add(evt);

                    size++;
                }

                assertEquals(1, size);

                latch.countDown();
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
            cache.put(1, 1);

            startGrid("anotherGrid");

            cache.put(2, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : all;

            assertEquals(2, all.size());
        }
        finally {
            stopGrid("anotherGrid");
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

                assertEquals(CONTINUOUS, qe.queryType());
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

                assertEquals(CONTINUOUS, qe.queryType());
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

            IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

            ContinuousQuery<Integer, Integer> qry = Query.continuous();

            qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                    // No-op.
                }
            });

            qry.setRemoteFilter(new CacheEntryEventFilter<Integer, Integer>() {
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
