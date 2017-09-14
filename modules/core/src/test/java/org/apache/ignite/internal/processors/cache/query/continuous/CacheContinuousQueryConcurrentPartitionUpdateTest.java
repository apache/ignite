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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryConcurrentPartitionUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private AtomicInteger cntr = new AtomicInteger(0);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR,
            cntr.getAndIncrement()));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdatePartitionAtomic() throws Exception {
        concurrentUpdatePartition(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdatePartitionTx() throws Exception {
        concurrentUpdatePartition(TRANSACTIONAL, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdatePartitionAtomicCacheGroup() throws Exception {
        concurrentUpdatePartition(ATOMIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdatePartitionTxCacheGroup() throws Exception {
        concurrentUpdatePartition(TRANSACTIONAL, true);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheGrp {@code True} if test cache multiple caches in the same group.
     * @throws Exception If failed.
     */
    private void concurrentUpdatePartition(CacheAtomicityMode atomicityMode, boolean cacheGrp) throws Exception {
        Ignite srv = startGrid(0);

        client = true;

        Ignite client = startGrid(1);

        List<AtomicInteger> cntrs = new ArrayList<>();
        List<String> caches = new ArrayList<>();

        if (cacheGrp) {
            for (int i = 0; i < 3; i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + i);

                ccfg.setGroupName("testGroup");
                ccfg.setWriteSynchronizationMode(FULL_SYNC);
                ccfg.setAtomicityMode(atomicityMode);

                IgniteCache<Object, Object> cache = client.createCache(ccfg);

                caches.add(cache.getName());

                cntrs.add(startListener(cache).get1());
            }
        }
        else {
            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(atomicityMode);

            IgniteCache<Object, Object> cache = client.createCache(ccfg);

            caches.add(cache.getName());

            cntrs.add(startListener(cache).get1());
        }

        Affinity<Integer> aff = srv.affinity(caches.get(0));

        final List<Integer> keys = new ArrayList<>();

        final int KEYS = 10;

        for (int i = 0; i < 100_000; i++) {
            if (aff.partition(i) == 0) {
                keys.add(i);

                if (keys.size() == KEYS)
                    break;
            }
        }

        assertEquals(KEYS, keys.size());

        final int THREADS = 10;
        final int UPDATES = 1000;

        final List<IgniteCache<Object, Object>> srvCaches = new ArrayList<>();

        for (String cacheName : caches)
            srvCaches.add(srv.cache(cacheName));

        for (int i = 0; i < 15; i++) {
            log.info("Iteration: " + i);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < UPDATES; i++) {
                        for (int c = 0; c < srvCaches.size(); c++)
                            srvCaches.get(c).put(keys.get(rnd.nextInt(KEYS)), i);
                    }

                    return null;
                }
            }, THREADS, "update");

            for (final AtomicInteger evtCnt : cntrs) {
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        log.info("Events: " + evtCnt.get());

                        return evtCnt.get() >= THREADS * UPDATES;
                    }
                }, 5000);

                assertEquals(THREADS * UPDATES, evtCnt.get());

                evtCnt.set(0);
            }
        }
    }

    /**
     * @param cache Cache.
     * @return Event counter.
     */
    private T2<AtomicInteger, QueryCursor> startListener(IgniteCache<Object, Object> cache) {
        final AtomicInteger evtCnt = new AtomicInteger();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                for (CacheEntryEvent evt : evts) {
                    assertNotNull(evt.getKey());
                    assertNotNull(evt.getValue());

                    if ((Integer)evt.getValue() >= 0)
                        evtCnt.incrementAndGet();
                }
            }
        });

        QueryCursor cur = cache.query(qry);

        return new T2<>(evtCnt, cur);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdatesAndQueryStartAtomic() throws Exception {
        concurrentUpdatesAndQueryStart(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdatesAndQueryStartTx() throws Exception {
        concurrentUpdatesAndQueryStart(TRANSACTIONAL, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testConcurrentUpdatesAndQueryStartAtomicCacheGroup() throws Exception {
        concurrentUpdatesAndQueryStart(ATOMIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testConcurrentUpdatesAndQueryStartTxCacheGroup() throws Exception {
        concurrentUpdatesAndQueryStart(TRANSACTIONAL, true);
    }

    public void test1() throws Exception {
        Ignite srv = startGrid(0);

        client = true;

        Ignite client = startGrid(1);

        List<String> caches = new ArrayList<>();

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(ATOMIC);

        ccfg.setAffinity(new GridCacheModuloAffinityFunction(1, ccfg.getBackups()));

        IgniteCache clientCache = client.createCache(ccfg);
        IgniteCache serverCache = srv.cache(DEFAULT_CACHE_NAME);

        Affinity<Integer> aff = srv.affinity(DEFAULT_CACHE_NAME);

        final List<Integer> keys = new ArrayList<>();

        assert aff.partition(1) == 0;
        assert aff.partition(2) == 0;

        multithreadedAsync(new Runnable() {
            @Override public void run() {

                serverCache.put(1, 1);
            }
        }, 1);

        Thread.sleep(1000);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();
        Set<Integer> keysReceived = new HashSet<>();
        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                for (CacheEntryEvent evt : evts)
                    keysReceived.add((Integer)evt.getKey());
            }
        });

        clientCache.query(qry);

        serverCache.put(2, 1);

        serverCache.put(3, 1);

        Thread.sleep(1000);

        for (Integer integer : keysReceived)
            System.out.println("Lister received key: " + integer + " ");
    }

    private static class TestInterceptor extends CacheInterceptorAdapter {
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            return super.onBeforePut(entry, newVal);
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheGrp {@code True} if test cache multiple caches in the same group.
     * @throws Exception If failed.
     */
    private void concurrentUpdatesAndQueryStart(CacheAtomicityMode atomicityMode, boolean cacheGrp) throws Exception {
        Ignite srv = startGrid(0);

        client = true;

        Ignite client = startGrid(1);

        List<String> caches = new ArrayList<>();

        if (cacheGrp) {
            for (int i = 0; i < 3; i++) {
                CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME + i);

                ccfg.setGroupName("testGroup");
                ccfg.setWriteSynchronizationMode(FULL_SYNC);
                ccfg.setAtomicityMode(atomicityMode);

                IgniteCache cache = client.createCache(ccfg);

                caches.add(cache.getName());
            }
        }
        else {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(atomicityMode);

            IgniteCache cache = client.createCache(ccfg);

            caches.add(cache.getName());
        }

        Affinity<Integer> aff = srv.affinity(caches.get(0));

        final List<Integer> keys = new ArrayList<>();

        final int KEYS = 10;

        for (int i = 0; i < 100_000; i++) {
            if (aff.partition(i) == 0) {
                keys.add(i);

                if (keys.size() == KEYS)
                    break;
            }
        }

        assertEquals(KEYS, keys.size());

        final int THREADS = 10;
        final int UPDATES = 1000;

        final List<IgniteCache<Object, Object>> srvCaches = new ArrayList<>();

        for (String cacheName : caches)
            srvCaches.add(srv.cache(cacheName));

        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            final AtomicBoolean stop = new AtomicBoolean();

            List<T2<AtomicInteger, QueryCursor> > qrys = new ArrayList<>();

            try {
                IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            for (IgniteCache<Object, Object> srvCache : srvCaches)
                                srvCache.put(keys.get(rnd.nextInt(KEYS)), rnd.nextInt(100) - 200);
                        }

                        return null;
                    }
                }, THREADS, "update");

                U.sleep(1000);

                for (String cache : caches)
                    qrys.add(startListener(client.cache(cache)));

                U.sleep(1000);

                stop.set(true);

                fut.get();
            }
            finally {
                stop.set(true);
            }

            GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < UPDATES; i++) {
                        for (IgniteCache<Object, Object> srvCache : srvCaches)
                            srvCache.put(keys.get(rnd.nextInt(KEYS)), i);
                    }

                    return null;
                }
            }, THREADS, "update");

            for (T2<AtomicInteger, QueryCursor>  qry : qrys) {
                final AtomicInteger evtCnt = qry.get1();

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        log.info("Events: " + evtCnt.get());

                        return evtCnt.get() >= THREADS * UPDATES;
                    }
                }, 5000);

                assertEquals(THREADS * UPDATES, qry.get1().get());

                qry.get2().close();
            }
        }
    }
}
