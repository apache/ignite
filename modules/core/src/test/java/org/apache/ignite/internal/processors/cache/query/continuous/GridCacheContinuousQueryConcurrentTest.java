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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder.SingletonFactory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.cache.configuration.FactoryBuilder.factoryOf;

/**
 *
 */
public class GridCacheContinuousQueryConcurrentTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setPeerClassLoadingEnabled(false);

        if (gridName.endsWith(String.valueOf(NODES)))
            cfg.setClientMode(ThreadLocalRandom.current().nextBoolean());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTx() throws Exception {
        testRegistration(cacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL, 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartReplicated() throws Exception {
        testRestartRegistration(cacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartPartition() throws Exception {
        testRestartRegistration(cacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartPartitionTx() throws Exception {
        testRestartRegistration(cacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomic() throws Exception {
        testRegistration(cacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionTx() throws Exception {
        testRegistration(cacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionAtomic() throws Exception {
        testRegistration(cacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRegistration(CacheConfiguration ccfg) throws Exception {
        ExecutorService execSrv = newSingleThreadExecutor();

        try {
            final IgniteCache<Integer, String> cache = grid(0).getOrCreateCache(ccfg);

            for (int i = 0; i < 10; i++) {
                log.info("Start iteration: " + i);

                final int i0 = i;
                final AtomicBoolean stop = new AtomicBoolean(false);
                final CountDownLatch latch = new CountDownLatch(1);
                final int conQryCnt = 50;

                Future<List<IgniteFuture<String>>> fut = execSrv.submit(
                    new Callable<List<IgniteFuture<String>>>() {
                        @Override public List<IgniteFuture<String>> call() throws Exception {
                            int count = 0;
                            List<IgniteFuture<String>> futures = new ArrayList<>();

                            while (!stop.get()) {
                                futures.add(waitForKey(i0, cache, count));

                                if (log.isDebugEnabled())
                                    log.debug("Started cont query count: " + count);

                                if (++count >= conQryCnt)
                                    latch.countDown();
                            }

                            return futures;
                        }
                    });

                assert U.await(latch, 1, MINUTES);

                cache.put(i, "v");

                stop.set(true);

                List<IgniteFuture<String>> contQries = fut.get();

                for (IgniteFuture<String> contQry : contQries)
                    contQry.get(2, TimeUnit.SECONDS);
            }
        }
        finally {
            execSrv.shutdownNow();

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartRegistration(CacheConfiguration ccfg) throws Exception {
        ExecutorService execSrv = newSingleThreadExecutor();

        final AtomicBoolean stopRes = new AtomicBoolean(false);

        IgniteInternalFuture<?> restartFut = null;

        try {
            final IgniteCache<Integer, String> cache = grid(0).getOrCreateCache(ccfg);

            restartFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stopRes.get()) {
                        startGrid(NODES);

                        assert GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return grid(0).cluster().nodes().size() == NODES + 1;
                            }
                        }, 5000L);

                        Thread.sleep(300);

                        stopGrid(NODES);

                        assert GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return grid(0).cluster().nodes().size() == NODES;
                            }
                        }, 5000L);

                        Thread.sleep(300);
                    }

                    return null;
                }
            });

            U.sleep(100);

            for (int i = 0; i < 10; i++) {
                log.info("Start iteration: " + i);

                final int i0 = i;
                final AtomicBoolean stop = new AtomicBoolean(false);
                final CountDownLatch latch = new CountDownLatch(1);
                final int conQryCnt = 50;

                Future<List<IgniteFuture<String>>> fut = execSrv.submit(
                    new Callable<List<IgniteFuture<String>>>() {
                        @Override public List<IgniteFuture<String>> call() throws Exception {
                            int count = 0;
                            List<IgniteFuture<String>> futures = new ArrayList<>();

                            while (!stop.get()) {
                                futures.add(waitForKey(i0, cache, count));

                                if (log.isDebugEnabled())
                                    log.debug("Started cont query count: " + count);

                                if (++count >= conQryCnt)
                                    latch.countDown();
                            }

                            return futures;
                        }
                    });

                latch.await();

                cache.put(i, "v");

                assertEquals("v", cache.get(i));

                stop.set(true);

                List<IgniteFuture<String>> contQries = fut.get();

                for (IgniteFuture<String> contQry : contQries)
                    contQry.get(5, TimeUnit.SECONDS);
            }
        }
        finally {
            execSrv.shutdownNow();

            grid(0).destroyCache(ccfg.getName());

            if (restartFut != null) {
                stopRes.set(true);

                restartFut.get();

                stopGrid(NODES);
            }
        }
    }

    /**
     * @param key Key
     * @param cache Cache.
     * @param id ID.
     * @return Future.
     */
    public IgniteFuture<String> waitForKey(Integer key, final IgniteCache<Integer, String> cache, final int id) {
        String v = cache.get(key);

        // From now on, all futures will be completed immediately (since the key has been
        // inserted).
        if (v != null)
            return new IgniteFinishedFutureImpl<>("immediately");

        final IgniteFuture<String> promise = new IgniteFutureImpl<>(new GridFutureAdapter<String>());

        final CacheEntryListenerConfiguration<Integer, String> cfg =
            createCacheListener(key, promise, id);

        promise.listen(new IgniteInClosure<IgniteFuture<String>>() {
            @Override public void apply(IgniteFuture<String> future) {
                GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.deregisterCacheEntryListener(cfg);

                        return null;
                    }
                });
            }
        });

        // Start listening.
        // Assumption: When the call returns, the listener is guaranteed to have been registered.
        cache.registerCacheEntryListener(cfg);

        // Now must check the cache again, to make sure that we didn't miss the key insert while we
        // were busy setting up the cache listener.
        // Check asynchronously.
        IgniteCache<Integer, String> asyncCache = cache.withAsync();
        asyncCache.get(key);

        // Complete the promise if the key was inserted concurrently.
        asyncCache.<String>future().listen(new IgniteInClosure<IgniteFuture<String>>() {
            @Override public void apply(IgniteFuture<String> f) {
                String value = f.get();

                if (value != null) {
                    log.info("Completed by get: " + id);

                    (((GridFutureAdapter)((IgniteFutureImpl)promise).internalFuture())).onDone("by get");
                }
            }
        });

        return promise;
    }

    /**
     * @param key Key.
     * @param result Result.
     * @param id Listener ID.
     * @return Listener
     */
    private CacheEntryListenerConfiguration<Integer, String> createCacheListener(
        Integer key,
        IgniteFuture<String> result,
        int id) {
        return new MutableCacheEntryListenerConfiguration<>(
            factoryOf(new CacheListener(result, id)),
            new SingletonFactory<>(new KeyEventFilter(key, id)), false, true);
    }



    /**
     * @param cacheMode Cache mode.
     * @param atomicMode Atomicy mode.
     * @param backups Backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration(CacheMode cacheMode,
        CacheAtomicityMode atomicMode, int backups) {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>("test-" + cacheMode + atomicMode + backups);

        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setBackups(backups);
        cfg.setReadFromBackup(false);

        return cfg;
    }

    /**
     *
     */
    private static class CacheListener implements CacheEntryCreatedListener<Integer, String>, Serializable {
        /** */
        final IgniteFuture<String> result;

        /** */
        private final int id;

        /**
         * @param result Result.
         * @param id ID.
         */
        CacheListener(IgniteFuture<String> result, int id) {
            this.result = result;
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {
            (((GridFutureAdapter)((IgniteFutureImpl)result).internalFuture())).onDone("by listener");
        }
    }

    /**
     *
     */
    private static class KeyEventFilter implements CacheEntryEventFilter<Integer, String>, Serializable {
        /** */
        private static final long serialVersionUID = 42L;

        /** */
        private final Object key;

        /** */
        private final int id;

        /**
         * @param key Key.
         * @param id ID.
         */
        KeyEventFilter(Object key, int id) {
            this.key = key;
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {
            return e.getKey().equals(key);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass())
                && key.equals(((KeyEventFilter) o).key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }
    }
}
