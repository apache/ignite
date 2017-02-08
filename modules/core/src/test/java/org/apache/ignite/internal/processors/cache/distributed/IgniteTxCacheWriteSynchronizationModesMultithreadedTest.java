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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionOptimisticException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteTxCacheWriteSynchronizationModesMultithreadedTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 2;

    /** */
    private static final int NODES = SRVS + CLIENTS;

    /** */
    private boolean clientMode;

    /** */
    private static final int MULTITHREADED_TEST_KEYS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(clientMode);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        clientMode = true;

        for (int i = 0; i < CLIENTS; i++) {
            Ignite client = startGrid(SRVS + i);

            assertTrue(client.configuration().isClientMode());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPrimarySyncRestart() throws Exception {
        multithreadedTests(PRIMARY_SYNC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPrimarySync() throws Exception {
        multithreadedTests(PRIMARY_SYNC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedFullSync() throws Exception {
        multithreadedTests(FULL_SYNC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedFullSyncRestart() throws Exception {
        multithreadedTests(FULL_SYNC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedFullAsync() throws Exception {
        multithreadedTests(FULL_ASYNC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedFullAsyncRestart() throws Exception {
        multithreadedTests(FULL_ASYNC, true);
    }

    /**
     * @param syncMode Write synchronization mode.
     * @param restart Restart flag.
     * @throws Exception If failed.
     */
    private void multithreadedTests(CacheWriteSynchronizationMode syncMode, boolean restart) throws Exception {
        multithreaded(syncMode, 0, false, false, restart);

        multithreaded(syncMode, 1, false, false, restart);

        multithreaded(syncMode, 1, true, false, restart);

        multithreaded(syncMode, 2, false, false, restart);
    }

    /**
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param store If {@code true} sets store in cache configuration.
     * @param nearCache If {@code true} creates near cache on one of client nodes.
     * @param restart If {@code true} restarts one node during test.
     * @throws Exception If failed.
     */
    private void multithreaded(CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean store,
        boolean nearCache,
        boolean restart) throws Exception {
        final Ignite ignite = ignite(0);

        createCache(ignite, cacheConfiguration(null, syncMode, backups, store), nearCache);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restartFut = null;

        try {
            if (restart) {
                restartFut = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        while (!stop.get()) {
                            startGrid(NODES);

                            U.sleep(100);

                            stopGrid(NODES);
                        }
                        return null;
                    }
                }, "restart-thread");
            }

            commitMultithreaded(new IgniteBiInClosure<Ignite, IgniteCache<Integer, Integer>>() {
                @Override public void apply(Ignite ignite, IgniteCache<Integer, Integer> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                    cache.put(key, rnd.nextInt());
                }
            });

            commitMultithreaded(new IgniteBiInClosure<Ignite, IgniteCache<Integer, Integer>>() {
                @Override public void apply(Ignite ignite, IgniteCache<Integer, Integer> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Map<Integer, Integer> map = new TreeMap<>();

                    for (int i = 0; i < 100; i++) {
                        Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                        map.put(key, rnd.nextInt());
                    }

                    cache.putAll(map);
                }
            });

            commitMultithreaded(new IgniteBiInClosure<Ignite, IgniteCache<Integer, Integer>>() {
                @Override public void apply(Ignite ignite, IgniteCache<Integer, Integer> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Map<Integer, Integer> map = new TreeMap<>();

                    for (int i = 0; i < 100; i++) {
                        Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                        map.put(key, rnd.nextInt());
                    }

                    try {
                        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            for (Map.Entry<Integer, Integer> e : map.entrySet())
                                cache.put(e.getKey(), e.getValue());

                            tx.commit();
                        }
                    }
                    catch (CacheException | IgniteException ignored) {
                        // No-op.
                    }
                }
            });

            commitMultithreaded(new IgniteBiInClosure<Ignite, IgniteCache<Integer, Integer>>() {
                @Override public void apply(Ignite ignite, IgniteCache<Integer, Integer> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Map<Integer, Integer> map = new LinkedHashMap<>();

                    for (int i = 0; i < 10; i++) {
                        Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                        map.put(key, rnd.nextInt());
                    }

                    while (true) {
                        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                            for (Map.Entry<Integer, Integer> e : map.entrySet())
                                cache.put(e.getKey(), e.getValue());

                            tx.commit();

                            break;
                        }
                        catch (TransactionOptimisticException ignored) {
                           // Retry.
                        }
                        catch (CacheException | IgniteException ignored) {
                            break;
                        }
                    }
                }
            });
        }
        finally {
            stop.set(true);

            ignite.destroyCache(null);

            if (restartFut != null)
                restartFut.get();
        }
    }

    /**
     * @param c Test iteration closure.
     * @throws Exception If failed.
     */
    public void commitMultithreaded(final IgniteBiInClosure<Ignite, IgniteCache<Integer, Integer>> c) throws Exception {
        final long stopTime = System.currentTimeMillis() + 10_000;

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                int nodeIdx = idx % NODES;

                Thread.currentThread().setName("tx-thread-" + nodeIdx);

                Ignite ignite = ignite(nodeIdx);

                IgniteCache<Integer, Integer> cache = ignite.cache(null);

                while (System.currentTimeMillis() < stopTime)
                    c.apply(ignite, cache);
            }
        }, NODES * 3, "tx-thread");

        final IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        for (int key = 0; key < MULTITHREADED_TEST_KEYS; key++) {
            final Integer key0 = key;

            boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    final Integer val = cache.get(key0);

                    for (int i = 1; i < NODES; i++) {
                        IgniteCache<Integer, Integer> cache = ignite(i).cache(null);

                        if (!val.equals(cache.get(key0)))
                            return false;
                    }
                    return true;
                }
            }, 5000);

            assertTrue(wait);
        }
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @param nearCache If {@code true} creates near cache on one of client nodes.
     * @return Created cache.
     */
    private <K, V> IgniteCache<K, V> createCache(Ignite ignite, CacheConfiguration<K, V> ccfg,
        boolean nearCache) {
        IgniteCache<K, V> cache = ignite.createCache(ccfg);

        if (nearCache)
            ignite(NODES - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

        return cache;
    }

    /**
     * @param name Cache name.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param store If {@code true} configures cache store.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean store) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName(name);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }
}
