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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheNearReaderUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 3;

    /** */
    private static Map<Integer, Integer> storeMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** */
    @Before
    public void beforeCacheNearReaderUpdateTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);
        startClientGridsMultiThreaded(SRVS, CLIENTS);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoBackups() throws Exception {
        runTestGetUpdateMultithreaded(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneBackup() throws Exception {
        runTestGetUpdateMultithreaded(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneBackupNearEnabled() throws Exception {
        runTestGetUpdateMultithreaded(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneBackupStoreEnabled() throws Exception {
        runTestGetUpdateMultithreaded(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, false));
    }

    /**
     * @throws Exception If failed.
     */
    private void runTestGetUpdateMultithreaded(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        final List<Ignite> putNodes = new ArrayList<>();

        for (int i = 0; i < SRVS + CLIENTS - 1; i++)
            putNodes.add(ignite(i));

        final List<Ignite> getNodes = new ArrayList<>();

        getNodes.add(ignite(SRVS + CLIENTS - 1));
        getNodes.add(ignite(0));

        logCacheInfo(ccfg);

        runTestGetUpdateMultithreaded(ccfg, putNodes, getNodes, null, null);

        if (ccfg.getAtomicityMode() == TRANSACTIONAL) {
            runTestGetUpdateMultithreaded(ccfg, putNodes, getNodes, PESSIMISTIC, REPEATABLE_READ);

            runTestGetUpdateMultithreaded(ccfg, putNodes, getNodes, OPTIMISTIC, REPEATABLE_READ);

            runTestGetUpdateMultithreaded(ccfg, putNodes, getNodes, OPTIMISTIC, SERIALIZABLE);
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param putNodes Nodes executing updates.
     * @param getNodes Nodes executing gets.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void runTestGetUpdateMultithreaded(CacheConfiguration<Integer, Integer> ccfg,
        final List<Ignite> putNodes,
        final List<Ignite> getNodes,
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-627");

        log.info("Execute updates [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        final Ignite ignite0 = ignite(0);

        final String cacheName = ignite0.createCache(ccfg).getName();

        try {
            for (int i = 0; i < 5; i++) {
                final Integer key = i;

                final AtomicInteger putThreadIdx = new AtomicInteger();
                final AtomicInteger getThreadIdx = new AtomicInteger();

                final int PUT_THREADS = 20;
                final int GET_THREAD = 20;

                final CyclicBarrier barrier = new CyclicBarrier(PUT_THREADS + GET_THREAD);

                final IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int idx = putThreadIdx.getAndIncrement() % putNodes.size();

                        Ignite ignite = putNodes.get(idx);

                        IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                        IgniteTransactions txs = ignite.transactions();

                        Thread.currentThread().setName("update-thread-" + ignite.name());

                        barrier.await();

                        for (int i = 0; i < 100; i++) {
                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            if (concurrency != null) {
                                try (Transaction tx = txs.txStart(concurrency, isolation)) {
                                    cache.put(key, rnd.nextInt());

                                    tx.commit();
                                }
                                catch (TransactionOptimisticException ignore) {
                                    assertEquals(concurrency, OPTIMISTIC);
                                    assertEquals(isolation, SERIALIZABLE);
                                }
                            }
                            else
                                cache.put(key, rnd.nextInt());
                        }

                        return null;
                    }
                }, PUT_THREADS, "update-thread");

                IgniteInternalFuture<?> getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int idx = getThreadIdx.getAndIncrement() % getNodes.size();

                        Ignite ignite = getNodes.get(idx);

                        IgniteCache<Integer, Integer> cache;

                        if (ignite.configuration().isClientMode())
                            cache = ignite.createNearCache(cacheName, new NearCacheConfiguration<Integer, Integer>());
                        else
                            cache = ignite.cache(cacheName);

                        Thread.currentThread().setName("get-thread-" + ignite.name());

                        barrier.await();

                        while (!updateFut.isDone())
                            cache.get(key);

                        return null;
                    }
                }, GET_THREAD, "get-thread");

                updateFut.get();
                getFut.get();

                Integer val = (Integer)ignite0.cache(cacheName).get(key);

                log.info("Iteration [iter=" + i + ", val=" + val + ']');

                for (Ignite getNode : getNodes) {
                    IgniteCache<Integer, Integer> cache = getNode.cache(cacheName);

                    if (getNode.configuration().isClientMode() ||
                        cache.getConfiguration(CacheConfiguration.class).getNearConfiguration() != null)
                    assertNotNull(getNode.cache(cacheName).localPeek(key));
                }

                checkValue(key, val, cacheName);

                for (int n = 0; n < SRVS + CLIENTS; n++) {
                    val = n;

                    ignite(n).cache(cacheName).put(key, val);

                    checkValue(key, val, cacheName);
                }
            }
        }
        finally {
            destroyCache(ignite0, cacheName);
        }
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     * @param cacheName Cache name.
     */
    private void checkValue(Object key, Object expVal, String cacheName) {
        for (int i = 0; i < SRVS + CLIENTS; i++) {
            IgniteCache<Object, Object> cache = ignite(i).cache(cacheName);

            assertEquals(expVal, cache.get(key));
        }
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void logCacheInfo(CacheConfiguration<?, ?> ccfg) {
        log.info("Test cache [mode=" + ccfg.getCacheMode() +
            ", sync=" + ccfg.getWriteSynchronizationMode() +
            ", backups=" + ccfg.getBackups() +
            ", near=" + (ccfg.getNearConfiguration() != null) +
            ", store=" + ccfg.isWriteThrough() +
            ", evictPlc=" + (ccfg.getEvictionPolicy() != null) +
            ']');
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     */
    private void destroyCache(Ignite ignite, String cacheName) {
        storeMap.clear();

        ignite.destroyCache(cacheName);
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param storeEnabled If {@code true} adds cache store.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean storeEnabled,
        boolean nearCache) {
        if (storeEnabled)
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (storeEnabled) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setWriteThrough(true);
            ccfg.setReadThrough(true);
        }

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Integer, Integer> create() {
            return new CacheStoreAdapter<Integer, Integer>() {
                @Override public Integer load(Integer key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
                    storeMap.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) {
                    storeMap.remove(key);
                }
            };
        }
    }
}
