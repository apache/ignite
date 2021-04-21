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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheOptimisticTransactionsWithFilterTest extends GridCommonAbstractTest {
    /** */
    private static final TransactionIsolation[] ISOLATIONS = {REPEATABLE_READ, READ_COMMITTED};

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(serversNumber());

        startClientGrid(serversNumber());

        startClientGrid(serversNumber() + 1);
    }

    /**
     * @return Number of server nodes. In addition 2 clients are started.
     */
    protected int serversNumber() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCasReplace() throws Exception {
        executeTestForAllCaches(new TestClosure() {
            @Override public void apply(Ignite ignite, String cacheName) throws Exception {
                int nodeIdx = ThreadLocalRandom.current().nextInt(serversNumber() + 2);

                final IgniteCache<Integer, Integer> otherCache = ignite(nodeIdx).cache(cacheName);

                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                for (final Integer key : testKeys(cache)) {
                    for (int i = 0; i < 3; i++) {
                        for (TransactionIsolation isolation : ISOLATIONS) {
                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.replace(key, 1, 2));

                                assertNull(cache.get(key));

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.replace(key, 1, 2));

                                assertNull(cache.get(key));

                                tx.rollback();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.replace(key, 1, 2));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 1);
                                    }
                                }).get();

                                assertNull(cache.get(key));

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 1), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.replace(key, 2, 3));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 10);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 10), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertTrue(cache.replace(key, 10, 1));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 2);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 2), cacheName);

                            cache.remove(key);
                        }
                    }
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsent() throws Exception {
        executeTestForAllCaches(new TestClosure() {
            @Override public void apply(Ignite ignite, String cacheName) throws Exception {
                int nodeIdx = ThreadLocalRandom.current().nextInt(serversNumber() + 2);

                final IgniteCache<Integer, Integer> otherCache = ignite(nodeIdx).cache(cacheName);

                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                for (final Integer key : testKeys(cache)) {
                    for (int i = 0; i < 3; i++) {
                        for (TransactionIsolation isolation : ISOLATIONS) {
                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertTrue(cache.putIfAbsent(key, 1));

                                assertEquals((Integer)1, cache.get(key));

                                tx.rollback();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertTrue(cache.putIfAbsent(key, 1));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 2);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 2), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.putIfAbsent(key, 3));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.remove(key);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);
                        }
                    }
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplace() throws Exception {
        executeTestForAllCaches(new TestClosure() {
            @Override public void apply(Ignite ignite, String cacheName) throws Exception {
                int nodeIdx = ThreadLocalRandom.current().nextInt(serversNumber() + 2);

                final IgniteCache<Integer, Integer> otherCache = ignite(nodeIdx).cache(cacheName);

                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                for (final Integer key : testKeys(cache)) {
                    for (int i = 0; i < 3; i++) {
                        for (TransactionIsolation isolation : ISOLATIONS) {
                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.replace(key, 1));

                                assertNull(cache.get(key));

                                tx.rollback();
                            }

                            assertNull(cache.get(key));

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.replace(key, 1));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 2);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 2), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertTrue(cache.replace(key, 3));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.remove(key);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);
                        }
                    }
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveWithOldValue() throws Exception {
        executeTestForAllCaches(new TestClosure() {
            @Override public void apply(Ignite ignite, String cacheName) throws Exception {
                int nodeIdx = ThreadLocalRandom.current().nextInt(serversNumber() + 2);

                final IgniteCache<Integer, Integer> otherCache = ignite(nodeIdx).cache(cacheName);

                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                for (final Integer key : testKeys(cache)) {
                    for (int i = 0; i < 3; i++) {
                        for (TransactionIsolation isolation : ISOLATIONS) {
                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.remove(key, 1));

                                assertNull(cache.get(key));

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.remove(key, 1));

                                assertNull(cache.get(key));

                                tx.rollback();
                            }

                            checkCacheData(F.asMap(key, null), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.remove(key, 1));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 1);
                                    }
                                }).get();

                                assertNull(cache.get(key));

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 1), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertFalse(cache.remove(key, 2));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 10);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 10), cacheName);

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation)) {
                                assertTrue(cache.remove(key, 10));

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        otherCache.put(key, 2);
                                    }
                                }).get();

                                tx.commit();
                            }

                            checkCacheData(F.asMap(key, 2), cacheName);

                            cache.remove(key);
                        }
                    }
                }
            }
        });
    }

    /**
     * @param c Closure.
     * @throws Exception If failed.
     */
    private void executeTestForAllCaches(TestClosure c) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations()) {
            ignite(0).createCache(ccfg);

            log.info("Run test for cache [cache=" + ccfg.getCacheMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + (ccfg.getNearConfiguration() != null) + "]");

            ignite(serversNumber() + 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

            try {
                for (int i = 0; i < serversNumber() + 2; i++) {
                    log.info("Run test for node [node=" + i + ", client=" + ignite(i).configuration().isClientMode() + ']');

                    c.apply(ignite(i), ccfg.getName());
                }
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @param cache Cache.
     * @return Test keys.
     * @throws Exception If failed.
     */
    private List<Integer> testKeys(IgniteCache<Integer, Integer> cache) throws Exception {
        CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

        List<Integer> keys = new ArrayList<>();

        if (!cache.unwrap(Ignite.class).configuration().isClientMode()) {
            if (ccfg.getCacheMode() == PARTITIONED && serversNumber() > 1)
                keys.add(nearKey(cache));

            keys.add(primaryKey(cache));

            if (ccfg.getBackups() != 0 && serversNumber() > 1)
                keys.add(backupKey(cache));
        }
        else
            keys.add(nearKey(cache));

        return keys;
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> cfgs = new ArrayList<>();

        cfgs.add(cacheConfiguration(PARTITIONED, 0, false));
        cfgs.add(cacheConfiguration(PARTITIONED, 1, false));
        cfgs.add(cacheConfiguration(PARTITIONED, 1, true));
        cfgs.add(cacheConfiguration(REPLICATED, 0, false));

        return cfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        boolean nearCache) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        return ccfg;
    }

    /**
     *
     */
    static interface TestClosure {
        /**
         * @param ignite Node.
         * @param cacheName Cache name.
         * @throws Exception If failed.
         */
        public void apply(Ignite ignite, String cacheName) throws Exception;
    }
}
