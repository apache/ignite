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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public abstract class TxSavepointsTransactionalCacheTest extends GridCacheAbstractSelfTest {
    /** Node, where transaction will be started. */
    private Ignite txOwner;

    /** Primary node for transaction. */
    private Ignite primaryForKey;
    
    /** */
    private final static int FUT_TIMEOUT = 3_000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        txOwner = grid(2);
        primaryForKey = grid(1);
    }

    /** */
    protected CacheConfiguration<Integer, Integer> getConfig() {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheMode(cacheMode());

        cfg.setName(cacheMode().name());

        cfg.setBackups(1);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** Override this method to use different cache modes for tests.*/
    protected abstract CacheMode cacheMode();

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", null, cache.get(key1));

                    tx.rollbackToSavepoint("sp");

                    IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> cache.putIfAbsent(key1, 1),
                        cacheMode().name() + "_get");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache.get(key1));

                    tx.commit();
                } finally {
                    cache.remove(key1);
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    cache.put(key1, 0);

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> assertTrue(cache.putIfAbsent(key1, 1)),
                        "_put");

                    waitForSecondCandidate(concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache.get(key1));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", (Integer) 1, cache.get(key1));
                } finally {
                    cache.remove(key1);
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    cache.invoke(key1, (CacheEntryProcessor<Integer, Integer, Void>)(entry, objects) -> {
                        entry.setValue(0);

                        return null;
                    });

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> assertTrue(cache.putIfAbsent(key1, 1)),
                        "_put");

                    waitForSecondCandidate(concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache.get(key1));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", (Integer) 1, cache.get(key1));
                } finally {
                    cache.remove(key1);
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                cache.put(key1, 1);

                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertTrue(cache.remove(key1));

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> assertTrue(cache.remove(key1, 1)),
                        "_remove");

                    waitForSecondCandidate(concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.",  null, cache.get(key1));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                    " transaction.",  null, cache.get(key1));
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);
        int key2 = generateKey(getConfig(), primaryForKey, key1 + 1);
        int key3 = generateKey(getConfig(), primaryForKey, key2 + 1);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    Map<Integer, Integer> entries = new HashMap<>(3);

                    entries.put(key1, 1);
                    entries.put(key2, 1);
                    entries.put(key3, 1);

                    cache.putAll(entries);

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                        IgniteFuture<Boolean> fut1 = cache.putIfAbsentAsync(key1, 1);
                        IgniteFuture<Boolean> fut2 = cache.putIfAbsentAsync(key2, 2);
                        IgniteFuture<Boolean> fut3 = cache.putIfAbsentAsync(key3, 3);

                        assertTrue(fut1.get());
                        assertTrue(fut2.get());
                        assertTrue(fut3.get());
                    }, "_putAll");

                    waitForSecondCandidate(concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.", (Integer) 1, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.", (Integer) 2, cache.get(key2));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.", (Integer) 3, cache.get(key3));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                            " transaction.", (Integer) 1, cache.get(key1));
                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                            " transaction.", (Integer) 2, cache.get(key2));
                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                            " transaction.", (Integer) 3, cache.get(key3));
                } finally {
                    cache.remove(key1);
                    cache.remove(key2);
                    cache.remove(key3);
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAll() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);
        int key2 = generateKey(getConfig(), primaryForKey, key1 + 1);
        int key3 = generateKey(getConfig(), primaryForKey, key2 + 1);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                cache.put(key1, 1);
                cache.put(key2, 1);
                cache.put(key3, 1);

                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    Set<Integer> entries = new HashSet<>(3);

                    entries.add(key1);
                    entries.add(key2);
                    entries.add(key3);

                    cache.removeAll(entries);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.", null, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.", null, cache.get(key2));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.", null, cache.get(key3));

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                        IgniteFuture<Boolean> fut1 = cache.removeAsync(key1, 1);
                        IgniteFuture<Boolean> fut2 = cache.removeAsync(key2, 1);
                        IgniteFuture<Boolean> fut3 = cache.removeAsync(key3, 1);

                        assertTrue(fut1.get());
                        assertTrue(fut2.get());
                        assertTrue(fut3.get());
                    }, "_remove");

                    waitForSecondCandidate(concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.",  null, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.",  null, cache.get(key2));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                            ' ' + isolation + " transaction.",  null, cache.get(key3));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.",  null, cache.get(key1));
                assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.",  null, cache.get(key2));
                assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.",  null, cache.get(key3));
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleActions() throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", null, cache.getAndReplace(key1, 1));

                    cache.put(key1, 2);

                    tx.rollbackToSavepoint("sp");

                    IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> cache.putIfAbsent(key1, 3),
                        cacheMode().name() + "_multiOps");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 3, cache.get(key1));

                    cache.put(key1, 4);

                    tx.rollbackToSavepoint("sp");

                    fut = GridTestUtils.runAsync(() -> cache.replace(key1, 3, 5),
                        cacheMode().name() + "_multiOps");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 5, cache.get(key1));

                    assertTrue(cache.remove(key1, 5));

                    tx.commit();
                }

                assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                    ' ' + isolation + " transaction.", null, cache.get(key1));
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCrossCacheWithLocal() throws Exception {
        checkGetCrossCaches(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + LOCAL.name()).setCacheMode(LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCrossCacheWithPartitioned() throws Exception {
        checkGetCrossCaches(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + PARTITIONED.name()).setCacheMode(PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCrossCacheWithReplicated() throws Exception {
        checkGetCrossCaches(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + REPLICATED.name()).setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetCrossCaches(CacheConfiguration<Integer, Integer> cfg1,
        CacheConfiguration<Integer, Integer> cfg2) throws Exception {
        IgniteCache<Integer, Integer> cache1 = txOwner.getOrCreateCache(cfg1);
        IgniteCache<Integer, Integer> cache2 = txOwner.getOrCreateCache(cfg2);

        int key1 = generateKey(cfg1, primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", null, cache1.get(key1));
                    assertEquals("Broken savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", null, cache2.get(1));

                    tx.rollbackToSavepoint("sp");

                    IgniteInternalFuture<Boolean> fut1 = GridTestUtils.runAsync(() ->cache1.putIfAbsent(key1, 1),
                        cacheMode().name() + "_get1");

                    IgniteInternalFuture<Boolean> fut2 = GridTestUtils.runAsync(() -> cache2.putIfAbsent(key1, 1),
                        cacheMode().name() + "_get2");

                    assertTrue(fut1.get(FUT_TIMEOUT));
                    assertTrue(fut2.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache1.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache2.get(key1));

                    tx.commit();
                } finally {
                    cache1.remove(key1);
                    cache2.remove(key1);
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMultiCachesWithLocal() throws Exception {
        checkPutCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + LOCAL.name()).setCacheMode(LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutCrossCacheWithPartitioned() throws Exception {
        checkPutCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + PARTITIONED.name()).setCacheMode(PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutCrossCacheWithReplicated() throws Exception {
        checkPutCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + REPLICATED.name()).setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutCrossCache(CacheConfiguration<Integer, Integer> cfg1,
        CacheConfiguration<Integer, Integer> cfg2) throws Exception {
        IgniteCache<Integer, Integer> cache1 = txOwner.getOrCreateCache(cfg1);
        IgniteCache<Integer, Integer> cache2 = txOwner.getOrCreateCache(cfg2);

        int key1 = generateKey(cfg1, primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    cache1.putIfAbsent(key1, 0);
                    cache2.putIfAbsent(key1, 0);

                    IgniteInternalFuture fut1 = GridTestUtils.runAsync(() -> assertTrue(cache1.putIfAbsent(key1, 1)),
                        "_putMultiCaches1");

                    IgniteInternalFuture fut2 = GridTestUtils.runAsync(() -> assertTrue(cache2.putIfAbsent(key1, 1)),
                        "_putMultiCaches2");

                    waitForSecondCandidate(concurrency, cache1, key1);
                    waitForSecondCandidate(concurrency, cache2, key1);

                    tx.rollbackToSavepoint("sp");

                    fut1.get(FUT_TIMEOUT);
                    fut2.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache1.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache2.get(key1));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", (Integer) 1, cache1.get(key1));
                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", (Integer) 1, cache2.get(key1));
                } finally {
                    cache1.remove(key1);
                    cache2.remove(key1);
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveCrossCacheWithLocal() throws Exception {
        checkRemoveCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + LOCAL.name()).setCacheMode(LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveCrossCacheWithPartitioned() throws Exception {
        checkRemoveCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + PARTITIONED.name()).setCacheMode(PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveCrossCacheWithReplicated() throws Exception {
        checkRemoveCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + REPLICATED.name()).setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkRemoveCrossCache(CacheConfiguration<Integer, Integer> cfg1,
        CacheConfiguration<Integer, Integer> cfg2) throws Exception {
        IgniteCache<Integer, Integer> cache1 = txOwner.getOrCreateCache(cfg1);
        IgniteCache<Integer, Integer> cache2 = txOwner.getOrCreateCache(cfg2);

        int key1 = generateKey(cfg1, primaryForKey);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                cache1.put(key1, 1);
                cache2.put(key1, 1);

                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertTrue(cache1.remove(key1));
                    assertTrue(cache2.remove(key1));

                    IgniteInternalFuture fut1 = GridTestUtils.runAsync(() -> assertTrue(cache1.remove(key1, 1)),
                        "_removeMultiCaches1");

                    IgniteInternalFuture fut2 = GridTestUtils.runAsync(() -> assertTrue(cache2.remove(key1, 1)),
                        "_removeMultiCaches2");

                    waitForSecondCandidate(concurrency, cache1, key1);
                    waitForSecondCandidate(concurrency, cache2, key1);

                    tx.rollbackToSavepoint("sp");

                    fut1.get(FUT_TIMEOUT);
                    fut2.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", null, cache1.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", null, cache2.get(key1));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                    " transaction.", null, cache1.get(key1));
                assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                    " transaction.", null, cache2.get(key1));
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMultipleKeysOnSamePrimary() throws Exception {
        checkPutMultipleKeys(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMultipleKeysOnDifferentPrimaries() throws Exception {
        checkPutMultipleKeys(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutMultipleKeys(boolean samePrimaryNodes) throws Exception {
        IgniteCache<Integer, Integer> cache = txOwner.getOrCreateCache(getConfig());

        int key1 = generateKey(getConfig(), primaryForKey);
        int key2 = generateKey(getConfig(), grid(samePrimaryNodes ? 1 : 0), key1 + 1);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = txOwner.transactions().txStart(concurrency, isolation)) {
                    cache.put(key1, 0);

                    tx.savepoint("sp");

                    cache.put(key1, 1);

                    assertEquals((Integer) 1, cache.get(key1));

                    cache.put(key2, 0);

                    IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> cache.putIfAbsent(key2, 1),
                        "_putMultiKeys");

                    waitForSecondCandidate(concurrency, cache, key2);

                    tx.rollbackToSavepoint("sp");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 0, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        ' ' + isolation + " transaction.", (Integer) 1, cache.get(key2));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", (Integer) 0, cache.get(key1));
                    assertEquals("Broken rollback to savepoint in " + concurrency + ' ' + isolation +
                        " transaction.", (Integer) 1, cache.get(key2));
                } finally {
                    cache.remove(key1);
                    cache.remove(key2);
                }
            }
    }

    /**
     * @param cfg Cache configuration.
     * @param ignite This node will be primary for generated key.
     * @return Generated key that is primary for presented node or 1 as key for local cache.
     */
    private int generateKey(CacheConfiguration<Integer, Integer> cfg, Ignite ignite) {
        return generateKey(cfg, ignite, 1);
    }

    /**
     * @param cfg Cache configuration.
     * @param ignite This node will be primary for generated key.
     * @param beginingIdx Index to start generating.
     * @return Generated key that is primary for presented node or beginingIdx as key for local cache.
     */
    private int generateKey(CacheConfiguration<Integer, Integer> cfg, Ignite ignite, int beginingIdx) {
        if (cfg.getCacheMode() == LOCAL)
            return beginingIdx;

        Affinity<Object> aff = txOwner.affinity(cfg.getName());

        for (int key = beginingIdx;; key++) {
            if (aff.isPrimary(ignite.cluster().localNode(), key))
                return key;
        }
    }

    /**
     * Waits for second candidate to lock key. Optimistic transactions don't lock keys until commit/rollback.
     *
     * @param concurrency Transaction concurrency.
     * @param cache Cache.
     * @param key Key to check.
     * @throws IgniteInterruptedCheckedException If was interrupted.
     * @throws IgniteTxTimeoutCheckedException If lock candidate didn't appeared for 3 seconds.
     */
    private void waitForSecondCandidate(TransactionConcurrency concurrency, IgniteCache cache, int key)
        throws IgniteInterruptedCheckedException, IgniteTxTimeoutCheckedException {
        if (concurrency == TransactionConcurrency.PESSIMISTIC &&
            !GridTestUtils.waitForCondition(() -> {
                try {
                    return
                        ((IgniteEx)grid(((IgniteCacheProxy)cache).context().cache().affinity().mapKeyToNode(key)))
                        .cachex(cache.getName()).context().cache().entryEx(key).localCandidates().size() == 2;
                }
                catch (GridCacheEntryRemovedException e) {
                    throw new IgniteException("Wait for second lock candidate was failed", e);
                }
            }, FUT_TIMEOUT))
            throw new IgniteTxTimeoutCheckedException("Wait for second lock candidate was timed out.");
    }
}
