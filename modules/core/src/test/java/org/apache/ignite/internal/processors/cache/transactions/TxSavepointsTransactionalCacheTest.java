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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public abstract class TxSavepointsTransactionalCacheTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int FUT_TIMEOUT = 3_000;

    /** Transaction concurrency and isolation levels. */
    private static final TxType[] txTypes = {
        new TxType(OPTIMISTIC, READ_COMMITTED),
        new TxType(OPTIMISTIC, REPEATABLE_READ),
        new TxType(OPTIMISTIC, SERIALIZABLE),
        new TxType(PESSIMISTIC, READ_COMMITTED),
        new TxType(PESSIMISTIC, REPEATABLE_READ),
        new TxType(PESSIMISTIC, SERIALIZABLE)
    };

    /**
     * We use this combinations to check different situations for transaction:
     * when tx owner (not) equal primary for the key used in transaction,
     * same or different primaries for the keys when test case have several keys
     * and where is happened second tx.
     * <p>
     * 2 can be client, so we don't use it for primaries.
     */
    protected NodeCombination[] nodeCombinations() {
        return new NodeCombination[] {
            new NodeCombination(2, 3, 1, 0),
            new NodeCombination(2, 3, 1, 1),
            new NodeCombination(2, 2, 1, 0),
            new NodeCombination(2, 2, 1, 1),
            new NodeCombination(1, 3, 1, 0),
            new NodeCombination(1, 3, 1, 1),
            new NodeCombination(1, 1, 1, 0),
            new NodeCombination(1, 1, 1, 1)
        };
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
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
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache < Integer, Integer > cache = nodes.txOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                        " transaction.", null, cache.get(key1));

                    tx.rollbackToSavepoint("sp");

                    IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> cache.putIfAbsent(key1, 1),
                        cacheMode().name() + "_get");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));

                    tx.commit();
                }
                finally {
                    cache.remove(key1);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutWithImplicitTx() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    cache.put(key1, 0);

                    IgniteInternalFuture fut = GridTestUtils.runAsync(
                        () -> assertTrue(cacheAsync.putIfAbsent(key1, 1)),
                        "_put");

                    waitForSecondCandidate(txType.concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' '
                        + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                }
                finally {
                    cache.remove(key1);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutWithExplicitTx() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    cache.put(key1, 0);

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                            try (Transaction tx0 = nodes.anotherTxOwner().transactions().txStart()) {
                                assertTrue(cacheAsync.putIfAbsent(key1, 1));

                                tx0.commit();
                            }
                        },
                        "_put");

                    waitForSecondCandidate(txType.concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' '
                        + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                }
                finally {
                    cache.remove(key1);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    cache.invoke(key1, (CacheEntryProcessor<Integer, Integer, Void>)(entry, objects) -> {
                        entry.setValue(0);

                        return null;
                    });

                    IgniteInternalFuture fut = GridTestUtils.runAsync(
                        () -> assertTrue(cacheAsync.putIfAbsent(key1, 1)),
                        "_put");

                    waitForSecondCandidate(txType.concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                }
                finally {
                    cache.remove(key1);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                cache.put(key1, 1);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    assertTrue(cache.remove(key1));

                    IgniteInternalFuture fut = GridTestUtils.runAsync(
                        () -> assertTrue(cacheAsync.remove(key1, 1)),
                        "_remove");

                    waitForSecondCandidate(txType.concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key1));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                    " transaction.", null, cache.get(key1));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());
            int key2 = generateKey(getConfig(), nodes.primaryForKey(), key1 + 1);
            int key3 = generateKey(getConfig(), nodes.primaryForKey(), key2 + 1);

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    Map<Integer, Integer> entries = new HashMap<>(3);

                    entries.put(key1, 1);
                    entries.put(key2, 1);
                    entries.put(key3, 1);

                    cache.putAll(entries);

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                        IgniteFuture<Boolean> fut1 = cacheAsync.putIfAbsentAsync(key1, 1);
                        IgniteFuture<Boolean> fut2 = cacheAsync.putIfAbsentAsync(key2, 2);
                        IgniteFuture<Boolean> fut3 = cacheAsync.putIfAbsentAsync(key3, 3);

                        assertTrue(fut1.get());
                        assertTrue(fut2.get());
                        assertTrue(fut3.get());
                    }, "_putAll");

                    waitForSecondCandidate(txType.concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)2, cache.get(key2));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)3, cache.get(key3));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)2, cache.get(key2));
                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)3, cache.get(key3));
                }
                finally {
                    cache.remove(key1);
                    cache.remove(key2);
                    cache.remove(key3);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllSuspendResumeInSameThread() throws Exception {
        checkPutAllSuspendResume(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllSuspendResumeInDifferentThread() throws Exception {
        checkPutAllSuspendResume(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutAllSuspendResume(boolean sameThread) throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());
            int key2 = generateKey(getConfig(), nodes.primaryForKey(), key1 + 1);
            int key3 = generateKey(getConfig(), nodes.primaryForKey(), key2 + 1);

            for (int i = 0; i < 3; i++) {
                TxType txType = txTypes[i];

                info("Transaction type " + txType);

                try {
                    Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation);

                    tx.savepoint("sp");

                    Map<Integer, Integer> entries = new HashMap<>(3);

                    entries.put(key1, 0);
                    entries.put(key2, 0);
                    entries.put(key3, 0);

                    cache.putAll(entries);

                    tx.suspend();

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                        IgniteFuture<Boolean> fut1 = cacheAsync.putIfAbsentAsync(key1, 1);
                        IgniteFuture<Boolean> fut2 = cacheAsync.putIfAbsentAsync(key2, 2);
                        IgniteFuture<Boolean> fut3 = cacheAsync.putIfAbsentAsync(key3, 3);

                        assertTrue(fut1.get());
                        assertTrue(fut2.get());
                        assertTrue(fut3.get());
                    }, "_putAll");

                    Callable<Void> c = () -> {
                        waitForSecondCandidate(txType.concurrency, cache, key1);

                        tx.resume();

                        tx.rollbackToSavepoint("sp");

                        fut.get(FUT_TIMEOUT);

                        assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                            ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                        assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                            ' ' + txType.isolation + " transaction.", (Integer)2, cache.get(key2));
                        assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                            ' ' + txType.isolation + " transaction.", (Integer)3, cache.get(key3));

                        tx.commit();
                        tx.close();

                        assertEquals("Broken rollback to savepoint in " + txType.concurrency
                            + ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key1));
                        assertEquals("Broken rollback to savepoint in " + txType.concurrency
                            + ' ' + txType.isolation + " transaction.", (Integer)2, cache.get(key2));
                        assertEquals("Broken rollback to savepoint in " + txType.concurrency
                            + ' ' + txType.isolation + " transaction.", (Integer)3, cache.get(key3));

                        return null;
                    };

                    if (sameThread)
                        c.call();
                    else
                        GridTestUtils.runAsync(c).get();
                }
                finally {
                    cache.remove(key1);
                    cache.remove(key2);
                    cache.remove(key3);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAll() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());
            int key2 = generateKey(getConfig(), nodes.primaryForKey(), key1 + 1);
            int key3 = generateKey(getConfig(), nodes.primaryForKey(), key2 + 1);

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                cache.put(key1, 1);
                cache.put(key2, 1);
                cache.put(key3, 1);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    Set<Integer> entries = new HashSet<>(3);

                    entries.add(key1);
                    entries.add(key2);
                    entries.add(key3);

                    cache.removeAll(entries);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key2));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key3));

                    IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                        IgniteFuture<Boolean> fut1 = cacheAsync.removeAsync(key1, 1);
                        IgniteFuture<Boolean> fut2 = cacheAsync.removeAsync(key2, 1);
                        IgniteFuture<Boolean> fut3 = cacheAsync.removeAsync(key3, 1);

                        assertTrue(fut1.get());
                        assertTrue(fut2.get());
                        assertTrue(fut3.get());
                    }, "_remove");

                    waitForSecondCandidate(txType.concurrency, cache, key1);

                    tx.rollbackToSavepoint("sp");

                    fut.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key2));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key3));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                    " transaction.", null, cache.get(key1));
                assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                    " transaction.", null, cache.get(key2));
                assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                    " transaction.", null, cache.get(key3));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleActions() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                        " transaction.", null, cache.getAndReplace(key1, 1));

                    cache.put(key1, 2);

                    tx.rollbackToSavepoint("sp");

                    IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(
                        () -> cacheAsync.putIfAbsent(key1, 3),
                        cacheMode().name() + "_multiOps");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)3, cache.get(key1));

                    cache.put(key1, 4);

                    tx.rollbackToSavepoint("sp");

                    fut = GridTestUtils.runAsync(() -> cacheAsync.replace(key1, 3, 5),
                        cacheMode().name() + "_multiOps");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)5, cache.get(key1));

                    assertTrue(cache.remove(key1, 5));

                    tx.commit();
                }

                assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                    ' ' + txType.isolation + " transaction.", null, cache.get(key1));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCrossCacheWithLocal() throws Exception {
        if (getConfig().getCacheMode() != LOCAL) {
            fail("Cross-cache operations don't work with LOCAL cache. See " +
                "https://issues.apache.org/jira/browse/IGNITE-9110");
        }

        checkGetCrossCaches(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + LOCAL.name()).setCacheMode(LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCrossCacheWithPartitioned() throws Exception {
        if (getConfig().getCacheMode() == LOCAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-9110");

        checkGetCrossCaches(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + PARTITIONED.name()).setCacheMode(PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCrossCacheWithReplicated() throws Exception {
        if (getConfig().getCacheMode() == LOCAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-9110");

        checkGetCrossCaches(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + REPLICATED.name()).setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetCrossCaches(CacheConfiguration<Integer, Integer> cfg1,
        CacheConfiguration<Integer, Integer> cfg2) throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache1 = nodes.txOwner().getOrCreateCache(cfg1);
            IgniteCache<Integer, Integer> cache1Async = nodes.anotherTxOwner().getOrCreateCache(cfg1);
            IgniteCache<Integer, Integer> cache2 = nodes.txOwner().getOrCreateCache(cfg2);

            int key1 = generateKey(cfg1, nodes.primaryForKey());
            int key2 = generateKey(cfg2, nodes.primaryForKey());


            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                        " transaction.", null, cache1.get(key1));
                    assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                        " transaction.", null, cache2.get(key2));

                    tx.rollbackToSavepoint("sp");

                    IgniteInternalFuture<Boolean> fut1 = GridTestUtils.runAsync(() -> cache1Async.putIfAbsent(key1, 1),
                        cacheMode().name() + "_get1");

                    IgniteInternalFuture<Boolean> fut2 = GridTestUtils.runAsync(() -> cache2.putIfAbsent(key2, 1),
                        cacheMode().name() + "_get2");

                    assertTrue(fut1.get(FUT_TIMEOUT));
                    assertTrue(fut2.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache1.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache2.get(key2));

                    tx.commit();
                }
                finally {
                    cache1.remove(key1);
                    cache2.remove(key2);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutCrossCacheWithLocal() throws Exception {
        fail("Cross-cache operations don't work with LOCAL cache. See " +
            "https://issues.apache.org/jira/browse/IGNITE-9110");

        checkPutCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + LOCAL.name()).setCacheMode(LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutCrossCacheWithPartitioned() throws Exception {
        if (getConfig().getCacheMode() == LOCAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-9110");

        checkPutCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + PARTITIONED.name()).setCacheMode(PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutCrossCacheWithReplicated() throws Exception {
        if (getConfig().getCacheMode() == LOCAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-9110");

        checkPutCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + REPLICATED.name()).setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutCrossCache(CacheConfiguration<Integer, Integer> cfg1,
        CacheConfiguration<Integer, Integer> cfg2) throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache1 = nodes.txOwner().getOrCreateCache(cfg1);
            IgniteCache<Integer, Integer> cache1Async = nodes.anotherTxOwner().getOrCreateCache(cfg1);
            IgniteCache<Integer, Integer> cache2 = nodes.txOwner().getOrCreateCache(cfg2);

            int key1 = generateKey(cfg1, nodes.primaryForKey());
            int key2 = generateKey(cfg2, nodes.primaryForKey());

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    cache1.putIfAbsent(key1, 0);
                    cache2.putIfAbsent(key2, 0);

                    IgniteInternalFuture fut1 = GridTestUtils.runAsync(() -> assertTrue(cache1Async.putIfAbsent(key1, 1)),
                        "_putMultiCaches1");

                    IgniteInternalFuture fut2 = GridTestUtils.runAsync(() -> assertTrue(cache2.putIfAbsent(key2, 1)),
                        "_putMultiCaches2");

                    waitForSecondCandidate(txType.concurrency, cache1, key1);
                    waitForSecondCandidate(txType.concurrency, cache2, key2);

                    tx.rollbackToSavepoint("sp");

                    fut1.get(FUT_TIMEOUT);
                    fut2.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache1.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache2.get(key2));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)1, cache1.get(key1));
                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)1, cache2.get(key2));
                }
                finally {
                    cache1.remove(key1);
                    cache2.remove(key2);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveCrossCacheWithLocal() throws Exception {
        fail("Cross-cache operations don't work with LOCAL cache. See " +
            "https://issues.apache.org/jira/browse/IGNITE-9110");

        checkRemoveCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + LOCAL.name()).setCacheMode(LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveCrossCacheWithPartitioned() throws Exception {
        if (getConfig().getCacheMode() == LOCAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-9110");

        checkRemoveCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + PARTITIONED.name()).setCacheMode(PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveCrossCacheWithReplicated() throws Exception {
        if (getConfig().getCacheMode() == LOCAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-9110");

        checkRemoveCrossCache(getConfig(),
            getConfig().setName(cacheMode().name() + '_' + REPLICATED.name()).setCacheMode(REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkRemoveCrossCache(CacheConfiguration<Integer, Integer> cfg1,
        CacheConfiguration<Integer, Integer> cfg2) throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache1 = nodes.txOwner().getOrCreateCache(cfg1);
            IgniteCache<Integer, Integer> cache1Async = nodes.anotherTxOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cache2 = nodes.txOwner().getOrCreateCache(cfg2);

            int key1 = generateKey(cfg1, nodes.primaryForKey());
            int key2 = generateKey(cfg2, nodes.primaryForAnotherKey());
            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                cache1.put(key1, 1);
                cache2.put(key2, 1);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    tx.savepoint("sp");

                    assertTrue(cache1.remove(key1));
                    assertTrue(cache2.remove(key2));

                    IgniteInternalFuture fut1 = GridTestUtils.runAsync(() -> assertTrue(cache1Async.remove(key1, 1)),
                        "_removeMultiCaches1");

                    IgniteInternalFuture fut2 = GridTestUtils.runAsync(() -> assertTrue(cache2.remove(key2, 1)),
                        "_removeMultiCaches2");

                    waitForSecondCandidate(txType.concurrency, cache1, key1);
                    waitForSecondCandidate(txType.concurrency, cache2, key2);

                    tx.rollbackToSavepoint("sp");

                    fut1.get(FUT_TIMEOUT);
                    fut2.get(FUT_TIMEOUT);

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache1.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache2.get(key2));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                    " transaction.", null, cache1.get(key1));
                assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                    " transaction.", null, cache2.get(key2));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMultipleKeys() throws Exception {
        checkPutMultipleKeys();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutMultipleKeys() throws Exception {
        for (NodeCombination nodes : nodeCombinations()) {
            info("Nodes " + nodes);

            IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(getConfig());
            IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(getConfig());

            int key1 = generateKey(getConfig(), nodes.primaryForKey());
            int key2 = generateKey(getConfig(), nodes.primaryForAnotherKey(), key1 + 1);

            for (TxType txType : txTypes) {
                info("Transaction type " + txType);

                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                    cache.put(key1, 0);

                    tx.savepoint("sp");

                    cache.put(key1, 1);

                    assertEquals((Integer)1, cache.get(key1));

                    cache.put(key2, 0);

                    IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> cacheAsync.putIfAbsent(key2, 1),
                        "_putMultiKeys");

                    waitForSecondCandidate(txType.concurrency, cache, key2);

                    tx.rollbackToSavepoint("sp");

                    assertTrue(fut.get(FUT_TIMEOUT));

                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)0, cache.get(key1));
                    assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key2));

                    tx.commit();

                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)0, cache.get(key1));
                    assertEquals("Broken rollback to savepoint in " + txType.concurrency
                        + ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key2));
                }
                finally {
                    cache.remove(key1);
                    cache.remove(key2);
                }
            }
        }
    }

    /**
     * @param cfg Cache configuration.
     * @param primary This node will be primary for generated key.
     * @return Generated key that is primary for presented node or 1 as key for local cache.
     */
    private int generateKey(CacheConfiguration<Integer, Integer> cfg, Ignite primary) {
        return generateKey(cfg, primary, null, 1);
    }

    /**
     * @param cfg Cache configuration.
     * @param primary This node will be primary for generated key.
     * @param backup This node will be backup for generated key.
     * @return Generated key that is primary for presented node or 1 as key for local cache.
     */
    private int generateKey(CacheConfiguration<Integer, Integer> cfg, Ignite primary, Ignite backup) {
        return generateKey(cfg, primary, backup, 1);
    }

    /**
     * @param cfg Cache configuration.
     * @param primary This node will be primary for generated key.
     * @param beginingIdx Index to start generating.
     * @return Generated key that is primary for presented node or 1 as key for local cache.
     */
    private int generateKey(CacheConfiguration<Integer, Integer> cfg, Ignite primary,  int beginingIdx) {
        return generateKey(cfg, primary, null, beginingIdx);
    }

    /**
     * @param cfg Cache configuration.
     * @param primary This node will be primary for generated key.
     * @param backup This node will be backup for generated key.
     * @param beginingIdx Index to start generating.
     * @return Generated key that is primary for presented node or beginingIdx as key for local cache.
     */
    private int generateKey(
        CacheConfiguration<Integer, Integer> cfg,
        Ignite primary,
        Ignite backup,
        int beginingIdx
    ) {
        if (cfg.getCacheMode() == LOCAL)
            return beginingIdx;

        Affinity<Object> aff = primary.affinity(cfg.getName());

        for (int key = beginingIdx;; key++) {
            if (aff.isPrimary(primary.cluster().localNode(), key) &&
                (backup == null || aff.isBackup(backup.cluster().localNode(), key)))
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
     */
    private void waitForSecondCandidate(TransactionConcurrency concurrency, IgniteCache cache, int key)
        throws IgniteInterruptedCheckedException {
        if (concurrency == TransactionConcurrency.PESSIMISTIC) {
            assertTrue("Wait for second lock candidate was timed out.", GridTestUtils.waitForCondition(() -> {
                try {
                    return
                        ((IgniteEx)grid(((IgniteCacheProxy)cache).context().cache().affinity().mapKeyToNode(key)))
                            .cachex(cache.getName()).context().cache().entryEx(key).localCandidates().size() == 2;
                }
                catch (GridCacheEntryRemovedException e) {
                    throw new IgniteException("Wait for second lock candidate was failed", e);
                }
            }, FUT_TIMEOUT));
        }
    }

    /** Transaction concurrency and isolation level. */
    private static class TxType {
        /** Concurrency. */
        final TransactionConcurrency concurrency;

        /** Isolation. */
        final TransactionIsolation isolation;

        /**
         * @param concurrency Transaction concurrency.
         * @param isolation Transaction isolation.
         */
        private TxType(TransactionConcurrency concurrency, TransactionIsolation isolation) {
            this.concurrency = concurrency;
            this.isolation = isolation;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return concurrency + " " + isolation;
        }
    }

    /** */
    protected class NodeCombination {
        /** Node, where transaction will be started. */
        final int txOwnerNodeId;

        /** Node, where second transaction will be started. */
        final int anotherTxOwnerNodeId;

        /** Primary node for a key. */
        final int primaryIdForKey;

        /** Primary node for second key. */
        final int primaryIdForAnotherKey;

        /**
         * @param txOwnerNodeId Node, where transaction will be started.
         * @param anotherTxOwnerNodeId Node, where second transaction will be started.
         * @param primaryIdForKey Primary node for a key.
         * @param primaryIdForAnotherKey Primary node for second key.
         */
        protected NodeCombination(
            int txOwnerNodeId,
            int anotherTxOwnerNodeId,
            int primaryIdForKey,
            int primaryIdForAnotherKey
        ) {
            this.txOwnerNodeId = txOwnerNodeId;
            this.anotherTxOwnerNodeId = anotherTxOwnerNodeId;
            this.primaryIdForKey = primaryIdForKey;
            this.primaryIdForAnotherKey = primaryIdForAnotherKey;
        }

        /**
         * @return Transaction owner node.
         */
        IgniteEx txOwner() {
            return grid(txOwnerNodeId);
        }

        /**
         * @return Transaction owner node.
         */
        IgniteEx anotherTxOwner() {
            return grid(anotherTxOwnerNodeId);
        }

        /**
         * @return Node which should be primary for a key.
         */
        IgniteEx primaryForKey() {
            return grid(primaryIdForKey);
        }

        /**
         * @return Node which should be primary for another key.
         */
        IgniteEx primaryForAnotherKey() {
            return grid(primaryIdForAnotherKey);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "txOwner = " + txOwnerNodeId + ", anotherTxOwner = " + anotherTxOwnerNodeId
                + ", primary = " + primaryIdForKey + ", anotherPrimary = " + primaryIdForAnotherKey;
        }
    }
}
