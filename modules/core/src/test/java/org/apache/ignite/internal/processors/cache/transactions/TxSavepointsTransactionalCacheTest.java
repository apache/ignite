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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.util.typedef.CIX3;
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
public class TxSavepointsTransactionalCacheTest extends GridCacheAbstractSelfTest {
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
    private NodeCombination[] nodeCombinations = new NodeCombination[] {
            new NodeCombination(2, 3, 1, 0),
            new NodeCombination(2, 3, 1, 1),
            new NodeCombination(2, 2, 1, 0),
            new NodeCombination(2, 2, 1, 1),
            new NodeCombination(1, 3, 1, 0),
            new NodeCombination(1, 3, 1, 1),
            new NodeCombination(1, 1, 1, 0),
            new NodeCombination(1, 1, 1, 1)
    };

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    for (TxType txType : txTypes) {
                        info("Transaction type [" + txType + ']');

                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                                " transaction.", null, cache.get(key));

                            tx.rollbackToSavepoint("sp");

                            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> cacheAsync.putIfAbsent(key, 1),
                                cacheMode().name() + "_get");

                            assertTrue(fut.get(FUT_TIMEOUT));

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key));

                            tx.commit();
                        }
                        finally {
                            cache.remove(key);
                        }
                    }
        }});
    }

    /**
     *
     */
    public void testPutWithImplicitTx() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    int key = generateKey(cfg, nodes.primaryForKey());
                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    for (TxType txType : txTypes) {
                        info("Transaction type " + txType);

                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            cache.put(key, 0);

                            IgniteInternalFuture fut = GridTestUtils.runAsync(
                                () -> assertTrue(cacheAsync.putIfAbsent(key, 1)),
                                "_put");

                            waitForSecondCandidate(txType.concurrency, cache, key);

                            tx.rollbackToSavepoint("sp");

                            fut.get(FUT_TIMEOUT);

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key));

                            tx.commit();

                            assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' '
                                + txType.isolation + " transaction.", (Integer)1, cache.get(key));
                        }
                        finally {
                            cache.remove(key);
                        }
                    }
        }});
    }

    /**
     *
     */
    public void testPutWithExplicitTx() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    for (TxType txType : txTypes) {
                        info("Transaction type " + txType);

                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            cache.put(key, 0);

                            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                                    try (Transaction tx0 = nodes.anotherTxOwner().transactions().txStart()) {
                                        assertTrue(cacheAsync.putIfAbsent(key, 1));

                                        tx0.commit();
                                    }
                                },
                                "_put");

                            waitForSecondCandidate(txType.concurrency, cache, key);

                            tx.rollbackToSavepoint("sp");

                            fut.get(FUT_TIMEOUT);

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key));

                            tx.commit();

                            assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' '
                                + txType.isolation + " transaction.", (Integer)1, cache.get(key));
                        }
                        finally {
                            cache.remove(key);
                        }
                    }
        }});
    }

    /**
     *
     */
    public void testInvoke() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    for (TxType txType : txTypes) {
                        info("Transaction type " + txType);

                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            cache.invoke(key, (CacheEntryProcessor<Integer, Integer, Void>)(entry, objects) -> {
                                entry.setValue(0);

                                return null;
                            });

                            IgniteInternalFuture fut = GridTestUtils.runAsync(
                                () -> assertTrue(cacheAsync.putIfAbsent(key, 1)),
                                "_put");

                            waitForSecondCandidate(txType.concurrency, cache, key);

                            tx.rollbackToSavepoint("sp");

                            fut.get(FUT_TIMEOUT);

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key));

                            tx.commit();

                            assertEquals("Broken rollback to savepoint in " + txType.concurrency
                                + ' ' + txType.isolation + " transaction.", (Integer)1, cache.get(key));
                        }
                        finally {
                            cache.remove(key);
                        }
                    }
        }});
    }

    /**
     *
     */
    public void testRemove() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    for (TxType txType : txTypes) {
                        info("Transaction type " + txType);

                        cache.put(key, 1);

                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            assertTrue(cache.remove(key));

                            IgniteInternalFuture fut = GridTestUtils.runAsync(
                                () -> assertTrue(cacheAsync.remove(key, 1)),
                                "_remove");

                            waitForSecondCandidate(txType.concurrency, cache, key);

                            tx.rollbackToSavepoint("sp");

                            fut.get(FUT_TIMEOUT);

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", null, cache.get(key));

                            tx.commit();
                        }

                        assertEquals("Broken rollback to savepoint in " + txType.concurrency + ' ' + txType.isolation +
                            " transaction.", null, cache.get(key));
                    }
        }});
    }

    /**
     *
     */
    public void testPutAll() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    int key1 = generateKey(cfg, nodes.primaryForKey());
                    int key2 = generateKey(cfg, nodes.primaryForKey(), key1 + 1);
                    int key3 = generateKey(cfg, nodes.primaryForKey(), key2 + 1);

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
        }});
    }
/*
    *//**
     * @throws Exception If failed.
     *//*
    public void testPutAllSuspendResumeInSameThread() throws Exception {
        checkPutAllSuspendResume(true);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testPutAllSuspendResumeInDifferentThread() throws Exception {
        checkPutAllSuspendResume(false);
    }

    *//**
     * @throws Exception If failed.
     *//*
    private void checkPutAllSuspendResume(boolean sameThread) throws Exception {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                int key1 = generateKey(cfg, nodes.primaryForKey());
                int key2 = generateKey(cfg, nodes.primaryForKey(), key1 + 1);
                int key3 = generateKey(cfg, nodes.primaryForKey(), key2 + 1);

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
        }});
    }*/

    /**
     *
     */
    public void testRemoveAll() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    int key1 = generateKey(cfg, nodes.primaryForKey());
                    int key2 = generateKey(cfg, nodes.primaryForKey(), key1 + 1);
                    int key3 = generateKey(cfg, nodes.primaryForKey(), key2 + 1);

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
        }});
    }

    /**
     *
     */
    public void testMultipleActions() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    for (TxType txType : txTypes) {
                        info("Transaction type " + txType);

                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                                " transaction.", null, cache.getAndReplace(key, 1));

                            cache.put(key, 2);

                            tx.rollbackToSavepoint("sp");

                            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(
                                () -> cacheAsync.putIfAbsent(key, 3),
                                cacheMode().name() + "_multiOps");

                            assertTrue(fut.get(FUT_TIMEOUT));

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", (Integer)3, cache.get(key));

                            cache.put(key, 4);

                            tx.rollbackToSavepoint("sp");

                            fut = GridTestUtils.runAsync(() -> cacheAsync.replace(key, 3, 5),
                                cacheMode().name() + "_multiOps");

                            assertTrue(fut.get(FUT_TIMEOUT));

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                ' ' + txType.isolation + " transaction.", (Integer)5, cache.get(key));

                            assertTrue(cache.remove(key, 5));

                            tx.commit();
                        }

                        assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                        ' ' + txType.isolation + " transaction.", null, cache.get(key));
                }
        }});
    }

    /**
     *
     */
    public void testGetCrossCaches() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache1, CacheConfiguration<Integer, Integer> cfg1)
                    throws IgniteCheckedException {
                    for (CacheConfiguration<Integer, Integer> cfg2 : cacheConfigurations()) {
                        // TODO https://issues.apache.org/jira/browse/IGNITE-9110
                        if (cfg1.getCacheMode() == LOCAL && cfg2.getCacheMode() == LOCAL ||
                            cfg1.getCacheMode() != LOCAL && cfg2.getCacheMode() != LOCAL)
                            continue;

                        if (cfg1.getName().equals(cfg2.getName()))
                            cfg2.setName(cfg2.getName() + "_second");

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
        }});
    }

    /**
     *
     */
    public void testPutCrossCache() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache1, CacheConfiguration<Integer, Integer> cfg1)
                    throws IgniteCheckedException {
                    for (CacheConfiguration<Integer, Integer> cfg2 : cacheConfigurations()) {
                        // TODO https://issues.apache.org/jira/browse/IGNITE-9110
                        if (cfg1.getCacheMode() == LOCAL && cfg2.getCacheMode() == LOCAL ||
                            cfg1.getCacheMode() != LOCAL && cfg2.getCacheMode() != LOCAL)
                            continue;

                        if (cfg1.getName().equals(cfg2.getName()))
                            cfg2.setName(cfg2.getName() + "_second");

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

                                waitForSecondCandidate(txType.concurrency, nodes.primaryForKey(), cache1, cache2, key1);
                                waitForSecondCandidate(txType.concurrency, nodes.primaryForAnotherKey(),
                                    cache1, cache2, key2);

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
        }}});
    }

    /**
     *
     */
    public void testRemoveCrossCache() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache1, CacheConfiguration<Integer, Integer> cfg1)
                    throws IgniteCheckedException {
                    for (CacheConfiguration<Integer, Integer> cfg2 : cacheConfigurations()) {
                        // TODO https://issues.apache.org/jira/browse/IGNITE-9110
                        if (cfg1.getCacheMode() == LOCAL && cfg2.getCacheMode() == LOCAL ||
                            cfg1.getCacheMode() != LOCAL && cfg2.getCacheMode() != LOCAL)
                            continue;

                        if (cfg1.getName().equals(cfg2.getName()))
                            cfg2.setName(cfg2.getName() + "_second");

                        IgniteCache<Integer, Integer> cache1Async = nodes.anotherTxOwner().getOrCreateCache(cfg1);
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

                                waitForSecondCandidate(txType.concurrency, nodes.primaryForKey(), cache1, cache2, key1);
                                waitForSecondCandidate(txType.concurrency, nodes.primaryForAnotherKey(),
                                    cache1, cache2, key2);

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
        }}});
    }

    /**
     *
     */
    public void testPutMultipleKeys() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                    throws IgniteCheckedException {
                    IgniteCache<Integer, Integer> cacheAsync = nodes.anotherTxOwner().getOrCreateCache(cfg);

                    int key1 = generateKey(cfg, nodes.primaryForKey());
                    int key2 = generateKey(cfg, nodes.primaryForAnotherKey(), key1 + 1);

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
        }});
    }

    /**
     * Tests savepoint.
     */
    public void testSavepoints() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
            @Override public void applyx(
                NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg) {
                int key = generateKey(cfg, nodes.primaryForKey());

                for (TxType txType : txTypes) {
                    cache.put(key, 0);

                    try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                        cache.put(key, 1);

                        tx.savepoint("s1");

                        cache.put(key, 2);

                        tx.savepoint("s2");

                        cache.put(key, 3);

                        tx.savepoint("s3");

                        tx.rollbackToSavepoint("s2");

                        assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                            (Integer)2, cache.get(key));

                        tx.rollbackToSavepoint("s1");

                        assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                            (Integer)1, cache.get(key));

                        tx.commit();
                    }

                    assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                        (Integer)1, cache.get(key));
                }
            }});
    }

    /**
     * Tests valid and invalid rollbacks to savepoint.
     */
    public void testFailRollbackToSavepoint() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg) {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    for (TxType txType : txTypes) {
                        GridTestUtils.assertThrows(
                            log, () -> {
                                cache.put(key, 0);

                                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                                    cache.put(key, 1);

                                    tx.savepoint("s1");

                                    cache.put(key, 2);

                                    tx.savepoint("s2");

                                    cache.put(key, 3);

                                    tx.savepoint("s3");

                                    tx.rollbackToSavepoint("s2");

                                    assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                                        (Integer)2, cache.get(key));

                                    tx.rollbackToSavepoint("s3");
                                }

                                return null;
                            },
                            IllegalArgumentException.class,
                            "No such savepoint.");
                    }
            }});
    }

    /**
     * Tests savepoint deleting.
     */
    public void testOverwriteSavepoints() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
                @Override public void applyx(
                    NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg) {
                    int key = generateKey(cfg, nodes.primaryForKey());

                    for (TxType txType : txTypes) {
                        GridTestUtils.assertThrows(
                            log, () -> {
                                cache.put(key, 0);

                                try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                                    cache.put(key, 1);

                                    tx.savepoint("s1");

                                    cache.put(key, 2);

                                    tx.savepoint("s2");

                                    cache.put(key, 3);

                                    tx.savepoint("s3");

                                    tx.savepoint("s1", true);

                                    tx.rollbackToSavepoint("s2");
                                }

                                return null;
                            },
                            IllegalArgumentException.class,
                            "No such savepoint.");
                    }
            }});
    }

    /**
     * Tests savepoint deleting.
     */
    public void testFailOverwriteSavepoints() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
            @Override public void applyx(
                NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg) {
                int key = generateKey(cfg, nodes.primaryForKey());

                for (TxType txType : txTypes) {
                    GridTestUtils.assertThrows(
                        log, () -> {
                            cache.put(key, 0);

                            try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                                cache.put(key, 1);

                                tx.savepoint("s1");

                                tx.savepoint("s1");
                            }

                            return null;
                        },
                        IllegalArgumentException.class,
                        "Savepoint \"s1\" already exists.");
                }
            }});
    }

    /**
     * Tests rollbacks to the same savepoint instance.
     */
    public void testMultipleRollbacksToSavepoint() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
            @Override public void applyx(
                NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg) {
                int key = generateKey(cfg, nodes.primaryForKey());

                for (TxType txType : txTypes) {
                    cache.put(key, 0);

                    try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                        cache.put(key, 1);

                        tx.savepoint("s1");

                        cache.put(key, 2);

                        tx.savepoint("s2");

                        cache.put(key, 3);

                        tx.savepoint("s3");

                        tx.rollbackToSavepoint("s2");

                        assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                            (Integer)2, cache.get(key));

                        cache.put(key, 4);

                        assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                            (Integer)4, cache.get(key));

                        tx.rollbackToSavepoint("s2");

                        assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                            (Integer)2, cache.get(key));

                        tx.commit();
                    }

                    assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                        (Integer)2, cache.get(key));
                }
            }});
    }

    /**
     * Tests savepoints in failed transaction.
     */
    public void testRollbackAfterRelease() {
        executeTestForAllCaches(
            new CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>>() {
            @Override public void applyx(
                NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg) {
                int key = generateKey(cfg, nodes.primaryForKey());

                for (TxType txType : txTypes) {
                    try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                        cache.put(key, 1);

                        tx.savepoint("s1");

                        cache.put(key, 2);

                        tx.savepoint("s2");

                        cache.put(key, 3);

                        tx.savepoint("s3");

                        tx.releaseSavepoint("s3");

                        tx.rollbackToSavepoint("s2");

                        assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                            (Integer)2, cache.get(key));

                        tx.rollback();
                    }

                    assertEquals("Failed in " + txType.concurrency + ' ' + txType.isolation + " transaction.",
                        null, cache.get(key));
                }
            }});
    }

    /**
     *
     */
    public void testGetAllOutTx() {
        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < 10; i++)
            keys.add(i);

        executeTestForAllCaches(new TestClosure() {
            @Override public void applyx(
                NodeCombination nodes, IgniteCache<Integer, Integer> cache, CacheConfiguration<Integer, Integer> cfg)
                throws IgniteCheckedException {
                for (TxType txType : txTypes) {
                        try (Transaction tx = nodes.txOwner().transactions().txStart(txType.concurrency, txType.isolation)) {
                            tx.savepoint("sp");

                            assertEquals("Broken savepoint in " + txType.concurrency + ' ' + txType.isolation +
                                " transaction.", 0, cache.getAllOutTx(keys).size());

                            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
                                boolean b = true;

                                for (Integer k : keys)
                                    b &= cache.putIfAbsent(k, 1);

                                return b;
                            },cacheMode().name() + "_get");

                            assertTrue(fut.get(1_000));

                            tx.rollbackToSavepoint("sp");

                            assertEquals("Broken multithreaded rollback to savepoint in " + txType.concurrency +
                                    ' ' + txType.isolation + " transaction.",
                                (Integer)keys.size(),
                                (Integer)cache.getAll(keys).values().stream().mapToInt(Integer::intValue).sum());

                            tx.commit();
                        }
                        finally {
                            cache.removeAll();
                        }
                    }
            }});
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> cfgs = new ArrayList<>();

        cfgs.add(cacheConfiguration(PARTITIONED, 0, false));
        cfgs.add(cacheConfiguration(PARTITIONED, 0, true));
        cfgs.add(cacheConfiguration(PARTITIONED, 1, false));
        cfgs.add(cacheConfiguration(PARTITIONED, 1, true));
        cfgs.add(cacheConfiguration(REPLICATED, 0, false));
        cfgs.add(cacheConfiguration(REPLICATED, 0, true));
        cfgs.add(cacheConfiguration(LOCAL, 0, false));
        cfgs.add(cacheConfiguration(LOCAL, 0, true));

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
        boolean nearCache
    ) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName(cacheMode + "_" + backups + (nearCache ? "_near" : "_noNear"));
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());
        else
            ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /**
     * @param c Closure.
     */
    private void executeTestForAllCaches(
        CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>> c) {
        for (CacheConfiguration<Integer, Integer> cfg : cacheConfigurations()) {
            log.info("Run test for cache [cache=" + cfg.getCacheMode() +
                ", backups=" + cfg.getBackups() +
                ", near=" + (cfg.getNearConfiguration() != null) + "]");

            for (NodeCombination nodes : nodeCombinations) {
                if (cfg.getCacheMode() == LOCAL && nodes.txOwnerNodeId != nodes.anotherTxOwnerNodeId)
                    continue;

                info("Nodes [" + nodes + ']');

                IgniteCache<Integer, Integer> cache = nodes.txOwner().getOrCreateCache(cfg);

                c.apply(nodes, cache, cfg);

                cache.clear();
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

    private abstract class TestClosure extends
        CIX3<NodeCombination, IgniteCache<Integer, Integer>, CacheConfiguration<Integer, Integer>> {}

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
