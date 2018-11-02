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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeTest extends GridCommonAbstractTest {
    /** Transaction timeout. */
    private static final long TX_TIMEOUT = 200;

    /** Future timeout */
    private static final int FUT_TIMEOUT = 5000;

    /** */
    private static final int CLIENT_CNT = 2;

    /** */
    private static final int SERVER_CNT = 4;

    /** */
    private static final int GRID_CNT = CLIENT_CNT + SERVER_CNT;

    /**
     * List of closures to execute transaction operation that prohibited in suspended state.
     */
    private static final List<CI1Exc<Transaction>> SUSPENDED_TX_PROHIBITED_OPS = Arrays.asList(
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.suspend();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.close();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.commit();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.commitAsync().get(FUT_TIMEOUT);
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.rollback();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.rollbackAsync().get(FUT_TIMEOUT);
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.setRollbackOnly();
            }
        }
    );

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        boolean client = idx >= SERVER_CNT && idx < GRID_CNT;

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Ignite client = ignite(gridCount() - 1);

        assertTrue(client.cluster().localNode().isClient());

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            grid(0).createCache(ccfg);

            client.createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
        }

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (CacheConfiguration ccfg : cacheConfigurations())
            ignite(0).destroyCache(ccfg.getName());
    }

    /**
     * @return Number of server nodes.
     */
    protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * Test for transaction starting in one thread, continuing in another.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxInAnotherThread() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                    final AtomicInteger cntr = new AtomicInteger(0);

                    cache.put(-1, -1);
                    cache.put(cntr.get(), cntr.getAndIncrement());

                    tx.suspend();

                    assertEquals(SUSPENDED, tx.state());

                    assertNull("Thread already have tx", ignite.transactions().tx());

                    assertNull(cache.get(-1));
                    assertNull(cache.get(cntr.get()));

                    for (int i = 0; i < 10; i++) {
                        GridTestUtils.runAsync(new Runnable() {
                            @Override public void run() {
                                assertEquals(SUSPENDED, tx.state());

                                tx.resume();

                                assertEquals(ACTIVE, tx.state());

                                cache.put(cntr.get(), cntr.getAndIncrement());

                                tx.suspend();
                            }
                        }).get(FUT_TIMEOUT);
                    }

                    tx.resume();

                    cache.remove(-1);

                    tx.commit();

                    assertEquals(COMMITTED, tx.state());

                    for (int i = 0; i < cntr.get(); i++)
                        assertEquals(i, (int)cache.get(i));

                    assertFalse(cache.containsKey(-1));

                    cache.removeAll();
                }
            }
        });
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     * Cache operations performed for a couple of caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTxInAnotherThread() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final IgniteCache<Integer, Integer> otherCache = ignite.getOrCreateCache(
                        cacheConfiguration("otherCache", PARTITIONED, 0, false));

                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                    final AtomicInteger cntr = new AtomicInteger(0);

                    cache.put(-1, -1);
                    otherCache.put(-1, -1);

                    tx.suspend();

                    for (int i = 0; i < 10; i++) {
                        GridTestUtils.runAsync(new Runnable() {
                            @Override public void run() {
                                tx.resume();

                                assertEquals(ACTIVE, tx.state());

                                cache.put(cntr.get(), cntr.get());
                                otherCache.put(cntr.get(), cntr.getAndIncrement());

                                tx.suspend();
                            }
                        }).get(FUT_TIMEOUT);
                    }

                    tx.resume();

                    cache.remove(-1);
                    otherCache.remove(-1);

                    tx.commit();

                    assertEquals(COMMITTED, tx.state());

                    for (int i = 0; i < cntr.get(); i++) {
                        assertEquals(i, (int)cache.get(i));
                        assertEquals(i, (int)otherCache.get(i));
                    }

                    assertFalse(cache.containsKey(-1));
                    assertFalse(otherCache.containsKey(-1));

                    cache.removeAll();
                    otherCache.removeAll();
                }
            }
        });
    }

    /**
     * Test for transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTxRollback() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                    cache.put(1, 1);
                    cache.put(2, 2);

                    tx.suspend();

                    assertNull("There is no transaction for current thread", ignite.transactions().tx());

                    assertEquals(SUSPENDED, tx.state());

                    GridTestUtils.runAsync(new Runnable() {
                        @Override public void run() {
                            tx.resume();

                            assertEquals(ACTIVE, tx.state());

                            cache.put(3, 3);

                            tx.rollback();
                        }
                    }).get(FUT_TIMEOUT);

                    assertTrue(GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return tx.state() == ROLLED_BACK;
                        }
                    }, getTestTimeout()));

                    assertEquals(ROLLED_BACK, tx.state());

                    assertFalse(cache.containsKey(1));
                    assertFalse(cache.containsKey(2));
                    assertFalse(cache.containsKey(3));

                    cache.removeAll();
                }
            }
        });
    }

    /**
     * Test for starting and suspending transactions, and then resuming and committing in another thread.
     *
     * @throws Exception If failed.
     */
    public void testMultiTxSuspendResume() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final List<Transaction> clientTxs = new ArrayList<>();

                    for (int i = 0; i < 10; i++) {
                        Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                        cache.put(i, i);

                        tx.suspend();

                        clientTxs.add(tx);
                    }

                    GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                        @Override public void applyx(Integer idx) throws Exception {
                            Transaction tx = clientTxs.get(idx);

                            assertEquals(SUSPENDED, tx.state());

                            tx.resume();

                            assertEquals(ACTIVE, tx.state());

                            tx.commit();
                        }
                    }, 10, "th-suspend");

                    for (int i = 0; i < 10; i++)
                        assertEquals(i, (int)cache.get(i));

                    cache.removeAll();
                }
            }
        });
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction from the other thread are prohibited.
     *
     * @throws Exception If failed.
     */
    public void testOpsProhibitedOnSuspendedTxFromOtherThread() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (final CI1Exc<Transaction> txOperation : SUSPENDED_TX_PROHIBITED_OPS) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                        cache.put(1, 1);

                        tx.suspend();

                        multithreaded(new RunnableX() {
                            @Override public void runx() throws Exception {
                                GridTestUtils.assertThrowsWithCause(txOperation, tx, IgniteException.class);
                            }
                        }, 1);

                        tx.resume();
                        tx.close();

                        assertNull(cache.get(1));
                    }
                }
            }
        });
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction are prohibited.
     *
     * @throws Exception If failed.
     */
    public void testOpsProhibitedOnSuspendedTx() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (CI1Exc<Transaction> txOperation : SUSPENDED_TX_PROHIBITED_OPS) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                        cache.put(1, 1);

                        tx.suspend();

                        GridTestUtils.assertThrowsWithCause(txOperation, tx, IgniteException.class);

                        tx.resume();
                        tx.close();

                        assertNull(cache.get(1));
                    }
                }
            }
        });
    }

    /**
     * Test checking timeout on resumed transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxTimeoutOnResumed() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation, TX_TIMEOUT, 0);

                    cache.put(1, 1);

                    tx.suspend();

                    long finishTime = U.currentTimeMillis() + TX_TIMEOUT;

                    do {
                        U.sleep(TX_TIMEOUT + (TX_TIMEOUT / 4));
                    }
                    while (finishTime >= U.currentTimeMillis());

                    GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            tx.resume();

                            return null;
                        }
                    }, TransactionTimeoutException.class);

                    assertTrue(GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return tx.state() == ROLLED_BACK;
                        }
                    }, getTestTimeout()));

                    assertEquals(ROLLED_BACK, tx.state());

                    tx.close();
                }
            }
        });
    }

    /**
     * Test checking timeout on suspended transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxTimeoutOnSuspend() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation, TX_TIMEOUT, 0);

                    cache.put(1, 1);

                    long finishTime = U.currentTimeMillis() + TX_TIMEOUT;

                    do {
                        U.sleep(TX_TIMEOUT + (TX_TIMEOUT / 4));
                    }
                    while (finishTime >= U.currentTimeMillis());

                    GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            tx.suspend();

                            return null;
                        }
                    }, TransactionTimeoutException.class);

                    assertTrue(GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return tx.state() == ROLLED_BACK;
                        }
                    }, getTestTimeout()));

                    assertEquals(ROLLED_BACK, tx.state());

                    tx.close();

                    assertNull(cache.get(1));
                }
            }
        });
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNew() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
                    for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                        Transaction tx1 = ignite.transactions().txStart(OPTIMISTIC, tx1Isolation);

                        cache.put(1, 1);

                        tx1.suspend();

                        assertFalse(cache.containsKey(1));

                        Transaction tx2 = ignite.transactions().txStart(OPTIMISTIC, tx2Isolation);

                        cache.put(1, 2);

                        tx2.commit();

                        assertEquals(2, (int)cache.get(1));

                        tx1.resume();

                        assertEquals(1, (int)cache.get(1));

                        tx1.close();

                        cache.removeAll();
                    }
                }
            }
        });
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key.
     */
    public void testSuspendTxAndStartNewWithoutCommit() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
                    for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                        Transaction tx1 = ignite.transactions().txStart(OPTIMISTIC, tx1Isolation);

                        cache.put(1, 1);

                        tx1.suspend();

                        assertFalse(cache.containsKey(1));

                        Transaction tx2 = ignite.transactions().txStart(OPTIMISTIC, tx2Isolation);

                        cache.put(1, 2);

                        tx2.suspend();

                        assertFalse(cache.containsKey(1));

                        tx1.resume();

                        assertEquals(1, (int)cache.get(1));

                        tx1.suspend();

                        tx2.resume();

                        assertEquals(2, (int)cache.get(1));

                        tx2.rollback();

                        tx1.resume();
                        tx1.rollback();

                        cache.removeAll();
                    }
                }
            }
        });
    }

    /**
     * Test we can resume and complete transaction if topology changed while transaction is suspended.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndResumeAfterTopologyChange() throws Exception {
        Ignite srv = ignite(ThreadLocalRandom.current().nextInt(SERVER_CNT));
        Ignite client = ignite(SERVER_CNT);
        Ignite clientNear = ignite(SERVER_CNT + 1);

        Map<String, List<List<Integer>>> cacheKeys = generateKeys(srv, TransactionIsolation.values().length);

        for (Ignite node : new Ignite[] {srv, client, clientNear}) {
            ClusterNode locNode = node.cluster().localNode();

            log.info("Run test for node [node=" + locNode.id() + ", client=" + locNode.isClient() + ']');

            doCheckSuspendTxAndResume(node, cacheKeys);
        }
    }

    /**
     * @param node Ignite isntance.
     * @param cacheKeys Different key types mapped to cache name.
     * @throws Exception If failed.
     */
    private void doCheckSuspendTxAndResume(Ignite node, Map<String, List<List<Integer>>> cacheKeys) throws Exception {
        List<T2<IgniteCache<Integer, Integer>, List<T2<Transaction, Integer>>>> cacheTxMapping = new ArrayList<>();

        for (Map.Entry<String, List<List<Integer>>> cacheKeysEntry : cacheKeys.entrySet()) {
            String cacheName = cacheKeysEntry.getKey();

            IgniteCache<Integer, Integer> cache = node.cache(cacheName);

            List<T2<Transaction, Integer>> txs = new ArrayList<>();

            for (List<Integer> keysList : cacheKeysEntry.getValue()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    Transaction tx = node.transactions().txStart(OPTIMISTIC, isolation);

                    int key = keysList.get(isolation.ordinal());

                    cache.put(key, key);

                    tx.suspend();

                    txs.add(new T2<>(tx, key));

                    String msg = "node=" + node.cluster().localNode() +
                        ", cache=" + cacheName + ", isolation=" + isolation + ", key=" + key;

                    assertEquals(msg, SUSPENDED, tx.state());
                }
            }

            cacheTxMapping.add(new T2<>(cache, txs));
        }

        int newNodeIdx = gridCount();

        startGrid(newNodeIdx);

        try {
            for (T2<IgniteCache<Integer, Integer>, List<T2<Transaction, Integer>>>  entry : cacheTxMapping) {
                IgniteCache<Integer, Integer> cache = entry.getKey();

                List<T2<Transaction, Integer>> txEntries = entry.getValue();

                for (T2<Transaction, Integer> txEntry : txEntries) {
                    Transaction tx = txEntry.get1();

                    Integer key = txEntry.get2();

                    tx.resume();

                    String msg = "node=" + node.cluster().localNode() +
                        ", cache=" + cache.getName() + ", isolation=" + tx.isolation() + ", key=" + key;

                    assertEquals(msg, ACTIVE, tx.state());

                    assertEquals(msg, key, cache.get(key));

                    tx.commit();

                    assertEquals(msg, key, cache.get(key));
                }
            }
        } finally {
            stopGrid(newNodeIdx);

            for (T2<IgniteCache<Integer, Integer>, List<T2<Transaction, Integer>>> entry : cacheTxMapping)
                entry.getKey().removeAll();
        }
    }

    /**
     * Test for correct exception handling when misuse transaction API - resume active tx.
     *
     * @throws Exception If failed.
     */
    public void testResumeActiveTx() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                    cache.put(1, 1);

                    try {
                        tx.resume();

                        fail("Exception must be thrown");
                    }
                    catch (Throwable e) {
                        assertTrue(X.hasCause(e, IgniteException.class));

                        assertFalse(X.hasCause(e, AssertionError.class));
                    }

                    tx.close();

                    assertFalse(cache.containsKey(1));
                }
            }
        });
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> cfgs = new ArrayList<>();

        cfgs.add(cacheConfiguration("cache1", PARTITIONED, 0, false));
        cfgs.add(cacheConfiguration("cache2", PARTITIONED, 1, false));
        cfgs.add(cacheConfiguration("cache3", PARTITIONED, 1, true));
        cfgs.add(cacheConfiguration("cache4", REPLICATED, 0, false));

        return cfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        String name,
        CacheMode cacheMode,
        int backups,
        boolean nearCache) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(name);

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
     * @param c Closure.
     */
    private void executeTestForAllCaches(CI2<Ignite, IgniteCache<Integer, Integer>> c) {
        for (int i = 0; i < gridCount(); i++) {
            Ignite ignite = ignite(i);

            ClusterNode locNode = ignite.cluster().localNode();

            log.info("Run test for node [node=" + locNode.id() + ", client=" + locNode.isClient() + ']');

            for (CacheConfiguration ccfg : cacheConfigurations())
                c.apply(ignite, ignite.cache(ccfg.getName()));
        }
    }

    /**
     * Generates list of keys.
     *
     * @param ignite Ignite instance.
     * @return List of different type keys mapped to cache.
     */
    private Map<String, List<List<Integer>>> generateKeys(Ignite ignite, int keysCnt) {
        Map<String, List<List<Integer>>> cacheKeys = new HashMap<>();

        for (CacheConfiguration cfg : cacheConfigurations()) {
            String cacheName = cfg.getName();

            IgniteCache cache = ignite.cache(cacheName);

            List<List<Integer>> keys = new ArrayList<>();

            for (int type = 0; type < 3; type++) {
                if (type == 1 && cfg.getCacheMode() == PARTITIONED && cfg.getBackups() == 0)
                    continue;

                if (type == 2 && cfg.getCacheMode() == REPLICATED)
                    continue;

                List<Integer> keys0 = findKeys(cache, keysCnt, type * 100_000, type);

                assertEquals(cacheName, keysCnt, keys0.size());

                keys.add(keys0);
            }

            cacheKeys.put(cacheName, keys);
        }

        return cacheKeys;
    }

    /**
     * Closure with 2 parameters that can throw any exception.
     *
     * @param <E1> Type of first closure parameter.
     * @param <E2> Type of second closure parameter.
     */
    public abstract static class CI2Exc<E1, E2> implements CI2<E1, E2> {
        /**
         * Closure body.
         *
         * @param e1 First closure argument.
         * @param e2 Second closure argument.
         * @throws Exception If failed.
         */
        public abstract void applyx(E1 e1, E2 e2) throws Exception;

        /** {@inheritDoc} */
        @Override public void apply(E1 e1, E2 e2) {
            try {
                applyx(e1, e2);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Closure that can throw any exception.
     *
     * @param <T> Type of closure parameter.
     */
    public abstract static class CI1Exc<T> implements CI1<T> {
        /**
         * Closure body.
         *
         * @param o Closure argument.
         * @throws Exception If failed.
         */
        public abstract void applyx(T o) throws Exception;

        /** {@inheritDoc} */
        @Override public void apply(T o) {
            try {
                applyx(o);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Runnable that can throw any exception.
     */
    public abstract static class RunnableX implements Runnable {
        /**
         * Closure body.
         *
         * @throws Exception If failed.
         */
        public abstract void runx() throws Exception;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                runx();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
