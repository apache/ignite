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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.PA;
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
    private boolean client = false;

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

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(serversNumber());

        if (serversNumber() > 1) {
            client = true;

            startGrid(serversNumber());

            startGrid(serversNumber() + 1);

            client = false;
        }

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @return Number of server nodes.
     */
    protected int serversNumber() {
        return 1;
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
                    final IgniteCache<Integer, Integer> otherCache =
                        ignite.getOrCreateCache(cacheConfiguration(PARTITIONED, 0, false).setName("otherCache"));

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

                    long start = U.currentTimeMillis();

                    while(TX_TIMEOUT >= U.currentTimeMillis() - start)
                        Thread.sleep(TX_TIMEOUT * 2);

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

                    long start = U.currentTimeMillis();

                    while(TX_TIMEOUT >= U.currentTimeMillis() - start)
                        Thread.sleep(TX_TIMEOUT * 2);

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
     *
     * @throws Exception If failed.
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
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                    cache.put(1, 1);

                    tx.suspend();

                    assertEquals(SUSPENDED, tx.state());

                    try (IgniteEx g = startGrid(serversNumber() + 3)) {
                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        assertEquals(1, (int)cache.get(1));

                        tx.commit();

                        assertEquals(1, (int)cache.get(1));
                    }

                    cache.removeAll();
                }
            }
        });
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
     * @param c Closure.
     * @throws Exception If failed.
     */
    private void executeTestForAllCaches(CI2<Ignite, IgniteCache<Integer, Integer>> c) throws Exception {
        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            ignite(0).createCache(ccfg);

            log.info("Run test for cache [cache=" + ccfg.getCacheMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + (ccfg.getNearConfiguration() != null) + "]");

            awaitPartitionMapExchange();

            int srvNum = serversNumber();
            if (serversNumber() > 1) {
                ignite(serversNumber() + 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
                srvNum += 2;
            }

            try {
                for (int i = 0; i < srvNum; i++) {
                    Ignite ignite = ignite(i);

                    log.info("Run test for node [node=" + i + ", client=" + ignite.configuration().isClientMode() + ']');

                    c.apply(ignite, ignite.<Integer, Integer>cache(ccfg.getName()));
                }
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * Closure with 2 parameters that can throw any exception.
     *
     * @param <E1> Type of first closure parameter.
     * @param <E2> Type of second closure parameter.
     */
    public static abstract class CI2Exc<E1, E2> implements CI2<E1, E2> {
        /**
         * Closure body.
         *
         * @param e1 First closure argument.
         * @param e2 Second closure argument.
         * @throws Exception If failed.
         */
        public abstract void applyx(E1 e1, E2 e2) throws Exception;

        /** {@inheritdoc} */
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
    public static abstract class CI1Exc<T> implements CI1<T> {
        /**
         * Closure body.
         *
         * @param o Closure argument.
         * @throws Exception If failed.
         */
        public abstract void applyx(T o) throws Exception;

        /** {@inheritdoc} */
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
    public static abstract class RunnableX implements Runnable {
        /**
         * Closure body.
         *
         * @throws Exception If failed.
         */
        public abstract void runx() throws Exception;

        /** {@inheritdoc} */
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
