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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 *
 */
public abstract class IgniteAbstractTxSuspendResumeTest extends GridCommonAbstractTest {
    /** Force mvcc. */
    protected static final boolean FORCE_MVCC =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, false);

    /** Transaction timeout. */
    private static final long TX_TIMEOUT = 400L;

    /** Future timeout */
    protected static final int FUT_TIMEOUT = 5000;

    /** */
    protected static final int CLIENT_CNT = 2;

    /** */
    protected static final int SERVER_CNT = 4;

    /** */
    protected static final int GRID_CNT = CLIENT_CNT + SERVER_CNT;

    /**
     * List of closures to execute transaction operation that prohibited in suspended state.
     */
    private static final List<CI1<Transaction>> SUSPENDED_TX_PROHIBITED_OPS = Arrays.asList(
        Transaction::suspend,
        Transaction::close,
        Transaction::commit,
        tx -> tx.commitAsync().get(FUT_TIMEOUT),
        Transaction::rollback,
        tx -> tx.rollbackAsync().get(FUT_TIMEOUT),
        Transaction::setRollbackOnly
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

            if (ccfg.getCacheMode() != LOCAL && !FORCE_MVCC)
                client.createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
        }

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            ignite(0).destroyCache(ccfg.getName());

        super.afterTest();
    }

    /**
     * @return Number of server nodes.
     */
    protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @return Transaction concurrency.
     */
    protected abstract TransactionConcurrency transactionConcurrency();

    /**
     * Test for transaction starting in one thread, continuing in another.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResumeTxInAnotherThread() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                    final AtomicInteger cntr = new AtomicInteger(0);

                    for (int j = -1; j > -10; j--)
                        cache.put(j, j);

                    cache.put(cntr.get(), cntr.getAndIncrement());

                    tx.suspend();

                    assertEquals(SUSPENDED, tx.state());

                    assertNull("Thread already have tx", ignite.transactions().tx());

                    assertNull(cache.get(-1));
                    assertNull(cache.get(cntr.get()));

                    for (int i = 0; i < 10; i++) {
                        GridTestUtils.runAsync(() -> {
                            assertEquals(SUSPENDED, tx.state());

                            tx.resume();

                            assertEquals(ACTIVE, tx.state());

                            for (int j = -1; j > -10; j--)
                                cache.put(j, j);

                            cache.put(cntr.get(), cntr.getAndIncrement());

                            tx.suspend();
                        }).get(FUT_TIMEOUT);
                    }

                    tx.resume();

                    for (int j = -1; j > -10; j--)
                        cache.remove(j);

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
    @Test
    public void testCrossCacheTxInAnotherThread() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                // TODO: IGNITE-9110 Optimistic tx hangs in cross-cache operations with LOCAL and non LOCAL caches.
                if (transactionConcurrency() == TransactionConcurrency.OPTIMISTIC
                    && cache.getConfiguration(CacheConfiguration.class).getCacheMode() == LOCAL)
                    return;

                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final IgniteCache<Integer, Integer> otherCache = ignite.getOrCreateCache(
                        cacheConfiguration("otherCache", PARTITIONED, 0, false));

                    final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                    final AtomicInteger cntr = new AtomicInteger(0);

                    cache.put(-1, -1);
                    otherCache.put(-1, -1);

                    tx.suspend();

                    for (int i = 0; i < 10; i++) {
                        GridTestUtils.runAsync(() -> {
                            tx.resume();

                            assertEquals(ACTIVE, tx.state());

                            cache.put(-1, -1);
                            otherCache.put(-1, -1);

                            cache.put(cntr.get(), cntr.get());
                            otherCache.put(cntr.get(), cntr.getAndIncrement());

                            tx.suspend();
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
    @Test
    public void testTxRollback() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                    cache.put(1, 1);
                    cache.put(2, 2);

                    tx.suspend();

                    assertNull("There is no transaction for current thread", ignite.transactions().tx());

                    assertEquals(SUSPENDED, tx.state());

                    GridTestUtils.runAsync(() -> {
                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache.put(3, 3);

                        tx.rollback();
                    }).get(FUT_TIMEOUT);

                    assertTrue(GridTestUtils.waitForCondition(() -> tx.state() == ROLLED_BACK, getTestTimeout()));

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
    @Test
    public void testMultiTxSuspendResume() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final List<Transaction> clientTxs = new ArrayList<>();

                    for (int i = 0; i < 10; i++) {
                        Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                        cache.put(i, i);

                        tx.suspend();

                        clientTxs.add(tx);
                    }

                    GridTestUtils.runMultiThreaded(new CI1<Integer>() {
                        @Override public void apply(Integer idx) {
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
    @Test
    public void testOpsProhibitedOnSuspendedTxFromOtherThread() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (final CI1<Transaction> txOperation : SUSPENDED_TX_PROHIBITED_OPS) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                        cache.put(1, 1);

                        tx.suspend();

                        multithreaded(() -> GridTestUtils.assertThrowsWithCause(txOperation, tx, IgniteException.class),
                            1);

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
    @Test
    public void testOpsProhibitedOnSuspendedTx() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (CI1<Transaction> txOperation : SUSPENDED_TX_PROHIBITED_OPS) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

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
    @Test
    public void testTxTimeoutOnResumed() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = initTxWithTimeout(ignite, cache, isolation, true);

                    while (tx.startTime() + TX_TIMEOUT >= U.currentTimeMillis())
                        U.sleep(100L);

                    GridTestUtils.assertThrowsWithCause(tx::resume, TransactionTimeoutException.class);

                    assertTrue(GridTestUtils.waitForCondition(() -> tx.state() == ROLLED_BACK, getTestTimeout()));

                    assertEquals(ROLLED_BACK, tx.state());

                    // Here we check that we can start any transactional operation in the same thread after a suspended
                    // transaction is timed-out.
                    assertFalse(cache.containsKey(1));

                    tx.close();

                    assertFalse(cache.containsKey(1));
                    assertFalse(cache.containsKey(2));
                    assertFalse(cache.containsKey(3));
                }
            }
        });
    }

    /**
     * Test checking timeout on suspended transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTxTimeoutOnSuspend() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = initTxWithTimeout(ignite, cache, isolation, false);

                    while (tx.startTime() + TX_TIMEOUT >= U.currentTimeMillis())
                        U.sleep(100L);

                    GridTestUtils.assertThrowsWithCause(tx::suspend, TransactionTimeoutException.class);

                    assertTrue(GridTestUtils.waitForCondition(() -> tx.state() == ROLLED_BACK, getTestTimeout()));

                    assertEquals(ROLLED_BACK, tx.state());

                    tx.close();

                    assertNull(cache.get(1));
                    assertNull(cache.get(2));
                    assertNull(cache.get(3));
                }
            }
        });
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param isolation Isolation.
     * @param suspended Left transaction in suspended state.
     */
    private Transaction initTxWithTimeout(Ignite ignite, IgniteCache cache, TransactionIsolation isolation,
        boolean suspended) throws Exception {
        final int RETRIES = 5;

        // Make several attempts to init transaction, sometimes it takes more time then given timeout.
        for (int i = 0; i < RETRIES; i++) {
            try {
                final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation,
                    TX_TIMEOUT, 0);

                cache.put(1, 1);

                tx.suspend();

                GridTestUtils.runAsync(() -> {
                    tx.resume();

                    cache.put(1, 1);
                    cache.put(2, 2);

                    tx.suspend();
                }).get(FUT_TIMEOUT);

                tx.resume();

                cache.put(1, 1);
                cache.put(2, 2);
                cache.put(3, 3);

                if (suspended)
                    tx.suspend();

                return tx;
            }
            catch (Exception e) {
                if (X.hasCause(e, TransactionTimeoutException.class)) {
                    if (i == RETRIES - 1) {
                        throw new Exception("Can't init transaction within given timeout [isolation=" +
                            isolation + ", cache=" + cache.getName() + ", ignite=" +
                            ignite.configuration().getIgniteInstanceName() + ']', e);
                    }
                    else {
                        log.info("Got timeout on transaction init [attempt=" + i + ", isolation=" + isolation +
                            ", cache=" + cache.getName() + ", ignite=" + ignite.configuration().getIgniteInstanceName() + ']');
                    }
                }
                else
                    throw e;
            }
        }

        return null;
    }

    /**
     * Test for correct exception handling when misuse transaction API - resume active tx.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResumeActiveTx() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

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
    protected List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> cfgs = new ArrayList<>();

        cfgs.add(cacheConfiguration("cache1", PARTITIONED, 0, false));
        cfgs.add(cacheConfiguration("cache2", PARTITIONED, 1, false));
        cfgs.add(cacheConfiguration("cache3", PARTITIONED, 1, true));
        cfgs.add(cacheConfiguration("cache4", REPLICATED, 0, false));

        if (!FORCE_MVCC)
            cfgs.add(cacheConfiguration("cache5", LOCAL, 0, false));

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
    protected void executeTestForAllCaches(CI2<Ignite, IgniteCache<Integer, Integer>> c) {
        for (int i = 0; i < gridCount(); i++) {
            Ignite ignite = ignite(i);

            ClusterNode locNode = ignite.cluster().localNode();

            log.info(">>> Run test for node [node=" + locNode.id() + ", client=" + locNode.isClient() + ']');

            for (CacheConfiguration ccfg : cacheConfigurations()) {
                if (locNode.isClient() && ccfg.getCacheMode() == LOCAL)
                    continue;

                log.info(">>>> Run test for cache " + ccfg.getName());

                c.apply(ignite, ignite.cache(ccfg.getName()));
            }
        }
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
}
