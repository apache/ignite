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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
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
    private static final long TX_TIMEOUT = 100;

    /** Future timeout */
    private static final int FUT_TIMEOUT = 5000;

    /**
     * List of closures to execute transaction operation that prohibited in suspended state.
     */
    private static final List<CI1Exc<Transaction>> SUSP_TX_PROHIB_OPS = Arrays.asList(
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

        cfg.setClientMode(false);
        cfg.setCacheConfiguration(defaultCacheConfiguration()
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        jcache().removeAll();
    }

    /**
     * Test for transaction starting in one thread, continuing in another.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxInAnotherThread() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteCache<Integer, String> cache = jcache();

            final Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation);

            cache.put(0, "0");
            cache.put(20, "20");

            tx.suspend();

            assertEquals(SUSPENDED, tx.state());

            assertNull(grid().transactions().tx());

            assertNull(cache.get(1));

            for (int i = 1; i < 4; i++) {
                final int finalI = i;
                GridTestUtils.runAsync(new Runnable() {
                    @Override public void run() {
                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache.put(finalI, Integer.toString(finalI));

                        tx.suspend();
                    }
                }).get(FUT_TIMEOUT);
            }

            tx.resume();

            cache.remove(20);

            tx.commit();

            assertEquals(COMMITTED, tx.state());

            for (int i = 0; i < 4; i++)
                assertEquals(Integer.toString(i), cache.get(i));

            assertFalse(cache.containsKey(20));

            cache.removeAll();
        }
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     * Cache operations performed for a couple of caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTxInAnotherThread() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteCache<Integer, String> cache1 =
                grid().getOrCreateCache("cache1");

            final IgniteCache<Integer, String> cache2 =
                grid().getOrCreateCache("cache2");

            final Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation);

            cache1.put(10, "0");
            cache2.put(20, "0");

            tx.suspend();

            for (int i = 1; i < 4; i++) {
                final int finalI = i;
                GridTestUtils.runAsync(new Runnable() {
                    @Override public void run() {
                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache1.put(10 + finalI, Integer.toString(finalI));
                        cache2.put(20 + finalI, Integer.toString(finalI));

                        tx.suspend();
                    }
                }).get(FUT_TIMEOUT);
            }

            tx.resume();

            cache1.remove(10);
            cache2.remove(20);

            tx.commit();

            assertEquals(COMMITTED, tx.state());

            for (int i = 1; i < 4; i++) {
                assertEquals(Integer.toString(i), cache1.get(10 + i));
                assertEquals(Integer.toString(i), cache2.get(20 + i));
            }

            assertFalse(cache1.containsKey(10));
            assertFalse(cache2.containsKey(20));

            cache1.removeAll();
            cache2.removeAll();
        }
    }

    /**
     * Test for transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTxRollback() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteCache<Integer, String> cache = jcache();

            final Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation);

            cache.put(1, "1");
            cache.put(2, "2");

            tx.suspend();

            assertNull(grid().transactions().tx());

            assertEquals(SUSPENDED, tx.state());

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    cache.put(3, "3");

                    tx.rollback();
                }
            }).get(FUT_TIMEOUT);

            assertEquals(ROLLED_BACK, tx.state());

            assertFalse(cache.containsKey(1));
            assertFalse(cache.containsKey(2));
            assertFalse(cache.containsKey(3));

            cache.removeAll();
        }
    }

    /**
     * Test for starting and suspending transactions, and then resuming and committing in another thread.
     *
     * @throws Exception If failed.
     */
    public void testMultiTxSuspendResume() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            IgniteCache<Integer, String> cache = jcache();

            final List<Transaction> clientTxs = new ArrayList<>();

            for (int i = 0; i < 3; i++) {
                Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation);

                cache.put(i, Integer.toString(i));

                tx.suspend();

                clientTxs.add(tx);
            }

            GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                public void applyx(Integer idx) throws Exception {
                    Transaction tx = clientTxs.get(idx);

                    assertEquals(SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    tx.commit();
                }
            }, 3, "th-suspend");

            for (int i = 0; i < 3; i++)
                assertEquals(Integer.toString(i), cache.get(i));

            cache.removeAll();
        }
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction from the other thread are prohibited.
     *
     * @throws Exception If failed.
     */
    public void testOpsProhibitedOnSuspendedTxFromOtherThread() throws Exception {
        for (final CI1Exc<Transaction> txOperation : SUSP_TX_PROHIB_OPS) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final IgniteCache<Integer, String> cache = jcache();

                final Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");

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

    /**
     * Test checking all operations(exception resume) on suspended transaction are prohibited.
     *
     * @throws Exception If failed.
     */
    public void testOpsProhibitedOnSuspendedTx() throws Exception {
        for (CI1Exc<Transaction> txOperation : SUSP_TX_PROHIB_OPS) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final IgniteCache<Integer, String> cache = jcache();

                Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");

                tx.suspend();

                GridTestUtils.assertThrowsWithCause(txOperation, tx, IgniteException.class);

                tx.resume();
                tx.close();

                assertNull(cache.get(1));
            }
        }
    }

    /**
     * Test checking timeout on resumed transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxTimeoutOnResumed() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation, TX_TIMEOUT, 0);

            jcache().put(1, "1");

            tx.suspend();

            Thread.sleep(TX_TIMEOUT * 2);

            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    tx.resume();

                    return null;
                }
            }, TransactionTimeoutException.class);

            tx.close();
        }
    }

    /**
     * Test checking timeout on suspended transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxTimeoutOnSuspend() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final Transaction tx = grid().transactions().txStart(OPTIMISTIC, isolation, TX_TIMEOUT, 0);

            jcache().put(1, "1");

            Thread.sleep(TX_TIMEOUT * 2);

            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    tx.suspend();

                    return null;
                }
            }, TransactionTimeoutException.class);

            tx.close();

            assertNull(jcache().get(1));
        }
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNew() throws Exception {
        for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
            for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                IgniteCache<Integer, String> cache = grid().cache(DEFAULT_CACHE_NAME);

                Transaction tx1 = grid().transactions().txStart(OPTIMISTIC, tx1Isolation);

                cache.put(1, "1");

                tx1.suspend();

                Transaction tx2 = grid().transactions().txStart(OPTIMISTIC, tx2Isolation);

                cache.put(1, "2");

                tx2.commit();

                assertEquals("2", cache.get(1));

                tx1.resume();
                tx1.close();

                cache.removeAll();
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
