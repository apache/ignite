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
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionTimeoutException;

/**
 *
 */
public class OptimisticTransactionsInMultipleThreadsTest extends AbstractTransactionsInMultipleThreadsTest {
    /** Name for test cache */
    private static final String TEST_CACHE_NAME = "testCache";

    /** Name for second test cache */
    private static final String TEST_CACHE_NAME2 = "testCache2";

    /** Transaction timeout. */
    private static final long TIMEOUT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);
    }

    /**
     * Test for transaction starting in one thread, continuing in another.
     *
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThread() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                simpleTransactionInAnotherThread();

                return null;
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void simpleTransactionInAnotherThread() throws IgniteCheckedException {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        assertNull(cache.get("key1"));

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);

        tx.suspend();

        assertNull(cache.get("key1"));

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache.remove("key2");

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals((long)1, (long)cache.get("key1"));
        assertEquals((long)3, (long)cache.get("key3"));
        assertFalse(cache.containsKey("key2"));

        cache.removeAll();
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     *
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThreadContinued() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                simpleTransactionInAnotherThreadContinued();

                return null;
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void simpleTransactionInAnotherThreadContinued() throws IgniteCheckedException {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        assertNull(cache.get("key1"));

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key1'", 1);

        tx.suspend();

        assertNull(cache.get("key1"));

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache.put("key2'", 2);
                cache.remove("key2", 2);

                tx.suspend();

                return true;
            }
        });

        fut.get(5000);

        assertNull(txs.tx());
        assertEquals(TransactionState.SUSPENDED, tx.state());

        tx.resume();

        assertEquals(TransactionState.ACTIVE, tx.state());

        cache.remove("key1'", 1);
        cache.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals((long)1, (long)cache.get("key1"));
        assertEquals((long)3, (long)cache.get("key3"));
        assertEquals((long)3, (long)cache.get("key3'"));
        assertFalse(cache.containsKey("key2"));
        assertFalse(cache.containsKey("key1'"));
        assertFalse(cache.containsKey("key2'"));

        cache.removeAll();
    }

    /**
     * Test for transaction starting in one thread, continuing in another. Cache operations performed for a couple of
     * caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                crossCacheTransactionInAnotherThread();

                return null;
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void crossCacheTransactionInAnotherThread() throws IgniteCheckedException {
        Ignite ignite = ignite(txInitiatorNodeId);
        final IgniteTransactions txs = ignite.transactions();
        final IgniteCache<String, Integer> cache1 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME));
        final IgniteCache<String, Integer> cache2 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME2));

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        cache1.put("key1", 1);
        cache2.put("key2", 2);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache1.put("key3", 3);
                cache2.remove("key2");

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals((long)1, (long)cache1.get("key1"));
        assertEquals((long)3, (long)cache1.get("key3"));
        assertFalse(cache2.containsKey("key2"));

        cache2.removeAll();
        cache1.removeAll();
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     * Cache operations performed for a couple of caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThreadContinued() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                crossCacheTransactionInAnotherThreadContinued();

                return null;
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void crossCacheTransactionInAnotherThreadContinued() throws IgniteCheckedException {
        Ignite ignite = ignite(txInitiatorNodeId);
        final IgniteTransactions txs = ignite.transactions();
        final IgniteCache<String, Integer> cache1 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME));
        final IgniteCache<String, Integer> cache2 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME2));

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        cache1.put("key1", 1);
        cache2.put("key2", 2);
        cache1.put("key1'", 1);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache1.put("key3", 3);
                cache2.put("key2'", 2);
                cache2.remove("key2");

                tx.suspend();

                return true;
            }
        });

        fut.get(5000);

        assertNull(txs.tx());
        assertEquals(TransactionState.SUSPENDED, tx.state());

        tx.resume();

        assertEquals(TransactionState.ACTIVE, tx.state());

        cache1.remove("key1'", 1);
        cache2.remove("key2'", 2);
        cache1.put("key3'", 3);

        tx.commit();

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals((long)1, (long)cache1.get("key1"));
        assertEquals((long)3, (long)cache1.get("key3"));
        assertEquals((long)3, (long)cache1.get("key3'"));
        assertFalse(cache2.containsKey("key2"));
        assertFalse(cache2.containsKey("key2'"));
        assertFalse(cache1.containsKey("key1'"));

        cache1.removeAll();
        cache2.removeAll();
    }

    /**
     * Test for transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTransactionRollback() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                transactionRollback();

                return null;
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void transactionRollback() throws IgniteCheckedException {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);

        tx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);

                assertTrue(cache.remove("key2"));

                tx.rollback();

                return true;
            }
        });

        fut.get(5000);

        assertEquals(TransactionState.ROLLED_BACK, tx.state());
        assertFalse(cache.containsKey("key1"));
        assertFalse(cache.containsKey("key2"));
        assertFalse(cache.containsKey("key3"));

        cache.removeAll();
    }

    /**
     * Test for starting and suspending transactions, and then resuming and committing in another thread.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testMultipleTransactionsSuspendResume() throws IgniteCheckedException {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            transactionIsolation = isolation;

            multipleTransactionsSuspendResume();
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void multipleTransactionsSuspendResume() throws IgniteCheckedException {
        final List<Transaction> txs = new ArrayList<>();
        IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        Ignite clientNode = ignite(txInitiatorNodeId);
        Transaction clientTx;

        for (int i = 0; i < 10; i++) {
            clientTx = clientNode.transactions().txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

            clientCache.put("1", i);

            clientTx.suspend();

            txs.add(clientTx);
        }

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(ignite(txInitiatorNodeId).transactions().tx());

                for (int i = 0; i < 10; i++) {
                    Transaction clientTx = txs.get(i);

                    assertEquals(TransactionState.SUSPENDED, clientTx.state());

                    clientTx.resume();

                    assertEquals(TransactionState.ACTIVE, clientTx.state());

                    clientTx.commit();
                }

                return true;
            }
        });

        fut.get(5000);

        assertEquals(9, jcache(0).get("1"));

        clientCache.removeAll();
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction are prohibited.
     */
    public void testOperationsAreProhibitedOnSuspendedTx() {
        for (int opIdx = 0; opIdx < 7; opIdx++)
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                transactionIsolation = isolation;

                operationsAreProhibitedOnSuspendedTx(opIdx);
            }
    }

    /**
     * Test checking operation(exception resume) on suspended transaction is prohibited.
     *
     * @param opIdx Operation index.
     */
    private void operationsAreProhibitedOnSuspendedTx(final int opIdx) {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        try {
            cache.put("key1", 1);

            tx.suspend();

            performOperation(tx, opIdx);

            fail("Operation on suspended transaction is prohibited.");
        }
        catch (Throwable ignore) {
            // ignoring exception on suspended transaction.
        }

        tx.resume();

        tx.close();

        assertNull(cache.get("key1"));
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction from the other thread are prohibited.
     */
    public void testOperationsAreProhibitedOnSuspendedTxFromTheOtherThread() throws Exception {
        for (int opIdx = 0; opIdx < 7; opIdx++)
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                transactionIsolation = isolation;

                operationsAreProhibitedOnSuspendedTxFromTheOtherThread(opIdx);
            }
    }

    /**
     * Test checking operation(exception resume) on suspended transaction from the other thread is prohibited.
     *
     * @param opIdx Operation index.
     */
    private void operationsAreProhibitedOnSuspendedTxFromTheOtherThread(final int opIdx) {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        try {
            cache.put("key1", 1);

            tx.suspend();

            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    performOperation(tx, opIdx);

                    return null;
                }
            }, 1);

            fail("Operation on suspended transaction is prohibited from the other thread.");
        }
        catch (Throwable ignore) {
            // ignoring exception on suspended transaction.
        }

        tx.resume();

        tx.close();

        assertNull(cache.get("key1"));
    }

    /**
     * Performs operation based on its index. Resume operation is not supported.
     *
     * @param tx Transaction, operation is performed on.
     * @param opIdx Operation index.
     */
    private void performOperation(Transaction tx, final int opIdx) {
        switch (opIdx) {
            case 0:
                tx.suspend();

                break;

            case 1:
                tx.close();

                break;

            case 2:
                tx.commit();

                break;

            case 3:
                tx.commitAsync();

                break;

            case 4:

                tx.rollback();

                break;

            case 5:
                tx.rollbackAsync();

                break;
            case 6:
                tx.setRollbackOnly();

                break;

            default:
                assert false;
        }
    }

    /**
     * Test checking timeout on resumed transaction.
     *
     * @throws Exception If failed.
     */
    public void testTransactionTimeoutOnResumedTransaction() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            transactionIsolation = isolation;

            transactionTimeoutOnResumedTransaction();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void transactionTimeoutOnResumedTransaction() throws Exception {
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        boolean tryResume = false;

        try (Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation, TIMEOUT, 0)) {
            tx.suspend();

            long sleep = TIMEOUT * 2;

            Thread.sleep(sleep);

            tryResume = true;

            tx.resume();

            fail("Transaction must have timed out.");
        }
        catch (Exception e) {
            if (!(X.hasCause(e, TransactionTimeoutException.class)))
                throw e;
        }

        assert tryResume;
    }

    /**
     * Test checking timeout on suspended transaction.
     *
     * @throws Exception If failed.
     */
    public void testTransactionTimeoutOnSuspendedTransaction() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            transactionIsolation = isolation;

            transactionTimeoutOnSuspendedTransaction();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void transactionTimeoutOnSuspendedTransaction() throws Exception {
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);

        boolean trySuspend = false;

        try (Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation, TIMEOUT, 0)) {
            cache.put("key1", 1);

            long sleep = TIMEOUT * 2;

            Thread.sleep(sleep);

            trySuspend = true;

            tx.suspend();

            fail("Transaction must have timed out.");
        }
        catch (Exception e) {
            if (!(X.hasCause(e, TransactionTimeoutException.class)))
                throw e;
        }

        assertNull(cache.get("key1"));

        assert trySuspend;
    }
}
