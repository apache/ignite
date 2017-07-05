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

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jsr166.LongAdder8;
import org.junit.Assert;

/**
 *
 */
public class TransactionsInMultipleThreadsClientTest extends TransactionsInMultipleThreadsTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        startGrid(getTestIgniteInstanceName(1), getConfiguration().setClientMode(true));
        awaitPartitionMapExchange();

        txInitiatorNodeId = 1;
    }

    /**
     * Test for pessimistic entry locking twice: before suspend, and after resume.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testPessimisticTxDoubleLock() throws IgniteCheckedException {
        transactionConcurrency = TransactionConcurrency.PESSIMISTIC;

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            transactionIsolation = isolation;

            pessimisticTxDoubleLock();
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void pessimisticTxDoubleLock() throws IgniteCheckedException {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);
        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        IgniteTransactions transactions = ignite(txInitiatorNodeId).transactions();

        final Transaction clientTx = transactions.txStart(transactionConcurrency, transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, clientTx.state());

                clientTx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, clientTx.state());

                Integer val = clientCache.get(remotePrimaryKey);

                clientCache.put(remotePrimaryKey, val + 1);

                clientTx.commit();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertEquals(TransactionState.COMMITTED, clientTx.state());
        Assert.assertEquals(2, jcache(0).get(remotePrimaryKey));

        clientCache.removeAll();
    }

    /**
     * Test start 1 transaction, resuming it in another thread. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx() throws Exception {
        for (TransactionIsolation firstTxIsolation : TransactionIsolation.values())
            runWithAllIsolationsAndConcurrencies(new IgniteCallable<Void>() {
                @Override public Void call() throws Exception {
                    resumeTxWhileStartingAnotherTx(firstTxIsolation);

                    return null;
                }
            });
    }

    /**
     * @param firstTxIsolation Isolation level for first tx.
     * @throws IgniteCheckedException If failed.
     */
    private void resumeTxWhileStartingAnotherTx(TransactionIsolation firstTxIsolation) throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        IgniteTransactions transactions = ignite(txInitiatorNodeId).transactions();

        final Transaction clientTx = transactions.txStart(TransactionConcurrency.OPTIMISTIC,
            firstTxIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, clientTx.state());

                clientTx.resume();

                clientCache.put(remotePrimaryKey, 2);

                barrier.await();

                barrier.await();

                clientTx.commit();

                return true;
            }
        });

        barrier.await();

        final Transaction clientTx2 = ignite(txInitiatorNodeId).transactions().txStart(transactionConcurrency,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 3);

        clientTx2.commit();

        barrier.await();

        fut.get(5000);

        Assert.assertEquals(2, jcache(0).get(remotePrimaryKey));

        jcache(0).removeAll();
    }

    /**
     * Test start 1 transaction, suspend it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNewTx() throws Exception {
        for (TransactionIsolation firstTxIsolation : TransactionIsolation.values())
            runWithAllIsolationsAndConcurrencies(new IgniteCallable<Void>() {
                @Override public Void call() throws Exception {
                    suspendTxAndStartNewTx(firstTxIsolation);

                    return null;
                }
            });
    }

    /**
     * @param isolation1 Isolation level for first tx.
     * @throws IgniteCheckedException If failed.
     */
    private void suspendTxAndStartNewTx(TransactionIsolation isolation1) throws IgniteCheckedException {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);
        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));
        Ignite clientIgnite = ignite(txInitiatorNodeId);

        final Transaction clientTx = clientIgnite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, isolation1);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        final Transaction clientTx2 = clientIgnite.transactions().txStart(transactionConcurrency, transactionIsolation);

        clientCache.put(remotePrimaryKey, 2);

        clientTx2.commit();

        Assert.assertEquals(2, jcache(0).get(remotePrimaryKey));

        clientTx.close();

        remoteCache.removeAll();
    }

    /**
     * Test for concurrent transaction suspend.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentSuspend() throws Exception {
        runWithAllIsolationsAndConcurrencies(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txConcurrentSuspend();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void txConcurrentSuspend() throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(11);
        LongAdder8 failedTxNumber = new LongAdder8();
        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        try(Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(transactionConcurrency, transactionIsolation))
        {
            clientCache.put(remotePrimaryKey, 1);

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        barrier.await();

                        clientTx.suspend();
                        fail("Concurrent suspension must failed, because it doesn't own transaction.");
                    }
                    catch (IgniteException e) {
                        failedTxNumber.increment();
                    }

                    return null;
                }
            }, 10, "th-suspend");

            barrier.await();

            clientTx.suspend();

            fut.get();
        }

        Assert.assertEquals(10, failedTxNumber.intValue());
        Assert.assertNull(remoteCache.get(remotePrimaryKey));
    }

    /**
     * Test for concurrent transaction resume.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentResume() throws Exception {
        runWithAllIsolationsAndConcurrencies(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txConcurrentResume();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void txConcurrentResume() throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(11);
        LongAdder8 failNumber = new LongAdder8();
        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(transactionConcurrency, transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {

                try {
                    barrier.await();

                    clientTx.resume();
                }
                catch (IgniteException e) {
                    failNumber.increment();
                    return null;
                }

                clientTx.close();

                return null;
            }
        }, 11);

        Assert.assertEquals(10, failNumber.intValue());
        Assert.assertNull(remoteCache.get(remotePrimaryKey));
    }

    /**
     * Test for concurrent transaction commit.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentCommit() throws Exception {
        runWithAllIsolationsAndConcurrencies(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txConcurrentCommit();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void txConcurrentCommit() throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(11);
        AtomicInteger failNumber = new AtomicInteger();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(transactionConcurrency, transactionIsolation);
        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientTx.resume();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try {
                            barrier.await();

                            clientTx.commit();
                            fail("Concurrent commit must failed, because it doesn't own transaction.");
                        }
                        catch (IgniteException e) {
                            failNumber.incrementAndGet();
                        }

                        return null;
                    }
                }, 10, "th-commit");

                barrier.await();

                clientTx.commit();

                fut.get();

                return null;
            }
        }, 1);

        Assert.assertEquals(10, failNumber.get());
        Assert.assertEquals(1, jcache(0).get(remotePrimaryKey));
    }
}
