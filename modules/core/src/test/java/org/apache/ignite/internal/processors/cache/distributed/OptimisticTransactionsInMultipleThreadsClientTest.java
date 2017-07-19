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

/**
 *
 */
public class OptimisticTransactionsInMultipleThreadsClientTest extends OptimisticTransactionsInMultipleThreadsTest {
    /** Number of concurrently running threads, which tries to perform transaction operations. */
    private int concurrentThreadsNum = 25;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(1), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();

        txInitiatorNodeId = 1;
    }

    /**
     * Test start 1 transaction, resuming it in another thread. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx() throws Exception {
        for (final TransactionIsolation firstTxIsolation : TransactionIsolation.values())
            runWithAllIsolations(new IgniteCallable<Void>() {
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

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        final Transaction clientTx = txs.txStart(TransactionConcurrency.OPTIMISTIC,
            firstTxIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, clientTx.state());

                clientTx.resume();

                clientCache.put(remotePrimaryKey, 2);

                barrier.await();

                barrier.await();

                clientTx.commit();

                return true;
            }
        });

        barrier.await();

        final Transaction clientTx2 = ignite(txInitiatorNodeId).transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 3);

        clientTx2.commit();

        barrier.await();

        fut.get(5000);

        assertEquals(2, jcache(0).get(remotePrimaryKey));

        jcache(0).removeAll();
    }

    /**
     * Test start 1 transaction, suspend it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNewTx() throws Exception {
        for (final TransactionIsolation firstTxIsolation : TransactionIsolation.values())
            runWithAllIsolations(new IgniteCallable<Void>() {
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

        final Transaction clientTx2 = clientIgnite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 2);

        clientTx2.commit();

        assertEquals(2, jcache(0).get(remotePrimaryKey));

        clientTx.resume();

        clientTx.close();

        remoteCache.removeAll();
    }

    /**
     * Test for concurrent transaction suspend.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentSuspend() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
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

        final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
        final LongAdder8 failedTxNumber = new LongAdder8();
        final AtomicInteger threadCnt = new AtomicInteger();
        final AtomicInteger successfulResume = new AtomicInteger();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failedTxNumber);

                return null;
            }
        }, concurrentThreadsNum, "th-suspend");

        barrier.await();

        clientTx.suspend();

        fut.get();

        // if transaction was not closed after resume, then close it now.
        if (successfulResume.get() == 0) {
            clientTx.resume();

            clientTx.close();
        }

        assertTrue(successfulResume.get() < 2);
        assertEquals(concurrentThreadsNum, failedTxNumber.intValue() + successfulResume.intValue());
        assertNull(remoteCache.get(remotePrimaryKey));
    }

    /**
     * Test for concurrent transaction resume.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentResume() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
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

        final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum);
        final LongAdder8 failNumber = new LongAdder8();
        final AtomicInteger threadCnt = new AtomicInteger();
        final AtomicInteger successfulResume = new AtomicInteger();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                return null;
            }
        }, concurrentThreadsNum);

        assertEquals(1, successfulResume.get());
        assertEquals(concurrentThreadsNum - 1, failNumber.intValue());
        assertNull(remoteCache.get(remotePrimaryKey));
    }

    /**
     * Test for concurrent transaction commit.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentCommit() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txConcurrentCommit();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void txConcurrentCommit() throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);

        final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
        final LongAdder8 failNumber = new LongAdder8();
        final AtomicInteger threadCnt = new AtomicInteger();
        final AtomicInteger successfulResume = new AtomicInteger();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientTx.resume();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                        return null;
                    }
                }, concurrentThreadsNum, "th-commit");

                barrier.await();

                clientTx.commit();

                fut.get();

                return null;
            }
        }, 1);

        assertEquals(0, successfulResume.get());
        assertEquals(concurrentThreadsNum, failNumber.intValue());
        assertEquals(1, jcache(0).get(remotePrimaryKey));
    }

    /**
     * Test for concurrent transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentRollback() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txConcurrentRollback();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void txConcurrentRollback() throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);

        final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
        final LongAdder8 failNumber = new LongAdder8();
        final AtomicInteger threadCnt = new AtomicInteger();
        final AtomicInteger successfulResume = new AtomicInteger();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientTx.resume();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                        return null;
                    }
                }, concurrentThreadsNum, "th-rollback");

                barrier.await();

                clientTx.rollback();

                fut.get();

                return null;
            }
        }, 1);

        assertEquals(0, successfulResume.get());
        assertEquals(concurrentThreadsNum, failNumber.intValue());
        assertNull(jcache(0).get(remotePrimaryKey));
    }

    /**
     * Test for concurrent transaction close.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentClose() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txConcurrentClose();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void txConcurrentClose() throws Exception {
        final IgniteCache<String, Integer> clientCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache(0);

        final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
        final LongAdder8 failNumber = new LongAdder8();
        final AtomicInteger threadCnt = new AtomicInteger();
        final AtomicInteger successfulResume = new AtomicInteger();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction clientTx = ignite(txInitiatorNodeId).transactions().txStart(TransactionConcurrency.OPTIMISTIC,
            transactionIsolation);

        clientCache.put(remotePrimaryKey, 1);

        clientTx.suspend();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientTx.resume();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                        return null;
                    }
                }, concurrentThreadsNum, "th-close");

                barrier.await();

                clientTx.close();

                fut.get();

                return null;
            }
        }, 1);

        assertEquals(0, successfulResume.get());
        assertEquals(concurrentThreadsNum, failNumber.intValue());
        assertNull(jcache(0).get(remotePrimaryKey));
    }

    /**
     * Thread begin waiting on barrier and then performs some operation.
     *
     * @param threadCnt Common counter for threads.
     * @param barrier Barrier, all threads are waiting on.
     * @param clientTx Transaction instance that we test.
     * @param successfulResume Counter for successful resume operations.
     * @param failedTxNumber Counter for failed operations.
     * @throws Exception If failed.
     */
    private void waitAndPerformOperation(AtomicInteger threadCnt, CyclicBarrier barrier, Transaction clientTx,
        AtomicInteger successfulResume, LongAdder8 failedTxNumber) throws Exception {
        try {
            int threadNum = threadCnt.incrementAndGet();

            switch (threadNum % 5) {
                case 0:
                    barrier.await();

                    clientTx.suspend();

                    break;

                case 1:
                    barrier.await();

                    clientTx.resume();

                    successfulResume.incrementAndGet();

                    clientTx.close();

                    return;

                case 2:
                    barrier.await();

                    clientTx.commit();

                    break;

                case 3:
                    barrier.await();

                    clientTx.rollback();

                    break;

                case 4:
                    barrier.await();

                    clientTx.close();

                    break;

                default:
                    assert false;
            }

            fail("Concurrent operation must failed, because it doesn't own transaction.");
        }
        catch (Throwable e) {
            failedTxNumber.increment();
        }
    }
}
