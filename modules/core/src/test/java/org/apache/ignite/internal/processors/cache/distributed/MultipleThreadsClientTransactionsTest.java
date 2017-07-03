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
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 * Tests of transactions on multiple threads.
 */
public class MultipleThreadsClientTransactionsTest extends AbstractMultipleThreadsTransactionsTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getCacheConfiguration()[0].setBackups(0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(2), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected int txInitiatorNodeId() {
        return 2;
    }

    /**
     * Test for starting and suspending transactions, and then resuming and committing in another thread.
     *
     * @throws Exception If failed.
     */
    public void testMultipleTransactionsSuspendResume() throws Exception {
        for (TransactionIsolation txIsolation : TransactionIsolation.values())
            multipleTransactionsSuspendResume(OPTIMISTIC, txIsolation);
    }

    /**
     * @throws Exception In case of an error.
     */
    private void multipleTransactionsSuspendResume(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId());

        Ignite clientNode = ignite(txInitiatorNodeId());

        final List<Transaction> transactions = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Transaction tx = clientNode.transactions().txStart(txConcurrency, txIsolation);

            cache.put("1", i);

            tx.suspend();

            transactions.add(tx);
        }

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(ignite(txInitiatorNodeId()).transactions().tx());

                for (int i = 0; i < 10; i++) {
                    Transaction tx = transactions.get(i);

                    assertEquals(TransactionState.SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(TransactionState.ACTIVE, tx.state());

                    tx.commit();
                }

                return true;
            }
        });

        fut.get(5000);

        assertEquals(9, (long)jcache().get("1"));

        cache.removeAll();
    }

    /**
     * Test for pessimistic entry locking twice: before suspend, and after resume.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticTxDoubleLock() throws Exception {
        for (TransactionIsolation txIsolation : TransactionIsolation.values())
            pessimisticTxDoubleLock(PESSIMISTIC, txIsolation);
    }

    /**
     * @throws Exception In case of an error.
     */
    private void pessimisticTxDoubleLock(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId());
        final IgniteCache<String, Integer> remoteCache = jcache();

        final String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction tx = ignite(txInitiatorNodeId()).transactions().txStart(txConcurrency, txIsolation);

        cache.put(remotePrimaryKey, 1);

        tx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions().tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                Integer val = cache.get(remotePrimaryKey);

                cache.put(remotePrimaryKey, val + 1);

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals(2, (long)jcache().get(remotePrimaryKey));

        cache.removeAll();
    }

    /**
     * Test starts 1 transaction, resuming it in another thread. And then start another transaction,
     * trying to write the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx() throws Exception {
        for (TransactionIsolation tx1Isolation : TransactionIsolation.values())
            for (TransactionIsolation tx2Isolation : TransactionIsolation.values())
                resumeTxWhileStartingAnotherTx(OPTIMISTIC, OPTIMISTIC, tx1Isolation, tx2Isolation);
    }

    /**
     * Test starts 1 transaction, resuming it in another thread. And then start another transaction,
     * trying to write the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx2() throws Exception {
        for (TransactionIsolation tx1Isolation : TransactionIsolation.values())
            for (TransactionIsolation tx2Isolation : TransactionIsolation.values())
                resumeTxWhileStartingAnotherTx(OPTIMISTIC, PESSIMISTIC, tx1Isolation, tx2Isolation);
    }

    /**
     * @param tx1Concurrency Concurrency control for first tx.
     * @param tx2Concurrency Concurrency control for second tx.
     * @param tx1Isolation Isolation level for first tx.
     * @param tx2Isolation Isolation level for second tx.
     * @throws Exception In case of an error.
     */
    private void resumeTxWhileStartingAnotherTx(
        TransactionConcurrency tx1Concurrency, TransactionConcurrency tx2Concurrency,
        TransactionIsolation tx1Isolation, TransactionIsolation tx2Isolation) throws Exception {

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Object mux = new Object();

        final IgniteCache<String, Integer> localCache = jcache(txInitiatorNodeId());
        final IgniteCache<String, Integer> remoteCache = jcache();

        final String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction localTx = ignite(txInitiatorNodeId()).transactions().txStart(tx1Concurrency, tx1Isolation);

        localCache.put(remotePrimaryKey, 1);

        localTx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions().tx());
                assertEquals(TransactionState.SUSPENDED, localTx.state());

                localTx.resume();

                localCache.put(remotePrimaryKey, 2);

                barrier.await();

                synchronized (mux) {
                    mux.wait();
                }

                localTx.commit();

                return true;
            }
        });

        barrier.await();

        final Transaction localTx2 = ignite(txInitiatorNodeId()).transactions().txStart(tx2Concurrency, tx2Isolation);

        localCache.put(remotePrimaryKey, 3);

        localTx2.commit();

        synchronized (mux) {
            mux.notify();
        }

        fut.get(5000);

        assertEquals(2, (long)jcache().get(remotePrimaryKey));

        jcache().removeAll();
    }

    /**
     * Test start 1 transaction, suspend it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNewTx() throws Exception {
        for (TransactionIsolation tx1Isolation : TransactionIsolation.values())
            for (TransactionIsolation tx2Isolation : TransactionIsolation.values())
                suspendTxAndStartNewTx(OPTIMISTIC, PESSIMISTIC, tx1Isolation, tx2Isolation);
    }

    /**
     * Test start 1 transaction, suspend it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNewTx2() throws Exception {
        for (TransactionIsolation tx1Isolation : TransactionIsolation.values())
            for (TransactionIsolation tx2Isolation : TransactionIsolation.values())
                suspendTxAndStartNewTx(OPTIMISTIC, OPTIMISTIC, tx1Isolation, tx2Isolation);
    }

    /**
     * @param tx1Concurrency Concurrency control for first tx.
     * @param tx2Concurrency Concurrency control for second tx.
     * @param tx1Isolation Isolation level for first tx.
     * @param tx2Isolation Isolation level for second tx.
     * @throws Exception In case of an error.
     */
    private void suspendTxAndStartNewTx(
        TransactionConcurrency tx1Concurrency, TransactionConcurrency tx2Concurrency,
        TransactionIsolation tx1Isolation, TransactionIsolation tx2Isolation) throws Exception {

        final IgniteCache<String, Integer> localCache = jcache(txInitiatorNodeId());
        final IgniteCache<String, Integer> remoteCache = jcache();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        Ignite localIgnite = ignite(txInitiatorNodeId());

        final Transaction localTx = localIgnite.transactions().txStart(tx1Concurrency, tx1Isolation);

        localCache.put(remotePrimaryKey, 1);

        localTx.suspend();

        final Transaction localTx2 = localIgnite.transactions().txStart(tx2Concurrency, tx2Isolation);

        localCache.put(remotePrimaryKey, 2);

        localTx2.commit();

        assertEquals(2, (int)jcache().get(remotePrimaryKey));

        localTx.resume();

        localTx.close();
    }
}
