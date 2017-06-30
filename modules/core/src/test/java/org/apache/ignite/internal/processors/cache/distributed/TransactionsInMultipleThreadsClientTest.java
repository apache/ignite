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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Assert;

/**
 *
 */
public class TransactionsInMultipleThreadsClientTest extends TransactionsInMultipleThreadsTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /**
     * {@inheritDoc}
     *
     * @param igniteInstanceName
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.getCacheConfiguration()[0].setBackups(0);

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(2), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();

        txInitiatorNodeId = 2;
    }

    /**
     * Test for starting and suspending transactions, and then resuming and committing in another thread.
     *
     * @throws Exception If failed.
     */
    public void testMultipleTransactionsSuspendResume() throws Exception {
        transactionConcurrency = TransactionConcurrency.OPTIMISTIC;

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            transactionIsolation = isolation;

            multipleTransactionsSuspendResume();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void multipleTransactionsSuspendResume() throws Exception {
        List<Transaction> txs = new ArrayList<>();

        Transaction tx;

        IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);

        Ignite clientNode = ignite(txInitiatorNodeId);

        for (int i = 0; i < 10; i++) {
            tx = clientNode.transactions().txStart(transactionConcurrency, transactionIsolation);

            cache.put("1", i);

            tx.suspend();

            txs.add(tx);
        }

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(ignite(txInitiatorNodeId).transactions().tx());

                for (int i = 0; i < 10; i++) {
                    Transaction tx = txs.get(i);

                    Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                    tx.resume();

                    Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                    tx.commit();
                }

                return true;
            }
        });

        fut.get(5000);

        Assert.assertEquals(9, (long)jcache().get("1"));

        cache.removeAll();
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
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction tx = ignite(txInitiatorNodeId).transactions().txStart(transactionConcurrency, transactionIsolation);

        cache.put(remotePrimaryKey, 1);

        tx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions().tx());
                Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                Integer val = cache.get(remotePrimaryKey);

                cache.put(remotePrimaryKey, val + 1);

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals(2, (long)jcache().get(remotePrimaryKey));

        cache.removeAll();
    }

    /**
     * Test start 1 transaction, resuming it in another thread. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx() throws Exception {
        for (TransactionIsolation isolation1 : TransactionIsolation.values())
            for (TransactionIsolation isolation2 : TransactionIsolation.values())
                resumeTxWhileStartingAnotherTx(TransactionConcurrency.OPTIMISTIC, TransactionConcurrency.OPTIMISTIC,
                    isolation1, isolation2);

        for (TransactionIsolation isolation1 : TransactionIsolation.values())
            for (TransactionIsolation isolation2 : TransactionIsolation.values())
                resumeTxWhileStartingAnotherTx(TransactionConcurrency.OPTIMISTIC, TransactionConcurrency.PESSIMISTIC,
                    isolation1, isolation2);
    }

    /**
     * @param concurrency1 Concurrency control for first tx.
     * @param concurrency2 Concurrency control for second tx.
     * @param isolation1 Isolation level for first tx.
     * @param isolation2 Isolation level for second tx.
     * @throws IgniteCheckedException If failed.
     */
    private void resumeTxWhileStartingAnotherTx(TransactionConcurrency concurrency1,
        TransactionConcurrency concurrency2,
        TransactionIsolation isolation1, TransactionIsolation isolation2) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Object mux = new Object();

        final IgniteCache<String, Integer> localCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        final Transaction localTx = ignite(txInitiatorNodeId).transactions().txStart(concurrency1, isolation1);

        localCache.put(remotePrimaryKey, 1);

        localTx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions().tx());
                Assert.assertEquals(TransactionState.SUSPENDED, localTx.state());

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

        final Transaction localTx2 = ignite(txInitiatorNodeId).transactions().txStart(concurrency2, isolation2);

        localCache.put(remotePrimaryKey, 3);

        localTx2.commit();

        synchronized (mux) {
            mux.notify();
        }

        fut.get(5000);

        Assert.assertEquals(2, (long)jcache().get(remotePrimaryKey));

        jcache().removeAll();
    }

    /**
     * Test start 1 transaction, suspend it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testSuspendTxAndStartNewTx() throws IgniteCheckedException {
        for (TransactionIsolation isolation1 : TransactionIsolation.values())
            for (TransactionIsolation isolation2 : TransactionIsolation.values())
                suspendTxAndStartNewTx(TransactionConcurrency.OPTIMISTIC, TransactionConcurrency.OPTIMISTIC,
                    isolation1, isolation2);

        for (TransactionIsolation isolation1 : TransactionIsolation.values())
            for (TransactionIsolation isolation2 : TransactionIsolation.values())
                suspendTxAndStartNewTx(TransactionConcurrency.OPTIMISTIC, TransactionConcurrency.PESSIMISTIC,
                    isolation1, isolation2);
    }

    /**
     * @param concurrency1 Concurrency control for first tx.
     * @param concurrency2 Concurrency control for second tx.
     * @param isolation1 Isolation level for first tx.
     * @param isolation2 Isolation level for second tx.
     * @throws IgniteCheckedException If failed.
     */
    private void suspendTxAndStartNewTx(TransactionConcurrency concurrency1, TransactionConcurrency concurrency2,
        TransactionIsolation isolation1, TransactionIsolation isolation2) throws IgniteCheckedException {
        final IgniteCache<String, Integer> localCache = jcache(txInitiatorNodeId);
        final IgniteCache<String, Integer> remoteCache = jcache();

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        Ignite localIgnite = ignite(txInitiatorNodeId);

        final Transaction localTx = localIgnite.transactions().txStart(concurrency1, isolation1);

        localCache.put(remotePrimaryKey, 1);

        localTx.suspend();

        final Transaction localTx2 = localIgnite.transactions().txStart(concurrency2, isolation2);

        localCache.put(remotePrimaryKey, 2);

        localTx2.commit();

        Assert.assertEquals(2, (long)jcache().get(remotePrimaryKey));

        localTx.resume();

        localTx.close();
    }
}
