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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionState;

/**
 *
 */
public class OptimisticTransactionsInMultipleThreadsFailoverTest extends AbstractTransactionsInMultipleThreadsTest {
    /**
     * Starts transaction, breaks node and then resuming it in another thread.
     *
     * @param key Key to put.
     * @param breakNodeIdx Node id to brake.
     * @param initiatingNodeIdx Node, starting transaction on.
     * @throws IgniteCheckedException If failed.
     */
    private void performTransactionFailover(String key,
        int breakNodeIdx, int initiatingNodeIdx) throws IgniteCheckedException {
        final IgniteTransactions txs = grid(initiatingNodeIdx).transactions();
        IgniteCache<String, Integer> cache = jcache(initiatingNodeIdx);

        final Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, transactionIsolation);

        cache.put(key, 1);

        tx.suspend();

        G.stop(ignite(breakNodeIdx).name(), true);

        assertNull(txs.tx());

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(txs.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                tx.commit();

                return true;
            }
        });

        fut.get();
    }

    /**
     * Starts tx locally with remote residing keys and then remote node fails.
     */
    public void testTxRemoteNodeFailover() throws Exception {
        startGrid(getTestIgniteInstanceName(0));

        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txRemoteNodeFailover();

                return null;
            }
        });

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    private void txRemoteNodeFailover() throws Exception {
        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<String, Integer> remoteCache = jcache(1);

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        assert remotePrimaryKey != null;

        performTransactionFailover(remotePrimaryKey, 1, 0);

        waitAllTransactionsHasFinished();

        IgniteCache<String, Integer> clientCache = jcache(0);

        assertEquals(1, (long)clientCache.get(remotePrimaryKey));

        clientCache.removeAll();
    }

    /**
     * Starts tx locally with locally residing keys and then local node fails.
     */
    public void testTxLocalNodeFailover() throws Exception {
        startGrid(getTestIgniteInstanceName(0));

        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txLocalNodeFailover();

                return null;
            }
        });

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    private void txLocalNodeFailover() throws Exception {
        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache localCache = jcache(1);

        String localPrimaryKey = String.valueOf(primaryKey(localCache));

        assert localPrimaryKey != null;

        try {
            performTransactionFailover(localPrimaryKey, 1, 1);
        }
        catch (IgniteCheckedException ignore) {
            // ignoring node breakage exception.
        }

        IgniteCache<String, Integer> remoteCache = jcache(0);

        assertFalse(remoteCache.containsKey(localPrimaryKey));
    }

    /**
     * Starts tx locally on client, and break remote primary node.
     */
    public void testTxOnClientBreakRemote() throws Exception {
        startGrid(2);

        startGrid(getTestIgniteInstanceName(0), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();

        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                txOnClientBreakRemote();

                return null;
            }
        });

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    private void txOnClientBreakRemote() throws Exception {
        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache remoteCache = jcache(1);

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        assert remotePrimaryKey != null;

        performTransactionFailover(remotePrimaryKey, 1, 0);

        waitAllTransactionsHasFinished();

        IgniteCache<String, Integer> clientCache = jcache(0);

        assertEquals(1, (long)clientCache.get(remotePrimaryKey));

        clientCache.removeAll();
    }
}
