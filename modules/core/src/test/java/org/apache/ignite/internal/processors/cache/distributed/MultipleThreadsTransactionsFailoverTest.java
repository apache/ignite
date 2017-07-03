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
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/**
 * Failover test.
 */
public class MultipleThreadsTransactionsFailoverTest extends AbstractMultipleThreadsTransactionsTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(2), getConfiguration().setClientMode(true));
    }

    /**
     * Starts tx locally with locally residing keys and then local node fails.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalNodeFailover() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                txLocalNodeFailover(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void txLocalNodeFailover(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache localCache = jcache(1);

        String localPrimaryKey = String.valueOf(primaryKey(localCache));

        assert localPrimaryKey != null;

        try {
            performTransactionFailover(localPrimaryKey, 1, 1, txConcurrency, txIsolation);
        }
        catch (Exception ignored) {
            // ignoring node breakage exception
        }

        IgniteCache<String, Integer> remoteCache = jcache(0);

        assertFalse(remoteCache.containsKey(localPrimaryKey));
    }

    /**
     * Starts tx locally on client, and break remote primary node.
     *
     * @throws Exception If failed.
     */
    public void testTxOnClientBreakRemote() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                txOnClientBreakRemote(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void txOnClientBreakRemote(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache remoteCache = jcache(1);

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        assert remotePrimaryKey != null;

        try {
            performTransactionFailover(remotePrimaryKey, 1, 2, txConcurrency, txIsolation);

            if (txConcurrency.equals(TransactionConcurrency.PESSIMISTIC))
                fail("Broken remote node must have caused exception.");
        }
        catch (Exception ignored) {
            // ignoring node breakage exception
        }

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheAdapter<?, ?> cache = ((IgniteKernal)ignite(0)).internalCache(DEFAULT_CACHE_NAME);

                IgniteTxManager txMgr = cache.isNear() ? ((GridNearCacheAdapter)cache).dht().context().tm() : cache.context().tm();

                return txMgr.idMapSize() == 0;
            }
        }, 10000);

        assertTrue(txFinished);

        IgniteCache<String, Integer> clientCache = jcache(2);

        if (txConcurrency.equals(TransactionConcurrency.OPTIMISTIC))
            assertEquals(1, (long)clientCache.get(remotePrimaryKey));
        else
            assertNull(clientCache.get(remotePrimaryKey));

        clientCache.removeAll();
    }

    /**
     * Starts tx locally with remote residing keys and then remote node fails.
     *
     * @throws Exception If failed.
     */
    public void testTxRemoteNodeFailover() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                txRemoteNodeFailover(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void txRemoteNodeFailover(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<String, Integer> remoteCache = jcache(1);

        String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

        assert remotePrimaryKey != null;

        try {
            performTransactionFailover(remotePrimaryKey, 1, 0, txConcurrency, txIsolation);

            if (txConcurrency.equals(TransactionConcurrency.PESSIMISTIC))
                fail("Broken remote node must have caused exception");
        }
        catch (Exception ignored) {
            // ignoring node breakage exception
        }

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheAdapter<?, ?> cache = ((IgniteKernal)ignite(0)).internalCache(DEFAULT_CACHE_NAME);

                IgniteTxManager txMgr = cache.isNear() ? ((GridNearCacheAdapter)cache).dht().context().tm() : cache.context().tm();

                return txMgr.idMapSize() == 0;
            }
        }, 10000);

        assertTrue(txFinished);

        IgniteCache<String, Integer> localCache = jcache();

        if (txConcurrency.equals(TransactionConcurrency.OPTIMISTIC))
            assertEquals(1, (long)localCache.get(remotePrimaryKey));
        else
            assertNull(localCache.get(remotePrimaryKey));

        localCache.removeAll();
    }

    /**
     * Starts transaction, breaks node and then resuming it in another thread.
     *
     * @param key Key to put.
     * @param breakNodeIdx Node id to brake.
     * @param initiatingNodeIdx Node, starting transaction on.
     * @throws Exception In case of an error.
     */
    private void performTransactionFailover(
        String key,
        int breakNodeIdx,
        int initiatingNodeIdx,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation)
        throws Exception {

        final IgniteTransactions transactions = grid(initiatingNodeIdx).transactions();

        IgniteCache<String, Integer> cache = jcache(initiatingNodeIdx);

        final Transaction localTx = transactions.txStart(txConcurrency, txIsolation);

        cache.put(key, 1);

        localTx.suspend();

        G.stop(ignite(breakNodeIdx).name(), true);

        assertNull(transactions.tx());

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions.tx());
                assertEquals(TransactionState.SUSPENDED, localTx.state());

                localTx.resume();

                assertEquals(TransactionState.ACTIVE, localTx.state());

                localTx.commit();

                return true;
            }
        });

        fut.get();
    }
}
