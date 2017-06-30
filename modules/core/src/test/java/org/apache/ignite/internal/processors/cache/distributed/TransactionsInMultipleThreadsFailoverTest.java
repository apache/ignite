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
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Assert;

/**
 *
 */
public class TransactionsInMultipleThreadsFailoverTest extends AbstractTransactionsInMultipleThreadsTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(2), getConfiguration().setClientMode(true));
    }

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
        IgniteTransactions ts = grid(initiatingNodeIdx).transactions();

        IgniteCache<String, Integer> cache = jcache(initiatingNodeIdx);

        Transaction localTx = ts.txStart(transactionConcurrency, transactionIsolation);

        cache.put(key, 1);

        localTx.suspend();

        G.stop(ignite(breakNodeIdx).name(), true);

        Assert.assertNull(ts.tx());

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(ts.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, localTx.state());

                localTx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, localTx.state());

                localTx.commit();

                return true;
            }
        });

        fut.get();
    }

    /**
     * Starts tx locally with remote residing keys and then remote node fails.
     */
    public void testTxRemoteNodeFailover() {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    txRemoteNodeFailover();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception.");
                }

                return null;
            }
        });
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

        try {
            performTransactionFailover(remotePrimaryKey, 1, 0);

            if (transactionConcurrency.equals(TransactionConcurrency.PESSIMISTIC))
                fail("Broken remote node must have caused exception");
        }
        catch (ClusterTopologyCheckedException ignore) {
            // ignoring node breakage exception
        }

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheAdapter<?, ?> cache = ((IgniteKernal)ignite(0)).internalCache(DEFAULT_CACHE_NAME);

                IgniteTxManager txMgr = cache.isNear() ?
                    ((GridNearCacheAdapter)cache).dht().context().tm() :
                    cache.context().tm();

                int txNum = txMgr.idMapSize();

                return txNum == 0;
            }
        }, 10000);

        assertTrue(txFinished);

        IgniteCache<String, Integer> localCache = jcache();

        if (transactionConcurrency.equals(TransactionConcurrency.OPTIMISTIC))
            Assert.assertEquals(1, (long)localCache.get(remotePrimaryKey));
        else
            Assert.assertNull(localCache.get(remotePrimaryKey));

        localCache.removeAll();
    }

    /**
     * Starts tx locally with locally residing keys and then local node fails.
     */
    public void testTxLocalNodeFailover() {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    txLocalNodeFailover();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception.");
                }

                return null;
            }
        });
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
            // ignoring node breakage exception
        }

        IgniteCache<String, Integer> remoteCache = jcache(0);

        Assert.assertFalse(remoteCache.containsKey(localPrimaryKey));
    }

    /**
     * Starts tx locally on client, and break remote primary node.
     */
    public void testTxOnClientBreakRemote() {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    txOnClientBreakRemote();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception.");
                }

                return null;
            }
        });
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

        try {
            performTransactionFailover(remotePrimaryKey, 1, 2);

            if (transactionConcurrency.equals(TransactionConcurrency.PESSIMISTIC))
                fail("Broken remote node must have caused exception.");
        }
        catch (IgniteCheckedException ignore) {
            // ignoring node breakage exception
        }

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheAdapter<?, ?> cache = ((IgniteKernal)ignite(0)).internalCache(DEFAULT_CACHE_NAME);

                IgniteTxManager txMgr = cache.isNear() ?
                    ((GridNearCacheAdapter)cache).dht().context().tm() :
                    cache.context().tm();

                int txNum = txMgr.idMapSize();

                return txNum == 0;
            }
        }, 10000);

        assertTrue(txFinished);

        IgniteCache<String, Integer> clientCache = jcache(2);

        if (transactionConcurrency.equals(TransactionConcurrency.OPTIMISTIC))
            Assert.assertEquals(1, (long)clientCache.get(remotePrimaryKey));
        else
            Assert.assertNull(clientCache.get(remotePrimaryKey));

        clientCache.removeAll();
    }
}
