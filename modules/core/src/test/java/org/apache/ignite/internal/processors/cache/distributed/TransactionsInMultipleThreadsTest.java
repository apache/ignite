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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Assert;

/**
 *
 */
public class TransactionsInMultipleThreadsTest extends AbstractTransactionsInMultipleThreadsTest {
    /**
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThread() {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    simpleTransactionInAnotherThread();
                }
                catch (IgniteCheckedException e) {
                    throw U.convertException(e);
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void simpleTransactionInAnotherThread() throws IgniteCheckedException {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions transactions = ignite(txInitiatorNodeId).transactions();

        assertNull(cache.get("key1"));

        final Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);

        tx.suspend();

        assertNull(cache.get("key1"));

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache.remove("key2");

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long)1, (long)cache.get("key1"));
        Assert.assertEquals((long)3, (long)cache.get("key3"));
        Assert.assertFalse(cache.containsKey("key2"));

        cache.removeAll();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSimpleTransactionInAnotherThreadContinued() throws Exception {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    simpleTransactionInAnotherThreadContinued();
                }
                catch (IgniteCheckedException e) {
                    throw U.convertException(e);
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void simpleTransactionInAnotherThreadContinued() throws IgniteCheckedException {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions transactions = ignite(txInitiatorNodeId).transactions();

        final Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key1'", 1);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache.put("key2'", 2);
                cache.remove("key2", 2);

                tx.suspend();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertNull(transactions.tx());
        Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

        tx.resume();

        Assert.assertEquals(TransactionState.ACTIVE, tx.state());

        cache.remove("key1'", 1);
        cache.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long)1, (long)cache.get("key1"));
        Assert.assertEquals((long)3, (long)cache.get("key3"));
        Assert.assertEquals((long)3, (long)cache.get("key3'"));
        Assert.assertFalse(cache.containsKey("key2"));
        Assert.assertFalse(cache.containsKey("key1'"));
        Assert.assertFalse(cache.containsKey("key2'"));

        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    crossCacheTransactionInAnotherThread();
                }
                catch (Exception e) {
                    fail("Unexpected exception: " + e);
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void crossCacheTransactionInAnotherThread() throws Exception {
        Ignite ignite1 = ignite(txInitiatorNodeId);
        final IgniteTransactions transactions = ignite1.transactions();

        final IgniteCache<String, Integer> cache = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache"));
        final IgniteCache<String, Integer> cache2 = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache2"));

        final Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);

        cache.put("key1", 1);
        cache2.put("key2", 2);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache2.remove("key2");

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long)1, (long)cache.get("key1"));
        Assert.assertEquals((long)3, (long)cache.get("key3"));
        Assert.assertFalse(cache2.containsKey("key2"));

        cache2.removeAll();
        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThreadContinued() throws Exception {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    crossCacheTransactionInAnotherThreadContinued();
                }
                catch (Exception e) {
                    fail("Unexpected exception: " + e);
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void crossCacheTransactionInAnotherThreadContinued() throws Exception {
        Ignite ignite1 = ignite(txInitiatorNodeId);
        final IgniteTransactions transactions = ignite1.transactions();

        final IgniteCache<String, Integer> cache = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache"));
        final IgniteCache<String, Integer> cache2 = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache2"));

        final Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);

        cache.put("key1", 1);
        cache2.put("key2", 2);
        cache.put("key1'", 1);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache2.put("key2'", 2);
                cache2.remove("key2");

                tx.suspend();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertNull(transactions.tx());
        Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

        tx.resume();

        Assert.assertEquals(TransactionState.ACTIVE, tx.state());

        cache.remove("key1'", 1);
        cache2.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long)1, (long)cache.get("key1"));
        Assert.assertEquals((long)3, (long)cache.get("key3"));
        Assert.assertEquals((long)3, (long)cache.get("key3'"));
        Assert.assertFalse(cache2.containsKey("key2"));
        Assert.assertFalse(cache2.containsKey("key2'"));
        Assert.assertFalse(cache.containsKey("key1'"));

        cache.removeAll();
        cache2.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionRollback() throws Exception {
        withAllIsolationsAndConcurrencies(new IgniteClosure<Object, Void>() {
            @Override public Void apply(Object o) {
                try {
                    transactionRollback();
                }
                catch (IgniteCheckedException e) {
                    throw U.convertException(e);
                }
                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void transactionRollback() throws IgniteCheckedException {
        final IgniteCache<String, Integer> cache1 = jcache(txInitiatorNodeId);

        final Transaction tx = ignite(txInitiatorNodeId).transactions().txStart(transactionConcurrency, transactionIsolation);

        cache1.put("key1", 1);
        cache1.put("key2", 2);

        tx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Assert.assertNull(transactions().tx());
                Assert.assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                Assert.assertEquals(TransactionState.ACTIVE, tx.state());

                cache1.put("key3", 3);

                Assert.assertTrue(cache1.remove("key2"));

                tx.rollback();

                return true;
            }
        });

        fut.get(5000);

        Assert.assertEquals(TransactionState.ROLLED_BACK, tx.state());
        Assert.assertFalse(cache1.containsKey("key1"));
        Assert.assertFalse(cache1.containsKey("key2"));
        Assert.assertFalse(cache1.containsKey("key3"));

        cache1.removeAll();
    }
}
