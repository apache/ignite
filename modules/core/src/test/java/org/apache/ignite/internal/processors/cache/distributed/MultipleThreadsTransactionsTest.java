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
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/**
 * Tests of multiple threads transactions.
 */
public class MultipleThreadsTransactionsTest extends AbstractMultipleThreadsTransactionsTest {
    /**
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThread() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                simpleTransactionInAnotherThread(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void simpleTransactionInAnotherThread(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId());
        final IgniteTransactions transactions = ignite(txInitiatorNodeId()).transactions();

        assertNull(cache.get("key1"));

        final Transaction tx = transactions.txStart(txConcurrency, txIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);

        tx.suspend();

        assertNull(cache.get("key1"));

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions.tx());
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
        assertEquals(1, (int)cache.get("key1"));
        assertEquals(3, (int)cache.get("key3"));
        assertFalse(cache.containsKey("key2"));

        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThreadContinued() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                simpleTransactionInAnotherThreadContinued(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void simpleTransactionInAnotherThreadContinued(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId());
        final IgniteTransactions transactions = ignite(txInitiatorNodeId()).transactions();
        final Transaction tx = transactions.txStart(txConcurrency, txIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key1'", 1);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions.tx());
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

        assertNull(transactions.tx());
        assertEquals(TransactionState.SUSPENDED, tx.state());

        tx.resume();

        assertEquals(TransactionState.ACTIVE, tx.state());

        cache.remove("key1'", 1);
        cache.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals(1, (int)cache.get("key1"));
        assertEquals(3, (int)cache.get("key3"));
        assertEquals(3, (int)cache.get("key3'"));
        assertFalse(cache.containsKey("key2"));
        assertFalse(cache.containsKey("key1'"));
        assertFalse(cache.containsKey("key2'"));

        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                crossCacheTransactionInAnotherThread(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void crossCacheTransactionInAnotherThread(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        Ignite ignite = ignite(txInitiatorNodeId());

        final IgniteCache<String, Integer> cache = ignite.getOrCreateCache(cacheConfiguration(null).setName("testCache"));
        final IgniteCache<String, Integer> cache2 = ignite.getOrCreateCache(cacheConfiguration(null).setName("testCache2"));

        final IgniteTransactions transactions = ignite.transactions();
        final Transaction tx = transactions.txStart(txConcurrency, txIsolation);

        cache.put("key1", 1);
        cache2.put("key2", 2);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache2.remove("key2");

                tx.commit();

                return true;
            }
        });

        fut.get(5000);

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals(1, (int)cache.get("key1"));
        assertEquals(3, (int)cache.get("key3"));
        assertFalse(cache2.containsKey("key2"));

        cache2.removeAll();
        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThreadContinued() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                crossCacheTransactionInAnotherThreadContinued(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void crossCacheTransactionInAnotherThreadContinued(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        Ignite ignite = ignite(txInitiatorNodeId());

        final IgniteCache<String, Integer> cache = ignite.getOrCreateCache(cacheConfiguration(null).setName("testCache"));
        final IgniteCache<String, Integer> cache2 = ignite.getOrCreateCache(cacheConfiguration(null).setName("testCache2"));

        final IgniteTransactions transactions = ignite.transactions();
        final Transaction tx = transactions.txStart(txConcurrency, txIsolation);

        cache.put("key1", 1);
        cache2.put("key2", 2);
        cache.put("key1'", 1);

        tx.suspend();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions.tx());
                assertEquals(TransactionState.SUSPENDED, tx.state());

                tx.resume();

                assertEquals(TransactionState.ACTIVE, tx.state());

                cache.put("key3", 3);
                cache2.put("key2'", 2);
                cache2.remove("key2");

                tx.suspend();

                return true;
            }
        });

        fut.get(5000);

        assertNull(transactions.tx());
        assertEquals(TransactionState.SUSPENDED, tx.state());

        tx.resume();

        assertEquals(TransactionState.ACTIVE, tx.state());

        cache.remove("key1'", 1);
        cache2.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        assertEquals(TransactionState.COMMITTED, tx.state());
        assertEquals(1, (int)cache.get("key1"));
        assertEquals(3, (int)cache.get("key3"));
        assertEquals(3, (int)cache.get("key3'"));
        assertFalse(cache2.containsKey("key2"));
        assertFalse(cache2.containsKey("key2'"));
        assertFalse(cache.containsKey("key1'"));

        cache.removeAll();
        cache2.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionRollback() throws Exception {
        runMixIsolationsAndConcurrencies(new Testable() {
            @Override public void test(TransactionConcurrency txConcurrency,
                TransactionIsolation txIsolation) throws Exception {

                transactionRollback(txConcurrency, txIsolation);
            }
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    private void transactionRollback(TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {

        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId());

        final Transaction tx = ignite(txInitiatorNodeId()).transactions().txStart(txConcurrency, txIsolation);

        cache.put("key1", 1);
        cache.put("key2", 2);

        tx.suspend();

        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                assertNull(transactions().tx());
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
}
