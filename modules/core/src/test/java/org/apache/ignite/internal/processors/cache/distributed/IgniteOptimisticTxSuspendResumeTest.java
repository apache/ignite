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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeTest extends IgniteAbstractTxSuspendResumeTest {
    /** {@inheritDoc} */
    @Override protected TransactionConcurrency transactionConcurrency() {
        return TransactionConcurrency.OPTIMISTIC;
    }

    /**
     * Test we can resume and complete transaction if topology changed while transaction is suspended.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndResumeAfterTopologyChange() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                    cache.put(1, 1);

                    tx.suspend();

                    assertEquals(SUSPENDED, tx.state());

                    try (IgniteEx g = startGrid(serversNumber() + 3)) {
                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        assertEquals(1, (int)cache.get(1));

                        tx.commit();

                        assertEquals(1, (int)cache.get(1));
                    }

                    cache.removeAll();
                }
            }
        });
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNewWithoutCommit() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
                    for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                        Transaction tx1 = ignite.transactions().txStart(transactionConcurrency(), tx1Isolation);

                        cache.put(1, 1);

                        tx1.suspend();

                        assertFalse(cache.containsKey(1));

                        Transaction tx2 = ignite.transactions().txStart(transactionConcurrency(), tx2Isolation);

                        cache.put(1, 2);

                        tx2.suspend();

                        assertFalse(cache.containsKey(1));

                        tx1.resume();

                        assertEquals(1, (int)cache.get(1));

                        tx1.suspend();

                        tx2.resume();

                        assertEquals(2, (int)cache.get(1));

                        tx2.rollback();

                        tx1.resume();
                        tx1.rollback();

                        cache.removeAll();
                    }
                }
            }
        });
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNew() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
                    for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                        Transaction tx1 = ignite.transactions().txStart(transactionConcurrency(), tx1Isolation);

                        cache.put(1, 1);

                        tx1.suspend();

                        assertFalse(cache.containsKey(1));

                        Transaction tx2 = ignite.transactions().txStart(transactionConcurrency(), tx2Isolation);

                        cache.put(1, 2);

                        tx2.commit();

                        assertEquals(2, (int)cache.get(1));

                        tx1.resume();

                        assertEquals(1, (int)cache.get(1));

                        tx1.close();

                        cache.removeAll();
                    }
                }
            }
        });
    }

    /**
     * Test for correct exception handling when misuse transaction API - resume active tx.
     *
     * @throws Exception If failed.
     */
    public void testResumeActiveTx() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    final Transaction tx = ignite.transactions().txStart(OPTIMISTIC, isolation);

                    cache.put(1, 1);

                    try {
                        tx.resume();

                        fail("Exception must be thrown");
                    }
                    catch (Throwable e) {
                        assertTrue(X.hasCause(e, IgniteException.class));

                        assertFalse(X.hasCause(e, AssertionError.class));
                    }

                    tx.close();

                    assertFalse(cache.containsKey(1));
                }
            }
        });
    }
}
