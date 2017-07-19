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
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

/**
 *
 */
public class PessimisticTransactionsInMultipleThreadsTest extends AbstractTransactionsInMultipleThreadsTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        awaitPartitionMapExchange();
    }

    /**
     * Test for suspension on pessimistic transaction.
     *
     * @throws Exception If failed.
     */
    public void testSuspendPessimisticTransaction() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                suspendPessimisticTransaction();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void suspendPessimisticTransaction() throws Exception {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, transactionIsolation)) {
            cache.put("key1", 1);

            tx.suspend();

            fail("Suspend must fail, because it isn't supported for pessimistic transactions.");
        }
        catch (Throwable e) {
            if (!X.hasCause(e, UnsupportedOperationException.class))
                throw e;
        }

        assertNull(cache.get("key1"));
    }

    /**
     * Test for resuming on pessimistic transaction.
     *
     * @throws Exception If failed.
     */
    public void testResumePessimisticTransaction() throws Exception {
        runWithAllIsolations(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                resumePessimisticTransaction();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void resumePessimisticTransaction() throws Exception {
        final IgniteCache<String, Integer> cache = jcache(txInitiatorNodeId);
        final IgniteTransactions txs = ignite(txInitiatorNodeId).transactions();

        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, transactionIsolation)) {
            cache.put("key1", 1);

            tx.suspend();

            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    tx.resume();

                    return null;
                }
            });

            fut.get();

            fail("Resume must fail, because it isn't supported for pessimistic transactions.");
        }
        catch (Throwable e) {
            if (!X.hasCause(e, UnsupportedOperationException.class))
                throw e;
        }

        assertNull(cache.get("key1"));
    }
}
