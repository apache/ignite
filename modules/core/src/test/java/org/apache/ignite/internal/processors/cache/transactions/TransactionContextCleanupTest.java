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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.function.BiConsumer;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Test checks that transaction context is cleaned up on explicit commit or rollback.
 */
public class TransactionContextCleanupTest extends GridCommonAbstractTest {
    /** Transaction timeout. */
    public static final int TX_TIMEOUT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testContextCleanupOnCommit() throws Exception {
        checkContextCleanup(
            tx -> doSleep(TX_TIMEOUT * 2), // Rollback on timeout.
            (cache, tx) -> tx.commit(),
            TransactionTimeoutException.class
        );
    }

    /** */
    @Test
    public void testContextCleanupOnRollback() throws Exception {
        checkContextCleanup(
            tx -> doSleep(TX_TIMEOUT * 2), // Rollback on timeout.
            (cache, tx) -> {
                try {
                    cache.put(1, 2);
                }
                finally {
                    tx.rollback();
                }
            },
            TransactionTimeoutException.class);
    }

    /** */
    @Test
    public void testContextCleanupAfterAsyncRollback() throws Exception {
        checkContextCleanup(
            tx -> runAsync(tx::rollback).get(), // Rollback from other thread.
            (cache, tx) -> {
                try {
                    cache.put(1, 2);
                }
                finally {
                    tx.rollback();
                }
            },
            TransactionRollbackException.class);
    }

    /** */
    private void checkContextCleanup(ConsumerX<Transaction> rollbackAction,
        BiConsumer<IgniteCache<Integer, Integer>, Transaction> txAction, Class<? extends Throwable> eCls) throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);
        cache.put(1, 1);

        IgniteTransactions transactions = ignite.transactions();
        Transaction tx = transactions.txStart(PESSIMISTIC, READ_COMMITTED, TX_TIMEOUT, 0);

        cache.put(1, 2);

        // Ensure rollback.
        rollbackAction.accept(tx);

        assertTrue("Transaction was not rolled back",
            waitForCondition(() -> TransactionState.ROLLED_BACK == tx.state(), getTestTimeout() / 2));

        assertNotNull("Transaction in context expected", transactions.tx());
        assertEquals("Transactions are not equal", tx, transactions.tx());

        Throwable t = assertThrows(null,
            () -> {
                txAction.accept(cache, tx);

                return null;
            },
            eCls,
            null);

        assertTrue(t.getClass().equals(eCls) || t instanceof CacheException);

        assertNull("No transaction expected in context", transactions.tx());

        assertEquals("Value should not be commited", 1, (int)cache.get(1));
    }
}
