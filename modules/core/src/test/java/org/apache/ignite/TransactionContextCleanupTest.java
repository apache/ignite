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

package org.apache.ignite;

import java.util.List;
import java.util.function.BiConsumer;
import javax.cache.CacheException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test checks that transaction context is cleaned up on explicit commit or rollback.
 */
@RunWith(Parameterized.class)
public class TransactionContextCleanupTest extends GridCommonAbstractTest {
    /** Transaction timeout. */
    public static final int TX_TIMEOUT = 1000;

    /** Transaction concurrency. */
    @Parameter(0)
    public TransactionConcurrency concurrency;

    /** Transaction isolation. */
    @Parameter(1)
    public TransactionIsolation isolation;

    /** Cache backups. */
    @Parameter(2)
    public int backups;

    /** */
    @Parameters(name="concurrency={0}, isolation={1}, backups={2}")
    public static List<Object[]> parameters() {
        return List.of(
            new Object[]{PESSIMISTIC, READ_COMMITTED, 0},
            new Object[]{PESSIMISTIC, READ_COMMITTED, 1},
            new Object[]{PESSIMISTIC, READ_COMMITTED, 2},

            new Object[]{PESSIMISTIC, REPEATABLE_READ, 0},
            new Object[]{PESSIMISTIC, REPEATABLE_READ, 1},
            new Object[]{PESSIMISTIC, REPEATABLE_READ, 2},

            new Object[]{OPTIMISTIC, REPEATABLE_READ, 0},
            new Object[]{OPTIMISTIC, REPEATABLE_READ, 1},
            new Object[]{OPTIMISTIC, REPEATABLE_READ, 2},

            new Object[]{OPTIMISTIC, SERIALIZABLE, 0},
            new Object[]{OPTIMISTIC, SERIALIZABLE, 1},
            new Object[]{OPTIMISTIC, SERIALIZABLE, 2}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(backups)
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
        checkContextCleanup((cache, tx) -> tx.commit());
    }

    /** */
    @Test
    public void testContextCleanupOnRollback() throws Exception {
        checkContextCleanup((cache, tx) -> {
            try {
                cache.put(1, 2);
            }
            finally {
                tx.rollback();
            }
        });
    }

    /**
     * @param txAction Transaction action.
     */
    private void checkContextCleanup(BiConsumer<IgniteCache<Integer, Integer>, Transaction> txAction) throws Exception {
        IgniteEx ignite = startGrids(3);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);
        cache.put(1, 1);

        IgniteTransactions transactions = ignite.transactions();
        Transaction tx = transactions.txStart(concurrency, isolation, TX_TIMEOUT, 0);

        cache.put(1, 2);

        // Ensure acynchronous rollback.
        doSleep(TX_TIMEOUT * 2);
        assertEquals("Unexpected transaction state", TransactionState.ROLLED_BACK, tx.state());

        Throwable t = assertThrows(null, () -> {
                txAction.accept(cache, tx);

                return null;
            },
            TransactionTimeoutException.class,
            null);

        assertTrue(t instanceof TransactionTimeoutException || t instanceof CacheException);

        GridCacheContext<?, ?> cctx = ignite.cachex(DEFAULT_CACHE_NAME).context();
        assertNull("Context was not cleared:" + U.nl(), cctx.tm().threadLocalTx(cctx));

        assertEquals(1, (int)cache.get(1));
    }
}
