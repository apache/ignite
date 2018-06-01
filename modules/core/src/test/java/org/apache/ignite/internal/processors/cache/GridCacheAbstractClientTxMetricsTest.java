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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public abstract class GridCacheAbstractClientTxMetricsTest extends GridCacheAbstractSelfTest {
    /** Expected txs finish count. */
    private static final long EXPECTED_TXS_FINISH_COUNT = 11;

    /** Metrics update frequency in ms. */
    private static final int METRICS_UPDATE_FREQUENCY = 500;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        return super.cacheConfiguration(igniteInstanceName).setStatisticsEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setMetricsUpdateFrequency(METRICS_UPDATE_FREQUENCY);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount() - 1);

        startGrid(getConfiguration(getTestIgniteInstanceName(gridCount() - 1)).setClientMode(true));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(DEFAULT_CACHE_NAME).removeAll();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(DEFAULT_CACHE_NAME).localMxBean().clear();

            g.transactions().resetMetrics();
        }
    }

    /**
     *
     */
    public void testCommitPessimisticReadCommittedTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     *
     */
    public void testCommitPessimisticRepeatableReadTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     *
     */
    public void testCommitPessimisticSerializableTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     *
     */
    public void testCommitOptimisticReadCommittedTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     *
     */
    public void testCommitOptimisticRepeatableReadTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     *
     */
    public void testCommitOptimisticSerializableTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     *
     */
    public void testRollbackPessimisticReadCommittedTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     *
     */
    public void testRollbackPessimisticRepeatableReadTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     *
     */
    public void testRollbackPessimisticSerializableTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     *
     */
    public void testRollbackOptimisticReadCommittedTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     *
     */
    public void testRollbackOptimisticRepeatableReadTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     *
     */
    public void testRollbackOptimisticSerializableTxOnClient() throws IgniteInterruptedCheckedException {
        doPutOnClient(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @param transactionConcurrency Transaction concurrency.
     * @param transactionIsolation Transaction isolation.
     * @param commit {@code True} if should commit, otherwise - rollback transaction.
     */
    private void doPutOnClient(TransactionConcurrency transactionConcurrency,
        TransactionIsolation transactionIsolation, boolean commit) throws IgniteInterruptedCheckedException {
        Ignite client = ignite(gridCount() - 1);

        IgniteCache<Integer, String> cache = client.cache(DEFAULT_CACHE_NAME);

        IgniteTransactions transactions = client.transactions();

        for (int i = 0; i < EXPECTED_TXS_FINISH_COUNT; i++) {
            try (Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation)) {
                cache.put(i, "value");

                if (commit)
                    tx.commit();
                else
                    tx.rollback();
            }
        }

        if (commit) {
            assertTrue(GridTestUtils.waitForCondition(() -> {
                boolean metricsUpdated =
                    client.cache(DEFAULT_CACHE_NAME).metrics().getCacheTxCommits() == EXPECTED_TXS_FINISH_COUNT
                        && transactions.metrics().txCommits() == EXPECTED_TXS_FINISH_COUNT;

                return metricsUpdated;

            }, SECONDS.toMillis(3)));
        }
        else {
            assertTrue(GridTestUtils.waitForCondition(() -> {
                boolean metricsUpdated =
                    client.cache(DEFAULT_CACHE_NAME).metrics().getCacheTxRollbacks() == EXPECTED_TXS_FINISH_COUNT
                        && transactions.metrics().txRollbacks() == EXPECTED_TXS_FINISH_COUNT;

                return metricsUpdated;

            }, SECONDS.toMillis(3)));
        }
    }
}
