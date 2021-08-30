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

import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionMetrics;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 * Transactional cache metrics test.
 */
public abstract class GridCacheTransactionalAbstractMetricsSelfTest extends GridCacheAbstractMetricsSelfTest {
    /** */
    private static final int TX_CNT = 3;

    /** Transaction timeout. */
    private static final long TX_TIMEOUT = 500L;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticReadCommittedCommits() throws Exception {
        testCommits(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticReadCommittedCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticRepeatableReadCommits() throws Exception {
        testCommits(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticRepeatableReadCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSerializableCommits() throws Exception {
        testCommits(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSerializableCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticReadCommittedCommits() throws Exception {
        testCommits(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticReadCommittedCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableReadCommits() throws Exception {
        testCommits(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableReadCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializableCommits() throws Exception {
        testCommits(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializableCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticReadCommittedRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticReadCommittedRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticRepeatableReadRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticRepeatableReadRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSerializableRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSerializableRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticReadCommittedRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticReadCommittedRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableReadRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableReadRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializableRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializableRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSuspendedReadCommittedTxTimeoutRollbacks() throws Exception {
        doTestSuspendedTxTimeoutRollbacks(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSuspendedRepeatableReadTxTimeoutRollbacks() throws Exception {
        doTestSuspendedTxTimeoutRollbacks(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSuspendedSerializableTxTimeoutRollbacks() throws Exception {
        doTestSuspendedTxTimeoutRollbacks(OPTIMISTIC, SERIALIZABLE);
    }

    /** */
    @Test
    public void testCommitTime() {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        HistogramMetricImpl m = metric("CommitTime");

        assertTrue(Arrays.stream(m.value()).allMatch(v -> v == 0));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.put(1, 1);

            tx.commit();
        }

        assertEquals(1, Arrays.stream(m.value()).filter(v -> v == 1).count());
    }

    /** */
    @Test
    public void testRollbackTime() {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        HistogramMetricImpl m = metric("RollbackTime");

        assertTrue(Arrays.stream(m.value()).allMatch(v -> v == 0));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.put(1, 1);

            tx.rollback();
        }

        assertEquals(1, Arrays.stream(m.value()).filter(v -> v == 1).count());
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testCommits(TransactionConcurrency concurrency, TransactionIsolation isolation, boolean put)
        throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < TX_CNT; i++) {
            Transaction tx = grid(0).transactions().txStart(concurrency, isolation);

            if (put)
                for (int j = 0; j < keyCount(); j++)
                    cache.put(j, j);

            // Waiting 30 ms for metrics. U.currentTimeMillis() method has 10 ms discretization.
            U.sleep(30);

            tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            TransactionMetrics metrics = grid(i).transactions().metrics();
            CacheMetrics cacheMetrics = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            if (i == 0) {
                assertEquals(TX_CNT, metrics.txCommits());

                if (put) {
                    assertEquals(TX_CNT, cacheMetrics.getCacheTxCommits());

                    // Expected metric value should be in microseconds.
                    assert cacheMetrics.getAverageTxCommitTime() > 1000 : cacheMetrics.getAverageTxCommitTime();
                }
            }
            else {
                assertEquals(0, metrics.txCommits());
                assertEquals(0, cacheMetrics.getCacheTxCommits());
            }

            assertEquals(0, metrics.txRollbacks());
            assertEquals(0, cacheMetrics.getCacheTxRollbacks());
        }
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testRollbacks(TransactionConcurrency concurrency, TransactionIsolation isolation,
        boolean put) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < TX_CNT; i++) {
            Transaction tx = grid(0).transactions().txStart(concurrency, isolation);

            if (put)
                for (int j = 0; j < keyCount(); j++)
                    cache.put(j, j);

            // Waiting 30 ms for metrics. U.currentTimeMillis() method has 10 ms discretization.
            U.sleep(30);

            tx.rollback();
        }

        for (int i = 0; i < gridCount(); i++) {
            TransactionMetrics metrics = grid(i).transactions().metrics();
            CacheMetrics cacheMetrics = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            assertEquals(0, metrics.txCommits());
            assertEquals(0, cacheMetrics.getCacheTxCommits());

            if (i == 0) {
                assertEquals(TX_CNT, metrics.txRollbacks());

                if (put) {
                    assertEquals(TX_CNT, cacheMetrics.getCacheTxRollbacks());

                    // Expected metric value should be in microseconds.
                    assert cacheMetrics.getAverageTxRollbackTime() > 1000 : cacheMetrics.getAverageTxRollbackTime();
                }
            }
            else {
                assertEquals(0, metrics.txRollbacks());
                assertEquals(0, cacheMetrics.getCacheTxRollbacks());
            }
        }
    }

    /**
     * Metrics test for transaction timeout rollback.
     *
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void doTestSuspendedTxTimeoutRollbacks(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        IgniteTransactions transactions = grid(0).transactions();

        for (int i = 0; i < TX_CNT; i++) {
            Transaction tx = transactions.txStart(concurrency, isolation, TX_TIMEOUT, 0);

            cache.put(1, 1);

            tx.suspend();

            boolean res = GridTestUtils.waitForCondition(() -> tx.state() == ROLLED_BACK, TX_TIMEOUT * 10);

            assertTrue(res);

            tx.close();
        }

        TransactionMetrics txMetrics = transactions.metrics();
        CacheMetrics cacheMetrics = cache.localMetrics();

        assertEquals(0, txMetrics.txCommits());
        assertEquals(0, cacheMetrics.getCacheTxCommits());

        assertEquals(TX_CNT, txMetrics.txRollbacks());
        assertEquals(TX_CNT, cacheMetrics.getCacheTxRollbacks());

        assertTrue(cacheMetrics.getAverageTxRollbackTime() > 0);

        for (int i = 1; i < gridCount(); i++) {
            txMetrics = grid(i).transactions().metrics();
            cacheMetrics = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            assertEquals(0, txMetrics.txCommits());
            assertEquals(0, cacheMetrics.getCacheTxCommits());

            assertEquals(0, txMetrics.txRollbacks());
            assertEquals(0, cacheMetrics.getCacheTxRollbacks());
        }
    }
}
