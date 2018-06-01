package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class GridCachePartitionedClientTxMetricsTest extends GridCacheAbstractSelfTest {
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
    public void testCommitPessimisticReadCommittedTxOnClient() {
        doPutOnClient(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     *
     */
    public void testCommitPessimisticRepeatableReadTxOnClient() {
        doPutOnClient(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     *
     */
    public void testCommitPessimisticSerializableTxOnClient() {
        doPutOnClient(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     *
     */
    public void testCommitOptimisticReadCommittedTxOnClient() {
        doPutOnClient(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     *
     */
    public void testCommitOptimisticRepeatableReadTxOnClient() {
        doPutOnClient(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     *
     */
    public void testCommitOptimisticSerializableTxOnClient() {
        doPutOnClient(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     *
     */
    public void testRollbackPessimisticReadCommittedTxOnClient() {
        doPutOnClient(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     *
     */
    public void testRollbackPessimisticRepeatableReadTxOnClient() {
        doPutOnClient(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     *
     */
    public void testRollbackPessimisticSerializableTxOnClient() {
        doPutOnClient(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     *
     */
    public void testRollbackOptimisticReadCommittedTxOnClient() {
        doPutOnClient(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     *
     */
    public void testRollbackOptimisticRepeatableReadTxOnClient() {
        doPutOnClient(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     *
     */
    public void testRollbackOptimisticSerializableTxOnClient() {
        doPutOnClient(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @param transactionConcurrency transaction concurrency.
     * @param transactionIsolation transaction isolation.
     * @param commit {@code True} if should commit, otherwise - rollback transaction.
     */
    private void doPutOnClient(TransactionConcurrency transactionConcurrency,
        TransactionIsolation transactionIsolation, boolean commit) {
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

        try {
            Thread.sleep(2 * METRICS_UPDATE_FREQUENCY);
        }
        catch (InterruptedException e) {
            Thread.interrupted();

            throw new RuntimeException(e);
        }

        if (commit) {
            assertEquals(EXPECTED_TXS_FINISH_COUNT, client.cache(DEFAULT_CACHE_NAME).metrics().getCacheTxCommits());
            assertEquals(EXPECTED_TXS_FINISH_COUNT, transactions.metrics().txCommits());
        }
        else {
            assertEquals(EXPECTED_TXS_FINISH_COUNT, client.cache(DEFAULT_CACHE_NAME).metrics().getCacheTxRollbacks());
            assertEquals(EXPECTED_TXS_FINISH_COUNT, transactions.metrics().txRollbacks());
        }
    }
}