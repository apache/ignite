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

import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests concurrent execution of the tx recovery.
 */
public class TxRecoveryConcurrentOnPrimaryFailTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setSystemThreadPoolSize(1);

        cfg.setStripedPoolSize(1);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setCacheMode(PARTITIONED)
            .setBackups(2).setAtomicityMode(TRANSACTIONAL).setAffinity(new RendezvousAffinityFunction(false, 1)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * The test enforces the concurrent processing of the same prepared transaction
     * both in the tx recovery procedure started due to primary node left and in the
     * tx recovery request handler invoked by message from another backup node.
     * <p>
     * The idea is to have a 3-nodes cluster and a cache with 2 backups. So there
     * will be 2 backup nodes to execute the tx recovery in parallel if primary one
     * would fail. These backup nodes will send the tx recovery requests to each
     * other, so the tx recovery request handler will be invoked as well.
     * <p>
     * Use several attempts to reproduce the race condition.
     * <p>
     * Expected result: transaction is finished on both backup nodes and the partition
     * map exchange is completed as well.
     */
    @Test
    public void testRecoveryNotDeadLockOnPrimaryFail() throws Exception {
        final IgniteEx grid0 = startGrid(0);

        final IgniteEx grid1 = startGrid(1);

        final CyclicBarrier grid1BlockerBarrier = new CyclicBarrier(3);

        final Runnable grid1BlockerTask = () -> {
            try {
                grid1BlockerBarrier.await();
            }
            catch (InterruptedException | BrokenBarrierException e) {
                // Just supress.
            }
        };

        for (int iter = 0; iter < 100; iter++) {
            final IgniteEx grid2 = startGrid(2);

            awaitPartitionMapExchange();

            final IgniteCache<Object, Object> cache = grid2.cache(DEFAULT_CACHE_NAME);

            final Transaction tx = grid2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

            // Key for which the grid2 node is primary.
            final Integer grid2PrimaryKey = primaryKeys(cache, 1, 0).get(0);

            cache.put(grid2PrimaryKey, Boolean.TRUE);

            final TransactionProxyImpl<?, ?> p = (TransactionProxyImpl<?, ?>)tx;

            p.tx().prepare(true);

            final Collection<IgniteInternalTx> txs0 = grid0.context().cache().context().tm().activeTransactions();
            final Collection<IgniteInternalTx> txs1 = grid1.context().cache().context().tm().activeTransactions();
            final Collection<IgniteInternalTx> txs2 = grid2.context().cache().context().tm().activeTransactions();

            assertTrue(txs0.size() == 1);
            assertTrue(txs1.size() == 1);
            assertTrue(txs2.size() == 1);

            final IgniteInternalTx tx0 = txs0.iterator().next();
            final IgniteInternalTx tx1 = txs1.iterator().next();

            // Block recovery procedure processing on grid1.
            grid1.context().pools().getSystemExecutorService().execute(grid1BlockerTask);

            // Block stripe tx recovery request processing on grid1 (note that the only stripe is configured in the executor).
            grid1.context().pools().getStripedExecutorService().execute(0, grid1BlockerTask);

            // Prevent finish request processing on grid0.
            spi(grid2).blockMessages(GridDhtTxFinishRequest.class, grid0.name());

            // Prevent finish request processing on grid1.
            spi(grid2).blockMessages(GridDhtTxFinishRequest.class, grid1.name());

            runAsync(() -> {
                grid2.close();

                return null;
            });

            // Wait until grid1 receives the tx recovery request and the corresponding processing task is added into
            // the stripe queue (note that the only stripe is configured in the executor).
            assertTrue("failed to wait the tx recovery request received on grid1", GridTestUtils.waitForCondition(() ->
                grid1.context().pools().getStripedExecutorService().queueStripeSize(0) == 1, 5_000, 10));

            // Unblock processing in grid1. Simultaneously in striped and system pools to start recovery procedure and
            // the tx recovery request processing at the "same" moment (for the same transaction). This should increase
            // chances for race condition occur in the IgniteTxAdapter#markFinalizing.
            grid1BlockerBarrier.await();

            waitForTopology(2);

            awaitPartitionMapExchange();

            assertTrue(tx0.finishFuture().isDone());
            assertTrue(tx1.finishFuture().isDone());
        }
    }
}
