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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests concurrent execution of the tx recovery.
 */
public class TxRecoveryConcurrentTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setSystemThreadPoolSize(1);
        cfg.setStripedPoolSize(1);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setAtomicityMode(TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 1)));

        return cfg;
    }

    /**
     * The test enforces the concurrent processing of the same prepared transaction
     * both in the tx recovery procedure started due to near and primary node left and in the
     * tx recovery request handler invoked by message from another backup node.
     * <p>
     * The idea is to have a 3-nodes cluster and a cache with 2 backups. So there
     * will be 2 backup nodes to execute the tx recovery in parallel if primary one
     * would fail. These backup nodes will send the tx recovery requests to each
     * other, so the tx recovery request handler will be invoked as well.
     * <p>
     * Blocking is used to force concurrent processing on one of the backup nodes. Another
     * backup works unconstrained to provide the right environment for the blocked one.
     * In particular, it should send a tx recovery request to the blocked backup.
     * <p>
     * Use several attempts to reproduce the race condition in the blocked backup node.
     * <p>
     * Expected result: transaction is finished on both backup nodes and the partition
     * map exchange is completed as well.
     */
    @Test
    public void testRecoveryNotDeadLockOnNearAndPrimaryFail() throws Exception {
        startGrids(2);

        final CyclicBarrier ensurePoolsAreBlockedBarrier = new CyclicBarrier(3);

        final CyclicBarrier unblockPoolsBarrier = new CyclicBarrier(3);

        final Runnable backupBlockerTask = () -> {
            try {
                ensurePoolsAreBlockedBarrier.await();

                unblockPoolsBarrier.await();
            }
            catch (InterruptedException | BrokenBarrierException ignored) {
                // Just supress.
            }
        };

        for (int iter = 0; iter < 100; iter++) {
            startGrid(iter + 2);

            awaitPartitionMapExchange();

            final Ignite primary = primaryNode(0, DEFAULT_CACHE_NAME);

            final List<Ignite> backups = backupNodes(0, DEFAULT_CACHE_NAME);

            final IgniteCache<Object, Object> cache = primary.cache(DEFAULT_CACHE_NAME);

            final TransactionProxyImpl<?, ?> tx = (TransactionProxyImpl<?, ?>)primary.transactions().txStart();

            cache.put(0, 0);

            tx.tx().prepare(true);

            for (Ignite grid : G.allGrids())
                assertTrue(((IgniteEx)grid).context().cache().context().tm().activeTransactions().size() == 1);

            Collection<IgniteInternalTx> backupTransactions = new LinkedList<>();

            for (Ignite grid : backups)
                backupTransactions.addAll(((IgniteEx)grid).context().cache().context().tm().activeTransactions());

            assertTrue(backupTransactions.size() == 2);

            // Choose one backup node to force concurrent execution on it.
            final IgniteEx blockedBackup = (IgniteEx)backups.get(0);

            // Block recovery procedure processing on one of the backup nodes.
            blockedBackup.context().pools().getSystemExecutorService().execute(backupBlockerTask);

            // Block stripe tx recovery request processing on one of the backup nodes (note that the only stripe is
            // configured in the executor).
            blockedBackup.context().pools().getStripedExecutorService().execute(0, backupBlockerTask);

            // Prevent tx finish request processing on both backup nodes.
            for (Ignite grid : backups)
                spi(primary).blockMessages(GridDhtTxFinishRequest.class, grid.name());

            // Wait both pools are blocked in the blocked backup node.
            ensurePoolsAreBlockedBarrier.await();

            runAsync(primary::close);

            // Wait until blocked backup node receives the tx recovery request and the corresponding processing task is
            // added into the stripe queue (note that the only stripe is configured in the executor).
            assertTrue("failed to wait the tx recovery request on backup", GridTestUtils.waitForCondition(() ->
                blockedBackup.context().pools().getStripedExecutorService().queueStripeSize(0) > 0, 5_000, 10));

            // Wait until blocked backup node detects primary node failure and the tx recovery task is added into
            // the system pool queue.
            assertTrue("failed to wait the tx recovery task on backup", GridTestUtils.waitForCondition(() ->
                !((ThreadPoolExecutor)blockedBackup.context().pools().getSystemExecutorService()).getQueue().isEmpty(), 5_000, 10));

            // Unblock processing in blocked backup node. Simultaneously in striped and system pools to start recovery
            // procedure and the tx recovery request processing at the "same" moment (for the same transaction). This
            // should increase chances for race condition occur in the IgniteTxAdapter#markFinalizing.
            unblockPoolsBarrier.await();

            waitForTopology(2);

            awaitPartitionMapExchange();

            for (IgniteInternalTx transaction : backupTransactions)
                assertTrue(transaction.finishFuture().isDone());
        }
    }
}
