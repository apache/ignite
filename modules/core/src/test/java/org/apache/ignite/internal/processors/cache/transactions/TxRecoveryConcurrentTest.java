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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
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

        final int key = 0;

        for (int iter = 0; iter < 100; iter++) {
            startGrid(iter + 2);

            awaitPartitionMapExchange();

            final Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);
            final List<Ignite> backups = backupNodes(key, DEFAULT_CACHE_NAME);

            final IgniteCache<Object, Object> cache = primary.cache(DEFAULT_CACHE_NAME);

            final TransactionProxyImpl<?, ?> tx = (TransactionProxyImpl<?, ?>)primary.transactions().txStart();

            cache.put(key, key);

            tx.tx().prepare(true);

            for (Ignite grid : G.allGrids())
                assertTrue(((IgniteEx)grid).context().cache().context().tm().activeTransactions().size() == 1);

            final Collection<IgniteInternalTx> backupTransactions = new LinkedList<>();

            for (Ignite backup : backups)
                backupTransactions.addAll(((IgniteEx)backup).context().cache().context().tm().activeTransactions());

            assertTrue(backupTransactions.size() == 2);

            final CountDownLatch ensureBothPoolsAreBlockedLatch = new CountDownLatch(2);
            final CountDownLatch unblockBothPoolsLatch = new CountDownLatch(1);

            final Runnable poolBlockerTask = () -> {
                try {
                    ensureBothPoolsAreBlockedLatch.countDown();
                    unblockBothPoolsLatch.await();
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            };

            final IgniteEx blockedBackup = (IgniteEx)backups.get(0);

            blockedBackup.context().pools().getSystemExecutorService().execute(poolBlockerTask);

            blockedBackup.context().pools().getStripedExecutorService().execute(0, poolBlockerTask);

            ensureBothPoolsAreBlockedLatch.await();

            runAsync(primary::close);

            waitForTxRecoveryRequestEnqueuedOn(blockedBackup);

            waitForTxRecoveryTaskEnqueuedOn(blockedBackup);

            // Unblock processing in blocked backup node. Simultaneously in striped and system pools to start recovery
            // procedure and the tx recovery request processing at the "same" moment (for the same transaction). This
            // should increase chances for race condition occur in the IgniteTxAdapter#markFinalizing.
            unblockBothPoolsLatch.countDown();

            waitForTopology(2);

            awaitPartitionMapExchange();

            for (IgniteInternalTx transaction : backupTransactions)
                assertTrue(transaction.finishFuture().isDone());
        }
    }

    /** */
    private void waitForTxRecoveryRequestEnqueuedOn(IgniteEx grid) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() ->
            grid.context().pools().getStripedExecutorService().queueStripeSize(0) > 0, 5_000, 10));
    }

    /** */
    private void waitForTxRecoveryTaskEnqueuedOn(IgniteEx grid) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() ->
            !((ThreadPoolExecutor)grid.context().pools().getSystemExecutorService()).getQueue().isEmpty(), 5_000, 10));
    }
}
