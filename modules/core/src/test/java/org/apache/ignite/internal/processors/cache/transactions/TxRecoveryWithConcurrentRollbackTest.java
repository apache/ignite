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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 */
public class TxRecoveryWithConcurrentRollbackTest extends GridCommonAbstractTest {
    /** Backups. */
    private int backups;

    /** Persistence. */
    private boolean persistence;

    /** Sync mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        if (persistence) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration().
                    setWalSegmentSize(4 * 1024 * 1024).
                    setWalHistorySize(1000).
                    setCheckpointFrequency(Integer.MAX_VALUE).
                    setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(50 * 1024 * 1024)));
        }

        cfg.setActiveOnStart(false);
        cfg.setClientMode("client".equals(name));
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setCacheMode(PARTITIONED).
            setBackups(backups).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(syncMode));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * The test enforces specific order in messages processing during concurrent tx rollback and tx recovery due to
     * node left.
     * <p>
     * Expected result: both DHT transactions produces same COMMITTED state on tx finish.
     * */
    @Test
    public void testRecoveryNotBreakingTxAtomicityOnNearFail() throws Exception {
        backups = 1;
        persistence = false;

        final IgniteEx node0 = startGrids(3);
        node0.cluster().active(true);

        final Ignite client = startGrid("client");

        final IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        final List<Integer> g0Keys = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 100);
        final List<Integer> g1Keys = primaryKeys(grid(1).cache(DEFAULT_CACHE_NAME), 100);

        final List<Integer> g2BackupKeys = backupKeys(grid(2).cache(DEFAULT_CACHE_NAME), 100, 0);

        Integer k1 = null;
        Integer k2 = null;

        for (Integer key : g2BackupKeys) {
            if (g0Keys.contains(key))
                k1 = key;
            else if (g1Keys.contains(key))
                k2 = key;

            if (k1 != null && k2 != null)
                break;
        }

        assertNotNull(k1);
        assertNotNull(k2);

        List<IgniteInternalTx> txs0 = null;
        List<IgniteInternalTx> txs1 = null;

        CountDownLatch stripeBlockLatch = new CountDownLatch(1);

        int[] stripeHolder = new int[1];

        try (final Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(k1, Boolean.TRUE);
            cache.put(k2, Boolean.TRUE);

            TransactionProxyImpl p = (TransactionProxyImpl)tx;
            p.tx().prepare(true);

            txs0 = txs(grid(0));
            txs1 = txs(grid(1));
            List<IgniteInternalTx> txs2 = txs(grid(2));

            assertTrue(txs0.size() == 1);
            assertTrue(txs1.size() == 1);
            assertTrue(txs2.size() == 2);

            // Prevent recovery request for grid1 tx branch to go to grid0.
            TestRecordingCommunicationSpi.spi(grid(1)).blockMessages(GridCacheTxRecoveryRequest.class, grid(0).name());
            // Prevent finish(false) request processing on node0.
            TestRecordingCommunicationSpi.spi(client).blockMessages(GridNearTxFinishRequest.class, grid(0).name());

            int stripe = U.safeAbs(p.tx().xidVersion().hashCode());

            stripeHolder[0] = stripe;

            // Blocks stripe processing for rollback request on node1.
            grid(1).context().getStripedExecutorService().execute(stripe, () -> U.awaitQuiet(stripeBlockLatch));
            // Dummy task to ensure msg is processed.
            grid(1).context().getStripedExecutorService().execute(stripe, () -> {});

            runAsync(() -> {
                TestRecordingCommunicationSpi.spi(client).waitForBlocked();

                client.close();

                return null;
            });

            tx.rollback();

            fail();
        }
        catch (Exception ignored) {
            // Expected.
        }

        // Wait until tx0 is committed by recovery on node0.
        assertNotNull(txs0);
        try {
            txs0.get(0).finishFuture().get(3_000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            // If timeout happens recovery message from g0 to g1 is mapped to the same stripe as near finish request.
            // We will complete latch to allow sequential processing.
            stripeBlockLatch.countDown();

            // Wait until sequential processing is finished.
            assertTrue("sequential processing", GridTestUtils.waitForCondition(() ->
                grid(1).context().getStripedExecutorService().queueStripeSize(stripeHolder[0]) == 0, 5_000));

            // Unblock recovery message from g1 to g0 because tx is in RECOVERY_FINISH state and waits for recovery end.
            TestRecordingCommunicationSpi.spi(grid(1)).stopBlock();

            txs0.get(0).finishFuture().get();
            txs1.get(0).finishFuture().get();

            final TransactionState s1 = txs0.get(0).state();
            final TransactionState s2 = txs1.get(0).state();

            assertEquals(s1, s2);

            return;
        }

        // Release rollback request processing, triggering an attempt to rollback the transaction during recovery.
        stripeBlockLatch.countDown();

        // Wait until finish message is processed.
        assertTrue("concurrent processing", GridTestUtils.waitForCondition(() ->
            grid(1).context().getStripedExecutorService().queueStripeSize(stripeHolder[0]) == 0, 5_000));

        // Proceed with recovery on grid1 -> grid0. Tx0 is committed so tx1 also should be committed.
        TestRecordingCommunicationSpi.spi(grid(1)).stopBlock();

        assertNotNull(txs1);
        txs1.get(0).finishFuture().get();

        final TransactionState s1 = txs0.get(0).state();
        final TransactionState s2 = txs1.get(0).state();

        assertEquals(s1, s2);
    }

    /** */
    @Test
    public void testRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail_FULL_SYNC() throws Exception {
        doTestRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail(FULL_SYNC);
    }

    /** */
    @Test
    public void testRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail_PRIMARY_SYNC() throws Exception {
        doTestRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail(PRIMARY_SYNC);
    }

    /** */
    @Test
    public void testRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail_FULL_ASYNC() throws Exception {
        doTestRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail(FULL_ASYNC);
    }

    /**
     * Stop near and primary node after primary tx is rolled back with enabled persistence.
     * <p>
     * Expected result: after restarting a primary node all partitions are consistent.
     */
    private void doTestRecoveryNotBreakingTxAtomicityOnNearAndPrimaryFail(CacheWriteSynchronizationMode syncMode)
        throws Exception {
        backups = 2;
        persistence = true;
        this.syncMode = syncMode;

        final IgniteEx node0 = startGrids(3);
        node0.cluster().active(true);

        final Ignite client = startGrid("client");

        final IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        final Integer pk = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        IgniteInternalFuture<Void> fut = null;

        List<IgniteInternalTx> tx0 = null;
        List<IgniteInternalTx> tx2 = null;

        try (final Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(pk, Boolean.TRUE);

            TransactionProxyImpl p = (TransactionProxyImpl)tx;
            p.tx().prepare(true);

            tx0 = txs(grid(0));
            tx2 = txs(grid(2));

            TestRecordingCommunicationSpi.spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridDhtTxFinishRequest);

            fut = runAsync(() -> {
                TestRecordingCommunicationSpi.spi(grid(1)).waitForBlocked(2);

                client.close();
                grid(1).close();

                return null;
            });

            tx.rollback();
        }
        catch (Exception e) {
            // No-op.
        }

        fut.get();

        final IgniteInternalTx tx_0 = tx0.get(0);
        tx_0.finishFuture().get();

        final IgniteInternalTx tx_2 = tx2.get(0);
        tx_2.finishFuture().get();

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        startGrid(1);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @param g Grid.
     */
    private List<IgniteInternalTx> txs(IgniteEx g) {
        return new ArrayList<>(g.context().cache().context().tm().activeTransactions());
    }
}
