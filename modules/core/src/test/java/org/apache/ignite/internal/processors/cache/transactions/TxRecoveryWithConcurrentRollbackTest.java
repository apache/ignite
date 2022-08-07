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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.util.typedef.PE;
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
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

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
        cfg.setClientMode(name.startsWith("client"));
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureHandler(new StopNodeFailureHandler());

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
        node0.cluster().state(ACTIVE);

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
            spi(grid(1)).blockMessages(GridCacheTxRecoveryRequest.class, grid(0).name());
            // Prevent finish(false) request processing on node0.
            spi(client).blockMessages(GridNearTxFinishRequest.class, grid(0).name());

            int stripe = U.safeAbs(p.tx().xidVersion().hashCode());

            stripeHolder[0] = stripe;

            // Blocks stripe processing for rollback request on node1.
            grid(1).context().pools().getStripedExecutorService().execute(stripe, () -> U.awaitQuiet(stripeBlockLatch));
            // Dummy task to ensure msg is processed.
            grid(1).context().pools().getStripedExecutorService().execute(stripe, () -> {});

            runAsync(() -> {
                spi(client).waitForBlocked();

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
                grid(1).context().pools().getStripedExecutorService().queueStripeSize(stripeHolder[0]) == 0, 5_000));

            // Unblock recovery message from g1 to g0 because tx is in RECOVERY_FINISH state and waits for recovery end.
            spi(grid(1)).stopBlock();

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
            grid(1).context().pools().getStripedExecutorService().queueStripeSize(stripeHolder[0]) == 0, 5_000));

        // Proceed with recovery on grid1 -> grid0. Tx0 is committed so tx1 also should be committed.
        spi(grid(1)).stopBlock();

        assertNotNull(txs1);
        txs1.get(0).finishFuture().get();

        final TransactionState s1 = txs0.get(0).state();
        final TransactionState s2 = txs1.get(0).state();

        assertEquals(s1, s2);
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
        backups = 2;
        persistence = false;

        for (int iter = 0; iter < 100; iter++) {
            final IgniteEx grid0 = startGrid(0);

            final IgniteEx grid1 = startGrid(1, (UnaryOperator<IgniteConfiguration>)cfg -> cfg
                .setSystemThreadPoolSize(1).setStripedPoolSize(1));

            final IgniteEx grid2 = startGrid(2);

            grid0.cluster().state(ACTIVE);

            final IgniteCache<Object, Object> cache = grid2.cache(DEFAULT_CACHE_NAME);

            final Transaction tx = grid2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

            final Integer g2Key = primaryKeys(cache, 1, 0).get(0);

            cache.put(g2Key, Boolean.TRUE);

            final TransactionProxyImpl<?, ?> p = (TransactionProxyImpl<?, ?>)tx;

            p.tx().prepare(true);

            final List<IgniteInternalTx> txs0 = txs(grid0);
            final List<IgniteInternalTx> txs1 = txs(grid1);
            final List<IgniteInternalTx> txs2 = txs(grid2);

            assertTrue(txs0.size() == 1);
            assertTrue(txs1.size() == 1);
            assertTrue(txs2.size() == 1);

            final CountDownLatch grid1NodeLeftEventLatch = new CountDownLatch(1);

            grid1.events().localListen(new PE() {
                @Override public boolean apply(Event evt) {
                    grid1NodeLeftEventLatch.countDown();

                    return true;
                }
            }, EventType.EVT_NODE_LEFT);

            final CountDownLatch grid1BlockLatch = new CountDownLatch(1);

            // Block recovery procedure processing on grid1.
            grid1.context().pools().getSystemExecutorService().execute(() -> U.awaitQuiet(grid1BlockLatch));

            final int stripe = U.safeAbs(p.tx().xidVersion().hashCode());

            // Block stripe tx recovery request processing on grid1.
            grid1.context().pools().getStripedExecutorService().execute(stripe, () -> U.awaitQuiet(grid1BlockLatch));

            // Prevent finish request processing on grid0.
            spi(grid2).blockMessages(GridDhtTxFinishRequest.class, grid0.name());

            // Prevent finish request processing on grid1.
            spi(grid2).blockMessages(GridDhtTxFinishRequest.class, grid1.name());

            runAsync(() -> {
                grid2.close();

                return null;
            });

            try {
                tx.close();
            }
            catch (Exception ignored) {
                // Don't bother if the transaction close throws in case grid2 appear to be stopping or stopped already
                // for this thread.
            }

            // Wait until grid1 node detects primary node left.
            grid1NodeLeftEventLatch.await();

            // Wait until grid1 receives the tx recovery request and the corresponding processing task is added into
            // the queue.
            assertTrue("tx recovery request received on grid1", GridTestUtils.waitForCondition(() -> grid1.context()
                .pools().getStripedExecutorService().queueStripeSize(stripe) == 1, 5_000));

            // Unblock processing in grid1. Simultaneously in striped and system pools to start recovery procedure and
            // the tx recovery request processing at the "same" moment (for the same transaction). This should increase
            // chances for race condition occur in the IgniteTxAdapter#markFinalizing.
            grid1BlockLatch.countDown();

            waitForTopology(2);

            awaitPartitionMapExchange();

            assertTrue(txs0.get(0).finishFuture().isDone());
            assertTrue(txs1.get(0).finishFuture().isDone());

            stopAllGrids();
        }
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
        node0.cluster().state(ACTIVE);

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

            spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridDhtTxFinishRequest);

            fut = runAsync(() -> {
                spi(grid(1)).waitForBlocked(2);

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

    /**
     * Start 3 servers,
     * start 2 clients,
     * start two OPTIMISTIC transactions with the same key from different client nodes,
     * trying to transfer both to PREPARED state,
     * stop one client node.
     */
    @Test
    public void testTxDoesntBecomePreparedAfterError() throws Exception {
        backups = 2;
        persistence = true;
        syncMode = FULL_ASYNC;

        final IgniteEx node0 = startGrids(3);

        node0.cluster().state(ACTIVE);

        final IgniteEx client1 = startGrid("client1");
        final IgniteEx client2 = startGrid("client2");

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = client1.cache(DEFAULT_CACHE_NAME);
        final IgniteCache<Object, Object> cache2 = client2.cache(DEFAULT_CACHE_NAME);

        final Integer pk = primaryKey(node0.cache(DEFAULT_CACHE_NAME));

        CountDownLatch txPrepareLatch = new CountDownLatch(1);

        GridTestUtils.runMultiThreadedAsync(() -> {
            try (final Transaction tx = client1.transactions().withLabel("tx1").txStart(OPTIMISTIC, READ_COMMITTED, 5000, 1)) {
                cache.put(pk, Boolean.TRUE);

                TransactionProxyImpl p = (TransactionProxyImpl)tx;

                // To prevent tx rollback on exit from try-with-resource block, this should cause another tx timeout fail.
                spi(client1).blockMessages((node, msg) -> msg instanceof GridNearTxFinishRequest);

                log.info("Test, preparing tx: xid=" + tx.xid() + ", tx=" + tx);

                // Doing only prepare to try to lock the key, commit is not needed here.
                p.tx().prepareNearTxLocal();

                p.tx().currentPrepareFuture().listen(fut -> txPrepareLatch.countDown());
            }
            catch (Exception e) {
                // No-op.
            }
        }, 1, "tx1-thread");

        try (final Transaction tx = client2.transactions().withLabel("tx2").txStart(OPTIMISTIC, READ_COMMITTED, 5000, 1)) {
            cache2.put(pk, Boolean.TRUE);

            TransactionProxyImpl p = (TransactionProxyImpl)tx;

            log.info("Test, preparing tx: xid=" + tx.xid() + ", tx=" + tx);

            p.tx().prepareNearTxLocal();

            p.tx().currentPrepareFuture().listen(fut -> txPrepareLatch.countDown());

            txPrepareLatch.await(6, TimeUnit.SECONDS);

            if (txPrepareLatch.getCount() > 0)
                fail("Failed to await for tx prepare.");

            AtomicReference<GridDhtTxLocal> dhtTxLocRef = new AtomicReference<>();

            assertTrue(waitForCondition(() -> {
                dhtTxLocRef.set((GridDhtTxLocal)txs(node0).stream()
                    .filter(t -> t.state() == TransactionState.PREPARING)
                    .findFirst()
                    .orElse(null)
                );

                return dhtTxLocRef.get() != null;
            }, 6_000));

            assertNotNull(dhtTxLocRef.get());

            UUID clientNodeToFail = dhtTxLocRef.get().eventNodeId();

            GridDhtTxPrepareFuture prep = GridTestUtils.getFieldValue(dhtTxLocRef.get(), "prepFut");

            prep.get();

            List<IgniteInternalTx> txs = txs(node0);

            String txsStr = txs.stream().map(Object::toString).collect(Collectors.joining(", "));

            log.info("Transactions check point [count=" + txs.size() + ", txs=" + txsStr + "]");

            if (clientNodeToFail.equals(client1.localNode().id()))
                client1.close();
            else if (clientNodeToFail.equals(client2.localNode().id()))
                client2.close();
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        U.sleep(500);

        assertEquals(3, grid(1).context().discovery().aliveServerNodes().size());

        assertEquals(txs(client1).toString() + ", " + txs(client2).toString(), 1, txs(client1).size() + txs(client2).size());
    }
}
