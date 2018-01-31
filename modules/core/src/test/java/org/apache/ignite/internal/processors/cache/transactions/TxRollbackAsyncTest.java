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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

import static java.lang.Thread.sleep;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.*;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 * Tests an ability to async rollback near transactions.
 */
public class TxRollbackAsyncTest extends GridCommonAbstractTest {
    /** */
    public static final int DURATION = 20_000;

    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String CACHE_NAME_2 = "test2";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final int THREADS_CNT = 1;

    /** */
    public static final long MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        if (persistenceEnabled())
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY).setPageSize(1024).
                setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                    setInitialSize(100 * MB).setMaxSize(100 * MB)));

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            if (nearCacheEnabled())
                ccfg.setNearConfiguration(new NearCacheConfiguration());

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @return Near cache flag.
     */
    protected boolean nearCacheEnabled() {
        return false;
    }

    /**
     *
     * @return {@code True} if persistence must be enabled for test.
     */
    protected boolean persistenceEnabled() { return false; }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        final IgniteEx crd = startGrid(0);

        startGridsMultiThreaded(1, GRID_CNT - 1);

        crd.active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Started client.
     * @throws Exception If f nodeailed.
     */
    private Ignite startClient() throws Exception {
        Ignite client = startGrid("client1");

        assertTrue(client.configuration().isClientMode());

        if (nearCacheEnabled())
            client.createNearCache(CACHE_NAME, new NearCacheConfiguration<>());
        else
            assertNotNull(client.cache(CACHE_NAME));

        return client;
    }

    /**
     *
     */
    public void testRollbackSync() throws Exception {
        startClient();

        for (Ignite ignite : G.allGrids()) {
            testRollbackSync0(ignite);

            ignite.cache(CACHE_NAME).clear();
        }
    }

    /**
     *
     */
    private void testRollbackSync0(Ignite near) throws Exception {
        // Normal rollback after put.
        Transaction tx = near.transactions().txStart(PESSIMISTIC, READ_COMMITTED);

        near.cache(CACHE_NAME).put(0, 0);

        tx.rollback();

        assertNull(near.cache(CACHE_NAME).get(0));

        // Normal rollback before put.
        tx = near.transactions().txStart();

        tx.rollback();

        near.cache(CACHE_NAME).put(0, 1);

        assertEquals(1, near.cache(CACHE_NAME).get(0));

        // Normal rollback async after put.
        tx = near.transactions().txStart();

        near.cache(CACHE_NAME).put(1, 0);

        final Transaction finalTx = tx;

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                finalTx.rollback();
            }
        }, 1, "rollback-thread").get();

        try {
            assertNull(near.cache(CACHE_NAME).get(0));

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        try {
            near.cache(CACHE_NAME).put(1, 1);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        try {
            near.cache(CACHE_NAME).remove(0);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        // Normal rollback async before put.
        tx = near.transactions().txStart();

        final Transaction finalTx1 = tx;

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                finalTx1.rollback();
            }
        }, 1, "rollback-thread").get();

        try {
            assertNull(near.cache(CACHE_NAME).get(0));

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        try {
            near.cache(CACHE_NAME).put(1, 1);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        try {
            near.cache(CACHE_NAME).remove(0);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        checkFutures();
    }

    /**
     *
     */
    public void testSynchronousRollback() throws Exception {
        Ignite client = startClient();

        for (int i = 0; i < GRID_CNT; i++)
            testSynchronousRollback0(grid(0), grid(i), false);

        testSynchronousRollback0(grid(0), client, false);

        for (int i = 0; i < GRID_CNT; i++)
            testSynchronousRollback0(grid(0), grid(i), true);

        testSynchronousRollback0(grid(0), client, true);

        for (int i = 0; i < GRID_CNT; i++)
            testSynchronousRollback0(grid(1), grid(i), false);

        testSynchronousRollback0(grid(1), client, false);

        for (int i = 0; i < GRID_CNT; i++)
            testSynchronousRollback0(grid(1), grid(i), true);

        testSynchronousRollback0(grid(1), client, true);
    }

    /**
     *
     */
    private void testSynchronousRollback0(Ignite holdLockNode, final Ignite tryLockNode, final boolean useTimeout) throws Exception {
        final CountDownLatch keyLocked = new CountDownLatch(1);

        CountDownLatch waitCommit = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = lockAsync(holdLockNode, keyLocked, waitCommit, 0, true);

        U.awaitQuiet(keyLocked);

        final CountDownLatch rollbackLatch = new CountDownLatch(1);

        final int txCnt = 1000;

        final IgniteKernal k = (IgniteKernal)tryLockNode;

        final GridCacheSharedContext<Object, Object> ctx = k.context().cache().context();

        final GridCacheContext<Object, Object> cctx = ctx.cacheContext(CU.cacheId(CACHE_NAME));

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(keyLocked);

                for (int i = 0; i < txCnt; i++) {
                    GridNearTxLocal tx0 = ctx.tm().threadLocalTx(cctx);

                    assertTrue(tx0 == null || tx0.state() == ROLLED_BACK);

                    rollbackLatch.countDown();

                    try (Transaction tx = tryLockNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ,
                        useTimeout ? 500 : 0, 1)) {
                        // Will block on lock request until rolled back asynchronously.
                        tryLockNode.cache(CACHE_NAME).get(0);

                        fail("Tx must be rolled back asynchronously");
                    }
                    catch (Exception e) {
                        // Expected.
                    }
                }

                stop.set(true);
            }
        }, 1, "tx-thread");

        IgniteInternalFuture<?> rollbackFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(rollbackLatch);

                doSleep(50);

                Set<IgniteUuid> rolledBackVers = new HashSet<>();

                while(!stop.get()) {
                    for (Transaction tx : tryLockNode.transactions().localActiveTransactions()) {
                        TransactionProxyImpl tx0 = (TransactionProxyImpl)tx;

                        // Roll back only read transactions.
                        if (!tx0.tx().writeMap().isEmpty())
                            continue;

                        if (rolledBackVers.contains(tx.xid()))
                            fail("Rollback version is expected");

                        tx.rollback();

                        rolledBackVers.add(tx.xid());
                    }
                }

                assertEquals("Unexpected size", txCnt, rolledBackVers.size());
            }
        }, 1, "rollback-thread");

        rollbackFut.get();

        txFut.get();

        log.info("All transactions are rolled back: holdLockNode=" + holdLockNode + ", tryLockNode=" + tryLockNode);

        waitCommit.countDown();

        lockFut.get();

        assertEquals(0, holdLockNode.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testRollbackDelayLockRequest() throws Exception {
        final Ignite client = startClient();

        final Ignite prim = primaryNode(0, CACHE_NAME);

        final TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearLockRequest.class, prim.name());

        final IgniteInternalFuture<Void> rollbackFut = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                spi.waitForBlocked();

                client.transactions().localActiveTransactions().iterator().next().rollback();

                return null;
            }
        }, "tx-rollback-thread");

        try(final Transaction tx = client.transactions().txStart()) {
            client.cache(CACHE_NAME).put(0, 0);

            fail();
        }
        catch (CacheException e) {
            assertTrue(X.hasCause(e, TransactionRollbackException.class));
        }

        rollbackFut.get();

        spi.stopBlock(true);

        doSleep(500);

        checkFutures();
    }

    /**
     *
     */
    public void testRollbackDelayFinishRequest() throws Exception {
        final Ignite client = startClient();

        final Ignite prim = primaryNode(0, CACHE_NAME);

        final TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        final AtomicReference<Transaction> txRef = new AtomicReference<>();

        // Block commit request to primary node.
        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridNearTxFinishRequest) {
                    GridNearTxFinishRequest r = (GridNearTxFinishRequest)msg;

                    return r.commit() && node.equals(prim.cluster().localNode());
                }

                return false;
            }
        });

        final IgniteInternalFuture<Void> rollbackFut = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                spi.waitForBlocked();

                final IgniteFuture<?> fut = txRef.get().rollbackAsync();

                doSleep(500);

                spi.stopBlock(true);

                fut.get();

                return null;
            }
        }, "tx-rollback-thread");

        try(final Transaction tx = client.transactions().txStart()) {
            txRef.set(tx);

            client.cache(CACHE_NAME).put(0, 0);

            tx.commit();
        }
        catch (CacheException e) {
            assertTrue(X.hasCause(e, TransactionRollbackException.class));
        }

        rollbackFut.get();

        doSleep(500);

        checkFutures();
    }

    /**
     *
     */
    public void testRollbackUnderContention() {

    }

    /**
     *
     */
    public void testMixedAsyncRollbackTypes() throws Exception {
        final Ignite client = startClient();

        final AtomicBoolean stop = new AtomicBoolean();

        final int keysCnt = 1000;

        final int txSize = 200;

        for (int k = 0; k < keysCnt; k++)
            grid(0).cache(CACHE_NAME).put(k, (long)0);

        final long seed = System.currentTimeMillis();

        final Random r = new Random(seed);

        log.info("Using seed: " + seed);

        final TransactionConcurrency[] TC_VALS = TransactionConcurrency.values();
        final TransactionIsolation[] TI_VALS = TransactionIsolation.values();

        final LongAdder8 total = new LongAdder8();
        final LongAdder8 completed = new LongAdder8();
        final LongAdder8 failed = new LongAdder8();
        final LongAdder8 rolledBack = new LongAdder8();

        IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    int nodeId = r.nextInt(GRID_CNT + 1);

                    Ignite node = nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                    TransactionConcurrency conc = TC_VALS[r.nextInt(TC_VALS.length)];
                    TransactionIsolation isolation = TI_VALS[r.nextInt(TI_VALS.length)];

                    long timeout = r.nextInt(50) + 50;

                    try (Transaction tx = node.transactions().txStart(conc, isolation, timeout, txSize)) {
                        int[] keys = new int[txSize];

                        for (int i = 0; i < txSize; i++)
                            keys[i] = r.nextInt(keysCnt);

                        Long v = null;
                        for (int i = 0; i < txSize; i++)
                            v = (Long)node.cache(CACHE_NAME).get(keys[i]);

                        sleep(r.nextInt(50));

                        node.cache(CACHE_NAME).put(keys[r.nextInt(txSize)], v + 1);

                        tx.commit();

                        completed.add(1);
                    }
                    catch (Exception e) {
                        failed.add(1);
                    }

                    total.add(1);
                }
            }
        }, THREADS_CNT, "tx-thread");

        final AtomicInteger nodeIdx = new AtomicInteger();

        IgniteInternalFuture<?> rollbackFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int nodeId = nodeIdx.getAndIncrement();

                int concurrentRollbackCnt = 10;

                List<IgniteFuture<?>> futs = new ArrayList<>(concurrentRollbackCnt);

                while (!stop.get()) {
                    Ignite node = nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                    Collection<Transaction> transactions = node.transactions().localActiveTransactions();

                    for (Transaction transaction : transactions) {
                        rolledBack.add(1);

                        try {
                            futs.add(transaction.rollbackAsync());
                        }
                        catch (IgniteException e) {
                            // No-op.
                        }

                        if (futs.size() == concurrentRollbackCnt) {
                            for (IgniteFuture<?> fut : futs)
                                fut.get();

                            futs.clear();
                        }
                    }
                }

                for (IgniteFuture<?> fut : futs)
                    fut.get();

            }
        }, 1, "rollback-thread");

        doSleep(DURATION);

        stop.set(true);

        txFut.get();

        rollbackFut.get();

        log.info("total=" + total.sum() + ", completed=" + completed.sum() + ", failed=" + failed.sum() +
            ", rolledBack=" + rolledBack.sum());

        assertEquals("total != completed + failed", total.sum(), completed.sum() + failed.sum());

        checkFutures();
    }

    /**
     * @param node Near node.
     * @param keyLocked Key locked latch.
     * @param waitCommit Wait commit latch.
     * @param timeout Timeout.
     * @param commit {@code True} If an entry must be committed.
     */
    private IgniteInternalFuture<?> lockAsync(final Ignite node, final CountDownLatch keyLocked,
        final CountDownLatch waitCommit, final int timeout, final boolean commit) throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 1);

                node.cache(CACHE_NAME).put(0, 0);

                keyLocked.countDown();

                if (commit) {
                    U.awaitQuiet(waitCommit);

                    tx.commit();
                }
            }
        }, 1, "tx-lock-thread");
    }

    private IgniteInternalFuture<?> runInTx(final Ignite node, final int timeout, final boolean doCommit,
        final IgniteBiInClosure<Transaction, IgniteCache> clo) throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 1);

                clo.apply(tx, node.cache(CACHE_NAME));

                if (doCommit)
                    tx.commit();
            }
        }, 1, "tx-op-thread");
    }

    /**
     * Checks if all tx futures are finished.
     */
    private void checkFutures() {
        checkFutures(true);
    }

    private void checkFutures(boolean fail) {
        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final Collection<GridCacheFuture<?>> futs = ig.context().cache().context().mvcc().activeFutures();

            for (GridCacheFuture<?> fut : futs)
                log.info("Waiting for future: " + fut);

            if (fail)
                assertTrue("Expecting no active futures: node=" + ig.localNode().id(), futs.isEmpty());
        }
    }

//    /** {@inheritDoc} */
//    @Override protected long getTestTimeout() {
//        return DURATION + 10_000;
//    }

    /**
     * @param cls Message class.
     * @param nodeToBlock Node to block.
     * @param block Block.
     */
    private void toggleBlocking(Class<? extends Message> cls, @Nullable Ignite from, Ignite nodeToBlock, boolean block) {
        for (Ignite ignite : G.allGrids()) {
            if (ignite == nodeToBlock || ignite == from)
                continue;

            final TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            if (block)
                spi.blockMessages(cls, nodeToBlock.name());
            else
                spi.stopBlock(true);
        }
    }

    private void waitForBlock(Ignite from) {

    }

    /**
     * @param tx Tx to rollback.
     */
    private void rollbackAsync(final Transaction tx) throws Exception {
        multithreadedAsync(new Runnable() {
            @Override public void run() {
                tx.rollbackAsync();
            }
        }, 1, "tx-rollback-thread");
    }

}

