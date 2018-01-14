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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jsr166.LongAdder8;

import static java.lang.Thread.sleep;
import static java.util.Collections.synchronizedList;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 * Tests an ability to async rollback near transactions.
 */
public class TxRollbackAsyncTest extends GridCommonAbstractTest {
    /** */
    public static final int DURATION = 15_000;

    /** */
    public static final int ROLLBACK_TIMEOUT = 500;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        startGridsMultiThreaded(1, GRID_CNT - 1);

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
     * @param idx Index.
     */
    private Ignite startClient(int idx) throws Exception {
        Ignite client = startGrid("client" + idx);

        assertTrue(client.configuration().isClientMode());

        if (nearCacheEnabled())
            client.createNearCache(CACHE_NAME, new NearCacheConfiguration<>());
        else
            assertNotNull(client.cache(CACHE_NAME));

        return client;
    }

    public void testRaceLockRollback() throws Exception {
        final Ignite client = startClient();

        final CountDownLatch opLatch = new CountDownLatch(1);

        final CountDownLatch op2Latch = new CountDownLatch(1);

        final CountDownLatch commitLatch = new CountDownLatch(1);

        final Ignite prim = primaryNode(0, CACHE_NAME);

        IgniteInternalFuture<?> wLockFut = runInTx(client, 0, true, new IgniteBiInClosure<Transaction, IgniteCache>() {
            @Override public void apply(Transaction tx, IgniteCache cache) {
                cache.put(0, 100);

                opLatch.countDown();

                U.awaitQuiet(commitLatch);
            }
        });

        final AtomicReference<Transaction> txHolder = new AtomicReference<>();

        IgniteInternalFuture<?> rLockFut = runInTx(client, 0, false, new IgniteBiInClosure<Transaction, IgniteCache>() {
            @Override public void apply(Transaction tx, IgniteCache cache) {
                txHolder.set(tx);

                U.awaitQuiet(opLatch);

                toggleBlocking(GridNearLockRequest.class, prim, true);

                op2Latch.countDown();

                cache.get(0);
            }
        });

        U.awaitQuiet(op2Latch);

        // Sleep until lock request is issued.
        Thread.sleep(500);

        try {
            txHolder.get().rollback();
        }
        catch (Throwable e) {
            e.printStackTrace();
        }

        // rollback (no lock is held)
        toggleBlocking(GridNearLockRequest.class, prim, false);

        try {
            rLockFut.get();
        }
        catch (IgniteCheckedException e) {
            // Expected. Lock will be rolled back locally, but request will go to dht node.
        }

        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final Collection<GridCacheFuture<?>> futs = ig.context().cache().context().mvcc().activeFutures();

            for (GridCacheFuture<?> fut : futs)
                log.info("Waiting for future: " + fut);
        }

        commitLatch.countDown();

        wLockFut.get();

        checkFutures();

        Transaction tx0 = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);

        client.cache(CACHE_NAME).put(0, 200);

        tx0.commit();
    }

    /**
     *
     */
    public void testRollbackSync() throws Exception {
        final Ignite client = startClient();

        // Normal rollback after put.
        Transaction tx = client.transactions().txStart();

        client.cache(CACHE_NAME).put(0, 0);

        tx.rollback();

        assertNull(client.cache(CACHE_NAME).get(0));

        // Normal rollback before put.
        tx = client.transactions().txStart();

        tx.rollback();

        client.cache(CACHE_NAME).put(0, 1);

        assertEquals(1, client.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testRollbackAsync() throws Exception {
        final Ignite client = startClient();

        client.cache(CACHE_NAME).put(0, 0);

        assertEquals(GRID_CNT + 1, G.allGrids().size());

        for (Ignite ignite : G.allGrids())
            testRollbackAsync0(ignite, THREADS_CNT);

        testRollbackAsync0(client, THREADS_CNT);
    }

    /**
     * Test a transaction which is rolled back in empty state. All subsequent operations must fail.
     */
    public void testRollbackAsyncEmptyState() throws Exception {
        final Ignite client = startClient();

        final int threadsCnt = 1;

        final CountDownLatch keyLocked = new CountDownLatch(1);

        final CountDownLatch txStarted = new CountDownLatch(threadsCnt);

        final CountDownLatch txCommitted = new CountDownLatch(1);

        final CountDownLatch rollbackLatch = new CountDownLatch(1);

        final List<Transaction> txs = synchronizedList(new ArrayList<Transaction>());

        IgniteInternalFuture<?> tx1 = putAsync(client, keyLocked, txCommitted, 0, true);

        final IgniteInternalFuture<?> tx2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);

                txs.add(tx);

                txStarted.countDown();

                U.awaitQuiet(keyLocked);

                U.awaitQuiet(rollbackLatch);

                try {
                    client.cache(CACHE_NAME).get(0);

                    fail("No tx op is allowed after rollback");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }
            }
        }, threadsCnt, "tx-async");

        U.awaitQuiet(txStarted);

        txs.get(0).rollback();

        rollbackLatch.countDown();

        txCommitted.countDown();

        tx1.get();

        tx2.get();

        checkFutures();
    }

    /**
     *
     */
    private void testRollbackAsync0(final Ignite node, int threadsCnt) throws Exception {
        final CountDownLatch keyLocked = new CountDownLatch(1);

        final CountDownLatch txStarted = new CountDownLatch(threadsCnt);

        final CountDownLatch waitCommit = new CountDownLatch(1);

        final List<Transaction> txs = synchronizedList(new ArrayList<Transaction>());

        IgniteInternalFuture<?> lockFut = putAsync(node, keyLocked, waitCommit, 0, true);

        final IgniteInternalFuture<?> testFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);

                txs.add(tx);

                txStarted.countDown();

                U.awaitQuiet(keyLocked);

                try {
                    node.cache(CACHE_NAME).get(0);

                    fail("Read after async rollback must fail");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }

                try {
                    node.cache(CACHE_NAME).put(0, 1);

                    fail("Write attempt after async rollback must fail");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }

                try {
                    node.cache(CACHE_NAME).remove(0);

                    fail("Write attempt after async rollback must fail");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }
            }
        }, threadsCnt, "tx-async");

        U.awaitQuiet(txStarted);

        Thread.sleep(500);

        List<IgniteFuture<Void>> futs = new ArrayList<>();

        for (Transaction tx : txs)
            futs.add(tx.rollbackAsync());

        for (IgniteFuture<Void> future : futs)
            future.get();

        testFut.get();

        waitCommit.countDown();

        lockFut.get();

        assertEquals(0, node.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testSequentialRollback() throws Exception {
        Ignite client = startClient();

        for (int i = 0; i < GRID_CNT; i++)
            testSequentialRollback0(grid(0), grid(i), false);

        testSequentialRollback0(grid(0), client, false);

        for (int i = 0; i < GRID_CNT; i++)
            testSequentialRollback0(grid(0), grid(i), true);

        testSequentialRollback0(grid(0), client, true);
    }

    /**
     *
     */
    private void testSequentialRollback0(Ignite holdLockNode, final Ignite tryLockNode, final boolean useTimeout) throws Exception {
        final CountDownLatch keyLocked = new CountDownLatch(1);

        CountDownLatch waitCommit = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = putAsync(holdLockNode, keyLocked, waitCommit, 0, true);

        U.awaitQuiet(keyLocked);

        final CountDownLatch rollbackLatch = new CountDownLatch(1);

        final int txCnt = 500;

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
                        // Will wait for lock und must be unblocked by async rollback.
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

        log.info("All transactions are rolled back");

        waitCommit.countDown();

        lockFut.get();

        assertEquals(0, holdLockNode.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testMixedAsyncRollbackTypes() throws Exception {
        final Ignite client = startClient();

        final AtomicBoolean stop = new AtomicBoolean();

        final int keysCnt = 100;

        final int txSize = 10;

        for (int k = 0; k < keysCnt; k++)
            grid(0).cache(CACHE_NAME).put(k, (long)0);

        final Random r = new Random();

        r.setSeed(0);

        final TransactionConcurrency[] TC_VALS = TransactionConcurrency.values();
        final TransactionIsolation[] TI_VALS = TransactionIsolation.values();

        final LongAdder8 completed = new LongAdder8();
        final LongAdder8 failed = new LongAdder8();
        final LongAdder8 rolledBack = new LongAdder8();

        IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    int nodeId = r.nextInt(GRID_CNT + 1);

                    Ignite node = client; // nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                    TransactionConcurrency conc = PESSIMISTIC; // TC_VALS[r.nextInt(TC_VALS.length)];
                    TransactionIsolation isolation = REPEATABLE_READ; // TI_VALS[r.nextInt(TI_VALS.length)];

                    long timeout = 0; // r.nextInt(200) + 50;

                    try (Transaction tx = node.transactions().txStart(conc, isolation, timeout, txSize)) {
                        int[] keys = new int[txSize];

                        for (int i = 0; i < keys.length; i++)
                            keys[i] = r.nextInt(keysCnt);

                        Long v = null;
                        for (int key : keys) {
                            v = (Long)node.cache(CACHE_NAME).get(key);

                            assertTrue("Expecting not null value for active transaction: tx=" + tx + ", key=" + key +
                                ", node=" + node, v != null || tx.state() != ACTIVE);
                        }

                        final int delay = r.nextInt(100);

                        log.info("Tx sleep: " + delay);

                        sleep(delay);

                        node.cache(CACHE_NAME).put(keys[txSize - 1], v + 1);

                        tx.commit();

                        completed.add(1);
                    }
                    catch (Exception e) {
                        failed.add(1);
                    }
                }
            }
        }, THREADS_CNT, "tx-thread");

        final AtomicInteger nodeIdx = new AtomicInteger();

        IgniteInternalFuture<?> rollbackFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int nodeId = nodeIdx.getAndIncrement();

                while (!stop.get()) {
                    int sleep = r.nextInt(50);

                    log.info("Rollback sleep: " + sleep);

                    doSleep(sleep);

                    Ignite node = client; // nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                    Collection<Transaction> transactions = node.transactions().localActiveTransactions();

                    for (Transaction transaction : transactions) {
                        rolledBack.add(1);

                        transaction.rollbackAsync();
                    }
                }
            }
        }, 1, "rollback-thread");

        doSleep(DURATION);

        stop.set(true);

        txFut.get();

        rollbackFut.get();

        log.info("Completed txs: " + completed.sum() + ", failed txs: " + failed.sum() +
            ", rolled back: " + rolledBack.sum());

        assertTrue(completed.sum() > 0 && failed.sum() > 0 && rolledBack.sum() > 0);

        checkFutures();
    }

    /**
     * Tests rollback of active local transactions.
     */
    public void testRollbackActiveTransactions() throws Exception {
        final Ignite client = startClient();

        final Collection<Transaction> transactions = client.transactions().localActiveTransactions();

        for (Transaction transaction : transactions)
            transaction.rollback();

        assertTrue(client.transactions().localActiveTransactions().isEmpty());
    }

    /**
     * @param node Near node.
     * @param keyLocked Key locked latch.
     * @param waitCommit Wait commit latch.
     * @param timeout Timeout.
     * @param commit {@code True} If an entry must be committed.
     */
    private IgniteInternalFuture<?> putAsync(final Ignite node, final CountDownLatch keyLocked,
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
    private void toggleBlocking(Class<? extends Message> cls, Ignite nodeToBlock, boolean block) {
        for (Ignite ignite : G.allGrids()) {
            if (ignite == nodeToBlock)
                continue;

            final TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            if (block)
                spi.blockMessages(cls, nodeToBlock.name());
            else
                spi.stopBlock(true);
        }
    }

}

