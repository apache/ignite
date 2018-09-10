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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;

import static java.lang.Thread.interrupted;
import static java.lang.Thread.yield;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 * Tests an ability to async rollback near transactions.
 */
public class TxRollbackAsyncTest extends GridCommonAbstractTest {
    /** */
    public static final int DURATION = 60_000;

    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final long MB = 1024 * 1024;

    /** */
    public static final String LABEL = "wLockTx";

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
            ccfg.setOnheapCacheEnabled(false);

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

        crd.cluster().active(true);

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
        Ignite client = startGrid("client");

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
    public void testRollbackSimple() throws Exception {
        startClient();

        for (Ignite ignite : G.allGrids()) {
            testRollbackSimple0(ignite);

            ignite.cache(CACHE_NAME).clear();
        }
    }

    /**
     *
     */
    private void testRollbackSimple0(Ignite near) throws Exception {
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

        rollbackAsync(tx).get();

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

        rollbackAsync(tx).get();

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
     * @param holdLockNode Node holding the write lock.
     * @param tryLockNode Node trying to acquire lock.
     * @param useTimeout {@code True} if need to start tx with timeout.
     *
     * @throws Exception If failed.
     */
    private void testSynchronousRollback0(Ignite holdLockNode, final Ignite tryLockNode, final boolean useTimeout) throws Exception {
        final CountDownLatch keyLocked = new CountDownLatch(1);

        CountDownLatch waitCommit = new CountDownLatch(1);

        // Used for passing tx instance to rollback thread.
        IgniteInternalFuture<?> lockFut = lockInTx(holdLockNode, keyLocked, waitCommit, 0);

        U.awaitQuiet(keyLocked);

        final int txCnt = 1000;

        final IgniteKernal k = (IgniteKernal)tryLockNode;

        final GridCacheSharedContext<Object, Object> ctx = k.context().cache().context();

        final GridCacheContext<Object, Object> cctx = ctx.cacheContext(CU.cacheId(CACHE_NAME));

        GridFutureAdapter<Transaction> txReadyFut = new GridFutureAdapter<>();

        long seed = System.currentTimeMillis();

        Random r = new Random(seed);

        log.info("Running: node0=" + holdLockNode.cluster().localNode().consistentId() +
            ", node1=" + tryLockNode.cluster().localNode().consistentId() +
            ", useTimeout=" + useTimeout +
            ", seed=" + seed);

        IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(keyLocked);

                for (int i = 0; i < txCnt; i++) {
                    GridNearTxLocal tx0 = ctx.tm().threadLocalTx(cctx);

                    assertTrue(tx0 == null || tx0.state() == ROLLED_BACK);

                    try (Transaction tx = tryLockNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ,
                        useTimeout ? 50 : 0, 1)) {

                        txReadyFut.onDone(tx);

                        // Will block on lock request until rolled back asynchronously.
                        Object o = tryLockNode.cache(CACHE_NAME).get(0);

                        assertNull(o); // If rolled back by close, previous get will return null.
                    }
                    catch (Exception e) {
                        // If rolled back by rollback, previous get will throw an exception.
                    }
                }

                txReadyFut.onDone((Transaction)null);
            }
        }, 1, "tx-get-thread");

        IgniteInternalFuture<?> rollbackFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Set<IgniteUuid> rolledBackVers = new HashSet<>();

                int proc = 1;

                while(true) {
                    try {
                        Transaction tx = txReadyFut.get();

                        txReadyFut.reset();

                        if (tx == null)
                            break;

                        doSleep(r.nextInt(15)); // Wait a bit to reduce chance of rolling back empty transactions.

                        if (rolledBackVers.contains(tx.xid()))
                            fail("Rollback version is expected");

                        try {
                            if (proc % 2 == 0)
                                tx.rollback();
                            else
                                tx.close();
                        }
                        catch (IgniteException e) {
                            log.warning("Got exception while rolling back a transaction", e);
                        }

                        rolledBackVers.add(tx.xid());

                        if (proc % 100 == 0)
                            log.info("Rolled back: " + proc);

                        proc++;
                    }
                    catch (IgniteCheckedException e) {
                        fail(e.getMessage());
                    }
                }
            }
        }, 1, "tx-rollback-thread");

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
    public void testEnlistManyRead() throws Exception {
        testEnlistMany(false, REPEATABLE_READ, PESSIMISTIC);
    }

    /**
     *
     */
    public void testEnlistManyWrite() throws Exception {
        testEnlistMany(true, REPEATABLE_READ, PESSIMISTIC);
    }

    /**
     *
     */
    public void testEnlistManyReadOptimistic() throws Exception {
        testEnlistMany(false, SERIALIZABLE, OPTIMISTIC);
    }

    /**
     *
     */
    public void testEnlistManyWriteOptimistic() throws Exception {
        testEnlistMany(true, SERIALIZABLE, OPTIMISTIC);
    }


    /**
     *
     */
    private void testEnlistMany(boolean write, TransactionIsolation isolation, TransactionConcurrency conc) throws Exception {
        final Ignite client = startClient();

        Map<Integer, Integer> entries = new HashMap<>();

        for (int i = 0; i < 1000000; i++)
            entries.put(i, i);

        IgniteInternalFuture<?> fut = null;

        try(Transaction tx = client.transactions().txStart(conc, isolation, 0, 0)) {
            fut = rollbackAsync(tx, 200);

            if (write)
                client.cache(CACHE_NAME).putAll(entries);
            else
                client.cache(CACHE_NAME).getAll(entries.keySet());

            tx.commit();

            fail("Commit must fail");
        }
        catch (Throwable t) {
            assertTrue(X.hasCause(t, TransactionRollbackException.class));
        }

        fut.get();

        assertEquals(0, client.cache(CACHE_NAME).size());

        checkFutures();
    }

    /**
     * Rollback tx while near lock request is delayed.
     */
    public void testRollbackDelayNearLockRequest() throws Exception {
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
     * Tests rollback with concurrent commit.
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
    public void testMixedAsyncRollbackTypes() throws Exception {
        final Ignite client = startClient();

        final AtomicBoolean stop = new AtomicBoolean();

        final int threadCnt = Runtime.getRuntime().availableProcessors() * 2;

        final int txSize = 200;

        for (int k = 0; k < txSize; k++)
            grid(0).cache(CACHE_NAME).put(k, (long)0);

        final long seed = System.currentTimeMillis();

        final Random r = new Random(seed);

        log.info("Using seed: " + seed);

        final TransactionConcurrency[] TC_VALS = TransactionConcurrency.values();
        final TransactionIsolation[] TI_VALS = TransactionIsolation.values();

        final LongAdder total = new LongAdder();
        final LongAdder completed = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder rolledBack = new LongAdder();

        ConcurrentMap<Ignite, BlockingQueue<Transaction>> perNodeTxs = new ConcurrentHashMap<>();

        for (Ignite ignite : G.allGrids())
            perNodeTxs.put(ignite, new ArrayBlockingQueue<>(1000));

        IgniteInternalFuture<?> txFut = multithreadedAsync(() -> {
            while (!stop.get()) {
                int nodeId = r.nextInt(GRID_CNT + 1);

                // Choose random node to start tx on.
                Ignite node = nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                TransactionConcurrency conc = TC_VALS[r.nextInt(TC_VALS.length)];
                TransactionIsolation isolation = TI_VALS[r.nextInt(TI_VALS.length)];

                // Timeout is necessary otherwise deadlock is possible due to randomness of lock acquisition.
                long timeout = r.nextInt(50) + 50;

                try (Transaction tx = node.transactions().txStart(conc, isolation, timeout, txSize)) {
                    BlockingQueue<Transaction> nodeQ = perNodeTxs.get(node);

                    nodeQ.put(tx);

                    int setSize = r.nextInt(txSize / 2) + 1;

                    for (int i = 0; i < setSize; i++) {
                        switch (r.nextInt(4)) {
                            case 0:
                                node.cache(CACHE_NAME).remove(r.nextInt(txSize));

                                break;

                            case 1:
                                node.cache(CACHE_NAME).get(r.nextInt(txSize));

                                break;

                            case 2:
                                final Integer v = (Integer)node.cache(CACHE_NAME).get(r.nextInt(txSize));

                                node.cache(CACHE_NAME).put(r.nextInt(txSize), (v == null ? 0 : v) + 1);

                                break;

                            case 3:
                                node.cache(CACHE_NAME).put(r.nextInt(txSize), 0);

                                break;

                            default:
                                fail("Unexpected opcode");
                        }
                    }

                    tx.commit();

                    completed.add(1);
                }
                catch (Throwable e) {
                    failed.add(1);
                }

                total.add(1);
            }
        }, threadCnt, "tx-thread");

        final AtomicIntegerArray idx = new AtomicIntegerArray(GRID_CNT + 1);

        CIX1<Transaction> rollbackClo = new CIX1<Transaction>() {
            @Override public void applyx(Transaction tx) throws IgniteCheckedException {
                try {
                    IgniteFuture<Void> rollbackFut = tx.rollbackAsync();

                    rollbackFut.listen(new IgniteInClosure<IgniteFuture<Void>>() {
                        @Override public void apply(IgniteFuture<Void> fut) {
                            tx.close();
                        }
                    });
                }
                catch (Throwable t) {
                    log.error("Exception on async rollback", t);

                    throw new IgniteCheckedException("Rollback failed", t);
                }
            }
        };

        IgniteInternalFuture<?> rollbackFut = multithreadedAsync(() -> {
            while (!interrupted()) {
                // Choose node randomly.
                final int nodeId = r.nextInt(GRID_CNT + 1);

                // Reserve node for rollback.
                if (!idx.compareAndSet(nodeId, 0, 1)) {
                    yield();

                    continue;
                }

                Ignite node = nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                BlockingQueue<Transaction> nodeQ = perNodeTxs.get(node);

                Transaction tx;

                // Rollback all transaction
                while((tx = nodeQ.poll()) != null) {
                    rolledBack.add(1);

                    doSleep(r.nextInt(50)); // Add random sleep to increase completed txs count.

                    if (rolledBack.sum() % 1000 == 0)
                        info("Rolled back so far: " + rolledBack.sum());

                    rollbackClo.apply(tx);
                }

                idx.set(nodeId, 0);
            }
        }, 3, "rollback-thread"); // Rollback by multiple threads.

        doSleep(DURATION);

        stop.set(true);

        txFut.get(); // Stop tx generation.

        rollbackFut.cancel();

        try {
            rollbackFut.get();
        }
        catch (IgniteFutureCancelledCheckedException e) {
            // Expected.
        }

        // Rollback remaining transactions.
        for (BlockingQueue<Transaction> queue : perNodeTxs.values()) {
            Transaction tx;

            while((tx = queue.poll()) != null) {
                rolledBack.add(1);

                rollbackClo.apply(tx);
            }
        }

        log.info("total=" + total.sum() + ", completed=" + completed.sum() + ", failed=" + failed.sum() +
            ", rolledBack=" + rolledBack.sum());

        assertEquals("total != completed + failed", total.sum(), completed.sum() + failed.sum());

        checkFutures();
    }

    /**
     * Tests proxy object returned by {@link IgniteTransactions#localActiveTransactions()}
     */
    public void testRollbackProxy() throws Exception {
        final CountDownLatch keyLocked = new CountDownLatch(1);

        CountDownLatch waitCommit = new CountDownLatch(1);

        Ignite ig = ignite(0);

        IgniteInternalFuture<?> lockFut = lockInTx(ig, keyLocked, waitCommit, 0);

        U.awaitQuiet(keyLocked);

        Collection<Transaction> txs = ig.transactions().localActiveTransactions();

        for (Transaction tx : txs) {
            try {
                tx.timeout(1);

                fail("timeout");
            }
            catch (Exception e) {
                // No-op.
            }

            try {
                tx.setRollbackOnly();

                fail("setRollbackOnly");
            }
            catch (Exception e) {
                // No-op.
            }

            try {
                tx.commit();

                fail("commit");
            }
            catch (Exception e) {
                // No-op.
            }

            try {
                tx.commitAsync();

                fail("commitAsync");
            }
            catch (Exception e) {
                // No-op.
            }

            try {
                tx.suspend();

                fail("suspend");
            }
            catch (Exception e) {
                // No-op.
            }

            try {
                tx.resume();

                fail("resume");
            }
            catch (Exception e) {
                // No-op.
            }

            tx.rollback();

            tx.rollbackAsync().get();
        }

        waitCommit.countDown();

        try {
            lockFut.get();

            fail();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }
    }

    /**
     *
     */
    public void testRollbackOnTopologyLockPessimistic() throws Exception {
        final Ignite client = startClient();

        Ignite crd = grid(0);

        List<Integer> keys = primaryKeys(grid(1).cache(CACHE_NAME), 1);

        assertTrue(crd.cluster().localNode().order() == 1);

        CountDownLatch txLatch = new CountDownLatch(1);
        CountDownLatch tx2Latch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        // Start tx holding topology.
        IgniteInternalFuture txFut = runAsync(new Runnable() {
            @Override public void run() {
                List<Integer> keys = primaryKeys(grid(0).cache(CACHE_NAME), 1);

                try (Transaction tx = client.transactions().txStart()) {
                    client.cache(CACHE_NAME).put(keys.get(0), 0);

                    txLatch.countDown();

                    assertTrue(U.await(commitLatch, 10, TimeUnit.SECONDS));

                    tx.commit();

                    fail();
                }
                catch (Exception e) {
                    // Expected.
                }
            }
        });

        U.awaitQuiet(txLatch);

        crd.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        try(Transaction tx = crd.transactions().withLabel("testLbl").txStart()) {
                            // Wait for node start.
                            waitForCondition(new GridAbsPredicate() {
                                @Override public boolean apply() {
                                    return crd.cluster().topologyVersion() != GRID_CNT +
                                        /** client node */ 1  + /** stop server node */ 1 + /** start server node */ 1;
                                }
                            }, 10_000);

                            tx2Latch.countDown();

                            crd.cache(CACHE_NAME).put(keys.get(0), 0);

                            assertTrue(U.await(commitLatch, 10, TimeUnit.SECONDS));

                            tx.commit();

                            fail();
                        }
                        catch (Exception e) {
                            // Expected.
                        }
                    }
                });

                return false;
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        IgniteInternalFuture restartFut = runAsync(new Runnable() {
            @Override public void run() {
                stopGrid(2);

                try {
                    startGrid(2);
                }
                catch (Exception e) {
                    fail();
                }
            }
        });

        U.awaitQuiet(tx2Latch);

        // Rollback tx using kill task.
        VisorTxTaskArg arg =
            new VisorTxTaskArg(VisorTxOperation.KILL, null, null, null, null, null, null, null, null, null);

        Map<ClusterNode, VisorTxTaskResult> res = client.compute(client.cluster().forPredicate(F.alwaysTrue())).
            execute(new VisorTxTask(), new VisorTaskArgument<>(client.cluster().localNode().id(), arg, false));

        int expCnt = 0;

        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            if (entry.getValue().getInfos().isEmpty())
                continue;

            for (VisorTxInfo info : entry.getValue().getInfos()) {
                log.info(info.toUserString());

                expCnt++;
            }
        }

        assertEquals("Expecting 2 transactions", 2, expCnt);

        commitLatch.countDown();

        txFut.get();
        restartFut.get();

        checkFutures();
    }

    /**
     * Locks entry in tx and delays commit until signalled.
     *
     * @param node Near node.
     * @param keyLocked Latch for notifying until key is locked.
     * @param waitCommit Latch for waiting until commit is allowed.
     * @param timeout Timeout.
     *
     * @return tx completion future.
     */
    private IgniteInternalFuture<?> lockInTx(final Ignite node, final CountDownLatch keyLocked,
        final CountDownLatch waitCommit, final int timeout) throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = node.transactions().withLabel(LABEL).txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 1);

                node.cache(CACHE_NAME).put(0, 0);

                keyLocked.countDown();

                try {
                    U.await(waitCommit);
                }
                catch (IgniteInterruptedCheckedException e) {
                    fail("Lock thread was interrupted while waiting");
                }

                tx.commit();
            }
        }, 1, "tx-lock-thread");
    }

    /**
     * @param tx Tx to rollback.
     */
    private IgniteInternalFuture<?> rollbackAsync(final Transaction tx) throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                tx.rollback();
            }
        }, 1, "tx-rollback-thread");
    }

    /**
     * @param tx Tx to rollback.
     * @param delay Delay in millis.
     */
    private IgniteInternalFuture<?> rollbackAsync(final Transaction tx, long delay) throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                doSleep(delay);

                tx.rollback();
            }
        }, 1, "tx-rollback-thread");
    }

}

