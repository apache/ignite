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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
@RunWith(Parameterized.class)
public class GridExchangeFreeCellularSwitchIsolationTest extends GridExchangeFreeCellularSwitchAbstractTest {
    /** Start from. */
    @Parameterized.Parameter(0)
    public TransactionCoordinatorNode startFrom;

    /**
     *
     */
    @Parameterized.Parameters(name = "Started from = {0}")
    public static Collection<Object[]> runConfig() {
        ArrayList<Object[]> params = new ArrayList<>();

        for (TransactionCoordinatorNode from : TransactionCoordinatorNode.values())
            params.add(new Object[] {from});

        return params;
    }

    /**
     * Tests checks that switch finished only when all transactions required recovery are recovered.
     * Based on corner case found at TeamCity runs:
     *
     * We have 2 cells, the first contains partitions for k1, second for k2.
     * Tx with put(k1,v1) and put(k2,v2) started and prepared.
     * Then node from the first cell, which is the primary for k1, failed.
     * The second cell (with key2) should NOT finish the cellular switch before tx recovered,
     * otherwice stale data read is possible.
     */
    @Test
    public void testMutliKeyTxRecoveryHappenBeforeTheSwitchOnCellularSwitch() throws Exception {
        int nodes = 6;

        startGridsMultiThreaded(nodes);

        blockRecoveryMessages();

        CellularCluster cluster = resolveCellularCluster(nodes, startFrom);

        Ignite orig = cluster.orig;
        Ignite failed = cluster.failed;
        List<Ignite> brokenCellNodes = cluster.brokenCellNodes;
        List<Ignite> aliveCellNodes = cluster.aliveCellNodes;

        CountDownLatch prepLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        AtomicInteger key = new AtomicInteger();

        // Puts 2 entries, each on it's own cell.
        IgniteInternalFuture<?> putFut = multithreadedAsync(() -> {
            try {
                Transaction tx = orig.transactions().txStart();

                IgniteCache<Integer, Integer> cache = orig.getOrCreateCache(PART_CACHE_NAME);

                cache.put(primaryKey(failed.getOrCreateCache(PART_CACHE_NAME)), 42);

                key.set(primaryKey(aliveCellNodes.get(0).getOrCreateCache(PART_CACHE_NAME)));

                cache.put(key.get(), key.get());

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                prepLatch.countDown();

                commitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, 1);

        prepLatch.await();

        // Should be null white tx is uncommitted/unrecovered.
        assertNull(aliveCellNodes.get(0).getOrCreateCache(PART_CACHE_NAME).get(key.get()));

        failed.close(); // Stopping node.

        awaitForSwitchOnNodeLeft(failed);

        checkTransactionsCount( // Making sure txs still unrecovered.
            null, 0,
            brokenCellNodes, 1,
            aliveCellNodes, 1,
            null /*any*/);

        CountDownLatch getLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> getFut = multithreadedAsync(() -> {
            try {
                IgniteCache<Integer, Integer> cache = aliveCellNodes.get(0).getOrCreateCache(PART_CACHE_NAME);

                // Should be available for reading only after recovery happen (should be not null).
                assertEquals((Integer)key.get(), cache.get(key.get()));

                getLatch.countDown();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, 1);

        // Get should not happen while tx is not recovered.
        assertFalse(getLatch.await(10, TimeUnit.SECONDS));

        // Allowing recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, blockedMsg -> true);
        }

        // Allowing commit.
        commitLatch.countDown();

        putFut.get();

        // Awaiting for get on alive cell.
        getLatch.await();

        // Making sure get finished with recovered value.
        getFut.get();

        // Final check that any transactions are absent.
        checkTransactionsCount(
            null, 0,
            brokenCellNodes, 0,
            aliveCellNodes, 0,
            null /*any*/);
    }

    /**
     * Test checks than non-affected nodes (alive cells) finishes the switch asap,
     * that they wait only for the recovery related to these nodes (eg. replicated caches recovery that affects every node).
     */
    @Test
    public void testOnlyAffectedNodesWaitForRecovery() throws Exception {
        int nodes = 6;

        String recoveryStatusMsg = "TxRecovery Status and Timings [txs=";

        LogListener lsnrAny = matches(recoveryStatusMsg).build(); // Any.
        LogListener lsnrBrokenCell = matches(recoveryStatusMsg).times((nodes / 2) - 1 /*failed*/).build();
        LogListener lsnrAliveCell = matches(recoveryStatusMsg).times((nodes / 2)).build();

        listeningLog.registerListener(lsnrAny);

        startGridsMultiThreaded(nodes);

        blockRecoveryMessages();

        CellularCluster cluster = resolveCellularCluster(nodes, startFrom);

        Ignite orig = cluster.orig;
        Ignite failed = cluster.failed;
        List<Ignite> brokenCellNodes = cluster.brokenCellNodes;
        List<Ignite> aliveCellNodes = cluster.aliveCellNodes;

        List<Integer> partKeys = new ArrayList<>();
        List<Integer> replKeys = new ArrayList<>();

        for (Ignite node : G.allGrids()) {
            if (!node.configuration().isClientMode()) {
                partKeys.add(primaryKey(node.getOrCreateCache(PART_CACHE_NAME)));
                replKeys.add(primaryKey(node.getOrCreateCache(REPL_CACHE_NAME)));
            }
        }

        CountDownLatch partPreparedLatch = new CountDownLatch(nodes);
        CountDownLatch replPreparedLatch = new CountDownLatch(nodes);

        CountDownLatch partCommitLatch = new CountDownLatch(1);
        CountDownLatch replCommitLatch = new CountDownLatch(1);

        AtomicInteger partKeyIdx = new AtomicInteger();
        AtomicInteger replKeyIdx = new AtomicInteger();

        Set<GridCacheVersion> partTxVers = new GridConcurrentHashSet<>();
        Set<GridCacheVersion> replTxVers = new GridConcurrentHashSet<>();

        IgniteInternalFuture<?> partFut = multithreadedAsync(() -> {
            try {
                int idx = partKeyIdx.getAndIncrement();

                Transaction tx = orig.transactions().txStart();

                partTxVers.add(((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion());

                int key = partKeys.get(idx);

                orig.getOrCreateCache(PART_CACHE_NAME).put(key, key);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                partPreparedLatch.countDown();

                partCommitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, nodes);

        IgniteInternalFuture<?> replFut = multithreadedAsync(() -> {
            try {
                int idx = replKeyIdx.getAndIncrement();

                Transaction tx = orig.transactions().txStart();

                replTxVers.add(((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion());

                int key = replKeys.get(idx);

                orig.getOrCreateCache(REPL_CACHE_NAME).put(key, key);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                replPreparedLatch.countDown();

                replCommitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, nodes);

        partPreparedLatch.await();
        replPreparedLatch.await();

        checkTransactionsCount(
            orig, nodes,
            brokenCellNodes, nodes / 2,
            aliveCellNodes, nodes / 2,
            partTxVers);

        checkTransactionsCount(
            orig, nodes,
            brokenCellNodes, nodes,
            aliveCellNodes, nodes,
            replTxVers);

        assertFalse(lsnrAny.check());

        listeningLog.registerListener(lsnrAliveCell);

        failed.close(); // Stopping node.

        awaitForSwitchOnNodeLeft(failed);

        // In case of originating node failed all alive primaries will recover (commit) txs on tx cordinator falure.
        // Txs with failed primary will start recovery, but can't finish it since recovery messages are blocked.

        // Broken cell's nodes will have 1 unrecovered tx for partitioned cache.
        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, nodes,
            brokenCellNodes, orig == failed ? 1 : nodes / 2,
            aliveCellNodes, orig == failed ? 0 : nodes / 2,
            partTxVers);

        // All cell's nodes will have 1 unrecovered tx for replicated cache.
        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, nodes,
            brokenCellNodes, orig == failed ? 1 : nodes,
            aliveCellNodes, orig == failed ? 1 : nodes,
            replTxVers);

        // Counts tx's creations and preparations.
        BiConsumer<T2<Ignite, String>, T3<CountDownLatch, CountDownLatch, CountDownLatch>> txRun =
            (T2<Ignite, String> pair, T3</*create*/CountDownLatch, /*put*/CountDownLatch, /*commit*/CountDownLatch> latches) -> {
                try {
                    Ignite ignite = pair.get1();
                    String cacheName = pair.get2();

                    IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

                    try (Transaction tx = ignite.transactions().txStart()) {
                        latches.get1().countDown(); // Create.

                        // Avoiding intersection with prepared keys.
                        cache.put(primaryKeys(cache, 1, 1_000).get(0), 42);

                        latches.get2().countDown(); // Put.

                        tx.commit();

                        latches.get3().countDown(); // Commit.
                    }
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            };

        CountDownLatch partBrokenCellCreateLatch = new CountDownLatch(brokenCellNodes.size());
        CountDownLatch partBrokenCellPutLatch = new CountDownLatch(brokenCellNodes.size());
        CountDownLatch partBrokenCellCommitLatch = new CountDownLatch(brokenCellNodes.size());
        CountDownLatch partAliveCellCreateLatch = new CountDownLatch(aliveCellNodes.size());
        CountDownLatch partAliveCellPutLatch = new CountDownLatch(aliveCellNodes.size());
        CountDownLatch partAliveCellCommitLatch = new CountDownLatch(aliveCellNodes.size());

        CountDownLatch replBrokenCellCreateLatch = new CountDownLatch(brokenCellNodes.size());
        CountDownLatch replBrokenCellPutLatch = new CountDownLatch(brokenCellNodes.size());
        CountDownLatch replBrokenCellCommitLatch = new CountDownLatch(brokenCellNodes.size());
        CountDownLatch replAliveCellCreateLatch = new CountDownLatch(aliveCellNodes.size());
        CountDownLatch replAliveCellPutLatch = new CountDownLatch(aliveCellNodes.size());
        CountDownLatch replAliveCellCommitLatch = new CountDownLatch(aliveCellNodes.size());

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (Ignite brokenCellNode : brokenCellNodes) {
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(brokenCellNode, REPL_CACHE_NAME),
                    new T3<>(replBrokenCellCreateLatch, replBrokenCellPutLatch, replBrokenCellCommitLatch)), 1));
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(brokenCellNode, PART_CACHE_NAME),
                    new T3<>(partBrokenCellCreateLatch, partBrokenCellPutLatch, partBrokenCellCommitLatch)), 1));
        }

        for (Ignite aliveCellNode : aliveCellNodes) {
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(aliveCellNode, REPL_CACHE_NAME),
                    new T3<>(replAliveCellCreateLatch, replAliveCellPutLatch, replAliveCellCommitLatch)), 1));
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(aliveCellNode, PART_CACHE_NAME),
                    new T3<>(partAliveCellCreateLatch, partAliveCellPutLatch, partAliveCellCommitLatch)), 1));
        }

        // Switch in progress cluster-wide.
        // Alive nodes switch blocked until replicated caches recovery happen.
        checkUpcomingTransactionsState(
            partBrokenCellCreateLatch, 0, // Started.
            partBrokenCellPutLatch, brokenCellNodes.size(),
            partBrokenCellCommitLatch, brokenCellNodes.size(),
            partAliveCellCreateLatch, 0, // Started. Blocked by replicated cache recovery.
            partAliveCellPutLatch, aliveCellNodes.size(),
            partAliveCellCommitLatch, aliveCellNodes.size());

        checkUpcomingTransactionsState(
            replBrokenCellCreateLatch, 0, // Started.
            replBrokenCellPutLatch, brokenCellNodes.size(),
            replBrokenCellCommitLatch, brokenCellNodes.size(),
            replAliveCellCreateLatch, 0, // Started. Blocked by replicated cache recovery.
            replAliveCellPutLatch, aliveCellNodes.size(),
            replAliveCellCommitLatch, aliveCellNodes.size());

        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, nodes,
            brokenCellNodes, orig == failed ? 1 : nodes / 2,
            aliveCellNodes, orig == failed ? 0 : nodes / 2,
            partTxVers);

        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, nodes,
            brokenCellNodes, orig == failed ? 1 : nodes,
            aliveCellNodes, orig == failed ? 1 : nodes,
            replTxVers);

        // Replicated recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, blockedMsg -> {
                Message msg = blockedMsg.ioMessage().message();

                return replTxVers.contains(((GridCacheTxRecoveryRequest)msg).nearXidVersion());
            });
        }

        replCommitLatch.countDown();
        replFut.get();

        // Switch partially finished.
        // Broken cell still in switch.
        // Alive cell finished the switch.
        checkUpcomingTransactionsState(
            partBrokenCellCreateLatch, 0, // Started.
            partBrokenCellPutLatch, brokenCellNodes.size(),
            partBrokenCellCommitLatch, brokenCellNodes.size(),
            partAliveCellCreateLatch, 0, // Started.
            partAliveCellPutLatch, 0, // Alive cell nodes's able to start transactions on primaries,
            partAliveCellCommitLatch, 0); // Able to commit, since all primaries and backups are inside the alive cell.

        checkUpcomingTransactionsState(
            replBrokenCellCreateLatch, 0, // Started.
            replBrokenCellPutLatch, brokenCellNodes.size(),
            replBrokenCellCommitLatch, brokenCellNodes.size(),
            replAliveCellCreateLatch, 0, // Started.
            replAliveCellPutLatch, 0, // Alive cell's nodes able to start transactions on primaries,
            replAliveCellCommitLatch, aliveCellNodes.size()); // But not able to commit, since broken cell's nodes still in switch.

        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, nodes,
            brokenCellNodes, orig == failed ? 1 : nodes / 2,
            aliveCellNodes, orig == failed ? 0 : nodes / 2 /*to be committed*/, // New txs able to start while previous are in progress.
            partTxVers);

        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, 0,
            brokenCellNodes, 0,
            aliveCellNodes, 0,
            replTxVers);

        // Recovery finished on alive cell.
        assertTrue(waitForCondition(lsnrAliveCell::check, 5000));

        listeningLog.registerListener(lsnrBrokenCell);

        // Partitioned recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, blockedMsg -> {
                Message msg = blockedMsg.ioMessage().message();

                return partTxVers.contains(((GridCacheTxRecoveryRequest)msg).nearXidVersion());
            });
        }

        partCommitLatch.countDown();
        partFut.get();

        // Switches finished cluster-wide, all transactions can be committed.
        checkUpcomingTransactionsState(
            replBrokenCellCreateLatch, 0,
            replBrokenCellPutLatch, 0,
            replBrokenCellCommitLatch, 0,
            replAliveCellCreateLatch, 0,
            replAliveCellPutLatch, 0,
            replAliveCellCommitLatch, 0);

        checkUpcomingTransactionsState(
            partBrokenCellCreateLatch, 0,
            partBrokenCellPutLatch, 0,
            partBrokenCellCommitLatch, 0,
            partAliveCellCreateLatch, 0,
            partAliveCellPutLatch, 0,
            partAliveCellCommitLatch, 0);

        // Check that pre-failure transactions are absent.
        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, 0,
            brokenCellNodes, 0,
            aliveCellNodes, 0,
            partTxVers);

        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, 0,
            brokenCellNodes, 0,
            aliveCellNodes, 0,
            replTxVers);

        // Recovery finished on broken cell.
        assertTrue(waitForCondition(lsnrBrokenCell::check, 5000));

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        for (Ignite node : G.allGrids()) {
            for (int key : partKeys)
                assertEquals(key, node.getOrCreateCache(PART_CACHE_NAME).get(key));

            for (int key : replKeys)
                assertEquals(key, node.getOrCreateCache(REPL_CACHE_NAME).get(key));
        }

        // Final check that any transactions are absent.
        checkTransactionsCount(
            null, 0,
            brokenCellNodes, 0,
            aliveCellNodes, 0,
            null /*any*/);
    }

    /**
     *
     */
    private void checkUpcomingTransactionsState(
        CountDownLatch brokenCellCreateLatch,
        int brokenCellCreateCnt,
        CountDownLatch brokenCellPutLatch,
        int brokenCellPutCnt,
        CountDownLatch brokenCellCommitLatch,
        int brokenCellCommitCnt,
        CountDownLatch aliveCellCreateLatch,
        int aliveCellCreateCnt,
        CountDownLatch aliveCellPutLatch,
        int aliveCellPutCnt,
        CountDownLatch aliveCellCommitLatch,
        int aliveCellCommitCnt) throws InterruptedException {
        checkTransactionsState(brokenCellCreateLatch, brokenCellCreateCnt);
        checkTransactionsState(brokenCellPutLatch, brokenCellPutCnt);
        checkTransactionsState(brokenCellCommitLatch, brokenCellCommitCnt);
        checkTransactionsState(aliveCellCreateLatch, aliveCellCreateCnt);
        checkTransactionsState(aliveCellPutLatch, aliveCellPutCnt);
        checkTransactionsState(aliveCellCommitLatch, aliveCellCommitCnt);
    }

    /**
     *
     */
    private void checkTransactionsState(CountDownLatch latch, int cnt) throws InterruptedException {
        if (cnt == 0)
            latch.await(10, TimeUnit.SECONDS); // Switch finished (finishing).

        assertEquals(cnt, latch.getCount()); // Switch in progress.
    }
}
