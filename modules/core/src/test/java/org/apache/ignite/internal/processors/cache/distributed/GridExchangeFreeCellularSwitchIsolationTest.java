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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
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
     *
     */
    @Test
    public void testOnlyAffectedNodesWaitForRecovery() throws Exception {
        int nodes = 6;

        String recoveryStatusMsg = "TxRecovery Status and Timings [txs=";

        LogListener lsnrAny = matches(recoveryStatusMsg).build(); // Any.
        LogListener lsnrBackup = matches(recoveryStatusMsg).times((nodes / 2) - 1).build(); // Cell 1 (backups).
        LogListener lsnrNear = matches(recoveryStatusMsg).times((nodes / 2)).build(); // Cell 2 (near).

        listeningLog.registerListener(lsnrAny);

        startGridsMultiThreaded(nodes);

        blockRecoveryMessages();

        Ignite failed = G.allGrids().get(new Random().nextInt(nodes));

        Integer partKey = primaryKey(failed.getOrCreateCache(PART_CACHE_NAME));
        Integer replKey = primaryKey(failed.getOrCreateCache(REPL_CACHE_NAME));

        List<Ignite> backupNodes = backupNodes(partKey, PART_CACHE_NAME);
        List<Ignite> nearNodes = new ArrayList<>(G.allGrids());

        nearNodes.remove(failed);
        nearNodes.removeAll(backupNodes);

        assertTrue(Collections.disjoint(backupNodes, nearNodes));
        assertEquals(nodes / 2 - 1, backupNodes.size()); // Cell 1.
        assertEquals(nodes / 2, nearNodes.size()); // Cell 2.

        Ignite orig;

        switch (startFrom) {
            case PRIMARY:
                orig = failed;

                break;

            case BACKUP:
                orig = backupNodes.get(0);

                break;

            case NEAR:
                orig = nearNodes.get(0);

                break;

            case CLIENT:
                orig = startClientGrid();

                break;

            default:
                throw new UnsupportedOperationException();
        }

        Set<GridCacheVersion> vers = new GridConcurrentHashSet<>();

        CountDownLatch partPreparedLatch = new CountDownLatch(1);
        CountDownLatch partCommitLatch = new CountDownLatch(1);
        CountDownLatch replPreparedLatch = new CountDownLatch(1);
        CountDownLatch replCommitLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> partFut = multithreadedAsync(() -> {
            try {
                checkTransactionsCount(
                    orig, 0,
                    failed, 0,
                    backupNodes, 0,
                    nearNodes, 0,
                    vers);

                Transaction tx = orig.transactions().txStart();

                vers.add(((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion());

                checkTransactionsCount(
                    orig, 1,
                    failed, 0,
                    backupNodes, 0,
                    nearNodes, 0,
                    vers);

                orig.getOrCreateCache(PART_CACHE_NAME).put(partKey, 42);

                checkTransactionsCount(
                    orig, 1,
                    failed, 1,
                    backupNodes, 0,
                    nearNodes, 0,
                    vers);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                checkTransactionsCount(
                    orig, 1,
                    failed, 1,
                    backupNodes, 1,
                    nearNodes, 0,
                    vers);

                partPreparedLatch.countDown();

                partCommitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, 1);

        AtomicReference<GridCacheVersion> replTxVer = new AtomicReference<>();

        IgniteInternalFuture<?> replFut = multithreadedAsync(() -> {
            try {
                partPreparedLatch.await(); // Waiting for partitioned cache tx preparation.

                checkTransactionsCount(
                    orig, 1,
                    failed, 1,
                    backupNodes, 1,
                    nearNodes, 0,
                    vers);

                Transaction tx = orig.transactions().txStart();

                replTxVer.set(((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion());

                vers.add(replTxVer.get());

                checkTransactionsCount(
                    orig, 2,
                    failed, 1,
                    backupNodes, 1,
                    nearNodes, 0,
                    vers);

                orig.getOrCreateCache(REPL_CACHE_NAME).put(replKey, 43);

                checkTransactionsCount(
                    orig, 2,
                    failed, 2,
                    backupNodes, 1,
                    nearNodes, 0,
                    vers);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                checkTransactionsCount(
                    orig, 2,
                    failed, 2,
                    backupNodes, 2,
                    nearNodes, 1,
                    vers);

                replPreparedLatch.countDown();

                replCommitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        }, 1);

        partPreparedLatch.await();
        replPreparedLatch.await();

        checkTransactionsCount(
            orig, 2,
            failed, 2,
            backupNodes, 2,
            nearNodes, 1,
            vers);

        assertFalse(lsnrAny.check());

        listeningLog.registerListener(lsnrNear);

        failed.close(); // Stopping node.

        awaitForSwitchOnNodeLeft(failed);

        checkTransactionsCount(
            orig != failed ? orig : null /*stopped*/, 2 /* replicated + partitioned */,
            null /*stopped*/, 0,
            backupNodes, 2 /* replicated + partitioned */,
            nearNodes, 1 /* replicated */,
            vers);

        BiConsumer<T2<Ignite, String>, T3<CountDownLatch, CountDownLatch, CountDownLatch>> txRun = // Counts tx's creations and preparations.
            (T2<Ignite, String> pair, T3</*create*/CountDownLatch, /*put*/CountDownLatch, /*commit*/CountDownLatch> latches) -> {
                try {
                    Ignite ignite = pair.get1();
                    String cacheName = pair.get2();

                    IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

                    try (Transaction tx = ignite.transactions().txStart()) {
                        latches.get1().countDown(); // Create.

                        cache.put(primaryKeys(cache, 100).get(99), 2);

                        latches.get2().countDown(); // Put.

                        tx.commit();

                        latches.get3().countDown(); // Commit.
                    }
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            };

        CountDownLatch replBackupCreateLatch = new CountDownLatch(backupNodes.size());
        CountDownLatch replBackupPutLatch = new CountDownLatch(backupNodes.size());
        CountDownLatch replBackupCommitLatch = new CountDownLatch(backupNodes.size());
        CountDownLatch replNearCreateLatch = new CountDownLatch(nearNodes.size());
        CountDownLatch replNearPutLatch = new CountDownLatch(nearNodes.size());
        CountDownLatch replNearCommitLatch = new CountDownLatch(nearNodes.size());

        CountDownLatch partBackupCreateLatch = new CountDownLatch(backupNodes.size());
        CountDownLatch partBackupPutLatch = new CountDownLatch(backupNodes.size());
        CountDownLatch partBackupCommitLatch = new CountDownLatch(backupNodes.size());
        CountDownLatch partNearCreateLatch = new CountDownLatch(nearNodes.size());
        CountDownLatch partNearPutLatch = new CountDownLatch(nearNodes.size());
        CountDownLatch partNearCommitLatch = new CountDownLatch(nearNodes.size());

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (Ignite backup : backupNodes) {
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(backup, REPL_CACHE_NAME),
                    new T3<>(replBackupCreateLatch, replBackupPutLatch, replBackupCommitLatch)), 1));
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(backup, PART_CACHE_NAME),
                    new T3<>(partBackupCreateLatch, partBackupPutLatch, partBackupCommitLatch)), 1));
        }

        for (Ignite near : nearNodes) {
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(near, REPL_CACHE_NAME),
                    new T3<>(replNearCreateLatch, replNearPutLatch, replNearCommitLatch)), 1));
            futs.add(multithreadedAsync(() ->
                txRun.accept(new T2<>(near, PART_CACHE_NAME),
                    new T3<>(partNearCreateLatch, partNearPutLatch, partNearCommitLatch)), 1));
        }

        // Switch in progress cluster-wide.
        checkUpcomingTransactionsState(
            replBackupCreateLatch, 0, // Started.
            replBackupPutLatch, backupNodes.size(),
            replBackupCommitLatch, backupNodes.size(),
            replNearCreateLatch, 0, // Started.
            replNearPutLatch, nearNodes.size(),
            replNearCommitLatch, nearNodes.size());

        checkUpcomingTransactionsState(
            partBackupCreateLatch, 0, // Started.
            partBackupPutLatch, backupNodes.size(),
            partBackupCommitLatch, backupNodes.size(),
            partNearCreateLatch, 0, // Started.
            partNearPutLatch, nearNodes.size(),
            partNearCommitLatch, nearNodes.size());

        checkTransactionsCount(
            orig != failed ? orig : null, 2 /* replicated + partitioned */,
            null, 0,
            backupNodes, 2 /* replicated + partitioned */,
            nearNodes, 1 /* replicated */,
            vers);

        // Replicated recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, t -> {
                Message msg = t.get2().message();

                return ((GridCacheTxRecoveryRequest)msg).nearXidVersion().equals(replTxVer.get());
            });
        }

        replCommitLatch.countDown();
        replFut.get();

        // Switch partially finished.
        // Cell 1 (backups) still in switch.
        // Cell 2 (near nodes) finished the switch.
        checkUpcomingTransactionsState(
            replBackupCreateLatch, 0, // Started.
            replBackupPutLatch, backupNodes.size(),
            replBackupCommitLatch, backupNodes.size(),
            replNearCreateLatch, 0, // Started.
            replNearPutLatch, 0, // Near nodes able to start transactions on primaries (Cell 2),
            replNearCommitLatch, nearNodes.size()); // But not able to commit, since some backups (Cell 1) still in switch.

        checkUpcomingTransactionsState(
            partBackupCreateLatch, 0, // Started.
            partBackupPutLatch, backupNodes.size(),
            partBackupCommitLatch, backupNodes.size(),
            partNearCreateLatch, 0, // Started.
            partNearPutLatch, 0, // Near nodes able to start transactions on primaries (Cell 2),
            partNearCommitLatch, 0); // Able to commit, since all primaries and backups are in Cell 2.

        checkTransactionsCount(
            orig != failed ? orig : null, 1 /* partitioned */,
            null, 0,
            backupNodes, 1 /* partitioned */,
            nearNodes, 0,
            vers);

        assertTrue(waitForCondition(lsnrNear::check, 5000));

        listeningLog.registerListener(lsnrBackup);

        // Partitioned recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, (t) -> true);
        }

        partCommitLatch.countDown();
        partFut.get();

        // Switches finished cluster-wide, all transactions can be committed.
        checkUpcomingTransactionsState(
            replBackupCreateLatch, 0,
            replBackupPutLatch, 0,
            replBackupCommitLatch, 0,
            replNearCreateLatch, 0,
            replNearPutLatch, 0,
            replNearCommitLatch, 0);

        checkUpcomingTransactionsState(
            partBackupCreateLatch, 0,
            partBackupPutLatch, 0,
            partBackupCommitLatch, 0,
            partNearCreateLatch, 0,
            partNearPutLatch, 0,
            partNearCommitLatch, 0);

        // Check that pre-failure transactions are absent.
        checkTransactionsCount(
            orig != failed ? orig : null, 0,
            null, 0,
            backupNodes, 0,
            nearNodes, 0,
            vers);

        assertTrue(waitForCondition(lsnrBackup::check, 5000));

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        for (Ignite node : G.allGrids()) {
            assertEquals(42, node.getOrCreateCache(PART_CACHE_NAME).get(partKey));
            assertEquals(43, node.getOrCreateCache(REPL_CACHE_NAME).get(replKey));
        }

        // Final check that any transactions are absent.
        checkTransactionsCount(
            null, 0,
            null, 0,
            backupNodes, 0,
            nearNodes, 0,
            null);
    }

    /**
     *
     */
    private void checkUpcomingTransactionsState(
        CountDownLatch backupCreateLatch,
        int backupCreateCnt,
        CountDownLatch backupPutLatch,
        int backupPutCnt,
        CountDownLatch backupCommitLatch,
        int backupCommitCnt,
        CountDownLatch nearCreateLatch,
        int nearCreateCnt,
        CountDownLatch nearPutLatch,
        int nearPutCnt,
        CountDownLatch nearCommitLatch,
        int nearCommitCnt) throws InterruptedException {
        checkTransactionsState(backupCreateLatch, backupCreateCnt);
        checkTransactionsState(backupPutLatch, backupPutCnt);
        checkTransactionsState(backupCommitLatch, backupCommitCnt);
        checkTransactionsState(nearCreateLatch, nearCreateCnt);
        checkTransactionsState(nearPutLatch, nearPutCnt);
        checkTransactionsState(nearCommitLatch, nearCommitCnt);
    }

    /**
     *
     */
    private void checkTransactionsState(CountDownLatch latch, int cnt) throws InterruptedException {
        if (cnt > 0)
            assertEquals(cnt, latch.getCount()); // Switch in progress.
        else
            latch.await(); // Switch finished (finishing).
    }
}
