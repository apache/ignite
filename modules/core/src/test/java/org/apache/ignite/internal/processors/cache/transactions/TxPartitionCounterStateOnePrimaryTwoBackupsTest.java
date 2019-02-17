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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TODO add flag for stoppping primary w/o checkpoint.
 * TODO add txs with removes.
 */
@RunWith(JUnit4.class)
public class TxPartitionCounterStateOnePrimaryTwoBackupsTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int[] PREPARE_ORDER = new int[] {0, 1, 2};

    /** */
    private static final int[] PRIMARY_COMMIT_ORDER = new int[] {1, 2, 0};

    /** */
    private static final int[] BACKUP_COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    private static final int[] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum() + PRELOAD_KEYS_CNT;

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 2;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    @Test
    public void testPrepareCommitReorderFailBackupAfterTx1CommitWithRebalance() throws Exception {
        doTestPrepareCommitReorderFailBackupAfterTx1CommitWithRebalance(false);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailBackupAfterTx1CommitWithRebalance2() throws Exception {
        doTestPrepareCommitReorderFailBackupAfterTx1CommitWithRebalance(true);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft2Tx_1_1() throws Exception {
        // Txs not fully prepared and will be rolled back.
        doTestPartialPrepare_2tx(true, new int[] {3, 7}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, 0);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft2Tx_1_2() throws Exception {
        // Will generate hole due to skipped tx0. Should be closed by finalizeUpdateCounters.
        doTestPartialPrepare_2tx(true, new int[] {3, 7}, new int[] {0, 1}, new int[] {1, 0}, new int[] {1, 0}, 7);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft2Tx_2_1() throws Exception {
        doTestPartialPrepare_2tx(false, new int[] {3, 7}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, 0);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft2Tx_2_2() throws Exception {
        // Will generate hole due to skipped tx0. Should be closed by finalizeUpdateCounters.
        doTestPartialPrepare_2tx(false, new int[] {3, 7}, new int[] {0, 1}, new int[] {1, 0}, new int[] {1, 0}, 7);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft3Tx_1_1() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {2, 1, 0}, 0);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft3Tx_2_1() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {2, 1, 0}, 1);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft3Tx_3_1() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {2, 1, 0}, 2);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft3Tx_1_2() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {0, 1, 2}, 0);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft3Tx_2_2() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {0, 1, 2}, 1);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailOnBackupBecausePrimaryLeft3Tx_3_2() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {0, 1, 2}, 2);
    }

    /** */
    @Test
    public void testPrepareCommitReorderTestRebalanceFromPartitionWithMissedUpdatesDueToRollbackWithRebalance() throws Exception {
        doTestPartialPrepare_2TxCommitWithRebalance(true, new int[] {0, 1});
    }

    /** */
    @Test
    public void testPrepareCommitReorderTestRebalanceFromPartitionWithMissedUpdatesDueToRollbackWithRebalance2() throws Exception {
        doTestPartialPrepare_2TxCommitWithRebalance(true, new int[] {1, 0});
    }

    /** */
    @Test
    public void testSkipReservedCountersAfterRecoveryStopPrimary() throws Exception {
        doTestSkipReservedCountersAfterRecovery2(false);
    }

    /** */
    @Test
    public void testSkipReservedCountersAfterRecoveryStopPrimary2() throws Exception {
        doTestSkipReservedCountersAfterRecovery2(true);
    }

    /**
     * Test scenario:
     *
     * 1. Prepare all txs.
     * 2. Fail backup1 after first commit.
     * 3. Start failed backup.
     * 4. Check if the backup is rebalanced correctly from primary node.
     * 5. Stop primary node.
     * 6. Put data to remaining nodes.
     * 7. Start primary node.
     * 8. Check if primary is rebalanced correctly from new primary node.
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestPrepareCommitReorderFailBackupAfterTx1CommitWithRebalance(
        boolean skipCheckpoint) throws Exception {
        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT,
            new IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback>() {
                @Override public TxCallback apply(Map<Integer, T2<Ignite, List<Ignite>>> map) {
                    Ignite primary = map.get(PARTITION_ID).get1();
                    Ignite backup1 = map.get(PARTITION_ID).get2().get(0);

                    return new TwoPhaseCommitTxCallbackAdapter(U.map((IgniteEx)primary, PREPARE_ORDER),
                        U.map((IgniteEx)primary, PRIMARY_COMMIT_ORDER, (IgniteEx)backup1, BACKUP_COMMIT_ORDER),
                        SIZES.length) {
                        @Override protected boolean onBackupCommitted(IgniteEx backup, int idx) {
                            super.onBackupCommitted(backup, idx);

                            if (idx == BACKUP_COMMIT_ORDER[0]) {
                                PartitionUpdateCounterImpl cntr = (PartitionUpdateCounterImpl)counter(PARTITION_ID, backup.name());

                                assertFalse(cntr.gaps().isEmpty());

                                PartitionUpdateCounterImpl.Item gap = cntr.gaps().first();

                                assertEquals(PRELOAD_KEYS_CNT + SIZES[BACKUP_COMMIT_ORDER[1]] + SIZES[BACKUP_COMMIT_ORDER[2]], gap.start());
                                assertEquals(SIZES[BACKUP_COMMIT_ORDER[0]], gap.delta());

                                runAsync(new Runnable() {
                                    @Override public void run() {
                                        stopGrid(skipCheckpoint, backup.name()); // Will stop backup node before all commits are applied.
                                    }
                                });

                                return true;
                            }

                            throw new IgniteException("Should not commit other transactions");
                        }
                    };
                }
            },
            SIZES);

        T2<Ignite, List<Ignite>> txTop = txTops.get(PARTITION_ID);

        waitForTopology(NODES_CNT);

        awaitPartitionMapExchange();

        IgniteEx client = grid(CLIENT_GRID_NAME);

        assertEquals("Primary has not all committed transactions", TOTAL, client.cache(DEFAULT_CACHE_NAME).size());

        for (Ignite ignite : G.allGrids())
            TestRecordingCommunicationSpi.spi(ignite).stopBlock(false);

        String backupName = txTop.get2().get(0).name();

        IgniteEx backup = startGrid(backupName);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        // Check if holes are closed on rebalance.
        PartitionUpdateCounterImpl cntr = (PartitionUpdateCounterImpl)counter(PARTITION_ID, backup.name());

        assertTrue(cntr.gaps().isEmpty());

        assertEquals(TOTAL, cntr.get());

        String primaryName = txTop.get1().name();

        stopGrid(primaryName);

        awaitPartitionMapExchange();

        cntr = (PartitionUpdateCounterImpl)counter(PARTITION_ID, backup.name());

        assertEquals(TOTAL, cntr.reserved());

        // Make update to advance a counter.
        int addCnt = 10;

        loadDataToPartition(PARTITION_ID, backupName, DEFAULT_CACHE_NAME, addCnt, TOTAL);

        // Historical rebalance is not possible from checkpoint containing rebalance entries.
        // Next rebalance will be full. TODO FIXME repair this scenario ?
        IgniteEx grid0 = startGrid(primaryName);

        awaitPartitionMapExchange();

        cntr = (PartitionUpdateCounterImpl)counter(PARTITION_ID, grid0.name());

        assertEquals(TOTAL + addCnt, cntr.get());

        assertEquals(TOTAL + addCnt, cntr.reserved());

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /**
     * Test scenario:
     *
     * 1. Start 3 transactions.
     * 2. Assign counters in given order.
     * 3. Commit tx2.
     * 4. Prepare tx0 on backup1 (mode=0, 2).
     * 5. Prepare tx1 on backup2 (mode=1, 2).
     * 6. Fail primary triggering tx rollback on recovery.
     * 7. Validate partitions integrity after node left. No holes are expected (they should be closed by message with counters)
     *
     * @param skipCheckpoint Skip checkpoint on node stop.
     * @throws Exception
     */
    private void doTestPartialPrepare_3tx(boolean skipCheckpoint, int[] assignOrder, int mode) throws Exception {
        AtomicInteger cntr = new AtomicInteger();

        int expCntr = mode == 2 ? 1 : 2;

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT,
            map -> {
                Ignite primary = map.get(PARTITION_ID).get1();
                Ignite backup1 = map.get(PARTITION_ID).get2().get(0);
                Ignite backup2 = map.get(PARTITION_ID).get2().get(1);

                return new TwoPhaseCommitTxCallbackAdapter(
                    U.map((IgniteEx)primary, assignOrder),
                    U.map((IgniteEx)backup1, new int[] {2, 0, 1}, (IgniteEx)backup2, new int[] {2, 1, 0}),
                    new HashMap<>(),
                    SIZES.length) {
                    @Override protected boolean onBackupPrepared(IgniteEx backup, IgniteInternalTx tx, int idx) {
                        super.onBackupPrepared(backup, tx, idx);

                        switch (mode) {
                            case 0:
                                return idx == 0 && backup == backup1;
                            case 1:
                                return idx == 1 && backup == backup2;
                            case 2:
                                return idx == 0 && backup == backup1 || idx == 1 && backup == backup2;
                        }

                        return false;
                    }

                    @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> fut) {
                        int idx = order(nearXidVer);

                        log.info("TX: primary prepared: [node=" + primary.name() + ", txId=" + idx + ']');

                        if (cntr.getAndIncrement() == expCntr) {
                            log.info("Stopping primary [name=" + primary.name() + ']');

                            runAsync(new Runnable() {
                                @Override public void run() {
                                    stopGrid(skipCheckpoint, primary.name());
                                }
                            });
                        }

                        return idx != 2;
                    }

                    @Override public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> proceedFut) {
                        log.info("TX: primary finished: [node=" + primary.name() + ", txId=" + order(nearXidVer) + ']');

                        if (cntr.getAndIncrement() == expCntr) {
                            log.info("TX: Stopping primary [name=" + primary.name() + ']');

                            runAsync(new Runnable() {
                                @Override public void run() {
                                    stopGrid(skipCheckpoint, primary.name());

                                    TestRecordingCommunicationSpi.stopBlockAll();
                                }
                            });
                        }

                        return false;
                    }
                };
            },
            SIZES);

        waitForTopology(3);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

        startGrid(txTops.get(PARTITION_ID).get1().name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }

    /**
     * Test scenario:
     *
     * 1. Start 2 transactions.
     * 2. Assign counters in given order.
     * 4. Prepare tx0 on backup1.
     * 4. Prepare tx1 on backup2
     * 5. Fail primary.
     * 6. Validate partitions integrity after node left.
     *
     * @param skipCheckpoint
     * @throws Exception
     */
    private void doTestPartialPrepare_2tx(boolean skipCheckpoint, int[] sizes, int[] assignOrder, int[] backup1Order,
        int[] backup2Order, int expCommSize) throws Exception {
        AtomicInteger cntr = new AtomicInteger();

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT,
            map -> {
                Ignite primary = map.get(PARTITION_ID).get1();
                Ignite backup1 = map.get(PARTITION_ID).get2().get(0);
                Ignite backup2 = map.get(PARTITION_ID).get2().get(1);

                return new TwoPhaseCommitTxCallbackAdapter(
                    U.map((IgniteEx)primary, assignOrder),
                    U.map((IgniteEx)backup1, backup1Order, (IgniteEx)backup2, backup2Order),
                    new HashMap<>(),
                    sizes.length) {
                    @Override protected boolean onBackupPrepared(IgniteEx backup, IgniteInternalTx tx, int idx) {
                        super.onBackupPrepared(backup, tx, idx);

                        if (cntr.getAndIncrement() == 1) {
                            log.info("Stopping primary [name=" + primary.name() + ']');

                            runAsync(new Runnable() {
                                @Override public void run() {
                                    stopGrid(skipCheckpoint, primary.name());

                                    TestRecordingCommunicationSpi.stopBlockAll();
                                }
                            });
                        }

                        return true;
                    }
                };
            },
            sizes);

        waitForTopology(3);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

        assertEquals(PRELOAD_KEYS_CNT + expCommSize, grid("client").cache(DEFAULT_CACHE_NAME).size());

        // Start primary.
        startGrid(txTops.get(PARTITION_ID).get1().name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }

    /**
     * Test scenario:
     *
     * 1. Start 2 concurrent txs.
     * 2. Assign counters in specified order.
     * 3. Prepare tx0 only on backup2.
     * 4. Finish tx1 only on backup2.
     * 5. Stop primary and backup1.
     * 6. Validate partitions.
     * 7. Start backup2.
     * 8. Validate partitions again after (historical) rebalance.
     *
     * @param skipCheckpoint
     * @throws Exception
     */
    private void doTestPartialPrepare_2TxCommitWithRebalance(boolean skipCheckpoint,
        int[] assignOrder) throws Exception {
        AtomicInteger cntr = new AtomicInteger();

        int[] sizes = new int[] {3, 7};

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT,
            map -> {
                Ignite primary = map.get(PARTITION_ID).get1();
                Ignite backup1 = map.get(PARTITION_ID).get2().get(0);
                Ignite backup2 = map.get(PARTITION_ID).get2().get(1);

                return new TwoPhaseCommitTxCallbackAdapter(
                    U.map((IgniteEx)primary, assignOrder),
                    U.map((IgniteEx)backup1, new int[] {1, 0}, (IgniteEx)backup2, new int[] {1, 0}),
                    new HashMap<>(),
                    sizes.length) {
                    @Override protected boolean onBackupPrepared(IgniteEx backup, IgniteInternalTx tx, int idx) {
                        super.onBackupPrepared(backup, tx, idx);

                        if (idx == 1 && backup == backup1) {
                            if (cntr.getAndIncrement() == 2) {
                                log.info("Stopping primary [name=" + primary.name() + ']');

                                runAsync(new Runnable() {
                                    @Override public void run() {
                                        stopGrid(skipCheckpoint, primary.name());

                                        TestRecordingCommunicationSpi.stopBlockAll();
                                    }
                                });
                            }

                            return true;
                        }

                        return false;
                    }

                    @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup,
                        @Nullable IgniteInternalTx primaryTx,
                        IgniteInternalTx backupTx, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                        int idx = order(nearXidVer);

                        if (idx == 1 && backup == backup1) {
                            if (cntr.getAndIncrement() == 2) {
                                log.info("Stopping primary [name=" + primary.name() + ']');

                                // TODO FIXME refactor: stopGridAsync
                                runAsync(new Runnable() {
                                    @Override public void run() {
                                        stopGrid(skipCheckpoint, primary.name());

                                        TestRecordingCommunicationSpi.stopBlockAll();
                                    }
                                });
                            }

                            return true;
                        }

                        return false;
                    }

                    @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> fut) {
                        int idx = order(nearXidVer);

                        if (idx == 1 && backup == backup2) {
                            log.info("TX: committed on backup [name=" + backup.name() + ", txId=" + idx + ']');

                            if (cntr.getAndIncrement() == 2) {
                                log.info("Stopping primary [name=" + primary.name() + ']');

                                runAsync(new Runnable() {
                                    @Override public void run() {
                                        stopGrid(skipCheckpoint, backup1.name());
                                        stopGrid(skipCheckpoint, primary.name());

                                        TestRecordingCommunicationSpi.stopBlockAll();
                                    }
                                });
                            }

                            return true;
                        }

                        return super.afterBackupFinish(primary, backup, nearXidVer, fut);
                    }
                };
            },
            sizes);

        waitForTopology(2);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

        // Start backup1.
        startGrid(txTops.get(PARTITION_ID).get2().get(0).name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }

    /**
     * Tests when counter reserved on prepare should never be applied after recovery.
     *
     * @throws Exception
     */
    private void doTestSkipReservedCountersAfterRecovery2(boolean skipCheckpointOnStop) throws Exception {
        AtomicInteger cnt = new AtomicInteger();

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT, map -> {
            Ignite primary = map.get(PARTITION_ID).get1();
            Ignite backup1 = map.get(PARTITION_ID).get2().get(0);
            Ignite backup2 = map.get(PARTITION_ID).get2().get(1);

            return new TwoPhaseCommitTxCallbackAdapter(
                U.map((IgniteEx)primary, new int[] {0, 1, 2}),
                U.map((IgniteEx)primary, new int[] {0, 1, 2}, (IgniteEx)backup1, new int[] {0, 1, 2}, (IgniteEx)backup2, new int[] {0, 1, 2}),
                new HashMap<>(),
                SIZES.length) {
                @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx,
                    GridFutureAdapter<?> proceedFut) {
                    if (cnt.getAndIncrement() == 2) {
                        runAsync(new Runnable() {
                            @Override public void run() {
                                stopGrid(skipCheckpointOnStop, primary.name());

                                TestRecordingCommunicationSpi.stopBlockAll();
                            }
                        });
                    }

                    return true;

                }
            };
        }, SIZES);

        waitForTopology(NODES_CNT);

        awaitPartitionMapExchange();

        IgniteEx client = grid("client");

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

        startGrid(txTops.get(PARTITION_ID).get1().name());

        awaitPartitionMapExchange();

        assertCountersSame(PARTITION_ID, true);

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        // Recovery will commit backup transactions and primary should sync correctly.
        assertEquals(TOTAL, client.cache(DEFAULT_CACHE_NAME).size());
    }
}
