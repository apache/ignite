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
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests scenarios for tx reordering, missed updates and recovery for 2PC.
 */
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
    private static final int SERVERS_CNT = 3;

    /** */
    @Test
    public void testPartialPrepare_2TX_1_1() throws Exception {
        doTestPartialPrepare_2tx(true, new int[] {3, 7}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, 0);
    }

    /** */
    @Test
    public void testPartialPrepare_2TX_1_2() throws Exception {
        doTestPartialPrepare_2tx(true, new int[] {3, 7}, new int[] {0, 1}, new int[] {1, 0}, new int[] {1, 0}, 7);
    }

    /** */
    @Test
    public void testPartialPrepare_2TX_2_1() throws Exception {
        doTestPartialPrepare_2tx(false, new int[] {3, 7}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, 0);
    }

    /** */
    @Test
    public void testPartialPrepare_2TX_2_2() throws Exception {
        doTestPartialPrepare_2tx(false, new int[] {3, 7}, new int[] {0, 1}, new int[] {1, 0}, new int[] {1, 0}, 7);
    }

    /** */
    @Test
    public void testPartialCommit_2TX_1()
        throws Exception {
        doTestPartialCommit_2tx(true, new int[] {1, 0});
    }

    /** */
    @Test
    public void testPartialCommit_2TX_2()
        throws Exception {
        doTestPartialCommit_2tx(false, new int[] {1, 0});
    }

    /** */
    @Test
    public void testPartialCommit_2TX_3()
        throws Exception {
        doTestPartialCommit_2tx(true, new int[] {0, 1});
    }

    /** */
    @Test
    public void testPartialCommit_2TX_4()
        throws Exception {
        doTestPartialCommit_2tx(false, new int[] {0, 1});
    }

    /** */
    @Test
    public void testPartialCommit_3TX_1() throws Exception {
        doTestPartialCommit_3tx_1(false);
    }

    /** */
    @Test
    public void testPartialCommit_3TX_2() throws Exception {
        doTestPartialCommit_3tx_1(true);
    }

    /** */
    @Test
    public void testPrepareOnlyTxFailover_3TX_1() throws Exception {
        doTestPartialCommit_3tx_2(false);
    }

    /** */
    @Test
    public void testPrepareOnlyTxFailover_3TX_2() throws Exception {
        doTestPartialCommit_3tx_2(true);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_1_1() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {2, 1, 0}, 0);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_2_1() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {2, 1, 0}, 1);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_3_1() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {2, 1, 0}, 2);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_4_1() throws Exception {
        doTestPartialPrepare_3tx(false, new int[] {2, 1, 0}, 0);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_5_1() throws Exception {
        doTestPartialPrepare_3tx(false, new int[] {2, 1, 0}, 1);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_6_1() throws Exception {
        doTestPartialPrepare_3tx(false, new int[] {2, 1, 0}, 2);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_1_2() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {0, 1, 2}, 0);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_2_2() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {0, 1, 2}, 1);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_3_2() throws Exception {
        doTestPartialPrepare_3tx(true, new int[] {0, 1, 2}, 2);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_4_2() throws Exception {
        doTestPartialPrepare_3tx(false, new int[] {0, 1, 2}, 0);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_5_2() throws Exception {
        doTestPartialPrepare_3tx(false, new int[] {0, 1, 2}, 1);
    }

    /** */
    @Test
    public void testPartialPrepare_3TX_6_2() throws Exception {
        doTestPartialPrepare_3tx(false, new int[] {0, 1, 2}, 2);
    }

    /**
     * Test scenario:
     * <p>
     * 1. Prepare all txs.
     * <p>
     * 2. Fail backup1 after first commit.
     * <p>
     * 3. Start failed backup.
     * <p>
     * 4. Check if the backup is rebalanced correctly from primary node.
     * <p>
     * 5. Stop primary node.
     * <p>
     * 6. Put data to remaining nodes.
     * <p>
     * 7. Start primary node.
     * <p>
     * 8. Check if primary is rebalanced correctly from new primary node.
     *
     * @param skipCheckpointOnNodeStop Skip checkpoint on node stop.
     * @throws Exception If failed.
     */
    private void doTestPartialCommit_3tx_1(boolean skipCheckpointOnNodeStop) throws Exception {
        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, SERVERS_CNT,
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
                                PartitionUpdateCounter cntr = counter(PARTITION_ID, backup.name());

                                assertNotNull(cntr);

                                assertFalse(cntr.sequential());

                                long[] gap = cntr.iterator().next();

                                assertEquals(
                                    PRELOAD_KEYS_CNT + SIZES[BACKUP_COMMIT_ORDER[1]] + SIZES[BACKUP_COMMIT_ORDER[2]],
                                    gap[0]);
                                assertEquals(SIZES[BACKUP_COMMIT_ORDER[0]], gap[1]);

                                runAsync(() -> {
                                    stopGrid(skipCheckpointOnNodeStop, backup.name()); // Will stop backup node before all commits are applied.
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

        waitForTopology(SERVERS_CNT);

        awaitPartitionMapExchange();

        IgniteEx client = grid(CLIENT_GRID_NAME);

        assertEquals("Primary has not all committed transactions", TOTAL, client.cache(DEFAULT_CACHE_NAME).size());

        for (Ignite ignite : G.allGrids())
            TestRecordingCommunicationSpi.spi(ignite).stopBlock(false);

        String backupName = txTop.get2().get(0).name();

        IgniteEx backup = startGrid(backupName);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        PartitionUpdateCounter cntr = counter(PARTITION_ID, backup.name());

        assertNotNull(cntr);

        assertTrue(cntr.sequential());

        assertEquals(TOTAL, cntr.get());

        String primaryName = txTop.get1().name();

        stopGrid(primaryName);

        awaitPartitionMapExchange();

        assertNotNull(cntr = counter(PARTITION_ID, backup.name()));

        assertEquals(TOTAL, cntr.reserved());

        // Make update to advance a counter.
        int addCnt = 10;

        loadDataToPartition(PARTITION_ID, backupName, DEFAULT_CACHE_NAME, addCnt, TOTAL);

        // TODO https://issues.apache.org/jira/browse/IGNITE-11607
        // Historical rebalance is not possible from checkpoint containing rebalance entries.
        // Next rebalance will be full.
        IgniteEx grid0 = startGrid(primaryName);

        awaitPartitionMapExchange();

        assertNotNull(cntr = counter(PARTITION_ID, grid0.name()));

        assertEquals(TOTAL + addCnt, cntr.get());

        assertEquals(TOTAL + addCnt, cntr.reserved());

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /**
     * Tests counters consistency when transaction is not prepared on second backup.
     * <p>
     * Scenario:
     * <p>
     * 1. Start 3 transactions.
     * <p>
     * 2. Assign counters in given order.
     * <p>
     * 3. Commit tx2.
     * <p>
     * 4. Prepare tx0 on backup1 (modes 0, 2).
     * <p>
     * 5. Prepare tx1 on backup2 (modes 1, 2).
     * <p>
     * 6. Fail primary triggering tx rollback on recovery.
     * <p>
     * 7. Validate partitions integrity after node left.
     * <p>
     * Pass condition: partitions are consistent, no holes are expected (they should be closed by message with counters)
     *
     * @param skipCheckpointOnNodeStop Skip checkpoint on node stop.
     * @param assignOrder Tx assign order.
     * @param mode Mode.
     * @throws Exception If failed.
     */
    private void doTestPartialPrepare_3tx(boolean skipCheckpointOnNodeStop, int[] assignOrder, int mode) throws Exception {
        AtomicInteger cntr = new AtomicInteger();

        int expCntr = mode == 2 ? 1 : 2;

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, SERVERS_CNT,
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
                        GridFutureAdapter<?> proceedFut) {
                        int idx = order(nearXidVer);

                        log.info("TX: primary prepared: [node=" + primary.name() + ", txId=" + idx + ']');

                        if (cntr.getAndIncrement() == expCntr) {
                            log.info("Stopping primary [name=" + primary.name() + ']');

                            runAsync(() -> stopGrid(skipCheckpointOnNodeStop, primary.name()));
                        }

                        return idx != 2;
                    }

                    @Override public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> proceedFut) {
                        log.info("TX: primary finished: [node=" + primary.name() + ", txId=" + order(nearXidVer) + ']');

                        if (cntr.getAndIncrement() == expCntr) {
                            log.info("TX: Stopping primary [name=" + primary.name() + ']');

                            runAsync(() -> {
                                stopGrid(skipCheckpointOnNodeStop, primary.name());

                                TestRecordingCommunicationSpi.stopBlockAll();
                            });
                        }

                        return false;
                    }
                };
            },
            SIZES);

        waitForTopology(3);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(CLIENT_GRID_NAME), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

        startGrid(txTops.get(PARTITION_ID).get1().name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(CLIENT_GRID_NAME), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }

    /**
     * Test scenario:
     * <p>
     * 1. Start 2 transactions.
     * <p>
     * 2. Assign counters in given order.
     * <p>
     * 4. Prepare tx0 on backup1.
     * <p>
     * 5. Prepare tx1 on backup2
     * <p>
     * 6. Fail primary.
     * <p>
     * 7. Validate partitions integrity after node left.
     *
     * @param skipCheckpointOnNodeStop Skip checkpoint on node stop.
     * @param sizes Sizes.
     * @param assignOrder Assign order.
     * @param backup1Order Backup 1 order.
     * @param backup2Order Backup 2 order.
     * @param expCommittedSize Expected committed size.
     * @throws Exception If failed.
     */
    private void doTestPartialPrepare_2tx(boolean skipCheckpointOnNodeStop, int[] sizes, int[] assignOrder, int[] backup1Order,
        int[] backup2Order, int expCommittedSize) throws Exception {
        AtomicInteger cntr = new AtomicInteger();

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, SERVERS_CNT,
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

                            runAsync(() -> {
                                stopGrid(skipCheckpointOnNodeStop, primary.name());

                                TestRecordingCommunicationSpi.stopBlockAll();
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

        assertEquals(PRELOAD_KEYS_CNT + expCommittedSize, grid("client").cache(DEFAULT_CACHE_NAME).size());

        // Start primary.
        startGrid(txTops.get(PARTITION_ID).get1().name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }

    /**
     * Test scenario:
     * <p>
     * 1. Start 2 concurrent txs.
     * <p>
     * 2. Assign counters in specified order.
     * <p>
     * 3. Prepare tx0 and tx1.
     * <p>
     * 4. Prevent tx0 from finishing.
     * <p>
     * 5. Finish tx1 only on backup2.
     * <p>
     * 6. Stop backup1 and primary. tx0 must commit on backup2.
     * <p>
     * 7. Validate partitions.
     * <p>
     * 8. Start backup2.
     * <p>
     *
     * Pass condition: partitions are in sync after backup2 had joined topology.
     *
     * @param skipCheckpointOnNodeStop {@code True} to skip checkpoint on node restart.
     * @param assignOrder Tx counters assign order.
     * @throws Exception If failed.
     */
    private void doTestPartialCommit_2tx(boolean skipCheckpointOnNodeStop, int[] assignOrder) throws Exception {
        final int[] sizes = new int[] {3, 7};

        final int stopBackupIdx = 0; // Backup with the index will be restarted.

        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, SERVERS_CNT,
            map -> {
                Ignite primary = map.get(PARTITION_ID).get1();
                Ignite backup1 = map.get(PARTITION_ID).get2().get(stopBackupIdx);
                Ignite backup2 = map.get(PARTITION_ID).get2().get(1);

                return new TwoPhaseCommitTxCallbackAdapter(
                    U.map((IgniteEx)primary, assignOrder),
                    U.map((IgniteEx)backup1, new int[] {1, 0}, (IgniteEx)backup2, new int[] {1, 0}),
                    new HashMap<>(),
                    sizes.length) {
                    @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup,
                        @Nullable IgniteInternalTx primaryTx,
                        IgniteInternalTx backupTx, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
                        int idx = order(nearXidVer);

                        return idx == 0 || idx == 1 && backup == backup1 ||
                            super.beforeBackupFinish(primary, backup, primaryTx, backupTx, nearXidVer, proceedFut);
                    }

                    @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> proceedFut) {
                        int idx = order(nearXidVer);

                        log.info("TX: committed on backup [name=" + backup.name() + ", txId=" + idx + ']');

                        runAsync(() -> {
                            stopGrid(skipCheckpointOnNodeStop, backup1.name());
                            stopGrid(skipCheckpointOnNodeStop, primary.name());

                            TestRecordingCommunicationSpi.stopBlockAll();
                        });

                        return true;
                    }
                };
            }, sizes);

        waitForTopology(2);

        awaitPartitionMapExchange();

        int size = grid(0).cache(DEFAULT_CACHE_NAME).size();

        assertEquals(sizes[0] + sizes[1] + PRELOAD_KEYS_CNT, size); // Txs must be committed on last remaining backup.

        startGrid(txTops.get(PARTITION_ID).get2().get(stopBackupIdx).name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid("client"), DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }

    /**
     * Test scenario:
     * <p>
     * 1. Assign counters in specified order.
     * <p>
     * 2. Prepare all three txs.
     * <p>
     * 3. Prevent committing any tx.
     * <p>
     * 4. Fail primary to trigger recovery.
     * <p>
     * Pass condition: after primary joined partitions are consistent, all transactions are committed.
     *
     * @param skipCheckpointOnStop Skip checkpoint on node stop.
     * @throws Exception If failed.
     */
    private void doTestPartialCommit_3tx_2(boolean skipCheckpointOnStop) throws Exception {
        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runOnPartition(PARTITION_ID, null, BACKUPS, SERVERS_CNT, map -> {
            Ignite primary = map.get(PARTITION_ID).get1();
            Ignite backup1 = map.get(PARTITION_ID).get2().get(0);
            Ignite backup2 = map.get(PARTITION_ID).get2().get(1);

            return new TwoPhaseCommitTxCallbackAdapter(
                U.map((IgniteEx)primary, new int[] {0, 1, 2}),
                U.map(
                    (IgniteEx)primary, new int[] {0, 1, 2},
                    (IgniteEx)backup1, new int[] {0, 1, 2},
                    (IgniteEx)backup2, new int[] {0, 1, 2}),
                new HashMap<>(),
                SIZES.length) {
                @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx,
                    GridFutureAdapter<?> proceedFut) {
                    runAsync(() -> {
                        stopGrid(skipCheckpointOnStop, primary.name());

                        TestRecordingCommunicationSpi.stopBlockAll();
                    });

                    // Stop primary before any tx committed.
                    return true;
                }
            };
        }, SIZES);

        waitForTopology(SERVERS_CNT);

        awaitPartitionMapExchange();

        IgniteEx client = grid("client");

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

        startGrid(txTops.get(PARTITION_ID).get1().name());

        awaitPartitionMapExchange();

        // TODO assert with expected lwm value.
        assertCountersSame(PARTITION_ID, true);

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        // Recovery will commit backup transactions and primary should sync correctly.
        assertEquals(TOTAL, client.cache(DEFAULT_CACHE_NAME).size());
    }
}
