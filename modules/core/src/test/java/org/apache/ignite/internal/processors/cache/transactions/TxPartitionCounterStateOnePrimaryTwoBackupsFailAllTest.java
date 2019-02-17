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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.junit.Test;

/**
 */
public class TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 2;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommitFailAfterFirstCommit() throws Exception {
        doTestPrepareCommitReorder(false, new int[] {0, 1}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, new int[] {5, 5});
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommitFailAfterFirstCommit2() throws Exception {
        doTestPrepareCommitReorder(false, new int[] {0, 1}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, new int[] {8, 5});
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommitFailAfterSecondCommit() throws Exception {
        doTestPrepareCommitReorder2(false, new int[] {0, 1, 2}, new int[] {0, 1, 2}, new int[] {1, 2, 0}, new int[] {2, 1, 0}, new int[] {5, 7, 3});
    }

    /**
     * Test scenario:
     *
     */
    private void doTestPrepareCommitReorder(boolean skipCp, int[] prepareOrder, int[] primCommitOrder, int[] backup1CommitOrder, int[] backup2CommitOrder, int[] sizes) throws Exception {
        AtomicInteger cnt = new AtomicInteger();

        Map<IgniteEx, int[]> commits = new HashMap<>();

        Map<Integer, T2<Ignite, List<Ignite>>> txTop = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT,
            new IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback>() {
                @Override public TxCallback apply(Map<Integer, T2<Ignite, List<Ignite>>> map) {
                    T2<Ignite, List<Ignite>> txTop = map.get(PARTITION_ID);

                    Map<IgniteEx, int[]> prepares = new HashMap<IgniteEx, int[]>();

                    prepares.put((IgniteEx)txTop.get1(), prepareOrder);

                    Ignite backup1 = txTop.get2().get(0);
                    Ignite backup2 = txTop.get2().get(1);

                    commits.put((IgniteEx)txTop.get1(), primCommitOrder);
                    commits.put((IgniteEx)backup1, backup1CommitOrder);
                    commits.put((IgniteEx)backup2, backup2CommitOrder);

                    CountDownLatch l = new CountDownLatch(2);

                    return new TwoPhaseCommitTxCallbackAdapter(prepares, commits, sizes.length) {
                        @Override protected boolean onBackupCommitted(IgniteEx backup, int idx) {
                            super.onBackupCommitted(backup, idx);

                            if (idx == commits.get(backup)[0]) {
                                l.countDown();

                                try {
                                    assertTrue(U.await(l, 30_000, TimeUnit.MILLISECONDS));
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    fail(e.getMessage());
                                }

                                if (backup == backup1) {
                                    // Stop all backups first or recovery will commit a transaction on backups.
                                    stopGrid(skipCp, txTop.get2().get(0).name());
                                    stopGrid(skipCp, txTop.get2().get(1).name());
                                    stopAllGrids();
                                }

                                return true;
                            }

                            return true;
                        }
                    };
                }
            },
            sizes);

        waitForTopology(0);

        IgniteEx crd = startGrid(txTop.get(PARTITION_ID).get2().get(0).name());
        IgniteEx n2 = startGrid(txTop.get(PARTITION_ID).get2().get(1).name());

        crd.cluster().active(true);

        // Backup with gap in update counter should be stopped by failure handler.
        waitForTopology(1);

        IgniteEx backupNode = (IgniteEx)G.allGrids().iterator().next();

        PartitionUpdateCounterImpl cntr = (PartitionUpdateCounterImpl)counter(PARTITION_ID, backupNode.name());

        assertTrue(cntr.gaps().isEmpty());
    }

    /**
     * Test scenario:
     *
     * 1. All txs is prepared.
     * 2. All txs are committed on primary, tx 1 and tx 2 committed in different order on both backups, tx 0 is not committed.
     * 3. All nodes are stopped.
     * 4. Start backup1 and backup2, detect partition inconsistency and trigger failure handler.
     * 5. Verify nodes are stopped.
     */
    private void doTestPrepareCommitReorder2(boolean skipCp, int[] prepareOrder, int[] primCommitOrder, int[] backup1CommitOrder, int[] backup2CommitOrder, int[] sizes) throws Exception {
        Map<IgniteEx, int[]> commits = new HashMap<IgniteEx, int[]>();

        Map<Integer, T2<Ignite, List<Ignite>>> txTop = runOnPartition(PARTITION_ID, null, BACKUPS, NODES_CNT,
            new IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback>() {
                @Override public TxCallback apply(Map<Integer, T2<Ignite, List<Ignite>>> map) {
                    T2<Ignite, List<Ignite>> txTop = map.get(PARTITION_ID);

                    Map<IgniteEx, int[]> prepares = new HashMap<IgniteEx, int[]>();

                    prepares.put((IgniteEx)txTop.get1(), prepareOrder);

                    final Ignite backup1 = txTop.get2().get(0);
                    final Ignite backup2 = txTop.get2().get(1);

                    commits.put((IgniteEx)txTop.get1(), primCommitOrder);
                    commits.put((IgniteEx)backup1, backup1CommitOrder);
                    commits.put((IgniteEx)backup2, backup2CommitOrder);

                    CountDownLatch l = new CountDownLatch(2);

                    return new TwoPhaseCommitTxCallbackAdapter(prepares, commits, sizes.length) {
                        @Override protected boolean onBackupCommitted(IgniteEx backup, int idx) {
                            super.onBackupCommitted(backup, idx);

                            if (idx == commits.get(backup)[1]) {
                                l.countDown();

                                try {
                                    assertTrue(U.await(l, 30_000, TimeUnit.MILLISECONDS));
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    fail(e.getMessage());
                                }

                                if (backup == backup1) {
                                    // Stop all backups first or recovery will commit a transaction on backups.
                                    stopGrid(skipCp, txTop.get2().get(0).name());
                                    stopGrid(skipCp, txTop.get2().get(1).name());
                                    stopAllGrids();
                                }

                                return true;
                            }

                            return false;
                        }
                    };
                }
            },
            sizes);

        waitForTopology(0);

        IgniteEx crd = startGrid(txTop.get(PARTITION_ID).get2().get(0).name());
        IgniteEx n2 = startGrid(txTop.get(PARTITION_ID).get2().get(1).name());

        try {
            n2.cluster().active(true);
        }
        catch (Throwable t) {
            assertTrue(X.hasCause(t, NodeStoppingException.class));
        }

        // All nodes should be stopped by failure handler due to partition update counter inconsistency.
        waitForTopology(0);
    }
}
