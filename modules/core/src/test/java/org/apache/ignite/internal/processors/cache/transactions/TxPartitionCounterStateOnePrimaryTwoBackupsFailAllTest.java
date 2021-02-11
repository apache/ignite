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
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterTrackingImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY;

/**
 * Tests partition consistency recovery in case then all owners are lost in the middle of transaction.
 * TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11611
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
    public void testRestartAllOwnersAfterPartialCommit_2tx_1() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(false,
            new int[] {0, 1},
            new int[] {0, 1},
            new int[] {0, 1}, new int[] {1, 0},
            new int[] {5, 5},
            new int[] {0, 0},
            new int[] {0, 1},
            1,
            true);
    }

    /** */
    @Test
    public void testRestartAllOwnersAfterPartialCommit_2tx_2() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(true,
            new int[] {0, 1},
            new int[] {0, 1},
            new int[] {0, 1}, new int[] {1, 0},
            new int[] {5, 5},
            new int[] {0, 0},
            new int[] {0, 1},
            1,
            true);
    }

    /** */
    @Test
    public void testRestartAllOwnersAfterPartialCommit_2tx_3() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(false,
            new int[] {0, 1},
            new int[] {0, 1},
            new int[] {0, 1}, new int[] {1, 0},
            new int[] {5, 5},
            new int[] {0, 0},
            new int[] {1, 0},
            1,
            true);
    }

    /** */
    @Test
    public void testRestartAllOwnersAfterPartialCommit_2tx_4() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(true,
            new int[] {0, 1},
            new int[] {0, 1},
            new int[] {0, 1}, new int[] {1, 0},
            new int[] {5, 5},
            new int[] {0, 0},
            new int[] {1, 0},
            1,
            true);
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_1_1() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(false,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {2, 1, 0},
            new int[] {5, 7, 3},
            new int[] {1, 1},
            new int[] {0, 1},
            0,
            true
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_1_2() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(false,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {2, 1, 0},
            new int[] {5, 7, 3},
            new int[] {1, 1},
            new int[] {1, 0},
            0,
            true
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_2_1() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(true,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {2, 1, 0},
            new int[] {5, 7, 3},
            new int[] {1, 1},
            new int[] {0, 1},
            0,
            true
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_2_2() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(true,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {2, 1, 0},
            new int[] {5, 7, 3},
            new int[] {1, 1},
            new int[] {1, 0},
            0,
            true
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_3() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(false,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {0, 2, 1},
            new int[] {5, 7, 3},
            new int[] {0, 1},
            new int[] {0, 1},
            2,
            false
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_4() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(true,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {0, 2, 1},
            new int[] {5, 7, 3},
            new int[] {0, 1},
            new int[] {0, 1},
            2,
            false
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_5() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(false,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {0, 2, 1},
            new int[] {5, 7, 3},
            new int[] {0, 1},
            new int[] {1, 0},
            2,
            false
        );
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommit_3tx_6() throws Exception {
        doTestRestartAllOwnersAfterPartialCommit(true,
            new int[] {0, 1, 2},
            new int[] {0, 1, 2},
            new int[] {1, 2, 0}, new int[] {0, 2, 1},
            new int[] {5, 7, 3},
            new int[] {0, 1},
            new int[] {1, 0},
            2,
            false
        );
    }

    /**
     * Test scenario:
     * <p>
     * 1. All txs is prepared.
     * <p>
     * 2. All txs are committed on primary, all txs until waitCommitIdx are committed.
     * <p>
     * 3. Tx processing is paused, all nodes are stopped.
     * <p>
     * 4. Start backup1 and backup2 in specified order, detect unrecoverable partition and trigger failure handler.
     * <p>
     * Pass condition: nodes are stopped by failure handler if applicable.
     *
     * @param skipCheckpoint Skip checkpoint on node stop.
     * @param prepareOrder Prepare order.
     * @param primCommitOrder Prim commit order.
     * @param backup1CommitOrder Backup 1 commit order.
     * @param backup2CommitOrder Backup 2 commit order.
     * @param sizes Sizes.
     * @param waitCommitIdx Wait commit index.
     * @param backupsStartOrder Start order of backups (should work same for any order).
     * @param expectAliveNodes Expected alive nodes.
     * @param failNodesOnBadCntr {@code True} to trigger FH if consistency can't be recovered.
     */
    private void doTestRestartAllOwnersAfterPartialCommit(
        boolean skipCheckpoint,
        int[] prepareOrder,
        int[] primCommitOrder,
        int[] backup1CommitOrder,
        int[] backup2CommitOrder,
        int[] sizes,
        int[] waitCommitIdx,
        int[] backupsStartOrder,
        int expectAliveNodes,
        boolean failNodesOnBadCntr) throws Exception {
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

                            if (idx == commits.get(backup)[waitCommitIdx[backup == backup1 ? 0 : 1]]) {
                                l.countDown();

                                // Wait until both backups are committed required transactions.
                                try {
                                    assertTrue(U.await(l, 30_000, TimeUnit.MILLISECONDS));
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    fail(e.getMessage());
                                }

                                if (backup == backup1) {
                                    blockRecovery();

                                    stopGrid(skipCheckpoint, txTop.get2().get(0).name());
                                    stopGrid(skipCheckpoint, txTop.get2().get(1).name());
                                    stopAllGrids(); // Stop all remaining nodes.
                                }

                                return true;
                            }

                            return false;
                        }
                    };
                }
            },
            sizes);

        // All owners should be stopped.
        waitForTopology(0);

        if (failNodesOnBadCntr)
            System.setProperty(IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY, "true");

        try {
            // Start only backups in given order.
            startGrid(txTop.get(PARTITION_ID).get2().get(backupsStartOrder[0]).name());
            startGrid(txTop.get(PARTITION_ID).get2().get(backupsStartOrder[1]).name());

            try {
                grid(0).cluster().active(true);
            }
            catch (Throwable t) {
                // Nodes are expected to stop during activation due to irrecoverable partition consistency.
                assertTrue(X.hasCause(t, NodeStoppingException.class));
            }

            waitForTopology(expectAliveNodes);

            awaitPartitionMapExchange();
        }
        finally {
            System.clearProperty(IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY);
        }

        // Alive node should not have missed updates.
        if (expectAliveNodes == 1) {
            IgniteEx node = (IgniteEx)G.allGrids().iterator().next();

            PartitionUpdateCounterTrackingImpl cntr = (PartitionUpdateCounterTrackingImpl)counter(PARTITION_ID, node.name());

            assertTrue(cntr.sequential());
        }
    }
}
