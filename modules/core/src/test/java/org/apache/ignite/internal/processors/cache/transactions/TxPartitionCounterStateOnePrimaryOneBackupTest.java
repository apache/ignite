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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TODO add flag for stoppping primary w/o checkpoint.
 */
@RunWith(JUnit4.class)
public class TxPartitionCounterStateOnePrimaryOneBackupTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int[] PREPARE_ORDER = new int[] {0, 1, 2};

    /** */
    private static final int[] PRIMARY_COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    private static final int[] BACKUP_COMMIT_ORDER = new int[] {1, 2, 0};

    /** */
    private static final int [] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum() + PRELOAD_KEYS_CNT;

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 1;

    /** */
    private static final int NODES_CNT = 2;

    /** */
    @Test
    public void testPrepareCommitReorder() throws Exception {
        doTestPrepareCommitReorder(false);
    }

    /** */
    @Test
    public void testPrepareCommitReorderSkipCheckpoint() throws Exception {
        doTestPrepareCommitReorder(true);
    }

    /**
     * Test scenario
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpoint) throws Exception {
        T2<Ignite, List<Ignite>> txTop = runOnPartition(PARTITION_ID, -1, BACKUPS, NODES_CNT, new IgniteClosure2X<Ignite, List<Ignite>, TxCallback>() {
            @Override public TxCallback applyx(Ignite primary,
                List<Ignite> backups) throws IgniteCheckedException {
                return new OnePhasePessimisticTxCallbackAdapter(PREPARE_ORDER, PRIMARY_COMMIT_ORDER, BACKUP_COMMIT_ORDER) {
//                    @Override protected boolean onPrimaryCommitted(IgniteEx primary, int idx) {
//                        if (idx == PRIMARY_COMMIT_ORDER[0]) {
//                            Collection<ClusterNode> nodes = primary.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(PARTITION_ID);
//                            List<ClusterNode> nodesList = new ArrayList<>(nodes);
//
//                            Ignite backupNode = Ignition.ignite(nodesList.get(1).id());
//
//                            PartitionUpdateCounter cntr = counter(PARTITION_ID, backupNode.name());
//
//                            assertEquals(TOTAL, cntr.reserved());
//
//                            assertFalse(cntr.holes().isEmpty());
//
//                            PartitionUpdateCounter.Item gap = cntr.holes().first();
//
//                            assertEquals(PRELOAD_KEYS_CNT + SIZES[BACKUP_COMMIT_ORDER[1]] + SIZES[BACKUP_COMMIT_ORDER[2]], gap.start());
//                            assertEquals(SIZES[PRIMARY_COMMIT_ORDER[0]], gap.delta());
//
//                            stopGrid(skipCheckpoint, backupNode.name()); // Will stop backup node before all commits are applied.
//
//                            return true;
//                        }
//
//                        throw new IgniteException("Should not commit other transactions");
//                    }
                };
            }
        }, SIZES);

        //waitForTopology(1);

        IgniteEx client = grid(CLIENT_GRID_NAME);

        assertEquals("Primary has not all committed transactions", TOTAL, client.cache(DEFAULT_CACHE_NAME).size());

        for (Ignite ignite : G.allGrids())
            TestRecordingCommunicationSpi.spi(ignite).stopBlock(false);

//        String backupName = txTop.get2().get(0).name();
//
//        IgniteEx backup = startGrid(backupName);
//
//        awaitPartitionMapExchange();
//
//        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        // Check if holes are closed on rebalance.
//        PartitionUpdateCounter cntr = counter(PARTITION_ID, backup.name());
//
//        assertTrue(cntr.holes().isEmpty());
//
//        assertEquals(TOTAL, cntr.get());
//
//        String primaryName = txTop.get1().name();
//
//        stopGrid(primaryName);
//
//        awaitPartitionMapExchange();
//
//        cntr = counter(PARTITION_ID, backup.name());
//
//        assertEquals(TOTAL, cntr.reserved());
//
//        // Make update to advance a counter.
//        int addCnt = 10;
//
//        loadDataToPartition(PARTITION_ID, backupName, DEFAULT_CACHE_NAME, addCnt, TOTAL);
//
//        // Historical rebalance is not possible from checkpoint containing rebalance entries.
//        // Next rebalance will be full. TODO FIXME repair this scenario ?
//        IgniteEx grid0 = startGrid(primaryName);
//
//        awaitPartitionMapExchange();
//
//        cntr = counter(PARTITION_ID, grid0.name());
//
//        assertEquals(TOTAL + addCnt, cntr.get());
//
//        assertEquals(TOTAL + addCnt, cntr.reserved());
//
//        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class OnePhasePessimisticTxCallbackAdapter extends TxCallbackAdapter {
        /** */
        private Queue<Integer> prepOrder;

        /** */
        private Queue<Integer> primCommitOrder;

        /** */
        private Queue<Integer> backupCommitOrder;

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> prepFuts = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> primFinishFuts = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> backupFinishFuts = new ConcurrentHashMap<>();

        /** */
        private final int txCnt;

        /**
         * @param prepOrd Prepare order.
         * @param primCommitOrder Commit order.
         */
        public OnePhasePessimisticTxCallbackAdapter(int[] prepOrd, int[] primCommitOrder, int[] backupCommitOrder) {
            this.txCnt = prepOrd.length;

            prepOrder = new ConcurrentLinkedQueue<>();

            for (int aPrepOrd : prepOrd)
                prepOrder.add(aPrepOrd);

            this.primCommitOrder = new ConcurrentLinkedQueue<>();

            for (int aCommitOrd : primCommitOrder)
                this.primCommitOrder.add(aCommitOrd);

            this.backupCommitOrder = new ConcurrentLinkedQueue<>();

            for (int aCommitOrd : backupCommitOrder)
                this.backupCommitOrder.add(aCommitOrd);
        }

        /** */
        protected boolean onPrimaryPrepared(IgniteEx primary, IgniteInternalTx tx, int idx) {
            log.info("TX: prepared on primary [name=" + primary.name() + ", txId=" + idx + ", tx=" + CU.txString(tx) + ']');

            return false;
        }

        /**
         * @param primary Primary primary.
         */
        protected void onAllPrimaryPrepared(IgniteEx primary) {
            log.info("TX: all primary prepared [name=" + primary.name() + ']');
        }

        /**
         * @param primary Primary node.
         * @param idx Index.
         */
        protected boolean onPrimaryCommitted(IgniteEx primary, int idx) {
            log.info("TX: primary committed [name=" + primary.name() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * @param backup Backup node.
         * @param idx Index.
         */
        protected boolean onBackupCommitted(IgniteEx backup, int idx) {
            log.info("TX: backup committed [name=" + backup.name() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * @param primary Primary node.
         */
        protected void onAllPrimaryCommitted(IgniteEx primary) {
            log.info("TX: all primary committed [name=" + primary.name() + ']');
        }

        /**
         * @param backup Backup node.
         */
        protected void onAllBackupCommitted(IgniteEx backup) {
            log.info("TX: all backup committed [name=" + backup.name() + ']');
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                prepFuts.put(nearXidVer, proceedFut);

                // Order prepares.
                if (prepFuts.size() == txCnt) {// Wait until all prep requests queued and force prepare order.
                    prepFuts.remove(version(prepOrder.poll())).onDone();
                }
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                IgniteUuid nearXidVer = primaryTx.nearXidVersion().asGridUuid();

                onPrimaryPrepared(primary, primaryTx, order(nearXidVer));

                backupFinishFuts.put(nearXidVer, proceedFut);

                if (prepOrder.isEmpty() && backupFinishFuts.size() == txCnt) {
                    onAllPrimaryPrepared(primary);

                    assertEquals(txCnt, backupFinishFuts.size());

                    backupFinishFuts.remove(version(backupCommitOrder.poll())).onDone();

                    return;
                }

                prepFuts.remove(version(prepOrder.poll())).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            runAsync(() -> {
                primFinishFuts.put(nearXidVer, fut);

                if (onBackupCommitted(backup, order(nearXidVer)))
                    return;

                if (backupCommitOrder.isEmpty() && primFinishFuts.size() == txCnt) {
                    onAllBackupCommitted(primary);

                    assertEquals(txCnt, primFinishFuts.size());

                    primFinishFuts.remove(version(primCommitOrder.poll())).onDone();

                    return;
                }

                backupFinishFuts.remove(version(backupCommitOrder.poll())).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryPrepare(IgniteEx primary, @Nullable IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onPrimaryCommitted(primary, order(nearXidVer)))
                    return;

                if (primCommitOrder.isEmpty()) {
                    onAllPrimaryCommitted(primary);

                    return;
                }

                primFinishFuts.remove(version(primCommitOrder.poll())).onDone();
            });

            return false;
        }
    }
}
