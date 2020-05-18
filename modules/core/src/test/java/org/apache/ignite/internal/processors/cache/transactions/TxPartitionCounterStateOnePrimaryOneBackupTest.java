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

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 */
public class TxPartitionCounterStateOnePrimaryOneBackupTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int[] PREPARE_ORDER = new int[] {0, 1, 2};

    /** */
    private static final int[] PRIMARY_COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    private static final int[] BACKUP_COMMIT_ORDER = new int[] {1, 2, 0};

    /** */
    private static final int[] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum() + PRELOAD_KEYS_CNT;

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 1;

    /** */
    private static final int SERVERS_CNT = 2;

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

    /** */
    @Test
    public void testPrepareCommitReorderFailRebalance() throws Exception {
        doTestPrepareCommitReorder_2(false);
    }

    /** */
    @Test
    public void testPrepareCommitReorderFailRebalanceSkipCheckpoint() throws Exception {
        doTestPrepareCommitReorder_2(true);
    }

    /**
     * Tests reproduces the problem: if partition was marked for clear on rebalance, it should never be selected for
     * historical rebalance or only partial data defined by counter range will be loaded.
     *
     * @param skipCheckpoint Skip checkpoint.
     *
     * @throws Exception
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpoint) throws Exception {
        T2<Ignite, List<Ignite>> txTop = runTest(skipCheckpoint).get(PARTITION_ID);

        waitForTopology(SERVERS_CNT);

        awaitPartitionMapExchange();

        IgniteEx client = grid(CLIENT_GRID_NAME);

        assertEquals("Primary has not all committed transactions", TOTAL, client.cache(DEFAULT_CACHE_NAME).size());

        TestRecordingCommunicationSpi.stopBlockAll();

        String primaryName = txTop.get1().name();
        String backupName = txTop.get2().get(0).name();

        IgniteEx primary = startGrid(primaryName);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        // Check if gaps are closed on rebalance.
        PartitionUpdateCounter cntr = counter(PARTITION_ID, primary.name());

        assertNotNull(cntr);

        assertTrue(cntr.sequential());

        assertEquals(TOTAL, cntr.get());

        stopGrid(backupName);

        awaitPartitionMapExchange();

        assertNotNull(cntr = counter(PARTITION_ID, primary.name()));

        assertEquals(TOTAL, cntr.reserved());

        // Make update to advance a counter.
        int addCnt = 10;

        loadDataToPartition(PARTITION_ID, primaryName, DEFAULT_CACHE_NAME, addCnt, TOTAL);

        // TODO https://issues.apache.org/jira/browse/IGNITE-11607
        // Historical rebalance is not possible from history containing rebalanced entries.
        // Next rebalance will be full.
        IgniteEx grid0 = startGrid(backupName);

        awaitPartitionMapExchange();

        assertNotNull(cntr = counter(PARTITION_ID, grid0.name()));

        assertEquals(TOTAL + addCnt, cntr.get());

        assertEquals(TOTAL + addCnt, cntr.reserved());

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /**
     * Adds primary restart in the middle of rebalance.
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestPrepareCommitReorder_2(boolean skipCheckpoint) throws Exception {
        Map<Integer, T2<Ignite, List<Ignite>>> txTops = runTest(skipCheckpoint);

        T2<Ignite, List<Ignite>> txTop = txTops.get(PARTITION_ID);

        String primaryName = txTop.get1().name();
        String backupName = txTop.get2().get(0).name();

        IgniteEx client = grid(CLIENT_GRID_NAME);

        waitForTopology(SERVERS_CNT);

        // If not wait for PME backup might not switch to primary.
        awaitPartitionMapExchange();

        PartitionUpdateCounter cntr2 = counter(PARTITION_ID, backupName);
        assertNotNull(cntr2);

        assertEquals(TOTAL, cntr2.get());
        assertEquals(TOTAL, cntr2.reserved());
        assertTrue(cntr2.sequential());

        assertEquals("Backup has not all committed transactions", TOTAL, client.cache(DEFAULT_CACHE_NAME).size());

        TestRecordingCommunicationSpi.stopBlockAll();

        TestRecordingCommunicationSpi backupSpi = TestRecordingCommunicationSpi.spi(grid(backupName));

        backupSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage m0 = (GridDhtPartitionSupplyMessage)msg;

                return m0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
            }

            return false;
        });

        // Restart primary during rebalance.
        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            try {
                backupSpi.waitForBlocked();
            }
            catch (InterruptedException e) {
                fail("Unexpected interruption");
            }

            stopGrid(skipCheckpoint, primaryName);

            backupSpi.stopBlock();

            try {
                startGrid(primaryName);

                awaitPartitionMapExchange();
            }
            catch (Exception e) {
                fail();
            }
        }, 1);

        // Trigger rebalance.
        IgniteEx prim = startGrid(primaryName);

        // Wait for restart.
        fut.get();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class OnePhaseCommitTxCallbackAdapter extends TxCallbackAdapter {
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
        public OnePhaseCommitTxCallbackAdapter(int[] prepOrd, int[] primCommitOrder, int[] backupCommitOrder) {
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
                if (prepFuts.size() == txCnt) { // Wait until all prep requests queued and force prepare order.
                    prepFuts.remove(version(prepOrder.poll())).onDone();
                }
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                IgniteUuid nearXidVer = primaryTx.nearXidVersion().asIgniteUuid();

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
        @Override public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx backupTx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                primFinishFuts.put(nearXidVer, proceedFut);

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
            GridFutureAdapter<?> proceedFut) {
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

    /**
     * Test scenario:
     * <p>
     * 1. Assign counters for tx0, tx1, tx2 out of order.
     * <p>
     * 2. Commit all txs on backup.
     * <p>
     * 3. Commit only tx2 on primary.
     * <p>
     * 4. Stop primary node.
     * <p>
     *
     * Pass condition: after primary start partitions are in sync.
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private Map<Integer, T2<Ignite, List<Ignite>>> runTest(boolean skipCheckpoint) throws Exception {
        return super.runOnPartition(PARTITION_ID, null, BACKUPS, SERVERS_CNT,
            new IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback>() {
                private Map<Integer, T2<Ignite, List<Ignite>>> txTop;

                @Override public TxCallback apply(Map<Integer, T2<Ignite, List<Ignite>>> map) {
                    txTop = map;

                    return new OnePhaseCommitTxCallbackAdapter(PREPARE_ORDER, PRIMARY_COMMIT_ORDER, BACKUP_COMMIT_ORDER) {
                        @Override protected boolean onPrimaryCommitted(IgniteEx primary, int idx) {
                            if (idx == PRIMARY_COMMIT_ORDER[0]) {
                                PartitionUpdateCounter cntr = counter(PARTITION_ID, primary.name());

                                assertNotNull(cntr);

                                assertEquals(TOTAL, cntr.reserved());

                                assertFalse(cntr.sequential());

                                long[] upd = cntr.iterator().next();

                                assertEquals(
                                    cntr.toString(),
                                    PRELOAD_KEYS_CNT + SIZES[PRIMARY_COMMIT_ORDER[1]] + SIZES[PRIMARY_COMMIT_ORDER[2]],
                                    upd[0]);

                                assertEquals(
                                    cntr.toString(),
                                    SIZES[PRIMARY_COMMIT_ORDER[0]],
                                    upd[1]);

                                // Check backup counter.
                                String backup = txTop.get(PARTITION_ID).get2().get(0).name();

                                PartitionUpdateCounter cntr2 = counter(PARTITION_ID, backup);
                                assertNotNull(cntr2);

                                assertFalse(primary.name().equals(backup));

                                assertEquals(TOTAL, cntr2.get());
                                assertEquals(0, cntr2.reserved());
                                assertTrue(cntr2.sequential());
                                assertEquals(TOTAL, grid(backup).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.BACKUP));

                                // Will stop primary node before all remaining commits are applied.
                                stopGrid(skipCheckpoint, primary.name());

                                return true; // Stop commit chain.
                            }

                            throw new IgniteException("Should not commit other transactions");
                        }
                    };
                }
            }, SIZES);
    }
}
