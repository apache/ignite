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
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 */
@RunWith(JUnit4.class)
public class TxPartitionCounterStateOnePrimaryOneBackupTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int[] PREPARE_ORDER = new int[] {0, 1, 2};

    /** */
    private static final int[] PRIMARY_COMMIT_ORDER = new int[] {1, 2, 0};

    /** */
    private static final int[] BACKUP_COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    private static final int [] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum();

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

    /**
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpoint) throws Exception {
        runOnPartition(PARTITION_ID, BACKUPS, NODES_CNT,
            new TwoPhasePessimisticPrimaryBackupTxCallbackAdapter(PREPARE_ORDER, PRIMARY_COMMIT_ORDER, BACKUP_COMMIT_ORDER) {
                @Override protected boolean onBackupCommitted(IgniteEx backup, int idx) {
                    super.onBackupCommitted(backup, idx);

                    if (idx == BACKUP_COMMIT_ORDER[0]) {
                        Collection<ClusterNode> nodes = backup.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(PARTITION_ID);
                        List<ClusterNode> nodesList = new ArrayList<>(nodes);

                        Ignite backupNode = Ignition.ignite(nodesList.get(1).id());

                        PartitionUpdateCounter cntr = counter(PARTITION_ID, backupNode.name());

                        assertFalse(cntr.holes().isEmpty());

                        PartitionUpdateCounter.Item gap = cntr.holes().first();

                        assertEquals(gap.start(), SIZES[BACKUP_COMMIT_ORDER[1]] + SIZES[BACKUP_COMMIT_ORDER[2]]);
                        assertEquals(gap.delta(), SIZES[BACKUP_COMMIT_ORDER[0]]);

                        stopGrid(skipCheckpoint, backupNode.name()); // Will stop backup node before all commits are applied.

                        return true;
                    }

                    throw new IgniteException("Should not commit other transactions");
                }
            }, SIZES);

        waitForTopology(2);

        IgniteEx client = grid("client");

        assertEquals("Primary has not all committed transactions", TOTAL, client.cache(DEFAULT_CACHE_NAME).size());

        for (Ignite ignite : G.allGrids())
            TestRecordingCommunicationSpi.spi(ignite).stopBlock(false);

        IgniteEx backup = startGrid(1);

        awaitPartitionMapExchange();

        IdleVerifyResultV2 res = idleVerify(client, DEFAULT_CACHE_NAME);

        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }

        // Check if holes are closed on rebalance.
        PartitionUpdateCounter cntr = counter(PARTITION_ID, backup.name());

        assertTrue(cntr.holes().isEmpty());

        assertEquals(TOTAL, cntr.get());

        stopGrid(0);

        awaitPartitionMapExchange();

        cntr = counter(PARTITION_ID, backup.name());

        assertEquals(TOTAL, cntr.reserved());
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class TwoPhasePessimisticPrimaryBackupTxCallbackAdapter extends TxCallbackAdapter {
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

        /**
         * @param prepOrd Prepare order.
         * @param primCommitOrder Commit order.
         */
        public TwoPhasePessimisticPrimaryBackupTxCallbackAdapter(int[] prepOrd, int[] primCommitOrder,
            int[] backupCommitOrder) {
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
        protected boolean onPrepared(IgniteEx primary, IgniteInternalTx tx, int idx) {
            log.info("TX: prepared on primary " + idx + ", tx=" + CU.txString(tx));

            return false;
        }

        /** */
        protected void onAllPrimaryPrepared() {
            log.info("TX: all primary prepared");
        }

        /**
         * @param primary Primary node.
         * @param idx Index.
         */
        protected boolean onPrimaryCommitted(IgniteEx primary, int idx) {
            log.info("TX: primary committed " + idx);

            return false;
        }

        /**
         * @param backup Backup node.
         * @param idx Index.
         */
        protected boolean onBackupCommitted(IgniteEx backup, int idx) {
            log.info("TX: backup committed " + idx);

            return false;
        }

        /** */
        protected void onAllPrimaryCommited() {
            log.info("TX: all primary committed");
        }

        /** */
        protected void onAllBackupCommited() {
            log.info("TX: all backup committed");
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx node, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                prepFuts.put(nearXidVer, proceedFut);

                // Order prepares.
                if (prepFuts.size() == prepOrder.size()) {// Wait until all prep requests queued and force prepare order.
                    prepFuts.remove(version(prepOrder.poll())).onDone();
                }
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryPrepare(IgniteEx from, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onPrepared(from, tx, order(tx.nearXidVersion().asGridUuid())))
                    return;

                if (prepOrder.isEmpty()) {
                    onAllPrimaryPrepared();

                    return;
                }

                prepFuts.remove(version(prepOrder.poll())).onDone();
            });

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryFinish(IgniteEx primaryNode, IgniteInternalTx tx, GridFutureAdapter<?>
            proceedFut) {
            runAsync(() -> {
                primFinishFuts.put(tx.nearXidVersion().asGridUuid(), proceedFut);

                // Order prepares.
                if (primFinishFuts.size() == 3)
                    primFinishFuts.remove(version(primCommitOrder.poll())).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupFinish(IgniteEx prim, IgniteEx backup, @Nullable IgniteInternalTx primTx,
            IgniteInternalTx backupTx, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
            runAsync(() -> {
                backupFinishFuts.put(nearXidVer, fut);

                if (onPrimaryCommitted(prim, order(nearXidVer)))
                    return;

                if (primCommitOrder.isEmpty() && backupFinishFuts.size() == SIZES.length) {
                    onAllPrimaryCommited();

                    assertEquals(SIZES.length, backupFinishFuts.size());

                    backupFinishFuts.remove(version(backupCommitOrder.poll())).onDone();

                    return;
                }

                primFinishFuts.remove(version(primCommitOrder.poll())).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupFinish(IgniteEx n, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onBackupCommitted(n, order(nearXidVer)))
                    return;

                if (backupCommitOrder.isEmpty()) {
                    onAllBackupCommited();

                    return;
                }

                backupFinishFuts.remove(version(backupCommitOrder.poll())).onDone();
            });

            return false;
        }
    }
}
