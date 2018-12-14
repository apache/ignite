package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
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
    private static final int[] PRIMARY_COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    private static final int[] BACKUP_COMMIT_ORDER = new int[] {0, 2, 1};

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
        runOnPartition(PARTITION_ID, BACKUPS, NODES_CNT,
            new PrimaryBackupTxCallbackAdapter(PREPARE_ORDER, PRIMARY_COMMIT_ORDER, BACKUP_COMMIT_ORDER), SIZES);
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class PrimaryBackupTxCallbackAdapter extends TxCallbackAdapter {
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
        public PrimaryBackupTxCallbackAdapter(int[] prepOrd, int[] primCommitOrder, int[] backupCommitOrder) {
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
         * @param primary Backup node.
         * @param idx Index.
         */
        protected boolean onBackupCommitted(IgniteEx primary, int idx) {
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

                if (primCommitOrder.isEmpty()) {
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
