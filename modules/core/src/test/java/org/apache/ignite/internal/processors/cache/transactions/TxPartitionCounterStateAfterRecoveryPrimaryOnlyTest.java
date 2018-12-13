package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Test partition update counter generation only on primary node.
 */
public class TxPartitionCounterStateAfterRecoveryPrimaryOnlyTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int[] PREPARE_ORDER = new int[] {0, 1, 2};

    /** */
    private static final int[] COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    public static final int [] SIZES = new int[] {5, 7, 3};

    /** */
    public static final int TOTAL = IntStream.of(SIZES).sum();

    /** */
    public static final int PARTITION_ID = 0;

    /** */
    public void testPrepareCommitReorder() throws Exception {
        doTestPrepareCommitReorder(false, false);
    }

    /** */
    public void testPrepareCommitReorder2() throws Exception {
        doTestPrepareCommitReorder(true, false);
    }

    /** */
    public void testPrepareCommitReorderCheckpointBetweenCommits() throws Exception {
        doTestPrepareCommitReorder(false, true);
    }

    /** */
    public void testPrimaryPrepareCommitReorderNoStopCheckpoint2() throws Exception {
        doTestPrepareCommitReorder(true, true);
    }

    /** */
    public void testSkipReservedCountersAfterRecovery() throws Exception {
        doTestSkipReservedCountersAfterRecovery(false);
    }

    /** */
    public void testSkipReservedCountersAfterRecovery2() throws Exception {
        doTestSkipReservedCountersAfterRecovery(true);
    }

    /** */
    public void testMissedCommitsAfterRecovery() throws Exception {
        doTestPrepareCommitReorder2(false);
    }

    /** */
    public void testMissedCommitsAfterRecovery2() throws Exception {
        doTestPrepareCommitReorder2(true);
    }

    /**
     * Tests when counter reserved on prepare should never be applied after recovery.
     *
     * @throws Exception
     */
    private void doTestSkipReservedCountersAfterRecovery(boolean skipCheckpointOnStop) throws Exception {
        int[] prepOrd = new int[] {1, 0};
        int[] sizes = new int[] {3, 7};

        // For readability.
        int partId = 0;
        int backups = 0;
        int nodes = 1;

        runOnPartition(partId, backups, nodes, new PrimaryOrderingTxCallbackAdapter(prepOrd, new int[0]) {
            @Override protected void onAllPrepared() {
                new StopNodeClosure(-1, skipCheckpointOnStop, 0).apply(-1);
            }
        }, sizes);

        stopGrid("client");

        startGrid(0);

        PartitionUpdateCounter cntr = counter(partId);

        assertEquals(0, cntr.get());
        assertEquals(0, cntr.hwm());
    }

    /**
     * Test correct update counter processing on updates reorder and node restart.
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpointOnStop,
        boolean doCheckpoint) throws Exception {
        runOnPartition(PARTITION_ID, 0, 1, new PrimaryOrderingTxCallbackAdapter(PREPARE_ORDER, COMMIT_ORDER) {
            @Override protected boolean onCommitted(IgniteEx node, int idx) {
                if (idx == 2 && doCheckpoint) {
                    try {
                        forceCheckpoint(grid(0));
                    }
                    catch (IgniteCheckedException e) {
                        fail();
                    }
                }

                return super.onCommitted(node, idx);
            }
        }, SIZES);

        int size = grid("client").cache(DEFAULT_CACHE_NAME).size();

        assertEquals(TOTAL, size);

        PartitionUpdateCounter cntr = counter(PARTITION_ID);

        assertEquals(TOTAL, cntr.get());
        assertEquals(TOTAL, cntr.hwm());

        grid("client").close();

        if (skipCheckpointOnStop) {
            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)grid(0).context().cache().context().database();

            db.enableCheckpoints(false);
        }

        stopGrid(0, skipCheckpointOnStop);

        startGrid(0);

        cntr = counter(PARTITION_ID);

        assertEquals(TOTAL, cntr.get());
        assertEquals(TOTAL, cntr.hwm());
    }

    /**
     * Test correct update counter processing on updates reorder and node restart.
     */
    private void doTestPrepareCommitReorder2(boolean skipCheckpointOnStop) throws Exception {
        runOnPartition(PARTITION_ID, 0, 1, new PrimaryOrderingTxCallbackAdapter(PREPARE_ORDER, COMMIT_ORDER) {
            @Override protected boolean onCommitted(IgniteEx node, int idx) {
                if (idx == COMMIT_ORDER[0]) {
                    IgniteEx grid = grid(0);

                    if (skipCheckpointOnStop) {
                        GridCacheDatabaseSharedManager db =
                            (GridCacheDatabaseSharedManager)grid.context().cache().context().database();

                        db.enableCheckpoints(false);
                    }

                    stopGrid(0, skipCheckpointOnStop);

                    return true; // Stop further processing.
                }

                return false;
            }
        }, SIZES);

        stopGrid("client");

        IgniteEx ex = startGrid(0);

        int size = ex.cache(DEFAULT_CACHE_NAME).size();

        assertEquals(SIZES[COMMIT_ORDER[0]], size);

        PartitionUpdateCounter cntr = counter(PARTITION_ID);

        assertEquals(0, cntr.get());
        assertEquals(0, cntr.hwm());
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class PrimaryOrderingTxCallbackAdapter extends TxCallbackAdapter {
        /** */
        private Queue<Integer> prepOrder;

        /** */
        private Queue<Integer> commitOrder;

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> futs = new ConcurrentHashMap<>();

        /**
         * @param prepOrd Prepare order.
         * @param commitOrd Commit order.
         */
        public PrimaryOrderingTxCallbackAdapter(int[] prepOrd, int[] commitOrd) {
            prepOrder = new ConcurrentLinkedQueue<>();

            for (int aPrepOrd : prepOrd)
                prepOrder.add(aPrepOrd);

            commitOrder = new ConcurrentLinkedQueue<>();

            for (int aCommitOrd : commitOrd)
                commitOrder.add(aCommitOrd);
        }

        @Override public boolean beforePrimaryPrepare(IgniteEx node, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                futs.put(nearXidVer, proceedFut);

                // Order prepares.
                if (futs.size() == prepOrder.size()) {// Wait until all prep requests queued and force prepare order.
                    futs.remove(version(prepOrder.poll())).onDone();
                }
            });

            return true;
        }

        protected boolean onPrepared(IgniteEx from, int idx) {
            return false;
        }

        protected void onAllPrepared() {
            // No-op.
        }

        protected boolean onCommitted(IgniteEx primaryNode, int idx) {
            return false;
        }

        protected void onAllCommited() {
            // No-op.
        }

        @Override public boolean afterPrimaryPrepare(IgniteEx from, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onPrepared(from, order(tx.nearXidVersion().asGridUuid())) || prepOrder.isEmpty())
                    return;

                futs.remove(version(prepOrder.poll())).onDone();

                if (prepOrder.isEmpty())
                    onAllPrepared();
            });

            return false;
        }

        @Override public boolean beforePrimaryFinish(IgniteEx primaryNode, IgniteInternalTx tx, GridFutureAdapter<?>
            proceedFut) {
            runAsync(() -> {
                futs.put(tx.nearXidVersion().asGridUuid(), proceedFut);

                // Order prepares.
                if (futs.size() == 3)
                    futs.remove(version(commitOrder.poll())).onDone();

            });

            return true;
        }

        @Override public boolean afterPrimaryFinish(IgniteEx primaryNode, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                if (onCommitted(primaryNode, order(nearXidVer)) || commitOrder.isEmpty())
                    return;

                futs.remove(version(commitOrder.poll())).onDone();

                if (commitOrder.isEmpty())
                    onAllCommited();
            });

            return false;
        }
    }
}
