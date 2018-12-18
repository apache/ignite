package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test partition update counter generation on one primary node and near client node.
 */
@RunWith(JUnit4.class)
public class TxPartitionCounterStateAfterRecoveryOnePrimaryTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int[] PREPARE_ORDER = new int[] {0, 1, 2};

    /** */
    private static final int[] COMMIT_ORDER = new int[] {2, 1, 0};

    /** */
    private static final int [] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum();

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 0;

    /** */
    private static final int NODES_CNT = 1;

    /** */
    @Test
    public void testPrepareCommitReorder() throws Exception {
        doTestPrepareCommitReorder(false, false);
    }

    /** */
    @Test
    public void testPrepareCommitReorder2() throws Exception {
        doTestPrepareCommitReorder(true, false);
    }

    /** */
    @Test
    public void testPrepareCommitReorderCheckpointBetweenCommits() throws Exception {
        doTestPrepareCommitReorder(false, true);
    }

    /** */
    @Test
    public void testPrimaryPrepareCommitReorderNoStopCheckpoint2() throws Exception {
        doTestPrepareCommitReorder(true, true);
    }

    /** */
    @Test
    public void testSkipReservedCountersAfterRecovery() throws Exception {
        doTestSkipReservedCountersAfterRecovery(false);
    }

    /** */
    @Test
    public void testSkipReservedCountersAfterRecovery2() throws Exception {
        doTestSkipReservedCountersAfterRecovery(true);
    }

    /** */
    @Test
    public void testMissedCommitsAfterRecovery() throws Exception {
        doTestPrepareCommitReorder2(false);
    }

    /** */
    @Test
    public void testMissedCommitsAfterRecovery2() throws Exception {
        doTestPrepareCommitReorder2(true);
    }

    /**
     * Tests when counter reserved on prepare should never be applied after recovery.
     *
     * @throws Exception
     */
    private void doTestSkipReservedCountersAfterRecovery(boolean skipCheckpointOnStop) throws Exception {
        runOnPartition(PARTITION_ID, -1, BACKUPS, NODES_CNT, new IgniteClosure2X<Ignite, List<Ignite>, TxCallback>() {
            @Override public TxCallback applyx(Ignite ignite,
                List<Ignite> ignites) throws IgniteCheckedException {
                return new PrimaryTxCallbackAdapter(PREPARE_ORDER, COMMIT_ORDER) {
                    @Override protected void onAllPrepared() {
                        stopGrid(skipCheckpointOnStop, grid(0).name());
                    }
                };
            }
        }, SIZES);

        stopGrid("client");

        startGrid(0);

        PartitionUpdateCounter cntr = counter(PARTITION_ID);

        assertEquals(0, cntr.get());
        assertEquals(0, cntr.hwm());
    }

    /**
     * Test correct update counter processing on updates reorder and node restart.
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpointOnStop,
        boolean doCheckpoint) throws Exception {
        runOnPartition(PARTITION_ID, -1, BACKUPS, NODES_CNT, new IgniteClosure2X<Ignite, List<Ignite>, TxCallback>() {
            @Override public TxCallback applyx(Ignite primary,
                List<Ignite> backups) throws IgniteCheckedException {
                return new PrimaryTxCallbackAdapter(PREPARE_ORDER, COMMIT_ORDER) {
                    @Override protected boolean onCommitted(IgniteEx node, int idx) {
                        if (idx == COMMIT_ORDER[0] && doCheckpoint) {
                            try {
                                forceCheckpoint(grid(0));
                            }
                            catch (IgniteCheckedException e) {
                                fail();
                            }
                        }

                        return super.onCommitted(node, idx);
                    }
                };
            }
        }, SIZES);

        waitForTopology(2);

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
        runOnPartition(PARTITION_ID, -1, BACKUPS, NODES_CNT, new IgniteClosure2X<Ignite, List<Ignite>, TxCallback>() {
            @Override public TxCallback applyx(Ignite ignite,
                List<Ignite> ignites) throws IgniteCheckedException {
                return new PrimaryTxCallbackAdapter(PREPARE_ORDER, COMMIT_ORDER) {
                    @Override protected boolean onCommitted(IgniteEx node, int idx) {
                        super.onCommitted(node, idx);

                        // After reordered commit partition update counter must contain single hole corresponding to committed tx.
                        PartitionUpdateCounter cntr = counter(PARTITION_ID);

                        assertFalse(cntr.holes().isEmpty());

                        PartitionUpdateCounter.Item gap = cntr.holes().first();

                        assertEquals(gap.start(), SIZES[COMMIT_ORDER[1]] + SIZES[COMMIT_ORDER[2]]);
                        assertEquals(gap.delta(), SIZES[COMMIT_ORDER[0]]);

                        if (idx == COMMIT_ORDER[0]) {
                            stopGrid(skipCheckpointOnStop, grid(0).name());

                            return true; // Stop further processing.
                        }

                        return false;
                    }
                };
            }
        }, SIZES);

        stopGrid("client");

        IgniteEx ex = startGrid(0);

        int size = ex.cache(DEFAULT_CACHE_NAME).size();

        assertEquals(SIZES[COMMIT_ORDER[0]], size);

        // Only one transaction is applied with counter: 12-15
        PartitionUpdateCounter cntr = counter(PARTITION_ID);

        try {
            assertEquals(0, cntr.get());
            assertEquals(TOTAL, cntr.hwm());

            assertEquals(1, cntr.holes().size());

            PartitionUpdateCounter.Item hole = cntr.holes().first();

            assertEquals(SIZES[0] + SIZES[1], hole.start());
            assertEquals(SIZES[2], hole.delta());
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class PrimaryTxCallbackAdapter extends TxCallbackAdapter {
        /** */
        private Queue<Integer> prepOrder;

        /** */
        private Queue<Integer> commitOrder;

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> prepFuts = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> finishFuts = new ConcurrentHashMap<>();

        /**
         * @param prepOrd Prepare order.
         * @param commitOrd Commit order.
         */
        public PrimaryTxCallbackAdapter(int[] prepOrd, int[] commitOrd) {
            prepOrder = new ConcurrentLinkedQueue<>();

            for (int aPrepOrd : prepOrd)
                prepOrder.add(aPrepOrd);

            commitOrder = new ConcurrentLinkedQueue<>();

            for (int aCommitOrd : commitOrd)
                commitOrder.add(aCommitOrd);
        }

        /** */
        protected boolean onPrepared(IgniteEx from, IgniteInternalTx tx, int idx) {
            log.info("TX: prepared " + idx + ", tx=" + CU.txString(tx));

            return false;
        }

        /** */
        protected void onAllPrepared() {
            log.info("TX: all prepared");
        }

        /**
         * @param primaryNode Primary node.
         * @param idx Index.
         */
        protected boolean onCommitted(IgniteEx primaryNode, int idx) {
            log.info("TX: committed " + idx);

            return false;
        }

        /** */
        protected void onAllCommited() {
            log.info("TX: all committed");
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
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
        @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onPrepared(primary, tx, order(tx.nearXidVersion().asGridUuid())))
                    return;

                if (prepOrder.isEmpty()) {
                    onAllPrepared();

                    return;
                }

                prepFuts.remove(version(prepOrder.poll())).onDone();
            });

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?>
            proceedFut) {
            runAsync(() -> {
                finishFuts.put(tx.nearXidVersion().asGridUuid(), proceedFut);

                // Order prepares.
                if (finishFuts.size() == 3)
                    finishFuts.remove(version(commitOrder.poll())).onDone();

            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                if (onCommitted(primary, order(nearXidVer)))
                    return;

                if (commitOrder.isEmpty()) {
                    onAllCommited();

                    return;
                }

                finishFuts.remove(version(commitOrder.poll())).onDone();
            });

            return false;
        }
    }
}
