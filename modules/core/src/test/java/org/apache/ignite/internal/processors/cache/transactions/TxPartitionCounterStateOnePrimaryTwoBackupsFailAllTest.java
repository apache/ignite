package org.apache.ignite.internal.processors.cache.transactions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.stream.Collectors.toCollection;

/**
 */
@RunWith(JUnit4.class)
public class TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 2;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommitSameTxSizes() throws Exception {
        doTestPrepareCommitReorder(false, new int[] {0, 1}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, new int[] {5, 5});
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommitDifferentTxSizes() throws Exception {
        doTestPrepareCommitReorder(false, new int[] {0, 1}, new int[] {0, 1}, new int[] {0, 1}, new int[] {1, 0}, new int[] {8, 5});
    }

    /** */
    @Test
    public void testStopAllOwnersWithPartialCommitSameTxSizes2() throws Exception {
        doTestPrepareCommitReorder2(false, new int[] {0, 1, 2}, new int[] {0, 1, 2}, new int[] {1, 2, 0}, new int[] {2, 1, 0}, new int[] {5, 7, 3});
    }

    /**
     * Test scenario:
     *
     */
    private void doTestPrepareCommitReorder(boolean skipCp, int[] prepareOrder, int[] primCommitOrder, int[] backup1CommitOrder, int[] backup2CommitOrder, int[] sizes) throws Exception {
        AtomicInteger cnt = new AtomicInteger();

        Map<IgniteEx, int[]> commits = new HashMap<IgniteEx, int[]>();

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

                    return new TPCCommitTxCallbackAdapter(prepares, commits, sizes.length) {
                        @Override protected boolean onBackupCommitted(IgniteEx backup, int idx) {
                            super.onBackupCommitted(backup, idx);

                            if (cnt.incrementAndGet() == 2) {
                                // Stop all backups first or recovery will commit a transaction on backups.
                                stopGrid(skipCp, txTop.get2().get(0).name());
                                stopGrid(skipCp, txTop.get2().get(1).name());
                                stopAllGrids();
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

        PartitionUpdateCounter cntr = counter(PARTITION_ID, backupNode.name());

        assertTrue(cntr.gaps().isEmpty());
    }

    /**
     * Test scenario:
     *
     */
    private void doTestPrepareCommitReorder2(boolean skipCp, int[] prepareOrder, int[] primCommitOrder, int[] backup1CommitOrder, int[] backup2CommitOrder, int[] sizes) throws Exception {
        Map<IgniteEx, int[]> commits = new HashMap<IgniteEx, int[]>();
        Map<IgniteEx, CountDownLatch> awaits = new ConcurrentHashMap<>();

        AtomicBoolean finish = new AtomicBoolean();

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

                    awaits.put((IgniteEx)backup1, new CountDownLatch(2));
                    awaits.put((IgniteEx)backup2, new CountDownLatch(2));

                    return new TPCCommitTxCallbackAdapter(prepares, commits, sizes.length) {
                        @Override protected boolean onBackupCommitted(IgniteEx backup, int idx) {
                            super.onBackupCommitted(backup, idx);

                            CountDownLatch latch = awaits.get(backup);
                            latch.countDown();
                            if (latch.getCount() == 0) {
                                for (CountDownLatch countDownLatch : awaits.values()) {
                                    try {
                                        assertTrue(U.await(countDownLatch, 30_000, TimeUnit.MILLISECONDS));
                                    }
                                    catch (IgniteInterruptedCheckedException e) {
                                        fail();
                                    }
                                }

                                if (finish.compareAndSet(false, true)) {
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

        n2.cluster().active(true);

        // All nodes should be stopped by failure handler due to partition update counter inconsistency.
        waitForTopology(0);
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class TPCCommitTxCallbackAdapter extends TxCallbackAdapter {
        /** */
        private Map<T3<IgniteEx /** Node */, TxState /** State */, IgniteUuid /** Near xid */ >, GridFutureAdapter<?>>
            futures = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteEx, Queue<Integer>> prepares = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteEx, Queue<Integer>> commits = new ConcurrentHashMap<>();

        /** */
        private final int txCnt;

        /** */
        private Map<IgniteUuid, Boolean> allPrimaryCommitted = new HashMap<>();

        /** */
        private boolean allPrimaryCommittedFlag = false;

        /**
         * @param prepares Map of node to it's prepare order.
         * @param commits Map of node to it's commit order.
         */
        public TPCCommitTxCallbackAdapter(Map<IgniteEx, int[]> prepares, Map<IgniteEx, int[]> commits, int txCnt) {
            this.txCnt = txCnt;

            for (int[] ints : prepares.values())
                assertEquals("Wrong order of prepares", txCnt, ints.length);

            for (int[] ints : commits.values())
                assertEquals("Wrong order of commits", txCnt, ints.length);

            for (Map.Entry<IgniteEx, int[]> entry : prepares.entrySet())
                this.prepares.put(entry.getKey(),
                    IntStream.of(entry.getValue()).boxed().collect(toCollection(ConcurrentLinkedQueue::new)));

            for (Map.Entry<IgniteEx, int[]> entry : commits.entrySet()) {
                this.commits.put(entry.getKey(),
                    IntStream.of(entry.getValue()).boxed().collect(toCollection(ConcurrentLinkedQueue::new)));
            }
        }

        /** */
        protected boolean onPrepared(IgniteEx primary, IgniteInternalTx tx, int idx) {
            log.info("TX: prepared on primary [name=" + primary.name() + ", txId=" + idx + ']');

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
            log.info("TX: backup committed [name=" + backup.name() + ", id=" + backup.localNode().id() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * @param primary Primary node.
         */
        protected void onAllPrimaryCommitted(IgniteEx primary) {
            log.info("TX: all primary committed");
        }

        /**
         * @param backup Backup node.
         */
        protected void onAllBackupCommitted(IgniteEx backup) {
            log.info("TX: all backup committed: [name=" + backup.name() + ']');
        }

        /**
         * @param node Primary.
         * @param state State.
         * @return Count of futures for node.
         */
        private long countForNode(IgniteEx node, TxState state) {
            return futures.keySet().stream().filter(objects -> objects.get1() == node && objects.get2() == state).count();
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            runAsync(() -> {
                futures.put(new T3<>(primary, TxState.PREPARE, nearXidVer), proceedFut);

                // Order prepares.
                if (countForNode(primary, TxState.PREPARE) == txCnt) {// Wait until all prep requests queued and force prepare order.
                    futures.remove(new T3<>(primary, TxState.PREPARE, version(prepares.get(primary).poll()))).onDone();
                }
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onPrepared(primary, tx, order(nearXidVer)))
                    return;

                if (prepares.get(primary).isEmpty()) {
                    onAllPrimaryPrepared(primary);

                    return;
                }

                futures.remove(new T3<>(primary, TxState.PREPARE, version(prepares.get(primary).poll()))).onDone();
            });

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?>
            proceedFut) {
            runAsync(() -> {
                futures.put(new T3<>(primary, TxState.COMMIT, tx.nearXidVersion().asGridUuid()), proceedFut);

                long l = countForNode(primary, TxState.COMMIT);

                log.info("HHH: " + l);

                if (l == txCnt)
                    futures.remove(new T3<>(primary, TxState.COMMIT, version(commits.get(primary).poll()))).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
            runAsync(() -> {
                futures.put(new T3<>(backup, TxState.COMMIT, nearXidVer), fut);

                Boolean prev = allPrimaryCommitted.put(nearXidVer, Boolean.TRUE); // First finish message to backup means what tx was committed on primary.

                if (prev == null) {
                    if (onPrimaryCommitted(primary, order(nearXidVer)))
                        return;
                }

                if (countForNode(primary, TxState.COMMIT) == 0 && countForNode(backup, TxState.COMMIT) == txCnt) {
                    if (!allPrimaryCommittedFlag) {
                        onAllPrimaryCommitted(primary); // Report all primary committed once.

                        allPrimaryCommittedFlag = true;
                    }

                    // Proceed with commit to backups.
                    futures.remove(new T3<>(backup, TxState.COMMIT, version(commits.get(backup).poll()))).onDone();

                    return;
                }

                if (prev == null)
                    futures.remove(new T3<>(primary, TxState.COMMIT, version(commits.get(primary).poll()))).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            runAsync(() -> {
                if (onBackupCommitted(backup, order(nearXidVer)))
                    return;

                if (commits.get(backup).isEmpty()) {
                    onAllBackupCommitted(backup);

                    return;
                }

                futures.remove(new T3<>(backup, TxState.COMMIT, version(commits.get(backup).poll()))).onDone();
            });

            return false;
        }
    }

    /** */
    private enum TxState {
        /** Prepare. */ PREPARE,
        /** Commit. */COMMIT
    }
}
