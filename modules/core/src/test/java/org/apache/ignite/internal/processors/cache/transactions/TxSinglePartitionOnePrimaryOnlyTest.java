package org.apache.ignite.internal.processors.cache.transactions;

import java.util.stream.IntStream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Test partition update counter generation only on primary node.
 */
public class TxSinglePartitionOnePrimaryOnlyTest extends TxSinglePartitionAbstractTest {
    /** */
    public void testPrepareCommitReorder() throws Exception {
        doTestPrepareCommitReorder(false, null);
    }

    /** */
    public void testPrepareCommitReorder2() throws Exception {
        doTestPrepareCommitReorder(true, null);
    }

    /** */
    public void testPrepareCommitReorderCheckpointBetweenCommits() throws Exception {
        doTestPrepareCommitReorder(false, new DoCheckpointClosure(2, 0));
    }

    /** */
    public void testPrimaryPrepareCommitReorderNoStopCheckpoint2() throws Exception {
        doTestPrepareCommitReorder(true, new DoCheckpointClosure(2, 0));
    }

    /** */
    public void testSkipReservedCountersAfterRecovery() throws Exception {
        doTestSkipReservedCountersAfterRecovery(false);
    }

    /** */
    public void testSkipReservedCountersAfterRecovery2() throws Exception {
        doTestSkipReservedCountersAfterRecovery(true);
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

        runOnPartition(partId, backups, nodes, new OrderingTxCallbackAdapter(prepOrd, new int[0]) {
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
        @Nullable IgniteInClosure<Integer> commitClo) throws Exception {
        int[] prepOrd = new int[] {1, 2, 0};
        int[] commitOrd = new int[] {2, 1, 0};
        int[] sizes = new int[] {5, 7, 3};
        int total = IntStream.of(sizes).sum();

        // For readability.
        int partId = 0;
        int backups = 0;
        int nodes = 1;

        runOnPartition(partId, backups, nodes, new OrderingTxCallbackAdapter(prepOrd, commitOrd) {
            @Override protected void onPrepared(IgniteEx node, int idx) {
                log.info("TX: Prepared [node=" + node.name() + ", order=" + idx + ", cntr=" + counter(partId));
            }

            @Override protected void onAllPrepared() {
                PartitionUpdateCounter cntr = counter(partId);

                int i = 0;
                for (PartitionUpdateCounter.Item item : cntr.holes()) {
                    assertEquals(sizes[prepOrd[i]], item.delta());

                    i++;
                }
            }

            @Override protected void onCommitted(IgniteEx node, int idx) {
                if (commitClo != null)
                    commitClo.apply(idx);

                log.info("TX: Committed [node=" + node.name() + ", order=" + idx + ", cntr=" + counter(partId));
            }

            @Override protected void onAllCommited() {

            }
        }, sizes);

        int size = grid("client").cache(DEFAULT_CACHE_NAME).size();

        assertEquals(total, size);

        PartitionUpdateCounter cntr = counter(partId);

        assertEquals(total, cntr.get());
        assertEquals(total, cntr.hwm());

        grid("client").close();

        if (skipCheckpointOnStop) {
            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)grid(0).context().cache().context().database();

            db.enableCheckpoints(false);
        }

        stopGrid(0, skipCheckpointOnStop);

        startGrid(0);

        cntr = counter(partId);

        assertEquals(total, cntr.get());
        assertEquals(total, cntr.hwm());
    }
}
