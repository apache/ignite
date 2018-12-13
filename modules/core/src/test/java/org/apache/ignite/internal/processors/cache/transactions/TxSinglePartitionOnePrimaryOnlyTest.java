package org.apache.ignite.internal.processors.cache.transactions;

import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
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
    public void testPrimaryPrepareCommitReorder3TxsNoCheckpointBeforeCommit() throws Exception {
        doTestPrimaryPrepareCommitReorder3Txs(false, null);
    }

    /** */
    public void testPrimaryPrepareCommitReorder3TxsSkipCheckpointOnNodeStopNoCheckpointBeforeCommit() throws Exception {
        doTestPrimaryPrepareCommitReorder3Txs(true, null);
    }

    /** */
    public void testPrimaryPrepareCommitReorder3TxsNoCheckpointBeforeCommitCheckpointAfterCommit() throws Exception {
        doTestPrimaryPrepareCommitReorder3Txs(false, new DoCheckpointClosure(2, 0));
    }

    /** */
    public void testPrimaryPrepareCommitReorder3TxsSkipCheckpointOnNodeStopCheckpointAfterCommit() throws Exception {
        doTestPrimaryPrepareCommitReorder3Txs(true, new DoCheckpointClosure(2, 0));
    }

    /** */
    private void doTestPrimaryPrepareCommitReorder3Txs(boolean skipCheckpointOnStop, @Nullable IgniteInClosure<Integer> commitClo) throws Exception {
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
