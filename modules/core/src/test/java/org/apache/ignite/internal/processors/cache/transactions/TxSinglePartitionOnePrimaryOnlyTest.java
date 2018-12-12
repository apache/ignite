package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 */
public class TxSinglePartitionOnePrimaryOnlyTest extends TxSinglePartitionAbstractTest {
    /** Futures tracking map. */
    private Queue<IgniteInternalFuture<?>> taskFuts = new ConcurrentLinkedQueue<>();

    /** */
    public void testPrimaryPrepareCommitReorder() throws Exception {


        Map<IgniteUuid, GridFutureAdapter<?>> futs = new ConcurrentHashMap<>();

        int[] prepOrd = new int[] {1, 2, 0};
        int[] commitOrd = new int[] {2, 1, 0};
        int[] sizes = new int[] {5, 7, 3};
        int total = IntStream.of(sizes).sum();

        Queue<Integer> prepOrder = new ConcurrentLinkedQueue<>();
        for (int aPrepOrd : prepOrd)
            prepOrder.add(aPrepOrd);

        Queue<Integer> commitOrder = new ConcurrentLinkedQueue<>();
        for (int aCommitOrd : commitOrd)
            commitOrder.add(aCommitOrd);

        int partId = 0;
        int backups = 0;
        int nodes = 1;

        runOnPartition(partId, backups, nodes, new TxCallbackAdapter() {
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

            @Override public boolean afterPrimaryPrepare(IgniteEx from, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
                runAsync(() -> {
                    if (prepOrder.isEmpty())
                        return;

                    futs.remove(version(prepOrder.poll())).onDone();

                    if (prepOrder.isEmpty()) {
                        GridDhtLocalPartition part = internalCache(0).context().topology().localPartition(0);
                        PartitionUpdateCounter cntr = part.dataStore().partUpdateCounter();

                        int i = 0;
                        for (PartitionUpdateCounter.Item item : cntr.holes()) {
                            assertEquals(sizes[prepOrd[i]], item.delta());

                            i++;
                        }
                    }
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
                    if (commitOrder.isEmpty())
                        return;

                    futs.remove(version(commitOrder.poll())).onDone();
                });

                return false;
            }
        }, sizes);

        int size = grid("client").cache(DEFAULT_CACHE_NAME).size();

        assertEquals(total, size);

        @Nullable GridDhtLocalPartition part = internalCache(0).context().topology().localPartition(0);
        PartitionUpdateCounter cntr = part.dataStore().partUpdateCounter();

        assertEquals(total, cntr.get());
        assertEquals(total, cntr.hwm());

        // Check futures are executed without errors.
        for (IgniteInternalFuture<?> fut : taskFuts)
            fut.get();
    }

    /**
     * @param r Runnable.
     */
    public void runAsync(Runnable r) {
        IgniteInternalFuture fut = GridTestUtils.runAsync(r);

        taskFuts.add(fut);
    }
}
