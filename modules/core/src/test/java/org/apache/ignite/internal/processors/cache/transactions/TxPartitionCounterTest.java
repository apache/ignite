package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 */
public class TxPartitionCounterTest extends TxSinglePartitionAbstractTest {
    /** Futures tracking map. */
    private Queue<IgniteInternalFuture<?>> taskFuts = new ConcurrentLinkedQueue<>();

    public void test() throws Exception {
        Map<Integer, IgniteUuid> txMap = new ConcurrentHashMap<>();

        Map<IgniteUuid, GridFutureAdapter<?>> futs = new ConcurrentHashMap<>();

        int[] prepOrd = new int[] {1, 2, 0};
        int[] commitOrd = new int[] {2, 1, 0};
        int[] sizes = new int[] {5, 7, 3};
        int total = IntStream.of(sizes).sum();

        Queue<Integer> prepOrder = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < prepOrd.length; i++)
            prepOrder.add(prepOrd[i]);

        Queue<Integer> commitOrder = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < commitOrd.length; i++)
            commitOrder.add(commitOrd[i]);

        int partId = 0;
        int backups = 2;
        int nodes = 2;

        runOnPartition(partId, backups, nodes, new TxCallback() {
            @Override public boolean onBeforePrimaryPrepare(IgniteEx node, IgniteUuid ver,
                GridFutureAdapter<?> proceedFut) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        futs.put(ver, proceedFut);

                        // Order prepares.
                        if (futs.size() == prepOrder.size()) {// Wait until all prep requests queued and force prepare order.
                            futs.remove(txMap.get(prepOrder.poll())).onDone();
                        }
                    }
                });

                return true;
            }

            @Override public boolean onAfterPrimaryPrepare(IgniteEx node, IgniteInternalTx tx,
                GridFutureAdapter<?> proceedFut) {

                runAsync(new Runnable() {
                    @Override public void run() {
                        if (prepOrder.isEmpty())
                            return;

                        futs.remove(txMap.get(prepOrder.poll())).onDone();

                        if (prepOrder.isEmpty()) {
                            GridDhtLocalPartition part = internalCache(0).context().topology().localPartition(0);
                            PartitionUpdateCounter cntr = part.dataStore().partUpdateCounter();

                            int i = 0;
                            for (PartitionUpdateCounter.Item item : cntr.holes()) {
                                assertEquals(sizes[prepOrd[i]], item.delta());

                                i++;
                            }
                        }
                    }
                });

                return false;
            }

            @Override public boolean onBeforePrimaryFinish(IgniteEx n, IgniteInternalTx tx, GridFutureAdapter<?>
                proceedFut) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        futs.put(tx.nearXidVersion().asGridUuid(), proceedFut);

                        // Order prepares.
                        if (futs.size() == 3)
                            futs.remove(txMap.get(commitOrder.poll())).onDone();

                    }
                });

                return true;
            }

            @Override public boolean onAfterPrimaryFinish(IgniteEx n, IgniteUuid ver, GridFutureAdapter<?> proceedFut) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        if (commitOrder.isEmpty())
                            return;

                        futs.remove(txMap.get(commitOrder.poll())).onDone();
                    }
                });

                return false;
            }

            @Override public void onTxStart(Transaction tx, int idx) {
                txMap.put(idx, tx.xid());
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
