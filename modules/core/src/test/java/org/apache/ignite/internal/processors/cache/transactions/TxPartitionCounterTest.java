package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 */
public class TxPartitionCounterTest extends TxSinglePartitionAbstractTest {
    public void test() throws Exception {
        Map<Integer, IgniteUuid> txMap = new ConcurrentHashMap<>();

        Map<IgniteUuid, GridFutureAdapter<?>> futs = new ConcurrentHashMap<>();

        ConcurrentSkipListSet<Integer> prepOrder = new ConcurrentSkipListSet<>();
        prepOrder.add(0);
        prepOrder.add(1);
        prepOrder.add(2);

        ConcurrentSkipListSet<Integer> commitOrder = new ConcurrentSkipListSet<>();
        commitOrder.add(2);
        commitOrder.add(1);
        commitOrder.add(0);

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
                        if (futs.size() == 3) {// Wait until all prep requests queued and force prepare order.
                            futs.remove(txMap.get(prepOrder.pollFirst())).onDone();
                        }
                    }
                });

                return true;
            }

            @Override public boolean onAfterPrimaryPrepare(IgniteEx node, IgniteInternalTx tx,
                GridFutureAdapter<?> proceedFut) {

                runAsync(new Runnable() {
                    @Override public void run() {
                        futs.remove(txMap.get(prepOrder.pollFirst())).onDone();

                        if (prepOrder.isEmpty()) {
                            @Nullable GridDhtLocalPartition part = internalCache(0).context().topology().localPartition(0);
                            PartitionUpdateCounter cntr = part.dataStore().partUpdateCounter();

                            System.out.println();
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
                            futs.remove(txMap.get(commitOrder.pollFirst())).onDone();

                    }
                });

                return true;
            }

            @Override public boolean onAfterPrimaryFinish(IgniteEx n, IgniteUuid ver, GridFutureAdapter<?> proceedFut) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        futs.remove(txMap.get(commitOrder.pollFirst())).onDone();
                    }
                });

                return false;
            }

            @Override public void onTxStart(Transaction tx, int idx) {
                txMap.put(idx, tx.xid());
            }
        }, 5, 7, 3);

        int size = grid("client").cache(DEFAULT_CACHE_NAME).size();

        assertEquals(15, size);

        @Nullable GridDhtLocalPartition part = internalCache(0).context().topology().localPartition(0);
        PartitionUpdateCounter cntr = part.dataStore().partUpdateCounter();

        System.out.println();
    }
}
