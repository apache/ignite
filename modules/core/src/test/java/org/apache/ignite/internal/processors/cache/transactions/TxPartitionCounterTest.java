package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 */
public class TxPartitionCounterTest extends TxSinglePartitionAbstractTest {
    public void test() throws Exception {
        Map<Integer, IgniteUuid> txMap = new ConcurrentHashMap<>();

        Map<IgniteUuid, GridFutureAdapter<?>> futs = new ConcurrentHashMap<>();

        runOnPartition(0, 2, 3, new TxCallback() {
            @Override public boolean onBeforePrimaryPrepare(IgniteEx node, IgniteUuid ver,
                GridFutureAdapter<?> proceedFut) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        futs.put(ver, proceedFut);

                        // Order prepares.
                        if (futs.size() == 3) {
                            GridFutureAdapter<?> fut = futs.get(txMap.get(2));
                            fut.onDone();
                        }
                    }
                });

                return true;
            }

            @Override public boolean onAfterPrimaryPrepare(IgniteEx node, IgniteInternalTx tx,
                GridFutureAdapter<?> proceedFut) {

                runAsync(new Runnable() {
                    @Override public void run() {
                        int size = futs.size();

                        assertEquals(txMap.get(size - 1), tx.nearXidVersion().asGridUuid());
                        futs.remove(txMap.get(size - 1));
                        if (size - 2 >= 0)
                            futs.get(txMap.get(size - 2)).onDone();
                    }
                });

                return false;
            }

            @Override public boolean onBeforePrimaryFinish(IgniteEx n, IgniteInternalTx tx, GridFutureAdapter<?>
                proceedFut) {
                return false;
            }

            @Override public void onTxStart(Transaction tx, int idx) {
                txMap.put(idx, tx.xid());
            }
        }, 5, 7, 3);

        int size = grid("client").cache(DEFAULT_CACHE_NAME).size();

        assertEquals(15, size);
    }
}
