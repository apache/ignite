package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxRemote;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 *
 */
public class IgniteTxFinisher {
    private static final boolean TPP_ENABLED = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_ENABLE_THREAD_PER_PARTITION, true);

    private static final int DEDICATED_WORKER_IDX = 0;

    private static final int REMOTE_TX_DEDICATED_WORKER_IDX = 1;

    private final IgniteLogger log;

    private final GridCacheSharedContext cctx;

    private final ConcurrentLinkedHashMap<GridCacheVersion, Long> txOrdering = new ConcurrentLinkedHashMap<GridCacheVersion, Long>();

    private final ConcurrentLinkedHashMap<Long, IgniteInternalTx> txs = new ConcurrentLinkedHashMap<>();

    private final Map<Long, List<Runnable>> delayedSendings = new LinkedHashMap<>();

    private long ordered;

    private long send;

    public IgniteTxFinisher(GridCacheSharedContext cctx) {
        this.cctx = cctx;
        this.log = cctx.logger(getClass());
    }

    public void execute(IgniteInternalTx tx, Runnable transactionOp) {
        assert tx != null;

        if (!TPP_ENABLED) {
            transactionOp.run();

            return;
        }

        boolean remoteTx = (tx instanceof GridDhtTxRemote) || (tx instanceof GridNearTxRemote);

        cctx.kernalContext().getStripedExecutorService().executeDedicated(remoteTx ? REMOTE_TX_DEDICATED_WORKER_IDX : DEDICATED_WORKER_IDX, () -> {
            GridCacheVersion txId = tx.xidVersion();

            if (!remoteTx) {
                boolean isNew = !txOrdering.containsKey(txId);

                if (!txOrdering.containsKey(txId)) {
                    txOrdering.put(txId, ordered);

                    txs.put(ordered, tx);

                    ordered++;
                }

/*
                if (isNew)
                    log.warning("New tx = " + (ordered - 1));
*/
            }

            transactionOp.run();
        });
    }

    public void send(IgniteInternalTx tx, Runnable transactionSendOp) {
        if (!TPP_ENABLED) {
            transactionSendOp.run();

            return;
        }

        GridCacheVersion txId = tx.xidVersion();

        long order = txOrdering.get(txId);

        //log.warning("Send invoked -> " + order);

        //log.warning("Try send " + order + " " + tx + ". Already sent: " + send);

        // Allowed to immediately send tx.
        if (send >= order) {
            transactionSendOp.run();

            //log.warning("Send directly -> " + order + " " + tx);
        }
        else {
            delayedSendings.computeIfAbsent(order, o -> new ArrayList<>()).add(transactionSendOp);

            //log.warning("Send delayed -> " + order + " " + tx);
        }
    }

    public void finishSend(IgniteInternalTx tx) {
        if (!TPP_ENABLED)
            return;

        GridCacheVersion txId = tx.xidVersion();

        long order = txOrdering.get(txId);

        //log.warning("Finished send invoked -> " + order);

        if (order >= send)
            if (!delayedSendings.containsKey(order))
                delayedSendings.put(order, new ArrayList<>());

        //log.warning("Finish send: " + order + " " + tx + " Already sent: " + send);

        if (order == send) {
            delayedSendings.remove(send);
            txs.remove(send);

            send++;

            //log.warning("Send incremented: " + send);

            while (delayedSendings.containsKey(send)) {
                //log.warning("Send delayed 2 - > " + send);

                List<Runnable> delayed = delayedSendings.remove(send);
                txs.remove(send);

                for (Runnable sendClj : delayed)
                    sendClj.run();

                send++;
            }
        }
    }

    public long order(IgniteInternalTx tx) {
        return txOrdering.get(tx.xidVersion());
    }

    public void check() {
        if (TPP_ENABLED) {
            if (!Thread.currentThread().getName().contains("dedicated"))
                throw new AssertionError("Commit requested not from dedicated stripe.");
        }
    }
}
