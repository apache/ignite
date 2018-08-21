package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 *
 */
public class IgniteTxFinisher {
    private static final int DEDICATED_WORKER_IDX = 0;

    private final IgniteLogger log;

    private final GridCacheSharedContext cctx;

    private final ConcurrentLinkedHashMap<GridCacheVersion, Long> txOrdering = new ConcurrentLinkedHashMap<GridCacheVersion, Long>();

    private final ConcurrentLinkedHashMap<Long, IgniteTxAdapter> txs = new ConcurrentLinkedHashMap<>();

    private final Map<Long, List<Runnable>> delayedSendings = new LinkedHashMap<>();

    private long ordered;

    private long send;

    public IgniteTxFinisher(GridCacheSharedContext cctx) {
        this.cctx = cctx;
        this.log = cctx.logger(getClass());
    }

    public void execute(IgniteTxAdapter tx, Runnable transactionOp) {
        cctx.kernalContext().getStripedExecutorService().executeDedicated(DEDICATED_WORKER_IDX, () -> {
            GridCacheVersion txId = tx.xidVersion();

            if (txId != null) {
                boolean isNew = !txOrdering.containsKey(txId);

                if (!txOrdering.containsKey(txId)) {
                    txOrdering.put(txId, ordered);

                    txs.put(ordered, tx);

                    ordered++;
                }

                if (isNew)
                    log.warning("New tx = " + (ordered - 1) + " " + tx);
            }

            transactionOp.run();
        });
    }

    public void send(IgniteTxAdapter tx, Runnable transactionSendOp) {
        GridCacheVersion txId = tx.xidVersion();

        long order = txOrdering.get(txId);

        log.warning("Send invoked -> " + order);

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

    public void finishSend(IgniteTxAdapter tx) {
        GridCacheVersion txId = tx.xidVersion();

        long order = txOrdering.get(txId);

        log.warning("Finished send invoked -> " + order);

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
                log.warning("Send delayed 2 - > " + send);

                List<Runnable> delayed = delayedSendings.remove(send);
                txs.remove(send);

                for (Runnable sendClj : delayed)
                    sendClj.run();

                send++;
            }
        }
    }

    public long order(IgniteTxAdapter tx) {
        return txOrdering.get(tx.xidVersion());
    }
}
