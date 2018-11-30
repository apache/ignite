package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

public class DdCollaborator {
    private final GridCacheSharedContext<?, ?> cctx;

    public DdCollaborator(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
    }

    public void startComputation(MvccVersion waiterVersion, MvccVersion blockerVersion) {
        Optional<IgniteInternalTx> waitingTx = cctx.tm().activeTransactions().stream()
            .filter(tx -> belongToSameTx(waiterVersion, tx.mvccSnapshot()))
            .findAny();
        Optional<IgniteInternalTx> blockerTx = cctx.tm().activeTransactions().stream()
            .filter(tx -> belongToSameTx(blockerVersion, tx.mvccSnapshot()))
            .findAny();
        if (waitingTx.isPresent() && blockerTx.isPresent()) {
            sendProbe(
                waitingTx.get().nearXidVersion(),
                waitingTx.get().nearXidVersion(),
                blockerTx.get().nearXidVersion(),
                blockerTx.get().eventNodeId()
            );
        }
    }

    public void handleDeadlockProbe(DeadlockProbe probe) {
        cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.nearXidVersion().equals(probe.blockerVersion()) && tx.near() && tx.local())
            .findAny()
            .map(GridNearTxLocal.class::cast)
            .ifPresent(tx -> {
                if (tx.nearXidVersion().equals(probe.initiatorVersion()))
                    tx.rollbackAsync();
                else {
                    // probe each waiting key
                    // t0d0 multiple blockers
                    // t0d0 check if holding some lock already
                    // t0d0 first find all peers then send messages
                    collectBlockers(tx.nearXidVersion()).listen(fut -> {
                        try {
                            T2<GridCacheVersion, UUID> lockedKey = fut.get();
                            if (lockedKey == null)
                                return;

                            sendProbe(probe.initiatorVersion(), tx.nearXidVersion(), lockedKey.get1(), lockedKey.get2());
                        }
                        catch (IgniteCheckedException e) {
                            e.printStackTrace();
                        }
                    });
                }
            });
    }

    private IgniteInternalFuture<T2<GridCacheVersion, UUID>> collectBlockers(GridCacheVersion ver) {
        // t0d0 handle primaries local to near node
        // t0d0 different future types
        Optional<GridNearTxEnlistFuture> optFut = cctx.mvcc().activeFutures().stream()
            .filter(GridNearTxEnlistFuture.class::isInstance)
            .map(GridNearTxEnlistFuture.class::cast)
            .filter(enlistFut -> enlistFut.tx.nearXidVersion().equals(ver))
            .findAny();

        // t0d0 handle multiple batches
        // t0d0 it might be better if pending batches are provided by tx
        Optional<Map.Entry<UUID, GridNearTxEnlistFuture.Batch>> optBatch = optFut
            .flatMap(fut -> fut.batches.entrySet().stream().findAny());

        if (optBatch.isPresent()) {
            UUID nodeId = optBatch.get().getKey();
            return cctx.coordinators().checkWaiting(ver, nodeId);
        }

        return new GridFinishedFuture<>();
    }

    private void sendProbe(
        GridCacheVersion initiatorVer, GridCacheVersion waiterVer, GridCacheVersion blockerVer, UUID blockerNearNodeId) {
        // t0d0 review if message order is important here
        // t0d0 PROPER TOPIC
        DeadlockProbe probe = new DeadlockProbe(initiatorVer, waiterVer, blockerVer);
        try {
            cctx.gridIO().sendToGridTopic(
                blockerNearNodeId, TOPIC_CACHE_COORDINATOR, probe, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            // t0d0 handle send errors
            e.printStackTrace();
        }
    }

    public static boolean belongToSameTx(MvccVersion v1, MvccVersion v2) {
        return v1.coordinatorVersion() == v2.coordinatorVersion() && v1.counter() == v2.counter();
    }
}
