/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Future verifying that all remote transactions related to some
 * optimistic transaction were prepared.
 */
public class GridCachePessimisticCheckCommittedTxFuture<K, V> extends GridCompoundIdentityFuture<GridCacheCommittedTxInfo<K, V>>
    implements GridCacheFuture<GridCacheCommittedTxInfo<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Context. */
    private final GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private final GridUuid futId = GridUuid.randomUuid();

    /** Transaction. */
    private final GridCacheTxEx<K, V> tx;

    /** All involved nodes. */
    private final Map<UUID, ClusterNode> nodes;

    /** ID of failed node started transaction. */
    private final UUID failedNodeId;

    /** Flag indicating that future checks near node instead of checking all topology in case of primary node crash. */
    private boolean nearCheck;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param failedNodeId ID of failed node started transaction.
     */
    @SuppressWarnings("ConstantConditions")
    public GridCachePessimisticCheckCommittedTxFuture(GridCacheSharedContext<K, V> cctx, GridCacheTxEx<K, V> tx,
        UUID failedNodeId) {
        super(cctx.kernalContext(), new SingleReducer<K, V>());

        this.cctx = cctx;
        this.tx = tx;
        this.failedNodeId = failedNodeId;

        nodes = new GridLeanMap<>();

        for (ClusterNode node : CU.allNodes(cctx, tx.topologyVersion()))
            nodes.put(node.id(), node);
    }

    /**
     * Initializes future.
     */
    public void prepare() {
        if (log.isDebugEnabled())
            log.debug("Checking if transaction was committed on remote nodes: " + tx);

        // Check local node first (local node can be a backup node for some part of this transaction).
        long originatingThreadId = tx.threadId();

        if (tx instanceof GridCacheTxRemoteEx)
            originatingThreadId = ((GridCacheTxRemoteEx)tx).remoteThreadId();

        GridCacheCommittedTxInfo<K, V> txInfo = cctx.tm().txCommitted(tx.nearXidVersion(), tx.eventNodeId(),
            originatingThreadId);

        if (txInfo != null) {
            onDone(txInfo);

            markInitialized();

            return;
        }

        Collection<ClusterNode> checkNodes = CU.remoteNodes(cctx, tx.topologyVersion());

        if (tx instanceof GridDhtTxRemote) {
            // If we got primary node failure and near node has not failed.
            if (tx.nodeId().equals(failedNodeId) && !tx.eventNodeId().equals(failedNodeId)) {
                nearCheck = true;

                ClusterNode nearNode = cctx.discovery().node(tx.eventNodeId());

                if (nearNode == null) {
                    // Near node failed, separate check prepared future will take care of it.
                    onDone(new GridTopologyException("Failed to check near transaction state (near node left grid): " +
                        tx.eventNodeId()));

                    return;
                }

                checkNodes = Collections.singletonList(nearNode);
            }
        }

        for (ClusterNode rmtNode : checkNodes) {
            // Skip left nodes and local node.
            if (rmtNode.id().equals(failedNodeId))
                continue;

            /*
             * Send message to all cache nodes in the topology.
             */

            MiniFuture fut = new MiniFuture(rmtNode.id());

            GridCachePessimisticCheckCommittedTxRequest<K, V> req = new GridCachePessimisticCheckCommittedTxRequest<>(tx,
                originatingThreadId, futureId(), fut.futureId(), nearCheck);

            add(fut);

            try {
                cctx.io().send(rmtNode.id(), req);
            }
            catch (GridTopologyException ignored) {
                fut.onNodeLeft();
            }
            catch (GridException e) {
                fut.onError(e);

                break;
            }
        }

        markInitialized();
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridCachePessimisticCheckCommittedTxResponse<K, V> res) {
        if (!isDone()) {
            for (GridFuture<GridCacheCommittedTxInfo<K, V>> fut : pending()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        assert f.nodeId().equals(nodeId);

                        f.onResult(res);

                        break;
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends ClusterNode> nodes() {
        return nodes.values();
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (GridFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.nodeId().equals(nodeId)) {
                    f.onNodeLeft();

                    return true;
                }
            }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable GridCacheCommittedTxInfo<K, V> res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeFuture(this);

            if (log.isDebugEnabled())
                log.debug("Completing check committed tx future for transaction [tx=" + tx + ", res=" + res +
                    ", err=" + err + ']');

            if (err == null)
                cctx.tm().finishPessimisticTxOnRecovery(tx, res);
            else {
                if (log.isDebugEnabled())
                    log.debug("Failed to check prepared transactions, " +
                        "invalidating transaction [err=" + err + ", tx=" + tx + ']');

                if (nearCheck)
                    return true;

                cctx.tm().salvageTx(tx);
            }

            return true;
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(GridFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePessimisticCheckCommittedTxFuture.class, this, "super", super.toString());
    }

    /**
     *
     */
    private class MiniFuture extends GridFutureAdapter<GridCacheCommittedTxInfo<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Mini future ID. */
        private final GridUuid futId = GridUuid.randomUuid();

        /** Node ID. */
        private UUID nodeId;

        /**
         * Empty constructor required by {@link Externalizable}
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param nodeId Node ID.
         */
        private MiniFuture(UUID nodeId) {
            super(cctx.kernalContext());

            this.nodeId = nodeId;
        }

        /**
         * @return Node ID.
         */
        private UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Future ID.
         */
        private GridUuid futureId() {
            return futId;
        }

        /**
         * @param e Error.
         */
        private void onError(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            onDone(e);
        }

        /**
         */
        private void onNodeLeft() {
            if (log.isDebugEnabled())
                log.debug("Transaction node left grid (will ignore) [fut=" + this + ']');

            if (nearCheck) {
                onDone(new GridTopologyException("Failed to check near transaction state (near node left grid): " +
                    nodeId));

                return;
            }

            onDone((GridCacheCommittedTxInfo<K, V>)null);
        }

        /**
         * @param res Result callback.
         */
        private void onResult(GridCachePessimisticCheckCommittedTxResponse<K, V> res) {
            onDone(res.committedTxInfo());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "err", error());
        }
    }

    /**
     * Single value reducer.
     */
    private static class SingleReducer<K, V> implements
        GridReducer<GridCacheCommittedTxInfo<K, V>, GridCacheCommittedTxInfo<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private AtomicReference<GridCacheCommittedTxInfo<K, V>> collected = new AtomicReference<>();

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable GridCacheCommittedTxInfo<K, V> info) {
            if (info != null) {
                collected.compareAndSet(null, info);

                // Stop collecting on first collected info.
                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public GridCacheCommittedTxInfo<K, V> reduce() {
            return collected.get();
        }
    }
}
