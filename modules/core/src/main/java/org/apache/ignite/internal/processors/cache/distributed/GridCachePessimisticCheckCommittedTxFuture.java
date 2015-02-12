/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
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
    private final IgniteUuid futId = IgniteUuid.randomUuid();

    /** Transaction. */
    private final IgniteInternalTx<K, V> tx;

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
    public GridCachePessimisticCheckCommittedTxFuture(GridCacheSharedContext<K, V> cctx, IgniteInternalTx<K, V> tx,
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

        if (tx instanceof IgniteTxRemoteEx)
            originatingThreadId = ((IgniteTxRemoteEx)tx).remoteThreadId();

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
                    onDone(new ClusterTopologyCheckedException("Failed to check near transaction state (near node left grid): " +
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

            GridCachePessimisticCheckCommittedTxRequest<K, V> req = new GridCachePessimisticCheckCommittedTxRequest<>(
                tx,
                originatingThreadId, futureId(), nearCheck);

            if (rmtNode.isLocal())
                add(cctx.tm().checkPessimisticTxCommitted(req));
            else {
                MiniFuture fut = new MiniFuture(rmtNode.id());

                req.miniId(fut.futureId());

                add(fut);

                try {
                    cctx.io().send(rmtNode.id(), req, tx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException ignored) {
                    fut.onNodeLeft();
                }
                catch (IgniteCheckedException e) {
                    fut.onError(e);

                    break;
                }
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
            for (IgniteInternalFuture<GridCacheCommittedTxInfo<K, V>> fut : pending()) {
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
    @Override public IgniteUuid futureId() {
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
        for (IgniteInternalFuture<?> fut : futures())
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
    private boolean isMini(IgniteInternalFuture<?> f) {
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
        private final IgniteUuid futId = IgniteUuid.randomUuid();

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
        private IgniteUuid futureId() {
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
                onDone(new ClusterTopologyCheckedException("Failed to check near transaction state (near node left grid): " +
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
        IgniteReducer<GridCacheCommittedTxInfo<K, V>, GridCacheCommittedTxInfo<K, V>> {
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
