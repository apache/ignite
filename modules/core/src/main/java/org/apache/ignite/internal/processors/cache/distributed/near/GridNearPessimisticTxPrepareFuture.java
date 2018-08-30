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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxMapping;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 *
 */
public class GridNearPessimisticTxPrepareFuture extends GridNearTxPrepareFutureAdapter {
    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearPessimisticTxPrepareFuture(GridCacheSharedContext cctx, GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.pessimistic() : tx;
    }

    /** {@inheritDoc} */
    @Override protected boolean ignoreFailure(Throwable err) {
        return IgniteCheckedException.class.isAssignableFrom(err.getClass());
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            MiniFuture f = (MiniFuture)fut;

            if (f.node().id().equals(nodeId)) {
                ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Remote node left grid: " +
                    nodeId);

                e.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                f.onNodeLeft(e);

                found = true;
            }
        }

        return found;
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridNearTxPrepareResponse res) {
        if (!isDone()) {
            assert res.clientRemapVersion() == null : res;

            MiniFuture f = miniFuture(res.miniId());

            if (f != null) {
                assert f.node().id().equals(nodeId);

                f.onResult(res);
            }
            else {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near pessimistic prepare, failed to find mini future [txId=" + tx.nearXidVersion() +
                        ", node=" + nodeId +
                        ", res=" + res +
                        ", fut=" + this + ']');
                }
            }
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near pessimistic prepare, response for finished future [txId=" + tx.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private MiniFuture miniFuture(IgniteUuid miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (sync) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                MiniFuture mini = (MiniFuture) future(i);

                if (mini.futureId().equals(miniId)) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void prepare() {
        if (!tx.state(PREPARING)) {
            if (tx.setRollbackOnly()) {
                if (tx.remainingTime() == -1)
                    onDone(new IgniteTxTimeoutCheckedException("Transaction timed out and was rolled back: " + tx));
                else
                    onDone(new IgniteCheckedException("Invalid transaction state for prepare " +
                        "[state=" + tx.state() + ", tx=" + this + ']'));
            }
            else
                onDone(new IgniteTxRollbackCheckedException("Invalid transaction state for prepare " +
                    "[state=" + tx.state() + ", tx=" + this + ']'));

            return;
        }

        try {
            tx.userPrepare();

            cctx.mvcc().addFuture(this);

            preparePessimistic();
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     *
     */
    private void preparePessimistic() {
        Map<IgniteBiTuple<ClusterNode, Boolean>, GridDistributedTxMapping> mappings = new HashMap<>();

        AffinityTopologyVersion topVer = tx.topologyVersion();

        txMapping = new GridDhtTxMapping();

        for (IgniteTxEntry txEntry : tx.allEntries()) {
            txEntry.clearEntryReadVersion();

            GridCacheContext cacheCtx = txEntry.context();

            List<ClusterNode> nodes = cacheCtx.isLocal() ?
                cacheCtx.affinity().nodesByKey(txEntry.key(), topVer) :
                cacheCtx.topology().nodes(cacheCtx.affinity().partition(txEntry.key()), topVer);

            ClusterNode primary = F.first(nodes);

            boolean near = cacheCtx.isNear();

            IgniteBiTuple<ClusterNode, Boolean> key = F.t(primary, near);

            GridDistributedTxMapping nodeMapping = mappings.get(key);

            if (nodeMapping == null) {
                nodeMapping = new GridDistributedTxMapping(primary);

                nodeMapping.near(cacheCtx.isNear());

                mappings.put(key, nodeMapping);
            }

            txEntry.nodeId(primary.id());

            nodeMapping.add(txEntry);

            txMapping.addMapping(nodes);
        }

        tx.transactionNodes(txMapping.transactionNodes());

        checkOnePhase();

        long timeout = tx.remainingTime();

        if (timeout == -1)
            onDone(new IgniteTxTimeoutCheckedException("Transaction timed out and was rolled back: " + tx));

        for (final GridDistributedTxMapping m : mappings.values()) {
            final ClusterNode node = m.node();

            GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
                futId,
                tx.topologyVersion(),
                tx,
                timeout,
                m.reads(),
                m.writes(),
                m.near(),
                txMapping.transactionNodes(),
                true,
                tx.onePhaseCommit(),
                tx.needReturnValue() && tx.implicit(),
                tx.implicitSingle(),
                m.explicitLock(),
                tx.subjectId(),
                tx.taskNameHash(),
                false,
                tx.activeCachesDeploymentEnabled());

            for (IgniteTxEntry txEntry : m.entries()) {
                if (txEntry.op() == TRANSFORM)
                    req.addDhtVersion(txEntry.txKey(), null);
            }

            final MiniFuture fut = new MiniFuture(m);

            req.miniId(fut.futureId());

            add(fut);

            if (node.isLocal()) {
                IgniteInternalFuture<GridNearTxPrepareResponse> prepFut = cctx.tm().txHandler().prepareTx(node.id(),
                    tx,
                    req);

                prepFut.listen(new CI1<IgniteInternalFuture<GridNearTxPrepareResponse>>() {
                    @Override public void apply(IgniteInternalFuture<GridNearTxPrepareResponse> prepFut) {
                        try {
                            fut.onResult(prepFut.get());
                        }
                        catch (IgniteCheckedException e) {
                            fut.onError(e);
                        }
                    }
                });
            }
            else {
                try {
                    cctx.io().send(node, req, tx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Near pessimistic prepare, sent request [txId=" + tx.nearXidVersion() +
                            ", node=" + node.id() + ']');
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    e.retryReadyFuture(cctx.nextAffinityReadyFuture(topVer));

                    fut.onNodeLeft(e);
                }
                catch (IgniteCheckedException e) {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Near pessimistic prepare, failed send request [txId=" + tx.nearXidVersion() +
                            ", node=" + node.id() + ", err=" + e + ']');
                    }

                    fut.onError(e);

                    break;
                }
            }
        }

        markInitialized();
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable IgniteInternalTx res, @Nullable Throwable err) {
        if (err != null)
            ERR_UPD.compareAndSet(GridNearPessimisticTxPrepareFuture.this, null, err);

        err = this.err;

        if (err == null || tx.needCheckBackup())
            tx.state(PREPARED);

        if (super.onDone(tx, err)) {
            cctx.mvcc().removeMvccFuture(this);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).node().id() +
                    ", loc=" + ((MiniFuture)f).node().isLocal() +
                    ", done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridNearPessimisticTxPrepareFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     *
     */
    private class MiniFuture extends GridFutureAdapter<GridNearTxPrepareResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** */
        private GridDistributedTxMapping m;

        /**
         * @param m Mapping.
         */
        MiniFuture(GridDistributedTxMapping m) {
            this.m = m;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return m.node();
        }

        /**
         * @param res Response.
         */
        void onResult(GridNearTxPrepareResponse res) {
            if (res.error() != null)
                onError(res.error());
            else {
                onPrepareResponse(m, res);

                onDone(res);
            }
        }

        /**
         * @param e Error.
         */
        void onNodeLeft(ClusterTopologyCheckedException e) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near pessimistic prepare, mini future node left [txId=" + tx.nearXidVersion() +
                    ", nodeId=" + m.node().id() + ']');
            }

            if (tx.onePhaseCommit()) {
                tx.markForBackupCheck();

                // Do not fail future for one-phase transaction right away.
                onDone((GridNearTxPrepareResponse)null);
            }

            onError(e);
        }

        /**
         * @param e Error.
         */
        void onError(Throwable e) {
            if (isDone()) {
                U.warn(log, "Received error when future is done [fut=" + this + ", err=" + e + ", tx=" + tx + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Error on tx prepare [fut=" + this + ", err=" + e + ", tx=" + tx +  ']');

            if (ERR_UPD.compareAndSet(GridNearPessimisticTxPrepareFuture.this, null, e))
                tx.setRollbackOnly();

            onDone(e);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}
