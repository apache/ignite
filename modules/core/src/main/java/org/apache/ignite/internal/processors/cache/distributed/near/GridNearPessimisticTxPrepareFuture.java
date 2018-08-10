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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
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
    @Override public void onNearTxLocalTimeout() {
        // No-op.
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

            if (f.primary().id().equals(nodeId)) {
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
                assert f.primary().id().equals(nodeId);

                f.onResult(res, true);
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
    private MiniFuture miniFuture(int miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (this) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                MiniFuture mini = (MiniFuture)future(i);

                if (mini.futureId() == miniId) {
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
            if (tx.isRollbackOnly() || tx.setRollbackOnly()) {
                if (tx.remainingTime() == -1)
                    onDone(tx.timeoutException());
                else
                    onDone(tx.rollbackException());
            }
            else
                onDone(new IgniteCheckedException("Invalid transaction state for prepare " +
                    "[state=" + tx.state() + ", tx=" + this + ']'));

            return;
        }

        try {
            tx.userPrepare(Collections.<IgniteTxEntry>emptyList());

            cctx.mvcc().addFuture(this);

            preparePessimistic();
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * @param txNodes Tx nodes.
     * @param m Mapping.
     * @param timeout Timeout.
     * @param reads Reads.
     * @param writes Writes.
     * @return Request.
     */
    private GridNearTxPrepareRequest createRequest(Map<UUID, Collection<UUID>> txNodes,
        GridDistributedTxMapping m,
        long timeout,
        Collection<IgniteTxEntry> reads,
        Collection<IgniteTxEntry> writes) {
        GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
            futId,
            tx.topologyVersion(),
            tx,
            timeout,
            reads,
            writes,
            m.hasNearCacheEntries(),
            txNodes,
            true,
            tx.onePhaseCommit(),
            tx.needReturnValue() && tx.implicit(),
            tx.implicitSingle(),
            m.explicitLock(),
            tx.subjectId(),
            tx.taskNameHash(),
            false,
            true,
            tx.activeCachesDeploymentEnabled());

        for (IgniteTxEntry txEntry : writes) {
            if (txEntry.op() == TRANSFORM)
                req.addDhtVersion(txEntry.txKey(), null);
        }

        return req;
    }

    /**
     * @param req Request.
     * @param m Mapping.
     * @param miniId Mini future ID.
     * @param nearEntries {@code True} if prepare near cache entries.
     */
    private void prepareLocal(GridNearTxPrepareRequest req,
        GridDistributedTxMapping m,
        int miniId,
        final boolean nearEntries) {
        final MiniFuture fut = new MiniFuture(m, miniId);

        req.miniId(fut.futureId());

        add(fut);

        IgniteInternalFuture<GridNearTxPrepareResponse> prepFut = nearEntries ?
            cctx.tm().txHandler().prepareNearTxLocal(req) :
            cctx.tm().txHandler().prepareColocatedTx(tx, req);

        prepFut.listen(new CI1<IgniteInternalFuture<GridNearTxPrepareResponse>>() {
            @Override public void apply(IgniteInternalFuture<GridNearTxPrepareResponse> prepFut) {
                try {
                    fut.onResult(prepFut.get(), nearEntries);
                }
                catch (IgniteCheckedException e) {
                    fut.onError(e);
                }
            }
        });
    }

    /**
     *
     */
    private void preparePessimistic() {
        Map<UUID, GridDistributedTxMapping> mappings = new HashMap<>();

        AffinityTopologyVersion topVer = tx.topologyVersion();

        GridDhtTxMapping txMapping = new GridDhtTxMapping();

        boolean hasNearCache = false;

        for (IgniteTxEntry txEntry : tx.allEntries()) {
            txEntry.clearEntryReadVersion();

            GridCacheContext cacheCtx = txEntry.context();

            if (cacheCtx.isNear())
                hasNearCache = true;

            List<ClusterNode> nodes;

            if (!cacheCtx.isLocal()) {
                GridDhtPartitionTopology top = cacheCtx.topology();

                nodes = top.nodes(cacheCtx.affinity().partition(txEntry.key()), topVer);
            }
            else
                nodes = cacheCtx.affinity().nodesByKey(txEntry.key(), topVer);

            if (F.isEmpty(nodes)) {
                onDone(new ClusterTopologyServerNotFoundException("Failed to map keys to nodes (partition " +
                    "is not mapped to any node) [key=" + txEntry.key() +
                    ", partition=" + cacheCtx.affinity().partition(txEntry.key()) + ", topVer=" + topVer + ']'));

                return;
            }

            ClusterNode primary = nodes.get(0);

            GridDistributedTxMapping nodeMapping = mappings.get(primary.id());

            if (nodeMapping == null)
                mappings.put(primary.id(), nodeMapping = new GridDistributedTxMapping(primary));

            txEntry.nodeId(primary.id());

            nodeMapping.add(txEntry);

            txMapping.addMapping(nodes);
        }

        tx.transactionNodes(txMapping.transactionNodes());

        if (!hasNearCache)
            checkOnePhase(txMapping);

        long timeout = tx.remainingTime();

        if (timeout == -1) {
            onDone(new IgniteTxTimeoutCheckedException("Transaction timed out and was rolled back: " + tx));

            return;
        }

        int miniId = 0;

        Map<UUID, Collection<UUID>> txNodes = txMapping.transactionNodes();

        for (final GridDistributedTxMapping m : mappings.values()) {
            final ClusterNode primary = m.primary();

            if (primary.isLocal()) {
                if (m.hasNearCacheEntries() && m.hasColocatedCacheEntries()) {
                    GridNearTxPrepareRequest nearReq = createRequest(txMapping.transactionNodes(),
                        m,
                        timeout,
                        m.nearEntriesReads(),
                        m.nearEntriesWrites());

                    prepareLocal(nearReq, m, ++miniId, true);

                    GridNearTxPrepareRequest colocatedReq = createRequest(txNodes,
                        m,
                        timeout,
                        m.colocatedEntriesReads(),
                        m.colocatedEntriesWrites());

                    prepareLocal(colocatedReq, m, ++miniId, false);
                }
                else {
                    GridNearTxPrepareRequest req = createRequest(txNodes, m, timeout, m.reads(), m.writes());

                    prepareLocal(req, m, ++miniId, m.hasNearCacheEntries());
                }
            }
            else {
                GridNearTxPrepareRequest req = createRequest(txNodes,
                    m,
                    timeout,
                    m.reads(),
                    m.writes());

                final MiniFuture fut = new MiniFuture(m, ++miniId);

                req.miniId(fut.futureId());

                add(fut);

                try {
                    cctx.io().send(primary, req, tx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Near pessimistic prepare, sent request [txId=" + tx.nearXidVersion() +
                            ", node=" + primary.id() + ']');
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    e.retryReadyFuture(cctx.nextAffinityReadyFuture(topVer));

                    fut.onNodeLeft(e);
                }
                catch (IgniteCheckedException e) {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Near pessimistic prepare, failed send request [txId=" + tx.nearXidVersion() +
                            ", node=" + primary.id() + ", err=" + e + ']');
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
            cctx.mvcc().removeVersionedFuture(this);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).primary().id() +
                    ", loc=" + ((MiniFuture)f).primary().isLocal() +
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
        private final int futId;

        /** */
        private GridDistributedTxMapping m;

        /**
         * @param m Mapping.
         * @param futId Mini future ID.
         */
        MiniFuture(GridDistributedTxMapping m, int futId) {
            this.m = m;
            this.futId = futId;
        }

        /**
         * @return Future ID.
         */
        int futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode primary() {
            return m.primary();
        }

        /**
         * @param res Response.
         * @param updateMapping Update mapping flag.
         */
        void onResult(GridNearTxPrepareResponse res, boolean updateMapping) {
            if (res.error() != null)
                onError(res.error());
            else {
                onPrepareResponse(m, res, updateMapping);

                onDone(res);
            }
        }

        /**
         * @param e Error.
         */
        void onNodeLeft(ClusterTopologyCheckedException e) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near pessimistic prepare, mini future node left [txId=" + tx.nearXidVersion() +
                    ", nodeId=" + m.primary().id() + ']');
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
