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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionState.PREPARED;

/**
 * Future verifying that all remote transactions related to transaction were prepared or committed.
 */
public class GridCacheTxRecoveryFuture extends GridCompoundIdentityFuture<Boolean> implements GridCacheFuture<Boolean> {
    /** */         
    private static final long serialVersionUID = 0L;
    
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    private static IgniteLogger msgLog;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Future ID. */
    private final IgniteUuid futId = IgniteUuid.randomUuid();

    /** Transaction. */
    private final IgniteInternalTx tx;

    /** All involved nodes. */
    private final Map<UUID, ClusterNode> nodes;

    /** ID of failed nodes started transaction. */
    @GridToStringInclude
    private final Set<UUID> failedNodeIds;

    /** Transaction nodes mapping. */
    private final Map<UUID, Collection<UUID>> txNodes;

    /** */
    private final boolean nearTxCheck;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param failedNodeIds IDs of failed nodes started transaction.
     * @param txNodes Transaction mapping.
     */
    @SuppressWarnings("ConstantConditions")
    public GridCacheTxRecoveryFuture(GridCacheSharedContext<?, ?> cctx,
        IgniteInternalTx tx,
        Set<UUID> failedNodeIds,
        Map<UUID, Collection<UUID>> txNodes)
    {
        super(CU.boolReducer());

        this.cctx = cctx;
        this.tx = tx;
        this.txNodes = txNodes;
        this.failedNodeIds = failedNodeIds;

        if (log == null) {
            msgLog = cctx.txRecoveryMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridCacheTxRecoveryFuture.class);
        }

        nodes = new GridLeanMap<>();

        UUID locNodeId = cctx.localNodeId();

        for (Map.Entry<UUID, Collection<UUID>> e : tx.transactionNodes().entrySet()) {
            if (!locNodeId.equals(e.getKey()) && !failedNodeIds.contains(e.getKey()) && !nodes.containsKey(e.getKey())) {
                ClusterNode node = cctx.discovery().node(e.getKey());

                if (node != null)
                    nodes.put(node.id(), node);
                else if (log.isDebugEnabled())
                    log.debug("Transaction node left (will ignore) " + e.getKey());
            }

            for (UUID nodeId : e.getValue()) {
                if (!locNodeId.equals(nodeId) && !failedNodeIds.contains(nodeId) && !nodes.containsKey(nodeId)) {
                    ClusterNode node = cctx.discovery().node(nodeId);

                    if (node != null)
                        nodes.put(node.id(), node);
                    else if (log.isDebugEnabled())
                        log.debug("Transaction node left (will ignore) " + e.getKey());
                }
            }
        }

        UUID nearNodeId = tx.eventNodeId();

        nearTxCheck = !failedNodeIds.contains(nearNodeId) && cctx.discovery().alive(nearNodeId);
    }

    /**
     * Initializes future.
     */
    @SuppressWarnings("ConstantConditions")
    public void prepare() {
        if (nearTxCheck) {
            UUID nearNodeId = tx.eventNodeId();

            if (cctx.localNodeId().equals(nearNodeId)) {
                IgniteInternalFuture<Boolean> fut = cctx.tm().txCommitted(tx.nearXidVersion());

                fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        try {
                            onDone(fut.get());
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                    }
                });
            }
            else {
                MiniFuture fut = new MiniFuture(tx.eventNodeId());

                add(fut);

                GridCacheTxRecoveryRequest req = new GridCacheTxRecoveryRequest(
                    tx,
                    0,
                    true,
                    futureId(),
                    fut.futureId(),
                    tx.activeCachesDeploymentEnabled());

                try {
                    cctx.io().send(nearNodeId, req, tx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Recovery fut, sent request near tx [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + nearNodeId + ']');
                    }
                }
                catch (ClusterTopologyCheckedException ignore) {
                    fut.onNodeLeft(nearNodeId);
                }
                catch (IgniteCheckedException e) {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Recovery fut, failed to send request near tx [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + nearNodeId +
                            ", err=" + e + ']');
                    }

                    fut.onError(e);
                }

                markInitialized();
            }

            return;
        }

        // First check transactions on local node.
        int locTxNum = nodeTransactions(cctx.localNodeId());

        if (locTxNum > 1) {
            IgniteInternalFuture<Boolean> fut = cctx.tm().txsPreparedOrCommitted(tx.nearXidVersion(), locTxNum);

            if (fut == null || fut.isDone()) {
                boolean prepared;

                try {
                    prepared = fut == null ? true : fut.get();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Check prepared transaction future failed: " + e, e);

                    prepared = false;
                }

                if (!prepared) {
                    onDone(false);

                    markInitialized();

                    return;
                }
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        boolean prepared;

                        try {
                            prepared = fut.get();
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Check prepared transaction future failed: " + e, e);

                            prepared = false;
                        }

                        if (!prepared) {
                            onDone(false);

                            markInitialized();
                        }
                        else
                            proceedPrepare();
                    }
                });

                return;
            }
        }

        proceedPrepare();
    }

    /**
     * Process prepare after local check.
     */
    private void proceedPrepare() {
        for (Map.Entry<UUID, Collection<UUID>> entry : txNodes.entrySet()) {
            UUID nodeId = entry.getKey();

            // Skip left nodes and local node.
            if (!nodes.containsKey(nodeId) && nodeId.equals(cctx.localNodeId()))
                continue;

            /*
             * If primary node failed then send message to all backups, otherwise
             * send message only to primary node.
             */

            if (failedNodeIds.contains(nodeId)) {
                for (UUID id : entry.getValue()) {
                    // Skip backup node if it is local node or if it is also was mapped as primary.
                    if (txNodes.containsKey(id) || id.equals(cctx.localNodeId()))
                        continue;

                    MiniFuture fut = new MiniFuture(id);

                    add(fut);

                    GridCacheTxRecoveryRequest req = new GridCacheTxRecoveryRequest(tx,
                        nodeTransactions(id),
                        false,
                        futureId(),
                        fut.futureId(),
                        tx.activeCachesDeploymentEnabled());

                    try {
                        cctx.io().send(id, req, tx.ioPolicy());

                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("Recovery fut, sent request to backup [txId=" + tx.nearXidVersion() +
                                ", dhtTxId=" + tx.xidVersion() +
                                ", node=" + id + ']');
                        }
                    }
                    catch (ClusterTopologyCheckedException ignored) {
                        fut.onNodeLeft(id);
                    }
                    catch (IgniteCheckedException e) {
                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("Recovery fut, failed to send request to backup [txId=" + tx.nearXidVersion() +
                                ", dhtTxId=" + tx.xidVersion() +
                                ", node=" + id +
                                ", err=" + e + ']');
                        }

                        fut.onError(e);

                        break;
                    }
                }
            }
            else {
                MiniFuture fut = new MiniFuture(nodeId);

                add(fut);

                GridCacheTxRecoveryRequest req = new GridCacheTxRecoveryRequest(
                    tx,
                    nodeTransactions(nodeId),
                    false,
                    futureId(),
                    fut.futureId(),
                    tx.activeCachesDeploymentEnabled());

                try {
                    cctx.io().send(nodeId, req, tx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Recovery fut, sent request to primary [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + nodeId + ']');
                    }
                }
                catch (ClusterTopologyCheckedException ignored) {
                    fut.onNodeLeft(nodeId);
                }
                catch (IgniteCheckedException e) {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Recovery fut, failed to send request to primary [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + nodeId +
                            ", err=" + e + ']');
                    }

                    fut.onError(e);

                    break;
                }
            }
        }

        markInitialized();
    }

    /**
     * @param nodeId Node ID.
     * @return Number of transactions on node.
     */
    private int nodeTransactions(UUID nodeId) {
        int cnt = txNodes.containsKey(nodeId) ? 1 : 0; // +1 if node is primary.

        for (Collection<UUID> backups : txNodes.values()) {
            for (UUID backup : backups) {
                if (backup.equals(nodeId)) {
                    cnt++; // +1 if node is backup.

                    break;
                }
            }
        }

        return cnt;
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridCacheTxRecoveryResponse res) {
        if (!isDone()) {
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.nodeId().equals(nodeId);

                mini.onResult(res);
            }
            else {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Tx recovery fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + nodeId +
                        ", res=" + res +
                        ", fut=" + this + ']');
                }
            }
        }
        else {
            msgLog.debug("Tx recovery fut, response for finished future [txId=" + tx.nearXidVersion() +
                ", dhtTxId=" + tx.xidVersion() +
                ", node=" + nodeId +
                ", res=" + res +
                ", fut=" + this + ']');
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
                IgniteInternalFuture<Boolean> fut = future(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

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

    /**
     * @return Transaction.
     */
    public IgniteInternalTx tx() {
        return tx;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(final UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                final MiniFuture f = (MiniFuture)fut;

                if (f.nodeId().equals(nodeId)) {
                    cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            f.onNodeLeft(nodeId);
                        }
                    });
                }
            }
        }

        return true;
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
    @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeFuture(futId);

            if (err == null) {
                assert res != null;

                cctx.tm().finishTxOnRecovery(tx, res);
            }
            else {
                if (err instanceof ClusterTopologyCheckedException && nearTxCheck) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to check transaction on near node, " +
                            "ignoring [err=" + err + ", tx=" + tx + ']');
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Failed to check prepared transactions, " +
                            "invalidating transaction [err=" + err + ", tx=" + tx + ']');

                    cctx.tm().salvageTx(tx);
                }
            }
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
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).nodeId +
                    ", done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridCacheTxRecoveryFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     *
     */
    private class MiniFuture extends GridFutureAdapter<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Mini future ID. */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node ID. */
        private UUID nodeId;

        /**
         * @param nodeId Node ID.
         */
        private MiniFuture(UUID nodeId) {
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
         * @param nodeId Failed node ID.
         */
        private void onNodeLeft(UUID nodeId) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Tx recovery fut, mini future node left [txId=" + tx.nearXidVersion() +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + nodeId +
                    ", nearTxCheck=" + nearTxCheck + ']');
            }

            if (nearTxCheck) {
                if (tx.state() == PREPARED) {
                    Set<UUID> failedNodeIds0 = new HashSet<>(failedNodeIds);
                    failedNodeIds0.add(nodeId);

                    // Near and originating nodes left, need initiate tx check.
                    cctx.tm().commitIfPrepared(tx, failedNodeIds0);
                }

                onDone(new ClusterTopologyCheckedException("Transaction node left grid (will ignore)."));
            }
            else
                onDone(true);
        }

        /**
         * @param res Result callback.
         */
        private void onResult(GridCacheTxRecoveryResponse res) {
            onDone(res.success());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "err", error());
        }
    }
}
