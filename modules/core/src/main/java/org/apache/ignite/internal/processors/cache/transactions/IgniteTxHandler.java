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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxOnePhaseCommitAckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxRemote;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture.FINISH_NEAR_ONE_PHASE_SINCE;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.USER_FINISH;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;

/**
 * Isolated logic to process cache messages.
 */
public class IgniteTxHandler {
    /** Logger. */
    private IgniteLogger log;

    /** */
    private final IgniteLogger txPrepareMsgLog;

    /** */
    private final IgniteLogger txFinishMsgLog;

    /** */
    private final IgniteLogger txRecoveryMsgLog;

    /** Shared cache context. */
    private GridCacheSharedContext<?, ?> ctx;

    /**
     * @param nearNodeId Node ID.
     * @param req Request.
     * @return Prepare future.
     */
    public IgniteInternalFuture<?> processNearTxPrepareRequest(final UUID nearNodeId, GridNearTxPrepareRequest req) {
        if (txPrepareMsgLog.isDebugEnabled()) {
            txPrepareMsgLog.debug("Received near prepare request [txId=" + req.version() +
                ", node=" + nearNodeId + ']');
        }

        IgniteInternalFuture<GridNearTxPrepareResponse> fut = prepareTx(nearNodeId, null, req);

        assert req.txState() != null || fut.error() != null ||
            (ctx.tm().tx(req.version()) == null && ctx.tm().nearTx(req.version()) == null);

        return fut;
    }

    /**
     * @param ctx Shared cache context.
     */
    public IgniteTxHandler(GridCacheSharedContext ctx) {
        this.ctx = ctx;

        log = ctx.logger(IgniteTxHandler.class);

        txRecoveryMsgLog = ctx.logger(CU.TX_MSG_RECOVERY_LOG_CATEGORY);
        txPrepareMsgLog = ctx.logger(CU.TX_MSG_PREPARE_LOG_CATEGORY);
        txFinishMsgLog = ctx.logger(CU.TX_MSG_FINISH_LOG_CATEGORY);

        ctx.io().addHandler(0, GridNearTxPrepareRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxPrepareRequest(nodeId, (GridNearTxPrepareRequest)msg);
            }
        });

        ctx.io().addHandler(0, GridNearTxPrepareResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxPrepareResponse(nodeId, (GridNearTxPrepareResponse)msg);
            }
        });

        ctx.io().addHandler(0, GridNearTxFinishRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxFinishRequest(nodeId, (GridNearTxFinishRequest)msg);
            }
        });

        ctx.io().addHandler(0, GridNearTxFinishResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxFinishResponse(nodeId, (GridNearTxFinishResponse)msg);
            }
        });

        ctx.io().addHandler(0, GridDhtTxPrepareRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxPrepareRequest(nodeId, (GridDhtTxPrepareRequest)msg);
            }
        });

        ctx.io().addHandler(0, GridDhtTxPrepareResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxPrepareResponse(nodeId, (GridDhtTxPrepareResponse)msg);
            }
        });

        ctx.io().addHandler(0, GridDhtTxFinishRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxFinishRequest(nodeId, (GridDhtTxFinishRequest)msg);
            }
        });

        ctx.io().addHandler(0, GridDhtTxOnePhaseCommitAckRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxOnePhaseCommitAckRequest(nodeId, (GridDhtTxOnePhaseCommitAckRequest)msg);
            }
        });

        ctx.io().addHandler(0, GridDhtTxFinishResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxFinishResponse(nodeId, (GridDhtTxFinishResponse)msg);
            }
        });

        ctx.io().addHandler(0, GridCacheTxRecoveryRequest.class,
            new CI2<UUID, GridCacheTxRecoveryRequest>() {
                @Override public void apply(UUID nodeId, GridCacheTxRecoveryRequest req) {
                    processCheckPreparedTxRequest(nodeId, req);
                }
            });

        ctx.io().addHandler(0, GridCacheTxRecoveryResponse.class,
            new CI2<UUID, GridCacheTxRecoveryResponse>() {
                @Override public void apply(UUID nodeId, GridCacheTxRecoveryResponse res) {
                    processCheckPreparedTxResponse(nodeId, res);
                }
            });
    }

    /**
     * @param nearNodeId Near node ID that initiated transaction.
     * @param locTx Optional local transaction.
     * @param req Near prepare request.
     * @return Future for transaction.
     */
    public IgniteInternalFuture<GridNearTxPrepareResponse> prepareTx(
        UUID nearNodeId,
        @Nullable GridNearTxLocal locTx,
        GridNearTxPrepareRequest req
    ) {
        assert nearNodeId != null;
        assert req != null;

        if (locTx != null) {
            if (req.near()) {
                // Make sure not to provide Near entries to DHT cache.
                req.cloneEntries();

                return prepareNearTx(nearNodeId, req);
            }
            else
                return prepareColocatedTx(locTx, req);
        }
        else
            return prepareNearTx(nearNodeId, req);
    }

    /**
     * Prepares local colocated tx.
     *
     * @param locTx Local transaction.
     * @param req Near prepare request.
     * @return Prepare future.
     */
    private IgniteInternalFuture<GridNearTxPrepareResponse> prepareColocatedTx(
        final GridNearTxLocal locTx,
        final GridNearTxPrepareRequest req
    ) {
        req.txState(locTx.txState());

        IgniteInternalFuture<GridNearTxPrepareResponse> fut = locTx.prepareAsyncLocal(
            req.reads(),
            req.writes(),
            req.transactionNodes(),
            req.last());

        if (locTx.isRollbackOnly())
            locTx.rollbackAsync();

        return fut.chain(new C1<IgniteInternalFuture<GridNearTxPrepareResponse>, GridNearTxPrepareResponse>() {
            @Override public GridNearTxPrepareResponse apply(IgniteInternalFuture<GridNearTxPrepareResponse> f) {
                try {
                    return f.get();
                }
                catch (Exception e) {
                    locTx.setRollbackOnly(); // Just in case.

                    if (!X.hasCause(e, IgniteTxOptimisticCheckedException.class) &&
                        !X.hasCause(e, IgniteFutureCancelledException.class))
                        U.error(log, "Failed to prepare DHT transaction: " + locTx, e);

                    return new GridNearTxPrepareResponse(
                        req.version(),
                        req.futureId(),
                        req.miniId(),
                        req.version(),
                        req.version(),
                        null,
                        e,
                        null,
                        req.deployInfo() != null);
                }
            }
        });
    }

    /**
     * Prepares near transaction.
     *
     * @param nearNodeId Near node ID that initiated transaction.
     * @param req Near prepare request.
     * @return Prepare future.
     */
    private IgniteInternalFuture<GridNearTxPrepareResponse> prepareNearTx(
        final UUID nearNodeId,
        final GridNearTxPrepareRequest req
    ) {
        ClusterNode nearNode = ctx.node(nearNodeId);

        if (nearNode == null) {
            if (txPrepareMsgLog.isDebugEnabled()) {
                txPrepareMsgLog.debug("Received near prepare from node that left grid (will ignore) [" +
                    "txId=" + req.version() +
                    ", node=" + nearNodeId + ']');
            }

            return null;
        }

        IgniteTxEntry firstEntry = null;

        try {
            for (IgniteTxEntry e : F.concat(false, req.reads(), req.writes())) {
                e.unmarshal(ctx, false, ctx.deploy().globalLoader());

                if (firstEntry == null)
                    firstEntry = e;
            }
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        assert firstEntry != null : req;

        GridDhtTxLocal tx = null;

        GridCacheVersion mappedVer = ctx.tm().mappedVersion(req.version());

        if (mappedVer != null) {
            tx = ctx.tm().tx(mappedVer);

            if (tx == null)
                U.warn(log, "Missing local transaction for mapped near version [nearVer=" + req.version()
                    + ", mappedVer=" + mappedVer + ']');
            else {
                if (req.concurrency() == PESSIMISTIC)
                    tx.nearFutureId(req.futureId());
            }
        }
        else {
            GridDhtPartitionTopology top = null;

            if (req.firstClientRequest()) {
                assert req.concurrency() == OPTIMISTIC : req;
                assert CU.clientNode(nearNode) : nearNode;

                top = firstEntry.context().topology();

                top.readLock();
            }

            try {
                if (top != null && needRemap(req.topologyVersion(), top.topologyVersion(), req)) {
                    if (txPrepareMsgLog.isDebugEnabled()) {
                        txPrepareMsgLog.debug("Topology version mismatch for near prepare, need remap transaction [" +
                            "txId=" + req.version() +
                            ", node=" + nearNodeId +
                            ", reqTopVer=" + req.topologyVersion() +
                            ", locTopVer=" + top.topologyVersion() +
                            ", req=" + req + ']');
                    }

                    GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
                        req.version(),
                        req.futureId(),
                        req.miniId(),
                        req.version(),
                        req.version(),
                        null,
                        null,
                        top.topologyVersion(),
                        req.deployInfo() != null);

                    try {
                        ctx.io().send(nearNodeId, res, req.policy());

                        if (txPrepareMsgLog.isDebugEnabled()) {
                            txPrepareMsgLog.debug("Sent remap response for near prepare [txId=" + req.version() +
                                ", node=" + nearNodeId + ']');
                        }
                    }
                    catch (ClusterTopologyCheckedException ignored) {
                        if (txPrepareMsgLog.isDebugEnabled()) {
                            txPrepareMsgLog.debug("Failed to send remap response for near prepare, node failed [" +
                                "txId=" + req.version() +
                                ", node=" + nearNodeId + ']');
                        }
                    }
                    catch (IgniteCheckedException e) {
                        U.error(txPrepareMsgLog, "Failed to send remap response for near prepare " +
                            "[txId=" + req.version() +
                            ", node=" + nearNodeId +
                            ", req=" + req + ']', e);
                    }

                    return new GridFinishedFuture<>(res);
                }

                tx = new GridDhtTxLocal(
                    ctx,
                    req.topologyVersion(),
                    nearNode.id(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    req.threadId(),
                    req.implicitSingle(),
                    req.implicitSingle(),
                    req.system(),
                    req.explicitLock(),
                    req.policy(),
                    req.concurrency(),
                    req.isolation(),
                    req.timeout(),
                    req.isInvalidate(),
                    true,
                    req.onePhaseCommit(),
                    req.txSize(),
                    req.transactionNodes(),
                    req.subjectId(),
                    req.taskNameHash()
                );

                tx = ctx.tm().onCreated(null, tx);

                if (tx != null)
                    tx.topologyVersion(req.topologyVersion());
                else
                    U.warn(log, "Failed to create local transaction (was transaction rolled back?) [xid=" +
                        req.version() + ", req=" + req + ']');
            }
            finally {
                if (tx != null)
                    req.txState(tx.txState());

                if (top != null)
                    top.readUnlock();
            }
        }

        if (tx != null) {
            req.txState(tx.txState());

            if (req.explicitLock())
                tx.explicitLock(true);

            tx.transactionNodes(req.transactionNodes());

            // Set near on originating node flag only if the sender node has new version.
            if (req.near() && FINISH_NEAR_ONE_PHASE_SINCE.compareTo(nearNode.version()) <= 0)
                tx.nearOnOriginatingNode(true);

            if (req.onePhaseCommit()) {
                assert req.last();

                tx.onePhaseCommit(true);
            }

            if (req.returnValue())
                tx.needReturnValue(true);

            IgniteInternalFuture<GridNearTxPrepareResponse> fut = tx.prepareAsync(
                req.reads(),
                req.writes(),
                req.dhtVersions(),
                req.messageId(),
                req.miniId(),
                req.transactionNodes(),
                req.last());

            if (tx.isRollbackOnly() && !tx.commitOnPrepare()) {
                if (tx.state() != TransactionState.ROLLED_BACK && tx.state() != TransactionState.ROLLING_BACK)
                    tx.rollbackAsync();
            }

            final GridDhtTxLocal tx0 = tx;

            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> txFut) {
                    try {
                        txFut.get();
                    }
                    catch (IgniteCheckedException e) {
                        tx0.setRollbackOnly(); // Just in case.

                        if (!X.hasCause(e, IgniteTxOptimisticCheckedException.class) &&
                            !X.hasCause(e, IgniteFutureCancelledException.class) && !ctx.kernalContext().isStopping())
                            U.error(log, "Failed to prepare DHT transaction: " + tx0, e);
                    }
                }
            });

            return fut;
        }
        else
            return new GridFinishedFuture<>((GridNearTxPrepareResponse)null);
    }

    /**
     * @param expVer Expected topology version.
     * @param curVer Current topology version.
     * @param req Request.
     * @return {@code True} if cache affinity changed and request should be remapped.
     */
    private boolean needRemap(AffinityTopologyVersion expVer,
        AffinityTopologyVersion curVer,
        GridNearTxPrepareRequest req) {
        if (expVer.equals(curVer))
            return false;

        for (IgniteTxEntry e : F.concat(false, req.reads(), req.writes())) {
            GridCacheContext ctx = e.context();

            Collection<ClusterNode> cacheNodes0 = ctx.discovery().cacheAffinityNodes(ctx.cacheId(), expVer);
            Collection<ClusterNode> cacheNodes1 = ctx.discovery().cacheAffinityNodes(ctx.cacheId(), curVer);

            if (!cacheNodes0.equals(cacheNodes1) || ctx.affinity().affinityTopologyVersion().compareTo(curVer) < 0)
                return true;

            try {
                List<List<ClusterNode>> aff1 = ctx.affinity().assignments(expVer);
                List<List<ClusterNode>> aff2 = ctx.affinity().assignments(curVer);

                if (!aff1.equals(aff2))
                    return true;
            }
            catch (IllegalStateException ignored) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processNearTxPrepareResponse(UUID nodeId, GridNearTxPrepareResponse res) {
        if (txPrepareMsgLog.isDebugEnabled())
            txPrepareMsgLog.debug("Received near prepare response [txId=" + res.version() + ", node=" + nodeId + ']');

        GridNearTxPrepareFutureAdapter fut = (GridNearTxPrepareFutureAdapter)ctx.mvcc()
            .<IgniteInternalTx>mvccFuture(res.version(), res.futureId());

        if (fut == null) {
            U.warn(log, "Failed to find future for near prepare response [txId=" + res.version() +
                ", node=" + nodeId +
                ", res=" + res + ']');

            return;
        }

        IgniteInternalTx tx = fut.tx();

        assert tx != null;

        res.txState(tx.txState());

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processNearTxFinishResponse(UUID nodeId, GridNearTxFinishResponse res) {
        if (txFinishMsgLog.isDebugEnabled())
            txFinishMsgLog.debug("Received near finish response [txId=" + res.xid() + ", node=" + nodeId + ']');

        ctx.tm().onFinishedRemote(nodeId, res.threadId());

        GridNearTxFinishFuture fut = (GridNearTxFinishFuture)ctx.mvcc().<IgniteInternalTx>future(res.futureId());

        if (fut == null) {
            if (txFinishMsgLog.isDebugEnabled()) {
                txFinishMsgLog.debug("Failed to find future for near finish response [txId=" + res.xid() +
                    ", node=" + nodeId +
                    ", res=" + res + ']');
            }

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxPrepareResponse(UUID nodeId, GridDhtTxPrepareResponse res) {
        GridDhtTxPrepareFuture fut = (GridDhtTxPrepareFuture)ctx.mvcc().mvccFuture(res.version(), res.futureId());

        if (fut == null) {
            if (txPrepareMsgLog.isDebugEnabled()) {
                txPrepareMsgLog.debug("Failed to find future for dht prepare response [txId=null" +
                    ", dhtTxId=" + res.version() +
                    ", node=" + nodeId +
                    ", res=" + res + ']');
            }

            return;
        }
        else if (txPrepareMsgLog.isDebugEnabled())
            txPrepareMsgLog.debug("Received dht prepare response [txId=" + fut.tx().nearXidVersion() + ", node=" + nodeId + ']');

        IgniteInternalTx tx = fut.tx();

        assert tx != null;

        res.txState(tx.txState());

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxFinishResponse(UUID nodeId, GridDhtTxFinishResponse res) {
        assert nodeId != null;
        assert res != null;

        if (res.checkCommitted()) {
            GridNearTxFinishFuture fut = (GridNearTxFinishFuture)ctx.mvcc().<IgniteInternalTx>future(res.futureId());

            if (fut == null) {
                if (txFinishMsgLog.isDebugEnabled()) {
                    txFinishMsgLog.debug("Failed to find future for dht finish check committed response [txId=null" +
                        ", dhtTxId=" + res.xid() +
                        ", node=" + nodeId +
                        ", res=" + res + ']');
                }

                return;
            }
            else if (txFinishMsgLog.isDebugEnabled()) {
                txFinishMsgLog.debug("Received dht finish check committed response [txId=" + fut.tx().nearXidVersion() +
                    ", dhtTxId=" + res.xid() +
                    ", node=" + nodeId + ']');
            }

            fut.onResult(nodeId, res);
        }
        else {
            GridDhtTxFinishFuture fut = (GridDhtTxFinishFuture)ctx.mvcc().<IgniteInternalTx>future(res.futureId());

            if (fut == null) {
                if (txFinishMsgLog.isDebugEnabled()) {
                    txFinishMsgLog.debug("Failed to find future for dht finish response [txId=null" +
                        ", dhtTxId=" + res.xid() +
                        ", node=" + nodeId +
                        ", res=" + res);
                }

                return;
            }
            else if (txFinishMsgLog.isDebugEnabled()) {
                txFinishMsgLog.debug("Received dht finish response [txId=" + fut.tx().nearXidVersion() +
                    ", dhtTxId=" + res.xid() +
                    ", node=" + nodeId + ']');
            }

            fut.onResult(nodeId, res);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    @Nullable private IgniteInternalFuture<IgniteInternalTx> processNearTxFinishRequest(UUID nodeId,
        GridNearTxFinishRequest req) {
        if (txFinishMsgLog.isDebugEnabled())
            txFinishMsgLog.debug("Received near finish request [txId=" + req.version() + ", node=" + nodeId + ']');

        IgniteInternalFuture<IgniteInternalTx> fut = finish(nodeId, null, req);

        assert req.txState() != null || (fut != null && fut.error() != null) ||
            (ctx.tm().tx(req.version()) == null && ctx.tm().nearTx(req.version()) == null);

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param locTx Local transaction.
     * @param req Request.
     * @return Future.
     */
    @Nullable public IgniteInternalFuture<IgniteInternalTx> finish(UUID nodeId, @Nullable GridNearTxLocal locTx,
        GridNearTxFinishRequest req) {
        assert nodeId != null;
        assert req != null;

        if (locTx != null)
            req.txState(locTx.txState());

        // 'baseVersion' message field is re-used for version to be added in completed versions.
        if (!req.commit() && req.baseVersion() != null)
            ctx.tm().addRolledbackTx(null, req.baseVersion());

        // Transaction on local cache only.
        if (locTx != null && !locTx.nearLocallyMapped() && !locTx.colocatedLocallyMapped())
            return new GridFinishedFuture<IgniteInternalTx>(locTx);

        if (log.isDebugEnabled())
            log.debug("Processing near tx finish request [nodeId=" + nodeId + ", req=" + req + "]");

        IgniteInternalFuture<IgniteInternalTx> colocatedFinishFut = null;

        if (locTx != null && locTx.colocatedLocallyMapped())
            colocatedFinishFut = finishColocatedLocal(req.commit(), locTx);

        IgniteInternalFuture<IgniteInternalTx> nearFinishFut = null;

        if (locTx == null || locTx.nearLocallyMapped())
            nearFinishFut = finishDhtLocal(nodeId, locTx, req);

        if (colocatedFinishFut != null && nearFinishFut != null) {
            GridCompoundFuture<IgniteInternalTx, IgniteInternalTx> res = new GridCompoundFuture<>();

            res.add(colocatedFinishFut);
            res.add(nearFinishFut);

            res.markInitialized();

            return res;
        }

        if (colocatedFinishFut != null)
            return colocatedFinishFut;

        return nearFinishFut;
    }

    /**
     * @param nodeId Node ID initiated commit.
     * @param locTx Optional local transaction.
     * @param req Finish request.
     * @return Finish future.
     */
    private IgniteInternalFuture<IgniteInternalTx> finishDhtLocal(UUID nodeId, @Nullable GridNearTxLocal locTx,
        GridNearTxFinishRequest req) {
        GridCacheVersion dhtVer = ctx.tm().mappedVersion(req.version());

        GridDhtTxLocal tx = null;

        if (dhtVer == null) {
            if (log.isDebugEnabled())
                log.debug("Received transaction finish request for unknown near version (was lock explicit?): " + req);
        }
        else
            tx = ctx.tm().tx(dhtVer);

        if (tx != null)
            req.txState(tx.txState());

        if (tx == null && locTx != null && !req.commit()) {
            U.warn(log, "DHT local tx not found for near local tx rollback " +
                "[req=" + req + ", dhtVer=" + dhtVer + ", tx=" + locTx + ']');

            return null;
        }

        if (tx == null && !req.explicitLock()) {
            assert locTx == null : "DHT local tx should never be lost for near local tx: " + locTx;

            U.warn(txFinishMsgLog, "Received finish request for completed transaction (the message may be too late) [" +
                "txId=" + req.version() +
                ", dhtTxId=" + dhtVer +
                ", node=" + nodeId +
                ", commit=" + req.commit() + ']');

            // Always send finish response.
            GridCacheMessage res = new GridNearTxFinishResponse(req.version(), req.threadId(), req.futureId(),
                req.miniId(), new IgniteCheckedException("Transaction has been already completed."));

            try {
                ctx.io().send(nodeId, res, req.policy());

                if (txFinishMsgLog.isDebugEnabled()) {
                    txFinishMsgLog.debug("Sent near finish response for completed tx [txId=" + req.version() +
                        ", dhtTxId=" + dhtVer +
                        ", node=" + nodeId + ']');
                }
            }
            catch (Throwable e) {
                // Double-check.
                if (ctx.discovery().node(nodeId) == null) {
                    if (txFinishMsgLog.isDebugEnabled()) {
                        txFinishMsgLog.debug("Failed to send near finish response for completed tx, node failed [" +
                            "txId=" + req.version() +
                            ", dhtTxId=" + dhtVer +
                            ", node=" + nodeId + ']');
                    }
                }
                else {
                    U.error(txFinishMsgLog, "Failed to send near finish response for completed tx, node failed [" +
                        "txId=" + req.version() +
                        ", dhtTxId=" + dhtVer +
                        ", node=" + nodeId +
                        ", req=" + req +
                        ", res=" + res + ']', e);
                }

                if (e instanceof Error)
                    throw (Error)e;
            }

            return null;
        }

        try {
            assert tx != null : "Transaction is null for near finish request [nodeId=" +
                nodeId + ", req=" + req + "]";

            if (req.syncMode() == null) {
                boolean sync = req.commit() ? req.syncCommit() : req.syncRollback();

                tx.syncMode(sync ? FULL_SYNC : FULL_ASYNC);
            }
            else
                tx.syncMode(req.syncMode());

            if (req.commit()) {
                tx.storeEnabled(req.storeEnabled());

                if (!tx.markFinalizing(USER_FINISH)) {
                    if (log.isDebugEnabled())
                        log.debug("Will not finish transaction (it is handled by another thread): " + tx);

                    return null;
                }

                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());

                IgniteInternalFuture<IgniteInternalTx> commitFut = tx.commitAsync();

                // Only for error logging.
                commitFut.listen(CU.errorLogger(log));

                return commitFut;
            }
            else {
                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());

                IgniteInternalFuture<IgniteInternalTx> rollbackFut = tx.rollbackAsync();

                // Only for error logging.
                rollbackFut.listen(CU.errorLogger(log));

                return rollbackFut;
            }
        }
        catch (Throwable e) {
            tx.commitError(e);

            tx.systemInvalidate(true);

            U.error(log, "Failed completing transaction [commit=" + req.commit() + ", tx=" + tx + ']', e);

            IgniteInternalFuture<IgniteInternalTx> res;

            IgniteInternalFuture<IgniteInternalTx> rollbackFut = tx.rollbackAsync();

            // Only for error logging.
            rollbackFut.listen(CU.errorLogger(log));

            res = rollbackFut;

            if (e instanceof Error)
                throw (Error)e;

            return res;
        }
    }

    /**
     * @param commit Commit flag (rollback if {@code false}).
     * @param tx Transaction to commit.
     * @return Future.
     */
    public IgniteInternalFuture<IgniteInternalTx> finishColocatedLocal(boolean commit, GridNearTxLocal tx) {
        try {
            if (commit) {
                if (!tx.markFinalizing(USER_FINISH)) {
                    if (log.isDebugEnabled())
                        log.debug("Will not finish transaction (it is handled by another thread): " + tx);

                    return null;
                }

                return tx.commitAsyncLocal();
            }
            else
                return tx.rollbackAsyncLocal();
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + commit + ", tx=" + tx + ']', e);

            if (e instanceof Error)
                throw e;

            if (tx != null)
                return tx.rollbackAsync();

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    protected final void processDhtTxPrepareRequest(final UUID nodeId, final GridDhtTxPrepareRequest req) {
        if (txPrepareMsgLog.isDebugEnabled()) {
            txPrepareMsgLog.debug("Received dht prepare request [txId=" + req.nearXidVersion() +
                ", dhtTxId=" + req.version() +
                ", node=" + nodeId + ']');
        }

        assert nodeId != null;
        assert req != null;

        assert req.transactionNodes() != null;

        GridDhtTxRemote dhtTx = null;
        GridNearTxRemote nearTx = null;

        GridDhtTxPrepareResponse res;

        try {
            res = new GridDhtTxPrepareResponse(req.version(), req.futureId(), req.miniId(), req.deployInfo() != null);

            // Start near transaction first.
            nearTx = !F.isEmpty(req.nearWrites()) ? startNearRemoteTx(ctx.deploy().globalLoader(), nodeId, req) : null;
            dhtTx = startRemoteTx(nodeId, req, res);

            // Set evicted keys from near transaction.
            if (nearTx != null)
                res.nearEvicted(nearTx.evicted());

            if (dhtTx != null)
                req.txState(dhtTx.txState());
            else if (nearTx != null)
                req.txState(nearTx.txState());

            if (dhtTx != null && !F.isEmpty(dhtTx.invalidPartitions()))
                res.invalidPartitionsByCacheId(dhtTx.invalidPartitions());

            if (req.onePhaseCommit()) {
                assert req.last();

                if (dhtTx != null) {
                    dhtTx.onePhaseCommit(true);
                    dhtTx.needReturnValue(req.needReturnValue());

                    finish(dhtTx, req);
                }

                if (nearTx != null) {
                    nearTx.onePhaseCommit(true);

                    finish(nearTx, req);
                }
            }
        }
        catch (IgniteCheckedException e) {
            if (e instanceof IgniteTxRollbackCheckedException)
                U.error(log, "Transaction was rolled back before prepare completed: " + req, e);
            else if (e instanceof IgniteTxOptimisticCheckedException) {
                if (log.isDebugEnabled())
                    log.debug("Optimistic failure for remote transaction (will rollback): " + req);
            }
            else if (e instanceof IgniteTxHeuristicCheckedException) {
                U.warn(log, "Failed to commit transaction (all transaction entries were invalidated): " +
                    CU.txString(dhtTx));
            }
            else
                U.error(log, "Failed to process prepare request: " + req, e);

            if (nearTx != null)
                nearTx.rollback();

            res = new GridDhtTxPrepareResponse(req.version(), req.futureId(), req.miniId(), e,
                req.deployInfo() != null);
        }

        if (req.onePhaseCommit()) {
            IgniteInternalFuture completeFut;

            IgniteInternalFuture<IgniteInternalTx> dhtFin = dhtTx == null ?
                null : dhtTx.done() ? null : dhtTx.finishFuture();

            final IgniteInternalFuture<IgniteInternalTx> nearFin = nearTx == null ?
                null : nearTx.done() ? null : nearTx.finishFuture();

            if (dhtFin != null && nearFin != null) {
                GridCompoundFuture fut = new GridCompoundFuture();

                fut.add(dhtFin);
                fut.add(nearFin);

                fut.markInitialized();

                completeFut = fut;
            }
            else
                completeFut = dhtFin != null ? dhtFin : nearFin;

            if (completeFut != null) {
                final GridDhtTxPrepareResponse res0 = res;
                final GridDhtTxRemote dhtTx0 = dhtTx;
                final GridNearTxRemote nearTx0 = nearTx;

                completeFut.listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                        sendReply(nodeId, req, res0, dhtTx0, nearTx0);
                    }
                });
            }
            else
                sendReply(nodeId, req, res, dhtTx, nearTx);
        }
        else
            sendReply(nodeId, req, res, dhtTx, nearTx);

        assert req.txState() != null || res.error() != null ||
            (ctx.tm().tx(req.version()) == null && ctx.tm().nearTx(req.version()) == null);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected final void processDhtTxOnePhaseCommitAckRequest(final UUID nodeId,
        final GridDhtTxOnePhaseCommitAckRequest req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx one phase commit ack request [nodeId=" + nodeId + ", req=" + req + ']');

        for (GridCacheVersion ver : req.versions())
            ctx.tm().removeTxReturn(ver);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"unchecked"})
    protected final void processDhtTxFinishRequest(final UUID nodeId, final GridDhtTxFinishRequest req) {
        assert nodeId != null;
        assert req != null;

        if (req.checkCommitted()) {
            boolean committed = req.waitRemoteTransactions() || !ctx.tm().addRolledbackTx(null, req.version());

            if (!committed || !req.syncCommit())
                sendReply(nodeId, req, committed, null);
            else {
                IgniteInternalFuture<?> fut = ctx.tm().remoteTxFinishFuture(req.version());

                fut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> fut) {
                        sendReply(nodeId, req, true, null);
                    }
                });
            }

            return;
        }

        final GridDhtTxRemote dhtTx = ctx.tm().tx(req.version());
        GridNearTxRemote nearTx = ctx.tm().nearTx(req.version());

        final GridCacheVersion nearTxId =
            (dhtTx != null ? dhtTx.nearXidVersion() : (nearTx != null ? nearTx.nearXidVersion() : null));

        if (txFinishMsgLog.isDebugEnabled()) {
            txFinishMsgLog.debug("Received dht finish request [txId=" + nearTxId +
                ", dhtTxId=" + req.version() +
                ", node=" + nodeId + ']');
        }

        // Safety - local transaction will finish explicitly.
        if (nearTx != null && nearTx.local())
            nearTx = null;

        finish(nodeId, dhtTx, req);

        if (nearTx != null)
            finish(nodeId, nearTx, req);

        if (req.replyRequired()) {
            IgniteInternalFuture completeFut;

            IgniteInternalFuture<IgniteInternalTx> dhtFin = dhtTx == null ?
                null : dhtTx.done() ? null : dhtTx.finishFuture();

            final IgniteInternalFuture<IgniteInternalTx> nearFin = nearTx == null ?
                null : nearTx.done() ? null : nearTx.finishFuture();

            if (dhtFin != null && nearFin != null) {
                GridCompoundFuture fut = new GridCompoundFuture();

                fut.add(dhtFin);
                fut.add(nearFin);

                fut.markInitialized();

                completeFut = fut;
            }
            else
                completeFut = dhtFin != null ? dhtFin : nearFin;

            if (completeFut != null) {
                completeFut.listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> igniteTxIgniteFuture) {
                        sendReply(nodeId, req, true, nearTxId);
                    }
                });
            }
            else
                sendReply(nodeId, req, true, nearTxId);
        }
        else
            sendReply(nodeId, req, true, null);

        assert req.txState() != null || (ctx.tm().tx(req.version()) == null && ctx.tm().nearTx(req.version()) == null) : req;
    }

    /**
     * @param nodeId Node ID.
     * @param tx Transaction.
     * @param req Request.
     */
    protected void finish(
        UUID nodeId,
        IgniteTxRemoteEx tx,
        GridDhtTxFinishRequest req
    ) {
        // We don't allow explicit locks for transactions and
        // therefore immediately return if transaction is null.
        // However, we may decide to relax this restriction in
        // future.
        if (tx == null) {
            if (req.commit())
                // Must be some long time duplicate, but we add it anyway.
                ctx.tm().addCommittedTx(tx, req.version(), null);
            else
                ctx.tm().addRolledbackTx(tx, req.version());

            if (log.isDebugEnabled())
                log.debug("Received finish request for non-existing transaction (added to completed set) " +
                    "[senderNodeId=" + nodeId + ", res=" + req + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Received finish request for transaction [senderNodeId=" + nodeId + ", req=" + req +
                ", tx=" + tx + ']');

        req.txState(tx.txState());

        try {
            if (req.commit() || req.isSystemInvalidate()) {
                tx.commitVersion(req.commitVersion());
                tx.invalidate(req.isInvalidate());
                tx.systemInvalidate(req.isSystemInvalidate());

                // Complete remote candidates.
                tx.doneRemote(req.baseVersion(), null, null, null);

                tx.setPartitionUpdateCounters(
                    req.partUpdateCounters() != null ? req.partUpdateCounters().array() : null);

                tx.commit();
            }
            else {
                tx.doneRemote(req.baseVersion(), null, null, null);

                tx.rollback();
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + req.commit() + ", tx=" + tx + ']', e);

            // Mark transaction for invalidate.
            tx.invalidate(true);
            tx.systemInvalidate(true);

            try {
                tx.commit();
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to invalidate transaction: " + tx, ex);
            }

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @param tx Transaction.
     * @param req Request.
     */
    protected void finish(
        GridDistributedTxRemoteAdapter tx,
        GridDhtTxPrepareRequest req) throws IgniteTxHeuristicCheckedException {
        assert tx != null : "No transaction for one-phase commit prepare request: " + req;

        try {
            tx.commitVersion(req.writeVersion());
            tx.invalidate(req.isInvalidate());

            // Complete remote candidates.
            tx.doneRemote(req.version(), null, null, null);

            tx.commit();
        }
        catch (IgniteTxHeuristicCheckedException e) {
            // Just rethrow this exception. Transaction was already uncommitted.
            throw e;
        }
        catch (Throwable e) {
            U.error(log, "Failed committing transaction [tx=" + tx + ']', e);

            // Mark transaction for invalidate.
            tx.invalidate(true);
            tx.systemInvalidate(true);

            tx.rollback();

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @param nodeId Node id.
     * @param req Request.
     * @param res Response.
     * @param dhtTx Dht tx.
     * @param nearTx Near tx.
     */
    protected void sendReply(UUID nodeId,
        GridDhtTxPrepareRequest req,
        GridDhtTxPrepareResponse res,
        GridDhtTxRemote dhtTx,
        GridNearTxRemote nearTx) {
        try {
            // Reply back to sender.
            ctx.io().send(nodeId, res, req.policy());

            if (txPrepareMsgLog.isDebugEnabled()) {
                txPrepareMsgLog.debug("Sent dht prepare response [txId=" + req.nearXidVersion() +
                    ", dhtTxId=" + req.version() +
                    ", node=" + nodeId + ']');
            }
        }
        catch (IgniteCheckedException e) {
            if (e instanceof ClusterTopologyCheckedException) {
                if (txPrepareMsgLog.isDebugEnabled()) {
                    txPrepareMsgLog.debug("Failed to send dht prepare response, node left [txId=" + req.nearXidVersion() +
                        ", dhtTxId=" + req.version() +
                        ", node=" + nodeId + ']');
                }
            }
            else {
                U.warn(log, "Failed to send tx response to remote node (will rollback transaction) [" +
                    "txId=" + req.nearXidVersion() +
                    ", dhtTxId=" + req.version() +
                    ", node=" + nodeId +
                    ", err=" + e.getMessage() + ']');
            }

            if (nearTx != null)
                nearTx.rollback();

            if (dhtTx != null)
                dhtTx.rollback();
        }
    }

    /**
     * Sends tx finish response to remote node, if response is requested.
     *
     * @param nodeId Node id that originated finish request.
     * @param req Request.
     * @param committed {@code True} if transaction committed on this node.
     * @param nearTxId Near tx version.
     */
    protected void sendReply(UUID nodeId, GridDhtTxFinishRequest req, boolean committed, GridCacheVersion nearTxId) {
        if (req.replyRequired() || req.checkCommitted()) {
            GridDhtTxFinishResponse res = new GridDhtTxFinishResponse(req.version(), req.futureId(), req.miniId());

            if (req.checkCommitted()) {
                res.checkCommitted(true);

                if (committed) {
                    if (req.needReturnValue()) {
                        try {
                            GridCacheReturnCompletableWrapper wrapper = ctx.tm().getCommittedTxReturn(req.version());

                            if (wrapper != null)
                                res.returnValue(wrapper.fut().get());
                            else
                                assert !ctx.discovery().alive(nodeId) : nodeId;
                        }
                        catch (IgniteCheckedException ignored) {
                            if (txFinishMsgLog.isDebugEnabled()) {
                                txFinishMsgLog.debug("Failed to gain entry processor return value. [txId=" + nearTxId +
                                    ", dhtTxId=" + req.version() +
                                    ", node=" + nodeId + ']');
                            }
                        }
                    }
                }
                else {
                    ClusterTopologyCheckedException cause =
                        new ClusterTopologyCheckedException("Primary node left grid.");

                    res.checkCommittedError(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                        "(transaction has been rolled back on backup node): " + req.version(), cause));
                }
            }

            try {
                ctx.io().send(nodeId, res, req.policy());

                if (txFinishMsgLog.isDebugEnabled()) {
                    txFinishMsgLog.debug("Sent dht tx finish response [txId=" + nearTxId +
                        ", dhtTxId=" + req.version() +
                        ", node=" + nodeId +
                        ", checkCommitted=" + req.checkCommitted() + ']');
                }
            }
            catch (Throwable e) {
                // Double-check.
                if (ctx.discovery().node(nodeId) == null) {
                    if (txFinishMsgLog.isDebugEnabled()) {
                        txFinishMsgLog.debug("Node left while send dht tx finish response [txId=" + nearTxId +
                            ", dhtTxId=" + req.version() +
                            ", node=" + nodeId + ']');
                    }
                }
                else {
                    U.error(log, "Failed to send finish response to node [txId=" + nearTxId +
                        ", dhtTxId=" + req.version() +
                        ", nodeId=" + nodeId +
                        ", res=" + res + ']', e);
                }

                if (e instanceof Error)
                    throw (Error)e;
            }
        }
        else {
            if (txFinishMsgLog.isDebugEnabled()) {
                txFinishMsgLog.debug("Skip send dht tx finish response [txId=" + nearTxId +
                    ", dhtTxId=" + req.version() +
                    ", node=" + nodeId + ']');
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @param res Response.
     * @return Remote transaction.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable GridDhtTxRemote startRemoteTx(
        UUID nodeId,
        GridDhtTxPrepareRequest req,
        GridDhtTxPrepareResponse res
    ) throws IgniteCheckedException {
        if (!F.isEmpty(req.writes())) {
            GridDhtTxRemote tx = ctx.tm().tx(req.version());

            if (tx == null) {
                boolean single = req.last() && req.writes().size() == 1;

                tx = new GridDhtTxRemote(
                    ctx,
                    req.nearNodeId(),
                    req.futureId(),
                    nodeId,
                    req.topologyVersion(),
                    req.version(),
                    null,
                    req.system(),
                    req.policy(),
                    req.concurrency(),
                    req.isolation(),
                    req.isInvalidate(),
                    req.timeout(),
                    req.writes() != null ? Math.max(req.writes().size(), req.txSize()) : req.txSize(),
                    req.nearXidVersion(),
                    req.transactionNodes(),
                    req.subjectId(),
                    req.taskNameHash(),
                    single);

                tx.writeVersion(req.writeVersion());

                tx = ctx.tm().onCreated(null, tx);

                if (tx == null || !ctx.tm().onStarted(tx)) {
                    if (log.isDebugEnabled())
                        log.debug("Attempt to start a completed transaction (will ignore): " + tx);

                    return null;
                }

                if (ctx.discovery().node(nodeId) == null) {
                    tx.state(ROLLING_BACK);

                    tx.state(ROLLED_BACK);

                    ctx.tm().uncommitTx(tx);

                    return null;
                }
            }
            else {
                tx.writeVersion(req.writeVersion());
                tx.transactionNodes(req.transactionNodes());
            }

            if (!tx.isSystemInvalidate()) {
                int idx = 0;

                for (IgniteTxEntry entry : req.writes()) {
                    GridCacheContext cacheCtx = entry.context();

                    tx.addWrite(entry, ctx.deploy().globalLoader());

                    if (isNearEnabled(cacheCtx) && req.invalidateNearEntry(idx))
                        invalidateNearEntry(cacheCtx, entry.key(), req.version());

                    try {
                        if (req.needPreloadKey(idx)) {
                            GridCacheEntryEx cached = entry.cached();

                            if (cached == null)
                                cached = cacheCtx.cache().entryEx(entry.key(), req.topologyVersion());

                            GridCacheEntryInfo info = cached.info();

                            if (info != null && !info.isNew() && !info.isDeleted())
                                res.addPreloadEntry(info);
                        }

                        if (cacheCtx.readThroughConfigured() &&
                            !entry.skipStore() &&
                            entry.op() == TRANSFORM &&
                            entry.oldValueOnPrimary() &&
                            !entry.hasValue()) {
                            while (true) {
                                try {
                                    GridCacheEntryEx cached = entry.cached();

                                    if (cached == null)
                                        cached = cacheCtx.cache().entryEx(entry.key(), req.topologyVersion());

                                    CacheObject val = cached.innerGet(
                                        /*ver*/null,
                                        tx,
                                        /*readSwap*/true,
                                        /*readThrough*/false,
                                        /*updateMetrics*/false,
                                        /*evt*/false,
                                        /*tmp*/false,
                                        tx.subjectId(),
                                        /*transformClo*/null,
                                        tx.resolveTaskName(),
                                        /*expiryPlc*/null,
                                        /*keepBinary*/true);

                                    if (val == null)
                                        val = cacheCtx.toCacheObject(cacheCtx.store().load(null, entry.key()));

                                    if (val != null)
                                        entry.readValue(val);

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got entry removed exception, will retry: " + entry.txKey());

                                    entry.cached(cacheCtx.cache().entryEx(entry.key(), req.topologyVersion()));
                                }
                            }
                        }
                    }
                    catch (GridDhtInvalidPartitionException e) {
                        tx.addInvalidPartition(cacheCtx, e.partition());

                        tx.clearEntry(entry.txKey());
                    }

                    idx++;
                }
            }

            // Prepare prior to reordering, so the pending locks added
            // in prepare phase will get properly ordered as well.
            tx.prepare();

            if (req.last()) {
                assert !F.isEmpty(req.transactionNodes()) :
                    "Received last prepare request with empty transaction nodes: " + req;

                tx.transactionNodes(req.transactionNodes());

                tx.state(PREPARED);
            }

            res.invalidPartitionsByCacheId(tx.invalidPartitions());

            if (tx.empty() && req.last()) {
                tx.rollback();

                return null;
            }

            return tx;
        }

        return null;
    }

    /**
     * @param cacheCtx Context.
     * @param key Key
     * @param ver Version.
     * @throws IgniteCheckedException If invalidate failed.
     */
    private void invalidateNearEntry(GridCacheContext cacheCtx, KeyCacheObject key, GridCacheVersion ver)
        throws IgniteCheckedException {
        GridNearCacheAdapter near = cacheCtx.isNear() ? cacheCtx.near() : cacheCtx.dht().near();

        GridCacheEntryEx nearEntry = near.peekEx(key);

        if (nearEntry != null)
            nearEntry.invalidate(null, ver);
    }

    /**
     * Called while processing dht tx prepare request.
     *
     * @param ldr Loader.
     * @param nodeId Sender node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridNearTxRemote startNearRemoteTx(ClassLoader ldr, UUID nodeId,
        GridDhtTxPrepareRequest req) throws IgniteCheckedException {

        if (!F.isEmpty(req.nearWrites())) {
            GridNearTxRemote tx = ctx.tm().nearTx(req.version());

            if (tx == null) {
                tx = new GridNearTxRemote(
                    ctx,
                    req.topologyVersion(),
                    ldr,
                    nodeId,
                    req.nearNodeId(),
                    req.version(),
                    null,
                    req.system(),
                    req.policy(),
                    req.concurrency(),
                    req.isolation(),
                    req.isInvalidate(),
                    req.timeout(),
                    req.nearWrites(),
                    req.txSize(),
                    req.subjectId(),
                    req.taskNameHash()
                );

                tx.writeVersion(req.writeVersion());

                if (!tx.empty()) {
                    tx = ctx.tm().onCreated(null, tx);

                    if (tx == null || !ctx.tm().onStarted(tx))
                        throw new IgniteTxRollbackCheckedException("Attempt to start a completed transaction: " + tx);
                }
            }
            else
                tx.addEntries(ldr, req.nearWrites());

            tx.ownedVersions(req.owned());

            // Prepare prior to reordering, so the pending locks added
            // in prepare phase will get properly ordered as well.
            tx.prepare();

            if (req.last())
                tx.state(PREPARED);

            return tx;
        }

        return null;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected void processCheckPreparedTxRequest(final UUID nodeId,
        final GridCacheTxRecoveryRequest req) {
        if (txRecoveryMsgLog.isDebugEnabled()) {
            txRecoveryMsgLog.debug("Received tx recovery request [txId=" + req.nearXidVersion() +
                ", node=" + nodeId + ']');
        }

        IgniteInternalFuture<Boolean> fut = req.nearTxCheck() ? ctx.tm().txCommitted(req.nearXidVersion()) :
            ctx.tm().txsPreparedOrCommitted(req.nearXidVersion(), req.transactions());

        if (fut == null || fut.isDone()) {
            boolean prepared;

            try {
                prepared = fut == null ? true : fut.get();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Check prepared transaction future failed [req=" + req + ']', e);

                prepared = false;
            }

            sendCheckPreparedResponse(nodeId, req, prepared);
        }
        else {
            fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                    boolean prepared;

                    try {
                        prepared = fut.get();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Check prepared transaction future failed [req=" + req + ']', e);

                        prepared = false;
                    }

                    sendCheckPreparedResponse(nodeId, req, prepared);
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @param prepared {@code True} if all transaction prepared or committed.
     */
    private void sendCheckPreparedResponse(UUID nodeId,
        GridCacheTxRecoveryRequest req,
        boolean prepared) {
        GridCacheTxRecoveryResponse res = new GridCacheTxRecoveryResponse(req.version(),
            req.futureId(),
            req.miniId(),
            prepared,
            req.deployInfo() != null);

        try {
            ctx.io().send(nodeId, res, req.system() ? UTILITY_CACHE_POOL : SYSTEM_POOL);

            if (txRecoveryMsgLog.isDebugEnabled()) {
                txRecoveryMsgLog.debug("Sent tx recovery response [txId=" + req.nearXidVersion() +
                    ", node=" + nodeId + ", res=" + res + ']');
            }
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (txRecoveryMsgLog.isDebugEnabled())
                txRecoveryMsgLog.debug("Failed to send tx recovery response, node failed [" +
                    ", txId=" + req.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(txRecoveryMsgLog, "Failed to send tx recovery response [txId=" + req.nearXidVersion() +
                ", node=" + nodeId +
                ", req=" + req +
                ", res=" + res + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    protected void processCheckPreparedTxResponse(UUID nodeId, GridCacheTxRecoveryResponse res) {
        if (txRecoveryMsgLog.isDebugEnabled()) {
            txRecoveryMsgLog.debug("Received tx recovery response [txId=" + res.version() +
                ", node=" + nodeId +
                ", res=" + res + ']');
        }

        GridCacheTxRecoveryFuture fut = (GridCacheTxRecoveryFuture)ctx.mvcc().future(res.futureId());

        if (fut == null) {
            if (txRecoveryMsgLog.isDebugEnabled()) {
                txRecoveryMsgLog.debug("Failed to find future for tx recovery response [txId=" + res.version() +
                    ", node=" + nodeId + ", res=" + res + ']');
            }

            return;
        }
        else
            res.txState(fut.tx().txState());

        fut.onResult(nodeId, res);
    }
}
