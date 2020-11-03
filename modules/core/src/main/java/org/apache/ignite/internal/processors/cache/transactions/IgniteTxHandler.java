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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxOnePhaseCommitAckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridInvokeValue;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxRemote;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastResponse;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.USER_FINISH;
import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_FINISH_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_FINISH_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_ONE_PHASE_COMMIT_ACK_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_PREPARE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_PREPARE_RESP;
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
     * @param nearNodeId Sender node ID.
     * @param req Request.
     */
    private void processNearTxPrepareRequest(UUID nearNodeId, GridNearTxPrepareRequest req) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_NEAR_PREPARE_REQ, MTC.span()))) {
            if (txPrepareMsgLog.isDebugEnabled()) {
                txPrepareMsgLog.debug("Received near prepare request [txId=" + req.version() +
                    ", node=" + nearNodeId + ']');
            }

            ClusterNode nearNode = ctx.node(nearNodeId);

            if (nearNode == null) {
                if (txPrepareMsgLog.isDebugEnabled()) {
                    txPrepareMsgLog.debug("Received near prepare from node that left grid (will ignore) [" +
                        "txId=" + req.version() +
                        ", node=" + nearNodeId + ']');
                }

                return;
            }

            processNearTxPrepareRequest0(nearNode, req);
        }
    }

    /**
     * @param nearNode Sender node.
     * @param req Request.
     */
    private IgniteInternalFuture<GridNearTxPrepareResponse> processNearTxPrepareRequest0(ClusterNode nearNode, GridNearTxPrepareRequest req) {
        IgniteInternalFuture<GridNearTxPrepareResponse> fut;

        if (req.firstClientRequest() && req.allowWaitTopologyFuture()) {
            for (;;) {
                if (waitForExchangeFuture(nearNode, req))
                    return new GridFinishedFuture<>();

                fut = prepareNearTx(nearNode, req);

                if (fut != null)
                    break;
            }
        }
        else
            fut = prepareNearTx(nearNode, req);

        assert req.txState() != null || fut == null || fut.error() != null ||
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

        ctx.io().addCacheHandler(0, GridNearTxPrepareRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxPrepareRequest(nodeId, (GridNearTxPrepareRequest)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridNearTxPrepareResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxPrepareResponse(nodeId, (GridNearTxPrepareResponse)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridNearTxFinishRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxFinishRequest(nodeId, (GridNearTxFinishRequest)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridNearTxFinishResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processNearTxFinishResponse(nodeId, (GridNearTxFinishResponse)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridDhtTxPrepareRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxPrepareRequest(nodeId, (GridDhtTxPrepareRequest)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridDhtTxPrepareResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxPrepareResponse(nodeId, (GridDhtTxPrepareResponse)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridDhtTxFinishRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxFinishRequest(nodeId, (GridDhtTxFinishRequest)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridDhtTxOnePhaseCommitAckRequest.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxOnePhaseCommitAckRequest(nodeId, (GridDhtTxOnePhaseCommitAckRequest)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridDhtTxFinishResponse.class, new CI2<UUID, GridCacheMessage>() {
            @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                processDhtTxFinishResponse(nodeId, (GridDhtTxFinishResponse)msg);
            }
        });

        ctx.io().addCacheHandler(0, GridCacheTxRecoveryRequest.class,
            new CI2<UUID, GridCacheTxRecoveryRequest>() {
                @Override public void apply(UUID nodeId, GridCacheTxRecoveryRequest req) {
                    processCheckPreparedTxRequest(nodeId, req);
                }
            });

        ctx.io().addCacheHandler(0, GridCacheTxRecoveryResponse.class,
            new CI2<UUID, GridCacheTxRecoveryResponse>() {
                @Override public void apply(UUID nodeId, GridCacheTxRecoveryResponse res) {
                    processCheckPreparedTxResponse(nodeId, res);
                }
            });

        ctx.io().addCacheHandler(0, PartitionCountersNeighborcastRequest.class,
            new CI2<UUID, PartitionCountersNeighborcastRequest>() {
                @Override public void apply(UUID nodeId, PartitionCountersNeighborcastRequest req) {
                    processPartitionCountersRequest(nodeId, req);
                }
            });

        ctx.io().addCacheHandler(0, PartitionCountersNeighborcastResponse.class,
            new CI2<UUID, PartitionCountersNeighborcastResponse>() {
                @Override public void apply(UUID nodeId, PartitionCountersNeighborcastResponse res) {
                    processPartitionCountersResponse(nodeId, res);
                }
            });
    }

    /**
     * Prepares local colocated tx.
     *
     * @param locTx Local transaction.
     * @param req Near prepare request.
     * @return Prepare future.
     */
    public IgniteInternalFuture<GridNearTxPrepareResponse> prepareColocatedTx(
        final GridNearTxLocal locTx,
        final GridNearTxPrepareRequest req
    ) {
        req.txState(locTx.txState());

        IgniteInternalFuture<GridNearTxPrepareResponse> fut = locTx.prepareAsyncLocal(req);

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
                        req.partition(),
                        req.version(),
                        req.futureId(),
                        req.miniId(),
                        req.version(),
                        req.version(),
                        null,
                        e,
                        null,
                        req.onePhaseCommit(),
                        req.deployInfo() != null);
                }
            }
        });
    }

    /**
     * @param entries Entries.
     * @return First entry.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteTxEntry unmarshal(@Nullable Collection<IgniteTxEntry> entries) throws IgniteCheckedException {
        if (entries == null)
            return null;

        IgniteTxEntry firstEntry = null;

        for (IgniteTxEntry e : entries) {
            e.unmarshal(ctx, false, ctx.deploy().globalLoader());

            if (firstEntry == null)
                firstEntry = e;
        }

        return firstEntry;
    }

    /**
     * @param originTx Transaction for copy.
     * @param req Request.
     * @return Prepare future.
     */
    public IgniteInternalFuture<GridNearTxPrepareResponse> prepareNearTxLocal(
        final GridNearTxLocal originTx,
        final GridNearTxPrepareRequest req) {
        // Make sure not to provide Near entries to DHT cache.
        req.cloneEntries();

        return prepareNearTx(originTx, ctx.localNode(), req);
    }

    /**
     * @param nearNode Node that initiated transaction.
     * @param req Near prepare request.
     * @return Prepare future or {@code null} if need retry operation.
     */
    @Nullable private IgniteInternalFuture<GridNearTxPrepareResponse> prepareNearTx(
        final ClusterNode nearNode,
        final GridNearTxPrepareRequest req
    ) {
        return prepareNearTx(null, nearNode, req);
    }

    /**
     * @param originTx Transaction for copy.
     * @param nearNode Node that initiated transaction.
     * @param req Near prepare request.
     * @return Prepare future or {@code null} if need retry operation.
     */
    @Nullable private IgniteInternalFuture<GridNearTxPrepareResponse> prepareNearTx(
        final GridNearTxLocal originTx,
        final ClusterNode nearNode,
        final GridNearTxPrepareRequest req
    ) {
        IgniteTxEntry firstEntry;

        try {
            IgniteTxEntry firstWrite = unmarshal(req.writes());
            IgniteTxEntry firstRead = unmarshal(req.reads());

            firstEntry = firstWrite != null ? firstWrite : firstRead;
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

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
                assert firstEntry != null : req;

                assert req.concurrency() == OPTIMISTIC : req;
                assert nearNode.isClient() : nearNode;

                top = firstEntry.context().topology();

                top.readLock();

                if (req.allowWaitTopologyFuture()) {
                    GridDhtTopologyFuture topFut = top.topologyVersionFuture();

                    if (!topFut.isDone()) {
                        top.readUnlock();

                        return null;
                    }
                }
            }

            try {
                if (top != null ) {
                    boolean retry = false;

                    GridDhtTopologyFuture topFut = top.topologyVersionFuture();

                    if (!req.allowWaitTopologyFuture() && !topFut.isDone()) {
                        retry = true;

                        if (txPrepareMsgLog.isDebugEnabled()) {
                            txPrepareMsgLog.debug("Topology change is in progress, need remap transaction [" +
                                "txId=" + req.version() +
                                ", node=" + nearNode.id() +
                                ", reqTopVer=" + req.topologyVersion() +
                                ", locTopVer=" + top.readyTopologyVersion() +
                                ", req=" + req + ']');
                        }
                    }

                    if (!retry && needRemap(req.topologyVersion(), top.readyTopologyVersion(), req)) {
                        retry = true;

                        if (txPrepareMsgLog.isDebugEnabled()) {
                            txPrepareMsgLog.debug("Topology version mismatch for near prepare, need remap transaction [" +
                                "txId=" + req.version() +
                                ", node=" + nearNode.id() +
                                ", reqTopVer=" + req.topologyVersion() +
                                ", locTopVer=" + top.readyTopologyVersion() +
                                ", req=" + req + ']');
                        }
                    }

                    if (retry) {
                        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
                            req.partition(),
                            req.version(),
                            req.futureId(),
                            req.miniId(),
                            req.version(),
                            req.version(),
                            null,
                            null,
                            top.lastTopologyChangeVersion(),
                            req.onePhaseCommit(),
                            req.deployInfo() != null);

                        try {
                            ctx.io().send(nearNode, res, req.policy());

                            if (txPrepareMsgLog.isDebugEnabled()) {
                                txPrepareMsgLog.debug("Sent remap response for near prepare [txId=" + req.version() +
                                    ", node=" + nearNode.id() + ']');
                            }
                        }
                        catch (ClusterTopologyCheckedException ignored) {
                            if (txPrepareMsgLog.isDebugEnabled()) {
                                txPrepareMsgLog.debug("Failed to send remap response for near prepare, node failed [" +
                                    "txId=" + req.version() +
                                    ", node=" + nearNode.id() + ']');
                            }
                        }
                        catch (IgniteCheckedException e) {
                            U.error(txPrepareMsgLog, "Failed to send remap response for near prepare " +
                                "[txId=" + req.version() +
                                ", node=" + nearNode.id() +
                                ", req=" + req + ']', e);
                        }

                        return new GridFinishedFuture<>(res);
                    }

                    assert topFut.isDone();
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
                    req.taskNameHash(),
                    req.txLabel(),
                    originTx
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

            if (req.near())
                tx.nearOnOriginatingNode(true);

            if (req.onePhaseCommit()) {
                assert req.last() : req;

                tx.onePhaseCommit(true);
            }

            if (req.needReturnValue())
                tx.needReturnValue(true);

            IgniteInternalFuture<GridNearTxPrepareResponse> fut = tx.prepareAsync(req);

            if (tx.isRollbackOnly() && !tx.commitOnPrepare()) {
                if (tx.state() != TransactionState.ROLLED_BACK && tx.state() != TransactionState.ROLLING_BACK)
                    tx.rollbackDhtLocalAsync();
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
     * @param node Sender node.
     * @param req Request.
     * @return {@code True} if update will be retried from future listener or topology version future is timed out.
     */
    private boolean waitForExchangeFuture(final ClusterNode node, final GridNearTxPrepareRequest req) {
        assert req.firstClientRequest() : req;

        GridDhtTopologyFuture topFut = ctx.exchange().lastTopologyFuture();

        if (!topFut.isDone()) {
            Thread curThread = Thread.currentThread();

            if (curThread instanceof IgniteThread) {
                final IgniteThread thread = (IgniteThread)curThread;

                if (thread.cachePoolThread()) {
                    ctx.time().waitAsync(topFut, req.timeout(), (e, timedOut) -> {
                            if (e != null || timedOut) {
                                sendResponseOnTimeoutOrError(e, topFut, node, req);

                                return;
                            }
                            ctx.kernalContext().closure().runLocalWithThreadPolicy(thread, () -> {
                                try {
                                    processNearTxPrepareRequest0(node, req);
                                }
                                finally {
                                    ctx.io().onMessageProcessed(req);
                                }
                            });
                        }
                    );

                    return true;
                }
            }

            try {
                if (req.timeout() > 0)
                    topFut.get(req.timeout());
                else
                    topFut.get();
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                sendResponseOnTimeoutOrError(null, topFut, node, req);

                return true;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Topology future failed: " + e, e);
            }
        }

        return false;
    }

    /**
     * @param e Exception or null if timed out.
     * @param topFut Topology future.
     * @param node Node.
     * @param req Prepare request.
     */
    private void sendResponseOnTimeoutOrError(@Nullable IgniteCheckedException e,
        GridDhtTopologyFuture topFut,
        ClusterNode node,
        GridNearTxPrepareRequest req) {
        if (e == null)
            e = new IgniteTxTimeoutCheckedException("Failed to wait topology version for near prepare " +
                "[txId=" + req.version() +
                ", topVer=" + topFut.initialVersion() +
                ", node=" + node.id() +
                ", req=" + req + ']');

        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
            req.partition(),
            req.version(),
            req.futureId(),
            req.miniId(),
            req.version(),
            req.version(),
            null,
            e,
            null,
            req.onePhaseCommit(),
            req.deployInfo() != null);

        try {
            ctx.io().send(node.id(), res, req.policy());
        }
        catch (IgniteCheckedException e0) {
            U.error(txPrepareMsgLog, "Failed to send wait topology version response for near prepare " +
                "[txId=" + req.version() +
                ", topVer=" + topFut.initialVersion() +
                ", node=" + node.id() +
                ", req=" + req + ']', e0);
        }
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
        if (curVer.equals(expVer))
            return false;

        AffinityTopologyVersion lastAffChangedTopVer = ctx.exchange().lastAffinityChangedTopologyVersion(expVer);

        if (curVer.compareTo(expVer) <= 0 && curVer.compareTo(lastAffChangedTopVer) >= 0)
            return false;

        // TODO IGNITE-6754 check mvcc crd for mvcc enabled txs.

        for (IgniteTxEntry e : F.concat(false, req.reads(), req.writes())) {
            GridCacheContext ctx = e.context();

            Collection<ClusterNode> cacheNodes0 = ctx.discovery().cacheGroupAffinityNodes(ctx.groupId(), expVer);
            Collection<ClusterNode> cacheNodes1 = ctx.discovery().cacheGroupAffinityNodes(ctx.groupId(), curVer);

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
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_NEAR_PREPARE_RESP, MTC.span()))) {
            if (txPrepareMsgLog.isDebugEnabled())
                txPrepareMsgLog.debug("Received near prepare response [txId=" + res.version() + ", node=" +
                    nodeId + ']');

            GridNearTxPrepareFutureAdapter fut = (GridNearTxPrepareFutureAdapter)ctx.mvcc()
                .<IgniteInternalTx>versionedFuture(res.version(), res.futureId());

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
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processNearTxFinishResponse(UUID nodeId, GridNearTxFinishResponse res) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_NEAR_FINISH_RESP, MTC.span()))) {
            if (txFinishMsgLog.isDebugEnabled())
                txFinishMsgLog.debug("Received near finish response [txId=" + res.xid() + ", node=" + nodeId + ']');

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
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxPrepareResponse(UUID nodeId, GridDhtTxPrepareResponse res) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_PROCESS_DHT_PREPARE_RESP, MTC.span()))) {
            GridDhtTxPrepareFuture fut =
                (GridDhtTxPrepareFuture)ctx.mvcc().versionedFuture(res.version(), res.futureId());

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
                txPrepareMsgLog.debug("Received dht prepare response [txId=" + fut.tx().nearXidVersion() +
                    ", node=" + nodeId + ']');

            IgniteInternalTx tx = fut.tx();

            assert tx != null;

            res.txState(tx.txState());

            fut.onResult(nodeId, res);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxFinishResponse(UUID nodeId, GridDhtTxFinishResponse res) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_PROCESS_DHT_FINISH_RESP, MTC.span()))) {
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
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    @Nullable private IgniteInternalFuture<IgniteInternalTx> processNearTxFinishRequest(
        UUID nodeId,
        GridNearTxFinishRequest req
    ) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_NEAR_FINISH_REQ, MTC.span()))) {
            if (txFinishMsgLog.isDebugEnabled())
                txFinishMsgLog.debug("Received near finish request [txId=" + req.version() + ", node=" + nodeId +
                    ']');

            IgniteInternalFuture<IgniteInternalTx> fut = finish(nodeId, null, req);

            assert req.txState() != null || fut == null || fut.error() != null ||
                (ctx.tm().tx(req.version()) == null && ctx.tm().nearTx(req.version()) == null) :
                "[req=" + req + ", fut=" + fut + "]";

            return fut;
        }
    }

    /**
     * @param nodeId Node ID.
     * @param locTx Local transaction.
     * @param req Request.
     * @return Future.
     */
    @Nullable public IgniteInternalFuture<IgniteInternalTx> finish(UUID nodeId,
        @Nullable GridNearTxLocal locTx,
        GridNearTxFinishRequest req)
    {
        assert nodeId != null;
        assert req != null;

        if (locTx != null)
            req.txState(locTx.txState());

        // Always add near version to rollback history to prevent races with rollbacks.
        if (!req.commit())
            ctx.tm().addRolledbackTx(null, req.version());

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
    private IgniteInternalFuture<IgniteInternalTx> finishDhtLocal(UUID nodeId,
        @Nullable GridNearTxLocal locTx,
        GridNearTxFinishRequest req)
    {
        GridCacheVersion dhtVer = ctx.tm().mappedVersion(req.version());

        GridDhtTxLocal tx = null;

        if (dhtVer == null) {
            if (log.isDebugEnabled())
                log.debug("Received transaction finish request for unknown near version (was lock explicit?): " + req);
        }
        else
            tx = ctx.tm().tx(dhtVer);

        if (tx != null) {
            tx.mvccSnapshot(req.mvccSnapshot());

            req.txState(tx.txState());
        }

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
            GridCacheMessage res = new GridNearTxFinishResponse(
                req.partition(),
                req.version(),
                req.threadId(),
                req.futureId(),
                req.miniId(),
                new IgniteTxRollbackCheckedException("Transaction has been already completed or not started yet."));

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
            assert req.syncMode() != null : req;

            tx.syncMode(req.syncMode());
            tx.nearFinishFutureId(req.futureId());
            tx.nearFinishMiniId(req.miniId());
            tx.storeEnabled(req.storeEnabled());

            if (!tx.markFinalizing(USER_FINISH)) {
                if (log.isDebugEnabled())
                    log.debug("Will not finish transaction (it is handled by another thread) [commit=" + req.commit() + ", tx=" + tx + ']');

                return null;
            }

            if (req.commit()) {
                IgniteInternalFuture<IgniteInternalTx> commitFut = tx.commitDhtLocalAsync();

                // Only for error logging.
                commitFut.listen(CU.errorLogger(log));

                return commitFut;
            }
            else {
                IgniteInternalFuture<IgniteInternalTx> rollbackFut = tx.rollbackDhtLocalAsync();

                // Only for error logging.
                rollbackFut.listen(CU.errorLogger(log));

                return rollbackFut;
            }
        }
        catch (Throwable e) {
            try {
                if (tx != null) {
                    tx.commitError(e);

                    tx.systemInvalidate(true);

                    try {
                        IgniteInternalFuture<IgniteInternalTx> res = tx.rollbackDhtLocalAsync();

                        // Only for error logging.
                        res.listen(CU.errorLogger(log));

                        return res;
                    }
                    catch (Throwable e1) {
                        e.addSuppressed(e1);
                    }

                    tx.logTxFinishErrorSafe(log, req.commit(), e);
                }

                if (e instanceof Error)
                    throw (Error)e;

                return new GridFinishedFuture<>(e);
            }
            finally {
                ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
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
            try {
                if (tx != null) {
                    try {
                        return tx.rollbackNearTxLocalAsync();
                    }
                    catch (Throwable e1) {
                        e.addSuppressed(e1);
                    }

                    tx.logTxFinishErrorSafe(log, commit, e);
                }

                if (e instanceof Error)
                    throw e;

                return new GridFinishedFuture<>(e);
            }
            finally {
                ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    private void processDhtTxPrepareRequest(final UUID nodeId, final GridDhtTxPrepareRequest req) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_PROCESS_DHT_PREPARE_REQ, MTC.span()))) {
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
                res = new GridDhtTxPrepareResponse(
                    req.partition(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    req.deployInfo() != null);

                // Start near transaction first.
                nearTx = !F.isEmpty(req.nearWrites()) ? startNearRemoteTx(ctx.deploy().globalLoader(), nodeId, req) : null;
                dhtTx = startRemoteTx(nodeId, req, res);

                // Set evicted keys from near transaction.
                if (nearTx != null)
                    res.nearEvicted(nearTx.evicted());

                List<IgniteTxKey> writesCacheMissed = req.nearWritesCacheMissed();

                if (writesCacheMissed != null) {
                    Collection<IgniteTxKey> evicted0 = res.nearEvicted();

                    if (evicted0 != null)
                        writesCacheMissed.addAll(evicted0);

                    res.nearEvicted(writesCacheMissed);
                }

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
                else
                    U.error(log, "Failed to process prepare request: " + req, e);

                if (nearTx != null)
                    try {
                        nearTx.rollbackRemoteTx();
                    }
                    catch (Throwable e1) {
                        e.addSuppressed(e1);
                    }

                res = new GridDhtTxPrepareResponse(
                    req.partition(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    e,
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

            assert req.txState() != null || res.error() != null || (dhtTx == null && nearTx == null) :
                req + " tx=" + dhtTx + " nearTx=" + nearTx;
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processDhtTxOnePhaseCommitAckRequest(final UUID nodeId,
        final GridDhtTxOnePhaseCommitAckRequest req) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_PROCESS_DHT_ONE_PHASE_COMMIT_ACK_REQ, MTC.span()))) {
            assert nodeId != null;
            assert req != null;

            if (log.isDebugEnabled())
                log.debug("Processing dht tx one phase commit ack request [nodeId=" + nodeId + ", req=" + req + ']');

            for (GridCacheVersion ver : req.versions())
                ctx.tm().removeTxReturn(ver);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"unchecked"})
    private void processDhtTxFinishRequest(final UUID nodeId, final GridDhtTxFinishRequest req) {
        try (TraceSurroundings ignored =
                 MTC.support(ctx.kernalContext().tracing().create(TX_PROCESS_DHT_FINISH_REQ, MTC.span()))) {
            assert nodeId != null;
            assert req != null;

            if (req.checkCommitted()) {
                boolean committed = req.waitRemoteTransactions() || !ctx.tm().addRolledbackTx(null, req.version());

                if (!committed || req.syncMode() != FULL_SYNC)
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

            // Always add version to rollback history to prevent races with rollbacks.
            if (!req.commit())
                ctx.tm().addRolledbackTx(null, req.version());

            GridDhtTxRemote dhtTx = ctx.tm().tx(req.version());
            GridNearTxRemote nearTx = ctx.tm().nearTx(req.version());

            IgniteInternalTx anyTx = U.<IgniteInternalTx>firstNotNull(dhtTx, nearTx);

            final GridCacheVersion nearTxId = anyTx != null ? anyTx.nearXidVersion() : null;

            if (txFinishMsgLog.isDebugEnabled())
                txFinishMsgLog.debug("Received dht finish request [txId=" + nearTxId + ", dhtTxId=" + req.version() +
                    ", node=" + nodeId + ']');

            if (anyTx == null && req.commit())
                ctx.tm().addCommittedTx(null, req.version(), null);

            if (dhtTx != null)
                finish(nodeId, dhtTx, req);
            else {
                try {
                    applyPartitionsUpdatesCounters(req.updateCounters(), !req.commit(), false);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

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
                        @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                            sendReply(nodeId, req, true, nearTxId);
                        }
                    });
                }
                else
                    sendReply(nodeId, req, true, nearTxId);
            }
            else
                sendReply(nodeId, req, true, null);

            assert req.txState() != null || (dhtTx == null && nearTx == null) : req + " tx=" + dhtTx + " nearTx=" + nearTx;
        }
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
        assert tx != null;

        req.txState(tx.txState());

        try {
            if (req.commit() || req.isSystemInvalidate()) {
                tx.commitVersion(req.commitVersion());
                if (req.isInvalidate())
                    tx.invalidate(true);
                if (req.isSystemInvalidate())
                    tx.systemInvalidate(true);
                tx.mvccSnapshot(req.mvccSnapshot());

                // Complete remote candidates.
                tx.doneRemote(req.baseVersion(), null, null, null);

                tx.commitRemoteTx();
            }
            else {
                if (tx.dht() && req.updateCounters() != null)
                    tx.txCounters(true).updateCounters(req.updateCounters());

                tx.doneRemote(req.baseVersion(), null, null, null);
                tx.mvccSnapshot(req.mvccSnapshot());
                tx.rollbackRemoteTx();
            }
        }
        catch (IgniteTxHeuristicCheckedException e) {
            // Already uncommitted.
        }
        catch (Throwable e) {
            // Mark transaction for invalidate.
            tx.invalidate(true);
            tx.systemInvalidate(true);

            try {
                tx.commitRemoteTx();
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to invalidate transaction: " + tx, ex);
            }

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * Finish for one-phase distributed tx.
     *
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
            tx.mvccSnapshot(req.mvccSnapshot());

            // Complete remote candidates.
            tx.doneRemote(req.version(), null, null, null);

            tx.commitRemoteTx();
        }
        catch (IgniteTxHeuristicCheckedException e) {
            // Just rethrow this exception. Transaction was already uncommitted.
            throw e;
        }
        catch (Throwable e) {
            try {
                // Mark transaction for invalidate.
                tx.invalidate(true);
                tx.systemInvalidate(true);

                try {
                    tx.rollbackRemoteTx();
                }
                catch (Throwable e1) {
                    e.addSuppressed(e1);
                }

                tx.logTxFinishErrorSafe(log, true, e);

                if (e instanceof Error)
                    throw (Error)e;
            }
            finally {
                ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }
    }

    /**
     * @param nodeId Node id.
     * @param req Request.
     * @param res Response.
     * @param dhtTx Dht tx.
     * @param nearTx Near tx.
     */
    private void sendReply(UUID nodeId,
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
                try {
                    nearTx.rollbackRemoteTx();
                }
                catch (Throwable e1) {
                    e.addSuppressed(e1);
                }

            if (dhtTx != null)
                try {
                    dhtTx.rollbackRemoteTx();
                }
                catch (Throwable e1) {
                    e.addSuppressed(e1);
                }
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
    private void sendReply(UUID nodeId, GridDhtTxFinishRequest req, boolean committed, GridCacheVersion nearTxId) {
        if (req.replyRequired() || req.checkCommitted()) {
            GridDhtTxFinishResponse res = new GridDhtTxFinishResponse(
                req.partition(),
                req.version(),
                req.futureId(),
                req.miniId());

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
        if (req.queryUpdate() || !F.isEmpty(req.writes())) {
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
                    single,
                    req.storeWriteThrough(),
                    req.txLabel());

                tx.onePhaseCommit(req.onePhaseCommit());
                tx.writeVersion(req.writeVersion());

                tx = ctx.tm().onCreated(null, tx);

                if (tx == null || !ctx.tm().onStarted(tx)) {
                    if (log.isDebugEnabled())
                        log.debug("Attempt to start a completed transaction (will ignore): " + tx);

                    applyPartitionsUpdatesCounters(req.updateCounters(), true, false);

                    return null;
                }

                if (ctx.discovery().node(nodeId) == null) {
                    tx.state(ROLLING_BACK);

                    tx.state(ROLLED_BACK);

                    ctx.tm().uncommitTx(tx);

                    applyPartitionsUpdatesCounters(req.updateCounters(), true, false);

                    return null;
                }
            }
            else {
                tx.writeVersion(req.writeVersion());
                tx.transactionNodes(req.transactionNodes());
            }

            TxCounters txCounters = null;

            if (req.updateCounters() != null) {
                txCounters = tx.txCounters(true);

                txCounters.updateCounters(req.updateCounters());
            }

            if (!tx.isSystemInvalidate()) {
                int idx = 0;

                for (IgniteTxEntry entry : req.writes()) {
                    GridCacheContext cacheCtx = entry.context();

                    int part = cacheCtx.affinity().partition(entry.key());

                    GridDhtLocalPartition locPart = cacheCtx.topology().localPartition(part,
                        req.topologyVersion(),
                        false);

                    if (locPart != null && locPart.reserve()) {
                        try {
                            tx.addWrite(entry, ctx.deploy().globalLoader());

                            // Entry will be invalidated if a partition was moved to RENTING.
                            if (locPart.state() == RENTING)
                                continue;

                            if (txCounters != null) {
                                Long cntr = txCounters.generateNextCounter(entry.cacheId(), part);

                                if (cntr != null) // Counter is null if entry is no-op.
                                    entry.updateCounter(cntr);
                            }

                            if (isNearEnabled(cacheCtx) && req.invalidateNearEntry(idx))
                                invalidateNearEntry(cacheCtx, entry.key(), req.version());

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

                                            if (cached == null) {
                                                cached = cacheCtx.cache().entryEx(entry.key(), req.topologyVersion());

                                                entry.cached(cached);
                                            }

                                            CacheObject val = cached.innerGet(
                                                /*ver*/null,
                                                tx,
                                                /*readThrough*/false,
                                                /*updateMetrics*/false,
                                                /*evt*/false,
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
                            tx.addInvalidPartition(cacheCtx.cacheId(), e.partition());

                            tx.clearEntry(entry.txKey());
                        }
                        finally {
                            locPart.release();
                        }
                    }
                    else
                        tx.addInvalidPartition(cacheCtx.cacheId(), part);

                    idx++;
                }
            }

            // Prepare prior to reordering, so the pending locks added
            // in prepare phase will get properly ordered as well.
            tx.prepareRemoteTx();

            if (req.last()) {
                assert !F.isEmpty(req.transactionNodes()) :
                    "Received last prepare request with empty transaction nodes: " + req;

                tx.state(PREPARED);
            }

            res.invalidPartitionsByCacheId(tx.invalidPartitions());

            if (!req.queryUpdate() && tx.empty() && req.last()) {
                tx.skipCompletedVersions(req.skipCompletedVersion());

                tx.rollbackRemoteTx();

                return null;
            }

            return tx;
        }

        return null;
    }

    /**
     * Writes updated values on the backup node.
     *
     * @param tx Transaction.
     * @param ctx Cache context.
     * @param op Operation.
     * @param keys Keys.
     * @param vals Values sent from the primary node.
     * @param snapshot Mvcc snapshot.
     * @param batchNum Batch number.
     * @param futId Future id.
     * @throws IgniteCheckedException If failed.
     */
    public void mvccEnlistBatch(GridDhtTxRemote tx, GridCacheContext ctx, EnlistOperation op, List<KeyCacheObject> keys,
        List<Message> vals, MvccSnapshot snapshot, IgniteUuid futId, int batchNum) throws IgniteCheckedException {
        assert keys != null && (vals == null || vals.size() == keys.size());
        assert tx != null;

        GridDhtCacheAdapter dht = ctx.dht();

        tx.addActiveCache(ctx, false);

        for (int i = 0; i < keys.size(); i++) {
            KeyCacheObject key = keys.get(i);

            assert key != null;

            int part = ctx.affinity().partition(key);

            try {
                GridDhtLocalPartition locPart = ctx.topology().localPartition(part, tx.topologyVersion(), false);

                if (locPart != null && locPart.reserve()) {
                    try {
                        // Skip renting partitions.
                        if (locPart.state() == RENTING) {
                            tx.addInvalidPartition(ctx.cacheId(), part);

                            continue;
                        }

                        CacheObject val = null;
                        EntryProcessor entryProc = null;
                        Object[] invokeArgs = null;

                        boolean needOldVal = tx.txState().useMvccCaching(ctx.cacheId());

                        Message val0 = vals != null ? vals.get(i) : null;

                        CacheEntryInfoCollection entries =
                            val0 instanceof CacheEntryInfoCollection ? (CacheEntryInfoCollection)val0 : null;

                        if (entries == null && !op.isDeleteOrLock() && !op.isInvoke())
                            val = (val0 instanceof CacheObject) ? (CacheObject)val0 : null;

                        if (entries == null && op.isInvoke()) {
                            assert val0 instanceof GridInvokeValue;

                            GridInvokeValue invokeVal = (GridInvokeValue)val0;

                            entryProc = invokeVal.entryProcessor();
                            invokeArgs = invokeVal.invokeArgs();
                        }

                        assert entries != null || entryProc != null || !op.isInvoke() : "entryProc=" + entryProc + ", op=" + op;

                        GridDhtCacheEntry entry = dht.entryExx(key, tx.topologyVersion());

                        GridCacheUpdateTxResult updRes;

                        while (true) {
                            ctx.shared().database().checkpointReadLock();

                            try {
                                if (entries == null) {
                                    switch (op) {
                                        case DELETE:
                                            updRes = entry.mvccRemove(
                                                tx,
                                                ctx.localNodeId(),
                                                tx.topologyVersion(),
                                                snapshot,
                                                false,
                                                needOldVal,
                                                null,
                                                false);

                                            break;

                                        case INSERT:
                                        case TRANSFORM:
                                        case UPSERT:
                                        case UPDATE:
                                            updRes = entry.mvccSet(
                                                tx,
                                                ctx.localNodeId(),
                                                val,
                                                entryProc,
                                                invokeArgs,
                                                0,
                                                tx.topologyVersion(),
                                                snapshot,
                                                op.cacheOperation(),
                                                false,
                                                false,
                                                needOldVal,
                                                null,
                                                false,
                                                false);

                                            break;

                                        default:
                                            throw new IgniteSQLException("Cannot acquire lock for operation [op= "
                                                + op + "]" + "Operation is unsupported at the moment ",
                                                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                                    }
                                }
                                else {
                                    updRes = entry.mvccUpdateRowsWithPreloadInfo(tx,
                                        ctx.localNodeId(),
                                        tx.topologyVersion(),
                                        entries.infos(),
                                        op.cacheOperation(),
                                        snapshot,
                                        futId,
                                        batchNum);
                                }

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                entry = dht.entryExx(key);
                            }
                            finally {
                                ctx.shared().database().checkpointReadUnlock();
                            }
                        }

                        if (!updRes.filtered())
                            ctx.shared().mvccCaching().addEnlisted(key, updRes.newValue(), 0, 0, tx.xidVersion(),
                                updRes.oldValue(), tx.local(), tx.topologyVersion(), snapshot, ctx.cacheId(), tx, futId, batchNum);

                        assert updRes.updateFuture() == null : "Entry should not be locked on the backup";
                    }

                    finally {
                        locPart.release();
                    }
                }
                else
                    tx.addInvalidPartition(ctx.cacheId(), part);
            }
            catch (GridDhtInvalidPartitionException e) {
                tx.addInvalidPartition(ctx.cacheId(), e.partition());
            }
        }
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
            nearEntry.invalidate(ver);
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
    @Nullable private GridNearTxRemote startNearRemoteTx(ClassLoader ldr, UUID nodeId,
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
                    req.taskNameHash(),
                    req.txLabel()
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
            tx.prepareRemoteTx();

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
    private void processCheckPreparedTxRequest(final UUID nodeId,
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
        if (txRecoveryMsgLog.isInfoEnabled()) {
            txRecoveryMsgLog.info("Received tx recovery response [txId=" + res.version() +
                ", node=" + nodeId +
                ", res=" + res + ']');
        }

        GridCacheTxRecoveryFuture fut = (GridCacheTxRecoveryFuture)ctx.mvcc().future(res.futureId());

        if (fut == null) {
            if (txRecoveryMsgLog.isInfoEnabled()) {
                txRecoveryMsgLog.info("Failed to find future for tx recovery response [txId=" + res.version() +
                    ", node=" + nodeId + ", res=" + res + ']');
            }

            return;
        }
        else
            res.txState(fut.tx().txState());

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node id.
     * @param req Request.
     */
    private void processPartitionCountersRequest(UUID nodeId, PartitionCountersNeighborcastRequest req) {
        try {
            applyPartitionsUpdatesCounters(req.updateCounters(), true, false);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        try {
            ctx.io().send(nodeId, new PartitionCountersNeighborcastResponse(req.futId(), req.topologyVersion()), SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (txRecoveryMsgLog.isDebugEnabled())
                txRecoveryMsgLog.debug("Failed to send partition counters response, node left [node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(txRecoveryMsgLog, "Failed to send partition counters response [node=" + nodeId + ']', e);
        }
    }

    /**
     * @param nodeId Node id.
     * @param res Response.
     */
    private void processPartitionCountersResponse(UUID nodeId, PartitionCountersNeighborcastResponse res) {
        PartitionCountersNeighborcastFuture fut = ((PartitionCountersNeighborcastFuture)ctx.mvcc().future(res.futId()));

        if (fut == null) {
            log.warning("Failed to find future for partition counters response [futId=" + res.futId() +
                ", node=" + nodeId + ']');

            return;
        }

        fut.onResult(nodeId);
    }

    /**
     * Applies partition counter updates for transactions.
     * <p>
     * Called after entries are written to WAL on commit or during rollback to close gaps in update counter sequence.
     *
     * @param counters Counters.
     */
    public void applyPartitionsUpdatesCounters(Iterable<PartitionUpdateCountersMessage> counters)
        throws IgniteCheckedException {
        applyPartitionsUpdatesCounters(counters, false, false);
    }

    /**
     * Applies partition counter updates for transactions.
     * <p>
     * Called after entries are written to WAL on commit or during rollback to close gaps in update counter sequence.
     * <p>
     * On rollback counters should be applied on the primary only after backup nodes, otherwise if the primary fail
     * before sending rollback requests to backups remote transactions can be committed by recovery protocol and
     * partition consistency will not be restored when primary returns to the grid because RollbackRecord was written
     * (actual for persistent mode only).
     *
     * @param counters Counter values to be updated.
     * @param rollback {@code True} if applied during rollbacks.
     * @param rollbackOnPrimary {@code True} if rollback happens on primary node. Passed to CQ engine.
     */
    public void applyPartitionsUpdatesCounters(
        Iterable<PartitionUpdateCountersMessage> counters,
        boolean rollback,
        boolean rollbackOnPrimary
    ) throws IgniteCheckedException {
        if (counters == null)
            return;

        WALPointer ptr = null;

        try {
            for (PartitionUpdateCountersMessage counter : counters) {
                GridCacheContext ctx0 = ctx.cacheContext(counter.cacheId());

                GridDhtPartitionTopology top = ctx0.topology();

                AffinityTopologyVersion topVer = top.readyTopologyVersion();

                assert top != null;

                for (int i = 0; i < counter.size(); i++) {
                    boolean invalid = false;

                    try {
                        GridDhtLocalPartition part = top.localPartition(counter.partition(i));

                        if (part != null && part.reserve()) {
                            try {
                                if (part.state() != RENTING) { // Check is actual only for backup node.
                                    long start = counter.initialCounter(i);
                                    long delta = counter.updatesCount(i);

                                    boolean updated = part.updateCounter(start, delta);

                                    // Need to log rolled back range for logical recovery.
                                    if (updated && rollback) {
                                        CacheGroupContext grpCtx = part.group();

                                        if (grpCtx.persistenceEnabled() && grpCtx.walEnabled() && !grpCtx.mvccEnabled()) {
                                            RollbackRecord rec =
                                                new RollbackRecord(grpCtx.groupId(), part.id(), start, delta);

                                            ptr = ctx.wal().log(rec);
                                        }

                                        for (int cntr = 1; cntr <= delta; cntr++) {
                                            ctx0.continuousQueries().skipUpdateCounter(null, part.id(), start + cntr,
                                                topVer, rollbackOnPrimary);
                                        }
                                    }
                                }
                                else
                                    invalid = true;
                            }
                            finally {
                                part.release();
                            }
                        }
                        else
                            invalid = true;
                    }
                    catch (GridDhtInvalidPartitionException e) {
                        invalid = true;
                    }

                    if (log.isDebugEnabled() && invalid) {
                        log.debug("Received partition update counters message for invalid partition, ignoring: " +
                            "[cacheId=" + counter.cacheId() + ", part=" + counter.partition(i) + ']');
                    }
                }
            }
        }
        finally {
            if (ptr != null)
                ctx.wal().flush(ptr, false);
        }
    }

    /**
     * @param tx Transaction.
     * @param node Backup node.
     * @return Partition counters for the given backup node.
     */
    @Nullable public List<PartitionUpdateCountersMessage> filterUpdateCountersForBackupNode(
        IgniteInternalTx tx, ClusterNode node) {
        TxCounters txCntrs = tx.txCounters(false);
        Collection<PartitionUpdateCountersMessage> updCntrs;

        if (txCntrs == null || F.isEmpty(updCntrs = txCntrs.updateCounters()))
            return null;

        List<PartitionUpdateCountersMessage> res = new ArrayList<>(updCntrs.size());

        AffinityTopologyVersion top = tx.topologyVersionSnapshot();

        for (PartitionUpdateCountersMessage partCntrs : updCntrs) {
            GridDhtPartitionTopology topology = ctx.cacheContext(partCntrs.cacheId()).topology();

            PartitionUpdateCountersMessage resCntrs = new PartitionUpdateCountersMessage(partCntrs.cacheId(), partCntrs.size());

            for (int i = 0; i < partCntrs.size(); i++) {
                int part = partCntrs.partition(i);

                if (topology.nodes(part, top).indexOf(node) > 0)
                    resCntrs.add(part, partCntrs.initialCounter(i), partCntrs.updatesCount(i));
            }

            if (resCntrs.size() > 0)
                res.add(resCntrs);
        }

        return res;
    }
}
