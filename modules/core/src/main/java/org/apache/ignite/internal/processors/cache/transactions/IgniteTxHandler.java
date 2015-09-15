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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;
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

    /** Shared cache context. */
    private GridCacheSharedContext<?, ?> ctx;

    /**
     * @param nearNodeId Node ID.
     * @param req Request.
     * @return Prepare future.
     */
    public IgniteInternalFuture<?> processNearTxPrepareRequest(final UUID nearNodeId,
        final GridNearTxPrepareRequest req) {
        return prepareTx(nearNodeId, null, req);
    }

    /**
     * @param ctx Shared cache context.
     */
    public IgniteTxHandler(GridCacheSharedContext ctx) {
        this.ctx = ctx;

        log = ctx.logger(IgniteTxHandler.class);

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
        IgniteInternalFuture<GridNearTxPrepareResponse> fut = locTx.prepareAsyncLocal(
            req.reads(),
            req.writes(),
            req.transactionNodes(),
            req.last(),
            req.lastBackups());

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
                        null);
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
            if (log.isDebugEnabled())
                log.debug("Received transaction request from node that left grid (will ignore): " + nearNodeId);

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

        GridDhtTxLocal tx;

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
                    if (log.isDebugEnabled()) {
                        log.debug("Client topology version mismatch, need remap transaction [" +
                            "reqTopVer=" + req.topologyVersion() +
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
                        top.topologyVersion());

                    try {
                        ctx.io().send(nearNode, res, req.policy());
                    }
                    catch (ClusterTopologyCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send client tx remap response, client node failed " +
                                "[node=" + nearNode + ", req=" + req + ']');
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send client tx remap response " +
                            "[node=" + nearNode + ", req=" + req + ']', e);
                    }

                    return new GridFinishedFuture<>(res);
                }

                tx = new GridDhtTxLocal(
                    ctx,
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
                if (top != null)
                    top.readUnlock();
            }
        }

        if (tx != null) {
            if (req.explicitLock())
                tx.explicitLock(req.explicitLock());

            tx.transactionNodes(req.transactionNodes());

            // Set near on originating node flag only if the sender node has new version.
            if (req.near() && FINISH_NEAR_ONE_PHASE_SINCE.compareTo(nearNode.version()) <= 0)
                tx.nearOnOriginatingNode(true);

            if (req.onePhaseCommit()) {
                assert req.last();
                assert F.isEmpty(req.lastBackups()) || req.lastBackups().size() <= 1;

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
                req.last(),
                req.lastBackups());

            if (tx.isRollbackOnly()) {
                try {
                    tx.rollback();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to rollback transaction: " + tx, e);
                }
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
                            !X.hasCause(e, IgniteFutureCancelledException.class))
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

            Collection<ClusterNode> cacheNodes0 = ctx.discovery().cacheAffinityNodes(ctx.name(), expVer);
            Collection<ClusterNode> cacheNodes1 = ctx.discovery().cacheAffinityNodes(ctx.name(), curVer);

            if (!cacheNodes0.equals(cacheNodes1))
                return true;
        }

        return false;
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processNearTxPrepareResponse(UUID nodeId, GridNearTxPrepareResponse res) {
        GridNearTxPrepareFutureAdapter fut = (GridNearTxPrepareFutureAdapter)ctx.mvcc()
            .<IgniteInternalTx>future(res.version(), res.futureId());

        if (fut == null) {
            U.warn(log, "Failed to find future for prepare response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processNearTxFinishResponse(UUID nodeId, GridNearTxFinishResponse res) {
        ctx.tm().onFinishedRemote(nodeId, res.threadId());

        GridNearTxFinishFuture fut = (GridNearTxFinishFuture)ctx.mvcc().<IgniteInternalTx>future(
            res.xid(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for finish response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxPrepareResponse(UUID nodeId, GridDhtTxPrepareResponse res) {
        GridDhtTxPrepareFuture fut = (GridDhtTxPrepareFuture)ctx.mvcc().future(res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

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
            GridNearTxFinishFuture fut = (GridNearTxFinishFuture)ctx.mvcc().<IgniteInternalTx>future(
                res.xid(), res.futureId());

            if (fut == null) {
                if (log.isDebugEnabled())
                    log.debug("Received response for unknown future (will ignore): " + res);

                return;
            }

            fut.onResult(nodeId, res);
        }
        else {
            GridDhtTxFinishFuture fut = (GridDhtTxFinishFuture)ctx.mvcc().<IgniteInternalTx>future(
                res.xid(), res.futureId());

            if (fut == null) {
                if (log.isDebugEnabled())
                    log.debug("Received response for unknown future (will ignore): " + res);

                return;
            }

            fut.onResult(nodeId, res);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    @Nullable public IgniteInternalFuture<IgniteInternalTx> processNearTxFinishRequest(UUID nodeId, GridNearTxFinishRequest req) {
        return finish(nodeId, null, req);
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

        if (tx == null && locTx != null && !req.commit()) {
            U.warn(log, "DHT local tx not found for near local tx rollback " +
                "[req=" + req + ", dhtVer=" + dhtVer + ", tx=" + locTx + ']');

            return null;
        }

        if (tx == null && !req.explicitLock()) {
            assert locTx == null : "DHT local tx should never be lost for near local tx: " + locTx;

            U.warn(log, "Received finish request for completed transaction (the message may be too late " +
                "and transaction could have been DGCed by now) [commit=" + req.commit() +
                ", xid=" + req.version() + ']');

            // Always send finish response.
            GridCacheMessage res = new GridNearTxFinishResponse(req.version(), req.threadId(), req.futureId(),
                req.miniId(), new IgniteCheckedException("Transaction has been already completed."));

            try {
                ctx.io().send(nodeId, res, req.policy());
            }
            catch (Throwable e) {
                // Double-check.
                if (ctx.discovery().node(nodeId) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Node left while sending finish response [nodeId=" + nodeId + ", res=" + res +
                            ']');
                }
                else
                    U.error(log, "Failed to send finish response to node [nodeId=" + nodeId + ", " +
                        "res=" + res + ']', e);

                if (e instanceof Error)
                    throw (Error)e;
            }

            return null;
        }

        try {
            assert tx != null : "Transaction is null for near finish request [nodeId=" +
                nodeId + ", req=" + req + "]";

            if (req.commit()) {
                tx.storeEnabled(req.storeEnabled());

                if (!tx.markFinalizing(USER_FINISH)) {
                    if (log.isDebugEnabled())
                        log.debug("Will not finish transaction (it is handled by another thread): " + tx);

                    return null;
                }

                if (!tx.syncCommit())
                    tx.syncCommit(req.syncCommit());

                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());

                IgniteInternalFuture<IgniteInternalTx> commitFut = tx.commitAsync();

                // Only for error logging.
                commitFut.listen(CU.errorLogger(log));

                return commitFut;
            }
            else {
                tx.syncRollback(req.syncRollback());

                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());

                IgniteInternalFuture<IgniteInternalTx> rollbackFut = tx.rollbackAsync();

                // Only for error logging.
                rollbackFut.listen(CU.errorLogger(log));

                return rollbackFut;
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + req.commit() + ", tx=" + tx + ']', e);

            IgniteInternalFuture<IgniteInternalTx> res = null;

            if (tx != null) {
                IgniteInternalFuture<IgniteInternalTx> rollbackFut = tx.rollbackAsync();

                // Only for error logging.
                rollbackFut.listen(CU.errorLogger(log));

                res = rollbackFut;
            }

            if (e instanceof Error)
                throw (Error)e;

            return res == null ? new GridFinishedFuture<IgniteInternalTx>(e) : res;
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
    protected final void processDhtTxPrepareRequest(UUID nodeId, GridDhtTxPrepareRequest req) {
        assert nodeId != null;
        assert req != null;

        assert req.transactionNodes() != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx prepare request [locNodeId=" + ctx.localNodeId() +
                ", nodeId=" + nodeId + ", req=" + req + ']');

        GridDhtTxRemote dhtTx = null;
        GridNearTxRemote nearTx = null;

        GridDhtTxPrepareResponse res;

        try {
            res = new GridDhtTxPrepareResponse(req.version(), req.futureId(), req.miniId());

            // Start near transaction first.
            nearTx = !F.isEmpty(req.nearWrites()) ? startNearRemoteTx(ctx.deploy().globalLoader(), nodeId, req) : null;
            dhtTx = startRemoteTx(nodeId, req, res);

            // Set evicted keys from near transaction.
            if (nearTx != null)
                res.nearEvicted(nearTx.evicted());

            if (dhtTx != null && !F.isEmpty(dhtTx.invalidPartitions()))
                res.invalidPartitionsByCacheId(dhtTx.invalidPartitions());

            if (req.onePhaseCommit()) {
                assert req.last();

                if (dhtTx != null) {
                    dhtTx.onePhaseCommit(true);

                    finish(nodeId, dhtTx, req);
                }

                if (nearTx != null) {
                    nearTx.onePhaseCommit(true);

                    finish(nodeId, nearTx, req);
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

            res = new GridDhtTxPrepareResponse(req.version(), req.futureId(), req.miniId(), e);
        }

        try {
            // Reply back to sender.
            ctx.io().send(nodeId, res, req.system() ? UTILITY_CACHE_POOL : SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            if (e instanceof ClusterTopologyCheckedException) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send tx response to remote node (node left grid) [node=" + nodeId +
                        ", xid=" + req.version());
            }
            else
                U.warn(log, "Failed to send tx response to remote node (will rollback transaction) [node=" + nodeId +
                    ", xid=" + req.version() + ", err=" +  e.getMessage() + ']');

            if (nearTx != null)
                nearTx.rollback();

            if (dhtTx != null)
                dhtTx.rollback();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"unchecked"})
    protected final void processDhtTxFinishRequest(final UUID nodeId, final GridDhtTxFinishRequest req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx finish request [nodeId=" + nodeId + ", req=" + req + ']');

        if (req.checkCommitted()) {
            sendReply(nodeId, req, checkDhtRemoteTxCommitted(req.version()));

            return;
        }

        GridDhtTxRemote dhtTx = ctx.tm().tx(req.version());
        GridNearTxRemote nearTx = ctx.tm().nearTx(req.version());

        // Safety - local transaction will finish explicitly.
        if (nearTx != null && nearTx.local())
            nearTx = null;

        finish(nodeId, dhtTx, req);

        if (nearTx != null)
            finish(nodeId, nearTx, req);

        if (req.replyRequired()) {
            IgniteInternalFuture completeFut;

            IgniteInternalFuture<IgniteInternalTx> dhtFin = dhtTx == null ? null : dhtTx.done() ? null : dhtTx.finishFuture();
            IgniteInternalFuture<IgniteInternalTx> nearFin = nearTx == null ? null : nearTx.done() ? null : nearTx.finishFuture();

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
                    @Override
                    public void apply(IgniteInternalFuture<IgniteInternalTx> igniteTxIgniteFuture) {
                        sendReply(nodeId, req, true);
                    }
                });
            }
            else
                sendReply(nodeId, req, true);
        }
        else
            sendReply(nodeId, req, true);
    }

    /**
     * Checks whether DHT remote transaction with given version has been committed. If not, will add version
     * to rollback version set so that late response will not falsely commit this transaction.
     *
     * @param writeVer Write version to check.
     * @return {@code True} if transaction has been committed, {@code false} otherwise.
     */
    public boolean checkDhtRemoteTxCommitted(GridCacheVersion writeVer) {
        assert writeVer != null;

        boolean committed = true;

        if (ctx.tm().addRolledbackTx(writeVer))
            committed = false;

        return committed;
    }

    /**
     * @param nodeId Node ID.
     * @param tx Transaction.
     * @param req Request.
     */
    protected void finish(
        UUID nodeId,
        IgniteTxRemoteEx tx,
        GridDhtTxFinishRequest req) {
        // We don't allow explicit locks for transactions and
        // therefore immediately return if transaction is null.
        // However, we may decide to relax this restriction in
        // future.
        if (tx == null) {
            if (req.commit())
                // Must be some long time duplicate, but we add it anyway.
                ctx.tm().addCommittedTx(req.version(), null);
            else
                ctx.tm().addRolledbackTx(req.version());

            if (log.isDebugEnabled())
                log.debug("Received finish request for non-existing transaction (added to completed set) " +
                    "[senderNodeId=" + nodeId + ", res=" + req + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Received finish request for transaction [senderNodeId=" + nodeId + ", req=" + req +
                ", tx=" + tx + ']');

        try {
            if (req.commit() || req.isSystemInvalidate()) {
                tx.commitVersion(req.commitVersion());
                tx.invalidate(req.isInvalidate());
                tx.systemInvalidate(req.isSystemInvalidate());

                // Complete remote candidates.
                tx.doneRemote(req.baseVersion(), null, null, null);

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
     * @param nodeId Node ID.
     * @param tx Transaction.
     * @param req Request.
     */
    protected void finish(
        UUID nodeId,
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
     * Sends tx finish response to remote node, if response is requested.
     *
     * @param nodeId Node id that originated finish request.
     * @param req Request.
     */
    protected void sendReply(UUID nodeId, GridDhtTxFinishRequest req, boolean committed) {
        if (req.replyRequired()) {
            GridDhtTxFinishResponse res = new GridDhtTxFinishResponse(req.version(), req.futureId(), req.miniId());

            if (req.checkCommitted()) {
                res.checkCommitted(true);

                if (!committed)
                    res.checkCommittedError(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                        "(transaction has been rolled back on backup node): " + req.version()));
            }

            try {
                ctx.io().send(nodeId, res, req.system() ? UTILITY_CACHE_POOL : SYSTEM_POOL);
            }
            catch (Throwable e) {
                // Double-check.
                if (ctx.discovery().node(nodeId) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Node left while sending finish response [nodeId=" + nodeId + ", res=" + res + ']');
                }
                else
                    U.error(log, "Failed to send finish response to node [nodeId=" + nodeId + ", res=" + res + ']', e);

                if (e instanceof Error)
                    throw (Error)e;
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
                tx = new GridDhtTxRemote(
                    ctx,
                    req.nearNodeId(),
                    req.futureId(),
                    nodeId,
                    req.threadId(),
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
                    req.taskNameHash());

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

            if (!tx.isSystemInvalidate() && !F.isEmpty(req.writes())) {
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
                    ldr,
                    nodeId,
                    req.nearNodeId(),
                    req.threadId(),
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
        final GridCacheTxRecoveryRequest req)
    {
        if (log.isDebugEnabled())
            log.debug("Processing check prepared transaction requests [nodeId=" + nodeId + ", req=" + req + ']');

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
        GridCacheTxRecoveryResponse res =
            new GridCacheTxRecoveryResponse(req.version(), req.futureId(), req.miniId(), prepared);

        try {
            if (log.isDebugEnabled())
                log.debug("Sending check prepared transaction response [nodeId=" + nodeId + ", res=" + res + ']');

            ctx.io().send(nodeId, res, req.system() ? UTILITY_CACHE_POOL : SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send check prepared transaction response (did node leave grid?) [nodeId=" +
                    nodeId + ", res=" + res + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send response to node [nodeId=" + nodeId + ", res=" + res + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    protected void processCheckPreparedTxResponse(UUID nodeId, GridCacheTxRecoveryResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing check prepared transaction response [nodeId=" + nodeId + ", res=" + res + ']');

        GridCacheTxRecoveryFuture fut = (GridCacheTxRecoveryFuture)ctx.mvcc().
            <Boolean>future(res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

        fut.onResult(nodeId, res);
    }
}