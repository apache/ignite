/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheTxEx.FinalizationStatus.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

/**
 * Isolated logic to process cache messages.
 */
public class GridCacheTxHandler<K, V> {
    /** Logger. */
    private GridLogger log;

    /** Shared cache context. */
    private GridCacheSharedContext<K, V> ctx;

    public GridFuture<GridCacheTxEx<K, V>> processNearTxPrepareRequest(final UUID nearNodeId,
        final GridNearTxPrepareRequest<K, V> req) {
        return prepareTx(nearNodeId, req);
    }

    /**
     * @param ctx Shared cache context.
     */
    public GridCacheTxHandler(GridCacheSharedContext<K, V> ctx) {
        this.ctx = ctx;

        log = ctx.logger(GridCacheTxHandler.class);
    }

    /**
     * @param nearNodeId Near node ID that initiated transaction.
     * @param req Near prepare request.
     * @return Future for transaction.
     */
    public GridFuture<GridCacheTxEx<K, V>> prepareTx(final UUID nearNodeId,
        final GridNearTxPrepareRequest<K, V> req) {
        assert nearNodeId != null;
        assert req != null;

        // TODO move logic from cacheCtx.colocated().prepareTxLocally

        GridNode nearNode = ctx.node(nearNodeId);

        if (nearNode == null) {
            if (log.isDebugEnabled())
                log.debug("Received transaction request from node that left grid (will ignore): " + nearNodeId);

            return null;
        }

        try {
            for (GridCacheTxEntry<K, V> e : F.concat(false, req.reads(), req.writes()))
                e.unmarshal(ctx, ctx.deploy().globalLoader());
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }

        GridDhtTxLocal<K, V> tx;

        GridCacheVersion mappedVer = ctx.tm().mappedVersion(req.version());

        if (mappedVer != null) {
            tx = ctx.tm().tx(mappedVer);

            if (tx == null)
                U.warn(log, "Missing local transaction for mapped near version [nearVer=" + req.version()
                    + ", mappedVer=" + mappedVer + ']');
        }
        else {
            tx = new GridDhtTxLocal<>(
                nearNode.id(),
                req.version(),
                req.futureId(),
                req.miniId(),
                req.threadId(),
                /*implicit*/false,
                /*implicit-single*/false,
                ctx,
                req.concurrency(),
                req.isolation(),
                req.timeout(),
                false,
                req.txSize(),
                req.groupLockKey(),
                req.partitionLock(),
                req.transactionNodes(),
                req.subjectId(),
                req.taskNameHash()
            );

            tx = ctx.tm().onCreated(tx);

            if (tx != null)
                tx.topologyVersion(req.topologyVersion());
            else
                U.warn(log, "Failed to create local transaction (was transaction rolled back?) [xid=" +
                    tx.xid() + ", req=" + req + ']');
        }

        if (tx != null) {
            GridFuture<GridCacheTxEx<K, V>> fut = tx.prepareAsync(req.reads(), req.writes(),
                req.dhtVersions(), req.messageId(), req.miniId(), req.transactionNodes(), req.last(),
                req.lastBackups());

            if (tx.isRollbackOnly()) {
                try {
                    tx.rollback();
                }
                catch (GridException e) {
                    U.error(log, "Failed to rollback transaction: " + tx, e);
                }
            }

            final GridDhtTxLocal<K, V> tx0 = tx;

            fut.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public void apply(GridFuture<GridCacheTxEx<K, V>> txFut) {
                    try {
                        txFut.get();
                    }
                    catch (GridException e) {
                        tx0.setRollbackOnly(); // Just in case.

                        if (!(e instanceof GridCacheTxOptimisticException))
                            U.error(log, "Failed to prepare DHT transaction: " + tx0, e);
                    }
                }
            });

            return fut;
        }
        else
            return new GridFinishedFuture<>(ctx.kernalContext(), (GridCacheTxEx<K, V>)null);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processNearTxPrepareResponse(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
        // TODO same for dht tx prepare
        GridAbstractNearPrepareFuture<K, V> fut = (GridAbstractNearPrepareFuture<K, V>)ctx.mvcc()
            .<GridCacheTxEx<K, V>>future(res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for prepare response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }


    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    @Nullable public GridFuture<GridCacheTx> processNearTxFinishRequest(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
        return finish(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    @Nullable public GridFuture<GridCacheTx> finish(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        // TODO cacheCtx.colocated().finishLocal(commit, m.explicitLock(), tx);

        if (log.isDebugEnabled())
            log.debug("Processing near tx finish request [nodeId=" + nodeId + ", req=" + req + "]");

        GridCacheVersion dhtVer = ctx.tm().mappedVersion(req.version());

        GridDhtTxLocal<K, V> tx = null;

        if (dhtVer == null) {
            if (log.isDebugEnabled())
                log.debug("Received transaction finish request for unknown near version (was lock explicit?): " + req);
        }
        else
            tx = ctx.tm().tx(dhtVer);

        if (tx == null && !req.explicitLock()) {
            U.warn(log, "Received finish request for completed transaction (the message may be too late " +
                "and transaction could have been DGCed by now) [commit=" + req.commit() +
                ", xid=" + req.version() + ']');

            // Always send finish response.
            GridCacheMessage<K, V> res = new GridNearTxFinishResponse<>(req.version(), req.threadId(), req.futureId(),
                req.miniId(), new GridException("Transaction has been already completed."));

            try {
                ctx.io().send(nodeId, res);
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
            }

            return null;
        }

        try {
            if (req.commit()) {
                if (tx == null) {
                    // Create transaction and add entries.
                    tx = ctx.tm().onCreated(
                        new GridDhtTxLocal<>(
                            nodeId,
                            req.version(),
                            req.futureId(),
                            req.miniId(),
                            req.threadId(),
                            true,
                            false, /* we don't know, so assume false. */
                            ctx,
                            PESSIMISTIC,
                            READ_COMMITTED,
                            /*timeout */0,
                            req.explicitLock(),
                            req.txSize(),
                            req.groupLockKey(),
                            false,
                            null,
                            req.subjectId(),
                            req.taskNameHash()));

                    if (tx == null || !ctx.tm().onStarted(tx))
                        throw new GridCacheTxRollbackException("Attempt to start a completed transaction: " + req);

                    tx.topologyVersion(req.topologyVersion());
                }

                if (!tx.markFinalizing(USER_FINISH)) {
                    if (log.isDebugEnabled())
                        log.debug("Will not finish transaction (it is handled by another thread): " + tx);

                    return null;
                }

                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());
                tx.recoveryWrites(req.recoveryWrites());

                Collection<GridCacheTxEntry<K, V>> writeEntries = req.writes();

                if (!F.isEmpty(writeEntries)) {
                    // In OPTIMISTIC mode, we get the values at PREPARE stage.
                    assert tx.concurrency() == PESSIMISTIC;

                    for (GridCacheTxEntry<K, V> entry : writeEntries)
                        tx.addEntry(req.messageId(), entry);
                }

                if (tx.pessimistic())
                    tx.prepare();

                GridFuture<GridCacheTx> commitFut = tx.commitAsync();

                // Only for error logging.
                commitFut.listenAsync(CU.errorLogger(log));

                return commitFut;
            }
            else {
                assert tx != null : "Transaction is null for near rollback request [nodeId=" +
                    nodeId + ", req=" + req + "]";

                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());

                GridFuture<GridCacheTx> rollbackFut = tx.rollbackAsync();

                // Only for error logging.
                rollbackFut.listenAsync(CU.errorLogger(log));

                return rollbackFut;
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + req.commit() + ", tx=" + tx + ']', e);

            if (tx != null) {
                GridFuture<GridCacheTx> rollbackFut = tx.rollbackAsync();

                // Only for error logging.
                rollbackFut.listenAsync(CU.errorLogger(log));

                return rollbackFut;
            }

            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    protected final void processDhtTxPrepareRequest(UUID nodeId, GridDhtTxPrepareRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx prepare request [locNodeId=" + ctx.localNodeId() +
                ", nodeId=" + nodeId + ", req=" + req + ']');

        GridDhtTxRemote<K, V> dhtTx = null;
        GridNearTxRemote<K, V> nearTx = null;

        GridDhtTxPrepareResponse<K, V> res;

        try {
            res = new GridDhtTxPrepareResponse<>(req.version(), req.futureId(), req.miniId());

            // Start near transaction first.
            nearTx = !F.isEmpty(req.nearWrites()) ? startNearRemoteTx(ctx.deploy().globalLoader(), nodeId, req) : null;
            dhtTx = startRemoteTx(nodeId, req, res);

            // Set evicted keys from near transaction.
            if (nearTx != null) {
                if (nearTx.hasEvictedBytes())
                    res.nearEvictedBytes(nearTx.evictedBytes());
                else
                    res.nearEvicted(nearTx.evicted());
            }

            if (dhtTx != null && !F.isEmpty(dhtTx.invalidPartitions()))
                res.invalidPartitions(dhtTx.invalidPartitions());
        }
        catch (GridException e) {
            if (e instanceof GridCacheTxRollbackException)
                U.error(log, "Transaction was rolled back before prepare completed: " + dhtTx, e);
            else if (e instanceof GridCacheTxOptimisticException) {
                if (log.isDebugEnabled())
                    log.debug("Optimistic failure for remote transaction (will rollback): " + dhtTx);
            }
            else
                U.error(log, "Failed to process prepare request: " + req, e);

            if (nearTx != null)
                nearTx.rollback();

            if (dhtTx != null)
                dhtTx.rollback();

            res = new GridDhtTxPrepareResponse<>(req.version(), req.futureId(), req.miniId(), e);
        }

        try {
            // Reply back to sender.
            ctx.io().send(nodeId, res);
        }
        catch (GridException e) {
            if (e instanceof GridTopologyException) {
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
    protected final void processDhtTxFinishRequest(final UUID nodeId, final GridDhtTxFinishRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx finish request [nodeId=" + nodeId + ", req=" + req + ']');

        GridDhtTxRemote<K, V> dhtTx = ctx.tm().tx(req.version());
        GridCacheTxEx<K, V> nearTx = ctx.tm().nearTx(req.version());

        try {
            if (dhtTx == null && !F.isEmpty(req.writes()))
                dhtTx = startRemoteTxForFinish(nodeId, req);

            // One-phase commit transactions send finish requests to backup nodes.
            if (dhtTx != null && req.onePhaseCommit()) {
                dhtTx.onePhaseCommit(true);

                dhtTx.writeVersion(req.writeVersion());
            }

            if (nearTx == null && !F.isEmpty(req.nearWrites()) && req.groupLock())
                nearTx = startRemoteTxForFinish(nodeId, req);
        }
        catch (GridCacheTxRollbackException e) {
            if (log.isDebugEnabled())
                log.debug("Received finish request for completed transaction (will ignore) [req=" + req + ", err=" +
                    e.getMessage() + ']');

            sendReply(nodeId, req);

            return;
        }
        catch (GridException e) {
            U.error(log, "Failed to start remote DHT and Near transactions (will invalidate transactions) [dhtTx=" +
                dhtTx + ", nearTx=" + nearTx + ']', e);

            if (dhtTx != null)
                dhtTx.invalidate(true);

            if (nearTx != null)
                nearTx.invalidate(true);
        }
        catch (GridDistributedLockCancelledException ignore) {
            U.warn(log, "Received commit request to cancelled lock (will invalidate transaction) [dhtTx=" +
                dhtTx + ", nearTx=" + nearTx + ']');

            if (dhtTx != null)
                dhtTx.invalidate(true);

            if (nearTx != null)
                nearTx.invalidate(true);
        }

        // Safety - local transaction will finish explicitly.
        if (nearTx != null && nearTx.local())
            nearTx = null;

        finish(nodeId, dhtTx, req, req.writes());

        if (nearTx != null)
            finish(nodeId, (GridCacheTxRemoteEx<K, V>)nearTx, req, req.nearWrites());

        sendReply(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param tx Transaction.
     * @param req Request.
     * @param writes Writes.
     */
    protected void finish(
        UUID nodeId,
        GridCacheTxRemoteEx<K, V> tx,
        GridDhtTxFinishRequest<K, V> req,
        Collection<GridCacheTxEntry<K, V>> writes) {
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
                if (tx.commitVersion(req.commitVersion())) {
                    tx.invalidate(req.isInvalidate());
                    tx.systemInvalidate(req.isSystemInvalidate());

                    if (!F.isEmpty(writes)) {
                        // In OPTIMISTIC mode, we get the values at PREPARE stage.
                        assert tx.concurrency() == PESSIMISTIC;

                        for (GridCacheTxEntry<K, V> entry : writes) {
                            if (log.isDebugEnabled())
                                log.debug("Unmarshalled transaction entry from pessimistic transaction [key=" +
                                    entry.key() + ", value=" + entry.value() + ", tx=" + tx + ']');

                            if (!tx.setWriteValue(entry))
                                U.warn(log, "Received entry to commit that was not present in transaction [entry=" +
                                    entry + ", tx=" + tx + ']');
                        }
                    }

                    // Complete remote candidates.
                    tx.doneRemote(req.baseVersion(), null, null, null);

                    if (tx.pessimistic())
                        tx.prepare();

                    tx.commit();
                }
            }
            else {
                assert tx != null;

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
            catch (GridException ex) {
                U.error(log, "Failed to invalidate transaction: " + tx, ex);
            }
        }
    }

    /**
     * Sends tx finish response to remote node, if response is requested.
     *
     * @param nodeId Node id that originated finish request.
     * @param req Request.
     */
    protected void sendReply(UUID nodeId, GridDhtTxFinishRequest<K, V> req) {
        if (req.replyRequired()) {
            GridCacheMessage<K, V> res = new GridDhtTxFinishResponse<>(req.version(), req.futureId(), req.miniId());

            try {
                ctx.io().send(nodeId, res);
            }
            catch (Throwable e) {
                // Double-check.
                if (ctx.discovery().node(nodeId) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Node left while sending finish response [nodeId=" + nodeId + ", res=" + res + ']');
                }
                else
                    U.error(log, "Failed to send finish response to node [nodeId=" + nodeId + ", res=" + res + ']', e);
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @param res Response.
     * @return Remote transaction.
     * @throws GridException If failed.
     */
    @Nullable GridDhtTxRemote<K, V> startRemoteTx(UUID nodeId,
        GridDhtTxPrepareRequest<K, V> req,
        GridDhtTxPrepareResponse<K, V> res) throws GridException {
        if (!F.isEmpty(req.writes())) {
            GridDhtTxRemote<K, V> tx = ctx.tm().tx(req.version());

            assert F.isEmpty(req.candidatesByKey());

            if (tx == null) {
                tx = new GridDhtTxRemote<>(
                    req.nearNodeId(),
                    req.futureId(),
                    nodeId,
                    req.threadId(),
                    req.topologyVersion(),
                    req.version(),
                    req.commitVersion(),
                    req.concurrency(),
                    req.isolation(),
                    req.isInvalidate(),
                    req.timeout(),
                    ctx,
                    req.writes() != null ? Math.max(req.writes().size(), req.txSize()) : req.txSize(),
                    req.groupLockKey(),
                    req.nearXidVersion(),
                    req.transactionNodes(),
                    req.subjectId(),
                    req.taskNameHash());

                tx = ctx.tm().onCreated(tx);

                if (tx == null || !ctx.tm().onStarted(tx)) {
                    if (log.isDebugEnabled())
                        log.debug("Attempt to start a completed transaction (will ignore): " + tx);

                    return null;
                }
            }

            if (!tx.isSystemInvalidate() && !F.isEmpty(req.writes())) {
                int idx = 0;

                for (GridCacheTxEntry<K, V> entry : req.writes()) {
                    GridCacheContext<K, V> cacheCtx = entry.context();

                    tx.addWrite(entry, ctx.deploy().globalLoader());

                    if (isNearEnabled(cacheCtx) && req.invalidateNearEntry(idx))
                        invalidateNearEntry(cacheCtx, entry.key(), req.version());

                    try {
                        if (req.needPreloadKey(idx)) {
                            GridCacheEntryEx<K, V> cached = entry.cached();

                            if (cached == null)
                                cached = cacheCtx.cache().entryEx(entry.key(), req.topologyVersion());

                            GridCacheEntryInfo<K, V> info = cached.info();

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

            if (req.last())
                tx.state(PREPARED);

            res.invalidPartitions(tx.invalidPartitions());

            if (tx.empty()) {
                tx.rollback();

                return null;
            }

            return tx;
        }

        return null;
    }

    /**
     * @param key Key
     * @param ver Version.
     * @throws GridException If invalidate failed.
     */
    private void invalidateNearEntry(GridCacheContext<K, V> cacheCtx, K key, GridCacheVersion ver)
        throws GridException {
        GridCacheEntryEx<K, V> nearEntry = cacheCtx.near().peekEx(key);

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
     * @throws GridException If failed.
     */
    @Nullable public GridNearTxRemote<K, V> startNearRemoteTx(ClassLoader ldr, UUID nodeId,
        GridDhtTxPrepareRequest<K, V> req) throws GridException {
        assert F.isEmpty(req.candidatesByKey());

        if (!F.isEmpty(req.nearWrites())) {
            GridNearTxRemote<K, V> tx = ctx.tm().tx(req.version());

            if (tx == null) {
                tx = new GridNearTxRemote<>(
                    ldr,
                    nodeId,
                    req.nearNodeId(),
                    req.threadId(),
                    req.version(),
                    req.commitVersion(),
                    req.concurrency(),
                    req.isolation(),
                    req.isInvalidate(),
                    req.timeout(),
                    req.nearWrites(),
                    ctx,
                    req.txSize(),
                    req.groupLockKey(),
                    req.subjectId(),
                    req.taskNameHash()
                );

                if (!tx.empty()) {
                    tx = ctx.tm().onCreated(tx);

                    if (tx == null || !ctx.tm().onStarted(tx))
                        throw new GridCacheTxRollbackException("Attempt to start a completed transaction: " + tx);
                }
            }
            else
                tx.addEntries(ldr, req.nearWrites());

            tx.ownedVersions(req.owned());

            // Prepare prior to reordering, so the pending locks added
            // in prepare phase will get properly ordered as well.
            tx.prepare();

            return tx;
        }

        return null;
    }

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws GridException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable GridDhtTxRemote<K, V> startRemoteTxForFinish(UUID nodeId, GridDhtTxFinishRequest<K, V> req)
        throws GridException, GridDistributedLockCancelledException {

        GridDhtTxRemote<K, V> tx = null;

        boolean marked = false;

        for (GridCacheTxEntry<K, V> txEntry : req.writes()) {
            GridDistributedCacheEntry<K, V> entry = null;

            GridCacheContext<K, V> cacheCtx = txEntry.context();

            while (true) {
                try {
                    int part = cacheCtx.affinity().partition(txEntry.key());

                    GridDhtLocalPartition<K, V> locPart = cacheCtx.topology().localPartition(part,
                        req.topologyVersion(), false);

                    // Handle implicit locks for pessimistic transactions.
                    if (tx == null)
                        tx = ctx.tm().tx(req.version());

                    if (locPart == null || !locPart.reserve()) {
                        if (log.isDebugEnabled())
                            log.debug("Local partition for given key is already evicted (will remove from tx) " +
                                "[key=" + txEntry.key() + ", part=" + part + ", locPart=" + locPart + ']');

                        if (tx != null)
                            tx.clearEntry(txEntry.txKey());

                        break;
                    }

                    try {
                        entry = (GridDistributedCacheEntry<K, V>)cacheCtx.cache().entryEx(txEntry.key(),
                            req.topologyVersion());

                        if (tx == null) {
                            tx = new GridDhtTxRemote<>(
                                req.nearNodeId(),
                                req.futureId(),
                                nodeId,
                                // We can pass null as nearXidVersion as transaction will be committed right away.
                                null,
                                req.threadId(),
                                req.topologyVersion(),
                                req.version(),
                                /*commitVer*/null,
                                PESSIMISTIC,
                                req.isolation(),
                                req.isInvalidate(),
                                0,
                                ctx,
                                req.txSize(),
                                req.groupLockKey(),
                                req.subjectId(),
                                req.taskNameHash());

                            tx = ctx.tm().onCreated(tx);

                            if (tx == null || !ctx.tm().onStarted(tx))
                                throw new GridCacheTxRollbackException("Failed to acquire lock " +
                                    "(transaction has been completed): " + req.version());
                        }

                        tx.addWrite(cacheCtx, txEntry.op(), txEntry.txKey(), txEntry.keyBytes(), txEntry.value(),
                            txEntry.valueBytes(), txEntry.transformClosures(), txEntry.drVersion());

                        if (!marked) {
                            if (tx.markFinalizing(USER_FINISH))
                                marked = true;
                            else {
                                tx.clearEntry(txEntry.txKey());

                                return null;
                            }
                        }

                        // Add remote candidate before reordering.
                        if (txEntry.explicitVersion() == null && !txEntry.groupLockEntry())
                            entry.addRemote(
                                req.nearNodeId(),
                                nodeId,
                                req.threadId(),
                                req.version(),
                                0,
                                /*tx*/true,
                                tx.implicitSingle(),
                                null
                            );

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nearNodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            tx.rollback();

                            return null;
                        }

                        // Entry is legit.
                        break;
                    }
                    finally {
                        locPart.release();
                    }
                }
                catch (GridCacheEntryRemovedException ignored) {
                    assert entry.obsoleteVersion() != null : "Obsolete flag not set on removed entry: " +
                        entry;

                    if (log.isDebugEnabled())
                        log.debug("Received entry removed exception (will retry on renewed entry): " + entry);

                    tx.clearEntry(txEntry.txKey());

                    if (log.isDebugEnabled())
                        log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                            entry + ", tx=" + tx + ']');
                }
                catch (GridDhtInvalidPartitionException p) {
                    if (log.isDebugEnabled())
                        log.debug("Received invalid partition (will clear entry from tx) [part=" + p + ", req=" +
                            req + ", txEntry=" + txEntry + ']');

                    if (tx != null)
                        tx.clearEntry(txEntry.txKey());

                    break;
                }
            }
        }

        if (tx != null && tx.empty()) {
            tx.rollback();

            return null;
        }

        return tx;
    }

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws GridException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable public GridNearTxRemote<K, V> startNearRemoteTxForFinish(UUID nodeId, GridDhtTxFinishRequest<K, V> req)
        throws GridException, GridDistributedLockCancelledException {
        assert req.groupLock();

        GridNearTxRemote<K, V> tx = null;

        ClassLoader ldr = ctx.deploy().globalLoader();

        if (ldr != null) {
            boolean marked = false;

            for (GridCacheTxEntry<K, V> txEntry : req.nearWrites()) {
                GridDistributedCacheEntry<K, V> entry = null;

                GridCacheContext<K, V> cacheCtx = txEntry.context();

                while (true) {
                    try {
                        entry = cacheCtx.dht().near().peekExx(txEntry.key());

                        if (entry != null) {
                            entry.keyBytes(txEntry.keyBytes());

                            // Handle implicit locks for pessimistic transactions.
                            tx = ctx.tm().tx(req.version());

                            if (tx == null) {
                                tx = new GridNearTxRemote<>(
                                    nodeId,
                                    req.nearNodeId(),
                                    // We can pass null as nearXidVer as transaction will be committed right away.
                                    null,
                                    req.threadId(),
                                    req.version(),
                                    null,
                                    PESSIMISTIC,
                                    req.isolation(),
                                    req.isInvalidate(),
                                    0,
                                    ctx,
                                    req.txSize(),
                                    req.groupLockKey(),
                                    req.subjectId(),
                                    req.taskNameHash());

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !ctx.tm().onStarted(tx))
                                    throw new GridCacheTxRollbackException("Failed to acquire lock " +
                                        "(transaction has been completed): " + req.version());

                                if (!marked)
                                    marked = tx.markFinalizing(USER_FINISH);

                                if (!marked)
                                    return null;
                            }

                            if (tx.local())
                                return null;

                            if (!marked)
                                marked = tx.markFinalizing(USER_FINISH);

                            if (marked)
                                tx.addEntry(cacheCtx, txEntry.txKey(), txEntry.keyBytes(), txEntry.op(), txEntry.value(),
                                    txEntry.valueBytes(), txEntry.drVersion());
                            else
                                return null;

                            if (req.groupLock()) {
                                tx.markGroupLock();

                                if (!txEntry.groupLockEntry())
                                    tx.groupLockKey(txEntry.txKey());
                            }

                            // Add remote candidate before reordering.
                            if (txEntry.explicitVersion() == null && !txEntry.groupLockEntry())
                                entry.addRemote(
                                    req.nearNodeId(),
                                    nodeId,
                                    req.threadId(),
                                    req.version(),
                                    0,
                                    /*tx*/true,
                                    tx.implicitSingle(),
                                    null
                                );
                        }

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nearNodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            if (tx != null)
                                tx.rollback();

                            return null;
                        }

                        // Entry is legit.
                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        assert entry.obsoleteVersion() != null : "Obsolete flag not set on removed entry: " +
                            entry;

                        if (log.isDebugEnabled())
                            log.debug("Received entry removed exception (will retry on renewed entry): " + entry);

                        if (tx != null) {
                            tx.clearEntry(txEntry.txKey());

                            if (log.isDebugEnabled())
                                log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                                    entry + ", tx=" + tx + ']');
                        }

                        // Will retry in while loop.
                    }
                }
            }
        }
        else {
            String err = "Failed to acquire deployment class loader for message: " + req;

            U.warn(log, err);

            throw new GridException(err);
        }

        return tx;
    }
}
