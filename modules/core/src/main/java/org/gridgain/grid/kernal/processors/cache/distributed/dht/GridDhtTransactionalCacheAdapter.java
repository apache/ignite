/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheTxEx.FinalizationStatus.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

/**
 * Base class for transactional DHT caches.
 */
public abstract class GridDhtTransactionalCacheAdapter<K, V> extends GridDhtCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridDhtTransactionalCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    protected GridDhtTransactionalCacheAdapter(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * Constructor used for near-only cache.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDhtTransactionalCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        super.start();

        preldr = new GridDhtPreloader<>(ctx);

        preldr.start();

        ctx.io().addHandler(GridNearGetRequest.class, new CI2<UUID, GridNearGetRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearGetRequest<K, V> req) {
                processNearGetRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtTxPrepareRequest.class, new CI2<UUID, GridDhtTxPrepareRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtTxPrepareRequest<K, V> req) {
                processDhtTxPrepareRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtTxPrepareResponse.class, new CI2<UUID, GridDhtTxPrepareResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtTxPrepareResponse<K, V> res) {
                processDhtTxPrepareResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(GridNearTxPrepareRequest.class, new CI2<UUID, GridNearTxPrepareRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearTxPrepareRequest<K, V> req) {
                processNearTxPrepareRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridNearTxFinishRequest.class, new CI2<UUID, GridNearTxFinishRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
                processNearTxFinishRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtTxFinishRequest.class, new CI2<UUID, GridDhtTxFinishRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtTxFinishRequest<K, V> req) {
                processDhtTxFinishRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtTxFinishResponse.class, new CI2<UUID, GridDhtTxFinishResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtTxFinishResponse<K, V> req) {
                processDhtTxFinishResponse(nodeId, req);
            }
        });

        ctx.io().addHandler(GridNearLockRequest.class, new CI2<UUID, GridNearLockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearLockRequest<K, V> req) {
                processNearLockRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtLockRequest.class, new CI2<UUID, GridDhtLockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtLockRequest<K, V> req) {
                processDhtLockRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtLockResponse.class, new CI2<UUID, GridDhtLockResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtLockResponse<K, V> req) {
                processDhtLockResponse(nodeId, req);
            }
        });

        ctx.io().addHandler(GridNearUnlockRequest.class, new CI2<UUID, GridNearUnlockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearUnlockRequest<K, V> req) {
                processNearUnlockRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtUnlockRequest.class, new CI2<UUID, GridDhtUnlockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtUnlockRequest<K, V> req) {
                processDhtUnlockRequest(nodeId, req);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public abstract GridNearTransactionalCache<K, V> near();

    /** {@inheritDoc} */
    @Override public void dgc() {
        ctx.dgc().dgc();
    }

    /** {@inheritDoc} */
    @Override public void dgc(long suspectLockTimeout, boolean global, boolean rmvLocks) {
        ctx.dgc().dgc(suspectLockTimeout, global, rmvLocks);
    }

    /**
     * @param req Request.
     * @throws GridException If failed.
     */
    private void unmarshal(GridDistributedTxPrepareRequest<K, V> req) throws GridException {
        for (GridCacheTxEntry<K, V> e : F.concat(false, req.reads(), req.writes()))
            e.unmarshal(ctx, ctx.deploy().globalLoader());
    }

    /**
     * @param nearNode Near node that initiated transaction.
     * @param req Near prepare request.
     * @return Future for transaction.
     */
    public GridFuture<GridCacheTxEx<K, V>> prepareTx(final GridNode nearNode,
        final GridNearTxPrepareRequest<K, V> req) {
        try {
            unmarshal(req);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }

        GridFuture<Object> fut = ctx.preloader().request(
            F.viewReadOnly(F.concat(false, req.reads(), req.writes()), CU.<K, V>tx2key()), req.topologyVersion());

        return new GridEmbeddedFuture<>(
            ctx.kernalContext(),
            fut,
            new C2<Object, Exception, GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public GridFuture<GridCacheTxEx<K, V>> apply(Object o, Exception ex) {
                    if (ex != null)
                        throw new GridClosureException(ex);

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
                            req.isInvalidate(),
                            req.syncCommit(),
                            req.syncRollback(),
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

                        if (tx.isRollbackOnly())
                            try {
                                tx.rollback();
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to rollback transaction: " + tx, e);
                            }

                        return fut;
                    }
                    else
                        return new GridFinishedFuture<>(ctx.kernalContext(), (GridCacheTxEx<K, V>)null);
                }
            },
            new C2<GridCacheTxEx<K, V>, Exception, GridCacheTxEx<K, V>>() {
                @Nullable
                @Override public GridCacheTxEx<K, V> apply(GridCacheTxEx<K, V> tx, Exception e) {
                    if (e != null) {
                        if (tx != null)
                            tx.setRollbackOnly(); // Just in case.

                        if (!(e instanceof GridCacheTxOptimisticException))
                            U.error(log, "Failed to prepare DHT transaction: " + tx, e);
                    }

                    return tx;
                }
            }
        );
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    public GridFuture<GridCacheTx> commitTx(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
        GridFuture<GridCacheTx> f = finish(nodeId, req);

        return f == null ? new GridFinishedFuture<GridCacheTx>(ctx.kernalContext()) : f;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    public GridFuture<GridCacheTx> rollbackTx(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
        GridFuture<GridCacheTx> f = finish(nodeId, req);

        return f == null ? new GridFinishedFuture<GridCacheTx>(ctx.kernalContext()) : f;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     * @return Future.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    @Nullable private GridFuture<GridCacheTx> finish(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

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
                            req.isInvalidate(),
                            req.commit() && req.replyRequired(),
                            !req.commit() && req.replyRequired(),
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

                return tx.commitAsync();
            }
            else {
                assert tx != null : "Transaction is null for near rollback request [nodeId=" +
                    nodeId + ", req=" + req + "]";

                tx.nearFinishFutureId(req.futureId());
                tx.nearFinishMiniId(req.miniId());

                return tx.rollbackAsync();
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + req.commit() + ", tx=" + tx + ']', e);

            if (tx != null)
                return tx.rollbackAsync();

            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }
    }

    /**
     * @param ctx Context.
     * @param nodeId Node ID.
     * @param tx Transaction.
     * @param req Request.
     * @param writes Writes.
     */
    protected void finish(GridCacheContext<K, V> ctx,
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
     * @param nodeId Node ID.
     * @param req Request.
     * @param res Response.
     * @return Remote transaction.
     * @throws GridException If failed.
     */
    @Nullable GridDhtTxRemote<K, V> startRemoteTx(UUID nodeId,
        GridDhtTxPrepareRequest<K, V> req,
        GridDhtTxPrepareResponse res) throws GridException {
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
                    tx.addWrite(entry, ctx.deploy().globalLoader());

                    if (isNearEnabled(ctx) && req.invalidateNearEntry(idx))
                        invalidateNearEntry(entry.key(), req.version());

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

            while (true) {
                try {
                    int part = ctx.affinity().partition(txEntry.key());

                    GridDhtLocalPartition<K, V> locPart = ctx.topology().localPartition(part, req.topologyVersion(),
                        false);

                    // Handle implicit locks for pessimistic transactions.
                    if (tx == null)
                        tx = ctx.tm().tx(req.version());

                    if (locPart == null || !locPart.reserve()) {
                        if (log.isDebugEnabled())
                            log.debug("Local partition for given key is already evicted (will remove from tx) " +
                                "[key=" + txEntry.key() + ", part=" + part + ", locPart=" + locPart + ']');

                        if (tx != null)
                            tx.clearEntry(txEntry.key());

                        break;
                    }

                    try {
                        entry = entryExx(txEntry.key(), req.topologyVersion());

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

                        tx.addWrite(txEntry.op(), txEntry.key(), txEntry.keyBytes(), txEntry.value(),
                            txEntry.valueBytes(), txEntry.transformClosures(), txEntry.drVersion());

                        if (!marked) {
                            if (tx.markFinalizing(USER_FINISH))
                                marked = true;
                            else {
                                tx.clearEntry(txEntry.key());

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

                    tx.clearEntry(entry.key());

                    if (log.isDebugEnabled())
                        log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                            entry + ", tx=" + tx + ']');
                }
                catch (GridDhtInvalidPartitionException p) {
                    if (log.isDebugEnabled())
                        log.debug("Received invalid partition (will clear entry from tx) [part=" + p + ", req=" +
                            req + ", txEntry=" + txEntry + ']');

                    if (tx != null)
                        tx.clearEntry(entry.key());

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
     * @param res Response.
     * @return Remote transaction.
     * @throws GridException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable GridDhtTxRemote<K, V> startRemoteTx(UUID nodeId,
        GridDhtLockRequest<K, V> req,
        GridDhtLockResponse<K, V> res)
        throws GridException, GridDistributedLockCancelledException {
        List<K> keys = req.keys();
        List<GridCacheTxEntry<K, V>> writes = req.writeEntries();

        GridDhtTxRemote<K, V> tx = null;

        int size = F.size(keys);

        for (int i = 0; i < size; i++) {
            K key = keys.get(i);

            if (key == null)
                continue;

            GridCacheTxEntry<K, V> writeEntry = writes == null ? null : writes.get(i);

            assert F.isEmpty(req.candidatesByIndex(i));

            GridCacheVersion drVer = req.drVersionByIndex(i);

            if (log.isDebugEnabled())
                log.debug("Unmarshalled key: " + key);

            GridDistributedCacheEntry<K, V> entry = null;

            while (true) {
                try {
                    int part = ctx.affinity().partition(key);

                    GridDhtLocalPartition<K, V> locPart = ctx.topology().localPartition(part, req.topologyVersion(),
                        false);

                    if (locPart == null || !locPart.reserve()) {
                        if (log.isDebugEnabled())
                            log.debug("Local partition for given key is already evicted (will add to invalid " +
                                "partition list) [key=" + key + ", part=" + part + ", locPart=" + locPart + ']');

                        res.addInvalidPartition(part);

                        // Invalidate key in near cache, if any.
                        if (isNearEnabled(cacheCfg))
                            invalidateNearEntry(key, req.version());

                        break;
                    }

                    try {
                        // Handle implicit locks for pessimistic transactions.
                        if (req.inTx()) {
                            if (tx == null)
                                tx = ctx.tm().tx(req.version());

                            if (tx == null) {
                                tx = new GridDhtTxRemote<>(
                                    req.nodeId(),
                                    req.futureId(),
                                    nodeId,
                                    req.nearXidVersion(),
                                    req.threadId(),
                                    req.topologyVersion(),
                                    req.version(),
                                    /*commitVer*/null,
                                    PESSIMISTIC,
                                    req.isolation(),
                                    req.isInvalidate(),
                                    req.timeout(),
                                    ctx,
                                    req.txSize(),
                                    req.groupLockKey(),
                                    req.subjectId(),
                                    req.taskNameHash());

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !ctx.tm().onStarted(tx))
                                    throw new GridCacheTxRollbackException("Failed to acquire lock (transaction " +
                                        "has been completed) [ver=" + req.version() + ", tx=" + tx + ']');
                            }

                            tx.addWrite(
                                writeEntry == null ? NOOP : writeEntry.op(),
                                key,
                                req.keyBytes() != null ? req.keyBytes().get(i) : null,
                                writeEntry == null ? null : writeEntry.value(),
                                writeEntry == null ? null : writeEntry.valueBytes(),
                                writeEntry == null ? null : writeEntry.transformClosures(),
                                drVer);

                            if (req.groupLock())
                                tx.groupLockKey(key);
                        }

                        entry = entryExx(key, req.topologyVersion());

                        // Add remote candidate before reordering.
                        entry.addRemote(
                            req.nodeId(),
                            nodeId,
                            req.threadId(),
                            req.version(),
                            req.timeout(),
                            tx != null,
                            tx != null && tx.implicitSingle(),
                            null
                        );

                        // Invalidate key in near cache, if any.
                        if (isNearEnabled(cacheCfg) && req.invalidateNearEntry(i))
                            invalidateNearEntry(key, req.version());

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            entry.removeLock(req.version());

                            if (tx != null) {
                                tx.clearEntry(entry.key());

                                // If there is a concurrent salvage, there could be a case when tx is moved to
                                // COMMITTING state, but this lock is never acquired.
                                if (tx.state() == COMMITTING)
                                    tx.forceCommit();
                                else
                                    tx.rollback();
                            }

                            return null;
                        }

                        // Entry is legit.
                        break;
                    }
                    finally {
                        locPart.release();
                    }
                }
                catch (GridDhtInvalidPartitionException e) {
                    if (log.isDebugEnabled())
                        log.debug("Received invalid partition exception [e=" + e + ", req=" + req + ']');

                    res.addInvalidPartition(e.partition());

                    // Invalidate key in near cache, if any.
                    if (isNearEnabled(cacheCfg))
                        invalidateNearEntry(key, req.version());

                    if (tx != null) {
                        tx.clearEntry(key);

                        if (log.isDebugEnabled())
                            log.debug("Cleared invalid entry from remote transaction (will skip) [entry=" +
                                entry + ", tx=" + tx + ']');
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    assert entry.obsoleteVersion() != null : "Obsolete flag not set on removed entry: " +
                        entry;

                    if (log.isDebugEnabled())
                        log.debug("Received entry removed exception (will retry on renewed entry): " + entry);

                    if (tx != null) {
                        tx.clearEntry(entry.key());

                        if (log.isDebugEnabled())
                            log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                                entry + ", tx=" + tx + ']');
                    }
                }
            }
        }

        if (tx != null && tx.empty()) {
            if (log.isDebugEnabled())
                log.debug("Rolling back remote DHT transaction because it is empty [req=" + req + ", res=" + res + ']');

            tx.rollback();

            tx = null;
        }

        return tx;
    }
    /**
     * @param nodeId Near node ID.
     * @param req Request.
     */
    private void processNearTxPrepareRequest(UUID nodeId, GridNearTxPrepareRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);
        assert nodeId != null;
        assert req != null;

        GridNode nearNode = ctx.node(nodeId);

        if (nearNode == null) {
            if (log.isDebugEnabled())
                log.debug("Received transaction request from node that left grid (will ignore): " + nodeId);

            return;
        }

        prepareTx(nearNode, req);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processNearTxFinishRequest(UUID nodeId, GridNearTxFinishRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);

        if (log.isDebugEnabled())
            log.debug("Processing near tx finish request [nodeId=" + nodeId + ", req=" + req + "]");

        GridFuture<?> f = finish(nodeId, req);

        if (f != null)
            // Only for error logging.
            f.listenAsync(CU.errorLogger(log));
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    protected final void processDhtTxPrepareRequest(UUID nodeId, GridDhtTxPrepareRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx prepare request [locNodeId=" + locNodeId + ", nodeId=" + nodeId + ", req=" +
                req + ']');

        GridDhtTxRemote<K, V> dhtTx = null;
        GridNearTxRemote<K, V> nearTx = null;

        GridDhtTxPrepareResponse<K, V> res;

        try {
            res = new GridDhtTxPrepareResponse<>(req.version(), req.futureId(), req.miniId());

            // Start near transaction first.
            nearTx = isNearEnabled(cacheCfg) ? near().startRemoteTx(ctx.deploy().globalLoader(), nodeId, req) : null;
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
        if (req.onePhaseCommit() && beforePessimisticLock != null) {
            GridFuture<Object> f = beforePessimisticLock.apply(F.viewReadOnly(req.writes(), CU.<K, V>tx2key()), true);

            if (f != null && !f.isDone()) {
                f.listenAsync(new CI1<GridFuture<Object>>() {
                    @Override public void apply(GridFuture<Object> t) {
                        processDhtTxFinishRequest0(nodeId, req);
                    }
                });
            }
            else
                processDhtTxFinishRequest0(nodeId, req);
        }
        else
            processDhtTxFinishRequest0(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"unchecked"})
    protected final void processDhtTxFinishRequest0(final UUID nodeId, final GridDhtTxFinishRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing dht tx finish request [nodeId=" + nodeId + ", req=" + req + ']');

        GridDhtTxRemote<K, V> dhtTx = ctx.tm().tx(req.version());
        GridCacheTxEx<K, V> nearTx = isNearEnabled(cacheCfg) ? near().context().tm().tx(req.version()) : null;

        try {
            if (dhtTx == null && !F.isEmpty(req.writes()))
                dhtTx = startRemoteTxForFinish(nodeId, req);

            // One-phase commit transactions send finish requests to backup nodes.
            if (dhtTx != null && req.onePhaseCommit()) {
                dhtTx.onePhaseCommit(true);

                dhtTx.writeVersion(req.writeVersion());
            }

            if (isNearEnabled(cacheCfg) && nearTx == null && !F.isEmpty(req.nearWrites()) && req.groupLock())
                nearTx = near().startRemoteTxForFinish(nodeId, req);
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

        finish(ctx, nodeId, dhtTx, req, req.writes());

        if (nearTx != null && isNearEnabled(cacheCfg))
            finish(near().context(), nodeId, (GridCacheTxRemoteEx<K, V>)nearTx, req, req.nearWrites());

        sendReply(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected final void processDhtLockRequest(final UUID nodeId, final GridDhtLockRequest<K, V> req) {
        GridFuture<Object> keyFut = F.isEmpty(req.keys()) ? null :
            ctx.dht().dhtPreloader().request(req.keys(), req.topologyVersion());

        if (beforePessimisticLock != null) {
            keyFut = keyFut == null ?
                beforePessimisticLock.apply(req.keys(), req.inTx()) :
                new GridEmbeddedFuture<>(true, keyFut,
                    new C2<Object, Exception, GridFuture<Object>>() {
                        @Override public GridFuture<Object> apply(Object o, Exception e) {
                            return beforePessimisticLock.apply(req.keys(), req.inTx());
                        }
                    }, ctx.kernalContext());
        }

        if (keyFut == null || keyFut.isDone())
            processDhtLockRequest0(nodeId, req);
        else {
            keyFut.listenAsync(new CI1<GridFuture<Object>>() {
                @Override public void apply(GridFuture<Object> t) {
                    processDhtLockRequest0(nodeId, req);
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected final void processDhtLockRequest0(UUID nodeId, GridDhtLockRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;
        assert !nodeId.equals(locNodeId);

        if (log.isDebugEnabled())
            log.debug("Processing dht lock request [locNodeId=" + locNodeId + ", nodeId=" + nodeId + ", req=" + req +
                ']');

        int cnt = F.size(req.keys());

        GridDhtLockResponse<K, V> res;

        GridDhtTxRemote<K, V> dhtTx = null;
        GridNearTxRemote<K, V> nearTx = null;

        boolean fail = false;
        boolean cancelled = false;

        try {
            res = new GridDhtLockResponse<>(req.version(), req.futureId(), req.miniId(), cnt);

            dhtTx = startRemoteTx(nodeId, req, res);
            nearTx = isNearEnabled(cacheCfg) ? near().startRemoteTx(nodeId, req) : null;

            if (nearTx != null) {
                // This check allows to avoid extra serialization.
                if (nearTx.hasEvictedBytes())
                    res.nearEvictedBytes(nearTx.evictedBytes());
                else
                    res.nearEvicted(nearTx.evicted());
            }
            else if (!F.isEmpty(req.nearKeyBytes()))
                res.nearEvictedBytes(req.nearKeyBytes());
            else if (!F.isEmpty(req.nearKeys()))
                res.nearEvicted(req.nearKeys());
        }
        catch (GridCacheTxRollbackException e) {
            String err = "Failed processing DHT lock request (transaction has been completed): " + req;

            U.error(log, err, e);

            res = new GridDhtLockResponse<>(req.version(), req.futureId(), req.miniId(),
                new GridCacheTxRollbackException(err, e));

            fail = true;
        }
        catch (GridException e) {
            String err = "Failed processing DHT lock request: " + req;

            U.error(log, err, e);

            res = new GridDhtLockResponse<>(req.version(), req.futureId(), req.miniId(), new GridException(err, e));

            fail = true;
        }
        catch (GridDistributedLockCancelledException ignored) {
            // Received lock request for cancelled lock.
            if (log.isDebugEnabled())
                log.debug("Received lock request for canceled lock (will ignore): " + req);

            res = null;

            fail = true;
            cancelled = true;
        }

        boolean releaseAll = false;

        if (res != null) {
            try {
                // Reply back to sender.
                ctx.io().send(nodeId, res);
            }
            catch (GridTopologyException ignored) {
                U.warn(log, "Failed to send lock reply to remote node because it left grid: " + nodeId);

                fail = true;
                releaseAll = true;
            }
            catch (GridException e) {
                U.error(log, "Failed to send lock reply to node (lock will not be acquired): " + nodeId, e);

                fail = true;
            }
        }

        if (fail) {
            if (dhtTx != null)
                dhtTx.rollback();

            if (nearTx != null) // Even though this should never happen, we leave this check for consistency.
                nearTx.rollback();

            List<K> keys = req.keys();

            if (keys != null) {
                for (K key : keys) {
                    while (true) {
                        GridDistributedCacheEntry<K, V> entry = peekExx(key);

                        try {
                            if (entry != null) {
                                // Release all locks because sender node left grid.
                                if (releaseAll)
                                    entry.removeExplicitNodeLocks(req.nodeId());
                                else
                                    entry.removeLock(req.version());
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Attempted to remove lock on removed entity during during failure " +
                                    "handling for dht lock request (will retry): " + entry);
                        }
                    }
                }
            }

            if (releaseAll && !cancelled)
                U.warn(log, "Sender node left grid in the midst of lock acquisition (locks have been released).");
        }
    }

    /** {@inheritDoc} */
    protected void processDhtUnlockRequest(UUID nodeId, GridDhtUnlockRequest<K, V> req) {
        clearLocks(nodeId, req);

        if (isNearEnabled(cacheCfg))
            near().clearLocks(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxPrepareResponse(UUID nodeId, GridDhtTxPrepareResponse<K, V> res) {
        GridDhtTxPrepareFuture<K, V> fut = (GridDhtTxPrepareFuture<K, V>)ctx.mvcc().
            <GridCacheTxEx<K, V>>future(res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

        fut.onResult(nodeId, res);
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
     */
    private void processNearLockRequest(UUID nodeId, GridNearLockRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing near lock request [locNodeId=" + locNodeId + ", nodeId=" + nodeId + ", req=" + req +
                ']');

        GridNode nearNode = ctx.discovery().node(nodeId);

        if (nearNode == null) {
            U.warn(log, "Received lock request from unknown node (will ignore): " + nodeId);

            return;
        }

        // Group lock can be only started from local node, so we never start group lock transaction on remote node.
        GridFuture<?> f = lockAllAsync(nearNode, req, null);

        // Register listener just so we print out errors.
        // Exclude lock timeout exception since it's not a fatal exception.
        f.listenAsync(CU.errorLogger(log, GridCacheLockTimeoutException.class,
            GridDistributedLockCancelledException.class));
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtTxFinishResponse(UUID nodeId, GridDhtTxFinishResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;

        GridDhtTxFinishFuture<K, V> fut = (GridDhtTxFinishFuture<K, V>)ctx.mvcc().<GridCacheTx>future(res.xid(),
            res.futureId());

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
    private void processDhtLockResponse(UUID nodeId, GridDhtLockResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;
        GridDhtLockFuture<K, V> fut = (GridDhtLockFuture<K, V>)ctx.mvcc().<Boolean>future(res.version(),
            res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

        fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys,
        long timeout,
        GridCacheTxLocalEx<K, V> txx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        GridCacheTxIsolation isolation,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return lockAllAsyncInternal(keys, timeout, txx, isInvalidate, isRead, retval, isolation, filter);
    }

    /**
     * Acquires locks in partitioned cache.
     *
     * @param keys Keys to lock.
     * @param timeout Lock timeout.
     * @param txx Transaction.
     * @param isInvalidate Invalidate flag.
     * @param isRead Read flag.
     * @param retval Return value flag.
     * @param isolation Transaction isolation.
     * @param filter Optional filter.
     * @return Lock future.
     */
    public GridDhtFuture<Boolean> lockAllAsyncInternal(@Nullable Collection<? extends K> keys,
        long timeout,
        GridCacheTxLocalEx<K, V> txx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        GridCacheTxIsolation isolation,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        if (keys == null || keys.isEmpty())
            return new GridDhtFinishedFuture<>(ctx.kernalContext(), true);

        GridDhtTxLocalAdapter<K, V> tx = (GridDhtTxLocalAdapter<K, V>)txx;

        assert tx != null;

        GridDhtLockFuture<K, V> fut = new GridDhtLockFuture<>(
            ctx,
            tx.nearNodeId(),
            tx.nearXidVersion(),
            tx.topologyVersion(),
            keys.size(),
            isRead,
            timeout,
            tx,
            tx.threadId(),
            filter);

        for (K key : keys) {
            if (key == null)
                continue;

            try {
                while (true) {
                    GridDhtCacheEntry<K, V> entry = entryExx(key, tx.topologyVersion());

                    try {
                        fut.addEntry(entry);

                        // Possible in case of cancellation or time out.
                        if (fut.isDone())
                            return fut;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry when adding lock (will retry): " + entry);
                    }
                    catch (GridDistributedLockCancelledException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got lock request for cancelled lock (will fail): " + entry);

                        return new GridDhtFinishedFuture<>(ctx.kernalContext(), e);
                    }
                }
            }
            catch (GridDhtInvalidPartitionException e) {
                fut.addInvalidPartition(e.partition());

                if (log.isDebugEnabled())
                    log.debug("Added invalid partition to DHT lock future [part=" + e.partition() + ", fut=" +
                        fut + ']');
            }
        }

        ctx.mvcc().addFuture(fut);

        fut.map();

        return fut;
    }

    /**
     * @param nearNode Near node.
     * @param req Request.
     * @param filter0 Filter.
     * @return Future.
     */
    public GridFuture<GridNearLockResponse<K, V>> lockAllAsync(final GridNode nearNode,
        final GridNearLockRequest<K, V> req,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter0) {
        final List<K> keys = req.keys();

        GridFuture<Object> keyFut = ctx.dht().dhtPreloader().request(keys, req.topologyVersion());

        if (beforePessimisticLock != null) {
            keyFut = new GridEmbeddedFuture<>(true, keyFut,
                new C2<Object, Exception, GridFuture<Object>>() {
                    @Override public GridFuture<Object> apply(Object o, Exception e) {
                        return beforePessimisticLock.apply(keys, req.inTx());
                    }
                }, ctx.kernalContext());
        }

        return new GridEmbeddedFuture<>(true, keyFut,
            new C2<Object, Exception, GridFuture<GridNearLockResponse<K,V>>>() {
                @Override public GridFuture<GridNearLockResponse<K, V>> apply(Object o, Exception exx) {
                    if (exx != null)
                        return new GridDhtFinishedFuture<>(ctx.kernalContext(), exx);

                    GridPredicate<GridCacheEntry<K, V>>[] filter = filter0;

                    // Set message into thread context.
                    GridDhtTxLocal<K, V> tx = null;

                    try {
                        int cnt = keys.size();

                        if (req.inTx()) {
                            GridCacheVersion dhtVer = ctx.tm().mappedVersion(req.version());

                            if (dhtVer != null)
                                tx = ctx.tm().tx(dhtVer);
                        }

                        final List<GridCacheEntryEx<K, V>> entries = new ArrayList<>(cnt);

                        // Unmarshal filter first.
                        if (filter == null)
                            filter = req.filter();

                        GridDhtLockFuture<K, V> fut = null;

                        if (!req.inTx()) {
                            fut = new GridDhtLockFuture<>(ctx, nearNode.id(), req.version(),
                                req.topologyVersion(), cnt, req.txRead(), req.timeout(), tx, req.threadId(), filter);

                            // Add before mapping.
                            if (!ctx.mvcc().addFuture(fut))
                                throw new IllegalStateException("Duplicate future ID: " + fut);
                        }

                        boolean timedout = false;

                        for (K key : keys) {
                            if (timedout)
                                break;

                            while (true) {
                                // Specify topology version to make sure containment is checked
                                // based on the requested version, not the latest.
                                GridDhtCacheEntry<K, V> entry = entryExx(key, req.topologyVersion());

                                try {
                                    if (fut != null) {
                                        // This method will add local candidate.
                                        // Entry cannot become obsolete after this method succeeded.
                                        fut.addEntry(key == null ? null : entry);

                                        if (fut.isDone()) {
                                            timedout = true;

                                            break;
                                        }
                                    }

                                    entries.add(entry);

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed entry when adding lock (will retry): " + entry);
                                }
                                catch (GridDistributedLockCancelledException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got lock request for cancelled lock (will ignore): " +
                                            entry);

                                    fut.onError(e);

                                    return new GridDhtFinishedFuture<>(ctx.kernalContext(), e);
                                }
                            }
                        }

                        // Handle implicit locks for pessimistic transactions.
                        if (req.inTx()) {
                            if (tx == null) {
                                tx = new GridDhtTxLocal<>(
                                    nearNode.id(),
                                    req.version(),
                                    req.futureId(),
                                    req.miniId(),
                                    req.threadId(),
                                    req.implicitTx(),
                                    req.implicitSingleTx(),
                                    ctx,
                                    PESSIMISTIC,
                                    req.isolation(),
                                    req.timeout(),
                                    req.isInvalidate(),
                                    req.syncCommit(),
                                    req.syncRollback(),
                                    false,
                                    req.txSize(),
                                    req.groupLockKey(),
                                    req.partitionLock(),
                                    null,
                                    req.subjectId(),
                                    req.taskNameHash());

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !tx.init()) {
                                    String msg = "Failed to acquire lock (transaction has been completed): " +
                                        req.version();

                                    U.warn(log, msg);

                                    if (tx != null)
                                        tx.rollback();

                                    return new GridDhtFinishedFuture<>(ctx.kernalContext(), new GridException(msg));
                                }

                                tx.topologyVersion(req.topologyVersion());
                            }

                            ctx.tm().txContext(tx);

                            if (log.isDebugEnabled())
                                log.debug("Performing DHT lock [tx=" + tx + ", entries=" + entries + ']');

                            assert req.writeEntries() == null || req.writeEntries().size() == entries.size();

                            GridFuture<GridCacheReturn<V>> txFut = tx.lockAllAsync(
                                entries,
                                req.writeEntries(),
                                req.onePhaseCommit(),
                                req.drVersions(),
                                req.messageId(),
                                req.implicitTx(),
                                req.txRead());

                            final GridDhtTxLocal<K, V> t = tx;

                            return new GridDhtEmbeddedFuture<>(
                                txFut,
                                new C2<GridCacheReturn<V>, Exception, GridFuture<GridNearLockResponse<K, V>>>() {
                                    @Override public GridFuture<GridNearLockResponse<K, V>> apply(GridCacheReturn<V> o,
                                                                                                  Exception e) {
                                        if (e != null)
                                            e = U.unwrap(e);

                                        assert !t.empty();

                                        // Create response while holding locks.
                                        final GridNearLockResponse<K, V> resp = createLockReply(nearNode,
                                            entries, req, t, t.xidVersion(), e);

                                        if (resp.error() == null && t.onePhaseCommit()) {
                                            assert t.implicit();

                                            return t.commitAsync().chain(
                                                new C1<GridFuture<GridCacheTx>, GridNearLockResponse<K, V>>() {
                                                    @Override public GridNearLockResponse<K, V> apply(GridFuture<GridCacheTx> f) {
                                                        try {
                                                            // Check for error.
                                                            f.get();
                                                        }
                                                        catch (GridException e1) {
                                                            resp.error(e1);
                                                        }

                                                        sendLockReply(nearNode, t, req, resp);

                                                        return resp;
                                                    }
                                                });
                                        }
                                        else {
                                            sendLockReply(nearNode, t, req, resp);

                                            return new GridFinishedFutureEx<>(resp);
                                        }
                                    }
                                },
                                ctx.kernalContext());
                        }
                        else {
                            assert fut != null;

                            // This will send remote messages.
                            fut.map();

                            final GridCacheVersion mappedVer = fut.version();

                            return new GridDhtEmbeddedFuture<>(
                                ctx.kernalContext(),
                                fut,
                                new C2<Boolean, Exception, GridNearLockResponse<K, V>>() {
                                    @Override public GridNearLockResponse<K, V> apply(Boolean b, Exception e) {
                                        if (e != null)
                                            e = U.unwrap(e);
                                        else if (!b)
                                            e = new GridCacheLockTimeoutException(req.version());

                                        GridNearLockResponse<K, V> res = createLockReply(nearNode, entries, req,
                                            null, mappedVer, e);

                                        sendLockReply(nearNode, null, req, res);

                                        return res;
                                    }
                                });
                        }
                    }
                    catch (GridException e) {
                        String err = "Failed to unmarshal at least one of the keys for lock request message: " + req;

                        U.error(log, err, e);

                        if (tx != null) {
                            try {
                                tx.rollback();
                            }
                            catch (GridException ex) {
                                U.error(log, "Failed to rollback the transaction: " + tx, ex);
                            }
                        }

                        return new GridDhtFinishedFuture<>(ctx.kernalContext(),
                            new GridException(err, e));
                    }
                }
            },
            ctx.kernalContext());
    }

    /**
     * @param nearNode Near node.
     * @param entries Entries.
     * @param req Lock request.
     * @param tx Transaction.
     * @param mappedVer Mapped version.
     * @param err Error.
     * @return Response.
     */
    private GridNearLockResponse<K, V> createLockReply(
        GridNode nearNode,
        List<GridCacheEntryEx<K, V>> entries,
        GridNearLockRequest<K, V> req,
        @Nullable GridDhtTxLocalAdapter<K,V> tx,
        GridCacheVersion mappedVer,
        Throwable err) {
        assert mappedVer != null;
        assert tx == null || tx.xidVersion().equals(mappedVer);

        try {
            // Send reply back to originating near node.
            GridNearLockResponse<K, V> res = new GridNearLockResponse<>(
                req.version(), req.futureId(), req.miniId(), tx != null && tx.onePhaseCommit(), entries.size(), err);

            if (err == null) {
                res.pending(localDhtPendingVersions(entries, mappedVer));

                // We have to add completed versions for cases when nearLocal and remote transactions
                // execute concurrently.
                res.completedVersions(ctx.tm().committedVersions(req.version()),
                    ctx.tm().rolledbackVersions(req.version()));

                int i = 0;

                for (ListIterator<GridCacheEntryEx<K, V>> it = entries.listIterator(); it.hasNext();) {
                    GridCacheEntryEx<K, V> e = it.next();

                    assert e != null;

                    while (true) {
                        try {
                            // Don't return anything for invalid partitions.
                            if (tx == null || !tx.isRollbackOnly()) {
                                GridCacheVersion dhtVer = req.dhtVersion(i);

                                try {
                                    GridCacheVersion ver = e.version();

                                    boolean ret = req.returnValue(i) || dhtVer == null || !dhtVer.equals(ver);

                                    V val = null;

                                    if (ret)
                                        val = e.innerGet(tx,
                                            /*swap*/true,
                                            /*read-through*/true,
                                            /*fail-fast.*/false,
                                            /*unmarshal*/false,
                                            /*update-metrics*/true,
                                            /*event notification*/req.returnValue(i),
                                            CU.subjectId(tx, ctx),
                                            null, // TODO: GG-8999: Is it fine?
                                            tx != null ? tx.resolveTaskName() : null,
                                            CU.<K, V>empty());

                                    assert e.lockedBy(mappedVer) ||
                                        (ctx.mvcc().isRemoved(mappedVer) && req.timeout() > 0) :
                                        "Entry does not own lock for tx [locNodeId=" + ctx.localNodeId() +
                                            ", entry=" + e +
                                            ", mappedVer=" + mappedVer + ", ver=" + ver +
                                            ", tx=" + tx + ", req=" + req +
                                            ", err=" + err + ']';

                                    boolean filterPassed = false;

                                    if (tx != null && tx.onePhaseCommit()) {
                                        GridCacheTxEntry<K, V> writeEntry = tx.entry(e.key());

                                        assert writeEntry != null :
                                            "Missing tx entry for locked cache entry: " + e;

                                        filterPassed = writeEntry.filtersPassed();
                                    }

                                    // We include values into response since they are required for local
                                    // calls and won't be serialized. We are also including DHT version.
                                    res.addValueBytes(
                                        val,
                                        ret ? e.valueBytes(null).getIfMarshaled() : null,
                                        filterPassed,
                                        ver,
                                        mappedVer,
                                        ctx);
                                }
                                catch (GridCacheFilterFailedException ex) {
                                    assert false : "Filter should never fail if fail-fast is false.";

                                    ex.printStackTrace();
                                }
                            }
                            else {
                                // We include values into response since they are required for local
                                // calls and won't be serialized. We are also including DHT version.
                                res.addValueBytes(null, null, false, e.version(), mappedVer, ctx);
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry when sending reply to DHT lock request " +
                                    "(will retry): " + e);

                            e = entryExx(e.key());

                            it.set(e);
                        }
                    }

                    i++;
                }
            }

            return res;
        }
        catch (GridException e) {
            U.error(log, "Failed to get value for lock reply message for node [node=" +
                U.toShortString(nearNode) + ", req=" + req + ']', e);

            return new GridNearLockResponse<>(req.version(), req.futureId(), req.miniId(), false, entries.size(), e);
        }
    }

    /**
     * Send lock reply back to near node.
     *
     * @param nearNode Near node.
     * @param tx Transaction.
     * @param req Lock request.
     * @param res Lock response.
     */
    private void sendLockReply(
        GridNode nearNode,
        @Nullable GridCacheTxEx<K,V> tx,
        GridNearLockRequest<K, V> req,
        GridNearLockResponse<K, V> res
    ) {
        Throwable err = res.error();

        // Log error before sending reply.
        if (err != null && !(err instanceof GridCacheLockTimeoutException))
            U.error(log, "Failed to acquire lock for request: " + req, err);

        try {
            // Don't send reply message to this node or if lock was cancelled.
            if (!nearNode.id().equals(ctx.nodeId()) && !X.hasCause(err, GridDistributedLockCancelledException.class))
                ctx.io().send(nearNode, res);
        }
        catch (GridException e) {
            U.error(log, "Failed to send lock reply to originating node (will rollback transaction) [node=" +
                U.toShortString(nearNode) + ", req=" + req + ']', e);

            if (tx != null)
                tx.rollbackAsync();

            // Convert to closure exception as this method is only called form closures.
            throw new GridClosureException(e);
        }
    }

    /**
     * Collects versions of pending candidates versions less then base.
     *
     * @param entries Tx entries to process.
     * @param baseVer Base version.
     * @return Collection of pending candidates versions.
     */
    private Collection<GridCacheVersion> localDhtPendingVersions(Iterable<GridCacheEntryEx<K, V>> entries,
        GridCacheVersion baseVer) {
        Collection<GridCacheVersion> lessPending = new GridLeanSet<>(5);

        for (GridCacheEntryEx<K, V> entry : entries) {
            // Since entries were collected before locks are added, some of them may become obsolete.
            while (true) {
                try {
                    for (GridCacheMvccCandidate cand : entry.localCandidates()) {
                        if (cand.version().isLess(baseVer))
                            lessPending.add(cand.version());
                    }

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry is localDhtPendingVersions (will retry): " + entry);

                    entry = entryExx(entry.key());
                }
            }
        }

        return lessPending;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private void clearLocks(UUID nodeId, GridDistributedUnlockRequest<K, V> req) {
        assert nodeId != null;

        List<K> keys = req.keys();

        if (keys != null) {
            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    boolean created = false;

                    if (entry == null) {
                        entry = entryExx(key);

                        created = true;
                    }

                    try {
                        entry.doneRemote(
                            req.version(),
                            req.version(),
                            null,
                            null,
                            null,
                            /*system invalidate*/false);

                        // Note that we don't reorder completed versions here,
                        // as there is no point to reorder relative to the version
                        // we are about to remove.
                        if (entry.removeLock(req.version())) {
                            if (afterPessimisticUnlock != null)
                                afterPessimisticUnlock.apply(entry.key(), false, NOOP);

                            if (log.isDebugEnabled())
                                log.debug("Removed lock [lockId=" + req.version() + ", key=" + key + ']');
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Received unlock request for unknown candidate " +
                                    "(added to cancelled locks set): " + req);
                        }

                        if (created && entry.markObsolete(req.version()))
                            removeEntry(entry);

                        ctx.evicts().touch(entry, ctx.affinity().affinityTopologyVersion());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Received remove lock request for removed entry (will retry) [entry=" +
                                entry + ", req=" + req + ']');
                    }
                }
            }
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param req Request.
     */
    @SuppressWarnings({"RedundantTypeArguments", "TypeMayBeWeakened"})
    private void processNearUnlockRequest(UUID nodeId, GridNearUnlockRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);
        assert nodeId != null;

        removeLocks(nodeId, req.version(), req.keys(), true);
    }

    /**
     * @param nodeId Sender node ID.
     * @param topVer Topology version.
     * @param cached Entry.
     * @param readers Readers for this entry.
     * @param dhtMap DHT map.
     * @param nearMap Near map.
     * @throws GridException If failed.
     */
    private void map(UUID nodeId,
        long topVer,
        GridCacheEntryEx<K,V> cached,
        Collection<UUID> readers,
        Map<GridNode, List<T2<K, byte[]>>> dhtMap,
        Map<GridNode, List<T2<K, byte[]>>> nearMap)
        throws GridException {
        Collection<GridNode> dhtNodes = ctx.dht().topology().nodes(cached.partition(), topVer);

        GridNode primary = F.first(dhtNodes);

        assert primary != null;

        if (!primary.id().equals(ctx.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Primary node mismatch for unlock [entry=" + cached + ", expected=" + ctx.nodeId() +
                    ", actual=" + U.toShortString(primary) + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.toShortString(dhtNodes) + ", entry=" + cached + ']');

        Collection<GridNode> nearNodes = null;

        if (!F.isEmpty(readers)) {
            nearNodes = ctx.discovery().nodes(readers, F0.not(F.idForNodeId(nodeId)));

            if (log.isDebugEnabled())
                log.debug("Mapping entry to near nodes [nodes=" + U.toShortString(nearNodes) + ", entry=" + cached +
                    ']');
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Entry has no near readers: " + cached);
        }

        map(cached, F.view(dhtNodes, F.remoteNodes(ctx.nodeId())), dhtMap); // Exclude local node.
        map(cached, nearNodes, nearMap);
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param map Map.
     * @throws GridException If failed.
     */
    @SuppressWarnings( {"MismatchedQueryAndUpdateOfCollection"})
    private void map(GridCacheEntryEx<K, V> entry,
        @Nullable Iterable<? extends GridNode> nodes,
        Map<GridNode, List<T2<K, byte[]>>> map) throws GridException {
        if (nodes != null) {
            for (GridNode n : nodes) {
                List<T2<K, byte[]>> keys = map.get(n);

                if (keys == null)
                    map.put(n, keys = new LinkedList<>());

                keys.add(new T2<>(entry.key(), entry.getOrMarshalKeyBytes()));
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param ver Version.
     * @param keys Keys.
     * @param unmap Flag for un-mapping version.
     */
    public void removeLocks(UUID nodeId, GridCacheVersion ver, Iterable<? extends K> keys, boolean unmap) {
        assert nodeId != null;
        assert ver != null;

        if (F.isEmpty(keys))
            return;

        // Remove mapped versions.
        GridCacheVersion dhtVer = unmap ? ctx.mvcc().unmapVersion(ver) : ver;

        Map<GridNode, List<T2<K, byte[]>>> dhtMap = new HashMap<>();
        Map<GridNode, List<T2<K, byte[]>>> nearMap = new HashMap<>();

        GridCacheVersion obsoleteVer = null;

        for (K key : keys) {
            while (true) {
                boolean created = false;

                GridDhtCacheEntry<K, V> entry = peekExx(key);

                if (entry == null) {
                    entry = entryExx(key);

                    created = true;
                }

                try {
                    GridCacheMvccCandidate<K> cand = null;

                    if (dhtVer == null) {
                        cand = entry.localCandidateByNearVersion(ver, true);

                        if (cand != null)
                            dhtVer = cand.version();
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failed to locate lock candidate based on dht or near versions [nodeId=" +
                                    nodeId + ", ver=" + ver + ", unmap=" + unmap + ", keys=" + keys + ']');

                            entry.removeLock(ver);

                            if (created) {
                                if (obsoleteVer == null)
                                    obsoleteVer = ctx.versions().next();

                                if (entry.markObsolete(obsoleteVer))
                                    removeEntry(entry);
                            }

                            break;
                        }
                    }

                    if (cand == null)
                        cand = entry.candidate(dhtVer);

                    long topVer = cand == null ? -1 : cand.topologyVersion();

                    // Note that we obtain readers before lock is removed.
                    // Even in case if entry would be removed just after lock is removed,
                    // we must send release messages to backups and readers.
                    Collection<UUID> readers = entry.readers();

                    // Note that we don't reorder completed versions here,
                    // as there is no point to reorder relative to the version
                    // we are about to remove.
                    if (entry.removeLock(dhtVer)) {
                        if (afterPessimisticUnlock != null)
                            afterPessimisticUnlock.apply(entry.key(), false, NOOP);

                        // Map to backups and near readers.
                        map(nodeId, topVer, entry, readers, dhtMap, nearMap);

                        if (log.isDebugEnabled())
                            log.debug("Removed lock [lockId=" + ver + ", key=" + key + ']');
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Received unlock request for unknown candidate " +
                            "(added to cancelled locks set) [ver=" + ver + ", entry=" + entry + ']');

                    if (created && entry.markObsolete(dhtVer))
                        removeEntry(entry);

                    ctx.evicts().touch(entry, topVer);

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Received remove lock request for removed entry (will retry): " + entry);
                }
                catch (GridException e) {
                    U.error(log, "Failed to remove locks for keys: " + keys, e);
                }
            }
        }

        Collection<GridCacheVersion> committed = ctx.tm().committedVersions(ver);
        Collection<GridCacheVersion> rolledback = ctx.tm().rolledbackVersions(ver);

        // Backups.
        for (Map.Entry<GridNode, List<T2<K, byte[]>>> entry : dhtMap.entrySet()) {
            GridNode n = entry.getKey();

            List<T2<K, byte[]>> keyBytes = entry.getValue();

            GridDhtUnlockRequest<K, V> req = new GridDhtUnlockRequest<>(keyBytes.size());

            req.version(dhtVer);

            try {
                for (T2<K, byte[]> key : keyBytes)
                    req.addKey(key.get1(), key.get2(), ctx);

                keyBytes = nearMap.get(n);

                if (keyBytes != null)
                    for (T2<K, byte[]> key : keyBytes)
                        req.addNearKey(key.get1(), key.get2(), ctx);

                req.completedVersions(committed, rolledback);

                ctx.io().send(n, req);
            }
            catch (GridTopologyException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Node left while sending unlock request: " + n);
            }
            catch (GridException e) {
                U.error(log, "Failed to send unlock request to node (will make best effort to complete): " + n, e);
            }
        }

        // Readers.
        for (Map.Entry<GridNode, List<T2<K, byte[]>>> entry : nearMap.entrySet()) {
            GridNode n = entry.getKey();

            if (!dhtMap.containsKey(n)) {
                List<T2<K, byte[]>> keyBytes = entry.getValue();

                GridDhtUnlockRequest<K, V> req = new GridDhtUnlockRequest<>(keyBytes.size());

                req.version(dhtVer);

                try {
                    for (T2<K, byte[]> key : keyBytes)
                        req.addNearKey(key.get1(), key.get2(), ctx);

                    req.completedVersions(committed, rolledback);

                    ctx.io().send(n, req);
                }
                catch (GridTopologyException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Node left while sending unlock request: " + n);
                }
                catch (GridException e) {
                    U.error(log, "Failed to send unlock request to node (will make best effort to complete): " + n, e);
                }
            }
        }
    }

    /**
     * @param key Key
     * @param ver Version.
     * @throws GridException If invalidate failed.
     */
    private void invalidateNearEntry(K key, GridCacheVersion ver) throws GridException {
        GridCacheEntryEx<K, V> nearEntry = near().peekEx(key);

        if (nearEntry != null)
            nearEntry.invalidate(null, ver);
    }
}
