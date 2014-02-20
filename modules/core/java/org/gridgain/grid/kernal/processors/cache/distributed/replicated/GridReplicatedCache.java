// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Fully replicated cache implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedCache<K, V> extends GridDistributedCacheAdapter<K, V> {
    /** Preloader. */
    private GridCachePreloader<K, V> preldr;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridReplicatedCache() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     */
    public GridReplicatedCache(GridCacheContext<K, V> ctx) {
        super(ctx, ctx.config().getStartSize());
    }

    /** {@inheritDoc} */
    @Override public boolean isReplicated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(
        boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean syncCommit,
        boolean syncRollback,
        boolean swapEnabled,
        boolean storeEnabled,
        int txSize,
        @Nullable Object grpLockKey,
        boolean partLock
    ) {
        return new GridReplicatedTxLocal<>(ctx, implicit, implicitSingle, concurrency, isolation, timeout,
            invalidate, syncCommit, syncRollback, swapEnabled, storeEnabled, txSize, grpLockKey, partLock);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridReplicatedCacheEntry<>(ctx, key, hash, val, next, ttl, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        super.start();

        GridCacheIoManager<K, V> io = ctx.io();

        io.addHandler(GridDistributedLockRequest.class, new CI2<UUID, GridDistributedLockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedLockRequest<K, V> req) {
                processLockRequest(nodeId, req);
            }
        });

        io.addHandler(GridDistributedLockResponse.class, new CI2<UUID, GridDistributedLockResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedLockResponse<K, V> res) {
                processLockResponse(nodeId, res);
            }
        });

        io.addHandler(GridDistributedTxFinishRequest.class, new CI2<UUID, GridDistributedTxFinishRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedTxFinishRequest<K, V> req) {
                processFinishRequest(nodeId, req);
            }
        });

        io.addHandler(GridDistributedTxFinishResponse.class, new CI2<UUID, GridDistributedTxFinishResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedTxFinishResponse<K, V> res) {
                processFinishResponse(nodeId, res);
            }
        });

        io.addHandler(GridDistributedTxPrepareRequest.class, new CI2<UUID, GridDistributedTxPrepareRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedTxPrepareRequest<K, V> req) {
                processPrepareRequest(nodeId, req);
            }
        });

        io.addHandler(GridDistributedTxPrepareResponse.class, new CI2<UUID, GridDistributedTxPrepareResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedTxPrepareResponse<K, V> res) {
                processPrepareResponse(nodeId, res);
            }
        });

        io.addHandler(GridDistributedUnlockRequest.class, new CI2<UUID, GridDistributedUnlockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDistributedUnlockRequest<K, V> req) {
                processUnlockRequest(nodeId, req);
            }
        });

        preldr = new GridReplicatedPreloader<>(ctx);

        preldr.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        super.stop();

        if (preldr != null)
            preldr.stop();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        if (preldr != null)
            preldr.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        super.onKernalStop();

        if (preldr != null)
            preldr.onKernalStop();
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader<K, V> preloader() {
        return preldr;
    }

    /**
     * Processes lock request.
     *
     * @param nodeId Sender node ID.
     * @param msg Lock request.
     */
    private void processLockRequest(final UUID nodeId, final GridDistributedLockRequest<K, V> msg) {
        GridFuture<Object> req = ctx.preloader().request(msg.keys(), ctx.kernalContext().discovery().topologyVersion());

        if (req.isDone())
            processLockRequest0(nodeId, msg);
        else {
            req.listenAsync(new CI1<GridFuture<Object>>() {
                @Override public void apply(GridFuture<Object> t) {
                    processLockRequest0(nodeId, msg);
                }
            });
        }
    }

    /**
     * Processes lock request.
     *
     * @param nodeId Sender node ID.
     * @param msg Lock request.
     */
    @SuppressWarnings({"unchecked", "ThrowableInstanceNeverThrown"})
    private void processLockRequest0(UUID nodeId, GridDistributedLockRequest<K, V> msg) {
        assert !nodeId.equals(locNodeId);

        List<K> keys = msg.keys();
        List<GridCacheTxEntry<K, V>> writes = msg.writeEntries();

        assert writes == null || writes.size() == keys.size();

        int cnt = keys.size();

        GridReplicatedTxRemote<K, V> tx = null;

        GridDistributedLockResponse res;

        try {
            res = new GridDistributedLockResponse(msg.version(), msg.futureId(), cnt);

            for (int i = 0; i < keys.size(); i++) {
                K key = msg.keys().get(i);
                GridCacheTxEntry<K, V> writeEntry = writes == null ? null : writes.get(i);

                Collection<GridCacheMvccCandidate<K>> cands = msg.candidatesByIndex(i);

                if (log.isDebugEnabled())
                    log.debug("Unmarshalled key: " + key);

                GridDistributedCacheEntry<K, V> entry = null;

                while (true) {
                    try {
                        entry = entryexx(key);

                        // Handle implicit locks for pessimistic transactions.
                        if (msg.inTx()) {
                            tx = ctx.tm().tx(msg.version());

                            if (tx == null) {
                                tx = new GridReplicatedTxRemote<>(
                                    nodeId,
                                    msg.threadId(),
                                    msg.version(),
                                    null,
                                    PESSIMISTIC,
                                    msg.isolation(),
                                    msg.isInvalidate(),
                                    msg.timeout(),
                                    ctx,
                                    msg.txSize(),
                                    msg.groupLockKey());

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !ctx.tm().onStarted(tx))
                                    throw new GridCacheTxRollbackException("Failed to acquire lock " +
                                        "(transaction has been completed): " + msg.version());
                            }

                            if (msg.txRead())
                                tx.addRead(key, msg.keyBytes() != null ? msg.keyBytes().get(i) : null,
                                    msg.drVersionByIndex(i));
                            else
                                tx.addWrite(
                                    key,
                                    msg.keyBytes() != null ? msg.keyBytes().get(i) : null,
                                    writeEntry == null ? GridCacheOperation.NOOP : writeEntry.op(),
                                    writeEntry == null ? null : writeEntry.value(),
                                    writeEntry == null ? null : writeEntry.valueBytes(),
                                    writeEntry.drVersion());
                        }

                        // Add remote candidate before reordering.
                        entry.addRemote(
                            msg.nodeId(),
                            null,
                            msg.threadId(),
                            msg.version(),
                            msg.timeout(),
                            tx != null,
                            tx != null && tx.implicitSingle(),
                            null
                        );

                        // Remote candidates for ordered lock queuing.
                        entry.addRemoteCandidates(
                            cands,
                            msg.version(),
                            msg.committedVersions(),
                            msg.rolledbackVersions());

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(msg.nodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + msg);

                            if (tx != null)
                                tx.rollback();

                            return;
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
                            tx.clearEntry(entry.key());

                            if (log.isDebugEnabled())
                                log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                                    entry + ", tx=" + tx + ']');
                        }
                    }
                }
            }

            Collection<GridCacheVersion> committedVers = ctx.tm().committedVersions(msg.version());
            Collection<GridCacheVersion> rolledBackVers = ctx.tm().rolledbackVersions(msg.version());

            // Add local candidates in reverse order.
            for (int i = keys.size() - 1; i >= 0; i--) {
                K key = msg.keys().get(i);

                GridDistributedCacheEntry<K, V> entry = entryexx(key);

                try {
                    res.setCandidates(
                        i,
                        entry.localCandidates(),
                        committedVers,
                        rolledBackVers);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    assert false : "Entry should not get obsolete when remote lock is added: " + entry;
                }
            }
        }
        catch (GridCacheTxRollbackException e) {
            if (log.isDebugEnabled())
                log.debug("Received lock request for completed transaction (will ignore): " + e);

            res = new GridDistributedLockResponse(msg.version(), msg.futureId(), e);
        }
        catch (GridDistributedLockCancelledException ignored) {
            // Received lock request for cancelled lock.
            if (log.isDebugEnabled())
                log.debug("Received lock request for canceled lock (will ignore): " + msg);

            if (tx != null)
                tx.rollback();

            // Don't send response back.
            return;
        }

        boolean releaseAll = false;

        if (res != null) {
            try {
                // Reply back to sender.
                ctx.io().send(nodeId, res);
            }
            catch (GridTopologyException ignored) {
                U.warn(log, "Failed to send lock reply to remote node because it left grid: " + nodeId);

                releaseAll = true;
            }
            catch (GridException e) {
                U.error(log, "Failed to send lock reply to node (lock will not be acquired): " + nodeId, e);
            }
        }

        // Release all locks because sender node left grid.
        if (releaseAll) {
            for (K key : msg.keys()) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekexx(key);

                    try {
                        if (entry != null)
                            entry.removeExplicitNodeLocks(msg.nodeId());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to remove lock on removed entity during failure " +
                                "of replicated lock request handling (will retry): " + entry);
                    }
                }
            }

            U.warn(log, "Sender node left grid in the midst of lock acquisition (locks will be released).");
        }
    }

    /**
     * @param lockVer Lock version.
     * @param futId Future ID.
     * @return Lock future.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private GridReplicatedLockFuture<K, V> futurex(GridCacheVersion lockVer, GridUuid futId) {
        return (GridReplicatedLockFuture)ctx.mvcc().future(lockVer, futId);
    }

    /**
     * Processes lock response.
     *
     * @param nodeId Sender node ID.
     * @param res Lock response.
     */
    private void processLockResponse(UUID nodeId, GridDistributedLockResponse<K, V> res) {
        GridReplicatedLockFuture<K, V> fut = futurex(res.version(), res.futureId());

        if (fut == null) {
            U.warn(log, "Received lock response for non-existing future (will ignore): " + res);
        }
        else {
            fut.onResult(nodeId, res);

            if (fut.isDone()) {
                ctx.mvcc().removeFuture(fut);

                if (log.isDebugEnabled())
                    log.debug("Received all replies for future (future was removed): " + fut);
            }
        }
    }

    /**
     * Processes unlock request.
     *
     * @param nodeId Sender node ID.
     * @param req Unlock request.
     */
    @SuppressWarnings({"unchecked"})
    private void processUnlockRequest(UUID nodeId, GridDistributedUnlockRequest<K, V> req) {
        assert nodeId != null;

        List<K> keys = req.keys();

        for (K key : keys) {
            while (true) {
                boolean created = false;

                GridDistributedCacheEntry<K, V> entry = peekexx(key);

                if (entry == null) {
                    entry = entryexx(key);

                    created = true;
                }

                try {
                    entry.doneRemote(
                        req.version(),
                        req.version(),
                        null,
                        req.committedVersions(),
                        req.rolledbackVersions(),
                        /*system invalidate*/false);

                    // Note that we don't reorder completed versions here,
                    // as there is no point to reorder relative to the version
                    // we are about to remove.
                    if (entry.removeLock(req.version())) {
                        if (log.isDebugEnabled())
                            log.debug("Removed lock [lockId=" + req.version() + ", key=" + key + ']');

                        if (created && entry.markObsolete(req.version()))
                            removeIfObsolete(entry.key());
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Received unlock request for unknown candidate " +
                            "(added to cancelled locks set): " + req);

                    ctx.evicts().touch(entry);

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Received remove lock request for removed entry (will retry) [entry=" + entry +
                            ", req=" + req + ']');
                }
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Prepare request.
     */
    private void processPrepareRequest(final UUID nodeId, final GridDistributedTxPrepareRequest<K, V> msg) {
        GridFuture<Object> fut = ctx.preloader().request(F.viewReadOnly(msg.writes(), CU.<K, V>tx2key()),
            ctx.discovery().topologyVersion());

        if (fut.isDone())
            processPrepareRequestPreloaderSafe(fut, nodeId, msg);
        else {
            fut.listenAsync(new CI1<GridFuture<Object>>() {
                @Override public void apply(GridFuture<Object> f) {
                    processPrepareRequestPreloaderSafe(f, nodeId, msg);
                }
            });
        }
    }

    /**
     * @param fut Preloader future, must be completed.
     * @param nodeId Sender node ID.
     * @param req Prepare request.
     */
    private void processPrepareRequestPreloaderSafe(GridFuture<?> fut, UUID nodeId,
        GridDistributedTxPrepareRequest<K, V> req) {
        try {
            assert fut.isDone();

            fut.get();

            processPrepareRequest0(nodeId, req);
        }
        catch (GridException e) {
            GridDistributedTxPrepareResponse<K, V> res = new GridDistributedTxPrepareResponse<>(req.version());

            res.error(e);

            try {
                // Reply back to sender.
                ctx.io().send(nodeId, res);
            }
            catch (GridException e1) {
                U.error(log, "Failed to send tx response to node (did the node leave grid?) [node=" + nodeId +
                    ", msg=" + res + ']', e1);
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Prepare request.
     */
    @SuppressWarnings({"InstanceofCatchParameter"})
    private void processPrepareRequest0(UUID nodeId, GridDistributedTxPrepareRequest<K, V> msg) {
        assert nodeId != null;
        assert msg != null;

        GridReplicatedTxRemote<K, V> tx = null;

        GridDistributedTxPrepareResponse<K, V> res;

        try {
            tx = new GridReplicatedTxRemote<>(
                ctx.deploy().globalLoader(),
                nodeId,
                msg.threadId(),
                msg.version(),
                msg.commitVersion(),
                msg.concurrency(),
                msg.isolation(),
                msg.isInvalidate(),
                msg.timeout(),
                msg.reads(),
                msg.writes(),
                ctx,
                msg.txSize(),
                msg.transactionNodes(),
                msg.groupLockKey());

            tx = ctx.tm().onCreated(tx);

            if (tx == null || !ctx.tm().onStarted(tx))
                throw new GridCacheTxRollbackException("Attempt to start a completed transaction: " + tx);

            // Prepare prior to reordering, so the pending locks added
            // in prepare phase will get properly ordered as well.
            tx.prepare();

            // Add remote candidates and reorder completed and uncompleted versions.
            tx.addRemoteCandidates(msg.candidatesByKey(), msg.committedVersions(), msg.rolledbackVersions());

            res = new GridDistributedTxPrepareResponse<>(msg.version());

            Map<K, Collection<GridCacheMvccCandidate<K>>> cands = tx.localCandidates();

            // Add local candidates (completed version must be set below).
            res.candidates(cands);
        }
        catch (GridException e) {
            if (e instanceof GridCacheTxRollbackException) {
                if (log.isDebugEnabled())
                    log.debug("Transaction was rolled back before prepare completed: " + tx);
            }
            else if (e instanceof GridCacheTxOptimisticException) {
                if (log.isDebugEnabled())
                    log.debug("Optimistic failure for remote transaction (will rollback): " + tx);
            }
            else {
                U.error(log, "Failed to process prepare request: " + msg, e);
            }

            if (tx != null)
                // Automatically rollback remote transactions.
                tx.rollback();

            res = new GridDistributedTxPrepareResponse<>(msg.version());

            res.error(e);
        }

        // Add completed versions.
        res.completedVersions(
            ctx.tm().committedVersions(msg.version()),
            ctx.tm().rolledbackVersions(msg.version()));

        try {
            // Reply back to sender.
            ctx.io().send(nodeId, res);
        }
        catch (GridException e) {
            U.error(log, "Failed to send tx response to node (did the node leave grid?) [node=" + nodeId +
                ", msg=" + res + ']', e);

            if (tx != null)
                tx.rollback();
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Response to prepare request.
     */
    private void processPrepareResponse(UUID nodeId, GridDistributedTxPrepareResponse<K, V> msg) {
        assert nodeId != null;
        assert msg != null;

        GridReplicatedTxLocal<K, V> tx = ctx.tm().tx(msg.version());

        if (tx == null) {
            if (log.isDebugEnabled())
                log.debug("Received prepare response for non-existing transaction [senderNodeId=" + nodeId +
                    ", res=" + msg + ']');

            return;
        }

        GridReplicatedTxPrepareFuture<K, V> fut = (GridReplicatedTxPrepareFuture<K, V>)tx.future();

        if (fut != null)
            fut.onResult(nodeId, msg);
        else
            U.error(log, "Received prepare response for transaction with no future [res=" + msg + ", tx=" + tx + ']');
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Finish transaction message.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void processFinishRequest(UUID nodeId, GridDistributedTxFinishRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        GridReplicatedTxRemote<K, V> tx = ctx.tm().tx(req.version());

        try {
            ClassLoader ldr = ctx.deploy().globalLoader();

            if (req.commit()) {
                // If lock was acquired explicitly or node joined after lock requests were sent.
                if (tx == null) {
                    // Create transaction and add entries.
                    tx = ctx.tm().onCreated(
                        new GridReplicatedTxRemote<>(
                            ldr,
                            nodeId,
                            req.threadId(),
                            req.version(),
                            req.commitVersion(),
                            PESSIMISTIC,
                            READ_COMMITTED,
                            req.isInvalidate(),
                            /*timeout */0,
                            /*read entries*/null,
                            req.writes(),
                            ctx,
                            req.txSize(),
                            null,
                            req.groupLockKey()));

                    if (tx == null || !ctx.tm().onStarted(tx))
                        throw new GridCacheTxRollbackException("Attempt to start a completed " +
                            "transaction: " + req);
                }
                else {
                    boolean set = tx.commitVersion(req.commitVersion());

                    assert set;
                }

                Collection<GridCacheTxEntry<K, V>> writeEntries = req.writes();

                if (!F.isEmpty(writeEntries)) {
                    // In OPTIMISTIC mode, we get the values at PREPARE stage.
                    assert tx.concurrency() == PESSIMISTIC;

                    for (GridCacheTxEntry<K, V> entry : writeEntries) {
                        // Unmarshal write entries.
                        entry.unmarshal(ctx, ldr);

                        if (log.isDebugEnabled())
                            log.debug("Unmarshalled transaction entry from pessimistic transaction [key=" +
                                entry.key() + ", value=" + entry.value() + ", tx=" + tx + ']');

                        if (!tx.setWriteValue(entry))
                            U.warn(log, "Received entry to commit that was not present in transaction [entry=" +
                                entry + ", tx=" + tx + ']');
                    }
                }

                // Add completed versions.
                tx.doneRemote(req.baseVersion(), req.committedVersions(), req.rolledbackVersions(),
                    Collections.<GridCacheVersion>emptyList());

                if (tx.pessimistic())
                    tx.prepare();

                tx.commit();
            }
            else if (tx != null) {
                tx.doneRemote(req.baseVersion(), req.committedVersions(), req.rolledbackVersions(),
                    Collections.<GridCacheVersion>emptyList());

                tx.rollback();
            }

            sendReply(nodeId, req);
        }
        catch (GridCacheTxRollbackException e) {
            if (log.isDebugEnabled())
                log.debug("Attempted to start a completed transaction (will ignore): " + e);

            sendReply(nodeId, req);
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + req.commit() + ", tx=" + CU.txString(tx) + ']', e);

            if (tx != null)
                tx.rollback();
        }
    }

    /**
     * Send finish reply to the source node, if required.
     *
     * @param nodeId Node id that originated finish request.
     * @param req Finish request itself.
     */
    private void sendReply(UUID nodeId, GridDistributedTxFinishRequest req) {
        if (req.replyRequired()) {
            GridCacheMessage<K, V> res = new GridDistributedTxFinishResponse<>(req.version(), req.futureId());

            try {
                ctx.io().send(nodeId, res);
            }
            catch (Throwable th) {
                // Double-check.
                if (ctx.discovery().node(nodeId) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Node left while sending finish response [nodeId=" + nodeId + ", res=" + res +
                            ']');
                }
                else
                    U.error(log, "Failed to send finish response to node [nodeId=" + nodeId + ", res=" + res + ']',
                        th);
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Finish transaction response.
     */
    private void processFinishResponse(UUID nodeId, GridDistributedTxFinishResponse<K, V> msg) {
        GridReplicatedTxCommitFuture<K, V> fut =
            (GridReplicatedTxCommitFuture<K, V>)ctx.mvcc().<GridCacheTx>future(msg.xid(), msg.futureId());

        if (fut != null)
            fut.onResult(nodeId);
        else
            U.warn(log, "Received finish response for unknown transaction: " + msg);
    }

    /**
     * @param key Cache key.
     * @return Replicated cache entry.
     */
    @Nullable GridDistributedCacheEntry<K, V> peekexx(K key) {
        return (GridDistributedCacheEntry<K, V>)peekEx(key);
    }

    /**
     * @param key Cache key.
     * @return Replicated cache entry.
     */
    GridDistributedCacheEntry<K, V> entryexx(K key) {
        return (GridDistributedCacheEntry<K, V>)entryEx(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "ThrowableInstanceNeverThrown"})
    @Override protected GridFuture<Boolean> lockAllAsync(final Collection<? extends K> keys, final long timeout,
        final GridCacheTxLocalEx<K, V> tx, final boolean isInvalidate, final boolean isRead, final boolean retval,
        final GridCacheTxIsolation isolation, final GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        if (keys.isEmpty())
            return new GridFinishedFuture<>(ctx.kernalContext(), true);

        GridFuture<Object> fut = preloader().request(keys,
            tx != null ? tx.topologyVersion() : ctx.discovery().topologyVersion());

        final long threadId = Thread.currentThread().getId();

        if (fut.isDone()) {
            try {
                fut.get();
            }
            catch (GridException e) {
                return new GridFinishedFuture<>(ctx.kernalContext(), e);
            }

            return lockAllAsync0(keys, timeout, threadId, tx, isInvalidate, isRead, retval, isolation, filter);
        }

        return new GridEmbeddedFuture<>(true, fut,
            new C2<Object, Exception, GridFuture<Boolean>>() {
                @Override public GridFuture<Boolean> apply(Object o, Exception err) {
                    if (err != null)
                        return new GridFinishedFuture<>(ctx.kernalContext(), err);

                    return lockAllAsync0(keys, timeout, threadId, tx, isInvalidate, isRead, retval, isolation, filter);
                }
            },
            ctx.kernalContext());
    }

    /**
     * @param keys Keys to lock.
     * @param timeout Timeout.
     * @param threadId Locking thread ID.
     * @param tx Transaction
     * @param isInvalidate Invalidation flag.
     * @param isRead Indicates whether value is read or written.
     * @param retval Flag to return value.
     * @param isolation Transaction isolation.
     * @param filter Optional filter.
     * @return Future for locks.
     */
    protected GridFuture<Boolean> lockAllAsync0(
        Collection<? extends K> keys,
        long timeout,
        long threadId,
        GridCacheTxLocalEx<K, V> tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        GridCacheTxIsolation isolation,
        GridPredicate<? super GridCacheEntry<K, V>>[] filter
    ) {
        Collection<GridNode> nodes = ctx.affinity().remoteNodes(keys);

        final GridReplicatedLockFuture<K, V> fut = new GridReplicatedLockFuture<>(ctx, keys, threadId, tx, this, nodes,
            timeout, filter);

        GridDistributedLockRequest<K, V> req = new GridDistributedLockRequest<>(
            locNodeId,
            tx != null ? tx.xidVersion() : null,
            threadId,
            fut.futureId(),
            fut.version(),
            tx != null,
            isRead,
            isolation,
            isInvalidate,
            timeout,
            keys.size(),
            tx == null ? keys.size() : tx.size(),
            tx == null ? null : tx.groupLockKey(),
            tx != null && tx.partitionLock()
        );

        try {
            // Set topology version if it was not set.
            if (tx != null)
                tx.topologyVersion(ctx.discovery().topologyVersion());

            // Must add future before redying locks.
            if (!ctx.mvcc().addFuture(fut))
                throw new IllegalStateException("Duplicate future ID: " + fut);

            boolean distribute = false;

            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = null;

                    try {
                        entry = entryexx(key);

                        if (!ctx.isAll(entry.wrap(false), filter)) {
                            if (log.isDebugEnabled())
                                log.debug("Entry being locked did not pass filter (will not lock): " + entry);

                            fut.onDone(false);

                            return fut;
                        }

                        // Removed exception may be thrown here.
                        GridCacheMvccCandidate<K> cand = fut.addEntry(entry);

                        if (cand != null) {
                            GridCacheTxEntry<K, V> writeEntry = tx != null ? tx.writeMap().get(key) : null;

                            req.addKeyBytes(
                                key,
                                cand.reentry() ? null : entry.getOrMarshalKeyBytes(),
                                writeEntry,
                                retval,
                                entry.localCandidates(fut.version()),
                                null, // DR is disabled for REPLICATED cache.
                                ctx);

                            req.completedVersions(
                                ctx.tm().committedVersions(fut.version()),
                                ctx.tm().rolledbackVersions(fut.version()));

                            distribute = !cand.reentry();

                            // Clear transfer required flag if message will be sent over.
                            if (distribute && writeEntry != null)
                                writeEntry.transferRequired(false);
                        }
                        else if (fut.isDone())
                            return fut;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in lockAsync(..) method (will retry): " + entry);
                    }
                }
            }

            // If nothing to distribute at this point,
            // then all locks are reentries.
            if (!distribute)
                fut.complete(true);

            if (nodes.isEmpty())
                fut.readyLocks();

            // No reason to send request if all locks are locally re-entered,
            // or if timeout is negative and local locks could not be acquired.
            if (fut.isDone())
                return fut;

            try {
                ctx.io().safeSend(
                    fut.nodes(),
                    req,
                    new P1<GridNode>() {
                        @Override public boolean apply(GridNode node) {
                            fut.onNodeLeft(node.id());

                            return !fut.isDone();
                        }
                    }
                );
            }
            catch (GridException e) {
                U.error(log, "Failed to send lock request to node [nodes=" + U.toShortString(nodes) +
                    ", req=" + req + ']', e);

                fut.onError(e);
            }

            return fut;
        }
        catch (GridException e) {
            Throwable err = new GridException("Failed to acquire asynchronous lock for keys: " + keys, e);

            // Clean-up.
            fut.onError(err);

            ctx.mvcc().removeFuture(fut);

            return fut;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void unlockAll(Collection<? extends K> keys,
        GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        if (keys == null || keys.isEmpty())
            return;

        Collection<? extends GridNode> nodes = ctx.affinity().remoteNodes(keys);

        try {
            GridDistributedUnlockRequest<K, V> req = new GridDistributedUnlockRequest<>(keys.size());

            for (K key : keys) {
                GridDistributedCacheEntry<K, V> entry = entryexx(key);

                if (!ctx.isAll(entry.wrap(false), filter))
                    continue;

                // Unlock local lock first.
                GridCacheMvccCandidate<K> rmv = entry.removeLock();

                if (rmv != null && !nodes.isEmpty()) {
                    if (!rmv.reentry()) {
                        req.addKey(entry.key(), entry.getOrMarshalKeyBytes(), ctx);

                        // We are assuming that lock ID is the same for all keys.
                        req.version(rmv.version());

                        if (log.isDebugEnabled())
                            log.debug("Removed lock (will distribute): " + rmv);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Locally unlocked lock reentry without distributing to other nodes [removed=" +
                                rmv + ", entry=" + entry + ']');
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Current thread still owns lock (or there are no other nodes) [lock=" + rmv +
                            ", curThreadId=" + Thread.currentThread().getId() + ']');
                }

                ctx.evicts().touch(entry);
            }

            // Don't proceed of no keys to unlock.
            if (F.isEmpty(req.keyBytes()) && F.isEmpty(req.keys())) {
                if (log.isDebugEnabled())
                    log.debug("No keys to unlock locally (was it reentry unlock?): " + keys);

                return;
            }

            // We don't wait for reply to this message. Receiving side will have
            // to make sure that unlock requests don't come before lock requests.
            ctx.io().safeSend(nodes, req, null);
        }
        catch (GridException e) {
            U.error(log, "Failed to unlock keys: " + keys, e);
        }
    }

    /**
     * Removes locks regardless of whether they are owned or not for given
     * version and keys.
     *
     * @param ver Lock version.
     * @param keys Keys.
     */
    @SuppressWarnings({"unchecked"})
    public void removeLocks(GridCacheVersion ver, Collection<? extends K> keys) {
        if (keys.isEmpty())
            return;

        Collection<GridNode> nodes = ctx.affinity().remoteNodes(keys);

        try {
            // Send request to remove from remote nodes.
            GridDistributedUnlockRequest<K, V> req = new GridDistributedUnlockRequest<>(keys.size());

            req.version(ver);

            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekexx(key);

                    try {
                        if (entry != null) {
                            GridCacheMvccCandidate<K> cand = entry.candidate(ver);

                            if (cand != null) {
                                // Remove candidate from local node first.
                                if (entry.removeLock(cand.version())) {
                                    // If there is only local node in this lock's topology,
                                    // then there is no reason to distribute the request.
                                    if (nodes.isEmpty())
                                        continue;

                                    req.addKey(entry.key(),
                                        entry.getOrMarshalKeyBytes(),
                                        ctx);
                                }
                            }
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to remove lock from removed entry (will retry) [rmvVer=" +
                                ver + ", entry=" + entry + ']');
                    }
                }
            }

            if (nodes.isEmpty())
                return;

            req.completedVersions(
                ctx.tm().committedVersions(ver),
                ctx.tm().rolledbackVersions(ver));

            if (!F.isEmpty(req.keyBytes()) || !F.isEmpty(req.keys()))
                // We don't wait for reply to this message.
                ctx.io().safeSend(nodes, req, null);
        }
        catch (GridException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedCache.class, this, "super", super.toString());
    }
}
