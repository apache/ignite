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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Replicated user transaction.
 *
 * @author @java.author
 * @version @java.version
 */
class GridReplicatedTxLocal<K, V> extends GridCacheTxLocalAdapter<K, V> {
    /** All keys participating in transaction. */
    private Set<K> allKeys;

    /** Future. */
    private final AtomicReference<GridReplicatedTxPrepareFuture<K, V>> prepareFut =
        new AtomicReference<>();

    /** Future. */
    private final AtomicReference<GridReplicatedTxCommitFuture<K, V>> commitFut =
        new AtomicReference<>();

    /** Future. */
    private final AtomicReference<GridReplicatedTxCommitFuture<K, V>> rollbackFut =
        new AtomicReference<>();

    /** */
    private boolean syncCommit;

    /** */
    private boolean syncRollback;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridReplicatedTxLocal() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit with one key flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param invalidate Invalidation policy.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param swapEnabled Whether to use swap storage.
     * @param storeEnabled Whether to use read/write through.
     * @param txSize Expected number of entries in transaction.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction and whole partition should be locked.
     */
    GridReplicatedTxLocal(
        GridCacheContext<K, V> ctx,
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
        super(ctx, ctx.versions().next(), implicit, implicitSingle, concurrency, isolation, timeout, invalidate,
            swapEnabled, storeEnabled, txSize, grpLockKey, partLock);

        assert ctx != null;

        this.syncCommit = syncCommit;
        this.syncRollback = syncRollback;
    }

    /** {@inheritDoc} */
    @Override public boolean syncCommit() {
        return syncCommit;
    }

    /** {@inheritDoc} */
    @Override public boolean syncRollback() {
        return syncRollback;
    }

    /** {@inheritDoc} */
    @Override public boolean needsCompletedVersions() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        GridReplicatedTxCommitFuture<K, V> fut = commitFut.get();

        return fut != null && fut.onOwnerChanged(entry, owner);
    }

    /**
     * @return Prepare fut.
     */
    @Override public GridFuture<GridCacheTxEx<K, V>> future() {
        return prepareFut.get();
    }

    /** {@inheritDoc} */
    @Override public boolean replicated() {
        return true;
    }

    /**
     *
     */
    private void initializeKeys() {
        if (allKeys == null) {
            Collection<K> readSet = readSet();
            Collection<K> writeSet = writeSet();

            Set<K> allKeys = new HashSet<>(readSet.size() + writeSet.size(), 1.0f);

            allKeys.addAll(readSet);
            allKeys.addAll(writeSet);

            this.allKeys = allKeys;
        }
    }

    /**
     * @return Node group for transaction.
     */
    private Collection<GridNode> resolveNodes() {
        initializeKeys();

        if (allKeys.isEmpty())
            return Collections.emptyList();

        // Do not include local node for transaction processing, as
        // it is included by doing local transaction commit/rollback
        // operations already.
        return cctx.affinity().remoteNodes(allKeys);
    }

    /** {@inheritDoc} */
    @Override public boolean finish(boolean commit) throws GridException {
        initializeKeys();

        GridReplicatedTxPrepareFuture<K, V> prep = prepareFut.get();

        GridReplicatedTxCommitFuture<K, V> fin = commit ? commitFut.get() : rollbackFut.get();

        assert fin != null;

        Collection<? extends GridNode> nodes =
            commit ?
                prep == null ? Collections.<GridNode>emptyList() : prep.nodes() :
                fin.txNodes();

        GridException err = null;

        try {
            // Commit to DB first. This way if there is a failure, transaction
            // won't be committed.
            try {
                if (commit && !isRollbackOnly())
                    userCommit();
                else
                    userRollback();
            }
            catch (GridException e) {
                err = e;

                commit = false;

                // If heuristic error.
                if (!isRollbackOnly()) {
                    invalidate = true;

                    U.warn(log, "Set transaction invalidation flag to true due to error [tx=" + this +
                        ", err=" + err + ']');
                }
            }

            if (!allKeys.isEmpty() && !nodes.isEmpty()) {
                assert !fin.trackable() || cctx.mvcc().hasFuture(fin);

                // We write during commit only for pessimistic transactions.
                Collection<GridCacheTxEntry<K, V>> writeEntries = pessimistic() ? writeEntries() : null;

                boolean reply = (syncCommit && commit) || (syncRollback && !commit);

                GridCacheMessage<K, V> req = new GridDistributedTxFinishRequest<>(
                    xidVersion(),
                    fin.futureId(),
                    commitVersion(),
                    threadId,
                    commit,
                    isInvalidate(),
                    completedBase(),
                    committedVersions(),
                    rolledbackVersions(),
                    size(),
                    commit ? writeEntries : null,
                    commit && pessimistic() ? F.view(writeEntries(), CU.<K, V>transferRequired()) : null,
                    reply,
                    grpLockKey);

                try {
                    cctx.io().safeSend(nodes, req, null);
                }
                catch (Throwable e) {
                    String msg = "Failed to send finish request to nodes [node=" + U.toShortString(nodes) +
                        ", req=" + req + ']';

                    U.error(log, msg, e);

                    if (err == null)
                        err = new GridException("Failed to finish transaction " +
                            "(it may remain on some nodes until cleaned by timeout): " + this, e);
                }
            }
            else if (log.isDebugEnabled()) {
                if (allKeys.isEmpty())
                    log.debug("Transaction has no keys to persist: " + this);
                else {
                    assert nodes.isEmpty();

                    log.debug("Transaction has no remote nodes to send commit request to: " + this);
                }
            }
        }
        catch (Throwable e) {
            if (err == null)
                err = new GridException("Failed to finish transaction: " + this, e);
            else
                U.error(log, "Failed to finish transaction: " + this, e);
        }

        if (err != null) {
            state(UNKNOWN);

            if (prep != null)
                prep.onError(err);

            fin.onError(err);

            throw err;
        }
        else {
            if (!state(commit ? COMMITTED : ROLLED_BACK)) {
                state(UNKNOWN);

                if (prep != null)
                    prep.onError(new GridException("Invalid transaction state for commit or rollback: " + this));
            }

            fin.onTxFinished();
        }

        return true;
    }

    /**
     *
     * @param req Request.
     * @param txEntries Transaction entries.
     */
    void candidatesByKey(GridDistributedBaseMessage<K, V> req, Map<K, GridCacheTxEntry<K, V>> txEntries) {
        if (txEntries != null) {
            for (GridCacheTxEntry<K, V> txEntry : txEntries.values()) {
                while (true) {
                    try {
                        GridCacheMapEntry<K, V> entry = (GridCacheMapEntry<K, V>)txEntry.cached();

                        req.candidatesByKey(entry.key(), entry.localCandidates(/*exclude version*/xidVer));

                        break;
                    }
                    // Possible if entry cached within transaction is obsolete.
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry while setting local candidates for entry (will retry) [entry=" +
                                txEntry.cached() + ", tx=" + this + ']');

                        txEntry.cached(cctx.cache().entryEx(txEntry.key()), txEntry.keyBytes());
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTxEx<K, V>> prepareAsync() {
        GridReplicatedTxPrepareFuture<K, V> fut = prepareFut.get();

        if (fut == null) {
            Collection<GridNode> nodeGrp = resolveNodes();

            // Set topology version if it was not set.
            topologyVersion(cctx.discovery().topologyVersion());

            // Future must be created before any exception can be thrown.
            if (!prepareFut.compareAndSet(null, fut = new GridReplicatedTxPrepareFuture<>(cctx, this, nodeGrp)))
                return prepareFut.get();
        }
        else
            return fut;

        if (!state(PREPARING)) {
            if (setRollbackOnly()) {
                if (timedOut())
                    fut.onError(new GridCacheTxTimeoutException("Transaction timed out and was rolled back: " + this));
                else
                    fut.onError(new GridException("Invalid transaction state for prepare [state=" + state() +
                        ", tx=" + this + ']'));
            }
            else
                fut.onError(new GridCacheTxRollbackException("Invalid transaction state for prepare [state=" + state()
                    + ", tx=" + this + ']'));

            return fut;
        }

        // For pessimistic mode we don't distribute prepare request.
        if (pessimistic()) {
            try {
                userPrepare();

                if (!state(PREPARED)) {
                    setRollbackOnly();

                    fut.onError(new GridException("Invalid transaction state for commit [state=" + state() +
                        ", tx=" + this + ']'));

                    return fut;
                }

                fut.complete();

                return fut;
            }
            catch (GridException e) {
                fut.onError(e);

                return fut;
            }
        }

        GridFuture<Object> preloadFut = cctx.preloader().request(
            F.viewReadOnly(F.concat(false, readEntries(), writeEntries()), CU.<K, V>tx2key()), topologyVersion());

        if (preloadFut.isDone()) {
            try {
                preloadFut.get();

                prepareAsync0(fut);
            }
            catch (GridException e) {
                fut.onError(e);
            }

            return fut;
        }

        final GridReplicatedTxPrepareFuture<K, V> fut0 = fut;

        return new GridEmbeddedFuture<>(true, preloadFut,
            new C2<Object, Exception, GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public GridFuture<GridCacheTxEx<K, V>> apply(Object o, Exception err) {
                    if (err != null)
                        fut0.onError(err);
                    else
                        prepareAsync0(fut0);

                    return fut0;
                }
            }, cctx.kernalContext());
    }

    /**
     * @param fut Prepare future.
     */
    private void prepareAsync0(GridReplicatedTxPrepareFuture<K, V> fut) {
        // OPTIMISTIC locking.
        if (allKeys.isEmpty()) {
            // Move transition to committed state.
            if (state(PREPARED))
                fut.complete();
            else if (state(ROLLING_BACK)) {
                if (doneFlag.compareAndSet(false, true)) {
                    cctx.tm().rollbackTx(this);

                    state(ROLLED_BACK);

                    fut.onError(new GridCacheTxRollbackException("Transaction was rolled back: " + this));

                    if (log.isDebugEnabled())
                        log.debug("Rolled back empty transaction: " + this);
                }
            }

            fut.complete();

            return;
        }

        try {
            userPrepare();

            if (fut.nodes().isEmpty())
                fut.onAllReplies();
            else {
                GridDistributedBaseMessage<K, V> req = new GridDistributedTxPrepareRequest<>(
                    this,
                    optimistic() && serializable() ? readEntries() : null,
                    writeEntries(),
                    groupLockKey(),
                    partitionLock(),
                    fut.nodesMapping());

                // Set local candidates.
                candidatesByKey(req, writeMap());

                // Completed versions.
                req.completedVersions(cctx.tm().committedVersions(minVer), cctx.tm().rolledbackVersions(minVer));

                try {
                    cctx.mvcc().addFuture(fut);

                    cctx.io().safeSend(fut.nodes(), req, new GridPredicate<GridNode>() {
                        @Override public boolean apply(GridNode n) {
                            GridReplicatedTxPrepareFuture<K, V> fut = prepareFut.get();

                            fut.onNodeLeft(n.id());

                            return !fut.isDone();
                        }
                    });
                }
                catch (GridException e) {
                    String msg = "Failed to send prepare request to nodes [req=" + req + ", nodes=" +
                        U.toShortString(fut.nodes()) + ']';

                    U.error(log, msg, e);

                    fut.onError(new GridCacheTxRollbackException(msg, e));
                }
            }
        }
        catch (GridCacheTxTimeoutException | GridCacheTxOptimisticException e) {
            fut.onError(e);
        }
        catch (GridException e) {
            setRollbackOnly();

            try {
                rollback();
            }
            catch (GridException e1) {
                U.error(log(), "Failed to rollback transaction: " + this, e1);
            }

            fut.onError(new GridCacheTxRollbackException("Failed to prepare transaction: " + this, e));
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public GridFuture<GridCacheTx> commitAsync() {
        prepareAsync();

        GridReplicatedTxPrepareFuture<K, V> prep = prepareFut.get();

        assert prep != null;

        GridReplicatedTxCommitFuture<K, V> fin = commitFut.get();

        if (fin == null)
            // Future must be created before any exception can be thrown.
            if (!commitFut.compareAndSet(null, fin = new GridReplicatedTxCommitFuture<>(cctx, this, prep.nodes())))
                return commitFut.get();

        assert allKeys != null;

        // OPTIMISTIC locking.
        if (allKeys.isEmpty()) {
            // Move transition to committed state.
            if (state(COMMITTING)) {
                if (doneFlag.compareAndSet(false, true)) {
                    cctx.tm().commitTx(this);

                    state(COMMITTED);

                    if (log.isDebugEnabled())
                        log.debug("Committed empty transaction: " + this);
                }
            }
            else if (state(ROLLING_BACK)) {
                if (doneFlag.compareAndSet(false, true)) {
                    cctx.tm().rollbackTx(this);

                    state(ROLLED_BACK);

                    if (log.isDebugEnabled())
                        log.debug("Rolled back empty transaction: " + this);
                }
            }

            fin.complete();

            return fin;
        }

        final GridReplicatedTxCommitFuture<K, V> fut = fin;

        prep.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
            @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                try {
                    f.get();
                }
                catch (GridException e) {
                    commitErr.compareAndSet(null, e);

                    // Failed during prepare, can't commit.
                    fut.onError(e);

                    return;
                }

                if (!state(COMMITTING)) {
                    GridCacheTxState state = state();

                    if (state != COMMITTING && state != COMMITTED) {
                        fut.onError(new GridException("Invalid transaction state for commit [state=" + state() +
                            ", tx=" + this + ']', commitErr.get()));

                        return;
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Invalid transaction state for commit (another thread is committing): " + this);

                        return;
                    }
                }

                cctx.mvcc().addFuture(fut);

                fut.init();
            }
        });

        return fin;
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws GridException {
        rollbackAsync().get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTx> rollbackAsync() {
        setRollbackOnly();

        GridReplicatedTxCommitFuture<K, V> fin = rollbackFut.get();

        GridReplicatedTxPrepareFuture<K, V> prep = prepareFut.get();

        Collection<? extends GridNode> nodes =
            optimistic() ?
                prep == null ? Collections.<GridNode>emptyList() : prep.nodes() :
                resolveNodes();

        if (fin == null) {
            // Future must be created before any exception can be thrown.
            if (!rollbackFut.compareAndSet(null, fin = new GridReplicatedTxCommitFuture<>(cctx, this, nodes)))
                return rollbackFut.get();
        }

        // If state is UNKNOWN, do best effort to rollback.
        if (!state(ROLLING_BACK) && state() != UNKNOWN) {
            if (log.isDebugEnabled())
                log.debug("Invalid transaction state for rollback [state=" + state() + ", tx=" + this + ']');

            return new GridFinishedFuture<>(cctx.kernalContext(),
                new GridException("Invalid transaction state for rollback [state=" + state() + ", tx=" +
                    CU.txString(this) + ']'));
        }

        assert fin != null;

        cctx.mvcc().addFuture(fin);

        fin.init();

        if (syncRollback) {
            if (F.isEmpty(fin.nodes()))
                fin.complete();
        }
        else
            fin.complete();

        return fin;
    }

    /** {@inheritDoc} */
    @Override public void addLocalCandidates(K key, Collection<GridCacheMvccCandidate<K>> cands) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public Map<K, Collection<GridCacheMvccCandidate<K>>> localCandidates() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridReplicatedTxLocal.class, this, "super", super.toString());
    }
}
