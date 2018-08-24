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

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.InvalidEnvironmentException;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryOperationFuture;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFilterFailedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxCommitEntriesFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxCommitFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.RELOAD;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_BACKUP;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public abstract class GridDistributedTxRemoteAdapter extends IgniteTxAdapter
    implements IgniteTxRemoteEx {
    /** */
    private static final long serialVersionUID = 0L;

    /** Commit allowed field updater. */
    private static final AtomicIntegerFieldUpdater<GridDistributedTxRemoteAdapter> COMMIT_ALLOWED_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDistributedTxRemoteAdapter.class, "commitAllowed");

    /** Explicit versions. */
    @GridToStringInclude
    private List<GridCacheVersion> explicitVers;

    /** Started flag. */
    @GridToStringInclude
    private boolean started;

    /** {@code True} only if all write entries are locked by this transaction. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringInclude
    private volatile int commitAllowed;

    /** */
    @GridToStringInclude
    protected IgniteTxRemoteState txState;

    /** {@code True} if tx should skip adding itself to completed version map on finish. */
    private boolean skipCompletedVers;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDistributedTxRemoteAdapter() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     * @param nodeId Node ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param plc IO policy.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridDistributedTxRemoteAdapter(
        GridCacheSharedContext<?, ?> ctx,
        UUID nodeId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(
            ctx,
            nodeId,
            xidVer,
            ctx.versions().last(),
            Thread.currentThread().getId(),
            sys,
            plc,
            concurrency,
            isolation,
            timeout,
            txSize,
            subjId,
            taskNameHash);

        this.invalidate = invalidate;

        commitVersion(commitVer);

        // Must set started flag after concurrency and isolation.
        started = true;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public UUID originatingNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean activeCachesDeploymentEnabled() {
        return false;
    }

    /**
     * @return Checks if transaction has no entries.
     */
    @Override public boolean empty() {
        return txState.empty();
    }

    /** {@inheritDoc} */
    @Override public void invalidate(boolean invalidate) {
        this.invalidate = invalidate;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return txState.writeMap();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return txState.readMap();
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridTuple<CacheObject> peek(GridCacheContext cacheCtx,
        boolean failFast,
        KeyCacheObject key)
        throws GridCacheFilterFailedException {
        assert false : "Method peek can only be called on user transaction: " + this;

        throw new IllegalStateException("Method peek can only be called on user transaction: " + this);
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        return txState.entry(key);
    }

    /**
     * Clears entry from transaction as it never happened.
     *
     * @param key key to be removed.
     */
    public void clearEntry(IgniteTxKey key) {
        txState.clearEntry(key);
    }

    /**
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    @Override public void doneRemote(GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers) {
        Map<IgniteTxKey, IgniteTxEntry> readMap = txState.readMap();

        if (readMap != null && !readMap.isEmpty()) {
            for (IgniteTxEntry txEntry : readMap.values())
                doneRemote(txEntry, baseVer, committedVers, rolledbackVers, pendingVers);
        }

        Map<IgniteTxKey, IgniteTxEntry> writeMap = txState.writeMap();

        if (writeMap != null && !writeMap.isEmpty()) {
            for (IgniteTxEntry txEntry : writeMap.values())
                doneRemote(txEntry, baseVer, committedVers, rolledbackVers, pendingVers);
        }
    }

    /** {@inheritDoc} */
    @Override public void setPartitionUpdateCounters(long[] cntrs) {
        if (writeMap() != null && !writeMap().isEmpty() && cntrs != null && cntrs.length > 0) {
            int i = 0;

            for (IgniteTxEntry txEntry : writeMap().values()) {
                txEntry.updateCounter(cntrs[i]);

                ++i;
            }
        }
    }

    /**
     * Adds completed versions to an entry.
     *
     * @param txEntry Entry.
     * @param baseVer Base version for completed versions.
     * @param committedVers Completed versions relative to base version.
     * @param rolledbackVers Rolled back versions relative to base version.
     * @param pendingVers Pending versions.
     */
    private void doneRemote(IgniteTxEntry txEntry,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers) {
        while (true) {
            GridDistributedCacheEntry entry = (GridDistributedCacheEntry)txEntry.cached();

            try {
                // Handle explicit locks.
                GridCacheVersion doneVer = txEntry.explicitVersion() != null ? txEntry.explicitVersion() : xidVer;

                entry.doneRemote(doneVer, baseVer, pendingVers, committedVers, rolledbackVers, isSystemInvalidate());

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                assert entry.obsoleteVersion() != null;

                if (log.isDebugEnabled())
                    log.debug("Replacing obsolete entry in remote transaction [entry=" + entry + ", tx=" + this + ']');

                // Replace the entry.
                txEntry.cached(txEntry.context().cache().entryEx(txEntry.key(), topologyVersion()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (!hasWriteKey(entry.txKey()))
            return false;

        IgniteTxCommitEntriesFuture commitFut = startCommitEntries();

        commitFut.listen(f -> cctx.tm().finisher().execute(this, () -> {
            try {
                finishCommitEntries(commitFut);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to commit remote transaction: " + this, e);

                invalidate(true);
                systemInvalidate(true);

                rollbackRemoteTx();
            }
        }));

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return started;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return txState.hasWriteKey(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return txState.readSet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        return txState.writeSet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return txState.allEntries();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return txState.writeEntries();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return txState.readEntries();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public final void prepareRemoteTx() throws IgniteCheckedException {
        // If another thread is doing prepare or rollback.
        if (!state(PREPARING)) {
            // In optimistic mode prepare may be called multiple times.
            if (state() != PREPARING || !optimistic()) {
                if (log.isDebugEnabled())
                    log.debug("Invalid transaction state for prepare: " + this);

                return;
            }
        }

        try {
            cctx.tm().prepareTx(this, null);

            if (pessimistic() || isSystemInvalidate())
                state(PREPARED);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            throw e;
        }
    }

    /**
     * @throws IgniteCheckedException If commit failed.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private IgniteTxCommitEntriesFuture startCommitEntries() {
        if (state() != COMMITTING) {
//            U.dumpStack(log, "Start commit has end due to not COMMITING state -> " + this);

            return IgniteTxCommitEntriesFuture.FINISHED;
        }

        if (checkLocks()) {
//            log.warning("Start commit has end due to locks -> " + this);

            return IgniteTxCommitEntriesFuture.FINISHED;
        }

        Map<IgniteTxKey, IgniteTxEntry> writeMap = txState.writeMap();

        if (!COMMIT_ALLOWED_UPD.compareAndSet(this, 0, 1)) {
//            log.warning("Start commit has end due to twice entered -> " + this);

            return IgniteTxCommitEntriesFuture.FINISHED;
        }

        if (F.isEmpty(writeMap)) {
//            log.warning("Write map is empty -> " + this);

            return IgniteTxCommitEntriesFuture.EMPTY;
        }

        GridCacheReturnCompletableWrapper wrapper = null;

        GridCacheReturn ret = null;

        if (!near() && !local() && onePhaseCommit()) {
            if (needReturnValue()) {
                ret = new GridCacheReturn(null, cctx.localNodeId().equals(otherNodeId()), true, null, true);

                UUID origNodeId = otherNodeId(); // Originating node.

                cctx.tm().addCommittedTxReturn(this,
                    wrapper = new GridCacheReturnCompletableWrapper(
                        !cctx.localNodeId().equals(origNodeId) ? origNodeId : null));
            }
            else
                cctx.tm().addCommittedTx(this, this.nearXidVersion(), null);
        }

        // Register this transaction as completed prior to write-phase to
        // ensure proper lock ordering for removed entries.
        cctx.tm().addCommittedTx(this);

        AffinityTopologyVersion topVer = topologyVersion();

        try {
            IgniteTxCommitEntriesFuture commitEntriesFut = new IgniteTxCommitEntriesFuture();

            Collection<IgniteTxEntry> entries = commitEntries();

            try {
                batchStoreCommit(writeMap().values());

                // Node that for near transactions we grab all entries.
                for (IgniteTxEntry txEntry : entries) {
                    // Prepare context for transaction entry.
                    TransactionEntryContext txContext = prepareContext(txEntry, ret);

                    // Nothing to perform.
                    if (txContext == null) {
                        commitEntriesFut.add(new GridFinishedFuture<>());

                        continue;
                    }

                    CacheEntryOperationFuture<GridCacheUpdateTxResult> operationFut;

                    if (txContext.operation == CREATE || txContext.operation == UPDATE) {
                        operationFut = cctx.cache().executor().execute(txContext.entry, e -> {
                                // Invalidate only for near nodes (backups cannot be invalidated).
                                if (isSystemInvalidate() || (isInvalidate() && txContext.cacheContext.isNear()))
                                    return e.innerRemove(this,
                                        eventNodeId(),
                                        nodeId,
                                        false,
                                        true,
                                        true,
                                        txEntry.keepBinary(),
                                        txEntry.hasOldValue(),
                                        txEntry.oldValue(),
                                        topVer,
                                        null,
                                        txContext.cacheContext.isDrEnabled() ? DR_BACKUP : DR_NONE,
                                        near() ? null : txContext.explicitVer,
                                        CU.subjectId(this, cctx),
                                        resolveTaskName(),
                                        txContext.dhtVer,
                                        null);
                                else {
                                    assert txContext.value != null : txEntry;

                                    return e.innerSet(this,
                                        eventNodeId(),
                                        nodeId,
                                        txContext.value,
                                        false,
                                        false,
                                        txEntry.ttl(),
                                        true,
                                        true,
                                        txEntry.keepBinary(),
                                        txEntry.hasOldValue(),
                                        txEntry.oldValue(),
                                        topVer,
                                        null,
                                        txContext.cacheContext.isDrEnabled() ? DR_BACKUP : DR_NONE,
                                        txEntry.conflictExpireTime(),
                                        near() ? null : txContext.explicitVer,
                                        CU.subjectId(this, cctx),
                                        resolveTaskName(),
                                        txContext.dhtVer,
                                        null);
                                }
                            },
                            e -> refresh(txEntry),
                            (e, result) -> {
                                if (txContext.nearEntry == null)
                                    return;

                                // Keep near entry up to date.
                                try {
                                    CacheObject valBytes = e.valueBytes();

                                    txContext.nearEntry.updateOrEvict(xidVer,
                                        valBytes,
                                        e.expireTime(),
                                        e.ttl(),
                                        nodeId,
                                        topVer);
                                } catch (GridCacheEntryRemovedException re) {
                                    // This is impossible in TPP way, but anyway throw exception.
                                    throw new IgniteCheckedException("Failed to get value bytes for entry: " + e, re);
                                }
                            });
                    } else if (txContext.operation == DELETE) {
                        operationFut = cctx.cache().executor().execute(txContext.entry, e -> {
                                return e.innerRemove(this,
                                    eventNodeId(),
                                    nodeId,
                                    false,
                                    true,
                                    true,
                                    txEntry.keepBinary(),
                                    txEntry.hasOldValue(),
                                    txEntry.oldValue(),
                                    topVer,
                                    null,
                                    txContext.cacheContext.isDrEnabled() ? DR_BACKUP : DR_NONE,
                                    near() ? null : txContext.explicitVer,
                                    CU.subjectId(this, cctx),
                                    resolveTaskName(),
                                    txContext.dhtVer,
                                    txEntry.updateCounter()
                                );
                            },
                            e -> refresh(txEntry),
                            (e, result) -> {
                                // Keep near entry up to date.
                                if (txContext.nearEntry != null)
                                    txContext.nearEntry.updateOrEvict(xidVer, null, 0, 0, nodeId, topVer);
                            });
                    } else if (txContext.operation == RELOAD) {
                        operationFut = cctx.cache().executor().execute(txContext.entry, e -> {
                                CacheObject val = e.innerReload();

                                return new GridCacheUpdateTxResult(true, val, null);
                            },
                            e -> refresh(txEntry),
                            (e, result) -> {
                                if (txContext.nearEntry == null)
                                    return;

                                // Keep near entry up to date.
                                GridNearCacheEntry nearEntry = txContext.nearEntry;

                                for (; ; ) {
                                    try {
                                        CacheObject reloaded = nearEntry.innerReload();

                                        nearEntry.updateOrEvict(e.version(),
                                            reloaded,
                                            e.expireTime(),
                                            e.ttl(),
                                            nodeId,
                                            topVer);

                                        break;
                                    } catch (GridCacheEntryRemovedException re) {
                                        nearEntry = txContext.cacheContext.dht().near().peekExx(txEntry.key());
                                    }
                                }
                            }
                        );
                    } else if (txContext.operation == READ) {
                        assert near();

                        operationFut = new CacheEntryOperationFuture<>();

                        operationFut.onDone(new GridCacheUpdateTxResult(false, null, null));

                        if (log.isDebugEnabled())
                            log.debug("Ignoring READ entry when committing: " + txEntry);
                    }
                    // No-op.
                    else {
                        operationFut = cctx.cache().executor().execute(txContext.entry, e -> {
                                if (txContext.conflictContext == null || !txContext.conflictContext.isUseOld()) {
                                    if (txEntry.ttl() != CU.TTL_NOT_CHANGED)
                                        e.updateTtl(null, txEntry.ttl());
                                }

                                return new GridCacheUpdateTxResult(true, null, null);
                            },
                            e -> refresh(txEntry),
                            (e, result) -> {
                                if (txContext.nearEntry == null)
                                    return;

                                try {
                                    CacheObject valBytes = e.valueBytes();

                                    txContext.nearEntry.updateOrEvict(xidVer,
                                        valBytes,
                                        e.expireTime(),
                                        e.ttl(),
                                        nodeId,
                                        topVer);
                                } catch (GridCacheEntryRemovedException re) {
                                    // This is impossible in TPP way, but anyway throw exception.
                                    throw new IgniteCheckedException("Failed to get value bytes for entry: " + e, re);
                                }
                            });
                    }

                    assert operationFut != null;

                    commitEntriesFut.add(operationFut);

                    // Assert after setting values as we want to make sure
                    // that if we replaced removed entries.
                    assert
                        txEntry.op() == READ || onePhaseCommit() ||
                            // If candidate is not there, then lock was explicit
                            // and we simply allow the commit to proceed.
                            !txContext.entry.hasLockCandidateUnsafe(xidVer) || txContext.entry.lockedByUnsafe(xidVer) :
                        "Transaction does not own lock for commit [entry=" + txContext.entry +
                            ", tx=" + this + ']';
                }
            }
            catch (Throwable t) {
                commitEntriesFut.markInitialized();

                commitEntriesFut.onDone(t);

                return commitEntriesFut;
            }

            commitEntriesFut.markInitialized();

            return commitEntriesFut;
        }
        finally {
            if (wrapper != null)
                wrapper.initialize(ret);
        }
    }

    private boolean checkLocks() {
        for (IgniteTxEntry txEntry : writeEntries()) {
            assert txEntry != null : "Missing transaction entry for tx: " + this;

            for (;;) {
                GridCacheEntryEx entry = txEntry.cached();

                assert entry != null : "Missing cached entry for transaction entry: " + txEntry;

                try {
                    GridCacheVersion ver = txEntry.explicitVersion() != null ? txEntry.explicitVersion() : xidVer;

                    // If locks haven't been acquired yet, keep waiting.
                    if (!entry.lockedBy(ver)) {
                        if (log.isDebugEnabled())
                            log.debug("Transaction does not own lock for entry (will wait) [entry=" + entry +
                                ", tx=" + this + ']');

                        return true;
                    }

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while committing (will retry): " + txEntry);

                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key(), topologyVersion()));
                }
            }
        }

        return false;
    }

    private Collection<IgniteTxEntry> commitEntries() {
        return near() || cctx.snapshot().needTxReadLogging() ? allEntries() : writeEntries();
    }

    private void finishCommitEntries(IgniteTxCommitEntriesFuture commitEntriesFuture) throws IgniteCheckedException {
        assert commitEntriesFuture.isDone();

        cctx.tm().finisher().check();

        if (state() == COMMITTED) {
//            log.warning("Already committed");

            return;
        }

        // Nothing to commit.
        if (!commitEntriesFuture.initialized()) {
//            log.warning("Nothing to commit -> " + commitEntriesFuture);

            return;
        }

        IgniteCheckedException err = null;

        try {
            WALPointer latestPtr = null;

            try {
                commitEntriesFuture.get(); // Check for errors.

                Collection<IgniteTxEntry> commitEntries = commitEntries();

                List<IgniteInternalFuture<GridCacheUpdateTxResult>> futures = commitEntriesFuture.futures();

                assert commitEntries.size() == futures.size();

                int txEntryIdx = 0;

                for (IgniteTxEntry txEntry : commitEntries) {
                    GridCacheUpdateTxResult txResult = futures.get(txEntryIdx).get();

                    if (txResult != null && txResult.success()) {
                        if (txResult.updatePartitionCounter() != -1)
                            txEntry.updateCounter(txResult.updatePartitionCounter());

                        WALPointer loggedPtr = txResult.loggedPointer();

                        // Find latest logged WAL pointer.
                        if (loggedPtr != null)
                            if (latestPtr == null || loggedPtr.compareTo(latestPtr) > 0)
                                latestPtr = loggedPtr;
                    }

                    txEntryIdx++;
                }
            }
            catch (Throwable ex) {
                boolean hasIOIssue = X.hasCause(ex, InvalidEnvironmentException.class);

                // In case of error, we still make the best effort to commit,
                // as there is no way to rollback at this point.
                err = new IgniteTxHeuristicCheckedException("Commit produced a runtime exception " +
                    "(all transaction entries will be invalidated): " + CU.txString(this), ex);

                if (hasIOIssue) {
                    U.warn(log, "Failed to commit transaction, node is stopping [tx=" + this +
                        ", err=" + ex + ']');
                }
                else
                    U.error(log, "Commit failed.", err);

                uncommit(hasIOIssue);

                state(UNKNOWN);

                if (ex instanceof Error)
                    throw (Error)ex;
            }

            if (err == null) {
                if (latestPtr != null && !cctx.tm().logTxRecords())
                    cctx.wal().flush(latestPtr, false);
            }
        }
        catch (StorageException e) {
            throw new IgniteCheckedException("Failed to log transaction record " +
                "(transaction will be rolled back): " + this, e);
        }

        if (err != null) {
            state(UNKNOWN);

            throw err;
        }

        cctx.tm().commitTx(this);

        state(COMMITTED);
    }

    /** {@inheritDoc} */
    @Override public IgniteTxCommitFuture startCommit() {
        try {
            cctx.tm().finisher().check();

            if (optimistic())
                state(PREPARED);

//            log.warning("I'm here 1");

            if (!state(COMMITTING)) {
                TransactionState state = state();

                // If other thread is doing commit, then no-op.
                if (state == COMMITTING || state == COMMITTED)
                    return new IgniteTxCommitFuture(false);

                if (log.isDebugEnabled())
                    log.debug("Failed to set COMMITTING transaction state (will rollback): " + this);

                setRollbackOnly();

                if (!isSystemInvalidate())
                    throw new IgniteCheckedException("Invalid transaction state for commit [state=" + state + ", tx=" + this + ']');

                rollbackRemoteTx();

                return new IgniteTxCommitFuture(true);
            }

//            log.warning("I'm here 2");

            return new IgniteTxCommitFuture(startCommitEntries(), true);
        }
        catch (Throwable t) {
            return new IgniteTxCommitFuture(t);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishCommit(IgniteTxCommitFuture commitFut) throws IgniteCheckedException {
        assert commitFut.isDone();

        commitFut.get(); // Check for errors.

        if (commitFut.commitEntriesFuture() != null)
            finishCommitEntries(commitFut.commitEntriesFuture());
    }

    /**
     * Forces commit for this tx.
     *
     * @throws IgniteCheckedException If commit failed.
     */
    public void forceCommit() throws IgniteCheckedException {
//        log.warning("Commit forced");

        IgniteTxCommitEntriesFuture commitFut = startCommitEntries();

        commitFut.listen(f -> cctx.tm().finisher().execute(this, () -> {
            try {
                finishCommitEntries(commitFut);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to commit remote transaction: " + this, e);

                invalidate(true);
                systemInvalidate(true);

                rollbackRemoteTx();
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        GridFutureAdapter<IgniteInternalTx> fut = new GridFutureAdapter<>();

        cctx.tm().finisher().execute(this, () -> {
            IgniteTxCommitFuture commitFut = startCommit();

            commitFut.listen(f -> cctx.tm().finisher().execute(this, () -> {
                try {
                    finishCommit(commitFut);

                    fut.onDone(this);
                }
                catch (IgniteCheckedException e) {
                    fut.onDone(e);
                }
            }));
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<?> salvageTx() {
        try {
            systemInvalidate(true);

            prepareRemoteTx();

            if (state() == PREPARING) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring transaction in PREPARING state as it is currently handled " +
                        "by another thread: " + this);

                return null;
            }

            doneRemote(xidVersion(),
                Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList());

            return commitAsync();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to invalidate transaction: " + xidVersion(), e);
        }

        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public final void rollbackRemoteTx() {
        try {
            // Note that we don't evict near entries here -
            // they will be deleted by their corresponding transactions.
            if (state(ROLLING_BACK) || state() == UNKNOWN) {
                cctx.tm().rollbackTx(this, false, skipCompletedVers);

                state(ROLLED_BACK);
            }
        }
        catch (RuntimeException | Error e) {
            state(UNKNOWN);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        rollbackRemoteTx();

        return new GridFinishedFuture<IgniteInternalTx>(this);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheVersion> alternateVersions() {
        return explicitVers == null ? Collections.<GridCacheVersion>emptyList() : explicitVers;
    }

    /** {@inheritDoc} */
    @Override public void commitError(Throwable e) {
        // No-op.
    }

    /**
     * @return {@code True} if tx should skip adding itself to completed version map on finish.
     */
    public boolean skipCompletedVersions() {
        return skipCompletedVers;
    }

    /**
     * @param skipCompletedVers {@code True} if tx should skip adding itself to completed version map on finish.
     */
    public void skipCompletedVersions(boolean skipCompletedVers) {
        this.skipCompletedVers = skipCompletedVers;
    }

    /**
     * Adds explicit version if there is one.
     *
     * @param e Transaction entry.
     */
    protected void addExplicit(IgniteTxEntry e) {
        if (e.explicitVersion() != null) {
            if (explicitVers == null)
                explicitVers = new LinkedList<>();

            if (!explicitVers.contains(e.explicitVersion())) {
                explicitVers.add(e.explicitVersion());

                if (log.isDebugEnabled())
                    log.debug("Added explicit version to transaction [explicitVer=" + e.explicitVersion() +
                        ", tx=" + this + ']');

                // Register alternate version with TM.
                cctx.tm().addAlternateVersion(e.explicitVersion(), this);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxRemoteAdapter.class, this, "super", super.toString());
    }

    private GridCacheEntryEx refresh(IgniteTxEntry txEntry) {
        return txEntry.context().cache().entryEx(txEntry.key(), topologyVersion());
    }

    private TransactionEntryContext prepareContext(IgniteTxEntry txEntry, GridCacheReturn ret) throws IgniteCheckedException {
        GridCacheContext cacheCtx = txEntry.context();

        for (;;) {
            GridCacheEntryEx cached = txEntry.cached();

            try {
                if (cached == null)
                    txEntry.cached(cached = cacheCtx.cache().entryEx(txEntry.key(), topologyVersion()));

                if (near() && cacheCtx.dr().receiveEnabled()) {
                    cached.markObsolete(xidVer);

                    return null;
                }

                final GridNearCacheEntry nearCached = updateNearCache(cacheCtx, txEntry.key(), topVer)
                    ? cacheCtx.dht().near().peekExx(txEntry.key())
                    : null;

                if (!F.isEmpty(txEntry.entryProcessors()))
                    txEntry.cached().unswap(false);

                IgniteBiTuple<GridCacheOperation, CacheObject> res = applyTransformClosures(txEntry, false, ret);

                GridCacheOperation op = res.get1();
                CacheObject val = res.get2();

                GridCacheVersion explicitVer = txEntry.conflictVersion();

                if (explicitVer == null)
                    explicitVer = writeVersion();

                if (txEntry.ttl() == CU.TTL_ZERO)
                    op = DELETE;

                boolean conflictNeedResolve = cacheCtx.conflictNeedResolve();

                GridCacheVersionConflictContext conflictCtx = null;

                if (conflictNeedResolve) {
                    IgniteBiTuple<GridCacheOperation, GridCacheVersionConflictContext>
                        drRes = conflictResolve(op, txEntry, val, explicitVer, cached);

                    assert drRes != null;

                    conflictCtx = drRes.get2();

                    if (conflictCtx.isUseOld())
                        op = NOOP;
                    else if (conflictCtx.isUseNew()) {
                        txEntry.ttl(conflictCtx.ttl());
                        txEntry.conflictExpireTime(conflictCtx.expireTime());
                    }
                    else if (conflictCtx.isMerge()) {
                        op = drRes.get1();
                        val = txEntry.context().toCacheObject(conflictCtx.mergeValue());
                        explicitVer = writeVersion();

                        txEntry.ttl(conflictCtx.ttl());
                        txEntry.conflictExpireTime(conflictCtx.expireTime());
                    }
                }
                else
                    // Nullify explicit version so that innerSet/innerRemove will work as usual.
                    explicitVer = null;

                GridCacheVersion dhtVer = cached.isNear() ? writeVersion() : null;

                return new TransactionEntryContext(cacheCtx, op, dhtVer, explicitVer, val, cached, nearCached, conflictCtx);
            }
            catch (GridCacheEntryRemovedException re) {
                txEntry.cached(cacheCtx.cache().entryEx(txEntry.key(), topologyVersion()));
            }
        }
    }

    /**
     * Context to perform operation with {@link IgniteTxEntry}.
     */
    class TransactionEntryContext {
        private final GridCacheContext cacheContext;
        private final GridCacheOperation operation;
        private final GridCacheVersion dhtVer;
        private final GridCacheVersion explicitVer;
        private final CacheObject value;
        private final GridCacheEntryEx entry;
        private final GridNearCacheEntry nearEntry;
        private final GridCacheVersionConflictContext conflictContext;

        public TransactionEntryContext(GridCacheContext cacheContext, GridCacheOperation operation, GridCacheVersion dhtVer, GridCacheVersion explicitVer, CacheObject value, GridCacheEntryEx entry, GridNearCacheEntry nearEntry, GridCacheVersionConflictContext conflictContext) {
            this.cacheContext = cacheContext;
            this.operation = operation;
            this.dhtVer = dhtVer;
            this.explicitVer = explicitVer;
            this.value = value;
            this.entry = entry;
            this.nearEntry = nearEntry;
            this.conflictContext = conflictContext;
        }
    }
}
