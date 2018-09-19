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

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.InvalidEnvironmentException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryProcessorResourceInjectorProxy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFilterFailedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCounters;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.RELOAD;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRIMARY;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Transaction adapter for cache transactions.
 */
public abstract class IgniteTxLocalAdapter extends IgniteTxAdapter implements IgniteTxLocalEx {
    /** */
    private static final long serialVersionUID = 0L;

    /** Commit error updater. */
    protected static final AtomicReferenceFieldUpdater<IgniteTxLocalAdapter, Throwable> COMMIT_ERR_UPD =
        AtomicReferenceFieldUpdater.newUpdater(IgniteTxLocalAdapter.class, Throwable.class, "commitErr");

    /** Done flag updater. */
    protected static final AtomicIntegerFieldUpdater<IgniteTxLocalAdapter> DONE_FLAG_UPD =
        AtomicIntegerFieldUpdater.newUpdater(IgniteTxLocalAdapter.class, "doneFlag");

    /** Minimal version encountered (either explicit lock or XID of this transaction). */
    protected GridCacheVersion minVer;

    /** Flag indicating with TM commit happened. */
    @SuppressWarnings("UnusedDeclaration")
    protected volatile int doneFlag;

    /** Committed versions, relative to base. */
    private Collection<GridCacheVersion> committedVers = Collections.emptyList();

    /** Rolled back versions, relative to base. */
    private Collection<GridCacheVersion> rolledbackVers = Collections.emptyList();

    /** Base for completed versions. */
    private GridCacheVersion completedBase;

    /** Flag indicating that transformed values should be sent to remote nodes. */
    private boolean sndTransformedVals;

    /** Commit error. */
    protected volatile Throwable commitErr;

    /** Implicit transaction result. */
    protected GridCacheReturn implicitRes;

    /** Flag indicating whether deployment is enabled for caches from this transaction or not. */
    private boolean depEnabled;

    /** */
    @GridToStringInclude
    protected IgniteTxLocalState txState;

    /** */
    protected CacheWriteSynchronizationMode syncMode;

    /** */
    private GridLongList mvccWaitTxs;

    /** */
    private volatile boolean qryEnlisted;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected IgniteTxLocalAdapter() {
        // No-op.
    }

    /**
     * @param cctx Cache registry.
     * @param xidVer Transaction ID.
     * @param implicit {@code True} if transaction was implicitly started by the system,
     *      {@code false} if it was started explicitly by user.
     * @param implicitSingle {@code True} if transaction is implicit with only one key.
     * @param sys System flag.
     * @param plc IO policy.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     */
    protected IgniteTxLocalAdapter(
        GridCacheSharedContext cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean storeEnabled,
        boolean onePhaseCommit,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(
            cctx,
            xidVer,
            implicit,
            /*local*/true,
            sys,
            plc,
            concurrency,
            isolation,
            timeout,
            invalidate,
            storeEnabled,
            onePhaseCommit,
            txSize,
            subjId,
            taskNameHash
        );

        minVer = xidVer;

        txState = implicitSingle ? new IgniteTxImplicitSingleStateImpl() : new IgniteTxStateImpl();
    }

    public GridLongList mvccWaitTransactions() {
        return mvccWaitTxs;
    }

    /**
     * @return Transaction write synchronization mode.
     */
    public final CacheWriteSynchronizationMode syncMode() {
        if (syncMode != null)
            return syncMode;

        return txState().syncMode(cctx);
    }

    /**
     * @param syncMode Write synchronization mode.
     */
    public void syncMode(CacheWriteSynchronizationMode syncMode) {
        this.syncMode = syncMode;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxLocalState txState() {
        return txState;
    }

    /**
     * Creates result instance.
     */
    protected void initResult() {
        implicitRes = new GridCacheReturn(localResult(), false);
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return cctx.localNodeId();
    }

    /** {@inheritDoc} */
    @Override public UUID originatingNodeId() {
        return cctx.localNodeId();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return txState.empty();
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        return Collections.singleton(nodeId);
    }

    /** {@inheritDoc} */
    @Override public Throwable commitError() {
        return commitErr;
    }

    /** {@inheritDoc} */
    @Override public void commitError(Throwable e) {
        COMMIT_ERR_UPD.compareAndSet(this, null, e);
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean activeCachesDeploymentEnabled() {
        return depEnabled;
    }

    /** {@inheritDoc} */
    @Override public void activeCachesDeploymentEnabled(boolean depEnabled) {
        this.depEnabled = depEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return txState.initialized();
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return txState.hasWriteKey(key);
    }

    /**
     * @return Transaction read set.
     */
    @Override public Set<IgniteTxKey> readSet() {
        return txState.readSet();
    }

    /**
     * @return Transaction write set.
     */
    @Override public Set<IgniteTxKey> writeSet() {
        return txState.writeSet();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return txState.readMap();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return txState.writeMap();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return txState.allEntries();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return txState.readEntries();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return txState.writeEntries();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteTxEntry entry(IgniteTxKey key) {
        return txState.entry(key);
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        txState.seal();
    }

    /**
     * @param ret Result.
     */
    public void implicitSingleResult(GridCacheReturn ret) {
        assert ret != null;

        if (ret.invokeResult())
            implicitRes.mergeEntryProcessResults(ret);
        else
            implicitRes = ret;
    }

    /**
     * @return {@code True} if transaction participates in a cache that has an interceptor configured.
     */
    public boolean hasInterceptor() {
        return txState().hasInterceptor(cctx);
    }

    /**
     * @param snd {@code True} if values in tx entries should be replaced with transformed values and sent
     * to remote nodes.
     */
    public void sendTransformedValues(boolean snd) {
        sndTransformedVals = snd;
    }

    /**
     * @return {@code True} if should be committed after lock is acquired.
     */
    protected boolean commitAfterLock() {
        return implicit() && (!dht() || colocated());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable @Override public GridTuple<CacheObject> peek(
        GridCacheContext cacheCtx,
        boolean failFast,
        KeyCacheObject key
    ) throws GridCacheFilterFailedException {
        IgniteTxEntry e = entry(cacheCtx.txKey(key));

        if (e != null)
            return e.hasPreviousValue() ? F.t(e.previousValue()) : null;

        return null;
    }

    /**
     * Gets minimum version present in transaction.
     *
     * @return Minimum versions.
     */
    @Override public GridCacheVersion minVersion() {
        return minVer;
    }

    /**
     * @param entries Entries to lock or {@code null} if use default {@link IgniteInternalTx#optimisticLockEntries()}.
     * @throws IgniteCheckedException If prepare step failed.
     */
    @SuppressWarnings({"CatchGenericClass"})
    public void userPrepare(@Nullable Collection<IgniteTxEntry> entries) throws IgniteCheckedException {
        if (state() != PREPARING) {
            if (remainingTime() == -1)
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            TransactionState state = state();

            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for prepare [state=" +
                state + ", tx=" + this + ']');
        }

        checkValid();

        try {
            cctx.tm().prepareTx(this, entries);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Throwable e) {
            setRollbackOnly();

            if (e instanceof Error)
                throw e;

            throw new IgniteCheckedException("Transaction validation produced a runtime exception: " + this, e);
        }
    }

    /**
     * Checks that locks are in proper state for commit.
     *
     * @param entry Cache entry to check.
     */
    private void checkCommitLocks(GridCacheEntryEx entry) {
        assert ownsLockUnsafe(entry) : "Lock is not owned for commit [entry=" + entry +
            ", tx=" + this + ']';
    }

    /**
     * Gets cache entry for given key.
     *
     * @param cacheCtx Cache context.
     * @param key Key.
     * @return Cache entry.
     */
    protected GridCacheEntryEx entryEx(GridCacheContext cacheCtx, IgniteTxKey key) {
        return cacheCtx.cache().entryEx(key.key());
    }

    /**
     * Gets cache entry for given key and topology version.
     *
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param topVer Topology version.
     * @return Cache entry.
     */
    protected GridCacheEntryEx entryEx(GridCacheContext cacheCtx, IgniteTxKey key, AffinityTopologyVersion topVer) {
        return cacheCtx.cache().entryEx(key.key(), topVer);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass"})
    @Override public final void userCommit() throws IgniteCheckedException {
        TransactionState state = state();

        if (state != COMMITTING) {
            if (remainingTime() == -1)
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for commit [state=" + state + ", tx=" + this + ']');
        }

        checkValid();

        Collection<IgniteTxEntry> commitEntries = (near() || cctx.snapshot().needTxReadLogging()) ? allEntries() : writeEntries();

        boolean empty = F.isEmpty(commitEntries) && !queryEnlisted();

        // Register this transaction as completed prior to write-phase to
        // ensure proper lock ordering for removed entries.
        // We add colocated transaction to committed set even if it is empty to correctly order
        // locks on backup nodes.
        if (!empty || colocated())
            cctx.tm().addCommittedTx(this);

        if (!empty) {
            assert mvccWaitTxs == null;

            batchStoreCommit(writeEntries());

            WALPointer ptr = null;

            Exception err = null;

            cctx.database().checkpointReadLock();

            try {
                cctx.tm().txContext(this);

                AffinityTopologyVersion topVer = topologyVersion();

                /*
                 * Commit to cache. Note that for 'near' transaction we loop through all the entries.
                 */
                for (IgniteTxEntry txEntry : commitEntries) {
                    GridCacheContext cacheCtx = txEntry.context();

                    GridDrType drType = cacheCtx.isDrEnabled() ? DR_PRIMARY : DR_NONE;

                    UUID nodeId = txEntry.nodeId() == null ? this.nodeId : txEntry.nodeId();

                    try {
                        while (true) {
                            try {
                                GridCacheEntryEx cached = txEntry.cached();

                                // Must try to evict near entries before committing from
                                // transaction manager to make sure locks are held.
                                if (!evictNearEntry(txEntry, false)) {
                                    if (cacheCtx.isNear() && cacheCtx.dr().receiveEnabled()) {
                                        cached.markObsolete(xidVer);

                                        break;
                                    }

                                    if (cached.detached())
                                        break;

                                    boolean updateNearCache = updateNearCache(cacheCtx, txEntry.key(), topVer);

                                    boolean metrics = true;

                                    if (!updateNearCache && cacheCtx.isNear() && txEntry.locallyMapped())
                                        metrics = false;

                                    boolean evt = !isNearLocallyMapped(txEntry, false);

                                    if (!F.isEmpty(txEntry.entryProcessors()) || !F.isEmpty(txEntry.filters()))
                                        txEntry.cached().unswap(false);

                                    IgniteBiTuple<GridCacheOperation, CacheObject> res = applyTransformClosures(txEntry,
                                        true, null);

                                    GridCacheVersion dhtVer = null;

                                    // For near local transactions we must record DHT version
                                    // in order to keep near entries on backup nodes until
                                    // backup remote transaction completes.
                                    if (cacheCtx.isNear()) {
                                        if (txEntry.op() == CREATE || txEntry.op() == UPDATE ||
                                            txEntry.op() == DELETE || txEntry.op() == TRANSFORM)
                                            dhtVer = txEntry.dhtVersion();

                                        if ((txEntry.op() == CREATE || txEntry.op() == UPDATE) &&
                                            txEntry.conflictExpireTime() == CU.EXPIRE_TIME_CALCULATE) {
                                            ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(txEntry);

                                            if (expiry != null) {
                                                txEntry.cached().unswap(false);

                                                Duration duration = cached.hasValue() ?
                                                    expiry.getExpiryForUpdate() : expiry.getExpiryForCreation();

                                                txEntry.ttl(CU.toTtl(duration));
                                            }
                                        }
                                    }

                                    GridCacheOperation op = res.get1();
                                    CacheObject val = res.get2();

                                    // Deal with conflicts.
                                    GridCacheVersion explicitVer = txEntry.conflictVersion() != null ?
                                        txEntry.conflictVersion() : writeVersion();

                                    if ((op == CREATE || op == UPDATE) &&
                                        txEntry.conflictExpireTime() == CU.EXPIRE_TIME_CALCULATE) {
                                        ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(txEntry);

                                        if (expiry != null) {
                                            Duration duration = cached.hasValue() ?
                                                expiry.getExpiryForUpdate() : expiry.getExpiryForCreation();

                                            long ttl = CU.toTtl(duration);

                                            txEntry.ttl(ttl);

                                            if (ttl == CU.TTL_ZERO)
                                                op = DELETE;
                                        }
                                    }

                                    boolean conflictNeedResolve = cacheCtx.conflictNeedResolve();

                                    GridCacheVersionConflictContext<?, ?> conflictCtx = null;

                                    if (conflictNeedResolve) {
                                        IgniteBiTuple<GridCacheOperation, GridCacheVersionConflictContext> conflictRes =
                                            conflictResolve(op, txEntry, val, explicitVer, cached);

                                        assert conflictRes != null;

                                        conflictCtx = conflictRes.get2();

                                        if (conflictCtx.isUseOld())
                                            op = NOOP;
                                        else if (conflictCtx.isUseNew()) {
                                            txEntry.ttl(conflictCtx.ttl());
                                            txEntry.conflictExpireTime(conflictCtx.expireTime());
                                        }
                                        else {
                                            assert conflictCtx.isMerge();

                                            op = conflictRes.get1();
                                            val = txEntry.context().toCacheObject(conflictCtx.mergeValue());
                                            explicitVer = writeVersion();

                                            txEntry.ttl(conflictCtx.ttl());
                                            txEntry.conflictExpireTime(conflictCtx.expireTime());
                                        }
                                    }
                                    else
                                        // Nullify explicit version so that innerSet/innerRemove will work as usual.
                                        explicitVer = null;

                                    if (sndTransformedVals || conflictNeedResolve) {
                                        assert sndTransformedVals && cacheCtx.isReplicated() || conflictNeedResolve;

                                        txEntry.value(val, true, false);
                                        txEntry.op(op);
                                        txEntry.entryProcessors(null);
                                        txEntry.conflictVersion(explicitVer);
                                    }

                                    if (dhtVer == null)
                                        dhtVer = explicitVer != null ? explicitVer : writeVersion();

                                    if (op == CREATE || op == UPDATE) {
                                        assert val != null : txEntry;

                                        GridCacheUpdateTxResult updRes = cached.innerSet(
                                            this,
                                            eventNodeId(),
                                            txEntry.nodeId(),
                                            val,
                                            false,
                                            false,
                                            txEntry.ttl(),
                                            evt,
                                            metrics,
                                            txEntry.keepBinary(),
                                            txEntry.hasOldValue(),
                                            txEntry.oldValue(),
                                            topVer,
                                            null,
                                            cached.detached() ? DR_NONE : drType,
                                            txEntry.conflictExpireTime(),
                                            cached.isNear() ? null : explicitVer,
                                            CU.subjectId(this, cctx),
                                            resolveTaskName(),
                                            dhtVer,
                                            null,
                                            mvccSnapshot());

                                        if (updRes.success()) {
                                            txEntry.updateCounter(updRes.updateCounter());

                                            GridLongList waitTxs = updRes.mvccWaitTransactions();

                                            updateWaitTxs(waitTxs);
                                        }

                                        if (updRes.loggedPointer() != null)
                                            ptr = updRes.loggedPointer();

                                        if (updRes.success() && updateNearCache) {
                                            final CacheObject val0 = val;
                                            final boolean metrics0 = metrics;
                                            final GridCacheVersion dhtVer0 = dhtVer;

                                            updateNearEntrySafely(cacheCtx, txEntry.key(), entry -> entry.innerSet(
                                                null,
                                                eventNodeId(),
                                                nodeId,
                                                val0,
                                                false,
                                                false,
                                                txEntry.ttl(),
                                                false,
                                                metrics0,
                                                txEntry.keepBinary(),
                                                txEntry.hasOldValue(),
                                                txEntry.oldValue(),
                                                topVer,
                                                CU.empty0(),
                                                DR_NONE,
                                                txEntry.conflictExpireTime(),
                                                null,
                                                CU.subjectId(this, cctx),
                                                resolveTaskName(),
                                                dhtVer0,
                                                null,
                                                mvccSnapshot())
                                            );
                                        }
                                    }
                                    else if (op == DELETE) {
                                        GridCacheUpdateTxResult updRes = cached.innerRemove(
                                            this,
                                            eventNodeId(),
                                            txEntry.nodeId(),
                                            false,
                                            evt,
                                            metrics,
                                            txEntry.keepBinary(),
                                            txEntry.hasOldValue(),
                                            txEntry.oldValue(),
                                            topVer,
                                            null,
                                            cached.detached() ? DR_NONE : drType,
                                            cached.isNear() ? null : explicitVer,
                                            CU.subjectId(this, cctx),
                                            resolveTaskName(),
                                            dhtVer,
                                            null,
                                            mvccSnapshot());

                                        if (updRes.success()) {
                                            txEntry.updateCounter(updRes.updateCounter());

                                            GridLongList waitTxs = updRes.mvccWaitTransactions();

                                            updateWaitTxs(waitTxs);
                                        }

                                        if (updRes.loggedPointer() != null)
                                            ptr = updRes.loggedPointer();

                                        if (updRes.success() && updateNearCache) {
                                            final boolean metrics0 = metrics;
                                            final GridCacheVersion dhtVer0 = dhtVer;

                                            updateNearEntrySafely(cacheCtx, txEntry.key(), entry -> entry.innerRemove(
                                                null,
                                                eventNodeId(),
                                                nodeId,
                                                false,
                                                false,
                                                metrics0,
                                                txEntry.keepBinary(),
                                                txEntry.hasOldValue(),
                                                txEntry.oldValue(),
                                                topVer,
                                                CU.empty0(),
                                                DR_NONE,
                                                null,
                                                CU.subjectId(this, cctx),
                                                resolveTaskName(),
                                                dhtVer0,
                                                null,
                                                mvccSnapshot())
                                            );
                                        }
                                    }
                                    else if (op == RELOAD) {
                                        cached.innerReload();

                                        if (updateNearCache)
                                            updateNearEntrySafely(cacheCtx, txEntry.key(), entry -> entry.innerReload());
                                    }
                                    else if (op == READ) {
                                        CacheGroupContext grp = cacheCtx.group();

                                        if (grp.persistenceEnabled() && grp.walEnabled() &&
                                            cctx.snapshot().needTxReadLogging()) {
                                            ptr = cctx.wal().log(new DataRecord(new DataEntry(
                                                cacheCtx.cacheId(),
                                                txEntry.key(),
                                                val,
                                                op,
                                                nearXidVersion(),
                                                writeVersion(),
                                                0,
                                                txEntry.key().partition(),
                                                txEntry.updateCounter())));
                                        }

                                        ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(txEntry);

                                        if (expiry != null) {
                                            Duration duration = expiry.getExpiryForAccess();

                                            if (duration != null)
                                                cached.updateTtl(null, CU.toTtl(duration));
                                        }

                                        if (log.isDebugEnabled())
                                            log.debug("Ignoring READ entry when committing: " + txEntry);
                                    }
                                    else {
                                        assert ownsLock(txEntry.cached()) :
                                            "Transaction does not own lock for group lock entry during  commit [tx=" +
                                                this + ", txEntry=" + txEntry + ']';

                                        if (conflictCtx == null || !conflictCtx.isUseOld()) {
                                            if (txEntry.ttl() != CU.TTL_NOT_CHANGED)
                                                cached.updateTtl(null, txEntry.ttl());
                                        }

                                        if (log.isDebugEnabled())
                                            log.debug("Ignoring NOOP entry when committing: " + txEntry);
                                    }
                                }

                                // Check commit locks after set, to make sure that
                                // we are not changing obsolete entries.
                                // (innerSet and innerRemove will throw an exception
                                // if an entry is obsolete).
                                if (txEntry.op() != READ)
                                    checkCommitLocks(cached);

                                // Break out of while loop.
                                break;
                            }
                            // If entry cached within transaction got removed.
                            catch (GridCacheEntryRemovedException ignored) {
                                if (log.isDebugEnabled())
                                    log.debug("Got removed entry during transaction commit (will retry): " + txEntry);

                                txEntry.cached(entryEx(cacheCtx, txEntry.txKey(), topologyVersion()));
                            }
                        }
                    }
                    catch (Throwable ex) {
                        // We are about to initiate transaction rollback when tx has started to committing.
                        // Need to remove version from committed list.
                        cctx.tm().removeCommittedTx(this);

                        boolean isNodeStopping = X.hasCause(ex, NodeStoppingException.class);
                        boolean hasInvalidEnvironmentIssue = X.hasCause(ex, InvalidEnvironmentException.class);

                        IgniteCheckedException err0 = new IgniteTxHeuristicCheckedException("Failed to locally write to cache " +
                            "(all transaction entries will be invalidated, however there was a window when " +
                            "entries for this transaction were visible to others): " + this, ex);

                        if (isNodeStopping) {
                            U.warn(log, "Failed to commit transaction, node is stopping [tx=" + this +
                                ", err=" + ex + ']');
                        }
                        else if (hasInvalidEnvironmentIssue) {
                            U.warn(log, "Failed to commit transaction, node is in invalid state and will be stopped [tx=" + this +
                                ", err=" + ex + ']');
                        }
                        else
                            U.error(log, "Commit failed.", err0);

                        COMMIT_ERR_UPD.compareAndSet(this, null, err0);

                        state(UNKNOWN);

                        if (hasInvalidEnvironmentIssue)
                            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
                        else if (!isNodeStopping) { // Skip fair uncommit in case of node stopping or invalidation.
                            try {
                                // Courtesy to minimize damage.
                                uncommit();
                            }
                            catch (Throwable ex1) {
                                U.error(log, "Failed to uncommit transaction: " + this, ex1);

                                if (ex1 instanceof Error)
                                    throw ex1;
                            }
                        }

                        if (ex instanceof Error)
                            throw ex;

                        throw err0;
                    }
                }

                applyTxCounters();

                if (ptr != null && !cctx.tm().logTxRecords())
                    cctx.wal().flush(ptr, false);
            }
            catch (StorageException e) {
                err = e;

                throw new IgniteCheckedException("Failed to log transaction record " +
                    "(transaction will be rolled back): " + this, e);
            }
            finally {
                cctx.database().checkpointReadUnlock();

                notifyDrManager(state() == COMMITTING && err == null);

                cctx.tm().resetContext();
            }
        }

        // Do not unlock transaction entries if one-phase commit.
        if (!onePhaseCommit()) {
            if (DONE_FLAG_UPD.compareAndSet(this, 0, 1)) {
                // Unlock all locks.
                cctx.tm().commitTx(this);

                boolean needsCompletedVersions = needsCompletedVersions();

                assert !needsCompletedVersions || completedBase != null;
                assert !needsCompletedVersions || committedVers != null;
                assert !needsCompletedVersions || rolledbackVers != null;
            }
        }
    }

    /**
     * @param waitTxs Tx ids to wait for.
     */
    private void updateWaitTxs(@Nullable GridLongList waitTxs) {
        if (waitTxs != null) {
            if (this.mvccWaitTxs == null)
                this.mvccWaitTxs = waitTxs;
            else
                this.mvccWaitTxs.addAll(waitTxs);
        }
    }

    /**
     * Safely performs {@code updateClojure} operation on near cache entry with given {@code entryKey}.
     * In case of {@link GridCacheEntryRemovedException} operation will be retried.
     *
     * @param cacheCtx Cache context.
     * @param entryKey Entry key.
     * @param updateClojure Near entry update clojure.
     * @throws IgniteCheckedException If update is failed.
     */
    private void updateNearEntrySafely(
        GridCacheContext cacheCtx,
        KeyCacheObject entryKey,
        NearEntryUpdateClojure<GridCacheEntryEx> updateClojure
    ) throws IgniteCheckedException {
        while (true) {
            GridCacheEntryEx nearCached = cacheCtx.dht().near().peekEx(entryKey);

            if (nearCached == null)
                break;

            try {
                updateClojure.apply(nearCached);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Got removed entry during transaction commit (will retry): " + nearCached);

                cacheCtx.dht().near().removeEntry(nearCached);
            }
        }
    }


    /**
     * Commits transaction to transaction manager. Used for one-phase commit transactions only.
     *
     * @param commit If {@code true} commits transaction, otherwise rollbacks.
     * @param clearThreadMap If {@code true} removes {@link GridNearTxLocal} from thread map.
     * @param nodeStop If {@code true} tx is cancelled on node stop.
     * @throws IgniteCheckedException If failed.
     */
    public void tmFinish(boolean commit, boolean nodeStop, boolean clearThreadMap) throws IgniteCheckedException {
        assert onePhaseCommit();

        if (DONE_FLAG_UPD.compareAndSet(this, 0, 1)) {
            if (!nodeStop) {
                // Unlock all locks.
                if (commit)
                    cctx.tm().commitTx(this);
                else
                    cctx.tm().rollbackTx(this, clearThreadMap, false);
            }

            state(commit ? COMMITTED : ROLLED_BACK);

            if (commit) {
                boolean needsCompletedVersions = needsCompletedVersions();

                assert !needsCompletedVersions || completedBase != null : "Missing completed base for transaction: " + this;
                assert !needsCompletedVersions || committedVers != null : "Missing committed versions for transaction: " + this;
                assert !needsCompletedVersions || rolledbackVers != null : "Missing rolledback versions for transaction: " + this;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void completedVersions(
        GridCacheVersion completedBase,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers) {
        this.completedBase = completedBase;
        this.committedVers = committedVers;
        this.rolledbackVers = rolledbackVers;
    }

    /**
     * @return Completed base for ordering.
     */
    public GridCacheVersion completedBase() {
        return completedBase;
    }

    /**
     * @return Committed versions.
     */
    public Collection<GridCacheVersion> committedVersions() {
        return committedVers;
    }

    /**
     * @return Rolledback versions.
     */
    public Collection<GridCacheVersion> rolledbackVersions() {
        return rolledbackVers;
    }

    /** {@inheritDoc} */
    @Override public void userRollback(boolean clearThreadMap) throws IgniteCheckedException {
        TransactionState state = state();

        notifyDrManager(false);

        if (state != ROLLING_BACK && state != ROLLED_BACK) {
            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for rollback [state=" + state +
                ", tx=" + this + ']');
        }

        if (near()) {
            // Must evict near entries before rolling back from
            // transaction manager, so they will be removed from cache.
            for (IgniteTxEntry e : allEntries())
                evictNearEntry(e, false);
        }

        if (DONE_FLAG_UPD.compareAndSet(this, 0, 1)) {
            cctx.tm().rollbackTx(this, clearThreadMap, false);

            if (!internal()) {
                Collection<CacheStoreManager> stores = txState.stores(cctx);

                if (stores != null && !stores.isEmpty()) {
                    assert isWriteToStoreFromDhtValid(stores) :
                        "isWriteToStoreFromDht can't be different within one transaction";

                    boolean isWriteToStoreFromDht = F.first(stores).isWriteToStoreFromDht();

                    if (!stores.isEmpty() && (near() || isWriteToStoreFromDht))
                        sessionEnd(stores, false);
                }
            }
        }
    }

    /**
     * @param ctx Cache context.
     * @param key Key.
     * @param expiryPlc Expiry policy.
     * @return Expiry policy wrapper for entries accessed locally in optimistic transaction.
     */
    protected IgniteCacheExpiryPolicy accessPolicy(
        GridCacheContext ctx,
        IgniteTxKey key,
        @Nullable ExpiryPolicy expiryPlc
    ) {
        return null;
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys.
     * @return Expiry policy.
     */
    protected IgniteCacheExpiryPolicy accessPolicy(GridCacheContext cacheCtx, Collection<KeyCacheObject> keys) {
        return null;
    }

    /**
     * Post lock processing for put or remove.
     *
     * @param cacheCtx Context.
     * @param keys Keys.
     * @param ret Return value.
     * @param rmv {@code True} if remove.
     * @param retval Flag to return value or not.
     * @param read {@code True} if read.
     * @param accessTtl TTL for read operation.
     * @param filter Filter to check entries.
     * @param computeInvoke If {@code true} computes return value for invoke operation.
     * @throws IgniteCheckedException If error.
     */
    @SuppressWarnings("unchecked")
    protected final void postLockWrite(
        GridCacheContext cacheCtx,
        Iterable<KeyCacheObject> keys,
        GridCacheReturn ret,
        boolean rmv,
        boolean retval,
        boolean read,
        long accessTtl,
        CacheEntryPredicate[] filter,
        boolean computeInvoke
    ) throws IgniteCheckedException {
        for (KeyCacheObject k : keys) {
            IgniteTxEntry txEntry = entry(cacheCtx.txKey(k));

            if (txEntry == null)
                throw new IgniteCheckedException("Transaction entry is null (most likely collection of keys passed into cache " +
                    "operation was changed before operation completed) [missingKey=" + k + ", tx=" + this + ']');

            while (true) {
                GridCacheEntryEx cached = txEntry.cached();

                try {
                    assert cached.detached() || cached.lockedByThread(threadId) || isRollbackOnly() :
                        "Transaction lock is not acquired [entry=" + cached + ", tx=" + this +
                            ", nodeId=" + cctx.localNodeId() + ", threadId=" + threadId + ']';

                    if (log.isDebugEnabled())
                        log.debug("Post lock write entry: " + cached);

                    CacheObject v = txEntry.previousValue();
                    boolean hasPrevVal = txEntry.hasPreviousValue();

                    if (onePhaseCommit())
                        filter = txEntry.filters();

                    // If we have user-passed filter, we must read value into entry for peek().
                    if (!F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter))
                        retval = true;

                    boolean invoke = txEntry.op() == TRANSFORM;

                    if (retval || invoke) {
                        if (!cacheCtx.isNear()) {
                            if (!hasPrevVal) {
                                // For non-local cache should read from store after lock on primary.
                                boolean readThrough = cacheCtx.isLocal() &&
                                    (invoke || cacheCtx.loadPreviousValue()) &&
                                    !txEntry.skipStore();

                                v = cached.innerGet(
                                    null,
                                    this,
                                    readThrough,
                                    /*metrics*/!invoke,
                                    /*event*/!invoke && !dht(),
                                    CU.subjectId(this, cctx),
                                    null,
                                    resolveTaskName(),
                                    null,
                                    txEntry.keepBinary(),
                                    null);  // TODO IGNITE-7371
                            }
                        }
                        else {
                            if (!hasPrevVal)
                                v = cached.rawGet();
                        }

                        if (txEntry.op() == TRANSFORM) {
                            if (computeInvoke) {
                                txEntry.readValue(v);

                                GridCacheVersion ver;

                                try {
                                    ver = cached.version();
                                }
                                catch (GridCacheEntryRemovedException e) {
                                    assert optimistic() : txEntry;

                                    if (log.isDebugEnabled())
                                        log.debug("Failed to get entry version: [msg=" + e.getMessage() + ']');

                                    ver = null;
                                }

                                addInvokeResult(txEntry, v, ret, ver);
                            }
                        }
                        else
                            ret.value(cacheCtx, v, txEntry.keepBinary());
                    }

                    boolean pass = F.isEmpty(filter) || cacheCtx.isAll(cached, filter);

                    // For remove operation we return true only if we are removing s/t,
                    // i.e. cached value is not null.
                    ret.success(pass && (!retval ? !rmv || cached.hasValue() || v != null : !rmv || v != null));

                    if (onePhaseCommit())
                        txEntry.filtersPassed(pass);

                    boolean updateTtl = read;

                    if (pass) {
                        txEntry.markValid();

                        if (log.isDebugEnabled())
                            log.debug("Filter passed in post lock for key: " + k);
                    }
                    else {
                        // Revert operation to previous. (if no - NOOP, so entry will be unlocked).
                        txEntry.setAndMarkValid(txEntry.previousOperation(), cacheCtx.toCacheObject(ret.value()));
                        txEntry.filters(CU.empty0());
                        txEntry.filtersSet(false);

                        updateTtl = !cacheCtx.putIfAbsentFilter(filter);
                    }

                    if (updateTtl) {
                        if (!read) {
                            ExpiryPolicy expiryPlc = cacheCtx.expiryForTxEntry(txEntry);

                            if (expiryPlc != null)
                                txEntry.ttl(CU.toTtl(expiryPlc.getExpiryForAccess()));
                        }
                        else
                            txEntry.ttl(accessTtl);
                    }

                    break; // While.
                }
                // If entry cached within transaction got removed before lock.
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in putAllAsync method (will retry): " + cached);

                    txEntry.cached(entryEx(cached.context(), txEntry.txKey(), topologyVersion()));
                }
            }
        }
    }

    /**
     * @param txEntry Entry.
     * @param cacheVal Value.
     * @param ret Return value to update.
     * @param ver Entry version.
     */
    protected final void addInvokeResult(IgniteTxEntry txEntry,
        CacheObject cacheVal,
        GridCacheReturn ret,
        GridCacheVersion ver)
    {
        GridCacheContext ctx = txEntry.context();

        Object key0 = null;
        Object val0 = null;

        IgniteThread.onEntryProcessorEntered(true);

        try {
            Object res = null;

            for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : txEntry.entryProcessors()) {
                CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry<>(txEntry.key(), key0, cacheVal,
                    val0, ver, txEntry.keepBinary(), txEntry.cached());

                EntryProcessor<Object, Object, ?> entryProcessor = t.get1();

                res = entryProcessor.process(invokeEntry, t.get2());

                val0 = invokeEntry.getValue(txEntry.keepBinary());

                key0 = invokeEntry.key();
            }

            if (val0 != null) // no validation for remove case
                ctx.validateKeyAndValue(txEntry.key(), ctx.toCacheObject(val0));

            if (res != null)
                ret.addEntryProcessResult(ctx, txEntry.key(), key0, res, null, txEntry.keepBinary());
        }
        catch (Exception e) {
            ret.addEntryProcessResult(ctx, txEntry.key(), key0, null, e, txEntry.keepBinary());
        }
        finally {
            IgniteThread.onEntryProcessorLeft();
        }
    }

    /**
     * Initializes read map.
     *
     * @return {@code True} if transaction was successfully  started.
     */
    public boolean init() {
        return !txState.init(txSize) || cctx.tm().onStarted(this);
    }

    /** {@inheritDoc} */
    @Override public final void addActiveCache(GridCacheContext cacheCtx, boolean recovery) throws IgniteCheckedException {
        txState.addActiveCache(cacheCtx, recovery, this);
    }

    /**
     * Checks transaction expiration.
     *
     * @throws IgniteCheckedException If transaction check failed.
     */
    protected void checkValid() throws IgniteCheckedException {
        checkValid(true);
    }

    /**
     * Checks transaction expiration.
     *
     * @param checkTimeout Whether timeout should be checked.
     * @throws IgniteCheckedException If transaction check failed.
     */
    protected void checkValid(boolean checkTimeout) throws IgniteCheckedException {
        if (local() && !dht() && remainingTime() == -1 && checkTimeout)
            state(MARKED_ROLLBACK, true);

        if (isRollbackOnly()) {
            if (remainingTime() == -1)
                throw new IgniteTxTimeoutCheckedException("Cache transaction timed out: " + CU.txString(this));

            TransactionState state = state();

            if (state == ROLLING_BACK || state == ROLLED_BACK)
                throw new IgniteTxRollbackCheckedException("Cache transaction is marked as rollback-only " +
                    "(will be rolled back automatically): " + this);

            if (state == UNKNOWN)
                throw new IgniteTxHeuristicCheckedException("Cache transaction is in unknown state " +
                    "(remote transactions will be invalidated): " + this);

            throw rollbackException();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheVersion> alternateVersions() {
        return Collections.emptyList();
    }

    /**
     * @param op Cache operation.
     * @param val Value.
     * @param expiryPlc Explicitly specified expiry policy.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param entryProcessor Entry processor.
     * @param entry Cache entry.
     * @param filter Filter.
     * @param filtersSet {@code True} if filter should be marked as set.
     * @param drTtl DR TTL (if any).
     * @param drExpireTime DR expire time (if any).
     * @param drVer DR version.
     * @param skipStore Skip store flag.
     * @return Transaction entry.
     */
    public final IgniteTxEntry addEntry(GridCacheOperation op,
        @Nullable CacheObject val,
        @Nullable EntryProcessor entryProcessor,
        Object[] invokeArgs,
        GridCacheEntryEx entry,
        @Nullable ExpiryPolicy expiryPlc,
        CacheEntryPredicate[] filter,
        boolean filtersSet,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer,
        boolean skipStore,
        boolean keepBinary,
        boolean addReader
    ) {
        assert invokeArgs == null || op == TRANSFORM;

        IgniteTxKey key = entry.txKey();

        checkInternal(key);

        IgniteTxEntry old = entry(key);

        // Keep old filter if already have one (empty filter is always overridden).
        if (!filtersSet || !F.isEmptyOrNulls(filter)) {
            // Replace filter if previous filter failed.
            if (old != null && old.filtersSet())
                filter = old.filters();
        }

        IgniteTxEntry txEntry;

        if (old != null) {
            if (entryProcessor != null) {
                assert val == null;
                assert op == TRANSFORM;

                // Will change the op.
                old.addEntryProcessor(entryProcessor, invokeArgs);
            }
            else {
                assert old.op() != TRANSFORM;

                old.op(op);
                old.value(val, op == CREATE || op == UPDATE || op == DELETE, op == READ);
            }

            // Keep old ttl value.
            old.cached(entry);
            old.filters(filter);

            // Keep old skipStore and keepBinary flags.
            old.skipStore(skipStore);
            old.keepBinary(keepBinary);

            // Update ttl if specified.
            if (drTtl >= 0L) {
                assert drExpireTime >= 0L;

                entryTtlDr(key, drTtl, drExpireTime);
            }
            else
                entryExpiry(key, expiryPlc);

            txEntry = old;

            if (log.isDebugEnabled())
                log.debug("Updated transaction entry: " + txEntry);
        }
        else {
            boolean hasDrTtl = drTtl >= 0;

            txEntry = new IgniteTxEntry(entry.context(),
                this,
                op,
                val,
                EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), entryProcessor),
                invokeArgs,
                hasDrTtl ? drTtl : -1L,
                entry,
                filter,
                drVer,
                skipStore,
                keepBinary,
                addReader);

            txEntry.conflictExpireTime(drExpireTime);

            if (!hasDrTtl)
                txEntry.expiry(expiryPlc);

            txState.addEntry(txEntry);

            if (log.isDebugEnabled())
                log.debug("Created transaction entry: " + txEntry);
        }

        txEntry.filtersSet(filtersSet);

        while (true) {
            try {
                updateExplicitVersion(txEntry, entry);

                return txEntry;
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Got removed entry in transaction newEntry method (will retry): " + entry);

                entry = entryEx(entry.context(), txEntry.txKey(), topologyVersion());

                txEntry.cached(entry);
            }
        }
    }

    /**
     * Updates explicit version for tx entry based on current entry lock owner.
     *
     * @param txEntry Tx entry to update.
     * @param entry Entry.
     * @throws GridCacheEntryRemovedException If entry was concurrently removed.
     */
    protected void updateExplicitVersion(IgniteTxEntry txEntry, GridCacheEntryEx entry)
        throws GridCacheEntryRemovedException {
        if (!entry.context().isDht()) {
            // All put operations must wait for async locks to complete,
            // so it is safe to get acquired locks.
            GridCacheMvccCandidate explicitCand = entry.localOwner();

            if (explicitCand == null)
                explicitCand = cctx.mvcc().explicitLock(threadId(), entry.txKey());

            if (explicitCand != null) {
                GridCacheVersion explicitVer = explicitCand.version();

                boolean locCand = false;

                if (explicitCand.nearLocal() || explicitCand.local())
                    locCand = cctx.localNodeId().equals(explicitCand.nodeId());
                else if (explicitCand.dhtLocal())
                    locCand = cctx.localNodeId().equals(explicitCand.otherNodeId());

                if (!explicitVer.equals(xidVer) && explicitCand.threadId() == threadId && !explicitCand.tx() && locCand) {
                    txEntry.explicitVersion(explicitVer);

                    if (explicitVer.isLess(minVer))
                        minVer = explicitVer;
                }
            }
        }
    }

    /**
     * @return Map of affected partitions: cacheId -> partId.
     */
    public Map<Integer, Set<Integer>> partsMap() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void touchPartition(int cacheId, int partId) {
        txState.touchPartition(cacheId, partId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteTxLocalAdapter.class, this, "super", super.toString(),
            "size", allEntries().size());
    }

    /**
     * @param key Key.
     * @param expiryPlc Expiry policy.
     */
    void entryExpiry(IgniteTxKey key, @Nullable ExpiryPolicy expiryPlc) {
        assert key != null;

        IgniteTxEntry e = entry(key);

        if (e != null) {
            e.expiry(expiryPlc);
            e.conflictExpireTime(CU.EXPIRE_TIME_CALCULATE);
        }
    }

    /**
     * @param key Key.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @return {@code true} if tx entry exists for this key, {@code false} otherwise.
     */
    boolean entryTtlDr(IgniteTxKey key, long ttl, long expireTime) {
        assert key != null;
        assert ttl >= 0;

        IgniteTxEntry e = entry(key);

        if (e != null) {
            e.ttl(ttl);

            e.conflictExpireTime(expireTime);

            e.expiry(null);
        }

        return e != null;
    }

    /**
     * @param key Key.
     * @return Tx entry time to live.
     */
    public long entryTtl(IgniteTxKey key) {
        assert key != null;

        IgniteTxEntry e = entry(key);

        return e != null ? e.ttl() : 0;
    }

    /**
     * @param key Key.
     * @return Tx entry expire time.
     */
    public long entryExpireTime(IgniteTxKey key) {
        assert key != null;

        IgniteTxEntry e = entry(key);

        if (e != null) {
            long ttl = e.ttl();

            assert ttl != -1;

            if (ttl > 0) {
                long expireTime = U.currentTimeMillis() + ttl;

                if (expireTime > 0)
                    return expireTime;
            }
        }

        return 0;
    }

    /**
     * Merges mvcc update counters to the partition update counters. For mvcc transactions we update partitions
     * counters only on commit phase.
     */
    private Map<Integer, PartitionUpdateCounters> applyAndCollectLocalUpdateCounters() {
        if (F.isEmpty(txState.touchedPartitions()))
            return null;

        HashMap<Integer, PartitionUpdateCounters> updCntrs = new HashMap<>();

        for (Map.Entry<Integer, Set<Integer>> entry : txState.touchedPartitions().entrySet()) {
            Integer cacheId = entry.getKey();

            Set<Integer> parts = entry.getValue();

            assert !F.isEmpty(parts);

            GridCacheContext ctx0 = cctx.cacheContext(cacheId);

            Map<Integer, Long> partCntrs = new HashMap<>(parts.size());

            for (Integer p : parts) {
                GridDhtLocalPartition dhtPart = ctx0.topology().localPartition(p);

                assert dhtPart != null;

                long cntr = dhtPart.mvccUpdateCounter();

                dhtPart.updateCounter(cntr);

                partCntrs.put(p, cntr);
            }

            updCntrs.put(cacheId, new PartitionUpdateCounters(partCntrs));
        }

        return updCntrs;
    }

    /**
     * @return {@code True} if there are entries, enlisted by query.
     */
    public boolean queryEnlisted() {
        return qryEnlisted;
    }

    /**
     * @param ver Mvcc version.
     */
    public void markQueryEnlisted(MvccSnapshot ver) {
        if (!qryEnlisted) {
            if (mvccSnapshot == null)
                mvccSnapshot = ver;

            cctx.coordinators().registerLocalTransaction(ver.coordinatorVersion(), ver.counter());

            qryEnlisted = true;
        }
    }

    /** {@inheritDoc} */
    @Override protected void applyTxCounters() {
        super.applyTxCounters();

        Map<Integer, PartitionUpdateCounters> updCntrs = applyAndCollectLocalUpdateCounters();

        // remember counters for subsequent sending to backups
        txCounters(true).updateCounters(updCntrs);
    }

    /**
     * Post-lock closure alias.
     *
     * @param <T> Return type.
     */
    protected abstract class PLC1<T> extends PostLockClosure1<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         */
        protected PLC1(T arg) {
            super(arg);
        }

        /**
         * @param arg Argument.
         * @param commit Commit flag.
         */
        protected PLC1(T arg, boolean commit) {
            super(arg, commit);
        }
    }

    /**
     * Post-lock closure alias.
     *
     * @param <T> Return type.
     */
    protected abstract class PLC2<T> extends PostLockClosure2<T> {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Post-lock closure alias.
     *
     * @param <T> Return type.
     */
    protected abstract class PMC<T> extends PostMissClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Post-lock closure.
     *
     * @param <T> Return type.
     */
    protected abstract class PostLockClosure1<T> implements IgniteBiClosure<Boolean, Exception, IgniteInternalFuture<T>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Closure argument. */
        private T arg;

        /** Commit flag. */
        private boolean commit;

        /**
         * Creates a Post-Lock closure that will pass the argument given to the {@code postLock} method.
         *
         * @param arg Argument for {@code postLock}.
         */
        protected PostLockClosure1(T arg) {
            this(arg, true);
        }

        /**
         * Creates a Post-Lock closure that will pass the argument given to the {@code postLock} method.
         *
         * @param arg Argument for {@code postLock}.
         * @param commit Flag indicating whether commit should be done after postLock.
         */
        protected PostLockClosure1(T arg, boolean commit) {
            this.arg = arg;
            this.commit = commit;
        }

        /** {@inheritDoc} */
        @Override public final IgniteInternalFuture<T> apply(Boolean locked, @Nullable final Exception e) {
            TransactionDeadlockException deadlockErr = X.cause(e, TransactionDeadlockException.class);

            if (e != null && deadlockErr == null) {
                setRollbackOnly();

                if (commit && commitAfterLock())
                    return rollbackAsync().chain(new C1<IgniteInternalFuture<IgniteInternalTx>, T>() {
                        @Override public T apply(IgniteInternalFuture<IgniteInternalTx> f) {
                            throw new GridClosureException(e);
                        }
                    });

                throw new GridClosureException(e);
            }

            if (deadlockErr != null || !locked) {
                setRollbackOnly();

                final GridClosureException ex = new GridClosureException(
                    new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout " +
                        "for transaction [timeout=" + timeout() + ", tx=" + CU.txString(IgniteTxLocalAdapter.this) +
                        ']', deadlockErr)
                );

                if (commit && commitAfterLock())
                    return rollbackAsync().chain(new C1<IgniteInternalFuture<IgniteInternalTx>, T>() {
                        @Override public T apply(IgniteInternalFuture<IgniteInternalTx> f) {
                            throw ex;
                        }
                    });

                throw ex;
            }

            boolean rollback = true;

            try {
                final T r = postLock(arg);

                // Commit implicit transactions.
                if (commit && commitAfterLock()) {
                    rollback = false;

                    return commitAsync().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, T>() {
                        @Override public T applyx(IgniteInternalFuture<IgniteInternalTx> f) throws IgniteCheckedException {
                            f.get();

                            return r;
                        }
                    });
                }

                rollback = false;

                return new GridFinishedFuture<>(r);
            }
            catch (final IgniteCheckedException ex) {
                if (commit && commitAfterLock())
                    return rollbackAsync().chain(new C1<IgniteInternalFuture<IgniteInternalTx>, T>() {
                        @Override public T apply(IgniteInternalFuture<IgniteInternalTx> f) {
                            throw new GridClosureException(ex);
                        }
                    });

                throw new GridClosureException(ex);
            }
            finally {
                if (rollback)
                    setRollbackOnly();
            }
        }

        /**
         * Post lock callback.
         *
         * @param val Argument.
         * @return Future return value.
         * @throws IgniteCheckedException If operation failed.
         */
        protected abstract T postLock(T val) throws IgniteCheckedException;
    }

    /**
     * Post-lock closure.
     *
     * @param <T> Return type.
     */
    protected abstract class PostLockClosure2<T> implements IgniteBiClosure<Boolean, Exception, IgniteInternalFuture<T>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public final IgniteInternalFuture<T> apply(Boolean locked, @Nullable Exception e) {
            boolean rollback = true;

            try {
                if (e != null)
                    throw new GridClosureException(e);

                if (!locked)
                    throw new GridClosureException(new IgniteTxTimeoutCheckedException("Failed to acquire lock " +
                        "within provided timeout for transaction [timeout=" + timeout() +
                        ", tx=" + CU.txString(IgniteTxLocalAdapter.this) + ']'));

                IgniteInternalFuture<T> fut = postLock();

                rollback = false;

                return fut;
            }
            catch (IgniteCheckedException ex) {
                throw new GridClosureException(ex);
            }
            finally {
                if (rollback)
                    setRollbackOnly();
            }
        }

        /**
         * Post lock callback.
         *
         * @return Future return value.
         * @throws IgniteCheckedException If operation failed.
         */
        protected abstract IgniteInternalFuture<T> postLock() throws IgniteCheckedException;
    }

    /**
     * Post-lock closure.
     *
     * @param <T> Return type.
     */
    protected abstract class PostMissClosure<T> implements IgniteBiClosure<T, Exception, IgniteInternalFuture<T>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public final IgniteInternalFuture<T> apply(T t, Exception e) {
            boolean rollback = true;

            try {
                if (e != null)
                    throw new GridClosureException(e);

                IgniteInternalFuture<T> fut = postMiss(t);

                rollback = false;

                return fut;
            }
            catch (IgniteCheckedException ex) {
                throw new GridClosureException(ex);
            }
            finally {
                if (rollback)
                    setRollbackOnly();
            }
        }

        /**
         * Post lock callback.
         *
         * @param t Post-miss parameter.
         * @return Future return value.
         * @throws IgniteCheckedException If operation failed.
         */
        protected abstract IgniteInternalFuture<T> postMiss(T t) throws IgniteCheckedException;
    }

    /**
     * Clojure to perform operations with near cache entries.
     */
    @FunctionalInterface
    private interface NearEntryUpdateClojure<E extends GridCacheEntryEx> {
        /**
         * Apply clojure to given {@code entry}.
         *
         * @param entry Entry.
         * @throws IgniteCheckedException If operation is failed.
         * @throws GridCacheEntryRemovedException If entry is removed.
         */
        public void apply(E entry) throws IgniteCheckedException, GridCacheEntryRemovedException;
    }
}
