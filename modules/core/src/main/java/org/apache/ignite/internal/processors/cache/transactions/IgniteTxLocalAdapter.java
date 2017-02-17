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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.EntryProcessorResourceInjectorProxy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFilterFailedException;
import org.apache.ignite.internal.processors.cache.GridCacheIndexUpdateException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.RELOAD;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.SER_READ_EMPTY_ENTRY_VER;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.SER_READ_NOT_EMPTY_VER;
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

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
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

    /**
     * @param depEnabled Flag indicating whether deployment is enabled for caches from this transaction or not.
     */
    public void activeCachesDeploymentEnabled(boolean depEnabled) {
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

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> loadMissing(
        final GridCacheContext cacheCtx,
        final AffinityTopologyVersion topVer,
        final boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        boolean skipVals,
        boolean needVer,
        boolean keepBinary,
        final ExpiryPolicy expiryPlc,
        final GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c
    ) {
        assert cacheCtx.isLocal() : cacheCtx.name();

        if (!readThrough || !cacheCtx.readThrough()) {
            for (KeyCacheObject key : keys)
                c.apply(key, null, SER_READ_EMPTY_ENTRY_VER);

            return new GridFinishedFuture<>();
        }

        try {
            IgniteCacheExpiryPolicy expiryPlc0 = optimistic() ?
                accessPolicy(cacheCtx, keys) :
                cacheCtx.cache().expiryPolicy(expiryPlc);

            Map<KeyCacheObject, GridCacheVersion> misses = null;

            for (KeyCacheObject key : keys) {
                while (true) {
                    IgniteTxEntry txEntry = entry(cacheCtx.txKey(key));

                    GridCacheEntryEx entry = txEntry == null ? cacheCtx.cache().entryEx(key) :
                        txEntry.cached();

                    if (entry == null)
                        continue;

                    try {
                        EntryGetResult res = entry.innerGetVersioned(
                            null,
                            this,
                            /*readSwap*/true,
                            /*unmarshal*/true,
                            /*update-metrics*/!skipVals,
                            /*event*/!skipVals,
                            CU.subjectId(this, cctx),
                            null,
                            resolveTaskName(),
                            expiryPlc0,
                            txEntry == null ? keepBinary : txEntry.keepBinary(),
                            null);

                        if (res == null) {
                            if (misses == null)
                                misses = new LinkedHashMap<>();

                            misses.put(key, entry.version());
                        }
                        else
                            c.apply(key, skipVals ? true : res.value(), res.version());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry, will retry: " + key);

                        if (txEntry != null)
                            txEntry.cached(cacheCtx.cache().entryEx(key, topologyVersion()));
                    }
                }
            }

            if (misses != null) {
                final Map<KeyCacheObject, GridCacheVersion> misses0 = misses;

                cacheCtx.store().loadAll(this, misses.keySet(), new CI2<KeyCacheObject, Object>() {
                    @Override public void apply(KeyCacheObject key, Object val) {
                        GridCacheVersion ver = misses0.remove(key);

                        assert ver != null : key;

                        if (val != null) {
                            CacheObject cacheVal = cacheCtx.toCacheObject(val);

                            while (true) {
                                GridCacheEntryEx entry = cacheCtx.cache().entryEx(key, topVer);

                                try {
                                    T2<CacheObject, GridCacheVersion> verVal = entry.versionedValue(cacheVal,
                                        ver,
                                        null,
                                        null,
                                        null);

                                    if (log.isDebugEnabled()) {
                                        log.debug("Set value loaded from store into entry [" +
                                            "oldVer=" + ver +
                                            ", newVer=" + verVal.get2() +
                                            ", entry=" + entry + ']');
                                    }

                                    ver = verVal.get2();

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed entry, (will retry): " + entry);
                                }
                                catch (IgniteCheckedException e) {
                                    // Wrap errors (will be unwrapped).
                                    throw new GridClosureException(e);
                                }
                            }
                        }
                        else
                            ver = SER_READ_EMPTY_ENTRY_VER;

                        c.apply(key, val, ver);
                    }
                });

                for (KeyCacheObject key : misses0.keySet())
                    c.apply(key, null, SER_READ_EMPTY_ENTRY_VER);
            }

            return new GridFinishedFuture<>();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
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
     * @throws IgniteCheckedException If prepare step failed.
     */
    @SuppressWarnings({"CatchGenericClass"})
    public void userPrepare() throws IgniteCheckedException {
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
            cctx.tm().prepareTx(this);
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

    /** {@inheritDoc} */
    @Override public void commit() throws IgniteCheckedException {
        try {
            commitAsync().get();
        }
        finally {
            cctx.tm().resetContext();
        }
    }

    /** {@inheritDoc} */
    @Override public void prepare() throws IgniteCheckedException {
        prepareAsync().get();
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
    @Override public void userCommit() throws IgniteCheckedException {
        TransactionState state = state();

        if (state != COMMITTING) {
            if (remainingTime() == -1)
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for commit [state=" + state + ", tx=" + this + ']');
        }

        checkValid();

        Collection<IgniteTxEntry> commitEntries = near() ? allEntries() : writeEntries();

        boolean empty = F.isEmpty(commitEntries);

        // Register this transaction as completed prior to write-phase to
        // ensure proper lock ordering for removed entries.
        // We add colocated transaction to committed set even if it is empty to correctly order
        // locks on backup nodes.
        if (!empty || colocated())
            cctx.tm().addCommittedTx(this);

        if (!empty) {
            batchStoreCommit(writeMap().values());

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

                                    GridCacheEntryEx nearCached = null;

                                    boolean metrics = true;

                                    if (updateNearCache(cacheCtx, txEntry.key(), topVer))
                                        nearCached = cacheCtx.dht().near().peekEx(txEntry.key());
                                    else if (cacheCtx.isNear() && txEntry.locallyMapped())
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
                                            null);

                                        if (updRes.success())
                                            txEntry.updateCounter(updRes.updatePartitionCounter());

                                        if (nearCached != null && updRes.success()) {
                                            nearCached.innerSet(
                                                null,
                                                eventNodeId(),
                                                nodeId,
                                                val,
                                                false,
                                                false,
                                                txEntry.ttl(),
                                                false,
                                                metrics,
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
                                                dhtVer,
                                                null);
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
                                            cached.detached()  ? DR_NONE : drType,
                                            cached.isNear() ? null : explicitVer,
                                            CU.subjectId(this, cctx),
                                            resolveTaskName(),
                                            dhtVer,
                                            null);

                                        if (updRes.success())
                                            txEntry.updateCounter(updRes.updatePartitionCounter());

                                        if (nearCached != null && updRes.success()) {
                                            nearCached.innerRemove(
                                                null,
                                                eventNodeId(),
                                                nodeId,
                                                false,
                                                false,
                                                metrics,
                                                txEntry.keepBinary(),
                                                txEntry.hasOldValue(),
                                                txEntry.oldValue(),
                                                topVer,
                                                CU.empty0(),
                                                DR_NONE,
                                                null,
                                                CU.subjectId(this, cctx),
                                                resolveTaskName(),
                                                dhtVer,
                                                null);
                                        }
                                    }
                                    else if (op == RELOAD) {
                                        cached.innerReload();

                                        if (nearCached != null)
                                            nearCached.innerReload();
                                    }
                                    else if (op == READ) {
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
                                        assert ownsLock(txEntry.cached()):
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

                        if (X.hasCause(ex, GridCacheIndexUpdateException.class) && cacheCtx.cache().isMongoDataCache()) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to update mongo document index (transaction entry will " +
                                    "be ignored): " + txEntry);

                            // Set operation to NOOP.
                            txEntry.op(NOOP);

                            errorWhenCommitting();

                            throw ex;
                        }
                        else {
                            IgniteCheckedException err = new IgniteTxHeuristicCheckedException("Failed to locally write to cache " +
                                "(all transaction entries will be invalidated, however there was a window when " +
                                "entries for this transaction were visible to others): " + this, ex);

                            U.error(log, "Heuristic transaction failure.", err);

                            COMMIT_ERR_UPD.compareAndSet(this, null, err);

                            state(UNKNOWN);

                            try {
                                // Courtesy to minimize damage.
                                uncommit();
                            }
                            catch (Throwable ex1) {
                                U.error(log, "Failed to uncommit transaction: " + this, ex1);

                                if (ex1 instanceof Error)
                                    throw ex1;
                            }

                            if (ex instanceof Error)
                                throw ex;

                            throw err;
                        }
                    }
                }
            }
            finally {
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
     * Commits transaction to transaction manager. Used for one-phase commit transactions only.
     *
     * @param commit If {@code true} commits transaction, otherwise rollbacks.
     * @throws IgniteCheckedException If failed.
     */
    public void tmFinish(boolean commit) throws IgniteCheckedException {
        assert onePhaseCommit();

        if (DONE_FLAG_UPD.compareAndSet(this, 0, 1)) {
            // Unlock all locks.
            if (commit)
                cctx.tm().commitTx(this);
            else
                cctx.tm().rollbackTx(this);

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
    @Override public void userRollback() throws IgniteCheckedException {
        TransactionState state = state();

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
            cctx.tm().rollbackTx(this);

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
     * @param entry Entry.
     * @return {@code True} if local node is current primary for given entry.
     */
    private boolean primaryLocal(GridCacheEntryEx entry) {
        return entry.context().affinity().primaryByPartition(cctx.localNode(), entry.partition(), AffinityTopologyVersion.NONE);
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Key to enlist.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param map Return map.
     * @param missed Map of missed keys.
     * @param keysCnt Keys count (to avoid call to {@code Collection.size()}).
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param skipStore Skip store flag.
     * @throws IgniteCheckedException If failed.
     * @return Enlisted keys.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private <K, V> Collection<KeyCacheObject> enlistRead(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<KeyCacheObject> keys,
        @Nullable ExpiryPolicy expiryPlc,
        Map<K, V> map,
        Map<KeyCacheObject, GridCacheVersion> missed,
        int keysCnt,
        boolean deserializeBinary,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean skipStore,
        final boolean needVer
    ) throws IgniteCheckedException {
        assert !F.isEmpty(keys);
        assert keysCnt == keys.size();

        cacheCtx.checkSecurity(SecurityPermission.CACHE_READ);

        boolean single = keysCnt == 1;

        Collection<KeyCacheObject> lockKeys = null;

        AffinityTopologyVersion topVer = entryTopVer != null ? entryTopVer : topologyVersion();

        boolean needReadVer = (serializable() && optimistic()) || needVer;

        // In this loop we cover only read-committed or optimistic transactions.
        // Transactions that are pessimistic and not read-committed are covered
        // outside of this loop.
        for (KeyCacheObject key : keys) {
            if ((pessimistic() || needReadVer) && !readCommitted() && !skipVals)
                addActiveCache(cacheCtx);

            IgniteTxKey txKey = cacheCtx.txKey(key);

            // Check write map (always check writes first).
            IgniteTxEntry txEntry = entry(txKey);

            // Either non-read-committed or there was a previous write.
            if (txEntry != null) {
                CacheObject val = txEntry.value();

                if (txEntry.hasValue()) {
                    if (!F.isEmpty(txEntry.entryProcessors()))
                        val = txEntry.applyEntryProcessors(val);

                    if (val != null) {
                        GridCacheVersion ver = null;

                        if (needVer) {
                            if (txEntry.op() != READ)
                                ver = IgniteTxEntry.GET_ENTRY_INVALID_VER_UPDATED;
                            else {
                                ver = txEntry.entryReadVersion();

                                if (ver == null && pessimistic()) {
                                    while (true) {
                                        try {
                                            GridCacheEntryEx cached = txEntry.cached();

                                            ver = cached.isNear() ?
                                                ((GridNearCacheEntry)cached).dhtVersion() : cached.version();

                                            break;
                                        }
                                        catch (GridCacheEntryRemovedException ignored) {
                                            txEntry.cached(entryEx(cacheCtx, txEntry.txKey(), topVer));
                                        }
                                    }
                                }

                                if (ver == null) {
                                    assert optimistic() && repeatableRead() : this;

                                    ver = IgniteTxEntry.GET_ENTRY_INVALID_VER_AFTER_GET;
                                }
                            }

                            assert ver != null;
                        }

                        cacheCtx.addResult(map, key, val, skipVals, keepCacheObjects, deserializeBinary, false, ver);
                    }
                }
                else {
                    assert txEntry.op() == TRANSFORM;

                    while (true) {
                        try {
                            GridCacheVersion readVer = null;

                            Object transformClo =
                                (txEntry.op() == TRANSFORM &&
                                    cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                    F.first(txEntry.entryProcessors()) : null;

                            if (needVer) {
                                EntryGetResult res = txEntry.cached().innerGetVersioned(
                                    null,
                                    this,
                                    /*swap*/true,
                                    /*unmarshal*/true,
                                    /*update-metrics*/true,
                                    /*event*/!skipVals,
                                    CU.subjectId(this, cctx),
                                    transformClo,
                                    resolveTaskName(),
                                    null,
                                    txEntry.keepBinary(),
                                    null);

                                if (res != null) {
                                    val = res.value();
                                    readVer = res.version();
                                }
                            }
                            else {
                                val = txEntry.cached().innerGet(
                                    null,
                                    this,
                                    /*swap*/true,
                                    /*read-through*/false,
                                    /*metrics*/true,
                                    /*event*/!skipVals,
                                    /*temporary*/false,
                                    CU.subjectId(this, cctx),
                                    transformClo,
                                    resolveTaskName(),
                                    null,
                                    txEntry.keepBinary());
                            }

                            if (val != null) {
                                if (!readCommitted() && !skipVals)
                                    txEntry.readValue(val);

                                if (!F.isEmpty(txEntry.entryProcessors()))
                                    val = txEntry.applyEntryProcessors(val);

                                cacheCtx.addResult(map,
                                    key,
                                    val,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    false,
                                    readVer);
                            }
                            else
                                missed.put(key, txEntry.cached().version());

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            txEntry.cached(entryEx(cacheCtx, txEntry.txKey(), topVer));
                        }
                    }
                }
            }
            // First time access within transaction.
            else {
                if (lockKeys == null && !skipVals)
                    lockKeys = single ? Collections.singleton(key) : new ArrayList<KeyCacheObject>(keysCnt);

                if (!single && !skipVals)
                    lockKeys.add(key);

                while (true) {
                    GridCacheEntryEx entry = entryEx(cacheCtx, txKey, topVer);

                    try {
                        GridCacheVersion ver = entry.version();

                        CacheObject val = null;
                        GridCacheVersion readVer = null;

                        if (!pessimistic() || readCommitted() && !skipVals) {
                            IgniteCacheExpiryPolicy accessPlc =
                                optimistic() ? accessPolicy(cacheCtx, txKey, expiryPlc) : null;

                            if (needReadVer) {
                                EntryGetResult res = primaryLocal(entry) ?
                                    entry.innerGetVersioned(
                                        null,
                                        this,
                                        /*swap*/true,
                                        /*unmarshal*/true,
                                        /*metrics*/true,
                                        /*event*/true,
                                        CU.subjectId(this, cctx),
                                        null,
                                        resolveTaskName(),
                                        accessPlc,
                                        !deserializeBinary,
                                        null) : null;

                                if (res != null) {
                                    val = res.value();
                                    readVer = res.version();
                                }
                            }
                            else {
                                val = entry.innerGet(
                                    null,
                                    this,
                                    /*swap*/true,
                                    /*read-through*/false,
                                    /*metrics*/true,
                                    /*event*/true,
                                    /*temporary*/false,
                                    CU.subjectId(this, cctx),
                                    null,
                                    resolveTaskName(),
                                    accessPlc,
                                    !deserializeBinary);
                            }

                            if (val != null) {
                                cacheCtx.addResult(map,
                                    key,
                                    val,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    false,
                                    needVer ? readVer : null);
                            }
                            else
                                missed.put(key, ver);
                        }
                        else
                            // We must wait for the lock in pessimistic mode.
                            missed.put(key, ver);

                        if (!readCommitted() && !skipVals) {
                            txEntry = addEntry(READ,
                                val,
                                null,
                                null,
                                entry,
                                expiryPlc,
                                null,
                                true,
                                -1L,
                                -1L,
                                null,
                                skipStore,
                                !deserializeBinary);

                            // As optimization, mark as checked immediately
                            // for non-pessimistic if value is not null.
                            if (val != null && !pessimistic()) {
                                txEntry.markValid();

                                if (needReadVer) {
                                    assert readVer != null;

                                    txEntry.entryReadVersion(readVer);
                                }
                            }
                        }

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in transaction getAllAsync(..) (will retry): " + key);
                    }
                    finally {
                        if (entry != null && readCommitted()) {
                            if (cacheCtx.isNear()) {
                                if (cacheCtx.affinity().partitionBelongs(cacheCtx.localNode(), entry.partition(), topVer)) {
                                    if (entry.markObsolete(xidVer))
                                        cacheCtx.cache().removeEntry(entry);
                                }
                            }
                            else
                                entry.context().evicts().touch(entry, topVer);
                        }
                    }
                }
            }
        }

        return lockKeys != null ? lockKeys : Collections.<KeyCacheObject>emptyList();
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
     * @param cacheCtx Cache context.
     * @param topVer Topology version.
     * @param map Return map.
     * @param missedMap Missed keys.
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param skipStore Skip store flag.
     * @param expiryPlc Expiry policy.
     * @return Loaded key-value pairs.
     */
    private <K, V> IgniteInternalFuture<Map<K, V>> checkMissed(
        final GridCacheContext cacheCtx,
        final AffinityTopologyVersion topVer,
        final Map<K, V> map,
        final Map<KeyCacheObject, GridCacheVersion> missedMap,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore,
        final boolean needVer,
        final ExpiryPolicy expiryPlc

    ) {
        if (log.isDebugEnabled())
            log.debug("Loading missed values for missed map: " + missedMap);

        final boolean needReadVer = (serializable() && optimistic()) || needVer;

        return new GridEmbeddedFuture<>(
            new C2<Void, Exception, Map<K, V>>() {
                @Override public Map<K, V> apply(Void v, Exception e) {
                    if (e != null) {
                        setRollbackOnly();

                        throw new GridClosureException(e);
                    }

                    return map;
                }
            },
            loadMissing(
                cacheCtx,
                topVer,
                !skipStore,
                false,
                missedMap.keySet(),
                skipVals,
                needReadVer,
                !deserializeBinary,
                expiryPlc,
                new GridInClosure3<KeyCacheObject, Object, GridCacheVersion>() {
                    @Override public void apply(KeyCacheObject key, Object val, GridCacheVersion loadVer) {
                        if (isRollbackOnly()) {
                            if (log.isDebugEnabled())
                                log.debug("Ignoring loaded value for read because transaction was rolled back: " +
                                    IgniteTxLocalAdapter.this);

                            return;
                        }

                        CacheObject cacheVal = cacheCtx.toCacheObject(val);

                        CacheObject visibleVal = cacheVal;

                        IgniteTxKey txKey = cacheCtx.txKey(key);

                        IgniteTxEntry txEntry = entry(txKey);

                        if (txEntry != null) {
                            if (!readCommitted())
                                txEntry.readValue(cacheVal);

                            if (!F.isEmpty(txEntry.entryProcessors()))
                                visibleVal = txEntry.applyEntryProcessors(visibleVal);
                        }

                        assert txEntry != null || readCommitted() || skipVals;

                        GridCacheEntryEx e = txEntry == null ? entryEx(cacheCtx, txKey, topVer) : txEntry.cached();

                        if (readCommitted() || skipVals) {
                            cacheCtx.evicts().touch(e, topologyVersion());

                            if (visibleVal != null) {
                                cacheCtx.addResult(map,
                                    key,
                                    visibleVal,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    false,
                                    needVer ? loadVer : null);
                            }
                        }
                        else {
                            assert txEntry != null;

                            txEntry.setAndMarkValid(cacheVal);

                            if (needReadVer) {
                                assert loadVer != null;

                                txEntry.entryReadVersion(loadVer);
                            }

                            if (visibleVal != null) {
                                cacheCtx.addResult(map,
                                    key,
                                    visibleVal,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    false,
                                    needVer ? loadVer : null);
                            }
                        }
                    }
                })
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> IgniteInternalFuture<Map<K, V>> getAllAsync(
        final GridCacheContext cacheCtx,
        @Nullable final AffinityTopologyVersion entryTopVer,
        Collection<KeyCacheObject> keys,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore,
        final boolean needVer) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        init();

        int keysCnt = keys.size();

        boolean single = keysCnt == 1;

        try {
            checkValid();

            final Map<K, V> retMap = new GridLeanMap<>(keysCnt);

            final Map<KeyCacheObject, GridCacheVersion> missed = new GridLeanMap<>(pessimistic() ? keysCnt : 0);

            CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            ExpiryPolicy expiryPlc = opCtx != null ? opCtx.expiry() : null;

            final Collection<KeyCacheObject> lockKeys = enlistRead(cacheCtx,
                entryTopVer,
                keys,
                expiryPlc,
                retMap,
                missed,
                keysCnt,
                deserializeBinary,
                skipVals,
                keepCacheObjects,
                skipStore,
                needVer);

            if (single && missed.isEmpty())
                return new GridFinishedFuture<>(retMap);

            // Handle locks.
            if (pessimistic() && !readCommitted() && !skipVals) {
                if (expiryPlc == null)
                    expiryPlc = cacheCtx.expiry();

                long accessTtl = expiryPlc != null ? CU.toTtl(expiryPlc.getExpiryForAccess()) : CU.TTL_NOT_CHANGED;
                long createTtl = expiryPlc != null ? CU.toTtl(expiryPlc.getExpiryForCreation()) : CU.TTL_NOT_CHANGED;

                long timeout = remainingTime();

                if (timeout == -1)
                    return new GridFinishedFuture<>(timeoutException());

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(lockKeys,
                    timeout,
                    this,
                    true,
                    true,
                    isolation,
                    isInvalidate(),
                    createTtl,
                    accessTtl);

                final ExpiryPolicy expiryPlc0 = expiryPlc;

                PLC2<Map<K, V>> plc2 = new PLC2<Map<K, V>>() {
                    @Override public IgniteInternalFuture<Map<K, V>> postLock() throws IgniteCheckedException {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for read on keys: " + lockKeys);

                        // Load keys only after the locks have been acquired.
                        for (KeyCacheObject cacheKey : lockKeys) {
                            K keyVal = (K)
                                (keepCacheObjects ? cacheKey :
                                cacheCtx.cacheObjectContext().unwrapBinaryIfNeeded(cacheKey, !deserializeBinary));

                            if (retMap.containsKey(keyVal))
                                // We already have a return value.
                                continue;

                            IgniteTxKey txKey = cacheCtx.txKey(cacheKey);

                            IgniteTxEntry txEntry = entry(txKey);

                            assert txEntry != null;

                            // Check if there is cached value.
                            while (true) {
                                GridCacheEntryEx cached = txEntry.cached();

                                CacheObject val = null;
                                GridCacheVersion readVer = null;

                                try {
                                    Object transformClo =
                                        (!F.isEmpty(txEntry.entryProcessors()) &&
                                            cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                            F.first(txEntry.entryProcessors()) : null;

                                    if (needVer) {
                                        EntryGetResult res = cached.innerGetVersioned(
                                            null,
                                            IgniteTxLocalAdapter.this,
                                            /*swap*/cacheCtx.isSwapOrOffheapEnabled(),
                                            /*unmarshal*/true,
                                            /*update-metrics*/true,
                                            /*event*/!skipVals,
                                            CU.subjectId(IgniteTxLocalAdapter.this, cctx),
                                            transformClo,
                                            resolveTaskName(),
                                            null,
                                            txEntry.keepBinary(),
                                            null);

                                        if (res != null) {
                                            val = res.value();
                                            readVer = res.version();
                                        }
                                    }
                                    else{
                                        val = cached.innerGet(
                                            null,
                                            IgniteTxLocalAdapter.this,
                                            cacheCtx.isSwapOrOffheapEnabled(),
                                            /*read-through*/false,
                                            /*metrics*/true,
                                            /*events*/!skipVals,
                                            /*temporary*/false,
                                            CU.subjectId(IgniteTxLocalAdapter.this, cctx),
                                            transformClo,
                                            resolveTaskName(),
                                            null,
                                            txEntry.keepBinary());
                                    }

                                    // If value is in cache and passed the filter.
                                    if (val != null) {
                                        missed.remove(cacheKey);

                                        txEntry.setAndMarkValid(val);

                                        if (!F.isEmpty(txEntry.entryProcessors()))
                                            val = txEntry.applyEntryProcessors(val);

                                        cacheCtx.addResult(retMap,
                                            cacheKey,
                                            val,
                                            skipVals,
                                            keepCacheObjects,
                                            deserializeBinary,
                                            false,
                                            readVer);

                                        if (readVer != null)
                                            txEntry.entryReadVersion(readVer);
                                    }

                                    // Even though we bring the value back from lock acquisition,
                                    // we still need to recheck primary node for consistent values
                                    // in case of concurrent transactional locks.

                                    break; // While.
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed exception in get postLock (will retry): " +
                                            cached);

                                    txEntry.cached(entryEx(cacheCtx, txKey, topologyVersion()));
                                }
                            }
                        }

                        if (!missed.isEmpty() && cacheCtx.isLocal()) {
                            AffinityTopologyVersion topVer = topologyVersionSnapshot();

                            if (topVer == null)
                                topVer = entryTopVer;

                            return checkMissed(cacheCtx,
                                topVer != null ? topVer : topologyVersion(),
                                retMap,
                                missed,
                                deserializeBinary,
                                skipVals,
                                keepCacheObjects,
                                skipStore,
                                needVer,
                                expiryPlc0);
                        }

                        return new GridFinishedFuture<>(Collections.<K, V>emptyMap());
                    }
                };

                FinishClosure<Map<K, V>> finClos = new FinishClosure<Map<K, V>>() {
                    @Override Map<K, V> finish(Map<K, V> loaded) {
                        retMap.putAll(loaded);

                        return retMap;
                    }
                };

                if (fut.isDone()) {
                    try {
                        IgniteInternalFuture<Map<K, V>> fut1 = plc2.apply(fut.get(), null);

                        return fut1.isDone() ?
                            new GridFinishedFuture<>(finClos.apply(fut1.get(), null)) :
                            new GridEmbeddedFuture<>(finClos, fut1);
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return plc2.apply(false, e);
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return new GridEmbeddedFuture<>(
                        fut,
                        plc2,
                        finClos);
                }
            }
            else {
                assert optimistic() || readCommitted() || skipVals;

                if (!missed.isEmpty()) {
                    if (!readCommitted())
                        for (Iterator<KeyCacheObject> it = missed.keySet().iterator(); it.hasNext(); ) {
                            KeyCacheObject cacheKey = it.next();

                            K keyVal =
                                (K)(keepCacheObjects ? cacheKey : cacheKey.value(cacheCtx.cacheObjectContext(), false));

                            if (retMap.containsKey(keyVal))
                                it.remove();
                        }

                    if (missed.isEmpty())
                        return new GridFinishedFuture<>(retMap);

                    AffinityTopologyVersion topVer = topologyVersionSnapshot();

                    if (topVer == null)
                        topVer = entryTopVer;

                    return checkMissed(cacheCtx,
                        topVer != null ? topVer : topologyVersion(),
                        retMap,
                        missed,
                        deserializeBinary,
                        skipVals,
                        keepCacheObjects,
                        skipStore,
                        needVer,
                        expiryPlc);
                }

                return new GridFinishedFuture<>(retMap);
            }
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> IgniteInternalFuture<GridCacheReturn> putAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Map<? extends K, ? extends V> map,
        boolean retval
    ) {
        return (IgniteInternalFuture<GridCacheReturn>)putAllAsync0(cacheCtx,
            entryTopVer,
            map,
            null,
            null,
            null,
            retval);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalFuture<GridCacheReturn> putAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        V val,
        boolean retval,
        CacheEntryPredicate filter) {
        return putAsync0(cacheCtx,
            entryTopVer,
            key,
            val,
            null,
            null,
            retval,
            filter);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalFuture<GridCacheReturn> invokeAsync(GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        EntryProcessor<K, V, Object> entryProcessor,
        Object... invokeArgs) {
        return (IgniteInternalFuture)putAsync0(cacheCtx,
            entryTopVer,
            key,
            null,
            entryProcessor,
            invokeArgs,
            true,
            null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheDrInfo> drMap
    ) {
        Map<KeyCacheObject, Object> map = F.viewReadOnly(drMap, new IgniteClosure<GridCacheDrInfo, Object>() {
            @Override public Object apply(GridCacheDrInfo val) {
                return val.value();
            }
        });

        return this.<Object, Object>putAllAsync0(cacheCtx,
            null,
            map,
            null,
            null,
            drMap,
            false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V, T> IgniteInternalFuture<GridCacheReturn> invokeAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> map,
        Object... invokeArgs
    ) {
        return (IgniteInternalFuture<GridCacheReturn>)putAllAsync0(cacheCtx,
            entryTopVer,
            null,
            map,
            invokeArgs,
            null,
            true);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheVersion> drMap
    ) {
        return removeAllAsync0(cacheCtx, null, null, drMap, false, null, false);
    }

    /**
     * Checks filter for non-pessimistic transactions.
     *
     * @param cctx Cache context.
     * @param key Key.
     * @param val Value.
     * @param filter Filter to check.
     * @return {@code True} if passed or pessimistic.
     */
    private boolean filter(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        CacheEntryPredicate[] filter) {
        return pessimistic() || (optimistic() && implicit()) || isAll(cctx, key, val, filter);
    }

    /**
     * @param cacheCtx Cache context.
     * @param cacheKey Key to enlist.
     * @param val Value.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param entryProcessor Entry processor (for invoke operation).
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Flag indicating whether a value should be returned.
     * @param lockOnly If {@code true}, then entry will be enlisted as noop.
     * @param filter User filters.
     * @param ret Return value.
     * @param skipStore Skip store flag.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @return Future for entry values loading.
     */
    private <K, V> IgniteInternalFuture<Void> enlistWrite(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        KeyCacheObject cacheKey,
        Object val,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable EntryProcessor<K, V, Object> entryProcessor,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        boolean lockOnly,
        final CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        boolean skipStore,
        final boolean singleRmv,
        boolean keepBinary,
        Byte dataCenterId) {
        try {
            addActiveCache(cacheCtx);

            final boolean hasFilters = !F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter);
            final boolean needVal = singleRmv || retval || hasFilters;
            final boolean needReadVer = needVal && (serializable() && optimistic());

            if (entryProcessor != null)
                transform = true;

            GridCacheVersion drVer = dataCenterId != null ? cctx.versions().next(dataCenterId) : null;

            boolean loadMissed = enlistWriteEntry(cacheCtx,
                entryTopVer,
                cacheKey,
                val,
                entryProcessor,
                invokeArgs,
                expiryPlc,
                retval,
                lockOnly,
                filter,
                /*drVer*/drVer,
                /*drTtl*/-1L,
                /*drExpireTime*/-1L,
                ret,
                /*enlisted*/null,
                skipStore,
                singleRmv,
                hasFilters,
                needVal,
                needReadVer,
                keepBinary);

            if (loadMissed) {
                AffinityTopologyVersion topVer = topologyVersionSnapshot();

                if (topVer == null)
                    topVer = entryTopVer;

                return loadMissing(cacheCtx,
                    topVer != null ? topVer : topologyVersion(),
                    Collections.singleton(cacheKey),
                    filter,
                    ret,
                    needReadVer,
                    singleRmv,
                    hasFilters,
                    /*read through*/(entryProcessor != null || cacheCtx.config().isLoadPreviousValue()) && !skipStore,
                    retval,
                    keepBinary,
                    expiryPlc);
            }

            return new GridFinishedFuture<>();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Internal routine for <tt>putAll(..)</tt>
     *
     * @param cacheCtx Cache context.
     * @param keys Keys to enlist.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param lookup Value lookup map ({@code null} for remove).
     * @param invokeMap Map with entry processors for invoke operation.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Flag indicating whether a value should be returned.
     * @param lockOnly If {@code true}, then entry will be enlisted as noop.
     * @param filter User filters.
     * @param ret Return value.
     * @param enlisted Collection of keys enlisted into this transaction.
     * @param drPutMap DR put map (optional).
     * @param drRmvMap DR remove map (optional).
     * @param skipStore Skip store flag.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @param keepBinary Keep binary flag.
     * @param dataCenterId Optional data center ID.
     * @return Future for missing values loading.
     */
    private <K, V> IgniteInternalFuture<Void> enlistWrite(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<?> keys,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Map<?, ?> lookup,
        @Nullable Map<?, EntryProcessor<K, V, Object>> invokeMap,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        boolean lockOnly,
        final CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        Collection<KeyCacheObject> enlisted,
        @Nullable Map<KeyCacheObject, GridCacheDrInfo> drPutMap,
        @Nullable Map<KeyCacheObject, GridCacheVersion> drRmvMap,
        boolean skipStore,
        final boolean singleRmv,
        final boolean keepBinary,
        Byte dataCenterId
    ) {
        assert retval || invokeMap == null;

        try {
            addActiveCache(cacheCtx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        boolean rmv = lookup == null && invokeMap == null;

        final boolean hasFilters = !F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter);
        final boolean needVal = singleRmv || retval || hasFilters;
        final boolean needReadVer = needVal && (serializable() && optimistic());

        try {
            // Set transform flag for transaction.
            if (invokeMap != null)
                transform = true;

            Set<KeyCacheObject> missedForLoad = null;

            for (Object key : keys) {
                if (key == null) {
                    rollback();

                    throw new NullPointerException("Null key.");
                }

                Object val = rmv || lookup == null ? null : lookup.get(key);
                EntryProcessor entryProcessor = invokeMap == null ? null : invokeMap.get(key);

                GridCacheVersion drVer;
                long drTtl;
                long drExpireTime;

                if (drPutMap != null) {
                    GridCacheDrInfo info = drPutMap.get(key);

                    assert info != null;

                    drVer = info.version();
                    drTtl = info.ttl();
                    drExpireTime = info.expireTime();
                }
                else if (drRmvMap != null) {
                    assert drRmvMap.get(key) != null;

                    drVer = drRmvMap.get(key);
                    drTtl = -1L;
                    drExpireTime = -1L;
                }
                else if (dataCenterId != null) {
                    drVer = cctx.versions().next(dataCenterId);
                    drTtl = -1L;
                    drExpireTime = -1L;
                }
                else {
                    drVer = null;
                    drTtl = -1L;
                    drExpireTime = -1L;
                }

                if (!rmv && val == null && entryProcessor == null) {
                    setRollbackOnly();

                    throw new NullPointerException("Null value.");
                }

                KeyCacheObject cacheKey = cacheCtx.toCacheKeyObject(key);

                boolean loadMissed = enlistWriteEntry(cacheCtx,
                    entryTopVer,
                    cacheKey,
                    val,
                    entryProcessor,
                    invokeArgs,
                    expiryPlc,
                    retval,
                    lockOnly,
                    filter,
                    drVer,
                    drTtl,
                    drExpireTime,
                    ret,
                    enlisted,
                    skipStore,
                    singleRmv,
                    hasFilters,
                    needVal,
                    needReadVer,
                    keepBinary);

                if (loadMissed) {
                    if (missedForLoad == null)
                        missedForLoad = new HashSet<>();

                    missedForLoad.add(cacheKey);
                }
            }

            if (missedForLoad != null) {
                AffinityTopologyVersion topVer = topologyVersionSnapshot();

                if (topVer == null)
                    topVer = entryTopVer;

                return loadMissing(cacheCtx,
                    topVer != null ? topVer : topologyVersion(),
                    missedForLoad,
                    filter,
                    ret,
                    needReadVer,
                    singleRmv,
                    hasFilters,
                    /*read through*/(invokeMap != null || cacheCtx.config().isLoadPreviousValue()) && !skipStore,
                    retval,
                    keepBinary,
                    expiryPlc);
            }

            return new GridFinishedFuture<>();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to load.
     * @param filter Filter.
     * @param ret Return value.
     * @param needReadVer Read version flag.
     * @param singleRmv {@code True} for single remove operation.
     * @param hasFilters {@code True} if filters not empty.
     * @param readThrough Read through flag.
     * @param retval Return value flag.
     * @param expiryPlc Expiry policy.
     * @return Load future.
     */
    private IgniteInternalFuture<Void> loadMissing(
        final GridCacheContext cacheCtx,
        final AffinityTopologyVersion topVer,
        final Set<KeyCacheObject> keys,
        final CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        final boolean needReadVer,
        final boolean singleRmv,
        final boolean hasFilters,
        final boolean readThrough,
        final boolean retval,
        final boolean keepBinary,
        final ExpiryPolicy expiryPlc) {
        GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c =
            new GridInClosure3<KeyCacheObject, Object, GridCacheVersion>() {
                @Override public void apply(KeyCacheObject key,
                    @Nullable Object val,
                    @Nullable GridCacheVersion loadVer) {
                    if (log.isDebugEnabled())
                        log.debug("Loaded value from remote node [key=" + key + ", val=" + val + ']');

                    IgniteTxEntry e = entry(new IgniteTxKey(key, cacheCtx.cacheId()));

                    assert e != null;

                    if (needReadVer) {
                        assert loadVer != null;

                        e.entryReadVersion(singleRmv && val != null ? SER_READ_NOT_EMPTY_VER : loadVer);
                    }

                    if (singleRmv) {
                        assert !hasFilters && !retval;
                        assert val == null || Boolean.TRUE.equals(val) : val;

                        ret.set(cacheCtx, null, val != null, keepBinary);
                    }
                    else {
                        CacheObject cacheVal = cacheCtx.toCacheObject(val);

                        if (e.op() == TRANSFORM) {
                            GridCacheVersion ver;

                            e.readValue(cacheVal);

                            try {
                                ver = e.cached().version();
                            }
                            catch (GridCacheEntryRemovedException ex) {
                                assert optimistic() : e;

                                if (log.isDebugEnabled())
                                    log.debug("Failed to get entry version: [msg=" + ex.getMessage() + ']');

                                ver = null;
                            }

                            addInvokeResult(e, cacheVal, ret, ver);
                        }
                        else {
                            boolean success;

                            if (hasFilters) {
                                success = isAll(e.context(), key, cacheVal, filter);

                                if (!success)
                                    e.value(cacheVal, false, false);
                            }
                            else
                                success = true;

                            ret.set(cacheCtx, cacheVal, success, keepBinary);
                        }
                    }
                }
            };

        return loadMissing(
            cacheCtx,
            topVer,
            readThrough,
            /*async*/true,
            keys,
            /*skipVals*/singleRmv,
            needReadVer,
            keepBinary,
            expiryPlc,
            c);
    }

    /**
     * @param cacheCtx Cache context.
     * @param cacheKey Key.
     * @param val Value.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param retval Return value flag.
     * @param lockOnly Lock only flag.
     * @param filter Filter.
     * @param drVer DR version.
     * @param drTtl DR ttl.
     * @param drExpireTime DR expire time.
     * @param ret Return value.
     * @param enlisted Enlisted keys collection.
     * @param skipStore Skip store flag.
     * @param singleRmv {@code True} for single remove operation.
     * @param hasFilters {@code True} if filters not empty.
     * @param needVal {@code True} if value is needed.
     * @param needReadVer {@code True} if need read entry version.
     * @return {@code True} if entry value should be loaded.
     * @throws IgniteCheckedException If failed.
     */
    private boolean enlistWriteEntry(GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        final KeyCacheObject cacheKey,
        @Nullable final Object val,
        @Nullable final EntryProcessor<?, ?, ?> entryProcessor,
        @Nullable final Object[] invokeArgs,
        @Nullable final ExpiryPolicy expiryPlc,
        final boolean retval,
        final boolean lockOnly,
        final CacheEntryPredicate[] filter,
        final GridCacheVersion drVer,
        final long drTtl,
        long drExpireTime,
        final GridCacheReturn ret,
        @Nullable final Collection<KeyCacheObject> enlisted,
        boolean skipStore,
        boolean singleRmv,
        boolean hasFilters,
        final boolean needVal,
        boolean needReadVer,
        boolean keepBinary
    ) throws IgniteCheckedException {
        boolean loadMissed = false;

        final boolean rmv = val == null && entryProcessor == null;

        IgniteTxKey txKey = cacheCtx.txKey(cacheKey);

        IgniteTxEntry txEntry = entry(txKey);

        // First time access.
        if (txEntry == null) {
            while (true) {
                GridCacheEntryEx entry = entryEx(cacheCtx, txKey, entryTopVer != null ? entryTopVer : topologyVersion());

                try {
                    entry.unswap(false);

                    // Check if lock is being explicitly acquired by the same thread.
                    if (!implicit && cctx.kernalContext().config().isCacheSanityCheckEnabled() &&
                        entry.lockedByThread(threadId, xidVer)) {
                        throw new IgniteCheckedException("Cannot access key within transaction if lock is " +
                            "externally held [key=" + CU.value(cacheKey, cacheCtx, false) +
                            ", entry=" + entry +
                            ", xidVer=" + xidVer +
                            ", threadId=" + threadId +
                            ", locNodeId=" + cctx.localNodeId() + ']');
                    }

                    CacheObject old = null;
                    GridCacheVersion readVer = null;

                    if (optimistic() && !implicit()) {
                        try {
                            if (needReadVer) {
                                EntryGetResult res = primaryLocal(entry) ?
                                    entry.innerGetVersioned(
                                        null,
                                        this,
                                        /*swap*/false,
                                        /*unmarshal*/retval || needVal,
                                        /*metrics*/retval,
                                        /*events*/retval,
                                        CU.subjectId(this, cctx),
                                        entryProcessor,
                                        resolveTaskName(),
                                        null,
                                        keepBinary,
                                        null) : null;

                                if (res != null) {
                                    old = res.value();
                                    readVer = res.version();
                                }
                            }
                            else {
                                old = entry.innerGet(
                                    null,
                                    this,
                                    /*swap*/false,
                                    /*read-through*/false,
                                    /*metrics*/retval,
                                    /*events*/retval,
                                    /*temporary*/false,
                                    CU.subjectId(this, cctx),
                                    entryProcessor,
                                    resolveTaskName(),
                                    null,
                                    keepBinary);
                            }
                        }
                        catch (ClusterTopologyCheckedException e) {
                            entry.context().evicts().touch(entry, topologyVersion());

                            throw e;
                        }
                    }
                    else
                        old = retval ? entry.rawGetOrUnmarshal(false) : entry.rawGet();

                    final GridCacheOperation op = lockOnly ? NOOP : rmv ? DELETE :
                        entryProcessor != null ? TRANSFORM : old != null ? UPDATE : CREATE;

                    if (old != null && hasFilters && !filter(entry.context(), cacheKey, old, filter)) {
                        ret.set(cacheCtx, old, false, keepBinary);

                        if (!readCommitted()) {
                            if (optimistic() && serializable()) {
                                txEntry = addEntry(op,
                                    old,
                                    entryProcessor,
                                    invokeArgs,
                                    entry,
                                    expiryPlc,
                                    filter,
                                    true,
                                    drTtl,
                                    drExpireTime,
                                    drVer,
                                    skipStore,
                                    keepBinary);
                            }
                            else {
                                txEntry = addEntry(READ,
                                    old,
                                    null,
                                    null,
                                    entry,
                                    null,
                                    CU.empty0(),
                                    false,
                                    -1L,
                                    -1L,
                                    null,
                                    skipStore,
                                    keepBinary);
                            }

                            txEntry.markValid();

                            if (needReadVer) {
                                assert readVer != null;

                                txEntry.entryReadVersion(singleRmv ? SER_READ_NOT_EMPTY_VER : readVer);
                            }
                        }

                        if (readCommitted())
                            cacheCtx.evicts().touch(entry, topologyVersion());

                        break; // While.
                    }

                    txEntry = addEntry(op,
                        cacheCtx.toCacheObject(val),
                        entryProcessor,
                        invokeArgs,
                        entry,
                        expiryPlc,
                        filter,
                        true,
                        drTtl,
                        drExpireTime,
                        drVer,
                        skipStore,
                        keepBinary);

                    if (!implicit() && readCommitted() && !cacheCtx.offheapTiered())
                        cacheCtx.evicts().touch(entry, topologyVersion());

                    if (enlisted != null)
                        enlisted.add(cacheKey);

                    if (!pessimistic() && !implicit()) {
                        txEntry.markValid();

                        if (old == null) {
                            if (needVal)
                                loadMissed = true;
                            else {
                                assert !implicit() || !transform : this;
                                assert txEntry.op() != TRANSFORM : txEntry;

                                if (retval)
                                    ret.set(cacheCtx, null, true, keepBinary);
                                else
                                    ret.success(true);
                            }
                        }
                        else {
                            if (needReadVer) {
                                assert readVer != null;

                                txEntry.entryReadVersion(singleRmv ? SER_READ_NOT_EMPTY_VER : readVer);
                            }

                            if (retval && !transform)
                                ret.set(cacheCtx, old, true, keepBinary);
                            else {
                                if (txEntry.op() == TRANSFORM) {
                                    GridCacheVersion ver;

                                    try {
                                        ver = entry.version();
                                    }
                                    catch (GridCacheEntryRemovedException ex) {
                                        assert optimistic() : txEntry;

                                        if (log.isDebugEnabled())
                                            log.debug("Failed to get entry version " +
                                                "[err=" + ex.getMessage() + ']');

                                        ver = null;
                                    }

                                    addInvokeResult(txEntry, old, ret, ver);
                                }
                                else
                                    ret.success(true);
                            }
                        }
                    }
                    // Pessimistic.
                    else {
                        if (retval && !transform)
                            ret.set(cacheCtx, old, true, keepBinary);
                        else
                            ret.success(true);
                    }

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in transaction putAll0 method: " + entry);
                }
            }
        }
        else {
            if (entryProcessor == null && txEntry.op() == TRANSFORM)
                throw new IgniteCheckedException("Failed to enlist write value for key (cannot have update value in " +
                    "transaction after EntryProcessor is applied): " + CU.value(cacheKey, cacheCtx, false));

            GridCacheEntryEx entry = txEntry.cached();

            CacheObject v = txEntry.value();

            boolean del = txEntry.op() == DELETE && rmv;

            if (!del) {
                if (hasFilters && !filter(entry.context(), cacheKey, v, filter)) {
                    ret.set(cacheCtx, v, false, keepBinary);

                    return loadMissed;
                }

                GridCacheOperation op = rmv ? DELETE : entryProcessor != null ? TRANSFORM :
                    v != null ? UPDATE : CREATE;

                txEntry = addEntry(op,
                    cacheCtx.toCacheObject(val),
                    entryProcessor,
                    invokeArgs,
                    entry,
                    expiryPlc,
                    filter,
                    true,
                    drTtl,
                    drExpireTime,
                    drVer,
                    skipStore,
                    keepBinary);

                if (enlisted != null)
                    enlisted.add(cacheKey);

                if (txEntry.op() == TRANSFORM) {
                    GridCacheVersion ver;

                    try {
                        ver = entry.version();
                    }
                    catch (GridCacheEntryRemovedException e) {
                        assert optimistic() : txEntry;

                        if (log.isDebugEnabled())
                            log.debug("Failed to get entry version: [msg=" + e.getMessage() + ']');

                        ver = null;
                    }

                    addInvokeResult(txEntry, txEntry.value(), ret, ver);
                }
            }

            if (!pessimistic()) {
                txEntry.markValid();

                if (retval && !transform)
                    ret.set(cacheCtx, v, true, keepBinary);
                else
                    ret.success(true);
            }
        }

        return loadMissed;
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return {@code True} if filter passed.
     */
    private boolean isAll(GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        CacheEntryPredicate[] filter) {
        GridCacheEntryEx e = new GridDhtDetachedCacheEntry(cctx, key, 0, val, null, 0) {
            @Nullable @Override public CacheObject peekVisibleValue() {
                return rawGet();
            }
        };

        for (CacheEntryPredicate p0 : filter) {
            if (p0 != null && !p0.apply(e))
                return false;
        }

        return true;
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
     * @throws IgniteCheckedException If error.
     * @param computeInvoke If {@code true} computes return value for invoke operation.
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
                                    /*swap*/true,
                                    readThrough,
                                    /*metrics*/!invoke,
                                    /*event*/!invoke && !dht(),
                                    /*temporary*/false,
                                    CU.subjectId(this, cctx),
                                    null,
                                    resolveTaskName(),
                                    null,
                                    txEntry.keepBinary());
                            }
                        }
                        else {
                            if (!hasPrevVal)
                                v = cached.rawGetOrUnmarshal(false);
                        }

                        if (txEntry.op() == TRANSFORM) {
                            if (computeInvoke) {
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
    private void addInvokeResult(IgniteTxEntry txEntry, CacheObject cacheVal, GridCacheReturn ret,
        GridCacheVersion ver) {
        GridCacheContext ctx = txEntry.context();

        Object key0 = null;
        Object val0 = null;

        try {
            Object res = null;

            for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : txEntry.entryProcessors()) {
                CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry<>(txEntry.key(), key0, cacheVal,
                    val0, ver, txEntry.keepBinary(), txEntry.cached());

                EntryProcessor<Object, Object, ?> entryProcessor = t.get1();

                res = entryProcessor.process(invokeEntry, t.get2());

                val0 = invokeEntry.value();

                key0 = invokeEntry.key();
            }

            if (res != null)
                ret.addEntryProcessResult(ctx, txEntry.key(), key0, res, null, txEntry.keepBinary());
        }
        catch (Exception e) {
            ret.addEntryProcessResult(ctx, txEntry.key(), key0, null, e, txEntry.keepBinary());
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @throws IgniteCheckedException If updates are not allowed.
     */
    private void checkUpdatesAllowed(GridCacheContext cacheCtx) throws IgniteCheckedException {
        if (!cacheCtx.updatesAllowed()) {
            throw new IgniteTxRollbackCheckedException(new CacheException(
                "Updates are not allowed for transactional cache: " + cacheCtx.name() + ". Configure " +
                "persistence store on client or use remote closure execution to start transactions " +
                "from server nodes."));
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param retval Return value flag.
     * @throws IgniteCheckedException If failed.
     */
    private void beforePut(GridCacheContext cacheCtx, boolean retval) throws IgniteCheckedException {
        checkUpdatesAllowed(cacheCtx);

        cacheCtx.checkSecurity(SecurityPermission.CACHE_PUT);

        if (retval)
            needReturnValue(true);

        checkValid();

        init();
    }

    /**
     * Internal method for single update operation.
     *
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param val Value.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Return value flag.
     * @param filter Filter.
     * @return Operation future.
     */
    private <K, V> IgniteInternalFuture putAsync0(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        @Nullable V val,
        @Nullable EntryProcessor<K, V, Object> entryProcessor,
        @Nullable final Object[] invokeArgs,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter
    ) {
        assert key != null;

        try {
            beforePut(cacheCtx, retval);

            final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

            CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            final Byte dataCenterId = opCtx != null ? opCtx.dataCenterId() : null;

            KeyCacheObject cacheKey = cacheCtx.toCacheKeyObject(key);

            boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            final CacheEntryPredicate[] filters = CU.filterArray(filter);

            final IgniteInternalFuture<Void> loadFut = enlistWrite(
                cacheCtx,
                entryTopVer,
                cacheKey,
                val,
                opCtx != null ? opCtx.expiry() : null,
                entryProcessor,
                invokeArgs,
                retval,
                /*lockOnly*/false,
                filters,
                ret,
                opCtx != null && opCtx.skipStore(),
                /*singleRmv*/false,
                keepBinary,
                dataCenterId);

            if (pessimistic()) {
                assert loadFut == null || loadFut.isDone() : loadFut;

                if (loadFut != null)
                    loadFut.get();

                final Collection<KeyCacheObject> enlisted = Collections.singleton(cacheKey);

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for put on key: " + enlisted);

                long timeout = remainingTime();

                if (timeout == -1)
                    return new GridFinishedFuture<>(timeoutException());

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(enlisted,
                    timeout,
                    this,
                    /*read*/entryProcessor != null, // Needed to force load from store.
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override public GridCacheReturn postLock(GridCacheReturn ret)
                        throws IgniteCheckedException
                    {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for put on keys: " + enlisted);

                        postLockWrite(cacheCtx,
                            enlisted,
                            ret,
                            /*remove*/false,
                            retval,
                            /*read*/false,
                            -1L,
                            filters,
                            /*computeInvoke*/true);

                        return ret;
                    }
                };

                if (fut.isDone()) {
                    try {
                        return nonInterruptable(plc1.apply(fut.get(), null));
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return nonInterruptable(plc1.apply(false, e));
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return nonInterruptable(new GridEmbeddedFuture<>(
                        fut,
                        plc1
                    ));
                }
            }
            else
                return optimisticPutFuture(cacheCtx, loadFut, ret, keepBinary);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * Internal method for all put and transform operations. Only one of {@code map}, {@code transformMap}
     * maps must be non-null.
     *
     * @param cacheCtx Context.
     * @param map Key-value map to store.
     * @param invokeMap Invoke map.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param drMap DR map.
     * @param retval Key-transform value map to store.
     * @return Operation future.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture putAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        @Nullable Map<? extends K, ? extends V> map,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> invokeMap,
        @Nullable final Object[] invokeArgs,
        @Nullable Map<KeyCacheObject, GridCacheDrInfo> drMap,
        final boolean retval
    ) {
        try {
            beforePut(cacheCtx, retval);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        final CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

        final Byte dataCenterId;

        if (opCtx != null && opCtx.hasDataCenterId()) {
            assert drMap == null : drMap;
            assert map != null || invokeMap != null;

            dataCenterId = opCtx.dataCenterId();
        }
        else
            dataCenterId = null;

        // Cached entry may be passed only from entry wrapper.
        final Map<?, ?> map0 = map;
        final Map<?, EntryProcessor<K, V, Object>> invokeMap0 = (Map<K, EntryProcessor<K, V, Object>>)invokeMap;

        if (log.isDebugEnabled())
            log.debug("Called putAllAsync(...) [tx=" + this + ", map=" + map0 + ", retval=" + retval + "]");

        assert map0 != null || invokeMap0 != null;

        final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

        if (F.isEmpty(map0) && F.isEmpty(invokeMap0)) {
            if (implicit())
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }

            return new GridFinishedFuture<>(ret.success(true));
        }

        try {
            Set<?> keySet = map0 != null ? map0.keySet() : invokeMap0.keySet();

            final Collection<KeyCacheObject> enlisted = new ArrayList<>(keySet.size());

            final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            final IgniteInternalFuture<Void> loadFut = enlistWrite(
                cacheCtx,
                entryTopVer,
                keySet,
                opCtx != null ? opCtx.expiry() : null,
                map0,
                invokeMap0,
                invokeArgs,
                retval,
                false,
                CU.filterArray(null),
                ret,
                enlisted,
                drMap,
                null,
                opCtx != null && opCtx.skipStore(),
                false,
                keepBinary,
                dataCenterId);

            if (pessimistic()) {
                assert loadFut == null || loadFut.isDone() : loadFut;

                if (loadFut != null) {
                    try {
                        loadFut.get();
                    }
                    catch (IgniteCheckedException e) {
                        return new GridFinishedFuture(e);
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for put on keys: " + enlisted);

                long timeout = remainingTime();

                if (timeout == -1)
                    return new GridFinishedFuture<>(timeoutException());

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(enlisted,
                    timeout,
                    this,
                    /*read*/invokeMap != null, // Needed to force load from store.
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override public GridCacheReturn postLock(GridCacheReturn ret)
                        throws IgniteCheckedException
                    {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for put on keys: " + enlisted);

                        postLockWrite(cacheCtx,
                            enlisted,
                            ret,
                            /*remove*/false,
                            retval,
                            /*read*/false,
                            -1L,
                            CU.filterArray(null),
                            /*computeInvoke*/true);

                        return ret;
                    }
                };

                if (fut.isDone()) {
                    try {
                        return nonInterruptable(plc1.apply(fut.get(), null));
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return nonInterruptable(plc1.apply(false, e));
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return nonInterruptable(new GridEmbeddedFuture<>(
                        fut,
                        plc1
                    ));
                }
            }
            else
                return optimisticPutFuture(cacheCtx, loadFut, ret, keepBinary);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param loadFut Missing keys load future.
     * @param ret Future result.
     * @param keepBinary Keep binary flag.
     * @return Future.
     */
    private IgniteInternalFuture optimisticPutFuture(
        final GridCacheContext cacheCtx,
        IgniteInternalFuture<Void> loadFut,
        final GridCacheReturn ret,
        final boolean keepBinary
    ) {
        if (implicit()) {
            // Should never load missing values for implicit transaction as values will be returned
            // with prepare response, if required.
            assert loadFut.isDone();

            try {
                loadFut.get();
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }

            return nonInterruptable(commitAsync().chain(
                new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn>() {
                    @Override public GridCacheReturn applyx(IgniteInternalFuture<IgniteInternalTx> txFut)
                        throws IgniteCheckedException {
                        try {
                            txFut.get();

                            Object res = implicitRes.value();

                            if (implicitRes.invokeResult()) {
                                assert res == null || res instanceof Map : implicitRes;

                                res = cacheCtx.unwrapInvokeResult((Map)res, keepBinary);
                            }

                            return new GridCacheReturn(cacheCtx, true, keepBinary, res, implicitRes.success());
                        }
                        catch (IgniteCheckedException | RuntimeException e) {
                            rollbackAsync();

                            throw e;
                        }
                    }
                }
            ));
        }
        else {
            return nonInterruptable(loadFut.chain(new CX1<IgniteInternalFuture<Void>, GridCacheReturn>() {
                @Override public GridCacheReturn applyx(IgniteInternalFuture<Void> f) throws IgniteCheckedException {
                    f.get();

                    return ret;
                }
            }));
        }
    }

    /**
     *
     */
    private void onException() {
        for (IgniteTxEntry txEntry : allEntries()) {
            GridCacheEntryEx cached0 = txEntry.cached();

            if (cached0 != null)
                txEntry.context().evicts().touch(cached0, topologyVersion());
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<? extends K> keys,
        boolean retval,
        CacheEntryPredicate filter,
        boolean singleRmv
    ) {
        return removeAllAsync0(cacheCtx, entryTopVer, keys, null, retval, filter, singleRmv);
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param drMap DR map.
     * @param retval Flag indicating whether a value should be returned.
     * @param filter Filter.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @return Future for asynchronous remove.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        @Nullable final Collection<? extends K> keys,
        @Nullable Map<KeyCacheObject, GridCacheVersion> drMap,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter,
        boolean singleRmv) {
        try {
            checkUpdatesAllowed(cacheCtx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        cacheCtx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        if (retval)
            needReturnValue(true);

        final Collection<?> keys0;

        if (drMap != null) {
            assert keys == null;

            keys0 = drMap.keySet();
        }
        else
            keys0 = keys;

        CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

        final Byte dataCenterId;

        if (opCtx != null && opCtx.hasDataCenterId()) {
            assert drMap == null : drMap;

            dataCenterId = opCtx.dataCenterId();
        }
        else
            dataCenterId = null;

        assert keys0 != null;

        if (log.isDebugEnabled())
            log.debug(S.toString("Called removeAllAsync(...)",
                "tx", this, false,
                "keys", keys0, true,
                "implicit", implicit, false,
                "retval", retval, false));

        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

        if (F.isEmpty(keys0)) {
            if (implicit()) {
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }
            }

            return new GridFinishedFuture<>(ret.success(true));
        }

        init();

        final Collection<KeyCacheObject> enlisted = new ArrayList<>();

        ExpiryPolicy plc;

        final CacheEntryPredicate[] filters = CU.filterArray(filter);

        if (!F.isEmpty(filters))
            plc = opCtx != null ? opCtx.expiry() : null;
        else
            plc = null;

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        final IgniteInternalFuture<Void> loadFut = enlistWrite(
            cacheCtx,
            entryTopVer,
            keys0,
            plc,
            /** lookup map */null,
            /** invoke map */null,
            /** invoke arguments */null,
            retval,
            /** lock only */false,
            filters,
            ret,
            enlisted,
            null,
            drMap,
            opCtx != null && opCtx.skipStore(),
            singleRmv,
            keepBinary,
            dataCenterId
        );

        if (log.isDebugEnabled())
            log.debug("Remove keys: " + enlisted);

        // Acquire locks only after having added operation to the write set.
        // Otherwise, during rollback we will not know whether locks need
        // to be rolled back.
        if (pessimistic()) {
            assert loadFut == null || loadFut.isDone() : loadFut;

            if (loadFut != null) {
                try {
                    loadFut.get();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }
            }

            if (log.isDebugEnabled())
                log.debug("Before acquiring transaction lock for remove on keys: " + enlisted);

            long timeout = remainingTime();

            if (timeout == -1)
                return new GridFinishedFuture<>(timeoutException());

            IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(enlisted,
                timeout,
                this,
                false,
                retval,
                isolation,
                isInvalidate(),
                -1L,
                -1L);

            PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                @Override protected GridCacheReturn postLock(GridCacheReturn ret)
                    throws IgniteCheckedException
                {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock for remove on keys: " + enlisted);

                    postLockWrite(cacheCtx,
                        enlisted,
                        ret,
                        /*remove*/true,
                        retval,
                        /*read*/false,
                        -1L,
                        filters,
                        /*computeInvoke*/false);

                    return ret;
                }
            };

            if (fut.isDone()) {
                try {
                    return nonInterruptable(plc1.apply(fut.get(), null));
                }
                catch (GridClosureException e) {
                    return new GridFinishedFuture<>(e.unwrap());
                }
                catch (IgniteCheckedException e) {
                    try {
                        return nonInterruptable(plc1.apply(false, e));
                    }
                    catch (Exception e1) {
                        return new GridFinishedFuture<>(e1);
                    }
                }
            }
            else
                return nonInterruptable(new GridEmbeddedFuture<>(
                    fut,
                    plc1
                ));
        }
        else {
            if (implicit()) {
                // Should never load missing values for implicit transaction as values will be returned
                // with prepare response, if required.
                assert loadFut.isDone();

                return nonInterruptable(commitAsync().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn>() {
                    @Override public GridCacheReturn applyx(IgniteInternalFuture<IgniteInternalTx> txFut)
                        throws IgniteCheckedException {
                        try {
                            txFut.get();

                            return new GridCacheReturn(cacheCtx, true, keepBinary,
                                implicitRes.value(), implicitRes.success());
                        }
                        catch (IgniteCheckedException | RuntimeException e) {
                            rollbackAsync();

                            throw e;
                        }
                    }
                }));
            }
            else {
                return nonInterruptable(loadFut.chain(new CX1<IgniteInternalFuture<Void>, GridCacheReturn>() {
                    @Override public GridCacheReturn applyx(IgniteInternalFuture<Void> f)
                        throws IgniteCheckedException {
                        f.get();

                        return ret;
                    }
                }));
            }
        }
    }

    /**
     * @param fut Future.
     * @return Future ignoring interrupts on {@code get()}.
     */
    private <T> IgniteInternalFuture<T> nonInterruptable(IgniteInternalFuture<T> fut) {
        // Safety.
        if (fut instanceof GridFutureAdapter)
            ((GridFutureAdapter)fut).ignoreInterrupts(true);

        return fut;
    }

    /**
     * Checks if binary values should be deserialized.
     *
     * @param cacheCtx Cache context.
     * @return {@code True} if binary should be deserialized, {@code false} otherwise.
     */
    private boolean deserializeBinaries(GridCacheContext cacheCtx) {
        CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

        return opCtx == null || !opCtx.isKeepBinary();
    }

    /**
     * Initializes read map.
     *
     * @return {@code True} if transaction was successfully  started.
     */
    public boolean init() {
        return !txState.init(txSize) || cctx.tm().onStarted(this);
    }

    /**
     * Adds cache to the list of active caches in transaction.
     *
     * @param cacheCtx Cache context to add.
     * @throws IgniteCheckedException If caches already enlisted in this transaction are not compatible with given
     *      cache (e.g. they have different stores).
     */
    protected final void addActiveCache(GridCacheContext cacheCtx) throws IgniteCheckedException {
        txState.addActiveCache(cacheCtx, this);
    }

    /**
     * Checks transaction expiration.
     *
     * @throws IgniteCheckedException If transaction check failed.
     */
    protected void checkValid() throws IgniteCheckedException {
        if (local() && !dht() && remainingTime() == -1)
            state(MARKED_ROLLBACK, true);

        if (isRollbackOnly()) {
            if (remainingTime() == -1)
                throw new IgniteTxTimeoutCheckedException("Cache transaction timed out: " + this);

            TransactionState state = state();

            if (state == ROLLING_BACK || state == ROLLED_BACK)
                throw new IgniteTxRollbackCheckedException("Cache transaction is marked as rollback-only " +
                    "(will be rolled back automatically): " + this);

            if (state == UNKNOWN)
                throw new IgniteTxHeuristicCheckedException("Cache transaction is in unknown state " +
                    "(remote transactions will be invalidated): " + this);

            throw new IgniteCheckedException("Cache transaction marked as rollback-only: " + this);
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
    protected final IgniteTxEntry addEntry(GridCacheOperation op,
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
        boolean keepBinary
    ) {
        assert invokeArgs == null || op == TRANSFORM;

        IgniteTxKey key = entry.txKey();

        checkInternal(key);

        TransactionState state = state();

        assert state == TransactionState.ACTIVE || remainingTime() == -1 :
            "Invalid tx state for adding entry [op=" + op + ", val=" + val + ", entry=" + entry + ", filter=" +
                Arrays.toString(filter) + ", txCtx=" + cctx.tm().txContextVersion() + ", tx=" + this + ']';

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
                keepBinary);

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
                        "for transaction [timeout=" + timeout() + ", tx=" + this + ']', deadlockErr)
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
                        ", tx=" + IgniteTxLocalAdapter.this + ']'));

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
     * Post-lock closure.
     *
     * @param <T> Return type.
     */
    protected abstract class FinishClosure<T> implements IgniteBiClosure<T, Exception, T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public final T apply(T t, @Nullable Exception e) {
            boolean rollback = true;

            try {
                if (e != null)
                    throw new GridClosureException(e);

                t = finish(t);

                // Commit implicit transactions.
                if (implicit())
                    commit();

                rollback = false;

                return t;
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
         * @param t Argument.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        abstract T finish(T t) throws IgniteCheckedException;
    }
}
