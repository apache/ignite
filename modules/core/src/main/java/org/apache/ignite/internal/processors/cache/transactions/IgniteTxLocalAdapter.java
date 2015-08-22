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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.store.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.dr.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 * Transaction adapter for cache transactions.
 */
public abstract class IgniteTxLocalAdapter extends IgniteTxAdapter
    implements IgniteTxLocalEx {
    /** */
    private static final long serialVersionUID = 0L;

    /** Per-transaction read map. */
    @GridToStringInclude
    protected Map<IgniteTxKey, IgniteTxEntry> txMap;

    /** Read view on transaction map. */
    @GridToStringExclude
    protected IgniteTxMap readView;

    /** Write view on transaction map. */
    @GridToStringExclude
    protected IgniteTxMap writeView;

    /** Minimal version encountered (either explicit lock or XID of this transaction). */
    protected GridCacheVersion minVer;

    /** Flag indicating with TM commit happened. */
    protected AtomicBoolean doneFlag = new AtomicBoolean(false);

    /** Committed versions, relative to base. */
    private Collection<GridCacheVersion> committedVers = Collections.emptyList();

    /** Rolled back versions, relative to base. */
    private Collection<GridCacheVersion> rolledbackVers = Collections.emptyList();

    /** Base for completed versions. */
    private GridCacheVersion completedBase;

    /** Flag indicating that transformed values should be sent to remote nodes. */
    private boolean sndTransformedVals;

    /** Commit error. */
    protected AtomicReference<Throwable> commitErr = new AtomicReference<>();

    /** Active cache IDs. */
    protected Set<Integer> activeCacheIds = new HashSet<>();

    /** Need return value. */
    protected boolean needRetVal;

    /** Implicit transaction result. */
    protected GridCacheReturn implicitRes;

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
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(cctx, xidVer, implicit, implicitSingle, /*local*/true, sys, plc, concurrency, isolation, timeout,
            invalidate, storeEnabled, txSize, subjId, taskNameHash);

        minVer = xidVer;
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
        return txMap.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        return Collections.singleton(nodeId);
    }

    /** {@inheritDoc} */
    @Override public Throwable commitError() {
        return commitErr.get();
    }

    /** {@inheritDoc} */
    @Override public void commitError(Throwable e) {
        commitErr.compareAndSet(null, e);
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        assert false;
        return false;
    }

    /**
     * Gets collection of active cache IDs for this transaction.
     *
     * @return Collection of active cache IDs.
     */
    @Override public Collection<Integer> activeCacheIds() {
        return activeCacheIds;
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return txMap != null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return writeView.containsKey(key);
    }

    /**
     * @return Transaction read set.
     */
    @Override public Set<IgniteTxKey> readSet() {
        return txMap == null ? Collections.<IgniteTxKey>emptySet() : readView.keySet();
    }

    /**
     * @return Transaction write set.
     */
    @Override public Set<IgniteTxKey> writeSet() {
        return txMap == null ? Collections.<IgniteTxKey>emptySet() : writeView.keySet();
    }

    /** {@inheritDoc} */
    @Override public boolean removed(IgniteTxKey key) {
        if (txMap == null)
            return false;

        IgniteTxEntry e = txMap.get(key);

        return e != null && e.op() == DELETE;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return readView == null ? Collections.<IgniteTxKey, IgniteTxEntry>emptyMap() : readView;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return writeView == null ? Collections.<IgniteTxKey, IgniteTxEntry>emptyMap() : writeView;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return txMap == null ? Collections.<IgniteTxEntry>emptySet() : txMap.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return readView == null ? Collections.<IgniteTxEntry>emptyList() : readView.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return writeView == null ? Collections.<IgniteTxEntry>emptyList() : writeView.values();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteTxEntry entry(IgniteTxKey key) {
        return txMap == null ? null : txMap.get(key);
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        if (readView != null)
            readView.seal();

        if (writeView != null)
            writeView.seal();
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn implicitSingleResult() {
        return implicitRes;
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
     * @return Flag indicating whether transaction needs return value.
     */
    public boolean needReturnValue() {
        return needRetVal;
    }

    /**
     * @param needRetVal Need return value flag.
     */
    public void needReturnValue(boolean needRetVal) {
        this.needRetVal = needRetVal;
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
        KeyCacheObject key,
        CacheEntryPredicate[] filter
    ) throws GridCacheFilterFailedException {
        IgniteTxEntry e = txMap == null ? null : txMap.get(cacheCtx.txKey(key));

        if (e != null) {
            // We should look at tx entry previous value. If this is a user peek then previous
            // value is the same as value. If this is a filter evaluation peek then previous value holds
            // value visible to filter while value contains value enlisted for write.
            if (!F.isEmpty(filter) && !F.isAll(e.cached(), filter))
                return e.hasPreviousValue() ? F.t(CU.<CacheObject>failed(failFast, e.previousValue())) : null;

            return e.hasPreviousValue() ? F.t(e.previousValue()) : null;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> loadMissing(
        final GridCacheContext cacheCtx,
        final boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        boolean deserializePortable,
        boolean skipVals,
        final IgniteBiInClosure<KeyCacheObject, Object> c
    ) {
        if (!async) {
            try {
                if (!readThrough || !cacheCtx.readThrough()) {
                    for (KeyCacheObject key : keys)
                        c.apply(key, null);

                    return new GridFinishedFuture<>(false);
                }

                return new GridFinishedFuture<>(
                    cacheCtx.store().loadAll(this, keys, c));
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }
        else {
            return cctx.kernalContext().closure().callLocalSafe(
                new GPC<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        if (!readThrough || !cacheCtx.readThrough()) {
                            for (KeyCacheObject key : keys)
                                c.apply(key, null);

                            return false;
                        }

                        return cacheCtx.store().loadAll(IgniteTxLocalAdapter.this, keys, c);
                    }
                },
                true);
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
            if (timedOut())
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            TransactionState state = state();

            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for prepare [state=" + state + ", tx=" + this + ']');
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
        assert ownsLockUnsafe(entry) : "Lock is not owned for commit in PESSIMISTIC mode [entry=" + entry +
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

    /**
     * Performs batch database operations. This commit must be called
     * before {@link #userCommit()}. This way if there is a DB failure,
     * cache transaction can still be rolled back.
     *
     * @param writeEntries Transaction write set.
     * @throws IgniteCheckedException If batch update failed.
     */
    @SuppressWarnings({"CatchGenericClass"})
    protected void batchStoreCommit(Iterable<IgniteTxEntry> writeEntries) throws IgniteCheckedException {
        if (!storeEnabled() || internal())
            return;

        Collection<CacheStoreManager> stores = stores();

        if (stores == null || stores.isEmpty())
            return;

        assert isWriteToStoreFromDhtValid(stores) : "isWriteToStoreFromDht can't be different within one transaction";

        boolean isWriteToStoreFromDht = F.first(stores).isWriteToStoreFromDht();

        if (near() || isWriteToStoreFromDht) {
            try {
                if (writeEntries != null) {
                    Map<Object, IgniteBiTuple<Object, GridCacheVersion>> putMap = null;
                    List<Object> rmvCol = null;
                    CacheStoreManager writeStore = null;

                    boolean skipNonPrimary = near() && isWriteToStoreFromDht;

                    for (IgniteTxEntry e : writeEntries) {
                        boolean skip = e.skipStore();

                        if (!skip && skipNonPrimary) {
                            skip = e.cached().isNear() ||
                                e.cached().detached() ||
                                !e.context().affinity().primary(e.cached().partition(), topologyVersion()).isLocal();
                        }

                        if (skip)
                            continue;

                        boolean intercept = e.context().config().getInterceptor() != null;

                        if (intercept || !F.isEmpty(e.entryProcessors()))
                            e.cached().unswap(false);

                        IgniteBiTuple<GridCacheOperation, CacheObject> res = applyTransformClosures(e, false);

                        GridCacheContext cacheCtx = e.context();

                        GridCacheOperation op = res.get1();
                        KeyCacheObject key = e.key();
                        CacheObject val = res.get2();
                        GridCacheVersion ver = writeVersion();

                        if (op == CREATE || op == UPDATE) {
                            // Batch-process all removes if needed.
                            if (rmvCol != null && !rmvCol.isEmpty()) {
                                assert writeStore != null;

                                writeStore.removeAll(this, rmvCol);

                                // Reset.
                                rmvCol.clear();

                                writeStore = null;
                            }

                            // Batch-process puts if cache ID has changed.
                            if (writeStore != null && writeStore != cacheCtx.store()) {
                                if (putMap != null && !putMap.isEmpty()) {
                                    writeStore.putAll(this, putMap);

                                    // Reset.
                                    putMap.clear();
                                }

                                writeStore = null;
                            }

                            if (intercept) {
                                Object interceptorVal = cacheCtx.config().getInterceptor()
                                    .onBeforePut(new CacheLazyEntry(cacheCtx, key, e.cached().rawGetOrUnmarshal(true)),
                                        CU.value(val, cacheCtx, false));

                                if (interceptorVal == null)
                                    continue;

                                val = cacheCtx.toCacheObject(cacheCtx.unwrapTemporary(interceptorVal));
                            }

                            if (writeStore == null)
                                writeStore = cacheCtx.store();

                            if (writeStore.isWriteThrough()) {
                                if (putMap == null)
                                    putMap = new LinkedHashMap<>(writeMap().size(), 1.0f);

                                putMap.put(CU.value(key, cacheCtx, false), F.t(CU.value(val, cacheCtx, false), ver));
                            }
                        }
                        else if (op == DELETE) {
                            // Batch-process all puts if needed.
                            if (putMap != null && !putMap.isEmpty()) {
                                assert writeStore != null;

                                writeStore.putAll(this, putMap);

                                // Reset.
                                putMap.clear();

                                writeStore = null;
                            }

                            if (writeStore != null && writeStore != cacheCtx.store()) {
                                if (rmvCol != null && !rmvCol.isEmpty()) {
                                    writeStore.removeAll(this, rmvCol);

                                    // Reset.
                                    rmvCol.clear();
                                }

                                writeStore = null;
                            }

                            if (intercept) {
                                IgniteBiTuple<Boolean, Object> t = cacheCtx.config().getInterceptor().onBeforeRemove(
                                    new CacheLazyEntry(cacheCtx, key, e.cached().rawGetOrUnmarshal(true)));

                                if (cacheCtx.cancelRemove(t))
                                    continue;
                            }

                            if (writeStore == null)
                                writeStore = cacheCtx.store();

                            if (writeStore.isWriteThrough()) {
                                if (rmvCol == null)
                                    rmvCol = new ArrayList<>();

                                rmvCol.add(key.value(cacheCtx.cacheObjectContext(), false));
                            }
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Ignoring NOOP entry for batch store commit: " + e);
                    }

                    if (putMap != null && !putMap.isEmpty()) {
                        assert rmvCol == null || rmvCol.isEmpty();
                        assert writeStore != null;

                        // Batch put at the end of transaction.
                        writeStore.putAll(this, putMap);
                    }

                    if (rmvCol != null && !rmvCol.isEmpty()) {
                        assert putMap == null || putMap.isEmpty();
                        assert writeStore != null;

                        // Batch remove at the end of transaction.
                        writeStore.removeAll(this, rmvCol);
                    }
                }

                // Commit while locks are held.
                sessionEnd(stores, true);
            }
            catch (IgniteCheckedException ex) {
                commitError(ex);

                setRollbackOnly();

                // Safe to remove transaction from committed tx list because nothing was committed yet.
                cctx.tm().removeCommittedTx(this);

                throw ex;
            }
            catch (Throwable ex) {
                commitError(ex);

                setRollbackOnly();

                // Safe to remove transaction from committed tx list because nothing was committed yet.
                cctx.tm().removeCommittedTx(this);

                if (ex instanceof Error)
                    throw (Error)ex;

                throw new IgniteCheckedException("Failed to commit transaction to database: " + this, ex);
            }
            finally {
                if (isRollbackOnly())
                    sessionEnd(stores, false);
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass"})
    @Override public void userCommit() throws IgniteCheckedException {
        TransactionState state = state();

        if (state != COMMITTING) {
            if (timedOut())
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for commit [state=" + state + ", tx=" + this + ']');
        }

        checkValid();

        boolean empty = F.isEmpty(near() ? txMap : writeMap());

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
                for (IgniteTxEntry txEntry : (near() ? allEntries() : writeEntries())) {
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
                                        true);

                                    // For near local transactions we must record DHT version
                                    // in order to keep near entries on backup nodes until
                                    // backup remote transaction completes.
                                    if (cacheCtx.isNear()) {
                                        if (txEntry.op() == CREATE || txEntry.op() == UPDATE ||
                                            txEntry.op() == DELETE || txEntry.op() == TRANSFORM)
                                            ((GridNearCacheEntry)cached).recordDhtVersion(txEntry.dhtVersion());

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
                                            topVer,
                                            null,
                                            cached.detached() ? DR_NONE : drType,
                                            txEntry.conflictExpireTime(),
                                            cached.isNear() ? null : explicitVer,
                                            CU.subjectId(this, cctx),
                                            resolveTaskName());

                                        if (nearCached != null && updRes.success())
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
                                                topVer,
                                                CU.empty0(),
                                                DR_NONE,
                                                txEntry.conflictExpireTime(),
                                                null,
                                                CU.subjectId(this, cctx),
                                                resolveTaskName());
                                    }
                                    else if (op == DELETE) {
                                        GridCacheUpdateTxResult updRes = cached.innerRemove(
                                            this,
                                            eventNodeId(),
                                            txEntry.nodeId(),
                                            false,
                                            false,
                                            evt,
                                            metrics,
                                            topVer,
                                            null,
                                            cached.detached()  ? DR_NONE : drType,
                                            cached.isNear() ? null : explicitVer,
                                            CU.subjectId(this, cctx),
                                            resolveTaskName());

                                        if (nearCached != null && updRes.success())
                                            nearCached.innerRemove(
                                                null,
                                                eventNodeId(),
                                                nodeId,
                                                false,
                                                false,
                                                false,
                                                metrics,
                                                topVer,
                                                CU.empty0(),
                                                DR_NONE,
                                                null,
                                                CU.subjectId(this, cctx),
                                                resolveTaskName());
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

                                txEntry.cached(entryEx(cacheCtx, txEntry.txKey()));
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

                            setRollbackOnly();

                            throw ex;
                        }
                        else {
                            IgniteCheckedException err = new IgniteTxHeuristicCheckedException("Failed to locally write to cache " +
                                "(all transaction entries will be invalidated, however there was a window when " +
                                "entries for this transaction were visible to others): " + this, ex);

                            U.error(log, "Heuristic transaction failure.", err);

                            commitErr.compareAndSet(null, err);

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
            if (doneFlag.compareAndSet(false, true)) {
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
     */
    public void tmCommit() {
        assert onePhaseCommit();

        if (doneFlag.compareAndSet(false, true)) {
            // Unlock all locks.
            cctx.tm().commitTx(this);

            state(COMMITTED);

            boolean needsCompletedVersions = needsCompletedVersions();

            assert !needsCompletedVersions || completedBase != null;
            assert !needsCompletedVersions || committedVers != null;
            assert !needsCompletedVersions || rolledbackVers != null;
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

            throw new IgniteCheckedException("Invalid transaction state for rollback [state=" + state + ", tx=" + this + ']',
                commitErr.get());
        }

        if (doneFlag.compareAndSet(false, true)) {
            try {
                if (near())
                    // Must evict near entries before rolling back from
                    // transaction manager, so they will be removed from cache.
                    for (IgniteTxEntry e : allEntries())
                        evictNearEntry(e, false);

                cctx.tm().rollbackTx(this);

                if (!internal()) {
                    Collection<CacheStoreManager> stores = stores();

                    if (stores != null && !stores.isEmpty()) {
                        assert isWriteToStoreFromDhtValid(stores) :
                            "isWriteToStoreFromDht can't be different within one transaction";

                        boolean isWriteToStoreFromDht = F.first(stores).isWriteToStoreFromDht();

                        if (stores != null && !stores.isEmpty() && (near() || isWriteToStoreFromDht))
                            sessionEnd(stores, false);
                    }
                }
            }
            catch (Error | IgniteCheckedException | RuntimeException e) {
                U.addLastCause(e, commitErr.get(), log);

                throw e;
            }
        }
    }

    /**
     * @param stores Store managers.
     * @param commit Commit flag.
     * @throws IgniteCheckedException In case of error.
     */
    private void sessionEnd(Collection<CacheStoreManager> stores, boolean commit) throws IgniteCheckedException {
        Iterator<CacheStoreManager> it = stores.iterator();

        while (it.hasNext()) {
            CacheStoreManager store = it.next();

            store.sessionEnd(this, commit, !it.hasNext());
        }
    }

    /**
     * Checks if there is a cached or swapped value for
     * {@link #getAllAsync(GridCacheContext, Collection, GridCacheEntryEx, boolean, boolean, boolean, boolean)} method.
     *
     * @param cacheCtx Cache context.
     * @param keys Key to enlist.
     * @param cached Cached entry, if called from entry wrapper.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param map Return map.
     * @param missed Map of missed keys.
     * @param keysCnt Keys count (to avoid call to {@code Collection.size()}).
     * @param deserializePortable Deserialize portable flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param skipStore Skip store flag.
     * @throws IgniteCheckedException If failed.
     * @return Enlisted keys.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private <K, V> Collection<KeyCacheObject> enlistRead(
        final GridCacheContext cacheCtx,
        Collection<KeyCacheObject> keys,
        @Nullable GridCacheEntryEx cached,
        @Nullable ExpiryPolicy expiryPlc,
        Map<K, V> map,
        Map<KeyCacheObject, GridCacheVersion> missed,
        int keysCnt,
        boolean deserializePortable,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean skipStore
    ) throws IgniteCheckedException {
        assert !F.isEmpty(keys);
        assert keysCnt == keys.size();
        assert cached == null || F.first(keys).equals(cached.key());

        cacheCtx.checkSecurity(SecurityPermission.CACHE_READ);

        boolean single = keysCnt == 1;

        Collection<KeyCacheObject> lockKeys = null;

        AffinityTopologyVersion topVer = topologyVersion();

        // In this loop we cover only read-committed or optimistic transactions.
        // Transactions that are pessimistic and not read-committed are covered
        // outside of this loop.
        for (KeyCacheObject key : keys) {
            if (pessimistic() && !readCommitted() && !skipVals)
                addActiveCache(cacheCtx);

            IgniteTxKey txKey = cacheCtx.txKey(key);

            // Check write map (always check writes first).
            IgniteTxEntry txEntry = entry(txKey);

            // Either non-read-committed or there was a previous write.
            if (txEntry != null) {
                CacheObject val = txEntry.value();

                // Read value from locked entry in group-lock transaction as well.
                if (txEntry.hasValue()) {
                    if (!F.isEmpty(txEntry.entryProcessors()))
                        val = txEntry.applyEntryProcessors(val);

                    if (val != null)
                        cacheCtx.addResult(map, key, val, skipVals, keepCacheObjects, deserializePortable, false);
                }
                else {
                    assert txEntry.op() == TRANSFORM;

                    while (true) {
                        try {
                            Object transformClo =
                                (txEntry.op() == TRANSFORM  && cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                    F.first(txEntry.entryProcessors()) : null;

                            val = txEntry.cached().innerGet(this,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail fast*/true,
                                /*unmarshal*/true,
                                /*metrics*/true,
                                /*event*/!skipVals,
                                /*temporary*/false,
                                CU.subjectId(this, cctx),
                                transformClo,
                                resolveTaskName(),
                                null);

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
                                    deserializePortable,
                                    false);
                            }
                            else
                                missed.put(key, txEntry.cached().version());

                            break;
                        }
                        catch (GridCacheFilterFailedException e) {
                            if (log.isDebugEnabled())
                                log.debug("Filter validation failed for entry: " + txEntry);

                            if (!readCommitted())
                                txEntry.readValue(e.<V>value());
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
                    GridCacheEntryEx entry;

                    if (cached != null) {
                        entry = cached;

                        cached = null;
                    }
                    else
                        entry = entryEx(cacheCtx, txKey, topVer);

                    try {
                        GridCacheVersion ver = entry.version();

                        CacheObject val = null;

                        if (!pessimistic() || readCommitted() && !skipVals) {
                            IgniteCacheExpiryPolicy accessPlc =
                                optimistic() ? accessPolicy(cacheCtx, txKey, expiryPlc) : null;

                            // This call will check for filter.
                            val = entry.innerGet(this,
                                /*swap*/true,
                                /*no read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /*metrics*/true,
                                /*event*/true,
                                /*temporary*/false,
                                CU.subjectId(this, cctx),
                                null,
                                resolveTaskName(),
                                accessPlc);

                            if (val != null) {
                                cacheCtx.addResult(map,
                                    key,
                                    val,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializePortable,
                                    false);
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
                                skipStore);


                            // As optimization, mark as checked immediately
                            // for non-pessimistic if value is not null.
                            if (val != null && !pessimistic())
                                txEntry.markValid();
                        }

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in transaction getAllAsync(..) (will retry): " + key);
                    }
                    catch (GridCacheFilterFailedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Filter validation failed for entry: " + entry);

                        if (!readCommitted()) {
                            // Value for which failure occurred.
                            CacheObject val = e.value();

                            txEntry = addEntry(READ,
                                val,
                                null,
                                null,
                                entry,
                                expiryPlc,
                                CU.empty0(),
                                false,
                                -1L,
                                -1L,
                                null,
                                skipStore);

                            // Mark as checked immediately for non-pessimistic.
                            if (val != null && !pessimistic())
                                txEntry.markValid();
                        }

                        break; // While loop.
                    }
                    finally {
                        if (cacheCtx.isNear() && entry != null && readCommitted()) {
                            if (cacheCtx.affinity().belongs(cacheCtx.localNode(), entry.partition(), topVer)) {
                                if (entry.markObsolete(xidVer))
                                    cacheCtx.cache().removeEntry(entry);
                            }
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
     * Adds skipped key.
     *
     * @param skipped Skipped set (possibly {@code null}).
     * @param key Key to add.
     * @return Skipped set.
     */
    private Set<KeyCacheObject> skip(Set<KeyCacheObject> skipped, KeyCacheObject key) {
        if (skipped == null)
            skipped = new GridLeanSet<>();

        skipped.add(key);

        if (log.isDebugEnabled())
            log.debug("Added key to skipped set: " + key);

        return skipped;
    }

    /**
     * Loads all missed keys for
     * {@link #getAllAsync(GridCacheContext, Collection, GridCacheEntryEx, boolean, boolean, boolean, boolean)} method.
     *
     * @param cacheCtx Cache context.
     * @param map Return map.
     * @param missedMap Missed keys.
     * @param redos Keys to retry.
     * @param deserializePortable Deserialize portable flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param skipStore Skip store flag.
     * @return Loaded key-value pairs.
     */
    private <K, V> IgniteInternalFuture<Map<K, V>> checkMissed(
        final GridCacheContext cacheCtx,
        final Map<K, V> map,
        final Map<KeyCacheObject, GridCacheVersion> missedMap,
        @Nullable final Collection<KeyCacheObject> redos,
        final boolean deserializePortable,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore
    ) {
        assert redos != null || pessimistic();

        if (log.isDebugEnabled())
            log.debug("Loading missed values for missed map: " + missedMap);

        final Collection<KeyCacheObject> loaded = new HashSet<>();

        return new GridEmbeddedFuture<>(
            new C2<Boolean, Exception, Map<K, V>>() {
                @Override public Map<K, V> apply(Boolean b, Exception e) {
                    if (e != null) {
                        setRollbackOnly();

                        throw new GridClosureException(e);
                    }

                    if (!b && !readCommitted()) {
                        // There is no store - we must mark the entries.
                        for (KeyCacheObject key : missedMap.keySet()) {
                            IgniteTxEntry txEntry = entry(cacheCtx.txKey(key));

                            if (txEntry != null)
                                txEntry.markValid();
                        }
                    }

                    if (readCommitted()) {
                        Collection<KeyCacheObject> notFound = new HashSet<>(missedMap.keySet());

                        notFound.removeAll(loaded);

                        // In read-committed mode touch entries that have just been read.
                        for (KeyCacheObject key : notFound) {
                            IgniteTxEntry txEntry = entry(cacheCtx.txKey(key));

                            GridCacheEntryEx entry = txEntry == null ? cacheCtx.cache().peekEx(key) :
                                txEntry.cached();

                            if (entry != null)
                                cacheCtx.evicts().touch(entry, topologyVersion());
                        }
                    }

                    return map;
                }
            },
            loadMissing(
                cacheCtx,
                !skipStore,
                false,
                missedMap.keySet(),
                deserializePortable,
                skipVals,
                new CI2<KeyCacheObject, Object>() {
                    /** */
                    private GridCacheVersion nextVer;

                    @Override public void apply(KeyCacheObject key, Object val) {
                        if (isRollbackOnly()) {
                            if (log.isDebugEnabled())
                                log.debug("Ignoring loaded value for read because transaction was rolled back: " +
                                    IgniteTxLocalAdapter.this);

                            return;
                        }

                        GridCacheVersion ver = missedMap.get(key);

                        if (ver == null) {
                            if (log.isDebugEnabled())
                                log.debug("Value from storage was never asked for [key=" + key + ", val=" + val + ']');

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

                        // In pessimistic mode we hold the lock, so filter validation
                        // should always be valid.
                        if (pessimistic())
                            ver = null;

                        // Initialize next version.
                        if (nextVer == null)
                            nextVer = cctx.versions().next(topologyVersion());

                        while (true) {
                            assert txEntry != null || readCommitted() || skipVals;

                            GridCacheEntryEx e = txEntry == null ? entryEx(cacheCtx, txKey) : txEntry.cached();

                            try {
                                // Must initialize to true since even if filter didn't pass,
                                // we still record the transaction value.
                                boolean set;

                                try {
                                    set = e.versionedValue(cacheVal, ver, nextVer);
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed entry in transaction getAll method " +
                                            "(will try again): " + e);

                                    if (pessimistic() && !readCommitted() && !isRollbackOnly()) {
                                        U.error(log, "Inconsistent transaction state (entry got removed while " +
                                            "holding lock) [entry=" + e + ", tx=" + IgniteTxLocalAdapter.this + "]");

                                        setRollbackOnly();

                                        return;
                                    }

                                    if (txEntry != null)
                                        txEntry.cached(entryEx(cacheCtx, txKey));

                                    continue; // While loop.
                                }

                                // In pessimistic mode, we should always be able to set.
                                assert set || !pessimistic();

                                if (readCommitted() || skipVals) {
                                    cacheCtx.evicts().touch(e, topologyVersion());

                                    if (visibleVal != null) {
                                        cacheCtx.addResult(map,
                                            key,
                                            visibleVal,
                                            skipVals,
                                            keepCacheObjects,
                                            deserializePortable,
                                            false);
                                    }
                                }
                                else {
                                    assert txEntry != null;

                                    txEntry.setAndMarkValid(cacheVal);

                                    if (visibleVal != null) {
                                        cacheCtx.addResult(map,
                                            key,
                                            visibleVal,
                                            skipVals,
                                            keepCacheObjects,
                                            deserializePortable,
                                            false);
                                    }
                                }

                                loaded.add(key);

                                if (log.isDebugEnabled())
                                    log.debug("Set value loaded from store into entry from transaction [set=" + set +
                                        ", matchVer=" + ver + ", newVer=" + nextVer + ", entry=" + e + ']');

                                break; // While loop.
                            }
                            catch (IgniteCheckedException ex) {
                                throw new IgniteException("Failed to put value for cache entry: " + e, ex);
                            }
                        }
                    }
                })
        );
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalFuture<Map<K, V>> getAllAsync(
        final GridCacheContext cacheCtx,
        Collection<KeyCacheObject> keys,
        @Nullable GridCacheEntryEx cached,
        final boolean deserializePortable,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore) {
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
                keys,
                cached,
                expiryPlc,
                retMap,
                missed,
                keysCnt,
                deserializePortable,
                skipVals,
                keepCacheObjects,
                skipStore);

            if (single && missed.isEmpty())
                return new GridFinishedFuture<>(retMap);

            // Handle locks.
            if (pessimistic() && !readCommitted() && !skipVals) {
                if (expiryPlc == null)
                    expiryPlc = cacheCtx.expiry();

                long accessTtl = expiryPlc != null ? CU.toTtl(expiryPlc.getExpiryForAccess()) : CU.TTL_NOT_CHANGED;

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(lockKeys,
                    lockTimeout(),
                    this,
                    true,
                    true,
                    isolation,
                    isInvalidate(),
                    accessTtl);

                PLC2<Map<K, V>> plc2 = new PLC2<Map<K, V>>() {
                    @Override public IgniteInternalFuture<Map<K, V>> postLock() throws IgniteCheckedException {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for read on keys: " + lockKeys);

                        // Load keys only after the locks have been acquired.
                        for (KeyCacheObject cacheKey : lockKeys) {
                            K keyVal =
                                (K)(keepCacheObjects ? cacheKey : cacheKey.value(cacheCtx.cacheObjectContext(), false));

                            if (retMap.containsKey(keyVal))
                                // We already have a return value.
                                continue;

                            IgniteTxKey txKey = cacheCtx.txKey(cacheKey);

                            IgniteTxEntry txEntry = entry(txKey);

                            assert txEntry != null;

                            // Check if there is cached value.
                            while (true) {
                                GridCacheEntryEx cached = txEntry.cached();

                                try {
                                    Object transformClo =
                                        (!F.isEmpty(txEntry.entryProcessors()) &&
                                            cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                            F.first(txEntry.entryProcessors()) : null;

                                    CacheObject val = cached.innerGet(IgniteTxLocalAdapter.this,
                                        cacheCtx.isSwapOrOffheapEnabled(),
                                        /*read-through*/false,
                                        /*fail-fast*/true,
                                        /*unmarshal*/true,
                                        /*metrics*/true,
                                        /*events*/!skipVals,
                                        /*temporary*/true,
                                        CU.subjectId(IgniteTxLocalAdapter.this, cctx),
                                        transformClo,
                                        resolveTaskName(),
                                        null);

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
                                            deserializePortable,
                                            false);
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

                                    txEntry.cached(entryEx(cacheCtx, txKey));
                                }
                                catch (GridCacheFilterFailedException e) {
                                    // Failed value for the filter.
                                    CacheObject val = e.value();

                                    if (val != null) {
                                        // If filter fails after lock is acquired, we don't reload,
                                        // regardless if value is null or not.
                                        missed.remove(cacheKey);

                                        txEntry.setAndMarkValid(val);
                                    }

                                    break; // While.
                                }
                            }
                        }

                        if (!missed.isEmpty() && cacheCtx.isLocal()) {
                            return checkMissed(cacheCtx,
                                retMap,
                                missed,
                                null,
                                deserializePortable,
                                skipVals,
                                keepCacheObjects,
                                skipStore);
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

                final Collection<KeyCacheObject> redos = new ArrayList<>();

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

                    IgniteInternalFuture<Map<K, V>> fut0 = checkMissed(cacheCtx,
                        retMap,
                        missed,
                        redos,
                        deserializePortable,
                        skipVals,
                        keepCacheObjects,
                        skipStore);

                    return new GridEmbeddedFuture<>(
                        // First future.
                        fut0,
                        // Closure that returns another future, based on result from first.
                        new PMC<Map<K, V>>() {
                            @Override public IgniteInternalFuture<Map<K, V>> postMiss(Map<K, V> map) {
                                if (redos.isEmpty())
                                    return new GridFinishedFuture<>(
                                        Collections.<K, V>emptyMap());

                                if (log.isDebugEnabled())
                                    log.debug("Starting to future-recursively get values for keys: " + redos);

                                // Future recursion.
                                return getAllAsync(cacheCtx,
                                    redos,
                                    null,
                                    deserializePortable,
                                    skipVals,
                                    true,
                                    skipStore);
                            }
                        },
                        // Finalize.
                        new FinishClosure<Map<K, V>>() {
                            @Override Map<K, V> finish(Map<K, V> loaded) {
                                for (Map.Entry<K, V> entry : loaded.entrySet()) {
                                    KeyCacheObject cacheKey = (KeyCacheObject)entry.getKey();

                                    IgniteTxEntry txEntry = entry(cacheCtx.txKey(cacheKey));

                                    CacheObject val = (CacheObject)entry.getValue();

                                    if (!readCommitted())
                                        txEntry.readValue(val);

                                    if (!F.isEmpty(txEntry.entryProcessors()))
                                        val = txEntry.applyEntryProcessors(val);

                                    cacheCtx.addResult(retMap,
                                        cacheKey,
                                        val,
                                        skipVals,
                                        keepCacheObjects,
                                        deserializePortable,
                                        false);
                                }

                                return retMap;
                            }
                        }
                    );
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
        Map<? extends K, ? extends V> map,
        boolean retval,
        @Nullable GridCacheEntryEx cached,
        long ttl,
        CacheEntryPredicate[] filter
    ) {
        return (IgniteInternalFuture<GridCacheReturn>)putAllAsync0(cacheCtx,
            map,
            null,
            null,
            null,
            retval,
            cached,
            filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheDrInfo> drMap
    ) {
        return this.<Object, Object>putAllAsync0(cacheCtx,
            null,
            null,
            null,
            drMap,
            false,
            null,
            null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V, T> IgniteInternalFuture<GridCacheReturn> invokeAsync(
        GridCacheContext cacheCtx,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> map,
        Object... invokeArgs
    ) {
        return (IgniteInternalFuture<GridCacheReturn>)putAllAsync0(cacheCtx,
            null,
            map,
            invokeArgs,
            null,
            true,
            null,
            null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheVersion> drMap
    ) {
        return removeAllAsync0(cacheCtx, null, drMap, null, false, null);
    }

    /**
     * Checks filter for non-pessimistic transactions.
     *
     * @param cached Cached entry.
     * @param filter Filter to check.
     * @return {@code True} if passed or pessimistic.
     * @throws IgniteCheckedException If failed.
     */
    private <K, V> boolean filter(GridCacheEntryEx cached,
        CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return pessimistic() || (optimistic() && implicit()) || cached.context().isAll(cached, filter);
    }

    /**
     * Internal routine for <tt>putAll(..)</tt>
     *
     * @param cacheCtx Cache context.
     * @param keys Keys to enlist.
     * @param cached Cached entry.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param implicit Implicit flag.
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
     * @return Future with skipped keys (the ones that didn't pass filter for pessimistic transactions).
     */
    protected <K, V> IgniteInternalFuture<Set<KeyCacheObject>> enlistWrite(
        final GridCacheContext cacheCtx,
        Collection<?> keys,
        @Nullable GridCacheEntryEx cached,
        @Nullable ExpiryPolicy expiryPlc,
        boolean implicit,
        @Nullable Map<?, ?> lookup,
        @Nullable Map<?, EntryProcessor<K, V, Object>> invokeMap,
        @Nullable Object[] invokeArgs,
        boolean retval,
        boolean lockOnly,
        CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        Collection<KeyCacheObject> enlisted,
        @Nullable Map<KeyCacheObject, GridCacheDrInfo> drPutMap,
        @Nullable Map<KeyCacheObject, GridCacheVersion> drRmvMap,
        boolean skipStore
    ) {
        assert cached == null || keys.size() == 1;
        assert cached == null || F.first(keys).equals(cached.key());

        try {
            addActiveCache(cacheCtx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        Set<KeyCacheObject> skipped = null;

        boolean rmv = lookup == null && invokeMap == null;

        Set<KeyCacheObject> missedForLoad = null;

        try {
            // Set transform flag for transaction.
            if (invokeMap != null)
                transform = true;

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

                IgniteTxKey txKey = cacheCtx.txKey(cacheKey);

                IgniteTxEntry txEntry = entry(txKey);

                // First time access.
                if (txEntry == null) {
                    while (true) {
                        GridCacheEntryEx entry;

                        if (cached != null) {
                            entry = cached;

                            cached = null;
                        }
                        else {
                            entry = entryEx(cacheCtx, txKey, topologyVersion());

                            entry.unswap(false);
                        }

                        try {
                            // Check if lock is being explicitly acquired by the same thread.
                            if (!implicit && cctx.kernalContext().config().isCacheSanityCheckEnabled() &&
                                entry.lockedByThread(threadId, xidVer))
                                throw new IgniteCheckedException("Cannot access key within transaction if lock is " +
                                    "externally held [key=" + key + ", entry=" + entry + ", xidVer=" + xidVer +
                                    ", threadId=" + threadId +
                                    ", locNodeId=" + cctx.localNodeId() + ']');

                            CacheObject old = null;

                            boolean readThrough = !skipStore && !F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter);

                            if (optimistic() && !implicit()) {
                                try {
                                    // Should read through if filter is specified.
                                    old = entry.innerGet(this,
                                        /*swap*/false,
                                        /*read-through*/readThrough && cacheCtx.loadPreviousValue(),
                                        /*fail-fast*/false,
                                        /*unmarshal*/retval,
                                        /*metrics*/retval,
                                        /*events*/retval,
                                        /*temporary*/false,
                                        CU.subjectId(this, cctx),
                                        entryProcessor,
                                        resolveTaskName(),
                                        null);
                                }
                                catch (ClusterTopologyCheckedException e) {
                                    entry.context().evicts().touch(entry, topologyVersion());

                                    throw e;
                                }
                                catch (GridCacheFilterFailedException e) {
                                    e.printStackTrace();

                                    assert false : "Empty filter failed: " + e;
                                }
                            }
                            else
                                old = retval ? entry.rawGetOrUnmarshal(false) : entry.rawGet();

                            if (!filter(entry, filter)) {
                                skipped = skip(skipped, cacheKey);

                                ret.set(cacheCtx, old, false);

                                if (!readCommitted() && old != null) {
                                    // Enlist failed filters as reads for non-read-committed mode,
                                    // so future ops will get the same values.
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
                                        skipStore);

                                    txEntry.markValid();
                                }

                                if (readCommitted() || old == null)
                                    cacheCtx.evicts().touch(entry, topologyVersion());

                                break; // While.
                            }

                            final GridCacheOperation op = lockOnly ? NOOP : rmv ? DELETE :
                                entryProcessor != null ? TRANSFORM : old != null ? UPDATE : CREATE;

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
                                skipStore);

                            if (!implicit() && readCommitted() && !cacheCtx.offheapTiered())
                                cacheCtx.evicts().touch(entry, topologyVersion());

                            enlisted.add(cacheKey);

                            if (!pessimistic() && !implicit()) {
                                txEntry.markValid();

                                if (old == null) {
                                    boolean load = retval && !readThrough;

                                    if (load) {
                                        if (missedForLoad == null)
                                            missedForLoad = new HashSet<>();

                                        missedForLoad.add(cacheKey);
                                    }
                                    else {
                                        assert !implicit() || !transform : this;
                                        assert txEntry.op() != TRANSFORM : txEntry;

                                        if (retval)
                                            ret.set(cacheCtx, null, true);
                                        else
                                            ret.success(true);
                                    }
                                }
                                else {
                                    if (retval && !transform)
                                        ret.set(cacheCtx, old, true);
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
                                    ret.set(cacheCtx, old, true);
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
                            "transaction after transform closure is applied): " + key);

                    GridCacheEntryEx entry = txEntry.cached();

                    CacheObject v = txEntry.value();

                    boolean del = txEntry.op() == DELETE && rmv;

                    if (!del) {
                        if (!filter(entry, filter)) {
                            skipped = skip(skipped, cacheKey);

                            ret.set(cacheCtx, v, false);

                            continue;
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
                            skipStore);

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
                            ret.set(cacheCtx, v, true);
                        else
                            ret.success(true);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        if (missedForLoad != null) {
            IgniteInternalFuture<Boolean> fut = loadMissing(
                cacheCtx,
                /*read through*/cacheCtx.config().isLoadPreviousValue() && !skipStore,
                /*async*/true,
                missedForLoad,
                deserializePortables(cacheCtx),
                /*skip values*/false,
                new CI2<KeyCacheObject, Object>() {
                    @Override public void apply(KeyCacheObject key, Object val) {
                        if (log.isDebugEnabled())
                            log.debug("Loaded value from remote node [key=" + key + ", val=" + val + ']');

                        IgniteTxEntry e = entry(new IgniteTxKey(key, cacheCtx.cacheId()));

                        assert e != null;

                        CacheObject cacheVal = cacheCtx.toCacheObject(val);

                        if (e.op() == TRANSFORM) {
                            GridCacheVersion ver;

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
                        else
                            ret.set(cacheCtx, cacheVal, true);
                    }
                });

            return new GridEmbeddedFuture<>(
                new C2<Boolean, Exception, Set<KeyCacheObject>>() {
                    @Override public Set<KeyCacheObject> apply(Boolean b, Exception e) {
                        if (e != null)
                            throw new GridClosureException(e);

                        return Collections.emptySet();
                    }
                }, fut
            );
        }

        return new GridFinishedFuture<>(skipped);
    }

    /**
     * Post lock processing for put or remove.
     *
     * @param cacheCtx Context.
     * @param keys Keys.
     * @param failed Collection of potentially failed keys (need to populate in this method).
     * @param ret Return value.
     * @param rmv {@code True} if remove.
     * @param retval Flag to return value or not.
     * @param read {@code True} if read.
     * @param accessTtl TTL for read operation.
     * @param filter Filter to check entries.
     * @return Failed keys.
     * @throws IgniteCheckedException If error.
     * @param computeInvoke If {@code true} computes return value for invoke operation.
     */
    @SuppressWarnings("unchecked")
    protected Set<KeyCacheObject> postLockWrite(
        GridCacheContext cacheCtx,
        Iterable<KeyCacheObject> keys,
        Set<KeyCacheObject> failed,
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
                            try {
                                if (!hasPrevVal) {
                                    boolean readThrough =
                                        (invoke || cacheCtx.loadPreviousValue()) && !txEntry.skipStore();

                                    v = cached.innerGet(this,
                                        /*swap*/true,
                                        readThrough,
                                        /*failFast*/false,
                                        /*unmarshal*/true,
                                        /*metrics*/!invoke,
                                        /*event*/!invoke && !dht(),
                                        /*temporary*/false,
                                        CU.subjectId(this, cctx),
                                        null,
                                        resolveTaskName(),
                                        null);
                                }
                            }
                            catch (GridCacheFilterFailedException e) {
                                e.printStackTrace();

                                assert false : "Empty filter failed: " + e;
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
                            ret.value(cacheCtx, v);
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
                        failed = skip(failed, k);

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

                    txEntry.cached(entryEx(cached.context(), txEntry.txKey()));
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Entries that failed after lock filter check: " + failed);

        return failed;
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
                CacheInvokeEntry<Object, Object> invokeEntry =
                    new CacheInvokeEntry(txEntry.context(), txEntry.key(), key0, cacheVal, val0, ver);

                EntryProcessor<Object, Object, ?> entryProcessor = t.get1();

                res = entryProcessor.process(invokeEntry, t.get2());

                val0 = invokeEntry.value();

                key0 = invokeEntry.key();
            }

            if (res != null)
                ret.addEntryProcessResult(ctx, txEntry.key(), key0, res, null);
        }
        catch (Exception e) {
            ret.addEntryProcessResult(ctx, txEntry.key(), key0, null, e);
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
     * Internal method for all put and transform operations. Only one of {@code map}, {@code transformMap}
     * maps must be non-null.
     *
     * @param cacheCtx Context.
     * @param map Key-value map to store.
     * @param invokeMap Invoke map.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param drMap DR map.
     * @param retval Key-transform value map to store.
     * @param cached Cached entry, if any.
     * @param filter Filter.
     * @return Operation future.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture putAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable Map<? extends K, ? extends V> map,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> invokeMap,
        @Nullable final Object[] invokeArgs,
        @Nullable final Map<KeyCacheObject, GridCacheDrInfo> drMap,
        final boolean retval,
        @Nullable GridCacheEntryEx cached,
        @Nullable final CacheEntryPredicate[] filter
    ) {
        assert filter == null || invokeMap == null;

        try {
            checkUpdatesAllowed(cacheCtx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        cacheCtx.checkSecurity(SecurityPermission.CACHE_PUT);

        if (retval)
            needReturnValue(true);

        // Cached entry may be passed only from entry wrapper.
        final Map<?, ?> map0;
        final Map<?, EntryProcessor<K, V, Object>> invokeMap0;

        if (drMap != null) {
            assert map == null;

            map0 = F.viewReadOnly(drMap, new IgniteClosure<GridCacheDrInfo, Object>() {
                @Override public Object apply(GridCacheDrInfo val) {
                    return val.value();
                }
            });

            invokeMap0 = null;
        }
        else {
            map0 = map;
            invokeMap0 = (Map<K, EntryProcessor<K, V, Object>>)invokeMap;
        }

        if (log.isDebugEnabled())
            log.debug("Called putAllAsync(...) [tx=" + this + ", map=" + map0 + ", retval=" + retval + "]");

        assert map0 != null || invokeMap0 != null;
        assert cached == null ||
            (map0 != null && map0.size() == 1) || (invokeMap0 != null && invokeMap0.size() == 1);

        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        init();

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

            Collection<KeyCacheObject> enlisted = new ArrayList<>();

            CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            final IgniteInternalFuture<Set<KeyCacheObject>> loadFut = enlistWrite(
                cacheCtx,
                keySet,
                cached,
                opCtx != null ? opCtx.expiry() : null,
                implicit,
                map0,
                invokeMap0,
                invokeArgs,
                retval,
                false,
                filter,
                ret,
                enlisted,
                drMap,
                null,
                opCtx != null && opCtx.skipStore());

            if (pessimistic()) {
                // Loose all skipped.
                final Set<KeyCacheObject> loaded = loadFut.get();

                final Collection<KeyCacheObject> keys = F.view(enlisted, F0.notIn(loaded));

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for put on keys: " + keys);

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(keys,
                    lockTimeout(),
                    this,
                    false,
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override public GridCacheReturn postLock(GridCacheReturn ret)
                        throws IgniteCheckedException
                    {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for put on keys: " + keys);

                        postLockWrite(cacheCtx,
                            keys,
                            loaded,
                            ret,
                            /*remove*/false,
                            retval,
                            /*read*/false,
                            -1L,
                            filter,
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

                    try {
                        loadFut.get();
                    }
                    catch (IgniteCheckedException e) {
                        return new GridFinishedFuture<>(e);
                    }

                    return nonInterruptable(commitAsync().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn>() {
                        @Override
                        public GridCacheReturn applyx(IgniteInternalFuture<IgniteInternalTx> txFut) throws IgniteCheckedException {
                            txFut.get();

                            return implicitRes;
                        }
                    }));
                }
                else
                    return nonInterruptable(loadFut.chain(new CX1<IgniteInternalFuture<Set<KeyCacheObject>>, GridCacheReturn>() {
                        @Override
                        public GridCacheReturn applyx(IgniteInternalFuture<Set<KeyCacheObject>> f) throws IgniteCheckedException {
                            f.get();

                            return ret;
                        }
                    }));
            }
        }
        catch (RuntimeException e) {
            for (IgniteTxEntry txEntry : txMap.values()) {
                GridCacheEntryEx cached0 = txEntry.cached();

                if (cached0 != null)
                    txEntry.context().evicts().touch(cached0, topologyVersion());
            }

            throw e;
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync(
        GridCacheContext cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx cached,
        boolean retval,
        CacheEntryPredicate[] filter
    ) {
        return removeAllAsync0(cacheCtx, keys, null, cached, retval, filter);
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param drMap DR map.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if size of keys collection is 1.
     * @param filter Filter.
     * @return Future for asynchronous remove.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable final Collection<? extends K> keys,
        @Nullable Map<KeyCacheObject, GridCacheVersion> drMap,
        @Nullable GridCacheEntryEx cached,
        final boolean retval,
        @Nullable final CacheEntryPredicate[] filter) {
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

        assert keys0 != null;
        assert cached == null || keys0.size() == 1;

        if (log.isDebugEnabled())
            log.debug("Called removeAllAsync(...) [tx=" + this + ", keys=" + keys0 + ", implicit=" + implicit +
                ", retval=" + retval + "]");

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

        try {
            Collection<KeyCacheObject> enlisted = new ArrayList<>();

            CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            ExpiryPolicy plc;

            if (!F.isEmpty(filter))
                plc = opCtx != null ? opCtx.expiry() : null;
            else
                plc = null;

            final IgniteInternalFuture<Set<KeyCacheObject>> loadFut = enlistWrite(
                cacheCtx,
                keys0,
                /** cached entry */null,
                plc,
                implicit,
                /** lookup map */null,
                /** invoke map */null,
                /** invoke arguments */null,
                retval,
                /** lock only */false,
                filter,
                ret,
                enlisted,
                null,
                drMap,
                opCtx != null && opCtx.skipStore()
            );

            if (log.isDebugEnabled())
                log.debug("Remove keys: " + enlisted);

            // Acquire locks only after having added operation to the write set.
            // Otherwise, during rollback we will not know whether locks need
            // to be rolled back.
            if (pessimistic()) {
                // Loose all skipped.
                final Collection<KeyCacheObject> passedKeys = F.view(enlisted, F0.notIn(loadFut.get()));

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for remove on keys: " + passedKeys);

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(passedKeys,
                    lockTimeout(),
                    this,
                    false,
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override protected GridCacheReturn postLock(GridCacheReturn ret)
                        throws IgniteCheckedException
                    {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for remove on keys: " + passedKeys);

                        postLockWrite(cacheCtx,
                            passedKeys,
                            loadFut.get(),
                            ret,
                            /*remove*/true,
                            retval,
                            /*read*/false,
                            -1L,
                            filter,
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
                            txFut.get();

                            return implicitRes;
                        }
                    }));
                }
                else
                    return nonInterruptable(loadFut.chain(new CX1<IgniteInternalFuture<Set<KeyCacheObject>>, GridCacheReturn>() {
                        @Override public GridCacheReturn applyx(IgniteInternalFuture<Set<KeyCacheObject>> f)
                            throws IgniteCheckedException {
                            f.get();

                            return ret;
                        }
                    }));
            }
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(e);
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
     * Checks if portable values should be deserialized.
     *
     * @param cacheCtx Cache context.
     * @return {@code True} if portables should be deserialized, {@code false} otherwise.
     */
    private boolean deserializePortables(GridCacheContext cacheCtx) {
        CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

        return opCtx == null || !opCtx.isKeepPortable();
    }

    /**
     * Initializes read map.
     *
     * @return {@code True} if transaction was successfully  started.
     */
    public boolean init() {
        if (txMap == null) {
            txMap = new LinkedHashMap<>(txSize > 0 ? txSize : 16, 1.0f);

            readView = new IgniteTxMap(txMap, CU.reads());
            writeView = new IgniteTxMap(txMap, CU.writes());

            return cctx.tm().onStarted(this);
        }

        return true;
    }

    /**
     * Adds cache to the list of active caches in transaction.
     *
     * @param cacheCtx Cache context to add.
     * @throws IgniteCheckedException If caches already enlisted in this transaction are not compatible with given
     *      cache (e.g. they have different stores).
     */
    protected void addActiveCache(GridCacheContext cacheCtx) throws IgniteCheckedException {
        int cacheId = cacheCtx.cacheId();

        // Check if we can enlist new cache to transaction.
        if (!activeCacheIds.contains(cacheId)) {
            String err = cctx.verifyTxCompatibility(this, activeCacheIds, cacheCtx);

            if (err != null) {
                StringBuilder cacheNames = new StringBuilder();

                int idx = 0;

                for (Integer activeCacheId : activeCacheIds) {
                    cacheNames.append(cctx.cacheContext(activeCacheId).name());

                    if (idx++ < activeCacheIds.size() - 1)
                        cacheNames.append(", ");
                }

                throw new IgniteCheckedException("Failed to enlist new cache to existing transaction (" +
                    err +
                    ") [activeCaches=[" + cacheNames + "]" +
                    ", cacheName=" + cacheCtx.name() +
                    ", cacheSystem=" + cacheCtx.systemTx() +
                    ", txSystem=" + system() + ']');
            }
            else
                activeCacheIds.add(cacheId);
        }
    }

    /**
     * Checks transaction expiration.
     *
     * @throws IgniteCheckedException If transaction check failed.
     */
    protected void checkValid() throws IgniteCheckedException {
        if (isRollbackOnly()) {
            if (timedOut())
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

        if (remainingTime() == 0 && setRollbackOnly())
            throw new IgniteTxTimeoutCheckedException("Cache transaction timed out " +
                "(was rolled back automatically): " + this);
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
        boolean skipStore) {
        assert invokeArgs == null || op == TRANSFORM;

        IgniteTxKey key = entry.txKey();

        checkInternal(key);

        TransactionState state = state();

        assert state == TransactionState.ACTIVE || timedOut() :
            "Invalid tx state for adding entry [op=" + op + ", val=" + val + ", entry=" + entry + ", filter=" +
                Arrays.toString(filter) + ", txCtx=" + cctx.tm().txContextVersion() + ", tx=" + this + ']';

        IgniteTxEntry old = txMap.get(key);

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
                entryProcessor,
                invokeArgs,
                hasDrTtl ? drTtl : -1L,
                entry,
                filter,
                drVer,
                skipStore);

            txEntry.conflictExpireTime(drExpireTime);

            if (!hasDrTtl)
                txEntry.expiry(expiryPlc);

            txMap.put(key, txEntry);

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
            "size", (txMap == null ? 0 : txMap.size()));
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
     * @param stores Store managers.
     * @return If {@code isWriteToStoreFromDht} value same for all stores.
     */
    private boolean isWriteToStoreFromDhtValid(Collection<CacheStoreManager> stores) {
        if (stores != null && !stores.isEmpty()) {
            boolean exp = F.first(stores).isWriteToStoreFromDht();

            for (CacheStoreManager store : stores) {
                if (store.isWriteToStoreFromDht() != exp)
                    return false;
            }
        }

        return true;
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
            if (e != null) {
                setRollbackOnly();

                if (commit && commitAfterLock())
                    return rollbackAsync().chain(new C1<IgniteInternalFuture<IgniteInternalTx>, T>() {
                        @Override public T apply(IgniteInternalFuture<IgniteInternalTx> f) {
                            throw new GridClosureException(e);
                        }
                    });

                throw new GridClosureException(e);
            }

            if (!locked) {
                setRollbackOnly();

                final GridClosureException ex = new GridClosureException(new IgniteTxTimeoutCheckedException("Failed to " +
                    "acquire lock within provided timeout for transaction [timeout=" + timeout() +
                    ", tx=" + this + ']'));

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
