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
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
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
public abstract class IgniteTxLocalAdapter<K, V> extends IgniteTxAdapter<K, V>
    implements IgniteTxLocalEx<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Per-transaction read map. */
    @GridToStringExclude
    protected Map<IgniteTxKey<K>, IgniteTxEntry<K, V>> txMap;

    /** Read view on transaction map. */
    @GridToStringExclude
    protected IgniteTxMap<K, V> readView;

    /** Write view on transaction map. */
    @GridToStringExclude
    protected IgniteTxMap<K, V> writeView;

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

    /** Flag indicating partition lock in group lock transaction. */
    private boolean partLock;

    /** Flag indicating that transformed values should be sent to remote nodes. */
    private boolean sndTransformedVals;

    /** Commit error. */
    protected AtomicReference<Throwable> commitErr = new AtomicReference<>();

    /** Active cache IDs. */
    protected Set<Integer> activeCacheIds = new HashSet<>();

    /** Need return value. */
    protected boolean needRetVal;

    /** Implicit transaction result. */
    protected GridCacheReturn<V> implicitRes = new GridCacheReturn<>(false);

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
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction and lock is acquired for whole partition.
     */
    protected IgniteTxLocalAdapter(
        GridCacheSharedContext<K, V> cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean storeEnabled,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(cctx, xidVer, implicit, implicitSingle, /*local*/true, sys, concurrency, isolation, timeout, invalidate,
            storeEnabled, txSize, grpLockKey, subjId, taskNameHash);

        assert !partLock || grpLockKey != null;

        this.partLock = partLock;

        minVer = xidVer;
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
    @Override public boolean partitionLock() {
        return partLock;
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
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
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
    @Override public boolean hasWriteKey(IgniteTxKey<K> key) {
        return writeView.containsKey(key);
    }

    /**
     * @return Transaction read set.
     */
    @Override public Set<IgniteTxKey<K>> readSet() {
        return txMap == null ? Collections.<IgniteTxKey<K>>emptySet() : readView.keySet();
    }

    /**
     * @return Transaction write set.
     */
    @Override public Set<IgniteTxKey<K>> writeSet() {
        return txMap == null ? Collections.<IgniteTxKey<K>>emptySet() : writeView.keySet();
    }

    /** {@inheritDoc} */
    @Override public boolean removed(IgniteTxKey<K> key) {
        if (txMap == null)
            return false;

        IgniteTxEntry<K, V> e = txMap.get(key);

        return e != null && e.op() == DELETE;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey<K>, IgniteTxEntry<K, V>> readMap() {
        return readView == null ? Collections.<IgniteTxKey<K>, IgniteTxEntry<K, V>>emptyMap() : readView;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey<K>, IgniteTxEntry<K, V>> writeMap() {
        return writeView == null ? Collections.<IgniteTxKey<K>, IgniteTxEntry<K, V>>emptyMap() : writeView;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry<K, V>> allEntries() {
        return txMap == null ? Collections.<IgniteTxEntry<K, V>>emptySet() : txMap.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry<K, V>> readEntries() {
        return readView == null ? Collections.<IgniteTxEntry<K, V>>emptyList() : readView.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry<K, V>> writeEntries() {
        return writeView == null ? Collections.<IgniteTxEntry<K, V>>emptyList() : writeView.values();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteTxEntry<K, V> entry(IgniteTxKey<K> key) {
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
    @Override public GridCacheReturn<V> implicitSingleResult() {
        return implicitRes;
    }

    /**
     * @param ret Result.
     */
    public void implicitSingleResult(GridCacheReturn<V> ret) {
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
    @Nullable @Override public GridTuple<V> peek(
        GridCacheContext<K, V> cacheCtx,
        boolean failFast,
        K key,
        IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) throws GridCacheFilterFailedException {
        IgniteTxEntry<K, V> e = txMap == null ? null : txMap.get(cacheCtx.txKey(key));

        if (e != null) {
            // We should look at tx entry previous value. If this is a user peek then previous
            // value is the same as value. If this is a filter evaluation peek then previous value holds
            // value visible to filter while value contains value enlisted for write.
            if (!F.isEmpty(filter) && !F.isAll(e.cached().wrapLazyValue(), filter))
                return e.hasPreviousValue() ? F.t(CU.<V>failed(failFast, e.previousValue())) : null;

            return e.hasPreviousValue() ? F.t(e.previousValue()) : null;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> loadMissing(
        final GridCacheContext<K, V> cacheCtx,
        final boolean readThrough,
        boolean async,
        final Collection<? extends K> keys,
        boolean deserializePortable,
        boolean skipVals,
        final IgniteBiInClosure<K, V> c
    ) {
        if (!async) {
            try {
                if (!readThrough || !cacheCtx.readThrough()) {
                    for (K key : keys)
                        c.apply(key, null);

                    return new GridFinishedFuture<>(cctx.kernalContext(), false);
                }

                return new GridFinishedFuture<>(cctx.kernalContext(),
                    cacheCtx.store().loadAllFromStore(this, keys, c));
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(cctx.kernalContext(), e);
            }
        }
        else
            return cctx.kernalContext().closure().callLocalSafe(
                new GPC<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        if (!readThrough || !cacheCtx.readThrough()) {
                            for (K key : keys)
                                c.apply(key, null);

                            return false;
                        }

                        return cacheCtx.store().loadAllFromStore(IgniteTxLocalAdapter.this, keys, c);
                    }
                },
                true);
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

            throw new IgniteCheckedException("Transaction validation produced a runtime exception: " + this, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void commit() throws IgniteCheckedException {
        try {
            commitAsync().get();
        }
        finally {
            cctx.tm().txContextReset();
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
    private void checkCommitLocks(GridCacheEntryEx<K, V> entry) {
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
    protected GridCacheEntryEx<K, V> entryEx(GridCacheContext<K, V> cacheCtx, IgniteTxKey<K> key) {
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
    protected GridCacheEntryEx<K, V> entryEx(GridCacheContext<K, V> cacheCtx, IgniteTxKey<K> key, long topVer) {
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
    protected void batchStoreCommit(Iterable<IgniteTxEntry<K, V>> writeEntries) throws IgniteCheckedException {
        GridCacheStoreManager<K, V> store = store();

        if (store != null && store.writeThrough() && storeEnabled() &&
            (!internal() || groupLock()) && (near() || store.writeToStoreFromDht())) {
            try {
                if (writeEntries != null) {
                    Map<K, IgniteBiTuple<V, GridCacheVersion>> putMap = null;
                    List<K> rmvCol = null;
                    GridCacheStoreManager<K, V> writeStore = null;

                    boolean skipNear = near() && store.writeToStoreFromDht();

                    for (IgniteTxEntry<K, V> e : writeEntries) {
                        if (skipNear && e.cached().isNear())
                            continue;

                        boolean intercept = e.context().config().getInterceptor() != null;

                        if (intercept || !F.isEmpty(e.entryProcessors()))
                            e.cached().unswap(true, false);

                        GridTuple3<GridCacheOperation, V, byte[]> res = applyTransformClosures(e, false);

                        GridCacheContext<K, V> cacheCtx = e.context();

                        GridCacheOperation op = res.get1();
                        K key = e.key();
                        V val = res.get2();
                        GridCacheVersion ver = writeVersion();

                        if (op == CREATE || op == UPDATE) {
                            // Batch-process all removes if needed.
                            if (rmvCol != null && !rmvCol.isEmpty()) {
                                assert writeStore != null;

                                writeStore.removeAllFromStore(this, rmvCol);

                                // Reset.
                                rmvCol.clear();

                                writeStore = null;
                            }

                            // Batch-process puts if cache ID has changed.
                            if (writeStore != null && writeStore != cacheCtx.store() && putMap != null && !putMap.isEmpty()) {
                                writeStore.putAllToStore(this, putMap);

                                // Reset.
                                putMap.clear();

                                writeStore = null;
                            }

                            if (intercept) {
                                V old = e.cached().rawGetOrUnmarshal(true);

                                val = (V)cacheCtx.config().getInterceptor().onBeforePut(key, old, val);

                                if (val == null)
                                    continue;

                                val = cacheCtx.unwrapTemporary(val);
                            }

                            if (putMap == null)
                                putMap = new LinkedHashMap<>(writeMap().size(), 1.0f);

                            putMap.put(key, F.t(val, ver));

                            writeStore = cacheCtx.store();
                        }
                        else if (op == DELETE) {
                            // Batch-process all puts if needed.
                            if (putMap != null && !putMap.isEmpty()) {
                                assert writeStore != null;

                                writeStore.putAllToStore(this, putMap);

                                // Reset.
                                putMap.clear();

                                writeStore = null;
                            }

                            if (writeStore != null && writeStore != cacheCtx.store() && rmvCol != null && !rmvCol.isEmpty()) {
                                writeStore.removeAllFromStore(this, rmvCol);

                                // Reset.
                                rmvCol.clear();

                                writeStore = null;
                            }

                            if (intercept) {
                                V old = e.cached().rawGetOrUnmarshal(true);

                                IgniteBiTuple<Boolean, V> t = cacheCtx.config().getInterceptor()
                                    .onBeforeRemove(key, old);

                                if (cacheCtx.cancelRemove(t))
                                    continue;
                            }

                            if (rmvCol == null)
                                rmvCol = new ArrayList<>();

                            rmvCol.add(key);

                            writeStore = cacheCtx.store();
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Ignoring NOOP entry for batch store commit: " + e);
                    }

                    if (putMap != null && !putMap.isEmpty()) {
                        assert rmvCol == null || rmvCol.isEmpty();
                        assert writeStore != null;

                        // Batch put at the end of transaction.
                        writeStore.putAllToStore(this, putMap);
                    }

                    if (rmvCol != null && !rmvCol.isEmpty()) {
                        assert putMap == null || putMap.isEmpty();
                        assert writeStore != null;

                        // Batch remove at the end of transaction.
                        writeStore.removeAllFromStore(this, rmvCol);
                    }
                }

                // Commit while locks are held.
                store.txEnd(this, true);
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

                throw new IgniteCheckedException("Failed to commit transaction to database: " + this, ex);
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

        if (groupLock())
            addGroupTxMapping(writeSet());

        if (!empty) {
            batchStoreCommit(writeMap().values());

            try {
                cctx.tm().txContext(this);

                long topVer = topologyVersion();

                /*
                 * Commit to cache. Note that for 'near' transaction we loop through all the entries.
                 */
                for (IgniteTxEntry<K, V> txEntry : (near() ? allEntries() : writeEntries())) {
                    GridCacheContext<K, V> cacheCtx = txEntry.context();

                    GridDrType drType = cacheCtx.isDrEnabled() ? DR_PRIMARY : DR_NONE;

                    UUID nodeId = txEntry.nodeId() == null ? this.nodeId : txEntry.nodeId();

                    try {
                        while (true) {
                            try {
                                GridCacheEntryEx<K, V> cached = txEntry.cached();

                                // Must try to evict near entries before committing from
                                // transaction manager to make sure locks are held.
                                if (!evictNearEntry(txEntry, false)) {
                                    if (cacheCtx.isNear() && cacheCtx.dr().receiveEnabled()) {
                                        cached.markObsolete(xidVer);

                                        break;
                                    }

                                    if (cached.detached())
                                        break;

                                    GridCacheEntryEx<K, V> nearCached = null;

                                    boolean metrics = true;

                                    if (updateNearCache(cacheCtx, txEntry.key(), topVer))
                                        nearCached = cacheCtx.dht().near().peekEx(txEntry.key());
                                    else if (cacheCtx.isNear() && txEntry.locallyMapped())
                                        metrics = false;

                                    boolean evt = !isNearLocallyMapped(txEntry, false);

                                    if (!F.isEmpty(txEntry.entryProcessors()) || !F.isEmpty(txEntry.filters()))
                                        txEntry.cached().unswap(true, false);

                                    GridTuple3<GridCacheOperation, V, byte[]> res = applyTransformClosures(txEntry,
                                        true);

                                    // For near local transactions we must record DHT version
                                    // in order to keep near entries on backup nodes until
                                    // backup remote transaction completes.
                                    if (cacheCtx.isNear()) {
                                        ((GridNearCacheEntry<K, V>)cached).recordDhtVersion(txEntry.dhtVersion());

                                        if (txEntry.op() == CREATE || txEntry.op() == UPDATE && txEntry.conflictExpireTime() == -1L) {
                                            ExpiryPolicy expiry = txEntry.expiry();

                                            if (expiry == null)
                                                expiry = cacheCtx.expiry();

                                            if (expiry != null) {
                                                Duration duration = cached.hasValue() ?
                                                    expiry.getExpiryForUpdate() : expiry.getExpiryForCreation();

                                                txEntry.ttl(CU.toTtl(duration));
                                            }
                                        }
                                    }

                                    GridCacheOperation op = res.get1();
                                    V val = res.get2();
                                    byte[] valBytes = res.get3();

                                    // Deal with DR conflicts.
                                    GridCacheVersion explicitVer = txEntry.conflictVersion() != null ?
                                        txEntry.conflictVersion() : writeVersion();

                                    if (op == CREATE || op == UPDATE && txEntry.conflictExpireTime() == -1L) {
                                        ExpiryPolicy expiry = txEntry.expiry();

                                        if (expiry == null)
                                            expiry = cacheCtx.expiry();

                                        if (expiry != null) {
                                            Duration duration = cached.hasValue() ?
                                                expiry.getExpiryForUpdate() : expiry.getExpiryForCreation();

                                            long ttl = CU.toTtl(duration);

                                            txEntry.ttl(ttl);

                                            if (ttl == CU.TTL_ZERO)
                                                op = DELETE;
                                        }
                                    }

                                    boolean drNeedResolve = cacheCtx.conflictNeedResolve();

                                    if (drNeedResolve) {
                                        IgniteBiTuple<GridCacheOperation, GridCacheVersionConflictContext<K, V>>
                                            drRes = conflictResolve(op, txEntry.key(), val, valBytes, txEntry.ttl(),
                                                txEntry.conflictExpireTime(), explicitVer, cached);

                                        assert drRes != null;

                                        GridCacheVersionConflictContext<K, V> conflictCtx = drRes.get2();

                                        if (conflictCtx.isUseOld())
                                            op = NOOP;
                                        else if (conflictCtx.isUseNew()) {
                                            txEntry.ttl(conflictCtx.ttl());

                                            if (conflictCtx.newEntry().dataCenterId() != cctx.dataCenterId())
                                                txEntry.conflictExpireTime(conflictCtx.expireTime());
                                            else
                                                txEntry.conflictExpireTime(-1L);
                                        }
                                        else {
                                            assert conflictCtx.isMerge();

                                            op = drRes.get1();
                                            val = conflictCtx.mergeValue();
                                            valBytes = null;
                                            explicitVer = writeVersion();

                                            txEntry.ttl(conflictCtx.ttl());
                                            txEntry.conflictExpireTime(-1L);
                                        }
                                    }
                                    else
                                        // Nullify explicit version so that innerSet/innerRemove will work as usual.
                                        explicitVer = null;

                                    if (sndTransformedVals || drNeedResolve) {
                                        assert sndTransformedVals && cacheCtx.isReplicated() || drNeedResolve;

                                        txEntry.value(val, true, false);
                                        txEntry.valueBytes(valBytes);
                                        txEntry.op(op);
                                        txEntry.entryProcessors(null);
                                        txEntry.conflictVersion(explicitVer);
                                    }

                                    if (op == CREATE || op == UPDATE) {
                                        GridCacheUpdateTxResult<V> updRes = cached.innerSet(
                                            this,
                                            eventNodeId(),
                                            txEntry.nodeId(),
                                            val,
                                            valBytes,
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
                                                valBytes,
                                                false,
                                                false,
                                                txEntry.ttl(),
                                                false,
                                                metrics,
                                                topVer,
                                                CU.<K, V>empty(),
                                                DR_NONE,
                                                txEntry.conflictExpireTime(),
                                                null,
                                                CU.subjectId(this, cctx),
                                                resolveTaskName());
                                    }
                                    else if (op == DELETE) {
                                        GridCacheUpdateTxResult<V> updRes = cached.innerRemove(
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
                                                CU.<K, V>empty(),
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
                                        ExpiryPolicy expiry = txEntry.expiry();

                                        if (expiry == null)
                                            expiry = cacheCtx.expiry();

                                        if (expiry != null) {
                                            Duration duration = expiry.getExpiryForAccess();

                                            if (duration != null)
                                                cached.updateTtl(null, CU.toTtl(duration));
                                        }

                                        if (log.isDebugEnabled())
                                            log.debug("Ignoring READ entry when committing: " + txEntry);
                                    }
                                    else {
                                        assert !groupLock() || txEntry.groupLockEntry() || ownsLock(txEntry.cached()):
                                            "Transaction does not own lock for group lock entry during  commit [tx=" +
                                                this + ", txEntry=" + txEntry + ']';

                                        if (txEntry.ttl() != CU.TTL_NOT_CHANGED)
                                            cached.updateTtl(null, txEntry.ttl());

                                        if (log.isDebugEnabled())
                                            log.debug("Ignoring NOOP entry when committing: " + txEntry);
                                    }
                                }

                                // Check commit locks after set, to make sure that
                                // we are not changing obsolete entries.
                                // (innerSet and innerRemove will throw an exception
                                // if an entry is obsolete).
                                if (txEntry.op() != READ && !txEntry.groupLockEntry())
                                    checkCommitLocks(cached);

                                // Break out of while loop.
                                break;
                            }
                            // If entry cached within transaction got removed.
                            catch (GridCacheEntryRemovedException ignored) {
                                if (log.isDebugEnabled())
                                    log.debug("Got removed entry during transaction commit (will retry): " + txEntry);

                                txEntry.cached(entryEx(cacheCtx, txEntry.txKey()), txEntry.keyBytes());
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
                            }

                            throw err;
                        }
                    }
                }
            }
            finally {
                cctx.tm().txContextReset();
            }
        }
        else {
            GridCacheStoreManager<K, V> store = store();

            if (store != null && (!internal() || groupLock())) {
                try {
                    store.txEnd(this, true);
                }
                catch (IgniteCheckedException e) {
                    commitError(e);

                    setRollbackOnly();

                    cctx.tm().removeCommittedTx(this);

                    throw e;
                }
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
                    for (IgniteTxEntry<K, V> e : allEntries())
                        evictNearEntry(e, false);

                cctx.tm().rollbackTx(this);

                GridCacheStoreManager<K, V> store = store();

                if (store != null && (near() || store.writeToStoreFromDht())) {
                    if (!internal() || groupLock())
                        store.txEnd(this, false);
                }
            }
            catch (Error | IgniteCheckedException | RuntimeException e) {
                U.addLastCause(e, commitErr.get(), log);

                throw e;
            }
        }
    }

    /**
     * Checks if there is a cached or swapped value for
     * {@link #getAllAsync(GridCacheContext, Collection, GridCacheEntryEx, boolean, boolean)} method.
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
     * @throws IgniteCheckedException If failed.
     * @return Enlisted keys.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private Collection<K> enlistRead(
        final GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        @Nullable ExpiryPolicy expiryPlc,
        Map<K, V> map,
        Map<K, GridCacheVersion> missed,
        int keysCnt,
        boolean deserializePortable,
        boolean skipVals
    ) throws IgniteCheckedException {
        assert !F.isEmpty(keys);
        assert keysCnt == keys.size();
        assert cached == null || F.first(keys).equals(cached.key());

        cacheCtx.checkSecurity(GridSecurityPermission.CACHE_READ);

        groupLockSanityCheck(cacheCtx, keys);

        boolean single = keysCnt == 1;

        Collection<K> lockKeys = null;

        long topVer = topologyVersion();

        // In this loop we cover only read-committed or optimistic transactions.
        // Transactions that are pessimistic and not read-committed are covered
        // outside of this loop.
        for (K key : keys) {
            if (key == null)
                continue;

            if (pessimistic() && !readCommitted() && !skipVals)
                addActiveCache(cacheCtx);

            IgniteTxKey<K> txKey = cacheCtx.txKey(key);

            // Check write map (always check writes first).
            IgniteTxEntry<K, V> txEntry = entry(txKey);

            // Either non-read-committed or there was a previous write.
            if (txEntry != null) {
                V val = txEntry.value();

                // Read value from locked entry in group-lock transaction as well.
                if (txEntry.hasValue()) {
                    if (!F.isEmpty(txEntry.entryProcessors()))
                        val = txEntry.applyEntryProcessors(val);

                    if (val != null) {
                        V val0 = val;

                        if (cacheCtx.portableEnabled())
                            val0 = (V)cacheCtx.unwrapPortableIfNeeded(val, !deserializePortable);

                        map.put(key, (V)CU.skipValue(val0, skipVals));
                    }
                }
                else {
                    assert txEntry.op() == TRANSFORM || (groupLock() && !txEntry.groupLockEntry());

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

                                V val0 = val;

                                if (cacheCtx.portableEnabled())
                                    val0 = (V)cacheCtx.unwrapPortableIfNeeded(val, !deserializePortable);

                                map.put(key, (V)CU.skipValue(val0, skipVals));
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
                            txEntry.cached(entryEx(cacheCtx, txEntry.txKey(), topVer), txEntry.keyBytes());
                        }
                    }
                }
            }
            // First time access within transaction.
            else {
                if (lockKeys == null && !skipVals)
                    lockKeys = single ? (Collection<K>)keys : new ArrayList<K>(keysCnt);

                if (!single && !skipVals)
                    lockKeys.add(key);

                while (true) {
                    GridCacheEntryEx<K, V> entry;

                    if (cached != null) {
                        entry = cached;

                        cached = null;
                    }
                    else
                        entry = entryEx(cacheCtx, txKey, topVer);

                    try {
                        GridCacheVersion ver = entry.version();

                        V val = null;

                        if (!pessimistic() || readCommitted() || groupLock() && !skipVals) {
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
                                V val0 = val;

                                if (cacheCtx.portableEnabled())
                                    val0 = (V)cacheCtx.unwrapPortableIfNeeded(val, !deserializePortable);

                                map.put(key, (V)CU.skipValue(val0, skipVals));
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
                                null);

                            if (groupLock())
                                txEntry.groupLockEntry(true);

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
                            V val = e.<V>value();

                            txEntry = addEntry(READ,
                                val,
                                null,
                                null,
                                entry,
                                expiryPlc,
                                CU.<K, V>empty(),
                                false,
                                -1L,
                                -1L,
                                null);

                            // Mark as checked immediately for non-pessimistic.
                            if (val != null && !pessimistic())
                                txEntry.markValid();
                        }

                        break; // While loop.
                    }
                }
            }
        }

        return lockKeys != null ? lockKeys : Collections.<K>emptyList();
    }

    /**
     * @param ctx Cache context.
     * @param key Key.
     * @param expiryPlc Expiry policy.
     * @return Expiry policy wrapper for entries accessed locally in optimistic transaction.
     */
    protected IgniteCacheExpiryPolicy accessPolicy(
        GridCacheContext ctx,
        IgniteTxKey<K> key,
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
    private Set<K> skip(Set<K> skipped, K key) {
        if (skipped == null)
            skipped = new GridLeanSet<>();

        skipped.add(key);

        if (log.isDebugEnabled())
            log.debug("Added key to skipped set: " + key);

        return skipped;
    }

    /**
     * Loads all missed keys for
     * {@link #getAllAsync(GridCacheContext, Collection, GridCacheEntryEx, boolean, boolean)} method.
     *
     * @param cacheCtx Cache context.
     * @param map Return map.
     * @param missedMap Missed keys.
     * @param redos Keys to retry.
     * @param deserializePortable Deserialize portable flag.
     * @return Loaded key-value pairs.
     */
    private IgniteInternalFuture<Map<K, V>> checkMissed(
        final GridCacheContext<K, V> cacheCtx,
        final Map<K, V> map,
        final Map<K, GridCacheVersion> missedMap,
        @Nullable final Collection<K> redos,
        final boolean deserializePortable,
        final boolean skipVals
    ) {
        assert redos != null || pessimistic();

        if (log.isDebugEnabled())
            log.debug("Loading missed values for missed map: " + missedMap);

        final Collection<K> loaded = new HashSet<>();

        return new GridEmbeddedFuture<>(cctx.kernalContext(),
            loadMissing(
                cacheCtx,
                true, false, missedMap.keySet(), deserializePortable, skipVals, new CI2<K, V>() {
                /** */
                private GridCacheVersion nextVer;

                @Override public void apply(K key, V val) {
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

                    V visibleVal = val;

                    IgniteTxKey<K> txKey = cacheCtx.txKey(key);

                    IgniteTxEntry<K, V> txEntry = entry(txKey);

                    if (txEntry != null) {
                        if (!readCommitted())
                            txEntry.readValue(val);

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
                        assert txEntry != null || readCommitted() || groupLock() || skipVals;

                        GridCacheEntryEx<K, V> e = txEntry == null ? entryEx(cacheCtx, txKey) : txEntry.cached();

                        try {
                            // Must initialize to true since even if filter didn't pass,
                            // we still record the transaction value.
                            boolean set;

                            try {
                                set = e.versionedValue(val, ver, nextVer);
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                if (log.isDebugEnabled())
                                    log.debug("Got removed entry in transaction getAll method " +
                                        "(will try again): " + e);

                                if (pessimistic() && !readCommitted() && !isRollbackOnly() &&
                                    (!groupLock() || F.eq(e.key(), groupLockKey()))) {
                                    U.error(log, "Inconsistent transaction state (entry got removed while " +
                                        "holding lock) [entry=" + e + ", tx=" + IgniteTxLocalAdapter.this + "]");

                                    setRollbackOnly();

                                    return;
                                }

                                if (txEntry != null)
                                    txEntry.cached(entryEx(cacheCtx, txKey), txEntry.keyBytes());

                                continue; // While loop.
                            }

                            // In pessimistic mode, we should always be able to set.
                            assert set || !pessimistic();

                            if (readCommitted() || groupLock() || skipVals) {
                                cacheCtx.evicts().touch(e, topologyVersion());

                                if (visibleVal != null)
                                    map.put(key, (V)CU.skipValue(visibleVal, skipVals));
                            }
                            else {
                                assert txEntry != null;

                                txEntry.setAndMarkValid(val);

                                if (visibleVal != null)
                                    map.put(key, visibleVal);
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
            }),
            new C2<Boolean, Exception, Map<K, V>>() {
                @Override public Map<K, V> apply(Boolean b, Exception e) {
                    if (e != null) {
                        setRollbackOnly();

                        throw new GridClosureException(e);
                    }

                    if (!b && !readCommitted()) {
                        // There is no store - we must mark the entries.
                        for (K key : missedMap.keySet()) {
                            IgniteTxEntry<K, V> txEntry = entry(cacheCtx.txKey(key));

                            if (txEntry != null)
                                txEntry.markValid();
                        }
                    }

                    if (readCommitted()) {
                        Collection<K> notFound = new HashSet<>(missedMap.keySet());

                        notFound.removeAll(loaded);

                        // In read-committed mode touch entries that have just been read.
                        for (K key : notFound) {
                            IgniteTxEntry<K, V> txEntry = entry(cacheCtx.txKey(key));

                            GridCacheEntryEx<K, V> entry = txEntry == null ? cacheCtx.cache().peekEx(key) :
                                txEntry.cached();

                            if (entry != null)
                                cacheCtx.evicts().touch(entry, topologyVersion());
                        }
                    }

                    return map;
                }
            });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        final GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        final boolean deserializePortable,
        final boolean skipVals) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(cctx.kernalContext(), Collections.<K, V>emptyMap());

        init();

        int keysCnt = keys.size();

        boolean single = keysCnt == 1;

        try {
            checkValid();

            final Map<K, V> retMap = new GridLeanMap<>(keysCnt);

            final Map<K, GridCacheVersion> missed = new GridLeanMap<>(pessimistic() ? keysCnt : 0);

            GridCacheProjectionImpl<K, V> prj = cacheCtx.projectionPerCall();

            ExpiryPolicy expiryPlc = prj != null ? prj.expiry() : null;

            final Collection<K> lockKeys = enlistRead(cacheCtx,
                keys,
                cached,
                expiryPlc,
                retMap,
                missed,
                keysCnt,
                deserializePortable,
                skipVals);

            if (single && missed.isEmpty())
                return new GridFinishedFuture<>(cctx.kernalContext(), retMap);

            // Handle locks.
            if (pessimistic() && !readCommitted() && !groupLock() && !skipVals) {
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
                    accessTtl,
                    CU.<K, V>empty());

                PLC2<Map<K, V>> plc2 = new PLC2<Map<K, V>>() {
                    @Override public IgniteInternalFuture<Map<K, V>> postLock() throws IgniteCheckedException {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for read on keys: " + lockKeys);

                        // Load keys only after the locks have been acquired.
                        for (K key : lockKeys) {
                            if (retMap.containsKey(key))
                                // We already have a return value.
                                continue;

                            IgniteTxKey<K> txKey = cacheCtx.txKey(key);

                            IgniteTxEntry<K, V> txEntry = entry(txKey);

                            assert txEntry != null;

                            // Check if there is cached value.
                            while (true) {
                                GridCacheEntryEx<K, V> cached = txEntry.cached();

                                try {
                                    Object transformClo =
                                        (!F.isEmpty(txEntry.entryProcessors()) &&
                                            cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                            F.first(txEntry.entryProcessors()) : null;

                                    V val = cached.innerGet(IgniteTxLocalAdapter.this,
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
                                        missed.remove(key);

                                        txEntry.setAndMarkValid(val);

                                        if (!F.isEmpty(txEntry.entryProcessors()))
                                            val = txEntry.applyEntryProcessors(val);

                                        if (cacheCtx.portableEnabled())
                                            val = (V)cacheCtx.unwrapPortableIfNeeded(val, !deserializePortable);

                                        retMap.put(key, val);
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

                                    txEntry.cached(entryEx(cacheCtx, txKey), txEntry.keyBytes());
                                }
                                catch (GridCacheFilterFailedException e) {
                                    // Failed value for the filter.
                                    V val = e.value();

                                    if (val != null) {
                                        // If filter fails after lock is acquired, we don't reload,
                                        // regardless if value is null or not.
                                        missed.remove(key);

                                        txEntry.setAndMarkValid(val);
                                    }

                                    break; // While.
                                }
                            }
                        }

                        if (!missed.isEmpty() && (cacheCtx.isReplicated() || cacheCtx.isLocal()))
                            return checkMissed(cacheCtx, retMap, missed, null, deserializePortable, skipVals);

                        return new GridFinishedFuture<>(cctx.kernalContext(), Collections.<K, V>emptyMap());
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
                            new GridFinishedFutureEx<>(finClos.apply(fut1.get(), null)) :
                            new GridEmbeddedFuture<>(cctx.kernalContext(), fut1, finClos);
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(cctx.kernalContext(), e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return plc2.apply(false, e);
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(cctx.kernalContext(), e1);
                        }
                    }
                }
                else {
                    return new GridEmbeddedFuture<>(
                        cctx.kernalContext(),
                        fut,
                        plc2,
                        finClos);
                }
            }
            else {
                assert optimistic() || readCommitted() || groupLock() || skipVals;

                final Collection<K> redos = new ArrayList<>();

                if (!missed.isEmpty()) {
                    if (!readCommitted())
                        for (Iterator<K> it = missed.keySet().iterator(); it.hasNext(); )
                            if (retMap.containsKey(it.next()))
                                it.remove();

                    if (missed.isEmpty())
                        return new GridFinishedFuture<>(cctx.kernalContext(), retMap);

                    return new GridEmbeddedFuture<>(
                        cctx.kernalContext(),
                        // First future.
                        checkMissed(cacheCtx, retMap, missed, redos, deserializePortable, skipVals),
                        // Closure that returns another future, based on result from first.
                        new PMC<Map<K, V>>() {
                            @Override public IgniteInternalFuture<Map<K, V>> postMiss(Map<K, V> map) {
                                if (redos.isEmpty())
                                    return new GridFinishedFuture<>(cctx.kernalContext(),
                                        Collections.<K, V>emptyMap());

                                if (log.isDebugEnabled())
                                    log.debug("Starting to future-recursively get values for keys: " + redos);

                                // Future recursion.
                                return getAllAsync(cacheCtx, redos, null, deserializePortable, skipVals);
                            }
                        },
                        // Finalize.
                        new FinishClosure<Map<K, V>>() {
                            @Override Map<K, V> finish(Map<K, V> loaded) {
                                for (Map.Entry<K, V> entry : loaded.entrySet()) {
                                    IgniteTxEntry<K, V> txEntry = entry(cacheCtx.txKey(entry.getKey()));

                                    V val = entry.getValue();

                                    if (!readCommitted())
                                        txEntry.readValue(val);

                                    if (!F.isEmpty(txEntry.entryProcessors()))
                                        val = txEntry.applyEntryProcessors(val);

                                    retMap.put(entry.getKey(), val);
                                }

                                return retMap;
                            }
                        }
                    );
                }

                return new GridFinishedFuture<>(cctx.kernalContext(), retMap);
            }
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn<V>> putAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, ? extends V> map,
        boolean retval,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        return (IgniteInternalFuture<GridCacheReturn<V>>)putAllAsync0(cacheCtx,
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
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, GridCacheDrInfo<V>> drMap
    ) {
        return putAllAsync0(cacheCtx,
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
    @Override public <T> IgniteInternalFuture<GridCacheReturn<Map<K, EntryProcessorResult<T>>>> invokeAsync(
        GridCacheContext<K, V> cacheCtx,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> map,
        Object... invokeArgs
    ) {
        return (IgniteInternalFuture<GridCacheReturn<Map<K, EntryProcessorResult<T>>>>)putAllAsync0(cacheCtx,
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
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, GridCacheVersion> drMap
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
    private boolean filter(GridCacheEntryEx<K, V> cached,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) throws IgniteCheckedException {
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
     * @return Future with skipped keys (the ones that didn't pass filter for pessimistic transactions).
     */
    protected IgniteInternalFuture<Set<K>> enlistWrite(
        final GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        @Nullable ExpiryPolicy expiryPlc,
        boolean implicit,
        @Nullable Map<? extends K, ? extends V> lookup,
        @Nullable Map<? extends K, EntryProcessor<K, V, Object>> invokeMap,
        @Nullable Object[] invokeArgs,
        boolean retval,
        boolean lockOnly,
        IgnitePredicate<Cache.Entry<K, V>>[] filter,
        final GridCacheReturn<V> ret,
        Collection<K> enlisted,
        @Nullable Map<? extends K, GridCacheDrInfo<V>> drPutMap,
        @Nullable Map<? extends K, GridCacheVersion> drRmvMap
    ) {
        assert cached == null || keys.size() == 1;
        assert cached == null || F.first(keys).equals(cached.key());

        try {
            addActiveCache(cacheCtx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        Set<K> skipped = null;

        boolean rmv = lookup == null && invokeMap == null;

        Set<K> missedForLoad = null;

        try {
            // Set transform flag for transaction.
            if (invokeMap != null)
                transform = true;

            groupLockSanityCheck(cacheCtx, keys);

            for (K key : keys) {
                if (key == null) {
                    setRollbackOnly();

                    throw new NullPointerException("Null key.");
                }

                V val = rmv || lookup == null ? null : lookup.get(key);
                EntryProcessor entryProcessor = invokeMap == null ? null : invokeMap.get(key);

                GridCacheVersion drVer;
                long drTtl;
                long drExpireTime;

                if (drPutMap != null) {
                    GridCacheDrInfo<V> info = drPutMap.get(key);

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

                if (cacheCtx.portableEnabled())
                    key = (K)cacheCtx.marshalToPortable(key);

                IgniteTxKey<K> txKey = cacheCtx.txKey(key);

                IgniteTxEntry<K, V> txEntry = entry(txKey);

                // First time access.
                if (txEntry == null) {
                    while (true) {
                        GridCacheEntryEx<K, V> entry;

                        if (cached != null) {
                            entry = cached;

                            cached = null;
                        }
                        else {
                            entry = entryEx(cacheCtx, txKey, topologyVersion());

                            entry.unswap(true, false);
                        }

                        try {
                            // Check if lock is being explicitly acquired by the same thread.
                            if (!implicit && cctx.kernalContext().config().isCacheSanityCheckEnabled() &&
                                entry.lockedByThread(threadId, xidVer))
                                throw new IgniteCheckedException("Cannot access key within transaction if lock is " +
                                    "externally held [key=" + key + ", entry=" + entry + ", xidVer=" + xidVer +
                                    ", threadId=" + threadId +
                                    ", locNodeId=" + cctx.localNodeId() + ']');

                            V old = null;

                            boolean readThrough = !F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter);

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
                                catch (ClusterTopologyException e) {
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
                                skipped = skip(skipped, key);

                                ret.set(old, false);

                                if (!readCommitted() && old != null) {
                                    // Enlist failed filters as reads for non-read-committed mode,
                                    // so future ops will get the same values.
                                    txEntry = addEntry(READ,
                                        old,
                                        null,
                                        null,
                                        entry,
                                        null,
                                        CU.<K, V>empty(),
                                        false,
                                        -1L,
                                        -1L,
                                        null);

                                    txEntry.markValid();
                                }

                                if (readCommitted() || old == null)
                                    cacheCtx.evicts().touch(entry, topologyVersion());

                                break; // While.
                            }

                            final GridCacheOperation op = lockOnly ? NOOP : rmv ? DELETE :
                                entryProcessor != null ? TRANSFORM : old != null ? UPDATE : CREATE;

                            txEntry = addEntry(op,
                                val,
                                entryProcessor,
                                invokeArgs,
                                entry,
                                expiryPlc,
                                filter,
                                true,
                                drTtl,
                                drExpireTime,
                                drVer);

                            if (!implicit() && readCommitted())
                                cacheCtx.evicts().touch(entry, topologyVersion());

                            if (groupLock() && !lockOnly)
                                txEntry.groupLockEntry(true);

                            enlisted.add(key);

                            if ((!pessimistic() && !implicit()) || (groupLock() && !lockOnly)) {
                                txEntry.markValid();

                                if (old == null) {
                                    boolean load = retval && !readThrough;

                                    if (load) {
                                        if (missedForLoad == null)
                                            missedForLoad = new HashSet<>();

                                        missedForLoad.add(key);
                                    }
                                    else {
                                        assert !transform;
                                        assert txEntry.op() != TRANSFORM;

                                        if (retval)
                                            ret.set(null, true);
                                        else
                                            ret.success(true);
                                    }
                                }
                                else {
                                    if (retval && !transform)
                                        ret.set(old, true);
                                    else {
                                        if (txEntry.op() == TRANSFORM)
                                            addInvokeResult(txEntry, old, ret);
                                        else
                                            ret.success(true);
                                    }
                                }
                            }
                            // Pessimistic.
                            else {
                                if (retval && !transform)
                                    ret.set(old, true);
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

                    GridCacheEntryEx<K, V> entry = txEntry.cached();

                    V v = txEntry.value();

                    boolean del = txEntry.op() == DELETE && rmv;

                    if (!del) {
                        if (!filter(entry, filter)) {
                            skipped = skip(skipped, key);

                            ret.set(v, false);

                            continue;
                        }

                        GridCacheOperation op = rmv ? DELETE : entryProcessor != null ? TRANSFORM :
                            v != null ? UPDATE : CREATE;

                        txEntry = addEntry(op,
                            val,
                            entryProcessor,
                            invokeArgs,
                            entry,
                            expiryPlc,
                            filter,
                            true,
                            drTtl,
                            drExpireTime,
                            drVer);

                        enlisted.add(key);

                        if (txEntry.op() == TRANSFORM)
                            addInvokeResult(txEntry, txEntry.value(), ret);
                    }

                    if (!pessimistic()) {
                        txEntry.markValid();

                        if (retval && !transform)
                            ret.set(v, true);
                        else
                            ret.success(true);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        if (missedForLoad != null) {
            IgniteInternalFuture<Boolean> fut = loadMissing(
                cacheCtx,
                /*read through*/cacheCtx.config().isLoadPreviousValue(),
                /*async*/true,
                missedForLoad,
                deserializePortables(cacheCtx),
                /*skip values*/false,
                new CI2<K, V>() {
                    @Override public void apply(K key, V val) {
                        if (log.isDebugEnabled())
                            log.debug("Loaded value from remote node [key=" + key + ", val=" + val + ']');

                        IgniteTxEntry<K, V> e = entry(new IgniteTxKey<>(key, cacheCtx.cacheId()));

                        assert e != null;

                        if (e.op() == TRANSFORM)
                            addInvokeResult(e, val, ret);
                        else
                            ret.set(val, true);
                    }
                });

            return new GridEmbeddedFuture<>(
                cctx.kernalContext(),
                fut,
                new C2<Boolean, Exception, Set<K>>() {
                    @Override public Set<K> apply(Boolean b, Exception e) {
                        if (e != null)
                            throw new GridClosureException(e);

                        return Collections.emptySet();
                    }
                }
            );
        }

        return new GridFinishedFuture<>(cctx.kernalContext(), skipped);
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
    protected Set<K> postLockWrite(
        GridCacheContext<K, V> cacheCtx,
        Iterable<? extends K> keys,
        Set<K> failed,
        GridCacheReturn ret,
        boolean rmv,
        boolean retval,
        boolean read,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter,
        boolean computeInvoke
    ) throws IgniteCheckedException {
        for (K k : keys) {
            IgniteTxEntry<K, V> txEntry = entry(cacheCtx.txKey(k));

            if (txEntry == null)
                throw new IgniteCheckedException("Transaction entry is null (most likely collection of keys passed into cache " +
                    "operation was changed before operation completed) [missingKey=" + k + ", tx=" + this + ']');

            while (true) {
                GridCacheEntryEx<K, V> cached = txEntry.cached();

                try {
                    assert cached.detached() || cached.lockedByThread(threadId) || isRollbackOnly() :
                        "Transaction lock is not acquired [entry=" + cached + ", tx=" + this +
                            ", nodeId=" + cctx.localNodeId() + ", threadId=" + threadId + ']';

                    if (log.isDebugEnabled())
                        log.debug("Post lock write entry: " + cached);

                    V v = txEntry.previousValue();
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
                                if (!hasPrevVal)
                                    v = cached.innerGet(this,
                                        /*swap*/true,
                                        /*read-through*/invoke || cacheCtx.loadPreviousValue(),
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
                            if (computeInvoke)
                                addInvokeResult(txEntry, v, ret);
                        }
                        else
                            ret.value(v);
                    }

                    boolean pass = cacheCtx.isAll(cached, filter);

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
                        txEntry.setAndMarkValid(txEntry.previousOperation(), (V)ret.value());
                        txEntry.filters(CU.<K, V>empty());
                        txEntry.filtersSet(false);

                        updateTtl = filter != cacheCtx.noPeekArray();
                    }

                    if (updateTtl) {
                        if (!read) {
                            ExpiryPolicy expiryPlc = txEntry.expiry() != null ? txEntry.expiry() : cacheCtx.expiry();

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

                    txEntry.cached(entryEx(cached.context(), txEntry.txKey()), txEntry.keyBytes());
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Entries that failed after lock filter check: " + failed);

        return failed;
    }

    /**
     * @param txEntry Entry.
     * @param val Value.
     * @param ret Return value to update.
     */
    private void addInvokeResult(IgniteTxEntry<K, V> txEntry, V val, GridCacheReturn ret) {
        try {
            Object res = null;

            for (T2<EntryProcessor<K, V, ?>, Object[]> t : txEntry.entryProcessors()) {
                CacheInvokeEntry<K, V> invokeEntry = new CacheInvokeEntry<>(txEntry.context(), txEntry.key(), val);

                EntryProcessor<K, V, ?> entryProcessor = t.get1();

                res = entryProcessor.process(invokeEntry, t.get2());

                val = invokeEntry.getValue();
            }

            if (res != null)
                ret.addEntryProcessResult(txEntry.key(), new CacheInvokeResult<>(res));
        }
        catch (Exception e) {
            ret.addEntryProcessResult(txEntry.key(), new CacheInvokeResult(e));
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
    private IgniteInternalFuture putAllAsync0(
        final GridCacheContext<K, V> cacheCtx,
        @Nullable Map<? extends K, ? extends V> map,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> invokeMap,
        @Nullable final Object[] invokeArgs,
        @Nullable final Map<? extends K, GridCacheDrInfo<V>> drMap,
        final boolean retval,
        @Nullable GridCacheEntryEx<K, V> cached,
        @Nullable final IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        assert filter == null || invokeMap == null;

        cacheCtx.checkSecurity(GridSecurityPermission.CACHE_PUT);

        if (retval)
            needReturnValue(true);

        // Cached entry may be passed only from entry wrapper.
        final Map<K, V> map0;
        final Map<K, EntryProcessor<K, V, Object>> invokeMap0;

        if (drMap != null) {
            assert map == null;

            map0 = (Map<K, V>)F.viewReadOnly(drMap, new IgniteClosure<GridCacheDrInfo<V>, V>() {
                @Override public V apply(GridCacheDrInfo<V> val) {
                    return val.value();
                }
            });

            invokeMap0 = null;
        }
        else if (cacheCtx.portableEnabled()) {
            if (map != null) {
                map0 = U.newHashMap(map.size());

                try {
                    for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
                        K key = (K)cacheCtx.marshalToPortable(e.getKey());
                        V val = (V)cacheCtx.marshalToPortable(e.getValue());

                        map0.put(key, val);
                    }
                }
                catch (IgniteException e) {
                    return new GridFinishedFuture<>(cctx.kernalContext(), e);
                }
            }
            else
                map0 = null;

            if (invokeMap != null) {
                invokeMap0 = U.newHashMap(invokeMap.size());

                try {
                    for (Map.Entry<? extends K, ? extends EntryProcessor<K, V, Object>> e : invokeMap.entrySet()) {
                        K key = (K)cacheCtx.marshalToPortable(e.getKey());

                        invokeMap0.put(key, e.getValue());
                    }
                }
                catch (IgniteException e) {
                    return new GridFinishedFuture<>(cctx.kernalContext(), e);
                }
            }
            else
                invokeMap0 = null;
        }
        else {
            map0 = (Map<K, V>)map;
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
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        init();

        final GridCacheReturn<V> ret = new GridCacheReturn<>(false);

        if (F.isEmpty(map0) && F.isEmpty(invokeMap0)) {
            if (implicit())
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(cctx.kernalContext(), e);
                }

            return new GridFinishedFuture<>(cctx.kernalContext(), ret.success(true));
        }

        try {
            Set<? extends K> keySet = map0 != null ? map0.keySet() : invokeMap0.keySet();

            Collection<K> enlisted = new ArrayList<>();

            GridCacheProjectionImpl<K, V> prj = cacheCtx.projectionPerCall();

            final IgniteInternalFuture<Set<K>> loadFut = enlistWrite(
                cacheCtx,
                keySet,
                cached,
                prj != null ? prj.expiry() : null,
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
                null);

            if (pessimistic() && !groupLock()) {
                // Loose all skipped.
                final Set<K> loaded = loadFut.get();

                final Collection<K> keys;

                if (keySet != null ) {
                    keys = new ArrayList<>(keySet.size());

                    for (K k : keySet) {
                        if (k != null && (loaded == null || !loaded.contains(k)))
                            keys.add(k);
                    }
                }
                else
                    keys = Collections.emptyList();

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for put on keys: " + keys);

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(keys,
                    lockTimeout(),
                    this,
                    false,
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    CU.<K, V>empty());

                PLC1<GridCacheReturn<V>> plc1 = new PLC1<GridCacheReturn<V>>(ret) {
                    @Override public GridCacheReturn<V> postLock(GridCacheReturn<V> ret) throws IgniteCheckedException {
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
                        return plc1.apply(fut.get(), null);
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(cctx.kernalContext(), e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return plc1.apply(false, e);
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(cctx.kernalContext(), e1);
                        }
                    }
                }
                else
                    return new GridEmbeddedFuture<>(
                        fut,
                        plc1,
                        cctx.kernalContext());
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
                        return new GridFinishedFutureEx<>(new GridCacheReturn<V>(), e);
                    }

                    return commitAsync().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn<V>>() {
                        @Override public GridCacheReturn<V> applyx(IgniteInternalFuture<IgniteInternalTx> txFut) throws IgniteCheckedException {
                            txFut.get();

                            return implicitRes;
                        }
                    });
                }
                else
                    return loadFut.chain(new CX1<IgniteInternalFuture<Set<K>>, GridCacheReturn<V>>() {
                        @Override public GridCacheReturn<V> applyx(IgniteInternalFuture<Set<K>> f) throws IgniteCheckedException {
                            f.get();

                            return ret;
                        }
                    });
            }
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn<V>> removeAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        boolean retval,
        IgnitePredicate<Cache.Entry<K, V>>[] filter
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
    private IgniteInternalFuture<GridCacheReturn<V>> removeAllAsync0(
        final GridCacheContext<K, V> cacheCtx,
        @Nullable final Collection<? extends K> keys,
        @Nullable Map<? extends  K, GridCacheVersion> drMap,
        @Nullable GridCacheEntryEx<K, V> cached,
        final boolean retval,
        @Nullable final IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        cacheCtx.checkSecurity(GridSecurityPermission.CACHE_REMOVE);

        if (retval)
            needReturnValue(true);

        final Collection<? extends K> keys0;

        if (drMap != null) {
            assert keys == null;

            keys0 = drMap.keySet();
        }
        else if (cacheCtx.portableEnabled()) {
            try {
                if (keys != null) {
                    Collection<K> pKeys = new ArrayList<>(keys.size());

                    for (K key : keys)
                        pKeys.add((K)cacheCtx.marshalToPortable(key));

                    keys0 = pKeys;
                }
                else
                    keys0 = null;
            }
            catch (IgniteException e) {
                return new GridFinishedFuture<>(cctx.kernalContext(), e);
            }
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
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        final GridCacheReturn<V> ret = new GridCacheReturn<>(false);

        if (F.isEmpty(keys0)) {
            if (implicit()) {
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(cctx.kernalContext(), e);
                }
            }

            return new GridFinishedFuture<>(cctx.kernalContext(), ret.success(true));
        }

        init();

        try {
            Collection<K> enlisted = new ArrayList<>();

            ExpiryPolicy plc;

            if (!F.isEmpty(filter)) {
                GridCacheProjectionImpl<K, V> prj = cacheCtx.projectionPerCall();

                plc = prj != null ? prj.expiry() : null;
            }
            else
                plc = null;

            final IgniteInternalFuture<Set<K>> loadFut = enlistWrite(
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
                drMap
            );

            if (log.isDebugEnabled())
                log.debug("Remove keys: " + enlisted);

            // Acquire locks only after having added operation to the write set.
            // Otherwise, during rollback we will not know whether locks need
            // to be rolled back.
            if (pessimistic() && !groupLock()) {
                // Loose all skipped.
                final Collection<? extends K> passedKeys = F.view(enlisted, F0.notIn(loadFut.get()));

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for remove on keys: " + passedKeys);

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(passedKeys,
                    lockTimeout(),
                    this,
                    false,
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    CU.<K, V>empty());

                PLC1<GridCacheReturn<V>> plc1 = new PLC1<GridCacheReturn<V>>(ret) {
                    @Override protected GridCacheReturn<V> postLock(GridCacheReturn<V> ret) throws IgniteCheckedException {
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
                        return plc1.apply(fut.get(), null);
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(cctx.kernalContext(), e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return plc1.apply(false, e);
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(cctx.kernalContext(), e1);
                        }
                    }
                }
                else
                    return new GridEmbeddedFuture<>(
                        fut,
                        plc1,
                        cctx.kernalContext());
            }
            else {
                if (implicit()) {
                    // Should never load missing values for implicit transaction as values will be returned
                    // with prepare response, if required.
                    assert loadFut.isDone();

                    return commitAsync().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn<V>>() {
                        @Override public GridCacheReturn<V> applyx(IgniteInternalFuture<IgniteInternalTx> txFut) throws IgniteCheckedException {
                            txFut.get();

                            return implicitRes;
                        }
                    });
                }
                else
                    return loadFut.chain(new CX1<IgniteInternalFuture<Set<K>>, GridCacheReturn<V>>() {
                        @Override public GridCacheReturn<V> applyx(IgniteInternalFuture<Set<K>> f) throws IgniteCheckedException {
                            f.get();

                            return ret;
                        }
                    });
            }
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /**
     * Checks if portable values should be deserialized.
     *
     * @param cacheCtx Cache context.
     * @return {@code True} if portables should be deserialized, {@code false} otherwise.
     */
    private boolean deserializePortables(GridCacheContext<K, V> cacheCtx) {
        GridCacheProjectionImpl<K, V> prj = cacheCtx.projectionPerCall();

        return prj == null || prj.deserializePortables();
    }

    /**
     * Adds key mapping to transaction.
     * @param keys Keys to add.
     */
    protected void addGroupTxMapping(Collection<IgniteTxKey<K>> keys) {
        // No-op. This method is overriden in transactions that store key to remote node mapping
        // for commit.
    }

    /**
     * Checks that affinity keys are enlisted in group transaction on start.
     *
     * @param cacheCtx Cache context.
     * @param keys Keys to check.
     * @throws IgniteCheckedException If sanity check failed.
     */
    private void groupLockSanityCheck(GridCacheContext<K, V> cacheCtx, Iterable<? extends K> keys) throws IgniteCheckedException {
        if (groupLock() && cctx.kernalContext().config().isCacheSanityCheckEnabled()) {
            // Note that affinity is called without mapper on purpose.
            int affinityPart = cacheCtx.config().getAffinity().partition(grpLockKey.key());

            for (K key : keys) {
                if (partitionLock()) {
                    int part = cacheCtx.affinity().partition(key);

                    if (affinityPart != part)
                        throw new IgniteCheckedException("Failed to enlist key into group-lock transaction (given " +
                            "key does not belong to locked partition) [key=" + key + ", affinityPart=" + affinityPart +
                            ", part=" + part + ", groupLockKey=" + grpLockKey + ']');
                }
                else {
                    IgniteTxKey affinityKey = cacheCtx.txKey(
                        (K)cacheCtx.config().getAffinityMapper().affinityKey(key));

                    if (!grpLockKey.equals(affinityKey))
                        throw new IgniteCheckedException("Failed to enlist key into group-lock transaction (affinity key was " +
                            "not enlisted to transaction on start) [key=" + key + ", affinityKey=" + affinityKey +
                            ", groupLockKey=" + grpLockKey + ']');
                }
            }
        }
    }

    /**
     * Performs keys locking for affinity-based group lock transactions.
     * @return Lock future.
     */
    @Override public IgniteInternalFuture<?> groupLockAsync(GridCacheContext<K, V> cacheCtx, Collection<K> keys) {
        assert groupLock();

        try {
            init();

            GridCacheReturn<V> ret = new GridCacheReturn<>(false);

            Collection<K> enlisted = new ArrayList<>();

            Set<K> skipped = enlistWrite(
                cacheCtx,
                keys,
                /** cached entry */null,
                /** expiry - leave unchanged */null,
                /** implicit */false,
                /** lookup map */null,
                /** invoke map */null,
                /** invoke arguments */null,
                /** retval */false,
                /** lock only */true,
                CU.<K, V>empty(),
                ret,
                enlisted,
                null,
                null
            ).get();

            // No keys should be skipped with empty filter.
            assert F.isEmpty(skipped);

            // Lock group key in pessimistic mode only.
            return pessimistic() ?
                cacheCtx.cache().txLockAsync(enlisted,
                    lockTimeout(),
                    this,
                    false,
                    false,
                    isolation,
                    isInvalidate(),
                    -1L,
                    CU.<K, V>empty()) :
                new GridFinishedFuture<>(cctx.kernalContext());
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<Object>(cctx.kernalContext(), e);
        }
    }

    /**
     * Initializes read map.
     *
     * @return {@code True} if transaction was successfully  started.
     */
    public boolean init() {
        if (txMap == null) {
            txMap = new LinkedHashMap<>(txSize > 0 ? txSize : 16, 1.0f);

            readView = new IgniteTxMap<>(txMap, CU.<K, V>reads());
            writeView = new IgniteTxMap<>(txMap, CU.<K, V>writes());

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
    protected void addActiveCache(GridCacheContext<K, V> cacheCtx) throws IgniteCheckedException {
        int cacheId = cacheCtx.cacheId();

        // Check if we can enlist new cache to transaction.
        if (!activeCacheIds.contains(cacheId)) {
            if (!cctx.txCompatible(this, activeCacheIds, cacheCtx)) {
                StringBuilder cacheNames = new StringBuilder();

                for (Integer activeCacheId : activeCacheIds) {
                    cacheNames.append(cctx.cacheContext(activeCacheId).name());

                    cacheNames.append(", ");
                }

                cacheNames.setLength(cacheNames.length() - 2);

                throw new IgniteCheckedException("Failed to enlist new cache to existing transaction " +
                    "(cache configurations are not compatible) [activeCaches=[" + cacheNames +
                    "], cacheName=" + cacheCtx.name() + ", txSystem=" + system() +
                    ", cacheSystem=" + cacheCtx.system() + ']');
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
     * @return Transaction entry.
     */
    protected final IgniteTxEntry<K, V> addEntry(GridCacheOperation op,
        @Nullable V val,
        @Nullable EntryProcessor entryProcessor,
        Object[] invokeArgs,
        GridCacheEntryEx<K, V> entry,
        @Nullable ExpiryPolicy expiryPlc,
        IgnitePredicate<Cache.Entry<K, V>>[] filter,
        boolean filtersSet,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer) {
        assert invokeArgs == null || op == TRANSFORM;

        IgniteTxKey<K> key = entry.txKey();

        checkInternal(key);

        TransactionState state = state();

        assert state == TransactionState.ACTIVE || timedOut() :
            "Invalid tx state for adding entry [op=" + op + ", val=" + val + ", entry=" + entry + ", filter=" +
                Arrays.toString(filter) + ", txCtx=" + cctx.tm().txContextVersion() + ", tx=" + this + ']';

        IgniteTxEntry<K, V> old = txMap.get(key);

        // Keep old filter if already have one (empty filter is always overridden).
        if (!filtersSet || !F.isEmptyOrNulls(filter)) {
            // Replace filter if previous filter failed.
            if (old != null && old.filtersSet())
                filter = old.filters();
        }

        IgniteTxEntry<K, V> txEntry;

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
            old.cached(entry, old.keyBytes());
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

            txEntry = new IgniteTxEntry<>(entry.context(),
                this,
                op,
                val,
                entryProcessor,
                invokeArgs,
                hasDrTtl ? drTtl : -1L,
                entry,
                filter,
                drVer);

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

                txEntry.cached(entry, txEntry.keyBytes());
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
    protected void updateExplicitVersion(IgniteTxEntry<K, V> txEntry, GridCacheEntryEx<K, V> entry)
        throws GridCacheEntryRemovedException {
        if (!entry.context().isDht()) {
            // All put operations must wait for async locks to complete,
            // so it is safe to get acquired locks.
            GridCacheMvccCandidate<K> explicitCand = entry.localOwner();

            if (explicitCand != null) {
                GridCacheVersion explicitVer = explicitCand.version();

                if (!explicitVer.equals(xidVer) && explicitCand.threadId() == threadId && !explicitCand.tx()) {
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
     * @param ttl Time to live.
     * @return {@code true} if tx entry exists for this key, {@code false} otherwise.
     */
    public boolean entryTtl(IgniteTxKey<K> key, long ttl) {
        assert key != null;

        IgniteTxEntry<K, V> e = entry(key);

        if (e != null) {
            e.ttl(ttl);
            e.conflictExpireTime(-1L);
        }

        return e != null;
    }

    /**
     * @param key Key.
     * @param expiryPlc Expiry policy.
     */
    void entryExpiry(IgniteTxKey<K> key, @Nullable ExpiryPolicy expiryPlc) {
        assert key != null;

        IgniteTxEntry<K, V> e = entry(key);

        if (e != null)
            e.expiry(expiryPlc);
    }

    /**
     * @param key Key.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @return {@code true} if tx entry exists for this key, {@code false} otherwise.
     */
    boolean entryTtlDr(IgniteTxKey<K> key, long ttl, long expireTime) {
        assert key != null;
        assert ttl >= 0;

        IgniteTxEntry<K, V> e = entry(key);

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
    public long entryTtl(IgniteTxKey<K> key) {
        assert key != null;

        IgniteTxEntry<K, V> e = entry(key);

        return e != null ? e.ttl() : 0;
    }

    /**
     * @param key Key.
     * @return Tx entry expire time.
     */
    public long entryExpireTime(IgniteTxKey<K> key) {
        assert key != null;

        IgniteTxEntry<K, V> e = entry(key);

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

                return new GridFinishedFuture<>(cctx.kernalContext(), r);
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
                        "within provided timeout for transaction [timeout=" + timeout() + ", tx=" + this + ']'));

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
