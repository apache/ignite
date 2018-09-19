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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.transactions.TransactionState.PREPARED;

/**
 *
 */
@SuppressWarnings("unchecked")
public final class GridDhtTxPrepareFuture extends GridCacheCompoundFuture<IgniteInternalTx, GridNearTxPrepareResponse>
    implements GridCacheVersionedFuture<GridNearTxPrepareResponse>, IgniteDiagnosticAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Error updater. */
    private static final AtomicReferenceFieldUpdater<GridDhtTxPrepareFuture, Throwable> ERR_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridDhtTxPrepareFuture.class, Throwable.class, "err");

    /** */
    private static final IgniteReducer<IgniteInternalTx, GridNearTxPrepareResponse> REDUCER =
        new IgniteReducer<IgniteInternalTx, GridNearTxPrepareResponse>() {
            @Override public boolean collect(IgniteInternalTx e) {
                return true;
            }

            @Override public GridNearTxPrepareResponse reduce() {
                // Nothing to aggregate.
                return null;
            }
        };

    /** Replied flag updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxPrepareFuture> REPLIED_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxPrepareFuture.class, "replied");

    /** Mapped flag updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxPrepareFuture> MAPPED_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxPrepareFuture.class, "mapped");

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    private static IgniteLogger msgLog;

    /** Context. */
    private GridCacheSharedContext<?, ?> cctx;

    /** Future ID. */
    private IgniteUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridDhtTxLocalAdapter tx;

    /** Near mappings. */
    private Map<UUID, GridDistributedTxMapping> nearMap;

    /** DHT mappings. */
    private Map<UUID, GridDistributedTxMapping> dhtMap;

    /** Error. */
    @SuppressWarnings("UnusedDeclaration")
    private volatile Throwable err;

    /** Replied flag. */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int replied;

    /** All replies flag. */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int mapped;

    /** Prepare request. */
    private GridNearTxPrepareRequest req;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Near mini future id. */
    private int nearMiniId;

    /** DHT versions map. */
    private Map<IgniteTxKey, GridCacheVersion> dhtVerMap;

    /** {@code True} if this is last prepare operation for node. */
    private boolean last;

    /** Needs return value flag. */
    private boolean retVal;

    /** Return value. */
    private GridCacheReturn ret;

    /** Keys that did not pass the filter. */
    private Collection<IgniteTxKey> filterFailedKeys;

    /** Keys that should be locked. */
    @GridToStringInclude
    private final Set<IgniteTxKey> lockKeys = new HashSet<>();

    /** Force keys future for correct transforms. */
    private IgniteInternalFuture<?> forceKeysFut;

    /** Locks ready flag. */
    private volatile boolean locksReady;

    /** */
    private boolean invoke;

    /** Timeout object. */
    private final PrepareTimeoutObject timeoutObj;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param timeout Timeout.
     * @param nearMiniId Near mini future id.
     * @param dhtVerMap DHT versions map.
     * @param last {@code True} if this is last prepare operation for node.
     * @param retVal Return value flag.
     */
    public GridDhtTxPrepareFuture(
        GridCacheSharedContext cctx,
        final GridDhtTxLocalAdapter tx,
        long timeout,
        int nearMiniId,
        Map<IgniteTxKey, GridCacheVersion> dhtVerMap,
        boolean last,
        boolean retVal
    ) {
        super(REDUCER);

        this.cctx = cctx;
        this.tx = tx;
        this.dhtVerMap = dhtVerMap;
        this.last = last;

        futId = IgniteUuid.randomUuid();

        this.nearMiniId = nearMiniId;

        if (log == null) {
            msgLog = cctx.txPrepareMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridDhtTxPrepareFuture.class);
        }

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        this.retVal = retVal;

        assert dhtMap != null;
        assert nearMap != null;

        timeoutObj = timeout > 0 ? new PrepareTimeoutObject(timeout) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteLogger logger() {
        return log;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Near mini future id.
     */
    int nearMiniId() {
        return nearMiniId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        boolean rmv;

        synchronized (this) {
            rmv = lockKeys.remove(entry.txKey());
        }

        return rmv && mapIfLocked();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Transaction.
     */
    public GridDhtTxLocalAdapter tx() {
        return tx;
    }

    /**
     * @return {@code True} if all locks are owned.
     */
    private boolean checkLocks() {
        if (!locksReady)
            return false;

        synchronized (this) {
            return lockKeys.isEmpty();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onNodeLeft();

                    return true;
                }
            }

        return false;
    }

    /**
     *
     */
    private void onEntriesLocked() {
        ret = new GridCacheReturn(null, tx.localResult(), true, null, true);

        for (IgniteTxEntry writeEntry : req.writes()) {
            IgniteTxEntry txEntry = tx.entry(writeEntry.txKey());

            assert txEntry != null : writeEntry;

            GridCacheContext cacheCtx = txEntry.context();

            GridCacheEntryEx cached = txEntry.cached();

            ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(txEntry);

            cctx.database().checkpointReadLock();

            try {
                if ((txEntry.op() == CREATE || txEntry.op() == UPDATE) &&
                    txEntry.conflictExpireTime() == CU.EXPIRE_TIME_CALCULATE) {
                    if (expiry != null) {
                        cached.unswap(true);

                        Duration duration = cached.hasValue() ?
                            expiry.getExpiryForUpdate() : expiry.getExpiryForCreation();

                        txEntry.ttl(CU.toTtl(duration));
                    }
                }

                boolean hasFilters = !F.isEmptyOrNulls(txEntry.filters()) && !F.isAlwaysTrue(txEntry.filters());

                CacheObject val;
                CacheObject oldVal = null;

                boolean readOld = hasFilters || retVal || txEntry.op() == DELETE || txEntry.op() == TRANSFORM ||
                    tx.nearOnOriginatingNode() || tx.hasInterceptor();

                if (readOld) {
                    boolean readThrough = !txEntry.skipStore() &&
                        (txEntry.op() == TRANSFORM || ((retVal || hasFilters) && cacheCtx.config().isLoadPreviousValue()));

                    boolean evt = retVal || txEntry.op() == TRANSFORM;

                    EntryProcessor entryProc = null;

                    if (evt && txEntry.op() == TRANSFORM)
                        entryProc = F.first(txEntry.entryProcessors()).get1();

                    final boolean keepBinary = txEntry.keepBinary();

                    val = oldVal = cached.innerGet(
                        null,
                        tx,
                        readThrough,
                        /*metrics*/retVal,
                        /*event*/evt,
                        tx.subjectId(),
                        entryProc,
                        tx.resolveTaskName(),
                        null,
                        keepBinary,
                        null);  // TODO IGNITE-7371

                    if (retVal || txEntry.op() == TRANSFORM) {
                        if (!F.isEmpty(txEntry.entryProcessors())) {
                            invoke = true;

                            if (txEntry.hasValue())
                                val = txEntry.value();

                            KeyCacheObject key = txEntry.key();

                            Object procRes = null;
                            Exception err = null;

                            boolean modified = false;

                            txEntry.oldValueOnPrimary(val != null);

                            for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : txEntry.entryProcessors()) {
                                CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry<>(key, val,
                                    txEntry.cached().version(), keepBinary, txEntry.cached());

                                IgniteThread.onEntryProcessorEntered(false);

                                try {
                                    EntryProcessor<Object, Object, Object> processor = t.get1();

                                    procRes = processor.process(invokeEntry, t.get2());

                                    val = cacheCtx.toCacheObject(invokeEntry.getValue(true));

                                    if (val != null) // no validation for remove case
                                        cacheCtx.validateKeyAndValue(key, val);
                                }
                                catch (Exception e) {
                                    err = e;

                                    break;
                                }
                                finally {
                                    IgniteThread.onEntryProcessorLeft();
                                }

                                modified |= invokeEntry.modified();
                            }

                            if (modified)
                                val = cacheCtx.toCacheObject(cacheCtx.unwrapTemporary(val));

                            GridCacheOperation op = modified ? (val == null ? DELETE : UPDATE) : NOOP;

                            if (op == NOOP) {
                                GridCacheAdapter<?, ?> cache = writeEntry.context().cache();

                                if (cache.context().statisticsEnabled())
                                    cache.metrics0().onReadOnlyInvoke(oldVal != null);

                                if (expiry != null) {
                                    long ttl = CU.toTtl(expiry.getExpiryForAccess());

                                    txEntry.ttl(ttl);

                                    if (ttl == CU.TTL_ZERO)
                                        op = DELETE;
                                }
                            }

                            txEntry.entryProcessorCalculatedValue(new T2<>(op, op == NOOP ? null : val));

                            if (retVal) {
                                if (err != null || procRes != null)
                                    ret.addEntryProcessResult(txEntry.context(), key, null, procRes, err, keepBinary);
                                else
                                    ret.invokeResult(true);
                            }
                        }
                        else if (retVal)
                            ret.value(cacheCtx, val, keepBinary);
                    }

                    if (hasFilters && !cacheCtx.isAll(cached, txEntry.filters())) {
                        if (expiry != null)
                            txEntry.ttl(CU.toTtl(expiry.getExpiryForAccess()));

                        txEntry.op(GridCacheOperation.NOOP);

                        if (filterFailedKeys == null)
                            filterFailedKeys = new ArrayList<>();

                        filterFailedKeys.add(cached.txKey());

                        ret.success(false);
                    }
                    else
                        ret.success(txEntry.op() != DELETE || cached.hasValue());
                }

                // Send old value in case if rebalancing is not finished.
                final boolean sndOldVal = !cacheCtx.isLocal() &&
                    !cacheCtx.topology().rebalanceFinished(tx.topologyVersion());

                if (sndOldVal) {
                    if (oldVal == null && !readOld) {
                        oldVal = cached.innerGet(
                            null,
                            tx,
                            /*readThrough*/false,
                            /*metrics*/false,
                            /*event*/false,
                            /*subjectId*/tx.subjectId(),
                            /*transformClo*/null,
                            /*taskName*/null,
                            /*expiryPlc*/null,
                            /*keepBinary*/true,
                            null); // TODO IGNITE-7371
                    }

                    if (oldVal != null)
                        oldVal.prepareMarshal(cacheCtx.cacheObjectContext());

                    txEntry.oldValue(oldVal);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to get result value for cache entry: " + cached, e);
            }
            catch (GridCacheEntryRemovedException e) {
                assert false : "Got entry removed exception while holding transactional lock on entry [e=" + e + ", cached=" + cached + ']';
            }
            finally {
                cctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param t Error.
     */
    public void onError(Throwable t) {
        onDone(null, t);
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridDhtTxPrepareResponse res) {
        if (isDone()) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("DHT prepare fut, response for finished future [txId=" + tx.nearXidVersion() +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }

            return;
        }

        MiniFuture mini = miniFuture(res.miniId());

        if (mini != null) {
            assert mini.node().id().equals(nodeId);

            mini.onResult(res);
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("DHT prepare fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private MiniFuture miniFuture(int miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (this) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                IgniteInternalFuture<IgniteInternalTx> fut = future(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

                if (mini.futureId() == miniId) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }

        return null;
    }

    /**
     * Marks all locks as ready for local transaction.
     */
    private void readyLocks() {
        // Ready all locks.
        if (log.isDebugEnabled())
            log.debug("Marking all local candidates as ready: " + this);

        readyLocks(req.writes());

        if (tx.serializable() && tx.optimistic())
            readyLocks(req.reads());

        locksReady = true;
    }

    /**
     * @param checkEntries Entries.
     */
    private void readyLocks(Iterable<IgniteTxEntry> checkEntries) {
        for (IgniteTxEntry txEntry : checkEntries) {
            GridCacheContext cacheCtx = txEntry.context();

            if (cacheCtx.isLocal())
                continue;

            GridDistributedCacheEntry entry = (GridDistributedCacheEntry)txEntry.cached();

            if (entry == null) {
                entry = (GridDistributedCacheEntry)cacheCtx.cache().entryEx(txEntry.key(), tx.topologyVersion());

                txEntry.cached(entry);
            }

            if (tx.optimistic() && txEntry.explicitVersion() == null) {
                synchronized (this) {
                    lockKeys.add(txEntry.txKey());
                }
            }

            while (true) {
                try {
                    assert txEntry.explicitVersion() == null || entry.lockedBy(txEntry.explicitVersion());

                    CacheLockCandidates owners = entry.readyLock(tx.xidVersion());

                    if (log.isDebugEnabled())
                        log.debug("Current lock owners for entry [owner=" + owners + ", entry=" + entry + ']');

                    break; // While.
                }
                // Possible if entry cached within transaction is obsolete.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in future onAllReplies method (will retry): " + txEntry);

                    entry = (GridDistributedCacheEntry)cacheCtx.cache().entryEx(txEntry.key(), tx.topologyVersion());

                    txEntry.cached(entry);
                }
            }
        }
    }

    /**
     * Checks if all ready locks are acquired and sends requests to remote nodes in this case.
     *
     * @return {@code True} if all locks are acquired, {@code false} otherwise.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private boolean mapIfLocked() {
        if (checkLocks()) {
            if (!MAPPED_UPD.compareAndSet(this, 0, 1))
                return false;

            if (forceKeysFut == null || (forceKeysFut.isDone() && forceKeysFut.error() == null))
                prepare0();
            else {
                forceKeysFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        try {
                            f.get();

                            prepare0();
                        }
                        catch (IgniteCheckedException e) {
                            onError(e);
                        }
                        finally {
                            cctx.txContextReset();
                        }
                    }
                });
            }

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(GridNearTxPrepareResponse res0, Throwable err) {
        assert err != null || (initialized() && !hasPending()) : "On done called for prepare future that has " +
            "pending mini futures: " + this;

        ERR_UPD.compareAndSet(this, null, err);

        // Must clear prepare future before response is sent or listeners are notified.
        if (tx.optimistic())
            tx.clearPrepareFuture(this);

        // Do not commit one-phase commit transaction if originating node has near cache enabled.
        if (tx.onePhaseCommit() && tx.commitOnPrepare()) {
            assert last;

            Throwable prepErr = this.err;

            // Must create prepare response before transaction is committed to grab correct return value.
            final GridNearTxPrepareResponse res = createPrepareResponse(prepErr);

            onComplete(res);

            if (tx.commitOnPrepare()) {
                if (tx.markFinalizing(IgniteInternalTx.FinalizationStatus.USER_FINISH)) {
                    IgniteInternalFuture<IgniteInternalTx> fut = null;

                    CIX1<IgniteInternalFuture<IgniteInternalTx>> resClo =
                        new CIX1<IgniteInternalFuture<IgniteInternalTx>>() {
                            @Override public void applyx(IgniteInternalFuture<IgniteInternalTx> fut) {
                                if(res.error() == null && fut.error() != null)
                                    res.error(fut.error());

                                if (REPLIED_UPD.compareAndSet(GridDhtTxPrepareFuture.this, 0, 1))
                                    sendPrepareResponse(res);
                            }
                        };

                    if (prepErr == null) {
                        try {
                            fut = tx.commitAsync();
                        }
                        catch (RuntimeException | Error e) {
                            Exception hEx = new IgniteTxHeuristicCheckedException("Commit produced a runtime " +
                                "exception: " + CU.txString(tx), e);

                            res.error(hEx);

                            tx.systemInvalidate(true);

                            try {
                                fut = tx.rollbackAsync();

                                fut.listen(resClo);
                            }
                            catch (Throwable e1) {
                                e.addSuppressed(e1);
                            }

                            throw e;
                        }

                    }
                    else if (!cctx.kernalContext().isStopping())
                        try {
                            fut = tx.rollbackAsync();
                        }
                        catch (Throwable e) {
                            err.addSuppressed(e);
                            fut = null;
                        }

                    if (fut != null)
                        fut.listen(resClo);
                }
            }
            else {
                if (REPLIED_UPD.compareAndSet(this, 0, 1))
                    sendPrepareResponse(res);
            }

            return true;
        }
        else {
            if (REPLIED_UPD.compareAndSet(this, 0, 1)) {
                GridNearTxPrepareResponse res = createPrepareResponse(this.err);

                // Will call super.onDone().
                onComplete(res);

                sendPrepareResponse(res);

                return true;
            }
            else {
                // Other thread is completing future. Wait for it to complete.
                try {
                    if (err != null)
                        get();
                }
                catch (IgniteInterruptedException e) {
                    onError(new IgniteCheckedException("Got interrupted while waiting for replies to be sent.", e));
                }
                catch (IgniteCheckedException ignored) {
                    // No-op, get() was just synchronization.
                }

                return false;
            }
        }
    }

    /**
     * @param res Response.
     */
    private void sendPrepareResponse(GridNearTxPrepareResponse res) {
        if (!tx.nearNodeId().equals(cctx.localNodeId())) {
            Throwable err = this.err;

            if (err != null && err instanceof IgniteFutureCancelledException) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT prepare fut, skip send response [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + tx.nearNodeId() +
                        ", err=" + err +
                        ", res=" + res + ']');
                }

                return;
            }

            try {
                cctx.io().send(tx.nearNodeId(), res, tx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT prepare fut, sent response [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + tx.nearNodeId() +
                        ", res=" + res + ']');
                }
            }
            catch (ClusterTopologyCheckedException e) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Failed to send prepare response, node left [txId=" + tx.nearXidVersion() + "," +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + tx.nearNodeId() +
                        ", res=" + res + ']');
                }
            }
            catch (IgniteCheckedException e) {
                U.error(msgLog, "Failed to send prepare response [txId=" + tx.nearXidVersion() + "," +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + tx.nearNodeId() +
                    ", res=" + res,
                    ", tx=" + tx + ']',
                    e);
            }
        }
    }

    /**
     * @param prepErr Error.
     * @return Prepare response.
     */
    private GridNearTxPrepareResponse createPrepareResponse(@Nullable Throwable prepErr) {
        assert F.isEmpty(tx.invalidPartitions());

        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
            -1,
            tx.nearXidVersion(),
            tx.colocated() ? tx.xid() : tx.nearFutureId(),
            nearMiniId,
            tx.xidVersion(),
            tx.writeVersion(),
            ret,
            prepErr,
            null,
            tx.onePhaseCommit(),
            tx.activeCachesDeploymentEnabled());

        res.mvccSnapshot(tx.mvccSnapshot());

        if (prepErr == null) {
            if (tx.needReturnValue() || tx.nearOnOriginatingNode() || tx.hasInterceptor())
                addDhtValues(res);

            GridCacheVersion min = tx.minVersion();

            if (tx.needsCompletedVersions()) {
                IgnitePair<Collection<GridCacheVersion>> versPair = cctx.tm().versions(min);

                res.completedVersions(versPair.get1(), versPair.get2());
            }

            res.pending(localDhtPendingVersions(tx.writeEntries(), min));

            tx.implicitSingleResult(ret);
        }

        res.filterFailedKeys(filterFailedKeys);

        return res;
    }

    /**
     * @param res Response being sent.
     */
    private void addDhtValues(GridNearTxPrepareResponse res) {
        // Interceptor on near node needs old values to execute callbacks.
        if (!F.isEmpty(req.writes())) {
            for (IgniteTxEntry e : req.writes()) {
                IgniteTxEntry txEntry = tx.entry(e.txKey());

                assert txEntry != null : "Missing tx entry for key [tx=" + tx + ", key=" + e.txKey() + ']';

                GridCacheContext cacheCtx = txEntry.context();

                while (true) {
                    try {
                        GridCacheEntryEx entry = txEntry.cached();

                        GridCacheVersion dhtVer = entry.version();

                        CacheObject val0 = entry.valueBytes();

                        if (val0 != null)
                            res.addOwnedValue(txEntry.txKey(), dhtVer, val0);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // Retry.
                        txEntry.cached(cacheCtx.cache().entryEx(txEntry.key(), tx.topologyVersion()));
                    }
                }
            }
        }

        for (Map.Entry<IgniteTxKey, GridCacheVersion> ver : dhtVerMap.entrySet()) {
            IgniteTxEntry txEntry = tx.entry(ver.getKey());

            if (res.hasOwnedValue(ver.getKey()))
                continue;

            assert txEntry != null : ver;

            GridCacheContext cacheCtx = txEntry.context();

            while (true) {
                try {
                    GridCacheEntryEx entry = txEntry.cached();

                    GridCacheVersion dhtVer = entry.version();

                    if (ver.getValue() == null || !ver.getValue().equals(dhtVer)) {
                        CacheObject val0 = entry.valueBytes();

                        res.addOwnedValue(txEntry.txKey(), dhtVer, val0);
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // Retry.
                    txEntry.cached(cacheCtx.cache().entryEx(txEntry.key(), tx.topologyVersion()));
                }
            }
        }
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * Completeness callback.
     *
     * @param res Response.
     * @return {@code True} if {@code done} flag was changed as a result of this call.
     */
    private boolean onComplete(@Nullable GridNearTxPrepareResponse res) {
        if ((last || tx.isSystemInvalidate()) && !(tx.near() && tx.local()))
            tx.state(PREPARED);

        if (super.onDone(res, res == null ? err : null)) {
            // Don't forget to clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * Completes this future.
     */
    public void complete() {
        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse();

        res.error(err != null ? err : new IgniteCheckedException("Failed to prepare transaction."));

        onComplete(res);
    }

    /**
     * Initializes future.
     *
     * @param req Prepare request.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void prepare(GridNearTxPrepareRequest req) {
        assert req != null;

        if (tx.empty() && !req.queryUpdate()) {
            tx.setRollbackOnly();

            onDone((GridNearTxPrepareResponse)null);
        }

        this.req = req;

        boolean ser = tx.serializable() && tx.optimistic();

        if (!F.isEmpty(req.writes()) || (ser && !F.isEmpty(req.reads()))) {
            Map<Integer, Collection<KeyCacheObject>> forceKeys = null;

            for (IgniteTxEntry entry : req.writes())
                forceKeys = checkNeedRebalanceKeys(entry, forceKeys);

            if (ser) {
                for (IgniteTxEntry entry : req.reads())
                    forceKeys = checkNeedRebalanceKeys(entry, forceKeys);
            }

            forceKeysFut = forceRebalanceKeys(forceKeys);
        }

        readyLocks();

        if (timeoutObj != null && !isDone()) {
            // Start timeout tracking after 'readyLocks' to avoid race with timeout processing.
            cctx.time().addTimeoutObject(timeoutObj);
        }

        mapIfLocked();
    }

    /**
     * Checks if this transaction needs previous value for the given tx entry. Will use passed in map to store
     * required key or will create new map if passed in map is {@code null}.
     *
     * @param e TX entry.
     * @param map Map with needed preload keys.
     * @return Map if it was created.
     */
    private Map<Integer, Collection<KeyCacheObject>> checkNeedRebalanceKeys(
        IgniteTxEntry e,
        Map<Integer, Collection<KeyCacheObject>> map
    ) {
        if (retVal ||
            !F.isEmpty(e.entryProcessors()) ||
            !F.isEmpty(e.filters()) ||
            e.entryReadVersion() != null) {
            if (map == null)
                map = new HashMap<>();

            Collection<KeyCacheObject> keys = map.get(e.cacheId());

            if (keys == null) {
                keys = new ArrayList<>();

                map.put(e.cacheId(), keys);
            }

            keys.add(e.key());
        }

        return map;
    }

    /**
     * @param keysMap Keys to request.
     * @return Keys request future.
     */
    private IgniteInternalFuture<Object> forceRebalanceKeys(Map<Integer, Collection<KeyCacheObject>> keysMap) {
        if (F.isEmpty(keysMap))
            return null;

        GridCompoundFuture<Object, Object> compFut = null;
        IgniteInternalFuture<Object> lastForceFut = null;

        for (Map.Entry<Integer, Collection<KeyCacheObject>> entry : keysMap.entrySet()) {
            if (lastForceFut != null && compFut == null) {
                compFut = new GridCompoundFuture();

                compFut.add(lastForceFut);
            }

            int cacheId = entry.getKey();

            Collection<KeyCacheObject> keys = entry.getValue();

            GridCacheContext ctx = cctx.cacheContext(cacheId);

            lastForceFut = ctx.group().preloader().request(ctx, keys, tx.topologyVersion());

            if (compFut != null && lastForceFut != null)
                compFut.add(lastForceFut);
        }

        if (compFut != null) {
            compFut.markInitialized();

            return compFut;
        }
        else
            return lastForceFut;
    }

    /**
     * @param entries Entries.
     * @return Not null exception if version check failed.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private IgniteCheckedException checkReadConflict(Iterable<IgniteTxEntry> entries)
        throws IgniteCheckedException {
        try {
            for (IgniteTxEntry entry : entries) {
                GridCacheVersion serReadVer = entry.entryReadVersion();

                if (serReadVer != null) {
                    entry.cached().unswap();

                    if (!entry.cached().checkSerializableReadVersion(serReadVer))
                        return versionCheckError(entry);
                }
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            assert false : "Got removed exception on entry with dht local candidate: " + entries;
        }

        return null;
    }

    /**
     * @param entry Entry.
     * @return Optimistic version check error.
     */
    private IgniteTxOptimisticCheckedException versionCheckError(IgniteTxEntry entry) {
        StringBuilder msg = new StringBuilder("Failed to prepare transaction, read/write conflict [");

        GridCacheContext cctx = entry.context();

        try {
            Object key = cctx.unwrapBinaryIfNeeded(entry.key(), entry.keepBinary(), false);

            assert key != null : entry.key();

            if (S.INCLUDE_SENSITIVE)
                msg.append("key=").append(key.toString()).append(", keyCls=").append(key.getClass().getName());
        }
        catch (Exception e) {
            msg.append("key=<failed to get key: ").append(e.toString()).append(">");
        }

        try {
            GridCacheEntryEx entryEx = entry.cached();

            CacheObject cacheVal = entryEx != null ? entryEx.rawGet() : null;

            Object val = cacheVal != null ? cctx.unwrapBinaryIfNeeded(cacheVal, entry.keepBinary(), false) : null;

            if (val != null) {
                if (S.INCLUDE_SENSITIVE)
                    msg.append(", val=").append(val.toString()).append(", valCls=").append(val.getClass().getName());
            }
            else
                msg.append(", val=null");
        }
        catch (Exception e) {
            msg.append(", val=<failed to get value: ").append(e.toString()).append(">");
        }

        msg.append(", cache=").append(cctx.name()).append(", thread=").append(Thread.currentThread()).append("]");

        return new IgniteTxOptimisticCheckedException(msg.toString());
    }

    /**
     *
     */
    private void prepare0() {
        boolean skipInit = false;

        try {
            if (tx.serializable() && tx.optimistic()) {
                IgniteCheckedException err0;

                try {
                    err0 = checkReadConflict(req.writes());

                    if (err0 == null)
                        err0 = checkReadConflict(req.reads());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to check entry version: " + e, e);

                    err0 = e;
                }

                if (err0 != null) {
                    ERR_UPD.compareAndSet(this, null, err0);

                    try {
                        tx.rollbackAsync();
                    }
                    catch (Throwable e) {
                        err0.addSuppressed(e);
                    }

                    final GridNearTxPrepareResponse res = createPrepareResponse(err);

                    onDone(res, res.error());

                    return;
                }
            }

            IgniteInternalFuture<MvccSnapshot> waitCrdCntrFut = null;

            if (req.requestMvccCounter()) {
                assert last;

                assert tx.txState().mvccEnabled(cctx);

                try {
                    // Request snapshot locally only because
                    // Mvcc Coordinator is expected to be local.
                    MvccSnapshot snapshot = cctx.coordinators().tryRequestSnapshotLocal(tx);

                    assert snapshot != null : tx.topologyVersion();

                    tx.mvccSnapshot(snapshot);
                }
                catch (ClusterTopologyCheckedException e) {
                    onDone(e);

                    return;
                }
            }

            onEntriesLocked();

            // We are holding transaction-level locks for entries here, so we can get next write version.
            tx.writeVersion(cctx.versions().next(tx.topologyVersion()));

            // Assign keys to primary nodes.
            if (!F.isEmpty(req.writes())) {
                for (IgniteTxEntry write : req.writes())
                    map(tx.entry(write.txKey()));
            }

            if (!F.isEmpty(req.reads())) {
                for (IgniteTxEntry read : req.reads())
                    map(tx.entry(read.txKey()));
            }

            if (isDone())
                return;

            if (last) {
                if (waitCrdCntrFut != null) {
                    skipInit = true;

                    waitCrdCntrFut.listen(new IgniteInClosure<IgniteInternalFuture<MvccSnapshot>>() {
                        @Override public void apply(IgniteInternalFuture<MvccSnapshot> fut) {
                            try {
                                fut.get();

                                sendPrepareRequests();

                                markInitialized();
                            }
                            catch (Throwable e) {
                                U.error(log, "Failed to get mvcc version for tx [txId=" + tx.nearXidVersion() +
                                    ", err=" + e + ']', e);

                                GridNearTxPrepareResponse res = createPrepareResponse(e);

                                onDone(res, res.error());
                            }
                        }
                    });
                }
                else
                    sendPrepareRequests();
            }
        }
        finally {
            if (!skipInit)
                markInitialized();
        }
    }

    /**
     *
     */
    private void sendPrepareRequests() {
        if (tx.onePhaseCommit() && !tx.nearMap().isEmpty()) {
            for (GridDistributedTxMapping nearMapping : tx.nearMap().values()) {
                if (!tx.dhtMap().containsKey(nearMapping.primary().id())) {
                    tx.onePhaseCommit(false);

                    break;
                }
            }
        }

        assert !tx.txState().mvccEnabled(cctx) || !tx.onePhaseCommit() || tx.mvccSnapshot() != null;

        int miniId = 0;

        assert tx.transactionNodes() != null;

        final long timeout = timeoutObj != null ? timeoutObj.timeout : 0;

        // Do not need process active transactions on backups.
        MvccSnapshot mvccSnapshot = tx.mvccSnapshot();

        if (mvccSnapshot != null)
            mvccSnapshot = mvccSnapshot.withoutActiveTransactions();

        // Create mini futures.
        for (GridDistributedTxMapping dhtMapping : tx.dhtMap().values()) {
            assert !dhtMapping.empty() || dhtMapping.queryUpdate();

            ClusterNode n = dhtMapping.primary();

            assert !n.isLocal();

            GridDistributedTxMapping nearMapping = tx.nearMap().get(n.id());

            Collection<IgniteTxEntry> nearWrites = nearMapping == null ? null : nearMapping.writes();

            Collection<IgniteTxEntry> dhtWrites = dhtMapping.writes();

            if (!dhtMapping.queryUpdate() && F.isEmpty(dhtWrites) && F.isEmpty(nearWrites))
                continue;

            MiniFuture fut = new MiniFuture(n.id(), ++miniId, dhtMapping, nearMapping);

            add(fut); // Append new future.

            assert req.transactionNodes() != null;

            GridDhtTxPrepareRequest req = new GridDhtTxPrepareRequest(
                futId,
                fut.futureId(),
                tx.topologyVersion(),
                tx,
                timeout,
                dhtWrites,
                nearWrites,
                this.req.transactionNodes(),
                tx.nearXidVersion(),
                true,
                tx.onePhaseCommit(),
                tx.subjectId(),
                tx.taskNameHash(),
                tx.activeCachesDeploymentEnabled(),
                tx.storeWriteThrough(),
                retVal,
                mvccSnapshot);

            req.queryUpdate(dhtMapping.queryUpdate());

            int idx = 0;

            for (IgniteTxEntry entry : dhtWrites) {
                try {
                    GridDhtCacheEntry cached = (GridDhtCacheEntry)entry.cached();

                    GridCacheContext<?, ?> cacheCtx = cached.context();

                    // Do not invalidate near entry on originating transaction node.
                    req.invalidateNearEntry(idx, !tx.nearNodeId().equals(n.id()) &&
                        cached.readerId(n.id()) != null);

                    if (cached.isNewLocked()) {
                        List<ClusterNode> owners = cacheCtx.topology().owners(cached.partition(),
                            tx != null ? tx.topologyVersion() : cacheCtx.affinity().affinityTopologyVersion());

                        // Do not preload if local node is a partition owner.
                        if (!owners.contains(cctx.localNode()))
                            req.markKeyForPreload(idx);
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    assert false : "Got removed exception on entry with dht local candidate: " + entry;
                }

                idx++;
            }

            if (!F.isEmpty(nearWrites)) {
                for (IgniteTxEntry entry : nearWrites) {
                    try {
                        if (entry.explicitVersion() == null) {
                            GridCacheMvccCandidate added = entry.cached().candidate(version());

                            assert added != null : "Missing candidate for cache entry:" + entry;
                            assert added.dhtLocal();

                            if (added.ownerVersion() != null)
                                req.owned(entry.txKey(), added.ownerVersion());
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        assert false : "Got removed exception on entry with dht local candidate: " + entry;
                    }
                }
            }

            assert req.transactionNodes() != null;

            try {
                cctx.io().send(n, req, tx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT prepare fut, sent request dht [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + n.id() + ']');
                }
            }
            catch (ClusterTopologyCheckedException ignored) {
                fut.onNodeLeft();
            }
            catch (IgniteCheckedException e) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT prepare fut, failed to send request dht [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + n.id() + ']');
                }

                fut.onResult(e);
            }
        }

        for (GridDistributedTxMapping nearMapping : tx.nearMap().values()) {
            if (!tx.dhtMap().containsKey(nearMapping.primary().id())) {
                if (tx.remainingTime() == -1)
                    return;

                MiniFuture fut = new MiniFuture(nearMapping.primary().id(), ++miniId, null, nearMapping);

                add(fut); // Append new future.

                GridDhtTxPrepareRequest req = new GridDhtTxPrepareRequest(
                    futId,
                    fut.futureId(),
                    tx.topologyVersion(),
                    tx,
                    timeout,
                    null,
                    nearMapping.writes(),
                    tx.transactionNodes(),
                    tx.nearXidVersion(),
                    true,
                    tx.onePhaseCommit(),
                    tx.subjectId(),
                    tx.taskNameHash(),
                    tx.activeCachesDeploymentEnabled(),
                    tx.storeWriteThrough(),
                    retVal,
                    mvccSnapshot);

                for (IgniteTxEntry entry : nearMapping.entries()) {
                    if (CU.writes().apply(entry)) {
                        try {
                            if (entry.explicitVersion() == null) {
                                GridCacheMvccCandidate added = entry.cached().candidate(version());

                                assert added != null : "Null candidate for non-group-lock entry " +
                                    "[added=" + added + ", entry=" + entry + ']';
                                assert added.dhtLocal() : "Got non-dht-local candidate for prepare future" +
                                    "[added=" + added + ", entry=" + entry + ']';

                                if (added != null && added.ownerVersion() != null)
                                    req.owned(entry.txKey(), added.ownerVersion());
                            }

                            break;
                        } catch (GridCacheEntryRemovedException ignore) {
                            assert false : "Got removed exception on entry with dht local candidate: " + entry;
                        }
                    }
                }

                assert req.transactionNodes() != null;

                try {
                    cctx.io().send(nearMapping.primary(), req, tx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("DHT prepare fut, sent request near [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + nearMapping.primary().id() + ']');
                    }
                }
                catch (ClusterTopologyCheckedException ignored) {
                    fut.onNodeLeft();
                }
                catch (IgniteCheckedException e) {
                    if (!cctx.kernalContext().isStopping()) {
                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("DHT prepare fut, failed to send request near [txId=" + tx.nearXidVersion() +
                                ", dhtTxId=" + tx.xidVersion() +
                                ", node=" + nearMapping.primary().id() + ']');
                        }

                        fut.onResult(e);
                    }
                    else {
                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("DHT prepare fut, failed to send request near, ignore [txId=" + tx.nearXidVersion() +
                                ", dhtTxId=" + tx.xidVersion() +
                                ", node=" + nearMapping.primary().id() +
                                ", err=" + e + ']');
                        }
                    }
                }
            }
        }
    }

    /**
     * @param entry Transaction entry.
     */
    private void map(IgniteTxEntry entry) {
        if (entry.cached().isLocal())
            return;

        GridDhtCacheEntry cached = (GridDhtCacheEntry)entry.cached();

        GridCacheContext cacheCtx = entry.context();

        GridDhtCacheAdapter<?, ?> dht = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

        ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(entry);

        if (expiry != null && (entry.op() == READ || entry.op() == NOOP)) {
            entry.op(NOOP);

            entry.ttl(CU.toTtl(expiry.getExpiryForAccess()));
        }

        while (true) {
            try {
                List<ClusterNode> dhtNodes = dht.topology().nodes(cached.partition(), tx.topologyVersion());

                assert !dhtNodes.isEmpty() && dhtNodes.get(0).id().equals(cctx.localNodeId()) :
                    "localNode = " + cctx.localNodeId() + ", dhtNodes = " + dhtNodes;

                if (log.isDebugEnabled())
                    log.debug("Mapping entry to DHT nodes [nodes=" + U.toShortString(dhtNodes) +
                        ", entry=" + entry + ']');

                for (int i = 1; i < dhtNodes.size(); i++) {
                    ClusterNode node = dhtNodes.get(i);

                    addMapping(entry, node, dhtMap);
                }

                Collection<UUID> readers = cached.readers();

                if (!F.isEmpty(readers)) {
                    for (UUID readerId : readers) {
                        if (readerId.equals(tx.nearNodeId()))
                            continue;

                        ClusterNode readerNode = cctx.discovery().node(readerId);

                        if (readerNode == null || canSkipNearReader(dht, readerNode, dhtNodes))
                            continue;

                        if (log.isDebugEnabled())
                            log.debug("Mapping entry to near node [node=" + readerNode + ", entry=" + entry + ']');

                        addMapping(entry, readerNode, nearMap);
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Entry has no near readers: " + entry);

                break;
            }
            catch (GridCacheEntryRemovedException ignore) {
                cached = dht.entryExx(entry.key(), tx.topologyVersion());

                entry.cached(cached);
            }
        }
    }

    /**
     * This method checks if we should skip mapping of an entry update to the near reader. We can skip the update
     * if the reader is a primary or a backup. If the reader is a partition owner, but not a primary or a backup,
     * we cannot skip the reader update and must attempt to update a near entry anyway.
     *
     * @param dhtCache DHT cache to check mapping.
     * @param readerNode Reader node.
     * @param dhtNodes Current DHT nodes (primary + backups first and other DHT nodes afterwards).
     * @return {@code true} if reader is either a primary or a backup.
     */
    private boolean canSkipNearReader(GridDhtCacheAdapter<?, ?> dhtCache, ClusterNode readerNode, List<ClusterNode> dhtNodes) {
        int limit = Math.min(dhtCache.configuration().getBackups() + 1, dhtNodes.size());

        for (int i = 0; i < limit; i++) {
            if (dhtNodes.get(i).id().equals(readerNode.id()))
                return true;
        }

        return false;
    }

    /**
     * @param entry Entry.
     * @param n Node.
     * @param globalMap Map.
     */
    private void addMapping(
        IgniteTxEntry entry,
        ClusterNode n,
        Map<UUID, GridDistributedTxMapping> globalMap
    ) {
        GridDistributedTxMapping global = globalMap.get(n.id());

        if (global == null)
            globalMap.put(n.id(), global = new GridDistributedTxMapping(n));

        global.add(entry);
    }

    /**
     * Collects versions of pending candidates versions less than base.
     *
     * @param entries Tx entries to process.
     * @param baseVer Base version.
     * @return Collection of pending candidates versions.
     */
    private Collection<GridCacheVersion> localDhtPendingVersions(Iterable<IgniteTxEntry> entries,
        GridCacheVersion baseVer) {
        Collection<GridCacheVersion> lessPending = new GridLeanSet<>(5);

        for (IgniteTxEntry entry : entries) {
            try {
                for (GridCacheMvccCandidate cand : entry.cached().localCandidates()) {
                    if (cand.version().isLess(baseVer))
                        lessPending.add(cand.version());
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op, no candidates.
            }
        }

        return lessPending;
    }

    /** {@inheritDoc} */
    @Override public void addDiagnosticRequest(IgniteDiagnosticPrepareContext req) {
        if (!isDone()) {
            for (IgniteInternalFuture fut : futures()) {
                if (!fut.isDone() && fut instanceof MiniFuture) {
                    MiniFuture f = (MiniFuture)fut;

                    if (!f.node().isLocal()) {
                        GridCacheVersion dhtVer = tx.xidVersion();
                        GridCacheVersion nearVer = tx.nearXidVersion();

                        req.remoteTxInfo(f.nodeId, dhtVer, nearVer, "GridDhtTxPrepareFuture " +
                            "waiting for response [node=" + f.nodeId +
                            ", topVer=" + tx.topologyVersion() +
                            ", dhtVer=" + dhtVer +
                            ", nearVer=" + nearVer +
                            ", futId=" + futId +
                            ", miniId=" + f.futId +
                            ", tx=" + tx + ']');

                        return;
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).node().id() +
                    ", loc=" + ((MiniFuture)f).node().isLocal() +
                    ", done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridDhtTxPrepareFuture.class, this,
            "xid", tx.xidVersion(),
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<IgniteInternalTx> {
        /** */
        private final int futId;

        /** Node ID. */
        private UUID nodeId;

        /** DHT mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping dhtMapping;

        /** Near mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping nearMapping;

        /**
         * @param nodeId Node ID.
         * @param futId Future ID.
         * @param dhtMapping Mapping.
         * @param nearMapping nearMapping.
         */
        MiniFuture(
            UUID nodeId,
            int futId,
            GridDistributedTxMapping dhtMapping,
            GridDistributedTxMapping nearMapping
        ) {
            assert dhtMapping == null || nearMapping == null || dhtMapping.primary().equals(nearMapping.primary());

            this.nodeId = nodeId;
            this.futId = futId;
            this.dhtMapping = dhtMapping;
            this.nearMapping = nearMapping;
        }

        /**
         * @return Future ID.
         */
        int futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return dhtMapping != null ? dhtMapping.primary() : nearMapping.primary();
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         */
        void onNodeLeft() {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("DHT prepare fut, mini future node left [txId=" + tx.nearXidVersion() +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + node().id() + ']');
            }

            if (tx != null)
                tx.removeMapping(nodeId);

            onDone(tx);
        }

        /**
         * @param res Result callback.
         */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void onResult(GridDhtTxPrepareResponse res) {
            if (res.error() != null)
                // Fail the whole compound future.
                onError(res.error());
            else {
                // Process evicted readers (no need to remap).
                if (nearMapping != null && !F.isEmpty(res.nearEvicted())) {
                    for (IgniteTxEntry entry : nearMapping.entries()) {
                        if (res.nearEvicted().contains(entry.txKey())) {
                            while (true) {
                                try {
                                    GridDhtCacheEntry cached = (GridDhtCacheEntry)entry.cached();

                                    cached.removeReader(nearMapping.primary().id(), res.messageId());

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    GridCacheEntryEx e = entry.context().cache().peekEx(entry.key());

                                    if (e == null)
                                        break;

                                    entry.cached(e);
                                }
                            }
                        }
                    }

                    nearMapping.evictReaders(res.nearEvicted());
                }

                // Process invalid partitions (no need to remap).
                if (!F.isEmpty(res.invalidPartitionsByCacheId())) {
                    Map<Integer, int[]> invalidPartsMap = res.invalidPartitionsByCacheId();

                    for (Iterator<IgniteTxEntry> it = dhtMapping.entries().iterator(); it.hasNext();) {
                        IgniteTxEntry entry  = it.next();

                        int[] invalidParts = invalidPartsMap.get(entry.cacheId());

                        if (invalidParts != null && F.contains(invalidParts, entry.cached().partition())) {
                            it.remove();

                            if (log.isDebugEnabled())
                                log.debug("Removed mapping for entry from dht mapping [key=" + entry.key() +
                                    ", tx=" + tx + ", dhtMapping=" + dhtMapping + ']');
                        }
                    }

                    if (!dhtMapping.queryUpdate() && dhtMapping.empty()) {
                        dhtMap.remove(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Removed mapping for node entirely because all partitions are invalid [nodeId=" +
                                nodeId + ", tx=" + tx + ']');
                    }
                }

                AffinityTopologyVersion topVer = tx.topologyVersion();

                boolean rec = cctx.gridEvents().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED);

                for (GridCacheEntryInfo info : res.preloadEntries()) {
                    GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(info.cacheId());

                    GridCacheAdapter<?, ?> cache0 = cacheCtx.cache();

                    if (cache0.isNear())
                        cache0 = ((GridNearCacheAdapter)cache0).dht();

                    while (true) {
                        GridCacheEntryEx entry = cache0.entryEx(info.key());

                        GridDrType drType = cacheCtx.isDrEnabled() ? GridDrType.DR_PRELOAD : GridDrType.DR_NONE;

                        cctx.database().checkpointReadLock();

                        try {
                            if (entry.initialValue(info.value(),
                                info.version(),
                                cacheCtx.mvccEnabled() ? ((MvccVersionAware)info).mvccVersion() : null,
                                cacheCtx.mvccEnabled() ? ((MvccUpdateVersionAware)info).newMvccVersion() : null,
                                cacheCtx.mvccEnabled() ? ((MvccVersionAware)info).mvccTxState() : TxState.NA,
                                cacheCtx.mvccEnabled() ? ((MvccUpdateVersionAware)info).newMvccTxState() : TxState.NA,
                                info.ttl(),
                                info.expireTime(),
                                true,
                                topVer,
                                drType,
                                false)) {
                                if (rec && !entry.isInternal())
                                    cacheCtx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                                        (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, info.value(), true, null,
                                        false, null, null, null, false);

                                if (retVal && !invoke)
                                    ret.value(cacheCtx, info.value(), false);
                            }

                            break;
                        }
                        catch (IgniteCheckedException e) {
                            // Fail the whole thing.
                            onDone(e);

                            return;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to set entry initial value (entry is obsolete, " +
                                    "will retry): " + entry);
                        }
                        finally {
                            cctx.database().checkpointReadUnlock();
                        }
                    }
                }

                // Finish mini future.
                onDone(tx);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }

    /**
     *
     */
    private class PrepareTimeoutObject extends GridTimeoutObjectAdapter {
        /** */
        private final long timeout;

        /**
         * @param timeout Timeout.
         */
        PrepareTimeoutObject(long timeout) {
            super(timeout);

            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            synchronized (GridDhtTxPrepareFuture.this) {
                clear();

                lockKeys.clear();
            }

            onError(new IgniteTxTimeoutCheckedException("Failed to acquire lock within " +
                "provided timeout for transaction [timeout=" + tx.timeout() + ", tx=" + tx + ']'));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PrepareTimeoutObject.class, this);
        }
    }
}
