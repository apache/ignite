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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.util.F0;
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
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;
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
public final class GridDhtTxPrepareFuture extends GridCompoundFuture<IgniteInternalTx, GridNearTxPrepareResponse>
    implements GridCacheMvccFuture<GridNearTxPrepareResponse> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

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

    /** Logger. */
    private static IgniteLogger log;

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
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Replied flag. */
    private AtomicBoolean replied = new AtomicBoolean(false);

    /** All replies flag. */
    private AtomicBoolean mapped = new AtomicBoolean(false);

    /** Prepare reads. */
    private Iterable<IgniteTxEntry> reads;

    /** Prepare writes. */
    private Iterable<IgniteTxEntry> writes;

    /** Tx nodes. */
    private Map<UUID, Collection<UUID>> txNodes;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Near mini future id. */
    private IgniteUuid nearMiniId;

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

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param nearMiniId Near mini future id.
     * @param dhtVerMap DHT versions map.
     * @param last {@code True} if this is last prepare operation for node.
     * @param retVal Return value flag.
     */
    public GridDhtTxPrepareFuture(
        GridCacheSharedContext cctx,
        final GridDhtTxLocalAdapter tx,
        IgniteUuid nearMiniId,
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

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtTxPrepareFuture.class);

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        this.retVal = retVal;

        assert dhtMap != null;
        assert nearMap != null;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Near mini future id.
     */
    public IgniteUuid nearMiniId() {
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

        synchronized (lockKeys) {
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
    GridDhtTxLocalAdapter tx() {
        return tx;
    }

    /**
     * @return {@code True} if all locks are owned.
     */
    private boolean checkLocks() {
        if (!locksReady)
            return false;

        synchronized (lockKeys) {
            return lockKeys.isEmpty();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onNodeLeft(new ClusterTopologyCheckedException("Remote node left grid: " + nodeId));

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

        for (IgniteTxEntry writeEntry : writes) {
            IgniteTxEntry txEntry = tx.entry(writeEntry.txKey());

            assert txEntry != null : writeEntry;

            GridCacheContext cacheCtx = txEntry.context();

            GridCacheEntryEx cached = txEntry.cached();

            ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(txEntry);

            try {
                if ((txEntry.op() == CREATE || txEntry.op() == UPDATE) &&
                    txEntry.conflictExpireTime() == CU.EXPIRE_TIME_CALCULATE) {
                    if (expiry != null) {
                        Duration duration = cached.hasValue() ?
                            expiry.getExpiryForUpdate() : expiry.getExpiryForCreation();

                        txEntry.ttl(CU.toTtl(duration));
                    }
                }

                boolean hasFilters = !F.isEmptyOrNulls(txEntry.filters()) && !F.isAlwaysTrue(txEntry.filters());

                if (hasFilters || retVal || txEntry.op() == DELETE || txEntry.op() == TRANSFORM) {
                    cached.unswap(retVal);

                    boolean readThrough = (retVal || hasFilters) &&
                        cacheCtx.config().isLoadPreviousValue() &&
                        !txEntry.skipStore();

                    CacheObject val = cached.innerGet(
                        tx,
                        /*swap*/true,
                        readThrough,
                        /*fail fast*/false,
                        /*unmarshal*/true,
                        /*metrics*/retVal,
                        /*event*/retVal,
                        /*tmp*/false,
                        null,
                        null,
                        null,
                        null,
                        txEntry.keepBinary());

                    if (retVal || txEntry.op() == TRANSFORM) {
                        if (!F.isEmpty(txEntry.entryProcessors())) {
                            invoke = true;

                            if (txEntry.hasValue())
                                val = txEntry.value();

                            KeyCacheObject key = txEntry.key();

                            Object procRes = null;
                            Exception err = null;

                             for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : txEntry.entryProcessors()) {
                                try {
                                    CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry<>(
                                        txEntry.context(), key, val, txEntry.cached().version(), txEntry.keepBinary());

                                    EntryProcessor<Object, Object, Object> processor = t.get1();

                                    procRes = processor.process(invokeEntry, t.get2());

                                    val = cacheCtx.toCacheObject(invokeEntry.getValue());
                                }
                                catch (Exception e) {
                                    err = e;

                                    break;
                                }
                            }

                            txEntry.entryProcessorCalculatedValue(val);

                            if (retVal) {
                                if (err != null || procRes != null)
                                    ret.addEntryProcessResult(txEntry.context(), key, null, procRes, err);
                                else
                                    ret.invokeResult(true);
                            }
                        }
                        else if (retVal)
                            ret.value(cacheCtx, val, txEntry.keepBinary());
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
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to get result value for cache entry: " + cached, e);
            }
            catch (GridCacheEntryRemovedException e) {
                assert false : "Got entry removed exception while holding transactional lock on entry [e=" + e + ", cached=" + cached + ']';
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
        if (!isDone()) {
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.node().id().equals(nodeId);

                mini.onResult(res);
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
    private MiniFuture miniFuture(IgniteUuid miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (futs) {
            // Avoid iterator creation.
            for (int i = 0; i < futs.size(); i++) {
                IgniteInternalFuture<IgniteInternalTx> fut = futs.get(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

                if (mini.futureId().equals(miniId)) {
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

        readyLocks(writes);

        if (tx.serializable() && tx.optimistic())
            readyLocks(reads);

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
                entry = (GridDistributedCacheEntry)cacheCtx.cache().entryEx(txEntry.key());

                txEntry.cached(entry);
            }

            if (tx.optimistic() && txEntry.explicitVersion() == null) {
                synchronized (lockKeys) {
                    lockKeys.add(txEntry.txKey());
                }
            }

            while (true) {
                try {
                    assert txEntry.explicitVersion() == null || entry.lockedBy(txEntry.explicitVersion());

                    GridCacheMvccCandidate c = entry.readyLock(tx.xidVersion());

                    if (log.isDebugEnabled())
                        log.debug("Current lock owner for entry [owner=" + c + ", entry=" + entry + ']');

                    break; // While.
                }
                // Possible if entry cached within transaction is obsolete.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in future onAllReplies method (will retry): " + txEntry);

                    entry = (GridDistributedCacheEntry)cacheCtx.cache().entryEx(txEntry.key());

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
    private boolean mapIfLocked() {
        if (checkLocks()) {
            if (!mapped.compareAndSet(false, true))
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

        this.err.compareAndSet(null, err);

        // Must clear prepare future before response is sent or listeners are notified.
        if (tx.optimistic())
            tx.clearPrepareFuture(this);

        // Do not commit one-phase commit transaction if originating node has near cache enabled.
        if (tx.onePhaseCommit() && tx.commitOnPrepare()) {
            assert last;

            Throwable prepErr = this.err.get();

            // Must create prepare response before transaction is committed to grab correct return value.
            final GridNearTxPrepareResponse res = createPrepareResponse(prepErr);

            onComplete(res);

            if (tx.commitOnPrepare()) {
                if (tx.markFinalizing(IgniteInternalTx.FinalizationStatus.USER_FINISH)) {
                    IgniteInternalFuture<IgniteInternalTx> fut = null;

                    if (prepErr == null)
                        fut = tx.commitAsync();
                    else if (!cctx.kernalContext().isStopping())
                        fut = tx.rollbackAsync();

                    if (fut != null) {
                        fut.listen(new CIX1<IgniteInternalFuture<IgniteInternalTx>>() {
                            @Override public void applyx(IgniteInternalFuture<IgniteInternalTx> fut) {
                                try {
                                    if (replied.compareAndSet(false, true))
                                        sendPrepareResponse(res);
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to send prepare response for transaction: " + tx, e);
                                }
                            }
                        });
                    }
                }
            }
            else {
                try {
                    if (replied.compareAndSet(false, true))
                        sendPrepareResponse(res);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send prepare response for transaction: " + tx, e);
                }
            }

            return true;
        }
        else {
            if (replied.compareAndSet(false, true)) {
                GridNearTxPrepareResponse res = createPrepareResponse(this.err.get());

                try {
                    sendPrepareResponse(res);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send prepare response for transaction: " + tx, e);
                }
                finally {
                    // Will call super.onDone().
                    onComplete(res);
                }

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
     * @throws IgniteCheckedException If failed to send response.
     */
    private void sendPrepareResponse(GridNearTxPrepareResponse res) throws IgniteCheckedException {
        if (!tx.nearNodeId().equals(cctx.localNodeId())) {
            Throwable err = this.err.get();

            if (err != null && err instanceof IgniteFutureCancelledException)
                return;

            cctx.io().send(tx.nearNodeId(), res, tx.ioPolicy());
        }
    }

    /**
     * @param prepErr Error.
     * @return Prepare response.
     */
    private GridNearTxPrepareResponse createPrepareResponse(@Nullable Throwable prepErr) {
        assert F.isEmpty(tx.invalidPartitions());

        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
            tx.nearXidVersion(),
            tx.colocated() ? tx.xid() : tx.nearFutureId(),
            nearMiniId == null ? tx.xid() : nearMiniId,
            tx.xidVersion(),
            tx.writeVersion(),
            ret,
            prepErr,
            null,
            tx.activeCachesDeploymentEnabled());

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
        if (!F.isEmpty(writes)) {
            for (IgniteTxEntry e : writes) {
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
                        txEntry.cached(cacheCtx.cache().entryEx(txEntry.key()));
                    }
                }
            }
        }

        for (Map.Entry<IgniteTxKey, GridCacheVersion> ver : dhtVerMap.entrySet()) {
            IgniteTxEntry txEntry = tx.entry(ver.getKey());

            if (res.hasOwnedValue(ver.getKey()))
                continue;

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
                    txEntry.cached(cacheCtx.cache().entryEx(txEntry.key()));
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
        if (last || tx.isSystemInvalidate())
            tx.state(PREPARED);

        if (super.onDone(res, err.get())) {
            // Don't forget to clean up.
            cctx.mvcc().removeMvccFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Completes this future.
     */
    public void complete() {
        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse();

        res.error(new IgniteCheckedException("Failed to prepare transaction."));

        onComplete(res);
    }

    /**
     * Initializes future.
     *
     * @param reads Read entries.
     * @param writes Write entries.
     * @param txNodes Transaction nodes mapping.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void prepare(Collection<IgniteTxEntry> reads, Collection<IgniteTxEntry> writes,
        Map<UUID, Collection<UUID>> txNodes) {
        if (tx.empty()) {
            tx.setRollbackOnly();

            onDone((GridNearTxPrepareResponse)null);
        }

        this.reads = reads;
        this.writes = writes;
        this.txNodes = txNodes;

        boolean ser = tx.serializable() && tx.optimistic();

        if (!F.isEmpty(writes) || (ser && !F.isEmpty(reads))) {
            Map<Integer, Collection<KeyCacheObject>> forceKeys = null;

            for (IgniteTxEntry entry : writes)
                forceKeys = checkNeedRebalanceKeys(entry, forceKeys);

            if (ser) {
                for (IgniteTxEntry entry : reads)
                    forceKeys = checkNeedRebalanceKeys(entry, forceKeys);
            }

            forceKeysFut = forceRebalanceKeys(forceKeys);
        }

        readyLocks();

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
            e.serializableReadVersion() != null) {
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

            lastForceFut = cctx.cacheContext(cacheId).preloader().request(keys, tx.topologyVersion());

            if (compFut != null)
                compFut.add(lastForceFut);
        }

        if (compFut != null) {
            compFut.markInitialized();

            return compFut;
        }
        else {
            assert lastForceFut != null;

            return lastForceFut;
        }
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
                GridCacheVersion serReadVer = entry.serializableReadVersion();

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
        GridCacheContext cctx = entry.context();

        return new IgniteTxOptimisticCheckedException("Failed to prepare transaction, " +
            "read/write conflict [key=" + entry.key().value(cctx.cacheObjectContext(), false) +
            ", cache=" + cctx.name() + ']');
    }

    /**
     *
     */
    private void prepare0() {
        try {
            if (tx.serializable() && tx.optimistic()) {
                IgniteCheckedException err0;

                try {
                    err0 = checkReadConflict(writes);

                    if (err0 == null)
                        err0 = checkReadConflict(reads);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to check entry version: " + e, e);

                    err0 = e;
                }

                if (err0 != null) {
                    err.compareAndSet(null, err0);

                    tx.rollbackAsync();

                    final GridNearTxPrepareResponse res = createPrepareResponse(err.get());

                    onDone(res, res.error());

                    return;
                }
            }

            // We are holding transaction-level locks for entries here, so we can get next write version.
            onEntriesLocked();

            // We are holding transaction-level locks for entries here, so we can get next write version.
            tx.writeVersion(cctx.versions().next(tx.topologyVersion()));

            {
                // Assign keys to primary nodes.
                if (!F.isEmpty(writes)) {
                    for (IgniteTxEntry write : writes)
                        map(tx.entry(write.txKey()));
                }

                if (!F.isEmpty(reads)) {
                    for (IgniteTxEntry read : reads)
                        map(tx.entry(read.txKey()));
                }
            }

            if (isDone())
                return;

            if (last) {
                assert tx.transactionNodes() != null;

                // Create mini futures.
                for (GridDistributedTxMapping dhtMapping : tx.dhtMap().values()) {
                    assert !dhtMapping.empty();

                    ClusterNode n = dhtMapping.node();

                    assert !n.isLocal();

                    GridDistributedTxMapping nearMapping = tx.nearMap().get(n.id());

                    Collection<IgniteTxEntry> nearWrites = nearMapping == null ? null : nearMapping.writes();

                    Collection<IgniteTxEntry> dhtWrites = dhtMapping.writes();

                    if (F.isEmpty(dhtWrites) && F.isEmpty(nearWrites))
                        continue;

                    MiniFuture fut = new MiniFuture(n.id(), dhtMapping, nearMapping);

                    add(fut); // Append new future.

                    assert txNodes != null;

                    GridDhtTxPrepareRequest req = new GridDhtTxPrepareRequest(
                        futId,
                        fut.futureId(),
                        tx.topologyVersion(),
                        tx,
                        dhtWrites,
                        nearWrites,
                        txNodes,
                        tx.nearXidVersion(),
                        true,
                        tx.onePhaseCommit(),
                        tx.subjectId(),
                        tx.taskNameHash(),
                        tx.activeCachesDeploymentEnabled());

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
                    }
                    catch (ClusterTopologyCheckedException e) {
                        fut.onNodeLeft(e);
                    }
                    catch (IgniteCheckedException e) {
                        if (!cctx.kernalContext().isStopping())
                            fut.onResult(e);
                    }
                }

                for (GridDistributedTxMapping nearMapping : tx.nearMap().values()) {
                    if (!tx.dhtMap().containsKey(nearMapping.node().id())) {
                        assert nearMapping.writes() != null;

                        MiniFuture fut = new MiniFuture(nearMapping.node().id(), null, nearMapping);

                        add(fut); // Append new future.

                        GridDhtTxPrepareRequest req = new GridDhtTxPrepareRequest(
                            futId,
                            fut.futureId(),
                            tx.topologyVersion(),
                            tx,
                            null,
                            nearMapping.writes(),
                            tx.transactionNodes(),
                            tx.nearXidVersion(),
                            true,
                            tx.onePhaseCommit(),
                            tx.subjectId(),
                            tx.taskNameHash(),
                            tx.activeCachesDeploymentEnabled());

                        for (IgniteTxEntry entry : nearMapping.writes()) {
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
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                assert false : "Got removed exception on entry with dht local candidate: " + entry;
                            }
                        }

                        assert req.transactionNodes() != null;

                        try {
                            cctx.io().send(nearMapping.node(), req, tx.system() ? UTILITY_CACHE_POOL : SYSTEM_POOL);
                        }
                        catch (ClusterTopologyCheckedException e) {
                            fut.onNodeLeft(e);
                        }
                        catch (IgniteCheckedException e) {
                            if (!cctx.kernalContext().isStopping())
                                fut.onResult(e);
                        }
                    }
                }
            }
        }
        finally {
            markInitialized();
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
                Collection<ClusterNode> dhtNodes = dht.topology().nodes(cached.partition(), tx.topologyVersion());

                if (log.isDebugEnabled())
                    log.debug("Mapping entry to DHT nodes [nodes=" + U.toShortString(dhtNodes) +
                        ", entry=" + entry + ']');

                // Exclude local node.
                map(entry, F.view(dhtNodes, F.remoteNodes(cctx.localNodeId())), dhtMap);

                Collection<UUID> readers = cached.readers();

                if (!F.isEmpty(readers)) {
                    Collection<ClusterNode> nearNodes =
                        cctx.discovery().nodes(readers, F0.not(F.idForNodeId(tx.nearNodeId())));

                    if (log.isDebugEnabled())
                        log.debug("Mapping entry to near nodes [nodes=" + U.toShortString(nearNodes) +
                            ", entry=" + entry + ']');

                    // Exclude DHT nodes.
                    map(entry, F.view(nearNodes, F0.notIn(dhtNodes)), nearMap);
                }
                else if (log.isDebugEnabled())
                    log.debug("Entry has no near readers: " + entry);

                break;
            }
            catch (GridCacheEntryRemovedException ignore) {
                cached = dht.entryExx(entry.key());

                entry.cached(cached);
            }
        }
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param globalMap Map.
     */
    private void map(
        IgniteTxEntry entry,
        Iterable<ClusterNode> nodes,
        Map<UUID, GridDistributedTxMapping> globalMap
    ) {
        if (nodes != null) {
            for (ClusterNode n : nodes) {
                GridDistributedTxMapping global = globalMap.get(n.id());

                if (!F.isEmpty(entry.entryProcessors())) {
                    GridDhtPartitionState state = entry.context().topology().partitionState(n.id(),
                        entry.cached().partition());

                    if (state != GridDhtPartitionState.OWNING && state != GridDhtPartitionState.EVICTED) {
                        CacheObject procVal = entry.entryProcessorCalculatedValue();

                        entry.op(procVal == null ? DELETE : UPDATE);
                        entry.value(procVal, true, false);
                        entry.entryProcessors(null);
                    }
                }

                if (global == null)
                    globalMap.put(n.id(), global = new GridDistributedTxMapping(n));

                global.add(entry);
            }
        }
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
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

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
         * @param dhtMapping Mapping.
         * @param nearMapping nearMapping.
         */
        MiniFuture(
            UUID nodeId,
            GridDistributedTxMapping dhtMapping,
            GridDistributedTxMapping nearMapping
        ) {
            assert dhtMapping == null || nearMapping == null || dhtMapping.node().equals(nearMapping.node());

            this.nodeId = nodeId;
            this.dhtMapping = dhtMapping;
            this.nearMapping = nearMapping;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return dhtMapping != null ? dhtMapping.node() : nearMapping.node();
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
         * @param e Node failure.
         */
        void onNodeLeft(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will ignore): " + this);

            if (tx != null)
                tx.removeMapping(nodeId);

            onDone(tx);
        }

        /**
         * @param res Result callback.
         */
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

                                    cached.removeReader(nearMapping.node().id(), res.messageId());

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
                // Keep this loop for backward compatibility.
                if (!F.isEmpty(res.invalidPartitions())) {
                    for (Iterator<IgniteTxEntry> it = dhtMapping.entries().iterator(); it.hasNext();) {
                        IgniteTxEntry entry  = it.next();

                        if (res.invalidPartitions().contains(entry.cached().partition())) {
                            it.remove();

                            if (log.isDebugEnabled())
                                log.debug("Removed mapping for entry from dht mapping [key=" + entry.key() +
                                    ", tx=" + tx + ", dhtMapping=" + dhtMapping + ']');
                        }
                    }
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

                    if (dhtMapping.empty()) {
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

                    while (true) {
                        GridCacheEntryEx entry = cacheCtx.cache().entryEx(info.key());

                        GridDrType drType = cacheCtx.isDrEnabled() ? GridDrType.DR_PRELOAD : GridDrType.DR_NONE;

                        try {
                            if (entry.initialValue(info.value(), info.version(),
                                info.ttl(), info.expireTime(), true, topVer, drType)) {
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
}
