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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.dr.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 *
 */
@SuppressWarnings("unchecked")
public final class GridDhtTxPrepareFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheMvccFuture<IgniteInternalTx> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

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

    /** IDs of backup nodes receiving last prepare request during this prepare. */
    private Collection<UUID> lastBackups;

    /** Needs return value flag. */
    private boolean retVal;

    /** Return value. */
    private GridCacheReturn ret;

    /** Keys that did not pass the filter. */
    private Collection<IgniteTxKey> filterFailedKeys;

    /** Keys that should be locked. */
    private GridConcurrentHashSet<IgniteTxKey> lockKeys = new GridConcurrentHashSet<>();

    /** Locks ready flag. */
    private volatile boolean locksReady;

    /** */
    private boolean invoke;

    /** */
    private IgniteInClosure<GridNearTxPrepareResponse> completeCb;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param nearMiniId Near mini future id.
     * @param dhtVerMap DHT versions map.
     * @param last {@code True} if this is last prepare operation for node.
     * @param lastBackups IDs of backup nodes receiving last prepare request during this prepare.
     */
    public GridDhtTxPrepareFuture(
        GridCacheSharedContext cctx,
        final GridDhtTxLocalAdapter tx,
        IgniteUuid nearMiniId,
        Map<IgniteTxKey, GridCacheVersion> dhtVerMap,
        boolean last,
        boolean retVal,
        Collection<UUID> lastBackups,
        IgniteInClosure<GridNearTxPrepareResponse> completeCb
    ) {
        super(cctx.kernalContext(), new IgniteReducer<IgniteInternalTx, IgniteInternalTx>() {
            @Override public boolean collect(IgniteInternalTx e) {
                return true;
            }

            @Override public IgniteInternalTx reduce() {
                // Nothing to aggregate.
                return tx;
            }
        });

        this.cctx = cctx;
        this.tx = tx;
        this.dhtVerMap = dhtVerMap;
        this.last = last;
        this.lastBackups = lastBackups;

        futId = IgniteUuid.randomUuid();

        this.nearMiniId = nearMiniId;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtTxPrepareFuture.class);

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        this.retVal = retVal;

        this.completeCb = completeCb;

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

    /**
     * @return Involved nodes.
     */
    @Override public Collection<? extends ClusterNode> nodes() {
        return
            F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
                @Nullable @Override public ClusterNode apply(IgniteInternalFuture<?> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        boolean rmv = lockKeys.remove(entry.txKey());

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
        return locksReady && lockKeys.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /**
     *
     */
    private void onEntriesLocked() {
        ret = new GridCacheReturn(null, tx.localResult(), null, true);

        for (IgniteTxEntry txEntry : tx.optimisticLockEntries()) {
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

                if (hasFilters || retVal || txEntry.op() == GridCacheOperation.DELETE) {
                    cached.unswap(true, retVal);

                    CacheObject val = cached.innerGet(
                        tx,
                        /*swap*/true,
                        /*read through*/(retVal || hasFilters) && cacheCtx.config().isLoadPreviousValue(),
                        /*fail fast*/false,
                        /*unmarshal*/true,
                        /*metrics*/retVal,
                        /*event*/retVal,
                        /*tmp*/false,
                        null,
                        null,
                        null,
                        null);

                    if (retVal) {
                        if (!F.isEmpty(txEntry.entryProcessors())) {
                            invoke = true;

                            KeyCacheObject key = txEntry.key();

                            Object procRes = null;
                            Exception err = null;

                             for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : txEntry.entryProcessors()) {
                                try {
                                    CacheInvokeEntry<Object, Object> invokeEntry =
                                        new CacheInvokeEntry<>(txEntry.context(), key, val);

                                    EntryProcessor<Object, Object, Object> processor = t.get1();

                                    procRes = processor.process(invokeEntry, t.get2());

                                    val = cacheCtx.toCacheObject(invokeEntry.getValue());
                                }
                                catch (Exception e) {
                                    err = e;

                                    break;
                                }
                            }

                            if (err != null || procRes != null)
                                ret.addEntryProcessResult(txEntry.context(), key, null, procRes, err);
                            else
                                ret.invokeResult(true);
                        }
                        else
                            ret.value(cacheCtx, val);
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
                        ret.success(txEntry.op() != GridCacheOperation.DELETE || cached.hasValue());
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to get result value for cache entry: " + cached, e);
            }
            catch (GridCacheEntryRemovedException e) {
                assert false : "Got entry removed exception while holding transactional lock on entry: " + e;
            }
            catch (GridCacheFilterFailedException e) {
                assert false : "Got filter failed exception with fail fast false " + e;
            }
        }
    }

    /**
     * @param t Error.
     */
    public void onError(Throwable t) {
        onDone(tx, t);
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridDhtTxPrepareResponse res) {
        if (!isDone()) {
            for (IgniteInternalFuture<IgniteInternalTx> fut : pending()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        assert f.node().id().equals(nodeId);

                        f.onResult(res);

                        break;
                    }
                }
            }
        }
    }

    /**
     * Marks all locks as ready for local transaction.
     */
    private void readyLocks() {
        // Ready all locks.
        if (log.isDebugEnabled())
            log.debug("Marking all local candidates as ready: " + this);

        Iterable<IgniteTxEntry> checkEntries = tx.groupLock() ?
            Collections.singletonList(tx.groupLockEntry()) : writes;

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
                if (!tx.groupLock() || tx.groupLockKey().equals(entry.txKey()))
                    lockKeys.add(txEntry.txKey());
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

        locksReady = true;
    }

    /**
     * Checks if all ready locks are acquired and sends requests to remote nodes in this case.
     *
     * @return {@code True} if all locks are acquired, {@code false} otherwise.
     */
    private boolean mapIfLocked() {
        if (checkLocks()) {
            prepare0();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx0, Throwable err) {
        assert err != null || (initialized() && !hasPending()) : "On done called for prepare future that has " +
            "pending mini futures: " + this;

        this.err.compareAndSet(null, err);

        // Must clear prepare future before response is sent or listeners are notified.
        if (tx.optimistic())
            tx.clearPrepareFuture(this);

        if (tx.onePhaseCommit()) {
            assert last;

            // Must create prepare response before transaction is committed to grab correct return value.
            final GridNearTxPrepareResponse res = createPrepareResponse();

            onComplete();

            if (!tx.near()) {
                if (tx.markFinalizing(IgniteInternalTx.FinalizationStatus.USER_FINISH)) {
                    IgniteInternalFuture<IgniteInternalTx> fut = this.err.get() == null ?
                        tx.commitAsync() : tx.rollbackAsync();

                    fut.listen(new CIX1<IgniteInternalFuture<IgniteInternalTx>>() {
                        @Override
                        public void applyx(IgniteInternalFuture<IgniteInternalTx> fut) {
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
                try {
                    sendPrepareResponse(createPrepareResponse());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send prepare response for transaction: " + tx, e);
                }
                finally {
                    // Will call super.onDone().
                    onComplete();
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
     * @throws IgniteCheckedException If failed to send response.
     */
    private void sendPrepareResponse(GridNearTxPrepareResponse res) throws IgniteCheckedException {
        if (!tx.nearNodeId().equals(cctx.localNodeId()))
            cctx.io().send(tx.nearNodeId(), res, tx.ioPolicy());
        else {
            assert completeCb != null;

            completeCb.apply(res);
        }
    }

    /**
     * @return Prepare response.
     */
    private GridNearTxPrepareResponse createPrepareResponse() {
        // Send reply back to originating near node.
        Throwable prepErr = err.get();

        GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
            tx.nearXidVersion(),
            tx.colocated() ? tx.xid() : tx.nearFutureId(),
            nearMiniId == null ? tx.xid() : nearMiniId,
            tx.xidVersion(),
            tx.invalidPartitions(),
            ret,
            prepErr);

        if (prepErr == null) {
            addDhtValues(res);

            GridCacheVersion min = tx.minVersion();

            res.completedVersions(cctx.tm().committedVersions(min), cctx.tm().rolledbackVersions(min));

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

                GridCacheContext cacheCtx = txEntry.context();

                assert txEntry != null : "Missing tx entry for key [tx=" + tx + ", key=" + e.txKey() + ']';

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
     * @return {@code True} if {@code done} flag was changed as a result of this call.
     */
    private boolean onComplete() {
        if (last || tx.isSystemInvalidate())
            tx.state(PREPARED);

        if (super.onDone(tx, err.get())) {
            // Don't forget to clean up.
            cctx.mvcc().removeFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Completes this future.
     */
    public void complete() {
        onComplete();
    }

    /**
     * Initializes future.
     *
     * @param reads Read entries.
     * @param writes Write entries.
     * @param txNodes Transaction nodes mapping.
     */
    public void prepare(Iterable<IgniteTxEntry> reads, Iterable<IgniteTxEntry> writes,
        Map<UUID, Collection<UUID>> txNodes) {
        if (tx.empty()) {
            tx.setRollbackOnly();

            onDone(tx);
        }

        this.reads = reads;
        this.writes = writes;
        this.txNodes = txNodes;

        readyLocks();

        mapIfLocked();
    }

    /**
     * @param backupId Backup node ID.
     * @return {@code True} if backup node receives last prepare request for this transaction.
     */
    private boolean lastBackup(UUID backupId) {
        return lastBackups != null && lastBackups.contains(backupId);
    }

    /**
     *
     */
    private void prepare0() {
        if (!mapped.compareAndSet(false, true))
            return;

        try {
            // We are holding transaction-level locks for entries here, so we can get next write version.
            onEntriesLocked();

            tx.writeVersion(cctx.versions().next(tx.topologyVersion()));

            {
                Map<UUID, GridDistributedTxMapping> futDhtMap = new HashMap<>();
                Map<UUID, GridDistributedTxMapping> futNearMap = new HashMap<>();

                boolean hasRemoteNodes = false;

                // Assign keys to primary nodes.
                if (!F.isEmpty(writes)) {
                    for (IgniteTxEntry write : writes)
                        hasRemoteNodes |= map(tx.entry(write.txKey()), futDhtMap, futNearMap);
                }

                if (!F.isEmpty(reads)) {
                    for (IgniteTxEntry read : reads)
                        hasRemoteNodes |= map(tx.entry(read.txKey()), futDhtMap, futNearMap);
                }

                tx.needsCompletedVersions(hasRemoteNodes);
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
                        tx.groupLockKey(),
                        tx.partitionLock(),
                        txNodes,
                        tx.nearXidVersion(),
                        true,
                        tx.onePhaseCommit(),
                        tx.subjectId(),
                        tx.taskNameHash());

                    int idx = 0;

                    for (IgniteTxEntry entry : dhtWrites) {
                        try {
                            GridDhtCacheEntry cached = (GridDhtCacheEntry)entry.cached();

                            GridCacheContext<K, V> cacheCtx = cached.context();

                            if (entry.explicitVersion() == null) {
                                GridCacheMvccCandidate added = cached.candidate(version());

                                assert added != null || entry.groupLockEntry() : "Null candidate for non-group-lock entry " +
                                    "[added=" + added + ", entry=" + entry + ']';
                                assert added == null || added.dhtLocal() : "Got non-dht-local candidate for prepare future" +
                                    "[added=" + added + ", entry=" + entry + ']';

                                if (added != null && added.ownerVersion() != null)
                                    req.owned(entry.txKey(), added.ownerVersion());
                            }

                            // Do not invalidate near entry on originating transaction node.
                            req.invalidateNearEntry(idx, !tx.nearNodeId().equals(n.id()) &&
                                cached.readerId(n.id()) != null);

                            if (cached.isNewLocked()) {
                                List<ClusterNode> owners = cacheCtx.topology().owners(cached.partition(),
                                    tx != null ? tx.topologyVersion() : cacheCtx.affinity().affinityTopologyVersion());

                                // Do not preload if local node is partition owner.
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
                                GridCacheMvccCandidate added = entry.cached().candidate(version());

                                assert added != null;
                                assert added.dhtLocal();

                                if (added.ownerVersion() != null)
                                    req.owned(entry.txKey(), added.ownerVersion());

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                assert false : "Got removed exception on entry with dht local candidate: " + entry;
                            }
                        }
                    }

                    assert req.transactionNodes() != null;

                    //noinspection TryWithIdenticalCatches
                    try {
                        cctx.io().send(n, req, tx.ioPolicy());
                    }
                    catch (ClusterTopologyCheckedException e) {
                        fut.onResult(e);
                    }
                    catch (IgniteCheckedException e) {
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
                            tx.groupLockKey(),
                            tx.partitionLock(),
                            tx.transactionNodes(),
                            tx.nearXidVersion(),
                            true,
                            tx.onePhaseCommit(),
                            tx.subjectId(),
                            tx.taskNameHash());

                        for (IgniteTxEntry entry : nearMapping.writes()) {
                            try {
                                GridCacheMvccCandidate added = entry.cached().candidate(version());

                                assert added != null || entry.groupLockEntry() : "Null candidate for non-group-lock entry " +
                                    "[added=" + added + ", entry=" + entry + ']';
                                assert added == null || added.dhtLocal() : "Got non-dht-local candidate for prepare future" +
                                    "[added=" + added + ", entry=" + entry + ']';

                                if (added != null && added.ownerVersion() != null)
                                    req.owned(entry.txKey(), added.ownerVersion());

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                assert false : "Got removed exception on entry with dht local candidate: " + entry;
                            }
                        }

                        assert req.transactionNodes() != null;

                        //noinspection TryWithIdenticalCatches
                        try {
                            cctx.io().send(nearMapping.node(), req, tx.system() ? UTILITY_CACHE_POOL : SYSTEM_POOL);
                        }
                        catch (ClusterTopologyCheckedException e) {
                            fut.onResult(e);
                        }
                        catch (IgniteCheckedException e) {
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
     * @param futDhtMap DHT mapping.
     * @param futNearMap Near mapping.
     * @return {@code True} if mapped.
     */
    private boolean map(
        IgniteTxEntry entry,
        Map<UUID, GridDistributedTxMapping> futDhtMap,
        Map<UUID, GridDistributedTxMapping> futNearMap) {
        if (entry.cached().isLocal())
            return false;

        GridDhtCacheEntry cached = (GridDhtCacheEntry)entry.cached();

        GridCacheContext cacheCtx = entry.context();

        GridDhtCacheAdapter<K, V> dht = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

        ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(entry);

        if (expiry != null && entry.op() == READ) {
            entry.op(NOOP);

            entry.ttl(CU.toTtl(expiry.getExpiryForAccess()));
        }

        boolean ret;

        while (true) {
            try {
                Collection<ClusterNode> dhtNodes = dht.topology().nodes(cached.partition(), tx.topologyVersion());

                if (log.isDebugEnabled())
                    log.debug("Mapping entry to DHT nodes [nodes=" + U.toShortString(dhtNodes) +
                        ", entry=" + entry + ']');

                Collection<UUID> readers = cached.readers();

                Collection<ClusterNode> nearNodes = null;

                if (!F.isEmpty(readers)) {
                    nearNodes = cctx.discovery().nodes(readers, F0.not(F.idForNodeId(tx.nearNodeId())));

                    if (log.isDebugEnabled())
                        log.debug("Mapping entry to near nodes [nodes=" + U.toShortString(nearNodes) +
                            ", entry=" + entry + ']');
                }
                else if (log.isDebugEnabled())
                    log.debug("Entry has no near readers: " + entry);

                // Exclude local node.
                ret = map(entry, F.view(dhtNodes, F.remoteNodes(cctx.localNodeId())), dhtMap, futDhtMap);

                // Exclude DHT nodes.
                ret |= map(entry, F.view(nearNodes, F0.notIn(dhtNodes)), nearMap, futNearMap);

                break;
            }
            catch (GridCacheEntryRemovedException ignore) {
                cached = dht.entryExx(entry.key());

                entry.cached(cached);
            }
        }

        return ret;
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param globalMap Map.
     * @param locMap Exclude map.
     * @return {@code True} if mapped.
     */
    private boolean map(IgniteTxEntry entry, Iterable<ClusterNode> nodes,
        Map<UUID, GridDistributedTxMapping> globalMap, Map<UUID, GridDistributedTxMapping> locMap) {
        boolean ret = false;

        if (nodes != null) {
            for (ClusterNode n : nodes) {
                GridDistributedTxMapping global = globalMap.get(n.id());

                if (global == null)
                    globalMap.put(n.id(), global = new GridDistributedTxMapping(n));

                global.add(entry);

                GridDistributedTxMapping loc = locMap.get(n.id());

                if (loc == null)
                    locMap.put(n.id(), loc = new GridDistributedTxMapping(n));

                loc.add(entry);

                ret = true;
            }
        }

        return ret;
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
        return S.toString(GridDhtTxPrepareFuture.class, this, "xid", tx.xidVersion(), "super", super.toString());
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
            assert dhtMapping == null || nearMapping == null || dhtMapping.node() == nearMapping.node();

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
        void onResult(ClusterTopologyCheckedException e) {
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
                    GridCacheContext<K, V> cacheCtx = cctx.cacheContext(info.cacheId());

                    while (true) {
                        GridCacheEntryEx entry = cacheCtx.cache().entryEx(info.key());

                        GridDrType drType = cacheCtx.isDrEnabled() ? GridDrType.DR_PRELOAD : GridDrType.DR_NONE;

                        try {
                            if (entry.initialValue(info.value(), info.version(),
                                info.ttl(), info.expireTime(), true, topVer, drType)) {
                                if (rec && !entry.isInternal())
                                    cacheCtx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                                        (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, info.value(), true, null,
                                        false, null, null, null);

                                if (retVal && !invoke)
                                    ret.value(cacheCtx, info.value());
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
