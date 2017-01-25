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

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Replicated user transaction.
 */
public abstract class GridDhtTxLocalAdapter extends IgniteTxLocalAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near mappings. */
    protected Map<UUID, GridDistributedTxMapping> nearMap = new ConcurrentHashMap8<>();

    /** DHT mappings. */
    protected Map<UUID, GridDistributedTxMapping> dhtMap = new ConcurrentHashMap8<>();

    /** Mapped flag. */
    protected volatile boolean mapped;

    /** */
    protected boolean explicitLock;

    /** Versions of pending locks for entries of this tx. */
    private Collection<GridCacheVersion> pendingVers;

    /** Flag indicating that originating node has near cache. */
    private boolean nearOnOriginatingNode;

    /** Nodes where transactions were started on lock step. */
    private Set<ClusterNode> lockTxNodes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridDhtTxLocalAdapter() {
        // No-op.
    }

    /**
     * @param xidVer Transaction version.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit-with-single-key flag.
     * @param cctx Cache context.
     * @param sys System flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     */
    protected GridDhtTxLocalAdapter(
        GridCacheSharedContext cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        boolean explicitLock,
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
            implicitSingle,
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

        assert cctx != null;

        this.explicitLock = explicitLock;

        threadId = Thread.currentThread().getId();
    }

    /**
     * @param node Node.
     */
    public void addLockTransactionNode(ClusterNode node) {
        assert node != null;
        assert !node.isLocal();

        if (lockTxNodes == null)
            lockTxNodes = new HashSet<>();

        lockTxNodes.add(node);
    }

    /**
     * Sets flag that indicates that originating node has a near cache that participates in this transaction.
     *
     * @param hasNear Has near cache flag.
     */
    public void nearOnOriginatingNode(boolean hasNear) {
        nearOnOriginatingNode = hasNear;
    }

    /**
     * Gets flag that indicates that originating node has a near cache that participates in this transaction.
     *
     * @return Has near cache flag.
     */
    public boolean nearOnOriginatingNode() {
        return nearOnOriginatingNode;
    }

    /**
     * @return {@code True} if explicit lock transaction.
     */
    public boolean explicitLock() {
        return explicitLock;
    }

    /**
     * @param explicitLock Explicit lock flag.
     */
    public void explicitLock(boolean explicitLock) {
        this.explicitLock = explicitLock;
    }

    /**
     * @return Nodes where transactions were started on lock step.
     */
    @Nullable public Set<ClusterNode> lockTransactionNodes() {
        return lockTxNodes;
    }

    /**
     * @return Near node id.
     */
    protected abstract UUID nearNodeId();

    /**
     * @return Near future ID.
     */
    protected abstract IgniteUuid nearFutureId();

    /**
     * Adds reader to cached entry.
     *
     * @param msgId Message ID.
     * @param cached Cached entry.
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @return {@code True} if reader was added as a result of this call.
     */
    @Nullable protected abstract IgniteInternalFuture<Boolean> addReader(long msgId,
        GridDhtCacheEntry cached,
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer);

    /**
     * @param err Error, if any.
     */
    protected abstract void sendFinishReply(@Nullable Throwable err);

    /** {@inheritDoc} */
    @Override public boolean needsCompletedVersions() {
        return nearOnOriginatingNode;
    }

    /**
     * @return Versions for all pending locks that were in queue before tx locks were released.
     */
    Collection<GridCacheVersion> pendingVersions() {
        return pendingVers == null ? Collections.<GridCacheVersion>emptyList() : pendingVers;
    }

    /**
     * @param pendingVers Versions for all pending locks that were in queue before tx locsk were released.
     */
    public void pendingVersions(Collection<GridCacheVersion> pendingVers) {
        this.pendingVers = pendingVers;
    }

    /**
     * Map explicit locks.
     */
    protected void mapExplicitLocks() {
        if (!mapped) {
            // Explicit locks may participate in implicit transactions only.
            if (!implicit()) {
                mapped = true;

                return;
            }

            Map<ClusterNode, List<GridDhtCacheEntry>> dhtEntryMap = null;
            Map<ClusterNode, List<GridDhtCacheEntry>> nearEntryMap = null;

            for (IgniteTxEntry e : allEntries()) {
                assert e.cached() != null;

                GridCacheContext cacheCtx = e.cached().context();

                if (cacheCtx.isNear())
                    continue;

                if (e.cached().obsolete()) {
                    GridCacheEntryEx cached = cacheCtx.cache().entryEx(e.key(), topologyVersion());

                    e.cached(cached);
                }

                if (e.cached().detached() || e.cached().isLocal())
                    continue;

                while (true) {
                    try {
                        // Map explicit locks.
                        if (e.explicitVersion() != null && !e.explicitVersion().equals(xidVer)) {
                            if (dhtEntryMap == null)
                                dhtEntryMap = new GridLeanMap<>();

                            if (nearEntryMap == null)
                                nearEntryMap = new GridLeanMap<>();

                            cacheCtx.dhtMap(
                                (GridDhtCacheEntry)e.cached(),
                                e.explicitVersion(),
                                log,
                                dhtEntryMap,
                                nearEntryMap);
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        GridCacheEntryEx cached = cacheCtx.cache().entryEx(e.key(), topologyVersion());

                        e.cached(cached);
                    }
                }
            }

            if (!F.isEmpty(dhtEntryMap))
                addDhtNodeEntryMapping(dhtEntryMap);

            if (!F.isEmpty(nearEntryMap))
                addNearNodeEntryMapping(nearEntryMap);

            mapped = true;
        }
    }

    /**
     * @return DHT map.
     */
    Map<UUID, GridDistributedTxMapping> dhtMap() {
        mapExplicitLocks();

        return dhtMap;
    }

    /**
     * @return Near map.
     */
    Map<UUID, GridDistributedTxMapping> nearMap() {
        mapExplicitLocks();

        return nearMap;
    }

    /**
     * @param mappings Mappings to add.
     */
    void addDhtNodeEntryMapping(Map<ClusterNode, List<GridDhtCacheEntry>> mappings) {
        addMapping(mappings, dhtMap);
    }

    /**
     * @param mappings Mappings to add.
     */
    void addNearNodeEntryMapping(Map<ClusterNode, List<GridDhtCacheEntry>> mappings) {
        addMapping(mappings, nearMap);
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if mapping was removed.
     */
    public boolean removeMapping(UUID nodeId) {
        return removeMapping(nodeId, null, dhtMap) | removeMapping(nodeId, null, nearMap);
    }

    /**
     * @param nodeId Node ID.
     * @param entry Entry to remove.
     * @return {@code True} if was removed.
     */
    boolean removeDhtMapping(UUID nodeId, GridCacheEntryEx entry) {
        return removeMapping(nodeId, entry, dhtMap);
    }

    /**
     * @param nodeId Node ID.
     * @param entry Entry to remove.
     * @return {@code True} if was removed.
     */
    boolean removeNearMapping(UUID nodeId, GridCacheEntryEx entry) {
        return removeMapping(nodeId, entry, nearMap);
    }

    /**
     * @param nodeId Node ID.
     * @param entry Entry to remove.
     * @param map Map to remove from.
     * @return {@code True} if was removed.
     */
    private boolean removeMapping(UUID nodeId, @Nullable GridCacheEntryEx entry,
        Map<UUID, GridDistributedTxMapping> map) {
        if (entry != null) {
            if (log.isDebugEnabled())
                log.debug("Removing mapping for entry [nodeId=" + nodeId + ", entry=" + entry + ']');

            IgniteTxEntry txEntry = entry(entry.txKey());

            if (txEntry == null)
                return false;

            GridDistributedTxMapping m = map.get(nodeId);

            boolean ret = m != null && m.removeEntry(txEntry);

            if (m != null && m.empty())
                map.remove(nodeId);

            return ret;
        }
        else
            return map.remove(nodeId) != null;
    }

    /**
     * @param mappings Entry mappings.
     * @param dst Transaction mappings.
     */
    private void addMapping(
        Map<ClusterNode, List<GridDhtCacheEntry>> mappings,
        Map<UUID, GridDistributedTxMapping> dst
    ) {
        for (Map.Entry<ClusterNode, List<GridDhtCacheEntry>> mapping : mappings.entrySet()) {
            ClusterNode n = mapping.getKey();

            GridDistributedTxMapping m = dst.get(n.id());

            List<GridDhtCacheEntry> entries = mapping.getValue();

            for (GridDhtCacheEntry entry : entries) {
                IgniteTxEntry txEntry = entry(entry.txKey());

                if (txEntry != null) {
                    if (m == null)
                        dst.put(n.id(), m = new GridDistributedTxMapping(n));

                    m.add(txEntry);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(GridCacheContext ctx, int part) {
        assert false : "DHT transaction encountered invalid partition [part=" + part + ", tx=" + this + ']';
    }

    /**
     * @param msgId Message ID.
     * @param e Entry to add.
     * @return Future for active transactions for the time when reader was added.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteInternalFuture<Boolean> addEntry(long msgId, IgniteTxEntry e) throws IgniteCheckedException {
        init();

        TransactionState state = state();

        assert state == PREPARING : "Invalid tx state for " +
            "adding entry [msgId=" + msgId + ", e=" + e + ", tx=" + this + ']';

        e.unmarshal(cctx, false, cctx.deploy().globalLoader());

        checkInternal(e.txKey());

        GridCacheContext cacheCtx = e.context();

        GridDhtCacheAdapter dhtCache = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

        try {
            IgniteTxEntry existing = entry(e.txKey());

            if (existing != null) {
                // Must keep NOOP operation if received READ because it means that the lock was sent to a backup node.
                if (e.op() == READ) {
                    if (existing.op() != NOOP)
                        existing.op(e.op());
                }
                else
                    existing.op(e.op()); // Absolutely must set operation, as default is DELETE.

                existing.value(e.value(), e.hasWriteValue(), e.hasReadValue());
                existing.entryProcessors(e.entryProcessors());
                existing.ttl(e.ttl());
                existing.filters(e.filters());
                existing.expiry(e.expiry());

                existing.conflictExpireTime(e.conflictExpireTime());
                existing.conflictVersion(e.conflictVersion());
            }
            else {
                existing = e;

                addActiveCache(dhtCache.context());

                GridDhtCacheEntry cached = dhtCache.entryExx(existing.key(), topologyVersion());

                existing.cached(cached);

                GridCacheVersion explicit = existing.explicitVersion();

                if (explicit != null) {
                    GridCacheVersion dhtVer = cctx.mvcc().mappedVersion(explicit);

                    if (dhtVer == null)
                        throw new IgniteCheckedException("Failed to find dht mapping for explicit entry version: " + existing);

                    existing.explicitVersion(dhtVer);
                }

                txState.addEntry(existing);

                if (log.isDebugEnabled())
                    log.debug("Added entry to transaction: " + existing);
            }

            return addReader(msgId, dhtCache.entryExx(existing.key()), existing, topologyVersion());
        }
        catch (GridDhtInvalidPartitionException ex) {
            addInvalidPartition(cacheCtx, ex.partition());

            return new GridFinishedFuture<>(true);
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param entries Entries to lock.
     * @param msgId Message ID.
     * @param read Read flag.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @param needRetVal Return value flag.
     * @param skipStore Skip store flag.
     * @return Lock future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    IgniteInternalFuture<GridCacheReturn> lockAllAsync(
        GridCacheContext cacheCtx,
        List<GridCacheEntryEx> entries,
        long msgId,
        final boolean read,
        final boolean needRetVal,
        long createTtl,
        long accessTtl,
        boolean skipStore,
        boolean keepBinary
    ) {
        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

        if (F.isEmpty(entries))
            return new GridFinishedFuture<>(ret);

        init();

        onePhaseCommit(onePhaseCommit);

        try {
            Set<KeyCacheObject> skipped = null;

            AffinityTopologyVersion topVer = topologyVersion();

            GridDhtCacheAdapter dhtCache = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

            // Enlist locks into transaction.
            for (int i = 0; i < entries.size(); i++) {
                GridCacheEntryEx entry = entries.get(i);

                KeyCacheObject key = entry.key();

                IgniteTxEntry txEntry = entry(entry.txKey());

                // First time access.
                if (txEntry == null) {
                    GridDhtCacheEntry cached;

                    if (dhtCache.context().isSwapOrOffheapEnabled()) {
                        while (true) {
                            try {
                                cached = dhtCache.entryExx(key, topVer);

                                cached.unswap(read);

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                if (log.isDebugEnabled())
                                    log.debug("Get removed entry: " + key);
                            }
                        }
                    }
                    else
                        cached = dhtCache.entryExx(key, topVer);

                    addActiveCache(dhtCache.context());

                    txEntry = addEntry(NOOP,
                        null,
                        null,
                        null,
                        cached,
                        null,
                        CU.empty0(),
                        false,
                        -1L,
                        -1L,
                        null,
                        skipStore,
                        keepBinary);

                    if (read)
                        txEntry.ttl(accessTtl);

                    txEntry.cached(cached);

                    addReader(msgId, cached, txEntry, topVer);
                }
                else {
                    if (skipped == null)
                        skipped = new GridLeanSet<>();

                    skipped.add(key);
                }
            }

            assert pessimistic();

            Collection<KeyCacheObject> keys = F.viewReadOnly(entries, CU.entry2Key());

            // Acquire locks only after having added operation to the write set.
            // Otherwise, during rollback we will not know whether locks need
            // to be rolled back.
            // Loose all skipped and previously locked (we cannot reenter locks here).
            final Collection<KeyCacheObject> passedKeys = skipped != null ? F.view(keys, F0.notIn(skipped)) : keys;

            if (log.isDebugEnabled())
                log.debug("Lock keys: " + passedKeys);

            return obtainLockAsync(cacheCtx,
                ret,
                passedKeys,
                read,
                needRetVal,
                createTtl,
                accessTtl,
                null,
                skipStore,
                keepBinary);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param cacheCtx Context.
     * @param ret Return value.
     * @param passedKeys Passed keys.
     * @param read {@code True} if read.
     * @param needRetVal Return value flag.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @param filter Entry write filter.
     * @param skipStore Skip store flag.
     * @return Future for lock acquisition.
     */
    private IgniteInternalFuture<GridCacheReturn> obtainLockAsync(
        final GridCacheContext cacheCtx,
        GridCacheReturn ret,
        final Collection<KeyCacheObject> passedKeys,
        final boolean read,
        final boolean needRetVal,
        final long createTtl,
        final long accessTtl,
        @Nullable final CacheEntryPredicate[] filter,
        boolean skipStore,
        boolean keepBinary) {
        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys [keys=" + passedKeys + ']');

        if (passedKeys.isEmpty())
            return new GridFinishedFuture<>(ret);

        GridDhtTransactionalCacheAdapter<?, ?> dhtCache =
            cacheCtx.isNear() ? cacheCtx.nearTx().dht() : cacheCtx.dhtTx();

        long timeout = remainingTime();

        if (timeout == -1)
            return new GridFinishedFuture<>(timeoutException());

        IgniteInternalFuture<Boolean> fut = dhtCache.lockAllAsyncInternal(passedKeys,
            timeout,
            this,
            isInvalidate(),
            read,
            needRetVal,
            isolation,
            createTtl,
            accessTtl,
            CU.empty0(),
            skipStore,
            keepBinary);

        return new GridEmbeddedFuture<>(
            fut,
            new PLC1<GridCacheReturn>(ret) {
                @Override protected GridCacheReturn postLock(GridCacheReturn ret) throws IgniteCheckedException {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock on keys: " + passedKeys);

                    postLockWrite(cacheCtx,
                        passedKeys,
                        ret,
                        /*remove*/false,
                        /*retval*/false,
                        /*read*/read,
                        accessTtl,
                        filter == null ? CU.empty0() : filter,
                        /*computeInvoke*/false);

                    return ret;
                }
            }
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean finish(boolean commit) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Finishing dht local tx [tx=" + this + ", commit=" + commit + "]");

        if (optimistic())
            state(PREPARED);

        if (commit) {
            if (!state(COMMITTING)) {
                TransactionState state = state();

                if (state != COMMITTING && state != COMMITTED)
                    throw new IgniteCheckedException("Invalid transaction state for commit [state=" + state() +
                        ", tx=" + this + ']');
                else {
                    if (log.isDebugEnabled())
                        log.debug("Invalid transaction state for commit (another thread is committing): " + this);

                    return false;
                }
            }
        }
        else {
            if (!state(ROLLING_BACK)) {
                if (log.isDebugEnabled())
                    log.debug("Invalid transaction state for rollback [state=" + state() + ", tx=" + this + ']');

                return false;
            }
        }

        IgniteCheckedException err = null;

        // Commit to DB first. This way if there is a failure, transaction
        // won't be committed.
        try {
            if (commit && !isRollbackOnly())
                userCommit();
            else
                userRollback();
        }
        catch (IgniteCheckedException e) {
            err = e;

            commit = false;

            // If heuristic error.
            if (!isRollbackOnly()) {
                systemInvalidate(true);

                U.warn(log, "Set transaction invalidation flag to true due to error [tx=" + CU.txString(this) +
                    ", err=" + err + ']');
            }
        }

        if (err != null) {
            state(UNKNOWN);

            throw err;
        }
        else {
            // Committed state will be set in finish future onDone callback.
            if (commit) {
                if (!onePhaseCommit()) {
                    if (!state(COMMITTED)) {
                        state(UNKNOWN);

                        throw new IgniteCheckedException("Invalid transaction state for commit: " + this);
                    }
                }
            }
            else {
                if (!state(ROLLED_BACK)) {
                    state(UNKNOWN);

                    throw new IgniteCheckedException("Invalid transaction state for rollback: " + this);
                }
            }
        }

        return true;
    }

    /**
     * Removes previously created prepare future from atomic reference.
     *
     * @param fut Expected future.
     */
    protected abstract void clearPrepareFuture(GridDhtTxPrepareFuture fut);

    /**
     * @return {@code True} if transaction is finished on prepare step.
     */
    public final boolean commitOnPrepare() {
        return onePhaseCommit() && !near() && !nearOnOriginatingNode;
    }

    /**
     * @param prepFut Prepare future.
     * @return If transaction if finished on prepare step returns future which is completed after transaction finish.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    protected final IgniteInternalFuture<GridNearTxPrepareResponse> chainOnePhasePrepare(
        final GridDhtTxPrepareFuture prepFut) {
        if (commitOnPrepare()) {
            return finishFuture().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, GridNearTxPrepareResponse>() {
                @Override public GridNearTxPrepareResponse applyx(IgniteInternalFuture<IgniteInternalTx> finishFut)
                    throws IgniteCheckedException
                {
                    return prepFut.get();
                }
            });
        }

        return prepFut;
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IgniteCheckedException {
        try {
            rollbackAsync().get();
        }
        finally {
            cctx.tm().resetContext();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxLocalAdapter.class, this, "nearNodes", nearMap.keySet(),
            "dhtNodes", dhtMap.keySet(), "explicitLock", explicitLock, "super", super.toString());
    }
}
