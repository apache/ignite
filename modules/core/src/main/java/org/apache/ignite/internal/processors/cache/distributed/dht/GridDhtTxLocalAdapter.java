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
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 * Replicated user transaction.
 */
public abstract class GridDhtTxLocalAdapter extends IgniteTxLocalAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near mappings. */
    protected Map<UUID, GridDistributedTxMapping> nearMap =
        new ConcurrentHashMap8<>();

    /** DHT mappings. */
    protected Map<UUID, GridDistributedTxMapping> dhtMap =
        new ConcurrentHashMap8<>();

    /** Mapped flag. */
    private AtomicBoolean mapped = new AtomicBoolean();

    /** */
    private long dhtThreadId;

    /** */
    private boolean needsCompletedVers;

    /** Versions of pending locks for entries of this tx. */
    private Collection<GridCacheVersion> pendingVers;

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
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock If this is a group-lock transaction and the whole partition should be locked.
     */
    protected GridDhtTxLocalAdapter(
        GridCacheSharedContext cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        GridIoPolicy plc,
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
        super(cctx, xidVer, implicit, implicitSingle, sys, plc, concurrency, isolation, timeout, invalidate,
            storeEnabled, txSize, grpLockKey, partLock, subjId, taskNameHash);

        assert cctx != null;

        threadId = Thread.currentThread().getId();
        dhtThreadId = threadId;
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
     * @return Near future mini ID.
     */
    protected abstract IgniteUuid nearMiniId();

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
     * @param commit Commit flag.
     * @param err Error, if any.
     */
    protected abstract void sendFinishReply(boolean commit, @Nullable Throwable err);

    /**
     * @param needsCompletedVers {@code True} if needs completed versions.
     */
    public void needsCompletedVersions(boolean needsCompletedVers) {
        this.needsCompletedVers |= needsCompletedVers;
    }

    /** {@inheritDoc} */
    @Override public boolean needsCompletedVersions() {
        return needsCompletedVers;
    }

    /**
     * @return Versions for all pending locks that were in queue before tx locks were released.
     */
    public Collection<GridCacheVersion> pendingVersions() {
        return pendingVers == null ? Collections.<GridCacheVersion>emptyList() : pendingVers;
    }

    /**
     * @param pendingVers Versions for all pending locks that were in queue before tx locsk were released.
     */
    public void pendingVersions(Collection<GridCacheVersion> pendingVers) {
        this.pendingVers = pendingVers;
    }

    /**
     * @return DHT thread ID.
     */
    long dhtThreadId() {
        return dhtThreadId;
    }

    /**
     * Map explicit locks.
     */
    protected void mapExplicitLocks() {
        if (!mapped.get()) {
            // Explicit locks may participate in implicit transactions only.
            if (!implicit()) {
                mapped.set(true);

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
                    GridCacheEntryEx cached = cacheCtx.cache().entryEx(e.key());

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

                            cacheCtx.dhtMap(nearNodeId(), topologyVersion(),
                                (GridDhtCacheEntry)e.cached(), log, dhtEntryMap, nearEntryMap);
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        GridCacheEntryEx cached = cacheCtx.cache().entryEx(e.key());

                        e.cached(cached);
                    }
                }
            }

            if (!F.isEmpty(dhtEntryMap))
                addDhtNodeEntryMapping(dhtEntryMap);

            if (!F.isEmpty(nearEntryMap))
                addNearNodeEntryMapping(nearEntryMap);

            mapped.set(true);
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
     * @param nodeId Node ID.
     * @return Mapping.
     */
    GridDistributedTxMapping dhtMapping(UUID nodeId) {
        return dhtMap.get(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @return Mapping.
     */
    GridDistributedTxMapping nearMapping(UUID nodeId) {
        return nearMap.get(nodeId);
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
     * @param mappings Mappings to add.
     */
    public void addDhtMapping(Map<UUID, GridDistributedTxMapping> mappings) {
        addMapping0(mappings, dhtMap);
    }

    /**
     * @param mappings Mappings to add.
     */
    public void addNearMapping(Map<UUID, GridDistributedTxMapping> mappings) {
        addMapping0(mappings, nearMap);
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

            IgniteTxEntry txEntry = txMap.get(entry.txKey());

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
                IgniteTxEntry txEntry = txMap.get(entry.txKey());

                if (txEntry != null) {
                    if (m == null)
                        dst.put(n.id(), m = new GridDistributedTxMapping(n));

                    m.add(txEntry);
                }
            }
        }
    }

    /**
     * @param mappings Mappings to add.
     * @param dst Map to add to.
     */
    private void addMapping0(
        Map<UUID, GridDistributedTxMapping> mappings,
        Map<UUID, GridDistributedTxMapping> dst
    ) {
        for (Map.Entry<UUID, GridDistributedTxMapping> entry : mappings.entrySet()) {
            GridDistributedTxMapping targetMapping = dst.get(entry.getKey());

            if (targetMapping == null)
                dst.put(entry.getKey(), entry.getValue());
            else {
                for (IgniteTxEntry txEntry : entry.getValue().entries())
                    targetMapping.add(txEntry);
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
            IgniteTxEntry entry = txMap.get(e.txKey());

            if (entry != null) {
                entry.op(e.op()); // Absolutely must set operation, as default is DELETE.
                entry.value(e.value(), e.hasWriteValue(), e.hasReadValue());
                entry.entryProcessors(e.entryProcessors());
                entry.ttl(e.ttl());
                entry.filters(e.filters());
                entry.expiry(e.expiry());

                entry.conflictExpireTime(e.conflictExpireTime());
                entry.conflictVersion(e.conflictVersion());
            }
            else {
                entry = e;

                addActiveCache(dhtCache.context());

                GridDhtCacheEntry cached = dhtCache.entryExx(entry.key(), topologyVersion());

                entry.cached(cached);

                GridCacheVersion explicit = entry.explicitVersion();

                if (explicit != null) {
                    GridCacheVersion dhtVer = cctx.mvcc().mappedVersion(explicit);

                    if (dhtVer == null)
                        throw new IgniteCheckedException("Failed to find dht mapping for explicit entry version: " + entry);

                    entry.explicitVersion(dhtVer);
                }

                txMap.put(entry.txKey(), entry);

                if (log.isDebugEnabled())
                    log.debug("Added entry to transaction: " + entry);
            }

            return addReader(msgId, dhtCache.entryExx(entry.key()), entry, topologyVersion());
        }
        catch (GridDhtInvalidPartitionException ex) {
            addInvalidPartition(cacheCtx, ex.partition());

            return new GridFinishedFuture<>(true);
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param entries Entries to lock.
     * @param onePhaseCommit One phase commit flag.
     * @param msgId Message ID.
     * @param read Read flag.
     * @param accessTtl TTL for read operation.
     * @return Lock future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    IgniteInternalFuture<GridCacheReturn> lockAllAsync(
        GridCacheContext cacheCtx,
        List<GridCacheEntryEx> entries,
        boolean onePhaseCommit,
        long msgId,
        final boolean read,
        final boolean needRetVal,
        long accessTtl
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
                    GridDhtCacheEntry cached = dhtCache.entryExx(key, topVer);

                    addActiveCache(dhtCache.context());

                    cached.unswap(!read, read);

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
                        null);

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

            return obtainLockAsync(cacheCtx, ret, passedKeys, read, needRetVal, skipped, accessTtl, null);
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
     * @param skipped Skipped keys.
     * @param accessTtl TTL for read operation.
     * @param filter Entry write filter.
     * @return Future for lock acquisition.
     */
    private IgniteInternalFuture<GridCacheReturn> obtainLockAsync(
        final GridCacheContext cacheCtx,
        GridCacheReturn ret,
        final Collection<KeyCacheObject> passedKeys,
        final boolean read,
        final boolean needRetVal,
        final Set<KeyCacheObject> skipped,
        final long accessTtl,
        @Nullable final CacheEntryPredicate[] filter) {
        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys [passedKeys=" + passedKeys + ", skipped=" +
                skipped + ']');

        if (passedKeys.isEmpty())
            return new GridFinishedFuture<>(ret);

        GridDhtTransactionalCacheAdapter<?, ?> dhtCache = cacheCtx.isNear() ? cacheCtx.nearTx().dht() : cacheCtx.dhtTx();

        IgniteInternalFuture<Boolean> fut = dhtCache.lockAllAsyncInternal(passedKeys,
            lockTimeout(),
            this,
            isInvalidate(),
            read,
            needRetVal,
            isolation,
            accessTtl,
            CU.empty0());

        return new GridEmbeddedFuture<>(
            fut,
            new PLC1<GridCacheReturn>(ret) {
                @Override protected GridCacheReturn postLock(GridCacheReturn ret) throws IgniteCheckedException {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock on keys: " + passedKeys);

                    postLockWrite(cacheCtx,
                        passedKeys,
                        skipped,
                        ret,
                        /*remove*/false,
                        /*retval*/false,
                        /*read*/read,
                        accessTtl,
                        filter == null ? CU.empty0() : filter,
                        /**computeInvoke*/false);

                    return ret;
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override protected void addGroupTxMapping(Collection<IgniteTxKey> keys) {
        assert groupLock();

        for (GridDistributedTxMapping mapping : dhtMap.values())
            mapping.entries(Collections.unmodifiableCollection(txMap.values()), true);

        // Here we know that affinity key for all given keys is our group lock key.
        // Just add entries to dht mapping.
        // Add near readers. If near cache is disabled on all nodes, do nothing.
        Collection<UUID> backupIds = dhtMap.keySet();

        Map<ClusterNode, List<GridDhtCacheEntry>> locNearMap = null;

        for (IgniteTxKey key : keys) {
            IgniteTxEntry txEntry = entry(key);

            if (!txEntry.groupLockEntry() || txEntry.context().isNear())
                continue;

            assert txEntry.cached() instanceof GridDhtCacheEntry : "Invalid entry type: " + txEntry.cached();

            while (true) {
                try {
                    GridDhtCacheEntry entry = (GridDhtCacheEntry)txEntry.cached();

                    Collection<UUID> readers = entry.readers();

                    if (!F.isEmpty(readers)) {
                        Collection<ClusterNode> nearNodes = cctx.discovery().nodes(readers, F0.notEqualTo(nearNodeId()),
                            F.notIn(backupIds));

                        if (log.isDebugEnabled())
                            log.debug("Mapping entry to near nodes [nodes=" + U.nodeIds(nearNodes) + ", entry=" +
                                entry + ']');

                        for (ClusterNode n : nearNodes) {
                            if (locNearMap == null)
                                locNearMap = new HashMap<>();

                            List<GridDhtCacheEntry> entries = locNearMap.get(n);

                            if (entries == null)
                                locNearMap.put(n, entries = new LinkedList<>());

                            entries.add(entry);
                        }
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // Retry.
                    txEntry.cached(txEntry.context().dht().entryExx(key.key(), topologyVersion()));
                }
            }
        }

        if (locNearMap != null)
            addNearNodeEntryMapping(locNearMap);
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
            "dhtNodes", dhtMap.keySet(), "super", super.toString());
    }
}
