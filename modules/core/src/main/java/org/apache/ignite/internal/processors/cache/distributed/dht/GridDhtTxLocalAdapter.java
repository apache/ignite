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
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxState.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Replicated user transaction.
 */
public abstract class GridDhtTxLocalAdapter<K, V> extends IgniteTxLocalAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near mappings. */
    protected Map<UUID, GridDistributedTxMapping<K, V>> nearMap =
        new ConcurrentHashMap8<>();

    /** DHT mappings. */
    protected Map<UUID, GridDistributedTxMapping<K, V>> dhtMap =
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
        GridCacheSharedContext<K, V> cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean storeEnabled,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(cctx, xidVer, implicit, implicitSingle, sys, concurrency, isolation, timeout, invalidate, storeEnabled,
            txSize, grpLockKey, partLock, subjId, taskNameHash);

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
        GridDhtCacheEntry<K, V> cached,
        IgniteTxEntry<K, V> entry,
        long topVer);

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

            Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> dhtEntryMap = null;
            Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> nearEntryMap = null;

            for (IgniteTxEntry<K, V> e : allEntries()) {
                assert e.cached() != null;

                GridCacheContext<K, V> cacheCtx = e.cached().context();

                if (cacheCtx.isNear())
                    continue;

                if (e.cached().obsolete()) {
                    GridCacheEntryEx<K, V> cached = cacheCtx.cache().entryEx(e.key());

                    e.cached(cached, cached.keyBytes());
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
                                (GridDhtCacheEntry<K, V>)e.cached(), log, dhtEntryMap, nearEntryMap);
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        GridCacheEntryEx<K, V> cached = cacheCtx.cache().entryEx(e.key());

                        e.cached(cached, cached.keyBytes());
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
    Map<UUID, GridDistributedTxMapping<K, V>> dhtMap() {
        mapExplicitLocks();

        return dhtMap;
    }

    /**
     * @return Near map.
     */
    Map<UUID, GridDistributedTxMapping<K, V>> nearMap() {
        mapExplicitLocks();

        return nearMap;
    }

    /**
     * @param nodeId Node ID.
     * @return Mapping.
     */
    GridDistributedTxMapping<K, V> dhtMapping(UUID nodeId) {
        return dhtMap.get(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @return Mapping.
     */
    GridDistributedTxMapping<K, V> nearMapping(UUID nodeId) {
        return nearMap.get(nodeId);
    }

    /**
     * @param mappings Mappings to add.
     */
    void addDhtNodeEntryMapping(Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> mappings) {
        addMapping(mappings, dhtMap);
    }

    /**
     * @param mappings Mappings to add.
     */
    void addNearNodeEntryMapping(Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> mappings) {
        addMapping(mappings, nearMap);
    }

    /**
     * @param mappings Mappings to add.
     */
    public void addDhtMapping(Map<UUID, GridDistributedTxMapping<K, V>> mappings) {
        addMapping0(mappings, dhtMap);
    }

    /**
     * @param mappings Mappings to add.
     */
    public void addNearMapping(Map<UUID, GridDistributedTxMapping<K, V>> mappings) {
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
    boolean removeDhtMapping(UUID nodeId, GridCacheEntryEx<K, V> entry) {
        return removeMapping(nodeId, entry, dhtMap);
    }

    /**
     * @param nodeId Node ID.
     * @param entry Entry to remove.
     * @return {@code True} if was removed.
     */
    boolean removeNearMapping(UUID nodeId, GridCacheEntryEx<K, V> entry) {
        return removeMapping(nodeId, entry, nearMap);
    }

    /**
     * @param nodeId Node ID.
     * @param entry Entry to remove.
     * @param map Map to remove from.
     * @return {@code True} if was removed.
     */
    private boolean removeMapping(UUID nodeId, @Nullable GridCacheEntryEx<K, V> entry,
        Map<UUID, GridDistributedTxMapping<K, V>> map) {
        if (entry != null) {
            if (log.isDebugEnabled())
                log.debug("Removing mapping for entry [nodeId=" + nodeId + ", entry=" + entry + ']');

            IgniteTxEntry<K, V> txEntry = txMap.get(entry.txKey());

            if (txEntry == null)
                return false;

            GridDistributedTxMapping<K, V> m = map.get(nodeId);

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
        Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> mappings,
        Map<UUID, GridDistributedTxMapping<K, V>> dst
    ) {
        for (Map.Entry<ClusterNode, List<GridDhtCacheEntry<K, V>>> mapping : mappings.entrySet()) {
            ClusterNode n = mapping.getKey();

            GridDistributedTxMapping<K, V> m = dst.get(n.id());

            List<GridDhtCacheEntry<K, V>> entries = mapping.getValue();

            for (GridDhtCacheEntry<K, V> entry : entries) {
                IgniteTxEntry<K, V> txEntry = txMap.get(entry.txKey());

                if (txEntry != null) {
                    if (m == null)
                        dst.put(n.id(), m = new GridDistributedTxMapping<>(n));

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
        Map<UUID, GridDistributedTxMapping<K, V>> mappings,
        Map<UUID, GridDistributedTxMapping<K, V>> dst
    ) {
        for (Map.Entry<UUID, GridDistributedTxMapping<K, V>> entry : mappings.entrySet()) {
            GridDistributedTxMapping<K, V> targetMapping = dst.get(entry.getKey());

            if (targetMapping == null)
                dst.put(entry.getKey(), entry.getValue());
            else {
                for (IgniteTxEntry<K, V> txEntry : entry.getValue().entries())
                    targetMapping.add(txEntry);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(GridCacheContext<K, V> ctx, int part) {
        assert false : "DHT transaction encountered invalid partition [part=" + part + ", tx=" + this + ']';
    }

    /**
     * @param msgId Message ID.
     * @param e Entry to add.
     * @return Future for active transactions for the time when reader was added.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteInternalFuture<Boolean> addEntry(long msgId, IgniteTxEntry<K, V> e) throws IgniteCheckedException {
        init();

        IgniteTxState state = state();

        assert state == PREPARING : "Invalid tx state for " +
            "adding entry [msgId=" + msgId + ", e=" + e + ", tx=" + this + ']';

        e.unmarshal(cctx, false, cctx.deploy().globalLoader());

        checkInternal(e.txKey());

        GridCacheContext<K, V> cacheCtx = e.context();

        GridDhtCacheAdapter<K, V> dhtCache = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

        try {
            IgniteTxEntry<K, V> entry = txMap.get(e.txKey());

            if (entry != null) {
                entry.op(e.op()); // Absolutely must set operation, as default is DELETE.
                entry.value(e.value(), e.hasWriteValue(), e.hasReadValue());
                entry.entryProcessors(e.entryProcessors());
                entry.valueBytes(e.valueBytes());
                entry.ttl(e.ttl());
                entry.filters(e.filters());
                entry.expiry(e.expiry());
                entry.drExpireTime(e.drExpireTime());
                entry.drVersion(e.drVersion());
            }
            else {
                entry = e;

                addActiveCache(dhtCache.context());

                while (true) {
                    GridDhtCacheEntry<K, V> cached = dhtCache.entryExx(entry.key(), topologyVersion());

                    try {
                        // Set key bytes to avoid serializing in future.
                        cached.keyBytes(entry.keyBytes());

                        entry.cached(cached, entry.keyBytes());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry when adding to dht tx (will retry): " + cached);
                    }
                }

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

            return new GridFinishedFuture<>(cctx.kernalContext(), true);
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
    IgniteInternalFuture<GridCacheReturn<V>> lockAllAsync(
        GridCacheContext<K, V> cacheCtx,
        List<GridCacheEntryEx<K, V>> entries,
        boolean onePhaseCommit,
        long msgId,
        final boolean read,
        long accessTtl
    ) {
        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        final GridCacheReturn<V> ret = new GridCacheReturn<>(false);

        if (F.isEmpty(entries))
            return new GridFinishedFuture<>(cctx.kernalContext(), ret);

        init();

        onePhaseCommit(onePhaseCommit);

        try {
            Set<K> skipped = null;

            long topVer = topologyVersion();

            GridDhtCacheAdapter<K, V> dhtCache = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

            // Enlist locks into transaction.
            for (int i = 0; i < entries.size(); i++) {
                GridCacheEntryEx<K, V> entry = entries.get(i);

                K key = entry.key();

                IgniteTxEntry<K, V> txEntry = entry(entry.txKey());

                // First time access.
                if (txEntry == null) {
                    GridDhtCacheEntry<K, V> cached = dhtCache.entryExx(key, topVer);

                    addActiveCache(dhtCache.context());

                    cached.unswap(!read, read);

                    txEntry = addEntry(NOOP,
                        null,
                        null,
                        null,
                        cached,
                        null,
                        CU.<K, V>empty(),
                        false,
                        -1L,
                        -1L,
                        null);

                    if (read)
                        txEntry.ttl(accessTtl);

                    txEntry.cached(cached, txEntry.keyBytes());

                    addReader(msgId, cached, txEntry, topVer);
                }
                else {
                    if (skipped == null)
                        skipped = new GridLeanSet<>();

                    skipped.add(key);
                }
            }

            assert pessimistic();

            Collection<K> keys = F.viewReadOnly(entries, CU.<K, V>entry2Key());

            // Acquire locks only after having added operation to the write set.
            // Otherwise, during rollback we will not know whether locks need
            // to be rolled back.
            // Loose all skipped and previously locked (we cannot reenter locks here).
            final Collection<? extends K> passedKeys = skipped != null ? F.view(keys, F0.notIn(skipped)) : keys;

            if (log.isDebugEnabled())
                log.debug("Lock keys: " + passedKeys);

            return obtainLockAsync(cacheCtx, ret, passedKeys, read, skipped, accessTtl, null);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
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
    private IgniteInternalFuture<GridCacheReturn<V>> obtainLockAsync(
        final GridCacheContext<K, V> cacheCtx,
        GridCacheReturn<V> ret,
        final Collection<? extends K> passedKeys,
        final boolean read,
        final Set<K> skipped,
        final long accessTtl,
        @Nullable final IgnitePredicate<CacheEntry<K, V>>[] filter) {
        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys [passedKeys=" + passedKeys + ", skipped=" +
                skipped + ']');

        if (passedKeys.isEmpty())
            return new GridFinishedFuture<>(cctx.kernalContext(), ret);

        GridDhtTransactionalCacheAdapter<K, V> dhtCache = cacheCtx.isNear() ? cacheCtx.nearTx().dht() : cacheCtx.dhtTx();

        IgniteInternalFuture<Boolean> fut = dhtCache.lockAllAsyncInternal(passedKeys,
            lockTimeout(),
            this,
            isInvalidate(),
            read,
            /*retval*/false,
            isolation,
            accessTtl,
            CU.<K, V>empty());

        return new GridEmbeddedFuture<>(
            fut,
            new PLC1<GridCacheReturn<V>>(ret) {
                @Override protected GridCacheReturn<V> postLock(GridCacheReturn<V> ret) throws IgniteCheckedException {
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
                        filter == null ? CU.<K, V>empty() : filter,
                        /**computeInvoke*/false);

                    return ret;
                }
            },
            cctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override protected void addGroupTxMapping(Collection<IgniteTxKey<K>> keys) {
        assert groupLock();

        for (GridDistributedTxMapping<K, V> mapping : dhtMap.values())
            mapping.entries(Collections.unmodifiableCollection(txMap.values()), true);

        // Here we know that affinity key for all given keys is our group lock key.
        // Just add entries to dht mapping.
        // Add near readers. If near cache is disabled on all nodes, do nothing.
        Collection<UUID> backupIds = dhtMap.keySet();

        Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> locNearMap = null;

        for (IgniteTxKey<K> key : keys) {
            IgniteTxEntry<K, V> txEntry = entry(key);

            if (!txEntry.groupLockEntry() || txEntry.context().isNear())
                continue;

            assert txEntry.cached() instanceof GridDhtCacheEntry : "Invalid entry type: " + txEntry.cached();

            while (true) {
                try {
                    GridDhtCacheEntry<K, V> entry = (GridDhtCacheEntry<K, V>)txEntry.cached();

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

                            List<GridDhtCacheEntry<K, V>> entries = locNearMap.get(n);

                            if (entries == null)
                                locNearMap.put(n, entries = new LinkedList<>());

                            entries.add(entry);
                        }
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // Retry.
                    txEntry.cached(txEntry.context().dht().entryExx(key.key(), topologyVersion()), txEntry.keyBytes());
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
                IgniteTxState state = state();

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
    protected abstract void clearPrepareFuture(GridDhtTxPrepareFuture<K, V> fut);

    /** {@inheritDoc} */
    @Override public void rollback() throws IgniteCheckedException {
        try {
            rollbackAsync().get();
        }
        finally {
            cctx.tm().txContextReset();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxLocalAdapter.class, this, "nearNodes", nearMap.keySet(),
            "dhtNodes", dhtMap.keySet(), "super", super.toString());
    }
}
