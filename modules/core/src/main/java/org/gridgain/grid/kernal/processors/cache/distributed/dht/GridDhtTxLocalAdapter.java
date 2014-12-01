/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Replicated user transaction.
 */
public abstract class GridDhtTxLocalAdapter<K, V> extends GridCacheTxLocalAdapter<K, V> {
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
    private boolean explicitLock;

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
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param explicitLock Explicit lock flag.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock If this is a group-lock transaction and the whole partition should be locked.
     */
    protected GridDhtTxLocalAdapter(
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        GridCacheSharedContext<K, V> cctx,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean explicitLock,
        int txSize,
        @Nullable GridCacheTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(cctx, xidVer, implicit, implicitSingle, concurrency, isolation, timeout, txSize, grpLockKey, partLock,
            subjId, taskNameHash);

        assert cctx != null;

        this.explicitLock = explicitLock;

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
    protected abstract GridUuid nearFutureId();

    /**
     * @return Near future mini ID.
     */
    protected abstract GridUuid nearMiniId();

    /**
     * Adds reader to cached entry.
     *
     * @param msgId Message ID.
     * @param cached Cached entry.
     * @param entry Transaction entry.
     * @return {@code True} if reader was added as a result of this call.
     */
    @Nullable protected abstract GridFuture<Boolean> addReader(long msgId, GridDhtCacheEntry<K, V> cached,
        GridCacheTxEntry<K, V> entry, long topVer);

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
     * @return Explicit lock flag.
     */
    boolean explicitLock() {
        return explicitLock;
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

            Map<GridNode, List<GridDhtCacheEntry<K, V>>> dhtEntryMap = null;
            Map<GridNode, List<GridDhtCacheEntry<K, V>>> nearEntryMap = null;

            for (GridCacheTxEntry<K, V> e : allEntries()) {
                assert e.cached() != null;

                GridCacheContext<K, V> cacheCtx = e.cached().context();

                if (cacheCtx.isNear())
                    continue;

                if (e.cached().obsolete()) {
                    GridCacheEntryEx<K, V> cached = cacheCtx.cache().entryEx(e.key());

                    e.cached(cached, cached.keyBytes());
                }

                if (e.cached().detached())
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
                addDhtMapping(dhtEntryMap);

            if (!F.isEmpty(nearEntryMap))
                addNearMapping(nearEntryMap);

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
    void addDhtMapping(Map<GridNode, List<GridDhtCacheEntry<K, V>>> mappings) {
        addMapping(mappings, dhtMap);
    }

    /**
     * @param mappings Mappings to add.
     */
    void addNearMapping(Map<GridNode, List<GridDhtCacheEntry<K, V>>> mappings) {
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

            GridCacheTxEntry<K, V> txEntry = txMap.get(entry.txKey());

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
     * @param map Transaction mappings.
     */
    private void addMapping(Map<GridNode, List<GridDhtCacheEntry<K, V>>> mappings,
        Map<UUID, GridDistributedTxMapping<K, V>> map) {
        for (Map.Entry<GridNode, List<GridDhtCacheEntry<K, V>>> mapping : mappings.entrySet()) {
            GridNode n = mapping.getKey();

            for (GridDhtCacheEntry<K, V> entry : mapping.getValue()) {
                GridCacheTxEntry<K, V> txEntry = txMap.get(entry.txKey());

                if (txEntry != null) {
                    GridDistributedTxMapping<K, V> m = map.get(n.id());

                    if (m == null)
                        map.put(n.id(), m = new GridDistributedTxMapping<>(n));

                    m.add(txEntry);
                }
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
     * @throws GridException If failed.
     */
    @Nullable public GridFuture<Boolean> addEntry(long msgId, GridCacheTxEntry<K, V> e) throws GridException {
        init();

        GridCacheTxState state = state();

        assert state == ACTIVE || (state == PREPARING && optimistic()) : "Invalid tx state for " +
            "adding entry [msgId=" + msgId + ", e=" + e + ", tx=" + this + ']';

        e.unmarshal(cctx, false, cctx.deploy().globalLoader());

        checkInternal(e.txKey());

        state = state();

        assert state == ACTIVE || (state == PREPARING && optimistic()): "Invalid tx state for adding entry: " + e;

        GridCacheContext<K, V> cacheCtx = e.context();

        GridDhtCacheAdapter<K, V> dhtCache = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

        try {
            GridCacheTxEntry<K, V> entry = txMap.get(e.txKey());

            if (entry != null) {
                entry.op(e.op()); // Absolutely must set operation, as default is DELETE.
                entry.value(e.value(), e.hasWriteValue(), e.hasReadValue());
                entry.transformClosures(e.transformClosures());
                entry.valueBytes(e.valueBytes());
                entry.ttl(e.ttl());
                entry.filters(e.filters());
                entry.drExpireTime(e.drExpireTime());
            }
            else {
                entry = e;

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
                        throw new GridException("Failed to find dht mapping for explicit entry version: " + entry);

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
     * @param entries Entries to lock.
     * @param writeEntries Write entries for implicit transactions mapped to one node.
     * @param drVers DR versions.
     * @param msgId Message ID.
     * @param implicit Implicit flag.
     * @param read Read flag.
     * @return Lock future.
     */
    GridFuture<GridCacheReturn<V>> lockAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Collection<GridCacheEntryEx<K, V>> entries,
        List<GridCacheTxEntry<K, V>> writeEntries,
        boolean onePhaseCommit,
        GridCacheVersion[] drVers,
        long msgId,
        boolean implicit,
        final boolean read
    ) {
        try {
            checkValid();
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        final GridCacheReturn<V> ret = new GridCacheReturn<>(false);

        if (F.isEmpty(entries))
            return new GridFinishedFuture<>(cctx.kernalContext(), ret);

        init();

        onePhaseCommit(onePhaseCommit);

        try {
            assert drVers == null || entries.size() == drVers.length;

            Set<K> skipped = null;

            int idx = 0;
            int drVerIdx = 0;

            long topVer = topologyVersion();

            GridDhtCacheAdapter<K, V> dhtCache = cacheCtx.isNear() ? cacheCtx.near().dht() : cacheCtx.dht();

            // Enlist locks into transaction.
            for (GridCacheEntryEx<K, V> entry : entries) {
                K key = entry.key();

                GridCacheTxEntry<K, V> txEntry = entry(entry.txKey());

                // First time access.
                if (txEntry == null) {
                    GridDhtCacheEntry<K, V> cached = dhtCache.entryExx(key, topVer);

                    cached.unswap(!read, read);

                    GridCacheTxEntry<K, V> w = writeEntries == null ? null : writeEntries.get(idx++);

                    txEntry = addEntry(NOOP, null, null, cached, -1, CU.<K, V>empty(), false, -1L, -1L,
                        drVers != null ? drVers[drVerIdx++] : null);

                    if (w != null) {
                        assert key.equals(w.key()) : "Invalid entry [cached=" + cached + ", w=" + w + ']';

                        txEntry.op(w.op());
                        txEntry.value(w.value(), w.hasWriteValue(), w.hasReadValue());
                        txEntry.valueBytes(w.valueBytes());
                        txEntry.drVersion(w.drVersion());
                        txEntry.transformClosures(w.transformClosures());
                        txEntry.ttl(w.ttl());
                        txEntry.filters(w.filters());
                        txEntry.drExpireTime(w.drExpireTime());
                    }

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

            return obtainLockAsync(cacheCtx, ret, passedKeys, read, skipped, null);
        }
        catch (GridException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /**
     * @param ret Return value.
     * @param passedKeys Passed keys.
     * @param read {@code True} if read.
     * @param skipped Skipped keys.
     * @param filter Entry write filter.
     * @return Future for lock acquisition.
     */
    private GridFuture<GridCacheReturn<V>> obtainLockAsync(
        final GridCacheContext<K, V> cacheCtx,
        GridCacheReturn<V> ret,
        final Collection<? extends K> passedKeys,
        boolean read,
        final Set<K> skipped,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter) {
        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys [passedKeys=" + passedKeys + ", skipped=" +
                skipped + ']');

        if (passedKeys.isEmpty())
            return new GridFinishedFuture<>(cctx.kernalContext(), ret);

        GridDhtTransactionalCacheAdapter<K, V> dhtCache = cacheCtx.isNear() ? cacheCtx.nearTx().dht() : cacheCtx.dhtTx();

        GridFuture<Boolean> fut = dhtCache.lockAllAsyncInternal(passedKeys,
            lockTimeout(), this, isInvalidate(), read, /*retval*/false, isolation, CU.<K, V>empty());

        return new GridEmbeddedFuture<>(
            fut,
            new PLC1<GridCacheReturn<V>>(ret) {
                @Override protected GridCacheReturn<V> postLock(GridCacheReturn<V> ret) throws GridException {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock on keys: " + passedKeys);

                    postLockWrite(cacheCtx, passedKeys, skipped, null, null, ret, /*remove*/false, /*retval*/false,
                        filter == null ? CU.<K, V>empty() : filter);

                    return ret;
                }
            },
            cctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override protected void addGroupTxMapping(Collection<GridCacheTxKey<K>> keys) {
        assert groupLock();

        for (GridDistributedTxMapping<K, V> mapping : dhtMap.values())
            mapping.entries(Collections.unmodifiableCollection(txMap.values()), true);

        // Here we know that affinity key for all given keys is our group lock key.
        // Just add entries to dht mapping.
        // Add near readers. If near cache is disabled on all nodes, do nothing.
        Collection<UUID> backupIds = dhtMap.keySet();

        Map<GridNode, List<GridDhtCacheEntry<K, V>>> locNearMap = null;

        for (GridCacheTxKey<K> key : keys) {
            GridCacheTxEntry<K, V> txEntry = entry(key);

            if (!txEntry.groupLockEntry())
                continue;

            assert txEntry.cached() instanceof GridDhtCacheEntry;

            while (true) {
                try {
                    GridDhtCacheEntry<K, V> entry = (GridDhtCacheEntry<K, V>)txEntry.cached();

                    Collection<UUID> readers = entry.readers();

                    if (!F.isEmpty(readers)) {
                        Collection<GridNode> nearNodes = cctx.discovery().nodes(readers, F0.notEqualTo(nearNodeId()),
                            F.notIn(backupIds));

                        if (log.isDebugEnabled())
                            log.debug("Mapping entry to near nodes [nodes=" + U.nodeIds(nearNodes) + ", entry=" +
                                entry + ']');

                        for (GridNode n : nearNodes) {
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

            if (locNearMap != null)
                addNearMapping(locNearMap);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean finish(boolean commit) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Finishing dht local tx [tx=" + this + ", commit=" + commit + "]");

        if (optimistic())
            state(PREPARED);

        if (commit) {
            if (!state(COMMITTING)) {
                GridCacheTxState state = state();

                if (state != COMMITTING && state != COMMITTED)
                    throw new GridException("Invalid transaction state for commit [state=" + state() +
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

        GridException err = null;

        // Commit to DB first. This way if there is a failure, transaction
        // won't be committed.
        try {
            if (commit && !isRollbackOnly())
                userCommit();
            else
                userRollback();
        }
        catch (GridException e) {
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

                        throw new GridException("Invalid transaction state for commit: " + this);
                    }
                }
            }
            else {
                if (!state(ROLLED_BACK)) {
                    state(UNKNOWN);

                    throw new GridException("Invalid transaction state for rollback: " + this);
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
    @Override public void rollback() throws GridException {
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
