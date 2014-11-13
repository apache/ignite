/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridDistributedTxRemoteAdapter<K, V> extends GridCacheTxAdapter<K, V>
    implements GridCacheTxRemoteEx<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Read set. */
    @GridToStringInclude
    protected Map<GridCacheTxKey<K>, GridCacheTxEntry<K, V>> readMap;

    /** Write map. */
    @GridToStringInclude
    protected Map<GridCacheTxKey<K>, GridCacheTxEntry<K, V>> writeMap;

    /** Remote thread ID. */
    @GridToStringInclude
    private long rmtThreadId;

    /** Explicit versions. */
    @GridToStringInclude
    private List<GridCacheVersion> explicitVers;

    /** Started flag. */
    @GridToStringInclude
    private boolean started;

    /** {@code True} only if all write entries are locked by this transaction. */
    @GridToStringInclude
    private AtomicBoolean commitAllowed = new AtomicBoolean(false);

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDistributedTxRemoteAdapter() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     * @param nodeId Node ID.
     * @param rmtThreadId Remote thread ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     */
    public GridDistributedTxRemoteAdapter(
        GridCacheSharedContext<K, V> ctx,
        UUID nodeId,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable GridCacheTxKey grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(
            ctx,
            nodeId,
            xidVer,
            ctx.versions().last(),
            Thread.currentThread().getId(),
            concurrency,
            isolation,
            timeout,
            txSize,
            grpLockKey,
            subjId,
            taskNameHash);

        this.rmtThreadId = rmtThreadId;
        this.invalidate = invalidate;

        commitVersion(commitVer);

        // Must set started flag after concurrency and isolation.
        started = true;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        return Collections.singleton(nodeId);
    }

    /** {@inheritDoc} */
    @Override public UUID originatingNodeId() {
        return nodeId;
    }

    /**
     * @return Checks if transaction has no entries.
     */
    @Override public boolean empty() {
        return readMap.isEmpty() && writeMap.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean removed(GridCacheTxKey<K> key) {
        GridCacheTxEntry e = writeMap.get(key);

        return e != null && e.op() == DELETE;
    }

    /** {@inheritDoc} */
    @Override public void invalidate(boolean invalidate) {
        this.invalidate = invalidate;
    }

    /** {@inheritDoc} */
    @Override public Map<GridCacheTxKey<K>, GridCacheTxEntry<K, V>> writeMap() {
        return writeMap;
    }

    /** {@inheritDoc} */
    @Override public Map<GridCacheTxKey<K>, GridCacheTxEntry<K, V>> readMap() {
        return readMap;
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        // No-op.
    }

    /**
     * Adds group lock key to remote transaction.
     *
     * @param key Key.
     */
    public void groupLockKey(GridCacheTxKey key) {
        if (grpLockKey == null)
            grpLockKey = key;
    }

    /** {@inheritDoc} */
    @Override public GridTuple<V> peek(GridCacheContext<K, V> cacheCtx, boolean failFast, K key,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridCacheFilterFailedException {
        assert false : "Method peek can only be called on user transaction: " + this;

        throw new IllegalStateException("Method peek can only be called on user transaction: " + this);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxEntry<K, V> entry(GridCacheTxKey<K> key) {
        GridCacheTxEntry<K, V> e = writeMap == null ? null : writeMap.get(key);

        if (e == null)
            e = readMap == null ? null : readMap.get(key);

        return e;
    }

    /**
     * Clears entry from transaction as it never happened.
     *
     * @param key key to be removed.
     */
    public void clearEntry(GridCacheTxKey<K> key) {
        readMap.remove(key);
        writeMap.remove(key);
    }

    /**
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    @Override public void doneRemote(GridCacheVersion baseVer, Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers, Collection<GridCacheVersion> pendingVers) {
        if (readMap != null && !readMap.isEmpty()) {
            for (GridCacheTxEntry<K, V> txEntry : readMap.values())
                doneRemote(txEntry, baseVer, committedVers, rolledbackVers, pendingVers);
        }

        if (writeMap != null && !writeMap.isEmpty()) {
            for (GridCacheTxEntry<K, V> txEntry : writeMap.values())
                doneRemote(txEntry, baseVer, committedVers, rolledbackVers, pendingVers);
        }
    }

    /**
     * Adds completed versions to an entry.
     *
     * @param txEntry Entry.
     * @param baseVer Base version for completed versions.
     * @param committedVers Completed versions relative to base version.
     * @param rolledbackVers Rolled back versions relative to base version.
     * @param pendingVers Pending versions.
     */
    private void doneRemote(GridCacheTxEntry<K, V> txEntry, GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers) {
        while (true) {
            GridDistributedCacheEntry<K, V> entry = (GridDistributedCacheEntry<K, V>)txEntry.cached();

            try {
                // Handle explicit locks.
                GridCacheVersion doneVer = txEntry.explicitVersion() != null ? txEntry.explicitVersion() : xidVer;

                entry.doneRemote(doneVer, baseVer, pendingVers, committedVers, rolledbackVers, isSystemInvalidate());

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                assert entry.obsoleteVersion() != null;

                if (log.isDebugEnabled())
                    log.debug("Replacing obsolete entry in remote transaction [entry=" + entry + ", tx=" + this + ']');

                // Replace the entry.
                txEntry.cached(txEntry.context().cache().entryEx(txEntry.key().key()), txEntry.keyBytes());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        try {
            if (hasWriteKey(entry.txKey())) {
                commitIfLocked();

                return true;
            }
        }
        catch (GridException e) {
            U.error(log, "Failed to commit remote transaction: " + this, e);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return started;
    }

    /**
     * @return Remote node thread ID.
     */
    @Override public long remoteThreadId() {
        return rmtThreadId;
    }

    /**
     * @param key Key to add to read set.
     * @param keyBytes Key bytes.
     * @param drVer Data center replication version.
     */
    public void addRead(GridCacheContext<K, V> cacheCtx, GridCacheTxKey<K> key, byte[] keyBytes, @Nullable GridCacheVersion drVer) {
        checkInternal(key);

        GridCacheTxEntry<K, V> txEntry = new GridCacheTxEntry<>(cacheCtx, this, READ, null, 0L, -1L,
            cacheCtx.cache().entryEx(key.key()), drVer);

        txEntry.keyBytes(keyBytes);

        readMap.put(key, txEntry);
    }

    /**
     * @param key Key to add to write set.
     * @param keyBytes Key bytes.
     * @param op Cache operation.
     * @param val Write value.
     * @param valBytes Write value bytes.
     * @param drVer Data center replication version.
     */
    public void addWrite(GridCacheContext<K, V> cacheCtx, GridCacheTxKey<K> key, byte[] keyBytes, GridCacheOperation op, V val, byte[] valBytes,
        @Nullable GridCacheVersion drVer) {
        checkInternal(key);

        GridCacheTxEntry<K, V> txEntry = new GridCacheTxEntry<>(cacheCtx, this, op, val, 0L, -1L,
            cacheCtx.cache().entryEx(key.key()), drVer);

        txEntry.keyBytes(keyBytes);
        txEntry.valueBytes(valBytes);

        writeMap.put(key, txEntry);
    }

    /**
     * @param e Transaction entry to set.
     * @return {@code True} if value was set.
     */
    @Override public boolean setWriteValue(GridCacheTxEntry<K, V> e) {
        checkInternal(e.key());

        GridCacheTxEntry<K, V> entry = writeMap.get(e.key());

        if (entry == null) {
            GridCacheTxEntry<K, V> rmv = readMap.remove(e.key());

            if (rmv != null) {
                e.cached(rmv.cached(), rmv.keyBytes());

                writeMap.put(e.key(), e);
            }
            // If lock is explicit.
            else {
                e.cached(e.context().cache().entryEx(e.key().key()), null);

                // explicit lock.
                writeMap.put(e.key(), e);
            }
        }
        else {
            // Copy values.
            entry.value(e.value(), e.hasWriteValue(), e.hasReadValue());
            entry.transformClosures(e.transformClosures());
            entry.valueBytes(e.valueBytes());
            entry.op(e.op());
            entry.ttl(e.ttl());
            entry.explicitVersion(e.explicitVersion());
            entry.groupLockEntry(e.groupLockEntry());

            // DR stuff.
            entry.drVersion(e.drVersion());
            entry.drExpireTime(e.drExpireTime());
        }

        addExplicit(e);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(GridCacheTxKey<K> key) {
        return writeMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTxEx<K, V>> prepareAsync() {
        assert false;
        return null;
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheTxKey<K>> readSet() {
        return readMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheTxKey<K>> writeSet() {
        return writeMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> allEntries() {
        return F.concat(false, writeEntries(), readEntries());
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> writeEntries() {
        return writeMap.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> readEntries() {
        return readMap.values();
    }

    /**
     * Prepare phase.
     *
     * @throws GridException If prepare failed.
     */
    @Override public void prepare() throws GridException {
        // If another thread is doing prepare or rollback.
        if (!state(PREPARING)) {
            // In optimistic mode prepare may be called multiple times.
            if(state() != PREPARING || !optimistic()) {
                if (log.isDebugEnabled())
                    log.debug("Invalid transaction state for prepare: " + this);

                return;
            }
        }

        try {
            cctx.tm().prepareTx(this);

            if (pessimistic() || isSystemInvalidate())
                state(PREPARED);
        }
        catch (GridException e) {
            setRollbackOnly();

            throw e;
        }
    }

    /**
     * @throws GridException If commit failed.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void commitIfLocked() throws GridException {
        if (state() == COMMITTING) {
            for (GridCacheTxEntry<K, V> txEntry : writeMap.values()) {
                assert txEntry != null : "Missing transaction entry for tx: " + this;

                while (true) {
                    GridCacheEntryEx<K, V> cacheEntry = txEntry.cached();

                    assert cacheEntry != null : "Missing cached entry for transaction entry: " + txEntry;

                    try {
                        GridCacheVersion ver = txEntry.explicitVersion() != null ? txEntry.explicitVersion() : xidVer;

                        // If locks haven't been acquired yet, keep waiting.
                        if (!txEntry.groupLockEntry() && !cacheEntry.lockedBy(ver)) {
                            if (log.isDebugEnabled())
                                log.debug("Transaction does not own lock for entry (will wait) [entry=" + cacheEntry +
                                    ", tx=" + this + ']');

                            return;
                        }

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry while committing (will retry): " + txEntry);

                        txEntry.cached(txEntry.context().cache().entryEx(txEntry.key().key()), txEntry.keyBytes());
                    }
                }
            }

            // Only one thread gets to commit.
            if (commitAllowed.compareAndSet(false, true)) {
                GridException err = null;

                if (!F.isEmpty(writeMap)) {
                    // Register this transaction as completed prior to write-phase to
                    // ensure proper lock ordering for removed entries.
                    cctx.tm().addCommittedTx(this);

                    long topVer = topologyVersion();

                    // Node that for near transactions we grab all entries.
                    for (GridCacheTxEntry<K, V> txEntry : (near() ? allEntries() : writeEntries())) {
                        GridCacheContext<K, V> cacheCtx = txEntry.context();

                        boolean replicate = cacheCtx.isDrEnabled();

                        try {
                            while (true) {
                                try {
                                    GridCacheEntryEx<K, V> cached = txEntry.cached();

                                    if (cached == null)
                                        txEntry.cached(cached = cacheCtx.cache().entryEx(txEntry.key().key()), null);

                                    if (near() && cacheCtx.config().getDrReceiverConfiguration() != null) {
                                        cached.markObsolete(xidVer);

                                        break;
                                    }

                                    GridNearCacheEntry<K, V> nearCached = null;

                                    if (updateNearCache(cacheCtx, txEntry.key().key(), topVer))
                                        nearCached = cacheCtx.dht().near().peekExx(txEntry.key().key());

                                    if (!F.isEmpty(txEntry.transformClosures()) || !F.isEmpty(txEntry.filters()))
                                        txEntry.cached().unswap(true, false);

                                    GridTuple3<GridCacheOperation, V, byte[]> res = applyTransformClosures(txEntry,
                                        false);

                                    GridCacheOperation op = res.get1();
                                    V val = res.get2();
                                    byte[] valBytes = res.get3();

                                    // Preserve TTL if needed.
                                    if (txEntry.ttl() < 0)
                                        txEntry.ttl(cached.rawTtl());

                                    GridCacheVersion explicitVer = txEntry.drVersion();

                                    if (finalizationStatus() == FinalizationStatus.RECOVERY_FINISH || optimistic()) {
                                        // Primary node has left the grid so we have to process conflicts on backups.
                                        if (explicitVer == null)
                                            explicitVer = writeVersion(); // Force write version to be used.

                                        boolean drNeedResolve = cacheCtx.drNeedResolve(cached.version(), explicitVer);

                                        if (drNeedResolve) {
                                            GridBiTuple<GridCacheOperation, GridDrReceiverConflictContextImpl<K, V>>
                                                drRes = drResolveConflict(op, txEntry.key().key(), val, valBytes,
                                                txEntry.ttl(), txEntry.drExpireTime(), explicitVer, cached);

                                            assert drRes != null;

                                            GridDrReceiverConflictContextImpl<K, V> drCtx = drRes.get2();

                                            if (drCtx.isUseOld())
                                                op = NOOP;
                                            else if (drCtx.isUseNew()) {
                                                txEntry.ttl(drCtx.ttl());

                                                if (drCtx.newEntry().dataCenterId() !=
                                                    cctx.gridConfig().getDataCenterId())
                                                    txEntry.drExpireTime(drCtx.expireTime());
                                                else
                                                    txEntry.drExpireTime(-1L);
                                            }
                                            else if (drCtx.isMerge()) {
                                                op = drRes.get1();
                                                val = drCtx.mergeValue();
                                                valBytes = null;
                                                explicitVer = writeVersion();

                                                txEntry.ttl(drCtx.ttl());
                                                txEntry.drExpireTime(-1L);
                                            }
                                        }
                                        else
                                            // Nullify explicit version so that innerSet/innerRemove will work as usual.
                                            explicitVer = null;
                                    }

                                    if (op == CREATE || op == UPDATE) {
                                        // Invalidate only for near nodes (backups cannot be invalidated).
                                        if (isSystemInvalidate() || (isInvalidate() && cacheCtx.isNear()))
                                            cached.innerRemove(this, eventNodeId(), nodeId, false, false, true, true,
                                                topVer, txEntry.filters(), replicate ? DR_BACKUP : DR_NONE,
                                                near() ? null : explicitVer, CU.subjectId(this, cctx),
                                                resolveTaskName());
                                        else {
                                            cached.innerSet(this, eventNodeId(), nodeId, val, valBytes, false, false,
                                                txEntry.ttl(), true, true, topVer, txEntry.filters(),
                                                replicate ? DR_BACKUP : DR_NONE, txEntry.drExpireTime(),
                                                near() ? null : explicitVer, CU.subjectId(this, cctx),
                                                resolveTaskName());

                                            // Keep near entry up to date.
                                            if (nearCached != null) {
                                                V val0 = null;
                                                byte[] valBytes0 = null;

                                                GridCacheValueBytes valBytesTuple = cached.valueBytes();

                                                if (!valBytesTuple.isNull()) {
                                                    if (valBytesTuple.isPlain())
                                                        val0 = (V)valBytesTuple.get();
                                                    else
                                                        valBytes0 = valBytesTuple.get();
                                                }
                                                else
                                                    val0 = cached.rawGet();

                                                nearCached.updateOrEvict(xidVer, val0, valBytes0, cached.expireTime(),
                                                    cached.ttl(), nodeId);
                                            }
                                        }
                                    }
                                    else if (op == DELETE) {
                                        cached.innerRemove(this, eventNodeId(), nodeId, false, false, true, true,
                                            topVer, txEntry.filters(), replicate ? DR_BACKUP : DR_NONE,
                                            near() ? null : explicitVer, CU.subjectId(this, cctx), resolveTaskName());

                                        // Keep near entry up to date.
                                        if (nearCached != null)
                                            nearCached.updateOrEvict(xidVer, null, null, 0, 0, nodeId);
                                    }
                                    else if (op == RELOAD) {
                                        V reloaded = cached.innerReload(CU.<K, V>empty());

                                        if (nearCached != null) {
                                            nearCached.innerReload(CU.<K, V>empty());

                                            nearCached.updateOrEvict(cached.version(), reloaded, null,
                                                cached.expireTime(), cached.ttl(), nodeId);
                                        }
                                    }
                                    else if (op == READ) {
                                        assert near();

                                        if (log.isDebugEnabled())
                                            log.debug("Ignoring READ entry when committing: " + txEntry);
                                    }
                                    // No-op.
                                    else {
                                        assert !groupLock() || txEntry.groupLockEntry() || ownsLock(txEntry.cached()):
                                            "Transaction does not own lock for group lock entry during  commit [tx=" +
                                                this + ", txEntry=" + txEntry + ']';

                                        if (nearCached != null) {
                                            V val0 = null;
                                            byte[] valBytes0 = null;

                                            GridCacheValueBytes valBytesTuple = cached.valueBytes();

                                            if (!valBytesTuple.isNull()) {
                                                if (valBytesTuple.isPlain())
                                                    val0 = (V)valBytesTuple.get();
                                                else
                                                    valBytes0 = valBytesTuple.get();
                                            }
                                            else
                                                val0 = cached.rawGet();

                                            nearCached.updateOrEvict(xidVer, val0, valBytes0, cached.expireTime(),
                                                cached.ttl(), nodeId);
                                        }
                                    }

                                    // Assert after setting values as we want to make sure
                                    // that if we replaced removed entries.
                                    assert
                                        txEntry.op() == READ || onePhaseCommit() ||
                                            // If candidate is not there, then lock was explicit
                                            // and we simply allow the commit to proceed.
                                            !cached.hasLockCandidateUnsafe(xidVer) || cached.lockedByUnsafe(xidVer) :
                                        "Transaction does not own lock for commit [entry=" + cached +
                                            ", tx=" + this + ']';

                                    // Break out of while loop.
                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Attempting to commit a removed entry (will retry): " + txEntry);

                                    // Renew cached entry.
                                    txEntry.cached(cacheCtx.cache().entryEx(txEntry.key().key()), txEntry.keyBytes());
                                }
                            }
                        }
                        catch (Throwable ex) {
                            state(UNKNOWN);

                            // In case of error, we still make the best effort to commit,
                            // as there is no way to rollback at this point.
                            err = ex instanceof GridException ? (GridException)ex :
                                new GridException("Commit produced a runtime exception: " + this, ex);
                        }
                    }
                }

                if (err != null) {
                    state(UNKNOWN);

                    throw err;
                }

                cctx.tm().commitTx(this);

                state(COMMITTED);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void commit() throws GridException {
        if (optimistic())
            state(PREPARED);

        if (!state(COMMITTING)) {
            GridCacheTxState state = state();

            // If other thread is doing commit, then no-op.
            if (state == COMMITTING || state == COMMITTED)
                return;

            if (log.isDebugEnabled())
                log.debug("Failed to set COMMITTING transaction state (will rollback): " + this);

            setRollbackOnly();

            if (!isSystemInvalidate())
                throw new GridException("Invalid transaction state for commit [state=" + state + ", tx=" + this + ']');

            rollback();
        }

        commitIfLocked();
    }

    /**
     * Forces commit for this tx.
     *
     * @throws GridException If commit failed.
     */
    public void forceCommit() throws GridException {
        commitIfLocked();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTx> commitAsync() {
        try {
            commit();

            return new GridFinishedFutureEx<GridCacheTx>(this);
        }
        catch (GridException e) {
            return new GridFinishedFutureEx<>(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass"})
    @Override public void rollback() {
        try {
            // Note that we don't evict near entries here -
            // they will be deleted by their corresponding transactions.
            if (state(ROLLING_BACK)) {
                cctx.tm().rollbackTx(this);

                state(ROLLED_BACK);
            }
        }
        catch (RuntimeException | Error e) {
            state(UNKNOWN);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTx> rollbackAsync() {
        rollback();

        return new GridFinishedFutureEx<GridCacheTx>(this);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheVersion> alternateVersions() {
        return explicitVers == null ? Collections.<GridCacheVersion>emptyList() : explicitVers;
    }

    /**
     * Adds explicit version if there is one.
     *
     * @param e Transaction entry.
     */
    protected void addExplicit(GridCacheTxEntry<K, V> e) {
        if (e.explicitVersion() != null) {
            if (explicitVers == null)
                explicitVers = new LinkedList<>();

            if (!explicitVers.contains(e.explicitVersion())) {
                explicitVers.add(e.explicitVersion());

                if (log.isDebugEnabled())
                    log.debug("Added explicit version to transaction [explicitVer=" + e.explicitVersion() +
                        ", tx=" + this + ']');

                // Register alternate version with TM.
                cctx.tm().addAlternateVersion(e.explicitVersion(), this);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxRemoteAdapter.class, this, "super", super.toString());
    }
}
