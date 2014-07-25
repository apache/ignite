/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Replicated user transaction.
 */
class GridNearTxLocal<K, V> extends GridCacheTxLocalAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future. */
    private final AtomicReference<GridNearTxPrepareFuture<K, V>> prepFut =
        new AtomicReference<>();

    /** */
    private final AtomicReference<GridNearTxFinishFuture<K, V>> commitFut =
        new AtomicReference<>();

    /** */
    private final AtomicReference<GridNearTxFinishFuture<K, V>> rollbackFut =
        new AtomicReference<>();

    /** Topology snapshot on which this tx was started. */
    private final AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot =
        new AtomicReference<>();

    /** */
    private boolean syncCommit;

    /** */
    private boolean syncRollback;

    /** DHT mappings. */
    private ConcurrentMap<UUID, GridDistributedTxMapping<K, V>> mappings =
        new ConcurrentHashMap8<>();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxLocal() {
        // No-op.
    }

    /**
     * @param ctx   Cache registry.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit with one key flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param invalidate Invalidation policy.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param swapEnabled Whether to use swap storage.
     * @param storeEnabled Whether to use read/write through.
     * @param txSize Transaction size.
     * @param grpLockKey Group lock key if this is a group lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction and the whole partition should be locked.
     */
    GridNearTxLocal(
        GridCacheContext<K, V> ctx,
        boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean syncCommit,
        boolean syncRollback,
        boolean swapEnabled,
        boolean storeEnabled,
        int txSize,
        @Nullable Object grpLockKey,
        boolean partLock,
        @Nullable UUID subjId
    ) {
        super(ctx, ctx.versions().next(), implicit, implicitSingle, concurrency, isolation, timeout, invalidate,
            swapEnabled, storeEnabled && !ctx.writeToStoreFromDht(), txSize, grpLockKey, partLock, subjId);

        assert ctx != null;

        this.syncCommit = syncCommit;
        this.syncRollback = syncRollback;
    }

    /** {@inheritDoc} */
    @Override public boolean near() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion nearXidVersion() {
        return xidVer;
    }

    /** {@inheritDoc} */
    @Override public boolean enforceSerializable() {
        return false;
    }

    /**
     * @return DHT map.
     */
    ConcurrentMap<UUID, GridDistributedTxMapping<K, V>> mappings() {
        return mappings;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> recoveryWrites() {
        return F.view(writeEntries(), CU.<K, V>transferRequired());
    }

    /**
     * @param nodeId Node ID.
     * @param dhtVer DHT version.
     */
    void addDhtVersion(UUID nodeId, GridCacheVersion dhtVer) {
        // This step is very important as near and DHT versions grow separately.
        cctx.versions().onReceived(nodeId, dhtVer);

        GridDistributedTxMapping<K, V> m = mappings.get(nodeId);

        if (m != null)
            m.dhtVersion(dhtVer);
    }

    /**
     * @param nodeId Undo mapping.
     */
    public void removeMapping(UUID nodeId) {
        if (mappings.remove(nodeId) != null) {
            if (log.isDebugEnabled())
                log.debug("Removed mapping for node [nodeId=" + nodeId + ", tx=" + this + ']');
        }
        else if (log.isDebugEnabled())
            log.debug("Mapping for node was not found [nodeId=" + nodeId + ", tx=" + this + ']');
    }

    /** {@inheritDoc} */
    @Override protected void addGroupTxMapping(Collection<K> keys) {
        addKeyMapping(cctx.localNode(), keys);
    }

    /**
     * Adds key mapping to dht mapping.
     *
     * @param key Key to add.
     * @param node Node this key mapped to.
     */
    void addKeyMapping(K key, GridNode node) {
        GridDistributedTxMapping<K, V> m = mappings.get(node.id());

        if (m == null)
            mappings.put(node.id(), m = new GridDistributedTxMapping<>(node));

        GridCacheTxEntry<K, V> txEntry = txMap.get(key);

        assert txEntry != null;

        txEntry.nodeId(node.id());

        m.add(txEntry);

        if (log.isDebugEnabled())
            log.debug("Added mappings to transaction [locId=" + cctx.nodeId() + ", key=" + key + ", node=" + node +
                ", tx=" + this + ']');
    }

    /**
     * @param maps Mappings.
     */
    void addEntryMapping(@Nullable Collection<GridDistributedTxMapping<K, V>> maps) {
        if (!F.isEmpty(maps)) {
            for (GridDistributedTxMapping<K, V> map : maps) {
                GridNode n = map.node();

                GridDistributedTxMapping<K, V> m = mappings.get(n.id());

                if (m == null)
                    m = F.addIfAbsent(mappings, n.id(), new GridDistributedTxMapping<K, V>(n));

                assert m != null;

                for (GridCacheTxEntry<K, V> entry : map.entries())
                    m.add(entry);
            }

            if (log.isDebugEnabled())
                log.debug("Added mappings to transaction [locId=" + cctx.nodeId() + ", mappings=" + maps +
                    ", tx=" + this + ']');
        }
    }

    /**
     * Adds keys mapping.
     *
     * @param n Mapped node.
     * @param mappedKeys Mapped keys.
     */
    void addKeyMapping(GridNode n, Iterable<K> mappedKeys) {
        GridDistributedTxMapping<K, V> m = mappings.get(n.id());

        if (m == null)
            mappings.put(n.id(), m = new GridDistributedTxMapping<>(n));

        for (K key : mappedKeys) {
            GridCacheTxEntry<K, V> txEntry = txMap.get(key);

            assert txEntry != null;

            txEntry.nodeId(n.id());

            m.add(txEntry);
        }
    }

    /**
     * Removes mapping in case of optimistic tx failure on primary node.
     *
     * @param failedNodeId Failed node ID.
     * @param mapQueue Mappings queue.
     */
    void removeKeysMapping(UUID failedNodeId, Iterable<GridDistributedTxMapping<K, V>> mapQueue) {
        assert optimistic();
        assert failedNodeId != null;
        assert mapQueue != null;

        mappings.remove(failedNodeId);

        if (!F.isEmpty(mapQueue)) {
            for (GridDistributedTxMapping<K, V> m : mapQueue) {
                UUID nodeId = m.node().id();

                GridDistributedTxMapping<K, V> mapping = mappings.get(nodeId);

                if (mapping != null) {
                    for (GridCacheTxEntry<K, V> entry : m.entries())
                        mapping.removeEntry(entry);

                    if (mapping.entries().isEmpty())
                        mappings.remove(nodeId);
                }
            }
        }
    }

    /**
     * @param nodeId Node ID to mark with explicit lock.
     * @return {@code True} if mapping was found.
     */
    boolean markExplicit(UUID nodeId) {
        GridDistributedTxMapping<K, V> m = mappings.get(nodeId);

        if (m != null) {
            m.markExplicitLock();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean syncCommit() {
        return syncCommit;
    }

    /** {@inheritDoc} */
    @Override public boolean syncRollback() {
        return syncRollback;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        GridNearTxPrepareFuture<K, V> fut = prepFut.get();

        return fut != null && fut.onOwnerChanged(entry, owner);
    }

    /**
     * @return Commit fut.
     */
    @Override public GridFuture<GridCacheTxEx<K, V>> future() {
        return prepFut.get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> loadMissing(boolean async, final Collection<? extends K> keys,
        boolean deserializePortable, final GridBiInClosure<K, V> c) {
        return cctx.nearTx().txLoadAsync(this, keys, CU.<K, V>empty(), deserializePortable).chain(new C1<GridFuture<Map<K, V>>, Boolean>() {
            @Override public Boolean apply(GridFuture<Map<K, V>> f) {
                try {
                    Map<K, V> map = f.get();

                    // Must loop through keys, not map entries,
                    // as map entries may not have all the keys.
                    for (K key : keys)
                        c.apply(key, map.get(key));

                    return true;
                }
                catch (Exception e) {
                    setRollbackOnly();

                    throw new GridClosureException(e);
                }
            }
        });
    }

    /**
     * @param mapping Mapping to order.
     * @param pendingVers Pending versions.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    void readyNearLocks(GridDistributedTxMapping<K, V> mapping, Collection<GridCacheVersion> pendingVers,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers) {
        Collection<GridCacheTxEntry<K, V>> entries = groupLock() ?
            Collections.singletonList(groupLockEntry()) :
            F.concat(false, mapping.reads(), mapping.writes());

        for (GridCacheTxEntry<K, V> txEntry : entries) {
            while (true) {
                GridDistributedCacheEntry<K, V> entry = (GridDistributedCacheEntry<K, V>)txEntry.cached();

                try {
                    // Handle explicit locks.
                    GridCacheVersion base = txEntry.explicitVersion() != null ? txEntry.explicitVersion() : xidVer;

                    entry.readyNearLock(base, mapping.dhtVersion(), committedVers, rolledbackVers, pendingVers);

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    assert entry.obsoleteVersion() != null;

                    if (log.isDebugEnabled())
                        log.debug("Replacing obsolete entry in remote transaction [entry=" + entry +
                            ", tx=" + this + ']');

                    // Replace the entry.
                    txEntry.cached(cctx.cache().entryEx(txEntry.key()), entry.keyBytes());
                }
            }
        }
    }

    /**
     * @return Topology snapshot on which this tx was started.
     */
    GridDiscoveryTopologySnapshot topologySnapshot() {
        return topSnapshot.get();
    }

    /**
     * Sets topology snapshot on which this tx was started.
     *
     * @param topSnapshot Topology snapshot.
     * @return {@code True} if topology snapshot was set by this call.
     */
    boolean topologySnapshot(GridDiscoveryTopologySnapshot topSnapshot) {
        return this.topSnapshot.compareAndSet(null, topSnapshot);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean finish(boolean commit) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Finishing near local tx [tx=" + this + ", commit=" + commit + "]");

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
                invalidate = true;

                U.warn(log, "Set transaction invalidation flag to true due to error [tx=" + this + ", err=" + err + ']');
            }
        }

        if (err != null) {
            state(UNKNOWN);

            throw err;
        }
        else if (!state(commit ? COMMITTED : ROLLED_BACK)) {
            state(UNKNOWN);

            throw new GridException("Invalid transaction state for commit or rollback: " + this);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTxEx<K, V>> prepareAsync() {
        GridNearTxPrepareFuture<K, V> fut = prepFut.get();

        if (fut == null) {
            // Future must be created before any exception can be thrown.
            if (!prepFut.compareAndSet(null, fut = new GridNearTxPrepareFuture<>(cctx, this)))
                return prepFut.get();
        }
        else
            // Prepare was called explicitly.
            return fut;

        // For pessimistic mode we don't distribute prepare request and do not lock topology version
        // as it was fixed on first lock.
        if (pessimistic()) {
            if (!state(PREPARING)) {
                if (setRollbackOnly()) {
                    if (timedOut())
                        fut.onError(null, null, new GridCacheTxTimeoutException("Transaction timed out and was " +
                            "rolled back: " + this));
                    else
                        fut.onError(null, null, new GridException("Invalid transaction state for prepare [state=" +
                            state() + ", tx=" + this + ']'));
                }
                else
                    fut.onError(null, null, new GridCacheTxRollbackException("Invalid transaction state for prepare " +
                        "[state=" + state() + ", tx=" + this + ']'));

                return fut;
            }

            try {
                userPrepare();

                if (!state(PREPARED)) {
                    setRollbackOnly();

                    fut.onError(null, null, new GridException("Invalid transaction state for commit [state=" +
                        state() + ", tx=" + this + ']'));

                    return fut;
                }

                fut.complete();
            }
            catch (GridException e) {
                fut.onError(null, null, e);
            }
        }
        else
            // In optimistic mode we must wait for topology map update.
            prepareOnTopology();

        return fut;
    }

    /**
     * Waits for topology exchange future to be ready and then prepares user transaction.
     */
    private void prepareOnTopology() {
        cctx.topology().readLock();

        try {
            GridDhtTopologyFuture topFut = cctx.topology().topologyVersionFuture();

            if (topFut.isDone()) {
                GridNearTxPrepareFuture<K, V> fut = prepFut.get();

                assert fut != null : "Missing near tx prepare future in prepareOnTopology()";

                try {
                    if (!state(PREPARING)) {
                        if (setRollbackOnly()) {
                            if (timedOut())
                                fut.onError(null, null, new GridCacheTxTimeoutException("Transaction timed out and " +
                                    "was rolled back: " + this));
                            else
                                fut.onError(null, null, new GridException("Invalid transaction state for prepare " +
                                    "[state=" + state() + ", tx=" + this + ']'));
                        }
                        else
                            fut.onError(null, null, new GridCacheTxRollbackException("Invalid transaction state for " +
                                "prepare [state=" + state() + ", tx=" + this + ']'));

                        return;
                    }

                    GridDiscoveryTopologySnapshot snapshot = topFut.topologySnapshot();

                    topologyVersion(snapshot.topologyVersion());
                    topologySnapshot(snapshot);

                    userPrepare();

                    // Make sure to add future before calling prepare.
                    cctx.mvcc().addFuture(fut);

                    fut.prepare();
                }
                catch (GridCacheTxTimeoutException | GridCacheTxOptimisticException e) {
                    fut.onError(cctx.localNodeId(), null, e);
                }
                catch (GridException e) {
                    setRollbackOnly();

                    String msg = "Failed to prepare transaction (will attempt rollback): " + this;

                    U.error(log, msg, e);

                    try {
                        rollback();
                    }
                    catch (GridException e1) {
                        U.error(log, "Failed to rollback transaction: " + this, e1);
                    }

                    fut.onError(null, null, new GridCacheTxRollbackException(msg, e));
                }
            }
            else {
                topFut.syncNotify(false);

                topFut.listenAsync(new CI1<GridFuture<Long>>() {
                    @Override public void apply(GridFuture<Long> t) {
                        prepareOnTopology();
                    }
                });
            }
        }
        finally {
            cctx.topology().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public GridFuture<GridCacheTx> commitAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing near local tx: " + this);

        prepareAsync();

        GridNearTxFinishFuture<K, V> fut = commitFut.get();

        if (fut == null && !commitFut.compareAndSet(null, fut = new GridNearTxFinishFuture<>(cctx, this, true)))
            return commitFut.get();

        cctx.mvcc().addFuture(fut);

        prepFut.get().listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
            @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                GridNearTxFinishFuture<K, V> fut0 = commitFut.get();

                try {
                    // Make sure that here are no exceptions.
                    f.get();

                    if (finish(true))
                        fut0.finish();
                    else
                        fut0.onError(new GridException("Failed to commit transaction: " +
                            CU.txString(GridNearTxLocal.this)));
                }
                catch (Error | RuntimeException e) {
                    commitErr.compareAndSet(null, e);

                    throw e;
                }
                catch (GridException e) {
                    commitErr.compareAndSet(null, e);

                    fut0.onError(e);
                }
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws GridException {
        rollbackAsync().get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTx> rollbackAsync() {
        GridNearTxPrepareFuture<K, V> prepFut = this.prepFut.get();

        GridNearTxFinishFuture<K, V> fut = rollbackFut.get();

        if (fut == null && !rollbackFut.compareAndSet(null, fut = new GridNearTxFinishFuture<>(cctx, this, false)))
            return rollbackFut.get();

        try {
            cctx.mvcc().addFuture(fut);

            if (prepFut == null || prepFut.isDone()) {
                try {
                    // Check for errors in prepare future.
                    if (prepFut != null)
                        prepFut.get();
                }
                catch (GridException e) {
                    if (log.isDebugEnabled())
                        log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
                }

                if (finish(false) || state() == UNKNOWN)
                    fut.finish();
                else
                    fut.onError(new GridException("Failed to gracefully rollback transaction: " + CU.txString(this)));
            }
            else {
                prepFut.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
                    @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                        try {
                            // Check for errors in prepare future.
                            f.get();
                        }
                        catch (GridException e) {
                            if (log.isDebugEnabled())
                                log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
                        }

                        GridNearTxFinishFuture<K, V> fut0 = rollbackFut.get();

                        try {
                            if (finish(false) || state() == UNKNOWN)
                                fut0.finish();
                            else
                                fut0.onError(new GridException("Failed to gracefully rollback transaction: " +
                                    CU.txString(GridNearTxLocal.this)));
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to gracefully rollback transaction: " + this, e);

                            fut0.onError(e);
                        }
                    }
                });
            }

            return fut;
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void addLocalCandidates(K key, Collection<GridCacheMvccCandidate<K>> cands) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public Map<K, Collection<GridCacheMvccCandidate<K>>> localCandidates() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxLocal.class, this, "mappings", mappings.keySet(), "super", super.toString());
    }
}
