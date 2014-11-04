/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Colocated transaction. Embedded combination of near and dht local transactions
 * when near cache is disabled.
 */
public class GridDhtColocatedTxLocal<K, V> extends GridDhtTxLocalAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology snapshot on which this tx was started. */
    @GridToStringExclude
    private final AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot =
        new AtomicReference<>();

    /** DHT mappings. */
    private ConcurrentMap<UUID, GridDistributedTxMapping<K, V>> mappings =
        new ConcurrentHashMap8<>();

    /** Prepare future. */
    private final AtomicReference<GridFuture<GridCacheTxEx<K, V>>> prepFut =
        new AtomicReference<>();

    /** Commit future. */
    private final AtomicReference<GridDhtColocatedTxFinishFuture<K, V>> commitFut =
        new AtomicReference<>();

    /** Rollback future. */
    private final AtomicReference<GridDhtColocatedTxFinishFuture<K, V>> rollbackFut =
        new AtomicReference<>();

    /** Entries to lock on next step of prepare stage. */
    private Collection<GridCacheTxEntry<K, V>> optimisticLockEntries = Collections.emptyList();

    /**
     * Empty constructor, required by {@link Externalizable}
     */
    public GridDhtColocatedTxLocal() {
        // No-op.
    }

    /**
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit-with-single-key flag.
     * @param cctx Cache context.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param invalidate Invalidation policy.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param swapEnabled Flag indicating whether swap is enabled or not.
     * @param storeEnabled Flag indicating whether store is enabled or not.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction and whole partition should be locked.
     */
    public GridDhtColocatedTxLocal(
        boolean implicit,
        boolean implicitSingle,
        GridCacheContext<K, V> cctx,
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
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(cctx.versions().next(), implicit, implicitSingle, cctx, concurrency, isolation, timeout, invalidate,
            syncCommit, syncRollback, false, swapEnabled, storeEnabled, txSize, grpLockKey, partLock, subjId,
            taskNameHash);
    }

    /** {@inheritDoc} */
    @Override public boolean colocated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected UUID nearNodeId() {
        return cctx.localNodeId();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion nearXidVersion() {
        return xidVer;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> recoveryWrites() {
        return F.view(writeEntries(), CU.<K, V>transferRequired());
    }

    /** {@inheritDoc} */
    @Override protected GridUuid nearFutureId() {
        assert false : "nearFutureId should not be called for colocated transactions.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected GridUuid nearMiniId() {
        assert false : "nearMiniId should not be called for colocated transactions.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected GridFuture<Boolean> addReader(long msgId, GridDhtCacheEntry<K, V> cached,
        GridCacheTxEntry<K, V> entry, long topVer) {
        // We are in colocated transaction, do not add local node as reader.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void sendFinishReply(boolean commit, @Nullable Throwable err) {
        // We are in colocated transaction, do not send finish reply to local node.
    }

    /** {@inheritDoc} */
    @Override protected void clearPrepareFuture(GridDhtTxPrepareFuture<K, V> fut) {
        prepFut.compareAndSet(fut, null);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTxEx<K, V>> future() {
        return prepFut.get();
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
     * @return {@code True} if snapshot was set by this call.
     */
    boolean topologySnapshot(GridDiscoveryTopologySnapshot topSnapshot) {
        return this.topSnapshot.compareAndSet(null, topSnapshot);
    }

    /**
     * @return DHT map.
     */
    ConcurrentMap<UUID, GridDistributedTxMapping<K, V>> mappings() {
        return mappings;
    }

    /** {@inheritDoc} */
    @Override public boolean ownsLockUnsafe(GridCacheEntryEx<K, V> entry) {
        return entry.detached() || super.ownsLockUnsafe(entry);
    }

    /** {@inheritDoc} */
    @Override public boolean ownsLock(GridCacheEntryEx<K, V> entry) throws GridCacheEntryRemovedException {
        return entry.detached() || super.ownsLock(entry);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> optimisticLockEntries() {
        if (groupLock())
            return super.optimisticLockEntries();

        return optimisticLockEntries;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> loadMissing(
        GridCacheContext<K, V> cacheCtx,
        boolean async,
        final Collection<? extends K> keys,
        boolean deserializePortable,
        final GridBiInClosure<K, V> c
    ) {
        return cacheCtx.colocated().loadAsync(keys, /*reload*/false, /*force primary*/false, topologyVersion(),
            CU.subjectId(this, cctx), resolveTaskName(), deserializePortable, null)
            .chain(new C1<GridFuture<Map<K, V>>, Boolean>() {
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

    /** {@inheritDoc} */
    @Override protected void updateExplicitVersion(GridCacheTxEntry<K, V> txEntry, GridCacheEntryEx<K, V> entry)
        throws GridCacheEntryRemovedException {
        if (entry.detached()) {
            GridCacheMvccCandidate<K> cand = cctx.mvcc().explicitLock(threadId(), entry.key());

            if (cand != null && !xidVersion().equals(cand.version())) {
                GridCacheVersion candVer = cand.version();

                txEntry.explicitVersion(candVer);

                if (candVer.isLess(minVer))
                    minVer = candVer;
            }
        }
        else
            super.updateExplicitVersion(txEntry, entry);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTxEx<K, V>> prepareAsync() {
        GridFuture<GridCacheTxEx<K, V>> fut = prepFut.get();

        if (fut == null) {
            // Future must be created before any exception can be thrown.
            fut = pessimistic() ? new PessimisticPrepareFuture<>(cctx.kernalContext(), this) :
                new GridDhtColocatedTxPrepareFuture<>(cctx, this);

            if (!prepFut.compareAndSet(null, fut))
                return prepFut.get();
        }
        else
            // Prepare was called explicitly.
            return fut;

        mapExplicitLocks();

        // For pessimistic mode we don't distribute prepare request and do not lock topology version
        // as it was fixed on first lock.
        if (pessimistic()) {
            PessimisticPrepareFuture<K, V> pessimisticFut = (PessimisticPrepareFuture<K, V>)fut;

            if (!state(PREPARING)) {
                if (setRollbackOnly()) {
                    if (timedOut())
                        pessimisticFut.onError(new GridCacheTxTimeoutException("Transaction timed out and was " +
                            "rolled back: " + this));
                    else
                        pessimisticFut.onError(new GridException("Invalid transaction state for prepare [state=" +
                            state() + ", tx=" + this + ']'));
                }
                else
                    pessimisticFut.onError(new GridCacheTxRollbackException("Invalid transaction state for prepare" +
                        " [state=" + state() + ", tx=" + this + ']'));

                return fut;
            }

            try {
                userPrepare();

                if (!state(PREPARED)) {
                    setRollbackOnly();

                    pessimisticFut.onError(new GridException("Invalid transaction state for commit [state=" + state() +
                        ", tx=" + this + ']'));

                    return fut;
                }

                pessimisticFut.complete();
            }
            catch (GridException e) {
                pessimisticFut.onError(e);
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
                GridDhtColocatedTxPrepareFuture<K, V> fut = (GridDhtColocatedTxPrepareFuture<K, V>)prepFut.get();

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

                    // Make sure to add future before calling prepare.
                    cctx.mvcc().addFuture(fut);

                    fut.prepare();
                }
                catch (GridException e) {
                    setRollbackOnly();

                    String msg = "Failed to prepare transaction (will attempt rollback): " + this;

                    U.error(log, msg, e);

                    rollbackAsync();

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
    @Override public GridFuture<GridCacheTx> commitAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing colocated tx: " + this);

        prepareAsync();

        GridDhtColocatedTxFinishFuture<K, V> fut = commitFut.get();

        if (fut != null)
            return fut;

        if (!commitFut.compareAndSet(null, fut = new GridDhtColocatedTxFinishFuture<>(cctx, this)))
            return commitFut.get();

        cctx.mvcc().addFuture(fut);

        GridFuture<GridCacheTxEx<K, V>> prepareFut = prepFut.get();

        prepareFut.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
            @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                GridDhtColocatedTxFinishFuture<K, V> commitFut0 = commitFut.get();

                try {
                    // Make sure that here are no exceptions.
                    f.get();

                    if (finish(true))
                        commitFut0.finish(true);
                    else
                        commitFut0.onError(new GridException("Failed to commit transaction: " +
                            CU.txString(GridDhtColocatedTxLocal.this)));
                }
                catch (Error | RuntimeException e) {
                    commitErr.compareAndSet(null, e);

                    throw e;
                }
                catch (GridException e) {
                    commitErr.compareAndSet(null, e);

                    commitFut0.onError(e);
                }
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTx> rollbackAsync() {
        if (log.isDebugEnabled())
            log.debug("Rolling back colocated tx: " + this);

        GridDhtColocatedTxFinishFuture<K, V> fut = rollbackFut.get();

        if (fut != null)
            return fut;

        if (!rollbackFut.compareAndSet(null, fut = new GridDhtColocatedTxFinishFuture<>(cctx, this)))
            return rollbackFut.get();

        cctx.mvcc().addFuture(fut);

        GridFuture<GridCacheTxEx<K, V>> prepareFut = prepFut.get();

        if (prepareFut == null || prepareFut.isDone()) {
            try {
                if (finish(false) || state() == UNKNOWN)
                    fut.finish(false);
                else
                    fut.onError(new GridException("Failed to gracefully rollback transaction: " + CU.txString(this)));
            }
            catch (GridException e) {
                fut.onError(e);
            }
        }
        else {
            prepareFut.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                    try {
                        // Check for errors in prepare future.
                        f.get();
                    }
                    catch (GridException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
                    }

                    GridDhtColocatedTxFinishFuture<K, V> rollbackFut0 = rollbackFut.get();

                    try {
                        if (finish(false) || state() == UNKNOWN)
                            rollbackFut0.finish(false);
                        else
                            rollbackFut0.onError(new GridException("Failed to gracefully rollback transaction: " +
                                CU.txString(GridDhtColocatedTxLocal.this)));
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to gracefully rollback transaction: " +
                            CU.txString(GridDhtColocatedTxLocal.this), e);

                        rollbackFut0.onError(e);
                    }
                }
            });
        }

        return fut;
    }

    /**
     * Prepares next batch of entries in dht transaction.
     *
     * @param reads Read entries.
     * @param writes Write entries.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this is last prepare request.
     * @param lastBackups IDs of backup nodes receiving last prepare request.
     * @return Future that will be completed when locks are acquired.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public GridFuture<GridCacheTxEx<K, V>> prepareAsyncLocal(@Nullable Collection<GridCacheTxEntry<K, V>> reads,
        @Nullable Collection<GridCacheTxEntry<K, V>> writes, Map<UUID, Collection<UUID>> txNodes, boolean last,
        Collection<UUID> lastBackups) {
        assert optimistic();

        if (state() != PREPARING) {
            if (timedOut())
                return new GridFinishedFuture<>(cctx.kernalContext(),
                    new GridCacheTxTimeoutException("Transaction timed out: " + this));

            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(),
                new GridException("Invalid transaction state for prepare [state=" + state() + ", tx=" + this + ']'));
        }

        init();

        GridDhtTxPrepareFuture<K, V> fut = new GridDhtTxPrepareFuture<>(cctx, this, GridUuid.randomUuid(),
            Collections.<K, GridCacheVersion>emptyMap(), last, lastBackups);

        try {
            // At this point all the entries passed in must be enlisted in transaction because this is an
            // optimistic transaction.
            optimisticLockEntries = writes;

            userPrepare();

            // Make sure to add future before calling prepare on it.
            cctx.mvcc().addFuture(fut);

            if (isSystemInvalidate())
                fut.complete();
            else
                fut.prepare(reads, writes, txNodes);
        }
        catch (GridCacheTxTimeoutException | GridCacheTxOptimisticException e) {
            fut.onError(e);
        }
        catch (GridException e) {
            setRollbackOnly();

            fut.onError(new GridCacheTxRollbackException("Failed to prepare transaction: " + this, e));

            try {
                rollback();
            }
            catch (GridCacheTxOptimisticException e1) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e1 + ']');

                fut.onError(e);
            }
            catch (GridException e1) {
                U.error(log, "Failed to rollback transaction: " + this, e1);
            }
        }

        return fut;
    }

    /**
     * Commits local part of colocated transaction.
     *
     * @return Commit future.
     */
    GridFuture<GridCacheTx> commitAsyncLocal() {
        if (log.isDebugEnabled())
            log.debug("Committing colocated tx locally: " + this);

        // In optimistic mode prepare was called explicitly.
        if (pessimistic())
            prepareAsync();

        GridFuture<GridCacheTxEx<K, V>> prep = prepFut.get();

        // Do not create finish future if there are no remote nodes.
        if (F.isEmpty(dhtMap) && F.isEmpty(nearMap)) {
            if (prep != null)
                return (GridFuture<GridCacheTx>)(GridFuture)prep;

            return new GridFinishedFuture<GridCacheTx>(cctx.kernalContext(), this);
        }

        final GridDhtTxFinishFuture<K, V> fut = new GridDhtTxFinishFuture<>(cctx, this, /*commit*/true);

        cctx.mvcc().addFuture(fut);

        if (prep == null || prep.isDone()) {
            assert prep != null || optimistic();

            try {
                if (prep != null)
                    prep.get(); // Check for errors of a parent future.

                fut.finish();
            }
            catch (GridCacheTxOptimisticException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                fut.onError(e);
            }
            catch (GridException e) {
                U.error(log, "Failed to prepare transaction: " + this, e);

                fut.onError(e);
            }
        }
        else
            prep.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                    try {
                        f.get(); // Check for errors of a parent future.

                        fut.finish();
                    }
                    catch (GridCacheTxOptimisticException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                        fut.onError(e);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to prepare transaction: " + this, e);

                        fut.onError(e);
                    }
                }
            });

        return fut;
    }

    /**
     * Rolls back local part of colocated transaction.
     *
     * @return Commit future.
     */
    GridFuture<GridCacheTx> rollbackAsyncLocal() {
        if (log.isDebugEnabled())
            log.debug("Rolling back colocated tx locally: " + this);

        final GridDhtTxFinishFuture<K, V> fut = new GridDhtTxFinishFuture<>(cctx, this, /*commit*/false);

        cctx.mvcc().addFuture(fut);

        GridFuture<GridCacheTxEx<K, V>> prep = prepFut.get();

        if (prep == null || prep.isDone()) {
            try {
                if (prep != null)
                    prep.get();
            }
            catch (GridException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to prepare transaction during rollback (will ignore) [tx=" + this + ", msg=" +
                        e.getMessage() + ']');
            }

            fut.finish();
        }
        else
            prep.listenAsync(new CI1<GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public void apply(GridFuture<GridCacheTxEx<K, V>> f) {
                    try {
                        f.get(); // Check for errors of a parent future.
                    }
                    catch (GridException e) {
                        log.debug("Failed to prepare transaction during rollback (will ignore) [tx=" + this + ", msg=" +
                            e.getMessage() + ']');
                    }

                    fut.finish();
                }
            });

        return fut;
    }

    /** {@inheritDoc} */
    GridFuture<GridCacheReturn<V>> lockAllAsync(final Collection<? extends K> keys, boolean implicit, boolean read) {
        assert pessimistic();

        try {
            checkValid(CU.<K, V>empty());
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        final GridCacheReturn<V> ret = new GridCacheReturn<>(false);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(cctx.kernalContext(), ret);

        init();

        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys: " + keys);

        GridFuture<Boolean> fut = cctx.colocated().lockAllAsyncInternal(keys,
            lockTimeout(), this, isInvalidate(), read, /*retval*/false, isolation, CU.<K, V>empty());

        return new GridEmbeddedFuture<>(
            fut,
            new PLC1<GridCacheReturn<V>>(ret, false) {
                @Override protected GridCacheReturn<V> postLock(GridCacheReturn<V> ret) {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock on keys: " + keys);

                    return ret;
                }
            },
            cctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override protected void addGroupTxMapping(Collection<K> keys) {
        super.addGroupTxMapping(keys);

        GridDistributedTxMapping<K, V> m = mappings.get(cctx.localNodeId());

        if (m == null)
            mappings.put(cctx.localNodeId(), m = new GridDistributedTxMapping<>(cctx.localNode()));

        m.entries(Collections.unmodifiableCollection(txMap.values()), true);
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
    @Override protected GridCacheEntryEx<K, V> entryEx(K key) {
        GridCacheTxEntry<K, V> txEntry = entry(key);

        if (txEntry == null)
            return cctx.colocated().entryExx(key, topologyVersion(), true);

        GridCacheEntryEx<K, V> cached = txEntry.cached();

        assert cached != null;

        if (cached.detached())
            return cached;

        if (cached.obsoleteVersion() != null) {
            cached = cctx.colocated().entryExx(key, topologyVersion(), true);

            txEntry.cached(cached, txEntry.keyBytes());
        }

        return cached;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx<K, V> entryEx(K key, long topVer) {
        GridCacheTxEntry<K, V> txEntry = entry(key);

        if (txEntry == null)
            return cctx.colocated().entryExx(key, topVer, true);

        GridCacheEntryEx<K, V> cached = txEntry.cached();

        assert cached != null;

        if (cached.detached())
            return cached;

        if (cached.obsoleteVersion() != null) {
            cached = cctx.colocated().entryExx(key, topVer, true);

            txEntry.cached(cached, txEntry.keyBytes());
        }

        return cached;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtColocatedTxLocal.class, this, "topSnapshot", topSnapshot.get(), "super",
            super.toString());
    }

    /**
     *
     */
    private static class PessimisticPrepareFuture<K, V> extends GridFutureAdapter<GridCacheTxEx<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Transaction. */
        @GridToStringExclude
        private GridCacheTxEx<K, V> tx;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public PessimisticPrepareFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         * @param tx Transaction.
         */
        private PessimisticPrepareFuture(GridKernalContext ctx, GridCacheTxEx<K, V> tx) {
            super(ctx);
            this.tx = tx;
        }

        /**
         * @param e Exception.
         */
        void onError(Throwable e) {
            boolean marked = tx.setRollbackOnly();

            if (e instanceof GridCacheTxRollbackException) {
                if (marked) {
                    try {
                        tx.rollback();
                    }
                    catch (GridException ex) {
                        U.error(log, "Failed to automatically rollback transaction: " + tx, ex);
                    }
                }
            }

            onDone(tx, e);
        }

        /**
         * Completes future.
         */
        void complete() {
            onDone(tx);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PessimisticPrepareFuture[xidVer=" + tx.xidVersion() + ", done=" + isDone() + ']';
        }
    }
}
