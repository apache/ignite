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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxState.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Replicated user transaction.
 */
public class GridNearTxLocal<K, V> extends GridDhtTxLocalAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology snapshot on which this tx was started. */
    private final AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot =
        new AtomicReference<>();

    /** DHT mappings. */
    private ConcurrentMap<UUID, GridDistributedTxMapping<K, V>> mappings =
        new ConcurrentHashMap8<>();

    /** Future. */
    private final AtomicReference<IgniteFuture<IgniteTxEx<K, V>>> prepFut =
        new AtomicReference<>();

    /** */
    private final AtomicReference<GridNearTxFinishFuture<K, V>> commitFut =
        new AtomicReference<>();

    /** */
    private final AtomicReference<GridNearTxFinishFuture<K, V>> rollbackFut =
        new AtomicReference<>();

    /** Entries to lock on next step of prepare stage. */
    private Collection<IgniteTxEntry<K, V>> optimisticLockEntries = Collections.emptyList();

    /** True if transaction contains near cache entries mapped to local node. */
    private boolean nearLocallyMapped;

    /** True if transaction contains colocated cache entries mapped to local node. */
    private boolean colocatedLocallyMapped;

    /** Info for entries accessed locally in optimistic transaction. */
    private Map<IgniteTxKey, IgniteCacheExpiryPolicy> accessMap;

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
     * @param sys System flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param invalidate Invalidate flag.
     * @param storeEnabled Store enabled flag.
     * @param txSize Transaction size.
     * @param grpLockKey Group lock key if this is a group lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction and the whole partition should be locked.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridNearTxLocal(
        GridCacheSharedContext<K, V> ctx,
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
        super(
            ctx,
            ctx.versions().next(),
            implicit,
            implicitSingle,
            sys,
            concurrency,
            isolation,
            timeout,
            invalidate,
            storeEnabled,
            txSize,
            grpLockKey,
            partLock,
            subjId,
            taskNameHash);
    }

    /** {@inheritDoc} */
    @Override public boolean near() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean colocated() {
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

    /** {@inheritDoc} */
    @Override protected UUID nearNodeId() {
        return cctx.localNodeId();
    }

    /** {@inheritDoc} */
    @Override protected IgniteUuid nearFutureId() {
        assert false : "nearFutureId should not be called for colocated transactions.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteUuid nearMiniId() {
        assert false : "nearMiniId should not be called for colocated transactions.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteFuture<Boolean> addReader(long msgId, GridDhtCacheEntry<K, V> cached,
        IgniteTxEntry<K, V> entry, long topVer) {
        // We are in near transaction, do not add local node as reader.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void sendFinishReply(boolean commit, @Nullable Throwable err) {
        // We are in near transaction, do not send finish reply to local node.
    }

    /** {@inheritDoc} */
    @Override protected void clearPrepareFuture(GridDhtTxPrepareFuture<K, V> fut) {
        prepFut.compareAndSet(fut, null);
    }

    /** {@inheritDoc} */
    @Override public boolean syncCommit() {
        return sync();
    }

    /** {@inheritDoc} */
    @Override public boolean syncRollback() {
        return sync();
    }

    /**
     * Checks if transaction is fully synchronous.
     *
     * @return {@code True} if transaction is fully synchronous.
     */
    private boolean sync() {
        if (super.syncCommit())
            return true;

        for (int cacheId : activeCacheIds()) {
            if (cctx.cacheContext(cacheId).config().getWriteSynchronizationMode() == FULL_SYNC)
                return true;
        }

        return false;
    }

    /**
     * @return {@code True} if transaction contains at least one near cache key mapped to the local node.
     */
    public boolean nearLocallyMapped() {
        return nearLocallyMapped;
    }

    /**
     * @param nearLocallyMapped {@code True} if transaction contains near key mapped to the local node.
     */
    public void nearLocallyMapped(boolean nearLocallyMapped) {
        this.nearLocallyMapped = nearLocallyMapped;
    }

    /**
     * @return {@code True} if transaction contains colocated key mapped to the local node.
     */
    public boolean colocatedLocallyMapped() {
        return colocatedLocallyMapped;
    }

    /**
     * @param colocatedLocallyMapped {@code True} if transaction contains colocated key mapped to the local node.
     */
    public void colocatedLocallyMapped(boolean colocatedLocallyMapped) {
        this.colocatedLocallyMapped = colocatedLocallyMapped;
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
    @Override public Collection<IgniteTxEntry<K, V>> optimisticLockEntries() {
        if (groupLock())
            return super.optimisticLockEntries();

        return optimisticLockEntries;
    }

    /**
     * @param optimisticLockEntries Optimistic lock entries.
     */
    public void optimisticLockEntries(Collection<IgniteTxEntry<K, V>> optimisticLockEntries) {
        this.optimisticLockEntries = optimisticLockEntries;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> loadMissing(
        GridCacheContext<K, V> cacheCtx,
        boolean readThrough,
        boolean async,
        final Collection<? extends K> keys,
        boolean deserializePortable,
        final IgniteBiInClosure<K, V> c
    ) {
        if (cacheCtx.isNear()) {
            return cacheCtx.nearTx().txLoadAsync(this,
                keys,
                readThrough,
                CU.<K, V>empty(),
                deserializePortable,
                accessPolicy(cacheCtx, keys)).chain(new C1<IgniteFuture<Map<K, V>>, Boolean>() {
                @Override public Boolean apply(IgniteFuture<Map<K, V>> f) {
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
        else if (cacheCtx.isColocated()) {
            return cacheCtx.colocated().loadAsync(keys,
                readThrough,
                /*reload*/false,
                /*force primary*/false,
                topologyVersion(),
                CU.subjectId(this, cctx),
                resolveTaskName(),
                deserializePortable,
                null,
                accessPolicy(cacheCtx, keys)).chain(new C1<IgniteFuture<Map<K, V>>, Boolean>() {
                    @Override public Boolean apply(IgniteFuture<Map<K, V>> f) {
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
        else {
            assert cacheCtx.isLocal();

            return super.loadMissing(cacheCtx, readThrough, async, keys, deserializePortable, c);
        }
    }

    /** {@inheritDoc} */
    @Override protected void updateExplicitVersion(IgniteTxEntry<K, V> txEntry, GridCacheEntryEx<K, V> entry)
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

    /**
     * @return DHT map.
     */
    ConcurrentMap<UUID, GridDistributedTxMapping<K, V>> mappings() {
        return mappings;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry<K, V>> recoveryWrites() {
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
    @Override public boolean removeMapping(UUID nodeId) {
        if (mappings.remove(nodeId) != null) {
            if (log.isDebugEnabled())
                log.debug("Removed mapping for node [nodeId=" + nodeId + ", tx=" + this + ']');

            return true;
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Mapping for node was not found [nodeId=" + nodeId + ", tx=" + this + ']');

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override protected void addGroupTxMapping(Collection<IgniteTxKey<K>> keys) {
        super.addGroupTxMapping(keys);

        addKeyMapping(cctx.localNode(), keys);
    }

    /**
     * Adds key mapping to dht mapping.
     *
     * @param key Key to add.
     * @param node Node this key mapped to.
     */
    public void addKeyMapping(IgniteTxKey<K> key, ClusterNode node) {
        GridDistributedTxMapping<K, V> m = mappings.get(node.id());

        if (m == null)
            mappings.put(node.id(), m = new GridDistributedTxMapping<>(node));

        IgniteTxEntry<K, V> txEntry = txMap.get(key);

        assert txEntry != null;

        txEntry.nodeId(node.id());

        m.add(txEntry);

        if (log.isDebugEnabled())
            log.debug("Added mappings to transaction [locId=" + cctx.localNodeId() + ", key=" + key + ", node=" + node +
                ", tx=" + this + ']');
    }

    /**
     * Adds keys mapping.
     *
     * @param n Mapped node.
     * @param mappedKeys Mapped keys.
     */
    private void addKeyMapping(ClusterNode n, Iterable<IgniteTxKey<K>> mappedKeys) {
        GridDistributedTxMapping<K, V> m = mappings.get(n.id());

        if (m == null)
            mappings.put(n.id(), m = new GridDistributedTxMapping<>(n));

        for (IgniteTxKey<K> key : mappedKeys) {
            IgniteTxEntry<K, V> txEntry = txMap.get(key);

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
                ClusterNode n = map.node();

                GridDistributedTxMapping<K, V> m = mappings.get(n.id());

                if (m == null)
                    m = F.addIfAbsent(mappings, n.id(), new GridDistributedTxMapping<K, V>(n));

                assert m != null;

                for (IgniteTxEntry<K, V> entry : map.entries())
                    m.add(entry);
            }

            if (log.isDebugEnabled())
                log.debug("Added mappings to transaction [locId=" + cctx.localNodeId() + ", mappings=" + maps +
                    ", tx=" + this + ']');
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
                    for (IgniteTxEntry<K, V> entry : m.entries())
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
    public boolean markExplicit(UUID nodeId) {
        GridDistributedTxMapping<K, V> m = mappings.get(nodeId);

        if (m != null) {
            m.markExplicitLock();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        GridCacheMvccFuture<K, V, IgniteTxEx<K, V>> fut = (GridCacheMvccFuture<K, V, IgniteTxEx<K, V>>)prepFut
            .get();

        return fut != null && fut.onOwnerChanged(entry, owner);
    }

    /**
     * @param mapping Mapping to order.
     * @param pendingVers Pending versions.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    void readyNearLocks(GridDistributedTxMapping<K, V> mapping,
        Collection<GridCacheVersion> pendingVers,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers)
    {
        Collection<IgniteTxEntry<K, V>> entries = groupLock() ?
            Collections.singletonList(groupLockEntry()) :
            F.concat(false, mapping.reads(), mapping.writes());

        for (IgniteTxEntry<K, V> txEntry : entries) {
            while (true) {
                GridCacheContext<K, V> cacheCtx = txEntry.cached().context();

                if (!cacheCtx.isNear())
                    break;

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
                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key()), entry.keyBytes());
                }
            }
        }
    }

    /**
     * @return Topology snapshot on which this tx was started.
     */
    public GridDiscoveryTopologySnapshot topologySnapshot() {
        return topSnapshot.get();
    }

    /**
     * Sets topology snapshot on which this tx was started.
     *
     * @param topSnapshot Topology snapshot.
     * @return {@code True} if topology snapshot was set by this call.
     */
    public boolean topologySnapshot(GridDiscoveryTopologySnapshot topSnapshot) {
        return this.topSnapshot.compareAndSet(null, topSnapshot);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean finish(boolean commit) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Finishing near local tx [tx=" + this + ", commit=" + commit + "]");

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
                invalidate = true;

                systemInvalidate(true);

                U.warn(log, "Set transaction invalidation flag to true due to error [tx=" + this + ", err=" + err + ']');
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

    /** {@inheritDoc} */
    @Override public IgniteFuture<IgniteTxEx<K, V>> prepareAsync() {
        IgniteFuture<IgniteTxEx<K, V>> fut = prepFut.get();

        if (fut == null) {
            // Future must be created before any exception can be thrown.
            fut = pessimistic() ? new PessimisticPrepareFuture<>(cctx.kernalContext(), this) :
                new GridNearTxPrepareFuture<>(cctx, this);

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
                        pessimisticFut.onError(new IgniteTxTimeoutException("Transaction timed out and was " +
                            "rolled back: " + this));
                    else
                        pessimisticFut.onError(new IgniteCheckedException("Invalid transaction state for prepare [state=" +
                            state() + ", tx=" + this + ']'));
                }
                else
                    pessimisticFut.onError(new IgniteTxRollbackException("Invalid transaction state for prepare " +
                        "[state=" + state() + ", tx=" + this + ']'));

                return fut;
            }

            try {
                userPrepare();

                if (!state(PREPARED)) {
                    setRollbackOnly();

                    pessimisticFut.onError(new IgniteCheckedException("Invalid transaction state for commit [state=" +
                        state() + ", tx=" + this + ']'));

                    return fut;
                }

                pessimisticFut.complete();
            }
            catch (IgniteCheckedException e) {
                pessimisticFut.onError(e);
            }
        }
        else {
            // In optimistic mode we must wait for topology map update.
            GridNearTxPrepareFuture<K, V> pf = (GridNearTxPrepareFuture<K, V>)prepFut.get();

            pf.prepare();
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public IgniteFuture<IgniteTx> commitAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing near local tx: " + this);

        prepareAsync();

        GridNearTxFinishFuture<K, V> fut = commitFut.get();

        if (fut == null && !commitFut.compareAndSet(null, fut = new GridNearTxFinishFuture<>(cctx, this, true)))
            return commitFut.get();

        cctx.mvcc().addFuture(fut);

        IgniteFuture<IgniteTxEx<K, V>> prepareFut = prepFut.get();

        prepareFut.listenAsync(new CI1<IgniteFuture<IgniteTxEx<K, V>>>() {
            @Override public void apply(IgniteFuture<IgniteTxEx<K, V>> f) {
                GridNearTxFinishFuture<K, V> fut0 = commitFut.get();

                try {
                    // Make sure that here are no exceptions.
                    f.get();

                    if (finish(true))
                        fut0.finish();
                    else
                        fut0.onError(new IgniteCheckedException("Failed to commit transaction: " +
                            CU.txString(GridNearTxLocal.this)));
                }
                catch (Error | RuntimeException e) {
                    commitErr.compareAndSet(null, e);

                    throw e;
                }
                catch (IgniteCheckedException e) {
                    commitErr.compareAndSet(null, e);

                    fut0.onError(e);
                }
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<IgniteTx> rollbackAsync() {
        if (log.isDebugEnabled())
            log.debug("Rolling back near tx: " + this);

        GridNearTxFinishFuture<K, V> fut = rollbackFut.get();

        if (fut != null)
            return fut;

        if (!rollbackFut.compareAndSet(null, fut = new GridNearTxFinishFuture<>(cctx, this, false)))
            return rollbackFut.get();

        cctx.mvcc().addFuture(fut);

        IgniteFuture<IgniteTxEx<K, V>> prepFut = this.prepFut.get();

        if (prepFut == null || prepFut.isDone()) {
            try {
                // Check for errors in prepare future.
                if (prepFut != null)
                    prepFut.get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
            }

            try {
                if (finish(false) || state() == UNKNOWN)
                    fut.finish();
                else
                    fut.onError(new IgniteCheckedException("Failed to gracefully rollback transaction: " + CU.txString(this)));
            }
            catch (IgniteCheckedException e) {
                fut.onError(e);
            }
        }
        else {
            prepFut.listenAsync(new CI1<IgniteFuture<IgniteTxEx<K, V>>>() {
                @Override public void apply(IgniteFuture<IgniteTxEx<K, V>> f) {
                    try {
                        // Check for errors in prepare future.
                        f.get();
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
                    }

                    GridNearTxFinishFuture<K, V> fut0 = rollbackFut.get();

                    try {
                        if (finish(false) || state() == UNKNOWN)
                            fut0.finish();
                        else
                            fut0.onError(new IgniteCheckedException("Failed to gracefully rollback transaction: " +
                                CU.txString(GridNearTxLocal.this)));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to gracefully rollback transaction: " +
                            CU.txString(GridNearTxLocal.this), e);

                        fut0.onError(e);
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
    public IgniteFuture<IgniteTxEx<K, V>> prepareAsyncLocal(@Nullable Collection<IgniteTxEntry<K, V>> reads,
        @Nullable Collection<IgniteTxEntry<K, V>> writes, Map<UUID, Collection<UUID>> txNodes, boolean last,
        Collection<UUID> lastBackups) {
        assert optimistic();

        if (state() != PREPARING) {
            if (timedOut())
                return new GridFinishedFuture<>(cctx.kernalContext(),
                    new IgniteTxTimeoutException("Transaction timed out: " + this));

            setRollbackOnly();

            return new GridFinishedFuture<>(cctx.kernalContext(),
                new IgniteCheckedException("Invalid transaction state for prepare [state=" + state() + ", tx=" + this + ']'));
        }

        init();

        GridDhtTxPrepareFuture<K, V> fut = new GridDhtTxPrepareFuture<>(cctx, this, IgniteUuid.randomUuid(),
            Collections.<IgniteTxKey<K>, GridCacheVersion>emptyMap(), last, lastBackups);

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
        catch (IgniteTxTimeoutException | IgniteTxOptimisticException e) {
            fut.onError(e);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            fut.onError(new IgniteTxRollbackException("Failed to prepare transaction: " + this, e));

            try {
                rollback();
            }
            catch (IgniteTxOptimisticException e1) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e1 + ']');

                fut.onError(e);
            }
            catch (IgniteCheckedException e1) {
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
    public IgniteFuture<IgniteTx> commitAsyncLocal() {
        if (log.isDebugEnabled())
            log.debug("Committing colocated tx locally: " + this);

        // In optimistic mode prepare was called explicitly.
        if (pessimistic())
            prepareAsync();

        IgniteFuture<IgniteTxEx<K, V>> prep = prepFut.get();

        // Do not create finish future if there are no remote nodes.
        if (F.isEmpty(dhtMap) && F.isEmpty(nearMap)) {
            if (prep != null)
                return (IgniteFuture<IgniteTx>)(IgniteFuture)prep;

            return new GridFinishedFuture<IgniteTx>(cctx.kernalContext(), this);
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
            catch (IgniteTxOptimisticException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                fut.onError(e);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to prepare transaction: " + this, e);

                fut.onError(e);
            }
        }
        else
            prep.listenAsync(new CI1<IgniteFuture<IgniteTxEx<K, V>>>() {
                @Override public void apply(IgniteFuture<IgniteTxEx<K, V>> f) {
                    try {
                        f.get(); // Check for errors of a parent future.

                        fut.finish();
                    }
                    catch (IgniteTxOptimisticException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                        fut.onError(e);
                    }
                    catch (IgniteCheckedException e) {
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
    public IgniteFuture<IgniteTx> rollbackAsyncLocal() {
        if (log.isDebugEnabled())
            log.debug("Rolling back colocated tx locally: " + this);

        final GridDhtTxFinishFuture<K, V> fut = new GridDhtTxFinishFuture<>(cctx, this, /*commit*/false);

        cctx.mvcc().addFuture(fut);

        IgniteFuture<IgniteTxEx<K, V>> prep = prepFut.get();

        if (prep == null || prep.isDone()) {
            try {
                if (prep != null)
                    prep.get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to prepare transaction during rollback (will ignore) [tx=" + this + ", msg=" +
                        e.getMessage() + ']');
            }

            fut.finish();
        }
        else
            prep.listenAsync(new CI1<IgniteFuture<IgniteTxEx<K, V>>>() {
                @Override public void apply(IgniteFuture<IgniteTxEx<K, V>> f) {
                    try {
                        f.get(); // Check for errors of a parent future.
                    }
                    catch (IgniteCheckedException e) {
                        log.debug("Failed to prepare transaction during rollback (will ignore) [tx=" + this + ", msg=" +
                            e.getMessage() + ']');
                    }

                    fut.finish();
                }
            });

        return fut;
    }

    /** {@inheritDoc} */
    public IgniteFuture<GridCacheReturn<V>> lockAllAsync(GridCacheContext<K, V> cacheCtx,
        final Collection<? extends K> keys,
        boolean implicit,
        boolean read,
        long accessTtl) {
        assert pessimistic();

        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        final GridCacheReturn<V> ret = new GridCacheReturn<>(false);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(cctx.kernalContext(), ret);

        init();

        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys: " + keys);

        IgniteFuture<Boolean> fut = cacheCtx.colocated().lockAllAsyncInternal(keys,
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
    @Override protected GridCacheEntryEx<K, V> entryEx(GridCacheContext<K, V> cacheCtx, IgniteTxKey<K> key) {
        if (cacheCtx.isColocated()) {
            IgniteTxEntry<K, V> txEntry = entry(key);

            if (txEntry == null)
                return cacheCtx.colocated().entryExx(key.key(), topologyVersion(), true);

            GridCacheEntryEx<K, V> cached = txEntry.cached();

            assert cached != null;

            if (cached.detached())
                return cached;

            if (cached.obsoleteVersion() != null) {
                cached = cacheCtx.colocated().entryExx(key.key(), topologyVersion(), true);

                txEntry.cached(cached, txEntry.keyBytes());
            }

            return cached;
        }
        else
            return cacheCtx.cache().entryEx(key.key());
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx<K, V> entryEx(GridCacheContext<K, V> cacheCtx, IgniteTxKey<K> key, long topVer) {
        if (cacheCtx.isColocated()) {
            IgniteTxEntry<K, V> txEntry = entry(key);

            if (txEntry == null)
                return cacheCtx.colocated().entryExx(key.key(), topVer, true);

            GridCacheEntryEx<K, V> cached = txEntry.cached();

            assert cached != null;

            if (cached.detached())
                return cached;

            if (cached.obsoleteVersion() != null) {
                cached = cacheCtx.colocated().entryExx(key.key(), topVer, true);

                txEntry.cached(cached, txEntry.keyBytes());
            }

            return cached;
        }
        else
            return cacheCtx.cache().entryEx(key.key(), topVer);
    }

    /** {@inheritDoc} */
    @Override protected IgniteCacheExpiryPolicy accessPolicy(GridCacheContext ctx,
        IgniteTxKey key,
        @Nullable ExpiryPolicy expiryPlc)
    {
        assert optimistic();

        if (expiryPlc == null)
            expiryPlc = ctx.expiry();

        if (expiryPlc != null) {
            IgniteCacheExpiryPolicy plc = ctx.cache().accessExpiryPolicy(expiryPlc);

            if (plc != null) {
                if (accessMap == null)
                    accessMap = new HashMap<>();

                accessMap.put(key, plc);
            }

            return plc;
        }

        return null;
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys.
     * @return Expiry policy.
     */
    private IgniteCacheExpiryPolicy accessPolicy(GridCacheContext<K, V> cacheCtx, Collection<? extends K> keys) {
        if (accessMap != null) {
            for (Map.Entry<IgniteTxKey, IgniteCacheExpiryPolicy> e : accessMap.entrySet()) {
                if (e.getKey().cacheId() == cacheCtx.cacheId() && keys.contains(e.getKey().key()))
                    return e.getValue();
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        super.close();

        if (accessMap != null) {
            assert optimistic();

            for (Map.Entry<IgniteTxKey, IgniteCacheExpiryPolicy> e : accessMap.entrySet()) {
                if (e.getValue().entries() != null) {
                    GridCacheContext cctx0 = cctx.cacheContext(e.getKey().cacheId());

                    if (cctx0.isNear())
                        cctx0.near().dht().sendTtlUpdateRequest(e.getValue());
                    else
                        cctx0.dht().sendTtlUpdateRequest(e.getValue());
                }
            }

            accessMap = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxLocal.class, this, "mappings", mappings.keySet(), "super", super.toString());
    }

    /**
     *
     */
    private static class PessimisticPrepareFuture<K, V> extends GridFutureAdapter<IgniteTxEx<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Transaction. */
        @GridToStringExclude
        private IgniteTxEx<K, V> tx;

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
        private PessimisticPrepareFuture(GridKernalContext ctx, IgniteTxEx<K, V> tx) {
            super(ctx);
            this.tx = tx;
        }

        /**
         * @param e Exception.
         */
        void onError(Throwable e) {
            boolean marked = tx.setRollbackOnly();

            if (e instanceof IgniteTxRollbackException) {
                if (marked) {
                    try {
                        tx.rollback();
                    }
                    catch (IgniteCheckedException ex) {
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
