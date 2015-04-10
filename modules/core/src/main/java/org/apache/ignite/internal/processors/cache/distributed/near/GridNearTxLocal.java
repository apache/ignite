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
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 * Replicated user transaction.
 */
@SuppressWarnings("unchecked")
public class GridNearTxLocal extends GridDhtTxLocalAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** DHT mappings. */
    private ConcurrentMap<UUID, GridDistributedTxMapping> mappings =
        new ConcurrentHashMap8<>();

    /** Future. */
    @GridToStringExclude
    private final AtomicReference<IgniteInternalFuture<IgniteInternalTx>> prepFut =
        new AtomicReference<>();

    /** */
    @GridToStringExclude
    private final AtomicReference<GridNearTxFinishFuture> commitFut =
        new AtomicReference<>();

    /** */
    @GridToStringExclude
    private final AtomicReference<GridNearTxFinishFuture> rollbackFut =
        new AtomicReference<>();

    /** Entries to lock on next step of prepare stage. */
    private Collection<IgniteTxEntry> optimisticLockEntries = Collections.emptyList();

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
     * @param storeEnabled Store enabled flag.
     * @param txSize Transaction size.
     * @param grpLockKey Group lock key if this is a group lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction and the whole partition should be locked.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridNearTxLocal(
        GridCacheSharedContext ctx,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        GridIoPolicy plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
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
            false,
            plc,
            concurrency,
            isolation,
            timeout,
            false,
            storeEnabled,
            txSize,
            grpLockKey,
            partLock,
            subjId,
            taskNameHash);

        initResult();
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
    @Override protected IgniteInternalFuture<Boolean> addReader(
        long msgId, 
        GridDhtCacheEntry cached,
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer
    ) {
        // We are in near transaction, do not add local node as reader.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void sendFinishReply(boolean commit, @Nullable Throwable err) {
        // We are in near transaction, do not send finish reply to local node.
    }

    /** {@inheritDoc} */
    @Override protected void clearPrepareFuture(GridDhtTxPrepareFuture fut) {
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
    @Override public boolean ownsLockUnsafe(GridCacheEntryEx entry) {
        return entry.detached() || super.ownsLockUnsafe(entry);
    }

    /** {@inheritDoc} */
    @Override public boolean ownsLock(GridCacheEntryEx entry) throws GridCacheEntryRemovedException {
        return entry.detached() || super.ownsLock(entry);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> optimisticLockEntries() {
        if (groupLock())
            return super.optimisticLockEntries();

        return optimisticLockEntries;
    }

    /**
     * @param optimisticLockEntries Optimistic lock entries.
     */
    public void optimisticLockEntries(Collection<IgniteTxEntry> optimisticLockEntries) {
        this.optimisticLockEntries = optimisticLockEntries;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> loadMissing(
        final GridCacheContext cacheCtx,
        boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        boolean deserializePortable,
        boolean skipVals,
        final IgniteBiInClosure<KeyCacheObject, Object> c
    ) {
        if (cacheCtx.isNear()) {
            return cacheCtx.nearTx().txLoadAsync(this,
                keys,
                readThrough,
                deserializePortable,
                accessPolicy(cacheCtx, keys),
                skipVals).chain(new C1<IgniteInternalFuture<Map<Object, Object>>, Boolean>() {
                @Override public Boolean apply(IgniteInternalFuture<Map<Object, Object>> f) {
                    try {
                        Map<Object, Object> map = f.get();

                        // Must loop through keys, not map entries,
                        // as map entries may not have all the keys.
                        for (KeyCacheObject key : keys)
                            c.apply(key, map.get(key.value(cacheCtx.cacheObjectContext(), false)));

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
                accessPolicy(cacheCtx, keys),
                skipVals).chain(new C1<IgniteInternalFuture<Map<Object, Object>>, Boolean>() {
                    @Override public Boolean apply(IgniteInternalFuture<Map<Object, Object>> f) {
                        try {
                            Map<Object, Object> map = f.get();

                            // Must loop through keys, not map entries,
                            // as map entries may not have all the keys.
                            for (KeyCacheObject key : keys)
                                c.apply(key, map.get(key.value(cacheCtx.cacheObjectContext(), false)));

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

            return super.loadMissing(cacheCtx, readThrough, async, keys, deserializePortable, skipVals, c);
        }
    }

    /** {@inheritDoc} */
    @Override protected void updateExplicitVersion(IgniteTxEntry txEntry, GridCacheEntryEx entry)
        throws GridCacheEntryRemovedException {
        if (entry.detached()) {
            GridCacheMvccCandidate cand = cctx.mvcc().explicitLock(threadId(), entry.key());

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
    ConcurrentMap<UUID, GridDistributedTxMapping> mappings() {
        return mappings;
    }

    /**
     * @param nodeId Node ID.
     * @param dhtVer DHT version.
     */
    void addDhtVersion(UUID nodeId, GridCacheVersion dhtVer) {
        // This step is very important as near and DHT versions grow separately.
        cctx.versions().onReceived(nodeId, dhtVer);

        GridDistributedTxMapping m = mappings.get(nodeId);

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
    @Override protected void addGroupTxMapping(Collection<IgniteTxKey> keys) {
        super.addGroupTxMapping(keys);

        addKeyMapping(cctx.localNode(), keys);
    }

    /**
     * Adds key mapping to dht mapping.
     *
     * @param key Key to add.
     * @param node Node this key mapped to.
     */
    public void addKeyMapping(IgniteTxKey key, ClusterNode node) {
        GridDistributedTxMapping m = mappings.get(node.id());

        if (m == null)
            mappings.put(node.id(), m = new GridDistributedTxMapping(node));

        IgniteTxEntry txEntry = txMap.get(key);

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
    private void addKeyMapping(ClusterNode n, Iterable<IgniteTxKey> mappedKeys) {
        GridDistributedTxMapping m = mappings.get(n.id());

        if (m == null)
            mappings.put(n.id(), m = new GridDistributedTxMapping(n));

        for (IgniteTxKey key : mappedKeys) {
            IgniteTxEntry txEntry = txMap.get(key);

            assert txEntry != null;

            txEntry.nodeId(n.id());

            m.add(txEntry);
        }
    }

    /**
     * @param maps Mappings.
     */
    void addEntryMapping(@Nullable Collection<GridDistributedTxMapping> maps) {
        if (!F.isEmpty(maps)) {
            for (GridDistributedTxMapping map : maps) {
                ClusterNode n = map.node();

                GridDistributedTxMapping m = mappings.get(n.id());

                if (m == null)
                    m = F.addIfAbsent(mappings, n.id(), new GridDistributedTxMapping(n));

                assert m != null;

                for (IgniteTxEntry entry : map.entries())
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
    void removeKeysMapping(UUID failedNodeId, Iterable<GridDistributedTxMapping> mapQueue) {
        assert failedNodeId != null;
        assert mapQueue != null;

        mappings.remove(failedNodeId);

        if (!F.isEmpty(mapQueue)) {
            for (GridDistributedTxMapping m : mapQueue) {
                UUID nodeId = m.node().id();

                GridDistributedTxMapping mapping = mappings.get(nodeId);

                if (mapping != null) {
                    for (IgniteTxEntry entry : m.entries())
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
        explicitLock = true;

        GridDistributedTxMapping m = mappings.get(nodeId);

        if (m != null) {
            m.markExplicitLock();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        GridCacheMvccFuture<IgniteInternalTx> fut = (GridCacheMvccFuture<IgniteInternalTx>)prepFut.get();

        return fut != null && fut.onOwnerChanged(entry, owner);
    }

    /**
     * @param mapping Mapping to order.
     * @param pendingVers Pending versions.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    void readyNearLocks(GridDistributedTxMapping mapping,
        Collection<GridCacheVersion> pendingVers,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers)
    {
        Collection<IgniteTxEntry> entries = groupLock() ?
            Collections.singletonList(groupLockEntry()) :
            F.concat(false, mapping.reads(), mapping.writes());

        for (IgniteTxEntry txEntry : entries) {
            while (true) {
                GridCacheContext cacheCtx = txEntry.cached().context();

                assert cacheCtx.isNear();

                GridDistributedCacheEntry entry = (GridDistributedCacheEntry)txEntry.cached();

                try {
                    // Handle explicit locks.
                    GridCacheVersion explicit = txEntry.explicitVersion();

                    if (explicit == null)
                        entry.readyNearLock(xidVer, mapping.dhtVersion(), committedVers, rolledbackVers, pendingVers);

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    assert entry.obsoleteVersion() != null;

                    if (log.isDebugEnabled())
                        log.debug("Replacing obsolete entry in remote transaction [entry=" + entry +
                            ", tx=" + this + ']');

                    // Replace the entry.
                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key()));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean finish(boolean commit) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Finishing near local tx [tx=" + this + ", commit=" + commit + "]");

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
    @Override public IgniteInternalFuture<IgniteInternalTx> prepareAsync() {
        GridNearTxPrepareFuture fut = (GridNearTxPrepareFuture)prepFut.get();

        if (fut == null) {
            // Future must be created before any exception can be thrown.
            fut = new GridNearTxPrepareFuture<>(cctx, this);

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
            if (!state(PREPARING)) {
                if (setRollbackOnly()) {
                    if (timedOut())
                        fut.onError(new IgniteTxTimeoutCheckedException("Transaction timed out and was " +
                            "rolled back: " + this));
                    else
                        fut.onError(new IgniteCheckedException("Invalid transaction state for prepare [state=" +
                            state() + ", tx=" + this + ']'));
                }
                else
                    fut.onError(new IgniteTxRollbackCheckedException("Invalid transaction state for prepare " +
                        "[state=" + state() + ", tx=" + this + ']'));

                return fut;
            }

            try {
                userPrepare();

                // Make sure to add future before calling prepare.
                cctx.mvcc().addFuture(fut);

                fut.prepare();
            }
            catch (IgniteCheckedException e) {
                fut.onError(e);
            }
        }
        else {
            // In optimistic mode we must wait for topology map update.
            fut.prepare();
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing near local tx: " + this);

        prepareAsync();

        GridNearTxFinishFuture fut = commitFut.get();

        if (fut == null && !commitFut.compareAndSet(null, fut = new GridNearTxFinishFuture<>(cctx, this, true)))
            return commitFut.get();

        cctx.mvcc().addFuture(fut);

        IgniteInternalFuture<IgniteInternalTx> prepareFut = prepFut.get();

        prepareFut.listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
            @Override public void apply(IgniteInternalFuture<IgniteInternalTx> f) {
                GridNearTxFinishFuture fut0 = commitFut.get();

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
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        if (log.isDebugEnabled())
            log.debug("Rolling back near tx: " + this);

        GridNearTxFinishFuture fut = rollbackFut.get();

        if (fut != null)
            return fut;

        if (!rollbackFut.compareAndSet(null, fut = new GridNearTxFinishFuture<>(cctx, this, false)))
            return rollbackFut.get();

        cctx.mvcc().addFuture(fut);

        IgniteInternalFuture<IgniteInternalTx> prepFut = this.prepFut.get();

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
            prepFut.listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                @Override public void apply(IgniteInternalFuture<IgniteInternalTx> f) {
                    try {
                        // Check for errors in prepare future.
                        f.get();
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
                    }

                    GridNearTxFinishFuture fut0 = rollbackFut.get();

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
    public IgniteInternalFuture<IgniteInternalTx> prepareAsyncLocal(
        @Nullable Collection<IgniteTxEntry> reads,
        @Nullable Collection<IgniteTxEntry> writes,
        Map<UUID, Collection<UUID>> txNodes, boolean last,
        Collection<UUID> lastBackups,
        IgniteInClosure<GridNearTxPrepareResponse> completeCb
    ) {
        if (state() != PREPARING) {
            if (timedOut())
                return new GridFinishedFuture<>(
                    new IgniteTxTimeoutCheckedException("Transaction timed out: " + this));

            setRollbackOnly();

            return new GridFinishedFuture<>(
                new IgniteCheckedException("Invalid transaction state for prepare [state=" + state() + ", tx=" + this + ']'));
        }

        init();

        GridDhtTxPrepareFuture fut = new GridDhtTxPrepareFuture<>(
            cctx,
            this,
            IgniteUuid.randomUuid(),
            Collections.<IgniteTxKey, GridCacheVersion>emptyMap(),
            last,
            needReturnValue() && implicit(),
            lastBackups,
            completeCb);

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
        catch (IgniteTxTimeoutCheckedException | IgniteTxOptimisticCheckedException e) {
            fut.onError(e);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            fut.onError(new IgniteTxRollbackCheckedException("Failed to prepare transaction: " + this, e));

            try {
                rollback();
            }
            catch (IgniteTxOptimisticCheckedException e1) {
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
    public IgniteInternalFuture<IgniteInternalTx> commitAsyncLocal() {
        if (log.isDebugEnabled())
            log.debug("Committing colocated tx locally: " + this);

        // In optimistic mode prepare was called explicitly.
        if (pessimistic())
            prepareAsync();

        IgniteInternalFuture<IgniteInternalTx> prep = prepFut.get();

        // Do not create finish future if there are no remote nodes.
        if (F.isEmpty(dhtMap) && F.isEmpty(nearMap)) {
            if (prep != null)
                return (IgniteInternalFuture<IgniteInternalTx>)(IgniteInternalFuture)prep;

            return new GridFinishedFuture<IgniteInternalTx>(this);
        }

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, /*commit*/true);

        cctx.mvcc().addFuture(fut);

        if (prep == null || prep.isDone()) {
            assert prep != null || optimistic();

            try {
                if (prep != null)
                    prep.get(); // Check for errors of a parent future.

                fut.finish();
            }
            catch (IgniteTxOptimisticCheckedException e) {
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
            prep.listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                @Override public void apply(IgniteInternalFuture<IgniteInternalTx> f) {
                    try {
                        f.get(); // Check for errors of a parent future.

                        fut.finish();
                    }
                    catch (IgniteTxOptimisticCheckedException e) {
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
    public IgniteInternalFuture<IgniteInternalTx> rollbackAsyncLocal() {
        if (log.isDebugEnabled())
            log.debug("Rolling back colocated tx locally: " + this);

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, /*commit*/false);

        cctx.mvcc().addFuture(fut);

        IgniteInternalFuture<IgniteInternalTx> prep = prepFut.get();

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
            prep.listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                @Override public void apply(IgniteInternalFuture<IgniteInternalTx> f) {
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

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys.
     * @param implicit Implicit flag.
     * @param read Read flag.
     * @param accessTtl Access ttl.
     * @param <K> Key type.
     * @return Future with respond.
     */
    public <K> IgniteInternalFuture<GridCacheReturn> lockAllAsync(GridCacheContext cacheCtx,
        final Collection<? extends K> keys,
        boolean implicit,
        boolean read,
        long accessTtl) {
        assert pessimistic();

        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ret);

        init();

        if (log.isDebugEnabled())
            log.debug("Before acquiring transaction lock on keys: " + keys);

        IgniteInternalFuture<Boolean> fut = cacheCtx.colocated().lockAllAsyncInternal(keys,
            lockTimeout(),
            this,
            isInvalidate(),
            read,
            /*retval*/false,
            isolation,
            accessTtl,
            CU.empty0());

        return new GridEmbeddedFuture<>(
            fut,
            new PLC1<GridCacheReturn>(ret, false) {
                @Override protected GridCacheReturn postLock(GridCacheReturn ret) {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock on keys: " + keys);

                    return ret;
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx entryEx(GridCacheContext cacheCtx, IgniteTxKey key) {
        if (cacheCtx.isColocated()) {
            IgniteTxEntry txEntry = entry(key);

            if (txEntry == null)
                return cacheCtx.colocated().entryExx(key.key(), topologyVersion(), true);

            GridCacheEntryEx cached = txEntry.cached();

            assert cached != null;

            if (cached.detached())
                return cached;

            if (cached.obsoleteVersion() != null) {
                cached = cacheCtx.colocated().entryExx(key.key(), topologyVersion(), true);

                txEntry.cached(cached);
            }

            return cached;
        }
        else
            return cacheCtx.cache().entryEx(key.key());
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx entryEx(
        GridCacheContext cacheCtx, 
        IgniteTxKey key, 
        AffinityTopologyVersion topVer
    ) {
        if (cacheCtx.isColocated()) {
            IgniteTxEntry txEntry = entry(key);

            if (txEntry == null)
                return cacheCtx.colocated().entryExx(key.key(), topVer, true);

            GridCacheEntryEx cached = txEntry.cached();

            assert cached != null;

            if (cached.detached())
                return cached;

            if (cached.obsoleteVersion() != null) {
                cached = cacheCtx.colocated().entryExx(key.key(), topVer, true);

                txEntry.cached(cached);
            }

            return cached;
        }
        else
            return cacheCtx.cache().entryEx(key.key(), topVer);
    }

    /** {@inheritDoc} */
    @Override protected IgniteCacheExpiryPolicy accessPolicy(
        GridCacheContext ctx,
        IgniteTxKey key,
        @Nullable ExpiryPolicy expiryPlc
    ) {
        assert optimistic();

        IgniteCacheExpiryPolicy plc = ctx.cache().expiryPolicy(expiryPlc);

        if (plc != null) {
            if (accessMap == null)
                accessMap = new HashMap<>();

            accessMap.put(key, plc);
        }

        return plc;
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys.
     * @return Expiry policy.
     */
    private IgniteCacheExpiryPolicy accessPolicy(GridCacheContext cacheCtx, Collection<KeyCacheObject> keys) {
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
}
