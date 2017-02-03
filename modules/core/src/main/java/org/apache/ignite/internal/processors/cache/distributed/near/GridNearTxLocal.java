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

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

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
@SuppressWarnings("unchecked")
public class GridNearTxLocal extends GridDhtTxLocalAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Prepare future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, IgniteInternalFuture> PREP_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, IgniteInternalFuture.class, "prepFut");

    /** Prepare future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, GridNearTxFinishFuture> COMMIT_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, GridNearTxFinishFuture.class, "commitFut");

    /** Rollback future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, GridNearTxFinishFuture> ROLLBACK_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, GridNearTxFinishFuture.class, "rollbackFut");

    /** DHT mappings. */
    private IgniteTxMappings mappings;

    /** Prepare future. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringExclude
    private volatile IgniteInternalFuture<?> prepFut;

    /** Commit future. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringExclude
    private volatile GridNearTxFinishFuture commitFut;

    /** Rollback future. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringExclude
    private volatile GridNearTxFinishFuture rollbackFut;

    /** Entries to lock on next step of prepare stage. */
    private Collection<IgniteTxEntry> optimisticLockEntries = Collections.emptyList();

    /** True if transaction contains near cache entries mapped to local node. */
    private boolean nearLocallyMapped;

    /** True if transaction contains colocated cache entries mapped to local node. */
    private boolean colocatedLocallyMapped;

    /** Info for entries accessed locally in optimistic transaction. */
    private Map<IgniteTxKey, IgniteCacheExpiryPolicy> accessMap;

    /** */
    private Boolean needCheckBackup;

    /** */
    private boolean hasRemoteLocks;

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
     * @param plc IO policy.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param storeEnabled Store enabled flag.
     * @param txSize Transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridNearTxLocal(
        GridCacheSharedContext ctx,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean storeEnabled,
        int txSize,
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
            false,
            txSize,
            subjId,
            taskNameHash);

        mappings = implicitSingle ? new IgniteTxMappingsSingleImpl() : new IgniteTxMappingsImpl();

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
    @Override public GridCacheVersion nearXidVersion() {
        return xidVer;
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
    @Override protected void sendFinishReply(@Nullable Throwable err) {
        // We are in near transaction, do not send finish reply to local node.
    }

    /** {@inheritDoc} */
    @Override protected void clearPrepareFuture(GridDhtTxPrepareFuture fut) {
        PREP_FUT_UPD.compareAndSet(this, fut, null);
    }

    /**
     * Marks transaction to check if commit on backup.
     */
    public void markForBackupCheck() {
        needCheckBackup = true;
    }

    /**
     * @return If need to check tx commit on backup.
     */
    public boolean onNeedCheckBackup() {
        Boolean check = needCheckBackup;

        if (check != null && check) {
            needCheckBackup = false;

            return true;
        }

        return false;
    }

    /**
     * @return If backup check was requested.
     */
    public boolean needCheckBackup() {
        return needCheckBackup != null;
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
        return optimisticLockEntries;
    }

    /**
     * @param optimisticLockEntries Optimistic lock entries.
     */
    public void optimisticLockEntries(Collection<IgniteTxEntry> optimisticLockEntries) {
        this.optimisticLockEntries = optimisticLockEntries;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> loadMissing(
        final GridCacheContext cacheCtx,
        AffinityTopologyVersion topVer,
        boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        final boolean skipVals,
        final boolean needVer,
        boolean keepBinary,
        final ExpiryPolicy expiryPlc,
        final GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c
    ) {
        IgniteCacheExpiryPolicy expiryPlc0 = optimistic() ?
            accessPolicy(cacheCtx, keys) :
            cacheCtx.cache().expiryPolicy(expiryPlc);

        if (cacheCtx.isNear()) {
            return cacheCtx.nearTx().txLoadAsync(this,
                topVer,
                keys,
                readThrough,
                /*deserializeBinary*/false,
                expiryPlc0,
                skipVals,
                needVer).chain(new C1<IgniteInternalFuture<Map<Object, Object>>, Void>() {
                @Override public Void apply(IgniteInternalFuture<Map<Object, Object>> f) {
                    try {
                        Map<Object, Object> map = f.get();

                        processLoaded(map, keys, needVer, c);

                        return null;
                    }
                    catch (Exception e) {
                        setRollbackOnly();

                        throw new GridClosureException(e);
                    }
                }
            });
        }
        else if (cacheCtx.isColocated()) {
            if (keys.size() == 1) {
                final KeyCacheObject key = F.first(keys);

                return cacheCtx.colocated().loadAsync(
                    key,
                    readThrough,
                    /*force primary*/needVer || !cacheCtx.config().isReadFromBackup(),
                    topVer,
                    CU.subjectId(this, cctx),
                    resolveTaskName(),
                    /*deserializeBinary*/false,
                    expiryPlc0,
                    skipVals,
                    /*can remap*/true,
                    needVer,
                    /*keepCacheObject*/true
                ).chain(new C1<IgniteInternalFuture<Object>, Void>() {
                    @Override public Void apply(IgniteInternalFuture<Object> f) {
                        try {
                            Object val = f.get();

                            processLoaded(key, val, needVer, skipVals, c);

                            return null;
                        }
                        catch (Exception e) {
                            setRollbackOnly();

                            throw new GridClosureException(e);
                        }
                    }
                });
            }
            else {
                return cacheCtx.colocated().loadAsync(
                    keys,
                    readThrough,
                    /*force primary*/needVer || !cacheCtx.config().isReadFromBackup(),
                    topVer,
                    CU.subjectId(this, cctx),
                    resolveTaskName(),
                    /*deserializeBinary*/false,
                    expiryPlc0,
                    skipVals,
                    /*can remap*/true,
                    needVer,
                    /*keepCacheObject*/true
                ).chain(new C1<IgniteInternalFuture<Map<Object, Object>>, Void>() {
                    @Override public Void apply(IgniteInternalFuture<Map<Object, Object>> f) {
                        try {
                            Map<Object, Object> map = f.get();

                            processLoaded(map, keys, needVer, c);

                            return null;
                        }
                        catch (Exception e) {
                            setRollbackOnly();

                            throw new GridClosureException(e);
                        }
                    }
                });
            }
        }
        else {
            assert cacheCtx.isLocal();

            return super.loadMissing(cacheCtx,
                topVer,
                readThrough,
                async,
                keys,
                skipVals,
                keepBinary,
                needVer,
                expiryPlc,
                c);
        }
    }

    /**
     * @param map Loaded values.
     * @param keys Keys.
     * @param needVer If {@code true} version is required for loaded values.
     * @param c Closure.
     */
    private void processLoaded(
        Map<Object, Object> map,
        final Collection<KeyCacheObject> keys,
        boolean needVer,
        GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c) {
        for (KeyCacheObject key : keys)
            processLoaded(key, map.get(key), needVer, false, c);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param needVer If {@code true} version is required for loaded values.
     * @param skipVals Skip values flag.
     * @param c Closure.
     */
    private void processLoaded(
        KeyCacheObject key,
        @Nullable Object val,
        boolean needVer,
        boolean skipVals,
        GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c) {
        if (val != null) {
            Object v;
            GridCacheVersion ver;

            if (needVer) {
                T2<Object, GridCacheVersion> t = (T2)val;

                v = t.get1();
                ver = t.get2();
            }
            else {
                v = val;
                ver = null;
            }

            if (skipVals && v == Boolean.FALSE)
                c.apply(key, null, IgniteTxEntry.SER_READ_EMPTY_ENTRY_VER);
            else
                c.apply(key, v, ver);
        }
        else
            c.apply(key, null, IgniteTxEntry.SER_READ_EMPTY_ENTRY_VER);
    }

    /** {@inheritDoc} */
    @Override protected void updateExplicitVersion(IgniteTxEntry txEntry, GridCacheEntryEx entry)
        throws GridCacheEntryRemovedException {
        if (entry.detached()) {
            GridCacheMvccCandidate cand = cctx.mvcc().explicitLock(threadId(), entry.txKey());

            if (cand != null && !xidVersion().equals(cand.version())) {
                GridCacheVersion candVer = cand.version();

                txEntry.explicitVersion(candVer);

                if (candVer.compareTo(minVer) < 0)
                    minVer = candVer;
            }
        }
        else
            super.updateExplicitVersion(txEntry, entry);
    }

    /**
     * @return DHT map.
     */
    IgniteTxMappings mappings() {
        return mappings;
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

    /**
     * Adds key mapping to dht mapping.
     *
     * @param key Key to add.
     * @param node Node this key mapped to.
     */
    public void addKeyMapping(IgniteTxKey key, ClusterNode node) {
        GridDistributedTxMapping m = mappings.get(node.id());

        if (m == null)
            mappings.put(m = new GridDistributedTxMapping(node));

        IgniteTxEntry txEntry = entry(key);

        assert txEntry != null;

        txEntry.nodeId(node.id());

        m.add(txEntry);

        if (log.isDebugEnabled())
            log.debug("Added mappings to transaction [locId=" + cctx.localNodeId() + ", key=" + key + ", node=" + node +
                ", tx=" + this + ']');
    }

    /**
     * @return Non-null entry if tx has only one write entry.
     */
    @Nullable IgniteTxEntry singleWrite() {
        return txState.singleWrite();
    }

    /**
     * @param maps Mappings.
     */
    void addEntryMapping(@Nullable Collection<GridDistributedTxMapping> maps) {
        if (!F.isEmpty(maps)) {
            for (GridDistributedTxMapping map : maps) {
                ClusterNode n = map.node();

                GridDistributedTxMapping m = mappings.get(n.id());

                if (m == null) {
                    mappings.put(m = new GridDistributedTxMapping(n));

                    m.near(map.near());

                    if (map.explicitLock())
                        m.markExplicitLock();
                }

                for (IgniteTxEntry entry : map.entries())
                    m.add(entry);
            }

            if (log.isDebugEnabled())
                log.debug("Added mappings to transaction [locId=" + cctx.localNodeId() + ", mappings=" + maps +
                    ", tx=" + this + ']');
        }
    }

    /**
     * @param map Mapping.
     * @param entry Entry.
     */
    void addSingleEntryMapping(GridDistributedTxMapping map, IgniteTxEntry entry) {
        ClusterNode n = map.node();

        GridDistributedTxMapping m = new GridDistributedTxMapping(n);

        mappings.put(m);

        m.near(map.near());

        if (map.explicitLock())
            m.markExplicitLock();

        m.add(entry);
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
        GridCacheMvccFuture<IgniteInternalTx> fut = (GridCacheMvccFuture<IgniteInternalTx>)prepFut;

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
        // Process writes, then reads.
        for (IgniteTxEntry txEntry : mapping.entries()) {
            if (CU.writes().apply(txEntry))
                readyNearLock(txEntry, mapping.dhtVersion(), pendingVers, committedVers, rolledbackVers);
        }

        for (IgniteTxEntry txEntry : mapping.entries()) {
            if (CU.reads().apply(txEntry))
                readyNearLock(txEntry, mapping.dhtVersion(), pendingVers, committedVers, rolledbackVers);
        }
    }

    /**
     * @param txEntry TX entry.
     * @param dhtVer DHT version.
     * @param pendingVers Pending versions.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    void readyNearLock(IgniteTxEntry txEntry,
        GridCacheVersion dhtVer,
        Collection<GridCacheVersion> pendingVers,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers)
    {
        while (true) {
            GridCacheContext cacheCtx = txEntry.cached().context();

            assert cacheCtx.isNear();

            GridDistributedCacheEntry entry = (GridDistributedCacheEntry)txEntry.cached();

            try {
                // Handle explicit locks.
                GridCacheVersion explicit = txEntry.explicitVersion();

                if (explicit == null) {
                    entry.readyNearLock(xidVer,
                        dhtVer,
                        committedVers,
                        rolledbackVers,
                        pendingVers);
                }

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                assert entry.obsoleteVersion() != null;

                if (log.isDebugEnabled())
                    log.debug("Replacing obsolete entry in remote transaction [entry=" + entry +
                        ", tx=" + this + ']');

                // Replace the entry.
                txEntry.cached(txEntry.context().cache().entryEx(txEntry.key(), topologyVersion()));
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
    @Override public IgniteInternalFuture<?> prepareAsync() {
        GridNearTxPrepareFutureAdapter fut = (GridNearTxPrepareFutureAdapter)prepFut;

        if (fut == null) {
            long timeout = remainingTime();

            // Future must be created before any exception can be thrown.
            if (optimistic()) {
                fut = serializable() ?
                    new GridNearOptimisticSerializableTxPrepareFuture(cctx, this) :
                    new GridNearOptimisticTxPrepareFuture(cctx, this);
            }
            else
                fut = new GridNearPessimisticTxPrepareFuture(cctx, this);

            if (!PREP_FUT_UPD.compareAndSet(this, null, fut))
                return prepFut;

            if (timeout == -1) {
                fut.onDone(this, timeoutException());

                return fut;
            }
        }
        else
            // Prepare was called explicitly.
            return fut;

        mapExplicitLocks();

        fut.prepare();

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing near local tx: " + this);

        if (fastFinish()) {
            state(PREPARING);
            state(PREPARED);
            state(COMMITTING);

            cctx.tm().fastFinishTx(this, true);

            state(COMMITTED);

            return new GridFinishedFuture<>((IgniteInternalTx)this);
        }

        prepareAsync();

        GridNearTxFinishFuture fut = commitFut;

        if (fut == null &&
            !COMMIT_FUT_UPD.compareAndSet(this, null, fut = new GridNearTxFinishFuture<>(cctx, this, true)))
            return commitFut;

        cctx.mvcc().addFuture(fut, fut.futureId());

        final IgniteInternalFuture<?> prepareFut = prepFut;

        prepareFut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                GridNearTxFinishFuture fut0 = commitFut;

                try {
                    // Make sure that here are no exceptions.
                    prepareFut.get();

                    fut0.finish(true);
                }
                catch (Error | RuntimeException e) {
                    COMMIT_ERR_UPD.compareAndSet(GridNearTxLocal.this, null, e);

                    fut0.finish(false);

                    throw e;
                }
                catch (IgniteCheckedException e) {
                    COMMIT_ERR_UPD.compareAndSet(GridNearTxLocal.this, null, e);

                    fut0.finish(false);
                }
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        if (log.isDebugEnabled())
            log.debug("Rolling back near tx: " + this);

        if (fastFinish()) {
            state(PREPARING);
            state(PREPARED);
            state(ROLLING_BACK);

            cctx.tm().fastFinishTx(this, false);

            state(ROLLED_BACK);

            return new GridFinishedFuture<>((IgniteInternalTx)this);
        }

        GridNearTxFinishFuture fut = rollbackFut;

        if (fut != null)
            return fut;

        if (!ROLLBACK_FUT_UPD.compareAndSet(this, null, fut = new GridNearTxFinishFuture<>(cctx, this, false)))
            return rollbackFut;

        cctx.mvcc().addFuture(fut, fut.futureId());

        IgniteInternalFuture<?> prepFut = this.prepFut;

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

            fut.finish(false);
        }
        else {
            prepFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    try {
                        // Check for errors in prepare future.
                        f.get();
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got optimistic tx failure [tx=" + this + ", err=" + e + ']');
                    }

                    GridNearTxFinishFuture fut0 = rollbackFut;

                    fut0.finish(false);
                }
            });
        }

        return fut;
    }

    /**
     * @return {@code True} if 'fast finish' path can be used for transaction completion.
     */
    private boolean fastFinish() {
        return writeMap().isEmpty() && (optimistic() || readMap().isEmpty());
    }

    /**
     * Prepares next batch of entries in dht transaction.
     *
     * @param reads Read entries.
     * @param writes Write entries.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this is last prepare request.
     * @return Future that will be completed when locks are acquired.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public IgniteInternalFuture<GridNearTxPrepareResponse> prepareAsyncLocal(
        @Nullable Collection<IgniteTxEntry> reads,
        @Nullable Collection<IgniteTxEntry> writes,
        Map<UUID, Collection<UUID>> txNodes,
        boolean last
    ) {
        long timeout = remainingTime();

        if (state() != PREPARING) {
            if (timeout == -1)
                return new GridFinishedFuture<>(
                    new IgniteTxTimeoutCheckedException("Transaction timed out: " + this));

            setRollbackOnly();

            return new GridFinishedFuture<>(
                new IgniteCheckedException("Invalid transaction state for prepare [state=" + state() + ", tx=" + this + ']'));
        }

        if (timeout == -1)
            return new GridFinishedFuture<>(timeoutException());

        init();

        GridDhtTxPrepareFuture fut = new GridDhtTxPrepareFuture(
            cctx,
            this,
            timeout,
            IgniteUuid.randomUuid(),
            Collections.<IgniteTxKey, GridCacheVersion>emptyMap(),
            last,
            needReturnValue() && implicit());

        try {
            // At this point all the entries passed in must be enlisted in transaction because this is an
            // optimistic transaction.
            optimisticLockEntries = (serializable() && optimistic()) ? F.concat(false, writes, reads) : writes;

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

        return chainOnePhasePrepare(fut);
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

        IgniteInternalFuture<?> prep = prepFut;

        // Do not create finish future if there are no remote nodes.
        if (F.isEmpty(dhtMap) && F.isEmpty(nearMap)) {
            if (prep != null)
                return (IgniteInternalFuture<IgniteInternalTx>)(IgniteInternalFuture)prep;

            return new GridFinishedFuture<IgniteInternalTx>(this);
        }

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, true);

        cctx.mvcc().addFuture(fut, fut.futureId());

        if (prep == null || prep.isDone()) {
            assert prep != null || optimistic();

            IgniteCheckedException err = null;

            try {
                if (prep != null)
                    prep.get(); // Check for errors of a parent future.
            }
            catch (IgniteCheckedException e) {
                err = e;

                U.error(log, "Failed to prepare transaction: " + this, e);
            }

            if (err != null)
                fut.rollbackOnError(err);
            else
                fut.finish(true);
        }
        else
            prep.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    IgniteCheckedException err = null;

                    try {
                        f.get(); // Check for errors of a parent future.
                    }
                    catch (IgniteCheckedException e) {
                        err = e;

                        U.error(log, "Failed to prepare transaction: " + this, e);
                    }

                    if (err != null)
                        fut.rollbackOnError(err);
                    else
                        fut.finish(true);
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

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, false);

        cctx.mvcc().addFuture(fut, fut.futureId());

        IgniteInternalFuture<?> prep = prepFut;

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

            fut.finish(false);
        }
        else
            prep.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    try {
                        f.get(); // Check for errors of a parent future.
                    }
                    catch (IgniteCheckedException e) {
                        log.debug("Failed to prepare transaction during rollback (will ignore) [tx=" + this + ", msg=" +
                            e.getMessage() + ']');
                    }

                    fut.finish(false);
                }
            });

        return fut;
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys.
     * @param retval Return value flag.
     * @param read Read flag.
     * @param createTtl Create ttl.
     * @param accessTtl Access ttl.
     * @param <K> Key type.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @return Future with respond.
     */
    public <K> IgniteInternalFuture<GridCacheReturn> lockAllAsync(GridCacheContext cacheCtx,
        final Collection<? extends K> keys,
        boolean retval,
        boolean read,
        long createTtl,
        long accessTtl,
        boolean skipStore,
        boolean keepBinary) {
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

        long timeout = remainingTime();

        if (timeout == -1)
            return new GridFinishedFuture<>(timeoutException());

        IgniteInternalFuture<Boolean> fut = cacheCtx.colocated().lockAllAsyncInternal(keys,
            timeout,
            this,
            isInvalidate(),
            read,
            retval,
            isolation,
            createTtl,
            accessTtl,
            CU.empty0(),
            skipStore,
            keepBinary);

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

    /** {@inheritDoc} */
    @Override protected IgniteCacheExpiryPolicy accessPolicy(GridCacheContext cacheCtx, Collection<KeyCacheObject> keys) {
        assert optimistic();

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
    @SuppressWarnings("unchecked")
    @Nullable @Override public IgniteInternalFuture<?> currentPrepareFuture() {
        return prepFut;
    }

    /** {@inheritDoc} */
    @Override public void onRemap(AffinityTopologyVersion topVer) {
        assert cctx.kernalContext().clientNode();

        mapped = false;
        nearLocallyMapped = false;
        colocatedLocallyMapped = false;
        txNodes = null;
        onePhaseCommit = false;
        nearMap.clear();
        dhtMap.clear();
        mappings.clear();

        synchronized (this) {
            this.topVer = topVer;
        }
    }

    /**
     * @param hasRemoteLocks {@code True} if tx has remote locks acquired.
     */
    public void hasRemoteLocks(boolean hasRemoteLocks) {
        this.hasRemoteLocks = hasRemoteLocks;
    }

    /**
     * @return {@code True} if tx has remote locks acquired.
     */
    public boolean hasRemoteLocks() {
        return hasRemoteLocks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxLocal.class, this,
            "thread", IgniteUtils.threadName(threadId),
            "mappings", mappings,
            "super", super.toString());
    }
}
