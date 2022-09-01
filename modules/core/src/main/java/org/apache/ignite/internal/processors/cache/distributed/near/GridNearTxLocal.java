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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridInvokeValue;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.GridNearReadRepairCheckOnlyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.GridNearReadRepairFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorChangeAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxy;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyRollbackOnlyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.SER_READ_NOT_EMPTY_VER;
import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_ENLIST_READ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_ENLIST_WRITE;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Replicated user transaction.
 */
@SuppressWarnings("unchecked")
public class GridNearTxLocal extends GridDhtTxLocalAdapter implements GridTimeoutObject, AutoCloseable, MvccCoordinatorChangeAware {
    /** Prepare future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, IgniteInternalFuture> PREP_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, IgniteInternalFuture.class, "prepFut");

    /** Prepare future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, NearTxFinishFuture> FINISH_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, NearTxFinishFuture.class, "finishFut");

    /** */
    private static final String TX_TYPE_MISMATCH_ERR_MSG =
        "SQL queries and cache operations may not be used in the same transaction.";

    /** DHT mappings. */
    private final IgniteTxMappings mappings;

    /** Prepare future. */
    @GridToStringExclude
    private volatile IgniteInternalFuture<?> prepFut;

    /** Commit future. */
    @GridToStringExclude
    private volatile NearTxFinishFuture finishFut;

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

    /** If this transaction contains transform entries. */
    protected boolean transform;

    /** */
    private boolean trackTimeout;

    /**
     * Counts how much time this transaction has spent on system calls, in nanoseconds.
     */
    private final AtomicLong systemTime = new AtomicLong(0);

    /**
     * Stores the nano time value when current system time has started, or <code>0</code> if no system section
     * is running currently.
     */
    private final AtomicLong systemStartTime = new AtomicLong(0);

    /**
     * Stores the nano time value when prepare step has started, or <code>0</code> if no prepare step
     * has started yet.
     */
    private final AtomicLong prepareStartTime = new AtomicLong(0);

    /**
     * Stores prepare step duration, or <code>0</code> if it has not finished yet.
     */
    private final AtomicLong prepareTime = new AtomicLong(0);

    /**
     * Stores the nano time value when commit or rollback step has started, or <code>0</code> if it
     * has not started yet.
     */
    private final AtomicLong commitOrRollbackStartTime = new AtomicLong(0);

    /** Stores commit or rollback step duration, or <code>0</code> if it has not finished yet. */
    private final AtomicLong commitOrRollbackTime = new AtomicLong(0);

    /** */
    @GridToStringExclude
    private final IgniteTxManager.TxDumpsThrottling txDumpsThrottling;

    /** */
    @GridToStringExclude
    private TransactionProxyImpl proxy;

    /** */
    @GridToStringExclude
    private TransactionProxyImpl rollbackOnlyProxy;

    /** Tx label. */
    @Nullable private final String lb;

    /** Whether this is Mvcc transaction or not.<p>
     * {@code null} means there haven't been any calls made on this transaction, and first operation will give this
     * field actual value.
     */
    private Boolean mvccOp;

    /** */
    private long qryId = MVCC_TRACKER_ID_NA;

    /** */
    private long crdVer;

    /**
     * @param ctx Cache registry.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit with one key flag.
     * @param sys System flag.
     * @param plc IO policy.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param storeEnabled Store enabled flag.
     * @param mvccOp Whether this transaction was started via SQL API or not, or {@code null} if unknown.
     * @param txSize Transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param lb Label.
     * @param txDumpsThrottling Log throttling information.
     * @param tracingEnabled {@code true} if the transaction should be traced.
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
        Boolean mvccOp,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        @Nullable String lb,
        IgniteTxManager.TxDumpsThrottling txDumpsThrottling,
        boolean tracingEnabled
    ) {
        super(
            ctx,
            ctx.versions().next(ctx.kernalContext().discovery().topologyVersion()),
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
        this.lb = lb;

        mappings = implicitSingle ? new IgniteTxMappingsSingleImpl() : new IgniteTxMappingsImpl();

        this.mvccOp = mvccOp;

        this.txDumpsThrottling = txDumpsThrottling;

        initResult();

        trackTimeout = timeout() > 0 && !implicit() && cctx.time().addTimeoutObject(this);
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
       //No-op.
    }

    /**
     * Marks transaction to check if commit on backup.
     */
    void markForBackupCheck() {
        needCheckBackup = true;
    }

    /**
     * @return If need to check tx commit on backup.
     */
    boolean onNeedCheckBackup() {
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
    boolean needCheckBackup() {
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
    void nearLocallyMapped(boolean nearLocallyMapped) {
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
    @Override public long timeout(long timeout) {
        long old = super.timeout(timeout);

        if (old == timeout)
            return old;

        if (trackTimeout) {
            if (!cctx.time().removeTimeoutObject(this))
                return old; // Do nothing, because transaction is started to roll back.
        }

        if (timeout() > 0) // Method can be called only for explicit transactions.
            trackTimeout = cctx.time().addTimeoutObject(this);

        return old;
    }

    /** {@inheritDoc} */
    @Override public boolean ownsLock(GridCacheEntryEx entry) throws GridCacheEntryRemovedException {
        return entry.detached() || super.ownsLock(entry);
    }

    /**
     * @param cacheCtx Cache context.
     * @param map Map to put.
     * @param retval Flag indicating whether a value should be returned.
     * @return Future for put operation.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> putAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Map<? extends K, ? extends V> map,
        boolean retval
    ) {
        return (IgniteInternalFuture<GridCacheReturn>)putAllAsync0(cacheCtx,
            entryTopVer,
            map,
            null,
            null,
            null,
            retval);
    }

    /**
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param val Value.
     * @param retval Return value flag.
     * @param filter Filter.
     * @return Future for put operation.
     */
    public final <K, V> IgniteInternalFuture<GridCacheReturn> putAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        V val,
        boolean retval,
        CacheEntryPredicate filter) {
        return putAsync0(cacheCtx,
            entryTopVer,
            key,
            val,
            null,
            null,
            retval,
            filter);
    }

    /**
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for entry processor.
     * @return Operation future.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> invokeAsync(GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        EntryProcessor<K, V, Object> entryProcessor,
        Object... invokeArgs) {
        return (IgniteInternalFuture)putAsync0(cacheCtx,
            entryTopVer,
            key,
            null,
            entryProcessor,
            invokeArgs,
            true,
            null);
    }

    /**
     * @param cacheCtx Cache context.
     * @param map Entry processors map.
     * @param invokeArgs Optional arguments for entry processor.
     * @return Operation future.
     */
    public <K, V, T> IgniteInternalFuture<GridCacheReturn> invokeAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> map,
        Object... invokeArgs
    ) {
        return (IgniteInternalFuture<GridCacheReturn>)putAllAsync0(cacheCtx,
            entryTopVer,
            null,
            map,
            invokeArgs,
            null,
            true);
    }

    /**
     * @param cacheCtx Cache context.
     * @param drMap DR map to put.
     * @return Future for DR put operation.
     */
    public IgniteInternalFuture<?> putAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheDrInfo> drMap
    ) {
        Map<KeyCacheObject, Object> map = F.viewReadOnly(drMap, new IgniteClosure<GridCacheDrInfo, Object>() {
            @Override public Object apply(GridCacheDrInfo val) {
                return val.value();
            }
        });

        return this.<Object, Object>putAllAsync0(cacheCtx,
            null,
            map,
            null,
            null,
            drMap,
            false);
    }

    /**
     * @param cacheCtx Cache context.
     * @param drMap DR map.
     * @return Future for asynchronous remove.
     */
    public IgniteInternalFuture<?> removeAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheVersion> drMap
    ) {
        return removeAllAsync0(cacheCtx, null, null, drMap, false, null, false);
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param retval Flag indicating whether a value should be returned.
     * @param filter Filter.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @return Future for asynchronous remove.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<? extends K> keys,
        boolean retval,
        CacheEntryPredicate filter,
        boolean singleRmv
    ) {
        return removeAllAsync0(cacheCtx, entryTopVer, keys, null, retval, filter, singleRmv);
    }

    /**
     * Internal method for single update operation.
     *
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param val Value.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Return value flag.
     * @param filter Filter.
     * @return Operation future.
     */
    private <K, V> IgniteInternalFuture putAsync0(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        @Nullable V val,
        @Nullable EntryProcessor<K, V, Object> entryProcessor,
        @Nullable final Object[] invokeArgs,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter
    ) {
        assert key != null;

        if (cacheCtx.mvccEnabled())
            return mvccPutAllAsync0(cacheCtx, Collections.singletonMap(key, val),
                entryProcessor == null ? null : Collections.singletonMap(key, entryProcessor), invokeArgs, retval, filter);

        try {
            beforePut(cacheCtx, retval, false);

            final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

            CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            final Byte dataCenterId = opCtx != null ? opCtx.dataCenterId() : null;

            KeyCacheObject cacheKey = cacheCtx.toCacheKeyObject(key);

            boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            final CacheEntryPredicate[] filters = CU.filterArray(filter);

            final IgniteInternalFuture<Void> loadFut = enlistWrite(
                cacheCtx,
                entryTopVer,
                cacheKey,
                val,
                opCtx != null ? opCtx.expiry() : null,
                entryProcessor,
                invokeArgs,
                retval,
                /*lockOnly*/false,
                filters,
                ret,
                opCtx != null && opCtx.skipStore(),
                /*singleRmv*/false,
                keepBinary,
                opCtx != null && opCtx.recovery(),
                dataCenterId);

            try {
                loadFut.get();
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture(e);
            }

            long timeout = remainingTime();

            if (timeout == -1)
                return new GridFinishedFuture<>(timeoutException());

            if (isRollbackOnly())
                return new GridFinishedFuture<>(rollbackException());

            if (pessimistic()) {
                final Collection<KeyCacheObject> enlisted = Collections.singleton(cacheKey);

                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for put on key: " + enlisted);

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(enlisted,
                    timeout,
                    this,
                    /*read*/entryProcessor != null, // Needed to force load from store.
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override public GridCacheReturn postLock(
                        GridCacheReturn ret
                    ) throws IgniteCheckedException {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for put on keys: " + enlisted);

                        postLockWrite(cacheCtx,
                            enlisted,
                            ret,
                            /*remove*/false,
                            retval,
                            /*read*/false,
                            -1L,
                            filters,
                            /*computeInvoke*/true);

                        return ret;
                    }
                };

                if (fut.isDone()) {
                    try {
                        return nonInterruptable(plc1.apply(fut.get(), null));
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return nonInterruptable(plc1.apply(false, e));
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return nonInterruptable(new GridEmbeddedFuture<>(
                        fut,
                        plc1
                    ));
                }
            }
            else
                return optimisticPutFuture(cacheCtx, loadFut, ret, keepBinary);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * Internal method for put and transform operations in Mvcc mode.
     * Note: Only one of {@code map}, {@code transformMap} maps must be non-null.
     *
     * @param cacheCtx Context.
     * @param map Key-value map to store.
     * @param invokeMap Invoke map.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Key-transform value map to store.
     * @param filter Filter.
     * @return Operation future.
     */
    private <K, V> IgniteInternalFuture mvccPutAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable Map<? extends K, ? extends V> map,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> invokeMap,
        @Nullable final Object[] invokeArgs,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter
    ) {
        try {
            MvccUtils.requestSnapshot(this);

            beforePut(cacheCtx, retval, true);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        if (log.isDebugEnabled())
            log.debug("Called putAllAsync(...) [tx=" + this + ", map=" + map + ", retval=" + retval + "]");

        assert map != null || invokeMap != null;

        if (F.isEmpty(map) && F.isEmpty(invokeMap)) {
            if (implicit())
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }

            return new GridFinishedFuture<>(new GridCacheReturn(true, false));
        }

        // Set transform flag for operation.
        boolean transform = invokeMap != null;

        try {
            Set<?> keys = map != null ? map.keySet() : invokeMap.keySet();

            final Map<KeyCacheObject, Object> enlisted = new LinkedHashMap<>(keys.size());

            for (Object key : keys) {
                if (isRollbackOnly())
                    return new GridFinishedFuture<>(timedOut() ? timeoutException() : rollbackException());

                if (key == null) {
                    rollback();

                    throw new NullPointerException("Null key.");
                }

                Object val = map == null ? null : map.get(key);
                EntryProcessor entryProcessor = transform ? invokeMap.get(key) : null;

                if (val == null && entryProcessor == null) {
                    setRollbackOnly();

                    throw new NullPointerException("Null value.");
                }

                KeyCacheObject cacheKey = cacheCtx.toCacheKeyObject(key);

                if (transform)
                    enlisted.put(cacheKey, new GridInvokeValue(entryProcessor, invokeArgs));
                else
                    enlisted.put(cacheKey, val);
            }

            return updateAsync(cacheCtx, new UpdateSourceIterator<IgniteBiTuple<KeyCacheObject, Object>>() {

                private final Iterator<Map.Entry<KeyCacheObject, Object>> it = enlisted.entrySet().iterator();

                @Override public EnlistOperation operation() {
                    return transform ? EnlistOperation.TRANSFORM : EnlistOperation.UPSERT;
                }

                @Override public boolean hasNextX() throws IgniteCheckedException {
                    return it.hasNext();
                }

                @Override public IgniteBiTuple<KeyCacheObject, Object> nextX() throws IgniteCheckedException {
                    Map.Entry<KeyCacheObject, Object> next = it.next();

                    return new IgniteBiTuple<>(next.getKey(), next.getValue());
                }
            }, retval, filter, remainingTime(), true);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * Internal method for all put and transform operations. Only one of {@code map}, {@code transformMap}
     * maps must be non-null.
     *
     * @param cacheCtx Context.
     * @param map Key-value map to store.
     * @param invokeMap Invoke map.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param drMap DR map.
     * @param retval Key-transform value map to store.
     * @return Operation future.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture putAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        @Nullable Map<? extends K, ? extends V> map,
        @Nullable Map<? extends K, ? extends EntryProcessor<K, V, Object>> invokeMap,
        @Nullable final Object[] invokeArgs,
        @Nullable Map<KeyCacheObject, GridCacheDrInfo> drMap,
        final boolean retval
    ) {
        if (cacheCtx.mvccEnabled())
            return mvccPutAllAsync0(cacheCtx, map, invokeMap, invokeArgs, retval, null);

        try {
            beforePut(cacheCtx, retval, false);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        final CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

        final Byte dataCenterId;

        if (opCtx != null && opCtx.hasDataCenterId()) {
            assert drMap == null : drMap;
            assert map != null || invokeMap != null;

            dataCenterId = opCtx.dataCenterId();
        }
        else
            dataCenterId = null;

        // Cached entry may be passed only from entry wrapper.
        final Map<?, ?> map0 = map;
        final Map<?, EntryProcessor<K, V, Object>> invokeMap0 = (Map<K, EntryProcessor<K, V, Object>>)invokeMap;

        if (log.isDebugEnabled())
            log.debug("Called putAllAsync(...) [tx=" + this + ", map=" + map0 + ", retval=" + retval + "]");

        assert map0 != null || invokeMap0 != null;

        final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

        if (F.isEmpty(map0) && F.isEmpty(invokeMap0)) {
            if (implicit())
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }

            return new GridFinishedFuture<>(ret.success(true));
        }

        try {
            Set<?> keySet = map0 != null ? map0.keySet() : invokeMap0.keySet();

            final Collection<KeyCacheObject> enlisted = new ArrayList<>(keySet.size());

            final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            final IgniteInternalFuture<Void> loadFut = enlistWrite(
                cacheCtx,
                entryTopVer,
                keySet,
                opCtx != null ? opCtx.expiry() : null,
                map0,
                invokeMap0,
                invokeArgs,
                retval,
                false,
                CU.filterArray(null),
                ret,
                enlisted,
                drMap,
                null,
                opCtx != null && opCtx.skipStore(),
                false,
                keepBinary,
                opCtx != null && opCtx.recovery(),
                dataCenterId);

            try {
                loadFut.get();
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture(e);
            }

            long timeout = remainingTime();

            if (timeout == -1)
                return new GridFinishedFuture<>(timeoutException());

            if (isRollbackOnly())
                return new GridFinishedFuture<>(rollbackException());

            if (pessimistic()) {
                if (log.isDebugEnabled())
                    log.debug("Before acquiring transaction lock for put on keys: " + enlisted);

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(enlisted,
                    timeout,
                    this,
                    /*read*/invokeMap != null, // Needed to force load from store.
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override public GridCacheReturn postLock(
                        GridCacheReturn ret
                    ) throws IgniteCheckedException {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for put on keys: " + enlisted);

                        postLockWrite(cacheCtx,
                            enlisted,
                            ret,
                            /*remove*/false,
                            retval,
                            /*read*/false,
                            -1L,
                            CU.filterArray(null),
                            /*computeInvoke*/true);

                        return ret;
                    }
                };

                if (fut.isDone()) {
                    try {
                        return nonInterruptable(plc1.apply(fut.get(), null));
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return nonInterruptable(plc1.apply(false, e));
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return nonInterruptable(new GridEmbeddedFuture<>(
                        fut,
                        plc1
                    ));
                }
            }
            else
                return optimisticPutFuture(cacheCtx, loadFut, ret, keepBinary);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param cacheKey Key to enlist.
     * @param val Value.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param entryProcessor Entry processor (for invoke operation).
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Flag indicating whether a value should be returned.
     * @param lockOnly If {@code true}, then entry will be enlisted as noop.
     * @param filter User filters.
     * @param ret Return value.
     * @param skipStore Skip store flag.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @param recovery Recovery flag.
     * @param dataCenterId Optional data center Id.
     * @return Future for entry values loading.
     */
    private <K, V> IgniteInternalFuture<Void> enlistWrite(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        KeyCacheObject cacheKey,
        Object val,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable EntryProcessor<K, V, Object> entryProcessor,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        boolean lockOnly,
        final CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        boolean skipStore,
        final boolean singleRmv,
        boolean keepBinary,
        boolean recovery,
        Byte dataCenterId) {
        try (TraceSurroundings ignored2 =
                 MTC.support(context().kernalContext().tracing().create(TX_NEAR_ENLIST_WRITE, MTC.span()))) {
            GridFutureAdapter<Void> enlistFut = new GridFutureAdapter<>();

            try {
                if (!updateLockFuture(null, enlistFut))
                    return finishFuture(enlistFut, timedOut() ? timeoutException() : rollbackException(), false);

                addActiveCache(cacheCtx, recovery);

                final boolean hasFilters = !F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter);
                final boolean needVal = singleRmv || retval || hasFilters;
                final boolean needReadVer = needVal && (serializable() && optimistic());

                if (entryProcessor != null)
                    transform = true;

                GridCacheVersion drVer = dataCenterId != null ? cacheCtx.cache().nextVersion(dataCenterId) : null;

                boolean loadMissed = enlistWriteEntry(cacheCtx,
                    entryTopVer,
                    cacheKey,
                    val,
                    entryProcessor,
                    invokeArgs,
                    expiryPlc,
                    retval,
                    lockOnly,
                    filter,
                    /*drVer*/drVer,
                    /*drTtl*/-1L,
                    /*drExpireTime*/-1L,
                    ret,
                    /*enlisted*/null,
                    skipStore,
                    singleRmv,
                    hasFilters,
                    needVal,
                    needReadVer,
                    keepBinary,
                    recovery);

                if (loadMissed) {
                    AffinityTopologyVersion topVer = topologyVersionSnapshot();

                    if (topVer == null)
                        topVer = entryTopVer;

                    IgniteInternalFuture<Void> loadFut = loadMissing(cacheCtx,
                        topVer != null ? topVer : topologyVersion(),
                        Collections.singleton(cacheKey),
                        filter,
                        ret,
                        needReadVer,
                        singleRmv,
                        hasFilters,
                        /*read through*/(entryProcessor != null || cacheCtx.config().isLoadPreviousValue()) && !skipStore,
                        retval,
                        keepBinary,
                        recovery,
                        expiryPlc);

                    loadFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                        @Override public void apply(IgniteInternalFuture<Void> fut) {
                            try {
                                fut.get();

                                finishFuture(enlistFut, null, true);
                            }
                            catch (IgniteCheckedException e) {
                                finishFuture(enlistFut, e, true);
                            }
                        }
                    });

                    return enlistFut;
                }

                finishFuture(enlistFut, null, true);

                return enlistFut;
            }
            catch (IgniteCheckedException e) {
                return finishFuture(enlistFut, e, true);
            }
        }
    }

    /**
     * Internal routine for <tt>putAll(..)</tt>
     *
     * @param cacheCtx Cache context.
     * @param keys Keys to enlist.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param lookup Value lookup map ({@code null} for remove).
     * @param invokeMap Map with entry processors for invoke operation.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Flag indicating whether a value should be returned.
     * @param lockOnly If {@code true}, then entry will be enlisted as noop.
     * @param filter User filters.
     * @param ret Return value.
     * @param enlisted Collection of keys enlisted into this transaction.
     * @param drPutMap DR put map (optional).
     * @param drRmvMap DR remove map (optional).
     * @param skipStore Skip store flag.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @param keepBinary Keep binary flag.
     * @param recovery Recovery flag.
     * @param dataCenterId Optional data center ID.
     * @return Future for enlisting writes.
     */
    private <K, V> IgniteInternalFuture<Void> enlistWrite(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<?> keys,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Map<?, ?> lookup,
        @Nullable Map<?, EntryProcessor<K, V, Object>> invokeMap,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        boolean lockOnly,
        final CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        Collection<KeyCacheObject> enlisted,
        @Nullable Map<KeyCacheObject, GridCacheDrInfo> drPutMap,
        @Nullable Map<KeyCacheObject, GridCacheVersion> drRmvMap,
        boolean skipStore,
        final boolean singleRmv,
        final boolean keepBinary,
        final boolean recovery,
        Byte dataCenterId
    ) {
        assert retval || invokeMap == null;

        try (TraceSurroundings ignored2 =
                 MTC.support(context().kernalContext().tracing().create(TX_NEAR_ENLIST_WRITE, MTC.span()))) {
            GridFutureAdapter<Void> enlistFut = new GridFutureAdapter<>();

            if (!updateLockFuture(null, enlistFut))
                return finishFuture(enlistFut, timedOut() ? timeoutException() : rollbackException(), false);

            try {
                addActiveCache(cacheCtx, recovery);
            }
            catch (IgniteCheckedException e) {
                return finishFuture(enlistFut, e, false);
            }

            boolean rmv = lookup == null && invokeMap == null;

            final boolean hasFilters = !F.isEmptyOrNulls(filter) && !F.isAlwaysTrue(filter);
            final boolean needVal = singleRmv || retval || hasFilters;
            final boolean needReadVer = needVal && (serializable() && optimistic());

            try {
                // Set transform flag for transaction.
                if (invokeMap != null)
                    transform = true;

                Set<KeyCacheObject> missedForLoad = null;

                for (Object key : keys) {
                    if (isRollbackOnly())
                        return finishFuture(enlistFut, timedOut() ? timeoutException() : rollbackException(), false);

                    if (key == null) {
                        rollback();

                        throw new NullPointerException("Null key.");
                    }

                    Object val = rmv || lookup == null ? null : lookup.get(key);
                    EntryProcessor entryProcessor = invokeMap == null ? null : invokeMap.get(key);

                    GridCacheVersion drVer;
                    long drTtl;
                    long drExpireTime;

                    if (drPutMap != null) {
                        GridCacheDrInfo info = drPutMap.get(key);

                        assert info != null;

                        drVer = info.version();
                        drTtl = info.ttl();
                        drExpireTime = info.expireTime();
                    }
                    else if (drRmvMap != null) {
                        assert drRmvMap.get(key) != null;

                        drVer = drRmvMap.get(key);
                        drTtl = -1L;
                        drExpireTime = -1L;
                    }
                    else if (dataCenterId != null) {
                        drVer = cacheCtx.cache().nextVersion(dataCenterId);
                        drTtl = -1L;
                        drExpireTime = -1L;
                    }
                    else {
                        drVer = null;
                        drTtl = -1L;
                        drExpireTime = -1L;
                    }

                    if (!rmv && val == null && entryProcessor == null) {
                        setRollbackOnly();

                        throw new NullPointerException("Null value.");
                    }

                    KeyCacheObject cacheKey = cacheCtx.toCacheKeyObject(key);

                    boolean loadMissed = enlistWriteEntry(cacheCtx,
                        entryTopVer,
                        cacheKey,
                        val,
                        entryProcessor,
                        invokeArgs,
                        expiryPlc,
                        retval,
                        lockOnly,
                        filter,
                        drVer,
                        drTtl,
                        drExpireTime,
                        ret,
                        enlisted,
                        skipStore,
                        singleRmv,
                        hasFilters,
                        needVal,
                        needReadVer,
                        keepBinary,
                        recovery);

                    if (loadMissed) {
                        if (missedForLoad == null)
                            missedForLoad = new HashSet<>();

                        missedForLoad.add(cacheKey);
                    }
                }

                if (missedForLoad != null) {
                    AffinityTopologyVersion topVer = topologyVersionSnapshot();

                    if (topVer == null)
                        topVer = entryTopVer;

                    IgniteInternalFuture<Void> loadFut = loadMissing(cacheCtx,
                        topVer != null ? topVer : topologyVersion(),
                        missedForLoad,
                        filter,
                        ret,
                        needReadVer,
                        singleRmv,
                        hasFilters,
                        /*read through*/(invokeMap != null || cacheCtx.config().isLoadPreviousValue()) && !skipStore,
                        retval,
                        keepBinary,
                        recovery,
                        expiryPlc);

                    loadFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                        @Override public void apply(IgniteInternalFuture<Void> fut) {
                            try {
                                fut.get();

                                finishFuture(enlistFut, null, true);
                            }
                            catch (IgniteCheckedException e) {
                                finishFuture(enlistFut, e, true);
                            }
                        }
                    });

                    return enlistFut;
                }

                return finishFuture(enlistFut, null, true);
            }
            catch (IgniteCheckedException e) {
                return finishFuture(enlistFut, e, true);
            }
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param cacheKey Key.
     * @param val Value.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param retval Return value flag.
     * @param lockOnly Lock only flag.
     * @param filter Filter.
     * @param drVer DR version.
     * @param drTtl DR ttl.
     * @param drExpireTime DR expire time.
     * @param ret Return value.
     * @param enlisted Enlisted keys collection.
     * @param skipStore Skip store flag.
     * @param singleRmv {@code True} for single remove operation.
     * @param hasFilters {@code True} if filters not empty.
     * @param needVal {@code True} if value is needed.
     * @param needReadVer {@code True} if need read entry version.
     * @return {@code True} if entry value should be loaded.
     * @throws IgniteCheckedException If failed.
     */
    private boolean enlistWriteEntry(GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        final KeyCacheObject cacheKey,
        @Nullable final Object val,
        @Nullable final EntryProcessor<?, ?, ?> entryProcessor,
        @Nullable final Object[] invokeArgs,
        @Nullable final ExpiryPolicy expiryPlc,
        final boolean retval,
        final boolean lockOnly,
        final CacheEntryPredicate[] filter,
        final GridCacheVersion drVer,
        final long drTtl,
        long drExpireTime,
        final GridCacheReturn ret,
        @Nullable final Collection<KeyCacheObject> enlisted,
        boolean skipStore,
        boolean singleRmv,
        boolean hasFilters,
        final boolean needVal,
        boolean needReadVer,
        boolean keepBinary,
        boolean recovery
    ) throws IgniteCheckedException {
        boolean loadMissed = false;

        final boolean rmv = val == null && entryProcessor == null;

        IgniteTxKey txKey = cacheCtx.txKey(cacheKey);

        IgniteTxEntry txEntry = entry(txKey);

        // First time access.
        if (txEntry == null) {
            while (true) {
                GridCacheEntryEx entry = entryEx(cacheCtx, txKey, entryTopVer != null ? entryTopVer : topologyVersion());

                try {
                    entry.unswap(false);

                    // Check if lock is being explicitly acquired by the same thread.
                    if (!implicit && cctx.kernalContext().config().isCacheSanityCheckEnabled() &&
                        entry.lockedByThread(threadId, xidVer)) {
                        throw new IgniteCheckedException("Cannot access key within transaction if lock is " +
                            "externally held [key=" + CU.value(cacheKey, cacheCtx, false) +
                            ", entry=" + entry +
                            ", xidVer=" + xidVer +
                            ", threadId=" + threadId +
                            ", locNodeId=" + cctx.localNodeId() + ']');
                    }

                    CacheObject old = null;
                    GridCacheVersion readVer = null;

                    if (optimistic() && !implicit()) {
                        try {
                            if (needReadVer) {
                                if (primaryLocal(entry)) {
                                    cctx.database().checkpointReadLock();

                                    try {
                                        EntryGetResult res = entry.innerGetVersioned(
                                            null,
                                            this,
                                            /*metrics*/retval,
                                            /*events*/retval,
                                            entryProcessor,
                                            resolveTaskName(),
                                            null,
                                            keepBinary,
                                            null);

                                        if (res != null) {
                                            old = res.value();
                                            readVer = res.version();
                                        }
                                    }
                                    finally {
                                        cctx.database().checkpointReadUnlock();
                                    }
                                }
                            }
                            else {
                                cctx.database().checkpointReadLock();

                                try {
                                    old = entry.innerGet(
                                        null,
                                        this,
                                        /*read through*/false,
                                        /*metrics*/retval,
                                        /*events*/retval,
                                        entryProcessor,
                                        resolveTaskName(),
                                        null,
                                        keepBinary);
                                }
                                finally {
                                    cctx.database().checkpointReadUnlock();
                                }
                            }
                        }
                        catch (ClusterTopologyCheckedException e) {
                            entry.touch();

                            throw e;
                        }
                    }
                    else
                        old = entry.rawGet();

                    final GridCacheOperation op = lockOnly ? NOOP : rmv ? DELETE :
                        entryProcessor != null ? TRANSFORM : old != null ? UPDATE : CREATE;

                    if (old != null && hasFilters && !filter(entry.context(), cacheKey, old, filter)) {
                        ret.set(
                            cacheCtx,
                            old,
                            false,
                            keepBinary,
                            U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId)
                        );

                        if (!readCommitted()) {
                            if (optimistic() && serializable()) {
                                txEntry = addEntry(op,
                                    old,
                                    entryProcessor,
                                    invokeArgs,
                                    entry,
                                    expiryPlc,
                                    filter,
                                    true,
                                    drTtl,
                                    drExpireTime,
                                    drVer,
                                    skipStore,
                                    keepBinary,
                                    CU.isNearEnabled(cacheCtx));
                            }
                            else {
                                txEntry = addEntry(READ,
                                    old,
                                    null,
                                    null,
                                    entry,
                                    null,
                                    CU.empty0(),
                                    false,
                                    -1L,
                                    -1L,
                                    null,
                                    skipStore,
                                    keepBinary,
                                    CU.isNearEnabled(cacheCtx));
                            }

                            txEntry.markValid();

                            if (needReadVer) {
                                assert readVer != null;

                                txEntry.entryReadVersion(singleRmv ? SER_READ_NOT_EMPTY_VER : readVer);
                            }
                        }

                        if (readCommitted())
                            entry.touch();

                        break; // While.
                    }

                    CacheObject cVal = cacheCtx.toCacheObject(val);

                    if (op == CREATE || op == UPDATE)
                        cacheCtx.validateKeyAndValue(cacheKey, cVal);

                    txEntry = addEntry(op,
                        cVal,
                        entryProcessor,
                        invokeArgs,
                        entry,
                        expiryPlc,
                        filter,
                        true,
                        drTtl,
                        drExpireTime,
                        drVer,
                        skipStore,
                        keepBinary,
                        CU.isNearEnabled(cacheCtx));

                    if (op == TRANSFORM && txEntry.value() == null && old != null)
                        txEntry.value(cacheCtx.toCacheObject(old), false, false);

                    if (enlisted != null)
                        enlisted.add(cacheKey);

                    if (!pessimistic() && !implicit()) {
                        txEntry.markValid();

                        if (old == null) {
                            if (needVal)
                                loadMissed = true;
                            else {
                                assert !implicit() || !transform : this;
                                assert txEntry.op() != TRANSFORM : txEntry;

                                if (retval) {
                                    ret.set(
                                        cacheCtx,
                                        null,
                                        true,
                                        keepBinary,
                                        U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId)
                                    );
                                }
                                else
                                    ret.success(true);
                            }
                        }
                        else {
                            if (needReadVer) {
                                assert readVer != null;

                                txEntry.entryReadVersion(singleRmv ? SER_READ_NOT_EMPTY_VER : readVer);
                            }

                            if (retval && !transform)
                                ret.set(cacheCtx, old, true, keepBinary, U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                            else {
                                if (txEntry.op() == TRANSFORM) {
                                    GridCacheVersion ver;

                                    try {
                                        ver = entry.version();
                                    }
                                    catch (GridCacheEntryRemovedException ex) {
                                        assert optimistic() : txEntry;

                                        if (log.isDebugEnabled())
                                            log.debug("Failed to get entry version " +
                                                "[err=" + ex.getMessage() + ']');

                                        ver = null;
                                    }

                                    addInvokeResult(txEntry, old, ret, ver);
                                }
                                else
                                    ret.success(true);
                            }
                        }
                    }
                    // Pessimistic.
                    else {
                        if (retval && !transform)
                            ret.set(cacheCtx, old, true, keepBinary, U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                        else
                            ret.success(true);
                    }

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in transaction putAll0 method: " + entry);
                }
            }
        }
        else {
            if (entryProcessor == null && txEntry.op() == TRANSFORM)
                throw new IgniteCheckedException("Failed to enlist write value for key (cannot have update value in " +
                    "transaction after EntryProcessor is applied): " + CU.value(cacheKey, cacheCtx, false));

            GridCacheEntryEx entry = txEntry.cached();

            CacheObject v = txEntry.value();

            boolean del = txEntry.op() == DELETE && rmv;

            if (!del) {
                if (hasFilters && !filter(entry.context(), cacheKey, v, filter)) {
                    ret.set(cacheCtx, v, false, keepBinary, U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));

                    return loadMissed;
                }

                GridCacheOperation op = rmv ? DELETE : entryProcessor != null ? TRANSFORM :
                    v != null ? UPDATE : CREATE;

                CacheObject cVal = cacheCtx.toCacheObject(val);

                if (op == CREATE || op == UPDATE)
                    cacheCtx.validateKeyAndValue(cacheKey, cVal);

                txEntry = addEntry(op,
                    cVal,
                    entryProcessor,
                    invokeArgs,
                    entry,
                    expiryPlc,
                    filter,
                    true,
                    drTtl,
                    drExpireTime,
                    drVer,
                    skipStore,
                    keepBinary,
                    CU.isNearEnabled(cacheCtx));

                if (enlisted != null)
                    enlisted.add(cacheKey);

                if (txEntry.op() == TRANSFORM) {
                    GridCacheVersion ver;

                    try {
                        ver = entry.version();
                    }
                    catch (GridCacheEntryRemovedException e) {
                        assert optimistic() : txEntry;

                        if (log.isDebugEnabled())
                            log.debug("Failed to get entry version: [msg=" + e.getMessage() + ']');

                        ver = null;
                    }

                    addInvokeResult(txEntry, txEntry.value(), ret, ver);
                }
            }

            if (!pessimistic()) {
                txEntry.markValid();

                if (retval && !transform)
                    ret.set(cacheCtx, v, true, keepBinary, U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                else
                    ret.success(true);
            }
        }

        return loadMissed;
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param drMap DR map.
     * @param retval Flag indicating whether a value should be returned.
     * @param filter Filter.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @return Future for asynchronous remove.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        @Nullable final Collection<? extends K> keys,
        @Nullable Map<KeyCacheObject, GridCacheVersion> drMap,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter,
        boolean singleRmv) {
        if (cacheCtx.mvccEnabled())
            return mvccRemoveAllAsync0(cacheCtx, keys, retval, filter);

        try {
            checkUpdatesAllowed(cacheCtx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        cacheCtx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        if (retval)
            needReturnValue(true);

        final Collection<?> keys0;

        if (drMap != null) {
            assert keys == null;

            keys0 = drMap.keySet();
        }
        else
            keys0 = keys;

        CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

        final Byte dataCenterId;

        if (opCtx != null && opCtx.hasDataCenterId()) {
            assert drMap == null : drMap;

            dataCenterId = opCtx.dataCenterId();
        }
        else
            dataCenterId = null;

        assert keys0 != null;

        if (log.isDebugEnabled())
            log.debug(S.toString("Called removeAllAsync(...)",
                "tx", this, false,
                "keys", keys0, true,
                "implicit", implicit, false,
                "retval", retval, false));

        try {
            checkValid();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        final GridCacheReturn ret = new GridCacheReturn(localResult(), false);

        if (F.isEmpty(keys0)) {
            if (implicit()) {
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }
            }

            return new GridFinishedFuture<>(ret.success(true));
        }

        init();

        final Collection<KeyCacheObject> enlisted = new ArrayList<>();

        ExpiryPolicy plc;

        final CacheEntryPredicate[] filters = CU.filterArray(filter);

        if (!F.isEmpty(filters))
            plc = opCtx != null ? opCtx.expiry() : null;
        else
            plc = null;

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        final IgniteInternalFuture<Void> loadFut = enlistWrite(
            cacheCtx,
            entryTopVer,
            keys0,
            plc,
            /*lookup map*/null,
            /*invoke map*/null,
            /*invoke arguments*/null,
            retval,
            /*lock only*/false,
            filters,
            ret,
            enlisted,
            null,
            drMap,
            opCtx != null && opCtx.skipStore(),
            singleRmv,
            keepBinary,
            opCtx != null && opCtx.recovery(),
            dataCenterId
        );

        try {
            loadFut.get();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        long timeout = remainingTime();

        if (timeout == -1)
            return new GridFinishedFuture<>(timeoutException());

        if (isRollbackOnly())
            return new GridFinishedFuture<>(rollbackException());

        if (log.isDebugEnabled())
            log.debug("Remove keys: " + enlisted);

        // Acquire locks only after having added operation to the write set.
        // Otherwise, during rollback we will not know whether locks need
        // to be rolled back.
        if (pessimistic()) {
            if (log.isDebugEnabled())
                log.debug("Before acquiring transaction lock for remove on keys: " + enlisted);

            IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(enlisted,
                timeout,
                this,
                false,
                retval,
                isolation,
                isInvalidate(),
                -1L,
                -1L);

            PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                /** {@inheritDoc} */
                @Override protected GridCacheReturn postLock(GridCacheReturn ret)
                    throws IgniteCheckedException {
                    if (log.isDebugEnabled())
                        log.debug("Acquired transaction lock for remove on keys: " + enlisted);

                    postLockWrite(cacheCtx,
                        enlisted,
                        ret,
                        /*remove*/true,
                        retval,
                        /*read*/false,
                        -1L,
                        filters,
                        /*computeInvoke*/false);

                    return ret;
                }
            };

            if (fut.isDone()) {
                try {
                    return nonInterruptable(plc1.apply(fut.get(), null));
                }
                catch (GridClosureException e) {
                    return new GridFinishedFuture<>(e.unwrap());
                }
                catch (IgniteCheckedException e) {
                    try {
                        return nonInterruptable(plc1.apply(false, e));
                    }
                    catch (Exception e1) {
                        return new GridFinishedFuture<>(e1);
                    }
                }
            }
            else
                return nonInterruptable(new GridEmbeddedFuture<>(
                    fut,
                    plc1
                ));
        }
        else {
            if (implicit()) {
                // Should never load missing values for implicit transaction as values will be returned
                // with prepare response, if required.
                assert loadFut.isDone();

                return nonInterruptable(commitNearTxLocalAsync().chain(
                    new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn>() {
                        @Override public GridCacheReturn applyx(IgniteInternalFuture<IgniteInternalTx> txFut)
                            throws IgniteCheckedException {
                            try {
                                txFut.get();

                                return new GridCacheReturn(
                                    cacheCtx,
                                    true,
                                    keepBinary,
                                    U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId),
                                    implicitRes.value(),
                                    implicitRes.success()
                                );
                            }
                            catch (IgniteCheckedException | RuntimeException e) {
                                rollbackNearTxLocalAsync();

                                throw e;
                            }
                        }
                    }
                ));
            }
            else {
                return nonInterruptable(loadFut.chain(new CX1<IgniteInternalFuture<Void>, GridCacheReturn>() {
                    @Override public GridCacheReturn applyx(IgniteInternalFuture<Void> f)
                        throws IgniteCheckedException {
                        f.get();

                        return ret;
                    }
                }));
            }
        }
    }

    /**
     * Internal method for remove operations in Mvcc mode.
     *
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param retval Flag indicating whether a value should be returned.
     * @param filter Filter.
     * @return Future for asynchronous remove.
     */
    @SuppressWarnings("unchecked")
    private <K, V> IgniteInternalFuture<GridCacheReturn> mvccRemoveAllAsync0(
        final GridCacheContext cacheCtx,
        @Nullable final Collection<? extends K> keys,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter
    ) {
        try {
            MvccUtils.requestSnapshot(this);

            beforeRemove(cacheCtx, retval, true);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        if (F.isEmpty(keys)) {
            if (implicit()) {
                try {
                    commit();
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }
            }

            return new GridFinishedFuture<>(new GridCacheReturn(localResult(), true));
        }

        init();

        Set<KeyCacheObject> enlisted = new HashSet<>(keys.size());

        try {
            for (Object key : keys) {
                if (isRollbackOnly())
                    return new GridFinishedFuture<>(timedOut() ? timeoutException() : rollbackException());

                if (key == null) {
                    rollback();

                    throw new NullPointerException("Null key.");
                }

                KeyCacheObject cacheKey = cacheCtx.toCacheKeyObject(key);

                enlisted.add(cacheKey);
            }

        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }

        return updateAsync(cacheCtx, new UpdateSourceIterator<KeyCacheObject>() {

            private final Iterator<KeyCacheObject> it = enlisted.iterator();

            @Override public EnlistOperation operation() {
                return EnlistOperation.DELETE;
            }

            @Override public boolean hasNextX() throws IgniteCheckedException {
                return it.hasNext();
            }

            @Override public KeyCacheObject nextX() throws IgniteCheckedException {
                return it.next();
            }
        }, retval, filter, remainingTime(), true);
    }

    /**
     * @param cacheCtx Cache context.
     * @param cacheIds Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Timeout.
     * @return Operation future.
     */
    public IgniteInternalFuture<Long> updateAsync(GridCacheContext cacheCtx,
        int[] cacheIds, int[] parts, String schema, String qry, Object[] params,
        int flags, int pageSize, long timeout) {
        try {
            beforePut(cacheCtx, false, true);

            return updateAsync(new GridNearTxQueryEnlistFuture(
                cacheCtx,
                this,
                cacheIds,
                parts,
                schema,
                qry,
                params,
                flags,
                pageSize,
                timeout));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param it Entries iterator.
     * @param pageSize Page size.
     * @param timeout Timeout.
     * @param sequential Sequential locking flag.
     * @return Operation future.
     */
    public IgniteInternalFuture<Long> updateAsync(GridCacheContext cacheCtx,
        UpdateSourceIterator<?> it, int pageSize, long timeout, boolean sequential) {
        try {
            beforePut(cacheCtx, false, true);

            return updateAsync(new GridNearTxQueryResultsEnlistFuture(cacheCtx, this,
                timeout, it, pageSize, sequential));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture(e);
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * Executes key-value update operation in Mvcc mode.
     *
     * @param cacheCtx Cache context.
     * @param it Entries iterator.
     * @param retval Return value flag.
     * @param filter Filter.
     * @param timeout Timeout.
     * @param sequential Sequential locking flag.
     * @return Operation future.
     */
    private IgniteInternalFuture<GridCacheReturn> updateAsync(GridCacheContext cacheCtx,
        UpdateSourceIterator<?> it,
        boolean retval,
        @Nullable CacheEntryPredicate filter,
        long timeout,
        boolean sequential) {
        try {
            final CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            /* TODO: IGNITE-9688: 'sequential' is always true here which can slowdown bulk operations,
             but possibly we can safely optimize this. */

            GridNearTxEnlistFuture fut = new GridNearTxEnlistFuture(cacheCtx, this,
                timeout, it, 0, sequential, filter, retval, keepBinary);

            fut.init();

            return nonInterruptable(new GridEmbeddedFuture<>(fut.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, Boolean>() {
                @Override public Boolean applyx(IgniteInternalFuture<GridCacheReturn> fut0) throws IgniteCheckedException {
                    fut0.get();

                    return true;
                }
            }), new PLC1<GridCacheReturn>(null) {
                @Override protected GridCacheReturn postLock(GridCacheReturn ret) throws IgniteCheckedException {
                    GridCacheReturn futRes = fut.get();

                    assert futRes != null;

                    mvccSnapshot.incrementOperationCounter();

                    Object val = futRes.value();

                    if (futRes.invokeResult() && val != null) {
                        assert val instanceof Map;

                        val = cacheCtx.unwrapInvokeResult((Map)val, keepBinary);
                    }

                    return new GridCacheReturn(
                        cacheCtx,
                        true,
                        keepBinary,
                        U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId),
                        val,
                        futRes.success()
                    );
                }
            }));
        }
        catch (RuntimeException e) {
            onException();

            throw e;
        }
    }

    /**
     * Executes update query operation in Mvcc mode.
     *
     * @param fut Enlist future.
     * @return Operation future.
     */
    private IgniteInternalFuture<Long> updateAsync(GridNearTxQueryAbstractEnlistFuture fut) {
        try {
            fut.init();

            return nonInterruptable(new GridEmbeddedFuture<>(fut.chain(new CX1<IgniteInternalFuture<Long>, Boolean>() {
                @Override public Boolean applyx(IgniteInternalFuture<Long> fut0) throws IgniteCheckedException {
                    return fut0.get() != null;
                }
            }), new PLC1<Long>(null) {
                @Override protected Long postLock(Long val) throws IgniteCheckedException {
                    Long res = fut.get();

                    assert mvccSnapshot != null;
                    assert res != null;

                    if (res > 0) {
                        if (mvccSnapshot.operationCounter() == MvccUtils.MVCC_READ_OP_CNTR) {
                            throw new IgniteCheckedException("The maximum limit of the number of statements allowed in" +
                                " one transaction is reached. [max=" + mvccSnapshot.operationCounter() + ']');
                        }

                        mvccSnapshot.incrementOperationCounter();
                    }

                    return res;
                }
            }));
        }
        finally {
            cctx.tm().resetContext();
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to get.
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects
     * @param skipStore Skip store flag.
     * @param readRepairStrategy Read Repair strategy.
     * @return Future for this get.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalFuture<Map<K, V>> getAllAsync(
        final GridCacheContext cacheCtx,
        @Nullable final AffinityTopologyVersion entryTopVer,
        final Collection<KeyCacheObject> keys,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore,
        final boolean recovery,
        final ReadRepairStrategy readRepairStrategy,
        final boolean needVer) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (cacheCtx.mvccEnabled() && !isOperationAllowed(true))
            return txTypeMismatchFinishFuture();

        init();

        int keysCnt = keys.size();

        boolean single = keysCnt == 1;

        try {
            checkValid();

            GridFutureAdapter<?> enlistFut = new GridFutureAdapter<>();

            if (!updateLockFuture(null, enlistFut))
                return new GridFinishedFuture<>(timedOut() ? timeoutException() : rollbackException());

            final Map<K, V> retMap = new GridLeanMap<>(keysCnt);

            final Map<KeyCacheObject, GridCacheVersion> missed = new GridLeanMap<>(pessimistic() ? keysCnt : 0);

            CacheOperationContext opCtx = cacheCtx.operationContextPerCall();

            ExpiryPolicy expiryPlc = opCtx != null ? opCtx.expiry() : null;

            final Collection<KeyCacheObject> lockKeys;

            try {
                lockKeys = enlistRead(cacheCtx,
                    entryTopVer,
                    keys,
                    expiryPlc,
                    retMap,
                    missed,
                    keysCnt,
                    deserializeBinary,
                    skipVals,
                    keepCacheObjects,
                    skipStore,
                    recovery,
                    readRepairStrategy,
                    needVer);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
            finally {
                finishFuture(enlistFut, null, true);
            }

            if (isRollbackOnly())
                return new GridFinishedFuture<>(timedOut() ? timeoutException() : rollbackException());

            if (single && missed.isEmpty())
                return new GridFinishedFuture<>(retMap);

            // Handle locks.
            if (pessimistic() && !readCommitted() && !skipVals) {
                if (expiryPlc == null)
                    expiryPlc = cacheCtx.expiry();

                long accessTtl = expiryPlc != null ? CU.toTtl(expiryPlc.getExpiryForAccess()) : CU.TTL_NOT_CHANGED;
                long createTtl = expiryPlc != null ? CU.toTtl(expiryPlc.getExpiryForCreation()) : CU.TTL_NOT_CHANGED;

                long timeout = remainingTime();

                if (timeout == -1)
                    return new GridFinishedFuture<>(timeoutException());

                IgniteInternalFuture<Boolean> fut = cacheCtx.cache().txLockAsync(lockKeys,
                    timeout,
                    this,
                    true,
                    true,
                    isolation,
                    isInvalidate(),
                    createTtl,
                    accessTtl);

                final ExpiryPolicy expiryPlc0 = expiryPlc;

                PLC2<Map<K, V>> plc2 = new PLC2<Map<K, V>>() {
                    @Override public IgniteInternalFuture<Map<K, V>> postLock() throws IgniteCheckedException {
                        if (log.isDebugEnabled())
                            log.debug("Acquired transaction lock for read on keys: " + lockKeys);

                        // Load keys only after the locks have been acquired.
                        for (KeyCacheObject cacheKey : lockKeys) {
                            K keyVal = (K)
                                (keepCacheObjects ? cacheKey :
                                    cacheCtx.cacheObjectContext().unwrapBinaryIfNeeded(cacheKey, !deserializeBinary,
                                        true, null));

                            if (retMap.containsKey(keyVal))
                                // We already have a return value.
                                continue;

                            IgniteTxKey txKey = cacheCtx.txKey(cacheKey);

                            IgniteTxEntry txEntry = entry(txKey);

                            assert txEntry != null;

                            // Check if there is cached value.
                            while (true) {
                                GridCacheEntryEx cached = txEntry.cached();

                                CacheObject val = null;
                                GridCacheVersion readVer = null;
                                EntryGetResult getRes = null;

                                try {
                                    Object transformClo =
                                        (!F.isEmpty(txEntry.entryProcessors()) &&
                                            cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                            F.first(txEntry.entryProcessors()) : null;

                                    if (needVer) {
                                        getRes = cached.innerGetVersioned(
                                            null,
                                            GridNearTxLocal.this,
                                            /*update-metrics*/true,
                                            /*event*/!skipVals,
                                            transformClo,
                                            resolveTaskName(),
                                            null,
                                            txEntry.keepBinary(),
                                            null);

                                        if (getRes != null) {
                                            val = getRes.value();
                                            readVer = getRes.version();
                                        }
                                    }
                                    else {
                                        val = cached.innerGet(
                                            null,
                                            GridNearTxLocal.this,
                                            /*read through*/false,
                                            /*metrics*/true,
                                            /*events*/!skipVals,
                                            transformClo,
                                            resolveTaskName(),
                                            null,
                                            txEntry.keepBinary());
                                    }

                                    // If value is in cache and passed the filter.
                                    if (val != null) {
                                        missed.remove(cacheKey);

                                        txEntry.setAndMarkValid(val);

                                        if (!F.isEmpty(txEntry.entryProcessors()))
                                            val = txEntry.applyEntryProcessors(val);

                                        cacheCtx.addResult(retMap,
                                            cacheKey,
                                            val,
                                            skipVals,
                                            keepCacheObjects,
                                            deserializeBinary,
                                            false,
                                            getRes,
                                            readVer,
                                            0,
                                            0,
                                            needVer,
                                            U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));

                                        if (readVer != null)
                                            txEntry.entryReadVersion(readVer);
                                    }

                                    // Even though we bring the value back from lock acquisition,
                                    // we still need to recheck primary node for consistent values
                                    // in case of concurrent transactional locks.

                                    break; // While.
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed exception in get postLock (will retry): " +
                                            cached);

                                    txEntry.cached(entryEx(cacheCtx, txKey, topologyVersion()));
                                }
                            }
                        }

                        if (readRepairStrategy != null) { // Checking and repairing each locked entry (if necessary).
                            // Providing the guarantee that all copies are updated when read repair operation is finished.
                            syncMode(FULL_SYNC);

                            return new GridNearReadRepairFuture(
                                topVer != null ? topVer : topologyVersion(),
                                cacheCtx,
                                keys,
                                readRepairStrategy,
                                !skipStore,
                                taskName,
                                deserializeBinary,
                                recovery,
                                cacheCtx.cache().expiryPolicy(expiryPlc0),
                                GridNearTxLocal.this)
                                .init()
                                .chain(
                                    (fut) -> {
                                        try {
                                            // For every repaired entry.
                                            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.get().entrySet()) {
                                                EntryGetResult getRes = entry.getValue();
                                                KeyCacheObject key = entry.getKey();

                                                enlistWrite( // Fixing inconsistency on commit.
                                                    cacheCtx,
                                                    entryTopVer,
                                                    key,
                                                    getRes != null ? getRes.value() : null,
                                                    expiryPlc0,
                                                    null,
                                                    null,
                                                    false,
                                                    false,
                                                    null,
                                                    null,
                                                    skipStore,
                                                    false,
                                                    !deserializeBinary,
                                                    recovery,
                                                    null);

                                                // Rewriting repaired, initially filled by explicit lock operation.
                                                if (getRes != null)
                                                    cacheCtx.addResult(retMap,
                                                        key,
                                                        getRes.value(),
                                                        skipVals,
                                                        keepCacheObjects,
                                                        deserializeBinary,
                                                        false,
                                                        null,
                                                        getRes.version(),
                                                        0,
                                                        0,
                                                        needVer,
                                                        U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                                                else
                                                    retMap.remove(keepCacheObjects ? key :
                                                        cacheCtx.cacheObjectContext().unwrapBinaryIfNeeded(
                                                            key, !deserializeBinary, false,
                                                            U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId)));
                                            }

                                            return Collections.emptyMap();
                                        }
                                        catch (Exception e) {
                                            throw new GridClosureException(e);
                                        }
                                    }
                                );
                        }

                        return new GridFinishedFuture<>(Collections.emptyMap());
                    }
                };

                FinishClosure<Map<K, V>> finClos = new FinishClosure<Map<K, V>>() {
                    @Override Map<K, V> finish(Map<K, V> loaded) {
                        retMap.putAll(loaded);

                        return retMap;
                    }
                };

                if (fut.isDone()) {
                    try {
                        IgniteInternalFuture<Map<K, V>> fut1 = plc2.apply(fut.get(), null);

                        return nonInterruptable(fut1.isDone() ?
                            new GridFinishedFuture<>(finClos.apply(fut1.get(), null)) :
                            new GridEmbeddedFuture<>(finClos, fut1));
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return nonInterruptable(plc2.apply(false, e));
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return nonInterruptable(new GridEmbeddedFuture<>(
                        fut,
                        plc2,
                        finClos));
                }
            }
            else {
                assert optimistic() || readCommitted() || skipVals;

                if (!missed.isEmpty()) {
                    if (!readCommitted())
                        for (Iterator<KeyCacheObject> it = missed.keySet().iterator(); it.hasNext(); ) {
                            KeyCacheObject cacheKey = it.next();

                            K keyVal = (K)(keepCacheObjects ? cacheKey
                                : cacheCtx.cacheObjectContext()
                                .unwrapBinaryIfNeeded(cacheKey, !deserializeBinary, false, null));

                            if (retMap.containsKey(keyVal))
                                it.remove();
                        }

                    if (missed.isEmpty())
                        return new GridFinishedFuture<>(retMap);

                    AffinityTopologyVersion topVer = topologyVersionSnapshot();

                    if (topVer == null)
                        topVer = entryTopVer;

                    return checkMissed(cacheCtx,
                        topVer != null ? topVer : topologyVersion(),
                        retMap,
                        missed,
                        deserializeBinary,
                        skipVals,
                        keepCacheObjects,
                        skipStore,
                        recovery,
                        readRepairStrategy,
                        needVer,
                        expiryPlc);
                }

                return new GridFinishedFuture<>(retMap);
            }
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Key to enlist.
     * @param expiryPlc Explicitly specified expiry policy for entry.
     * @param map Return map.
     * @param missed Map of missed keys.
     * @param keysCnt Keys count (to avoid call to {@code Collection.size()}).
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param skipStore Skip store flag.
     * @param recovery Recovery flag..
     * @return Enlisted keys.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private <K, V> Collection<KeyCacheObject> enlistRead(
        final GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<KeyCacheObject> keys,
        @Nullable ExpiryPolicy expiryPlc,
        Map<K, V> map,
        Map<KeyCacheObject, GridCacheVersion> missed,
        int keysCnt,
        boolean deserializeBinary,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean skipStore,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy,
        final boolean needVer
    ) throws IgniteCheckedException {
        assert !F.isEmpty(keys);
        assert keysCnt == keys.size();

        try (TraceSurroundings ignored2 =
                 MTC.support(context().kernalContext().tracing().create(TX_NEAR_ENLIST_READ, MTC.span()))) {
            cacheCtx.checkSecurity(SecurityPermission.CACHE_READ);

            boolean single = keysCnt == 1;

            Collection<KeyCacheObject> lockKeys = null;

            AffinityTopologyVersion topVer = entryTopVer != null ? entryTopVer : topologyVersion();

            boolean needReadVer = (serializable() && optimistic()) || needVer;

            // In this loop we cover only read-committed or optimistic transactions.
            // Transactions that are pessimistic and not read-committed are covered
            // outside of this loop.
            for (KeyCacheObject key : keys) {
                if (isRollbackOnly())
                    throw timedOut() ? timeoutException() : rollbackException();

                if ((pessimistic() || needReadVer) && !readCommitted() && !skipVals)
                    addActiveCache(cacheCtx, recovery);

                IgniteTxKey txKey = cacheCtx.txKey(key);

                // Check write map (always check writes first).
                IgniteTxEntry txEntry = entry(txKey);

                // Either non-read-committed or there was a previous write.
                if (txEntry != null) {
                    CacheObject val = txEntry.value();

                    if (txEntry.hasValue()) {
                        if (!F.isEmpty(txEntry.entryProcessors()))
                            val = txEntry.applyEntryProcessors(val);

                        if (val != null) {
                            GridCacheVersion ver = null;

                            if (needVer) {
                                if (txEntry.op() != READ)
                                    ver = IgniteTxEntry.GET_ENTRY_INVALID_VER_UPDATED;
                                else {
                                    ver = txEntry.entryReadVersion();

                                    if (ver == null && pessimistic()) {
                                        while (true) {
                                            try {
                                                GridCacheEntryEx cached = txEntry.cached();

                                                ver = cached.isNear() ?
                                                    ((GridNearCacheEntry)cached).dhtVersion() : cached.version();

                                                break;
                                            }
                                            catch (GridCacheEntryRemovedException ignored) {
                                                txEntry.cached(entryEx(cacheCtx, txEntry.txKey(), topVer));
                                            }
                                        }
                                    }

                                    if (ver == null) {
                                        assert optimistic() && repeatableRead() : this;

                                        ver = IgniteTxEntry.GET_ENTRY_INVALID_VER_AFTER_GET;
                                    }
                                }

                                assert ver != null;
                            }

                            cacheCtx.addResult(map, key, val, skipVals, keepCacheObjects, deserializeBinary, false,
                                ver, 0, 0, U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                        }
                    }
                    else {
                        assert txEntry.op() == TRANSFORM;

                        while (true) {
                            try {
                                GridCacheVersion readVer = null;
                                EntryGetResult getRes = null;

                                Object transformClo =
                                    (txEntry.op() == TRANSFORM &&
                                        cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ)) ?
                                        F.first(txEntry.entryProcessors()) : null;

                                if (needVer) {
                                    getRes = txEntry.cached().innerGetVersioned(
                                        null,
                                        this,
                                        /*update-metrics*/true,
                                        /*event*/!skipVals,
                                        transformClo,
                                        resolveTaskName(),
                                        null,
                                        txEntry.keepBinary(),
                                        null);

                                    if (getRes != null) {
                                        val = getRes.value();
                                        readVer = getRes.version();
                                    }
                                }
                                else {
                                    val = txEntry.cached().innerGet(
                                        null,
                                        this,
                                        /*read-through*/false,
                                        /*metrics*/true,
                                        /*event*/!skipVals,
                                        transformClo,
                                        resolveTaskName(),
                                        null,
                                        txEntry.keepBinary());
                                }

                                if (val != null) {
                                    if (!readCommitted() && !skipVals)
                                        txEntry.readValue(val);

                                    if (!F.isEmpty(txEntry.entryProcessors()))
                                        val = txEntry.applyEntryProcessors(val);

                                    cacheCtx.addResult(map,
                                        key,
                                        val,
                                        skipVals,
                                        keepCacheObjects,
                                        deserializeBinary,
                                        false,
                                        getRes,
                                        readVer,
                                        0,
                                        0,
                                        needVer,
                                        U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                                }
                                else
                                    missed.put(key, txEntry.cached().version());

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                txEntry.cached(entryEx(cacheCtx, txEntry.txKey(), topVer));
                            }
                        }
                    }
                }
                // First time access within transaction.
                else {
                    if (lockKeys == null && !skipVals)
                        lockKeys = single ? Collections.singleton(key) : new ArrayList<KeyCacheObject>(keysCnt);

                    if (!single && !skipVals)
                        lockKeys.add(key);

                    while (true) {
                        GridCacheEntryEx entry = entryEx(cacheCtx, txKey, topVer);

                        try {
                            GridCacheVersion ver = entry.version();

                            CacheObject val = null;
                            GridCacheVersion readVer = null;
                            EntryGetResult getRes = null;

                            if ((!pessimistic() || (readCommitted() && !skipVals)) &&
                                readRepairStrategy == null) { // Read Repair must avoid local reads.
                                IgniteCacheExpiryPolicy accessPlc =
                                    optimistic() ? accessPolicy(cacheCtx, txKey, expiryPlc) : null;

                                if (needReadVer) {
                                    getRes = primaryLocal(entry) ?
                                        entry.innerGetVersioned(
                                            null,
                                            this,
                                            /*metrics*/true,
                                            /*event*/true,
                                            null,
                                            resolveTaskName(),
                                            accessPlc,
                                            !deserializeBinary,
                                            null) : null;

                                    if (getRes != null) {
                                        val = getRes.value();
                                        readVer = getRes.version();
                                    }
                                }
                                else {
                                    val = entry.innerGet(
                                        null,
                                        this,
                                        /*read-through*/false,
                                        /*metrics*/true,
                                        /*event*/!skipVals,
                                        null,
                                        resolveTaskName(),
                                        accessPlc,
                                        !deserializeBinary);
                                }

                                if (val != null) {
                                    cacheCtx.addResult(map,
                                        key,
                                        val,
                                        skipVals,
                                        keepCacheObjects,
                                        deserializeBinary,
                                        false,
                                        getRes,
                                        readVer,
                                        0,
                                        0,
                                        needVer,
                                        U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                                }
                                else
                                    missed.put(key, ver);
                            }
                            else
                                // We must wait for the lock in pessimistic mode.
                                missed.put(key, ver);

                            if (!readCommitted() && !skipVals) {
                                txEntry = addEntry(READ,
                                    val,
                                    null,
                                    null,
                                    entry,
                                    expiryPlc,
                                    null,
                                    true,
                                    -1L,
                                    -1L,
                                    null,
                                    skipStore,
                                    !deserializeBinary,
                                    CU.isNearEnabled(cacheCtx));

                                // As optimization, mark as checked immediately
                                // for non-pessimistic if value is not null.
                                if (val != null && !pessimistic()) {
                                    txEntry.markValid();

                                    if (needReadVer) {
                                        assert readVer != null;

                                        txEntry.entryReadVersion(readVer);
                                    }
                                }
                            }

                            break; // While.
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry in transaction getAllAsync(..) (will retry): " + key);
                        }
                        finally {
                            if (entry != null && readCommitted()) {
                                if (cacheCtx.isNear()) {
                                    if (cacheCtx.affinity().partitionBelongs(cacheCtx.localNode(), entry.partition(), topVer)) {
                                        if (entry.markObsolete(xidVer))
                                            cacheCtx.cache().removeEntry(entry);
                                    }
                                }
                                else
                                    entry.touch();
                            }
                        }
                    }
                }
            }

            return lockKeys != null ? lockKeys : Collections.<KeyCacheObject>emptyList();
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to load.
     * @param filter Filter.
     * @param ret Return value.
     * @param needReadVer Read version flag.
     * @param singleRmv {@code True} for single remove operation.
     * @param hasFilters {@code True} if filters not empty.
     * @param readThrough Read through flag.
     * @param retval Return value flag.
     * @param expiryPlc Expiry policy.
     * @return Load future.
     */
    private IgniteInternalFuture<Void> loadMissing(
        final GridCacheContext cacheCtx,
        final AffinityTopologyVersion topVer,
        final Set<KeyCacheObject> keys,
        final CacheEntryPredicate[] filter,
        final GridCacheReturn ret,
        final boolean needReadVer,
        final boolean singleRmv,
        final boolean hasFilters,
        final boolean readThrough,
        final boolean retval,
        final boolean keepBinary,
        final boolean recovery,
        final ExpiryPolicy expiryPlc) {
        GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c =
            new GridInClosure3<KeyCacheObject, Object, GridCacheVersion>() {
                @Override public void apply(KeyCacheObject key,
                    @Nullable Object val,
                    @Nullable GridCacheVersion loadVer) {
                    if (log.isDebugEnabled())
                        log.debug("Loaded value from remote node [key=" + key + ", val=" + val + ']');

                    IgniteTxEntry e = entry(new IgniteTxKey(key, cacheCtx.cacheId()));

                    assert e != null;

                    if (needReadVer) {
                        assert loadVer != null;

                        e.entryReadVersion(singleRmv && val != null ? SER_READ_NOT_EMPTY_VER : loadVer);
                    }

                    if (singleRmv) {
                        assert !hasFilters && !retval;
                        assert val == null || Boolean.TRUE.equals(val) : val;

                        ret.set(cacheCtx, null, val != null, keepBinary, U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                    }
                    else {
                        CacheObject cacheVal = cacheCtx.toCacheObject(val);

                        if (e.op() == TRANSFORM) {
                            GridCacheVersion ver;

                            e.readValue(cacheVal);

                            try {
                                ver = e.cached().version();
                            }
                            catch (GridCacheEntryRemovedException ex) {
                                assert optimistic() : e;

                                if (log.isDebugEnabled())
                                    log.debug("Failed to get entry version: [msg=" + ex.getMessage() + ']');

                                ver = null;
                            }

                            addInvokeResult(e, cacheVal, ret, ver);
                        }
                        else {
                            boolean success;

                            if (hasFilters) {
                                success = isAll(e.context(), key, cacheVal, filter);

                                if (!success) {
                                    e.value(cacheVal, false, false);

                                    e.op(READ);
                                }
                            }
                            else
                                success = true;

                            ret.set(
                                cacheCtx,
                                cacheVal,
                                success,
                                keepBinary,
                                U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId)
                            );
                        }
                    }
                }
            };

        return loadMissing(
            cacheCtx,
            topVer,
            readThrough,
            /*async*/true,
            keys,
            /*skipVals*/singleRmv,
            needReadVer,
            keepBinary,
            recovery,
            null,
            expiryPlc,
            c);
    }

    /**
     * @return Finished future with error message about tx type mismatch.
     */
    private static IgniteInternalFuture txTypeMismatchFinishFuture() {
        return new GridFinishedFuture(new IgniteCheckedException(TX_TYPE_MISMATCH_ERR_MSG));
    }

    /**
     * @param cacheCtx Cache context.
     * @param loadFut Missing keys load future.
     * @param ret Future result.
     * @param keepBinary Keep binary flag.
     * @return Future.
     */
    private IgniteInternalFuture optimisticPutFuture(
        final GridCacheContext cacheCtx,
        IgniteInternalFuture<Void> loadFut,
        final GridCacheReturn ret,
        final boolean keepBinary
    ) {
        if (implicit()) {
            // Should never load missing values for implicit transaction as values will be returned
            // with prepare response, if required.
            assert loadFut.isDone();

            return nonInterruptable(commitNearTxLocalAsync().chain(
                new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn>() {
                    @Override public GridCacheReturn applyx(IgniteInternalFuture<IgniteInternalTx> txFut)
                        throws IgniteCheckedException {
                        try {
                            txFut.get();

                            Object res = implicitRes.value();

                            if (implicitRes.invokeResult()) {
                                assert res == null || res instanceof Map : implicitRes;

                                res = cacheCtx.unwrapInvokeResult((Map)res, keepBinary);
                            }

                            return new GridCacheReturn(
                                cacheCtx,
                                true,
                                keepBinary,
                                U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId),
                                res,
                                implicitRes.success()
                            );
                        }
                        catch (IgniteCheckedException | RuntimeException e) {
                            if (!(e instanceof NodeStoppingException))
                                rollbackNearTxLocalAsync();

                            throw e;
                        }
                    }
                }
            ));
        }
        else {
            return nonInterruptable(loadFut.chain(new CX1<IgniteInternalFuture<Void>, GridCacheReturn>() {
                @Override public GridCacheReturn applyx(IgniteInternalFuture<Void> f) throws IgniteCheckedException {
                    f.get();

                    return ret;
                }
            }));
        }
    }

    /**
     *
     */
    private void onException() {
        for (IgniteTxEntry txEntry : allEntries()) {
            GridCacheEntryEx cached0 = txEntry.cached();

            if (cached0 != null)
                cached0.touch();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> optimisticLockEntries() {
        assert false : "Should not be called";

        throw new UnsupportedOperationException();
    }

    /**
     * @param cacheCtx Cache context.
     * @param readThrough Read through flag.
     * @param async if {@code True}, then loading will happen in a separate thread.
     * @param keys Keys.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} version is required for loaded values.
     * @param c Closure to be applied for loaded values.
     * @param readRepairStrategy Read Repair strategy.
     * @param expiryPlc Expiry policy.
     * @return Future with {@code True} value if loading took place.
     */
    private IgniteInternalFuture<Void> loadMissing(
        final GridCacheContext cacheCtx,
        AffinityTopologyVersion topVer,
        boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        final boolean skipVals,
        final boolean needVer,
        boolean keepBinary,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy,
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
                needVer || !cacheCtx.config().isReadFromBackup() || (optimistic() && serializable() && readThrough),
                /*deserializeBinary*/false,
                recovery,
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
        // cacheCtx.isColocated() == true
        else {
            if (readRepairStrategy != null) {
                return new GridNearReadRepairCheckOnlyFuture(
                    topVer,
                    cacheCtx,
                    keys,
                    readRepairStrategy,
                    readThrough,
                    taskName,
                    !keepBinary,
                    recovery,
                    expiryPlc0,
                    skipVals,
                    needVer,
                    true,
                    this)
                    .multi()
                    .chain((fut) -> {
                        try {
                            Map<Object, Object> map = fut.get();

                            processLoaded(map, keys, needVer, c);

                            return null;
                        }
                        catch (IgniteConsistencyViolationException e) {
                            for (KeyCacheObject key : keys)
                                txState().removeEntry(cacheCtx.txKey(key)); // Will be recreated after repair.

                            throw new GridClosureException(e);
                        }
                        catch (Exception e) {
                            setRollbackOnly();

                            throw new GridClosureException(e);
                        }
                    });
            }

            if (keys.size() == 1) {
                final KeyCacheObject key = F.first(keys);

                return cacheCtx.colocated().loadAsync(
                    key,
                    readThrough,
                    needVer || !cacheCtx.config().isReadFromBackup() || (optimistic() && serializable() && readThrough),
                    topVer,
                    resolveTaskName(),
                    /*deserializeBinary*/false,
                    expiryPlc0,
                    skipVals,
                    needVer,
                    /*keepCacheObject*/true,
                    recovery,
                    null,
                    label()
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
                    needVer || !cacheCtx.config().isReadFromBackup() || (optimistic() && serializable() && readThrough),
                    topVer,
                    resolveTaskName(),
                    /*deserializeBinary*/false,
                    recovery,
                    expiryPlc0,
                    skipVals,
                    needVer,
                    /*keepCacheObject*/true,
                    label(),
                    null
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
                EntryGetResult getRes = (EntryGetResult)val;

                v = getRes.value();
                ver = getRes.version();
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
    public IgniteTxMappings mappings() {
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

    /** {@inheritDoc} */
    @Override public boolean queryEnlisted() {
        if (!txState.mvccEnabled())
            return false;
        else if (qryEnlisted)
            return true;
        else if (mappings.single())
            return !mappings.empty() && mappings.singleMapping().queryUpdate();
        else
            return mappings.mappings().stream().anyMatch(GridDistributedTxMapping::queryUpdate);
    }

    /**
     * Requests version on coordinator.
     *
     * @return Future to wait for result.
     */
    public IgniteInternalFuture<MvccSnapshot> requestSnapshot() {
        if (isRollbackOnly())
            return new GridFinishedFuture<>(rollbackException());

        MvccSnapshot mvccSnapshot0 = mvccSnapshot;

        if (mvccSnapshot0 != null)
            return new GridFinishedFuture<>(mvccSnapshot0);

        MvccProcessor prc = cctx.coordinators();

        MvccCoordinator crd = prc.currentCoordinator();

        synchronized (this) {
            this.crdVer = crd.version();
        }

        if (crd.local())
            mvccSnapshot0 = prc.requestWriteSnapshotLocal();

        if (mvccSnapshot0 == null) {
            MvccSnapshotFuture fut = new MvccTxSnapshotFuture();

            prc.requestWriteSnapshotAsync(crd, fut);

            return fut;
        }

        GridFutureAdapter<MvccSnapshot> fut = new GridFutureAdapter<>();

        onResponse0(mvccSnapshot0, fut);

        return fut;
    }

    /** */
    private synchronized void onResponse0(MvccSnapshot res, GridFutureAdapter<MvccSnapshot> fut) {
        assert mvccSnapshot == null;

        if (state() != ACTIVE) {
            // The transaction were concurrently rolled back.
            // We need to notify the coordinator about that.
            assert isRollbackOnly();

            cctx.coordinators().ackTxRollback(res);

            fut.onDone(timedOut() ? timeoutException() : rollbackException());
        }
        else if (crdVer != res.coordinatorVersion()) {
            setRollbackOnly();

            fut.onDone(new IgniteTxRollbackCheckedException(
                "Mvcc coordinator has been changed during request. " +
                "Please retry on a stable topology."));
        }
        else
            fut.onDone(mvccSnapshot = res);
    }

    /** {@inheritDoc} */
    @Override public synchronized long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        if (isDone // Already finished.
            || crdVer == 0 // Mvcc snapshot has not been requested yet or it's an non-mvcc transaction.
            || newCrd.version() == crdVer) // Acceptable operations reordering.
            return MVCC_TRACKER_ID_NA;

        crdVer = newCrd.version();

        if (mvccSnapshot == null)
            return MVCC_TRACKER_ID_NA;

        if (qryId == MVCC_TRACKER_ID_NA) {
            long qryId0 = qryId = ID_CNTR.incrementAndGet();

            finishFuture().listen(f -> cctx.coordinators().ackQueryDone(mvccSnapshot, qryId0));
        }

        return qryId;
    }

    /** {@inheritDoc} */
    @Override public void mvccSnapshot(MvccSnapshot mvccSnapshot) {
        throw new UnsupportedOperationException();
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
     * Suspends transaction. It could be resumed later. Supported only for optimistic transactions.
     *
     * @throws IgniteCheckedException If the transaction is in an incorrect state, or timed out.
     */
    public void suspend() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Suspend near local tx: " + this);

        if (threadId() != Thread.currentThread().getId())
            throw new IgniteCheckedException("Only thread started transaction can suspend it.");

        synchronized (this) {
            checkValid();

            cctx.tm().suspendTx(this);
        }
    }

    /**
     * Resumes transaction (possibly in another thread) if it was previously suspended.
     *
     * @throws IgniteCheckedException If the transaction is in an incorrect state, or timed out.
     */
    public void resume() throws IgniteCheckedException {
        resume(true, Thread.currentThread().getId());
    }

    /**
     * Resumes transaction (possibly in another thread) if it was previously suspended.
     *
     * @param checkTimeout Whether timeout should be checked.
     * @param threadId Thread id to restore.
     * @throws IgniteCheckedException If the transaction is in an incorrect state, or timed out.
     */
    private void resume(boolean checkTimeout, long threadId) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Resume near local tx: " + this);

        synchronized (this) {
            checkValid(checkTimeout);

            cctx.tm().resumeTx(this, threadId);
        }
    }

    /**
     * @param maps Mappings.
     */
    void addEntryMapping(@Nullable Collection<GridDistributedTxMapping> maps) {
        if (!F.isEmpty(maps)) {
            for (GridDistributedTxMapping map : maps) {
                ClusterNode primary = map.primary();

                GridDistributedTxMapping m = mappings.get(primary.id());

                if (m == null) {
                    mappings.put(m = new GridDistributedTxMapping(primary));

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
        ClusterNode n = map.primary();

        GridDistributedTxMapping m = new GridDistributedTxMapping(n);

        mappings.put(m);

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
        GridCacheVersionedFuture<IgniteInternalTx> fut = (GridCacheVersionedFuture<IgniteInternalTx>)prepFut;

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
        Collection<GridCacheVersion> rolledbackVers
    ) {
        assert mapping.hasNearCacheEntries() : mapping;

        // Process writes, then reads.
        for (IgniteTxEntry txEntry : mapping.entries()) {
            if (CU.WRITE_FILTER_NEAR.apply(txEntry))
                readyNearLock(txEntry, mapping.dhtVersion(), pendingVers, committedVers, rolledbackVers);
        }

        for (IgniteTxEntry txEntry : mapping.entries()) {
            if (CU.READ_FILTER_NEAR.apply(txEntry))
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
    private void readyNearLock(IgniteTxEntry txEntry,
        GridCacheVersion dhtVer,
        Collection<GridCacheVersion> pendingVers,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers
    ) {
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
    @Override public boolean localFinish(boolean commit, boolean clearThreadMap) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Finishing near local tx [tx=" + this + ", commit=" + commit + "]");

        if (commit) {
            if (!state(COMMITTING)) {
                TransactionState state = state();

                if (state != COMMITTING && state != COMMITTED)
                    throw isRollbackOnly() ? timedOut() ? timeoutException() : rollbackException() :
                        new IgniteCheckedException("Invalid transaction state for commit [state=" + state() +
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
                userRollback(clearThreadMap);
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

    /**
     * Returns current amount of time that transaction has spent on system activities (acquiring locks, commiting,
     * rolling back, etc.)
     *
     * @return Amount of time in milliseconds.
     */
    public long systemTimeCurrent() {
        long systemTime0 = systemTime.get();

        long systemStartTime0 = systemStartTime.get();

        long t = systemStartTime0 == 0 ? 0 : (System.nanoTime() - systemStartTime0);

        return U.nanosToMillis(systemTime0 + t);
    }

    /** {@inheritDoc} */
    @Override public boolean state(TransactionState state) {
        boolean res = super.state(state);

        if (state == COMMITTED || state == ROLLED_BACK) {
            leaveSystemSection();

            // If commitOrRollbackTime != 0 it means that we already have written metrics and dumped it in log at least once.
            if (!commitOrRollbackTime.compareAndSet(0, System.nanoTime() - commitOrRollbackStartTime.get()))
                return res;

            long systemTimeMillis = U.nanosToMillis(this.systemTime.get());
            long totalTimeMillis = System.currentTimeMillis() - startTime();

            // In some cases totalTimeMillis can be less than systemTimeMillis, as they are calculated with different precision.
            long userTimeMillis = Math.max(totalTimeMillis - systemTimeMillis, 0);

            cctx.txMetrics().onNearTxComplete(systemTimeMillis, userTimeMillis);

            boolean willBeSkipped = txDumpsThrottling == null || txDumpsThrottling.skipCurrent();

            if (!willBeSkipped) {
                long transactionTimeDumpThreshold = cctx.tm().longTransactionTimeDumpThreshold();

                double transactionTimeDumpSamplesCoefficient = cctx.tm().transactionTimeDumpSamplesCoefficient();

                boolean isLong = transactionTimeDumpThreshold > 0 && totalTimeMillis > transactionTimeDumpThreshold;

                boolean randomlyChosen = transactionTimeDumpSamplesCoefficient > 0.0
                    && ThreadLocalRandom.current().nextDouble() <= transactionTimeDumpSamplesCoefficient;

                if (randomlyChosen || isLong) {
                    String txDump = completedTransactionDump(state, systemTimeMillis, userTimeMillis, isLong);

                    if (isLong)
                        log.warning(txDump);
                    else
                        log.info(txDump);

                    txDumpsThrottling.dump();
                }
            }
            else if (txDumpsThrottling != null)
                txDumpsThrottling.skip();
        }

        return res;
    }

    /**
     * Builds dump string for completed transaction.
     *
     * @param state Transaction state.
     * @param systemTimeMillis System time in milliseconds.
     * @param userTimeMillis User time in milliseconds.
     * @param isLong Whether the dumped transaction is long running or not.
     * @return Dump string.
     */
    private String completedTransactionDump(
        TransactionState state,
        long systemTimeMillis,
        long userTimeMillis,
        boolean isLong
    ) {
        long cacheOperationsTimeMillis =
            U.nanosToMillis(systemTime.get() - prepareTime.get() - commitOrRollbackTime.get());

        GridStringBuilder warning = new GridStringBuilder(isLong ? "Long transaction time dump " : "Transaction time dump ")
            .a("[startTime=")
            .a(IgniteUtils.DEBUG_DATE_FMT.format(Instant.ofEpochMilli(startTime)))
            .a(", totalTime=")
            .a(systemTimeMillis + userTimeMillis)
            .a(", systemTime=")
            .a(systemTimeMillis)
            .a(", userTime=")
            .a(userTimeMillis)
            .a(", cacheOperationsTime=")
            .a(cacheOperationsTimeMillis);

        if (state == COMMITTED) {
            warning
                .a(", prepareTime=")
                .a(timeMillis(prepareTime))
                .a(", commitTime=")
                .a(timeMillis(commitOrRollbackTime));
        }
        else {
            warning
                .a(", rollbackTime=")
                .a(timeMillis(commitOrRollbackTime));
        }

        warning
            .a(", tx=")
            .a(this)
            .a("]");

        return warning.toString();
    }

    /**
     * @return Tx prepare future.
     */
    public IgniteInternalFuture<?> prepareNearTxLocal() {
        enterSystemSection();

        // We assume that prepare start time should be set only once for the transaction.
        prepareStartTime.compareAndSet(0, System.nanoTime());

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

            if (trackTimeout) {
                prepFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        GridNearTxLocal.this.removeTimeoutHandler();
                    }
                });
            }

            if (timeout == -1) {
                fut.onDone(this, timeoutException());

                return fut;
            }
        }
        else
            // Prepare was called explicitly.
            return fut;

        mapExplicitLocks();

        if (cctx.kernalContext().deploy().enabled() && deploymentLdrId != null)
            U.restoreDeploymentContext(cctx.kernalContext(), deploymentLdrId);

        fut.prepare();

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> salvageTx() {
        assert false : "Should not be called for GridNearTxLocal";

        return null;
    }

    /**
     * @param awaitLastFuture If true - method will wait until transaction finish every action started before.
     * @throws IgniteCheckedException If failed.
     */
    public final void prepare(boolean awaitLastFuture) throws IgniteCheckedException {
        if (awaitLastFuture)
            txState().awaitLastFuture(cctx);

        prepareNearTxLocal().get();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void commit() throws IgniteCheckedException {
        commitNearTxLocalAsync().get();
    }

    /**
     * @return Finish future.
     */
    public IgniteInternalFuture<IgniteInternalTx> commitNearTxLocalAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing near local tx: " + this);

        final NearTxFinishFuture fut;
        final NearTxFinishFuture fut0 = finishFut;

        boolean fastFinish;

        if (fut0 != null || !FINISH_FUT_UPD.compareAndSet(this, null, fut = finishFuture(fastFinish = fastFinish(), true)))
            return chainFinishFuture(finishFut, true, true, false);

        if (!fastFinish) {
            IgniteInternalFuture<?> prepareFut;
            try {
                prepareFut = prepareNearTxLocal();
            }
            catch (Throwable t) {
                prepareFut = prepFut;

                // Properly finish prepFut in case of unchecked error.
                assert prepareFut != null; // Prep future must be set.

                ((GridNearTxPrepareFutureAdapter)prepFut).onDone(t);
            }

            prepareFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    // These values should not be changed after set once.
                    prepareTime.compareAndSet(0, System.nanoTime() - prepareStartTime.get());

                    commitOrRollbackStartTime.compareAndSet(0, System.nanoTime());

                    try {
                        // Make sure that here are no exceptions.
                        f.get();

                        fut.finish(true, true, false);
                    }
                    catch (Error | RuntimeException e) {
                        COMMIT_ERR_UPD.compareAndSet(GridNearTxLocal.this, null, e);

                        fut.finish(false, true, false);

                        throw e;
                    }
                    catch (IgniteCheckedException e) {
                        COMMIT_ERR_UPD.compareAndSet(GridNearTxLocal.this, null, e);

                        if (!(e instanceof NodeStoppingException))
                            fut.finish(false, true, true);
                        else
                            fut.onNodeStop(e);
                    }
                }
            });
        }
        else
            fut.finish(true, false, false);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        return commitNearTxLocalAsync();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void rollback() throws IgniteCheckedException {
        rollbackNearTxLocalAsync().get();
    }

    /**
     * @return Rollback future.
     */
    public IgniteInternalFuture<IgniteInternalTx> rollbackNearTxLocalAsync() {
        return rollbackNearTxLocalAsync(true, false);
    }

    /**
     * @param clearThreadMap {@code True} if needed to clear thread map.
     * @param onTimeout {@code True} if called from timeout handler.
     * Note: For async rollbacks (from timeouts or another thread) should not clear thread map
     * since thread started tx still should be able to see this tx to prevent subsequent operations.
     *
     * @return Rollback future.
     */
    public IgniteInternalFuture<IgniteInternalTx> rollbackNearTxLocalAsync(final boolean clearThreadMap,
        final boolean onTimeout) {
        if (log.isDebugEnabled())
            log.debug("Rolling back near tx: " + this);

        enterSystemSection();

        // This value should not be changed after set once.
        commitOrRollbackStartTime.compareAndSet(0, System.nanoTime());

        if (!onTimeout && trackTimeout)
            removeTimeoutHandler();

        IgniteInternalFuture<?> prepFut = this.prepFut;

        if (onTimeout && prepFut instanceof GridNearTxPrepareFutureAdapter && !prepFut.isDone())
            ((GridNearTxPrepareFutureAdapter)prepFut).onNearTxLocalTimeout();

        final NearTxFinishFuture fut;
        final NearTxFinishFuture fut0 = finishFut;

        boolean fastFinish;

        if (fut0 != null)
            return chainFinishFuture(finishFut, false, clearThreadMap, onTimeout);

        if (!FINISH_FUT_UPD.compareAndSet(this, null, fut = finishFuture(fastFinish = clearThreadMap && fastFinish(), false)))
            return chainFinishFuture(finishFut, false, clearThreadMap, onTimeout);

        rollbackFuture(fut);

        if (!fastFinish) {
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

                fut.finish(false, clearThreadMap, onTimeout);
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

                        fut.finish(false, clearThreadMap, onTimeout);
                    }
                });
            }
        }
        else
            fut.finish(false, true, onTimeout);

        return fut;
    }

    /**
     * Finish transaction.
     *
     * @param fast {@code True} in case of fast finish.
     * @param commit {@code True} if commit.
     * @return Transaction commit future.
     */
    private NearTxFinishFuture finishFuture(boolean fast, boolean commit) {
        NearTxFinishFuture fut = fast ? new GridNearTxFastFinishFuture(this, commit) :
            new GridNearTxFinishFuture<>(cctx, this, commit);

        return mvccSnapshot != null ? new GridNearTxFinishAndAckFuture(fut) : fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        return rollbackNearTxLocalAsync();
    }

    /**
     * @param fut Already started finish future.
     * @param commit Commit flag.
     * @param clearThreadMap Clear thread map.
     * @return Finish future.
     */
    private IgniteInternalFuture<IgniteInternalTx> chainFinishFuture(final NearTxFinishFuture fut, final boolean commit,
        final boolean clearThreadMap, final boolean onTimeout) {
        assert fut != null;

        if (fut.commit() != commit) {
            final GridNearTxLocal tx = this;

            if (!commit) {
                final GridNearTxFinishFuture rollbackFut = new GridNearTxFinishFuture<>(cctx, this, false);

                fut.listen(new IgniteInClosure<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut0) {
                        if (FINISH_FUT_UPD.compareAndSet(tx, fut, rollbackFut)) {
                            switch (tx.state()) {
                                case COMMITTED:
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to rollback, transaction is already committed: " + tx);

                                    // Fall-through.

                                case ROLLED_BACK:
                                    rollbackFut.forceFinish();

                                    assert rollbackFut.isDone() : rollbackFut;

                                    break;

                                default: // First finish attempt was unsuccessful. Try again.
                                    rollbackFut.finish(false, clearThreadMap, onTimeout);
                            }
                        }
                        else {
                            finishFut.listen(new IgniteInClosure<IgniteInternalFuture<IgniteInternalTx>>() {
                                @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                                    try {
                                        fut.get();

                                        rollbackFut.markInitialized();
                                    }
                                    catch (IgniteCheckedException e) {
                                        rollbackFut.onDone(e);
                                    }
                                }
                            });
                        }
                    }
                });

                return rollbackFut;
            }
            else {
                final GridFutureAdapter<IgniteInternalTx> fut0 = new GridFutureAdapter<>();

                fut.listen(new IgniteInClosure<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                        if (timedOut())
                            fut0.onDone(new IgniteTxTimeoutCheckedException("Failed to commit transaction, " +
                                "transaction is concurrently rolled back on timeout: " + tx));
                        else
                            fut0.onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction, " +
                                "transaction is concurrently rolled back: " + tx));
                    }
                });

                return fut0;
            }
        }

        return fut;
    }

    /**
     * @return {@code True} if 'fast finish' path can be used for transaction completion.
     */
    private boolean fastFinish() {
        return !queryEnlisted() && writeMap().isEmpty()
            && ((optimistic() && !serializable()) || readMap().isEmpty());
    }

    /**
     * Prepares next batch of entries in dht transaction.
     *
     * @param req Prepare request.
     * @return Future that will be completed when locks are acquired.
     */
    public IgniteInternalFuture<GridNearTxPrepareResponse> prepareAsyncLocal(GridNearTxPrepareRequest req) {
        long timeout = remainingTime();

        if (state() != PREPARING) {
            if (timeout == -1)
                return new GridFinishedFuture<>(timeoutException());

            setRollbackOnly();

            return new GridFinishedFuture<>(rollbackException());
        }

        if (timeout == -1)
            return new GridFinishedFuture<>(timeoutException());

        init();

        GridDhtTxPrepareFuture fut = new GridDhtTxPrepareFuture(
            cctx,
            this,
            timeout,
            0,
            Collections.<IgniteTxKey, GridCacheVersion>emptyMap(),
            req.last(),
            needReturnValue() && implicit());

        try {
            userPrepare((serializable() && optimistic()) ? F.concat(false, req.writes(), req.reads()) : req.writes());

            // Make sure to add future before calling prepare on it.
            cctx.mvcc().addFuture(fut);

            if (isSystemInvalidate())
                fut.complete();
            else
                fut.prepare(req);
        }
        catch (IgniteTxTimeoutCheckedException | IgniteTxOptimisticCheckedException e) {
            fut.onError(e);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            fut.onError(new IgniteTxRollbackCheckedException("Failed to prepare transaction: " + this, e));
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

        IgniteInternalFuture<?> prep = prepFut;

        // Do not create finish future if there are no remote nodes.
        if (F.isEmpty(dhtMap) && F.isEmpty(nearMap)) {
            IgniteInternalFuture fut = prep != null ? prep : new GridFinishedFuture<>(this);

            if (fut.isDone())
                cctx.tm().mvccFinish(this);
            else
                fut.listen(f -> cctx.tm().mvccFinish(this));

            return fut;
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
            catch (Throwable t) {
                fut.onDone(t);

                throw t;
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
                    catch (Throwable t) {
                        fut.onDone(t);

                        throw t;
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
        close(true);
    }

    /**
     * @param clearThreadMap Clear thread map.
     */
    public void close(boolean clearThreadMap) throws IgniteCheckedException {
        TransactionState state = state();

        try {
            if (state == COMMITTED || state == ROLLED_BACK)
                return;

            boolean rmv = false;

            if (trackTimeout)
                rmv = removeTimeoutHandler();

            if (state != COMMITTING && state != ROLLING_BACK &&
                (!trackTimeout || rmv || (prepFut != null && prepFut.isDone())))
                rollbackNearTxLocalAsync(clearThreadMap, false).get();

            synchronized (this) {
                try {
                    while (!done())
                        wait();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    if (!done())
                        throw new IgniteCheckedException("Got interrupted while waiting for transaction to complete: " +
                            this, e);
                }
            }
        }
        finally {
            if (clearThreadMap)
                cctx.tm().clearThreadMap(this);

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
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteInternalFuture<?> currentPrepareFuture() {
        return prepFut;
    }

    /**
     * @param topVer New topology version.
     * @param reset {@code True} if need to reset tx state.
     */
    public void onRemap(AffinityTopologyVersion topVer, boolean reset) {
        assert cctx.kernalContext().clientNode();

        if (reset) {
            mapped = false;
            nearLocallyMapped = false;
            colocatedLocallyMapped = false;
            txNodes = null;
            onePhaseCommit = false;
            nearMap.clear();
            dhtMap.clear();
            mappings.clear();
        }

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

    /**
     * @return {@code true} if this transaction does not have type flag set or it matches invoking operation,
     * {@code false} otherwise.
     */
    public boolean isOperationAllowed(boolean mvccOp) {
        if (this.mvccOp == null) {
            this.mvccOp = mvccOp;

            return true;
        }

        return this.mvccOp == mvccOp;
    }

    /**
     * @return Public API proxy.
     */
    public TransactionProxy proxy() {
        if (proxy == null)
            proxy = new TransactionProxyImpl(this, cctx, false);

        return proxy;
    }

    /**
     * @return Public API proxy.
     */
    public TransactionProxy rollbackOnlyProxy() {
        if (rollbackOnlyProxy == null)
            rollbackOnlyProxy = new TransactionProxyRollbackOnlyImpl<>(this, cctx, false);

        return rollbackOnlyProxy;
    }

    /**
     * @param cacheCtx Cache context.
     * @param topVer Topology version.
     * @param map Return map.
     * @param missedMap Missed keys.
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param skipStore Skip store flag.
     * @param readRepairStrategy Read Repair strategy.
     * @param expiryPlc Expiry policy.
     * @return Loaded key-value pairs.
     */
    private <K, V> IgniteInternalFuture<Map<K, V>> checkMissed(
        final GridCacheContext cacheCtx,
        final AffinityTopologyVersion topVer,
        final Map<K, V> map,
        final Map<KeyCacheObject, GridCacheVersion> missedMap,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore,
        final boolean recovery,
        final ReadRepairStrategy readRepairStrategy,
        final boolean needVer,
        final ExpiryPolicy expiryPlc
    ) {
        if (log.isDebugEnabled())
            log.debug("Loading missed values for missed map: " + missedMap);

        final boolean needReadVer = (serializable() && optimistic()) || needVer;

        return new GridEmbeddedFuture<>(
            new C2<Void, Exception, Map<K, V>>() {
                @Override public Map<K, V> apply(Void v, Exception e) {
                    if (e != null)
                        throw new GridClosureException(e);

                    if (isRollbackOnly()) {
                        if (timedOut())
                            throw new GridClosureException(new IgniteTxTimeoutCheckedException(
                                "Transaction has been timed out: " + GridNearTxLocal.this));
                        else
                            throw new GridClosureException(new IgniteTxRollbackCheckedException(
                                "Transaction has been rolled back: " + GridNearTxLocal.this));
                    }

                    return map;
                }
            },
            loadMissing(
                cacheCtx,
                topVer,
                !skipStore,
                false,
                missedMap.keySet(),
                skipVals,
                needReadVer,
                !deserializeBinary,
                recovery,
                readRepairStrategy,
                expiryPlc,
                new GridInClosure3<KeyCacheObject, Object, GridCacheVersion>() {
                    @Override public void apply(KeyCacheObject key, Object val, GridCacheVersion loadVer) {
                        CacheObject cacheVal = cacheCtx.toCacheObject(val);

                        CacheObject visibleVal = cacheVal;

                        IgniteTxKey txKey = cacheCtx.txKey(key);

                        IgniteTxEntry txEntry = entry(txKey);

                        if (txEntry != null) {
                            if (!readCommitted())
                                txEntry.readValue(cacheVal);

                            if (!F.isEmpty(txEntry.entryProcessors()))
                                visibleVal = txEntry.applyEntryProcessors(visibleVal);
                        }

                        assert txEntry != null || readCommitted() || skipVals;

                        GridCacheEntryEx e = txEntry == null ? entryEx(cacheCtx, txKey, topVer) : txEntry.cached();

                        if (readCommitted() || skipVals) {
                            e.touch();

                            if (visibleVal != null) {
                                cacheCtx.addResult(map,
                                    key,
                                    visibleVal,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    false,
                                    needVer ? loadVer : null,
                                    0,
                                    0,
                                    U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                            }
                        }
                        else {
                            assert txEntry != null;

                            txEntry.setAndMarkValid(cacheVal);

                            if (needReadVer) {
                                assert loadVer != null;

                                txEntry.entryReadVersion(loadVer);
                            }

                            if (visibleVal != null) {
                                cacheCtx.addResult(map,
                                    key,
                                    visibleVal,
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    false,
                                    needVer ? loadVer : null,
                                    0,
                                    0,
                                    U.deploymentClassLoader(cctx.kernalContext(), deploymentLdrId));
                            }
                        }
                    }
                })
        );
    }

    /**
     * @param entry Entry.
     * @return {@code True} if local node is current primary for given entry.
     */
    private boolean primaryLocal(GridCacheEntryEx entry) {
        return entry.context().affinity().primaryByPartition(cctx.localNode(), entry.partition(), AffinityTopologyVersion.NONE);
    }

    /**
     * Checks filter for non-pessimistic transactions.
     *
     * @param cctx Cache context.
     * @param key Key.
     * @param val Value.
     * @param filter Filter to check.
     * @return {@code True} if passed or pessimistic.
     */
    private boolean filter(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        CacheEntryPredicate[] filter) {
        return pessimistic() || (optimistic() && implicit()) || isAll(cctx, key, val, filter);
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param filter Filter.
     * @return {@code True} if filter passed.
     */
    private boolean isAll(GridCacheContext cctx,
        KeyCacheObject key,
        final CacheObject val0,
        CacheEntryPredicate[] filter) {
        GridCacheEntryEx e = new GridDhtDetachedCacheEntry(cctx, key) {
            @Nullable @Override public CacheObject peekVisibleValue() {
                return val0;
            }
        };

        for (CacheEntryPredicate p0 : filter) {
            if (p0 != null && !p0.apply(e))
                return false;
        }

        return true;
    }

    /**
     * @param cacheCtx Cache context.
     * @param retval Return value flag.
     * @param mvccOp SQL operation flag.
     * @throws IgniteCheckedException If failed.
     */
    private void beforePut(GridCacheContext cacheCtx, boolean retval, boolean mvccOp) throws IgniteCheckedException {
        assert !mvccOp || cacheCtx.mvccEnabled();

        checkUpdatesAllowed(cacheCtx);

        cacheCtx.checkSecurity(SecurityPermission.CACHE_PUT);

        if (cacheCtx.mvccEnabled() && !isOperationAllowed(mvccOp))
            throw new IgniteCheckedException(TX_TYPE_MISMATCH_ERR_MSG);

        if (retval)
            needReturnValue(true);

        checkValid();

        init();
    }

    /**
     * @param cacheCtx Cache context.
     * @param retval Return value flag.
     * @param mvccOp SQL operation flag.
     * @throws IgniteCheckedException If failed.
     */
    private void beforeRemove(GridCacheContext cacheCtx, boolean retval, boolean mvccOp) throws IgniteCheckedException {
        assert !mvccOp || cacheCtx.mvccEnabled();

        checkUpdatesAllowed(cacheCtx);

        cacheCtx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        if (cacheCtx.mvccEnabled() && !isOperationAllowed(mvccOp))
            throw new IgniteCheckedException(TX_TYPE_MISMATCH_ERR_MSG);

        if (retval)
            needReturnValue(true);

        checkValid();
    }

    /**
     * @param cacheCtx Cache context.
     * @throws IgniteCheckedException If updates are not allowed.
     */
    private void checkUpdatesAllowed(GridCacheContext cacheCtx) throws IgniteCheckedException {
        if (!cacheCtx.updatesAllowed()) {
            throw new IgniteTxRollbackCheckedException(new CacheException(
                "Updates are not allowed for transactional cache: " + cacheCtx.name() + ". Configure " +
                    "persistence store on client or use remote closure execution to start transactions " +
                    "from server nodes."));
        }
    }

    /**
     * @param fut Future.
     * @return Future ignoring interrupts on {@code get()}.
     */
    private static <T> IgniteInternalFuture<T> nonInterruptable(IgniteInternalFuture<T> fut) {
        // Safety.
        if (fut instanceof GridFutureAdapter)
            ((GridFutureAdapter)fut).ignoreInterrupts();

        return fut;
    }

    /**
     * @param threadId new owner of transaction.
     */
    public void threadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * Removes timeout handler.
     *
     * @return {@code True} if handler was removed.
     */
    private boolean removeTimeoutHandler() {
        assert trackTimeout;

        return cctx.time().removeTimeoutObject(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return xid();
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return startTime() + timeout();
    }

    /** {@inheritDoc} */
    @Override public String label() {
        return lb;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        boolean proceed;

        synchronized (this) {
            proceed = state() != PREPARED && state(MARKED_ROLLBACK, true);
        }

        if (proceed || (state() == MARKED_ROLLBACK)) {
            cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                @Override public void run() {
                    // Note: if rollback asynchronously on timeout should not clear thread map
                    // since thread started tx still should be able to see this tx.
                    rollbackNearTxLocalAsync(false, true);

                    U.warn(log, "The transaction was forcibly rolled back because a timeout is reached: " +
                        CU.txString(GridNearTxLocal.this));
                }
            });
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Skip rollback tx on timeout: " + this);
        }
    }

    /** */
    private long timeMillis(AtomicLong atomicNanoTime) {
        return U.nanosToMillis(atomicNanoTime.get());
    }

    /**
     * Enters the section when system time for this transaction is counted.
     */
    public void enterSystemSection() {
        // Setting systemStartTime only if it equals 0, otherwise it means that we are already in system section
        // and should do nothing.
        systemStartTime.compareAndSet(0, System.nanoTime());
    }

    /**
     * Leaves the section when system time for this transaction is counted.
     */
    public void leaveSystemSection() {
        long systemStartTime0 = systemStartTime.getAndSet(0);

        if (systemStartTime0 > 0)
            systemTime.addAndGet(System.nanoTime() - systemStartTime0);
    }

    /**
     * Post-lock closure.
     *
     * @param <T> Return type.
     */
    protected abstract class FinishClosure<T> implements IgniteBiClosure<T, Exception, T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public final T apply(T t, @Nullable Exception e) {
            boolean rollback = true;

            try {
                if (e != null)
                    throw new GridClosureException(e);

                t = finish(t);

                // Commit implicit transactions.
                if (implicit())
                    commit();

                rollback = false;

                return t;
            }
            catch (IgniteCheckedException ex) {
                throw new GridClosureException(ex);
            }
            finally {
                if (rollback)
                    setRollbackOnly();
            }
        }

        /**
         * @param t Argument.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        abstract T finish(T t) throws IgniteCheckedException;
    }

    /** */
    private class MvccTxSnapshotFuture extends MvccSnapshotFuture {
        /** {@inheritDoc} */
        @Override public void onResponse(MvccSnapshot res) {
            onResponse0(res, this);
        }

        /** {@inheritDoc} */
        @Override public void onError(IgniteCheckedException err) {
            setRollbackOnly();

            super.onError(err);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxLocal.class, this,
            "thread", IgniteUtils.threadName(threadId),
            "mappings", mappings,
            "super", super.toString());
    }
}
