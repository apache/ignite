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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxy;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyRollbackOnlyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.SER_READ_EMPTY_ENTRY_VER;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.SER_READ_NOT_EMPTY_VER;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Replicated user transaction.
 */
@SuppressWarnings("unchecked")
public class GridNearTxLocal extends GridDhtTxLocalAdapter implements GridTimeoutObject, AutoCloseable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Prepare future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, IgniteInternalFuture> PREP_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, IgniteInternalFuture.class, "prepFut");

    /** Prepare future updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxLocal, NearTxFinishFuture> FINISH_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxLocal.class, NearTxFinishFuture.class, "finishFut");

    /** DHT mappings. */
    private IgniteTxMappings mappings;

    /** Prepare future. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringExclude
    private volatile IgniteInternalFuture<?> prepFut;

    /** Commit future. */
    @SuppressWarnings("UnusedDeclaration")
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

    /** */
    @GridToStringExclude
    private TransactionProxyImpl proxy;

    /** */
    @GridToStringExclude
    private TransactionProxyImpl rollbackOnlyProxy;

    /** Tx label. */
    private @Nullable String lb;

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
     * @param lb Label.
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
        int taskNameHash,
        @Nullable String lb
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
        this.lb = lb;

        mappings = implicitSingle ? new IgniteTxMappingsSingleImpl() : new IgniteTxMappingsImpl();

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
        PREP_FUT_UPD.compareAndSet(this, fut, null);
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
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
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

        try {
            beforePut(cacheCtx, retval);

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

                IgniteInternalFuture<Boolean>fut = cacheCtx.cache().txLockAsync(enlisted,
                    timeout,
                    this,
                    /*read*/entryProcessor != null, // Needed to force load from store.
                    retval,
                    isolation,
                    isInvalidate(),
                    -1L,
                    -1L);

                PLC1<GridCacheReturn> plc1 = new PLC1<GridCacheReturn>(ret) {
                    @Override public GridCacheReturn postLock(GridCacheReturn ret)
                        throws IgniteCheckedException
                    {
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
        try {
            beforePut(cacheCtx, retval);
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
                    @Override public GridCacheReturn postLock(GridCacheReturn ret)
                        throws IgniteCheckedException
                    {
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

            GridCacheVersion drVer = dataCenterId != null ? cctx.versions().next(dataCenterId) : null;

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
                    drVer = cctx.versions().next(dataCenterId);
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
                                EntryGetResult res = primaryLocal(entry) ?
                                    entry.innerGetVersioned(
                                        null,
                                        this,
                                        /*metrics*/retval,
                                        /*events*/retval,
                                        CU.subjectId(this, cctx),
                                        entryProcessor,
                                        resolveTaskName(),
                                        null,
                                        keepBinary,
                                        null) : null;

                                if (res != null) {
                                    old = res.value();
                                    readVer = res.version();
                                }
                            }
                            else {
                                old = entry.innerGet(
                                    null,
                                    this,
                                    /*read through*/false,
                                    /*metrics*/retval,
                                    /*events*/retval,
                                    CU.subjectId(this, cctx),
                                    entryProcessor,
                                    resolveTaskName(),
                                    null,
                                    keepBinary);
                            }
                        }
                        catch (ClusterTopologyCheckedException e) {
                            entry.context().evicts().touch(entry, topologyVersion());

                            throw e;
                        }
                    }
                    else
                        old = entry.rawGet();

                    final GridCacheOperation op = lockOnly ? NOOP : rmv ? DELETE :
                        entryProcessor != null ? TRANSFORM : old != null ? UPDATE : CREATE;

                    if (old != null && hasFilters && !filter(entry.context(), cacheKey, old, filter)) {
                        ret.set(cacheCtx, old, false, keepBinary);

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
                            cacheCtx.evicts().touch(entry, topologyVersion());

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

                                if (retval)
                                    ret.set(cacheCtx, null, true, keepBinary);
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
                                ret.set(cacheCtx, old, true, keepBinary);
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
                            ret.set(cacheCtx, old, true, keepBinary);
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
                    ret.set(cacheCtx, v, false, keepBinary);

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
                    ret.set(cacheCtx, v, true, keepBinary);
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
                @Override protected GridCacheReturn postLock(GridCacheReturn ret)
                    throws IgniteCheckedException
                {
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

                return nonInterruptable(commitNearTxLocalAsync().chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, GridCacheReturn>() {
                    @Override public GridCacheReturn applyx(IgniteInternalFuture<IgniteInternalTx> txFut)
                        throws IgniteCheckedException {
                        try {
                            txFut.get();

                            return new GridCacheReturn(cacheCtx, true, keepBinary,
                                implicitRes.value(), implicitRes.success());
                        }
                        catch (IgniteCheckedException | RuntimeException e) {
                            rollbackNearTxLocalAsync();

                            throw e;
                        }
                    }
                }));
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
     * @param cacheCtx Cache context.
     * @param keys Keys to get.
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects
     * @param skipStore Skip store flag.
     * @return Future for this get.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalFuture<Map<K, V>> getAllAsync(
        final GridCacheContext cacheCtx,
        @Nullable final AffinityTopologyVersion entryTopVer,
        Collection<KeyCacheObject> keys,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean skipStore,
        final boolean recovery,
        final boolean needVer) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

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
                                        true));

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
                                            CU.subjectId(GridNearTxLocal.this, cctx),
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
                                            CU.subjectId(GridNearTxLocal.this, cctx),
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
                                            needVer);

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

                        if (!missed.isEmpty() && cacheCtx.isLocal()) {
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
                                needVer,
                                expiryPlc0);
                        }

                        return new GridFinishedFuture<>(Collections.<K, V>emptyMap());
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

                        return fut1.isDone() ?
                            new GridFinishedFuture<>(finClos.apply(fut1.get(), null)) :
                            new GridEmbeddedFuture<>(finClos, fut1);
                    }
                    catch (GridClosureException e) {
                        return new GridFinishedFuture<>(e.unwrap());
                    }
                    catch (IgniteCheckedException e) {
                        try {
                            return plc2.apply(false, e);
                        }
                        catch (Exception e1) {
                            return new GridFinishedFuture<>(e1);
                        }
                    }
                }
                else {
                    return new GridEmbeddedFuture<>(
                        fut,
                        plc2,
                        finClos);
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
                                .unwrapBinaryIfNeeded(cacheKey, !deserializeBinary, false));

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
     * @throws IgniteCheckedException If failed.
     * @return Enlisted keys.
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
        final boolean needVer
    ) throws IgniteCheckedException {
        assert !F.isEmpty(keys);
        assert keysCnt == keys.size();

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
                            ver, 0, 0);
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
                                    CU.subjectId(this, cctx),
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
                                    CU.subjectId(this, cctx),
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
                                    needVer);
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

                        if (!pessimistic() || readCommitted() && !skipVals) {
                            IgniteCacheExpiryPolicy accessPlc =
                                optimistic() ? accessPolicy(cacheCtx, txKey, expiryPlc) : null;

                            if (needReadVer) {
                                getRes = primaryLocal(entry) ?
                                    entry.innerGetVersioned(
                                        null,
                                        this,
                                        /*metrics*/true,
                                        /*event*/true,
                                        CU.subjectId(this, cctx),
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
                                    CU.subjectId(this, cctx),
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
                                    needVer);
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
                                entry.context().evicts().touch(entry, topVer);
                        }
                    }
                }
            }
        }

        return lockKeys != null ? lockKeys : Collections.<KeyCacheObject>emptyList();
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

                        ret.set(cacheCtx, null, val != null, keepBinary);
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

                            ret.set(cacheCtx, cacheVal, success, keepBinary);
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
            expiryPlc,
            c);
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

                            return new GridCacheReturn(cacheCtx, true, keepBinary, res, implicitRes.success());
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
                txEntry.context().evicts().touch(cached0, topologyVersion());
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> optimisticLockEntries() {
        assert false : "Should not be called";

        throw new UnsupportedOperationException();
    }

    /**
     * @param cacheCtx  Cache context.
     * @param readThrough Read through flag.
     * @param async if {@code True}, then loading will happen in a separate thread.
     * @param keys Keys.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} version is required for loaded values.
     * @param c Closure to be applied for loaded values.
     * @param expiryPlc Expiry policy.
     * @return Future with {@code True} value if loading took place.
     */
    public IgniteInternalFuture<Void> loadMissing(
        final GridCacheContext cacheCtx,
        AffinityTopologyVersion topVer,
        boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        final boolean skipVals,
        final boolean needVer,
        boolean keepBinary,
        boolean recovery,
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
                    needVer,
                    /*keepCacheObject*/true,
                    recovery
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
                    recovery,
                    expiryPlc0,
                    skipVals,
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

            return localCacheLoadMissing(cacheCtx,
                topVer,
                readThrough,
                async,
                keys,
                skipVals,
                needVer,
                keepBinary,
                recovery,
                expiryPlc,
                c);
        }
    }

    /**
     * @param cacheCtx  Cache context.
     * @param readThrough Read through flag.
     * @param async if {@code True}, then loading will happen in a separate thread.
     * @param keys Keys.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} version is required for loaded values.
     * @param c Closure to be applied for loaded values.
     * @param expiryPlc Expiry policy.
     * @return Future with {@code True} value if loading took place.
     */
    private IgniteInternalFuture<Void> localCacheLoadMissing(
        final GridCacheContext cacheCtx,
        final AffinityTopologyVersion topVer,
        final boolean readThrough,
        boolean async,
        final Collection<KeyCacheObject> keys,
        boolean skipVals,
        boolean needVer,
        boolean keepBinary,
        boolean recovery,
        final ExpiryPolicy expiryPlc,
        final GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c
    ) {
        assert cacheCtx.isLocal() : cacheCtx.name();

        if (!readThrough || !cacheCtx.readThrough()) {
            for (KeyCacheObject key : keys)
                c.apply(key, null, SER_READ_EMPTY_ENTRY_VER);

            return new GridFinishedFuture<>();
        }

        try {
            IgniteCacheExpiryPolicy expiryPlc0 = optimistic() ?
                accessPolicy(cacheCtx, keys) :
                cacheCtx.cache().expiryPolicy(expiryPlc);

            Map<KeyCacheObject, GridCacheVersion> misses = null;

            for (KeyCacheObject key : keys) {
                while (true) {
                    IgniteTxEntry txEntry = entry(cacheCtx.txKey(key));

                    GridCacheEntryEx entry = txEntry == null ? cacheCtx.cache().entryEx(key) :
                        txEntry.cached();

                    if (entry == null)
                        continue;

                    try {
                        EntryGetResult res = entry.innerGetVersioned(
                            null,
                            this,
                            /*update-metrics*/!skipVals,
                            /*event*/!skipVals,
                            CU.subjectId(this, cctx),
                            null,
                            resolveTaskName(),
                            expiryPlc0,
                            txEntry == null ? keepBinary : txEntry.keepBinary(),
                            null);

                        if (res == null) {
                            if (misses == null)
                                misses = new LinkedHashMap<>();

                            misses.put(key, entry.version());
                        }
                        else
                            c.apply(key, skipVals ? true : res.value(), res.version());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry, will retry: " + key);

                        if (txEntry != null)
                            txEntry.cached(cacheCtx.cache().entryEx(key, topologyVersion()));
                    }
                }
            }

            if (misses != null) {
                final Map<KeyCacheObject, GridCacheVersion> misses0 = misses;

                cacheCtx.store().loadAll(this, misses.keySet(), new CI2<KeyCacheObject, Object>() {
                    @Override public void apply(KeyCacheObject key, Object val) {
                        GridCacheVersion ver = misses0.remove(key);

                        assert ver != null : key;

                        if (val != null) {
                            CacheObject cacheVal = cacheCtx.toCacheObject(val);

                            while (true) {
                                GridCacheEntryEx entry = cacheCtx.cache().entryEx(key, topVer);

                                try {
                                    cacheCtx.shared().database().ensureFreeSpace(cacheCtx.dataRegion());

                                    EntryGetResult verVal = entry.versionedValue(cacheVal,
                                        ver,
                                        null,
                                        null,
                                        null);

                                    if (log.isDebugEnabled()) {
                                        log.debug("Set value loaded from store into entry [" +
                                            "oldVer=" + ver +
                                            ", newVer=" + verVal.version() +
                                            ", entry=" + entry + ']');
                                    }

                                    ver = verVal.version();

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed entry, (will retry): " + entry);
                                }
                                catch (IgniteCheckedException e) {
                                    // Wrap errors (will be unwrapped).
                                    throw new GridClosureException(e);
                                }
                            }
                        }
                        else
                            ver = SER_READ_EMPTY_ENTRY_VER;

                        c.apply(key, val, ver);
                    }
                });

                for (KeyCacheObject key : misses0.keySet())
                    c.apply(key, null, SER_READ_EMPTY_ENTRY_VER);
            }

            return new GridFinishedFuture<>();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
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

        if (pessimistic())
            throw new UnsupportedOperationException("Suspension is not supported for pessimistic transactions.");

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

        if (pessimistic())
            throw new UnsupportedOperationException("Resume is not supported for pessimistic transactions.");

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
        Collection<GridCacheVersion> rolledbackVers)
    {
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
     * @return Tx prepare future.
     */
    public IgniteInternalFuture<?> prepareNearTxLocal() {
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

        NearTxFinishFuture fut = finishFut;

        if (fut != null)
            return chainFinishFuture(fut, true, true, false);

        if (fastFinish()) {
            GridNearTxFastFinishFuture fut0;

            if (!FINISH_FUT_UPD.compareAndSet(this, null, fut0 = new GridNearTxFastFinishFuture(this, true)))
                return chainFinishFuture(finishFut, true, true, false);

            fut0.finish(false);

            return fut0;
        }

        final GridNearTxFinishFuture fut0;

        if (!FINISH_FUT_UPD.compareAndSet(this, null, fut0 = new GridNearTxFinishFuture<>(cctx, this, true)))
            return chainFinishFuture(finishFut, true, true, false);

        cctx.mvcc().addFuture(fut0, fut0.futureId());

        final IgniteInternalFuture<?> prepareFut = prepareNearTxLocal();

        prepareFut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                try {
                    // Make sure that here are no exceptions.
                    prepareFut.get();

                    fut0.finish(true, true, false);
                }
                catch (Error | RuntimeException e) {
                    COMMIT_ERR_UPD.compareAndSet(GridNearTxLocal.this, null, e);

                    fut0.finish(false, true, false);

                    throw e;
                }
                catch (IgniteCheckedException e) {
                    COMMIT_ERR_UPD.compareAndSet(GridNearTxLocal.this, null, e);

                    if (!(e instanceof NodeStoppingException))
                        fut0.finish(false, true, true);
                }
            }
        });

        return fut0;
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
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public IgniteInternalFuture<IgniteInternalTx> rollbackNearTxLocalAsync(final boolean clearThreadMap,
        final boolean onTimeout) {
        if (log.isDebugEnabled())
            log.debug("Rolling back near tx: " + this);

        if (!onTimeout && trackTimeout)
            removeTimeoutHandler();

        IgniteInternalFuture<?> prepFut = this.prepFut;

        if (onTimeout && prepFut instanceof GridNearTxPrepareFutureAdapter && !prepFut.isDone())
            ((GridNearTxPrepareFutureAdapter) prepFut).onNearTxLocalTimeout();

        NearTxFinishFuture fut = finishFut;

        if (fut != null)
            return chainFinishFuture(finishFut, false, clearThreadMap, onTimeout);

        // Enable fast finish only from tx thread.
        if (clearThreadMap && fastFinish()) {
            GridNearTxFastFinishFuture fut0;

            if (!FINISH_FUT_UPD.compareAndSet(this, null, fut0 = new GridNearTxFastFinishFuture(this, false)))
                return chainFinishFuture(finishFut, false, true, onTimeout);

            fut0.finish(true);

            return fut0;
        }

        final GridNearTxFinishFuture fut0;

        if (!FINISH_FUT_UPD.compareAndSet(this, null, fut0 = new GridNearTxFinishFuture<>(cctx, this, false)))
            return chainFinishFuture(finishFut, false, clearThreadMap, onTimeout);

        cctx.mvcc().addFuture(fut0, fut0.futureId());

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

            fut0.finish(false, clearThreadMap, onTimeout);
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

                    fut0.finish(false, clearThreadMap, onTimeout);
                }
            });
        }

        return fut0;
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
                            if (tx.state() == COMMITTED) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to rollback, transaction is already committed: " + tx);

                                rollbackFut.forceFinish();

                                assert rollbackFut.isDone() : rollbackFut;
                            }
                            else {
                                if (!cctx.mvcc().addFuture(rollbackFut, rollbackFut.futureId()))
                                    return;

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
        return writeMap().isEmpty() && ((optimistic() && !serializable()) || readMap().isEmpty());
    }

    /**
     * Prepares next batch of entries in dht transaction.
     *
     * @param req Prepare request.
     * @return Future that will be completed when locks are acquired.
     */
    @SuppressWarnings("TypeMayBeWeakened")
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
            if (prep != null)
                return (IgniteInternalFuture<IgniteInternalTx>)prep;

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

            if (state != COMMITTING && state != ROLLING_BACK && (!trackTimeout || rmv))
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
    @SuppressWarnings("unchecked")
    @Nullable @Override public IgniteInternalFuture<?> currentPrepareFuture() {
        return prepFut;
    }

    /**
     * @param topVer New topology version.
     */
    public void onRemap(AffinityTopologyVersion topVer) {
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
        final boolean needVer,
        final ExpiryPolicy expiryPlc
    ) {
        if (log.isDebugEnabled())
            log.debug("Loading missed values for missed map: " + missedMap);

        final boolean needReadVer = (serializable() && optimistic()) || needVer;

        return new GridEmbeddedFuture<>(
            new C2<Void, Exception, Map<K, V>>() {
                @Override public Map<K, V> apply(Void v, Exception e) {
                    if (e != null) {
                        setRollbackOnly();

                        throw new GridClosureException(e);
                    }

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
                            cacheCtx.evicts().touch(e, topologyVersion());

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
                                    0);
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
                                    0);
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
     * @throws IgniteCheckedException If failed.
     */
    private void beforePut(GridCacheContext cacheCtx, boolean retval) throws IgniteCheckedException {
        checkUpdatesAllowed(cacheCtx);

        cacheCtx.checkSecurity(SecurityPermission.CACHE_PUT);

        if (retval)
            needReturnValue(true);

        checkValid();

        init();
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
    private <T> IgniteInternalFuture<T> nonInterruptable(IgniteInternalFuture<T> fut) {
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

    /**
     * @return Tx label.
     */
    public String label() {
        return lb;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (state() == SUSPENDED) {
            try {
                resume(false, threadId());
            }
            catch (IgniteCheckedException e) {
                log.warning("Error resuming suspended transaction on timeout: " + this, e);
            }
        }

        boolean proceed;

        synchronized (this) {
            proceed = state() != PREPARED && state(MARKED_ROLLBACK, true);
        }

        if (proceed || (state() == MARKED_ROLLBACK)) {
            cctx.kernalContext().closure().runLocalSafe(new Runnable() {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxLocal.class, this,
            "thread", IgniteUtils.threadName(threadId),
            "mappings", mappings,
            "super", super.toString());
    }
}
