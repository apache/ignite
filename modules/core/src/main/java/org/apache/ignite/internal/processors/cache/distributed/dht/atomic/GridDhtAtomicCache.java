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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

/**
 * Non-transactional partitioned cache.
 */
@SuppressWarnings("unchecked")
@GridToStringExclude
public class GridDhtAtomicCache<K, V> extends GridDhtCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deferred update response buffer size. */
    private static final int DEFERRED_UPDATE_RESPONSE_BUFFER_SIZE =
        Integer.getInteger(IGNITE_ATOMIC_DEFERRED_ACK_BUFFER_SIZE, 256);

    /** Deferred update response timeout. */
    private static final int DEFERRED_UPDATE_RESPONSE_TIMEOUT =
        Integer.getInteger(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT, 500);

    /** Unsafe instance. */
    private static final sun.misc.Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Update reply closure. */
    private CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> updateReplyClos;

    /** Pending  */
    private ConcurrentMap<UUID, DeferredResponseBuffer> pendingResponses = new ConcurrentHashMap8<>();

    /** */
    private GridNearAtomicCache<K, V> near;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicCache() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     */
    public GridDhtAtomicCache(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * @param ctx Cache context.
     * @param map Cache concurrent map.
     */
    public GridDhtAtomicCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public boolean isDhtAtomic() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry create(GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key,
                int hash,
                CacheObject val,
                GridCacheMapEntry next,
                int hdrId)
            {
                if (ctx.useOffheapEntry())
                    return new GridDhtAtomicOffHeapCacheEntry(ctx, topVer, key, hash, val, next, hdrId);

                return new GridDhtAtomicCacheEntry(ctx, topVer, key, hash, val, next, hdrId);
            }
        });

        updateReplyClos = new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override public void apply(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
                if (ctx.config().getAtomicWriteOrderMode() == CLOCK) {
                    // Always send reply in CLOCK ordering mode.
                    sendNearUpdateReply(res.nodeId(), res);

                    return;
                }

                // Request should be for primary keys only in PRIMARY ordering mode.
                assert req.hasPrimary() : req;

                if (req.writeSynchronizationMode() != FULL_ASYNC)
                    sendNearUpdateReply(res.nodeId(), res);
                else {
                    if (!F.isEmpty(res.remapKeys()))
                        // Remap keys on primary node in FULL_ASYNC mode.
                        remapToNewPrimary(req);
                    else if (res.error() != null) {
                        U.error(log, "Failed to process write update request in FULL_ASYNC mode for keys: " +
                            res.failedKeys(), res.error());
                    }
                }
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "SimplifiableIfStatement"})
    @Override public void start() throws IgniteCheckedException {
        super.start();

        CacheMetricsImpl m = new CacheMetricsImpl(ctx);

        if (ctx.dht().near() != null)
            m.delegate(ctx.dht().near().metrics0());

        metrics = m;

        preldr = new GridDhtPreloader(ctx);

        preldr.start();

        ctx.io().addHandler(ctx.cacheId(), GridNearGetRequest.class, new CI2<UUID, GridNearGetRequest>() {
            @Override public void apply(UUID nodeId, GridNearGetRequest req) {
                processNearGetRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridNearAtomicUpdateRequest.class, new CI2<UUID, GridNearAtomicUpdateRequest>() {
            @Override public void apply(UUID nodeId, GridNearAtomicUpdateRequest req) {
                processNearAtomicUpdateRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridNearAtomicUpdateResponse.class, new CI2<UUID, GridNearAtomicUpdateResponse>() {
            @Override public void apply(UUID nodeId, GridNearAtomicUpdateResponse res) {
                processNearAtomicUpdateResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridDhtAtomicUpdateRequest.class, new CI2<UUID, GridDhtAtomicUpdateRequest>() {
            @Override public void apply(UUID nodeId, GridDhtAtomicUpdateRequest req) {
                processDhtAtomicUpdateRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridDhtAtomicUpdateResponse.class, new CI2<UUID, GridDhtAtomicUpdateResponse>() {
            @Override public void apply(UUID nodeId, GridDhtAtomicUpdateResponse res) {
                processDhtAtomicUpdateResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridDhtAtomicDeferredUpdateResponse.class,
            new CI2<UUID, GridDhtAtomicDeferredUpdateResponse>() {
                @Override public void apply(UUID nodeId, GridDhtAtomicDeferredUpdateResponse res) {
                    processDhtAtomicDeferredUpdateResponse(nodeId, res);
                }
            });

        if (near == null) {
            ctx.io().addHandler(ctx.cacheId(), GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse>() {
                @Override public void apply(UUID nodeId, GridNearGetResponse res) {
                    processNearGetResponse(nodeId, res);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        for (DeferredResponseBuffer buf : pendingResponses.values())
            buf.finish();
    }

    /**
     * @param near Near cache.
     */
    public void near(GridNearAtomicCache<K, V> near) {
        this.near = near;
    }

    /** {@inheritDoc} */
    @Override public GridNearCacheAdapter<K, V> near() {
        return near;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx entry,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializePortable,
        final boolean skipVals,
        final boolean canRemap
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(null, opCtx);

        final UUID subjId0 = subjId;

        final ExpiryPolicy expiryPlc = skipVals ? null : opCtx != null ? opCtx.expiry() : null;

        final boolean skipStore = opCtx != null && opCtx.skipStore();

        return asyncOp(new CO<IgniteInternalFuture<Map<K, V>>>() {
            @Override public IgniteInternalFuture<Map<K, V>> apply() {
                return getAllAsync0(ctx.cacheKeysView(keys),
                    false,
                    forcePrimary,
                    subjId0,
                    taskName,
                    deserializePortable,
                    expiryPlc,
                    skipVals,
                    skipStore,
                    canRemap);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val, @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return getAndPutAsync0(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public boolean put(K key, V val, CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return putAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndPutAsync0(K key, V val, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key");

        return updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            null,
            null,
            true,
            false,
            filter,
            true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> putAsync0(K key, V val, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key");

        return updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            null,
            null,
            false,
            false,
            filter,
            true);
    }

    /** {@inheritDoc} */
    @Override public V tryPutIfAbsent(K key, V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        return (V)updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            null,
            null,
            true,
            false,
            ctx.noValArray(),
            false).get();
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws IgniteCheckedException {
        return getAndPutIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutIfAbsentAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return getAndPutAsync(key, val, ctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws IgniteCheckedException {
        return putIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return putAsync(key, val, ctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) throws IgniteCheckedException {
        return getAndReplaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndReplaceAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return getAndPutAsync(key, val, ctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws IgniteCheckedException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return putAsync(key, val, ctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return replaceAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        return putAsync(key, newVal, ctx.equalsValArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn removex(K key, V val) throws IgniteCheckedException {
        return removexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return replacexAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn> removexAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return removeAllAsync0(F.asList(key), null, true, true, ctx.equalsValArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn> replacexAsync(K key, V oldVal, V newVal) {
        return updateAllAsync0(F.asMap(key, newVal),
            null,
            null,
            null,
            null,
            true,
            true,
            ctx.equalsValArray(oldVal),
            true);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        putAllAsync(m).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(Map<? extends K, ? extends V> m) {
        return updateAllAsync0(m,
            null,
            null,
            null,
            null,
            false,
            false,
            CU.empty0(),
            true).chain(RET2NULL);
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> conflictMap)
        throws IgniteCheckedException {
        putAllConflictAsync(conflictMap).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> conflictMap) {
        ctx.dr().onReceiveCacheEntriesReceived(conflictMap.size());

        return updateAllAsync0(null,
            null,
            null,
            conflictMap,
            null,
            false,
            false,
            null,
            true);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) throws IgniteCheckedException {
        return getAndRemoveAsync(key).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(K key) {
        A.notNull(key, "key");

        return removeAllAsync0(Collections.singletonList(key), null, true, false, CU.empty0());
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Collection<? extends K> keys) throws IgniteCheckedException {
        removeAllAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        return removeAllAsync0(keys, null, false, false, CU.empty0()).chain(RET2NULL);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws IgniteCheckedException {
        return removeAsync(key, CU.empty0()).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key");

        return removeAllAsync0(Collections.singletonList(key), null, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        return removeAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return removeAsync(key, ctx.equalsValArray(val));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localRemoveAll(CacheEntryPredicate filter) {
        return removeAllAsync(keySet(filter));
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> conflictMap)
        throws IgniteCheckedException {
        removeAllConflictAsync(conflictMap).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> conflictMap) {
        ctx.dr().onReceiveCacheEntriesReceived(conflictMap.size());

        return removeAllAsync0(null, conflictMap, false, false, null);
    }

    /**
     * @return {@code True} if store write-through enabled.
     */
    private boolean writeThrough() {
        return ctx.writeThrough() && ctx.store().configured();
    }

    /**
     * @param op Operation closure.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected <T> IgniteInternalFuture<T> asyncOp(final CO<IgniteInternalFuture<T>> op) {
        IgniteInternalFuture<T> fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteInternalFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                IgniteInternalFuture<T> f = new GridEmbeddedFuture<>(fut,
                    new C2<T, Exception, IgniteInternalFuture<T>>() {
                        @Override public IgniteInternalFuture<T> apply(T t, Exception e) {
                            return op.apply();
                        }
                    });

                saveFuture(holder, f);

                return f;
            }

            IgniteInternalFuture<T> f = op.apply();

            saveFuture(holder, f);

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<Boolean> lockAllAsync(Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable TransactionIsolation isolation,
        long accessTtl) {
        return new FinishedLockFuture(new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws IgniteCheckedException {
        EntryProcessorResult<T> res = invokeAsync(key, entryProcessor, args).get();

        return res != null ? res : new CacheInvokeResult<T>();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args)
        throws IgniteCheckedException {
        return invokeAllAsync(keys, entryProcessor, args).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        Map<? extends K, EntryProcessor> invokeMap =
            Collections.singletonMap(key, (EntryProcessor)entryProcessor);

        IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut = updateAllAsync0(null,
            invokeMap,
            args,
            null,
            null,
            false,
            false,
            null,
            true);

        return fut.chain(new CX1<IgniteInternalFuture<Map<K, EntryProcessorResult<T>>>, EntryProcessorResult<T>>() {
            @Override public EntryProcessorResult<T> applyx(IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut)
                throws IgniteCheckedException {
                Map<K, EntryProcessorResult<T>> resMap = fut.get();

                if (resMap != null) {
                    assert resMap.isEmpty() || resMap.size() == 1 : resMap.size();

                    return resMap.isEmpty() ? null : resMap.values().iterator().next();
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKeys(keys);

        Map<? extends K, EntryProcessor> invokeMap = F.viewAsMap(keys, new C1<K, EntryProcessor>() {
            @Override public EntryProcessor apply(K k) {
                return entryProcessor;
            }
        });

        return updateAllAsync0(null,
            invokeMap,
            args,
            null,
            null,
            false,
            false,
            null,
            true);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        return invokeAllAsync(map, args).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        return updateAllAsync0(null,
            map,
            args,
            null,
            null,
            false,
            false,
            null,
            true);
    }

    /**
     * Entry point for all public API put/transform methods.
     *
     * @param map Put map. Either {@code map}, {@code invokeMap} or {@code conflictPutMap} should be passed.
     * @param invokeMap Invoke map. Either {@code map}, {@code invokeMap} or {@code conflictPutMap} should be passed.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param conflictPutMap Conflict put map.
     * @param conflictRmvMap Conflict remove map.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter for atomic updates.
     * @param waitTopFut Whether to wait for topology future.
     * @return Completion future.
     */
    @SuppressWarnings("ConstantConditions")
    private IgniteInternalFuture updateAllAsync0(
        @Nullable final Map<? extends K, ? extends V> map,
        @Nullable final Map<? extends K, ? extends EntryProcessor> invokeMap,
        @Nullable Object[] invokeArgs,
        @Nullable final Map<KeyCacheObject, GridCacheDrInfo> conflictPutMap,
        @Nullable final Map<KeyCacheObject, GridCacheVersion> conflictRmvMap,
        final boolean retval,
        final boolean rawRetval,
        @Nullable final CacheEntryPredicate[] filter,
        final boolean waitTopFut
    ) {
        assert ctx.updatesAllowed();

        if (map != null && keyCheck)
            validateCacheKeys(map.keySet());

        ctx.checkSecurity(SecurityPermission.CACHE_PUT);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        UUID subjId = ctx.subjectIdPerCall(null, opCtx);

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        final GridNearAtomicUpdateFuture updateFut = new GridNearAtomicUpdateFuture(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            invokeMap != null ? TRANSFORM : UPDATE,
            map != null ? map.keySet() : invokeMap != null ? invokeMap.keySet() : conflictPutMap != null ?
                conflictPutMap.keySet() : conflictRmvMap.keySet(),
            map != null ? map.values() : invokeMap != null ? invokeMap.values() : null,
            invokeArgs,
            (Collection)(conflictPutMap != null ? conflictPutMap.values() : null),
            conflictRmvMap != null ? conflictRmvMap.values() : null,
            retval,
            rawRetval,
            opCtx != null ? opCtx.expiry() : null,
            filter,
            subjId,
            taskNameHash,
            opCtx != null && opCtx.skipStore(),
            opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES,
            waitTopFut);

        return asyncOp(new CO<IgniteInternalFuture<Object>>() {
            @Override public IgniteInternalFuture<Object> apply() {
                updateFut.map();

                return updateFut;
            }
        });
    }

    /**
     * Entry point for all public API remove methods.
     *
     * @param keys Keys to remove.
     * @param conflictMap Conflict map.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter for atomic removes.
     * @return Completion future.
     */
    private IgniteInternalFuture removeAllAsync0(
        @Nullable final Collection<? extends K> keys,
        @Nullable final Map<KeyCacheObject, GridCacheVersion> conflictMap,
        final boolean retval,
        boolean rawRetval,
        @Nullable final CacheEntryPredicate[] filter
    ) {
        assert ctx.updatesAllowed();

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        assert keys != null || conflictMap != null;

        if (keyCheck)
            validateCacheKeys(keys);

        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        UUID subjId = ctx.subjectIdPerCall(null, opCtx);

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        final GridNearAtomicUpdateFuture updateFut = new GridNearAtomicUpdateFuture(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            DELETE,
            keys != null ? keys : conflictMap.keySet(),
            null,
            null,
            null,
            keys != null ? null : conflictMap.values(),
            retval,
            rawRetval,
            (filter != null && opCtx != null) ? opCtx.expiry() : null,
            filter,
            subjId,
            taskNameHash,
            opCtx != null && opCtx.skipStore(),
            opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES,
            true);

        if (statsEnabled)
            updateFut.listen(new UpdateRemoveTimeStatClosure<>(metrics0(), start));

        return asyncOp(new CO<IgniteInternalFuture<Object>>() {
            @Override public IgniteInternalFuture<Object> apply() {
                updateFut.map();

                return updateFut;
            }
        });
    }

    /**
     * Entry point to all public API get methods.
     *
     * @param keys Keys to remove.
     * @param reload Reload flag.
     * @param forcePrimary Force primary flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param skipStore Skip store flag.
     * @return Get future.
     */
    private IgniteInternalFuture<Map<K, V>> getAllAsync0(@Nullable Collection<KeyCacheObject> keys,
        boolean reload,
        boolean forcePrimary,
        UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable ExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean skipStore,
        boolean canRemap
    ) {
        AffinityTopologyVersion topVer = canRemap ? ctx.affinity().affinityTopologyVersion() :
            ctx.shared().exchange().readyAffinityVersion();

        final IgniteCacheExpiryPolicy expiry = skipVals ? null : expiryPolicy(expiryPlc);

        // Optimisation: try to resolve value locally and escape 'get future' creation.
        if (!reload && !forcePrimary) {
            Map<K, V> locVals = U.newHashMap(keys.size());

            boolean success = true;

            // Optimistically expect that all keys are available locally (avoid creation of get future).
            for (KeyCacheObject key : keys) {
                GridCacheEntryEx entry = null;

                while (true) {
                    try {
                        entry = ctx.isSwapOrOffheapEnabled() ? entryEx(key) : peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            CacheObject v = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /**update-metrics*/false,
                                /*event*/!skipVals,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                expiry);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                GridCacheVersion obsoleteVer = context().versions().next();

                                if (isNew && entry.markObsoleteIfEmpty(obsoleteVer))
                                    removeIfObsolete(key);

                                success = false;
                            }
                            else
                                ctx.addResult(locVals, key, v, skipVals, false, deserializePortable, true);
                        }
                        else
                            success = false;

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // No-op, retry.
                    }
                    catch (GridCacheFilterFailedException ignored) {
                        // No-op, skip the key.
                        break;
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        success = false;

                        break; // While.
                    }
                    catch (IgniteCheckedException e) {
                        return new GridFinishedFuture<>(e);
                    }
                    finally {
                        if (entry != null)
                            ctx.evicts().touch(entry, topVer);
                    }
                }

                if (!success)
                    break;
                else if (!skipVals && ctx.config().isStatisticsEnabled())
                    metrics0().onRead(true);
            }

            if (success) {
                sendTtlUpdateRequest(expiry);

                return new GridFinishedFuture<>(locVals);
            }
        }

        if (expiry != null)
            expiry.reset();

        // Either reload or not all values are available locally.
        GridPartitionedGetFuture<K, V> fut = new GridPartitionedGetFuture<>(ctx,
            keys,
            topVer,
            !skipStore,
            reload,
            forcePrimary,
            subjId,
            taskName,
            deserializePortable,
            expiry,
            skipVals,
            canRemap);

        fut.init();

        return fut;
    }

    /**
     * Executes local update.
     *
     * @param nodeId Node ID.
     * @param req Update request.
     * @param completionCb Completion callback.
     */
    public void updateAllAsyncInternal(
        final UUID nodeId,
        final GridNearAtomicUpdateRequest req,
        final CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb
    ) {
        IgniteInternalFuture<Object> forceFut = preldr.request(req.keys(), req.topologyVersion());

        if (forceFut.isDone())
            updateAllAsyncInternal0(nodeId, req, completionCb);
        else {
            forceFut.listen(new CI1<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> t) {
                    updateAllAsyncInternal0(nodeId, req, completionCb);
                }
            });
        }
    }

    /**
     * Executes local update after preloader fetched values.
     *
     * @param nodeId Node ID.
     * @param req Update request.
     * @param completionCb Completion callback.
     */
    public void updateAllAsyncInternal0(
        UUID nodeId,
        GridNearAtomicUpdateRequest req,
        CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb
    ) {
        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(ctx.cacheId(),
            nodeId,
            req.futureVersion());

        List<KeyCacheObject> keys = req.keys();

        assert !req.returnValue() || (req.operation() == TRANSFORM || keys.size() == 1);

        GridDhtAtomicUpdateFuture dhtFut = null;

        boolean remap = false;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        IgniteCacheExpiryPolicy expiry = null;

        try {
            // If batch store update is enabled, we need to lock all entries.
            // First, need to acquire locks on cache entries, then check filter.
            List<GridDhtCacheEntry> locked = lockEntries(keys, req.topologyVersion());

            Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted = null;

            try {
                topology().readLock();

                try {
                    if (topology().stopping()) {
                        res.addFailedKeys(keys, new IgniteCheckedException("Failed to perform cache operation " +
                            "(cache is stopped): " + name()));

                        completionCb.apply(req, res);

                        return;
                    }

                    // Do not check topology version for CLOCK versioning since
                    // partition exchange will wait for near update future (if future is on server node).
                    // Also do not check topology version if topology was locked on near node by
                    // external transaction or explicit lock.
                    if ((req.fastMap() && !req.clientRequest()) || req.topologyLocked() ||
                        !needRemap(req.topologyVersion(), topology().topologyVersion())) {
                        ClusterNode node = ctx.discovery().node(nodeId);

                        if (node == null) {
                            U.warn(log, "Node originated update request left grid: " + nodeId);

                            return;
                        }

                        boolean hasNear = ctx.discovery().cacheNearNode(node, name());

                        GridCacheVersion ver = req.updateVersion();

                        if (ver == null) {
                            // Assign next version for update inside entries lock.
                            ver = ctx.versions().next(topology().topologyVersion());

                            if (hasNear)
                                res.nearVersion(ver);
                        }

                        assert ver != null : "Got null version for update request: " + req;

                        if (log.isDebugEnabled())
                            log.debug("Using cache version for update request on primary node [ver=" + ver +
                                ", req=" + req + ']');

                        dhtFut = createDhtFuture(ver, req, res, completionCb, false);

                        expiry = expiryPolicy(req.expiry());

                        GridCacheReturn retVal = null;

                        if (keys.size() > 1 &&                             // Several keys ...
                            writeThrough() && !req.skipStore() &&          // and store is enabled ...
                            !ctx.store().isLocal() &&                      // and this is not local store ...
                            !ctx.dr().receiveEnabled()                     // and no DR.
                        ) {
                            // This method can only be used when there are no replicated entries in the batch.
                            UpdateBatchResult updRes = updateWithBatch(node,
                                hasNear,
                                req,
                                res,
                                locked,
                                ver,
                                dhtFut,
                                completionCb,
                                ctx.isDrEnabled(),
                                taskName,
                                expiry);

                            deleted = updRes.deleted();
                            dhtFut = updRes.dhtFuture();

                            if (req.operation() == TRANSFORM)
                                retVal = updRes.invokeResults();
                        }
                        else {
                            UpdateSingleResult updRes = updateSingle(node,
                                hasNear,
                                req,
                                res,
                                locked,
                                ver,
                                dhtFut,
                                completionCb,
                                ctx.isDrEnabled(),
                                taskName,
                                expiry);

                            retVal = updRes.returnValue();
                            deleted = updRes.deleted();
                            dhtFut = updRes.dhtFuture();
                        }

                        if (retVal == null)
                            retVal = new GridCacheReturn(ctx, node.isLocal(), null, true);

                        res.returnValue(retVal);

                        if (dhtFut != null)
                            ctx.mvcc().addAtomicFuture(dhtFut.version(), dhtFut);
                    }
                    else
                        // Should remap all keys.
                        remap = true;
                }
                finally {
                    topology().readUnlock();
                }
            }
            catch (GridCacheEntryRemovedException e) {
                assert false : "Entry should not become obsolete while holding lock.";

                e.printStackTrace();
            }
            finally {
                if (locked != null)
                    unlockEntries(locked, req.topologyVersion());

                // Enqueue if necessary after locks release.
                if (deleted != null) {
                    assert !deleted.isEmpty();
                    assert ctx.deferredDelete(this) : this;

                    for (IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion> e : deleted)
                        ctx.onDeferredDelete(e.get1(), e.get2());
                }
            }
        }
        catch (GridDhtInvalidPartitionException ignore) {
            assert !req.fastMap() || req.clientRequest() : req;

            if (log.isDebugEnabled())
                log.debug("Caught invalid partition exception for cache entry (will remap update request): " + req);

            remap = true;
        }
        catch (Exception e) {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            U.error(log, "Unexpected exception during cache update", e);

            res.addFailedKeys(keys, e);

            completionCb.apply(req, res);

            return;
        }

        if (remap) {
            assert dhtFut == null;

            res.remapKeys(req.keys());

            completionCb.apply(req, res);
        }
        else {
            // If there are backups, map backup update future.
            if (dhtFut != null)
                dhtFut.map();
                // Otherwise, complete the call.
            else
                completionCb.apply(req, res);
        }

        sendTtlUpdateRequest(expiry);
    }

    /**
     * Updates locked entries using batched write-through.
     *
     * @param node Sender node.
     * @param hasNear {@code True} if originating node has near cache.
     * @param req Update request.
     * @param res Update response.
     * @param locked Locked entries.
     * @param ver Assigned version.
     * @param dhtFut Optional DHT future.
     * @param completionCb Completion callback to invoke when DHT future is completed.
     * @param replicate Whether replication is enabled.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @return Deleted entries.
     * @throws GridCacheEntryRemovedException Should not be thrown.
     */
    @SuppressWarnings("unchecked")
    private UpdateBatchResult updateWithBatch(
        ClusterNode node,
        boolean hasNear,
        GridNearAtomicUpdateRequest req,
        GridNearAtomicUpdateResponse res,
        List<GridDhtCacheEntry> locked,
        GridCacheVersion ver,
        @Nullable GridDhtAtomicUpdateFuture dhtFut,
        CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        boolean replicate,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiry
    ) throws GridCacheEntryRemovedException {
        assert !ctx.dr().receiveEnabled(); // Cannot update in batches during DR due to possible conflicts.
        assert !req.returnValue() || req.operation() == TRANSFORM; // Should not request return values for putAll.

        if (!F.isEmpty(req.filter()) && ctx.loadPreviousValue()) {
            try {
                reloadIfNeeded(locked);
            }
            catch (IgniteCheckedException e) {
                res.addFailedKeys(req.keys(), e);

                return new UpdateBatchResult();
            }
        }

        int size = req.keys().size();

        Map<Object, Object> putMap = null;

        Map<KeyCacheObject, EntryProcessor<Object, Object, Object>> entryProcessorMap = null;

        Collection<Object> rmvKeys = null;

        List<CacheObject> writeVals = null;

        UpdateBatchResult updRes = new UpdateBatchResult();

        List<GridDhtCacheEntry> filtered = new ArrayList<>(size);

        GridCacheOperation op = req.operation();

        GridCacheReturn invokeRes = null;

        int firstEntryIdx = 0;

        boolean intercept = ctx.config().getInterceptor() != null;

        for (int i = 0; i < locked.size(); i++) {
            GridDhtCacheEntry entry = locked.get(i);

            if (entry == null)
                continue;

            try {
                if (!checkFilter(entry, req, res)) {
                    if (expiry != null && entry.hasValue()) {
                        long ttl = expiry.forAccess();

                        if (ttl != CU.TTL_NOT_CHANGED) {
                            entry.updateTtl(null, ttl);

                            expiry.ttlUpdated(entry.key(),
                                entry.version(),
                                entry.readers());
                        }
                    }

                    if (log.isDebugEnabled())
                        log.debug("Entry did not pass the filter (will skip write) [entry=" + entry +
                            ", filter=" + Arrays.toString(req.filter()) + ", res=" + res + ']');

                    if (hasNear)
                        res.addSkippedIndex(i);

                    firstEntryIdx++;

                    continue;
                }

                if (op == TRANSFORM) {
                    EntryProcessor<Object, Object, Object> entryProcessor = req.entryProcessor(i);

                    CacheObject old = entry.innerGet(
                        null,
                        /*read swap*/true,
                        /*read through*/true,
                        /*fail fast*/false,
                        /*unmarshal*/true,
                        /*metrics*/true,
                        /*event*/true,
                        /*temporary*/true,
                        req.subjectId(),
                        entryProcessor,
                        taskName,
                        null);

                    Object oldVal = null;
                    Object updatedVal = null;

                    CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry(ctx, entry.key(), old,
                        entry.version());

                    CacheObject updated;

                    try {
                        Object computed = entryProcessor.process(invokeEntry, req.invokeArguments());

                        updatedVal = ctx.unwrapTemporary(invokeEntry.getValue());

                        updated = ctx.toCacheObject(updatedVal);

                        if (computed != null) {
                            if (invokeRes == null)
                                invokeRes = new GridCacheReturn(node.isLocal());

                            invokeRes.addEntryProcessResult(ctx, entry.key(), invokeEntry.key(), computed, null);
                        }
                    }
                    catch (Exception e) {
                        if (invokeRes == null)
                            invokeRes = new GridCacheReturn(node.isLocal());

                        invokeRes.addEntryProcessResult(ctx, entry.key(), invokeEntry.key(), null, e);

                        updated = old;
                    }

                    if (updated == null) {
                        if (intercept) {
                            CacheLazyEntry e = new CacheLazyEntry(ctx, entry.key(), invokeEntry.key(), old, oldVal);

                            IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor().onBeforeRemove(e);

                            if (ctx.cancelRemove(interceptorRes))
                                continue;
                        }

                        // Update previous batch.
                        if (putMap != null) {
                            dhtFut = updatePartialBatch(
                                hasNear,
                                firstEntryIdx,
                                filtered,
                                ver,
                                node,
                                writeVals,
                                putMap,
                                null,
                                entryProcessorMap,
                                dhtFut,
                                completionCb,
                                req,
                                res,
                                replicate,
                                updRes,
                                taskName,
                                expiry);

                            firstEntryIdx = i;

                            putMap = null;
                            writeVals = null;
                            entryProcessorMap = null;

                            filtered = new ArrayList<>();
                        }

                        // Start collecting new batch.
                        if (rmvKeys == null)
                            rmvKeys = new ArrayList<>(size);

                        rmvKeys.add(entry.key().value(ctx.cacheObjectContext(), false));
                    }
                    else {
                        if (intercept) {
                            CacheLazyEntry e = new CacheLazyEntry(ctx, entry.key(), invokeEntry.key(), old, oldVal);

                            Object val = ctx.config().getInterceptor().onBeforePut(e, updatedVal);

                            if (val == null)
                                continue;

                            updated = ctx.toCacheObject(ctx.unwrapTemporary(val));
                        }

                        // Update previous batch.
                        if (rmvKeys != null) {
                            dhtFut = updatePartialBatch(
                                hasNear,
                                firstEntryIdx,
                                filtered,
                                ver,
                                node,
                                null,
                                null,
                                rmvKeys,
                                entryProcessorMap,
                                dhtFut,
                                completionCb,
                                req,
                                res,
                                replicate,
                                updRes,
                                taskName,
                                expiry);

                            firstEntryIdx = i;

                            rmvKeys = null;
                            entryProcessorMap = null;

                            filtered = new ArrayList<>();
                        }

                        if (putMap == null) {
                            putMap = new LinkedHashMap<>(size, 1.0f);
                            writeVals = new ArrayList<>(size);
                        }

                        putMap.put(CU.value(entry.key(), ctx, false), CU.value(updated, ctx, false));
                        writeVals.add(updated);
                    }

                    if (entryProcessorMap == null)
                        entryProcessorMap = new HashMap<>();

                    entryProcessorMap.put(entry.key(), entryProcessor);
                }
                else if (op == UPDATE) {
                    CacheObject updated = req.value(i);

                    if (intercept) {
                        CacheObject old = entry.innerGet(
                             null,
                            /*read swap*/true,
                            /*read through*/ctx.loadPreviousValue(),
                            /*fail fast*/false,
                            /*unmarshal*/true,
                            /*metrics*/true,
                            /*event*/true,
                            /*temporary*/true,
                            req.subjectId(),
                            null,
                            taskName,
                            null);

                        Object val = ctx.config().getInterceptor().onBeforePut(new CacheLazyEntry(ctx, entry.key(),
                            old),
                            updated.value(ctx.cacheObjectContext(), false));

                        if (val == null)
                            continue;

                        updated = ctx.toCacheObject(ctx.unwrapTemporary(val));
                    }

                    assert updated != null;

                    if (putMap == null) {
                        putMap = new LinkedHashMap<>(size, 1.0f);
                        writeVals = new ArrayList<>(size);
                    }

                    putMap.put(CU.value(entry.key(), ctx, false), CU.value(updated, ctx, false));
                    writeVals.add(updated);
                }
                else {
                    assert op == DELETE;

                    if (intercept) {
                        CacheObject old = entry.innerGet(
                            null,
                            /*read swap*/true,
                            /*read through*/ctx.loadPreviousValue(),
                            /*fail fast*/false,
                            /*unmarshal*/true,
                            /*metrics*/true,
                            /*event*/true,
                            /*temporary*/true,
                            req.subjectId(),
                            null,
                            taskName,
                            null);

                        IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor()
                            .onBeforeRemove(new CacheLazyEntry(ctx, entry.key(), old));

                        if (ctx.cancelRemove(interceptorRes))
                            continue;
                    }

                    if (rmvKeys == null)
                        rmvKeys = new ArrayList<>(size);

                    rmvKeys.add(entry.key().value(ctx.cacheObjectContext(), false));
                }

                filtered.add(entry);
            }
            catch (IgniteCheckedException e) {
                res.addFailedKey(entry.key(), e);
            }
        }

        // Store final batch.
        if (putMap != null || rmvKeys != null) {
            dhtFut = updatePartialBatch(
                hasNear,
                firstEntryIdx,
                filtered,
                ver,
                node,
                writeVals,
                putMap,
                rmvKeys,
                entryProcessorMap,
                dhtFut,
                completionCb,
                req,
                res,
                replicate,
                updRes,
                taskName,
                expiry);
        }
        else
            assert filtered.isEmpty();

        updRes.dhtFuture(dhtFut);

        updRes.invokeResult(invokeRes);

        return updRes;
    }

    /**
     * @param entries Entries.
     * @throws IgniteCheckedException If failed.
     */
    private void reloadIfNeeded(final List<GridDhtCacheEntry> entries) throws IgniteCheckedException {
        Map<KeyCacheObject, Integer> needReload = null;

        for (int i = 0; i < entries.size(); i++) {
            GridDhtCacheEntry entry = entries.get(i);

            if (entry == null)
                continue;

            CacheObject val = entry.rawGetOrUnmarshal(false);

            if (val == null) {
                if (needReload == null)
                    needReload = new HashMap<>(entries.size(), 1.0f);

                needReload.put(entry.key(), i);
            }
        }

        if (needReload != null) {
            final Map<KeyCacheObject, Integer> idxMap = needReload;

            ctx.store().loadAll(null, needReload.keySet(), new CI2<KeyCacheObject, Object>() {
                @Override public void apply(KeyCacheObject k, Object v) {
                    Integer idx = idxMap.get(k);

                    if (idx != null) {
                        GridDhtCacheEntry entry = entries.get(idx);
                        try {
                            GridCacheVersion ver = entry.version();

                            entry.versionedValue(ctx.toCacheObject(v), null, ver);
                        }
                        catch (GridCacheEntryRemovedException e) {
                            assert false : "Entry should not get obsolete while holding lock [entry=" + entry +
                                ", e=" + e + ']';
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                }
            });
        }
    }

    /**
     * Updates locked entries one-by-one.
     *
     * @param node Originating node.
     * @param hasNear {@code True} if originating node has near cache.
     * @param req Update request.
     * @param res Update response.
     * @param locked Locked entries.
     * @param ver Assigned update version.
     * @param dhtFut Optional DHT future.
     * @param completionCb Completion callback to invoke when DHT future is completed.
     * @param replicate Whether DR is enabled for that cache.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @return Return value.
     * @throws GridCacheEntryRemovedException Should be never thrown.
     */
    private UpdateSingleResult updateSingle(
        ClusterNode node,
        boolean hasNear,
        GridNearAtomicUpdateRequest req,
        GridNearAtomicUpdateResponse res,
        List<GridDhtCacheEntry> locked,
        GridCacheVersion ver,
        @Nullable GridDhtAtomicUpdateFuture dhtFut,
        CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        boolean replicate,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiry
    ) throws GridCacheEntryRemovedException {
        GridCacheReturn retVal = null;
        Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted = null;

        List<KeyCacheObject> keys = req.keys();

        AffinityTopologyVersion topVer = req.topologyVersion();

        boolean checkReaders = hasNear || ctx.discovery().hasNearCache(name(), topVer);

        boolean readersOnly = false;

        boolean intercept = ctx.config().getInterceptor() != null;

        // Avoid iterator creation.
        for (int i = 0; i < keys.size(); i++) {
            KeyCacheObject k = keys.get(i);

            GridCacheOperation op = req.operation();

            // We are holding java-level locks on entries at this point.
            // No GridCacheEntryRemovedException can be thrown.
            try {
                GridDhtCacheEntry entry = locked.get(i);

                if (entry == null)
                    continue;

                GridCacheVersion newConflictVer = req.conflictVersion(i);
                long newConflictTtl = req.conflictTtl(i);
                long newConflictExpireTime = req.conflictExpireTime(i);

                assert !(newConflictVer instanceof GridCacheVersionEx) : newConflictVer;

                boolean primary = !req.fastMap() || ctx.affinity().primary(ctx.localNode(), entry.key(),
                    req.topologyVersion());

                Object writeVal = op == TRANSFORM ? req.entryProcessor(i) : req.writeValue(i);

                Collection<UUID> readers = null;
                Collection<UUID> filteredReaders = null;

                if (checkReaders) {
                    readers = entry.readers();
                    filteredReaders = F.view(entry.readers(), F.notEqualTo(node.id()));
                }

                GridCacheUpdateAtomicResult updRes = entry.innerUpdate(
                    ver,
                    node.id(),
                    locNodeId,
                    op,
                    writeVal,
                    req.invokeArguments(),
                    primary && writeThrough() && !req.skipStore(),
                    !req.skipStore(),
                    req.returnValue(),
                    expiry,
                    true,
                    true,
                    primary,
                    ctx.config().getAtomicWriteOrderMode() == CLOCK, // Check version in CLOCK mode on primary node.
                    topVer,
                    req.filter(),
                    replicate ? primary ? DR_PRIMARY : DR_BACKUP : DR_NONE,
                    newConflictTtl,
                    newConflictExpireTime,
                    newConflictVer,
                    true,
                    intercept,
                    req.subjectId(),
                    taskName);

                if (dhtFut == null && !F.isEmpty(filteredReaders)) {
                    dhtFut = createDhtFuture(ver, req, res, completionCb, true);

                    readersOnly = true;
                }

                if (dhtFut != null) {
                    if (updRes.sendToDht()) { // Send to backups even in case of remove-remove scenarios.
                        GridCacheVersionConflictContext<?, ?> conflictCtx = updRes.conflictResolveResult();

                        if (conflictCtx == null)
                            newConflictVer = null;
                        else if (conflictCtx.isMerge())
                            newConflictVer = null; // Conflict version is discarded in case of merge.

                        EntryProcessor<Object, Object, Object> entryProcessor = null;

                        if (!readersOnly) {
                            dhtFut.addWriteEntry(entry,
                                updRes.newValue(),
                                entryProcessor,
                                updRes.newTtl(),
                                updRes.conflictExpireTime(),
                                newConflictVer);
                        }

                        if (!F.isEmpty(filteredReaders))
                            dhtFut.addNearWriteEntries(filteredReaders,
                                entry,
                                updRes.newValue(),
                                entryProcessor,
                                updRes.newTtl(),
                                updRes.conflictExpireTime());
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Entry did not pass the filter or conflict resolution (will skip write) " +
                                "[entry=" + entry + ", filter=" + Arrays.toString(req.filter()) + ']');
                    }
                }

                if (hasNear) {
                    if (primary && updRes.sendToDht()) {
                        if (!ctx.affinity().belongs(node, entry.partition(), topVer)) {
                            // If put the same value as in request then do not need to send it back.
                            if (op == TRANSFORM || writeVal != updRes.newValue()) {
                                res.addNearValue(i,
                                    updRes.newValue(),
                                    updRes.newTtl(),
                                    updRes.conflictExpireTime());
                            }
                            else
                                res.addNearTtl(i, updRes.newTtl(), updRes.conflictExpireTime());

                            if (updRes.newValue() != null) {
                                IgniteInternalFuture<Boolean> f = entry.addReader(node.id(), req.messageId(), topVer);

                                assert f == null : f;
                            }
                        }
                        else if (F.contains(readers, node.id())) // Reader became primary or backup.
                            entry.removeReader(node.id(), req.messageId());
                        else
                            res.addSkippedIndex(i);
                    }
                    else
                        res.addSkippedIndex(i);
                }

                if (updRes.removeVersion() != null) {
                    if (deleted == null)
                        deleted = new ArrayList<>(keys.size());

                    deleted.add(F.t(entry, updRes.removeVersion()));
                }

                if (op == TRANSFORM) {
                    assert !req.returnValue();

                    IgniteBiTuple<Object, Exception> compRes = updRes.computedResult();

                    if (compRes != null && (compRes.get1() != null || compRes.get2() != null)) {
                        if (retVal == null)
                            retVal = new GridCacheReturn(node.isLocal());

                        retVal.addEntryProcessResult(ctx,
                            k,
                            null,
                            compRes.get1(),
                            compRes.get2());
                    }
                }
                else {
                    // Create only once.
                    if (retVal == null) {
                        CacheObject ret = updRes.oldValue();

                        retVal = new GridCacheReturn(ctx,
                            node.isLocal(),
                            req.returnValue() ? ret : null,
                            updRes.success());
                    }
                }
            }
            catch (IgniteCheckedException e) {
                res.addFailedKey(k, e);
            }
        }

        return new UpdateSingleResult(retVal, deleted, dhtFut);
    }

    /**
     * @param hasNear {@code True} if originating node has near cache.
     * @param firstEntryIdx Index of the first entry in the request keys collection.
     * @param entries Entries to update.
     * @param ver Version to set.
     * @param node Originating node.
     * @param writeVals Write values.
     * @param putMap Values to put.
     * @param rmvKeys Keys to remove.
     * @param entryProcessorMap Entry processors.
     * @param dhtFut DHT update future if has backups.
     * @param completionCb Completion callback to invoke when DHT future is completed.
     * @param req Request.
     * @param res Response.
     * @param replicate Whether replication is enabled.
     * @param batchRes Batch update result.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @return Deleted entries.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable private GridDhtAtomicUpdateFuture updatePartialBatch(
        boolean hasNear,
        int firstEntryIdx,
        List<GridDhtCacheEntry> entries,
        final GridCacheVersion ver,
        ClusterNode node,
        @Nullable List<CacheObject> writeVals,
        @Nullable Map<Object, Object> putMap,
        @Nullable Collection<Object> rmvKeys,
        @Nullable Map<KeyCacheObject, EntryProcessor<Object, Object, Object>> entryProcessorMap,
        @Nullable GridDhtAtomicUpdateFuture dhtFut,
        CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        final GridNearAtomicUpdateRequest req,
        final GridNearAtomicUpdateResponse res,
        boolean replicate,
        UpdateBatchResult batchRes,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiry
    ) {
        assert putMap == null ^ rmvKeys == null;

        assert req.conflictVersions() == null : "Cannot be called when there are conflict entries in the batch.";

        AffinityTopologyVersion topVer = req.topologyVersion();

        boolean checkReaders = hasNear || ctx.discovery().hasNearCache(name(), topVer);

        CacheStorePartialUpdateException storeErr = null;

        try {
            GridCacheOperation op;

            if (putMap != null) {
                // If fast mapping, filter primary keys for write to store.
                Map<Object, Object> storeMap = req.fastMap() ?
                    F.view(putMap, new P1<Object>() {
                        @Override public boolean apply(Object key) {
                            return ctx.affinity().primary(ctx.localNode(), key, req.topologyVersion());
                        }
                    }) :
                    putMap;

                try {
                    ctx.store().putAll(null, F.viewReadOnly(storeMap, new C1<Object, IgniteBiTuple<Object, GridCacheVersion>>() {
                        @Override public IgniteBiTuple<Object, GridCacheVersion> apply(Object v) {
                            return F.t(v, ver);
                        }
                    }));
                }
                catch (CacheStorePartialUpdateException e) {
                    storeErr = e;
                }

                op = UPDATE;
            }
            else {
                // If fast mapping, filter primary keys for write to store.
                Collection<Object> storeKeys = req.fastMap() ?
                    F.view(rmvKeys, new P1<Object>() {
                        @Override public boolean apply(Object key) {
                            return ctx.affinity().primary(ctx.localNode(), key, req.topologyVersion());
                        }
                    }) :
                    rmvKeys;

                try {
                    ctx.store().removeAll(null, storeKeys);
                }
                catch (CacheStorePartialUpdateException e) {
                    storeErr = e;
                }

                op = DELETE;
            }

            boolean intercept = ctx.config().getInterceptor() != null;

            // Avoid iterator creation.
            for (int i = 0; i < entries.size(); i++) {
                GridDhtCacheEntry entry = entries.get(i);

                assert Thread.holdsLock(entry);

                if (entry.obsolete()) {
                    assert req.operation() == DELETE : "Entry can become obsolete only after remove: " + entry;

                    continue;
                }

                if (storeErr != null &&
                    storeErr.failedKeys().contains(entry.key().value(ctx.cacheObjectContext(), false)))
                    continue;

                try {
                    // We are holding java-level locks on entries at this point.
                    CacheObject writeVal = op == UPDATE ? writeVals.get(i) : null;

                    assert writeVal != null || op == DELETE : "null write value found.";

                    boolean primary = !req.fastMap() || ctx.affinity().primary(ctx.localNode(), entry.key(),
                        req.topologyVersion());

                    Collection<UUID> readers = null;
                    Collection<UUID> filteredReaders = null;

                    if (checkReaders) {
                        readers = entry.readers();
                        filteredReaders = F.view(entry.readers(), F.notEqualTo(node.id()));
                    }

                    GridCacheUpdateAtomicResult updRes = entry.innerUpdate(
                        ver,
                        node.id(),
                        locNodeId,
                        op,
                        writeVal,
                        null,
                        /*write-through*/false,
                        /*read-through*/false,
                        /*retval*/false,
                        expiry,
                        /*event*/true,
                        /*metrics*/true,
                        primary,
                        ctx.config().getAtomicWriteOrderMode() == CLOCK, // Check version in CLOCK mode on primary node.
                        topVer,
                        null,
                        replicate ? primary ? DR_PRIMARY : DR_BACKUP : DR_NONE,
                        CU.TTL_NOT_CHANGED,
                        CU.EXPIRE_TIME_CALCULATE,
                        null,
                        /*conflict resolve*/false,
                        /*intercept*/false,
                        req.subjectId(),
                        taskName);

                    assert !updRes.success() || updRes.newTtl() == CU.TTL_NOT_CHANGED || expiry != null :
                        "success=" + updRes.success() + ", newTtl=" + updRes.newTtl() + ", expiry=" + expiry;

                    if (intercept) {
                        if (op == UPDATE) {
                            ctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(
                                ctx,
                                entry.key(),
                                updRes.newValue()));
                        }
                        else {
                            assert op == DELETE : op;

                            // Old value should be already loaded for 'CacheInterceptor.onBeforeRemove'.
                            ctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(ctx, entry.key(),
                                updRes.oldValue()));
                        }
                    }

                    batchRes.addDeleted(entry, updRes, entries);

                    if (dhtFut == null && !F.isEmpty(filteredReaders)) {
                        dhtFut = createDhtFuture(ver, req, res, completionCb, true);

                        batchRes.readersOnly(true);
                    }

                    if (dhtFut != null) {
                        EntryProcessor<Object, Object, Object> entryProcessor =
                            entryProcessorMap == null ? null : entryProcessorMap.get(entry.key());

                        if (!batchRes.readersOnly())
                            dhtFut.addWriteEntry(entry,
                                writeVal,
                                entryProcessor,
                                updRes.newTtl(),
                                CU.EXPIRE_TIME_CALCULATE,
                                null);

                        if (!F.isEmpty(filteredReaders))
                            dhtFut.addNearWriteEntries(filteredReaders,
                                entry,
                                writeVal,
                                entryProcessor,
                                updRes.newTtl(),
                                CU.EXPIRE_TIME_CALCULATE);
                    }

                    if (hasNear) {
                        if (primary) {
                            if (!ctx.affinity().belongs(node, entry.partition(), topVer)) {
                                int idx = firstEntryIdx + i;

                                if (req.operation() == TRANSFORM) {
                                    res.addNearValue(idx,
                                        writeVal,
                                        updRes.newTtl(),
                                        CU.EXPIRE_TIME_CALCULATE);
                                }
                                else
                                    res.addNearTtl(idx, updRes.newTtl(), CU.EXPIRE_TIME_CALCULATE);

                                if (writeVal != null || entry.hasValue()) {
                                    IgniteInternalFuture<Boolean> f = entry.addReader(node.id(), req.messageId(), topVer);

                                    assert f == null : f;
                                }
                            }
                            else if (readers.contains(node.id())) // Reader became primary or backup.
                                entry.removeReader(node.id(), req.messageId());
                            else
                                res.addSkippedIndex(firstEntryIdx + i);
                        }
                        else
                            res.addSkippedIndex(firstEntryIdx + i);
                    }
                }
                catch (GridCacheEntryRemovedException e) {
                    assert false : "Entry cannot become obsolete while holding lock.";

                    e.printStackTrace();
                }
            }
        }
        catch (IgniteCheckedException e) {
            res.addFailedKeys(putMap != null ? putMap.keySet() : rmvKeys, e, ctx);
        }

        if (storeErr != null)
            res.addFailedKeys(storeErr.failedKeys(), storeErr.getCause(), ctx);

        return dhtFut;
    }

    /**
     * Acquires java-level locks on cache entries. Returns collection of locked entries.
     *
     * @param keys Keys to lock.
     * @param topVer Topology version to lock on.
     * @return Collection of locked entries.
     * @throws GridDhtInvalidPartitionException If entry does not belong to local node. If exception is thrown,
     *      locks are released.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private List<GridDhtCacheEntry> lockEntries(List<KeyCacheObject> keys, AffinityTopologyVersion topVer)
        throws GridDhtInvalidPartitionException {
        if (keys.size() == 1) {
            KeyCacheObject key = keys.get(0);

            while (true) {
                try {
                    GridDhtCacheEntry entry = entryExx(key, topVer);

                    UNSAFE.monitorEnter(entry);

                    if (entry.obsolete())
                        UNSAFE.monitorExit(entry);
                    else
                        return Collections.singletonList(entry);
                }
                catch (GridDhtInvalidPartitionException e) {
                    // Ignore invalid partition exception in CLOCK ordering mode.
                    if (ctx.config().getAtomicWriteOrderMode() == CLOCK)
                        return Collections.singletonList(null);
                    else
                        throw e;
                }
            }
        }
        else {
            List<GridDhtCacheEntry> locked = new ArrayList<>(keys.size());

            while (true) {
                for (KeyCacheObject key : keys) {
                    try {
                        GridDhtCacheEntry entry = entryExx(key, topVer);

                        locked.add(entry);
                    }
                    catch (GridDhtInvalidPartitionException e) {
                        // Ignore invalid partition exception in CLOCK ordering mode.
                        if (ctx.config().getAtomicWriteOrderMode() == CLOCK)
                            locked.add(null);
                        else
                            throw e;
                    }
                }

                boolean retry = false;

                for (int i = 0; i < locked.size(); i++) {
                    GridCacheMapEntry entry = locked.get(i);

                    if (entry == null)
                        continue;

                    UNSAFE.monitorEnter(entry);

                    if (entry.obsolete()) {
                        // Unlock all locked.
                        for (int j = 0; j <= i; j++) {
                            if (locked.get(j) != null)
                                UNSAFE.monitorExit(locked.get(j));
                        }

                        // Clear entries.
                        locked.clear();

                        // Retry.
                        retry = true;

                        break;
                    }
                }

                if (!retry)
                    return locked;
            }
        }
    }

    /**
     * Releases java-level locks on cache entries.
     *
     * @param locked Locked entries.
     * @param topVer Topology version.
     */
    private void unlockEntries(Collection<GridDhtCacheEntry> locked, AffinityTopologyVersion topVer) {
        // Process deleted entries before locks release.
        assert ctx.deferredDelete(this) : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        Collection<KeyCacheObject> skip = null;

        try {
            for (GridCacheMapEntry entry : locked) {
                if (entry != null && entry.deleted()) {
                    if (skip == null)
                        skip = new HashSet<>(locked.size(), 1.0f);

                    skip.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (GridCacheMapEntry entry : locked) {
                if (entry != null)
                    UNSAFE.monitorExit(entry);
            }
        }

        // Try evict partitions.
        for (GridDhtCacheEntry entry : locked) {
            if (entry != null)
                entry.onUnlock();
        }

        if (skip != null && skip.size() == locked.size())
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (GridCacheMapEntry entry : locked) {
            if (entry != null && (skip == null || !skip.contains(entry.key())))
                ctx.evicts().touch(entry, topVer);
        }
    }

    /**
     * @param entry Entry to check.
     * @param req Update request.
     * @param res Update response. If filter evaluation failed, key will be added to failed keys and method
     *      will return false.
     * @return {@code True} if filter evaluation succeeded.
     */
    private boolean checkFilter(GridCacheEntryEx entry, GridNearAtomicUpdateRequest req,
        GridNearAtomicUpdateResponse res) {
        try {
            return ctx.isAllLocked(entry, req.filter());
        }
        catch (IgniteCheckedException e) {
            res.addFailedKey(entry.key(), e);

            return false;
        }
    }

    /**
     * @param req Request to remap.
     */
    private void remapToNewPrimary(GridNearAtomicUpdateRequest req) {
        if (log.isDebugEnabled())
            log.debug("Remapping near update request locally: " + req);

        Collection<?> vals;
        Collection<GridCacheDrInfo> drPutVals;
        Collection<GridCacheVersion> drRmvVals;

        if (req.conflictVersions() == null) {
            vals = req.values();

            drPutVals = null;
            drRmvVals = null;
        }
        else if (req.operation() == UPDATE) {
            int size = req.keys().size();

            drPutVals = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                long ttl = req.conflictTtl(i);

                if (ttl == CU.TTL_NOT_CHANGED)
                    drPutVals.add(new GridCacheDrInfo(req.value(i), req.conflictVersion(i)));
                else
                    drPutVals.add(new GridCacheDrExpirationInfo(req.value(i), req.conflictVersion(i), ttl,
                        req.conflictExpireTime(i)));
            }

            vals = null;
            drRmvVals = null;
        }
        else {
            assert req.operation() == DELETE;

            drRmvVals = req.conflictVersions();

            vals = null;
            drPutVals = null;
        }

        final GridNearAtomicUpdateFuture updateFut = new GridNearAtomicUpdateFuture(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            req.operation(),
            req.keys(),
            vals,
            req.invokeArguments(),
            drPutVals,
            drRmvVals,
            req.returnValue(),
            false,
            req.expiry(),
            req.filter(),
            req.subjectId(),
            req.taskNameHash(),
            req.skipStore(),
            MAX_RETRIES,
            true);

        updateFut.map();
    }

    /**
     * Creates backup update future if necessary.
     *
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     * @param completionCb Completion callback to invoke when future is completed.
     * @param force If {@code true} then creates future without optimizations checks.
     * @return Backup update future or {@code null} if there are no backups.
     */
    @Nullable private GridDhtAtomicUpdateFuture createDhtFuture(
        GridCacheVersion writeVer,
        GridNearAtomicUpdateRequest updateReq,
        GridNearAtomicUpdateResponse updateRes,
        CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        boolean force
    ) {
        if (!force) {
            if (updateReq.fastMap())
                return null;

            AffinityTopologyVersion topVer = updateReq.topologyVersion();

            Collection<ClusterNode> nodes = ctx.kernalContext().discovery().cacheAffinityNodes(name(), topVer);

            // We are on primary node for some key.
            assert !nodes.isEmpty() : "Failed to find affinity nodes [name=" + name() + ", topVer=" + topVer +
                ctx.kernalContext().discovery().discoCache(topVer) + ']';

            if (nodes.size() == 1) {
                if (log.isDebugEnabled())
                    log.debug("Partitioned cache topology has only one node, will not create DHT atomic update future " +
                        "[topVer=" + topVer + ", updateReq=" + updateReq + ']');

                return null;
            }
        }

        return new GridDhtAtomicUpdateFuture(ctx, completionCb, writeVer, updateReq, updateRes);
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near get response.
     */
    private void processNearGetResponse(UUID nodeId, GridNearGetResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing near get response [nodeId=" + nodeId + ", res=" + res + ']');

        GridPartitionedGetFuture<K, V> fut = (GridPartitionedGetFuture<K, V>)ctx.mvcc().<Map<K, V>>future(
            res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Near atomic update request.
     */
    private void processNearAtomicUpdateRequest(UUID nodeId, GridNearAtomicUpdateRequest req) {
        if (log.isDebugEnabled())
            log.debug("Processing near atomic update request [nodeId=" + nodeId + ", req=" + req + ']');

        req.nodeId(ctx.localNodeId());

        updateAllAsyncInternal(nodeId, req, updateReplyClos);
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near atomic update response.
     */
    @SuppressWarnings("unchecked")
    private void processNearAtomicUpdateResponse(UUID nodeId, GridNearAtomicUpdateResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing near atomic update response [nodeId=" + nodeId + ", res=" + res + ']');

        res.nodeId(ctx.localNodeId());

        GridNearAtomicUpdateFuture fut = (GridNearAtomicUpdateFuture)ctx.mvcc().atomicFuture(res.futureVersion());

        if (fut != null)
            fut.onResult(nodeId, res);
        else
            U.warn(log, "Failed to find near update future for update response (will ignore) " +
                "[nodeId=" + nodeId + ", res=" + res + ']');
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Dht atomic update request.
     */
    private void processDhtAtomicUpdateRequest(UUID nodeId, GridDhtAtomicUpdateRequest req) {
        if (log.isDebugEnabled())
            log.debug("Processing dht atomic update request [nodeId=" + nodeId + ", req=" + req + ']');

        GridCacheVersion ver = req.writeVersion();

        // Always send update reply.
        GridDhtAtomicUpdateResponse res = new GridDhtAtomicUpdateResponse(ctx.cacheId(), req.futureVersion());

        Boolean replicate = ctx.isDrEnabled();

        boolean intercept = req.forceTransformBackups() && ctx.config().getInterceptor() != null;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        for (int i = 0; i < req.size(); i++) {
            KeyCacheObject key = req.key(i);

            try {
                while (true) {
                    GridDhtCacheEntry entry = null;

                    try {
                        entry = entryExx(key);

                        CacheObject val = req.value(i);
                        EntryProcessor<Object, Object, Object> entryProcessor = req.entryProcessor(i);

                        GridCacheOperation op = entryProcessor != null ? TRANSFORM :
                            (val != null) ? UPDATE : DELETE;

                        long ttl = req.ttl(i);
                        long expireTime = req.conflictExpireTime(i);

                        GridCacheUpdateAtomicResult updRes = entry.innerUpdate(
                            ver,
                            nodeId,
                            nodeId,
                            op,
                            op == TRANSFORM ? entryProcessor : val,
                            op == TRANSFORM ? req.invokeArguments() : null,
                            /*write-through*/false,
                            /*read-through*/false,
                            /*retval*/false,
                            /*expiry policy*/null,
                            /*event*/true,
                            /*metrics*/true,
                            /*primary*/false,
                            /*check version*/!req.forceTransformBackups(),
                            req.topologyVersion(),
                            CU.empty0(),
                            replicate ? DR_BACKUP : DR_NONE,
                            ttl,
                            expireTime,
                            req.conflictVersion(i),
                            false,
                            intercept,
                            req.subjectId(),
                            taskName);

                        if (updRes.removeVersion() != null)
                            ctx.onDeferredDelete(entry, updRes.removeVersion());

                        entry.onUnlock();

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry while updating backup value (will retry): " + key);

                        entry = null;
                    }
                    finally {
                        if (entry != null)
                            ctx.evicts().touch(entry, req.topologyVersion());
                    }
                }
            }
            catch (GridDhtInvalidPartitionException ignored) {
                // Ignore.
            }
            catch (IgniteCheckedException e) {
                res.addFailedKey(key, new IgniteCheckedException("Failed to update key on backup node: " + key, e));
            }
        }

        if (isNearEnabled(cacheCfg))
            ((GridNearAtomicCache<K, V>)near()).processDhtAtomicUpdateRequest(nodeId, req, res);

        try {
            if (res.failedKeys() != null || res.nearEvicted() != null || req.writeSynchronizationMode() == FULL_SYNC)
                ctx.io().send(nodeId, res, ctx.ioPolicy());
            else {
                // No failed keys and sync mode is not FULL_SYNC, thus sending deferred response.
                sendDeferredUpdateResponse(nodeId, req.futureVersion());
            }
        }
        catch (ClusterTopologyCheckedException ignored) {
            U.warn(log, "Failed to send DHT atomic update response to node because it left grid: " +
                req.nodeId());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send DHT atomic update response (did node leave grid?) [nodeId=" + nodeId +
                ", req=" + req + ']', e);
        }
    }

    /**
     * @param nodeId Node ID to send message to.
     * @param ver Version to ack.
     */
    private void sendDeferredUpdateResponse(UUID nodeId, GridCacheVersion ver) {
        while (true) {
            DeferredResponseBuffer buf = pendingResponses.get(nodeId);

            if (buf == null) {
                buf = new DeferredResponseBuffer(nodeId);

                DeferredResponseBuffer old = pendingResponses.putIfAbsent(nodeId, buf);

                if (old == null) {
                    // We have successfully added buffer to map.
                    ctx.time().addTimeoutObject(buf);
                }
                else
                    buf = old;
            }

            if (!buf.addResponse(ver))
                // Some thread is sending filled up buffer, we can remove it.
                pendingResponses.remove(nodeId, buf);
            else
                break;
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Dht atomic update response.
     */
    @SuppressWarnings("unchecked")
    private void processDhtAtomicUpdateResponse(UUID nodeId, GridDhtAtomicUpdateResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing dht atomic update response [nodeId=" + nodeId + ", res=" + res + ']');

        GridDhtAtomicUpdateFuture updateFut = (GridDhtAtomicUpdateFuture)ctx.mvcc().
            atomicFuture(res.futureVersion());

        if (updateFut != null)
            updateFut.onResult(nodeId, res);
        else
            U.warn(log, "Failed to find DHT update future for update response [nodeId=" + nodeId +
                ", res=" + res + ']');
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Deferred atomic update response.
     */
    @SuppressWarnings("unchecked")
    private void processDhtAtomicDeferredUpdateResponse(UUID nodeId, GridDhtAtomicDeferredUpdateResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing deferred dht atomic update response [nodeId=" + nodeId + ", res=" + res + ']');

        for (GridCacheVersion ver : res.futureVersions()) {
            GridDhtAtomicUpdateFuture updateFut = (GridDhtAtomicUpdateFuture)ctx.mvcc().atomicFuture(ver);

            if (updateFut != null)
                updateFut.onResult(nodeId);
            else
                U.warn(log, "Failed to find DHT update future for deferred update response [nodeId=" +
                    nodeId + ", ver=" + ver + ", res=" + res + ']');
        }
    }

    /**
     * @param nodeId Originating node ID.
     * @param res Near update response.
     */
    private void sendNearUpdateReply(UUID nodeId, GridNearAtomicUpdateResponse res) {
        try {
            ctx.io().send(nodeId, res, ctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException ignored) {
            U.warn(log, "Failed to send near update reply to node because it left grid: " +
                nodeId);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send near update reply (did node leave grid?) [nodeId=" + nodeId +
                ", res=" + res + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicCache.class, this, super.toString());
    }

    /**
     * Result of {@link GridDhtAtomicCache#updateSingle} execution.
     */
    private static class UpdateSingleResult {
        /** */
        private final GridCacheReturn retVal;

        /** */
        private final Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted;

        /** */
        private final GridDhtAtomicUpdateFuture dhtFut;

        /**
         * @param retVal Return value.
         * @param deleted Deleted entries.
         * @param dhtFut DHT future.
         */
        private UpdateSingleResult(GridCacheReturn retVal,
            Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted,
            GridDhtAtomicUpdateFuture dhtFut) {
            this.retVal = retVal;
            this.deleted = deleted;
            this.dhtFut = dhtFut;
        }

        /**
         * @return Return value.
         */
        private GridCacheReturn returnValue() {
            return retVal;
        }

        /**
         * @return Deleted entries.
         */
        private Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted() {
            return deleted;
        }

        /**
         * @return DHT future.
         */
        public GridDhtAtomicUpdateFuture dhtFuture() {
            return dhtFut;
        }
    }

    /**
     * Result of {@link GridDhtAtomicCache#updateWithBatch} execution.
     */
    private static class UpdateBatchResult {
        /** */
        private Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted;

        /** */
        private GridDhtAtomicUpdateFuture dhtFut;

        /** */
        private boolean readersOnly;

        /** */
        private GridCacheReturn invokeRes;

        /**
         * @param entry Entry.
         * @param updRes Entry update result.
         * @param entries All entries.
         */
        private void addDeleted(GridDhtCacheEntry entry,
            GridCacheUpdateAtomicResult updRes,
            Collection<GridDhtCacheEntry> entries) {
            if (updRes.removeVersion() != null) {
                if (deleted == null)
                    deleted = new ArrayList<>(entries.size());

                deleted.add(F.t(entry, updRes.removeVersion()));
            }
        }

        /**
         * @return Deleted entries.
         */
        private Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted() {
            return deleted;
        }

        /**
         * @return DHT future.
         */
        public GridDhtAtomicUpdateFuture dhtFuture() {
            return dhtFut;
        }

        /**
         * @param invokeRes Result for invoke operation.
         */
        private void invokeResult(GridCacheReturn invokeRes) {
            this.invokeRes = invokeRes;
        }

        /**
         * @return Result for invoke operation.
         */
        GridCacheReturn invokeResults() {
            return invokeRes;
        }

        /**
         * @param dhtFut DHT future.
         */
        private void dhtFuture(@Nullable GridDhtAtomicUpdateFuture dhtFut) {
            this.dhtFut = dhtFut;
        }

        /**
         * @return {@code True} if only readers (not backups) should be updated.
         */
        private boolean readersOnly() {
            return readersOnly;
        }

        /**
         * @param readersOnly {@code True} if only readers (not backups) should be updated.
         */
        private void readersOnly(boolean readersOnly) {
            this.readersOnly = readersOnly;
        }
    }

    /**
     *
     */
    private static class FinishedLockFuture extends GridFinishedFuture<Boolean> implements GridDhtFuture<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public FinishedLockFuture() {
            // No-op.
        }

        /**
         * @param err Error.
         */
        private FinishedLockFuture(Throwable err) {
            super(err);
        }

        /** {@inheritDoc} */
        @Override public Collection<Integer> invalidPartitions() {
            return Collections.emptyList();
        }
    }

    /**
     * Deferred response buffer.
     */
    private class DeferredResponseBuffer extends ReentrantReadWriteLock implements GridTimeoutObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** Filled atomic flag. */
        private AtomicBoolean guard = new AtomicBoolean(false);

        /** Response versions. */
        private Collection<GridCacheVersion> respVers = new ConcurrentLinkedDeque8<>();

        /** Node ID. */
        private final UUID nodeId;

        /** Timeout ID. */
        private final IgniteUuid timeoutId;

        /** End time. */
        private final long endTime;

        /**
         * @param nodeId Node ID to send message to.
         */
        private DeferredResponseBuffer(UUID nodeId) {
            this.nodeId = nodeId;

            timeoutId = IgniteUuid.fromUuid(nodeId);

            endTime = U.currentTimeMillis() + DEFERRED_UPDATE_RESPONSE_TIMEOUT;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (guard.compareAndSet(false, true)) {
                ctx.closures().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        writeLock().lock();

                        try {
                            finish();
                        }
                        finally {
                            writeLock().unlock();
                        }
                    }
                });
            }
        }

        /**
         * Adds deferred response to buffer.
         *
         * @param ver Version to send.
         * @return {@code True} if response was handled, {@code false} if this buffer is filled and cannot be used.
         */
        public boolean addResponse(GridCacheVersion ver) {
            readLock().lock();

            boolean snd = false;

            try {
                if (guard.get())
                    return false;

                respVers.add(ver);

                if  (respVers.size() > DEFERRED_UPDATE_RESPONSE_BUFFER_SIZE && guard.compareAndSet(false, true))
                    snd = true;
            }
            finally {
                readLock().unlock();
            }

            if (snd) {
                // Wait all threads in read lock to finish.
                writeLock().lock();

                try {
                    finish();

                    ctx.time().removeTimeoutObject(this);
                }
                finally {
                    writeLock().unlock();
                }
            }

            return true;
        }

        /**
         * Sends deferred notification message and removes this buffer from pending responses map.
         */
        private void finish() {
            GridDhtAtomicDeferredUpdateResponse msg = new GridDhtAtomicDeferredUpdateResponse(ctx.cacheId(),
                respVers);

            try {
                ctx.kernalContext().gateway().readLock();

                try {
                    ctx.io().send(nodeId, msg, ctx.ioPolicy());
                }
                finally {
                    ctx.kernalContext().gateway().readUnlock();
                }
            }
            catch (IllegalStateException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send deferred dht update response to remote node (grid is stopping) " +
                        "[nodeId=" + nodeId + ", msg=" + msg + ']');
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send deferred dht update response to remote node (did node leave grid?) " +
                        "[nodeId=" + nodeId + ", msg=" + msg + ']');
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send deferred dht update response to remote node [nodeId="
                    + nodeId + ", msg=" + msg + ']', e);
            }

            pendingResponses.remove(nodeId, this);
        }
    }
}
