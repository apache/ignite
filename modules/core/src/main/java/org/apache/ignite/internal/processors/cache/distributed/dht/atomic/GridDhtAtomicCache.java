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

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheInvokeResult;
import org.apache.ignite.internal.processors.cache.CacheLazyEntry;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.CacheStorePartialUpdateException;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioBackPressureControl;
import org.apache.ignite.internal.util.nio.GridNioMessageTracker;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_DEFERRED_ACK_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_BACKUP;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRIMARY;

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

    /** */
    private final ThreadLocal<Map<UUID, GridDhtAtomicDeferredUpdateResponse>> defRes =
        new ThreadLocal<Map<UUID, GridDhtAtomicDeferredUpdateResponse>>() {
            @Override protected Map<UUID, GridDhtAtomicDeferredUpdateResponse> initialValue() {
                return new HashMap<>();
            }
        };

    /** Update reply closure. */
    @GridToStringExclude
    private UpdateReplyClosure updateReplyClos;

    /** */
    private GridNearAtomicCache<K, V> near;

    /** Logger. */
    private IgniteLogger msgLog;

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

        msgLog = ctx.shared().atomicMessageLogger();
    }

    /**
     * @param ctx Cache context.
     * @param map Cache concurrent map.
     */
    public GridDhtAtomicCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);

        msgLog = ctx.shared().atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override protected void checkJta() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isDhtAtomic() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        super.init();

        updateReplyClos = new UpdateReplyClosure() {
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override public void apply(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
                if (req.writeSynchronizationMode() != FULL_ASYNC)
                    sendNearUpdateReply(res.nodeId(), res);
                else {
                    if (res.remapTopologyVersion() != null)
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

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridNearGetRequest.class,
            new CI2<UUID, GridNearGetRequest>() {
                @Override public void apply(
                    UUID nodeId,
                    GridNearGetRequest req
                ) {
                    processNearGetRequest(
                        nodeId,
                        req);
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridNearSingleGetRequest.class,
            new CI2<UUID, GridNearSingleGetRequest>() {
                @Override public void apply(
                    UUID nodeId,
                    GridNearSingleGetRequest req
                ) {
                    processNearSingleGetRequest(
                        nodeId,
                        req);
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridNearAtomicAbstractUpdateRequest.class,
            new CI2<UUID, GridNearAtomicAbstractUpdateRequest>() {
                @Override public void apply(
                    UUID nodeId,
                    GridNearAtomicAbstractUpdateRequest req
                ) {
                    processNearAtomicUpdateRequest(
                        nodeId,
                        req);
                }

                @Override public String toString() {
                    return "GridNearAtomicAbstractUpdateRequest handler " +
                        "[msgIdx=" + GridNearAtomicAbstractUpdateRequest.CACHE_MSG_IDX + ']';
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridNearAtomicUpdateResponse.class,
            new CI2<UUID, GridNearAtomicUpdateResponse>() {
                @Override public void apply(
                    UUID nodeId,
                    GridNearAtomicUpdateResponse res
                ) {
                    processNearAtomicUpdateResponse(
                        nodeId,
                        res);
                }

                @Override public String toString() {
                    return "GridNearAtomicUpdateResponse handler " +
                        "[msgIdx=" + GridNearAtomicUpdateResponse.CACHE_MSG_IDX + ']';
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridDhtAtomicAbstractUpdateRequest.class,
            new CI2<UUID, GridDhtAtomicAbstractUpdateRequest>() {
                @Override public void apply(
                    UUID nodeId,
                    GridDhtAtomicAbstractUpdateRequest req
                ) {
                    processDhtAtomicUpdateRequest(
                        nodeId,
                        req);
                }

                @Override public String toString() {
                    return "GridDhtAtomicUpdateRequest handler " +
                        "[msgIdx=" + GridDhtAtomicUpdateRequest.CACHE_MSG_IDX + ']';
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridDhtAtomicUpdateResponse.class,
            new CI2<UUID, GridDhtAtomicUpdateResponse>() {
                @Override public void apply(
                    UUID nodeId,
                    GridDhtAtomicUpdateResponse res
                ) {
                    processDhtAtomicUpdateResponse(
                        nodeId,
                        res);
                }

                @Override public String toString() {
                    return "GridDhtAtomicUpdateResponse handler " +
                        "[msgIdx=" + GridDhtAtomicUpdateResponse.CACHE_MSG_IDX + ']';
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridDhtAtomicDeferredUpdateResponse.class,
            new CI2<UUID, GridDhtAtomicDeferredUpdateResponse>() {
                @Override public void apply(
                    UUID nodeId,
                    GridDhtAtomicDeferredUpdateResponse res
                ) {
                    processDhtAtomicDeferredUpdateResponse(
                        nodeId,
                        res);
                }

                @Override public String toString() {
                    return "GridDhtAtomicDeferredUpdateResponse handler " +
                        "[msgIdx=" + GridDhtAtomicDeferredUpdateResponse.CACHE_MSG_IDX + ']';
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridDhtAtomicNearResponse.class,
            new CI2<UUID, GridDhtAtomicNearResponse>() {
            @Override public void apply(UUID uuid, GridDhtAtomicNearResponse msg) {
                processDhtAtomicNearResponse(uuid, msg);
            }

            @Override public String toString() {
                return "GridDhtAtomicNearResponse handler " +
                    "[msgIdx=" + GridDhtAtomicNearResponse.CACHE_MSG_IDX + ']';
            }
        });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridNearAtomicCheckUpdateRequest.class,
            new CI2<UUID, GridNearAtomicCheckUpdateRequest>() {
                @Override public void apply(UUID uuid, GridNearAtomicCheckUpdateRequest msg) {
                    processCheckUpdateRequest(uuid, msg);
                }

                @Override public String toString() {
                    return "GridNearAtomicCheckUpdateRequest handler " +
                        "[msgIdx=" + GridNearAtomicCheckUpdateRequest.CACHE_MSG_IDX + ']';
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridDhtForceKeysRequest.class,
            new MessageHandler<GridDhtForceKeysRequest>() {
                @Override public void onMessage(ClusterNode node, GridDhtForceKeysRequest msg) {
                    processForceKeysRequest(node, msg);
                }
            });

        ctx.io().addCacheHandler(
            ctx.cacheId(),
            GridDhtForceKeysResponse.class,
            new MessageHandler<GridDhtForceKeysResponse>() {
                @Override public void onMessage(ClusterNode node, GridDhtForceKeysResponse msg) {
                    processForceKeyResponse(node, msg);
                }
            });

        if (near == null) {
            ctx.io().addCacheHandler(
                ctx.cacheId(),
                GridNearGetResponse.class,
                new CI2<UUID, GridNearGetResponse>() {
                    @Override public void apply(
                        UUID nodeId,
                        GridNearGetResponse res
                    ) {
                        processNearGetResponse(
                            nodeId,
                            res);
                    }
                });

            ctx.io().addCacheHandler(
                ctx.cacheId(),
                GridNearSingleGetResponse.class,
                new CI2<UUID, GridNearSingleGetResponse>() {
                    @Override public void apply(
                        UUID nodeId,
                        GridNearSingleGetResponse res
                    ) {
                        processNearSingleGetResponse(
                            nodeId,
                            res);
                    }
                });
        }
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

    /**
     *  @param key Key.
     *  @param deserializeBinary Deserialize binary.
     *  @param needVer Need version.
     *  @return Value.
     *  @throws IgniteCheckedException If failed.
     */
    @Nullable public V get0(K key, boolean deserializeBinary, boolean needVer) throws IgniteCheckedException {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (keyCheck)
            validateCacheKey(key);

        String taskName = ctx.kernalContext().job().currentTaskName();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        UUID subjId = ctx.subjectIdPerCall(null, opCtx);

        final ExpiryPolicy expiryPlc = opCtx != null ? opCtx.expiry() : null;

        final boolean skipStore = opCtx != null && opCtx.skipStore();

        try {
            return getAsync0(ctx.toCacheKeyObject(key),
                !ctx.config().isReadFromBackup(),
                subjId,
                taskName,
                deserializeBinary,
                opCtx != null && opCtx.recovery(),
                expiryPlc,
                false,
                skipStore,
                true,
                needVer).get();
        }
        catch (IgniteException e) {
            if (e.getCause(IgniteCheckedException.class) != null)
                throw e.getCause(IgniteCheckedException.class);
            else
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<V> getAsync(
        final K key,
        final boolean forcePrimary,
        final boolean skipTx,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean canRemap,
        final boolean needVer
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (keyCheck)
            validateCacheKey(key);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(null, opCtx);

        final UUID subjId0 = subjId;
        final ExpiryPolicy expiryPlc = skipVals ? null : opCtx != null ? opCtx.expiry() : null;
        final boolean skipStore = opCtx != null && opCtx.skipStore();
        final boolean recovery = opCtx != null && opCtx.recovery();

        return asyncOp(new CO<IgniteInternalFuture<V>>() {
            @Override public IgniteInternalFuture<V> apply() {
                return getAsync0(ctx.toCacheKeyObject(key),
                    forcePrimary,
                    subjId0,
                    taskName,
                    deserializeBinary,
                    recovery,
                    expiryPlc,
                    skipVals,
                    skipStore,
                    canRemap,
                    needVer);
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected Map<K, V> getAll0(Collection<? extends K> keys, boolean deserializeBinary, boolean needVer)
        throws IgniteCheckedException {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        return getAllAsyncInternal(keys,
            !ctx.config().isReadFromBackup(),
            null,
            ctx.kernalContext().job().currentTaskName(),
            deserializeBinary,
            opCtx != null && opCtx.recovery(),
            false,
            true,
            needVer,
            false).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializeBinary,
        final boolean recovery,
        final boolean skipVals,
        final boolean canRemap,
        final boolean needVer
    ) {
        return getAllAsyncInternal(keys,
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            skipVals,
            canRemap,
            needVer,
            true);
    }

    /**
     * @param keys Keys.
     * @param forcePrimary Force primary flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param canRemap Can remap flag.
     * @param needVer Need version flag.
     * @param asyncOp Async operation flag.
     * @return Future.
     */
    private IgniteInternalFuture<Map<K, V>> getAllAsyncInternal(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializeBinary,
        final boolean recovery,
        final boolean skipVals,
        final boolean canRemap,
        final boolean needVer,
        boolean asyncOp
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        final UUID subjId0 = subjId;

        final ExpiryPolicy expiryPlc = skipVals ? null : opCtx != null ? opCtx.expiry() : null;

        final boolean skipStore = opCtx != null && opCtx.skipStore();

        if (asyncOp) {return asyncOp(new CO<IgniteInternalFuture<Map<K, V>>>() {
            @Override public IgniteInternalFuture<Map<K, V>> apply() {
                return getAllAsync0(ctx.cacheKeysView(keys),
                    forcePrimary,
                    subjId0,
                    taskName,
                    deserializeBinary,
                    recovery,
                    expiryPlc,
                    skipVals,
                    skipStore,
                    canRemap,
                    needVer);
            }
        });}
        else {
            return getAllAsync0(ctx.cacheKeysView(keys),
                forcePrimary,
                subjId0,
                taskName,
                deserializeBinary,
                recovery,
                expiryPlc,
                skipVals,
                skipStore,
                canRemap,
                needVer);
        }
    }

    /** {@inheritDoc} */
    @Override protected V getAndPut0(K key, V val, @Nullable CacheEntryPredicate filter) throws IgniteCheckedException {
        return (V)update0(
            key,
            val,
            null,
            null,
            true,
            filter,
            false).get();
    }

    /** {@inheritDoc} */
    @Override protected boolean put0(K key, V val, CacheEntryPredicate filter) throws IgniteCheckedException {
        Boolean res = (Boolean)update0(
            key,
            val,
            null,
            null,
            false,
            filter,
            false).get();

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndPutAsync0(K key, V val, @Nullable CacheEntryPredicate filter) {
        return update0(
            key,
            val,
            null,
            null,
            true,
            filter,
            true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> putAsync0(K key, V val, @Nullable CacheEntryPredicate filter) {
        return update0(
            key,
            val,
            null,
            null,
            false,
            filter,
            true);
    }

    /** {@inheritDoc} */
    @Override protected void putAll0(Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        updateAll0(m,
            null,
            null,
            null,
            null,
            false,
            false,
            UPDATE,
            false).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync0(Map<? extends K, ? extends V> m) {
        return updateAll0(m,
            null,
            null,
            null,
            null,
            false,
            false,
            UPDATE,
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

        return updateAll0(null,
            null,
            null,
            conflictMap,
            null,
            false,
            false,
            UPDATE,
            true);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove0(K key) throws IgniteCheckedException {
        return (V)remove0(key, true, null, false).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndRemoveAsync0(K key) {
        return remove0(key, true, null, true);
    }

    /** {@inheritDoc} */
    @Override protected void removeAll0(Collection<? extends K> keys) throws IgniteCheckedException {
        removeAllAsync0(keys, null, false, false, false).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Object> removeAllAsync0(Collection<? extends K> keys) {
        return removeAllAsync0(keys, null, false, false, true).chain(RET2NULL);
    }

    /** {@inheritDoc} */
    @Override protected boolean remove0(K key, CacheEntryPredicate filter) throws IgniteCheckedException {
        return (Boolean)remove0(key, false, filter, false).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> removeAsync0(K key, @Nullable CacheEntryPredicate filter) {
        return remove0(key, false, filter, true);
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> conflictMap)
        throws IgniteCheckedException {
        removeAllConflictAsync(conflictMap).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> conflictMap) {
        ctx.dr().onReceiveCacheEntriesReceived(conflictMap.size());

        return removeAllAsync0(null, conflictMap, false, false, true);
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
    private <T> IgniteInternalFuture<T> asyncOp(final CO<IgniteInternalFuture<T>> op) {
        IgniteInternalFuture<T> fail = asyncOpAcquire(/*retry*/false);

        if (fail != null)
            return fail;

        IgniteInternalFuture<T> f = op.apply();

        f.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                asyncOpRelease(/*retry*/false);
            }
        });

        return f;
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<Boolean> lockAllAsync(Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable TransactionIsolation isolation,
        long createTtl,
        long accessTtl) {
        return new FinishedLockFuture(new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws IgniteCheckedException {
        IgniteInternalFuture<EntryProcessorResult<T>> invokeFut = invoke0(false, key, entryProcessor, args);

        EntryProcessorResult<T> res = invokeFut.get();

        return res != null ? res : new CacheInvokeResult<T>();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return invokeAll0(false, keys, entryProcessor, args).get();
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return invoke0(true, key, entryProcessor, args);
    }

    /**
     * @param async Async operation flag.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Entry processor arguments.
     * @return Future.
     */
    private <T> IgniteInternalFuture<EntryProcessorResult<T>> invoke0(
        boolean async,
        K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut = update0(
            key,
            null,
            entryProcessor,
            args,
            false,
            null,
            async);

        return fut.chain(new CX1<IgniteInternalFuture<Map<K, EntryProcessorResult<T>>>, EntryProcessorResult<T>>() {
            @Override public EntryProcessorResult<T> applyx(IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut)
                throws IgniteCheckedException {
                Map<K, EntryProcessorResult<T>> resMap = fut.get();

                if (resMap != null) {
                    assert resMap.isEmpty() || resMap.size() == 1 : resMap.size();

                    EntryProcessorResult<T> res = resMap.isEmpty() ? null : resMap.values().iterator().next();

                    if (res instanceof CacheInvokeResult) {
                        CacheInvokeResult invokeRes = (CacheInvokeResult)res;

                        if (invokeRes.result() != null)
                            res = CacheInvokeResult.fromResult((T)ctx.unwrapBinaryIfNeeded(invokeRes.result(),
                                keepBinary, false));
                    }

                    return res;
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
        return invokeAll0(true, keys, entryProcessor, args);
    }

    /**
     * @param async Async operation flag.
     * @param keys Keys.
     * @param entryProcessor Entry processor.
     * @param args Entry processor arguments.
     * @return Future.
     */
    private <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAll0(
        boolean async,
        Set<? extends K> keys,
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

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> resFut = updateAll0(null,
            invokeMap,
            args,
            null,
            null,
            false,
            false,
            TRANSFORM,
            async);

        return resFut.chain(
            new CX1<IgniteInternalFuture<Map<K, EntryProcessorResult<T>>>, Map<K, EntryProcessorResult<T>>>() {
                @Override public Map<K, EntryProcessorResult<T>> applyx(
                    IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut
                ) throws IgniteCheckedException {
                    Map<Object, EntryProcessorResult> resMap = (Map)fut.get();

                    return ctx.unwrapInvokeResult(resMap, keepBinary);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        return (Map<K, EntryProcessorResult<T>>)updateAll0(null,
            map,
            args,
            null,
            null,
            false,
            false,
            TRANSFORM,
            false).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        return updateAll0(null,
            map,
            args,
            null,
            null,
            false,
            false,
            TRANSFORM,
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
     * @param async Async operation flag.
     * @return Completion future.
     */
    @SuppressWarnings("ConstantConditions")
    private IgniteInternalFuture updateAll0(
        @Nullable Map<? extends K, ? extends V> map,
        @Nullable Map<? extends K, ? extends EntryProcessor> invokeMap,
        @Nullable Object[] invokeArgs,
        @Nullable Map<KeyCacheObject, GridCacheDrInfo> conflictPutMap,
        @Nullable Map<KeyCacheObject, GridCacheVersion> conflictRmvMap,
        final boolean retval,
        final boolean rawRetval,
        final GridCacheOperation op,
        boolean async
    ) {
        assert ctx.updatesAllowed();

        if (map != null && keyCheck)
            validateCacheKeys(map.keySet());

        ctx.checkSecurity(SecurityPermission.CACHE_PUT);

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        if (opCtx != null && opCtx.hasDataCenterId()) {
            assert conflictPutMap == null : conflictPutMap;
            assert conflictRmvMap == null : conflictRmvMap;

            if (op == GridCacheOperation.TRANSFORM) {
                assert invokeMap != null : invokeMap;

                conflictPutMap = F.viewReadOnly((Map)invokeMap,
                    new IgniteClosure<EntryProcessor, GridCacheDrInfo>() {
                        @Override public GridCacheDrInfo apply(EntryProcessor o) {
                            return new GridCacheDrInfo(o, ctx.versions().next(opCtx.dataCenterId()));
                        }
                    });

                invokeMap = null;
            }
            else if (op == GridCacheOperation.DELETE) {
                assert map != null : map;

                conflictRmvMap = F.viewReadOnly((Map)map, new IgniteClosure<V, GridCacheVersion>() {
                    @Override public GridCacheVersion apply(V o) {
                        return ctx.versions().next(opCtx.dataCenterId());
                    }
                });

                map = null;
            }
            else {
                assert map != null : map;

                conflictPutMap = F.viewReadOnly((Map)map, new IgniteClosure<V, GridCacheDrInfo>() {
                    @Override public GridCacheDrInfo apply(V o) {
                        return new GridCacheDrInfo(ctx.toCacheObject(o), ctx.versions().next(opCtx.dataCenterId()));
                    }
                });

                map = null;
            }
        }

        UUID subjId = ctx.subjectIdPerCall(null, opCtx);

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        final GridNearAtomicUpdateFuture updateFut = new GridNearAtomicUpdateFuture(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            op,
            map != null ? map.keySet() : invokeMap != null ? invokeMap.keySet() : conflictPutMap != null ?
                conflictPutMap.keySet() : conflictRmvMap.keySet(),
            map != null ? map.values() : invokeMap != null ? invokeMap.values() : null,
            invokeArgs,
            (Collection)(conflictPutMap != null ? conflictPutMap.values() : null),
            conflictRmvMap != null ? conflictRmvMap.values() : null,
            retval,
            rawRetval,
            opCtx != null ? opCtx.expiry() : null,
            CU.filterArray(null),
            subjId,
            taskNameHash,
            opCtx != null && opCtx.skipStore(),
            opCtx != null && opCtx.isKeepBinary(),
            opCtx != null && opCtx.recovery(),
            opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES);

        if (async) {
            return asyncOp(new CO<IgniteInternalFuture<Object>>() {
                @Override public IgniteInternalFuture<Object> apply() {
                    updateFut.map();

                    return updateFut;
                }
            });
        }
        else {
            updateFut.map();

            return updateFut;
        }
    }

    /**
     * Entry point for update/invoke with a single key.
     *
     * @param key Key.
     * @param val Value.
     * @param proc Entry processor.
     * @param invokeArgs Invoke arguments.
     * @param retval Return value flag.
     * @param filter Filter.
     * @param async Async operation flag.
     * @return Future.
     */
    private IgniteInternalFuture update0(
        K key,
        @Nullable V val,
        @Nullable EntryProcessor proc,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        @Nullable final CacheEntryPredicate filter,
        boolean async
    ) {
        assert val == null || proc == null;

        assert ctx.updatesAllowed();

        validateCacheKey(key);

        ctx.checkSecurity(SecurityPermission.CACHE_PUT);

        final GridNearAtomicAbstractUpdateFuture updateFut =
            createSingleUpdateFuture(key, val, proc, invokeArgs, retval, filter);

        if (async) {
            return asyncOp(new CO<IgniteInternalFuture<Object>>() {
                @Override public IgniteInternalFuture<Object> apply() {
                    updateFut.map();

                    return updateFut;
                }
            });
        }
        else {
            updateFut.map();

            return updateFut;
        }
    }

    /**
     * Entry point for remove with single key.
     *
     * @param key Key.
     * @param retval Whether to return
     * @param filter Filter.
     * @param async Async operation flag.
     * @return Future.
     */
    private IgniteInternalFuture remove0(K key, final boolean retval,
        @Nullable CacheEntryPredicate filter,
        boolean async) {
        assert ctx.updatesAllowed();

        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        final GridNearAtomicAbstractUpdateFuture updateFut = createSingleUpdateFuture(key,
            null,
            null,
            null,
            retval,
            filter);

        if (async) {
            return asyncOp(new CO<IgniteInternalFuture<Object>>() {
                @Override public IgniteInternalFuture<Object> apply() {
                    updateFut.map();

                    return updateFut;
                }
            });
        }
        else {
            updateFut.map();

            return updateFut;
        }
    }

    /**
     * Craete future for single key-val pair update.
     *
     * @param key Key.
     * @param val Value.
     * @param proc Processor.
     * @param invokeArgs Invoke arguments.
     * @param retval Return value flag.
     * @param filter Filter.
     * @return Future.
     */
    private GridNearAtomicAbstractUpdateFuture createSingleUpdateFuture(
        K key,
        @Nullable V val,
        @Nullable EntryProcessor proc,
        @Nullable Object[] invokeArgs,
        boolean retval,
        @Nullable CacheEntryPredicate filter
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        GridCacheOperation op;
        Object val0;

        if (val != null) {
            op = UPDATE;
            val0 = val;
        }
        else if (proc != null) {
            op = TRANSFORM;
            val0 = proc;
        }
        else {
            op = DELETE;
            val0 = null;
        }

        GridCacheDrInfo conflictPutVal = null;
        GridCacheVersion conflictRmvVer = null;

        if (opCtx != null && opCtx.hasDataCenterId()) {
            Byte dcId = opCtx.dataCenterId();

            assert dcId != null;

            if (op == UPDATE) {
                conflictPutVal = new GridCacheDrInfo(ctx.toCacheObject(val), ctx.versions().next(dcId));

                val0 = null;
            }
            else if (op == GridCacheOperation.TRANSFORM) {
                conflictPutVal = new GridCacheDrInfo(proc, ctx.versions().next(dcId));

                val0 = null;
            }
            else
                conflictRmvVer = ctx.versions().next(dcId);
        }

        CacheEntryPredicate[] filters = CU.filterArray(filter);

        if (conflictPutVal == null && conflictRmvVer == null) {
            return new GridNearAtomicSingleUpdateFuture(
                ctx,
                this,
                ctx.config().getWriteSynchronizationMode(),
                op,
                key,
                val0,
                invokeArgs,
                retval,
                false,
                opCtx != null ? opCtx.expiry() : null,
                filters,
                ctx.subjectIdPerCall(null, opCtx),
                ctx.kernalContext().job().currentTaskNameHash(),
                opCtx != null && opCtx.skipStore(),
                opCtx != null && opCtx.isKeepBinary(),
                opCtx != null && opCtx.recovery(),
                opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES
            );
        }
        else {
            return new GridNearAtomicUpdateFuture(
                ctx,
                this,
                ctx.config().getWriteSynchronizationMode(),
                op,
                Collections.singletonList(key),
                val0 != null ? Collections.singletonList(val0) : null,
                invokeArgs,
                conflictPutVal != null ? Collections.singleton(conflictPutVal) : null,
                conflictRmvVer != null ? Collections.singleton(conflictRmvVer) : null,
                retval,
                false,
                opCtx != null ? opCtx.expiry() : null,
                filters,
                ctx.subjectIdPerCall(null, opCtx),
                ctx.kernalContext().job().currentTaskNameHash(),
                opCtx != null && opCtx.skipStore(),
                opCtx != null && opCtx.isKeepBinary(),
                opCtx != null && opCtx.recovery(),
                opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES);
        }
    }

    /**
     * Entry point for all public API remove methods.
     *
     * @param keys Keys to remove.
     * @param conflictMap Conflict map.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @return Completion future.
     */
    private IgniteInternalFuture removeAllAsync0(
        @Nullable Collection<? extends K> keys,
        @Nullable Map<KeyCacheObject, GridCacheVersion> conflictMap,
        final boolean retval,
        boolean rawRetval,
        boolean async
    ) {
        assert ctx.updatesAllowed();

        assert keys != null || conflictMap != null;

        if (keyCheck)
            validateCacheKeys(keys);

        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        UUID subjId = ctx.subjectIdPerCall(null, opCtx);

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        Collection<GridCacheVersion> drVers = null;

        if (opCtx != null && keys != null && opCtx.hasDataCenterId()) {
            assert conflictMap == null : conflictMap;

            drVers = F.transform(keys, new C1<K, GridCacheVersion>() {
                @Override public GridCacheVersion apply(K k) {
                    return ctx.versions().next(opCtx.dataCenterId());
                }
            });
        }

        final GridNearAtomicUpdateFuture updateFut = new GridNearAtomicUpdateFuture(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            DELETE,
            keys != null ? keys : conflictMap.keySet(),
            null,
            null,
            null,
            drVers != null ? drVers : (keys != null ? null : conflictMap.values()),
            retval,
            rawRetval,
            opCtx != null ? opCtx.expiry() : null,
            CU.filterArray(null),
            subjId,
            taskNameHash,
            opCtx != null && opCtx.skipStore(),
            opCtx != null && opCtx.isKeepBinary(),
            opCtx != null && opCtx.recovery(),
            opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES);

        if (async) {
            return asyncOp(new CO<IgniteInternalFuture<Object>>() {
                @Override public IgniteInternalFuture<Object> apply() {
                    updateFut.map();

                    return updateFut;
                }
            });
        }
        else {
            updateFut.map();

            return updateFut;
        }
    }

    /**
     * Entry point to all public API single get methods.
     *
     * @param key Key.
     * @param forcePrimary Force primary flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param skipStore Skip store flag.
     * @param canRemap Can remap flag.
     * @param needVer Need version.
     * @return Get future.
     */
    private IgniteInternalFuture<V> getAsync0(KeyCacheObject key,
        boolean forcePrimary,
        UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable ExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean skipStore,
        boolean canRemap,
        boolean needVer
    ) {
        AffinityTopologyVersion topVer = canRemap ? ctx.affinity().affinityTopologyVersion() :
            ctx.shared().exchange().readyAffinityVersion();

        IgniteCacheExpiryPolicy expiry = skipVals ? null : expiryPolicy(expiryPlc);

        GridPartitionedSingleGetFuture fut = new GridPartitionedSingleGetFuture(ctx,
            key,
            topVer,
            !skipStore,
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            expiry,
            skipVals,
            canRemap,
            needVer,
            false,
            recovery);

        fut.init();

        return (IgniteInternalFuture<V>)fut;
    }

    /**
     * Entry point to all public API get methods.
     *
     * @param keys Keys.
     * @param forcePrimary Force primary flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param skipStore Skip store flag.
     * @param needVer Need version.
     * @return Get future.
     */
    private IgniteInternalFuture<Map<K, V>> getAllAsync0(@Nullable Collection<KeyCacheObject> keys,
        boolean forcePrimary,
        UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable ExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean skipStore,
        boolean canRemap,
        boolean needVer
    ) {
        AffinityTopologyVersion topVer = canRemap ? ctx.affinity().affinityTopologyVersion() :
            ctx.shared().exchange().readyAffinityVersion();

        final IgniteCacheExpiryPolicy expiry = skipVals ? null : expiryPolicy(expiryPlc);

        final boolean evt = !skipVals;

        // Optimisation: try to resolve value locally and escape 'get future' creation.
        if (!forcePrimary && ctx.affinityNode()) {
            try {
                Map<K, V> locVals = U.newHashMap(keys.size());

                boolean success = true;
                boolean readNoEntry = ctx.readNoEntry(expiry, false);

                // Optimistically expect that all keys are available locally (avoid creation of get future).
                for (KeyCacheObject key : keys) {
                    if (readNoEntry) {
                        CacheDataRow row = ctx.offheap().read(ctx, key);

                        if (row != null) {
                            long expireTime = row.expireTime();

                            if (expireTime == 0 || expireTime > U.currentTimeMillis()) {
                                ctx.addResult(locVals,
                                    key,
                                    row.value(),
                                    skipVals,
                                    false,
                                    deserializeBinary,
                                    true,
                                    null,
                                    row.version(),
                                    0,
                                    0,
                                    needVer);

                                if (evt) {
                                    ctx.events().readEvent(key,
                                        null,
                                        row.value(),
                                        subjId,
                                        taskName,
                                        !deserializeBinary);
                                }
                            }
                            else
                                success = false;
                        }
                        else
                            success = false;
                    }
                    else {
                        GridCacheEntryEx entry = null;

                        while (true) {
                            try {
                                entry = entryEx(key);

                                // If our DHT cache do has value, then we peek it.
                                if (entry != null) {
                                    boolean isNew = entry.isNewLocked();

                                    EntryGetResult getRes = null;
                                    CacheObject v = null;
                                    GridCacheVersion ver = null;

                                    if (needVer) {
                                        getRes = entry.innerGetVersioned(
                                            null,
                                            null,
                                            /*update-metrics*/false,
                                            /*event*/evt,
                                            subjId,
                                            null,
                                            taskName,
                                            expiry,
                                            true,
                                            null);

                                        if (getRes != null) {
                                            v = getRes.value();
                                            ver = getRes.version();
                                        }
                                    }
                                    else {
                                        v = entry.innerGet(
                                            null,
                                            null,
                                            /*read-through*/false,
                                            /*update-metrics*/false,
                                            /*event*/evt,
                                            subjId,
                                            null,
                                            taskName,
                                            expiry,
                                            !deserializeBinary);
                                    }

                                    // Entry was not in memory or in swap, so we remove it from cache.
                                    if (v == null) {
                                        if (isNew && entry.markObsoleteIfEmpty(context().versions().next()))
                                            removeEntry(entry);

                                        success = false;
                                    }
                                    else {
                                        ctx.addResult(locVals,
                                            key,
                                            v,
                                            skipVals,
                                            false,
                                            deserializeBinary,
                                            true,
                                            getRes,
                                            ver,
                                            0,
                                            0,
                                            needVer);
                                    }
                                }
                                else
                                    success = false;

                                break; // While.
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                // No-op, retry.
                            }
                            catch (GridDhtInvalidPartitionException ignored) {
                                success = false;

                                break; // While.
                            }
                            finally {
                                if (entry != null)
                                    ctx.evicts().touch(entry, topVer);
                            }
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
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }

        if (expiry != null)
            expiry.reset();

        // Either reload or not all values are available locally.
        GridPartitionedGetFuture<K, V> fut = new GridPartitionedGetFuture<>(ctx,
            keys,
            topVer,
            !skipStore,
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            expiry,
            skipVals,
            canRemap,
            needVer,
            false);

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
    void updateAllAsyncInternal(
        final UUID nodeId,
        final GridNearAtomicAbstractUpdateRequest req,
        final UpdateReplyClosure completionCb
    ) {
        IgniteInternalFuture<Object> forceFut = ctx.group().preloader().request(ctx, req, req.topologyVersion());

        if (forceFut == null || forceFut.isDone()) {
            try {
                if (forceFut != null)
                    forceFut.get();
            }
            catch (NodeStoppingException ignored) {
                return;
            }
            catch (IgniteCheckedException e) {
                onForceKeysError(nodeId, req, completionCb, e);

                return;
            }

            updateAllAsyncInternal0(nodeId, req, completionCb);
        }
        else {
            forceFut.listen(new CI1<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> fut) {
                    try {
                        fut.get();
                    }
                    catch (NodeStoppingException ignored) {
                        return;
                    }
                    catch (IgniteCheckedException e) {
                        onForceKeysError(nodeId, req, completionCb, e);

                        return;
                    }

                    updateAllAsyncInternal0(nodeId, req, completionCb);
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Update request.
     * @param completionCb Completion callback.
     * @param e Error.
     */
    private void onForceKeysError(final UUID nodeId,
        final GridNearAtomicAbstractUpdateRequest req,
        final UpdateReplyClosure completionCb,
        IgniteCheckedException e
    ) {
        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(ctx.cacheId(),
            nodeId,
            req.futureId(),
            req.partition(),
            false,
            ctx.deploymentEnabled());

        res.addFailedKeys(req.keys(), e);

        completionCb.apply(req, res);
    }

    /**
     * Executes local update after preloader fetched values.
     *
     * @param nodeId Node ID.
     * @param req Update request.
     * @param completionCb Completion callback.
     */
    private void updateAllAsyncInternal0(
        UUID nodeId,
        GridNearAtomicAbstractUpdateRequest req,
        UpdateReplyClosure completionCb
    ) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            U.warn(msgLog, "Skip near update request, node originated update request left [" +
                "futId=" + req.futureId() + ", node=" + nodeId + ']');

            return;
        }

        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(ctx.cacheId(),
            nodeId,
            req.futureId(),
            req.partition(),
            false,
            ctx.deploymentEnabled());

        assert !req.returnValue() || (req.operation() == TRANSFORM || req.size() == 1);

        GridDhtAtomicAbstractUpdateFuture dhtFut = null;

        boolean remap = false;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        IgniteCacheExpiryPolicy expiry = null;

        try {
            // If batch store update is enabled, we need to lock all entries.
            // First, need to acquire locks on cache entries, then check filter.
            List<GridDhtCacheEntry> locked = null;

            Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted = null;

            try {
                GridDhtPartitionTopology top = topology();

                top.readLock();

                try {
                    if (top.stopping()) {
                        res.addFailedKeys(req.keys(), new IgniteCheckedException("Failed to perform cache operation " +
                            "(cache is stopped): " + name()));

                        completionCb.apply(req, res);

                        return;
                    }

                    // Do not check topology version if topology was locked on near node by
                    // external transaction or explicit lock.
                    if (req.topologyLocked() || !needRemap(req.topologyVersion(), top.topologyVersion())) {
                        ctx.shared().database().ensureFreeSpace(ctx.memoryPolicy());

                        locked = lockEntries(req, req.topologyVersion());

                        boolean hasNear = req.nearCache();

                        // Assign next version for update inside entries lock.
                        GridCacheVersion ver = ctx.versions().next(top.topologyVersion());

                        if (hasNear)
                            res.nearVersion(ver);

                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("Assigned update version [futId=" + req.futureId() +
                                ", writeVer=" + ver + ']');
                        }

                        assert ver != null : "Got null version for update request: " + req;

                        boolean sndPrevVal = !top.rebalanceFinished(req.topologyVersion());

                        dhtFut = createDhtFuture(ver, req);

                        expiry = expiryPolicy(req.expiry());

                        GridCacheReturn retVal = null;

                        if (req.size() > 1 &&                    // Several keys ...
                            writeThrough() && !req.skipStore() && // and store is enabled ...
                            !ctx.store().isLocal() &&             // and this is not local store ...
                            // (conflict resolver should be used for local store)
                            !ctx.dr().receiveEnabled()            // and no DR.
                            ) {
                            // This method can only be used when there are no replicated entries in the batch.
                            UpdateBatchResult updRes = updateWithBatch(node,
                                hasNear,
                                req,
                                res,
                                locked,
                                ver,
                                dhtFut,
                                ctx.isDrEnabled(),
                                taskName,
                                expiry,
                                sndPrevVal);

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
                                ctx.isDrEnabled(),
                                taskName,
                                expiry,
                                sndPrevVal);

                            retVal = updRes.returnValue();
                            deleted = updRes.deleted();
                            dhtFut = updRes.dhtFuture();
                        }

                        if (retVal == null)
                            retVal = new GridCacheReturn(ctx, node.isLocal(), true, null, true);

                        res.returnValue(retVal);

                        if (dhtFut != null) {
                            if (req.writeSynchronizationMode() == PRIMARY_SYNC
                                // To avoid deadlock disable back-pressure for sender data node.
                                && !ctx.discovery().cacheAffinityNode(node, ctx.name())
                                && !dhtFut.isDone()) {
                                final IgniteRunnable tracker = GridNioBackPressureControl.threadTracker();

                                if (tracker != null && tracker instanceof GridNioMessageTracker) {
                                    ((GridNioMessageTracker)tracker).onMessageReceived();

                                    dhtFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                                        @Override public void apply(IgniteInternalFuture<Void> fut) {
                                            ((GridNioMessageTracker)tracker).onMessageProcessed();
                                        }
                                    });
                                }
                            }

                            ctx.mvcc().addAtomicFuture(dhtFut.id(), dhtFut);
                        }
                    }
                    else {
                        // Should remap all keys.
                        remap = true;

                        res.remapTopologyVersion(top.topologyVersion());
                    }
                }
                finally {
                    top.readUnlock();
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
                    assert ctx.deferredDelete() : this;

                    for (IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion> e : deleted)
                        ctx.onDeferredDelete(e.get1(), e.get2());
                }

                // TODO handle failure: probably drop the node from topology
                // TODO fire events only after successful fsync
                if (ctx.shared().wal() != null)
                    ctx.shared().wal().fsync(null);
            }
        }
        catch (GridDhtInvalidPartitionException ignore) {
            if (log.isDebugEnabled())
                log.debug("Caught invalid partition exception for cache entry (will remap update request): " + req);

            remap = true;

            res.remapTopologyVersion(ctx.topology().topologyVersion());
        }
        catch (Throwable e) {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            U.error(log, "Unexpected exception during cache update", e);

            res.addFailedKeys(req.keys(), e);

            completionCb.apply(req, res);

            if (e instanceof Error)
                throw (Error)e;

            return;
        }

        if (remap) {
            assert dhtFut == null;

            completionCb.apply(req, res);
        }
        else
            if (dhtFut != null)
                dhtFut.map(node, res.returnValue(), res, completionCb);

        if (req.writeSynchronizationMode() != FULL_ASYNC)
            req.cleanup(!node.isLocal());

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
     * @param replicate Whether replication is enabled.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @param sndPrevVal If {@code true} sends previous value to backups.
     * @return Deleted entries.
     * @throws GridCacheEntryRemovedException Should not be thrown.
     */
    @SuppressWarnings("unchecked")
    private UpdateBatchResult updateWithBatch(
        final ClusterNode node,
        final boolean hasNear,
        final GridNearAtomicAbstractUpdateRequest req,
        final GridNearAtomicUpdateResponse res,
        final List<GridDhtCacheEntry> locked,
        final GridCacheVersion ver,
        @Nullable GridDhtAtomicAbstractUpdateFuture dhtFut,
        final boolean replicate,
        final String taskName,
        @Nullable final IgniteCacheExpiryPolicy expiry,
        final boolean sndPrevVal
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

        int size = req.size();

        Map<KeyCacheObject, CacheObject> putMap = null;

        Map<KeyCacheObject, EntryProcessor<Object, Object, Object>> entryProcessorMap = null;

        Collection<KeyCacheObject> rmvKeys = null;

        List<CacheObject> writeVals = null;

        UpdateBatchResult updRes = new UpdateBatchResult();

        List<GridDhtCacheEntry> filtered = new ArrayList<>(size);

        GridCacheOperation op = req.operation();

        GridCacheReturn invokeRes = null;

        int firstEntryIdx = 0;

        boolean intercept = ctx.config().getInterceptor() != null;

        for (int i = 0; i < locked.size(); i++) {
            GridDhtCacheEntry entry = locked.get(i);

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
                        ver,
                        null,
                        /*read through*/true,
                        /*metrics*/true,
                        /*event*/true,
                        req.subjectId(),
                        entryProcessor,
                        taskName,
                        null,
                        req.keepBinary());

                    Object oldVal = null;
                    Object updatedVal = null;

                    CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry(entry.key(), old,
                        entry.version(), req.keepBinary(), entry);

                    CacheObject updated;

                    try {
                        Object computed = entryProcessor.process(invokeEntry, req.invokeArguments());

                        if (computed != null) {
                            if (invokeRes == null)
                                invokeRes = new GridCacheReturn(node.isLocal());

                            computed = ctx.unwrapTemporary(computed);

                            invokeRes.addEntryProcessResult(ctx, entry.key(), invokeEntry.key(), computed, null,
                                req.keepBinary());
                        }

                        if (!invokeEntry.modified())
                            continue;

                        updatedVal = ctx.unwrapTemporary(invokeEntry.getValue());

                        updated = ctx.toCacheObject(updatedVal);
                    }
                    catch (Exception e) {
                        if (invokeRes == null)
                            invokeRes = new GridCacheReturn(node.isLocal());

                        invokeRes.addEntryProcessResult(ctx, entry.key(), invokeEntry.key(), null, e, req.keepBinary());

                        updated = old;
                    }

                    if (updated == null) {
                        if (intercept) {
                            CacheLazyEntry e = new CacheLazyEntry(ctx, entry.key(), invokeEntry.key(), old, oldVal, req.keepBinary());

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
                                req,
                                res,
                                replicate,
                                updRes,
                                taskName,
                                expiry,
                                sndPrevVal);

                            firstEntryIdx = i;

                            putMap = null;
                            writeVals = null;
                            entryProcessorMap = null;

                            filtered = new ArrayList<>();
                        }

                        // Start collecting new batch.
                        if (rmvKeys == null)
                            rmvKeys = new ArrayList<>(size);

                        rmvKeys.add(entry.key());
                    }
                    else {
                        if (intercept) {
                            CacheLazyEntry e = new CacheLazyEntry(ctx, entry.key(), invokeEntry.key(), old, oldVal, req.keepBinary());

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
                                req,
                                res,
                                replicate,
                                updRes,
                                taskName,
                                expiry,
                                sndPrevVal);

                            firstEntryIdx = i;

                            rmvKeys = null;
                            entryProcessorMap = null;

                            filtered = new ArrayList<>();
                        }

                        if (putMap == null) {
                            putMap = new LinkedHashMap<>(size, 1.0f);
                            writeVals = new ArrayList<>(size);
                        }

                        putMap.put(entry.key(), updated);
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
                            null,
                            /*read through*/ctx.loadPreviousValue(),
                            /*metrics*/true,
                            /*event*/true,
                            req.subjectId(),
                            null,
                            taskName,
                            null,
                            req.keepBinary());

                        Object val = ctx.config().getInterceptor().onBeforePut(
                            new CacheLazyEntry(
                                ctx,
                                entry.key(),
                                old,
                                req.keepBinary()),
                            ctx.unwrapBinaryIfNeeded(
                                updated,
                                req.keepBinary(),
                                false));

                        if (val == null)
                            continue;

                        updated = ctx.toCacheObject(ctx.unwrapTemporary(val));
                    }

                    assert updated != null;

                    if (putMap == null) {
                        putMap = new LinkedHashMap<>(size, 1.0f);
                        writeVals = new ArrayList<>(size);
                    }

                    putMap.put(entry.key(), updated);
                    writeVals.add(updated);
                }
                else {
                    assert op == DELETE;

                    if (intercept) {
                        CacheObject old = entry.innerGet(
                            null,
                            null,
                            /*read through*/ctx.loadPreviousValue(),
                            /*metrics*/true,
                            /*event*/true,
                            req.subjectId(),
                            null,
                            taskName,
                            null,
                            req.keepBinary());

                        IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor()
                            .onBeforeRemove(new CacheLazyEntry(ctx, entry.key(), old, req.keepBinary()));

                        if (ctx.cancelRemove(interceptorRes))
                            continue;
                    }

                    if (rmvKeys == null)
                        rmvKeys = new ArrayList<>(size);

                    rmvKeys.add(entry.key());
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
                req,
                res,
                replicate,
                updRes,
                taskName,
                expiry,
                sndPrevVal);
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

            CacheObject val = entry.rawGet();

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

                            entry.versionedValue(ctx.toCacheObject(v), null, ver, null, null);
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
     * @param nearNode Originating node.
     * @param hasNear {@code True} if originating node has near cache.
     * @param req Update request.
     * @param res Update response.
     * @param locked Locked entries.
     * @param ver Assigned update version.
     * @param dhtFut Optional DHT future.
     * @param replicate Whether DR is enabled for that cache.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @param sndPrevVal If {@code true} sends previous value to backups.
     * @return Return value.
     * @throws GridCacheEntryRemovedException Should be never thrown.
     */
    private UpdateSingleResult updateSingle(
        ClusterNode nearNode,
        boolean hasNear,
        GridNearAtomicAbstractUpdateRequest req,
        GridNearAtomicUpdateResponse res,
        List<GridDhtCacheEntry> locked,
        GridCacheVersion ver,
        @Nullable GridDhtAtomicAbstractUpdateFuture dhtFut,
        boolean replicate,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiry,
        boolean sndPrevVal
    ) throws GridCacheEntryRemovedException {
        GridCacheReturn retVal = null;
        Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted = null;

        AffinityTopologyVersion topVer = req.topologyVersion();

        boolean intercept = ctx.config().getInterceptor() != null;

        AffinityAssignment affAssignment = ctx.affinity().assignment(topVer);

        // Avoid iterator creation.
        for (int i = 0; i < req.size(); i++) {
            KeyCacheObject k = req.key(i);

            GridCacheOperation op = req.operation();

            // We are holding java-level locks on entries at this point.
            // No GridCacheEntryRemovedException can be thrown.
            try {
                GridDhtCacheEntry entry = locked.get(i);

                GridCacheVersion newConflictVer = req.conflictVersion(i);
                long newConflictTtl = req.conflictTtl(i);
                long newConflictExpireTime = req.conflictExpireTime(i);

                assert !(newConflictVer instanceof GridCacheVersionEx) : newConflictVer;

                Object writeVal = op == TRANSFORM ? req.entryProcessor(i) : req.writeValue(i);

                // Get readers before innerUpdate (reader cleared after remove).
                GridDhtCacheEntry.ReaderId[] readers = entry.readersLocked();

                GridCacheUpdateAtomicResult updRes = entry.innerUpdate(
                    ver,
                    nearNode.id(),
                    locNodeId,
                    op,
                    writeVal,
                    req.invokeArguments(),
                    writeThrough() && !req.skipStore(),
                    !req.skipStore(),
                    sndPrevVal || req.returnValue(),
                    req.keepBinary(),
                    expiry,
                    /*event*/true,
                    /*metrics*/true,
                    /*primary*/true,
                    /*verCheck*/false,
                    topVer,
                    req.filter(),
                    replicate ? DR_PRIMARY : DR_NONE,
                    newConflictTtl,
                    newConflictExpireTime,
                    newConflictVer,
                    /*conflictResolve*/true,
                    intercept,
                    req.subjectId(),
                    taskName,
                    /*prevVal*/null,
                    /*updateCntr*/null,
                    dhtFut);

                if (dhtFut != null) {
                    if (updRes.sendToDht()) { // Send to backups even in case of remove-remove scenarios.
                        GridCacheVersionConflictContext<?, ?> conflictCtx = updRes.conflictResolveResult();

                        if (conflictCtx == null)
                            newConflictVer = null;
                        else if (conflictCtx.isMerge())
                            newConflictVer = null; // Conflict version is discarded in case of merge.

                        EntryProcessor<Object, Object, Object> entryProcessor = null;

                        dhtFut.addWriteEntry(
                            affAssignment,
                            entry,
                            updRes.newValue(),
                            entryProcessor,
                            updRes.newTtl(),
                            updRes.conflictExpireTime(),
                            newConflictVer,
                            sndPrevVal,
                            updRes.oldValue(),
                            updRes.updateCounter());

                        if (readers != null)
                            dhtFut.addNearWriteEntries(
                                nearNode,
                                readers,
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
                    if (updRes.sendToDht()) {
                        if (!ctx.affinity().partitionBelongs(nearNode, entry.partition(), topVer)) {
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
                                IgniteInternalFuture<Boolean> f =
                                    entry.addReader(nearNode.id(), req.messageId(), topVer);

                                assert f == null : f;
                            }
                        }
                        else if (GridDhtCacheEntry.ReaderId.contains(readers, nearNode.id())) {
                            // Reader became primary or backup.
                            entry.removeReader(nearNode.id(), req.messageId());
                        }
                        else
                            res.addSkippedIndex(i);
                    }
                    else
                        res.addSkippedIndex(i);
                }

                if (updRes.removeVersion() != null) {
                    if (deleted == null)
                        deleted = new ArrayList<>(req.size());

                    deleted.add(F.t(entry, updRes.removeVersion()));
                }

                if (op == TRANSFORM) {
                    assert !req.returnValue();

                    IgniteBiTuple<Object, Exception> compRes = updRes.computedResult();

                    if (compRes != null && (compRes.get1() != null || compRes.get2() != null)) {
                        if (retVal == null)
                            retVal = new GridCacheReturn(nearNode.isLocal());

                        retVal.addEntryProcessResult(ctx,
                            k,
                            null,
                            compRes.get1(),
                            compRes.get2(),
                            req.keepBinary());
                    }
                }
                else {
                    // Create only once.
                    if (retVal == null) {
                        CacheObject ret = updRes.oldValue();

                        retVal = new GridCacheReturn(ctx,
                            nearNode.isLocal(),
                            req.keepBinary(),
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
     * @param nearNode Originating node.
     * @param writeVals Write values.
     * @param putMap Values to put.
     * @param rmvKeys Keys to remove.
     * @param entryProcessorMap Entry processors.
     * @param dhtFut DHT update future if has backups.
     * @param req Request.
     * @param res Response.
     * @param replicate Whether replication is enabled.
     * @param batchRes Batch update result.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @param sndPrevVal If {@code true} sends previous value to backups.
     * @return Deleted entries.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable private GridDhtAtomicAbstractUpdateFuture updatePartialBatch(
        final boolean hasNear,
        final int firstEntryIdx,
        final List<GridDhtCacheEntry> entries,
        final GridCacheVersion ver,
        final ClusterNode nearNode,
        @Nullable final List<CacheObject> writeVals,
        @Nullable final Map<KeyCacheObject, CacheObject> putMap,
        @Nullable final Collection<KeyCacheObject> rmvKeys,
        @Nullable final Map<KeyCacheObject, EntryProcessor<Object, Object, Object>> entryProcessorMap,
        @Nullable GridDhtAtomicAbstractUpdateFuture dhtFut,
        final GridNearAtomicAbstractUpdateRequest req,
        final GridNearAtomicUpdateResponse res,
        final boolean replicate,
        final UpdateBatchResult batchRes,
        final String taskName,
        @Nullable final IgniteCacheExpiryPolicy expiry,
        final boolean sndPrevVal
    ) {
        assert putMap == null ^ rmvKeys == null;

        assert req.conflictVersions() == null : "Cannot be called when there are conflict entries in the batch.";

        AffinityTopologyVersion topVer = req.topologyVersion();

        CacheStorePartialUpdateException storeErr = null;

        try {
            GridCacheOperation op;

            if (putMap != null) {
                try {
                    Map<? extends KeyCacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>> view = F.viewReadOnly(putMap,
                        new C1<CacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>>() {
                            @Override public IgniteBiTuple<? extends CacheObject, GridCacheVersion> apply(CacheObject val) {
                                return F.t(val, ver);
                            }
                        });

                    ctx.store().putAll(null, view);
                }
                catch (CacheStorePartialUpdateException e) {
                    storeErr = e;
                }

                op = UPDATE;
            }
            else {
                try {
                    ctx.store().removeAll(null, rmvKeys);
                }
                catch (CacheStorePartialUpdateException e) {
                    storeErr = e;
                }

                op = DELETE;
            }

            boolean intercept = ctx.config().getInterceptor() != null;

            AffinityAssignment affAssignment = ctx.affinity().assignment(topVer);

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

                    // Get readers before innerUpdate (reader cleared after remove).
                    GridDhtCacheEntry.ReaderId[] readers = entry.readersLocked();

                    GridCacheUpdateAtomicResult updRes = entry.innerUpdate(
                        ver,
                        nearNode.id(),
                        locNodeId,
                        op,
                        writeVal,
                        null,
                        /*write-through*/false,
                        /*read-through*/false,
                        /*retval*/sndPrevVal,
                        req.keepBinary(),
                        expiry,
                        /*event*/true,
                        /*metrics*/true,
                        /*primary*/true,
                        /*verCheck*/false,
                        topVer,
                        null,
                        replicate ? DR_PRIMARY : DR_NONE,
                        CU.TTL_NOT_CHANGED,
                        CU.EXPIRE_TIME_CALCULATE,
                        null,
                        /*conflict resolve*/false,
                        /*intercept*/false,
                        req.subjectId(),
                        taskName,
                        null,
                        null,
                        dhtFut);

                    assert !updRes.success() || updRes.newTtl() == CU.TTL_NOT_CHANGED || expiry != null :
                        "success=" + updRes.success() + ", newTtl=" + updRes.newTtl() + ", expiry=" + expiry;

                    if (intercept) {
                        if (op == UPDATE) {
                            ctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(
                                ctx,
                                entry.key(),
                                updRes.newValue(),
                                req.keepBinary()));
                        }
                        else {
                            assert op == DELETE : op;

                            // Old value should be already loaded for 'CacheInterceptor.onBeforeRemove'.
                            ctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(ctx, entry.key(),
                                updRes.oldValue(), req.keepBinary()));
                        }
                    }

                    batchRes.addDeleted(entry, updRes, entries);

                    if (dhtFut != null) {
                        EntryProcessor<Object, Object, Object> entryProcessor =
                            entryProcessorMap == null ? null : entryProcessorMap.get(entry.key());

                        dhtFut.addWriteEntry(
                            affAssignment,
                            entry,
                            writeVal,
                            entryProcessor,
                            updRes.newTtl(),
                            CU.EXPIRE_TIME_CALCULATE,
                            null,
                            sndPrevVal,
                            updRes.oldValue(),
                            updRes.updateCounter());

                        if (readers != null)
                            dhtFut.addNearWriteEntries(
                                nearNode,
                                readers,
                                entry,
                                writeVal,
                                entryProcessor,
                                updRes.newTtl(),
                                CU.EXPIRE_TIME_CALCULATE);
                    }

                    if (hasNear) {
                        if (!ctx.affinity().partitionBelongs(nearNode, entry.partition(), topVer)) {
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
                                IgniteInternalFuture<Boolean> f = entry.addReader(nearNode.id(), req.messageId(), topVer);

                                assert f == null : f;
                            }
                        }
                        else if (GridDhtCacheEntry.ReaderId.contains(readers, nearNode.id())) {
                            // Reader became primary or backup.
                            entry.removeReader(nearNode.id(), req.messageId());
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
            res.addFailedKeys(putMap != null ? putMap.keySet() : rmvKeys, e);
        }

        if (storeErr != null) {
            ArrayList<KeyCacheObject> failed = new ArrayList<>(storeErr.failedKeys().size());

            for (Object failedKey : storeErr.failedKeys())
                failed.add(ctx.toCacheKeyObject(failedKey));

            res.addFailedKeys(failed, storeErr.getCause());
        }

        return dhtFut;
    }

    /**
     * Acquires java-level locks on cache entries. Returns collection of locked entries.
     *
     * @param req Request with keys to lock.
     * @param topVer Topology version to lock on.
     * @return Collection of locked entries.
     * @throws GridDhtInvalidPartitionException If entry does not belong to local node. If exception is thrown, locks
     * are released.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private List<GridDhtCacheEntry> lockEntries(GridNearAtomicAbstractUpdateRequest req, AffinityTopologyVersion topVer)
        throws GridDhtInvalidPartitionException {
        ctx.shared().database().checkpointReadLock();

        if (req.size() == 1) {
            KeyCacheObject key = req.key(0);

            while (true) {
                GridDhtCacheEntry entry = entryExx(key, topVer);

                GridUnsafe.monitorEnter(entry);

                if (entry.obsolete())
                    GridUnsafe.monitorExit(entry);
                else
                    return Collections.singletonList(entry);
            }
        }
        else {
            List<GridDhtCacheEntry> locked = new ArrayList<>(req.size());

            while (true) {
                for (int i = 0; i < req.size(); i++) {
                    GridDhtCacheEntry entry = entryExx(req.key(i), topVer);

                    locked.add(entry);
                }

                boolean retry = false;

                for (int i = 0; i < locked.size(); i++) {
                    GridCacheMapEntry entry = locked.get(i);

                    if (entry == null)
                        continue;

                    GridUnsafe.monitorEnter(entry);

                    if (entry.obsolete()) {
                        // Unlock all locked.
                        for (int j = 0; j <= i; j++) {
                            if (locked.get(j) != null)
                                GridUnsafe.monitorExit(locked.get(j));
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
    private void unlockEntries(List<GridDhtCacheEntry> locked, AffinityTopologyVersion topVer) {
        // Process deleted entries before locks release.
        assert ctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        Collection<KeyCacheObject> skip = null;

        int size = locked.size();

        try {
            for (int i = 0; i < size; i++) {
                GridCacheMapEntry entry = locked.get(i);
                if (entry != null && entry.deleted()) {
                    if (skip == null)
                        skip = U.newHashSet(locked.size());

                    skip.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (int i = 0; i < size; i++) {
                GridCacheMapEntry entry = locked.get(i);
                if (entry != null)
                    GridUnsafe.monitorExit(entry);
            }
        }

        // Try evict partitions.
        for (int i = 0; i < size; i++) {
            GridDhtCacheEntry entry = locked.get(i);
            if (entry != null)
                entry.onUnlock();
        }

        ctx.shared().database().checkpointReadUnlock();

        if (skip != null && skip.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (int i = 0; i < size; i++) {
            GridCacheMapEntry entry = locked.get(i);
            if (entry != null && (skip == null || !skip.contains(entry.key())))
                ctx.evicts().touch(entry, topVer);
        }
    }

    /**
     * @param entry Entry to check.
     * @param req Update request.
     * @param res Update response. If filter evaluation failed, key will be added to failed keys and method will return
     * false.
     * @return {@code True} if filter evaluation succeeded.
     */
    private boolean checkFilter(GridCacheEntryEx entry, GridNearAtomicAbstractUpdateRequest req,
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
    void remapToNewPrimary(GridNearAtomicAbstractUpdateRequest req) {
        assert req.writeSynchronizationMode() == FULL_ASYNC : req;

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
            assert req.operation() == DELETE : req;

            drRmvVals = req.conflictVersions();

            vals = null;
            drPutVals = null;
        }

        GridNearAtomicUpdateFuture updateFut = new GridNearAtomicUpdateFuture(
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
            req.keepBinary(),
            req.recovery(),
            MAX_RETRIES);

        updateFut.map();
    }

    /**
     * Creates backup update future if necessary.
     *
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @return Backup update future.
     */
    private GridDhtAtomicAbstractUpdateFuture createDhtFuture(
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq
    ) {
        if (updateReq.size() == 1)
            return new GridDhtAtomicSingleUpdateFuture(ctx, writeVer, updateReq);
        else
            return new GridDhtAtomicUpdateFuture(ctx, writeVer, updateReq);
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Near atomic update request.
     */
    private void processNearAtomicUpdateRequest(UUID nodeId, GridNearAtomicAbstractUpdateRequest req) {
        if (msgLog.isDebugEnabled()) {
            msgLog.debug("Received near atomic update request [futId=" + req.futureId() +
                ", node=" + nodeId + ']');
        }

        updateAllAsyncInternal(nodeId, req, updateReplyClos);
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near atomic update response.
     */
    private void processNearAtomicUpdateResponse(UUID nodeId, GridNearAtomicUpdateResponse res) {
        if (msgLog.isDebugEnabled())
            msgLog.debug("Received near atomic update response [futId" + res.futureId() +
                ", node=" + nodeId + ']');

        res.nodeId(ctx.localNodeId());

        GridNearAtomicAbstractUpdateFuture fut =
            (GridNearAtomicAbstractUpdateFuture)ctx.mvcc().atomicFuture(res.futureId());

        if (fut != null)
            fut.onPrimaryResponse(nodeId, res, false);
        else
            U.warn(msgLog, "Failed to find near update future for update response (will ignore) " +
                "[futId=" + res.futureId() + ", node=" + nodeId + ", res=" + res + ']');
    }

    /**
     * @param nodeId Node ID.
     * @param checkReq Request.
     */
    private void processCheckUpdateRequest(UUID nodeId, GridNearAtomicCheckUpdateRequest checkReq) {
        /*
         * Message is processed in the same stripe, so primary already processed update request. It is possible
         * response was not sent if operation result was empty. Near node will get original response or this one.
         */
        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(ctx.cacheId(),
            nodeId,
            checkReq.futureId(),
            checkReq.partition(),
            false,
            false);

        GridCacheReturn ret = new GridCacheReturn(false, true);

        res.returnValue(ret);

        sendNearUpdateReply(nodeId, res);
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Dht atomic update request.
     */
    private void processDhtAtomicUpdateRequest(UUID nodeId, GridDhtAtomicAbstractUpdateRequest req) {
        assert Thread.currentThread().getName().startsWith("sys-stripe-") : Thread.currentThread().getName();

        if (msgLog.isDebugEnabled()) {
            msgLog.debug("Received DHT atomic update request [futId=" + req.futureId() +
                ", writeVer=" + req.writeVersion() + ", node=" + nodeId + ']');
        }

        assert req.partition() >= 0 : req;

        GridCacheVersion ver = req.writeVersion();

        GridDhtAtomicNearResponse nearRes = null;

        if (req.nearNodeId() != null) {
            nearRes = new GridDhtAtomicNearResponse(ctx.cacheId(),
                req.partition(),
                req.nearFutureId(),
                nodeId,
                req.flags());
        }

        boolean replicate = ctx.isDrEnabled();

        boolean intercept = req.forceTransformBackups() && ctx.config().getInterceptor() != null;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        ctx.shared().database().checkpointReadLock();

        try {
            for (int i = 0; i < req.size(); i++) {
                KeyCacheObject key = req.key(i);

                try {
                    while (true) {
                        GridDhtCacheEntry entry = null;

                        try {
                            entry = entryExx(key);

                            CacheObject val = req.value(i);
                            CacheObject prevVal = req.previousValue(i);

                            EntryProcessor<Object, Object, Object> entryProcessor = req.entryProcessor(i);
                            Long updateIdx = req.updateCounter(i);

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
                                /*write-through*/(ctx.store().isLocal() && !ctx.shared().localStorePrimaryOnly())
                                    && writeThrough() && !req.skipStore(),
                                /*read-through*/false,
                                /*retval*/false,
                                req.keepBinary(),
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
                                taskName,
                                prevVal,
                                updateIdx,
                                null);

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
                catch (NodeStoppingException e){
                    U.error(log, "Failed to update key on backup (local node is stopping):" + key, e);

                    return;
                }
                catch (GridDhtInvalidPartitionException ignored) {
                    // Ignore.
                }
                catch (IgniteCheckedException e) {
                    IgniteCheckedException err = new IgniteCheckedException("Failed to update key on backup node: " + key, e);

                    if (nearRes != null)
                        nearRes.addFailedKey(key, err);

                    U.error(log, "Failed to update key on backup node: " + key, e);
                }
            }
        }
        finally {
            ctx.shared().database().checkpointReadUnlock();
        }

        GridDhtAtomicUpdateResponse dhtRes = null;

        if (req.nearSize() > 0) {
            List<KeyCacheObject> nearEvicted;

            if (isNearEnabled(ctx))
                nearEvicted = ((GridNearAtomicCache<K, V>)near()).processDhtAtomicUpdateRequest(nodeId, req, nearRes);
            else {
                nearEvicted = new ArrayList<>(req.nearSize());

                for (int i = 0; i < req.nearSize(); i++)
                    nearEvicted.add(req.nearKey(i));
            }

            if (nearEvicted != null) {
                dhtRes = new GridDhtAtomicUpdateResponse(ctx.cacheId(),
                    req.partition(),
                    req.futureId(),
                    ctx.deploymentEnabled());

                dhtRes.nearEvicted(nearEvicted);
            }
        }

        try {
            // TODO handle failure: probably drop the node from topology
            // TODO fire events only after successful fsync
            if (ctx.shared().wal() != null)
                ctx.shared().wal().fsync(null);
        }
        catch (StorageException e) {
            if (dhtRes != null)
                dhtRes.onError(new IgniteCheckedException(e));

            if (nearRes != null)
                nearRes.onClassError(e);
        }
        catch (IgniteCheckedException e) {
            if (dhtRes != null)
                dhtRes.onError(e);

            if (nearRes != null)
                nearRes.onClassError(e);
        }

        if (nearRes != null)
            sendDhtNearResponse(req, nearRes);

        if (dhtRes == null && req.replyWithoutDelay()) {
            dhtRes = new GridDhtAtomicUpdateResponse(ctx.cacheId(),
                req.partition(),
                req.futureId(),
                ctx.deploymentEnabled());
        }

        if (dhtRes != null)
            sendDhtPrimaryResponse(nodeId, req, dhtRes);
        else
            sendDeferredUpdateResponse(req.partition(), nodeId, req.futureId());
    }

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @param dhtRes Response to send.
     */
    private void sendDhtPrimaryResponse(UUID nodeId,
        GridDhtAtomicAbstractUpdateRequest req,
        GridDhtAtomicUpdateResponse dhtRes) {
        try {
            ctx.io().send(nodeId, dhtRes, ctx.ioPolicy());

            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Sent DHT response [futId=" + req.futureId() +
                    ", nearFutId=" + req.nearFutureId() +
                    ", writeVer=" + req.writeVersion() +
                    ", node=" + nodeId + ']');
            }
        }
        catch (ClusterTopologyCheckedException ignored) {
            U.warn(msgLog, "Failed to send DHT response, node left [futId=" + req.futureId() +
                ", nearFutId=" + req.nearFutureId() +
                ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(msgLog, "Failed to send DHT near response [futId=" + req.futureId() +
                ", nearFutId=" + req.nearFutureId() +
                ", node=" + nodeId +
                ", res=" + dhtRes + ']', e);
        }
    }

    /**
     * @param part Partition.
     * @param primaryId Primary ID.
     * @param futId Future ID.
     */
    private void sendDeferredUpdateResponse(int part, UUID primaryId, long futId) {
        Map<UUID, GridDhtAtomicDeferredUpdateResponse> resMap = defRes.get();

        GridDhtAtomicDeferredUpdateResponse msg = resMap.get(primaryId);

        if (msg == null) {
            msg = new GridDhtAtomicDeferredUpdateResponse(ctx.cacheId(),
                new GridLongList(DEFERRED_UPDATE_RESPONSE_BUFFER_SIZE));

            if (DEFERRED_UPDATE_RESPONSE_TIMEOUT > 0) {
                GridTimeoutObject timeoutSnd = new DeferredUpdateTimeout(part, primaryId);

                msg.timeoutSender(timeoutSnd);

                ctx.time().addTimeoutObject(timeoutSnd);
            }

            resMap.put(primaryId, msg);
        }

        GridLongList futIds = msg.futureIds();

        assert futIds.size() < DEFERRED_UPDATE_RESPONSE_BUFFER_SIZE : futIds.size();

        futIds.add(futId);

        if (futIds.size() >= DEFERRED_UPDATE_RESPONSE_BUFFER_SIZE) {
            resMap.remove(primaryId);

            sendDeferredUpdateResponse(primaryId, msg);
        }
    }

    /**
     * @param primaryId Primary ID.
     * @param msg Message.
     */
    private void sendDeferredUpdateResponse(UUID primaryId, GridDhtAtomicDeferredUpdateResponse msg) {
        try {
            GridTimeoutObject timeoutSnd = msg.timeoutSender();

            if (timeoutSnd != null)
                ctx.time().removeTimeoutObject(timeoutSnd);

            ctx.io().send(primaryId, msg, ctx.ioPolicy());

            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Sent deferred DHT update response [futIds=" + msg.futureIds() +
                    ", node=" + primaryId + ']');
            }
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Failed to send deferred DHT update response, node left [" +
                    "futIds=" + msg.futureIds() + ", node=" + primaryId + ']');
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send deferredDHT  update response to remote node [" +
                "futIds=" + msg.futureIds() + ", node=" + primaryId + ']', e);
        }
    }

    /**
     * @param req Request.
     * @param nearRes Response to send.
     */
    private void sendDhtNearResponse(final GridDhtAtomicAbstractUpdateRequest req, GridDhtAtomicNearResponse nearRes) {
        try {
            ClusterNode node = ctx.discovery().node(req.nearNodeId());

            if (node == null)
                throw new ClusterTopologyCheckedException("Node failed: " + req.nearNodeId());

            if (node.isLocal())
                processDhtAtomicNearResponse(node.id(), nearRes);
            else
                ctx.io().send(node, nearRes, ctx.ioPolicy());

            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Sent DHT near response [futId=" + req.futureId() +
                    ", nearFutId=" + req.nearFutureId() +
                    ", writeVer=" + req.writeVersion() +
                    ", node=" + req.nearNodeId() + ']');
            }
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Failed to send DHT near response, node left [futId=" + req.futureId() +
                    ", nearFutId=" + req.nearFutureId() +
                    ", node=" + req.nearNodeId() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            U.error(msgLog, "Failed to send DHT near response [futId=" + req.futureId() +
                ", nearFutId=" + req.nearFutureId() +
                ", node=" + req.nearNodeId() +
                ", res=" + nearRes + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtAtomicNearResponse(UUID nodeId, GridDhtAtomicNearResponse res) {
        GridNearAtomicAbstractUpdateFuture updateFut =
            (GridNearAtomicAbstractUpdateFuture)ctx.mvcc().atomicFuture(res.futureId());

        if (updateFut != null) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Received DHT atomic near response [futId=" + res.futureId() +
                    ", node=" + nodeId + ']');
            }

            updateFut.onDhtResponse(nodeId, res);
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Failed to find future for DHT atomic near response [futId=" + res.futureId() +
                    ", node=" + nodeId +
                    ", res=" + res + ']');
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Dht atomic update response.
     */
    @SuppressWarnings("unchecked")
    private void processDhtAtomicUpdateResponse(UUID nodeId, GridDhtAtomicUpdateResponse res) {
        GridDhtAtomicAbstractUpdateFuture updateFut =
            (GridDhtAtomicAbstractUpdateFuture)ctx.mvcc().atomicFuture(res.futureId());

        if (updateFut != null) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Received DHT atomic update response [futId=" + res.futureId() +
                        ", writeVer=" + updateFut.writeVersion() + ", node=" + nodeId + ']');
            }

            updateFut.onDhtResponse(nodeId, res);
        }
        else {
            U.warn(msgLog, "Failed to find DHT update future for update response [futId=" + res.futureId() +
                ", node=" + nodeId + ", res=" + res + ']');
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Deferred atomic update response.
     */
    @SuppressWarnings("unchecked")
    private void processDhtAtomicDeferredUpdateResponse(UUID nodeId, GridDhtAtomicDeferredUpdateResponse res) {
        GridLongList futIds = res.futureIds();

        assert futIds != null && futIds.size() > 0 : futIds;

        for (int i = 0; i < futIds.size(); i++) {
            Long id = futIds.get(i);

            GridDhtAtomicAbstractUpdateFuture updateFut = (GridDhtAtomicAbstractUpdateFuture)ctx.mvcc().atomicFuture(id);

            if (updateFut != null) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Received DHT atomic deferred update response [futId=" + id +
                        ", writeVer=" + res + ", node=" + nodeId + ']');
                }

                updateFut.onDeferredResponse(nodeId);
            }
            else {
                U.warn(msgLog, "Failed to find DHT update future for deferred update response [futId=" + id +
                    ", nodeId=" + nodeId + ", res=" + res + ']');
            }
        }
    }

    /**
     * @param nodeId Originating node ID.
     * @param res Near update response.
     */
    private void sendNearUpdateReply(UUID nodeId, GridNearAtomicUpdateResponse res) {
        try {
            ctx.io().send(nodeId, res, ctx.ioPolicy());

            if (msgLog.isDebugEnabled())
                msgLog.debug("Sent near update response [futId=" + res.futureId() + ", node=" + nodeId + ']');
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Failed to send near update response [futId=" + res.futureId() +
                    ", node=" + nodeId + ']');
            }
        }
        catch (IgniteCheckedException e) {
            U.error(msgLog, "Failed to send near update response [futId=" + res.futureId() +
                ", node=" + nodeId + ", res=" + res + ']', e);
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
        private final GridDhtAtomicAbstractUpdateFuture dhtFut;

        /**
         * @param retVal Return value.
         * @param deleted Deleted entries.
         * @param dhtFut DHT future.
         */
        private UpdateSingleResult(GridCacheReturn retVal,
            Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted,
            GridDhtAtomicAbstractUpdateFuture dhtFut) {
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
        public GridDhtAtomicAbstractUpdateFuture dhtFuture() {
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
        private GridDhtAtomicAbstractUpdateFuture dhtFut;

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
        public GridDhtAtomicAbstractUpdateFuture dhtFuture() {
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
        private void dhtFuture(@Nullable GridDhtAtomicAbstractUpdateFuture dhtFut) {
            this.dhtFut = dhtFut;
        }
    }

    /**
     *
     */
    private static class FinishedLockFuture extends GridFinishedFuture<Boolean> implements GridDhtFuture<Boolean> {
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
     *
     */
    interface UpdateReplyClosure extends CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicUpdateResponse> {
        // No-op.
    }

    /**
     *
     */
    private class DeferredUpdateTimeout implements GridTimeoutObject, Runnable {
        /** */
        private final int part;

        /** */
        private final UUID primaryId;

        /** */
        private final IgniteUuid id;

        /** */
        private final long endTime;

        /**
         * @param part Partition.
         * @param primaryId Primary ID.
         */
        DeferredUpdateTimeout(int part, UUID primaryId) {
            this.part = part;
            this.primaryId = primaryId;

            endTime = U.currentTimeMillis() + DEFERRED_UPDATE_RESPONSE_TIMEOUT;

            id = IgniteUuid.fromUuid(primaryId);
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Map<UUID, GridDhtAtomicDeferredUpdateResponse> resMap = defRes.get();

            GridDhtAtomicDeferredUpdateResponse msg = resMap.get(primaryId);

            if (msg != null && msg.timeoutSender() == this) {
                msg.timeoutSender(null);

                resMap.remove(primaryId);

                sendDeferredUpdateResponse(primaryId, msg);
            }
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            ctx.kernalContext().getStripedExecutorService().execute(part, this);
        }
    }
}
