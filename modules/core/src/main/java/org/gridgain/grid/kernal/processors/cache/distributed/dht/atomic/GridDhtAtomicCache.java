/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.kernal.processors.version.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;
import static org.gridgain.grid.util.direct.GridTcpCommunicationMessageAdapter.*;

/**
 * Non-transactional partitioned cache.
 */
@GridToStringExclude
public class GridDhtAtomicCache<K, V> extends GridDhtCacheAdapter<K, V> {
    /** Version where FORCE_TRANSFORM_BACKUP flag was introduced. */
    public static final GridProductVersion FORCE_TRANSFORM_BACKUP_SINCE = GridProductVersion.fromString("6.1.2");

    /** */
    private static final long serialVersionUID = 0L;

    /** Deferred update response buffer size. */
    private static final int DEFERRED_UPDATE_RESPONSE_BUFFER_SIZE =
        Integer.getInteger(GG_ATOMIC_DEFERRED_ACK_BUFFER_SIZE, 256);

    /** Deferred update response timeout. */
    private static final int DEFERRED_UPDATE_RESPONSE_TIMEOUT =
        Integer.getInteger(GG_ATOMIC_DEFERRED_ACK_TIMEOUT, 500);

    /** Unsafe instance. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Update reply closure. */
    private CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> updateReplyClos;

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
    public GridDhtAtomicCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public boolean isDhtAtomic() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridDhtAtomicCacheEntry<>(ctx, topVer, key, hash, val, next, ttl, hdrId);
            }
        });

        updateReplyClos = new CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>>() {
            @Override public void apply(GridNearAtomicUpdateRequest<K, V> req, GridNearAtomicUpdateResponse<K, V> res) {
                if (ctx.config().getAtomicWriteOrderMode() == CLOCK) {
                    // Always send reply in CLOCK ordering mode.
                    sendNearUpdateReply(res.nodeId(), res);

                    return;
                }

                // Request should be for primary keys only in PRIMARY ordering mode.
                assert req.hasPrimary();

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
    @Override public void start() throws GridException {
        resetMetrics();

        preldr = new GridDhtPreloader<>(ctx);

        preldr.start();

        ctx.io().addHandler(GridNearGetRequest.class, new CI2<UUID, GridNearGetRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearGetRequest<K, V> req) {
                processNearGetRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridNearAtomicUpdateRequest.class, new CI2<UUID, GridNearAtomicUpdateRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearAtomicUpdateRequest<K, V> req) {
                processNearAtomicUpdateRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridNearAtomicUpdateResponse.class, new CI2<UUID, GridNearAtomicUpdateResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearAtomicUpdateResponse<K, V> res) {
                processNearAtomicUpdateResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(GridDhtAtomicUpdateRequest.class, new CI2<UUID, GridDhtAtomicUpdateRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtAtomicUpdateRequest<K, V> req) {
                processDhtAtomicUpdateRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(GridDhtAtomicUpdateResponse.class, new CI2<UUID, GridDhtAtomicUpdateResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtAtomicUpdateResponse<K, V> res) {
                processDhtAtomicUpdateResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(GridDhtAtomicDeferredUpdateResponse.class,
            new CI2<UUID, GridDhtAtomicDeferredUpdateResponse<K, V>>() {
                @Override public void apply(UUID nodeId, GridDhtAtomicDeferredUpdateResponse<K, V> res) {
                    processDhtAtomicDeferredUpdateResponse(nodeId, res);
                }
            });

        if (near == null) {
            ctx.io().addHandler(GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse<K, V>>() {
                @Override public void apply(UUID nodeId, GridNearGetResponse<K, V> res) {
                    processNearGetResponse(nodeId, res);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        boolean isDrSndCache = cacheCfg.getDrSenderConfiguration() != null;
        boolean isDrRcvCache = cacheCfg.getDrReceiverConfiguration() != null;

        GridCacheMetricsAdapter m = new GridCacheMetricsAdapter(isDrSndCache, isDrRcvCache);

        if (ctx.dht().near() != null)
            m.delegate(ctx.dht().near().metrics0());

        metrics = m;
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
    @Override public GridCacheEntry<K, V> entry(K key) {
        return new GridDhtCacheEntryImpl<>(ctx.projectionPerCall(), ctx, key, null);
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable Collection<GridCachePeekMode> modes) throws GridException {
        GridTuple<V> val = null;

        if (ctx.isReplicated() || !modes.contains(NEAR_ONLY)) {
            try {
                val = peek0(true, key, modes, ctx.tm().txx());
            }
            catch (GridCacheFilterFailedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for key: " + key);

                return null;
            }
        }

        return val != null ? val.get() : null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(
        boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean syncCommit,
        boolean syncRollback,
        boolean swapOrOffheapEnabled,
        boolean storeEnabled,
        int txSize,
        @Nullable Object grpLockKey,
        boolean partLock
    ) {
        throw new UnsupportedOperationException("Transactions are not supported for " +
            "GridCacheAtomicityMode.ATOMIC mode (use GridCacheAtomicityMode.TRANSACTIONAL instead)");
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializePortable,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        subjId = ctx.subjectIdPerCall(subjId);

        final UUID subjId0 = subjId;

        return asyncOp(new CO<GridFuture<Map<K, V>>>() {
            @Override public GridFuture<Map<K, V>> apply() {
                return getAllAsync0(keys, false, forcePrimary, filter, subjId0, taskName, deserializePortable);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable GridCacheEntryEx<K, V> cached, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return putAsync(key, val, cached, ttl, filter).get();
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val, @Nullable GridCacheEntryEx<K, V> cached,
        long ttl, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return putxAsync(key, val, cached, ttl, filter).get();
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return putxAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry,
        long ttl, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return updateAllAsync0(F0.asMap(key, val), null, null, null, true, false, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return updateAllAsync0(F0.asMap(key, val), null, null, null, false, false, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) throws GridException {
        return putIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws GridException {
        return putxIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return putxAsync(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) throws GridException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> replaceAsync(K key, V val) {
        return putAsync(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws GridException {
        return replacexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replacexAsync(K key, V val) {
        return putxAsync(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws GridException {
        return replaceAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return putxAsync(key, newVal, ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> removex(K key, V val) throws GridException {
        return removexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws GridException {
        return replacexAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<GridCacheReturn<V>> removexAsync(K key, V val) {
        return removeAllAsync0(F.asList(key), null, null, true, true, ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal) {
        return updateAllAsync0(F.asMap(key, newVal), null, null, null, true, true, null, 0,
            ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        putAllAsync(m, filter).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return updateAllAsync0(m, null, null, null, false, false, null, 0, filter);
    }

    /** {@inheritDoc} */
    @Override public void putAllDr(Map<? extends K, GridCacheDrInfo<V>> drMap) throws GridException {
        putAllDrAsync(drMap).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> putAllDrAsync(Map<? extends K, GridCacheDrInfo<V>> drMap) {
        metrics.onReceiveCacheEntriesReceived(drMap.size());

        return updateAllAsync0(null, null, drMap, null, false, false, null, 0, null);
    }

    /** {@inheritDoc} */
    @Override public void transform(K key, GridClosure<V, V> transformer) throws GridException {
        transformAsync(key, transformer).get();
    }

    /** {@inheritDoc} */
    @Override public <R> R transformAndCompute(K key, GridClosure<V, GridBiTuple<V, R>> transformer)
        throws GridException {
        return (R)updateAllAsync0(null,
            Collections.singletonMap(key, new GridCacheTransformComputeClosure<>(transformer)), null, null, true,
            false, null, 0, null).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAsync(K key, GridClosure<V, V> transformer,
        @Nullable GridCacheEntryEx<K, V> entry, long ttl) {
        return updateAllAsync0(null, Collections.singletonMap(key, transformer), null, null, false, false, entry, ttl,
            null);
    }

    /** {@inheritDoc} */
    @Override public void transformAll(@Nullable Map<? extends K, ? extends GridClosure<V, V>> m) throws GridException {
        transformAllAsync(m).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAllAsync(@Nullable Map<? extends K, ? extends GridClosure<V, V>> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<Object>(ctx.kernalContext());

        return updateAllAsync0(null, m, null, null, false, false, null, 0, null);
    }

    /** {@inheritDoc} */
    @Override public V remove(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return removeAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return removeAllAsync0(Collections.singletonList(key), null, entry, true, false, filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        removeAllAsync(keys, filter).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllAsync(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return removeAllAsync0(keys, null, null, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return removexAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return removeAllAsync0(Collections.singletonList(key), null, entry, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws GridException {
        return removeAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removeAsync(K key, V val) {
        return removexAsync(key, ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        removeAllAsync(filter).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllAsync(GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return removeAllAsync(keySet(filter), filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAllDr(Map<? extends K, GridCacheVersion> drMap) throws GridException {
        removeAllDrAsync(drMap).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllDrAsync(Map<? extends K, GridCacheVersion> drMap) {
        metrics.onReceiveCacheEntriesReceived(drMap.size());

        return removeAllAsync0(null, drMap, null, false, false, null);
    }

    /**
     * @return {@code True} if store enabled.
     */
    private boolean storeEnabled() {
        return ctx.isStoreEnabled() && ctx.config().getStore() != null;
    }

    /**
     * @param op Operation closure.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected <T> GridFuture<T> asyncOp(final CO<GridFuture<T>> op) {
        GridFuture<T> fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            GridFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                GridFuture<T> f = new GridEmbeddedFuture<>(fut,
                    new C2<T, Exception, GridFuture<T>>() {
                        @Override public GridFuture<T> apply(T t, Exception e) {
                            return op.apply();
                        }
                    }, ctx.kernalContext());

                saveFuture(holder, f);

                return f;
            }

            GridFuture<T> f = op.apply();

            saveFuture(holder, f);

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override protected GridFuture<Boolean> lockAllAsync(Collection<? extends K> keys,
        long timeout,
        @Nullable GridCacheTxLocalEx<K, V> tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable GridCacheTxIsolation isolation,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return new FinishedLockFuture(new UnsupportedOperationException("Locks are not supported for " +
            "GridCacheAtomicityMode.ATOMIC mode (use GridCacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /**
     * Entry point for all public API put/transform methods.
     *
     * @param map Put map. Either {@code map}, {@code transformMap} or {@code drMap} should be passed.
     * @param transformMap Transform map. Either {@code map}, {@code transformMap} or {@code drMap} should be passed.
     * @param drPutMap DR put map.
     * @param drRmvMap DR remove map.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param cached Cached cache entry for key. May be passed if and only if map size is {@code 1}.
     * @param ttl Entry time-to-live.
     * @param filter Cache entry filter for atomic updates.
     * @return Completion future.
     */
    private GridFuture updateAllAsync0(
        @Nullable final Map<? extends K, ? extends V> map,
        @Nullable final Map<? extends K, ? extends GridClosure<V, V>> transformMap,
        @Nullable final Map<? extends K, GridCacheDrInfo<V>> drPutMap,
        @Nullable final Map<? extends K, GridCacheVersion> drRmvMap,
        final boolean retval,
        final boolean rawRetval,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        if (map != null)
            validateCacheKeys(map.keySet());

        ctx.checkSecurity(GridSecurityPermission.CACHE_PUT);

        UUID subjId = ctx.subjectIdPerCall(null);

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        final GridNearAtomicUpdateFuture<K, V> updateFut = new GridNearAtomicUpdateFuture<>(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            transformMap != null ? TRANSFORM : UPDATE,
            map != null ? map.keySet() : transformMap != null ? transformMap.keySet() : drPutMap != null ?
                drPutMap.keySet() : drRmvMap.keySet(),
            map != null ? map.values() : transformMap != null ? transformMap.values() : null,
            drPutMap != null ? drPutMap.values() : null,
            drRmvMap != null ? drRmvMap.values() : null,
            retval,
            rawRetval,
            cached,
            ttl,
            filter,
            subjId,
            taskNameHash);

        return asyncOp(new CO<GridFuture<Object>>() {
            @Override public GridFuture<Object> apply() {
                updateFut.map();

                return updateFut;
            }
        });
    }

    /**
     * Entry point for all public API remove methods.
     *
     * @param keys Keys to remove.
     * @param drMap DR map.
     * @param cached Cached cache entry for key. May be passed if and only if keys size is {@code 1}.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter for atomic removes.
     * @return Completion future.
     */
    private GridFuture removeAllAsync0(
        @Nullable final Collection<? extends K> keys,
        @Nullable final Map<? extends K, GridCacheVersion> drMap,
        @Nullable GridCacheEntryEx<K, V> cached,
        final boolean retval,
        boolean rawRetval,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        assert keys != null || drMap != null;

        validateCacheKeys(keys);

        ctx.checkSecurity(GridSecurityPermission.CACHE_REMOVE);

        UUID subjId = ctx.subjectIdPerCall(null);

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        final GridNearAtomicUpdateFuture<K, V> updateFut = new GridNearAtomicUpdateFuture<>(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            DELETE,
            keys != null ? keys : drMap.keySet(),
            null,
            null,
            keys != null ? null : drMap.values(),
            retval,
            rawRetval,
            cached,
            0,
            filter,
            subjId,
            taskNameHash);

        return asyncOp(new CO<GridFuture<Object>>() {
            @Override public GridFuture<Object> apply() {
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
     * @param filter Filter.
     * @return Get future.
     */
    private GridFuture<Map<K, V>> getAllAsync0(@Nullable Collection<? extends K> keys, boolean reload,
        boolean forcePrimary, @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter, UUID subjId, String taskName,
        boolean deserializePortable) {
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());

        validateCacheKeys(keys);

        long topVer = ctx.affinity().affinityTopologyVersion();

        // Optimisation: try to resolve value locally and escape 'get future' creation.
        if (!reload && !forcePrimary) {
            Map<K, V> locVals = new HashMap<>(keys.size(), 1.0f);

            GridCacheVersion obsoleteVer = null;

            boolean success = true;

            // Optimistically expect that all keys are available locally (avoid creation of get future).
            for (K key : keys) {
                GridCacheEntryEx<K, V> entry = null;

                while (true) {
                    try {
                        entry = ctx.isSwapOrOffheapEnabled() ? entryEx(key) : peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            V v = entry.innerGet(null, /*swap*/true, /*read-through*/false, /*fail-fast*/true,
                                /*unmarshal*/true, /**update-metrics*/true, /*event*/true, subjId, null, taskName,
                                filter);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                if (obsoleteVer == null)
                                    obsoleteVer = context().versions().next();

                                if (isNew && entry.markObsoleteIfEmpty(obsoleteVer))
                                    removeIfObsolete(key);

                                success = false;
                            }
                            else {
                                if (ctx.portableEnabled() && deserializePortable && v instanceof GridPortableObject)
                                    v = ((GridPortableObject)v).deserialize();

                                locVals.put(key, v);
                            }
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
                    catch (GridException e) {
                        return new GridFinishedFuture<>(ctx.kernalContext(), e);
                    }
                    finally {
                        if (entry != null)
                            ctx.evicts().touch(entry, topVer);
                    }
                }

                if (!success)
                    break;
            }

            if (success)
                return ctx.wrapCloneMap(new GridFinishedFuture<>(ctx.kernalContext(), locVals));
        }

        // Either reload or not all values are available locally.
        GridPartitionedGetFuture<K, V> fut = new GridPartitionedGetFuture<>(ctx, keys, topVer, reload, forcePrimary,
            filter, subjId, taskName, deserializePortable);

        fut.init();

        return ctx.wrapCloneMap(fut);
    }

    /**
     * Executes local update.
     *
     * @param nodeId Node ID.
     * @param req Update request.
     * @param cached Cached entry if updating single local entry.
     * @param completionCb Completion callback.
     */
    public void updateAllAsyncInternal(
        final UUID nodeId,
        final GridNearAtomicUpdateRequest<K, V> req,
        @Nullable final GridCacheEntryEx<K, V> cached,
        final CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb
    ) {
        GridFuture<Object> forceFut = preldr.request(req.keys(), req.topologyVersion());

        if (forceFut.isDone())
            updateAllAsyncInternal0(nodeId, req, cached, completionCb);
        else {
            forceFut.listenAsync(new CI1<GridFuture<Object>>() {
                @Override public void apply(GridFuture<Object> t) {
                    updateAllAsyncInternal0(nodeId, req, cached, completionCb);
                }
            });
        }
    }

    /**
     * Executes local update after preloader fetched values.
     *
     * @param nodeId Node ID.
     * @param req Update request.
     * @param cached Cached entry if updating single local entry.
     * @param completionCb Completion callback.
     */
    public void updateAllAsyncInternal0(
        UUID nodeId,
        GridNearAtomicUpdateRequest<K, V> req,
        @Nullable GridCacheEntryEx<K, V> cached,
        CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb
    ) {
        GridNearAtomicUpdateResponse<K, V> res = new GridNearAtomicUpdateResponse<>(nodeId, req.futureVersion());

        List<K> keys = req.keys();

        assert !req.returnValue() || keys.size() == 1;

        GridDhtAtomicUpdateFuture<K, V> dhtFut = null;

        boolean remap = false;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        try {
            // If batch store update is enabled, we need to lock all entries.
            // First, need to acquire locks on cache entries, then check filter.
            List<GridDhtCacheEntry<K, V>> locked = lockEntries(keys, req.topologyVersion());
            Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted = null;

            try {
                topology().readLock();

                try {
                    // Do not check topology version for CLOCK versioning since
                    // partition exchange will wait for near update future.
                    if (topology().topologyVersion() == req.topologyVersion() ||
                        ctx.config().getAtomicWriteOrderMode() == CLOCK) {
                        GridNode node = ctx.discovery().node(nodeId);

                        if (node == null) {
                            U.warn(log, "Node originated update request left grid: " + nodeId);

                            return;
                        }

                        checkClearForceTransformBackups(req, locked);

                        boolean hasNear = U.hasNearCache(node, name());

                        GridCacheVersion ver = req.updateVersion();

                        if (ver == null) {
                            // Assign next version for update inside entries lock.
                            ver = ctx.versions().next(req.topologyVersion());

                            if (hasNear)
                                res.nearVersion(ver);
                        }

                        assert ver != null : "Got null version for update request: " + req;

                        if (log.isDebugEnabled())
                            log.debug("Using cache version for update request on primary node [ver=" + ver +
                                ", req=" + req + ']');

                        dhtFut = createDhtFuture(ver, req, res, completionCb, false);

                        GridCacheReturn<Object> retVal = null;

                        boolean replicate = ctx.isDrEnabled();

                        if (storeEnabled() && keys.size() > 1 && cacheCfg.getDrReceiverConfiguration() == null) {
                            // This method can only be used when there are no replicated entries in the batch.
                            UpdateBatchResult<K, V> updRes = updateWithBatch(nodeId, hasNear, req, res, locked, ver,
                                dhtFut, completionCb, replicate, taskName);

                            deleted = updRes.deleted();
                            dhtFut = updRes.dhtFuture();
                        }
                        else {
                            UpdateSingleResult<K, V> updRes = updateSingle(nodeId, hasNear, req, res, locked, ver,
                                dhtFut, completionCb, replicate, taskName);

                            retVal = updRes.returnValue();
                            deleted = updRes.deleted();
                            dhtFut = updRes.dhtFuture();
                        }

                        if (retVal == null)
                            retVal = new GridCacheReturn<>(null, true);

                        res.returnValue(retVal);
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
                unlockEntries(locked, req.topologyVersion());

                // Enqueue if necessary after locks release.
                if (deleted != null) {
                    assert !deleted.isEmpty();
                    assert ctx.deferredDelete();

                    for (GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion> e : deleted)
                        ctx.onDeferredDelete(e.get1(), e.get2());
                }
            }
        }
        catch (GridDhtInvalidPartitionException ignore) {
            assert ctx.config().getAtomicWriteOrderMode() == PRIMARY;

            if (log.isDebugEnabled())
                log.debug("Caught invalid partition exception for cache entry (will remap update request): " + req);

            remap = true;
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
    }

    /**
     * Updates locked entries using batched write-through.
     *
     * @param nodeId Sender node ID.
     * @param hasNear {@code True} if originating node has near cache.
     * @param req Update request.
     * @param res Update response.
     * @param locked Locked entries.
     * @param ver Assigned version.
     * @param dhtFut Optional DHT future.
     * @param completionCb Completion callback to invoke when DHT future is completed.
     * @param replicate Whether replication is enabled.
     * @return Deleted entries.
     * @throws GridCacheEntryRemovedException Should not be thrown.
     */
    @SuppressWarnings("unchecked")
    private UpdateBatchResult<K, V> updateWithBatch(
        UUID nodeId,
        boolean hasNear,
        GridNearAtomicUpdateRequest<K, V> req,
        GridNearAtomicUpdateResponse<K, V> res,
        List<GridDhtCacheEntry<K, V>> locked,
        GridCacheVersion ver,
        @Nullable GridDhtAtomicUpdateFuture<K, V> dhtFut,
        CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb,
        boolean replicate,
        String taskName
    ) throws GridCacheEntryRemovedException {
        // Cannot update in batches during DR due to possible conflicts.
        assert !req.returnValue(); // Should not request return values for putAll.

        int size = req.keys().size();

        Map<K, V> putMap = null;
        Map<K, GridClosure<V, V>> transformMap = null;
        Collection<K> rmvKeys = null;
        UpdateBatchResult<K, V> updRes = new UpdateBatchResult<>();
        List<GridDhtCacheEntry<K, V>> filtered = new ArrayList<>(size);
        GridCacheOperation op = req.operation();

        int firstEntryIdx = 0;

        boolean intercept = ctx.config().getInterceptor() != null;

        for (int i = 0; i < locked.size(); i++) {
            GridDhtCacheEntry<K, V> entry = locked.get(i);

            if (entry == null)
                continue;

            try {
                if (!checkFilter(entry, req, res)) {
                    if (log.isDebugEnabled())
                        log.debug("Entry did not pass the filter (will skip write) [entry=" + entry +
                            ", filter=" + Arrays.toString(req.filter()) + ", res=" + res + ']');

                    if (hasNear)
                        res.addSkippedIndex(i);

                    firstEntryIdx++;

                    continue;
                }

                if (op == TRANSFORM) {
                    GridClosure<V, V> transform = req.transformClosure(i);

                    V old = entry.innerGet(
                        null,
                        /*read swap*/true,
                        /*read through*/true,
                        /*fail fast*/false,
                        /*unmarshal*/true,
                        /*metrics*/true,
                        /*event*/true,
                        req.subjectId(),
                        transform,
                        taskName,
                        CU.<K, V>empty());

                    if (transformMap == null)
                        transformMap = new HashMap<>();

                    transformMap.put(entry.key(), transform);

                    V updated = transform.apply(old);

                    if (updated == null) {
                        if (intercept) {
                            GridBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor().onBeforeRemove(
                                entry.key(), old);

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
                                nodeId,
                                putMap,
                                null,
                                transformMap,
                                dhtFut,
                                completionCb,
                                req,
                                res,
                                replicate,
                                updRes,
                                taskName);

                            firstEntryIdx = i + 1;

                            putMap = null;
                            transformMap = null;

                            filtered = new ArrayList<>();
                        }

                        // Start collecting new batch.
                        if (rmvKeys == null)
                            rmvKeys = new ArrayList<>(size);

                        rmvKeys.add(entry.key());
                    }
                    else {
                        if (intercept) {
                            updated = (V)ctx.config().getInterceptor().onBeforePut(entry.key(), old, updated);

                            if (updated == null)
                                continue;
                        }

                        // Update previous batch.
                        if (rmvKeys != null) {
                            dhtFut = updatePartialBatch(
                                hasNear,
                                firstEntryIdx,
                                filtered,
                                ver,
                                nodeId,
                                null,
                                rmvKeys,
                                transformMap,
                                dhtFut,
                                completionCb,
                                req,
                                res,
                                replicate,
                                updRes,
                                taskName);

                            firstEntryIdx = i + 1;

                            rmvKeys = null;
                            transformMap = null;

                            filtered = new ArrayList<>();
                        }

                        if (putMap == null)
                            putMap = new LinkedHashMap<>(size, 1.0f);

                        putMap.put(entry.key(), updated);
                    }
                }
                else if (op == UPDATE) {
                    V updated = req.value(i);

                    if (intercept) {
                        V old = entry.innerGet(
                             null,
                            /*read swap*/true,
                            /*read through*/true,
                            /*fail fast*/false,
                            /*unmarshal*/true,
                            /*metrics*/true,
                            /*event*/true,
                            req.subjectId(),
                            null,
                            taskName,
                            CU.<K, V>empty());

                        updated = (V)ctx.config().getInterceptor().onBeforePut(entry.key(), old, updated);

                        if (updated == null)
                            continue;
                    }

                    assert updated != null;

                    if (putMap == null)
                        putMap = new LinkedHashMap<>(size, 1.0f);

                    putMap.put(entry.key(), updated);
                }
                else {
                    assert op == DELETE;

                    if (intercept) {
                        V old = entry.innerGet(
                            null,
                            /*read swap*/true,
                            /*read through*/true,
                            /*fail fast*/false,
                            /*unmarshal*/true,
                            /*metrics*/true,
                            /*event*/true,
                            req.subjectId(),
                            null,
                            taskName,
                            CU.<K, V>empty());

                        GridBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor().onBeforeRemove(
                            entry.key(), old);

                        if (ctx.cancelRemove(interceptorRes))
                            continue;
                    }

                    if (rmvKeys == null)
                        rmvKeys = new ArrayList<>(size);

                    rmvKeys.add(entry.key());
                }

                filtered.add(entry);
            }
            catch (GridException e) {
                res.addFailedKey(entry.key(), e);
            }
            catch (GridCacheFilterFailedException ignore) {
                assert false : "Filter should never fail with failFast=false and empty filter.";
            }
        }

        // Store final batch.
        if (putMap != null || rmvKeys != null) {
            dhtFut = updatePartialBatch(
                hasNear,
                firstEntryIdx,
                filtered,
                ver,
                nodeId,
                putMap,
                rmvKeys,
                transformMap,
                dhtFut,
                completionCb,
                req,
                res,
                replicate,
                updRes,
                taskName);
        }
        else
            assert filtered.isEmpty();

        updRes.dhtFuture(dhtFut);

        return updRes;
    }

    /**
     * Updates locked entries one-by-one.
     *
     * @param nodeId Originating node ID.
     * @param hasNear {@code True} if originating node has near cache.
     * @param req Update request.
     * @param res Update response.
     * @param locked Locked entries.
     * @param ver Assigned update version.
     * @param dhtFut Optional DHT future.
     * @param completionCb Completion callback to invoke when DHT future is completed.
     * @param replicate Whether DR is enabled for that cache.
     * @return Return value.
     * @throws GridCacheEntryRemovedException Should be never thrown.
     */
    private UpdateSingleResult<K, V> updateSingle(
        UUID nodeId,
        boolean hasNear,
        GridNearAtomicUpdateRequest<K, V> req,
        GridNearAtomicUpdateResponse<K, V> res,
        List<GridDhtCacheEntry<K, V>> locked,
        GridCacheVersion ver,
        @Nullable GridDhtAtomicUpdateFuture<K, V> dhtFut,
        CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb,
        boolean replicate,
        String taskName
    ) throws GridCacheEntryRemovedException {
        GridCacheReturn<Object> retVal = null;
        Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted = null;

        List<K> keys = req.keys();

        long topVer = req.topologyVersion();

        boolean checkReaders = hasNear || ctx.discovery().hasNearCache(name(), topVer);

        boolean readersOnly = false;

        boolean intercept = ctx.config().getInterceptor() != null;

        // Avoid iterator creation.
        for (int i = 0; i < keys.size(); i++) {
            K k = keys.get(i);

            GridCacheOperation op = req.operation();

            // We are holding java-level locks on entries at this point.
            // No GridCacheEntryRemovedException can be thrown.
            try {
                GridDhtCacheEntry<K, V> entry = locked.get(i);

                if (entry == null)
                    continue;

                GridCacheVersion newDrVer = req.drVersion(i);
                long newDrTtl = req.drTtl(i);
                long newDrExpireTime = req.drExpireTime(i);

                assert !(newDrVer instanceof GridCacheVersionEx) : newDrVer; // Plain version is expected here.

                if (newDrVer == null)
                    newDrVer = ver;

                boolean primary = !req.fastMap() || ctx.affinity().primary(ctx.localNode(), entry.key(),
                    req.topologyVersion());

                byte[] newValBytes = req.valueBytes(i);

                Object writeVal = req.writeValue(i);

                Collection<UUID> readers = null;
                Collection<UUID> filteredReaders = null;

                if (checkReaders) {
                    readers = entry.readers();
                    filteredReaders = F.view(entry.readers(), F.notEqualTo(nodeId));
                }

                GridCacheUpdateAtomicResult<K, V> updRes = entry.innerUpdate(
                    ver,
                    nodeId,
                    locNodeId,
                    op,
                    writeVal,
                    newValBytes,
                    primary && storeEnabled(),
                    req.returnValue(),
                    req.ttl(),
                    true,
                    true,
                    primary,
                    ctx.config().getAtomicWriteOrderMode() == CLOCK, // Check version in CLOCK mode on primary node.
                    req.filter(),
                    replicate ? primary ? DR_PRIMARY : DR_BACKUP : DR_NONE,
                    newDrTtl,
                    newDrExpireTime,
                    newDrVer,
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
                        GridDrReceiverConflictContextImpl ctx = updRes.drConflictContext();

                        long ttl = updRes.newTtl();
                        long drExpireTime = updRes.drExpireTime();

                        if (ctx == null)
                            newDrVer = null;
                        else if (ctx.isMerge()) {
                            newDrVer = null; // DR version is discarded in case of merge.
                            newValBytes = null; // Value has been changed.
                        }

                        GridClosure<V, V> transformC = null;

                        if (req.forceTransformBackups() && op == TRANSFORM)
                            transformC = (GridClosure<V, V>)writeVal;

                        if (!readersOnly)
                            dhtFut.addWriteEntry(entry, updRes.newValue(), newValBytes, transformC,
                                drExpireTime >= 0L ? ttl : -1L, drExpireTime, newDrVer, drExpireTime < 0L ? ttl : 0L);

                        if (!F.isEmpty(filteredReaders))
                            dhtFut.addNearWriteEntries(filteredReaders, entry, updRes.newValue(), newValBytes,
                                transformC, drExpireTime < 0L ? ttl : 0L);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Entry did not pass the filter or conflict resolution (will skip write) " +
                                "[entry=" + entry + ", filter=" + Arrays.toString(req.filter()) + ']');
                    }
                }

                if (hasNear) {
                    if (primary && updRes.sendToDht()) {
                        if (!U.nodeIds(context().affinity().nodes(entry.partition(), topVer)).contains(nodeId)) {
                            GridDrReceiverConflictContextImpl ctx = updRes.drConflictContext();

                            res.nearTtl(updRes.newTtl());

                            if (ctx != null && ctx.isMerge())
                                newValBytes = null;

                            // If put the same value as in request then do not need to send it back.
                            if (op == TRANSFORM || writeVal != updRes.newValue())
                                res.addNearValue(i, updRes.newValue(), newValBytes);

                            if (updRes.newValue() != null || newValBytes != null) {
                                GridFuture<Boolean> f = entry.addReader(nodeId, req.messageId(), topVer);

                                assert f == null : f;
                            }
                        }
                        else if (F.contains(readers, nodeId)) // Reader became primary or backup.
                            entry.removeReader(nodeId, req.messageId());
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

                // Create only once.
                if (retVal == null) {
                    Object ret = updRes.oldValue();

                    if (op == TRANSFORM && writeVal instanceof GridCacheTransformComputeClosure) {
                        assert req.returnValue();

                        ret = ((GridCacheTransformComputeClosure<V, ?>)writeVal).returnValue();
                    }

                    retVal = new GridCacheReturn<>(req.returnValue() ? ret : null, updRes.success());
                }
            }
            catch (GridException e) {
                res.addFailedKey(k, e);
            }
        }

        return new UpdateSingleResult<>(retVal, deleted, dhtFut);
    }

    /**
     * @param hasNear {@code True} if originating node has near cache.
     * @param firstEntryIdx Index of the first entry in the request keys collection.
     * @param entries Entries to update.
     * @param ver Version to set.
     * @param nodeId Originating node ID.
     * @param putMap Values to put.
     * @param rmvKeys Keys to remove.
     * @param transformMap Transform closures.
     * @param dhtFut DHT update future if has backups.
     * @param completionCb Completion callback to invoke when DHT future is completed.
     * @param req Request.
     * @param res Response.
     * @param replicate Whether replication is enabled.
     * @param batchRes Batch update result.
     * @return Deleted entries.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable private GridDhtAtomicUpdateFuture<K, V> updatePartialBatch(
        boolean hasNear,
        int firstEntryIdx,
        List<GridDhtCacheEntry<K, V>> entries,
        final GridCacheVersion ver,
        UUID nodeId,
        @Nullable Map<K, V> putMap,
        @Nullable Collection<K> rmvKeys,
        @Nullable Map<K, GridClosure<V, V>> transformMap,
        @Nullable GridDhtAtomicUpdateFuture<K, V> dhtFut,
        CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb,
        final GridNearAtomicUpdateRequest<K, V> req,
        final GridNearAtomicUpdateResponse<K, V> res,
        boolean replicate,
        UpdateBatchResult<K, V> batchRes,
        String taskName
    ) {
        assert putMap == null ^ rmvKeys == null;

        assert req.drVersions() == null : "updatePartialBatch cannot be called when there are DR entries in the batch.";

        long topVer = req.topologyVersion();

        boolean checkReaders = hasNear || ctx.discovery().hasNearCache(name(), topVer);

        try {
            GridCacheOperation op;

            if (putMap != null) {
                // If fast mapping, filter primary keys for write to store.
                Map<K, V> storeMap = req.fastMap() ?
                    F.view(putMap, new P1<K>() {
                        @Override public boolean apply(K key) {
                            return ctx.affinity().primary(ctx.localNode(), key, req.topologyVersion());
                        }
                    }) :
                    putMap;

                ctx.store().putAllToStore(null, F.viewReadOnly(storeMap, new C1<V, GridBiTuple<V, GridCacheVersion>>() {
                    @Override public GridBiTuple<V, GridCacheVersion> apply(V v) {
                        return F.t(v, ver);
                    }
                }));

                op = UPDATE;
            }
            else {
                // If fast mapping, filter primary keys for write to store.
                Collection<K> storeKeys = req.fastMap() ?
                    F.view(rmvKeys, new P1<K>() {
                        @Override public boolean apply(K key) {
                            return ctx.affinity().primary(ctx.localNode(), key, req.topologyVersion());
                        }
                    }) :
                    rmvKeys;

                ctx.store().removeAllFromStore(null, storeKeys);

                op = DELETE;
            }

            boolean intercept = ctx.config().getInterceptor() != null;

            // Avoid iterator creation.
            for (int i = 0; i < entries.size(); i++) {
                GridDhtCacheEntry<K, V> entry = entries.get(i);

                assert Thread.holdsLock(entry);

                if (entry.obsolete()) {
                    assert req.operation() == DELETE : "Entry can become obsolete only after remove: " + entry;

                    continue;
                }

                try {
                    // We are holding java-level locks on entries at this point.
                    V writeVal = op == UPDATE ? putMap.get(entry.key()) : null;

                    assert writeVal != null || op == DELETE : "null write value found.";

                    boolean primary = !req.fastMap() || ctx.affinity().primary(ctx.localNode(), entry.key(),
                        req.topologyVersion());

                    Collection<UUID> readers = null;
                    Collection<UUID> filteredReaders = null;

                    if (checkReaders) {
                        readers = entry.readers();
                        filteredReaders = F.view(entry.readers(), F.notEqualTo(nodeId));
                    }

                    GridCacheUpdateAtomicResult<K, V> updRes = entry.innerUpdate(
                        ver,
                        nodeId,
                        locNodeId,
                        op,
                        writeVal,
                        null,
                        false,
                        false,
                        req.ttl(),
                        true,
                        true,
                        primary,
                        ctx.config().getAtomicWriteOrderMode() == CLOCK, // Check version in CLOCK mode on primary node.
                        req.filter(),
                        replicate ? primary ? DR_PRIMARY : DR_BACKUP : DR_NONE,
                        -1L,
                        -1L,
                        null,
                        false,
                        false,
                        req.subjectId(),
                        taskName);

                    if (intercept) {
                        if (op == UPDATE)
                            ctx.config().getInterceptor().onAfterPut(entry.key(), updRes.newValue());
                        else {
                            assert op == DELETE : op;

                            // Old value should be already loaded for 'GridCacheInterceptor.onBeforeRemove'.
                            ctx.config().<K, V>getInterceptor().onAfterRemove(entry.key(), updRes.oldValue());
                        }
                    }

                    batchRes.addDeleted(entry, updRes, entries);

                    if (dhtFut == null && !F.isEmpty(filteredReaders)) {
                        dhtFut = createDhtFuture(ver, req, res, completionCb, true);

                        batchRes.readersOnly(true);
                    }

                    if (dhtFut != null) {
                        GridCacheValueBytes valBytesTuple = op == DELETE ? GridCacheValueBytes.nil():
                            entry.valueBytes();

                        byte[] valBytes = valBytesTuple.getIfMarshaled();

                        GridClosure<V, V> transformC = transformMap == null ? null : transformMap.get(entry.key());

                        if (!batchRes.readersOnly())
                            dhtFut.addWriteEntry(entry, writeVal, valBytes, transformC, -1, -1, null, req.ttl());

                        if (!F.isEmpty(filteredReaders))
                            dhtFut.addNearWriteEntries(filteredReaders, entry, writeVal, valBytes, transformC,
                                req.ttl());
                    }

                    if (hasNear) {
                        if (primary) {
                            if (!U.nodeIds(context().affinity().nodes(entry.partition(), topVer)).contains(nodeId)) {
                                if (req.operation() == TRANSFORM) {
                                    int idx = firstEntryIdx + i;

                                    GridCacheValueBytes valBytesTuple = entry.valueBytes();

                                    byte[] valBytes = valBytesTuple.getIfMarshaled();

                                    res.addNearValue(idx, writeVal, valBytes);
                                }

                                res.nearTtl(req.ttl());

                                if (writeVal != null || !entry.valueBytes().isNull()) {
                                    GridFuture<Boolean> f = entry.addReader(nodeId, req.messageId(), topVer);

                                    assert f == null : f;
                                }
                            } else if (readers.contains(nodeId)) // Reader became primary or backup.
                                entry.removeReader(nodeId, req.messageId());
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
        catch (GridException e) {
            res.addFailedKeys(putMap != null ? putMap.keySet() : rmvKeys, e);
        }

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
    private List<GridDhtCacheEntry<K, V>> lockEntries(List<K> keys, long topVer)
        throws GridDhtInvalidPartitionException {
        if (keys.size() == 1) {
            K key = keys.get(0);

            while (true) {
                try {
                    GridDhtCacheEntry<K, V> entry = entryExx(key, topVer);

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
            List<GridDhtCacheEntry<K, V>> locked = new ArrayList<>(keys.size());

            while (true) {
                for (K key : keys) {
                    try {
                        GridDhtCacheEntry<K, V> entry = entryExx(key, topVer);

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
                    GridCacheMapEntry<K, V> entry = locked.get(i);

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
    private void unlockEntries(Collection<GridDhtCacheEntry<K, V>> locked, long topVer) {
        // Process deleted entries before locks release.
        assert ctx.deferredDelete();

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        Collection<K> skip = null;

        for (GridCacheMapEntry<K, V> entry : locked) {
            if (entry != null && entry.deleted()) {
                if (skip == null)
                    skip = new HashSet<>(locked.size(), 1.0f);

                skip.add(entry.key());
            }
        }

        // Release locks.
        for (GridCacheMapEntry<K, V> entry : locked) {
            if (entry != null)
                UNSAFE.monitorExit(entry);
        }

        // Try evict partitions.
        for (GridDhtCacheEntry<K, V> entry : locked) {
            if (entry != null)
                entry.onUnlock();
        }

        if (skip != null && skip.size() == locked.size())
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (GridCacheMapEntry<K, V> entry : locked) {
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
    private boolean checkFilter(GridCacheEntryEx<K, V> entry, GridNearAtomicUpdateRequest<K, V> req,
        GridNearAtomicUpdateResponse<K, V> res) {
        try {
            return ctx.isAll(entry.wrapFilterLocked(), req.filter());
        }
        catch (GridException e) {
            res.addFailedKey(entry.key(), e);

            return false;
        }
    }

    /**
     * @param req Request to remap.
     */
    private void remapToNewPrimary(GridNearAtomicUpdateRequest<K, V> req) {
        if (log.isDebugEnabled())
            log.debug("Remapping near update request locally: " + req);

        Collection<?> vals;
        Collection<GridCacheDrInfo<V>> drPutVals;
        Collection<GridCacheVersion> drRmvVals;

        if (req.drVersions() == null) {
            vals = req.values();

            drPutVals = null;
            drRmvVals = null;
        }
        else if (req.operation() == UPDATE) {
            int size = req.keys().size();

            drPutVals = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                Long ttl = req.drTtl(i);

                if (ttl == null)
                    drPutVals.add(new GridCacheDrInfo<>(req.value(i), req.drVersion(i)));
                else
                    drPutVals.add(new GridCacheDrExpirationInfo<>(req.value(i), req.drVersion(i), ttl,
                        req.drExpireTime(i)));
            }

            vals = null;
            drRmvVals = null;
        }
        else {
            assert req.operation() == DELETE;

            drRmvVals = req.drVersions();

            vals = null;
            drPutVals = null;
        }

        final GridNearAtomicUpdateFuture<K, V> updateFut = new GridNearAtomicUpdateFuture<>(
            ctx,
            this,
            ctx.config().getWriteSynchronizationMode(),
            req.operation(),
            req.keys(),
            vals,
            drPutVals,
            drRmvVals,
            req.returnValue(),
            false,
            null,
            req.ttl(),
            req.filter(),
            req.subjectId(),
            req.taskNameHash());

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
    @Nullable private GridDhtAtomicUpdateFuture<K, V> createDhtFuture(
        GridCacheVersion writeVer,
        GridNearAtomicUpdateRequest<K, V> updateReq,
        GridNearAtomicUpdateResponse<K, V> updateRes,
        CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb,
        boolean force
    ) {
        if (!force) {
            if (updateReq.fastMap())
                return null;

            long topVer = updateReq.topologyVersion();

            Collection<GridNode> nodes = ctx.kernalContext().discovery().cacheAffinityNodes(name(), topVer);

            // We are on primary node for some key.
            assert !nodes.isEmpty();

            if (nodes.size() == 1) {
                if (log.isDebugEnabled())
                    log.debug("Partitioned cache topology has only one node, will not create DHT atomic update future " +
                        "[topVer=" + topVer + ", updateReq=" + updateReq + ']');

                return null;
            }
        }

        GridDhtAtomicUpdateFuture<K, V> fut = new GridDhtAtomicUpdateFuture<>(ctx, completionCb, writeVer, updateReq,
            updateRes);

        ctx.mvcc().addAtomicFuture(fut.version(), fut);

        return fut;
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near get response.
     */
    private void processNearGetResponse(UUID nodeId, GridNearGetResponse<K, V> res) {
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
    private void processNearAtomicUpdateRequest(UUID nodeId, GridNearAtomicUpdateRequest<K, V> req) {
        if (log.isDebugEnabled())
            log.debug("Processing near atomic update request [nodeId=" + nodeId + ", req=" + req + ']');

        req.nodeId(ctx.localNodeId());

        updateAllAsyncInternal(nodeId, req, null, updateReplyClos);
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near atomic update response.
     */
    @SuppressWarnings("unchecked")
    private void processNearAtomicUpdateResponse(UUID nodeId, GridNearAtomicUpdateResponse<K, V> res) {
        if (log.isDebugEnabled())
            log.debug("Processing near atomic update response [nodeId=" + nodeId + ", res=" + res + ']');

        res.nodeId(ctx.localNodeId());

        GridNearAtomicUpdateFuture<K, V> fut = (GridNearAtomicUpdateFuture)ctx.mvcc().atomicFuture(res.futureVersion());

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
    private void processDhtAtomicUpdateRequest(UUID nodeId, GridDhtAtomicUpdateRequest<K, V> req) {
        if (log.isDebugEnabled())
            log.debug("Processing dht atomic update request [nodeId=" + nodeId + ", req=" + req + ']');

        GridCacheVersion ver = req.writeVersion();

        // Always send update reply.
        GridDhtAtomicUpdateResponse<K, V> res = new GridDhtAtomicUpdateResponse<>(req.futureVersion());

        Boolean replicate = ctx.isDrEnabled();

        boolean intercept = req.forceTransformBackups() && ctx.config().getInterceptor() != null;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        for (int i = 0; i < req.size(); i++) {
            K key = req.key(i);

            try {
                while (true) {
                    GridDhtCacheEntry<K, V> entry = null;

                    try {
                        entry = entryExx(key);

                        V val = req.value(i);
                        byte[] valBytes = req.valueBytes(i);
                        GridClosure<V, V> transform = req.transformClosure(i);

                        GridCacheOperation op = transform != null ? TRANSFORM :
                            (val != null || valBytes != null) ?
                                UPDATE :
                                DELETE;

                        GridCacheUpdateAtomicResult<K, V> updRes = entry.innerUpdate(
                            ver,
                            nodeId,
                            nodeId,
                            op,
                            op == TRANSFORM ? transform : val,
                            valBytes,
                            /*write-through*/false,
                            /*retval*/false,
                            req.ttl(),
                            /*event*/true,
                            /*metrics*/true,
                            /*primary*/false,
                            /*check version*/!req.forceTransformBackups(),
                            CU.<K, V>empty(),
                            replicate ? DR_BACKUP : DR_NONE,
                            req.drTtl(i),
                            req.drExpireTime(i),
                            req.drVersion(i),
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
            catch (GridException e) {
                res.addFailedKey(key, new GridException("Failed to update key on backup node: " + key, e));
            }
        }

        if (isNearEnabled(cacheCfg))
            ((GridNearAtomicCache<K, V>)near()).processDhtAtomicUpdateRequest(nodeId, req, res);

        try {
            if (res.failedKeys() != null || res.nearEvicted() != null || req.writeSynchronizationMode() == FULL_SYNC)
                ctx.io().send(nodeId, res);
            else {
                // No failed keys and sync mode is not FULL_SYNC, thus sending deferred response.
                sendDeferredUpdateResponse(nodeId, req.futureVersion());
            }
        }
        catch (GridTopologyException ignored) {
            U.warn(log, "Failed to send DHT atomic update response to node because it left grid: " +
                req.nodeId());
        }
        catch (GridException e) {
            U.error(log, "Failed to send DHT atomic update response (did node leave grid?) [nodeId=" + nodeId +
                ", req=" + req + ']', e);
        }
    }

    /**
     * Checks if entries being transformed are empty. Clears forceTransformBackup flag enforcing
     * sending transformed value to backups if at least one empty entry is found.
     *
     * @param req Near atomic update request.
     * @param locked Already locked entries (from the request).
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void checkClearForceTransformBackups(GridNearAtomicUpdateRequest<K, V> req,
        List<GridDhtCacheEntry<K, V>> locked) {
        if (ctx.isStoreEnabled() && req.operation() == TRANSFORM) {
            for (int i = 0; i < locked.size(); i++) {
                if (!locked.get(i).hasValue()) {
                    req.forceTransformBackups(false);

                    return;
                }
            }
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
    private void processDhtAtomicUpdateResponse(UUID nodeId, GridDhtAtomicUpdateResponse<K, V> res) {
        if (log.isDebugEnabled())
            log.debug("Processing dht atomic update response [nodeId=" + nodeId + ", res=" + res + ']');

        GridDhtAtomicUpdateFuture<K, V> updateFut = (GridDhtAtomicUpdateFuture<K, V>)ctx.mvcc().
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
    private void processDhtAtomicDeferredUpdateResponse(UUID nodeId, GridDhtAtomicDeferredUpdateResponse<K, V> res) {
        if (log.isDebugEnabled())
            log.debug("Processing deferred dht atomic update response [nodeId=" + nodeId + ", res=" + res + ']');

        for (GridCacheVersion ver : res.futureVersions()) {
            GridDhtAtomicUpdateFuture<K, V> updateFut = (GridDhtAtomicUpdateFuture<K, V>)ctx.mvcc().atomicFuture(ver);

            if (updateFut != null)
                updateFut.onResult(nodeId);
            else
                U.warn(log, "Failed to find DHT update future for deferred update response [nodeId=" +
                    nodeId + ", res=" + res + ']');
        }
    }

    /**
     * @param nodeId Originating node ID.
     * @param res Near update response.
     */
    private void sendNearUpdateReply(UUID nodeId, GridNearAtomicUpdateResponse<K, V> res) {
        try {
            ctx.io().send(nodeId, res);
        }
        catch (GridTopologyException ignored) {
            U.warn(log, "Failed to send near update reply to node because it left grid: " +
                nodeId);
        }
        catch (GridException e) {
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
    private static class UpdateSingleResult<K, V> {
        /** */
        private final GridCacheReturn<Object> retVal;

        /** */
        private final Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted;

        /** */
        private final GridDhtAtomicUpdateFuture<K, V> dhtFut;

        /**
         * @param retVal Return value.
         * @param deleted Deleted entries.
         * @param dhtFut DHT future.
         */
        private UpdateSingleResult(GridCacheReturn<Object> retVal,
            Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted,
            GridDhtAtomicUpdateFuture<K, V> dhtFut) {
            this.retVal = retVal;
            this.deleted = deleted;
            this.dhtFut = dhtFut;
        }

        /**
         * @return Return value.
         */
        private GridCacheReturn<Object> returnValue() {
            return retVal;
        }

        /**
         * @return Deleted entries.
         */
        private Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted() {
            return deleted;
        }

        /**
         * @return DHT future.
         */
        public GridDhtAtomicUpdateFuture<K, V> dhtFuture() {
            return dhtFut;
        }
    }

    /**
     * Result of {@link GridDhtAtomicCache#updateWithBatch} execution.
     */
    private static class UpdateBatchResult<K, V> {
        /** */
        private Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted;

        /** */
        private GridDhtAtomicUpdateFuture<K, V> dhtFut;

        /** */
        private boolean readersOnly;

        /**
         * @param entry Entry.
         * @param updRes Entry update result.
         * @param entries All entries.
         */
        private void addDeleted(GridDhtCacheEntry<K, V> entry, GridCacheUpdateAtomicResult<K, V> updRes,
            Collection<GridDhtCacheEntry<K, V>> entries) {
            if (updRes.removeVersion() != null) {
                if (deleted == null)
                    deleted = new ArrayList<>(entries.size());

                deleted.add(F.t(entry, updRes.removeVersion()));
            }
        }

        /**
         * @return Deleted entries.
         */
        private Collection<GridBiTuple<GridDhtCacheEntry<K, V>, GridCacheVersion>> deleted() {
            return deleted;
        }

        /**
         * @return DHT future.
         */
        public GridDhtAtomicUpdateFuture<K, V> dhtFuture() {
            return dhtFut;
        }

        /**
         * @param dhtFut DHT future.
         */
        private void dhtFuture(@Nullable GridDhtAtomicUpdateFuture<K, V> dhtFut) {
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
    private static class FinishedLockFuture extends GridFinishedFutureEx<Boolean> implements GridDhtFuture<Boolean> {
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
        private final GridUuid timeoutId;

        /** End time. */
        private final long endTime;

        /**
         * @param nodeId Node ID to send message to.
         */
        private DeferredResponseBuffer(UUID nodeId) {
            this.nodeId = nodeId;

            timeoutId = GridUuid.fromUuid(nodeId);

            endTime = U.currentTimeMillis() + DEFERRED_UPDATE_RESPONSE_TIMEOUT;
        }

        /** {@inheritDoc} */
        @Override public GridUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (guard.compareAndSet(false, true)) {
                writeLock().lock();

                try {
                    finish();
                }
                finally {
                    writeLock().unlock();
                }
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
            GridDhtAtomicDeferredUpdateResponse<K, V> msg = new GridDhtAtomicDeferredUpdateResponse<>(respVers);

            try {
                ctx.gate().enter();

                try {
                    ctx.io().send(nodeId, msg);
                }
                finally {
                    ctx.gate().leave();
                }
            }
            catch (IllegalStateException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send deferred dht update response to remote node (grid is stopping) " +
                        "[nodeId=" + nodeId + ", msg=" + msg + ']');
            }
            catch (GridTopologyException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send deferred dht update response to remote node (did node leave grid?) " +
                        "[nodeId=" + nodeId + ", msg=" + msg + ']');
            }
            catch (GridException e) {
                U.error(log, "Failed to send deferred dht update response to remote node [nodeId="
                    + nodeId + ", msg=" + msg + ']', e);
            }

            pendingResponses.remove(nodeId, this);
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class DhtAtomicUpdateRequestConverter603 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;

                case 1:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    assert commState.readSize == -1 : commState.readSize;

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;

                case 1:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    assert commState.readSize == -1 : commState.readSize;

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;
            }

            return true;
        }
    }

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class DhtAtomicUpdateResponseConverter603 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    assert commState.readSize == -1 : commState.readSize;

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;

            }

            return true;
        }
    }

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class NearAtomicUpdateResponseConverter603 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;

                case 1:
                    if (!commState.putLong(0))
                        return false;

                    commState.idx++;

                case 2:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;

                case 3:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;

                case 4:
                    if (!commState.putCacheVersion(null))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    assert commState.readSize == -1 : commState.readSize;

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;

                case 1:
                    if (buf.remaining() < 8)
                        return false;

                    long nearTtl = commState.getLong();

                    assert nearTtl == 0 : nearTtl;

                    commState.idx++;

                case 2:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    assert commState.readSize == -1 : commState.readSize;

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;

                case 3:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    assert commState.readSize == -1 : commState.readSize;

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;

                case 4:
                    GridCacheVersion nearVer0 = commState.getCacheVersion();

                    if (nearVer0 == CACHE_VER_NOT_READ)
                        return false;

                    assert nearVer0 == null : nearVer0;

                    commState.idx++;
            }

            return true;
        }
    }

    /**
     * GridNearAtomicUpdateRequest converter for version 6.1.2
     */
    @SuppressWarnings("PublicInnerClass")
    public static class GridNearAtomicUpdateRequestConverter612 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putBoolean(false))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0: {
                    if (buf.remaining() < 1)
                        return false;

                    commState.getBoolean();

                    commState.idx++;
                }
            }

            return true;
        }
    }

    /**
     * GridDhtAtomicUpdateRequest converter for version 6.1.2
     */
    @SuppressWarnings("PublicInnerClass")
    public static class GridDhtAtomicUpdateRequestConverter612 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0: {
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;
                }

                case 1: {
                    if (!commState.putBoolean(false))
                        return false;

                    commState.idx++;
                }

                case 2: {
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;
                }
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    if (commState.readSize >= 0) {
                        for (int i = commState.readItems; i < commState.readSize; i++) {
                            byte[] _val = commState.getByteArray();

                            if (_val == BYTE_ARR_NOT_READ)
                                return false;

                            commState.readItems++;
                        }
                    }

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;

                case 1:
                    if (buf.remaining() < 1)
                        return false;

                    commState.getBoolean();

                    commState.idx++;

                case 2:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    if (commState.readSize >= 0) {
                        for (int i = commState.readItems; i < commState.readSize; i++) {
                            byte[] _val = commState.getByteArray();

                            if (_val == BYTE_ARR_NOT_READ)
                                return false;

                            commState.readItems++;
                        }
                    }

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;
            }

            return true;
        }
    }
}
