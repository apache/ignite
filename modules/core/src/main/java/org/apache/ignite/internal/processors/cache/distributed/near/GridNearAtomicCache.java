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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;

/**
 * Near cache for atomic cache.
 */
public class GridNearAtomicCache<K, V> extends GridNearCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridDhtCacheAdapter<K, V> dht;

    /** Remove queue. */
    private GridCircularBuffer<T2<KeyCacheObject, GridCacheVersion>> rmvQueue;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearAtomicCache() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridNearAtomicCache(GridCacheContext<K, V> ctx) {
        super(ctx);

        int size = CU.isSystemCache(ctx.name()) ? 100 :
            Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, 1_000_000);

        rmvQueue = new GridCircularBuffer<>(U.ceilPow2(size / 10));
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.io().addHandler(ctx.cacheId(), GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse>() {
            @Override public void apply(UUID nodeId, GridNearGetResponse res) {
                processGetResponse(nodeId, res);
            }
        });
    }

    /**
     * @param dht DHT cache.
     */
    public void dht(GridDhtAtomicCache<K, V> dht) {
        this.dht = dht;
    }

    /** {@inheritDoc} */
    @Override public GridDhtCacheAdapter<K, V> dht() {
        return dht;
    }

    /**
     * @param req Update request.
     * @param res Update response.
     */
    public void processNearAtomicUpdateResponse(
        GridNearAtomicAbstractUpdateRequest req,
        GridNearAtomicUpdateResponse res
    ) {
        if (F.size(res.failedKeys()) == req.size())
            return;

        /*
         * Choose value to be stored in near cache: first check key is not in failed and not in skipped list,
         * then check if value was generated on primary node, if not then use value sent in request.
         */

        Collection<KeyCacheObject> failed = res.failedKeys();
        List<Integer> nearValsIdxs = res.nearValuesIndexes();
        List<Integer> skipped = res.skippedIndexes();

        GridCacheVersion ver = req.updateVersion();

        if (ver == null)
            ver = res.nearVersion();

        assert ver != null : "Failed to find version [req=" + req + ", res=" + res + ']';

        int nearValIdx = 0;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        for (int i = 0; i < req.size(); i++) {
            if (F.contains(skipped, i))
                continue;

            KeyCacheObject key = req.key(i);

            if (F.contains(failed, key))
                continue;

            if (ctx.affinity().partitionBelongs(ctx.localNode(), ctx.affinity().partition(key), req.topologyVersion())) { // Reader became backup.
                GridCacheEntryEx entry = peekEx(key);

                if (entry != null && entry.markObsolete(ver))
                    removeEntry(entry);

                continue;
            }

            CacheObject val = null;

            if (F.contains(nearValsIdxs, i)) {
                val = res.nearValue(nearValIdx);

                nearValIdx++;
            }
            else {
                assert req.operation() != TRANSFORM;

                if (req.operation() != DELETE)
                    val = req.value(i);
            }

            long ttl = res.nearTtl(i);
            long expireTime = res.nearExpireTime(i);

            if (ttl != CU.TTL_NOT_CHANGED && expireTime == CU.EXPIRE_TIME_CALCULATE)
                expireTime = CU.toExpireTime(ttl);

            try {
                processNearAtomicUpdateResponse(ver,
                    key,
                    val,
                    null,
                    ttl,
                    expireTime,
                    req.keepBinary(),
                    req.nodeId(),
                    req.subjectId(),
                    taskName);
            }
            catch (IgniteCheckedException e) {
                res.addFailedKey(key, new IgniteCheckedException("Failed to update key in near cache: " + key, e));
            }
        }
    }

    /**
     * @param ver Version.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param nodeId Node ID.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @throws IgniteCheckedException If failed.
     */
    private void processNearAtomicUpdateResponse(
        GridCacheVersion ver,
        KeyCacheObject key,
        @Nullable CacheObject val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime,
        boolean keepBinary,
        UUID nodeId,
        UUID subjId,
        String taskName
    ) throws IgniteCheckedException {
        try {
            while (true) {
                GridCacheEntryEx entry = null;

                AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

                try {
                    entry = entryEx(key, topVer);

                    GridCacheOperation op = (val != null || valBytes != null) ? UPDATE : DELETE;

                    GridCacheUpdateAtomicResult updRes = entry.innerUpdate(
                        ver,
                        nodeId,
                        nodeId,
                        op,
                        val,
                        null,
                        /*write-through*/false,
                        /*read-through*/false,
                        /*retval*/false,
                        keepBinary,
                        /*expiry policy*/null,
                        /*event*/true,
                        /*metrics*/true,
                        /*primary*/false,
                        /*check version*/true,
                        topVer,
                        CU.empty0(),
                        DR_NONE,
                        ttl,
                        expireTime,
                        null,
                        false,
                        false,
                        subjId,
                        taskName,
                        null,
                        null,
                        null);

                    if (updRes.removeVersion() != null)
                        ctx.onDeferredDelete(entry, updRes.removeVersion());

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while updating near cache value (will retry): " + key);

                    entry = null;
                }
                finally {
                    if (entry != null)
                        ctx.evicts().touch(entry, topVer);
                }
            }
        }
        catch (GridDhtInvalidPartitionException ignored) {
            // Ignore.
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Dht atomic update request.
     * @param res Dht atomic update response.
     */
    public void processDhtAtomicUpdateRequest(
        UUID nodeId,
        GridDhtAtomicAbstractUpdateRequest req,
        GridDhtAtomicUpdateResponse res
    ) {
        GridCacheVersion ver = req.writeVersion();

        assert ver != null;

        boolean intercept = req.forceTransformBackups() && ctx.config().getInterceptor() != null;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        for (int i = 0; i < req.nearSize(); i++) {
            KeyCacheObject key = req.nearKey(i);

            try {
                while (true) {
                    try {
                        GridCacheEntryEx entry = peekEx(key);

                        if (entry == null) {
                            res.addNearEvicted(key);

                            break;
                        }

                        if (req.hasKey(key)) { // Reader became backup.
                            if (entry.markObsolete(ver))
                                removeEntry(entry);

                            break;
                        }

                        CacheObject val = req.nearValue(i);
                        EntryProcessor<Object, Object, Object> entryProcessor = req.nearEntryProcessor(i);

                        GridCacheOperation op = entryProcessor != null ? TRANSFORM :
                            (val != null) ? UPDATE : DELETE;

                        long ttl = req.nearTtl(i);
                        long expireTime = req.nearExpireTime(i);

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
                            req.keepBinary(),
                            null,
                            /*event*/true,
                            /*metrics*/true,
                            /*primary*/false,
                            /*check version*/!req.forceTransformBackups(),
                            req.topologyVersion(),
                            CU.empty0(),
                            DR_NONE,
                            ttl,
                            expireTime,
                            null,
                            false,
                            intercept,
                            req.subjectId(),
                            taskName,
                            null,
                            null,
                            null);

                        if (updRes.removeVersion() != null)
                            ctx.onDeferredDelete(entry, updRes.removeVersion());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry while updating near value (will retry): " + key);
                    }
                }
            }
            catch (IgniteCheckedException e) {
                res.addFailedKey(key, new IgniteCheckedException("Failed to update near cache key: " + key, e));
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean skipVals,
        boolean canRemap,
        boolean needVer
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        return loadAsync(null,
            ctx.cacheKeysView(keys),
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            skipVals ? null : opCtx != null ? opCtx.expiry() : null,
            skipVals,
            opCtx != null && opCtx.skipStore(),
            canRemap,
            needVer);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val, @Nullable CacheEntryPredicate filter) throws IgniteCheckedException {
        return dht.getAndPut(key, val, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean put(K key, V val, CacheEntryPredicate filter) throws IgniteCheckedException {
        return dht.put(key, val, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndPutAsync0(K key, V val, @Nullable CacheEntryPredicate filter) {
        return dht.getAndPutAsync0(key, val, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> putAsync0(K key, V val, @Nullable CacheEntryPredicate filter) {
        return dht.putAsync0(key, val, filter);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V tryGetAndPut(K key, V val) throws IgniteCheckedException {
        return dht.tryGetAndPut(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m)
        throws IgniteCheckedException {
        dht.putAll(m);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(Map<? extends K, ? extends V> m) {
        return dht.putAllAsync(m);
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> drMap) throws IgniteCheckedException {
        dht.putAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException {
        return dht.putAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return dht.invoke(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return dht.invokeAll(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        return dht.invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws EntryProcessorException {
        return dht.invokeAsync(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        return dht.invokeAllAsync(map, args).get();
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return dht.invokeAllAsync(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, @Nullable CacheEntryPredicate filter) throws IgniteCheckedException {
        return dht.remove(key, filter);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) throws IgniteCheckedException {
        return dht.getAndRemove(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(K key) {
        return dht.getAndRemoveAsync(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Collection<? extends K> keys)
        throws IgniteCheckedException {
        dht.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(Collection<? extends K> keys) {
        return dht.removeAllAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws IgniteCheckedException {
        return dht.remove(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, @Nullable CacheEntryPredicate filter) {
        return dht.removeAsync(key, filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws IgniteCheckedException {
        dht.removeAll();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return dht.removeAllAsync();
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        dht.removeAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        return dht.removeAllConflictAsync(drMap);
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
        return dht.lockAllAsync(null, timeout);
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        dht.unlockAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert entry.isNear();

        try {
            T2<KeyCacheObject, GridCacheVersion> evicted = rmvQueue.add(new T2<>(entry.key(), ver));

            if (evicted != null)
                removeVersionedEntry(evicted.get1(), evicted.get2());
        }
        catch (InterruptedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to enqueue deleted entry [key=" + entry.key() + ", ver=" + ver + ']');

            Thread.currentThread().interrupt();
        }
    }
}
