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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

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

        int size = Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, 1_000_000);

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
        GridNearAtomicUpdateRequest req,
        GridNearAtomicUpdateResponse res
    ) {
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

        for (int i = 0; i < req.keys().size(); i++) {
            if (F.contains(skipped, i))
                continue;

            KeyCacheObject key = req.keys().get(i);

            if (F.contains(failed, key))
                continue;

            if (ctx.affinity().belongs(ctx.localNode(), key, req.topologyVersion())) { // Reader became backup.
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
                        /*retval*/false,
                        /**expiry policy*/null,
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
                        taskName);

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
        GridDhtAtomicUpdateRequest req,
        GridDhtAtomicUpdateResponse res
    ) {
        GridCacheVersion ver = req.writeVersion();

        assert ver != null;

        Collection<KeyCacheObject> backupKeys = req.keys();

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

                        if (F.contains(backupKeys, key)) { // Reader became backup.
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
                            /*retval*/false,
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
                            taskName);

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
        @Nullable GridCacheEntryEx entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        boolean skipVals
    ) {
        ctx.denyOnFlag(LOCAL);
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        subjId = ctx.subjectIdPerCall(subjId, prj);

        return loadAsync(null,
            ctx.cacheKeysView(keys),
            false,
            forcePrimary,
            subjId,
            taskName,
            deserializePortable,
            skipVals ? null : prj != null ? prj.expiry() : null,
            skipVals);
    }

    /** {@inheritDoc} */
    @Override public V put(
        K key,
        V val,
        @Nullable GridCacheEntryEx cached,
        long ttl,
        @Nullable CacheEntryPredicate[] filter
    ) throws IgniteCheckedException {
        return dht.put(key, val, cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        @Nullable GridCacheEntryEx cached,
        long ttl,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return dht.putx(key, val, cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return dht.putx(key, val, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> putAsync(K key,
        V val,
        @Nullable GridCacheEntryEx entry,
        long ttl,
        @Nullable CacheEntryPredicate... filter) {
        return dht.putAsync(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> putxAsync(K key,
        V val,
        @Nullable GridCacheEntryEx entry,
        long ttl,
        @Nullable CacheEntryPredicate... filter) {
        return dht.putxAsync(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) throws IgniteCheckedException {
        return dht.putIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putIfAbsentAsync(K key, V val) {
        return dht.putIfAbsentAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws IgniteCheckedException {
        return dht.putxIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return dht.putxIfAbsentAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) throws IgniteCheckedException {
        return dht.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> replaceAsync(K key, V val) {
        return dht.replaceAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws IgniteCheckedException {
        return dht.replacex(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replacexAsync(K key, V val) {
        return dht.replacexAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return dht.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return dht.replaceAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn removex(K key, V val) throws IgniteCheckedException {
        return dht.removex(key, val);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return dht.replacex(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn> removexAsync(K key, V val) {
        return dht.removexAsync(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn> replacexAsync(K key, V oldVal, V newVal) {
        return dht.replacexAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m, CacheEntryPredicate[] filter)
        throws IgniteCheckedException {
        dht.putAll(m, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable CacheEntryPredicate[] filter) {
        return dht.putAllAsync(m, filter);
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
    @Override public V remove(K key,
        @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return dht.remove(key, entry, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> removeAsync(K key,
        @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) {
        return dht.removeAsync(key, entry, filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Collection<? extends K> keys, CacheEntryPredicate... filter)
        throws IgniteCheckedException {
        dht.removeAll(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(Collection<? extends K> keys,
        CacheEntryPredicate[] filter) {
        return dht.removeAllAsync(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key,
        @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return dht.removex(key, entry, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> removexAsync(K key,
        @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) {
        return dht.removexAsync(key, entry, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        return dht.remove(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        return dht.removeAsync(key, val);
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
    @Override public void localRemoveAll() throws IgniteCheckedException {
        dht.localRemoveAll();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localRemoveAll(CacheEntryPredicate filter) {
        return dht.localRemoveAll(filter);
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
        long accessTtl,
        CacheEntryPredicate[] filter) {
        return dht.lockAllAsync(null, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        dht.unlockAll(keys, filter);
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
