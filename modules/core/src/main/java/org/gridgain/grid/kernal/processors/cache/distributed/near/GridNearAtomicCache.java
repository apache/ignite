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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.GridCacheFlag.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;

/**
 * Near cache for atomic cache.
 */
public class GridNearAtomicCache<K, V> extends GridNearCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridDhtCacheAdapter<K, V> dht;

    /** Remove queue. */
    private GridCircularBuffer<T2<K, GridCacheVersion>> rmvQueue;

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

        int size = Integer.getInteger(GG_ATOMIC_CACHE_DELETE_HISTORY_SIZE, 1_000_000);

        rmvQueue = new GridCircularBuffer<>(U.ceilPow2(size / 10));
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.io().addHandler(ctx.cacheId(), GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearGetResponse<K, V> res) {
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
        GridNearAtomicUpdateRequest<K, V> req,
        GridNearAtomicUpdateResponse<K, V> res
    ) {
        /*
         * Choose value to be stored in near cache: first check key is not in failed and not in skipped list,
         * then check if value was generated on primary node, if not then use value sent in request.
         */

        Collection<K> failed = res.failedKeys();
        List<Integer> nearValsIdxs = res.nearValuesIndexes();
        List<Integer> skipped = res.skippedIndexes();

        GridCacheVersion ver = req.updateVersion();

        if (ver == null)
            ver = res.nearVersion();

        assert ver != null;

        int nearValIdx = 0;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        for (int i = 0; i < req.keys().size(); i++) {
            if (F.contains(skipped, i))
                continue;

            K key = req.keys().get(i);

            if (F.contains(failed, key))
                continue;

            if (ctx.affinity().belongs(ctx.localNode(), key, req.topologyVersion())) { // Reader became backup.
                GridCacheEntryEx<K, V> entry = peekEx(key);

                if (entry != null && entry.markObsolete(ver))
                    removeEntry(entry);

                continue;
            }

            V val = null;
            byte[] valBytes = null;

            if (F.contains(nearValsIdxs, i)) {
                val = res.nearValue(nearValIdx);
                valBytes = res.nearValueBytes(nearValIdx);

                nearValIdx++;
            }
            else {
                assert req.operation() != TRANSFORM;

                if (req.operation() != DELETE) {
                    val = req.value(i);
                    valBytes = req.valueBytes(i);
                }
            }

            long ttl = res.nearTtl(i);
            long expireTime = res.nearExpireTime(i);

            if (ttl != -1L && expireTime == -1L)
                expireTime = GridCacheMapEntry.toExpireTime(ttl);

            try {
                processNearAtomicUpdateResponse(ver,
                    key,
                    val,
                    valBytes,
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
        K key,
        @Nullable V val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime,
        UUID nodeId,
        UUID subjId,
        String taskName
    ) throws IgniteCheckedException {
        try {
            while (true) {
                GridCacheEntryEx<K, V> entry = null;

                long topVer = ctx.affinity().affinityTopologyVersion();

                try {
                    entry = entryEx(key, topVer);

                    GridCacheOperation op = (val != null || valBytes != null) ? UPDATE : DELETE;

                    GridCacheUpdateAtomicResult<K, V> updRes = entry.innerUpdate(
                        ver,
                        nodeId,
                        nodeId,
                        op,
                        val,
                        valBytes,
                        null,
                        /*write-through*/false,
                        /*retval*/false,
                        /**expiry policy*/null,
                        /*event*/true,
                        /*metrics*/true,
                        /*primary*/false,
                        /*check version*/true,
                        CU.<K, V>empty(),
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
        GridDhtAtomicUpdateRequest<K, V> req,
        GridDhtAtomicUpdateResponse<K, V> res
    ) {
        GridCacheVersion ver = req.writeVersion();

        assert ver != null;

        Collection<K> backupKeys = req.keys();

        boolean intercept = req.forceTransformBackups() && ctx.config().getInterceptor() != null;

        String taskName = ctx.kernalContext().task().resolveTaskName(req.taskNameHash());

        for (int i = 0; i < req.nearSize(); i++) {
            K key = req.nearKey(i);

            try {
                while (true) {
                    try {
                        GridCacheEntryEx<K, V> entry = peekEx(key);

                        if (entry == null) {
                            res.addNearEvicted(key, req.nearKeyBytes(i));

                            break;
                        }

                        if (F.contains(backupKeys, key)) { // Reader became backup.
                            if (entry.markObsolete(ver))
                                removeEntry(entry);

                            break;
                        }

                        V val = req.nearValue(i);
                        byte[] valBytes = req.nearValueBytes(i);
                        EntryProcessor<K, V, ?> entryProcessor = req.nearEntryProcessor(i);

                        GridCacheOperation op = entryProcessor != null ? TRANSFORM :
                            (val != null || valBytes != null) ?
                                UPDATE :
                                DELETE;

                        long ttl = req.nearTtl(i);
                        long expireTime = req.nearExpireTime(i);

                        if (ttl != -1L && expireTime == -1L)
                            expireTime = GridCacheMapEntry.toExpireTime(ttl);

                        GridCacheUpdateAtomicResult<K, V> updRes = entry.innerUpdate(
                            ver,
                            nodeId,
                            nodeId,
                            op,
                            op == TRANSFORM ? entryProcessor : val,
                            valBytes,
                            op == TRANSFORM ? req.invokeArguments() : null,
                            /*write-through*/false,
                            /*retval*/false,
                            null,
                            /*event*/true,
                            /*metrics*/true,
                            /*primary*/false,
                            /*check version*/!req.forceTransformBackups(),
                            CU.<K, V>empty(),
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
    @Override protected IgniteFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter
    ) {
        ctx.denyOnFlag(LOCAL);
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());

        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        subjId = ctx.subjectIdPerCall(subjId, prj);

        return loadAsync(null,
            keys,
            false,
            forcePrimary,
            filter,
            subjId,
            taskName,
            deserializePortable,
            prj != null ? prj.expiry() : null);
    }

    /** {@inheritDoc} */
    @Override public V put(
        K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter
    ) throws IgniteCheckedException {
        return dht.put(key, val, cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return dht.putx(key, val, cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return dht.putx(key, val, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<V> putAsync(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return dht.putAsync(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<Boolean> putxAsync(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return dht.putxAsync(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) throws IgniteCheckedException {
        return dht.putIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> putIfAbsentAsync(K key, V val) {
        return dht.putIfAbsentAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws IgniteCheckedException {
        return dht.putxIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return dht.putxIfAbsentAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) throws IgniteCheckedException {
        return dht.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> replaceAsync(K key, V val) {
        return dht.replaceAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws IgniteCheckedException {
        return dht.replacex(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replacexAsync(K key, V val) {
        return dht.replacexAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return dht.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return dht.replaceAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> removex(K key, V val) throws IgniteCheckedException {
        return dht.removex(key, val);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return dht.replacex(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<GridCacheReturn<V>> removexAsync(K key, V val) {
        return dht.removexAsync(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal) {
        return dht.replacexAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m, IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws IgniteCheckedException {
        dht.putAll(m, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return dht.putAllAsync(m, filter);
    }

    /** {@inheritDoc} */
    @Override public void putAllDr(Map<? extends K, GridCacheDrInfo<V>> drMap) throws IgniteCheckedException {
        dht.putAllDr(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> putAllDrAsync(Map<? extends K, GridCacheDrInfo<V>> drMap) throws IgniteCheckedException {
        return dht.putAllDrAsync(drMap);
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
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        return dht.invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<EntryProcessorResult<T>> invokeAsync(K key,
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
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return dht.invokeAllAsync(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public V remove(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return dht.remove(key, entry, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<V> removeAsync(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return dht.removeAsync(key, entry, filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Collection<? extends K> keys, IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws IgniteCheckedException {
        dht.removeAll(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllAsync(Collection<? extends K> keys,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return dht.removeAllAsync(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return dht.removex(key, entry, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<Boolean> removexAsync(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return dht.removexAsync(key, entry, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        return dht.remove(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V val) {
        return dht.removeAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        dht.removeAll(keySet(filter));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllAsync(IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return dht.removeAllAsync(keySet(filter));
    }

    /** {@inheritDoc} */
    @Override public void removeAllDr(Map<? extends K, GridCacheVersion> drMap) throws IgniteCheckedException {
        dht.removeAllDr(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllDrAsync(Map<? extends K, GridCacheVersion> drMap) throws IgniteCheckedException {
        return dht.removeAllDrAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override protected IgniteFuture<Boolean> lockAllAsync(Collection<? extends K> keys,
        long timeout,
        @Nullable IgniteTxLocalEx<K, V> tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable IgniteTxIsolation isolation,
        long accessTtl,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return dht.lockAllAsync(keys, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        dht.unlockAll(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert entry.isNear();

        try {
            T2<K, GridCacheVersion> evicted = rmvQueue.add(new T2<>(entry.key(), ver));

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
