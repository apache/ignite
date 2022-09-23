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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;

/**
 * Bundled factory for cache updaters.
 */
public class DataStreamerCacheUpdaters {
    /** */
    private static final StreamReceiver INDIVIDUAL = new Individual();

    /** */
    private static final StreamReceiver BATCHED = new Batched();

    /** */
    private static final StreamReceiver BATCHED_SORTED = new BatchedSorted();

    /**
     * Updates cache using independent {@link IgniteCache#put(Object, Object)}and
     * {@link IgniteCache#remove(Object)} operations. Thus it is safe from deadlocks but performance
     * is not the best.
     *
     * @return Single updater.
     */
    public static <K, V> StreamReceiver<K, V> individual() {
        return INDIVIDUAL;
    }

    /**
     * Updates cache using batched methods {@link IgniteCache#putAll(Map)}and
     * {@link IgniteCache#removeAll()}. Can cause deadlocks if the same keys are getting
     * updated concurrently. Performance is generally better than in {@link #individual()}.
     *
     * @return Batched updater.
     */
    public static <K, V> StreamReceiver<K, V> batched() {
        return BATCHED;
    }

    /**
     * Updates cache using batched methods {@link IgniteCache#putAll(Map)} and
     * {@link IgniteCache#removeAll(Set)}. Keys are sorted in natural order and if all updates
     * use the same rule deadlock can not happen. Performance is generally better than in {@link #individual()}.
     *
     * @return Batched sorted updater.
     */
    public static <K extends Comparable<?>, V> StreamReceiver<K, V> batchedSorted() {
        return BATCHED_SORTED;
    }

    /**
     * Updates cache.
     *
     * @param cache Cache.
     * @param rmvCol Keys to remove.
     * @param putMap Entries to put.
     * @throws IgniteException If failed.
     */
    protected static <K, V> void updateAll(IgniteCache<K, V> cache, @Nullable Set<K> rmvCol,
        Map<K, V> putMap) {
        assert rmvCol != null || putMap != null;

        // Here we assume that there are no key duplicates, so the following calls are valid.
        if (rmvCol != null)
            cache.removeAll(rmvCol);

        if (putMap != null)
            cache.putAll(putMap);
    }

    /**
     * Simple cache updater implementation. Updates keys one by one thus is not dead lock prone.
     */
    private static class Individual<K, V> implements StreamReceiver<K, V>, InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) {
            assert cache != null;
            assert !F.isEmpty(entries);

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                if (val == null)
                    cache.remove(key);
                else
                    cache.put(key, val);
            }
        }
    }

    /**
     * Batched updater. Updates cache using batch operations thus is dead lock prone.
     */
    private static class Batched<K, V> implements StreamReceiver<K, V>, InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) {
            assert cache != null;
            assert !F.isEmpty(entries);

            Map<K, V> putAll = null;
            Set<K> rmvAll = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                if (val == null) {
                    if (rmvAll == null)
                        rmvAll = new HashSet<>();

                    rmvAll.add(key);
                }
                else {
                    if (putAll == null)
                        putAll = new HashMap<>();

                    putAll.put(key, val);
                }
            }

            updateAll(cache, rmvAll, putAll);
        }
    }

    /**
     * Batched updater. Updates cache using batch operations thus is dead lock prone.
     */
    private static class BatchedSorted<K, V> implements StreamReceiver<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) {
            assert cache != null;
            assert !F.isEmpty(entries);

            Map<K, V> putAll = null;
            Set<K> rmvAll = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key instanceof Comparable;

                V val = entry.getValue();

                if (val == null) {
                    if (rmvAll == null)
                        rmvAll = new TreeSet<>();

                    rmvAll.add(key);
                }
                else {
                    if (putAll == null)
                        putAll = new TreeMap<>();

                    putAll.put(key, val);
                }
            }

            updateAll(cache, rmvAll, putAll);
        }
    }

    /**
     * Isolated receiver which only loads entry initial value.
     */
    private static class IsolatedUpdater implements StreamReceiver<KeyCacheObject, CacheObject>, InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        @Override public void receive(IgniteCache<KeyCacheObject, CacheObject> cache,
            Collection<Map.Entry<KeyCacheObject, CacheObject>> entries) throws IgniteException {
            IgniteCacheProxy<KeyCacheObject, CacheObject> proxy = (IgniteCacheProxy<KeyCacheObject, CacheObject>)cache;

            GridCacheAdapter<KeyCacheObject, CacheObject> internalCache = proxy.context().cache();

            if (internalCache.isNear())
                internalCache = internalCache.context().near().dht();

            GridCacheContext<?, ?> cctx = internalCache.context();

            GridDhtTopologyFuture topFut = cctx.shared().exchange().lastFinishedFuture();

            AffinityTopologyVersion topVer = topFut.topologyVersion();

            GridCacheVersion ver = cctx.versions().isolatedStreamerVersion();

            long ttl = CU.TTL_ETERNAL;
            long expiryTime = CU.EXPIRE_TIME_ETERNAL;

            ExpiryPolicy plc = cctx.expiry();

            Collection<Integer> reservedParts = new HashSet<>();
            Collection<Integer> ignoredParts = new HashSet<>();

            try {
                for (Map.Entry<KeyCacheObject, CacheObject> e : entries) {
                    cctx.shared().database().checkpointReadLock();

                    try {
                        e.getKey().finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

                        int p = cctx.affinity().partition(e.getKey());

                        if (ignoredParts.contains(p))
                            continue;

                        if (!reservedParts.contains(p)) {
                            GridDhtLocalPartition part = cctx.topology().localPartition(p, topVer, true);

                            if (!part.reserve()) {
                                ignoredParts.add(p);

                                continue;
                            }
                            else {
                                // We must not allow to read from RENTING partitions.
                                if (part.state() == GridDhtPartitionState.RENTING) {
                                    part.release();

                                    ignoredParts.add(p);

                                    continue;
                                }

                                reservedParts.add(p);
                            }
                        }

                        GridCacheEntryEx entry = internalCache.entryEx(e.getKey(), topVer);

                        if (plc != null) {
                            ttl = CU.toTtl(plc.getExpiryForCreation());

                            if (ttl == CU.TTL_ZERO)
                                continue;
                            else if (ttl == CU.TTL_NOT_CHANGED)
                                ttl = 0;

                            expiryTime = CU.toExpireTime(ttl);
                        }

                        if (topFut != null) {
                            Throwable err = topFut.validateCache(cctx, false, false, entry.key(), null);

                            if (err != null)
                                throw new IgniteCheckedException(err);
                        }

                        boolean primary = cctx.affinity().primaryByKey(cctx.localNode(), entry.key(), topVer);

                        entry.initialValue(e.getValue(),
                            ver,
                            ttl,
                            expiryTime,
                            false,
                            topVer,
                            primary ? GridDrType.DR_LOAD : GridDrType.DR_PRELOAD,
                            false,
                            primary);

                        entry.touch();

                        CU.unwindEvicts(cctx);

                        entry.onUnlock();
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        ignoredParts.add(cctx.affinity().partition(e.getKey()));
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // No-op.
                    }
                    catch (IgniteCheckedException ex) {
                        IgniteLogger log = cache.unwrap(Ignite.class).log();

                        U.error(log, "Failed to set initial value for cache entry: " + e, ex);

                        throw new IgniteException("Failed to set initial value for cache entry.", ex);
                    }
                    finally {
                        cctx.shared().database().checkpointReadUnlock();
                    }
                }
            }
            finally {
                for (Integer part : reservedParts) {
                    GridDhtLocalPartition locPart = cctx.topology().localPartition(part, topVer, false);

                    assert locPart != null : "Evicted reserved partition: " + locPart;

                    locPart.release();
                }

                try {
                    if (!cctx.isNear() && cctx.shared().wal() != null)
                        cctx.shared().wal().flush(null, false);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write preloaded entries into write-ahead log.", e);

                    throw new IgniteException("Failed to write preloaded entries into write-ahead log.", e);
                }
            }
        }
    }

    /**
     * Marker interface for updaters which do not need to unwrap cache objects.
     */
    public static interface InternalUpdater {
        // No-op.
    }
}
