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
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllRunnable;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheLocalConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.CacheGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Common logic for near caches.
 */
public abstract class GridNearCacheAdapter<K, V> extends GridDistributedCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final CachePeekMode[] NEAR_PEEK_MODE = {CachePeekMode.NEAR};

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridNearCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    protected GridNearCacheAdapter(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (map == null) {
            map = new GridCacheLocalConcurrentMap(
                ctx,
                entryFactory(),
                ctx.config().getNearConfiguration().getNearStartSize());
        }
    }

    /**
     * @return Entry factory.
     */
    private GridCacheMapEntryFactory entryFactory() {
        return new GridCacheMapEntryFactory() {
            @Override public GridCacheMapEntry create(
                GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key
            ) {
                return new GridNearCacheEntry(ctx, key);
            }
        };
    }

    /**
     * @return DHT cache.
     */
    public abstract GridDhtCacheAdapter<K, V> dht();

    /** {@inheritDoc} */
    @Override public void forceKeyCheck() {
        super.forceKeyCheck();

        dht().forceKeyCheck();
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        map = new GridCacheLocalConcurrentMap(
            ctx,
            entryFactory(),
            ctx.config().getNearConfiguration().getNearStartSize());
    }

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader preloader() {
        return dht().preloader();
    }


    /** {@inheritDoc} */
    @Override public GridCacheMapEntry entryEx(KeyCacheObject key, AffinityTopologyVersion topVer) {
        GridNearCacheEntry entry = null;

        while (true) {
            try {
                entry = (GridNearCacheEntry)super.entryEx(key, topVer);

                entry.initializeFromDht(topVer);

                return entry;
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Got removed near entry while initializing from DHT entry (will retry): " + entry);
            }
        }
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return Entry.
     */
    public GridNearCacheEntry entryExx(KeyCacheObject key, AffinityTopologyVersion topVer) {
        return (GridNearCacheEntry)entryEx(key, topVer);
    }

    /**
     * @param key Key.
     * @return Entry.
     */
    @Nullable public GridNearCacheEntry peekExx(KeyCacheObject key) {
        return (GridNearCacheEntry)peekEx(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        return super.isLocked(key) || dht().isLocked(key);
    }

    /**
     * @param key Key.
     * @return If near entry is locked.
     */
    public boolean isLockedNearOnly(K key) {
        return super.isLocked(key);
    }

    /**
     * @param keys Keys.
     * @return If near entries for given keys are locked.
     */
    public boolean isAllLockedNearOnly(Iterable<? extends K> keys) {
        A.notNull(keys, "keys");

        for (K key : keys)
            if (!isLockedNearOnly(key))
                return false;

        return true;
    }

    /**
     * @param tx Transaction.
     * @param keys Keys to load.
     * @param forcePrimary Force primary flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVal Skip value flag.
     * @param skipStore Skip store flag.
     * @param canRemap Can remap flag.
     * @param needVer Need version.
     * @return Loaded values.
     */
    public IgniteInternalFuture<Map<K, V>> loadAsync(
        @Nullable IgniteInternalTx tx,
        @Nullable Collection<KeyCacheObject> keys,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable ExpiryPolicy expiryPlc,
        boolean skipVal,
        boolean skipStore,
        boolean canRemap,
        boolean needVer
    ) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        IgniteTxLocalEx txx = (tx != null && tx.local()) ? (IgniteTxLocalEx)tx : null;

        final IgniteCacheExpiryPolicy expiry = expiryPolicy(expiryPlc);

        GridNearGetFuture<K, V> fut = new GridNearGetFuture<>(ctx,
            keys,
            !skipStore,
            forcePrimary,
            txx,
            subjId,
            taskName,
            deserializeBinary,
            expiry,
            skipVal,
            canRemap,
            needVer,
            false,
            recovery);

        // init() will register future for responses if future has remote mappings.
        fut.init(null);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(IgniteBiPredicate<K, V> p, Object[] args) throws IgniteCheckedException {
        dht().localLoadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public void localLoad(Collection<? extends K> keys, ExpiryPolicy plc, boolean keepBinary) throws IgniteCheckedException {
        dht().localLoad(keys, plc, keepBinary);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(IgniteBiPredicate<K, V> p, Object[] args) {
        return dht().localLoadCacheAsync(p, args);
    }

    /**
     * @param nodeId Sender ID.
     * @param res Response.
     */
    protected void processGetResponse(UUID nodeId, GridNearGetResponse res) {
        CacheGetFuture fut = (CacheGetFuture)ctx.mvcc().future(res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return dht().size();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
        return nearEntries().size() + dht().size();
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return dht().primarySize();
    }

    /** {@inheritDoc} */
    @Override public long primarySizeLong() {
        return dht().primarySizeLong();
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return nearEntries().size();
    }

    /**
     * @return Near entries.
     */
    public Set<Cache.Entry<K, V>> nearEntries() {
        final AffinityTopologyVersion topVer = ctx.shared().exchange().readyAffinityVersion();

        return super.entrySet(new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx entry) {
                GridNearCacheEntry nearEntry = (GridNearCacheEntry)entry;

                return !nearEntry.deleted() && nearEntry.visitable(CU.empty0()) && nearEntry.valid(topVer);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(@Nullable final CacheEntryPredicate... filter) {
        CacheEntryPredicate p = new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx ex) {
                if (ex instanceof GridCacheMapEntry)
                    return ((GridCacheMapEntry)ex).visitable(filter);
                else
                    return !ex.deleted() && F.isAll(ex, filter);
            }
        };

        return new EntrySet(super.entrySet(p), dht().entrySet(p));
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        // Use unary 'and' to make sure that both sides execute.
        return super.evict(key) & dht().evict(key);
    }

    /** {@inheritDoc} */
    @Override public void evictAll(Collection<? extends K> keys) {
        super.evictAll(keys);

        dht().evictAll(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        return super.clearLocally(key) | dht().clearLocally(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set<? extends K> keys, boolean srv, boolean near, boolean readers) {
        super.clearLocallyAll(keys, srv, near, readers);

        dht().clearLocallyAll(keys, srv, near, readers);
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        return dht().offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        return dht().offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsDataCache() {
        return dht().isIgfsDataCache();
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceUsed() {
        return dht().igfsDataSpaceUsed();
    }

    /** {@inheritDoc} */
    @Override public void onIgfsDataSizeChanged(long delta) {
        dht().onIgfsDataSizeChanged(delta);
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoDataCache() {
        return dht().isMongoDataCache();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoMetaCache() {
        return dht().isMongoMetaCache();
    }

    /** {@inheritDoc} */
    @Override public List<GridCacheClearAllRunnable<K, V>> splitClearLocally(boolean srv, boolean near,
        boolean readers) {
        assert configuration().getNearConfiguration() != null;

        if (ctx.affinityNode()) {
            GridCacheVersion obsoleteVer = ctx.versions().next();

            List<GridCacheClearAllRunnable<K, V>> dhtJobs = dht().splitClearLocally(srv, near, readers);

            List<GridCacheClearAllRunnable<K, V>> res = new ArrayList<>(dhtJobs.size());

            for (GridCacheClearAllRunnable<K, V> dhtJob : dhtJobs)
                res.add(new GridNearCacheClearAllRunnable<>(this, obsoleteVer, dhtJob));

            return res;
        }
        else
            return super.splitClearLocally(srv, near, readers);
    }

    /**
     * Wrapper for entry set.
     */
    private class EntrySet extends AbstractSet<Cache.Entry<K, V>> {
        /** Near entry set. */
        private Set<Cache.Entry<K, V>> nearSet;

        /** Dht entry set. */
        private Set<Cache.Entry<K, V>> dhtSet;

        /**
         * @param nearSet Near entry set.
         * @param dhtSet Dht entry set.
         */
        private EntrySet(Set<Cache.Entry<K, V>> nearSet, Set<Cache.Entry<K, V>> dhtSet) {
            assert nearSet != null;
            assert dhtSet != null;

            this.nearSet = nearSet;
            this.dhtSet = dhtSet;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Cache.Entry<K, V>> iterator() {
            return new EntryIterator(nearSet.iterator(),
                F.iterator0(dhtSet, false, new P1<Cache.Entry<K, V>>() {
                    @Override public boolean apply(Cache.Entry<K, V> e) {
                        try {
                            return GridNearCacheAdapter.super.localPeek(e.getKey(), NEAR_PEEK_MODE, null) == null;
                        }
                        catch (IgniteCheckedException ex) {
                            throw new IgniteException(ex);
                        }
                    }
                }));
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return F.size(iterator());
        }
    }

    /**
     * Entry set iterator.
     */
    private class EntryIterator implements Iterator<Cache.Entry<K, V>> {
        /** */
        private Iterator<Cache.Entry<K, V>> dhtIter;

        /** */
        private Iterator<Cache.Entry<K, V>> nearIter;

        /** */
        private Iterator<Cache.Entry<K, V>> currIter;

        /** */
        private Cache.Entry<K, V> currEntry;

        /**
         * @param nearIter Near set iterator.
         * @param dhtIter Dht set iterator.
         */
        private EntryIterator(Iterator<Cache.Entry<K, V>> nearIter, Iterator<Cache.Entry<K, V>> dhtIter) {
            assert nearIter != null;
            assert dhtIter != null;

            this.nearIter = nearIter;
            this.dhtIter = dhtIter;

            currIter = nearIter;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nearIter.hasNext() || dhtIter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Cache.Entry<K, V> next() {
            if (!hasNext())
                throw new NoSuchElementException();

            if (!currIter.hasNext())
                currIter = dhtIter;

            return currEntry = currIter.next();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (currEntry == null)
                throw new IllegalStateException();

            assert currIter != null;

            currIter.remove();

            try {
                GridNearCacheAdapter.this.getAndRemove(currEntry.getKey());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearCacheAdapter.class, this);
    }
}
