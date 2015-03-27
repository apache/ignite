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
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.internal.processors.cache.GridCachePeekMode.*;

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
        super(ctx, ctx.config().getNearConfiguration().getNearStartSize());
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry create(
                GridCacheContext ctx,
                AffinityTopologyVersion topVer, 
                KeyCacheObject key,
                int hash,
                CacheObject val,
                GridCacheMapEntry next,
                long ttl,
                int hdrId
            ) {
                // Can't hold any locks here - this method is invoked when
                // holding write-lock on the whole cache map.
                return new GridNearCacheEntry(ctx, key, hash, val, next, ttl, hdrId);
            }
        });
    }

    /**
     * @return DHT cache.
     */
    public abstract GridDhtCacheAdapter<K, V> dht();

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader<K, V> preloader() {
        return dht().preloader();
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryEx entryEx(KeyCacheObject key, boolean touch) {
        GridNearCacheEntry entry = null;

        while (true) {
            try {
                entry = (GridNearCacheEntry)super.entryEx(key, touch);

                entry.initializeFromDht(ctx.affinity().affinityTopologyVersion());

                return entry;
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Got removed near entry while initializing from DHT entry (will retry): " + entry);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryEx entryEx(KeyCacheObject key, AffinityTopologyVersion topVer) {
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

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    @Override public IgniteInternalFuture<Object> readThroughAllAsync(
        Collection<KeyCacheObject> keys,
        boolean reload,
        boolean skipVals,
        IgniteInternalTx tx,
        @Nullable UUID subjId,
        String taskName,
        IgniteBiInClosure<KeyCacheObject, Object> vis
    ) {
        return (IgniteInternalFuture)loadAsync(tx,
            keys,
            reload,
            false,
            subjId,
            taskName,
            true,
            null,
            skipVals);
    }

    /** {@inheritDoc} */
    @Override public V reload(K key)
        throws IgniteCheckedException {
        V val;

        try {
            val = dht().reload(key);
        }
        catch (GridDhtInvalidPartitionException ignored) {
            return null;
        }

        V nearVal = super.reload(key);

        return val == null ? nearVal : val;
    }

    /**
     * @param tx Transaction.
     * @param keys Keys to load.
     * @param reload Reload flag.
     * @param forcePrimary Force primary flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     * @return Loaded values.
     */
    public IgniteInternalFuture<Map<K, V>> loadAsync(@Nullable IgniteInternalTx tx,
        @Nullable Collection<KeyCacheObject> keys,
        boolean reload,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable ExpiryPolicy expiryPlc,
        boolean skipVal
    ) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        IgniteTxLocalEx txx = (tx != null && tx.local()) ? (IgniteTxLocalEx)tx : null;

        final IgniteCacheExpiryPolicy expiry = expiryPolicy(expiryPlc);

        GridNearGetFuture<K, V> fut = new GridNearGetFuture<>(ctx,
            keys,
            true,
            reload,
            forcePrimary,
            txx,
            subjId,
            taskName,
            deserializePortable,
            expiry,
            skipVal);

        // init() will register future for responses if future has remote mappings.
        fut.init();

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(IgniteBiPredicate<K, V> p, Object[] args) throws IgniteCheckedException {
        dht().localLoadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public void localLoad(Collection<? extends K> keys, ExpiryPolicy plc) throws IgniteCheckedException {
        dht().localLoad(keys, plc);
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
        GridNearGetFuture<K, V> fut = (GridNearGetFuture<K, V>)ctx.mvcc().<Map<K, V>>future(
            res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return nearEntries().size() + dht().size();
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return dht().primarySize();
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return nearEntries().size();
    }

    /**
     * @return Near entries.
     */
    public Set<Cache.Entry<K, V>> nearEntries() {
        final AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        return super.entrySet(new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx entry) {
                GridNearCacheEntry nearEntry = (GridNearCacheEntry)entry;

                return nearEntry.valid(topVer);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(
        @Nullable CacheEntryPredicate... filter) {
        return new EntrySet(super.entrySet(filter), dht().entrySet(filter));
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(int part) {
        return dht().entrySet(part);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> primaryEntrySet(
        @Nullable final CacheEntryPredicate... filter) {
        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        Collection<Cache.Entry<K, V>> entries =
            F.flatCollections(
                F.viewReadOnly(
                    dht().topology().currentLocalPartitions(),
                    new C1<GridDhtLocalPartition, Collection<Cache.Entry<K, V>>>() {
                        @Override public Collection<Cache.Entry<K, V>> apply(GridDhtLocalPartition p) {
                            Collection<GridDhtCacheEntry> entries0 = p.entries();

                            if (!F.isEmpty(filter))
                                entries0 = F.view(entries0, new CacheEntryPredicateAdapter() {
                                    @Override public boolean apply(GridCacheEntryEx e) {
                                        return F.isAll(e, filter);
                                    }
                                });

                            return F.viewReadOnly(
                                entries0,
                                new C1<GridDhtCacheEntry, Cache.Entry<K, V>>() {
                                    @Override public Cache.Entry<K, V> apply(GridDhtCacheEntry e) {
                                        return e.wrapLazyValue();
                                    }
                                },
                                new P1<GridDhtCacheEntry>() {
                                    @Override public boolean apply(GridDhtCacheEntry e) {
                                        return !e.obsoleteOrDeleted();
                                    }
                                });
                        }
                    },
                    new P1<GridDhtLocalPartition>() {
                        @Override public boolean apply(GridDhtLocalPartition p) {
                            return p.primary(topVer);
                        }
                    }));

        return new GridCacheEntrySet<>(ctx, entries, null);
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet(@Nullable CacheEntryPredicate[] filter) {
        return new GridCacheKeySet<>(ctx, entrySet(filter), null);
    }

    /**
     * @param filter Entry filter.
     * @return Keys for near cache only.
     */
    public Set<K> nearKeySet(@Nullable CacheEntryPredicate filter) {
        return super.keySet(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<K> primaryKeySet(@Nullable CacheEntryPredicate... filter) {
        return new GridCacheKeySet<>(ctx, primaryEntrySet(filter), null);
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values(CacheEntryPredicate... filter) {
        return new GridCacheValueCollection<>(ctx, entrySet(filter), ctx.vararg(F.<K, V>cacheHasPeekValue()));
    }

    /** {@inheritDoc} */
    @Override public Collection<V> primaryValues(@Nullable CacheEntryPredicate... filter) {
        return new GridCacheValueCollection<>(
            ctx,
            entrySet(filter),
            ctx.vararg(
                CU.<K, V>cachePrimary0(ctx.grid().<K>affinity(ctx.name()), ctx.localNode())));
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key, @Nullable CacheEntryPredicate[] filter) {
        // Use unary 'and' to make sure that both sides execute.
        return super.evict(key, filter) & dht().evict(key, filter);
    }

    /**
     * @param key Key to evict.
     * @param filter Optional filter.
     * @return {@code True} if evicted.
     */
    public boolean evictNearOnly(K key, @Nullable CacheEntryPredicate[] filter) {
        return super.evict(key, filter);
    }

    /** {@inheritDoc} */
    @Override public void evictAll(Collection<? extends K> keys,
        @Nullable CacheEntryPredicate[] filter) {
        super.evictAll(keys, filter);

        dht().evictAll(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public Cache.Entry<K, V> entry(K key) {
        // We don't try wrap entry from near or dht cache.
        // Created object will be wrapped once some method is called.
        return new CacheEntryImpl<>(key, peek(key));
    }

    /**
     * Peeks only near cache without looking into DHT cache.
     *
     * @param key Key.
     * @return Peeked value.
     */
    @Nullable public V peekNearOnly(K key) {
        try {
            GridTuple<V> peek = peek0(true, key, SMART, CU.empty0());

            return peek != null ? peek.get() : null;
        }
        catch (GridCacheFilterFailedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Filter validation failed for key: " + key);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable CacheEntryPredicate filter) {
        try {
            GridTuple<V> res = peek0(false, key, SMART, filter);

            if (res != null)
                return res.get();
        }
        catch (GridCacheFilterFailedException e) {
            e.printStackTrace();

            assert false : "Filter should not fail since fail-fast is false";
        }

        return dht().peek(key, filter);
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable Collection<GridCachePeekMode> modes) throws IgniteCheckedException {
        GridTuple<V> val = null;

        if (!modes.contains(PARTITIONED_ONLY)) {
            try {
                val = peek0(true, key, modes, ctx.tm().txx());
            }
            catch (GridCacheFilterFailedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for key: " + key);

                return null;
            }
        }

        if (val != null)
            return val.get();

        return !modes.contains(NEAR_ONLY) ? dht().peek(key, modes) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally0(K key, @Nullable CacheEntryPredicate[] filter) {
        return super.clearLocally0(key, filter) | dht().clearLocally0(key, filter);
    }

    /** {@inheritDoc} */
    @Override public void clearLocally0(Collection<? extends K> keys,
        @Nullable CacheEntryPredicate[] filter) {
        super.clearLocally0(keys, filter);

        dht().clearLocally0(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public V promote(K key, boolean deserializePortable) throws IgniteCheckedException {
        ctx.denyOnFlags(F.asList(READ, SKIP_SWAP));

        // Unswap only from dht(). Near cache does not have swap storage.
        return dht().promote(key, deserializePortable);
    }

    /** {@inheritDoc} */
    @Override public V promote(K key) throws IgniteCheckedException {
        ctx.denyOnFlags(F.asList(READ, SKIP_SWAP));

        // Unswap only from dht(). Near cache does not have swap storage.
        return dht().promote(key);
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        ctx.denyOnFlags(F.asList(READ, SKIP_SWAP));

        // Unswap only from dht(). Near cache does not have swap storage.
        // In near-only cache this is a no-op.
        if (ctx.affinityNode())
            dht().promoteAll(keys);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Cache.Entry<K, V> randomEntry() {
        return ctx.affinityNode() && ctx.isNear() ? dht().randomEntry() : super.randomEntry();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> swapIterator() throws IgniteCheckedException {
        ctx.denyOnFlags(F.asList(SKIP_SWAP));

        return dht().swapIterator();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> offHeapIterator() throws IgniteCheckedException {
        return dht().offHeapIterator();
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
    @Override public long swapSize() throws IgniteCheckedException {
        return dht().swapSize();
    }

    /** {@inheritDoc} */
    @Override public long swapKeys() throws IgniteCheckedException {
        return dht().swapKeys();
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
    @Override public long igfsDataSpaceMax() {
        return dht().igfsDataSpaceMax();
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
    @Override public List<GridCacheClearAllRunnable<K, V>> splitClearLocally() {
        assert configuration().getNearConfiguration() != null;

        if (ctx.affinityNode()) {
            GridCacheVersion obsoleteVer = ctx.versions().next();

            List<GridCacheClearAllRunnable<K, V>> dhtJobs = dht().splitClearLocally();

            List<GridCacheClearAllRunnable<K, V>> res = new ArrayList<>(dhtJobs.size());

            for (GridCacheClearAllRunnable<K, V> dhtJob : dhtJobs)
                res.add(new GridNearCacheClearAllRunnable<>(this, obsoleteVer, dhtJob));

            return res;
        }
        else
            return super.splitClearLocally();
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
                GridNearCacheAdapter.this.remove(currEntry.getKey(), CU.empty0());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @return Near entries iterator.
     */
    public Iterator<Cache.Entry<K, V>> nearEntriesIterator() {
        return iterator(map.entries0().iterator(), !ctx.keepPortable());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearCacheAdapter.class, this);
    }
}
