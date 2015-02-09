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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 * Cache projection.
 */
public class GridCacheProjectionImpl<K, V> implements GridCacheProjectionEx<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key-value filter taking null values. */
    @GridToStringExclude
    private KeyValueFilter<K, V> withNullKvFilter;

    /** Key-value filter not allowing null values. */
    @GridToStringExclude
    private KeyValueFilter<K, V> noNullKvFilter;

    /** Entry filter built with {@link #withNullKvFilter}. */
    @GridToStringExclude
    private FullFilter<K, V> withNullEntryFilter;

    /** Entry filter built with {@link #noNullKvFilter}. */
    @GridToStringExclude
    private FullFilter<K, V> noNullEntryFilter;

    /** Base cache. */
    private GridCacheAdapter<K, V> cache;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Queries impl. */
    private CacheQueries<K, V> qry;

    /** Flags. */
    @GridToStringInclude
    private Set<CacheFlag> flags;

    /** Client ID which operates over this projection, if any, */
    private UUID subjId;

    /** */
    private boolean keepPortable;

    /** */
    private ExpiryPolicy expiryPlc;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheProjectionImpl() {
        // No-op.
    }

    /**
     * @param parent Parent projection.
     * @param cctx Cache context.
     * @param kvFilter Key-value filter.
     * @param entryFilter Entry filter.
     * @param flags Flags for new projection
     */
    @SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
    public GridCacheProjectionImpl(
        CacheProjection<K, V> parent,
        GridCacheContext<K, V> cctx,
        @Nullable IgniteBiPredicate<K, V> kvFilter,
        @Nullable IgnitePredicate<? super CacheEntry<K, V>> entryFilter,
        @Nullable Set<CacheFlag> flags,
        @Nullable UUID subjId,
        boolean keepPortable,
        @Nullable ExpiryPolicy expiryPlc) {
        assert parent != null;
        assert cctx != null;

        // Check if projection flags are conflicting with an ongoing transaction, if any.
        cctx.shared().checkTxFlags(flags);

        this.cctx = cctx;

        this.flags = !F.isEmpty(flags) ? EnumSet.copyOf(flags) : EnumSet.noneOf(CacheFlag.class);

        Set<CacheFlag> f = this.flags;

        this.flags = Collections.unmodifiableSet(f);

        withNullKvFilter = new KeyValueFilter<>(kvFilter, false);

        noNullKvFilter = new KeyValueFilter<>(kvFilter, true);

        withNullEntryFilter = new FullFilter<>(withNullKvFilter, entryFilter);

        noNullEntryFilter = new FullFilter<>(noNullKvFilter, entryFilter);

        this.subjId = subjId;

        cache = cctx.cache();

        qry = new GridCacheQueriesImpl<>(cctx, this);

        this.keepPortable = keepPortable;

        this.expiryPlc = expiryPlc;
    }

    /**
     * Gets cache context.
     *
     * @return Cache context.
     */
    public GridCacheContext<K, V> context() {
        return cctx;
    }

    /**
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return Entry filter for the flag.
     */
    IgnitePredicate<CacheEntry<K, V>> entryFilter(boolean noNulls) {
        return noNulls ? noNullEntryFilter : withNullEntryFilter;
    }

    /**
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return Key-value filter for the flag.
     */
    IgniteBiPredicate<K, V> kvFilter(boolean noNulls) {
        return noNulls ? noNullKvFilter : withNullKvFilter;
    }

    /**
     * @return Keep portable flag.
     */
    public boolean isKeepPortable() {
        return keepPortable;
    }

    /**
     * @return {@code True} if portables should be deserialized.
     */
    public boolean deserializePortables() {
        return !keepPortable;
    }

    /**
     * {@code Ands} passed in filter with projection filter.
     *
     * @param filter filter to {@code and}.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code Anded} filter array.
     */
    IgnitePredicate<CacheEntry<K, V>> and(
        IgnitePredicate<CacheEntry<K, V>> filter, boolean noNulls) {
        IgnitePredicate<CacheEntry<K, V>> entryFilter = entryFilter(noNulls);

        if (filter == null)
            return entryFilter;

        return F0.and(entryFilter, filter);
    }

    /**
     * {@code Ands} passed in filter with projection filter.
     *
     * @param filter filter to {@code and}.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code Anded} filter array.
     */
    @SuppressWarnings({"unchecked"})
    IgniteBiPredicate<K, V> and(final IgniteBiPredicate<K, V> filter, boolean noNulls) {
        final IgniteBiPredicate<K, V> kvFilter = kvFilter(noNulls);

        if (filter == null)
            return kvFilter;

        return new P2<K, V>() {
            @Override public boolean apply(K k, V v) {
                return F.isAll2(k, v, kvFilter) && filter.apply(k, v);
            }
        };
    }

    /**
     * {@code Ands} passed in filter with projection filter.
     *
     * @param filter filter to {@code and}.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code Anded} filter array.
     */
    @SuppressWarnings({"unchecked"})
    IgniteBiPredicate<K, V> and(final IgniteBiPredicate<K, V>[] filter, boolean noNulls) {
        final IgniteBiPredicate<K, V> kvFilter = kvFilter(noNulls);

        if (filter == null)
            return kvFilter;

        return new P2<K, V>() {
            @Override public boolean apply(K k, V v) {
                return F.isAll2(k, v, kvFilter) && F.isAll2(k, v, filter);
            }
        };
    }

    /**
     * {@code Ands} two passed in filters.
     *
     * @param f1 First filter.
     * @param nonNulls Flag indicating whether nulls should be included.
     * @return {@code Anded} filter.
     */
    private IgnitePredicate<CacheEntry<K, V>> and(@Nullable final IgnitePredicate<CacheEntry<K, V>>[] f1,
        boolean nonNulls) {
        IgnitePredicate<CacheEntry<K, V>> entryFilter = entryFilter(nonNulls);

        if (F.isEmpty(f1))
            return entryFilter;

        return F0.and(entryFilter, f1);
    }

    /**
     * @param e Entry to verify.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code True} if filter passed.
     */
    boolean isAll(CacheEntry<K, V> e, boolean noNulls) {
        CacheFlag[] f = cctx.forceLocalRead();

        try {
            return F.isAll(e, entryFilter(noNulls));
        }
        finally {
            cctx.forceFlags(f);
        }
    }

    /**
     * @param k Key.
     * @param v Value.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code True} if filter passed.
     */
    boolean isAll(K k, V v, boolean noNulls) {
        IgniteBiPredicate<K, V> p = kvFilter(noNulls);

        if (p != null) {
            CacheFlag[] f = cctx.forceLocalRead();

            try {
                if (!p.apply(k, v))
                    return false;
            }
            finally {
                cctx.forceFlags(f);
            }
        }

        return true;
    }

    /**
     * @param map Map.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code True} if filter passed.
     */
    Map<? extends K, ? extends V> isAll(Map<? extends K, ? extends V> map, boolean noNulls) {
        if (F.isEmpty(map))
            return Collections.<K, V>emptyMap();

        boolean failed = false;

        // Optimize for passing.
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            K k = e.getKey();
            V v = e.getValue();

            if (!isAll(k, v, noNulls)) {
                failed = true;

                break;
            }
        }

        if (!failed)
            return map;

        Map<K, V> cp = new HashMap<>(map.size(), 1.0f);

        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            K k = e.getKey();
            V v = e.getValue();

            if (isAll(k, v, noNulls))
                cp.put(k, v);
        }

        return cp;
    }

    /**
     * Entry projection-filter-aware visitor.
     *
     * @param vis Visitor.
     * @return Projection-filter-aware visitor.
     */
    private IgniteInClosure<CacheEntry<K, V>> visitor(final IgniteInClosure<CacheEntry<K, V>> vis) {
        return new CI1<CacheEntry<K, V>>() {
            @Override public void apply(CacheEntry<K, V> e) {
                if (isAll(e, true))
                    vis.apply(e);
            }
        };
    }

    /**
     * Entry projection-filter-aware visitor.
     *
     * @param vis Visitor.
     * @return Projection-filter-aware visitor.
     */
    private IgnitePredicate<CacheEntry<K, V>> visitor(final IgnitePredicate<CacheEntry<K, V>> vis) {
        return new P1<CacheEntry<K, V>>() {
            @Override public boolean apply(CacheEntry<K, V> e) {
                // If projection filter didn't pass, go to the next element.
                // Otherwise, delegate to the visitor.
                return !isAll(e, true) || vis.apply(e);
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public <K1, V1> GridCache<K1, V1> cache() {
        return (GridCache<K1, V1>)cctx.cache();
    }

    /** {@inheritDoc} */
    @Override public CacheQueries<K, V> queries() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> forSubjectId(UUID subjId) {
        A.notNull(subjId, "subjId");

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter,
            flags,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /**
     * Gets client ID for which this projection was created.
     *
     * @return Client ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public <K1, V1> CacheProjection<K1, V1> projection(
        Class<? super K1> keyType,
        Class<? super V1> valType
    ) {
        A.notNull(keyType, "keyType", valType, "valType");

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(keyType, valType);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>(
            (CacheProjection<K1, V1>)this,
            (GridCacheContext<K1, V1>)cctx,
            CU.<K1, V1>typeFilter(keyType, valType),
            (IgnitePredicate<CacheEntry>)noNullEntryFilter.entryFilter,
            flags,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl((GridCacheContext<K1, V1>)cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public CacheProjection<K, V> projection(IgniteBiPredicate<K, V> p) {
        if (p == null)
            return new GridCacheProxyImpl<>(cctx, this, this);

        IgniteBiPredicate<K, V> kvFilter = p;

        if (noNullKvFilter.kvFilter != null)
            kvFilter = and(p, true);

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(p);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            kvFilter,
            noNullEntryFilter.entryFilter,
            flags,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public CacheProjection<K, V> projection(IgnitePredicate<CacheEntry<K, V>> filter) {
        if (filter == null)
            return new GridCacheProxyImpl<>(cctx, this, this);

        if (noNullEntryFilter.entryFilter != null)
            filter = and(filter, true);

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(filter);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            noNullKvFilter.kvFilter,
            filter,
            flags,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }


    /** {@inheritDoc} */
    @Override public CacheProjection<K, V> flagsOn(@Nullable CacheFlag[] flags) {
        if (F.isEmpty(flags))
            return new GridCacheProxyImpl<>(cctx, this, this);

        Set<CacheFlag> res = EnumSet.noneOf(CacheFlag.class);

        if (!F.isEmpty(this.flags))
            res.addAll(this.flags);

        res.addAll(EnumSet.copyOf(F.asList(flags)));

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter,
            res,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public CacheProjection<K, V> flagsOff(@Nullable CacheFlag[] flags) {
        if (F.isEmpty(flags))
            return new GridCacheProxyImpl<>(cctx, this, this);

        Set<CacheFlag> res = EnumSet.noneOf(CacheFlag.class);

        if (!F.isEmpty(this.flags))
            res.addAll(this.flags);

        res.removeAll(EnumSet.copyOf(F.asList(flags)));

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter,
            res,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> CacheProjection<K1, V1> keepPortable() {
        if (cctx.portableEnabled()) {
            GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>(
                (CacheProjection<K1, V1>)this,
                (GridCacheContext<K1, V1>)cctx,
                (IgniteBiPredicate<K1, V1>)noNullKvFilter.kvFilter,
                (IgnitePredicate<CacheEntry>)noNullEntryFilter.entryFilter,
                flags,
                subjId,
                true,
                expiryPlc);

            return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)cctx, prj, prj);
        }
        else
            return new GridCacheProxyImpl<>(
                (GridCacheContext<K1, V1>)cctx,
                (GridCacheProjectionEx<K1, V1>)this,
                (GridCacheProjectionImpl<K1, V1>)this);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return keySet().size();
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.localSize(peekModes);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.size(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes) {
        return cache.sizeAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public int globalSize() throws IgniteCheckedException {
        return cache.globalSize();
    }

    /** {@inheritDoc} */
    @Override public int globalPrimarySize() throws IgniteCheckedException {
        return cache.globalPrimarySize();
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return cctx.config().getCacheMode() == PARTITIONED && isNearEnabled(cctx) ?
             cctx.near().nearKeySet(entryFilter(true)).size() : 0;
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return primaryKeySet().size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cache.isEmpty() || size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return cache.containsKey(key, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(K key) {
        return cache.containsKeyAsync(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V val) {
        return cache.containsValue(val, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public void forEach(IgniteInClosure<CacheEntry<K, V>> vis) {
        cache.forEach(visitor(vis));
    }

    /** {@inheritDoc} */
    @Override public boolean forAll(IgnitePredicate<CacheEntry<K, V>> vis) {
        return cache.forAll(visitor(vis));
    }

    /** {@inheritDoc} */
    @Override public V reload(K key) throws IgniteCheckedException {
        return cache.reload(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> reloadAsync(K key) {
        return cache.reloadAsync(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public void reloadAll() throws IgniteCheckedException {
        cache.reloadAll(entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> reloadAllAsync() {
        return cache.reloadAllAsync(entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public void reloadAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        cache.reloadAll(keys, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> reloadAllAsync(@Nullable Collection<? extends K> keys) {
        return cache.reloadAllAsync(keys, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws IgniteCheckedException {
        return cache.get(key, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V get(K key, @Nullable GridCacheEntryEx<K, V> entry, boolean deserializePortable,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return cache.get(key, entry, deserializePortable, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(K key) {
        return cache.getAsync(key, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws IgniteCheckedException {
        return cache.getForcePrimary(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getForcePrimaryAsync(K key) {
        return cache.getForcePrimaryAsync(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(List<K> keys) throws IgniteCheckedException {
        return cache.getAllOutTx(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(List<K> keys) {
        return cache.getAllOutTxAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isGgfsDataCache() {
        return cache.isGgfsDataCache();
    }

    /** {@inheritDoc} */
    @Override public long ggfsDataSpaceUsed() {
        return cache.ggfsDataSpaceUsed();
    }

    /** {@inheritDoc} */
    @Override public long ggfsDataSpaceMax() {
        return cache.ggfsDataSpaceMax();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoDataCache() {
        return cache.isMongoDataCache();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoMetaCache() {
        return cache.isMongoMetaCache();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        return cache.getAll(keys, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys) {
        return cache.getAllAsync(keys, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter)
        throws IgniteCheckedException {
        return putAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return cache.put(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putAsync(K key, V val,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return putAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        A.notNull(key, "key", val, "val");

        // Check k-v predicate first.
        if (!isAll(key, val, true))
            return new GridFinishedFuture<>(cctx.kernalContext());

        return cache.putAsync(key, val, entry, ttl, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return cache.putx(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return putxAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void putAllDr(Map<? extends K, GridCacheDrInfo<V>> drMap) throws IgniteCheckedException {
        cache.putAllDr(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllDrAsync(Map<? extends K, GridCacheDrInfo<V>> drMap)
        throws IgniteCheckedException {
        return cache.putAllDrAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws IgniteCheckedException {
        return cache.invoke(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return cache.invokeAll(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return cache.invokeAsync(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return cache.invokeAllAsync(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        return cache.invokeAll(map, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        return cache.invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxAsync(K key, V val,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return putxAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry,
        long ttl, @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        A.notNull(key, "key", val, "val");

        // Check k-v predicate first.
        if (!isAll(key, val, true))
            return new GridFinishedFuture<>(cctx.kernalContext(), false);

        return cache.putxAsync(key, val, entry, ttl, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) throws IgniteCheckedException {
        return putIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, cctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws IgniteCheckedException {
        return putxIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return putxAsync(key, val, cctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) throws IgniteCheckedException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> replaceAsync(K key, V val) {
        return putAsync(key, val, cctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws IgniteCheckedException {
        return replacexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replacexAsync(K key, V val) {
        return putxAsync(key, val, cctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return replaceAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        IgnitePredicate<CacheEntry<K, V>> fltr = and(F.<K, V>cacheContainsPeek(oldVal), false);

        return cache.putxAsync(key, newVal, fltr);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        putAllAsync(m, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        m = isAll(m, true);

        if (F.isEmpty(m))
            return new GridFinishedFuture<>(cctx.kernalContext());

        return cache.putAllAsync(m, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        return cache.keySet(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet(@Nullable IgnitePredicate<CacheEntry<K, V>>... filter) {
        return cache.keySet(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<K> primaryKeySet() {
        return cache.primaryKeySet(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values() {
        return cache.values(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Collection<V> primaryValues() {
        return cache.primaryValues(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Set<CacheEntry<K, V>> entrySet() {
        return cache.entrySet(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Set<CacheEntry<K, V>> entrySetx(IgnitePredicate<CacheEntry<K, V>>... filter) {
        return cache.entrySetx(F.and(filter, entryFilter(true)));
    }

    /** {@inheritDoc} */
    @Override public Set<CacheEntry<K, V>> primaryEntrySetx(IgnitePredicate<CacheEntry<K, V>>... filter) {
        return cache.primaryEntrySetx(F.and(filter, entryFilter(true)));
    }

    /** {@inheritDoc} */
    @Override public Set<CacheEntry<K, V>> entrySet(int part) {
        // TODO pass entry filter.
        return cache.entrySet(part);
    }

    /** {@inheritDoc} */
    @Override public Set<CacheEntry<K, V>> primaryEntrySet() {
        return cache.primaryEntrySet(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Set<CacheFlag> flags() {
        CacheFlag[] forced = cctx.forcedFlags();

        if (F.isEmpty(forced))
            return flags;

        // We don't expect too many flags, so default size is fine.
        Set<CacheFlag> ret = new HashSet<>();

        ret.addAll(flags);
        ret.addAll(F.asList(forced));

        return Collections.unmodifiableSet(ret);
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<CacheEntry<K, V>> predicate() {
        return withNullEntryFilter.hasFilter() ? withNullEntryFilter : null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.name();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup gridProjection() {
        return cache.gridProjection();
    }

    /** {@inheritDoc} */
    @Override public V peek(K key) {
        return cache.peek(key, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.localPeek(key, peekModes);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.localEntries(peekModes);
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable Collection<GridCachePeekMode> modes) throws IgniteCheckedException {
        V val = cache.peek(key, modes);

        return isAll(key, val, true) ? val : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntry<K, V> entry(K key) {
        V val = peek(key);

        if (!isAll(key, val, false))
            return null;

        return cache.entry(key);
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        return cache.evict(key, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection<? extends K> keys) {
        cache.evictAll(keys, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public void evictAll() {
        cache.evictAll(keySet());
    }

    /** {@inheritDoc} */
    @Override public void clearLocally() {
        cache.clearLocally();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        return cache.clearAsync();
    }

    /** {@inheritDoc} */
    @Override public void clear(long timeout) throws IgniteCheckedException {
        cache.clear(timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        return cache.clearLocally0(key, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public boolean compact(K key) throws IgniteCheckedException {
        return cache.compact(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public void compactAll() throws IgniteCheckedException {
        cache.compactAll(keySet());
    }

    /** {@inheritDoc} */
    @Override public V remove(K key,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return removeAsync(key, filter).get();
    }

    /** {@inheritDoc} */
    @Override public V remove(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return removeAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> removeAsync(K key, IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return removeAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) {
        return cache.removeAsync(key, entry, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return removexAsync(key, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void removeAllDr(Map<? extends K, GridCacheVersion> drMap) throws IgniteCheckedException {
        cache.removeAllDr(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllDrAsync(Map<? extends K, GridCacheVersion> drMap) throws IgniteCheckedException {
        return cache.removeAllDrAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return removexAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removexAsync(K key,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return removexAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) {
        return cache.removexAsync(key, entry, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        // Check k-v predicate first.
        if (!isAll(key, newVal, true))
            return new GridFinishedFuture<>(cctx.kernalContext(), new GridCacheReturn<V>(false));

        return cache.replacexAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return replacexAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> removex(K key, V val) throws IgniteCheckedException {
        return removexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn<V>> removexAsync(K key, V val) {
        return !isAll(key, val, true) ? new GridFinishedFuture<>(cctx.kernalContext(),
            new GridCacheReturn<V>(false)) : cache.removexAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        return removeAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        return !isAll(key, val, true) ? new GridFinishedFuture<>(cctx.kernalContext(), false) :
            cache.removeAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        cache.removeAll(keys, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return cache.removeAllAsync(keys, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public void removeAll()
        throws IgniteCheckedException {
        removeAllAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return cache.removeAllAsync();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable IgnitePredicate<CacheEntry<K, V>>... filter) {
        return cache.removeAllAsync(and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return cache.lock(key, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return cache.lockAsync(key, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return cache.lockAll(keys, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return cache.lockAllAsync(keys, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key, IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        cache.unlock(key, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        cache.unlockAll(keys, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        return cache.isLocked(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        return cache.isLockedByThread(key);
    }

    /** {@inheritDoc} */
    @Override public V promote(K key) throws IgniteCheckedException {
        return cache.promote(key, deserializePortables());
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        cache.promoteAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStart() throws IllegalStateException {
        return cache.txStart();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalTx txStartEx(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) {
        return cache.txStartEx(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStart(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) {
        return cache.txStart(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStart(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation,
        long timeout, int txSize) {
        return cache.txStart(concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStartAffinity(Object affinityKey, IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException {
        return cache.txStartAffinity(affinityKey, concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStartPartition(int partId, IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException {
        return cache.txStartPartition(partId, concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx tx() {
        return cache.tx();
    }

    /** {@inheritDoc} */
    @Override public ConcurrentMap<K, V> toMap() {
        return new GridCacheMapAdapter<>(this);
    }

    /** {@inheritDoc} */
    @Override public Iterator<CacheEntry<K, V>> iterator() {
        return cache.entrySet(entryFilter(true)).iterator();
    }

    /** {@inheritDoc} */
    @Override public @Nullable ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        return new GridCacheProjectionImpl<>(
            this,
            cctx,
            noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter,
            flags,
            subjId,
            true,
            plc);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);

        out.writeObject(noNullEntryFilter);
        out.writeObject(withNullEntryFilter);

        out.writeObject(noNullKvFilter);
        out.writeObject(withNullKvFilter);

        U.writeCollection(out, flags);

        out.writeBoolean(keepPortable);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cctx = (GridCacheContext<K, V>)in.readObject();

        noNullEntryFilter = (FullFilter<K, V>)in.readObject();
        withNullEntryFilter = (FullFilter<K, V>)in.readObject();

        noNullKvFilter = (KeyValueFilter<K, V>)in.readObject();
        withNullKvFilter = (KeyValueFilter<K, V>)in.readObject();

        flags = U.readSet(in);

        cache = cctx.cache();

        qry = new GridCacheQueriesImpl<>(cctx, this);

        keepPortable = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheProjectionImpl.class, this);
    }

    /**
     * @param <K> Key type.
     * @param <V> Value type.
     */
    public static class FullFilter<K, V> implements IgnitePredicate<CacheEntry<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key filter. */
        private KeyValueFilter<K, V> kvFilter;

        /** Entry filter. */
        private IgnitePredicate<? super CacheEntry<K, V>> entryFilter;

        /**
         * @param kvFilter Key-value filter.
         * @param entryFilter Entry filter.
         */
        private FullFilter(KeyValueFilter<K, V> kvFilter, IgnitePredicate<? super CacheEntry<K, V>> entryFilter) {
            this.kvFilter = kvFilter;
            this.entryFilter = entryFilter;
        }

        /**
         * @return {@code True} if has non-null key value or entry filter.
         */
        boolean hasFilter() {
            return (kvFilter != null && kvFilter.filter() != null) || entryFilter != null;
        }

        /**
         * @return Key-value filter.
         */
        public KeyValueFilter<K, V> keyValueFilter() {
            return kvFilter;
        }

        /**
         * @return Entry filter.
         */
        public IgnitePredicate<? super CacheEntry<K, V>> entryFilter() {
            return entryFilter;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(CacheEntry<K, V> e) {
            if (kvFilter != null) {
                if (!kvFilter.apply(e.getKey(), e.peek()))
                    return false;
            }

            return F.isAll(e, entryFilter);
        }
    }

    /**
     * @param <K> Key type.
     * @param <V> Value type.
     */
    public static class KeyValueFilter<K, V> implements IgniteBiPredicate<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key filter. */
        private IgniteBiPredicate<K, V> kvFilter;

        /** No nulls flag. */
        private boolean noNulls;

        /**
         * @param kvFilter Key-value filter.
         * @param noNulls Filter without null-values.
         */
        private KeyValueFilter(IgniteBiPredicate<K, V> kvFilter, boolean noNulls) {
            this.kvFilter = kvFilter;
            this.noNulls = noNulls;
        }

        /**
         * @return Key-value filter.
         */
        public IgniteBiPredicate<K, V> filter() {
            return kvFilter;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(K k, V v) {
            if (k == null)  // Should never happen, but just in case.
                return false;

            if (v == null)
                return !noNulls;

            if (kvFilter != null) {
                if (!kvFilter.apply(k, v))
                    return false;
            }

            return true;
        }
    }
}
