/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

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
    private GridCacheQueries<K, V> qry;

    /** Flags. */
    @GridToStringInclude
    private Set<GridCacheFlag> flags;

    /** Client ID which operates over this projection, if any, */
    private UUID subjId;

    /** */
    private boolean keepPortable;

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
        GridCacheProjection<K, V> parent,
        GridCacheContext<K, V> cctx,
        @Nullable GridBiPredicate<K, V> kvFilter,
        @Nullable GridPredicate<? super GridCacheEntry<K, V>> entryFilter,
        @Nullable Set<GridCacheFlag> flags,
        @Nullable UUID subjId,
        boolean keepPortable) {
        assert parent != null;
        assert cctx != null;

        // Check if projection flags are conflicting with an ongoing transaction, if any.
        cctx.checkTxFlags(flags);

        this.cctx = cctx;

        this.flags = !F.isEmpty(flags) ? EnumSet.copyOf(flags) : EnumSet.noneOf(GridCacheFlag.class);

        Set<GridCacheFlag> f = this.flags;

        this.flags = Collections.unmodifiableSet(f);

        withNullKvFilter = new KeyValueFilter<>(kvFilter, false);

        noNullKvFilter = new KeyValueFilter<>(kvFilter, true);

        withNullEntryFilter = new FullFilter<>(withNullKvFilter, entryFilter);

        noNullEntryFilter = new FullFilter<>(noNullKvFilter, entryFilter);

        this.subjId = subjId;

        cache = cctx.cache();

        qry = new GridCacheQueriesImpl<>(cctx, this);

        this.keepPortable = keepPortable;
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
    GridPredicate<GridCacheEntry<K, V>> entryFilter(boolean noNulls) {
        return noNulls ? noNullEntryFilter : withNullEntryFilter;
    }

    /**
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return Key-value filter for the flag.
     */
    GridBiPredicate<K, V> kvFilter(boolean noNulls) {
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
    GridPredicate<GridCacheEntry<K, V>> and(
        GridPredicate<GridCacheEntry<K, V>> filter, boolean noNulls) {
        GridPredicate<GridCacheEntry<K, V>> entryFilter = entryFilter(noNulls);

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
    @SuppressWarnings({"unchecked"}) GridBiPredicate<K, V> and(final GridBiPredicate<K, V> filter, boolean noNulls) {
        final GridBiPredicate<K, V> kvFilter = kvFilter(noNulls);

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
    @SuppressWarnings({"unchecked"}) GridBiPredicate<K, V> and(final GridBiPredicate<K, V>[] filter, boolean noNulls) {
        final GridBiPredicate<K, V> kvFilter = kvFilter(noNulls);

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
    private GridPredicate<GridCacheEntry<K, V>> and(@Nullable final GridPredicate<GridCacheEntry<K, V>>[] f1,
        boolean nonNulls) {
        GridPredicate<GridCacheEntry<K, V>> entryFilter = entryFilter(nonNulls);

        if (F.isEmpty(f1))
            return entryFilter;

        return F0.and(entryFilter, f1);
    }

    /**
     * @param e Entry to verify.
     * @param noNulls Flag indicating whether filter should accept nulls or not.
     * @return {@code True} if filter passed.
     */
    boolean isAll(GridCacheEntry<K, V> e, boolean noNulls) {
        GridCacheFlag[] f = cctx.forceLocalRead();

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
        GridBiPredicate<K, V> p = kvFilter(noNulls);

        if (p != null) {
            GridCacheFlag[] f = cctx.forceLocalRead();

            try {
                if (!p.apply(k, v)) {
                    return false;
                }
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
        if (F.isEmpty(map)) {
            return Collections.<K, V>emptyMap();
        }

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

        if (!failed) {
            return map;
        }

        Map<K, V> cp = new HashMap<>(map.size(), 1.0f);

        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            K k = e.getKey();
            V v = e.getValue();

            if (isAll(k, v, noNulls)) {
                cp.put(k, v);
            }
        }

        return cp;
    }

    /**
     * Entry projection-filter-aware visitor.
     *
     * @param vis Visitor.
     * @return Projection-filter-aware visitor.
     */
    private GridInClosure<GridCacheEntry<K, V>> visitor(final GridInClosure<GridCacheEntry<K, V>> vis) {
        return new CI1<GridCacheEntry<K, V>>() {
            @Override public void apply(GridCacheEntry<K, V> e) {
                if (isAll(e, true)) {
                    vis.apply(e);
                }
            }
        };
    }

    /**
     * Entry projection-filter-aware visitor.
     *
     * @param vis Visitor.
     * @return Projection-filter-aware visitor.
     */
    private GridPredicate<GridCacheEntry<K, V>> visitor(final GridPredicate<GridCacheEntry<K, V>> vis) {
        return new P1<GridCacheEntry<K, V>>() {
            @Override public boolean apply(GridCacheEntry<K, V> e) {
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
    @Override public GridCacheQueries<K, V> queries() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> forSubjectId(UUID subjId) {
        A.notNull(subjId, "subjId");

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, cctx, noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter, flags, subjId, keepPortable);

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
    @Override public <K1, V1> GridCacheProjection<K1, V1> projection(
        Class<? super K1> keyType,
        Class<? super V1> valType
    ) {
        A.notNull(keyType, "keyType", valType, "valType");

        if (!keepPortable && (GridPortableObject.class.isAssignableFrom(keyType) ||
            GridPortableObject.class.isAssignableFrom(valType)))
            throw new IllegalStateException("Failed to create cache projection for portable objects. " +
                "Use keepPortable() method instead.");

        if (keepPortable && (!isPortableType(keyType) || !isPortableType(valType)))
            throw new IllegalStateException("Failed to create typed cache projection. If keepPortable() was " +
                "called, projection can work only with portable classes (see GridPortables JavaDoc for details).");

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(keyType, valType);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>(
            (GridCacheProjection<K1, V1>)this,
            (GridCacheContext<K1, V1>)cctx,
            CU.<K1, V1>typeFilter(keyType, valType),
            (GridPredicate<GridCacheEntry>)noNullEntryFilter.entryFilter,
            flags,
            subjId,
            keepPortable);

        return new GridCacheProxyImpl((GridCacheContext<K1, V1>)cctx, prj, prj);
    }

    /**
     * @param cls Class.
     * @return Whether class is portable.
     */
    private boolean isPortableType(Class<?> cls) {
        return
            cls == Byte.class ||
            cls == Short.class ||
            cls == Integer.class ||
            cls == Long.class ||
            cls == Float.class ||
            cls == Double.class ||
            cls == Character.class ||
            cls == Boolean.class ||
            cls == String.class ||
            cls == UUID.class ||
            cls == Date.class ||
            cls == Timestamp.class ||
            cls == byte[].class ||
            cls == short[].class ||
            cls == int[].class ||
            cls == long[].class ||
            cls == float[].class ||
            cls == double[].class ||
            cls == char[].class ||
            cls == boolean[].class ||
            cls == String[].class ||
            cls == UUID[].class ||
            cls == Date[].class ||
            cls == Timestamp[].class ||
            GridPortableObject.class.isAssignableFrom(cls) ||
            Collection.class.isAssignableFrom(cls) ||
            Map.class.isAssignableFrom(cls) ||
            Map.Entry.class.isAssignableFrom(cls) ||
            cls.isEnum();
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection(GridBiPredicate<K, V> p) {
        if (p == null)
            return new GridCacheProxyImpl<>(cctx, this, this);

        GridBiPredicate<K, V> kvFilter = p;

        if (noNullKvFilter.kvFilter != null)
            kvFilter = and(p, true);

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(p);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, cctx, kvFilter,
            noNullEntryFilter.entryFilter, flags, subjId, keepPortable);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public GridCacheProjection<K, V> projection(GridPredicate <GridCacheEntry<K, V>> filter) {
        if (filter == null)
            return new GridCacheProxyImpl<>(cctx, this, this);

        if (noNullEntryFilter.entryFilter != null)
            filter = and(filter, true);

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(filter);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, cctx, noNullKvFilter.kvFilter,
            filter, flags, subjId, keepPortable);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }


    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> flagsOn(@Nullable GridCacheFlag[] flags) {
        if (F.isEmpty(flags))
            return new GridCacheProxyImpl<>(cctx, this, this);

        Set<GridCacheFlag> res = EnumSet.noneOf(GridCacheFlag.class);

        if (!F.isEmpty(this.flags)) {
            res.addAll(this.flags);
        }

        res.addAll(EnumSet.copyOf(F.asList(flags)));

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, cctx, noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter, res, subjId, keepPortable);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> flagsOff(@Nullable GridCacheFlag[] flags) {
        if (F.isEmpty(flags))
            return new GridCacheProxyImpl<>(cctx, this, this);

        Set<GridCacheFlag> res = EnumSet.noneOf(GridCacheFlag.class);

        if (!F.isEmpty(this.flags)) {
            res.addAll(this.flags);
        }

        res.removeAll(EnumSet.copyOf(F.asList(flags)));

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, cctx, noNullKvFilter.kvFilter,
            noNullEntryFilter.entryFilter, res, subjId, keepPortable);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> GridCacheProjection<K1, V1> keepPortable() {
        if (!cctx.portableEnabled())
            throw new IllegalStateException("GridCacheProjection.keepPortable() is called for " +
                "cache which doesn't work in portable mode. Consider enabling portable mode via " +
                "GridCacheConfiguration.setPortableEnabled property.");

        GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>(
            (GridCacheProjection<K1, V1>)this,
            (GridCacheContext<K1, V1>)cctx,
            (GridBiPredicate<K1, V1>)noNullKvFilter.kvFilter,
            (GridPredicate<GridCacheEntry>)noNullEntryFilter.entryFilter,
            flags,
            subjId,
            true);

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return keySet().size();
    }

    /** {@inheritDoc} */
    @Override public int globalSize() throws GridException {
        return cache.globalSize();
    }

    /** {@inheritDoc} */
    @Override public int globalPrimarySize() throws GridException {
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
    @Override public boolean containsValue(V val) {
        return cache.containsValue(val, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public void forEach(GridInClosure<GridCacheEntry<K, V>> vis) {
        cache.forEach(visitor(vis));
    }

    /** {@inheritDoc} */
    @Override public boolean forAll(GridPredicate<GridCacheEntry<K, V>> vis) {
        return cache.forAll(visitor(vis));
    }

    /** {@inheritDoc} */
    @Override public V reload(K key) throws GridException {
        return cache.reload(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> reloadAsync(K key) {
        return cache.reloadAsync(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public void reloadAll() throws GridException {
        cache.reloadAll(entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> reloadAllAsync() {
        return cache.reloadAllAsync(entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public void reloadAll(@Nullable Collection<? extends K> keys) throws GridException {
        cache.reloadAll(keys, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> reloadAllAsync(@Nullable Collection<? extends K> keys) {
        return cache.reloadAllAsync(keys, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws GridException {
        return cache.get(key, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V get(K key, @Nullable GridCacheEntryEx<K, V> entry, boolean deserializePortable,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return cache.get(key, entry, deserializePortable, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> getAsync(K key) {
        return cache.getAsync(key, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws GridException {
        return cache.getForcePrimary(key);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> getForcePrimaryAsync(K key) {
        return cache.getForcePrimaryAsync(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(List<K> keys) throws GridException {
        return cache.getAllOutTx(keys);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map<K, V>> getAllOutTxAsync(List<K> keys) {
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
    @Override public boolean isDrSystemCache() {
        return cache.isDrSystemCache();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws GridException {
        return cache.getAll(keys, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys) {
        return cache.getAllAsync(keys, deserializePortables(), entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridException {
        return putAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return cache.put(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> putAsync(K key, V val,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return putAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        A.notNull(key, "key", val, "val");

        // Check k-v predicate first.
        if (!isAll(key, val, true))
            return new GridFinishedFuture<>(cctx.kernalContext());

        return cache.putAsync(key, val, entry, ttl, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return cache.putx(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return putxAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void putAllDr(Map<? extends K, GridCacheDrInfo<V>> drMap) throws GridException {
        cache.putAllDr(drMap);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> putAllDrAsync(Map<? extends K, GridCacheDrInfo<V>> drMap)
        throws GridException {
        return cache.putAllDrAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public void transform(K key, GridClosure<V, V> transformer) throws GridException {
        A.notNull(key, "key", transformer, "valTransform");

        cache.transform(key, transformer);
    }

    /** {@inheritDoc} */
    @Override public <R> R transformAndCompute(K key, GridClosure<V, GridBiTuple<V, R>> transformer)
        throws GridException {
        A.notNull(key, "key", transformer, "transformer");

        return cache.transformAndCompute(key, transformer);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> putxAsync(K key, V val,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return putxAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry,
        long ttl, @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        A.notNull(key, "key", val, "val");

        // Check k-v predicate first.
        if (!isAll(key, val, true))
            return new GridFinishedFuture<>(cctx.kernalContext(), false);

        return cache.putxAsync(key, val, entry, ttl, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAsync(K key, GridClosure<V, V> transformer) {
        A.notNull(key, "key", transformer, "valTransform");

        return cache.transformAsync(key, transformer);
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) throws GridException {
        return putIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, cctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws GridException {
        return putxIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return putxAsync(key, val, cctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAsync(K key, GridClosure<V, V> transformer,
        @Nullable GridCacheEntryEx<K, V> entry, long ttl) {
        return cache.transformAsync(key, transformer, entry, ttl);
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) throws GridException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> replaceAsync(K key, V val) {
        return putAsync(key, val, cctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws GridException {
        return replacexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replacexAsync(K key, V val) {
        return putxAsync(key, val, cctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws GridException {
        return replaceAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        GridPredicate<GridCacheEntry<K, V>> fltr = and(F.<K, V>cacheContainsPeek(oldVal), false);

        return cache.putxAsync(key, newVal, fltr);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        putAllAsync(m, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void transformAll(@Nullable Map<? extends K, ? extends GridClosure<V, V>> m) throws GridException {
        if (F.isEmpty(m))
            return;

        cache.transformAll(m);
    }

    /** {@inheritDoc} */
    @Override public void transformAll(@Nullable Set<? extends K> keys, GridClosure<V, V> transformer)
        throws GridException {
        if (F.isEmpty(keys))
            return;

        cache.transformAll(keys, transformer);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        m = isAll(m, true);

        if (F.isEmpty(m))
            return new GridFinishedFuture<>(cctx.kernalContext());

        return cache.putAllAsync(m, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAllAsync(@Nullable Map<? extends K, ? extends GridClosure<V, V>> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<>(cctx.kernalContext());

        return cache.transformAllAsync(m);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAllAsync(@Nullable Set<? extends K> keys, GridClosure<V, V> transformer)
        throws GridException {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(cctx.kernalContext());

        return cache.transformAllAsync(keys, transformer);
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        return cache.keySet(entryFilter(true));
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
    @Override public Set<GridCacheEntry<K, V>> entrySet() {
        return cache.entrySet(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> entrySetx(GridPredicate<GridCacheEntry<K, V>>... filter) {
        return cache.entrySetx(F.and(filter, entryFilter(true)));
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> primaryEntrySetx(GridPredicate<GridCacheEntry<K, V>>... filter) {
        return cache.primaryEntrySetx(F.and(filter, entryFilter(true)));
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> entrySet(int part) {
        // TODO pass entry filter.
        return cache.entrySet(part);
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> primaryEntrySet() {
        return cache.primaryEntrySet(entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheFlag> flags() {
        GridCacheFlag[] forced = cctx.forcedFlags();

        if (F.isEmpty(forced))
            return flags;

        // We don't expect too many flags, so default size is fine.
        Set<GridCacheFlag> ret = new HashSet<>();

        ret.addAll(flags);
        ret.addAll(F.asList(forced));

        return Collections.unmodifiableSet(ret);
    }

    /** {@inheritDoc} */
    @Override public GridPredicate<GridCacheEntry<K, V>> predicate() {
        return withNullEntryFilter;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.name();
    }

    /** {@inheritDoc} */
    @Override public GridProjection gridProjection() {
        return cache.gridProjection();
    }

    /** {@inheritDoc} */
    @Override public V peek(K key) {
        return cache.peek(key, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable Collection<GridCachePeekMode> modes) throws GridException {
        V val = cache.peek(key, modes);

        return isAll(key, val, true) ? val : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntry<K, V> entry(K key) {
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
    @Override public void clearAll() {
        cache.clearAll();
    }

    /** {@inheritDoc} */
    @Override public void globalClearAll() throws GridException {
        cache.globalClearAll();
    }

    /** {@inheritDoc} */
    @Override public void globalClearAll(long timeout) throws GridException {
        cache.globalClearAll(timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean clear(K key) {
        return cache.clear0(key, entryFilter(true));
    }

    /** {@inheritDoc} */
    @Override public boolean compact(K key) throws GridException {
        return cache.compact(key, entryFilter(false));
    }

    /** {@inheritDoc} */
    @Override public void compactAll() throws GridException {
        cache.compactAll(keySet());
    }

    /** {@inheritDoc} */
    @Override public V remove(K key,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return removeAsync(key, filter).get();
    }

    /** {@inheritDoc} */
    @Override public V remove(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return removeAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> removeAsync(K key, GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return removeAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return cache.removeAsync(key, entry, and(filter, true));
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return removexAsync(key, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void removeAllDr(Map<? extends K, GridCacheVersion> drMap) throws GridException {
        cache.removeAllDr(drMap);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllDrAsync(Map<? extends K, GridCacheVersion> drMap) throws GridException {
        return cache.removeAllDrAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return removexAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removexAsync(K key,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return removexAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return cache.removexAsync(key, entry, and(filter, true));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        // Check k-v predicate first.
        if (!isAll(key, newVal, true))
            return new GridFinishedFuture<>(cctx.kernalContext(), new GridCacheReturn<V>(false));

        return cache.replacexAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws GridException {
        return replacexAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> removex(K key, V val) throws GridException {
        return removexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheReturn<V>> removexAsync(K key, V val) {
        return !isAll(key, val, true) ? new GridFinishedFuture<>(cctx.kernalContext(),
            new GridCacheReturn<V>(false)) : cache.removexAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws GridException {
        return removeAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removeAsync(K key, V val) {
        return !isAll(key, val, true) ? new GridFinishedFuture<>(cctx.kernalContext(), false) :
            cache.removeAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection<? extends K> keys,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        cache.removeAll(keys, and(filter, true));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllAsync(@Nullable Collection<? extends K> keys,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return cache.removeAllAsync(keys, and(filter, true));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable GridPredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        cache.removeAll(and(filter, true));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllAsync(@Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return cache.removeAllAsync(and(filter, true));
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return cache.lock(key, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> lockAsync(K key, long timeout,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return cache.lockAsync(key, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return cache.lockAll(keys, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return cache.lockAllAsync(keys, timeout, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key, GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        cache.unlock(key, and(filter, false));
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
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
    @Override public V promote(K key) throws GridException {
        return cache.promote(key);
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws GridException {
        cache.promoteAll(keys);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart() throws IllegalStateException {
        return cache.txStart();
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) {
        return cache.txStart(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
        long timeout, int txSize) {
        return cache.txStart(concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartAffinity(Object affinityKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, GridException {
        return cache.txStartAffinity(affinityKey, concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartPartition(int partId, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, GridException {
        return cache.txStartPartition(partId, concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx tx() {
        return cache.tx();
    }

    /** {@inheritDoc} */
    @Override public ConcurrentMap<K, V> toMap() {
        return new GridCacheMapAdapter<>(this);
    }

    /** {@inheritDoc} */
    @Override public Iterator<GridCacheEntry<K, V>> iterator() {
        return cache.entrySet(entryFilter(true)).iterator();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> drStateTransfer(Collection<Byte> dataCenterIds) {
        return cache.drStateTransfer(dataCenterIds);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDrStateTransferDescriptor> drListStateTransfers() {
        return cache.drListStateTransfers();
    }

    /** {@inheritDoc} */
    @Override public void drPause() {
        cache.drPause();
    }

    /** {@inheritDoc} */
    @Override public void drResume() {
        cache.drResume();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDrStatus drPauseState() {
        return cache.drPauseState();
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
    public static class FullFilter<K, V> implements GridPredicate<GridCacheEntry<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key filter. */
        private KeyValueFilter<K, V> kvFilter;

        /** Constant array to avoid recreation. */
        private GridPredicate<? super GridCacheEntry<K, V>> entryFilter;

        /**
         * @param kvFilter Key-value filter.
         * @param entryFilter Entry filter.
         */
        private FullFilter(KeyValueFilter<K, V> kvFilter, GridPredicate<? super GridCacheEntry<K, V>> entryFilter) {
            this.kvFilter = kvFilter;
            this.entryFilter = entryFilter;
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
        public GridPredicate<? super GridCacheEntry<K, V>> entryFilter() {
            return entryFilter;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridCacheEntry<K, V> e) {
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
    public static class KeyValueFilter<K, V> implements GridBiPredicate<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key filter. */
        private GridBiPredicate<K, V> kvFilter;

        /** No nulls flag. */
        private boolean noNulls;

        /**
         * @param kvFilter Key-value filter.
         * @param noNulls Filter without null-values.
         */
        private KeyValueFilter(GridBiPredicate<K, V> kvFilter, boolean noNulls) {
            this.kvFilter = kvFilter;
            this.noNulls = noNulls;
        }

        /**
         * @return Key-value filter.
         */
        public GridBiPredicate<K, V> filter() {
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
