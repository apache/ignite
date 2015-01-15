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
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Cache proxy.
 */
public class IgniteCacheProxy<K, V> extends IgniteAsyncSupportAdapter implements IgniteCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private GridCacheContext<K, V> ctx;

    /** Gateway. */
    private GridCacheGateway<K, V> gate;

    /** Delegate. */
    @GridToStringInclude
    private GridCacheProjectionEx<K, V> delegate;

    /** Projection. */
    private GridCacheProjectionImpl<K, V> prj;

    /** Query future storage */
    private final IgniteQueryFutureStorage queryStorage;

    /**
     * @param ctx Context.
     * @param delegate Delegate.
     * @param prj Projection.
     * @param async Async support flag.
     */
    public IgniteCacheProxy(GridCacheContext<K, V> ctx,
        GridCacheProjectionEx<K, V> delegate,
        @Nullable GridCacheProjectionImpl<K, V> prj,
        boolean async) {
        super(async);

        assert ctx != null;
        assert delegate != null;

        this.ctx = ctx;
        this.delegate = delegate;
        this.prj = prj;

        this.queryStorage = new IgniteQueryFutureStorage(ctx);

        gate = ctx.gate();
    }

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context() {
        onAccess();

        return ctx;
    }

    /**
     * @return Ignite instance.
     */
    @Override public GridEx ignite() {
        onAccess();

        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        onAccess();

        GridCacheConfiguration cfg = ctx.config();

        if (!clazz.isAssignableFrom(cfg.getClass()))
            throw new IllegalArgumentException();

        return clazz.cast(cfg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Entry<K, V> randomEntry() {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            GridCacheProjectionEx<K, V> prj0 = prj != null ? prj.withExpiryPolicy(plc) : delegate.withExpiryPolicy(plc);

            return new IgniteCacheProxy<>(ctx, prj0, (GridCacheProjectionImpl<K, V>)prj0, isAsync());
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                ClusterGroup nodes = ctx.kernalContext().grid().cluster().forCache(ctx.name());

                IgniteCompute comp = ctx.kernalContext().grid().compute(nodes).withNoFailover();

                comp.broadcast(new LoadCacheClosure<>(ctx.name(), p, args));
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                delegate.<K, V>cache().loadCache(p, 0, args);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIf(K key, V val, IgnitePredicate<GridCacheEntry<K, V>> filter) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.put(key, val, filter);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIf(K key, V val, IgnitePredicate<GridCacheEntry<K, V>> filter) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.putx(key, val, filter);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndRemoveIf(K key, IgnitePredicate<GridCacheEntry<K, V>> filter) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.remove(key, filter);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeIf(K key, IgnitePredicate<GridCacheEntry<K, V>> filter) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.removex(key, filter);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.putIfAbsent(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(IgnitePredicate filter) throws CacheException {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Set<? extends K> keys) throws CacheException {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isLocked(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isLockedByThread(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> localPartition(int part) throws CacheException {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.evictAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key, CachePeekMode... peekModes) {
        onAccess();

        // TODO IGNITE-1.
        if (peekModes.length != 0)
            throw new UnsupportedOperationException();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.peek(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                delegate.promoteAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean clear(Collection<? extends K> keys) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        onAccess();

        // TODO IGNITE-1.
        if (peekModes.length != 0)
            throw new UnsupportedOperationException();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.size();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        onAccess();

        // TODO IGNITE-1.
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.size();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.get(key);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.getAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /**
     * @param keys Keys.
     * @return Values map.
     */
    public Map<K, V> getAll(Collection<? extends K> keys) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.getAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /**
     * Gets entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Entry set.
     */
    public Set<GridCacheEntry<K, V>> entrySetx(IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.entrySetx(filter);
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     * @param filter Filter.
     */
    public void removeAll(IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.removeAll(filter);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.containsKey(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(Set<? extends K> keys,
        boolean replaceExistingValues,
        CompletionListener completionLsnr) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                delegate.putx(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.put(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                delegate.putAll(map);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.putxIfAbsent(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.removex(key);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.remove(key, oldVal);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.remove(key);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.replace(key, oldVal, newVal);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.replacex(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return delegate.replace(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                delegate.removeAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /**
     * @param keys Keys to remove.
     */
    public void removeAll(Collection<? extends K> keys) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                delegate.removeAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        onAccess();

        // TODO IGNITE-1.
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.removeAll();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        onAccess();

        // TODO IGNITE-1.
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.globalClearAll(0);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    IgniteFuture<EntryProcessorResult<T>> fut = delegate.invokeAsync(key, entryProcessor, args);

                    IgniteFuture<T> fut0 = fut.chain(new CX1<IgniteFuture<EntryProcessorResult<T>>, T>() {
                        @Override public T applyx(IgniteFuture<EntryProcessorResult<T>> fut)
                            throws IgniteCheckedException {
                            EntryProcessorResult<T> res = fut.get();

                            return res != null ? res.get() : null;
                        }
                    });

                    curFut.set(fut0);

                    return null;
                }
                else {
                    EntryProcessorResult<T> res = delegate.invoke(key, entryProcessor, args);

                    return res != null ? res.get() : null;
                }
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return saveOrGet(delegate.invokeAllAsync(keys, entryProcessor, args));
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        onAccess();

        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                return saveOrGet(delegate.invokeAllAsync(map, args));
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        onAccess();

        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        onAccess();

        // TODO IGNITE-45 (Support start/close/destroy cache correctly)
        IgniteCachingProvider provider = (IgniteCachingProvider)Caching.getCachingProvider(
            IgniteCachingProvider.class.getName(),
            IgniteCachingProvider.class.getClassLoader());

        if (provider == null)
            return null;

        return provider.findManager(this);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        onAccess();

        // TODO IGNITE-45 (Support start/close/destroy cache correctly)
        getCacheManager().destroyCache(getName());
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        onAccess();

        // TODO IGNITE-45 (Support start/close/destroy cache correctly)
        return getCacheManager() == null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> clazz) {
        onAccess();

        if (clazz.equals(IgniteCache.class))
            return (T)this;

        throw new IllegalArgumentException("Unsupported class: " + clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration cacheEntryLsnrConfiguration) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration cacheEntryLsnrConfiguration) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return new IgniteCacheIterator(delegate.queries().createScanQuery(null).execute(), queryStorage);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<Entry<K, V>> query(QueryPredicate<K, V> filter) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(QueryReducer<Entry<K, V>, R> rmtRdc, QueryPredicate<K, V> filter) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> queryFields(QuerySqlPredicate<K, V> filter) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> queryFields(QueryReducer<List<?>, R> rmtRdc, QuerySqlPredicate<K, V> filter) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<Entry<K, V>> localQuery(QueryPredicate<K, V> filter) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> localQueryFields(QuerySqlPredicate<K, V> filter) {
        onAccess();

        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> enableAsync() {
        onAccess();

        if (isAsync())
            return this;

        return new IgniteCacheProxy<>(ctx, delegate, prj, true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K1, V1> IgniteCache<K1, V1> keepPortable() {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            GridCacheProjectionImpl<K1, V1> prj0 = new GridCacheProjectionImpl<>(
                (GridCacheProjection<K1, V1>)(prj != null ? prj : delegate),
                (GridCacheContext<K1, V1>)ctx,
                null,
                null,
                prj != null ? prj.flags() : null,
                prj != null ? prj.subjectId() : null,
                true,
                prj != null ? prj.expiry() : null);

            return new IgniteCacheProxy<>((GridCacheContext<K1, V1>)ctx,
                prj0,
                prj0,
                isAsync());
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> flagsOn(@Nullable GridCacheFlag... flags) {
        onAccess();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            Set<GridCacheFlag> res = EnumSet.noneOf(GridCacheFlag.class);

            Set<GridCacheFlag> flags0 = prj !=null ? prj.flags() : null;

            if (flags0 != null && !flags0.isEmpty())
                res.addAll(flags0);

            res.addAll(EnumSet.copyOf(F.asList(flags)));

            GridCacheProjectionImpl<K, V> prj0 = new GridCacheProjectionImpl<>(
                (prj != null ? prj : delegate),
                ctx,
                null,
                null,
                res,
                prj != null ? prj.subjectId() : null,
                true,
                prj != null ? prj.expiry() : null);

            return new IgniteCacheProxy<>(ctx,
                prj0,
                prj0,
                isAsync());
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     * @param e Checked exception.
     * @return Cache exception.
     */
    private CacheException cacheException(IgniteCheckedException e) {
        if (e instanceof GridCachePartialUpdateException)
            return new CachePartialUpdateException((GridCachePartialUpdateException)e);

        return new CacheException(e);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        onAccess();

        out.writeObject(ctx);

        out.writeObject(delegate);

        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        onAccess();

        ctx = (GridCacheContext<K, V>)in.readObject();

        delegate = (GridCacheProjectionEx<K, V>)in.readObject();

        prj = (GridCacheProjectionImpl<K, V>)in.readObject();

        gate = ctx.gate();
    }

    /**
     * Checks if set was removed and handles iterators weak reference queue.
     */
    private void onAccess() {
        try {
            queryStorage.onAccess();
        } catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxy.class, this);
    }

    /**
     *
     */
    private static class LoadCacheClosure<K, V> implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String cacheName;

        /** */
        private IgniteBiPredicate<K, V> p;

        /** */
        private Object[] args;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Required by {@link Externalizable}.
         */
        public LoadCacheClosure() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param p Predicate.
         * @param args Arguments.
         */
        private LoadCacheClosure(String cacheName, IgniteBiPredicate<K, V> p, Object[] args) {
            this.cacheName = cacheName;
            this.p = p;
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            IgniteCache<K, V> cache = ignite.jcache(cacheName);

            assert cache != null : cacheName;

            cache.localLoadCache(p, args);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(p);

            out.writeObject(args);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            p = (IgniteBiPredicate<K, V>)in.readObject();

            args = (Object[])in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LoadCacheClosure.class, this);
        }
    }

    /**
     * Iterator over the cache.
     */
    private class IgniteCacheIterator implements Iterator<Cache.Entry<K, V>> {

        /** Iterator over the cache*/
        IgniteQueryFutureStorage.Iterator<Map.Entry<K, V>> iter;

        IgniteCacheIterator(GridCacheQueryFuture<Map.Entry<K, V>> fut, IgniteQueryFutureStorage storage) {
            iter = storage.iterator(fut);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            try {
                return iter.onHasNext();
            } catch (IgniteCheckedException e) {
                throw cacheException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Entry<K, V> next() {
            try {
                final Map.Entry<K, V> cur = iter.onNext();
                return new Cache.Entry<K, V>() {
                    @Override public K getKey() {
                        return cur.getKey();
                    }

                    @Override public V getValue() {
                        return cur.getValue();
                    }

                    @Override public <T> T unwrap(Class<T> clazz) {
                        throw new IllegalArgumentException();
                    }
                };
            }
            catch (IgniteCheckedException e) {
                throw cacheException(e);
            }


        }

        /** {@inheritDoc} */
        @Override public void remove() {
            Map.Entry<K, V> curEntry = iter.itemToRemove();
            try {
                delegate.removex(curEntry.getKey());
            }
            catch (IgniteCheckedException e) {
                throw cacheException(e);
            }
        }
    }
}
