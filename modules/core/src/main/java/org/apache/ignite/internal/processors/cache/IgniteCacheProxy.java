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
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Cache proxy.
 */
public class IgniteCacheProxy<K, V> extends IgniteAsyncSupportAdapter<IgniteCache<K, V>>
    implements IgniteCache<K, V>, Externalizable {
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

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteCacheProxy() {
        // No-op.
    }

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

        gate = ctx.gate();
    }

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return ctx.cache().metrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return ctx.cache().mxBean();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        CacheConfiguration cfg = ctx.config();

        if (!clazz.isAssignableFrom(cfg.getClass()))
            throw new IllegalArgumentException();

        return clazz.cast(cfg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Entry<K, V> randomEntry() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
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
    @Override public IgniteCache<K, V> withSkipStore() {
        return flagOn(CacheFlag.SKIP_STORE);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync())
                    curFut.set(ctx.cache().globalLoadCacheAsync(p, args));
                else
                    ctx.cache().globalLoadCache(p, args);
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync())
                    curFut.set(delegate.<K, V>cache().loadCacheAsync(p, 0, args));
                else
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
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.putIfAbsentAsync(key, val));

                    return null;
                }
                else
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
    @Override public Lock lock(K key) throws CacheException {
        return lockAll(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(final Collection<? extends K> keys) {
        return new CacheLockImpl<>(gate, delegate, prj, keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return byCurrThread ? delegate.isLockedByThread(key) : delegate.isLocked(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> localPartition(int part) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
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
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        // TODO IGNITE-1.
        if (peekModes.length != 0)
            throw new UnsupportedOperationException();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return ctx.cache().globalSize();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.getAsync(key));

                    return null;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.getAllAsync(keys));

                    return null;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.getAllAsync(keys));

                    return null;
                }
                else
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
    public Set<CacheEntry<K, V>> entrySetx(IgnitePredicate<CacheEntry<K, V>>... filter) {
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
    public void removeAll(IgnitePredicate<CacheEntry<K, V>>... filter) {
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
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            if (isAsync()) {
                curFut.set(delegate.containsKeyAsync(key));

                return false;
            }
            else
                return delegate.containsKey(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(
        Set<? extends K> keys,
        boolean replaceExisting,
        @Nullable final CompletionListener completionLsnr
    ) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            IgniteInternalFuture<?> fut = ctx.cache().loadAll(keys, replaceExisting);

            if (completionLsnr != null) {
                fut.listenAsync(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> fut) {
                        try {
                            fut.get();

                            completionLsnr.onCompletion();
                        }
                        catch (IgniteCheckedException e) {
                            completionLsnr.onException(cacheException(e));
                        }
                    }
                });
            }
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync())
                    curFut.set(delegate.putxAsync(key, val));
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.putAsync(key, val));

                    return null;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync())
                    curFut.set(delegate.putAllAsync(map));
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.putxIfAbsentAsync(key, val));

                    return false;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.removexAsync(key));

                    return false;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.removeAsync(key, oldVal));

                    return false;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.removeAsync(key));

                    return null;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.replaceAsync(key, oldVal, newVal));

                    return false;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.replacexAsync(key, val));

                    return false;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.replaceAsync(key, val));

                    return null;
                }
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync())
                    curFut.set(delegate.removeAllAsync(keys));
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync())
                    curFut.set(delegate.removeAllAsync(keys));
                else
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    IgniteInternalFuture<EntryProcessorResult<T>> fut = delegate.invokeAsync(key, entryProcessor, args);

                    IgniteInternalFuture<T> fut0 = fut.chain(new CX1<IgniteInternalFuture<EntryProcessorResult<T>>, T>() {
                        @Override public T applyx(IgniteInternalFuture<EntryProcessorResult<T>> fut)
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.invokeAllAsync(keys, entryProcessor, args));

                    return null;
                }
                else
                    return delegate.invokeAll(keys, entryProcessor, args);
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
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

            try {
                if (isAsync()) {
                    curFut.set(delegate.invokeAllAsync(map, args));

                    return null;
                }
                else
                    return delegate.invokeAll(map, args);
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
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public javax.cache.CacheManager getCacheManager() {
        // TODO IGNITE-45 (Support start/close/destroy cache correctly)
        CachingProvider provider = (CachingProvider)Caching.getCachingProvider(
            CachingProvider.class.getName(),
            CachingProvider.class.getClassLoader());

        if (provider == null)
            return null;

        return provider.findManager(this);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO IGNITE-45 (Support start/close/destroy cache correctly)
        getCacheManager().destroyCache(getName());
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        // TODO IGNITE-45 (Support start/close/destroy cache correctly)
        return getCacheManager() == null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.equals(IgniteCache.class))
            return (T)this;
        else if (clazz.equals(Ignite.class))
            return (T)ctx.grid();

        throw new IllegalArgumentException("Unsupported class: " + clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            ctx.continuousQueries().registerCacheEntryListener(lsnrCfg, true);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration lsnrCfg) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            ctx.continuousQueries().deregisterCacheEntryListener(lsnrCfg);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return ctx.cache().igniteIterator(this);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<Entry<K, V>> query(QueryPredicate<K, V> filter) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(QueryReducer<Entry<K, V>, R> rmtRdc, QueryPredicate<K, V> filter) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> queryFields(QuerySqlPredicate<K, V> filter) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> queryFields(QueryReducer<List<?>, R> rmtRdc, QuerySqlPredicate<K, V> filter) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<Entry<K, V>> localQuery(QueryPredicate<K, V> filter) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> localQueryFields(QuerySqlPredicate<K, V> filter) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<K, V> createAsyncInstance() {
        return new IgniteCacheProxy<>(ctx, delegate, prj, true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K1, V1> IgniteCache<K1, V1> keepPortable() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            GridCacheProjectionImpl<K1, V1> prj0 = new GridCacheProjectionImpl<>(
                (CacheProjection<K1, V1>)(prj != null ? prj : delegate),
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

    /**
     * @param flag Flag to turn on.
     * @return Cache with given flags enabled.
     */
    public IgniteCache<K, V> flagOn(CacheFlag flag) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            Set<CacheFlag> res;

            Set<CacheFlag> flags0 = prj != null ? prj.flags() : null;

            if (flags0 != null) {
                if (flags0.contains(flag))
                    return this;

                res = EnumSet.copyOf(flags0);
            }
            else
                res = EnumSet.noneOf(CacheFlag.class);

            res.add(flag);

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
        if (e instanceof CachePartialUpdateCheckedException)
            return new CachePartialUpdateException((CachePartialUpdateCheckedException)e);

        if (e.getCause() instanceof CacheException)
            return (CacheException)e.getCause();

        return new CacheException(e);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);

        out.writeObject(delegate);

        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();

        delegate = (GridCacheProjectionEx<K, V>)in.readObject();

        prj = (GridCacheProjectionImpl<K, V>)in.readObject();

        gate = ctx.gate();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxy.class, this);
    }
}
