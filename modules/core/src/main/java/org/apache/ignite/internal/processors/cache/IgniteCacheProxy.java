/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
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
public class IgniteCacheProxy<K, V> implements IgniteCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private GridCacheContext<K, V> ctx;

    /** Gateway. */
    private GridCacheGateway<K, V> gate;

    /** Cache. */
    @GridToStringInclude
    private GridCacheAdapter<K, V> delegate;

    /**
     * @param delegate Delegate.
     */
    public IgniteCacheProxy(GridCacheAdapter<K, V> delegate) {
        assert delegate != null;

        this.delegate = delegate;

        ctx = delegate.context();

        gate = ctx.gate();
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> getConfiguration() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Entry<K, V> randomEntry() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate p, @Nullable Object... args) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate p, @Nullable Object... args) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.putIfAbsent(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(IgnitePredicate filter) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Set<? extends K> keys) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(Object key) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(Object key) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(GridCachePeekMode... peekModes) throws CacheException {
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
        GridCacheProjectionImpl<K, V> prev = gate.enter(null);

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
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                delegate.promoteAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
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
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.get(key);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.getAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void loadAll(Set<? extends K> keys,
        boolean replaceExistingValues,
        CompletionListener completionLsnr) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                delegate.putx(key, val, null);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.put(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                delegate.putAll(map, null);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.putxIfAbsent(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.removex(key);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.remove(key, oldVal);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.remove(key, (GridCacheEntryEx<K, V>)null);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.replace(key, oldVal, newVal);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.replacex(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                return delegate.replace(key, val);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        try {
            GridCacheProjectionImpl<K, V> prev = gate.enter(null);

            try {
                delegate.removeAll(keys);
            }
            finally {
                gate.leave(prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, T> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.equals(IgniteCache.class))
            return (T)this;

        throw new IllegalArgumentException("Unsupported class: " + clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration cacheEntryLsnrConfiguration) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration cacheEntryLsnrConfiguration) {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
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
    @Override public IgniteCache<K, V> enableAsync() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(delegate);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        delegate = (GridCacheAdapter<K, V>)in.readObject();

        ctx = delegate.context();

        gate = ctx.gate();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxy.class, this);
    }
}
