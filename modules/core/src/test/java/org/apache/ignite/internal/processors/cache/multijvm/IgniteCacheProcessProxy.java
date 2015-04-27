/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.CacheManager;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class IgniteCacheProcessProxy<K, V> implements IgniteCache<K, V> {
    /** Compute. */
    private final transient IgniteCompute compute;

    /** Cache name. */
    private final String cacheName;

    /** Grid id. */
    private final UUID gridId;

    /**
     * @param name Name.
     * @param proxy Ignite Process Proxy.
     */
    public IgniteCacheProcessProxy(String name, IgniteExProcessProxy proxy) {
        cacheName = name;
        gridId = proxy.getId();

        ClusterGroup grp = proxy.localJvmGrid().cluster().forNodeId(proxy.getId());

        compute = proxy.localJvmGrid().compute(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Entry<K, V> randomEntry() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Collection<? extends K> keys) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public V localPeek(K key, CachePeekMode... peekModes) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        return 0; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        return 0; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override  public <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        return F.first(compute.broadcast(new IgniteClosureX<K, V>() {
            @Override public V applyx(K k) {
                return (V)Ignition.ignite(gridId).cache(cacheName).get(k);
            }
        }, key));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override  public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        compute.broadcast(new IgniteClosureX<List<?>, Object>() {
            @Override public Object applyx(List<?> l) {
                Ignition.ignite(gridId).cache(cacheName).put(l.get(0), l.get(1));

                return null;
            }
        }, Arrays.asList(key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return F.first(compute.broadcast(new IgniteClosureX<K, Boolean>() {
            @Override public Boolean applyx(K k) {
                return Ignition.ignite(gridId).cache(cacheName).remove(k);
            }
        }, key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void removeAll(final Set<? extends K> keys) {
        compute.broadcast(new IgniteClosureX<Set<?>, Void>() {
            @Override public Void applyx(Set<?> ks) {
                Ignition.ignite(gridId).cache(cacheName).removeAll(ks);

                return null;
            }
        }, keys);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override  public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override  public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override  public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebalance() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        return null; // TODO: CODE: implement.
    }
}
