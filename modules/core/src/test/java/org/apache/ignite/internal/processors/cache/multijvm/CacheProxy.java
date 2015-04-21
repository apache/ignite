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
public class CacheProxy<K, V> implements IgniteCache<K, V> {
    private transient IgniteExProxy proxy;
    private final String cacheName;
    private final UUID gridId;

    public CacheProxy(String name, IgniteExProxy proxy) {
        this.proxy = proxy;
        cacheName = name;
        gridId = proxy.getId();
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
    @Override
    public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
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
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        ClusterGroup grp = proxy.localJvmGrid().cluster().forNodeId(proxy.getId());

        IgniteCompute compute = proxy.localJvmGrid().compute(grp);

        return compute.broadcast(new MyClos2<K, V>(gridId, cacheName), key).iterator().next();
    }
    
    public static class MyClos2<K, V> extends IgniteClosureX<K, V> {
        private final UUID id;
        private final String name;

        public MyClos2(UUID id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override public V applyx(K k) {
            return (V)Ignition.ignite(id).cache(name).get(k);
        }
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
    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        // TODO: CODE: implement.
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
        return false; // TODO: CODE: implement.
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
        ClusterGroup grp = proxy.localJvmGrid().cluster().forNodeId(proxy.getId());

        IgniteCompute compute = proxy.localJvmGrid().compute(grp);
        
        compute.broadcast(new MyClos(proxy.getId(), proxy.name(), cacheName), keys);
    }
    
    public static class MyClos extends IgniteClosureX<Set<?>, String> {
        private UUID id;
        private final String gridName;
        private final String cacheName;

        public MyClos(UUID id, String gridName, String cacheName) {
            this.id = id;
            this.gridName = gridName;
            this.cacheName = cacheName;
        }

        @Override public String applyx(Set<?> ks) throws IgniteCheckedException {
            X.println(">>>>> Cache. Removing keys=" + ks);
            
            Ignite ignite = Ignition.ignite(id);

            X.println(">>>>> Cache. Ignite=" + ignite);

            IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            X.println(">>>>> Cache. Cache=" + cache);

            cache.removeAll(ks);

            return "";
        }
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
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
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
    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
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
