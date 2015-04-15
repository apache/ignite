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
import org.apache.ignite.cache.CacheManager;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
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
public class IgniteCacheProxy<K, V> extends AsyncSupportAdapter<IgniteCache<K, V>>
    implements IgniteCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Delegate. */
    @GridToStringInclude
    private IgniteCacheProxyLockFree<K, V> lockFreeCache;

    /** Projection. */
    private GridCacheProjectionImpl<K, V> prj;

    /** Gateway. */
    private GridCacheGateway<K, V> gate;

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
    public IgniteCacheProxy(
        GridCacheContext<K, V> ctx,
        GridCacheProjectionEx<K, V> delegate,
        @Nullable GridCacheProjectionImpl<K, V> prj,
        boolean async
    ) {
        super(async);

        assert ctx != null;

        gate = ctx.gate();

        this.prj = prj;

        lockFreeCache = new IgniteCacheProxyLockFree<>(ctx, delegate, prj, async);
    }

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context() {
        return lockFreeCache.context();
    }

    /**
     * @return Lock free instance.
     */
    public IgniteCacheProxyLockFree<K, V> lockFree() {
        return lockFreeCache;
    }

    /**
     * @return Gateway.
     */
    public GridCacheGateway<K, V> gate() {
        return gate;
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.metrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.metrics(grp);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.mxBean();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return lockFreeCache.getConfiguration(clazz);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Entry<K, V> randomEntry() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.randomEntry();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.withExpiryPolicy(plc);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        return skipStore();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.loadCache(p, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.localLoadCache(p, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.getAndPutIfAbsent(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        return lockFreeCache.lock(key);
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(final Collection<? extends K> keys) {
        return lockFreeCache.lockAll(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        return lockFreeCache.isLocalLocked(key, byCurrThread);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.query(qry);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.localEntries(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.queryMetrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.localEvict(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key, CachePeekMode... peekModes) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.localPeek(key, peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.localPromote(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.size(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.localSize(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.get(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.getAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     * @param keys Keys.
     * @return Values map.
     */
    public Map<K, V> getAll(Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.getAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     * Gets entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Entry set.
     */
    public Set<Entry<K, V>> entrySetx(CacheEntryPredicate... filter) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.entrySetx(filter);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.containsKey(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.containsKeys(keys);
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
            lockFreeCache.loadAll(keys, replaceExisting, completionLsnr);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.put(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.getAndPut(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.putAll(map);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.putIfAbsent(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.remove(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.remove(key, oldVal);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.getAndRemove(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.replace(key, oldVal, newVal);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.replace(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.getAndReplace(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.removeAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.removeAll();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.clear(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.clearAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.clear();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.localClear(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.localClearAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.invoke(key, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.invoke(key, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
                                                                   EntryProcessor<K, V, T> entryProcessor,
                                                                   Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.invokeAll(keys, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.invokeAll(keys, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.invokeAll(map, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return lockFreeCache.getName();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        return lockFreeCache.getCacheManager();
    }

    /**
     * @param cacheMgr Cache manager.
     */
    public void setCacheManager(CacheManager cacheMgr) {
        lockFreeCache.setCacheManager(cacheMgr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!gate.enterIfNotClosed())
            return;

        IgniteInternalFuture<?> fut;

        try {
            fut = context().kernalContext().cache().dynamicStopCache(context().name());
        }
        finally {
            gate.leave();
        }

        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        if (!gate.enterIfNotClosed())
            return true;

        try {
            return context().kernalContext().cache().context().closed(context());
        }
        finally {
            gate.leave();
        }
    }

    /**
     *
     */
    public GridCacheProjectionEx delegate() {
        return lockFreeCache.delegate();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> clazz) {
        return lockFreeCache.unwrap(clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.registerCacheEntryListener(lsnrCfg);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            lockFreeCache.deregisterCacheEntryListener(lsnrCfg);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return lockFreeCache.iterator();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<K, V> createAsyncInstance() {
        return lockFreeCache.createAsyncInstance();
    }

    /**
     * Creates projection that will operate with portable objects. <p> Projection returned by this method will force
     * cache not to deserialize portable objects, so keys and values will be returned from cache API methods without
     * changes. Therefore, signature of the projection can contain only following types: <ul> <li>{@code PortableObject}
     * for portable classes</li> <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     * <li>Arrays of primitives (byte[], int[], ...)</li> <li>{@link String} and array of {@link String}s</li>
     * <li>{@link UUID} and array of {@link UUID}s</li> <li>{@link Date} and array of {@link Date}s</li> <li>{@link
     * java.sql.Timestamp} and array of {@link java.sql.Timestamp}s</li> <li>Enums and array of enums</li> <li> Maps,
     * collections and array of objects (but objects inside them will still be converted if they are portable) </li>
     * </ul> <p> For example, if you use {@link Integer} as a key and {@code Value} class as a value (which will be
     * stored in portable format), you should acquire following projection to avoid deserialization:
     * <pre>
     * CacheProjection<Integer, GridPortableObject> prj = cache.keepPortable();
     *
     * // Value is not deserialized and returned in portable format.
     * GridPortableObject po = prj.get(1);
     * </pre>
     * <p> Note that this method makes sense only if cache is working in portable mode ({@code
     * CacheConfiguration#isPortableEnabled()} returns {@code true}. If not, this method is no-op and will return
     * current projection.
     *
     * @return Projection for portable objects.
     */
    public <K1, V1> IgniteCache<K1, V1> keepPortable() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            GridCacheProjectionImpl<K1, V1> prj0 = new GridCacheProjectionImpl<>(
                (CacheProjection<K1, V1>)(prj != null ? prj : lockFreeCache),
                (GridCacheContext<K1, V1>)context(),
                prj != null && prj.skipStore(),
                prj != null ? prj.subjectId() : null,
                true,
                prj != null ? prj.expiry() : null);

            return new IgniteCacheProxy<>((GridCacheContext<K1, V1>)context(),
                prj0,
                prj0,
                isAsync());
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     * @return Cache with skip store enabled.
     */
    public IgniteCache<K, V> skipStore() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            boolean skip = prj != null && prj.skipStore();

            if (skip)
                return this;

            GridCacheProjectionImpl<K, V> prj0 = new GridCacheProjectionImpl<>(
                (prj != null ? prj : lockFreeCache.delegate()),
                context(),
                true,
                prj != null ? prj.subjectId() : null,
                prj != null && prj.isKeepPortable(),
                prj != null ? prj.expiry() : null);

            return new IgniteCacheProxy<>(context(),
                prj0,
                prj0,
                isAsync());
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     * @return Legacy proxy.
     */
    @NotNull
    public GridCacheProxyImpl<K, V> legacyProxy() {
        return lockFreeCache.legacyProxy();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(lockFreeCache);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        lockFreeCache = (IgniteCacheProxyLockFree<K, V>)in.readObject();

        prj = lockFreeCache.projection();

        gate = lockFreeCache.context().gate();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebalance() {
        return lockFreeCache.rebalance();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxy.class, this);
    }
}
