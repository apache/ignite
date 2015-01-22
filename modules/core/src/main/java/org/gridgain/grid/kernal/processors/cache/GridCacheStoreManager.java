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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.interop.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Store manager.
 */
public class GridCacheStoreManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** */
    private static final String SES_ATTR = "STORE_SES";

    /** */
    private static final String SES_FIELD_NAME = "ses";

    /** */
    private final CacheStore<K, Object> store;

    /** */
    private final CacheStore<?, ?> cfgStore;

    /** */
    private final CacheStoreBalancingWrapper<K, Object> singleThreadGate;

    /** */
    private final ThreadLocal<SessionData> sesHolder;

    /** */
    private final boolean locStore;

    /** */
    private final boolean writeThrough;

    /** */
    private boolean convertPortable;

    /**
     * @param ctx Kernal context.
     * @param sesHolders Session holders map to use the same session holder for different managers if they use
     *        the same store instance.
     * @param cfgStore Store provided in configuration.
     * @param cfg Cache configuration.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    public GridCacheStoreManager(GridKernalContext ctx,
        IdentityHashMap<CacheStore, ThreadLocal> sesHolders,
        @Nullable CacheStore<K, Object> cfgStore,
        CacheConfiguration cfg) throws IgniteCheckedException {
        this.cfgStore = cfgStore;

        store = cacheStoreWrapper(ctx, cfgStore, cfg);

        singleThreadGate = store == null ? null : new CacheStoreBalancingWrapper<>(store);

        ThreadLocal<SessionData> sesHolder0 = null;

        writeThrough = cfg.isWriteThrough();

        if (cfgStore != null) {
            try {
                if (!sesHolders.containsKey(cfgStore)) {
                    sesHolder0 = new ThreadLocal<>();

                    Field sesField = CacheStore.class.getDeclaredField(SES_FIELD_NAME);

                    sesField.setAccessible(true);

                    sesField.set(cfgStore, new ThreadLocalSession(sesHolder0));

                    sesHolders.put(cfgStore, sesHolder0);
                }
                else
                    sesHolder0 = sesHolders.get(cfgStore);
            }
            catch (IllegalAccessException | NoSuchFieldException e) {
                throw new IgniteCheckedException(e);
            }
        }

        sesHolder = sesHolder0;

        locStore = U.hasAnnotation(cfgStore, CacheLocalStore.class);

        assert sesHolder != null || cfgStore == null;
    }

    /**
     * @return {@code True} is write-through is enabled.
     */
    public boolean writeThrough() {
        return writeThrough;
    }

    /**
     * @return Unwrapped store provided in configuration.
     */
    public CacheStore<?, ?> configuredStore() {
        return cfgStore;
    }

    /**
     * Creates a wrapped cache store if write-behind cache is configured.
     *
     * @param ctx Kernal context.
     * @param cfgStore Store provided in configuration.
     * @param cfg Cache configuration.
     * @return Instance if {@link GridCacheWriteBehindStore} if write-behind store is configured,
     *         or user-defined cache store.
     */
    @SuppressWarnings({"unchecked"})
    private CacheStore cacheStoreWrapper(GridKernalContext ctx,
        @Nullable CacheStore cfgStore,
        CacheConfiguration cfg) {
        if (cfgStore == null || !cfg.isWriteBehindEnabled())
            return cfgStore;

        GridCacheWriteBehindStore store = new GridCacheWriteBehindStore(ctx.gridName(),
            cfg.getName(),
            ctx.log(GridCacheWriteBehindStore.class),
            cfgStore);

        store.setFlushSize(cfg.getWriteBehindFlushSize());
        store.setFlushThreadCount(cfg.getWriteBehindFlushThreadCount());
        store.setFlushFrequency(cfg.getWriteBehindFlushFrequency());
        store.setBatchSize(cfg.getWriteBehindBatchSize());

        return store;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (store instanceof LifecycleAware) {
            // Avoid second start() call on store in case when near cache is enabled.
            if (cctx.config().isWriteBehindEnabled()) {
                if (!cctx.isNear())
                    ((LifecycleAware)store).start();
            }
            else {
                if (cctx.isNear() || !CU.isNearEnabled(cctx))
                    ((LifecycleAware)store).start();
            }
        }

        if (!cctx.config().isKeepPortableInStore()) {
            if (cctx.config().isPortableEnabled()) {
                if (store instanceof GridInteropAware)
                    ((GridInteropAware)store).configure(true);
                else
                    convertPortable = true;
            }
            else
                U.warn(log, "GridCacheConfiguration.isKeepPortableInStore() configuration property will " +
                    "be ignored because portable mode is not enabled for cache: " + cctx.namex());
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (store instanceof LifecycleAware) {
            try {
                // Avoid second start() call on store in case when near cache is enabled.
                if (cctx.config().isWriteBehindEnabled()) {
                    if (!cctx.isNear())
                        ((LifecycleAware)store).stop();
                }
                else {
                    if (cctx.isNear() || !CU.isNearEnabled(cctx))
                        ((LifecycleAware)store).stop();
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log(), "Failed to stop cache store.", e);
            }
        }
    }

    /**
     * @return {@code true} If local store is configured.
     */
    public boolean isLocalStore() {
        return locStore;
    }

    /**
     * @return {@code true} If store configured.
     */
    public boolean configured() {
        return store != null;
    }

    /**
     * Loads data from persistent store.
     *
     * @param tx Cache transaction.
     * @param key Cache key.
     * @return Loaded value, possibly <tt>null</tt>.
     * @throws IgniteCheckedException If data loading failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable public V loadFromStore(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        return (V)loadFromStore(tx, key, true);
    }

    /**
     * Loads data from persistent store.
     *
     * @param tx Cache transaction.
     * @param key Cache key.
     * @param convert Convert flag.
     * @return Loaded value, possibly <tt>null</tt>.
     * @throws IgniteCheckedException If data loading failed.
     */
    @Nullable private Object loadFromStore(@Nullable IgniteTx tx,
        K key,
        boolean convert)
        throws IgniteCheckedException {
        if (store != null) {
            if (key instanceof GridCacheInternal)
                // Never load internal keys from store as they are never persisted.
                return null;

            if (convertPortable)
                key = (K)cctx.unwrapPortableIfNeeded(key, false);

            if (log.isDebugEnabled())
                log.debug("Loading value from store for key: " + key);

            Object val = null;

            initSession(tx);

            try {
                val = singleThreadGate.load(key);
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }
            catch (CacheLoaderException e) {
                throw new IgniteCheckedException(e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(new CacheLoaderException(e));
            }
            finally {
                sesHolder.set(null);
            }

            if (log.isDebugEnabled())
                log.debug("Loaded value from store [key=" + key + ", val=" + val + ']');

            if (convert) {
                val = convert(val);

                return cctx.portableEnabled() ? cctx.marshalToPortable(val) : val;
            }
            else
                return val;
        }

        return null;
    }

    /**
     * @param val Internal value.
     * @return User value.
     */
    private V convert(Object val) {
        if (val == null)
            return null;

        return locStore ? ((IgniteBiTuple<V, GridCacheVersion>)val).get1() : (V)val;
    }

    /**
     * @return Whether DHT transaction can write to store from DHT.
     */
    public boolean writeToStoreFromDht() {
        return cctx.config().isWriteBehindEnabled() || locStore;
    }

    /**
     * @param tx Cache transaction.
     * @param keys Cache keys.
     * @param vis Closure to apply for loaded elements.
     * @throws IgniteCheckedException If data loading failed.
     */
    public void localStoreLoadAll(@Nullable IgniteTx tx,
        Collection<? extends K> keys,
        final GridInClosure3<K, V, GridCacheVersion> vis)
        throws IgniteCheckedException {
        assert store != null;
        assert locStore;

        loadAllFromStore(tx, keys, null, vis);
    }

    /**
     * Loads data from persistent store.
     *
     * @param tx Cache transaction.
     * @param keys Cache keys.
     * @param vis Closure.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If data loading failed.
     */
    @SuppressWarnings({"unchecked"})
    public boolean loadAllFromStore(@Nullable IgniteTx tx,
        Collection<? extends K> keys,
        final IgniteBiInClosure<K, V> vis) throws IgniteCheckedException {
        if (store != null) {
            loadAllFromStore(null, keys, vis, null);

            return true;
        }
        else {
            for (K key : keys)
                vis.apply(key, null);
        }

        return false;
    }

    /**
     * @param tx Cache transaction.
     * @param keys Keys to load.
     * @param vis Key/value closure (only one of vis or verVis can be specified).
     * @param verVis Key/value/version closure (only one of vis or verVis can be specified).
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void loadAllFromStore(@Nullable IgniteTx tx,
        Collection<? extends K> keys,
        final @Nullable IgniteBiInClosure<K, V> vis,
        final @Nullable GridInClosure3<K, V, GridCacheVersion> verVis)
        throws IgniteCheckedException {
        assert vis != null ^ verVis != null;
        assert verVis == null || locStore;

        final boolean convert = verVis == null;

        if (!keys.isEmpty()) {
            if (keys.size() == 1) {
                K key = F.first(keys);

                if (convert)
                    vis.apply(key, loadFromStore(tx, key));
                else {
                    IgniteBiTuple<V, GridCacheVersion> t =
                        (IgniteBiTuple<V, GridCacheVersion>)loadFromStore(tx, key, false);

                    if (t != null)
                        verVis.apply(key, t.get1(), t.get2());
                }

                return;
            }

            Collection<? extends K> keys0 = convertPortable ?
                F.viewReadOnly(keys, new C1<K, K>() {
                    @Override public K apply(K k) {
                        return (K)cctx.unwrapPortableIfNeeded(k, false);
                    }
                }) :
                keys;

            if (log.isDebugEnabled())
                log.debug("Loading values from store for keys: " + keys0);

            initSession(tx);

            try {
                CI2<K, Object> c = new CI2<K, Object>() {
                    @Override public void apply(K k, Object val) {
                        if (convert) {
                            V v = convert(val);

                            if (cctx.portableEnabled()) {
                                k = (K)cctx.marshalToPortable(k);
                                v = (V)cctx.marshalToPortable(v);
                            }

                            vis.apply(k, v);
                        }
                        else {
                            IgniteBiTuple<V, GridCacheVersion> v = (IgniteBiTuple<V, GridCacheVersion>)val;

                            if (v != null)
                                verVis.apply(k, v.get1(), v.get2());
                        }
                    }
                };

                if (keys.size() > singleThreadGate.loadAllThreshold()) {
                    Map<K, Object> map = store.loadAll(keys0);

                    if (map != null) {
                        for (Map.Entry<K, Object> e : map.entrySet())
                            c.apply(e.getKey(), e.getValue());
                    }
                }
                else
                    singleThreadGate.loadAll(keys0, c);
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }
            catch (CacheLoaderException e) {
                throw new IgniteCheckedException(e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(new CacheLoaderException(e));
            }
            finally {
                sesHolder.set(null);
            }

            if (log.isDebugEnabled())
                log.debug("Loaded values from store for keys: " + keys0);
        }
    }

    /**
     * Loads data from persistent store.
     *
     * @param vis Closer to cache loaded elements.
     * @param args User arguments.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If data loading failed.
     */
    @SuppressWarnings({"ErrorNotRethrown", "unchecked"})
    public boolean loadCache(final GridInClosure3<K, V, GridCacheVersion> vis, Object[] args)
        throws IgniteCheckedException {
        if (store != null) {
            if (log.isDebugEnabled())
                log.debug("Loading all values from store.");

            try {
                store.loadCache(new IgniteBiInClosure<K, Object>() {
                    @Override public void apply(K k, Object o) {
                        V v;
                        GridCacheVersion ver = null;

                        if (locStore) {
                            IgniteBiTuple<V, GridCacheVersion> t = (IgniteBiTuple<V, GridCacheVersion>)o;

                            v = t.get1();
                            ver = t.get2();
                        }
                        else
                            v = (V)o;

                        vis.apply(k, v, ver);
                    }
                }, args);
            }
            catch (CacheLoaderException e) {
                throw new IgniteCheckedException(e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(new CacheLoaderException(e));
            }

            if (log.isDebugEnabled())
                log.debug("Loaded all values from store.");

            return true;
        }

        LT.warn(log, null, "Calling GridCache.loadCache() method will have no effect, " +
            "GridCacheConfiguration.getStore() is not defined for cache: " + cctx.namexx());

        return false;
    }

    /**
     * Puts key-value pair into storage.
     *
     * @param tx Cache transaction.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @return {@code true} If there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean putToStore(@Nullable IgniteTx tx, K key, V val, GridCacheVersion ver)
        throws IgniteCheckedException {
        if (store != null) {
            // Never persist internal keys.
            if (key instanceof GridCacheInternal)
                return true;

            if (convertPortable) {
                key = (K)cctx.unwrapPortableIfNeeded(key, false);
                val = (V)cctx.unwrapPortableIfNeeded(val, false);
            }

            if (log.isDebugEnabled())
                log.debug("Storing value in cache store [key=" + key + ", val=" + val + ']');

            initSession(tx);

            try {
                store.write(new CacheEntryImpl<>(key, locStore ? F.t(val, ver) : val));
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }
            catch (CacheWriterException e) {
                throw new IgniteCheckedException(e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(new CacheWriterException(e));
            }
            finally {
                sesHolder.set(null);
            }

            if (log.isDebugEnabled())
                log.debug("Stored value in cache store [key=" + key + ", val=" + val + ']');

            return true;
        }

        return false;
    }

    /**
     * Puts key-value pair into storage.
     *
     * @param tx Cache transaction.
     * @param map Map.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean putAllToStore(@Nullable IgniteTx tx, Map<K, IgniteBiTuple<V, GridCacheVersion>> map)
        throws IgniteCheckedException {
        if (F.isEmpty(map))
            return true;

        if (map.size() == 1) {
            Map.Entry<K, IgniteBiTuple<V, GridCacheVersion>> e = map.entrySet().iterator().next();

            return putToStore(tx, e.getKey(), e.getValue().get1(), e.getValue().get2());
        }
        else {
            if (store != null) {
                EntriesView entries = new EntriesView(map);

                if (log.isDebugEnabled())
                    log.debug("Storing values in cache store [entries=" + entries + ']');

                initSession(tx);

                try {
                    store.writeAll(entries);
                }
                catch (ClassCastException e) {
                    handleClassCastException(e);
                }
                catch (Exception e) {
                    if (!entries.isEmpty()) {
                        List<Object> keys = new ArrayList<>(entries.size());

                        for (Cache.Entry<?, ?> entry : entries)
                            keys.add(entry.getKey());

                        throw new CacheStorePartialUpdateException(keys, e);
                    }

                    if (!(e instanceof CacheWriterException))
                        e = new CacheWriterException(e);

                    throw new IgniteCheckedException(e);
                }
                finally {
                    sesHolder.set(null);
                }

                if (log.isDebugEnabled())
                    log.debug("Stored value in cache store [entries=" + entries + ']');

                return true;
            }

            return false;
        }
    }

    /**
     * @param tx Cache transaction.
     * @param key Key.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean removeFromStore(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        if (store != null) {
            // Never remove internal key from store as it is never persisted.
            if (key instanceof GridCacheInternal)
                return false;

            if (convertPortable)
                key = (K)cctx.unwrapPortableIfNeeded(key, false);

            if (log.isDebugEnabled())
                log.debug("Removing value from cache store [key=" + key + ']');

            initSession(tx);

            try {
                store.delete(key);
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }
            catch (CacheWriterException e) {
                throw new IgniteCheckedException(e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(new CacheWriterException(e));
            }
            finally {
                sesHolder.set(null);
            }

            if (log.isDebugEnabled())
                log.debug("Removed value from cache store [key=" + key + ']');

            return true;
        }

        return false;
    }

    /**
     * @param tx Cache transaction.
     * @param keys Key.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    @SuppressWarnings("unchecked")
    public boolean removeAllFromStore(@Nullable IgniteTx tx, Collection<?> keys) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return true;

        if (keys.size() == 1) {
            Object key = keys.iterator().next();

            return removeFromStore(tx, (K)key);
        }

        if (store != null) {
            Collection<Object> keys0 = convertPortable ?
                cctx.unwrapPortablesIfNeeded((Collection<Object>)keys, false) : (Collection<Object>)keys;

            if (log.isDebugEnabled())
                log.debug("Removing values from cache store [keys=" + keys0 + ']');

            initSession(tx);

            try {
                store.deleteAll(keys0);
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }
            catch (Exception e) {
                if (!keys0.isEmpty())
                    throw new CacheStorePartialUpdateException(keys0, e);

                if (!(e instanceof CacheWriterException))
                    e = new CacheWriterException(e);

                throw new IgniteCheckedException(e);
            }
            finally {
                sesHolder.set(null);
            }

            if (log.isDebugEnabled())
                log.debug("Removed values from cache store [keys=" + keys0 + ']');

            return true;
        }

        return false;
    }

    /**
     * @return Store.
     */
    public CacheStore<K, Object> store() {
        return store;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void forceFlush() throws IgniteCheckedException {
        if (store instanceof GridCacheWriteBehindStore)
            ((GridCacheWriteBehindStore)store).forceFlush();
    }

    /**
     * @param tx Transaction.
     * @param commit Commit.
     * @throws IgniteCheckedException If failed.
     */
    public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        assert store != null;

        initSession(tx);

        try {
            store.txEnd(commit);
        }
        finally {
            sesHolder.set(null);

            ((GridMetadataAware)tx).removeMeta(SES_ATTR);
        }
    }

    /**
     * @param e Class cast exception.
     * @throws IgniteCheckedException Thrown exception.
     */
    private void handleClassCastException(ClassCastException e) throws IgniteCheckedException {
        assert e != null;

        if (cctx.portableEnabled() && e.getMessage() != null &&
            e.getMessage().startsWith("org.gridgain.grid.util.portable.GridPortableObjectImpl")) {
            throw new IgniteCheckedException("Cache store must work with portable objects if portables are " +
                "enabled for cache [cacheName=" + cctx.namex() + ']', e);
        }
        else
            throw e;
    }

    /**
     * @param tx Current transaction.
     */
    private void initSession(@Nullable IgniteTx tx) {
        SessionData ses;

        if (tx != null) {
            ses = ((GridMetadataAware)tx).meta(SES_ATTR);

            if (ses == null) {
                ses = new SessionData(tx, cctx.name());

                ((GridMetadataAware)tx).addMeta(SES_ATTR, ses);
            }
        }
        else
            ses = new SessionData(null, cctx.name());

        sesHolder.set(ses);
    }

    /**
     *
     */
    private static class SessionData {
        /** */
        private final IgniteTx tx;

        /** */
        private final String cacheName;

        /** */
        private Map<Object, Object> props;

        /**
         * @param tx Current transaction.
         * @param cacheName Cache name.
         */
        private SessionData(@Nullable IgniteTx tx, @Nullable String cacheName) {
            this.tx = tx;
            this.cacheName = cacheName;
        }

        /**
         * @return Transaction.
         */
        @Nullable private IgniteTx transaction() {
            return tx;
        }

        /**
         * @return Properties.
         */
        private Map<Object, Object> properties() {
            if (props == null)
                props = new GridLeanMap<>();

            return props;
        }

        /**
         * @return Cache name.
         */
        private String cacheName() {
            return cacheName;
        }
    }

    /**
     *
     */
    private static class ThreadLocalSession implements CacheStoreSession {
        /** */
        private final ThreadLocal<SessionData> sesHolder;

        /**
         * @param sesHolder Session holder.
         */
        private ThreadLocalSession(ThreadLocal<SessionData> sesHolder) {
            this.sesHolder = sesHolder;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteTx transaction() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? ses0.transaction() : null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public <K1, V1> Map<K1, V1> properties() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? (Map<K1, V1>)ses0.properties() : null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String cacheName() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? ses0.cacheName() : null;
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private class EntriesView extends AbstractCollection<Cache.Entry<? extends K, ?>> {
        /** */
        private final Map<K, IgniteBiTuple<V, GridCacheVersion>> map;

        /** */
        private Set<K> rmvd;

        /** */
        private boolean cleared;

        /**
         * @param map Map.
         */
        private EntriesView(Map<K, IgniteBiTuple<V, GridCacheVersion>> map) {
            assert map != null;

            this.map = map;
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return cleared ? 0 : (map.size() - (rmvd != null ? rmvd.size() : 0));
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return cleared || !iterator().hasNext();
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            if (cleared || !(o instanceof Cache.Entry))
                return false;

            Cache.Entry<? extends K, ?> e = (Cache.Entry<? extends K, ?>)o;

            return map.containsKey(e.getKey());
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Cache.Entry<? extends K, ?>> iterator() {
            if (cleared)
                return F.emptyIterator();

            final Iterator<Map.Entry<K, IgniteBiTuple<V, GridCacheVersion>>> it0 = map.entrySet().iterator();

            return new Iterator<Cache.Entry<? extends K, ?>>() {
                /** */
                private Cache.Entry<? extends K, ?> cur;

                /** */
                private Cache.Entry<? extends K, ?> next;

                /**
                 *
                 */
                {
                    checkNext();
                }

                /**
                 *
                 */
                private void checkNext() {
                    while (it0.hasNext()) {
                        Map.Entry<K, IgniteBiTuple<V, GridCacheVersion>> e = it0.next();

                        K k = e.getKey();

                        if (rmvd != null && rmvd.contains(k))
                            continue;

                        Object v = locStore ? e.getValue() : e.getValue().get1();

                        if (convertPortable) {
                            k = (K)cctx.unwrapPortableIfNeeded(k, false);
                            v = cctx.unwrapPortableIfNeeded(v, false);
                        }

                        next = new CacheEntryImpl<>(k, v);

                        break;
                    }
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                @Override public Cache.Entry<? extends K, ?> next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    cur = next;

                    next = null;

                    checkNext();

                    return cur;
                }

                @Override public void remove() {
                    if (cur == null)
                        throw new IllegalStateException();

                    addRemoved(cur);

                    cur = null;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public boolean add(Cache.Entry<? extends K, ?> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean addAll(Collection<? extends Cache.Entry<? extends K, ?>> col) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            if (cleared || !(o instanceof Cache.Entry))
                return false;

            Cache.Entry<? extends K, ?> e = (Cache.Entry<? extends K, ?>)o;

            if (rmvd != null && rmvd.contains(e.getKey()))
                return false;

            if (mapContains(e)) {
                addRemoved(e);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean containsAll(Collection<?> col) {
            if (cleared)
                return false;

            for (Object o : col) {
                if (contains(o))
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean removeAll(Collection<?> col) {
            if (cleared)
                return false;

            boolean modified = false;

            for (Object o : col) {
                 if (remove(o))
                     modified = true;
            }

            return modified;
        }

        /** {@inheritDoc} */
        @Override public boolean retainAll(Collection<?> col) {
            if (cleared)
                return false;

            boolean modified = false;

            for (Cache.Entry<? extends K, ?> e : this) {
                if (!col.contains(e)) {
                    addRemoved(e);

                    modified = true;
                }
            }

            return modified;
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            cleared = true;
        }

        /**
         * @param e Entry.
         */
        private void addRemoved(Cache.Entry<? extends K, ?> e) {
            if (rmvd == null)
                rmvd = new HashSet<>();

            rmvd.add(e.getKey());
        }

        /**
         * @param e Entry.
         * @return {@code True} if original map contains entry.
         */
        private boolean mapContains(Cache.Entry<? extends K, ?> e) {
            K key = (K)(convertPortable ? cctx.marshalToPortable(e.getKey()) : e.getKey());

            return map.containsKey(key);

        }

        /** {@inheritDoc} */
        public String toString() {
            Iterator<Cache.Entry<? extends K, ?>> it = iterator();

            if (!it.hasNext())
                return "[]";

            SB sb = new SB("[");

            while (true) {
                Cache.Entry<? extends K, ?> e = it.next();

                sb.a(e.toString());

                if (!it.hasNext())
                    return sb.a(']').toString();

                sb.a(", ");
            }
        }
    }
}
