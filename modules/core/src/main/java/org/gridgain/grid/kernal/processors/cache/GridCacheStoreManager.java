/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.processors.interop.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Store manager.
 */
public class GridCacheStoreManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** */
    private final GridCacheStore<K, Object> store;

    /** */
    private final GridCacheStoreBalancingWrapper<K, Object> singleThreadGate;

    /** */
    private final boolean locStore;

    /** */
    private boolean convertPortable;

    /**
     * @param store Store.
     */
    @SuppressWarnings("unchecked")
    public GridCacheStoreManager(@Nullable GridCacheStore<K, Object> store) {
        this.store = store;

        singleThreadGate = store == null ? null : new GridCacheStoreBalancingWrapper<>(store);

        if (store instanceof GridCacheWriteBehindStore)
            store = ((GridCacheWriteBehindStore)store).store();

        locStore = U.hasAnnotation(store, GridCacheLocalStore.class);
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
    @Nullable public V loadFromStore(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        if (store != null) {
            if (key instanceof GridCacheInternal)
                // Never load internal keys from store as they are never persisted.
                return null;

            if (convertPortable)
                key = (K)cctx.unwrapPortableIfNeeded(key, false);

            if (log.isDebugEnabled())
                log.debug("Loading value from store for key: " + key);

            V val = null;

            try {
                val = convert(singleThreadGate.load(tx, key));
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }

            if (log.isDebugEnabled())
                log.debug("Loaded value from store [key=" + key + ", val=" + val + ']');

            return cctx.portableEnabled() ? (V)cctx.marshalToPortable(val) : val;
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
            if (!keys.isEmpty()) {
                if (keys.size() == 1) {
                    K key = F.first(keys);

                    vis.apply(key, loadFromStore(tx, key));

                    return true;
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

                try {
                    singleThreadGate.loadAll(tx, keys0, new CI2<K, Object>() {
                        @Override public void apply(K k, Object o) {
                            V v = convert(o);

                            if (cctx.portableEnabled()) {
                                k = (K)cctx.marshalToPortable(k);
                                v = (V)cctx.marshalToPortable(v);
                            }

                            vis.apply(k, v);
                        }
                    });
                }
                catch (ClassCastException e) {
                    handleClassCastException(e);
                }
                catch (IgniteException e) {
                    throw U.cast(e);
                }

                if (log.isDebugEnabled())
                    log.debug("Loaded values from store for keys: " + keys0);
            }

            return true;
        }
        else {
            for (K key : keys)
                vis.apply(key, null);
        }

        return false;
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
            catch (IgniteException e) {
                throw U.cast(e);
            }
            catch (AssertionError e) {
                throw new IgniteCheckedException(e);
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

            try {
                store.put(tx, key, locStore ? F.t(val, ver) : val);
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
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
                Map<K, IgniteBiTuple<V, GridCacheVersion>> map0;

                if (convertPortable) {
                    map0 = U.newHashMap(map.size());

                    for (Map.Entry<K, IgniteBiTuple<V, GridCacheVersion>> e : map.entrySet()) {
                        IgniteBiTuple<V, GridCacheVersion> t = e.getValue();

                        map0.put((K)cctx.unwrapPortableIfNeeded(e.getKey(), false),
                            F.t((V)cctx.unwrapPortableIfNeeded(t.get1(), false), t.get2()));
                    }
                }
                else
                    map0 = map;

                if (log.isDebugEnabled())
                    log.debug("Storing values in cache store [map=" + map0 + ']');

                try {
                    store.putAll(tx, locStore ? map0 : F.viewReadOnly(map0,
                        new C1<IgniteBiTuple<V, GridCacheVersion>, Object>() {
                            @Override public Object apply(IgniteBiTuple<V, GridCacheVersion> t) {
                                return t.get1();
                            }
                    }));
                }
                catch (ClassCastException e) {
                    handleClassCastException(e);
                }

                if (log.isDebugEnabled())
                    log.debug("Stored value in cache store [map=" + map0 + ']');

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

            else {
                try {
                    store.remove(tx, key);
                }
                catch (ClassCastException e) {
                    handleClassCastException(e);
                }
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
    public boolean removeAllFromStore(@Nullable IgniteTx tx, Collection<? extends K> keys) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return true;

        if (keys.size() == 1) {
            K key = keys.iterator().next();

            return removeFromStore(tx, key);
        }

        if (store != null) {
            Collection<? extends K> keys0 = convertPortable ?
                F.viewReadOnly(keys, new C1<K, K>() {
                    @Override public K apply(K k) {
                        return (K)cctx.unwrapPortableIfNeeded(k, false);
                    }
                }) :
                keys;

            if (log.isDebugEnabled())
                log.debug("Removing values from cache store [keys=" + keys0 + ']');

            try {
                store.removeAll(tx, keys0);
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
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
    public GridCacheStore<K, Object> store() {
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
        store.txEnd(tx, commit);
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
}
