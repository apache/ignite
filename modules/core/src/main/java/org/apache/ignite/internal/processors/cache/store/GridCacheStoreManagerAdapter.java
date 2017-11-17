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

package org.apache.ignite.internal.processors.cache.store;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheStoreBalancingWrapper;
import org.apache.ignite.internal.processors.cache.CacheStorePartialUpdateException;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridSetWrapper;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Store manager.
 */
@SuppressWarnings({"AssignmentToCatchBlockParameter", "unchecked"})
public abstract class GridCacheStoreManagerAdapter extends GridCacheManagerAdapter implements CacheStoreManager {
    /** */
    private static final int SES_ATTR = GridMetadataAwareAdapter.EntryKey.CACHE_STORE_MANAGER_KEY.key();

    /** */
    protected CacheStore<Object, Object> store;

    /** */
    protected CacheStore<?, ?> cfgStore;

    /** */
    private CacheStoreBalancingWrapper<Object, Object> singleThreadGate;

    /** */
    private ThreadLocal<SessionData> sesHolder;

    /** */
    private ThreadLocalSession locSes;

    /** */
    private boolean locStore;

    /** */
    private boolean writeThrough;

    /** */
    private boolean readThrough;

    /** */
    private Collection<CacheStoreSessionListener> sesLsnrs;

    /** */
    private boolean globalSesLsnrs;

    /** Always keep binary. */
    protected boolean alwaysKeepBinary;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void initialize(@Nullable CacheStore cfgStore, Map sesHolders) throws IgniteCheckedException {
        GridKernalContext ctx = igniteContext();
        CacheConfiguration cfg = cacheConfiguration();

        writeThrough = cfg.isWriteThrough();

        readThrough = cfg.isReadThrough();

        this.cfgStore = cfgStore;

        store = cacheStoreWrapper(ctx, cfgStore, cfg);

        singleThreadGate = store == null ? null : new CacheStoreBalancingWrapper<>(store,
            cfg.getStoreConcurrentLoadAllThreshold());

        ThreadLocal<SessionData> sesHolder0 = null;

        if (cfgStore != null) {
            sesHolder0 = ((Map<CacheStore, ThreadLocal>)sesHolders).get(cfgStore);

            if (sesHolder0 == null) {
                sesHolder0 = new ThreadLocal<>();

                locSes = new ThreadLocalSession(sesHolder0);

                if (ctx.resource().injectStoreSession(cfgStore, locSes))
                    sesHolders.put(cfgStore, sesHolder0);
            }
            else
                locSes = new ThreadLocalSession(sesHolder0);
        }

        sesHolder = sesHolder0;

        locStore = U.hasAnnotation(cfgStore, CacheLocalStore.class);

        if (cfgStore instanceof CacheJdbcPojoStore)
            alwaysKeepBinary = true;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return writeThrough;
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

        GridCacheWriteBehindStore store = new GridCacheWriteBehindStore(this,
            ctx.igniteInstanceName(),
            cfg.getName(),
            ctx.log(GridCacheWriteBehindStore.class),
            cfgStore);

        store.setFlushSize(cfg.getWriteBehindFlushSize());
        store.setFlushThreadCount(cfg.getWriteBehindFlushThreadCount());
        store.setFlushFrequency(cfg.getWriteBehindFlushFrequency());
        store.setBatchSize(cfg.getWriteBehindBatchSize());
        store.setWriteCoalescing(cfg.getWriteBehindCoalescing());

        return store;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (store instanceof LifecycleAware) {
            try {
                // Avoid second start() call on store in case when near cache is enabled.
                if (cctx.config().isWriteBehindEnabled()) {
                    if (!cctx.isNear())
                        ((LifecycleAware)store).start();
                }
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start cache store: " + e, e);
            }
        }

        CacheConfiguration cfg = cctx.config();

        if (cfgStore != null) {
            if (!cfg.isWriteThrough() && !cfg.isReadThrough()) {
                U.quietAndWarn(log,
                    "Persistence store is configured, but both read-through and write-through are disabled. This " +
                    "configuration makes sense if the store implements loadCache method only. If this is the " +
                    "case, ignore this warning. Otherwise, fix the configuration for the cache: " + cfg.getName(),
                    "Persistence store is configured, but both read-through and write-through are disabled " +
                    "for cache: " + cfg.getName());
            }

            if (!cfg.isWriteThrough() && cfg.isWriteBehindEnabled()) {
                U.quietAndWarn(log,
                    "To enable write-behind mode for the cache store it's also required to set " +
                    "CacheConfiguration.setWriteThrough(true) property, otherwise the persistence " +
                    "store will be never updated. Consider fixing configuration for the cache: " + cfg.getName(),
                    "Write-behind mode for the cache store also requires CacheConfiguration.setWriteThrough(true) " +
                    "property. Fix configuration for the cache: " + cfg.getName());
            }
        }

        sesLsnrs = CU.startStoreSessionListeners(cctx.kernalContext(), cfg.getCacheStoreSessionListenerFactories());

        if (sesLsnrs == null) {
            sesLsnrs = cctx.shared().storeSessionListeners();

            globalSesLsnrs = true;
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel, boolean destroy) {
        if (store instanceof LifecycleAware) {
            try {
                // Avoid second start() call on store in case when near cache is enabled.
                if (cctx.config().isWriteBehindEnabled()) {
                    if (!cctx.isNear())
                        ((LifecycleAware)store).stop();
                }
            }
            catch (Exception e) {
                U.error(log(), "Failed to stop cache store.", e);
            }
        }

        if (!globalSesLsnrs) {
            try {
                CU.stopStoreSessionListeners(cctx.kernalContext(), sesLsnrs);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to stop store session listeners for cache: " + cctx.name(), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return locStore;
    }

    /** {@inheritDoc} */
    @Override public boolean configured() {
        return store != null;
    }

    /** {@inheritDoc} */
    @Override public CacheStore<?, ?> configuredStore() {
        return cfgStore;
    }

    /** {@inheritDoc} */
    @Override @Nullable public final Object load(@Nullable IgniteInternalTx tx, KeyCacheObject key)
        throws IgniteCheckedException {
        return loadFromStore(tx, key, true);
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
    @Nullable private Object loadFromStore(@Nullable IgniteInternalTx tx,
        KeyCacheObject key,
        boolean convert)
        throws IgniteCheckedException {
        if (store != null) {
            if (key.internal())
                // Never load internal keys from store as they are never persisted.
                return null;

            Object storeKey = cctx.unwrapBinaryIfNeeded(key, !convertBinary());

            if (log.isDebugEnabled())
                log.debug(S.toString("Loading value from store for key",
                    "key", storeKey, true));

            sessionInit0(tx, StoreOperation.READ, false);

            boolean threwEx = true;

            Object val = null;

            try {
                val = singleThreadGate.load(storeKey);

                threwEx = false;
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
                IgniteInternalTx tx0 = tx;

                if (tx0 != null && (tx0.dht() && tx0.local()))
                    tx0 = null;

                sessionEnd0(tx0, threwEx);
            }

            if (log.isDebugEnabled())
                log.debug(S.toString("Loaded value from store",
                    "key", key, true,
                    "val", val, true));

            if (convert) {
                val = convert(val);

                return val;
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
    private Object convert(Object val) {
        if (val == null)
            return null;

        return locStore ? ((IgniteBiTuple<Object, GridCacheVersion>)val).get1() : val;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehind() {
        return cctx.config().isWriteBehindEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteToStoreFromDht() {
        return isWriteBehind() || locStore;
    }

    /** {@inheritDoc} */
    @Override public final void localStoreLoadAll(@Nullable IgniteInternalTx tx, Collection keys, GridInClosure3 vis)
        throws IgniteCheckedException {
        assert store != null;
        assert locStore;

        loadAllFromStore(tx, keys, null, vis);
    }

    /** {@inheritDoc} */
    @Override public final boolean loadAll(@Nullable IgniteInternalTx tx, Collection keys, IgniteBiInClosure vis)
        throws IgniteCheckedException {
        if (store != null) {
            loadAllFromStore(tx, keys, vis, null);

            return true;
        }
        else {
            for (Object key : keys)
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
    private void loadAllFromStore(@Nullable IgniteInternalTx tx,
        Collection<? extends KeyCacheObject> keys,
        @Nullable final IgniteBiInClosure<KeyCacheObject, Object> vis,
        @Nullable final GridInClosure3<KeyCacheObject, Object, GridCacheVersion> verVis)
        throws IgniteCheckedException {
        assert vis != null ^ verVis != null;
        assert verVis == null || locStore;

        final boolean convert = verVis == null;

        if (!keys.isEmpty()) {
            if (keys.size() == 1) {
                KeyCacheObject key = F.first(keys);

                if (convert)
                    vis.apply(key, load(tx, key));
                else {
                    IgniteBiTuple<Object, GridCacheVersion> t =
                        (IgniteBiTuple<Object, GridCacheVersion>)loadFromStore(tx, key, false);

                    if (t != null)
                        verVis.apply(key, t.get1(), t.get2());
                }

                return;
            }

            Collection<Object> keys0 = F.viewReadOnly(keys,
                new C1<KeyCacheObject, Object>() {
                    @Override public Object apply(KeyCacheObject key) {
                        return cctx.unwrapBinaryIfNeeded(key, !convertBinary());
                    }
                });

            if (log.isDebugEnabled())
                log.debug("Loading values from store for keys: " + keys0);

            sessionInit0(tx, StoreOperation.READ, false);

            boolean threwEx = true;

            try {
                IgniteBiInClosure<Object, Object> c = new CI2<Object, Object>() {
                    @SuppressWarnings("ConstantConditions")
                    @Override public void apply(Object k, Object val) {
                        if (convert) {
                            Object v = convert(val);

                            vis.apply(cctx.toCacheKeyObject(k), v);
                        }
                        else {
                            IgniteBiTuple<Object, GridCacheVersion> v = (IgniteBiTuple<Object, GridCacheVersion>)val;

                            if (v != null)
                                verVis.apply(cctx.toCacheKeyObject(k), v.get1(), v.get2());
                        }
                    }
                };

                if (keys.size() > singleThreadGate.loadAllThreshold()) {
                    Map<Object, Object> map = store.loadAll(keys0);

                    if (map != null) {
                        for (Map.Entry<Object, Object> e : map.entrySet())
                            c.apply(cctx.toCacheKeyObject(e.getKey()), e.getValue());
                    }
                }
                else
                    singleThreadGate.loadAll(keys0, c);

                threwEx = false;
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
                sessionEnd0(tx, threwEx);
            }

            if (log.isDebugEnabled())
                log.debug("Loaded values from store for keys: " + keys0);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean loadCache(final GridInClosure3 vis, Object[] args) throws IgniteCheckedException {
        if (store != null) {
            if (log.isDebugEnabled())
                log.debug("Loading all values from store.");

            sessionInit0(null, StoreOperation.READ, false);

            boolean threwEx = true;

            try {
                store.loadCache(new IgniteBiInClosure<Object, Object>() {
                    @Override public void apply(Object k, Object o) {
                        Object v;
                        GridCacheVersion ver = null;

                        if (locStore) {
                            IgniteBiTuple<Object, GridCacheVersion> t = (IgniteBiTuple<Object, GridCacheVersion>)o;

                            v = t.get1();
                            ver = t.get2();
                        }
                        else
                            v = o;

                        KeyCacheObject cacheKey = cctx.toCacheKeyObject(k);

                        vis.apply(cacheKey, v, ver);
                    }
                }, args);

                threwEx = false;
            }
            catch (CacheLoaderException e) {
                throw new IgniteCheckedException(e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(new CacheLoaderException(e));
            }
            finally {
                sessionEnd0(null, threwEx);
            }

            if (log.isDebugEnabled())
                log.debug("Loaded all values from store.");

            return true;
        }

        LT.warn(log, "Calling Cache.loadCache() method will have no effect, " +
            "CacheConfiguration.getStore() is not defined for cache: " + cctx.name());

        return false;
    }

    /** {@inheritDoc} */
    @Override public final boolean put(@Nullable IgniteInternalTx tx, KeyCacheObject key, CacheObject val, GridCacheVersion ver)
        throws IgniteCheckedException {
        if (store != null) {
            // Never persist internal keys.
            if (key instanceof GridCacheInternal)
                return true;

            Object key0 = cctx.unwrapBinaryIfNeeded(key, !convertBinary());
            Object val0 = cctx.unwrapBinaryIfNeeded(val, !convertBinary());

            if (log.isDebugEnabled()) {
                log.debug(S.toString("Storing value in cache store",
                    "key", key0, true,
                    "val", val0, true));
            }

            sessionInit0(tx, StoreOperation.WRITE, false);

            boolean threwEx = true;

            try {
                store.write(new CacheEntryImpl<>(key0, locStore ? F.t(val0, ver) : val0));

                threwEx = false;
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
                sessionEnd0(tx, threwEx);
            }

            if (log.isDebugEnabled()) {
                log.debug(S.toString("Stored value in cache store",
                    "key", key0, true,
                    "val", val0, true));
            }

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public final boolean putAll(
        @Nullable IgniteInternalTx tx,
        Map<? extends KeyCacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>> map
    ) throws IgniteCheckedException {
        if (F.isEmpty(map))
            return true;

        if (map.size() == 1) {
            Map.Entry<? extends KeyCacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>> e =
                ((Map<? extends KeyCacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>>)map).entrySet().iterator().next();

            return put(tx, e.getKey(), e.getValue().get1(), e.getValue().get2());
        }
        else {
            if (store != null) {
                EntriesView entries = new EntriesView(map);

                if (log.isDebugEnabled())
                    log.debug("Storing values in cache store [entries=" + entries + ']');

                sessionInit0(tx, StoreOperation.WRITE, false);

                boolean threwEx = true;

                try {
                    store.writeAll(entries);

                    threwEx = false;
                }
                catch (ClassCastException e) {
                    handleClassCastException(e);
                }
                catch (Exception e) {
                    if (!(e instanceof CacheWriterException))
                        e = new CacheWriterException(e);

                    if (!entries.isEmpty()) {
                        List<Object> keys = new ArrayList<>(entries.size());

                        for (Cache.Entry<?, ?> entry : entries)
                            keys.add(entry.getKey());

                        throw new CacheStorePartialUpdateException(keys, e);
                    }

                    throw new IgniteCheckedException(e);
                }
                finally {
                    sessionEnd0(tx, threwEx);
                }

                if (log.isDebugEnabled())
                    log.debug("Stored value in cache store [entries=" + entries + ']');

                return true;
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean remove(@Nullable IgniteInternalTx tx, KeyCacheObject key) throws IgniteCheckedException {
        if (store != null) {
            // Never remove internal key from store as it is never persisted.
            if (key instanceof GridCacheInternal)
                return false;

            Object key0 = cctx.unwrapBinaryIfNeeded(key, !convertBinary());

            if (log.isDebugEnabled())
                log.debug(S.toString("Removing value from cache store", "key", key0, true));

            sessionInit0(tx, StoreOperation.WRITE, false);

            boolean threwEx = true;

            try {
                store.delete(key0);

                threwEx = false;
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
                sessionEnd0(tx, threwEx);
            }

            if (log.isDebugEnabled())
                log.debug(S.toString("Removed value from cache store", "key", key0, true));

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAll(
        @Nullable IgniteInternalTx tx,
        Collection<? extends KeyCacheObject> keys
    ) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return true;

        if (keys.size() == 1) {
            KeyCacheObject key = keys.iterator().next();

            return remove(tx, key);
        }

        if (store != null) {
            Collection<Object> keys0 = cctx.unwrapBinariesIfNeeded(keys, !convertBinary());

            if (log.isDebugEnabled())
                log.debug(S.toString("Removing values from cache store",
                    "keys", keys0, true));

            sessionInit0(tx, StoreOperation.WRITE, false);

            boolean threwEx = true;

            try {
                store.deleteAll(keys0);

                threwEx = false;
            }
            catch (ClassCastException e) {
                handleClassCastException(e);
            }
            catch (Exception e) {
                if (!(e instanceof CacheWriterException))
                    e = new CacheWriterException(e);

                if (!keys0.isEmpty())
                    throw new CacheStorePartialUpdateException(keys0, e);

                throw new IgniteCheckedException(e);
            }
            finally {
                sessionEnd0(tx, threwEx);
            }

            if (log.isDebugEnabled())
                log.debug(S.toString("Removed values from cache store",
                    "keys", keys0, true));

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public CacheStore<Object, Object> store() {
        return store;
    }

    /** {@inheritDoc} */
    @Override public void forceFlush() throws IgniteCheckedException {
        if (store instanceof GridCacheWriteBehindStore)
            ((GridCacheWriteBehindStore)store).forceFlush();
    }

    /** {@inheritDoc} */
    @Override public final void sessionEnd(IgniteInternalTx tx, boolean commit, boolean last,
        boolean storeSessionEnded) throws IgniteCheckedException {
        assert store != null;

        sessionInit0(tx, commit? StoreOperation.COMMIT: StoreOperation.ROLLBACK, false);

        try {
            if (sesLsnrs != null && sesHolder.get().contains(store)) {
                for (CacheStoreSessionListener lsnr : sesLsnrs)
                    lsnr.onSessionEnd(locSes, commit);
            }

            if (!sesHolder.get().ended(store) && !storeSessionEnded)
                store.sessionEnd(commit);
        }
        catch (Throwable e) {
            last = true;

            throw e;
        }
        finally {
            if (last && sesHolder != null) {
                sesHolder.set(null);

                tx.removeMeta(SES_ATTR);
            }
        }
    }

    /**
     * @param e Class cast exception.
     * @throws IgniteCheckedException Thrown exception.
     */
    private void handleClassCastException(ClassCastException e) throws IgniteCheckedException {
        assert e != null;

        if (e.getMessage() != null) {
            throw new IgniteCheckedException("Cache store must work with binary objects if binary are " +
                "enabled for cache [cacheName=" + cctx.name() + ']', e);
        }
        else
            throw e;
    }

    /** {@inheritDoc} */
    @Override public void writeBehindSessionInit() throws IgniteCheckedException {
        sessionInit0(null, null, true);
    }

    /** {@inheritDoc} */
    @Override public void writeBehindCacheStoreSessionListenerStart()  throws IgniteCheckedException {
        assert sesHolder.get() != null;

        notifyCacheStoreSessionListeners(sesHolder.get(), null, true);
    }

    /** {@inheritDoc} */
    @Override public void writeBehindSessionEnd(boolean threwEx) throws IgniteCheckedException {
        sessionEnd0(null, threwEx);
    }

    /**
     * @param tx Current transaction.
     * @param op Store operation.
     * @param writeBehindStoreInitiator {@code true} if method call is initiated by {@link GridCacheWriteBehindStore}.
     * @throws IgniteCheckedException If failed.
     */
    private void sessionInit0(@Nullable IgniteInternalTx tx, @Nullable StoreOperation op,
        boolean writeBehindStoreInitiator) throws IgniteCheckedException {
        assert sesHolder != null;

        SessionData ses;

        if (tx != null) {
            ses = tx.meta(SES_ATTR);

            if (ses == null) {
                ses = new SessionData(tx, cctx.name());

                tx.addMeta(SES_ATTR, ses);
            }
            else
                // Session cache name may change in cross-cache transaction.
                ses.cacheName(cctx.name());
        }
        else
            ses = new SessionData(null, cctx.name());

        sesHolder.set(ses);

        notifyCacheStoreSessionListeners(ses, op, writeBehindStoreInitiator);
    }

    /**
     * @param ses Current session.
     * @param op Store operation.
     * @param writeBehindStoreInitiator {@code True} if method call is initiated by {@link GridCacheWriteBehindStore}.
     * @throws IgniteCheckedException If failed.
     */
    private void notifyCacheStoreSessionListeners(SessionData ses, @Nullable StoreOperation op,
        boolean writeBehindStoreInitiator) throws IgniteCheckedException {
        try {
            boolean notifyLsnrs = false;

            if (writeBehindStoreInitiator)
                notifyLsnrs = !ses.started(store) && sesLsnrs != null;
            else {
                assert op != null;

                switch (op) {
                    case READ:
                        notifyLsnrs = readThrough && !ses.started(store) && sesLsnrs != null;
                        break;

                    case WRITE:
                        notifyLsnrs = !cacheConfiguration().isWriteBehindEnabled() && writeThrough
                            && !ses.started(store) && sesLsnrs != null;
                        break;

                    case COMMIT:
                    case ROLLBACK:
                        // No needs to start the session (if not started yet) and notify listeners.
                        break;

                    default:
                        assert false : "Unexpected operation: " + op.toString();
                }
            }
            if (notifyLsnrs) {
                for (CacheStoreSessionListener lsnr : sesLsnrs)
                    lsnr.onSessionStart(locSes);
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to start store session: " + e, e);
        }
    }

    /**
     * Clears session holder.
     */
    private void sessionEnd0(@Nullable IgniteInternalTx tx, boolean threwEx) throws IgniteCheckedException {
        try {
            if (tx == null) {
                if (sesLsnrs != null && sesHolder.get().contains(store)) {
                    for (CacheStoreSessionListener lsnr : sesLsnrs)
                        lsnr.onSessionEnd(locSes, !threwEx);
                }

                if (!sesHolder.get().ended(store))
                    store.sessionEnd(!threwEx);
            }
        }
        catch (Exception e) {
            if (!threwEx)
                throw U.cast(e);
        }
        finally {
            if (sesHolder != null)
                sesHolder.set(null);
        }
    }

    /**
     * @return Ignite context.
     */
    protected abstract GridKernalContext igniteContext();

    /**
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration();

    /**
     *
     */
    private static class SessionData {
        /** */
        @GridToStringExclude
        private final TxProxy tx;

        /** */
        private String cacheName;

        /** */
        @GridToStringInclude
        private Map<Object, Object> props;

        /** */
        private Object attach;

        /** */
        private final Set<CacheStore> started =
            new GridSetWrapper<>(new IdentityHashMap<CacheStore, Object>());

        /**
         * @param tx Current transaction.
         * @param cacheName Cache name.
         */
        private SessionData(@Nullable final IgniteInternalTx tx, @Nullable String cacheName) {
            this.tx = tx != null ? new TxProxy(tx) : null;
            this.cacheName = cacheName;
        }

        /**
         * @return Transaction.
         */
        @Nullable private Transaction transaction() {
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
         * @param attach Attachment.
         */
        private Object attach(Object attach) {
            Object prev = this.attach;

            this.attach = attach;

            return prev;
        }

        /**
         * @return Attachment.
         */
        private Object attachment() {
            return attach;
        }

        /**
         * @return Cache name.
         */
        private String cacheName() {
            return cacheName;
        }

        /**
         * @param cacheName Cache name.
         */
        private void cacheName(String cacheName) {
            this.cacheName = cacheName;
        }

        /**
         * @return If session is started.
         */
        private boolean started(CacheStore store) {
            return !started.add(store);
        }

        /**
         * @param store Cache store.
         * @return Whether session already ended on this store instance.
         */
        private boolean ended(CacheStore store) {
            return !started.remove(store);
        }

        /**
         * @param store Cache store.
         * @return {@code True} if session started.
         */
        private boolean contains(CacheStore store) {
            return started.contains(store);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SessionData.class, this, "tx", CU.txString(tx != null ? tx.tx : null));
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
        @Nullable @Override public Transaction transaction() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? ses0.transaction() : null;
        }

        /** {@inheritDoc} */
        @Override public boolean isWithinTransaction() {
            return transaction() != null;
        }

        /** {@inheritDoc} */
        @Override public Object attach(@Nullable Object attachment) {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? ses0.attach(attachment) : null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T attachment() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? (T)ses0.attachment() : null;
        }

        /** {@inheritDoc} */
        @Override public <K1, V1> Map<K1, V1> properties() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? (Map<K1, V1>)ses0.properties() : null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String cacheName() {
            SessionData ses0 = sesHolder.get();

            return ses0 != null ? ses0.cacheName() : null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ThreadLocalSession.class, this);
        }
    }

    /**
     *
     */
    private class EntriesView extends AbstractCollection<Cache.Entry<?, ?>> {
        /** */
        private final Map<? extends KeyCacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>> map;

        /** */
        private Set<Object> rmvd;

        /** */
        private boolean cleared;

        /**
         * @param map Map.
         */
        private EntriesView(Map<? extends KeyCacheObject, IgniteBiTuple<? extends CacheObject, GridCacheVersion>> map) {
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

            Cache.Entry<?, ?> e = (Cache.Entry<?, ?>)o;

            return map.containsKey(e.getKey());
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Cache.Entry<?, ?>> iterator() {
            if (cleared)
                return new GridEmptyIterator<>();

            final Iterator<Map.Entry<?, IgniteBiTuple<?, GridCacheVersion>>> it0 = (Iterator)map.entrySet().iterator();

            return new Iterator<Cache.Entry<?, ?>>() {
                /** */
                private Cache.Entry<?, ?> cur;

                /** */
                private Cache.Entry<?, ?> next;

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
                        Map.Entry<?, IgniteBiTuple<?, GridCacheVersion>> e = it0.next();

                        Object k = e.getKey();

                        Object v = locStore ? e.getValue() : e.getValue().get1();

                        k = cctx.unwrapBinaryIfNeeded(k, !convertBinary());
                        v = cctx.unwrapBinaryIfNeeded(v, !convertBinary());

                        if (rmvd != null && rmvd.contains(k))
                            continue;

                        next = new CacheEntryImpl<>(k, v);

                        break;
                    }
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                @Override public Cache.Entry<?, ?> next() {
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
        @Override public boolean add(Cache.Entry<?, ?> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean addAll(Collection<? extends Cache.Entry<?, ?>> col) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            if (cleared || !(o instanceof Cache.Entry))
                return false;

            Cache.Entry<?, ?> e = (Cache.Entry<?, ?>)o;

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

            for (Cache.Entry<?, ?> e : this) {
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
        private void addRemoved(Cache.Entry<?, ?> e) {
            if (rmvd == null)
                rmvd = new HashSet<>();

            rmvd.add(e.getKey());
        }

        /**
         * @param e Entry.
         * @return {@code True} if original map contains entry.
         */
        private boolean mapContains(Cache.Entry<?, ?> e) {
            return map.containsKey(e.getKey());
        }

        /** {@inheritDoc} */
        public String toString() {
            if (!S.INCLUDE_SENSITIVE)
                return "[size=" + size() + "]";

            Iterator<Cache.Entry<?, ?>> it = iterator();

            if (!it.hasNext())
                return "[]";

            SB sb = new SB("[");

            while (true) {
                Cache.Entry<?, ?> e = it.next();

                sb.a(e.toString());

                if (!it.hasNext())
                    return sb.a(']').toString();

                sb.a(", ");
            }
        }
    }

    /**
     *
     */
    private static class TxProxy implements Transaction {
        /** */
        private final IgniteInternalTx tx;

        /**
         * @param tx Transaction.
         */
        TxProxy(IgniteInternalTx tx) {
            assert tx != null;

            this.tx = tx;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid xid() {
            return tx.xid();
        }

        /** {@inheritDoc} */
        @Override public UUID nodeId() {
            return tx.nodeId();
        }

        /** {@inheritDoc} */
        @Override public long threadId() {
            return tx.threadId();
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            return tx.startTime();
        }

        /** {@inheritDoc} */
        @Override public TransactionIsolation isolation() {
            return tx.isolation();
        }

        /** {@inheritDoc} */
        @Override public TransactionConcurrency concurrency() {
            return tx.concurrency();
        }

        /** {@inheritDoc} */
        @Override public boolean implicit() {
            return tx.implicit();
        }

        /** {@inheritDoc} */
        @Override public boolean isInvalidate() {
            return tx.isInvalidate();
        }

        /** {@inheritDoc} */
        @Override public TransactionState state() {
            return tx.state();
        }

        /** {@inheritDoc} */
        @Override public void suspend() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            return tx.timeout();
        }

        /** {@inheritDoc} */
        @Override public long timeout(long timeout) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean setRollbackOnly() {
            return tx.setRollbackOnly();
        }

        /** {@inheritDoc} */
        @Override public boolean isRollbackOnly() {
            return tx.isRollbackOnly();
        }

        /** {@inheritDoc} */
        @Override public void commit() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void resume() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public IgniteAsyncSupport withAsync() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isAsync() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public <R> IgniteFuture<R> future() {
            throw new UnsupportedOperationException();
        }
    }

    /** Enumeration that represents possible operations on the underlying store. */
    private enum StoreOperation {
        /** Read key-value pair from the underlying store. */
        READ,

        /** Update or remove key from the underlying store. */
        WRITE,

        /** Commit changes to the underlying store. */
        COMMIT,

        /** Rollback changes to the underlying store. */
        ROLLBACK
    }
}
