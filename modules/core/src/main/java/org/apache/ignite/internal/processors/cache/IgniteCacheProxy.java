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
import org.apache.ignite.cache.CacheEntryEvent;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.event.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.IgniteEventType.*;

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

    /**
     * @return Ignite instance.
     */
    @Override public GridEx ignite() {
        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (!clazz.equals(GridCacheConfiguration.class))
            throw new IllegalArgumentException();

        return (C)ctx.config();
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
    @Override public void loadCache(@Nullable IgniteBiPredicate p, @Nullable Object... args) throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate p, @Nullable Object... args)
        throws CacheException {
        // TODO IGNITE-1.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIf(K key, V val, IgnitePredicate<GridCacheEntry<K, V>> filter) {
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
    @Override public boolean isLocked(K key) {
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
        throw new UnsupportedOperationException();
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
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration lsnrCfg) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            A.notNull(lsnrCfg, "lsnrCfg");

            Factory<CacheEntryListener<? super K, ? super V>> factory = lsnrCfg.getCacheEntryListenerFactory();

            A.notNull(factory, "cacheEntryListenerFactory");

            CacheEntryListener lsnr = factory.create();

            A.notNull(lsnr, "lsnr");

            EventCallback cb = new EventCallback(lsnr);

            Set<Integer> types = new HashSet<>();

            if (cb.create() || cb.update())
                types.add(EVT_CACHE_OBJECT_PUT);

            if (cb.remove())
                types.add(EVT_CACHE_OBJECT_REMOVED);

            if (cb.expire())
                types.add(EVT_CACHE_OBJECT_EXPIRED);

            if (types.isEmpty())
                throw new IllegalArgumentException();

            int[] types0 = new int[types.size()];

            int i = 0;

            for (Integer type : types)
                types0[i++] = type;

            EventFilter fltr = new EventFilter(cb.create(),
                cb.update(),
                lsnrCfg.getCacheEntryEventFilterFactory(),
                ignite(),
                ctx.name());

            IgniteFuture<UUID> fut = ctx.kernalContext().continuous().startRoutine(
                new GridEventConsumeHandler(cb, fltr, types0),
                1,
                0,
                true,
                null);

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     *
     */
    static class EventFilter implements IgnitePredicate<IgniteEvent>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private boolean update;

        /** */
        private boolean create;

        /** */
        private Factory<CacheEntryEventFilter> fltrFactory;

        /** */
        private CacheEntryEventFilter fltr;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteCache cache;

        /** */
        private String cacheName;

        /**
         *
         */
        public EventFilter() {
            // No-op.
        }

        /**
         * @param create {@code True} if listens for create event.
         * @param update {@code True} if listens for create event.
         * @param fltrFactory Filter factory.
         * @param ignite Ignite instance.
         * @param cacheName Cache name.
         */
        EventFilter(
            boolean create,
            boolean update,
            Factory<CacheEntryEventFilter> fltrFactory,
            Ignite ignite,
            @Nullable String cacheName) {
            this.update = update;
            this.create = create;
            this.fltrFactory = fltrFactory;
            this.ignite = ignite;
            this.cacheName = cacheName;

            if (fltrFactory != null)
                fltr = fltrFactory.create();

            cache = ignite.jcache(cacheName);

            assert cache != null : cacheName;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public boolean apply(IgniteEvent evt) {
            assert evt instanceof IgniteCacheEvent : evt;

            IgniteCacheEvent cacheEvt = (IgniteCacheEvent)evt;

            EventType evtType;

            switch (cacheEvt.type()) {
                case EVT_CACHE_OBJECT_REMOVED: {
                    evtType = EventType.REMOVED;

                    break;
                }

                case EVT_CACHE_OBJECT_PUT: {
                    assert update || create;

                    if (cacheEvt.hasOldValue()) {
                        if (!update)
                            return false;

                        evtType = EventType.UPDATED;
                    }
                    else {
                        if (!create)
                            return false;

                        evtType = EventType.CREATED;
                    }

                    break;
                }

                case EVT_CACHE_OBJECT_EXPIRED: {
                    evtType = EventType.EXPIRED;

                    break;
                }

                default:
                    assert false : cacheEvt;

                    throw new IgniteException("Unexpected event: " + cacheEvt);
            }

            if (cache == null) {
                cache = ignite.jcache(cacheName);

                assert cache != null : cacheName;
            }

            return fltr == null || fltr.evaluate(new CacheEntryEvent(cache, evtType, cacheEvt));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(create);

            out.writeBoolean(update);

            U.writeString(out, cacheName);

            out.writeObject(fltrFactory);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            create = in.readBoolean();

            update = in.readBoolean();

            cacheName = U.readString(in);

            fltrFactory = (Factory<CacheEntryEventFilter>)in.readObject();

            if (fltrFactory != null)
                fltr = fltrFactory.create();
        }
    }

    /**
     *
     */
    class EventCallback implements IgniteBiPredicate<UUID, IgniteEvent> {
        /** */
        private final CacheEntryCreatedListener createLsnr;

        /** */
        private final CacheEntryUpdatedListener updateLsnr;

        /** */
        private final CacheEntryRemovedListener rmvLsnr;

        /** */
        private final CacheEntryExpiredListener expireLsnr;

        /**
         * @param lsnr Listener.
         */
        EventCallback(CacheEntryListener lsnr) {
            createLsnr = lsnr instanceof CacheEntryCreatedListener ? (CacheEntryCreatedListener)lsnr : null;
            updateLsnr = lsnr instanceof CacheEntryUpdatedListener ? (CacheEntryUpdatedListener)lsnr : null;
            rmvLsnr = lsnr instanceof CacheEntryRemovedListener ? (CacheEntryRemovedListener)lsnr : null;
            expireLsnr = lsnr instanceof CacheEntryExpiredListener ? (CacheEntryExpiredListener)lsnr : null;
        }

        /**
         * @return {@code True} if listens for create event.
         */
        boolean create() {
            return createLsnr != null;
        }

        /**
         * @return {@code True} if listens for update event.
         */
        boolean update() {
            return updateLsnr != null;
        }

        /**
         * @return {@code True} if listens for remove event.
         */
        boolean remove() {
            return rmvLsnr != null;
        }

        /**
         * @return {@code True} if listens for expire event.
         */
        boolean expire() {
            return expireLsnr != null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public boolean apply(UUID uuid, IgniteEvent evt) {
            assert evt instanceof IgniteCacheEvent : evt;

            IgniteCacheEvent cacheEvt = (IgniteCacheEvent)evt;

            switch (cacheEvt.type()) {
                case EVT_CACHE_OBJECT_REMOVED: {
                    assert rmvLsnr != null;

                    CacheEntryEvent evt0 = new CacheEntryEvent(IgniteCacheProxy.this, EventType.REMOVED, cacheEvt);

                    rmvLsnr.onRemoved(Collections.singleton(evt0));

                    break;
                }

                case EVT_CACHE_OBJECT_PUT: {
                    if (cacheEvt.hasOldValue()) {
                        assert updateLsnr != null;

                        CacheEntryEvent evt0 = new CacheEntryEvent(IgniteCacheProxy.this, EventType.UPDATED, cacheEvt);

                        updateLsnr.onUpdated(Collections.singleton(evt0));
                    }
                    else {
                        assert createLsnr != null;

                        CacheEntryEvent evt0 = new CacheEntryEvent(IgniteCacheProxy.this, EventType.CREATED, cacheEvt);

                        createLsnr.onCreated(Collections.singleton(evt0));
                    }

                    break;
                }

                case EVT_CACHE_OBJECT_EXPIRED: {
                    assert expireLsnr != null;

                    CacheEntryEvent evt0 = new CacheEntryEvent(IgniteCacheProxy.this, EventType.EXPIRED, cacheEvt);

                    expireLsnr.onExpired(Collections.singleton(evt0));

                    break;
                }
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration lsnrCfg) {
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
        if (isAsync())
            return this;

        return new IgniteCacheProxy<>(ctx, delegate, prj, true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K1, V1> IgniteCache<K1, V1> keepPortable() {
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
