/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.event.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static javax.cache.event.EventType.*;
import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Continuous queries manager.
 */
public class GridCacheContinuousQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Ordered topic prefix. */
    private String topicPrefix;

    /** Listeners. */
    private final ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrs = new ConcurrentHashMap8<>();

    /** Listeners count. */
    private final AtomicInteger lsnrCnt = new AtomicInteger();

    /** Internal entries listeners. */
    private final ConcurrentMap<UUID, ListenerInfo<K, V>> intLsnrs = new ConcurrentHashMap8<>();

    /** Internal listeners count. */
    private final AtomicInteger intLsnrCnt = new AtomicInteger();

    /** Query sequence number for message topic. */
    private final AtomicLong seq = new AtomicLong();

    /** Continues queries created for cache event listeners. */
    private final ConcurrentMap<CacheEntryListenerConfiguration, GridCacheContinuousQuery<K, V>> lsnrQrys =
        new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        // Append cache name to the topic.
        topicPrefix = "CONTINUOUS_QUERY" + (cctx.name() == null ? "" : "_" + cctx.name());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (intLsnrCnt.get() > 0 || lsnrCnt.get() > 0) {
            Collection<ClusterNode> nodes = cctx.discovery().cacheNodes(cctx.name(), -1);

            for (ClusterNode n : nodes) {
                if (!n.version().greaterThanEqual(6, 2, 0))
                    throw new IgniteCheckedException("Rolling update is not supported for continuous queries " +
                        "for versions below 6.2.0");
            }
        }

        Iterable<CacheEntryListenerConfiguration<K, V>> lsnrCfgs = cctx.config().getCacheEntryListenerConfigurations();

        if (lsnrCfgs != null) {
            IgniteCacheProxy<K, V> cache = cctx.kernalContext().cache().jcache(cctx.name());

            for (CacheEntryListenerConfiguration<K, V> cfg : lsnrCfgs)
                cache.registerCacheEntryListener(cfg);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        for (CacheEntryListenerConfiguration lsnrCfg : lsnrQrys.keySet()) {
            try {
                deregisterCacheEntryListener(lsnrCfg);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to remove cache entry listener: " + e);
            }
        }
    }

    /**
     * @param prjPred Projection predicate.
     * @return New continuous query.
     */
    public GridCacheContinuousQuery<K, V> createQuery(@Nullable IgnitePredicate<GridCacheEntry<K, V>> prjPred) {
        Object topic = TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement());

        return new GridCacheContinuousQueryAdapter<>(cctx, topic, prjPred);
    }

    /**
     * @param e Cache entry.
     * @param key Key.
     * @param newVal New value.
     * @param newBytes New value bytes.
     * @param oldVal Old value.
     * @param oldBytes Old value bytes.
     * @param preload {@code True} if entry is updated during preloading.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryUpdate(GridCacheEntryEx<K, V> e,
        K key,
        @Nullable V newVal,
        @Nullable GridCacheValueBytes newBytes,
        V oldVal,
        @Nullable GridCacheValueBytes oldBytes,
        boolean preload) throws IgniteCheckedException {
        assert e != null;
        assert key != null;

        ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrCol;

        if (e.isInternal())
            lsnrCol = intLsnrCnt.get() > 0 ? intLsnrs : null;
        else
            lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        if (F.isEmpty(lsnrCol))
            return;

        oldVal = cctx.unwrapTemporary(oldVal);

        GridCacheContinuousQueryEntry<K, V> e0 = new GridCacheContinuousQueryEntry<>(
            cctx,
            e.wrap(false),
            key,
            newVal,
            newBytes,
            oldVal,
            oldBytes,
            false);

        e0.initValue(cctx.marshaller(), cctx.deploy().globalLoader());

        boolean recordEvt = !e.isInternal() && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

        for (ListenerInfo<K, V> lsnr : lsnrCol.values()) {
            if (preload && lsnr.entryListener())
                continue;

            lsnr.onEntryUpdate(e0, recordEvt);
        }
    }

    /**
     * @param e Entry.
     * @param key Key.
     * @param oldVal Old value.
     * @param oldBytes Old value bytes.
     */
    public void onEntryExpired(GridCacheEntryEx<K, V> e,
        K key,
        V oldVal,
        @Nullable GridCacheValueBytes oldBytes) {
        if (e.isInternal())
            return;

        ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrCol = lsnrs;

        if (F.isEmpty(lsnrCol))
            return;

        if (cctx.isReplicated() || cctx.affinity().primary(cctx.localNode(), key, -1)) {
            GridCacheContinuousQueryEntry<K, V> e0 = new GridCacheContinuousQueryEntry<>(
                cctx,
                e.wrap(false),
                key,
                null,
                null,
                oldVal,
                oldBytes,
                true);

            for (ListenerInfo<K, V> lsnr : lsnrCol.values()) {
                if (!lsnr.entryListener())
                    continue;

                lsnr.onEntryUpdate(e0, false);
            }
        }
    }

    /**
     * @param lsnrCfg Listener configuration.
     * @throws IgniteCheckedException If failed.
     */
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg)
        throws IgniteCheckedException {
        GridCacheContinuousQueryAdapter<K, V> qry = null;

        try {
            A.notNull(lsnrCfg, "lsnrCfg");

            Factory<CacheEntryListener<? super K, ? super V>> factory = lsnrCfg.getCacheEntryListenerFactory();

            A.notNull(factory, "cacheEntryListenerFactory");

            CacheEntryListener lsnr = factory.create();

            A.notNull(lsnr, "lsnr");

            IgniteCacheProxy<K, V> cache= cctx.kernalContext().cache().jcache(cctx.name());

            EntryListenerCallback cb = new EntryListenerCallback(cache, lsnr);

            if (!(cb.create() || cb.update() || cb.remove() || cb.expire()))
                throw new IllegalArgumentException("Listener must implement one of CacheEntryListener sub-interfaces.");

            qry = (GridCacheContinuousQueryAdapter<K, V>)cctx.cache().queries().createContinuousQuery();

            GridCacheContinuousQuery<K, V> old = lsnrQrys.putIfAbsent(lsnrCfg, qry);

            if (old != null)
                throw new IllegalArgumentException("Listener is already registered for configuration: " + lsnrCfg);

            qry.autoUnsubscribe(true);

            qry.bufferSize(1);

            qry.localCallback(cb);

            EntryListenerFilter<K, V> fltr = new EntryListenerFilter<>(cb.create(),
                cb.update(),
                cb.remove(),
                cb.expire(),
                lsnrCfg.getCacheEntryEventFilterFactory(),
                cctx.kernalContext().grid(),
                cctx.name());

            qry.remoteFilter(fltr);

            qry.execute(null, false, true, lsnrCfg.isSynchronous());
        }
        catch (IgniteCheckedException e) {
            lsnrQrys.remove(lsnrCfg, qry); // Remove query if failed to execute it.

            throw e;
        }
    }

    /**
     * @param lsnrCfg Listener configuration.
     * @throws IgniteCheckedException If failed.
     */
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration lsnrCfg) throws IgniteCheckedException {
        A.notNull(lsnrCfg, "lsnrCfg");

        GridCacheContinuousQuery<K, V> qry = lsnrQrys.remove(lsnrCfg);

        if (qry != null)
            qry.close();
    }

    /**
     * @param lsnrId Listener ID.
     * @param lsnr Listener.
     * @param internal Internal flag.
     * @param entryLsnr {@code True} if query created for {@link CacheEntryListener}.
     * @param sync {@code True} if query created for synchronous {@link CacheEntryListener}.
     * @return Whether listener was actually registered.
     */
    boolean registerListener(UUID lsnrId,
        GridCacheContinuousQueryListener<K, V> lsnr,
        boolean internal,
        boolean entryLsnr,
        boolean sync) {
        assert !sync || entryLsnr;

        ListenerInfo<K, V> info = new ListenerInfo<>(lsnr, entryLsnr, sync);

        boolean added;

        if (internal) {
            added = intLsnrs.putIfAbsent(lsnrId, info) == null;

            if (added)
                intLsnrCnt.incrementAndGet();
        }
        else {
            added = lsnrs.putIfAbsent(lsnrId, info) == null;

            if (added) {
                lsnrCnt.incrementAndGet();

                lsnr.onExecution();
            }
        }

        return added;
    }

    /**
     * @param internal Internal flag.
     * @param id Listener ID.
     */
    void unregisterListener(boolean internal, UUID id) {
        if (internal) {
            if (intLsnrs.remove(id) != null)
                intLsnrCnt.decrementAndGet();
        }
        else {
            if (lsnrs.remove(id) != null)
                lsnrCnt.decrementAndGet();
        }
    }

    /**
     * Iterates through existing data.
     *
     * @param internal Internal flag.
     * @param id Listener ID.
     */
    void iterate(boolean internal, UUID id) {
        ListenerInfo<K, V> info = internal ? intLsnrs.get(id) : lsnrs.get(id);

        assert info != null;

        Set<GridCacheEntry<K, V>> entries;

        if (cctx.isReplicated())
            entries = internal ? cctx.cache().entrySetx() :
                cctx.cache().entrySet();
        else
            entries = internal ? cctx.cache().primaryEntrySetx() :
                cctx.cache().primaryEntrySet();

        for (GridCacheEntry<K, V> e : entries) {
            GridCacheContinuousQueryEntry<K, V> qryEntry = new GridCacheContinuousQueryEntry<>(cctx,
                e,
                e.getKey(),
                e.getValue(),
                null,
                null,
                null,
                false);

            info.onIterate(qryEntry, !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ));
        }

        info.flushPending();
    }

    /**
     * Listener info.
     */
    private static class ListenerInfo<K, V> {
        /** Listener. */
        private final GridCacheContinuousQueryListener<K, V> lsnr;

        /** Pending entries. */
        private Collection<PendingEntry<K, V>> pending;

        /** */
        private final boolean entryLsnr;

        /** */
        private final boolean sync;

        /**
         * @param lsnr Listener.
         * @param entryLsnr {@code True} if listener created for {@link CacheEntryListener}.
         * @param sync {@code True} if listener is synchronous.
         */
        private ListenerInfo(GridCacheContinuousQueryListener<K, V> lsnr, boolean entryLsnr, boolean sync) {
            this.lsnr = lsnr;
            this.entryLsnr = entryLsnr;
            this.sync = sync;

            if (!entryLsnr)
                pending = new LinkedList<>();
        }

        /**
         * @param e Entry update callback.
         * @param recordEvt Whether to record event.
         */
        void onEntryUpdate(GridCacheContinuousQueryEntry<K, V> e, boolean recordEvt) {
            boolean notifyLsnr = true;

            synchronized (this) {
                if (pending != null) {
                    pending.add(new PendingEntry<>(e, recordEvt));

                    notifyLsnr = false;
                }
            }

            if (notifyLsnr)
                lsnr.onEntryUpdate(e, recordEvt, sync);
        }

        /**
         * @param e Entry iteration callback.
         * @param recordEvt Whether to record event.
         */
        void onIterate(GridCacheContinuousQueryEntry<K, V> e, boolean recordEvt) {
            lsnr.onEntryUpdate(e, recordEvt, sync);
        }

        /**
         * Flushes pending entries to listener.
         */
        void flushPending() {
            Collection<PendingEntry<K, V>> pending0;

            synchronized (this) {
                pending0 = pending;

                pending = null;
            }

            for (PendingEntry<K, V> e : pending0)
                lsnr.onEntryUpdate(e.entry, e.recordEvt, sync);
        }

        /**
         * @return {@code True} if listener created for {@link CacheEntryListener}.
         */
        boolean entryListener() {
            return entryLsnr;
        }
    }

    /**
     * Pending entry.
     */
    private static class PendingEntry<K, V> {
        /** Entry. */
        private final GridCacheContinuousQueryEntry<K, V> entry;

        /** Whether to record event. */
        private final boolean recordEvt;

        /**
         * @param entry Entry.
         * @param recordEvt Whether to record event.
         */
        private PendingEntry(GridCacheContinuousQueryEntry<K, V> entry, boolean recordEvt) {
            this.entry = entry;
            this.recordEvt = recordEvt;
        }
    }

    /**
     *
     */
    static class EntryListenerFilter<K1, V1> implements
        IgnitePredicate<org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry<K1, V1>>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private boolean create;

        /** */
        private boolean update;

        /** */
        private boolean rmv;

        /** */
        private boolean expire;

        /** */
        private Factory<CacheEntryEventFilter<? super K1, ? super V1>> fltrFactory;

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
        public EntryListenerFilter() {
            // No-op.
        }

        /**
         * @param create {@code True} if listens for create events.
         * @param update {@code True} if listens for create events.
         * @param rmv {@code True} if listens for remove events.
         * @param expire {@code True} if listens for expire events.
         * @param fltrFactory Filter factory.
         * @param ignite Ignite instance.
         * @param cacheName Cache name.
         */
        EntryListenerFilter(
            boolean create,
            boolean update,
            boolean rmv,
            boolean expire,
            Factory<CacheEntryEventFilter<? super K1, ? super V1>> fltrFactory,
            Ignite ignite,
            @Nullable String cacheName) {
            this.create = create;
            this.update = update;
            this.rmv = rmv;
            this.expire = expire;
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
        @Override public boolean apply(org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry<K1, V1> entry) {
            try {
                EventType evtType;

                if (entry.getValue() == null) {
                    if (((GridCacheContinuousQueryEntry)entry).expired()) { // Expire.
                        if (!expire)
                            return false;

                        evtType = EXPIRED;
                    }
                    else { // Remove.
                        if (!rmv)
                            return false;

                        evtType = REMOVED;
                    }
                }
                else {
                    if (entry.getOldValue() != null) { // Update.
                        if (!update)
                            return false;

                        evtType = UPDATED;
                    }
                    else { // Create.
                        if (!create)
                            return false;

                        evtType = CREATED;
                    }
                }

                if (cache == null) {
                    cache = ignite.jcache(cacheName);

                    assert cache != null : cacheName;
                }

                return fltr == null || fltr.evaluate(new org.apache.ignite.cache.CacheEntryEvent(cache, evtType, entry));
            }
            catch (CacheEntryListenerException e) {
                LT.warn(ignite.log(), e, "Cache entry event filter error: " + e);

                return false;
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(create);

            out.writeBoolean(update);

            out.writeBoolean(rmv);

            out.writeBoolean(expire);

            U.writeString(out, cacheName);

            out.writeObject(fltrFactory);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            create = in.readBoolean();

            update = in.readBoolean();

            rmv = in.readBoolean();

            expire = in.readBoolean();

            cacheName = U.readString(in);

            fltrFactory = (Factory<CacheEntryEventFilter<? super K1, ? super V1>>)in.readObject();

            if (fltrFactory != null)
                fltr = fltrFactory.create();
        }
    }

    /**
     *
     */
    private class EntryListenerCallback implements
        IgniteBiPredicate<UUID, Collection<org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry<K, V>>> {
        /** */
        private final IgniteCacheProxy<K, V> cache;

        /** */
        private final CacheEntryCreatedListener createLsnr;

        /** */
        private final CacheEntryUpdatedListener updateLsnr;

        /** */
        private final CacheEntryRemovedListener rmvLsnr;

        /** */
        private final CacheEntryExpiredListener expireLsnr;

        /**
         * @param cache Cache to be used as event source.
         * @param lsnr Listener.
         */
        EntryListenerCallback(IgniteCacheProxy<K, V> cache, CacheEntryListener lsnr) {
            this.cache = cache;

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
        @Override public boolean apply(UUID uuid, Collection<org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry<K, V>> entries) {
            for (org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry entry : entries) {
                try {
                    if (entry.getValue() == null) { // Remove.
                        if (((GridCacheContinuousQueryEntry)entry).expired()) { // Expire.
                            assert expireLsnr != null;

                            org.apache.ignite.cache.CacheEntryEvent evt0 =
                                new org.apache.ignite.cache.CacheEntryEvent(cache, EXPIRED, entry);

                            expireLsnr.onExpired(Collections.singleton(evt0));
                        }
                        else {
                            assert rmvLsnr != null;

                            org.apache.ignite.cache.CacheEntryEvent evt0 =
                                new org.apache.ignite.cache.CacheEntryEvent(cache, REMOVED, entry);

                            rmvLsnr.onRemoved(Collections.singleton(evt0));
                        }
                    }
                    else if (entry.getOldValue() != null) { // Update.
                        assert updateLsnr != null;

                        org.apache.ignite.cache.CacheEntryEvent evt0 =
                            new org.apache.ignite.cache.CacheEntryEvent(cache, UPDATED, entry);

                        updateLsnr.onUpdated(Collections.singleton(evt0));
                    }
                    else { // Create.
                        assert createLsnr != null;

                        org.apache.ignite.cache.CacheEntryEvent evt0 =
                            new org.apache.ignite.cache.CacheEntryEvent(cache, CREATED, entry);

                        createLsnr.onCreated(Collections.singleton(evt0));
                    }
                }
                catch (CacheEntryListenerException e) {
                    LT.warn(log, e, "Cache entry listener error: " + e);
                }
            }

            return true;
        }
    }
}
