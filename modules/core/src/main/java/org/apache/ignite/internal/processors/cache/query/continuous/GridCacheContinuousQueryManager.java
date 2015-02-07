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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.processors.cache.CacheEntryEvent;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.Cache.*;
import javax.cache.configuration.*;
import javax.cache.event.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static javax.cache.event.EventType.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;

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
    private final ConcurrentMap<CacheEntryListenerConfiguration, CacheContinuousQuery<K, V>> lsnrQrys =
        new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        // Append cache name to the topic.
        topicPrefix = "CONTINUOUS_QUERY" + (cctx.name() == null ? "" : "_" + cctx.name());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        Iterable<CacheEntryListenerConfiguration<K, V>> lsnrCfgs = cctx.config().getCacheEntryListenerConfigurations();

        if (lsnrCfgs != null) {
            for (CacheEntryListenerConfiguration<K, V> cfg : lsnrCfgs)
                registerCacheEntryListener(cfg, false);
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
    public CacheContinuousQuery<K, V> createQuery(@Nullable IgnitePredicate<Entry<K, V>> prjPred) {
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

        EventType evtType = newVal == null ? REMOVED :
            ((oldVal != null || (oldBytes != null && !oldBytes.isNull()) ? UPDATED : CREATED));

        GridCacheContinuousQueryEntry<K, V> e0 = new GridCacheContinuousQueryEntry<>(
            cctx,
            e.wrap(),
            key,
            newVal,
            newBytes,
            oldVal,
            oldBytes,
            evtType);

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
                e.wrap(),
                key,
                null,
                null,
                oldVal,
                oldBytes,
                EXPIRED);

            for (ListenerInfo<K, V> lsnr : lsnrCol.values()) {
                if (!lsnr.entryListener())
                    continue;

                lsnr.onEntryUpdate(e0, false);
            }
        }
    }

    /**
     * @param lsnrCfg Listener configuration.
     * @param addToCfg If {@code true} adds listener configuration to cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg, boolean addToCfg)
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

            CacheContinuousQuery<K, V> old = lsnrQrys.putIfAbsent(lsnrCfg, qry);

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

            qry.execute(null, false, true, lsnrCfg.isSynchronous(), lsnrCfg.isOldValueRequired());

            if (addToCfg)
                cctx.config().addCacheEntryListenerConfiguration(lsnrCfg);
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
    @SuppressWarnings("unchecked")
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration lsnrCfg) throws IgniteCheckedException {
        A.notNull(lsnrCfg, "lsnrCfg");

        CacheContinuousQuery<K, V> qry = lsnrQrys.remove(lsnrCfg);

        if (qry != null) {
            cctx.config().removeCacheEntryListenerConfiguration(lsnrCfg);

            qry.close();
        }
    }

    /**
     * @param lsnrId Listener ID.
     * @param lsnr Listener.
     * @param internal Internal flag.
     * @param entryLsnr {@code True} if query created for {@link CacheEntryListener}.
     * @return Whether listener was actually registered.
     */
    @SuppressWarnings("UnusedParameters")
    boolean registerListener(UUID lsnrId,
        GridCacheContinuousQueryListener<K, V> lsnr,
        boolean internal,
        boolean entryLsnr) {
        ListenerInfo<K, V> info = new ListenerInfo<>(lsnr, entryLsnr);

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
        ListenerInfo info;

        if (internal) {
            if ((info = intLsnrs.remove(id)) != null) {
                intLsnrCnt.decrementAndGet();

                info.lsnr.onUnregister();
            }
        }
        else {
            if ((info = lsnrs.remove(id)) != null) {
                lsnrCnt.decrementAndGet();

                info.lsnr.onUnregister();
            }
        }
    }

    /**
     * Iterates through existing data.
     *
     * @param internal Internal flag.
     * @param id Listener ID.
     * @param keepPortable Keep portable flag.
     */
    @SuppressWarnings("unchecked")
    void iterate(boolean internal, UUID id, boolean keepPortable) {
        ListenerInfo<K, V> info = internal ? intLsnrs.get(id) : lsnrs.get(id);

        assert info != null;

        GridCacheProjectionImpl<K, V> oldPrj = null;

        try {
            if (keepPortable) {
                oldPrj = cctx.projectionPerCall();

                cctx.projectionPerCall(cctx.cache().<K, V>keepPortable0());
            }

            Set<Entry<K, V>> entries;

            if (cctx.isReplicated())
                entries = internal ? cctx.cache().entrySetx() :
                    cctx.cache().entrySet();
            else
                entries = internal ? cctx.cache().primaryEntrySetx() :
                    cctx.cache().primaryEntrySet();

            boolean evt = !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            for (Entry<K, V> e : entries) {
                GridCacheContinuousQueryEntry<K, V> qryEntry = new GridCacheContinuousQueryEntry<>(cctx,
                    e,
                    e.getKey(),
                    e.getValue(),
                    null,
                    null,
                    null,
                    CREATED);

                info.onIterate(qryEntry, evt);
            }

            info.flushPending();
        }
        finally {
            if (keepPortable)
                cctx.projectionPerCall(oldPrj);
        }
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

        /**
         * @param lsnr Listener.
         * @param entryLsnr {@code True} if listener created for {@link CacheEntryListener}.
         */
        private ListenerInfo(GridCacheContinuousQueryListener<K, V> lsnr, boolean entryLsnr) {
            this.lsnr = lsnr;
            this.entryLsnr = entryLsnr;

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
                lsnr.onEntryUpdate(e, recordEvt);
        }

        /**
         * @param e Entry iteration callback.
         * @param recordEvt Whether to record event.
         */
        void onIterate(GridCacheContinuousQueryEntry<K, V> e, boolean recordEvt) {
            lsnr.onEntryUpdate(e, recordEvt);
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
                lsnr.onEntryUpdate(e.entry, e.recordEvt);
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
        IgnitePredicate<CacheContinuousQueryEntry<K1, V1>>, Externalizable {
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
        @Override public boolean apply(CacheContinuousQueryEntry<K1, V1> entry) {
            try {
                EventType evtType = (((GridCacheContinuousQueryEntry)entry).eventType());

                switch (evtType) {
                    case EXPIRED:
                        if (!expire)
                            return false;

                        break;

                    case REMOVED:
                        if (!rmv)
                            return false;

                        break;

                    case CREATED:
                        if (!create)
                            return false;

                        break;

                    case UPDATED:
                        if (!update)
                            return false;

                        break;

                    default:
                        assert false : evtType;
                }

                if (fltr == null)
                    return true;

                if (cache == null) {
                    cache = ignite.jcache(cacheName);

                    assert cache != null : cacheName;
                }

                return fltr.evaluate(new CacheEntryEvent(cache, evtType, entry));
            }
            catch (Exception e) {
                LT.warn(ignite.log(), e, "Cache entry event filter error: " + e);

                return true;
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
        IgniteBiPredicate<UUID, Collection<CacheContinuousQueryEntry<K, V>>> {
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
        @Override public boolean apply(UUID uuid,
            Collection<CacheContinuousQueryEntry<K, V>> entries) {
            for (CacheContinuousQueryEntry entry : entries) {
                try {
                    EventType evtType = (((GridCacheContinuousQueryEntry)entry).eventType());

                    switch (evtType) {
                        case EXPIRED: {
                            assert expireLsnr != null;

                            CacheEntryEvent evt0 =
                                new CacheEntryEvent(cache, EXPIRED, entry);

                            expireLsnr.onExpired(Collections.singleton(evt0));

                            break;
                        }

                        case REMOVED: {
                            assert rmvLsnr != null;

                            CacheEntryEvent evt0 =
                                new CacheEntryEvent(cache, REMOVED, entry);

                            rmvLsnr.onRemoved(Collections.singleton(evt0));

                            break;
                        }

                        case UPDATED: {
                            assert updateLsnr != null;

                            CacheEntryEvent evt0 =
                                new CacheEntryEvent(cache, UPDATED, entry);

                            updateLsnr.onUpdated(Collections.singleton(evt0));

                            break;
                        }

                        case CREATED: {
                            assert createLsnr != null;

                            CacheEntryEvent evt0 =
                                new CacheEntryEvent(cache, CREATED, entry);

                            createLsnr.onCreated(Collections.singleton(evt0));

                            break;
                        }

                        default:
                            assert false : evtType;
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
