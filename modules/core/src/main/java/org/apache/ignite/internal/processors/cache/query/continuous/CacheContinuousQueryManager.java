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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.LoggerResource;
import org.jsr166.ConcurrentHashMap8;

import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.EXPIRED;
import static javax.cache.event.EventType.REMOVED;
import static javax.cache.event.EventType.UPDATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;

/**
 * Continuous queries manager.
 */
public class CacheContinuousQueryManager extends GridCacheManagerAdapter {
    /** */
    private static final byte CREATED_FLAG = 0b0001;

    /** */
    private static final byte UPDATED_FLAG = 0b0010;

    /** */
    private static final byte REMOVED_FLAG = 0b0100;

    /** */
    private static final byte EXPIRED_FLAG = 0b1000;

    /** Listeners. */
    private final ConcurrentMap<UUID, CacheContinuousQueryListener> lsnrs = new ConcurrentHashMap8<>();

    /** Listeners count. */
    private final AtomicInteger lsnrCnt = new AtomicInteger();

    /** Internal entries listeners. */
    private final ConcurrentMap<UUID, CacheContinuousQueryListener> intLsnrs = new ConcurrentHashMap8<>();

    /** Internal listeners count. */
    private final AtomicInteger intLsnrCnt = new AtomicInteger();

    /** Query sequence number for message topic. */
    private final AtomicLong seq = new AtomicLong();

    /** JCache listeners. */
    private final ConcurrentMap<CacheEntryListenerConfiguration, JCacheQuery> jCacheLsnrs =
        new ConcurrentHashMap8<>();

    /** Ordered topic prefix. */
    private String topicPrefix;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        // Append cache name to the topic.
        topicPrefix = "CONTINUOUS_QUERY" + (cctx.name() == null ? "" : "_" + cctx.name());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        Iterable<CacheEntryListenerConfiguration> cfgs = cctx.config().getCacheEntryListenerConfigurations();

        if (cfgs != null) {
            for (CacheEntryListenerConfiguration cfg : cfgs)
                executeJCacheQuery(cfg, true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        for (JCacheQuery lsnr : jCacheLsnrs.values()) {
            try {
                lsnr.cancel();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to stop JCache entry listener: " + e.getMessage());
            }
        }
    }

    /**
     * @param e Cache entry.
     * @param key Key.
     * @param newVal New value.
     * @param oldVal Old value.
     * @param preload Whether update happened during preloading.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryUpdated(GridCacheEntryEx e,
        KeyCacheObject key,
        CacheObject newVal,
        CacheObject oldVal,
        boolean preload)
        throws IgniteCheckedException
    {
        assert e != null;
        assert key != null;

        boolean internal = e.isInternal() || !e.context().userCache();

        if (preload && !internal)
            return;

        ConcurrentMap<UUID, CacheContinuousQueryListener> lsnrCol;

        if (internal)
            lsnrCol = intLsnrCnt.get() > 0 ? intLsnrs : null;
        else
            lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        if (F.isEmpty(lsnrCol))
            return;

        boolean hasNewVal = newVal != null;
        boolean hasOldVal = oldVal != null;

        if (!hasNewVal && !hasOldVal)
            return;

        EventType evtType = !hasNewVal ? REMOVED : !hasOldVal ? CREATED : UPDATED;

        boolean initialized = false;

        boolean primary = cctx.affinity().primary(cctx.localNode(), key, AffinityTopologyVersion.NONE);
        boolean recordIgniteEvt = !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

        for (CacheContinuousQueryListener lsnr : lsnrCol.values()) {
            if (preload && !lsnr.notifyExisting())
                continue;

            if (!initialized) {
                if (lsnr.oldValueRequired()) {
                    oldVal = (CacheObject)cctx.unwrapTemporary(oldVal);

                    if (oldVal != null)
                        oldVal.finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());
                }

                if (newVal != null)
                    newVal.finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

                initialized = true;
            }

            CacheContinuousQueryEntry e0 = new CacheContinuousQueryEntry(
                cctx.cacheId(),
                evtType,
                key,
                newVal,
                lsnr.oldValueRequired() ? oldVal : null);

            CacheContinuousQueryEvent evt = new CacheContinuousQueryEvent<>(
                cctx.kernalContext().cache().jcache(cctx.name()), cctx, e0);

            lsnr.onEntryUpdated(evt, primary, recordIgniteEvt);
        }
    }

    /**
     * @param e Entry.
     * @param key Key.
     * @param oldVal Old value.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryExpired(GridCacheEntryEx e, KeyCacheObject key, CacheObject oldVal)
        throws IgniteCheckedException {
        assert e != null;
        assert key != null;

        if (e.isInternal())
            return;

        ConcurrentMap<UUID, CacheContinuousQueryListener> lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        if (F.isEmpty(lsnrCol))
            return;

        if (cctx.isReplicated() || cctx.affinity().primary(cctx.localNode(), key, AffinityTopologyVersion.NONE)) {
            boolean primary = cctx.affinity().primary(cctx.localNode(), key, AffinityTopologyVersion.NONE);
            boolean recordIgniteEvt = cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            boolean initialized = false;

            for (CacheContinuousQueryListener lsnr : lsnrCol.values()) {
                if (!initialized) {
                    if (lsnr.oldValueRequired())
                        oldVal = (CacheObject)cctx.unwrapTemporary(oldVal);

                    if (oldVal != null)
                        oldVal.finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

                    initialized = true;
                }

               CacheContinuousQueryEntry e0 = new CacheContinuousQueryEntry(
                   cctx.cacheId(),
                   EXPIRED,
                   key,
                   null,
                   lsnr.oldValueRequired() ? oldVal : null);

                CacheContinuousQueryEvent evt = new CacheContinuousQueryEvent(
                    cctx.kernalContext().cache().jcache(cctx.name()), cctx, e0);

                lsnr.onEntryUpdated(evt, primary, recordIgniteEvt);
            }
        }
    }

    /**
     * @param locLsnr Local listener.
     * @param rmtFilter Remote filter.
     * @param bufSize Buffer size.
     * @param timeInterval Time interval.
     * @param autoUnsubscribe Auto unsubscribe flag.
     * @param grp Cluster group.
     * @return Continuous routine ID.
     * @throws IgniteCheckedException In case of error.
     */
    public UUID executeQuery(CacheEntryUpdatedListener locLsnr,
        CacheEntryEventSerializableFilter rmtFilter,
        int bufSize,
        long timeInterval,
        boolean autoUnsubscribe,
        ClusterGroup grp) throws IgniteCheckedException
    {
        return executeQuery0(
            locLsnr,
            rmtFilter,
            bufSize,
            timeInterval,
            autoUnsubscribe,
            false,
            false,
            true,
            false,
            true,
            grp);
    }

    /**
     * @param locLsnr Local listener.
     * @param rmtFilter Remote filter.
     * @param loc Local flag.
     * @param notifyExisting Notify existing flag.
     * @return Continuous routine ID.
     * @throws IgniteCheckedException In case of error.
     */
    public UUID executeInternalQuery(CacheEntryUpdatedListener<?, ?> locLsnr,
        CacheEntryEventSerializableFilter rmtFilter,
        boolean loc,
        boolean notifyExisting)
        throws IgniteCheckedException
    {
        return executeQuery0(
            locLsnr,
            rmtFilter,
            ContinuousQuery.DFLT_PAGE_SIZE,
            ContinuousQuery.DFLT_TIME_INTERVAL,
            ContinuousQuery.DFLT_AUTO_UNSUBSCRIBE,
            true,
            notifyExisting,
            true,
            false,
            true,
            loc ? cctx.grid().cluster().forLocal() : null);
    }

    /**
     * @param routineId Consume ID.
     */
    public void cancelInternalQuery(UUID routineId) {
        try {
            cctx.kernalContext().continuous().stopRoutine(routineId).get();
        }
        catch (IgniteCheckedException | IgniteException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to stop internal continuous query: " + e.getMessage());
        }
    }

    /**
     * @param cfg Listener configuration.
     * @param onStart Whether listener is created on node start.
     * @throws IgniteCheckedException If failed.
     */
    public void executeJCacheQuery(CacheEntryListenerConfiguration cfg, boolean onStart)
        throws IgniteCheckedException {
        JCacheQuery lsnr = new JCacheQuery(cfg, onStart);

        JCacheQuery old = jCacheLsnrs.putIfAbsent(cfg, lsnr);

        if (old != null)
            throw new IllegalArgumentException("Listener is already registered for configuration: " + cfg);

        try {
            lsnr.execute();
        }
        catch (IgniteCheckedException e) {
            cancelJCacheQuery(cfg);

            throw e;
        }
    }

    /**
     * @param cfg Listener configuration.
     * @throws IgniteCheckedException In case of error.
     */
    public void cancelJCacheQuery(CacheEntryListenerConfiguration cfg) throws IgniteCheckedException {
        JCacheQuery lsnr = jCacheLsnrs.remove(cfg);

        if (lsnr != null)
            lsnr.cancel();
    }

    /**
     * @param locLsnr Local listener.
     * @param rmtFilter Remote filter.
     * @param bufSize Buffer size.
     * @param timeInterval Time interval.
     * @param autoUnsubscribe Auto unsubscribe flag.
     * @param internal Internal flag.
     * @param notifyExisting Notify existing flag.
     * @param oldValRequired Old value required flag.
     * @param sync Synchronous flag.
     * @param ignoreExpired Ignore expired event flag.
     * @param grp Cluster group.
     * @return Continuous routine ID.
     * @throws IgniteCheckedException In case of error.
     */
    private UUID executeQuery0(CacheEntryUpdatedListener locLsnr,
        final CacheEntryEventSerializableFilter rmtFilter,
        int bufSize,
        long timeInterval,
        boolean autoUnsubscribe,
        boolean internal,
        boolean notifyExisting,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        ClusterGroup grp) throws IgniteCheckedException
    {
        cctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (grp == null)
            grp = cctx.kernalContext().grid().cluster();

        Collection<ClusterNode> nodes = grp.nodes();

        if (nodes.isEmpty())
            throw new ClusterTopologyException("Failed to execute continuous query (empty cluster group is " +
                "provided).");

        boolean skipPrimaryCheck = false;

        switch (cctx.config().getCacheMode()) {
            case LOCAL:
                if (!nodes.contains(cctx.localNode()))
                    throw new ClusterTopologyException("Continuous query for LOCAL cache can be executed " +
                        "only locally (provided projection contains remote nodes only).");
                else if (nodes.size() > 1)
                    U.warn(log, "Continuous query for LOCAL cache will be executed locally (provided projection is " +
                        "ignored).");

                grp = grp.forNode(cctx.localNode());

                break;

            case REPLICATED:
                if (nodes.size() == 1 && F.first(nodes).equals(cctx.localNode()))
                    skipPrimaryCheck = cctx.affinityNode();

                break;
        }

        int taskNameHash = !internal && cctx.kernalContext().security().enabled() ?
            cctx.kernalContext().job().currentTaskNameHash() : 0;

        GridContinuousHandler hnd = new CacheContinuousQueryHandler(
            cctx.name(),
            TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
            locLsnr,
            rmtFilter,
            internal,
            notifyExisting,
            oldValRequired,
            sync,
            ignoreExpired,
            taskNameHash,
            skipPrimaryCheck);

        UUID id = cctx.kernalContext().continuous().startRoutine(hnd, bufSize, timeInterval,
            autoUnsubscribe, grp.predicate()).get();

        if (notifyExisting) {
            final Iterator<GridCacheEntryEx> it = cctx.cache().allEntries().iterator();

            locLsnr.onUpdated(new Iterable<CacheEntryEvent>() {
                @Override public Iterator<CacheEntryEvent> iterator() {
                    return new Iterator<CacheEntryEvent>() {
                        private CacheContinuousQueryEvent next;

                        {
                            advance();
                        }

                        @Override public boolean hasNext() {
                            return next != null;
                        }

                        @Override public CacheEntryEvent next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            CacheEntryEvent next0 = next;

                            advance();

                            return next0;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }

                        private void advance() {
                            next = null;

                            while (next == null) {
                                if (!it.hasNext())
                                    break;

                                GridCacheEntryEx e = it.next();

                                next = new CacheContinuousQueryEvent<>(
                                    cctx.kernalContext().cache().jcache(cctx.name()),
                                    cctx,
                                    new CacheContinuousQueryEntry(cctx.cacheId(), CREATED, e.key(), e.rawGet(), null));

                                if (rmtFilter != null && !rmtFilter.evaluate(next))
                                    next = null;
                            }
                        }
                    };
                }
            });
        }

        return id;
    }

    /**
     * @param lsnrId Listener ID.
     * @param lsnr Listener.
     * @param internal Internal flag.
     * @return Whether listener was actually registered.
     */
    GridContinuousHandler.RegisterStatus registerListener(UUID lsnrId,
        CacheContinuousQueryListener lsnr,
        boolean internal) {
        boolean added;

        if (internal) {
            added = intLsnrs.putIfAbsent(lsnrId, lsnr) == null;

            if (added)
                intLsnrCnt.incrementAndGet();
        }
        else {
            added = lsnrs.putIfAbsent(lsnrId, lsnr) == null;

            if (added) {
                lsnrCnt.incrementAndGet();

                lsnr.onExecution();
            }
        }

        return added ? GridContinuousHandler.RegisterStatus.REGISTERED : GridContinuousHandler.RegisterStatus.NOT_REGISTERED;
    }

    /**
     * @param internal Internal flag.
     * @param id Listener ID.
     */
    void unregisterListener(boolean internal, UUID id) {
        CacheContinuousQueryListener lsnr;

        if (internal) {
            if ((lsnr = intLsnrs.remove(id)) != null) {
                intLsnrCnt.decrementAndGet();

                lsnr.onUnregister();
            }
        }
        else {
            if ((lsnr = lsnrs.remove(id)) != null) {
                lsnrCnt.decrementAndGet();

                lsnr.onUnregister();
            }
        }
    }

    /**
     */
    private class JCacheQuery {
        /** */
        private final CacheEntryListenerConfiguration cfg;

        /** */
        private final boolean onStart;

        /** */
        private volatile UUID routineId;

        /**
         * @param cfg Listener configuration.
         * @param onStart {@code True} if executed on cache start.
         */
        private JCacheQuery(CacheEntryListenerConfiguration cfg, boolean onStart) {
            this.cfg = cfg;
            this.onStart = onStart;
        }

        /**
         * @throws IgniteCheckedException In case of error.
         */
        @SuppressWarnings("unchecked")
        void execute() throws IgniteCheckedException {
            if (!onStart)
                cctx.config().addCacheEntryListenerConfiguration(cfg);

            CacheEntryListener locLsnrImpl = (CacheEntryListener)cfg.getCacheEntryListenerFactory().create();

            if (locLsnrImpl == null)
                throw new IgniteCheckedException("Local CacheEntryListener is mandatory and can't be null.");

            byte types = 0;

            types |= locLsnrImpl instanceof CacheEntryCreatedListener ? CREATED_FLAG : 0;
            types |= locLsnrImpl instanceof CacheEntryUpdatedListener ? UPDATED_FLAG : 0;
            types |= locLsnrImpl instanceof CacheEntryRemovedListener ? REMOVED_FLAG : 0;
            types |= locLsnrImpl instanceof CacheEntryExpiredListener ? EXPIRED_FLAG : 0;

            if (types == 0)
                throw new IgniteCheckedException("Listener must implement one of CacheEntryListener sub-interfaces.");

            CacheEntryUpdatedListener locLsnr = new JCacheQueryLocalListener(
                locLsnrImpl,
                log);

            CacheEntryEventFilter fltr = null;

            if (cfg.getCacheEntryEventFilterFactory() != null) {
                fltr = (CacheEntryEventFilter) cfg.getCacheEntryEventFilterFactory().create();

                if (!(fltr instanceof Serializable))
                    throw new IgniteCheckedException("Cache entry event filter must implement java.io.Serializable: " + fltr);
            }

            CacheEntryEventSerializableFilter rmtFilter = new JCacheQueryRemoteFilter(fltr, types);

            routineId = executeQuery0(
                locLsnr,
                rmtFilter,
                ContinuousQuery.DFLT_PAGE_SIZE,
                ContinuousQuery.DFLT_TIME_INTERVAL,
                ContinuousQuery.DFLT_AUTO_UNSUBSCRIBE,
                false,
                false,
                cfg.isOldValueRequired(),
                cfg.isSynchronous(),
                false,
                null);
        }

        /**
         * @throws IgniteCheckedException In case of error.
         */
        @SuppressWarnings("unchecked")
        void cancel() throws IgniteCheckedException {
            UUID routineId0 = routineId;

            if (routineId0 != null)
                cctx.kernalContext().continuous().stopRoutine(routineId0).get();

            cctx.config().removeCacheEntryListenerConfiguration(cfg);
        }
    }

    /**
     */
    private static class JCacheQueryLocalListener<K, V> implements CacheEntryUpdatedListener<K, V> {
        /** */
        private final CacheEntryListener<K, V> impl;

        /** */
        private final IgniteLogger log;

        /**
         * @param impl Listener.
         */
        JCacheQueryLocalListener(CacheEntryListener<K, V> impl, IgniteLogger log) {
            assert impl != null;
            assert log != null;

            this.impl = impl;

            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
            for (CacheEntryEvent<? extends K, ? extends V> evt : evts) {
                try {
                    switch (evt.getEventType()) {
                        case CREATED:
                            assert impl instanceof CacheEntryCreatedListener;

                            ((CacheEntryCreatedListener<K, V>)impl).onCreated(singleton(evt));

                            break;

                        case UPDATED:
                            assert impl instanceof CacheEntryUpdatedListener;

                            ((CacheEntryUpdatedListener<K, V>)impl).onUpdated(singleton(evt));

                            break;

                        case REMOVED:
                            assert impl instanceof CacheEntryRemovedListener;

                            ((CacheEntryRemovedListener<K, V>)impl).onRemoved(singleton(evt));

                            break;

                        case EXPIRED:
                            assert impl instanceof CacheEntryExpiredListener;

                            ((CacheEntryExpiredListener<K, V>)impl).onExpired(singleton(evt));

                            break;

                        default:
                            throw new IllegalStateException("Unknown type: " + evt.getEventType());
                    }
                }
                catch (Exception e) {
                    U.error(log, "CacheEntryListener failed: " + e);
                }
            }
        }

        /**
         * @param evt Event.
         * @return Singleton iterable.
         */
        @SuppressWarnings("unchecked")
        private Iterable<CacheEntryEvent<? extends K, ? extends V>> singleton(
            CacheEntryEvent<? extends K, ? extends V> evt) {
            assert evt instanceof CacheContinuousQueryEvent;

            Collection<CacheEntryEvent<? extends K, ? extends V>> evts = new ArrayList<>(1);

            evts.add(evt);

            return evts;
        }
    }

    /**
     */
    private static class JCacheQueryRemoteFilter implements CacheEntryEventSerializableFilter, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private CacheEntryEventFilter impl;

        /** */
        private byte types;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * For {@link Externalizable}.
         */
        public JCacheQueryRemoteFilter() {
            // no-op.
        }

        /**
         * @param impl Filter.
         * @param types Types.
         */
        JCacheQueryRemoteFilter(CacheEntryEventFilter impl, byte types) {
            assert types != 0;

            this.impl = impl;
            this.types = types;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent evt) {
            try {
                return (types & flag(evt.getEventType())) != 0 && (impl == null || impl.evaluate(evt));
            }
            catch (Exception e) {
                U.error(log, "CacheEntryEventFilter failed: " + e);

                return true;
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(impl);
            out.writeByte(types);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            impl = (CacheEntryEventFilter)in.readObject();
            types = in.readByte();
        }

        /**
         * @param evtType Type.
         * @return Flag value.
         */
        private byte flag(EventType evtType) {
            switch (evtType) {
                case CREATED:
                    return CREATED_FLAG;

                case UPDATED:
                    return UPDATED_FLAG;

                case REMOVED:
                    return REMOVED_FLAG;

                case EXPIRED:
                    return EXPIRED_FLAG;

                default:
                    throw new IllegalStateException("Unknown type: " + evtType);
            }
        }
    }
}