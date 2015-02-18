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
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.resources.*;
import org.jdk8.backport.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.event.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static javax.cache.event.EventType.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;

/**
 * Continuous queries manager.
 */
public class CacheContinuousQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** */
    private static final byte CREATED_FLAG = 0b0001;

    /** */
    private static final byte UPDATED_FLAG = 0b0010;

    /** */
    private static final byte REMOVED_FLAG = 0b0100;

    /** */
    private static final byte EXPIRED_FLAG = 0b1000;

    /** Listeners. */
    private final ConcurrentMap<UUID, CacheContinuousQueryListener<K, V>> lsnrs = new ConcurrentHashMap8<>();

    /** Listeners count. */
    private final AtomicInteger lsnrCnt = new AtomicInteger();

    /** Internal entries listeners. */
    private final ConcurrentMap<UUID, CacheContinuousQueryListener<K, V>> intLsnrs = new ConcurrentHashMap8<>();

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
        Iterable<CacheEntryListenerConfiguration<K, V>> cfgs = cctx.config().getCacheEntryListenerConfigurations();

        if (cfgs != null) {
            for (CacheEntryListenerConfiguration<K, V> cfg : cfgs)
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
     * @param newBytes New value bytes.
     * @param oldVal Old value.
     * @param oldBytes Old value bytes.
     * @param preload Whether update happened during preloading.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryUpdated(GridCacheEntryEx<K, V> e, K key, V newVal, GridCacheValueBytes newBytes,
        V oldVal, GridCacheValueBytes oldBytes, boolean preload) throws IgniteCheckedException {
        assert e != null;
        assert key != null;

        boolean internal = e.isInternal();

        if (preload && !internal)
            return;

        ConcurrentMap<UUID, CacheContinuousQueryListener<K, V>> lsnrCol;

        if (internal)
            lsnrCol = intLsnrCnt.get() > 0 ? intLsnrs : null;
        else
            lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        if (F.isEmpty(lsnrCol))
            return;

        boolean hasNewVal = newVal != null || (newBytes != null && !newBytes.isNull());
        boolean hasOldVal = oldVal != null || (oldBytes != null && !oldBytes.isNull());

        if (!hasNewVal && !hasOldVal)
            return;

        EventType evtType = !hasNewVal ? REMOVED : !hasOldVal ? CREATED : UPDATED;

        boolean initialized = false;

        boolean primary = cctx.affinity().primary(cctx.localNode(), key, -1);
        boolean recordIgniteEvt = !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

        for (CacheContinuousQueryListener<K, V> lsnr : lsnrCol.values()) {
            if (preload && !lsnr.notifyExisting())
                continue;

            if (!initialized) {
                if (lsnr.oldValueRequired()) {
                    oldVal = cctx.unwrapTemporary(oldVal);

                    if (oldVal == null && oldBytes != null && !oldBytes.isNull())
                        oldVal = oldBytes.isPlain() ? (V)oldBytes.get() : cctx.marshaller().<V>unmarshal(oldBytes.get
                            (), cctx.deploy().globalLoader());
                }

                if (newVal == null && newBytes != null && !newBytes.isNull())
                    newVal = newBytes.isPlain() ? (V)newBytes.get() : cctx.marshaller().<V>unmarshal(newBytes.get(),
                        cctx.deploy().globalLoader());
            }

            CacheContinuousQueryEntry<K, V> e0 = new CacheContinuousQueryEntry<>(key, newVal, newBytes,
                lsnr.oldValueRequired() ? oldVal : null, lsnr.oldValueRequired() ? oldBytes : null);

            CacheContinuousQueryEvent<K, V> evt = new CacheContinuousQueryEvent<>(
                cctx.kernalContext().cache().jcache(cctx.name()), evtType, e0);

            lsnr.onEntryUpdated(evt, primary, recordIgniteEvt);
        }
    }

    /**
     * @param e Entry.
     * @param key Key.
     * @param oldVal Old value.
     * @param oldBytes Old value bytes.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryExpired(GridCacheEntryEx<K, V> e, K key, V oldVal, GridCacheValueBytes oldBytes)
        throws IgniteCheckedException {
        assert e != null;
        assert key != null;

        if (e.isInternal())
            return;

        ConcurrentMap<UUID, CacheContinuousQueryListener<K, V>> lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        if (F.isEmpty(lsnrCol))
            return;

        if (cctx.isReplicated() || cctx.affinity().primary(cctx.localNode(), key, -1)) {
            boolean primary = cctx.affinity().primary(cctx.localNode(), key, -1);
            boolean recordIgniteEvt = cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            boolean initialized = false;

            for (CacheContinuousQueryListener<K, V> lsnr : lsnrCol.values()) {
                if (!initialized) {
                    if (lsnr.oldValueRequired()) {
                        oldVal = cctx.unwrapTemporary(oldVal);

                        if (oldVal == null && oldBytes != null && !oldBytes.isNull())
                            oldVal = oldBytes.isPlain() ? (V)oldBytes.get() :
                                cctx.marshaller().<V>unmarshal(oldBytes.get(), cctx.deploy().globalLoader());
                    }
                }

                CacheContinuousQueryEntry<K, V> e0 = new CacheContinuousQueryEntry<>(key, null, null,
                    lsnr.oldValueRequired() ? oldVal : null, lsnr.oldValueRequired() ? oldBytes : null);

                CacheContinuousQueryEvent<K, V> evt = new CacheContinuousQueryEvent<>(
                    cctx.kernalContext().cache().jcache(cctx.name()), EXPIRED, e0);

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
    public UUID executeQuery(CacheEntryUpdatedListener<K, V> locLsnr, CacheEntryEventFilter<K, V> rmtFilter,
        int bufSize, long timeInterval, boolean autoUnsubscribe, ClusterGroup grp) throws IgniteCheckedException {
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
    public UUID executeInternalQuery(CacheEntryUpdatedListener<K, V> locLsnr, CacheEntryEventFilter<K, V> rmtFilter,
        boolean loc, boolean notifyExisting) throws IgniteCheckedException {
        return executeQuery0(
            locLsnr,
            rmtFilter,
            ContinuousQuery.DFLT_BUF_SIZE,
            ContinuousQuery.DFLT_TIME_INTERVAL,
            ContinuousQuery.DFLT_AUTO_UNSUBSCRIBE,
            true,
            notifyExisting,
            true,
            false,
            true,
            loc ? cctx.grid().forLocal() : null);
    }

    public void cancelInternalQuery(UUID routineId) {
        try {
            cctx.kernalContext().continuous().stopRoutine(routineId).get();
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to stop internal continuous query: " + e.getMessage());
        }
    }

    /**
     * @param cfg Listener configuration.
     * @param onStart Whether listener is created on node start.
     * @throws IgniteCheckedException
     */
    public void executeJCacheQuery(CacheEntryListenerConfiguration<K, V> cfg, boolean onStart)
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
    public void cancelJCacheQuery(CacheEntryListenerConfiguration<K, V> cfg) throws IgniteCheckedException {
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
    private UUID executeQuery0(CacheEntryUpdatedListener<K, V> locLsnr, final CacheEntryEventFilter<K, V> rmtFilter,
        int bufSize, long timeInterval, boolean autoUnsubscribe, boolean internal, boolean notifyExisting,
        boolean oldValRequired, boolean sync, boolean ignoreExpired, ClusterGroup grp) throws IgniteCheckedException {
        cctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (grp == null)
            grp = cctx.kernalContext().grid();

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
                if (nodes.size() == 1 && F.first(nodes).equals(cctx.localNode())) {
                    CacheDistributionMode distributionMode = cctx.config().getDistributionMode();

                    if (distributionMode == PARTITIONED_ONLY || distributionMode == NEAR_PARTITIONED)
                        skipPrimaryCheck = true;
                }

                break;
        }

        int taskNameHash = !internal && cctx.kernalContext().security().enabled() ?
            cctx.kernalContext().job().currentTaskNameHash() : 0;

        GridContinuousHandler hnd = new CacheContinuousQueryHandler<>(
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
            final Iterator<Cache.Entry<K, V>> it = cctx.cache().entrySetx().iterator();

            locLsnr.onUpdated(new Iterable<CacheEntryEvent<? extends K, ? extends V>>() {
                @Override public Iterator<CacheEntryEvent<? extends K, ? extends V>> iterator() {
                    return new Iterator<CacheEntryEvent<? extends K, ? extends V>>() {
                        private CacheContinuousQueryEvent<? extends K, ? extends V> next;

                        {
                            advance();
                        }

                        @Override public boolean hasNext() {
                            return next != null;
                        }

                        @Override public CacheEntryEvent<? extends K, ? extends V> next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            CacheEntryEvent<? extends K, ? extends V> next0 = next;

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

                                Cache.Entry<K, V> e = it.next();

                                next = new CacheContinuousQueryEvent<>(
                                    cctx.kernalContext().cache().jcache(cctx.name()), CREATED,
                                    new CacheContinuousQueryEntry<>(e.getKey(), e.getValue(), null, null, null));

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
    boolean registerListener(UUID lsnrId,
        CacheContinuousQueryListener<K, V> lsnr,
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

        return added;
    }

    /**
     * @param internal Internal flag.
     * @param id Listener ID.
     */
    void unregisterListener(boolean internal, UUID id) {
        CacheContinuousQueryListener<K, V> lsnr;

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
        private final CacheEntryListenerConfiguration<K, V> cfg;

        /** */
        private final boolean onStart;

        /** */
        private volatile UUID routineId;

        /**
         * @param cfg Listener configuration.
         */
        private JCacheQuery(CacheEntryListenerConfiguration<K, V> cfg, boolean onStart) {
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

            CacheEntryListener<? super K, ? super V> locLsnrImpl = cfg.getCacheEntryListenerFactory().create();

            if (locLsnrImpl == null)
                throw new IgniteCheckedException("Local CacheEntryListener is mandatory and can't be null.");

            byte types = 0;

            types |= locLsnrImpl instanceof CacheEntryCreatedListener ? CREATED_FLAG : 0;
            types |= locLsnrImpl instanceof CacheEntryUpdatedListener ? UPDATED_FLAG : 0;
            types |= locLsnrImpl instanceof CacheEntryRemovedListener ? REMOVED_FLAG : 0;
            types |= locLsnrImpl instanceof CacheEntryExpiredListener ? EXPIRED_FLAG : 0;

            if (types == 0)
                throw new IgniteCheckedException("Listener must implement one of CacheEntryListener sub-interfaces.");

            CacheEntryUpdatedListener<K, V> locLsnr = (CacheEntryUpdatedListener<K, V>)new JCacheQueryLocalListener(
                locLsnrImpl, cctx.kernalContext().cache().jcache(cctx.name()));

            CacheEntryEventFilter<K, V> rmtFilter = (CacheEntryEventFilter<K, V>)new JCacheQueryRemoteFilter<>(
                cfg.getCacheEntryEventFilterFactory() != null ? cfg.getCacheEntryEventFilterFactory().create() : null,
                types);

            routineId = executeQuery0(
                locLsnr,
                rmtFilter,
                ContinuousQuery.DFLT_BUF_SIZE,
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

            assert routineId0 != null;

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
        private final Cache<K, V> cache;

        /** */
        private final IgniteLogger log;

        /**
         * @param impl Listener.
         * @param cache Cache.
         */
        JCacheQueryLocalListener(CacheEntryListener<K, V> impl, Cache<K, V> cache) {
            assert impl != null;
            assert cache != null;

            this.impl = impl;
            this.cache = cache;

            log = cache.unwrap(Ignite.class).log().getLogger(CacheContinuousQueryManager.class);
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

            evts.add(new CacheContinuousQueryEvent<>(cache, evt.getEventType(),
                ((CacheContinuousQueryEvent<? extends K, ? extends V>)evt).entry()));

            return evts;
        }
    }

    /**
     */
    private static class JCacheQueryRemoteFilter<K, V> implements CacheEntryEventFilter<K, V>, Externalizable {
        /** */
        private CacheEntryEventFilter<K, V> impl;

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
        JCacheQueryRemoteFilter(CacheEntryEventFilter<K, V> impl, byte types) {
            assert types != 0;

            this.impl = impl;
            this.types = types;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> evt) {
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
            impl = (CacheEntryEventFilter<K, V>)in.readObject();
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
