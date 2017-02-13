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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.EXPIRED;
import static javax.cache.event.EventType.REMOVED;
import static javax.cache.event.EventType.UPDATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.QUERY_MSG_VER_2_SINCE;

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

    /** */
    private static final long BACKUP_ACK_FREQ = 5000;

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

        if (cctx.affinityNode()) {
            cctx.io().addHandler(cctx.cacheId(), CacheContinuousQueryBatchAck.class,
                new CI2<UUID, CacheContinuousQueryBatchAck>() {
                    @Override public void apply(UUID uuid, CacheContinuousQueryBatchAck msg) {
                        CacheContinuousQueryListener lsnr = lsnrs.get(msg.routineId());

                        if (lsnr != null)
                            lsnr.cleanupBackupQueue(msg.updateCntrs());
                    }
                });

            cctx.time().schedule(new BackupCleaner(lsnrs, cctx.kernalContext()), BACKUP_ACK_FREQ, BACKUP_ACK_FREQ);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        Iterable<CacheEntryListenerConfiguration> cfgs = cctx.config().getCacheEntryListenerConfigurations();

        if (cfgs != null) {
            for (CacheEntryListenerConfiguration cfg : cfgs)
                executeJCacheQuery(cfg, true, false);
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
     * @param lsnrs Listeners to notify.
     * @param key Entry key.
     * @param partId Partition id.
     * @param updCntr Updated counter.
     * @param primary Primary.
     * @param topVer Topology version.
     */
    public void skipUpdateEvent(Map<UUID, CacheContinuousQueryListener> lsnrs,
        KeyCacheObject key,
        int partId,
        long updCntr,
        boolean primary,
        AffinityTopologyVersion topVer) {
        assert lsnrs != null;

        for (CacheContinuousQueryListener lsnr : lsnrs.values()) {
            CacheContinuousQueryEntry e0 = new CacheContinuousQueryEntry(
                cctx.cacheId(),
                UPDATED,
                key,
                null,
                null,
                lsnr.keepBinary(),
                partId,
                updCntr,
                topVer);

            CacheContinuousQueryEvent evt = new CacheContinuousQueryEvent<>(
                cctx.kernalContext().cache().jcache(cctx.name()), cctx, e0);

            lsnr.skipUpdateEvent(evt, topVer, primary);
        }
    }

    /**
     * @param internal Internal entry flag (internal key or not user cache).
     * @param preload Whether update happened during preloading.
     * @return Registered listeners.
     */
    @Nullable public Map<UUID, CacheContinuousQueryListener> updateListeners(
        boolean internal,
        boolean preload) {
        if (preload && !internal)
            return null;

        ConcurrentMap<UUID, CacheContinuousQueryListener> lsnrCol;

        if (internal)
            lsnrCol = intLsnrCnt.get() > 0 ? intLsnrs : null;
        else
            lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        return F.isEmpty(lsnrCol) ? null : lsnrCol;
    }

    /**
     * @param key Key.
     * @param newVal New value.
     * @param oldVal Old value.
     * @param internal Internal entry (internal key or not user cache).
     * @param partId Partition.
     * @param primary {@code True} if called on primary node.
     * @param preload Whether update happened during preloading.
     * @param updateCntr Update counter.
     * @param fut Dht atomic future.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryUpdated(
        KeyCacheObject key,
        CacheObject newVal,
        CacheObject oldVal,
        boolean internal,
        int partId,
        boolean primary,
        boolean preload,
        long updateCntr,
        @Nullable GridDhtAtomicAbstractUpdateFuture fut,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        Map<UUID, CacheContinuousQueryListener> lsnrCol = updateListeners(internal, preload);

        if (lsnrCol != null) {
            onEntryUpdated(
                lsnrCol,
                key,
                newVal,
                oldVal,
                internal,
                partId,
                primary,
                preload,
                updateCntr,
                fut,
                topVer);
        }
    }

    /**
     * @param lsnrCol Listeners to notify.
     * @param key Key.
     * @param newVal New value.
     * @param oldVal Old value.
     * @param internal Internal entry (internal key or not user cache),
     * @param partId Partition.
     * @param primary {@code True} if called on primary node.
     * @param preload Whether update happened during preloading.
     * @param updateCntr Update counter.
     * @param topVer Topology version.
     * @param fut Dht atomic future.
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryUpdated(
        Map<UUID, CacheContinuousQueryListener> lsnrCol,
        KeyCacheObject key,
        CacheObject newVal,
        CacheObject oldVal,
        boolean internal,
        int partId,
        boolean primary,
        boolean preload,
        long updateCntr,
        @Nullable GridDhtAtomicAbstractUpdateFuture fut,
        AffinityTopologyVersion topVer)
        throws IgniteCheckedException
    {
        assert key != null;
        assert lsnrCol != null;

        boolean hasNewVal = newVal != null;
        boolean hasOldVal = oldVal != null;

        if (!hasNewVal && !hasOldVal) {
            skipUpdateEvent(lsnrCol, key, partId, updateCntr, primary, topVer);

            return;
        }

        EventType evtType = !hasNewVal ? REMOVED : !hasOldVal ? CREATED : UPDATED;

        boolean initialized = false;

        boolean recordIgniteEvt = primary && !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

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
                lsnr.oldValueRequired() ? oldVal : null,
                lsnr.keepBinary(),
                partId,
                updateCntr,
                topVer);

            CacheContinuousQueryEvent evt = new CacheContinuousQueryEvent<>(
                cctx.kernalContext().cache().jcache(cctx.name()), cctx, e0);

            lsnr.onEntryUpdated(evt, primary, recordIgniteEvt, fut);
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

        boolean primary = cctx.affinity().primaryByPartition(cctx.localNode(), e.partition(), AffinityTopologyVersion.NONE);

        if (cctx.isReplicated() || primary) {
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
                    lsnr.oldValueRequired() ? oldVal : null,
                    lsnr.keepBinary(),
                    e.partition(),
                    -1,
                    null);

                CacheContinuousQueryEvent evt = new CacheContinuousQueryEvent(
                    cctx.kernalContext().cache().jcache(cctx.name()), cctx, e0);

                lsnr.onEntryUpdated(evt, primary, recordIgniteEvt, null);
            }
        }
    }

    /**
     * @param locLsnr Local listener.
     * @param rmtFilter Remote filter.
     * @param bufSize Buffer size.
     * @param timeInterval Time interval.
     * @param autoUnsubscribe Auto unsubscribe flag.
     * @param loc Local flag.
     * @return Continuous routine ID.
     * @throws IgniteCheckedException In case of error.
     */
    public UUID executeQuery(final CacheEntryUpdatedListener locLsnr,
        @Nullable final CacheEntryEventSerializableFilter rmtFilter,
        @Nullable final Factory<? extends CacheEntryEventFilter> rmtFilterFactory,
        int bufSize,
        long timeInterval,
        boolean autoUnsubscribe,
        boolean loc,
        final boolean keepBinary,
        final boolean includeExpired) throws IgniteCheckedException
    {
        IgniteClosure<Boolean, CacheContinuousQueryHandler> clsr;

        if (rmtFilterFactory != null)
            clsr = new IgniteClosure<Boolean, CacheContinuousQueryHandler>() {
                @Override public CacheContinuousQueryHandler apply(Boolean v2) {
                    CacheContinuousQueryHandler hnd;

                    if (v2)
                        hnd = new CacheContinuousQueryHandlerV2(
                            cctx.name(),
                            TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
                            locLsnr,
                            rmtFilterFactory,
                            true,
                            false,
                            !includeExpired,
                            false,
                            null);
                    else {
                        CacheEntryEventFilter fltr = rmtFilterFactory.create();

                        if (!(fltr instanceof CacheEntryEventSerializableFilter))
                            throw new IgniteException("Topology has nodes of the old versions. In this case " +
                                "EntryEventFilter should implement " +
                                "org.apache.ignite.cache.CacheEntryEventSerializableFilter interface. Filter: " + fltr);

                        hnd = new CacheContinuousQueryHandler(
                            cctx.name(),
                            TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
                            locLsnr,
                            (CacheEntryEventSerializableFilter)fltr,
                            true,
                            false,
                            !includeExpired,
                            false);
                    }

                    return hnd;
                }
            };
        else
            clsr = new IgniteClosure<Boolean, CacheContinuousQueryHandler>() {
                @Override public CacheContinuousQueryHandler apply(Boolean ignore) {
                    return new CacheContinuousQueryHandler(
                        cctx.name(),
                        TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
                        locLsnr,
                        rmtFilter,
                        true,
                        false,
                        !includeExpired,
                        false);
                }
            };

        return executeQuery0(
            locLsnr,
            clsr,
            bufSize,
            timeInterval,
            autoUnsubscribe,
            false,
            false,
            loc,
            keepBinary,
            false);
    }

    /**
     * @param locLsnr Local listener.
     * @param rmtFilter Remote filter.
     * @param loc Local flag.
     * @param notifyExisting Notify existing flag.
     * @return Continuous routine ID.
     * @throws IgniteCheckedException In case of error.
     */
    public UUID executeInternalQuery(final CacheEntryUpdatedListener<?, ?> locLsnr,
        final CacheEntryEventSerializableFilter rmtFilter,
        final boolean loc,
        final boolean notifyExisting,
        final boolean ignoreClassNotFound)
        throws IgniteCheckedException
    {
        return executeQuery0(
            locLsnr,
            new IgniteClosure<Boolean, CacheContinuousQueryHandler>() {
                @Override public CacheContinuousQueryHandler apply(Boolean v2) {
                    return new CacheContinuousQueryHandler(
                        cctx.name(),
                        TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
                        locLsnr,
                        rmtFilter,
                        true,
                        false,
                        true,
                        ignoreClassNotFound);
                }
            },
            ContinuousQuery.DFLT_PAGE_SIZE,
            ContinuousQuery.DFLT_TIME_INTERVAL,
            ContinuousQuery.DFLT_AUTO_UNSUBSCRIBE,
            true,
            notifyExisting,
            loc,
            false,
            false);
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
    public void executeJCacheQuery(CacheEntryListenerConfiguration cfg, boolean onStart, boolean keepBinary)
        throws IgniteCheckedException {
        JCacheQuery lsnr = new JCacheQuery(cfg, onStart, keepBinary);

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
     * @param topVer Topology version.
     */
    public void beforeExchange(AffinityTopologyVersion topVer) {
        for (CacheContinuousQueryListener lsnr : lsnrs.values())
            lsnr.flushBackupQueue(cctx.kernalContext(), topVer);
    }

    /**
     * Partition evicted callback.
     *
     * @param part Partition number.
     */
    public void onPartitionEvicted(int part) {
        for (CacheContinuousQueryListener lsnr : lsnrs.values())
            lsnr.onPartitionEvicted(part);

        for (CacheContinuousQueryListener lsnr : intLsnrs.values())
            lsnr.onPartitionEvicted(part);
    }

    /**
     * @param locLsnr Local listener.
     * @param bufSize Buffer size.
     * @param timeInterval Time interval.
     * @param autoUnsubscribe Auto unsubscribe flag.
     * @param internal Internal flag.
     * @param notifyExisting Notify existing flag.
     * @param loc Local flag.
     * @param onStart Waiting topology exchange.
     * @return Continuous routine ID.
     * @throws IgniteCheckedException In case of error.
     */
    private UUID executeQuery0(CacheEntryUpdatedListener locLsnr,
        IgniteClosure<Boolean, CacheContinuousQueryHandler> clsr,
        int bufSize,
        long timeInterval,
        boolean autoUnsubscribe,
        boolean internal,
        boolean notifyExisting,
        boolean loc,
        final boolean keepBinary,
        boolean onStart) throws IgniteCheckedException
    {
        cctx.checkSecurity(SecurityPermission.CACHE_READ);

        int taskNameHash = !internal && cctx.kernalContext().security().enabled() ?
            cctx.kernalContext().job().currentTaskNameHash() : 0;

        boolean skipPrimaryCheck = loc && cctx.config().getCacheMode() == CacheMode.REPLICATED && cctx.affinityNode();

        boolean v2 = useV2Protocol(cctx.discovery().allNodes());

        final CacheContinuousQueryHandler hnd = clsr.apply(v2);

        hnd.taskNameHash(taskNameHash);
        hnd.skipPrimaryCheck(skipPrimaryCheck);
        hnd.notifyExisting(notifyExisting);
        hnd.internal(internal);
        hnd.keepBinary(keepBinary);
        hnd.localCache(cctx.isLocal());

        IgnitePredicate<ClusterNode> pred = (loc || cctx.config().getCacheMode() == CacheMode.LOCAL) ?
            F.nodeForNodeId(cctx.localNodeId()) : cctx.config().getNodeFilter();

        assert pred != null : cctx.config();

        UUID id = cctx.kernalContext().continuous().startRoutine(
            hnd,
            internal && loc,
            bufSize,
            timeInterval,
            autoUnsubscribe,
            pred).get();

        try {
            if (hnd.isQuery() && cctx.userCache() && !onStart)
                hnd.waitTopologyFuture(cctx.kernalContext());
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to start continuous query.", e);

            cctx.kernalContext().continuous().stopRoutine(id);

            throw new IgniteCheckedException("Failed to start continuous query.", e);
        }

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

                                CacheContinuousQueryEntry entry = new CacheContinuousQueryEntry(
                                    cctx.cacheId(),
                                    CREATED,
                                    e.key(),
                                    e.rawGet(),
                                    null,
                                    keepBinary,
                                    0,
                                    -1,
                                    null);

                                next = new CacheContinuousQueryEvent<>(
                                    cctx.kernalContext().cache().jcache(cctx.name()),
                                    cctx, entry);

                                if (hnd.getEventFilter() != null && !hnd.getEventFilter().evaluate(next))
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
     * @param keepBinary Keep binary flag.
     * @param filter Filter.
     * @return Iterable for events created for existing cache entries.
     * @throws IgniteCheckedException If failed.
     */
    public Iterable<CacheEntryEvent<?, ?>> existingEntries(final boolean keepBinary, final CacheEntryEventFilter filter)
        throws IgniteCheckedException {
        final Iterator<Cache.Entry<?, ?>> it = cctx.cache().igniteIterator(keepBinary);

        final Cache cache = cctx.kernalContext().cache().jcache(cctx.name());

        return new Iterable<CacheEntryEvent<?, ?>>() {
            @Override public Iterator<CacheEntryEvent<?, ?>> iterator() {
                return new Iterator<CacheEntryEvent<?, ?>>() {
                    private CacheQueryEntryEvent<?, ?> next;

                    {
                        advance();
                    }

                    @Override public boolean hasNext() {
                        return next != null;
                    }

                    @Override public CacheEntryEvent<?, ?> next() {
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

                            Cache.Entry e = it.next();

                            next = new CacheEntryEventImpl(
                                cache,
                                CREATED,
                                e.getKey(),
                                e.getValue());

                            if (filter != null && !filter.evaluate(next))
                                next = null;
                        }
                    }
                };
            }
        };
    }

    /**
     * @param nodes Nodes.
     * @return {@code True} if all nodes greater than {@link GridContinuousProcessor#QUERY_MSG_VER_2_SINCE},
     *     otherwise {@code false}.
     */
    private boolean useV2Protocol(Collection<ClusterNode> nodes) {
        for (ClusterNode node : nodes) {
            if (QUERY_MSG_VER_2_SINCE.compareTo(node.version()) > 0)
                return false;
        }

        return true;
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
     *
     */
    private class JCacheQuery {
        /** */
        private final CacheEntryListenerConfiguration cfg;

        /** */
        private final boolean onStart;

        /** */
        private final boolean keepBinary;

        /** */
        private volatile UUID routineId;

        /**
         * @param cfg Listener configuration.
         * @param onStart {@code True} if executed on cache start.
         */
        private JCacheQuery(CacheEntryListenerConfiguration cfg, boolean onStart, boolean keepBinary) {
            this.cfg = cfg;
            this.onStart = onStart;
            this.keepBinary = keepBinary;
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

            final byte types0 = types;

            final CacheEntryUpdatedListener locLsnr = new JCacheQueryLocalListener(
                locLsnrImpl,
                log);

            routineId = executeQuery0(
                locLsnr,
                new IgniteClosure<Boolean, CacheContinuousQueryHandler>() {
                    @Override public CacheContinuousQueryHandler apply(Boolean v2) {
                        CacheContinuousQueryHandler hnd;
                        Factory<CacheEntryEventFilter> rmtFilterFactory = cfg.getCacheEntryEventFilterFactory();

                        v2 = rmtFilterFactory != null && v2;

                        if (v2)
                            hnd = new CacheContinuousQueryHandlerV2(
                                cctx.name(),
                                TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
                                locLsnr,
                                rmtFilterFactory,
                                cfg.isOldValueRequired(),
                                cfg.isSynchronous(),
                                false,
                                false,
                                types0);
                        else {
                            JCacheQueryRemoteFilter jCacheFilter;

                            CacheEntryEventFilter filter = null;

                            if (rmtFilterFactory != null) {
                                filter = rmtFilterFactory.create();

                                if (!(filter instanceof Serializable))
                                    throw new IgniteException("Topology has nodes of the old versions. " +
                                        "In this case EntryEventFilter must implement java.io.Serializable " +
                                        "interface. Filter: " + filter);
                            }

                            jCacheFilter = new JCacheQueryRemoteFilter(filter, types0);

                            hnd = new CacheContinuousQueryHandler(
                                cctx.name(),
                                TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement()),
                                locLsnr,
                                jCacheFilter,
                                cfg.isOldValueRequired(),
                                cfg.isSynchronous(),
                                false,
                                false);
                        }

                        return hnd;
                    }
                },
                ContinuousQuery.DFLT_PAGE_SIZE,
                ContinuousQuery.DFLT_TIME_INTERVAL,
                ContinuousQuery.DFLT_AUTO_UNSUBSCRIBE,
                false,
                false,
                false,
                keepBinary,
                onStart
            );
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
     *
     */
    static class JCacheQueryLocalListener<K, V> implements CacheEntryUpdatedListener<K, V> {
        /** */
        final CacheEntryListener<K, V> impl;

        /** */
        private final IgniteLogger log;

        /**
         * @param impl Listener.
         * @param log Logger.
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
                            assert impl instanceof CacheEntryCreatedListener : evt;

                            ((CacheEntryCreatedListener<K, V>)impl).onCreated(singleton(evt));

                            break;

                        case UPDATED:
                            assert impl instanceof CacheEntryUpdatedListener : evt;

                            ((CacheEntryUpdatedListener<K, V>)impl).onUpdated(singleton(evt));

                            break;

                        case REMOVED:
                            assert impl instanceof CacheEntryRemovedListener : evt;

                            ((CacheEntryRemovedListener<K, V>)impl).onRemoved(singleton(evt));

                            break;

                        case EXPIRED:
                            assert impl instanceof CacheEntryExpiredListener : evt;

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

        /**
         * @return {@code True} if listener should be executed in non-system thread.
         */
        protected boolean async() {
            return U.hasAnnotation(impl, IgniteAsyncCallback.class);
        }
    }

    /**
     * For handler version 2.0 this filter should not be serialized.
     */
    protected static class JCacheQueryRemoteFilter implements CacheEntryEventSerializableFilter, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected CacheEntryEventFilter impl;

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
        JCacheQueryRemoteFilter(@Nullable CacheEntryEventFilter impl, byte types) {
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
         * @return {@code True} if filter should be executed in non-system thread.
         */
        protected boolean async() {
            return U.hasAnnotation(impl, IgniteAsyncCallback.class);
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

    /**
     * Task flash backup queue.
     */
    private static final class BackupCleaner implements Runnable {
        /** Listeners. */
        private final Map<UUID, CacheContinuousQueryListener> lsnrs;

        /** Context. */
        private final GridKernalContext ctx;

        /**
         * @param lsnrs Listeners.
         */
        public BackupCleaner(Map<UUID, CacheContinuousQueryListener> lsnrs, GridKernalContext ctx) {
            this.lsnrs = lsnrs;
            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            for (CacheContinuousQueryListener lsnr : lsnrs.values())
                lsnr.acknowledgeBackupOnTimeout(ctx);
        }
    }

    /**
     *
     */
    public static class CacheEntryEventImpl extends CacheQueryEntryEvent {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @GridToStringInclude(sensitive = true)
        private Object key;

        /** */
        @GridToStringInclude(sensitive = true)
        private Object val;

        /**
         * @param src Event source.
         * @param evtType Event type.
         * @param key Key.
         * @param val Value.
         */
        public CacheEntryEventImpl(Cache src, EventType evtType, Object key, Object val) {
            super(src, evtType);

            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public long getPartitionUpdateCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public Object getOldValue() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isOldValueAvailable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Object getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public Object getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public Object unwrap(Class cls) {
            if (cls.isAssignableFrom(getClass()))
                return cls.cast(this);

            throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheEntryEventImpl.class, this);
        }
    }
}
