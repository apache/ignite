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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeploymentManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager.JCacheQueryLocalListener;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager.JCacheQueryRemoteFilter;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousQueryBatch;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryFilter;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;

/**
 * Continuous query handler.
 */
public class CacheContinuousQueryHandler<K, V> implements GridContinuousHandler {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int BACKUP_ACK_THRESHOLD = 100;

    /** Cache name. */
    private String cacheName;

    /** Topic for ordered messages. */
    private Object topic;

    /** Local listener. */
    private transient CacheEntryUpdatedListener<K, V> locLsnr;

    /** Remote filter. */
    private CacheEntryEventSerializableFilter<K, V> rmtFilter;

    /** Deployable object for filter. */
    private DeployableObject rmtFilterDep;

    /** Internal flag. */
    private boolean internal;

    /** Notify existing flag. */
    private boolean notifyExisting;

    /** Old value required flag. */
    private boolean oldValRequired;

    /** Synchronous flag. */
    private boolean sync;

    /** Ignore expired events flag. */
    private boolean ignoreExpired;

    /** Task name hash code. */
    private int taskHash;

    /** Whether to skip primary check for REPLICATED cache. */
    private transient boolean skipPrimaryCheck;

    /** Backup queue. */
    private transient volatile Collection<CacheContinuousQueryEntry> backupQueue;

    /** */
    private boolean locCache;

    /** */
    private transient boolean keepBinary;

    /** */
    private transient ConcurrentMap<Integer, PartitionRecovery> rcvs;

    /** */
    private transient ConcurrentMap<Integer, EntryBuffer> entryBufs;

    /** */
    private transient AcknowledgeBuffer ackBuf;

    /** */
    private transient int cacheId;

    /** */
    private transient volatile Map<Integer, Long> initUpdCntrs;

    /** */
    private transient volatile Map<UUID, Map<Integer, Long>> initUpdCntrsPerNode;

    /** */
    private transient volatile AffinityTopologyVersion initTopVer;

    /** */
    private transient boolean ignoreClsNotFound;

    /** */
    private transient boolean asyncCallback;

    /** */
    private transient UUID nodeId;

    /** */
    private transient UUID routineId;

    /** */
    private transient GridKernalContext ctx;

    /** */
    private transient IgniteLogger log;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheContinuousQueryHandler() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param topic Topic for ordered messages.
     * @param locLsnr Local listener.
     * @param rmtFilter Remote filter.
     * @param oldValRequired Old value required flag.
     * @param sync Synchronous flag.
     * @param ignoreExpired Ignore expired events flag.
     */
    public CacheContinuousQueryHandler(
        String cacheName,
        Object topic,
        CacheEntryUpdatedListener<K, V> locLsnr,
        CacheEntryEventSerializableFilter<K, V> rmtFilter,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        boolean ignoreClsNotFound) {
        assert topic != null;
        assert locLsnr != null;

        this.cacheName = cacheName;
        this.topic = topic;
        this.locLsnr = locLsnr;
        this.rmtFilter = rmtFilter;
        this.oldValRequired = oldValRequired;
        this.sync = sync;
        this.ignoreExpired = ignoreExpired;
        this.ignoreClsNotFound = ignoreClsNotFound;

        cacheId = CU.cacheId(cacheName);
    }

    /**
     * @param internal Internal query.
     */
    public void internal(boolean internal) {
        this.internal = internal;
    }

    /**
     * @param notifyExisting Notify existing.
     */
    public void notifyExisting(boolean notifyExisting) {
        this.notifyExisting = notifyExisting;
    }

    /**
     * @param locCache Local cache.
     */
    public void localCache(boolean locCache) {
        this.locCache = locCache;
    }

    /**
     * @param taskHash Task hash.
     */
    public void taskNameHash(int taskHash) {
        this.taskHash = taskHash;
    }

    /**
     * @param skipPrimaryCheck Whether to skip primary check for REPLICATED cache.
     */
    public void skipPrimaryCheck(boolean skipPrimaryCheck) {
        this.skipPrimaryCheck = skipPrimaryCheck;
    }

    /** {@inheritDoc} */
    @Override public boolean isEvents() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isMessaging() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuery() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public void updateCounters(AffinityTopologyVersion topVer, Map<UUID, Map<Integer, Long>> cntrsPerNode,
        Map<Integer, Long> cntrs) {
        this.initUpdCntrsPerNode = cntrsPerNode;
        this.initUpdCntrs = cntrs;
        this.initTopVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(final UUID nodeId, final UUID routineId, final GridKernalContext ctx)
        throws IgniteCheckedException {
        assert nodeId != null;
        assert routineId != null;
        assert ctx != null;

        if (locLsnr != null) {
            if (locLsnr instanceof JCacheQueryLocalListener) {
                ctx.resource().injectGeneric(((JCacheQueryLocalListener)locLsnr).impl);

                asyncCallback = ((JCacheQueryLocalListener)locLsnr).async();
            }
            else {
                ctx.resource().injectGeneric(locLsnr);

                asyncCallback = U.hasAnnotation(locLsnr, IgniteAsyncCallback.class);
            }
        }

        final CacheEntryEventFilter filter = getEventFilter();

        if (filter != null) {
            if (filter instanceof JCacheQueryRemoteFilter) {
                if (((JCacheQueryRemoteFilter)filter).impl != null)
                    ctx.resource().injectGeneric(((JCacheQueryRemoteFilter)filter).impl);

                if (!asyncCallback)
                    asyncCallback = ((JCacheQueryRemoteFilter)filter).async();
            }
            else {
                ctx.resource().injectGeneric(filter);

                if (!asyncCallback)
                    asyncCallback = U.hasAnnotation(filter, IgniteAsyncCallback.class);
            }
        }

        entryBufs = new ConcurrentHashMap<>();

        backupQueue = new ConcurrentLinkedDeque8<>();

        ackBuf = new AcknowledgeBuffer();

        rcvs = new ConcurrentHashMap<>();

        this.nodeId = nodeId;

        this.routineId = routineId;

        this.ctx = ctx;

        final boolean loc = nodeId.equals(ctx.localNodeId());

        assert !skipPrimaryCheck || loc;

        log = ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY);

        CacheContinuousQueryListener<K, V> lsnr = new CacheContinuousQueryListener<K, V>() {
            @Override public void onExecution() {
                if (ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                    ctx.event().record(new CacheQueryExecutedEvent<>(
                        ctx.discovery().localNode(),
                        "Continuous query executed.",
                        EVT_CACHE_QUERY_EXECUTED,
                        CacheQueryType.CONTINUOUS.name(),
                        cacheName,
                        null,
                        null,
                        null,
                        filter instanceof CacheEntryEventSerializableFilter ?
                            (CacheEntryEventSerializableFilter)filter : null,
                        null,
                        nodeId,
                        taskName()
                    ));
                }
            }

            @Override public boolean keepBinary() {
                return keepBinary;
            }

            @Override public void onEntryUpdated(final CacheContinuousQueryEvent<K, V> evt,
                boolean primary,
                final boolean recordIgniteEvt,
                GridDhtAtomicAbstractUpdateFuture fut) {
                if (ignoreExpired && evt.getEventType() == EventType.EXPIRED)
                    return;

                if (log.isDebugEnabled())
                    log.debug("Entry updated on affinity node [evt=" + evt + ", primary=" + primary + ']');

                final GridCacheContext<K, V> cctx = cacheContext(ctx);

                // Check that cache stopped.
                if (cctx == null)
                    return;

                // skipPrimaryCheck is set only when listen locally for replicated cache events.
                assert !skipPrimaryCheck || (cctx.isReplicated() && ctx.localNodeId().equals(nodeId));

                if (asyncCallback) {
                    ContinuousQueryAsyncClosure clsr = new ContinuousQueryAsyncClosure(
                        primary,
                        evt,
                        recordIgniteEvt,
                        fut);

                    ctx.asyncCallbackPool().execute(clsr, evt.partitionId());
                }
                else {
                    final boolean notify = filter(evt, primary);

                    if (log.isDebugEnabled())
                        log.debug("Filter invoked for event [evt=" + evt + ", primary=" + primary
                            + ", notify=" + notify + ']');

                    if (primary || skipPrimaryCheck) {
                        if (fut == null)
                            onEntryUpdate(evt, notify, loc, recordIgniteEvt);
                        else {
                            fut.addContinuousQueryClosure(new CI1<Boolean>() {
                                @Override public void apply(Boolean suc) {
                                    if (!suc)
                                        evt.entry().markFiltered();

                                    onEntryUpdate(evt, notify, loc, recordIgniteEvt);
                                }
                            });
                        }
                    }
                }
            }

            @Override public void onUnregister() {
                if (filter instanceof PlatformContinuousQueryFilter)
                    ((PlatformContinuousQueryFilter)filter).onQueryUnregister();
            }

            @Override public void cleanupBackupQueue(Map<Integer, Long> updateCntrs) {
                Collection<CacheContinuousQueryEntry> backupQueue0 = backupQueue;

                if (backupQueue0 != null) {
                    Iterator<CacheContinuousQueryEntry> it = backupQueue0.iterator();

                    while (it.hasNext()) {
                        CacheContinuousQueryEntry backupEntry = it.next();

                        Long updateCntr = updateCntrs.get(backupEntry.partition());

                        if (updateCntr != null && backupEntry.updateCounter() <= updateCntr)
                            it.remove();
                    }
                }
            }

            @Override public void flushBackupQueue(GridKernalContext ctx, AffinityTopologyVersion topVer) {
                Collection<CacheContinuousQueryEntry> backupQueue0 = backupQueue;

                if (backupQueue0 == null)
                    return;

                try {
                    ClusterNode nodeId0 = ctx.discovery().node(nodeId);

                    if (nodeId0 != null) {
                        GridCacheContext<K, V> cctx = cacheContext(ctx);

                        for (CacheContinuousQueryEntry e : backupQueue0) {
                            if (!e.isFiltered())
                                prepareEntry(cctx, nodeId, e);

                            e.topologyVersion(topVer);
                        }

                        ctx.continuous().addBackupNotification(nodeId, routineId, backupQueue0, topic);
                    }
                    else
                        // Node which start CQ leave topology. Not needed to put data to backup queue.
                        backupQueue = null;

                    backupQueue0.clear();
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY),
                        "Failed to send backup event notification to node: " + nodeId, e);
                }
            }

            @Override public void acknowledgeBackupOnTimeout(GridKernalContext ctx) {
                sendBackupAcknowledge(ackBuf.acknowledgeOnTimeout(), routineId, ctx);
            }

            @Override public void skipUpdateEvent(CacheContinuousQueryEvent<K, V> evt,
                AffinityTopologyVersion topVer, boolean primary) {
                assert evt != null;

                CacheContinuousQueryEntry e = evt.entry();

                e.markFiltered();

                onEntryUpdated(evt, primary, false, null);
            }

            @Override public void onPartitionEvicted(int part) {
                Collection<CacheContinuousQueryEntry> backupQueue0 = backupQueue;

                if (backupQueue0 != null) {
                    for (Iterator<CacheContinuousQueryEntry> it = backupQueue0.iterator(); it.hasNext(); ) {
                        if (it.next().partition() == part)
                            it.remove();
                    }
                }
            }

            @Override public boolean oldValueRequired() {
                return oldValRequired;
            }

            @Override public boolean notifyExisting() {
                return notifyExisting;
            }

            private String taskName() {
                return ctx.security().enabled() ? ctx.task().resolveTaskName(taskHash) : null;
            }
        };

        CacheContinuousQueryManager mgr = manager(ctx);

        if (mgr == null)
            return RegisterStatus.DELAYED;

        return mgr.registerListener(routineId, lsnr, internal);
    }

    /**
     * @return Cache entry event filter.
     */
    public CacheEntryEventFilter getEventFilter() {
        return rmtFilter;
    }

    /**
     * @param cctx Context.
     * @param nodeId ID of the node that started routine.
     * @param entry Entry.
     * @throws IgniteCheckedException In case of error.
     */
    private void prepareEntry(GridCacheContext cctx, UUID nodeId, CacheContinuousQueryEntry entry)
        throws IgniteCheckedException {
        if (cctx.kernalContext().config().isPeerClassLoadingEnabled() && cctx.discovery().node(nodeId) != null) {
            entry.prepareMarshal(cctx);

            cctx.deploy().prepare(entry);
        }
        else
            entry.prepareMarshal(cctx);
    }

    /**
     * Wait topology.
     */
    public void waitTopologyFuture(GridKernalContext ctx) throws IgniteCheckedException {
        GridCacheContext<K, V> cctx = cacheContext(ctx);

        if (!cctx.isLocal()) {
            cacheContext(ctx).affinity().affinityReadyFuture(initTopVer).get();

            for (int partId = 0; partId < cacheContext(ctx).affinity().partitions(); partId++)
                getOrCreatePartitionRecovery(ctx, partId);
        }
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID routineId, GridKernalContext ctx) {
        assert routineId != null;
        assert ctx != null;

        GridCacheAdapter<K, V> cache = ctx.cache().internalCache(cacheName);

        if (cache != null)
            cache.context().continuousQueries().unregisterListener(internal, routineId);
    }

    /**
     * @param ctx Kernal context.
     * @return Continuous query manager.
     */
    private CacheContinuousQueryManager manager(GridKernalContext ctx) {
        GridCacheContext<K, V> cacheCtx = cacheContext(ctx);

        return cacheCtx == null ? null : cacheCtx.continuousQueries();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void notifyCallback(final UUID nodeId,
        final UUID routineId,
        Collection<?> objs,
        final GridKernalContext ctx) {
        assert nodeId != null;
        assert routineId != null;
        assert objs != null;
        assert ctx != null;

        if (objs.isEmpty())
            return;

        if (asyncCallback) {
            final List<CacheContinuousQueryEntry> entries = objs instanceof List ? (List)objs : new ArrayList(objs);

            IgniteStripedThreadPoolExecutor asyncPool = ctx.asyncCallbackPool();

            int threadId = asyncPool.threadId(entries.get(0).partition());

            int startIdx = 0;

            if (entries.size() != 1) {
                for (int i = 1; i < entries.size(); i++) {
                    int curThreadId = asyncPool.threadId(entries.get(i).partition());

                    // If all entries from one partition avoid creation new collections.
                    if (curThreadId == threadId)
                        continue;

                    final int i0 = i;
                    final int startIdx0 = startIdx;

                    asyncPool.execute(new Runnable() {
                        @Override public void run() {
                            notifyCallback0(nodeId, ctx, entries.subList(startIdx0, i0));
                        }
                    }, threadId);

                    startIdx = i0;
                    threadId = curThreadId;
                }
            }

            final int startIdx0 = startIdx;

            asyncPool.execute(new Runnable() {
                @Override public void run() {
                    notifyCallback0(nodeId, ctx,
                        startIdx0 == 0 ? entries : entries.subList(startIdx0, entries.size()));
                }
            }, threadId);
        }
        else
            notifyCallback0(nodeId, ctx, (Collection)objs);
    }

    /**
     * @param nodeId Node id.
     * @param ctx Kernal context.
     * @param entries Entries.
     */
    private void notifyCallback0(UUID nodeId,
        final GridKernalContext ctx,
        Collection<CacheContinuousQueryEntry> entries) {
        final GridCacheContext cctx = cacheContext(ctx);

        if (cctx == null) {
            IgniteLogger log = ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY);

            if (log.isDebugEnabled())
                log.debug("Failed to notify callback, cache is not found: " + cacheId);

            return;
        }

        final Collection<CacheEntryEvent<? extends K, ? extends V>> entries0 = new ArrayList<>(entries.size());

        for (CacheContinuousQueryEntry e : entries) {
            GridCacheDeploymentManager depMgr = cctx.deploy();

            ClassLoader ldr = depMgr.globalLoader();

            if (ctx.config().isPeerClassLoadingEnabled()) {
                GridDeploymentInfo depInfo = e.deployInfo();

                if (depInfo != null) {
                    depMgr.p2pContext(nodeId, depInfo.classLoaderId(), depInfo.userVersion(), depInfo.deployMode(),
                        depInfo.participants(), depInfo.localDeploymentOwner());
                }
            }

            try {
                e.unmarshal(cctx, ldr);

                Collection<CacheEntryEvent<? extends K, ? extends V>> evts = handleEvent(ctx, e);

                if (evts != null && !evts.isEmpty())
                    entries0.addAll(evts);
            }
            catch (IgniteCheckedException ex) {
                if (ignoreClsNotFound)
                    assert internal;
                else
                    U.error(ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY), "Failed to unmarshal entry.", ex);
            }
        }

        if (!entries0.isEmpty())
            locLsnr.onUpdated(entries0);
    }

    /**
     * @param ctx Context.
     * @param e entry.
     * @return Entry collection.
     */
    private Collection<CacheEntryEvent<? extends K, ? extends V>> handleEvent(GridKernalContext ctx,
        CacheContinuousQueryEntry e) {
        assert e != null;

        GridCacheContext<K, V> cctx = cacheContext(ctx);

        final IgniteCache cache = cctx.kernalContext().cache().jcache(cctx.name());

        if (internal) {
            if (e.isFiltered())
                return Collections.emptyList();
            else
                return F.<CacheEntryEvent<? extends K, ? extends V>>
                    asList(new CacheContinuousQueryEvent<K, V>(cache, cctx, e));
        }

        // Initial query entry or evicted entry. These events should be fired immediately.
        if (e.updateCounter() == -1L) {
            return !e.isFiltered() ? F.<CacheEntryEvent<? extends K, ? extends V>>asList(
                new CacheContinuousQueryEvent<K, V>(cache, cctx, e)) :
                Collections.<CacheEntryEvent<? extends K, ? extends V>>emptyList();
        }

        PartitionRecovery rec = getOrCreatePartitionRecovery(ctx, e.partition());

        return rec.collectEntries(e, cctx, cache);
    }

    /**
     * @param primary Primary.
     * @param evt Query event.
     * @return {@code True} if event passed filter otherwise {@code true}.
     */
    public boolean filter(CacheContinuousQueryEvent evt, boolean primary) {
        CacheContinuousQueryEntry entry = evt.entry();

        boolean notify = !entry.isFiltered();

        try {
            if (notify && getEventFilter() != null)
                notify = getEventFilter().evaluate(evt);
        }
        catch (Exception e) {
            U.error(log, "CacheEntryEventFilter failed: " + e);
        }

        if (!notify)
            entry.markFiltered();

        if (!primary && !internal && entry.updateCounter() != -1L /* Skip init query and expire entries */) {
            entry.markBackup();

            Collection<CacheContinuousQueryEntry> backupQueue0 = backupQueue;

            if (backupQueue0 != null)
                backupQueue0.add(entry.forBackupQueue());
        }

        return notify;
    }

    /**
     * @param evt Continuous query event.
     * @param notify Notify flag.
     * @param loc Listener deployed on this node.
     * @param recordIgniteEvt Record ignite event.
     */
    private void onEntryUpdate(CacheContinuousQueryEvent evt, boolean notify, boolean loc, boolean recordIgniteEvt) {
        try {
            GridCacheContext<K, V> cctx = cacheContext(ctx);

            if (cctx == null)
                return;

            final CacheContinuousQueryEntry entry = evt.entry();

            if (loc) {
                if (!locCache) {
                    Collection<CacheEntryEvent<? extends K, ? extends V>> evts = handleEvent(ctx, entry);

                    if (!evts.isEmpty())
                        locLsnr.onUpdated(evts);

                    if (!internal && !skipPrimaryCheck)
                        sendBackupAcknowledge(ackBuf.onAcknowledged(entry), routineId, ctx);
                }
                else {
                    if (!entry.isFiltered())
                        locLsnr.onUpdated(F.<CacheEntryEvent<? extends K, ? extends V>>asList(evt));
                }
            }
            else {
                if (!entry.isFiltered())
                    prepareEntry(cctx, nodeId, entry);

                CacheContinuousQueryEntry e = handleEntry(entry);

                if (e != null) {
                    if (log.isDebugEnabled())
                        log.debug("Send the following event to listener: " + e);

                    ctx.continuous().addNotification(nodeId, routineId, entry, topic, sync, true);
                }
            }
        }
        catch (ClusterTopologyCheckedException ex) {
            if (log.isDebugEnabled())
                log.debug("Failed to send event notification to node, node left cluster " +
                    "[node=" + nodeId + ", err=" + ex + ']');
        }
        catch (IgniteCheckedException ex) {
            U.error(ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY), "Failed to send event notification to node: " + nodeId, ex);
        }

        if (recordIgniteEvt && notify) {
            ctx.event().record(new CacheQueryReadEvent<>(
                ctx.discovery().localNode(),
                "Continuous query executed.",
                EVT_CACHE_QUERY_OBJECT_READ,
                CacheQueryType.CONTINUOUS.name(),
                cacheName,
                null,
                null,
                null,
                getEventFilter() instanceof CacheEntryEventSerializableFilter ?
                    (CacheEntryEventSerializableFilter)getEventFilter() : null,
                null,
                nodeId,
                taskName(),
                evt.getKey(),
                evt.getValue(),
                evt.getOldValue(),
                null
            ));
        }
    }

    /**
     * @return Task name.
     */
    private String taskName() {
        return ctx.security().enabled() ? ctx.task().resolveTaskName(taskHash) : null;
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected() {
        if (internal)
            return;

        for (PartitionRecovery rec : rcvs.values())
            rec.resetTopologyCache();
    }

    /**
     * @param ctx Context.
     * @param partId Partition id.
     * @return Partition recovery.
     */
    @NotNull private PartitionRecovery getOrCreatePartitionRecovery(GridKernalContext ctx, int partId) {
        PartitionRecovery rec = rcvs.get(partId);

        if (rec == null) {
            Long partCntr = null;

            AffinityTopologyVersion initTopVer0 = initTopVer;

            if (initTopVer0 != null) {
                GridCacheContext<K, V> cctx = cacheContext(ctx);

                GridCacheAffinityManager aff = cctx.affinity();

                if (initUpdCntrsPerNode != null) {
                    for (ClusterNode node : aff.nodesByPartition(partId, initTopVer)) {
                        Map<Integer, Long> map = initUpdCntrsPerNode.get(node.id());

                        if (map != null) {
                            partCntr = map.get(partId);

                            break;
                        }
                    }
                }
                else if (initUpdCntrs != null)
                    partCntr = initUpdCntrs.get(partId);
            }

            rec = new PartitionRecovery(ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY), initTopVer0, partCntr);

            PartitionRecovery oldRec = rcvs.putIfAbsent(partId, rec);

            if (oldRec != null)
                rec = oldRec;
        }

        return rec;
    }

    /**
     * @param e Entry.
     * @return Entry.
     */
    private CacheContinuousQueryEntry handleEntry(CacheContinuousQueryEntry e) {
        assert e != null;
        assert entryBufs != null;

        if (internal) {
            if (e.isFiltered())
                return null;
            else
                return e;
        }

        // Initial query entry.
        // This events should be fired immediately.
        if (e.updateCounter() == -1)
            return e;

        EntryBuffer buf = entryBufs.get(e.partition());

        if (buf == null) {
            buf = new EntryBuffer();

            EntryBuffer oldRec = entryBufs.putIfAbsent(e.partition(), buf);

            if (oldRec != null)
                buf = oldRec;
        }

        return buf.handle(e);
    }

    /**
     *
     */
    private static class PartitionRecovery {
        /** Event which means hole in sequence. */
        private static final CacheContinuousQueryEntry HOLE = new CacheContinuousQueryEntry();

        /** */
        private final static int MAX_BUFF_SIZE = 100;

        /** */
        private IgniteLogger log;

        /** */
        private long lastFiredEvt;

        /** */
        private AffinityTopologyVersion curTop = AffinityTopologyVersion.NONE;

        /** */
        private final Map<Long, CacheContinuousQueryEntry> pendingEvts = new TreeMap<>();

        /**
         * @param log Logger.
         * @param topVer Topology version.
         * @param initCntr Update counters.
         */
        PartitionRecovery(IgniteLogger log, AffinityTopologyVersion topVer, @Nullable Long initCntr) {
            this.log = log;

            if (initCntr != null) {
                assert topVer.topologyVersion() > 0 : topVer;

                this.lastFiredEvt = initCntr;

                curTop = topVer;
            }
        }

        /**
         * Resets cached topology.
         */
        void resetTopologyCache() {
            curTop = AffinityTopologyVersion.NONE;
        }

        /**
         * Add continuous entry.
         *
         * @param cctx Cache context.
         * @param cache Cache.
         * @param entry Cache continuous query entry.
         * @return Collection entries which will be fired. This collection should contains only non-filtered events.
         */
        <K, V> Collection<CacheEntryEvent<? extends K, ? extends V>> collectEntries(
            CacheContinuousQueryEntry entry,
            GridCacheContext cctx,
            IgniteCache cache
        ) {
            assert entry != null;

            if (entry.topologyVersion() == null) { // Possible if entry is sent from old node.
                assert entry.updateCounter() == 0L : entry;

                return F.<CacheEntryEvent<? extends K, ? extends V>>
                    asList(new CacheContinuousQueryEvent<K, V>(cache, cctx, entry));
            }

            List<CacheEntryEvent<? extends K, ? extends V>> entries;

            synchronized (pendingEvts) {
                if (log.isDebugEnabled()) {
                    log.debug("Handling event [lastFiredEvt=" + lastFiredEvt +
                        ", curTop=" + curTop +
                        ", entUpdCnt=" + entry.updateCounter() +
                        ", partId=" + entry.partition() +
                        ", pendingEvts=" + pendingEvts + ']');
                }

                // Received first event.
                if (curTop == AffinityTopologyVersion.NONE) {
                    lastFiredEvt = entry.updateCounter();

                    curTop = entry.topologyVersion();

                    if (log.isDebugEnabled()) {
                        log.debug("First event [lastFiredEvt=" + lastFiredEvt +
                            ", curTop=" + curTop +
                            ", entUpdCnt=" + entry.updateCounter() +
                            ", partId=" + entry.partition() + ']');
                    }

                    return !entry.isFiltered() ?
                        F.<CacheEntryEvent<? extends K, ? extends V>>
                            asList(new CacheContinuousQueryEvent<K, V>(cache, cctx, entry)) :
                        Collections.<CacheEntryEvent<? extends K, ? extends V>>emptyList();
                }

                if (curTop.compareTo(entry.topologyVersion()) < 0) {
                    if (entry.updateCounter() == 1L && !entry.isBackup()) {
                        entries = new ArrayList<>(pendingEvts.size());

                        for (CacheContinuousQueryEntry evt : pendingEvts.values()) {
                            if (evt != HOLE && !evt.isFiltered())
                                entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, evt));
                        }

                        pendingEvts.clear();

                        curTop = entry.topologyVersion();

                        lastFiredEvt = entry.updateCounter();

                        if (!entry.isFiltered())
                            entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, entry));

                        if (log.isDebugEnabled())
                            log.debug("Partition was lost [lastFiredEvt=" + lastFiredEvt +
                                ", curTop=" + curTop +
                                ", entUpdCnt=" + entry.updateCounter() +
                                ", partId=" + entry.partition() +
                                ", pendingEvts=" + pendingEvts + ']');

                        return entries;
                    }

                    curTop = entry.topologyVersion();
                }

                // Check duplicate.
                if (entry.updateCounter() > lastFiredEvt) {
                    pendingEvts.put(entry.updateCounter(), entry);

                    // Put filtered events.
                    if (entry.filteredEvents() != null) {
                        for (long cnrt : entry.filteredEvents()) {
                            if (cnrt > lastFiredEvt)
                                pendingEvts.put(cnrt, HOLE);
                        }
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Skip duplicate continuous query message: " + entry);

                    return Collections.emptyList();
                }

                if (pendingEvts.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Nothing sent to listener [lastFiredEvt=" + lastFiredEvt +
                            ", curTop=" + curTop +
                            ", entUpdCnt=" + entry.updateCounter() +
                            ", partId=" + entry.partition() + ']');
                    }

                    return Collections.emptyList();
                }

                Iterator<Map.Entry<Long, CacheContinuousQueryEntry>> iter = pendingEvts.entrySet().iterator();

                entries = new ArrayList<>();

                if (pendingEvts.size() >= MAX_BUFF_SIZE) {
                    for (int i = 0; i < MAX_BUFF_SIZE - (MAX_BUFF_SIZE / 10); i++) {
                        Map.Entry<Long, CacheContinuousQueryEntry> e = iter.next();

                        if (e.getValue() != HOLE && !e.getValue().isFiltered())
                            entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, e.getValue()));

                        lastFiredEvt = e.getKey();

                        iter.remove();
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Pending events reached max of buffer size [lastFiredEvt=" + lastFiredEvt +
                            ", curTop=" + curTop +
                            ", entUpdCnt=" + entry.updateCounter() +
                            ", partId=" + entry.partition() +
                            ", pendingEvts=" + pendingEvts + ']');
                    }
                }
                else {
                    // Elements are consistently.
                    while (iter.hasNext()) {
                        Map.Entry<Long, CacheContinuousQueryEntry> e = iter.next();

                        if (e.getKey() == lastFiredEvt + 1) {
                            ++lastFiredEvt;

                            if (e.getValue() != HOLE && !e.getValue().isFiltered())
                                entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, e.getValue()));

                            iter.remove();
                        }
                        else
                            break;
                    }
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Will send to listener the following events [entries=" + entries +
                    ", lastFiredEvt=" + lastFiredEvt +
                    ", curTop=" + curTop +
                    ", entUpdCnt=" + entry.updateCounter() +
                    ", partId=" + entry.partition() +
                    ", pendingEvts=" + pendingEvts + ']');
            }

            return entries;
        }
    }

    /**
     *
     */
    private static class EntryBuffer {
        /** */
        private final static int MAX_BUFF_SIZE = 100;

        /** */
        private final GridConcurrentSkipListSet<Long> buf = new GridConcurrentSkipListSet<>();

        /** */
        private AtomicLong lastFiredCntr = new AtomicLong();

        /**
         * @param newVal New value.
         * @return Old value if previous value less than new value otherwise {@code -1}.
         */
        private long updateFiredCounter(long newVal) {
            long prevVal = lastFiredCntr.get();

            while (prevVal < newVal) {
                if (lastFiredCntr.compareAndSet(prevVal, newVal))
                    return prevVal;
                else
                    prevVal = lastFiredCntr.get();
            }

            return prevVal >= newVal ? -1 : prevVal;
        }

        /**
         * Add continuous entry.
         *
         * @param e Cache continuous query entry.
         * @return Collection entries which will be fired.
         */
        public CacheContinuousQueryEntry handle(CacheContinuousQueryEntry e) {
            assert e != null;

            if (e.isFiltered()) {
                Long last = buf.lastx();
                Long first = buf.firstx();

                if (last != null && first != null && last - first >= MAX_BUFF_SIZE) {
                    NavigableSet<Long> prevHoles = buf.subSet(first, true, last, true);

                    GridLongList filteredEvts = new GridLongList((int)(last - first));

                    int size = 0;

                    Long cntr;

                    while ((cntr = prevHoles.pollFirst()) != null) {
                        filteredEvts.add(cntr);

                        ++size;
                    }

                    filteredEvts.truncate(size, true);

                    e.filteredEvents(filteredEvts);

                    return e;
                }

                if (lastFiredCntr.get() > e.updateCounter() || e.updateCounter() == 1)
                    return e;
                else {
                    buf.add(e.updateCounter());

                    // Double check. If another thread sent a event with counter higher than this event.
                    if (lastFiredCntr.get() > e.updateCounter() && buf.contains(e.updateCounter())) {
                        buf.remove(e.updateCounter());

                        return e;
                    }
                    else
                        return null;
                }
            }
            else {
                long prevVal = updateFiredCounter(e.updateCounter());

                if (prevVal == -1)
                    return e;
                else {
                    NavigableSet<Long> prevHoles = buf.subSet(prevVal, true, e.updateCounter(), true);

                    GridLongList filteredEvts = new GridLongList((int)(e.updateCounter() - prevVal));

                    int size = 0;

                    Long cntr;

                    while ((cntr = prevHoles.pollFirst()) != null) {
                        filteredEvts.add(cntr);

                        ++size;
                    }

                    filteredEvts.truncate(size, true);

                    e.filteredEvents(filteredEvts);

                    return e;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft() {
        Collection<CacheContinuousQueryEntry> backupQueue0 = backupQueue;

        if (backupQueue0 != null)
            backupQueue = null;
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (rmtFilter != null && !U.isGrid(rmtFilter.getClass()))
            rmtFilterDep = new DeployableObject(rmtFilter, ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        assert nodeId != null;
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (rmtFilterDep != null)
            rmtFilter = rmtFilterDep.unmarshal(nodeId, ctx);
    }

    /** {@inheritDoc} */
    @Override public GridContinuousBatch createBatch() {
        return new GridContinuousQueryBatch();
    }

    /** {@inheritDoc} */
    @Override public void onBatchAcknowledged(final UUID routineId,
        GridContinuousBatch batch,
        final GridKernalContext ctx) {
        sendBackupAcknowledge(ackBuf.onAcknowledged(batch), routineId, ctx);
    }

    /**
     * @param t Acknowledge information.
     * @param routineId Routine ID.
     * @param ctx Context.
     */
    private void sendBackupAcknowledge(final IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> t,
        final UUID routineId,
        final GridKernalContext ctx) {
        if (t != null) {
            ctx.closure().runLocalSafe(new Runnable() {
                @Override public void run() {
                    GridCacheContext<K, V> cctx = cacheContext(ctx);

                    CacheContinuousQueryBatchAck msg = new CacheContinuousQueryBatchAck(cctx.cacheId(),
                        routineId,
                        t.get1());

                    for (AffinityTopologyVersion topVer : t.get2()) {
                        for (ClusterNode node : ctx.discovery().cacheAffinityNodes(cctx.name(), topVer)) {
                            if (!node.isLocal() && node.version().compareTo(CacheContinuousQueryBatchAck.SINCE_VER) >= 0) {
                                try {
                                    cctx.io().send(node, msg, GridIoPolicy.SYSTEM_POOL);
                                }
                                catch (ClusterTopologyCheckedException ignored) {
                                    IgniteLogger log = ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY);

                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send acknowledge message, node left " +
                                            "[msg=" + msg + ", node=" + node + ']');
                                }
                                catch (IgniteCheckedException e) {
                                    IgniteLogger log = ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY);

                                    U.error(log, "Failed to send acknowledge message " +
                                        "[msg=" + msg + ", node=" + node + ']', e);
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object orderedTopic() {
        return topic;
    }

    /** {@inheritDoc} */
    @Override public GridContinuousHandler clone() {
        try {
            return (GridContinuousHandler)super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryHandler.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        out.writeObject(topic);

        boolean b = rmtFilterDep != null;

        out.writeBoolean(b);

        if (b)
            out.writeObject(rmtFilterDep);
        else
            out.writeObject(rmtFilter);

        out.writeBoolean(internal);
        out.writeBoolean(notifyExisting);
        out.writeBoolean(oldValRequired);
        out.writeBoolean(sync);
        out.writeBoolean(ignoreExpired);
        out.writeInt(taskHash);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        topic = in.readObject();

        boolean b = in.readBoolean();

        if (b)
            rmtFilterDep = (DeployableObject)in.readObject();
        else
            rmtFilter = (CacheEntryEventSerializableFilter<K, V>)in.readObject();

        internal = in.readBoolean();
        notifyExisting = in.readBoolean();
        oldValRequired = in.readBoolean();
        sync = in.readBoolean();
        ignoreExpired = in.readBoolean();
        taskHash = in.readInt();

        cacheId = CU.cacheId(cacheName);
    }

    /**
     * @param ctx Kernal context.
     * @return Cache context.
     */
    private GridCacheContext<K, V> cacheContext(GridKernalContext ctx) {
        assert ctx != null;

        return ctx.cache().<K, V>context().cacheContext(cacheId);
    }

    /** */
    private static class AcknowledgeBuffer {
        /** */
        private int size;

        /** */
        @GridToStringInclude
        private Map<Integer, Long> updateCntrs = new HashMap<>();

        /** */
        @GridToStringInclude
        private Set<AffinityTopologyVersion> topVers = U.newHashSet(1);

        /**
         * @param batch Batch.
         * @return Non-null tuple if acknowledge should be sent to backups.
         */
        @SuppressWarnings("unchecked")
        @Nullable synchronized IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>>
        onAcknowledged(GridContinuousBatch batch) {
            assert batch instanceof GridContinuousQueryBatch;

            size += ((GridContinuousQueryBatch)batch).entriesCount();

            Collection<CacheContinuousQueryEntry> entries = (Collection)batch.collect();

            for (CacheContinuousQueryEntry e : entries)
                addEntry(e);

            return size >= BACKUP_ACK_THRESHOLD ? acknowledgeData() : null;
        }

        /**
         * @param e Entry.
         * @return Non-null tuple if acknowledge should be sent to backups.
         */
        @Nullable synchronized IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>>
        onAcknowledged(CacheContinuousQueryEntry e) {
            size++;

            addEntry(e);

            return size >= BACKUP_ACK_THRESHOLD ? acknowledgeData() : null;
        }

        /**
         * @param e Entry.
         */
        private void addEntry(CacheContinuousQueryEntry e) {
            topVers.add(e.topologyVersion());

            Long cntr0 = updateCntrs.get(e.partition());

            if (cntr0 == null || e.updateCounter() > cntr0)
                updateCntrs.put(e.partition(), e.updateCounter());
        }

        /**
         * @return Non-null tuple if acknowledge should be sent to backups.
         */
        @Nullable synchronized IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>>
            acknowledgeOnTimeout() {
            return size > 0 ? acknowledgeData() : null;
        }

        /**
         * @return Tuple with acknowledge information.
         */
        private IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> acknowledgeData() {
            assert size > 0;

            Map<Integer, Long> cntrs = new HashMap<>(updateCntrs);

            IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> res =
                new IgniteBiTuple<>(cntrs, topVers);

            topVers = U.newHashSet(1);

            size = 0;

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AcknowledgeBuffer.class, this);
        }
    }

    /**
     *
     */
    private class ContinuousQueryAsyncClosure implements Runnable {
        /** */
        private final CacheContinuousQueryEvent<K, V> evt;

        /** */
        private final boolean primary;

        /** */
        private final boolean recordIgniteEvt;

        /** */
        private final IgniteInternalFuture<?> fut;

        /**
         * @param primary Primary flag.
         * @param evt Event.
         * @param recordIgniteEvt Fired event.
         * @param fut Dht future.
         */
        ContinuousQueryAsyncClosure(
            boolean primary,
            CacheContinuousQueryEvent<K, V> evt,
            boolean recordIgniteEvt,
            IgniteInternalFuture<?> fut) {
            this.primary = primary;
            this.evt = evt;
            this.recordIgniteEvt = recordIgniteEvt;
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            final boolean notify = filter(evt, primary);

            if (!primary())
                return;

            if (fut == null) {
                onEntryUpdate(evt, notify, nodeId.equals(ctx.localNodeId()), recordIgniteEvt);

                return;
            }

            if (fut.isDone()) {
                if (fut.error() != null)
                    evt.entry().markFiltered();

                onEntryUpdate(evt, notify, nodeId.equals(ctx.localNodeId()), recordIgniteEvt);
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        if (f.error() != null)
                            evt.entry().markFiltered();

                        ctx.asyncCallbackPool().execute(new Runnable() {
                            @Override public void run() {
                                onEntryUpdate(evt, notify, nodeId.equals(ctx.localNodeId()), recordIgniteEvt);
                            }
                        }, evt.entry().partition());
                    }
                });
            }
        }

        /**
         * @return {@code True} if event fired on this node.
         */
        private boolean primary() {
            return primary || skipPrimaryCheck;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(ContinuousQueryAsyncClosure.class, this);
        }
    }

    /**
     * Deployable object.
     */
    protected static class DeployableObject implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Serialized object. */
        private byte[] bytes;

        /** Deployment class name. */
        private String clsName;

        /** Deployment info. */
        private GridDeploymentInfo depInfo;

        /**
         * Required by {@link Externalizable}.
         */
        public DeployableObject() {
            // No-op.
        }

        /**
         * @param obj Object.
         * @param ctx Kernal context.
         * @throws IgniteCheckedException In case of error.
         */
        protected DeployableObject(Object obj, GridKernalContext ctx) throws IgniteCheckedException {
            assert obj != null;
            assert ctx != null;

            Class cls = U.detectClass(obj);

            clsName = cls.getName();

            GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy object: " + obj);

            depInfo = new GridDeploymentInfoBean(dep);

            bytes = U.marshal(ctx, obj);
        }

        /**
         * @param nodeId Node ID.
         * @param ctx Kernal context.
         * @return Deserialized object.
         * @throws IgniteCheckedException In case of error.
         */
        <T> T unmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
            assert ctx != null;

            GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
                depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to obtain deployment for class: " + clsName);

            return U.unmarshal(ctx, bytes, U.resolveClassLoader(dep.classLoader(), ctx.config()));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeByteArray(out, bytes);
            U.writeString(out, clsName);
            out.writeObject(depInfo);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            bytes = U.readByteArray(in);
            clsName = U.readString(in);
            depInfo = (GridDeploymentInfo)in.readObject();
        }
    }
}
