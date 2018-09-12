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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeploymentManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager.JCacheQueryLocalListener;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager.JCacheQueryRemoteFilter;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousQueryBatch;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryFilter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;

/**
 * Continuous query handler.
 */
public class CacheContinuousQueryHandler<K, V> implements GridContinuousHandler {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    static final int BACKUP_ACK_THRESHOLD =
        IgniteSystemProperties.getInteger("IGNITE_CONTINUOUS_QUERY_BACKUP_ACK_THRESHOLD", 100);

    /** */
    static final int LSNR_MAX_BUF_SIZE =
        IgniteSystemProperties.getInteger("IGNITE_CONTINUOUS_QUERY_LISTENER_MAX_BUFFER_SIZE", 10_000);

    /**
     * Transformer implementation for processing received remote events.
     * They are already transformed so we simply return transformed value for event.
     */
    private transient IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> returnValTrans =
        new IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, Object>() {
            @Override public Object apply(CacheEntryEvent<? extends K, ? extends V> evt) {
                assert evt.getKey() == null;

                return evt.getValue();
            }
        };

    /** Cache name. */
    private String cacheName;

    /** Topic for ordered messages. */
    private Object topic;

    /** Local listener. */
    private transient CacheEntryUpdatedListener<K, V> locLsnr;

    /** Remote filter. */
    private CacheEntryEventSerializableFilter<K, V> rmtFilter;

    /** Deployable object for filter. */
    private CacheContinuousQueryDeployableObject rmtFilterDep;

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

    /** */
    private boolean locCache;

    /** */
    private boolean keepBinary;

    /** */
    private transient ConcurrentMap<Integer, CacheContinuousQueryPartitionRecovery> rcvs;

    /** */
    private transient ConcurrentMap<Integer, CacheContinuousQueryEventBuffer> entryBufs;

    /** */
    private transient CacheContinuousQueryAcknowledgeBuffer ackBuf;

    /** */
    private transient int cacheId;

    /** */
    private transient volatile Map<Integer, T2<Long, Long>> initUpdCntrs;

    /** */
    private transient volatile Map<UUID, Map<Integer, T2<Long, Long>>> initUpdCntrsPerNode;

    /** */
    private transient volatile AffinityTopologyVersion initTopVer;

    /** */
    private transient volatile boolean nodeLeft;

    /** */
    private transient boolean ignoreClsNotFound;

    /** */
    transient boolean asyncCb;

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
        @Nullable CacheEntryUpdatedListener<K, V> locLsnr,
        @Nullable CacheEntryEventSerializableFilter<K, V> rmtFilter,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        boolean ignoreClsNotFound) {
        assert topic != null;

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
    @Override public void updateCounters(AffinityTopologyVersion topVer, Map<UUID, Map<Integer, T2<Long, Long>>> cntrsPerNode,
        Map<Integer, T2<Long, Long>> cntrs) {
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

                asyncCb = ((JCacheQueryLocalListener)locLsnr).async();
            }
            else {
                ctx.resource().injectGeneric(locLsnr);

                asyncCb = U.hasAnnotation(locLsnr, IgniteAsyncCallback.class);
            }
        }

        final CacheEntryEventFilter filter = getEventFilter();

        if (filter != null) {
            if (filter instanceof JCacheQueryRemoteFilter) {
                if (((JCacheQueryRemoteFilter)filter).impl != null)
                    ctx.resource().injectGeneric(((JCacheQueryRemoteFilter)filter).impl);

                if (!asyncCb)
                    asyncCb = ((JCacheQueryRemoteFilter)filter).async();
            }
            else {
                ctx.resource().injectGeneric(filter);

                if (!asyncCb)
                    asyncCb = U.hasAnnotation(filter, IgniteAsyncCallback.class);
            }
        }

        entryBufs = new ConcurrentHashMap<>();

        ackBuf = new CacheContinuousQueryAcknowledgeBuffer();

        rcvs = new ConcurrentHashMap<>();

        this.nodeId = nodeId;

        this.routineId = routineId;

        this.ctx = ctx;

        final boolean loc = nodeId.equals(ctx.localNodeId());

        assert !skipPrimaryCheck || loc;

        log = ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY);

        CacheContinuousQueryListener<K, V> lsnr = new CacheContinuousQueryListener<K, V>() {
            @Override public void onExecution() {
                GridCacheContext<K, V> cctx = cacheContext(ctx);

                if (cctx != null && cctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
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

                if (asyncCb) {
                    ContinuousQueryAsyncClosure clsr = new ContinuousQueryAsyncClosure(
                        primary,
                        evt,
                        recordIgniteEvt,
                        fut);

                    ctx.asyncCallbackPool().execute(clsr, evt.partitionId());
                }
                else {
                    final boolean notify = filter(evt);

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
                            }, sync);
                        }
                    }
                    else
                        handleBackupEntry(cctx, evt.entry());
                }
            }

            @Override public void onUnregister() {
                if (filter instanceof PlatformContinuousQueryFilter)
                    ((PlatformContinuousQueryFilter)filter).onQueryUnregister();
            }

            @Override public void cleanupBackupQueue(Map<Integer, Long> updateCntrs) {
                for (Map.Entry<Integer, Long> e : updateCntrs.entrySet()) {
                    CacheContinuousQueryEventBuffer buf = entryBufs.get(e.getKey());

                    if (buf != null)
                        buf.cleanupBackupQueue(e.getValue());
                }
            }

            @Override public void flushBackupQueue(GridKernalContext ctx, AffinityTopologyVersion topVer) {
                assert topVer != null;

                try {
                    GridCacheContext<K, V> cctx = cacheContext(ctx);

                    ClusterNode node = ctx.discovery().node(nodeId);

                    for (Map.Entry<Integer, CacheContinuousQueryEventBuffer> bufE : entryBufs.entrySet()) {
                        CacheContinuousQueryEventBuffer buf = bufE.getValue();

                        Collection<CacheContinuousQueryEntry> backupQueue = buf.flushOnExchange();

                        if (backupQueue != null && node != null) {
                            for (CacheContinuousQueryEntry e : backupQueue) {
                                e.markBackup();

                                if (!e.isFiltered())
                                    prepareEntry(cctx, nodeId, e);
                            }

                            ctx.continuous().addBackupNotification(nodeId, routineId, backupQueue, topic);
                        }
                    }
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

            @Override public CounterSkipContext skipUpdateCounter(final GridCacheContext cctx,
                @Nullable CounterSkipContext skipCtx,
                int part,
                long cntr,
                AffinityTopologyVersion topVer,
                boolean primary) {
                if (skipCtx == null)
                    skipCtx = new CounterSkipContext(part, cntr, topVer);

                if (loc) {
                    assert !locCache;

                    final Collection<CacheEntryEvent<? extends K, ? extends V>> evts = handleEvent(ctx, skipCtx.entry());

                    if (!evts.isEmpty()) {
                        if (asyncCb) {
                            ctx.asyncCallbackPool().execute(new Runnable() {
                                @Override public void run() {
                                    notifyLocalListener(evts, getTransformer());
                                }
                            }, part);
                        }
                        else
                            skipCtx.addProcessClosure(new Runnable() {
                                @Override public void run() {
                                    notifyLocalListener(evts, getTransformer());
                                }
                            });
                    }

                    return skipCtx;
                }

                CacheContinuousQueryEventBuffer buf = partitionBuffer(cctx, part);

                final Object entryOrList = buf.processEntry(skipCtx.entry(), !primary);

                if (entryOrList != null) {
                    skipCtx.addProcessClosure(new Runnable() {
                        @Override public void run() {
                            try {
                                ctx.continuous().addNotification(nodeId,
                                    routineId,
                                    entryOrList,
                                    topic,
                                    false,
                                    true);
                            }
                            catch (ClusterTopologyCheckedException ex) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send event notification to node, node left cluster " +
                                        "[node=" + nodeId + ", err=" + ex + ']');
                            }
                            catch (IgniteCheckedException ex) {
                                U.error(ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY),
                                    "Failed to send event notification to node: " + nodeId, ex);
                            }
                        }
                    });
                }

                return skipCtx;
            }

            @Override public void onPartitionEvicted(int part) {
                entryBufs.remove(part);
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
     * @return Cache entry event transformer.
     */
    @Nullable protected IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> getTransformer() {
        return null;
    }

    /**
     * @return Local listener of transformed events.
     */
    @Nullable protected EventListener<?> localTransformedEventListener() {
        return null;
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
     * @param ctx Context.
     * @throws IgniteCheckedException In case of error.
     */
    void waitTopologyFuture(GridKernalContext ctx) throws IgniteCheckedException {
        GridCacheContext<K, V> cctx = cacheContext(ctx);

        if (!cctx.isLocal()) {
            AffinityTopologyVersion topVer = initTopVer;

            cacheContext(ctx).affinity().affinityReadyFuture(topVer).get();

            for (int partId = 0; partId < cacheContext(ctx).affinity().partitions(); partId++)
                getOrCreatePartitionRecovery(ctx, partId, topVer);
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

        if (asyncCb) {
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
                    depMgr.p2pContext(
                        nodeId,
                        depInfo.classLoaderId(),
                        depInfo.userVersion(),
                        depInfo.deployMode(),
                        depInfo.participants()
                    );
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

        notifyLocalListener(entries0, returnValTrans);
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

        CacheContinuousQueryPartitionRecovery rec = getOrCreatePartitionRecovery(ctx, e.partition(), e.topologyVersion());

        return rec.collectEntries(e, cctx, cache);
    }

    /**
     * @param evt Query event.
     * @return {@code True} if event passed filter otherwise {@code true}.
     */
    public boolean filter(CacheContinuousQueryEvent evt) {
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

            CacheContinuousQueryEntry entry = evt.entry();

            IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> trans = getTransformer();

            if (loc) {
                if (!locCache) {
                    Collection<CacheEntryEvent<? extends K, ? extends V>> evts = handleEvent(ctx, entry);

                    notifyLocalListener(evts, trans);

                    if (!internal && !skipPrimaryCheck)
                        sendBackupAcknowledge(ackBuf.onAcknowledged(entry), routineId, ctx);
                }
                else {
                    if (!entry.isFiltered())
                        notifyLocalListener(F.<CacheEntryEvent<? extends K, ? extends V>>asList(evt), trans);
                }
            }
            else {
                if (!entry.isFiltered()) {
                    if (trans != null)
                        entry = transformToEntry(trans, evt);

                    prepareEntry(cctx, nodeId, entry);
                }

                Object entryOrList = handleEntry(cctx, entry);

                if (entryOrList != null) {
                    if (log.isDebugEnabled())
                        log.debug("Send the following event to listener: " + entryOrList);

                    ctx.continuous().addNotification(nodeId, routineId, entryOrList, topic, sync, true);
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
     * Notifies local listener.
     *
     * @param evts Events.
     * @param trans Transformer
     */
    private void notifyLocalListener(Collection<CacheEntryEvent<? extends K, ? extends V>> evts,
        @Nullable IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> trans) {
        EventListener locTransLsnr = localTransformedEventListener();

        assert (locLsnr != null && locTransLsnr == null) || (locLsnr == null && locTransLsnr != null);

        if (F.isEmpty(evts))
            return;

        if (locLsnr != null)
            locLsnr.onUpdated(evts);

        if (locTransLsnr != null)
            locTransLsnr.onUpdated(transform(trans, evts));
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

        for (CacheContinuousQueryPartitionRecovery rec : rcvs.values())
            rec.resetTopologyCache();
    }

    /**
     * @param ctx Context.
     * @param partId Partition id.
     * @param topVer Topology version for current operation.
     * @return Partition recovery.
     */
    @NotNull private CacheContinuousQueryPartitionRecovery getOrCreatePartitionRecovery(GridKernalContext ctx,
        int partId,
        AffinityTopologyVersion topVer) {
        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        CacheContinuousQueryPartitionRecovery rec = rcvs.get(partId);

        if (rec == null) {
            T2<Long, Long> partCntrs = null;

            Map<UUID, Map<Integer, T2<Long, Long>>> initUpdCntrsPerNode = this.initUpdCntrsPerNode;

            if (initUpdCntrsPerNode != null) {
                GridCacheContext<K, V> cctx = cacheContext(ctx);

                GridCacheAffinityManager aff = cctx.affinity();

                for (ClusterNode node : aff.nodesByPartition(partId, topVer)) {
                    Map<Integer, T2<Long, Long>> map = initUpdCntrsPerNode.get(node.id());

                    if (map != null) {
                        partCntrs = map.get(partId);

                        break;
                    }
                }
            }
            else if (initUpdCntrs != null)
                partCntrs = initUpdCntrs.get(partId);

            rec = new CacheContinuousQueryPartitionRecovery(ctx.log(CU.CONTINUOUS_QRY_LOG_CATEGORY), topVer,
                partCntrs != null ? partCntrs.get2() : null);

            CacheContinuousQueryPartitionRecovery oldRec = rcvs.putIfAbsent(partId, rec);

            if (oldRec != null)
                rec = oldRec;
        }

        return rec;
    }

    /**
     * @param cctx Cache context.
     * @param e Entry.
     */
    private void handleBackupEntry(final GridCacheContext cctx, CacheContinuousQueryEntry e) {
        if (internal || e.updateCounter() == -1L || nodeLeft) // Skip internal query and expire entries.
            return;

        CacheContinuousQueryEventBuffer buf = partitionBuffer(cctx, e.partition());

        buf.processEntry(e.copyWithDataReset(), true);
    }

    /**
     * @param cctx Cache context.
     * @param e Entry.
     * @return Entry.
     */
    private Object handleEntry(final GridCacheContext cctx, CacheContinuousQueryEntry e) {
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
        if (e.updateCounter() == -1L)
            return e;

        CacheContinuousQueryEventBuffer buf = partitionBuffer(cctx, e.partition());

        return buf.processEntry(e, false);
    }

    /**
     * @param cctx Cache context.
     * @param part Partition.
     * @return Event buffer.
     */
    private CacheContinuousQueryEventBuffer partitionBuffer(final GridCacheContext cctx, int part) {
        CacheContinuousQueryEventBuffer buf = entryBufs.get(part);

        if (buf == null) {
            buf = new CacheContinuousQueryEventBuffer(part) {
                @Override protected long currentPartitionCounter() {
                    GridDhtLocalPartition locPart = cctx.topology().localPartition(part, null, false);

                    if (locPart == null)
                        return -1L;

                    return locPart.updateCounter();
                }
            };

            CacheContinuousQueryEventBuffer oldBuf = entryBufs.putIfAbsent(part, buf);

            if (oldBuf != null)
                buf = oldBuf;
        }

        return buf;
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft() {
        nodeLeft = true;

        for (Map.Entry<Integer, CacheContinuousQueryEventBuffer> bufE : entryBufs.entrySet()) {
            CacheContinuousQueryEventBuffer buf = bufE.getValue();

            buf.flushOnExchange();
        }
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (rmtFilter != null && !U.isGrid(rmtFilter.getClass()))
            rmtFilterDep = new CacheContinuousQueryDeployableObject(rmtFilter, ctx);
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
                        for (ClusterNode node : ctx.discovery().cacheGroupAffinityNodes(cctx.groupId(), topVer)) {
                            if (!node.isLocal()) {
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
        out.writeBoolean(keepBinary);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        topic = in.readObject();

        boolean b = in.readBoolean();

        if (b)
            rmtFilterDep = (CacheContinuousQueryDeployableObject)in.readObject();
        else
            rmtFilter = (CacheEntryEventSerializableFilter<K, V>)in.readObject();

        internal = in.readBoolean();
        notifyExisting = in.readBoolean();
        oldValRequired = in.readBoolean();
        sync = in.readBoolean();
        ignoreExpired = in.readBoolean();
        taskHash = in.readInt();
        keepBinary = in.readBoolean();

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
            final boolean notify = filter(evt);

            if (primary || skipPrimaryCheck) {
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
            else
                handleBackupEntry(cacheContext(ctx), evt.entry());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ContinuousQueryAsyncClosure.class, this);
        }
    }

    /**
     * @param trans Transformer.
     * @param evts Source events.
     * @return Collection of transformed values.
     */
    private Iterable transform(final IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> trans,
        Collection<CacheEntryEvent<? extends K, ? extends V>> evts) {
        final Iterator<CacheEntryEvent<? extends K, ? extends V>> iter = evts.iterator();

        return new Iterable() {
            @NotNull @Override public Iterator iterator() {
                return new Iterator() {
                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override public Object next() {
                        return transform(trans, iter.next());
                    }
                };
            }
        };
    }

    /**
     * Transform event data with {@link #getTransformer()} if exists.
     *
     * @param trans Transformer.
     * @param evt Event to transform.
     * @return Entry contains only transformed data if transformer exists. Unchanged event if transformer is not set.
     * @see #getTransformer()
     */
    private CacheContinuousQueryEntry transformToEntry(IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> trans,
        CacheContinuousQueryEvent<? extends K, ? extends V> evt) {
        Object transVal = transform(trans, evt);

        return new CacheContinuousQueryEntry(evt.entry().cacheId(),
            evt.entry().eventType(),
            null,
            transVal == null ? null : cacheContext(ctx).toCacheObject(transVal),
            null,
            evt.entry().isKeepBinary(),
            evt.entry().partition(),
            evt.entry().updateCounter(),
            evt.entry().topologyVersion(),
            evt.entry().flags());
    }

    /**
     * @param trans Transformer.
     * @param evt Event.
     * @return Transformed value.
     */
    private Object transform(IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> trans,
        CacheEntryEvent<? extends K, ? extends V> evt) {
        assert trans != null;

        Object transVal = null;

        try {
            transVal = trans.apply(evt);
        }
        catch (Exception e) {
            U.error(log, e);
        }

        return transVal;
    }
}
