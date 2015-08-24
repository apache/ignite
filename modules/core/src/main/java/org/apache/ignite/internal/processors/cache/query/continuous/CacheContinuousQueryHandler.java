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
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.event.*;
import javax.cache.event.EventType;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Continuous query handler.
 */
class CacheContinuousQueryHandler<K, V> implements GridContinuousHandler {
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
    private transient Collection<CacheContinuousQueryEntry> backupQueue;

    /** */
    private transient Map<Integer, Long> rcvCntrs;

    /** */
    private transient IgnitePredicate<CacheContinuousQueryEntry> dupEvtFilter;

    /** */
    private transient AcknowledgeBuffer ackBuf;

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
     * @param internal Internal flag.
     * @param notifyExisting Notify existing flag.
     * @param oldValRequired Old value required flag.
     * @param sync Synchronous flag.
     * @param ignoreExpired Ignore expired events flag.
     * @param skipPrimaryCheck Whether to skip primary check for REPLICATED cache.
     * @param taskHash Task name hash code.
     * @param locCache {@code True} if local cache.
     */
    public CacheContinuousQueryHandler(
        String cacheName,
        Object topic,
        CacheEntryUpdatedListener<K, V> locLsnr,
        CacheEntryEventSerializableFilter<K, V> rmtFilter,
        boolean internal,
        boolean notifyExisting,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        int taskHash,
        boolean skipPrimaryCheck,
        boolean locCache) {
        assert topic != null;
        assert locLsnr != null;

        this.cacheName = cacheName;
        this.topic = topic;
        this.locLsnr = locLsnr;
        this.rmtFilter = rmtFilter;
        this.internal = internal;
        this.notifyExisting = notifyExisting;
        this.oldValRequired = oldValRequired;
        this.sync = sync;
        this.ignoreExpired = ignoreExpired;
        this.taskHash = taskHash;
        this.skipPrimaryCheck = skipPrimaryCheck;

        if (locCache)
            dupEvtFilter = F.alwaysTrue();
        else {
            rcvCntrs = new ConcurrentHashMap<>();

            dupEvtFilter = new DuplicateEventFilter();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isForEvents() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isForMessaging() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isForQuery() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(final UUID nodeId, final UUID routineId, final GridKernalContext ctx)
        throws IgniteCheckedException {
        assert nodeId != null;
        assert routineId != null;
        assert ctx != null;

        if (locLsnr != null)
            ctx.resource().injectGeneric(locLsnr);

        if (rmtFilter != null)
            ctx.resource().injectGeneric(rmtFilter);

        backupQueue = new ConcurrentLinkedDeque8<>();

        ackBuf = new AcknowledgeBuffer();

        final boolean loc = nodeId.equals(ctx.localNodeId());

        assert !skipPrimaryCheck || loc;

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
                        rmtFilter,
                        null,
                        nodeId,
                        taskName()
                    ));
                }
            }

            @Override public void onEntryUpdated(CacheContinuousQueryEvent<K, V> evt,
                boolean primary,
                boolean recordIgniteEvt) {
                if (ignoreExpired && evt.getEventType() == EventType.EXPIRED)
                    return;

                GridCacheContext<K, V> cctx = cacheContext(ctx);

                // skipPrimaryCheck is set only when listen locally for replicated cache events.
                assert !skipPrimaryCheck || (cctx.isReplicated() && ctx.localNodeId().equals(nodeId));

                boolean notify = true;

                if (rmtFilter != null) {
                    try {
                        notify = rmtFilter.evaluate(evt);
                    }
                    catch (Exception e) {
                        U.error(cctx.logger(CacheContinuousQueryHandler.class), "CacheEntryEventFilter failed: " + e);
                    }
                }

                if (notify) {
                    try {
                        final CacheContinuousQueryEntry entry = evt.entry();

                        if (primary || skipPrimaryCheck) {
                            if (loc) {
                                if (dupEvtFilter.apply(entry)) {
                                    locLsnr.onUpdated(F.<CacheEntryEvent<? extends K, ? extends V>>asList(evt));

                                    if (!skipPrimaryCheck)
                                        sendBackupAcknowledge(ackBuf.onAcknowledged(entry), routineId, ctx);
                                }
                            }
                            else {
                                prepareEntry(cctx, nodeId, entry);

                                ctx.continuous().addNotification(nodeId, routineId, entry, topic, sync, true);
                            }
                        }
                        else
                            backupQueue.add(entry);
                    }
                    catch (ClusterTopologyCheckedException ex) {
                        IgniteLogger log = ctx.log(getClass());

                        if (log.isDebugEnabled())
                            log.debug("Failed to send event notification to node, node left cluster " +
                                "[node=" + nodeId + ", err=" + ex + ']');
                    }
                    catch (IgniteCheckedException ex) {
                        U.error(ctx.log(getClass()), "Failed to send event notification to node: " + nodeId, ex);
                    }

                    if (recordIgniteEvt) {
                        ctx.event().record(new CacheQueryReadEvent<>(
                            ctx.discovery().localNode(),
                            "Continuous query executed.",
                            EVT_CACHE_QUERY_OBJECT_READ,
                            CacheQueryType.CONTINUOUS.name(),
                            cacheName,
                            null,
                            null,
                            null,
                            rmtFilter,
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
            }

            @Override public void onUnregister() {
                if (rmtFilter instanceof CacheContinuousQueryFilterEx)
                    ((CacheContinuousQueryFilterEx)rmtFilter).onQueryUnregister();
            }

            @Override public void cleanupBackupQueue(Map<Integer, Long> updateIdxs) {
                Iterator<CacheContinuousQueryEntry> it = backupQueue.iterator();

                while (it.hasNext()) {
                    CacheContinuousQueryEntry backupEntry = it.next();

                    Long updateIdx = updateIdxs.get(backupEntry.partition());

                    if (updateIdx != null && backupEntry.updateIndex() <= updateIdx)
                        it.remove();
                }
            }

            @Override public void flushBackupQueue(GridKernalContext ctx, AffinityTopologyVersion topVer) {
                if (backupQueue.isEmpty())
                    return;

                try {
                    GridCacheContext<K, V> cctx = cacheContext(ctx);

                    for (CacheContinuousQueryEntry e : backupQueue)
                        prepareEntry(cctx, nodeId, e);

                    ctx.continuous().addBackupNotification(nodeId, routineId, backupQueue, topic);

                    backupQueue.clear();
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.log(getClass()), "Failed to send backup event notification to node: " + nodeId, e);
                }
            }

            @Override public void acknowledgeBackupOnTimeout(GridKernalContext ctx) {
                sendBackupAcknowledge(ackBuf.acknowledgeOnTimeout(), routineId, ctx);
            }

            @Override public void onPartitionEvicted(int part) {
                for (Iterator<CacheContinuousQueryEntry> it = backupQueue.iterator(); it.hasNext();) {
                    if (it.next().partition() == part)
                        it.remove();
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

    /** {@inheritDoc} */
    @Override public void onListenerRegistered(UUID routineId, GridKernalContext ctx) {
        // No-op.
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
    @Override public void notifyCallback(UUID nodeId, UUID routineId, Collection<?> objs, GridKernalContext ctx) {
        assert nodeId != null;
        assert routineId != null;
        assert objs != null;
        assert ctx != null;

        Collection<CacheContinuousQueryEntry> entries = (Collection<CacheContinuousQueryEntry>)objs;

        final GridCacheContext cctx = cacheContext(ctx);

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
            }
            catch (IgniteCheckedException ex) {
                U.error(ctx.log(getClass()), "Failed to unmarshal entry.", ex);
            }
        }

        final IgniteCache cache = cctx.kernalContext().cache().jcache(cctx.name());

        Iterable<CacheEntryEvent<? extends K, ? extends V>> evts = F.viewReadOnly(entries,
            new C1<CacheContinuousQueryEntry, CacheEntryEvent<? extends K, ? extends V>>() {
                @Override public CacheEntryEvent<? extends K, ? extends V> apply(CacheContinuousQueryEntry e) {
                    return new CacheContinuousQueryEvent<>(cache, cctx, e);
                }
            },
            dupEvtFilter
        );

        locLsnr.onUpdated(evts);
    }

    /**
     * @param e Entry.
     * @return {@code True} if listener should be notified.
     */
    private boolean notifyListener(CacheContinuousQueryEntry e) {
        Integer part = e.partition();

        Long cntr = rcvCntrs.get(part);

        if (cntr != null) {
            long cntr0 = cntr;

            if (e.updateIndex() > cntr0) {
                // TODO IGNITE-426: remove assert.
                assert e.updateIndex() == cntr0 + 1 : "Invalid entry [cntr=" + cntr + ", e=" + e + ']';

                rcvCntrs.put(part, e.updateIndex());
            }
            else
                return false;
        }
        else
            rcvCntrs.put(part, e.updateIndex());

        return true;
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
        return new GridContinuousBatchAdapter();
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

                    Collection<ClusterNode> nodes = new HashSet<>();

                    for (AffinityTopologyVersion topVer : t.get2())
                        nodes.addAll(ctx.discovery().cacheNodes(topVer));

                    for (ClusterNode node : nodes) {
                        if (!node.id().equals(ctx.localNodeId())) {
                            try {
                                cctx.io().send(node, msg, GridIoPolicy.SYSTEM_POOL);
                            }
                            catch (ClusterTopologyCheckedException e) {
                                IgniteLogger log = ctx.log(getClass());

                                if (log.isDebugEnabled())
                                    log.debug("Failed to send acknowledge message, node left " +
                                        "[msg=" + msg + ", node=" + node + ']');
                            }
                            catch (IgniteCheckedException e) {
                                IgniteLogger log = ctx.log(getClass());

                                U.error(log, "Failed to send acknowledge message " +
                                    "[msg=" + msg + ", node=" + node + ']', e);
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
    }

    /**
     * @param ctx Kernal context.
     * @return Cache context.
     */
    private GridCacheContext<K, V> cacheContext(GridKernalContext ctx) {
        assert ctx != null;

        GridCacheAdapter<K, V> cache = ctx.cache().internalCache(cacheName);

        return cache == null ? null : cache.context();
    }

    /** */
    private static class AcknowledgeBuffer {
        /** */
        private int size;

        /** */
        @GridToStringInclude
        private Map<Integer, Long> updateIdxs = new HashMap<>();

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
            size += batch.size();

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

            Long cntr0 = updateIdxs.get(e.partition());

            if (cntr0 == null || e.updateIndex() > cntr0)
                updateIdxs.put(e.partition(), e.updateIndex());
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

            Map<Integer, Long> idxs = new HashMap<>(updateIdxs);

            IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> res =
                new IgniteBiTuple<>(idxs, topVers);

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
    private class DuplicateEventFilter implements IgnitePredicate<CacheContinuousQueryEntry> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(CacheContinuousQueryEntry e) {
            return notifyListener(e);
        }
    }

    /**
     * Deployable object.
     */
    private static class DeployableObject implements Externalizable {
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
        private DeployableObject(Object obj, GridKernalContext ctx) throws IgniteCheckedException {
            assert obj != null;
            assert ctx != null;

            Class cls = U.detectClass(obj);

            clsName = cls.getName();

            GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy object: " + obj);

            depInfo = new GridDeploymentInfoBean(dep);

            bytes = ctx.config().getMarshaller().marshal(obj);
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

            return ctx.config().getMarshaller().unmarshal(bytes, dep.classLoader());
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
