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
package org.apache.ignite.internal.processors.cache.binary;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.binary.BinaryUtils.mergeMetadata;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Provides API for discovery-based metadata exchange protocol and communication SPI-based metadata request protocol.
 *
 * It is responsible for sending update and metadata requests and manages message listeners for them.
 *
 * It also manages synchronization logic (blocking/unblocking threads requesting updates or up-to-date metadata etc)
 * around protocols.
 */
final class BinaryMetadataTransport {
    /** Special metadata version for remove. */
    public static final int REMOVED_VERSION = -2;

    /** */
    private final GridDiscoveryManager discoMgr;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final boolean clientNode;

    /** */
    private final ConcurrentMap<Integer, BinaryMetadataHolder> metaLocCache;

    /** */
    private final BinaryMetadataFileStore metadataFileStore;

    /** */
    private final Queue<MetadataUpdateResultFuture> unlabeledFutures = new ConcurrentLinkedQueue<>();

    /** */
    private final ConcurrentMap<SyncKey, MetadataUpdateResultFuture> syncMap = new ConcurrentHashMap<>();

    /** It store pending update future for typeId. It allow to do only one update in one moment. */
    private final ConcurrentMap<Integer, GridFutureAdapter<?>> pendingTypeIdMap = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Integer, ClientMetadataRequestFuture> clientReqSyncMap = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<SyncKey, GridFutureAdapter<?>> schemaWaitFuts = new ConcurrentHashMap<>();

    /** */
    private final List<BinaryMetadataUpdatedListener> binaryUpdatedLsnrs = new CopyOnWriteArrayList<>();

    /** */
    private final BinaryContext binCtx;

    /** */
    private final boolean isPersistenceEnabled;

    /** */
    private volatile boolean stopping;

    /**
     * @param metaLocCache Metadata locale cache.
     * @param metadataFileStore File store for binary metadata.
     * @param ctx Context.
     * @param log Logger.
     */
    BinaryMetadataTransport(
        ConcurrentMap<Integer, BinaryMetadataHolder> metaLocCache,
        BinaryMetadataFileStore metadataFileStore,
        BinaryContext binCtx,
        final GridKernalContext ctx,
        IgniteLogger log
    ) {
        this.metaLocCache = metaLocCache;
        this.metadataFileStore = metadataFileStore;
        this.ctx = ctx;
        this.binCtx = binCtx;
        this.log = log;

        discoMgr = ctx.discovery();

        clientNode = ctx.clientNode();
        isPersistenceEnabled = CU.isPersistenceEnabled(ctx.config()) && !clientNode;

        discoMgr.setCustomEventListener(MetadataUpdateProposedMessage.class, new MetadataUpdateProposedListener());
        discoMgr.setCustomEventListener(MetadataUpdateAcceptedMessage.class, new MetadataUpdateAcceptedListener());
        discoMgr.setCustomEventListener(MetadataRemoveProposedMessage.class, new MetadataRemoveProposedListener());
        discoMgr.setCustomEventListener(MetadataRemoveAcceptedMessage.class, new MetadataRemoveAcceptedListener());

        GridIoManager ioMgr = ctx.io();

        if (clientNode)
            ioMgr.addMessageListener(GridTopic.TOPIC_METADATA_REQ, new MetadataResponseListener());
        else
            ioMgr.addMessageListener(GridTopic.TOPIC_METADATA_REQ, new MetadataRequestListener(ioMgr));

        if (clientNode)
            ctx.event().addLocalEventListener(new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    DiscoveryEvent evt0 = (DiscoveryEvent)evt;

                    if (!ctx.isStopping()) {
                        for (ClientMetadataRequestFuture fut : clientReqSyncMap.values())
                            fut.onNodeLeft(evt0.eventNode().id());
                    }
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * Method checks if arrived metadata is obsolete comparing to the one from local cache.
     *
     * @param locP pendingVersion of metadata from local cache.
     * @param locA acceptedVersion of metadata from local cache.
     * @param remP pendingVersion of metadata from arrived message (client response/proposed/accepted).
     * @param remA acceptedVersion of metadata from arrived message (client response/proposed/accepted).
     * @return {@code true} is
     */
    private static boolean obsoleteUpdate(int locP, int locA, int remP, int remA) {
        return (remP < locP) || (remP == locP && remA < locA);
    }

    /**
     * Adds BinaryMetadata updates {@link BinaryMetadataUpdatedListener listener} to transport.
     *
     * @param lsnr Listener.
     */
    void addBinaryMetadataUpdateListener(BinaryMetadataUpdatedListener lsnr) {
        binaryUpdatedLsnrs.add(lsnr);
    }

    /**
     * Sends request to cluster proposing update for given metadata.
     *
     * @param newMeta Metadata proposed for update.
     * @return Future to wait for update result on.
     */
    GridFutureAdapter<MetadataUpdateResult> requestMetadataUpdate(BinaryMetadata newMeta) {
        int typeId = newMeta.typeId();

        MetadataUpdateResultFuture resFut;

        do {
            BinaryMetadataHolder metaHolder = metaLocCache.get(typeId);

            if (metaHolder != null && metaHolder.removing())
                throw new IgniteException("The metadata is removing for type [typeId=" + typeId + ']');

            BinaryMetadata oldMeta = Optional.ofNullable(metaHolder)
                .map(BinaryMetadataHolder::metadata)
                .orElse(null);

            BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta, null);

            if (mergedMeta == oldMeta) {
                if (metaHolder.pendingVersion() == metaHolder.acceptedVersion())
                    return null;

                return awaitMetadataUpdate(typeId, metaHolder.pendingVersion());
            }

            resFut = new MetadataUpdateResultFuture(typeId);
        }
        while (!putAndWaitPendingUpdate(typeId, resFut));

        BinaryMetadataHolder metadataHolder = metaLocCache.get(typeId);

        BinaryMetadata oldMeta = Optional.ofNullable(metadataHolder)
            .map(BinaryMetadataHolder::metadata)
            .orElse(null);

        Set<Integer> changedSchemas = new LinkedHashSet<>();

        //Ensure after putting pending future, metadata still has difference.
        BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta, changedSchemas);

        if (mergedMeta == oldMeta) {
            resFut.onDone(MetadataUpdateResult.createSuccessfulResult(-1));

            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Requesting metadata update [typeId=" + typeId +
                ", typeName=" + mergedMeta.typeName() +
                ", changedSchemas=" + changedSchemas +
                ", holder=" + metadataHolder +
                ", fut=" + resFut +
                ']');
        }

        try {
            synchronized (this) {
                unlabeledFutures.add(resFut);

                if (!stopping)
                    discoMgr.sendCustomEvent(new MetadataUpdateProposedMessage(mergedMeta, ctx.localNodeId()));
                else
                    resFut.onDone(MetadataUpdateResult.createUpdateDisabledResult());
            }
        }
        catch (Exception e) {
            resFut.onDone(MetadataUpdateResult.createUpdateDisabledResult(), e);
        }

        if (ctx.clientDisconnected())
            onDisconnected();

        return resFut;
    }

    /**
     * Put new update future and it are waiting pending future if it exists.
     *
     * @param typeId Type id.
     * @param newMetaFut New metadata update / remove future.
     * @return {@code true} If given future put successfully.
     */
    private boolean putAndWaitPendingUpdate(int typeId, GridFutureAdapter<?> newMetaFut) {
        GridFutureAdapter<?> oldFut = pendingTypeIdMap.putIfAbsent(typeId, newMetaFut);

        if (oldFut != null) {
            try {
                oldFut.get();
            }
            catch (IgniteCheckedException ignore) {
                //Stacktrace will be logged in thread which created this future.
                log.warning("Pending update metadata process was failed. Trying to update to new metadata.");
            }

            return false;
        }

        return true;
    }

    /**
     * Allows thread to wait for a metadata of given typeId and version to be accepted by the cluster.
     *
     * @param typeId ID of binary type.
     * @param ver version of given binary type (see {@link MetadataUpdateProposedMessage} javadoc for more details).
     * @return future to wait for update result on.
     */
    GridFutureAdapter<MetadataUpdateResult> awaitMetadataUpdate(int typeId, int ver) {
        SyncKey key = new SyncKey(typeId, ver);
        MetadataUpdateResultFuture resFut = new MetadataUpdateResultFuture(key);

        MetadataUpdateResultFuture oldFut = syncMap.putIfAbsent(key, resFut);

        if (oldFut != null)
            resFut = oldFut;

        BinaryMetadataHolder holder = metaLocCache.get(typeId);

        if (holder.acceptedVersion() >= ver)
            resFut.onDone(MetadataUpdateResult.createSuccessfulResult(-1));

        return resFut;
    }

    /**
     * Allows thread to wait for a metadata of given typeId and version to be removed.
     *
     * @param typeId ID of binary type.
     * @return future to wait for update result on.
     */
    GridFutureAdapter<MetadataUpdateResult> awaitMetadataRemove(int typeId) {
        SyncKey key = new SyncKey(typeId, REMOVED_VERSION);
        MetadataUpdateResultFuture resFut = new MetadataUpdateResultFuture(key);

        MetadataUpdateResultFuture oldFut = syncMap.putIfAbsent(key, resFut);

        if (oldFut != null)
            resFut = oldFut;

        if (!metaLocCache.containsKey(typeId))
            resFut.onDone(MetadataUpdateResult.createSuccessfulResult(-1));

        return resFut;
    }

    /**
     * Await specific schema update.
     *
     * @param typeId Type id.
     * @param schemaId Schema id.
     * @return Future which will be completed when schema is received.
     */
    GridFutureAdapter<?> awaitSchemaUpdate(int typeId, int schemaId) {
        GridFutureAdapter<Object> fut = new GridFutureAdapter<>();

        // Use version for schemaId.
        GridFutureAdapter<?> oldFut = schemaWaitFuts.putIfAbsent(new SyncKey(typeId, schemaId), fut);

        return oldFut == null ? fut : oldFut;
    }

    /**
     * Allows client node to request latest version of binary metadata for a given typeId from the cluster in case
     * client is able to detect that it has obsolete metadata in its local cache.
     *
     * @param typeId ID of binary type.
     * @return future to wait for request arrival on.
     */
    GridFutureAdapter<MetadataUpdateResult> requestUpToDateMetadata(int typeId) {
        ClientMetadataRequestFuture newFut = new ClientMetadataRequestFuture(ctx, typeId, clientReqSyncMap);

        ClientMetadataRequestFuture oldFut = clientReqSyncMap.putIfAbsent(typeId, newFut);

        if (oldFut != null)
            return oldFut;

        newFut.requestMetadata();

        return newFut;
    }

    /** */
    void stop() {
        stopping = true;

        cancelFutures(MetadataUpdateResult.createUpdateDisabledResult());
    }

    /** */
    void onDisconnected() {
        cancelFutures(MetadataUpdateResult.createFailureResult(new BinaryObjectException("Failed to update or wait for metadata, client node disconnected")));
    }

    /**
     * @param res result to cancel futures with.
     */
    private void cancelFutures(MetadataUpdateResult res) {
        for (MetadataUpdateResultFuture fut : unlabeledFutures)
            fut.onDone(res);

        unlabeledFutures.clear();

        for (MetadataUpdateResultFuture fut : syncMap.values())
            fut.onDone(res);

        for (ClientMetadataRequestFuture fut : clientReqSyncMap.values())
            fut.onDone(res);
    }

    /**
     * Remove metadata from cluster for specified type.
     *
     * @param typeId Type ID to remove metadata.
     */
    public GridFutureAdapter<MetadataUpdateResult> requestMetadataRemove(int typeId) {
        MetadataUpdateResultFuture resFut;

        do {
            resFut = new MetadataUpdateResultFuture(typeId);
        }
        while (!putAndWaitPendingUpdate(typeId, resFut));

        try {
            synchronized (this) {
                unlabeledFutures.add(resFut);

                if (!stopping)
                    discoMgr.sendCustomEvent(new MetadataRemoveProposedMessage(typeId, ctx.localNodeId()));
                else
                    resFut.onDone(MetadataUpdateResult.createUpdateDisabledResult());
            }
        }
        catch (Exception e) {
            resFut.onDone(MetadataUpdateResult.createUpdateDisabledResult(), e);
        }

        if (ctx.clientDisconnected())
            onDisconnected();

        return resFut;
    }

    /**
     * @param typeId Type ID.
     * @param pendingVer Pending version.
     * @param fut Future.
     */
    private void initSyncFor(int typeId, int pendingVer, final MetadataUpdateResultFuture fut) {
        if (stopping) {
            fut.onDone(MetadataUpdateResult.createUpdateDisabledResult());

            return;
        }

        SyncKey key = new SyncKey(typeId, pendingVer);

        MetadataUpdateResultFuture oldFut = syncMap.putIfAbsent(key, fut);

        if (oldFut != null) {
            oldFut.listen(new IgniteInClosure<IgniteInternalFuture<MetadataUpdateResult>>() {
                @Override public void apply(IgniteInternalFuture<MetadataUpdateResult> doneFut) {
                    fut.onDone(doneFut.result(), doneFut.error());
                }
            });
        }

        fut.key(key);
    }

    /**
     * Key for mapping arriving {@link MetadataUpdateAcceptedMessage} messages to {@link MetadataUpdateResultFuture}s
     * other threads may be waiting on.
     */
    private static final class SyncKey {
        /**
         *
         */
        private final int typeId;

        /**
         *
         */
        private final int ver;

        /**
         * @param typeId Type id.
         * @param ver Version.
         */
        private SyncKey(int typeId, int ver) {
            this.typeId = typeId;
            this.ver = ver;
        }

        /**
         * @return Type ID.
         */
        int typeId() {
            return typeId;
        }

        /**
         * @return Version.
         */
        int version() {
            return ver;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return typeId + ver;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == this)
                return true;

            if (!(o instanceof SyncKey))
                return false;

            SyncKey that = (SyncKey)o;

            return (typeId == that.typeId) && (ver == that.ver);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SyncKey.class, this);
        }
    }

    /**
     *
     */
    private final class MetadataUpdateProposedListener implements CustomEventListener<MetadataUpdateProposedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            MetadataUpdateProposedMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Received MetadataUpdateProposed message [typeId=" + msg.typeId() +
                    ", typeName=" + msg.metadata().typeName() +
                    ", pendingVer=" + msg.pendingVersion() +
                    ", acceptedVer=" + msg.acceptedVersion() +
                    ", schemasCnt=" + msg.metadata().schemas().size() + ']');

            int typeId = msg.typeId();

            BinaryMetadataHolder holder = metaLocCache.get(typeId);

            int pendingVer;
            int acceptedVer;

            if (msg.pendingVersion() == 0) {
                //coordinator receives update request
                if (holder != null) {
                    if (holder.removing()) {
                        msg.markRejected(new BinaryObjectException("The type is removing now [typeId=" + typeId + ']'));

                        pendingVer = REMOVED_VERSION;
                        acceptedVer = REMOVED_VERSION;
                    }
                    else {
                        pendingVer = holder.pendingVersion() + 1;
                        acceptedVer = holder.acceptedVersion();
                    }
                }
                else {
                    pendingVer = 1;
                    acceptedVer = 0;
                }

                if (!msg.rejected()) {
                    msg.pendingVersion(pendingVer);
                    msg.acceptedVersion(acceptedVer);

                    BinaryMetadata locMeta = holder != null ? holder.metadata() : null;

                    try {
                        Set<Integer> changedSchemas = new LinkedHashSet<>();

                        BinaryMetadata mergedMeta = mergeMetadata(locMeta, msg.metadata(), changedSchemas);

                        if (log.isDebugEnabled())
                            log.debug("Versions are stamped on coordinator" +
                                " [typeId=" + typeId +
                                ", changedSchemas=" + changedSchemas +
                                ", pendingVer=" + pendingVer +
                                ", acceptedVer=" + acceptedVer + "]"
                            );

                        msg.metadata(mergedMeta);
                    }
                    catch (BinaryObjectException err) {
                        log.warning("Exception with merging metadata for typeId: " + typeId, err);

                        msg.markRejected(err);
                    }
                }
            }
            else {
                pendingVer = msg.pendingVersion();
                acceptedVer = msg.acceptedVersion();
            }

            if (ctx.localNodeId().equals(msg.origNodeId())) {
                MetadataUpdateResultFuture fut = unlabeledFutures.poll();

                if (msg.rejected())
                    fut.onDone(MetadataUpdateResult.createFailureResult(msg.rejectionError()));
                else {
                    if (clientNode) {
                        BinaryMetadataHolder newHolder = new BinaryMetadataHolder(msg.metadata(), pendingVer, acceptedVer);

                        holder = metaLocCache.putIfAbsent(typeId, newHolder);

                        if (holder != null) {
                            boolean obsoleteUpd = false;

                            do {
                                holder = metaLocCache.get(typeId);

                                if (obsoleteUpdate(
                                    holder.pendingVersion(),
                                    holder.acceptedVersion(),
                                    pendingVer,
                                    acceptedVer)) {
                                    obsoleteUpd = true;

                                    fut.onDone(MetadataUpdateResult.createSuccessfulResult(-1));

                                    break;
                                }
                            }
                            while (!metaLocCache.replace(typeId, holder, newHolder));

                            if (!obsoleteUpd)
                                initSyncFor(typeId, pendingVer, fut);
                        }
                        else
                            initSyncFor(typeId, pendingVer, fut);
                    }
                    else {
                        initSyncFor(typeId, pendingVer, fut);

                        BinaryMetadataHolder newHolder = new BinaryMetadataHolder(msg.metadata(), pendingVer, acceptedVer);

                        if (log.isDebugEnabled())
                            log.debug("Updated metadata on originating node: " + newHolder);

                        metaLocCache.put(typeId, newHolder);

                        metadataFileStore.prepareMetadataWriting(msg.metadata(), pendingVer);
                    }
                }
            }
            else {
                if (!msg.rejected()) {
                    BinaryMetadata locMeta = holder != null && !holder.removing() ? holder.metadata() : null;

                    Set<Integer> changedSchemas = new LinkedHashSet<>();

                    try {
                        BinaryMetadata mergedMeta = mergeMetadata(locMeta, msg.metadata(), changedSchemas);

                        BinaryMetadataHolder newHolder = new BinaryMetadataHolder(mergedMeta, pendingVer, acceptedVer);

                        if (clientNode) {
                            holder = metaLocCache.putIfAbsent(typeId, newHolder);

                            if (holder != null) {
                                do {
                                    holder = metaLocCache.get(typeId);

                                    if (obsoleteUpdate(
                                        holder.pendingVersion(),
                                        holder.acceptedVersion(),
                                        pendingVer,
                                        acceptedVer))
                                        break;

                                }
                                while (!metaLocCache.replace(typeId, holder, newHolder));
                            }
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Updated metadata on server node [holder=" + newHolder +
                                    ", changedSchemas=" + changedSchemas + ']');

                            metaLocCache.put(typeId, newHolder);

                            metadataFileStore.prepareMetadataWriting(mergedMeta, pendingVer);
                        }
                    }
                    catch (BinaryObjectException ignored) {
                        assert false : msg;
                    }
                }
            }
        }
    }

    /**
     *
     */
    private final class MetadataUpdateAcceptedListener implements CustomEventListener<MetadataUpdateAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            MetadataUpdateAcceptedMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Received MetadataUpdateAcceptedMessage " + msg);

            if (msg.duplicated())
                return;

            int typeId = msg.typeId();

            BinaryMetadataHolder holder = metaLocCache.get(typeId);

            assert holder != null : "No metadata found for typeId " + typeId;

            int newAcceptedVer = msg.acceptedVersion();

            if (clientNode) {
                BinaryMetadataHolder newHolder = new BinaryMetadataHolder(holder.metadata(),
                    holder.pendingVersion(), newAcceptedVer);

                do {
                    holder = metaLocCache.get(typeId);

                    int oldAcceptedVer = holder.acceptedVersion();

                    if (oldAcceptedVer > newAcceptedVer)
                        break;
                }
                while (!metaLocCache.replace(typeId, holder, newHolder));
            }
            else {
                int oldAcceptedVer = holder.acceptedVersion();

                if (oldAcceptedVer >= newAcceptedVer) {
                    if (log.isDebugEnabled())
                        log.debug("Marking ack as duplicate [holder=" + holder +
                            ", newAcceptedVer=" + newAcceptedVer + ']');

                    //this is duplicate ack
                    msg.duplicated(true);

                    metadataFileStore.finishWrite(typeId, newAcceptedVer);

                    return;
                }

                metadataFileStore.writeMetadataAsync(typeId, newAcceptedVer);

                metaLocCache.put(typeId,
                    new BinaryMetadataHolder(holder.metadata(), holder.pendingVersion(), newAcceptedVer));
            }

            for (BinaryMetadataUpdatedListener lsnr : binaryUpdatedLsnrs)
                lsnr.binaryMetadataUpdated(holder.metadata());

            GridFutureAdapter<MetadataUpdateResult> fut = syncMap.get(new SyncKey(typeId, newAcceptedVer));

            holder = metaLocCache.get(typeId);

            if (log.isDebugEnabled())
                log.debug("Completing future " + fut + " for " + holder);

            if (!schemaWaitFuts.isEmpty()) {
                Iterator<Map.Entry<SyncKey, GridFutureAdapter<?>>> iter = schemaWaitFuts.entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry<SyncKey, GridFutureAdapter<?>> entry = iter.next();

                    SyncKey key = entry.getKey();

                    if (key.typeId() == typeId && holder.metadata().hasSchema(key.version())) {
                        entry.getValue().onDone();

                        iter.remove();
                    }
                }
            }

            if (fut != null)
                fut.onDone(MetadataUpdateResult.createSuccessfulResult(newAcceptedVer));
        }
    }

    /**
     * Future class responsible for blocking threads until particular events with metadata updates happen, e.g. arriving
     * {@link MetadataUpdateAcceptedMessage} acknowledgment or {@link MetadataResponseMessage} response.
     */
    public final class MetadataUpdateResultFuture extends GridFutureAdapter<MetadataUpdateResult> {
        /** */
        private SyncKey key;

        /** */
        MetadataUpdateResultFuture(int typeId) {
            this.key = new SyncKey(typeId, 0);
        }

        /**
         * @param key key in syncMap this future was added under.
         */
        MetadataUpdateResultFuture(SyncKey key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable MetadataUpdateResult res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, err);

            if (done && key != null) {
                syncMap.remove(key, this);
                pendingTypeIdMap.remove(key.typeId, this);
            }

            return done;
        }

        /**
         * @param key Key.
         */
        void key(SyncKey key) {
            this.key = key;
        }

        /** */
        public int typeVersion() {
            return key.ver;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetadataUpdateResultFuture.class, this);
        }
    }

    /**
     * Listener is registered on each server node in cluster waiting for metadata requests from clients.
     */
    private final class MetadataRequestListener implements GridMessageListener {
        /** */
        private final GridIoManager ioMgr;

        /**
         * @param ioMgr IO manager.
         */
        MetadataRequestListener(GridIoManager ioMgr) {
            this.ioMgr = ioMgr;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            assert msg instanceof MetadataRequestMessage : msg;

            MetadataRequestMessage msg0 = (MetadataRequestMessage)msg;

            int typeId = msg0.typeId();

            BinaryMetadataHolder metaHolder = metaLocCache.get(typeId);

            MetadataResponseMessage resp = new MetadataResponseMessage(typeId);

            byte[] binMetaBytes = null;

            if (metaHolder != null) {
                try {
                    binMetaBytes = U.marshal(ctx, metaHolder);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to marshal binary metadata for [typeId=" + typeId + ']', e);

                    resp.markErrorOnRequest();
                }
            }

            resp.binaryMetadataBytes(binMetaBytes);

            try {
                ioMgr.sendToGridTopic(nodeId, GridTopic.TOPIC_METADATA_REQ, resp, SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send metadata response, node failed: " + nodeId);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send up-to-date metadata response.", e);
            }
        }
    }

    /**
     * Listener is registered on each client node and listens for metadata responses from cluster.
     */
    private final class MetadataResponseListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            assert msg instanceof MetadataResponseMessage : msg;

            MetadataResponseMessage msg0 = (MetadataResponseMessage)msg;

            int typeId = msg0.typeId();

            byte[] binMetaBytes = msg0.binaryMetadataBytes();

            ClientMetadataRequestFuture fut = clientReqSyncMap.get(typeId);

            if (fut == null)
                return;

            if (msg0.metadataNotFound()) {
                fut.onDone(MetadataUpdateResult.createSuccessfulResult(-1));

                return;
            }

            try {
                BinaryMetadataHolder newHolder = U.unmarshal(ctx, binMetaBytes, U.resolveClassLoader(ctx.config()));

                BinaryMetadataHolder oldHolder = metaLocCache.putIfAbsent(typeId, newHolder);

                if (oldHolder != null) {
                    do {
                        oldHolder = metaLocCache.get(typeId);

                        // typeId metadata cannot be removed after initialization.
                        if (obsoleteUpdate(
                            oldHolder.pendingVersion(),
                            oldHolder.acceptedVersion(),
                            newHolder.pendingVersion(),
                            newHolder.acceptedVersion()))
                            break;
                    }
                    while (!metaLocCache.replace(typeId, oldHolder, newHolder));
                }

                fut.onDone(MetadataUpdateResult.createSuccessfulResult(-1));
            }
            catch (IgniteCheckedException e) {
                fut.onDone(MetadataUpdateResult.createFailureResult(new BinaryObjectException(e)));
            }
        }
    }

    /**
     *
     */
    private final class MetadataRemoveProposedListener implements CustomEventListener<MetadataRemoveProposedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            MetadataRemoveProposedMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Received MetadataRemoveProposed message: " + msg);

            int typeId = msg.typeId();

            BinaryMetadataHolder metaHld = metaLocCache.get(typeId);

            assert metaHld != null : "No metadata found for typeId: " + typeId;

            if (msg.isOnCoordinator()) {
                if (metaHld == null)
                    msg.markRejected(new BinaryObjectException("Type not found [typeId=" + typeId + ']'));

                if (metaHld.pendingVersion() != metaHld.acceptedVersion()) {
                    msg.markRejected(new BinaryObjectException(
                        "Remove type failed. " +
                            "Type is being updated now [typeId=" + typeId
                            + ", pendingVersion=" + metaHld.pendingVersion()
                            + ", acceptedVersion=" + metaHld.acceptedVersion()
                            + ']'));
                }

                if (metaHld.removing()) {
                    msg.markRejected(new BinaryObjectException(
                        "Remove type failed. " +
                            "Type is being removed now [typeId=" + typeId
                            + ']'));
                }

                msg.setOnCoordinator(false);
            }

            MetadataUpdateResultFuture fut = null;

            if (msg.origNodeId().equals(ctx.localNodeId()))
                fut = unlabeledFutures.poll();

            if (msg.rejected()) {
                if (fut != null)
                    fut.onDone(MetadataUpdateResult.createFailureResult(msg.rejectionError()));
            }
            else {
                if (fut != null)
                    initSyncFor(typeId, REMOVED_VERSION, fut);

                metaLocCache.put(typeId, metaHld.createRemoving());

                if (isPersistenceEnabled)
                    metadataFileStore.prepareMetadataRemove(typeId);
            }
        }
    }

    /**
     *
     */
    private final class MetadataRemoveAcceptedListener implements CustomEventListener<MetadataRemoveAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            MetadataRemoveAcceptedMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Received MetadataRemoveAccepted message: " + msg);

            if (msg.duplicated())
                return;

            final int typeId = msg.typeId();

            if (!metaLocCache.containsKey(typeId)) {
                msg.duplicated(true);

                return;
            }

            if (isPersistenceEnabled)
                metadataFileStore.removeMetadataAsync(typeId);

            GridFutureAdapter<MetadataUpdateResult> fut = syncMap.get(new SyncKey(typeId, REMOVED_VERSION));

            metaLocCache.remove(typeId);

            binCtx.removeType(typeId);

            if (fut != null)
                fut.onDone(MetadataUpdateResult.createSuccessfulResult(REMOVED_VERSION));
        }
    }
}
