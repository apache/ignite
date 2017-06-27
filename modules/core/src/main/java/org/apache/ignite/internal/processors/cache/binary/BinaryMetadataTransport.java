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

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
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
    /** */
    private final GridDiscoveryManager discoMgr;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final UUID locNodeId;

    /** */
    private final boolean clientNode;

    /** */
    private final ConcurrentMap<Integer, BinaryMetadataHolder> metaLocCache;

    /** */
    private final Queue<MetadataUpdateResultFuture> unlabeledFutures = new ConcurrentLinkedQueue<>();

    /** */
    private final ConcurrentMap<SyncKey, MetadataUpdateResultFuture> syncMap = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Integer, ClientMetadataRequestFuture> clientReqSyncMap = new ConcurrentHashMap8<>();

    /** */
    private volatile boolean stopping;

    /** */
    private final List<BinaryMetadataUpdatedListener> binaryUpdatedLsnrs = new CopyOnWriteArrayList<>();

    /**
     * @param metaLocCache Metadata locale cache.
     * @param ctx Context.
     * @param log Logger.
     */
    BinaryMetadataTransport(ConcurrentMap<Integer, BinaryMetadataHolder> metaLocCache, final GridKernalContext ctx, IgniteLogger log) {
        this.metaLocCache = metaLocCache;

        this.ctx = ctx;

        this.log = log;

        discoMgr = ctx.discovery();

        locNodeId = ctx.localNodeId();

        clientNode = ctx.clientNode();

        discoMgr.setCustomEventListener(MetadataUpdateProposedMessage.class, new MetadataUpdateProposedListener());

        discoMgr.setCustomEventListener(MetadataUpdateAcceptedMessage.class, new MetadataUpdateAcceptedListener());

        GridIoManager ioMgr = ctx.io();

        if (clientNode)
            ioMgr.addMessageListener(GridTopic.TOPIC_METADATA_REQ, new MetadataResponseListener());
        else
            ioMgr.addMessageListener(GridTopic.TOPIC_METADATA_REQ, new MetadataRequestListener(ioMgr));

        if (clientNode)
            ctx.event().addLocalEventListener(new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    DiscoveryEvent evt0 = (DiscoveryEvent) evt;

                    if (!ctx.isStopping()) {
                        for (ClientMetadataRequestFuture fut : clientReqSyncMap.values())
                            fut.onNodeLeft(evt0.eventNode().id());
                    }
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
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
     * @param metadata Metadata proposed for update.
     * @return Future to wait for update result on.
     */
    GridFutureAdapter<MetadataUpdateResult> requestMetadataUpdate(BinaryMetadata metadata) throws IgniteCheckedException {
        MetadataUpdateResultFuture resFut = new MetadataUpdateResultFuture();

        if (log.isDebugEnabled())
            log.debug("Requesting metadata update for " + metadata.typeId());

        synchronized (this) {
            unlabeledFutures.add(resFut);

            if (!stopping)
                discoMgr.sendCustomEvent(new MetadataUpdateProposedMessage(metadata, locNodeId));
            else
                resFut.onDone(MetadataUpdateResult.createUpdateDisabledResult());
        }

        return resFut;
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
            resFut.onDone(MetadataUpdateResult.createSuccessfulResult());

        return resFut;
    }

    /**
     * Allows client node to request latest version of binary metadata for a given typeId from the cluster
     * in case client is able to detect that it has obsolete metadata in its local cache.
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

        for (MetadataUpdateResultFuture fut : syncMap.values())
            fut.onDone(res);

        for (ClientMetadataRequestFuture fut : clientReqSyncMap.values())
            fut.onDone(res);
    }

    /** */
    private final class MetadataUpdateProposedListener implements CustomEventListener<MetadataUpdateProposedMessage> {

        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MetadataUpdateProposedMessage msg) {
            int typeId = msg.typeId();

            BinaryMetadataHolder holder = metaLocCache.get(typeId);

            int pendingVer;
            int acceptedVer;

            if (msg.pendingVersion() == 0) {
                //coordinator receives update request
                if (holder != null) {
                    pendingVer = holder.pendingVersion() + 1;
                    acceptedVer = holder.acceptedVersion();
                }
                else {
                    pendingVer = 1;
                    acceptedVer = 0;
                }

                if (log.isDebugEnabled())
                    log.debug("Versions are stamped on coordinator" +
                        " [typeId=" + typeId +
                        ", pendingVer=" + pendingVer +
                        ", acceptedVer=" + acceptedVer + "]"
                    );

                msg.pendingVersion(pendingVer);
                msg.acceptedVersion(acceptedVer);

                BinaryMetadata locMeta = holder != null ? holder.metadata() : null;

                try {
                    BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(locMeta, msg.metadata());

                    msg.metadata(mergedMeta);
                }
                catch (BinaryObjectException err) {
                    log.warning("Exception with merging metadata for typeId: " + typeId, err);

                    msg.markRejected(err);
                }
            }
            else {
                pendingVer = msg.pendingVersion();
                acceptedVer = msg.acceptedVersion();
            }

            if (locNodeId.equals(msg.origNodeId())) {
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

                                    fut.onDone(MetadataUpdateResult.createSuccessfulResult());

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
                    }
                }
            }
            else {
                if (!msg.rejected()) {
                    BinaryMetadata locMeta = holder != null ? holder.metadata() : null;

                    try {
                        BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(locMeta, msg.metadata());

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

                                } while (!metaLocCache.replace(typeId, holder, newHolder));
                            }
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Updated metadata on server node: " + newHolder);

                            metaLocCache.put(typeId, newHolder);
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
     * @param typeId Type ID.
     * @param pendingVer Pending version.
     * @param fut Future.
     */
    private void initSyncFor(int typeId, int pendingVer, MetadataUpdateResultFuture fut) {
        if (stopping) {
            fut.onDone(MetadataUpdateResult.createUpdateDisabledResult());

            return;
        }

        SyncKey key = new SyncKey(typeId, pendingVer);

        syncMap.put(key, fut);

        fut.key(key);
    }

    /**
     *
     */
    private final class MetadataUpdateAcceptedListener implements CustomEventListener<MetadataUpdateAcceptedMessage> {

        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MetadataUpdateAcceptedMessage msg) {
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
                            ", newAcceptedVer: " + newAcceptedVer + ']');

                    //this is duplicate ack
                    msg.duplicated(true);

                    return;
                }

                metaLocCache.put(typeId, new BinaryMetadataHolder(holder.metadata(), holder.pendingVersion(), newAcceptedVer));
            }

            for (BinaryMetadataUpdatedListener lsnr : binaryUpdatedLsnrs)
                lsnr.binaryMetadataUpdated(holder.metadata());

            GridFutureAdapter<MetadataUpdateResult> fut = syncMap.get(new SyncKey(typeId, newAcceptedVer));

            if (log.isDebugEnabled())
                log.debug("Completing future for " + metaLocCache.get(typeId));

            if (fut != null)
                fut.onDone(MetadataUpdateResult.createSuccessfulResult());
        }
    }

    /**
     * Future class responsible for blocking threads until particular events with metadata updates happen,
     * e.g. arriving {@link MetadataUpdateAcceptedMessage} acknowledgment or {@link MetadataResponseMessage} response.
     */
    private final class MetadataUpdateResultFuture extends GridFutureAdapter<MetadataUpdateResult> {
        /** */
        MetadataUpdateResultFuture() {
            // No-op.
        }

        /**
         * @param key key in syncMap this future was added under.
         */
        MetadataUpdateResultFuture(SyncKey key) {
            this.key = key;
        }

        /** */
        private SyncKey key;

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable MetadataUpdateResult res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, err);

            if (done && key != null)
                syncMap.remove(key, this);

            return done;
        }

        /**
         * @param key Key.
         */
        void key(SyncKey key) {
            this.key = key;
        }
    }

    /**
     * Key for mapping arriving {@link MetadataUpdateAcceptedMessage} messages
     * to {@link MetadataUpdateResultFuture}s other threads may be waiting on.
     */
    private static final class SyncKey {
        /** */
        private final int typeId;

        /** */
        private final int ver;

        /**
         * @param typeId Type id.
         * @param ver Version.
         */
        private SyncKey(int typeId, int ver) {
            this.typeId = typeId;
            this.ver = ver;
        }

        /** */
        int typeId() {
            return typeId;
        }

        /** */
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

            SyncKey that = (SyncKey) o;

            return (typeId == that.typeId) && (ver == that.ver);
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
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert msg instanceof MetadataRequestMessage : msg;

            MetadataRequestMessage msg0 = (MetadataRequestMessage) msg;

            int typeId = msg0.typeId();

            BinaryMetadataHolder metaHolder = metaLocCache.get(typeId);

            MetadataResponseMessage resp = new MetadataResponseMessage(typeId);

            byte[] binMetaBytes = null;

            if (metaHolder != null) {
                try {
                    binMetaBytes = U.marshal(ctx, metaHolder);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to marshal binary metadata for [typeId: " + typeId + "]", e);

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
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert msg instanceof MetadataResponseMessage : msg;

            MetadataResponseMessage msg0 = (MetadataResponseMessage) msg;

            int typeId = msg0.typeId();

            byte[] binMetaBytes = msg0.binaryMetadataBytes();

            ClientMetadataRequestFuture fut = clientReqSyncMap.get(typeId);

            if (fut == null)
                return;

            if (msg0.metadataNotFound()) {
                fut.onDone(MetadataUpdateResult.createSuccessfulResult());

                return;
            }

            try {
                BinaryMetadataHolder newHolder = U.unmarshal(ctx, binMetaBytes, U.resolveClassLoader(ctx.config()));

                BinaryMetadataHolder oldHolder = metaLocCache.putIfAbsent(typeId, newHolder);

                if (oldHolder != null) {
                    do {
                        oldHolder = metaLocCache.get(typeId);

                        if (oldHolder != null && obsoleteUpdate(
                                oldHolder.pendingVersion(),
                                oldHolder.acceptedVersion(),
                                newHolder.pendingVersion(),
                                newHolder.acceptedVersion()))
                            break;
                    }
                    while (!metaLocCache.replace(typeId, oldHolder, newHolder));
                }

                fut.onDone(MetadataUpdateResult.createSuccessfulResult());
            }
            catch (IgniteCheckedException e) {
                fut.onDone(MetadataUpdateResult.createFailureResult(new BinaryObjectException(e)));
            }
        }


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
}
