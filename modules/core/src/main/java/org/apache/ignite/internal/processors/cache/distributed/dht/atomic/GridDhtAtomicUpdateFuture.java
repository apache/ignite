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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * DHT atomic cache backup update future.
 */
public class GridDhtAtomicUpdateFuture extends GridFutureAdapter<Void>
    implements GridCacheAtomicFuture<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Future version. */
    private final GridCacheVersion futVer;

    /** Write version. */
    private final GridCacheVersion writeVer;

    /** Force transform backup flag. */
    private boolean forceTransformBackups;

    /** Completion callback. */
    @GridToStringExclude
    private final CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb;

    /** Mappings. */
    @GridToStringInclude
    private final Map<UUID, GridDhtAtomicUpdateRequest> mappings;

    /** Entries with readers. */
    private Map<KeyCacheObject, GridDhtCacheEntry> nearReadersEntries;

    /** Update request. */
    private final GridNearAtomicUpdateRequest updateReq;

    /** Update response. */
    private final GridNearAtomicUpdateResponse updateRes;

    /** Future keys. */
    private final Collection<KeyCacheObject> keys;

    /** */
    private final boolean waitForExchange;

    /** Response count. */
    private volatile int resCnt;

    /** */
    private Map<UUID, CacheContinuousQueryListener> lsnrs;

    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    public GridDhtAtomicUpdateFuture(
        GridCacheContext cctx,
        CI2<GridNearAtomicUpdateRequest,
        GridNearAtomicUpdateResponse> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicUpdateRequest updateReq,
        GridNearAtomicUpdateResponse updateRes
    ) {
        this.cctx = cctx;
        this.writeVer = writeVer;

        futVer = cctx.versions().next(updateReq.topologyVersion());
        this.updateReq = updateReq;
        this.completionCb = completionCb;
        this.updateRes = updateRes;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtAtomicUpdateFuture.class);

        keys = new ArrayList<>(updateReq.keys().size());
        mappings = U.newHashMap(updateReq.keys().size());

        waitForExchange = !(updateReq.topologyLocked() || (updateReq.fastMap() && !updateReq.clientRequest()));
    }

    /**
     * @param lsnrs Continuous query listeners.
     */
    void listeners(@Nullable Map<UUID, CacheContinuousQueryListener> lsnrs) {
        this.lsnrs = lsnrs;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Processing node leave event [fut=" + this + ", nodeId=" + nodeId + ']');

        return registerResponse(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if request found.
     */
    private boolean registerResponse(UUID nodeId) {
        int resCnt0;

        GridDhtAtomicUpdateRequest req = mappings.get(nodeId);

        if (req != null) {
            synchronized (this) {
                if (req.onResponse()) {
                    resCnt0 = resCnt;

                    resCnt0 += 1;

                    resCnt = resCnt0;
                }
                else
                    return false;
            }

            if (resCnt0 == mappings.size())
                onDone();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        if (waitForExchange && updateReq.topologyVersion().compareTo(topVer) < 0)
            return this;

        return null;
    }

    /**
     * @param entry Entry to map.
     * @param val Value to write.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} sends previous value to backups.
     * @param prevVal Previous value.
     * @param updateCntr Partition update counter.
     */
    public void addWriteEntry(GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr) {
        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        Collection<ClusterNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        keys.add(entry.key());

        for (ClusterNode node : dhtNodes) {
            UUID nodeId = node.id();

            if (!nodeId.equals(cctx.localNodeId())) {
                GridDhtAtomicUpdateRequest updateReq = mappings.get(nodeId);

                if (updateReq == null) {
                    updateReq = new GridDhtAtomicUpdateRequest(
                        cctx.cacheId(),
                        nodeId,
                        futVer,
                        writeVer,
                        syncMode,
                        topVer,
                        forceTransformBackups,
                        this.updateReq.subjectId(),
                        this.updateReq.taskNameHash(),
                        forceTransformBackups ? this.updateReq.invokeArguments() : null,
                        cctx.deploymentEnabled(),
                        this.updateReq.keepBinary());

                    mappings.put(nodeId, updateReq);
                }

                updateReq.addWriteValue(entry.key(),
                    val,
                    entryProcessor,
                    ttl,
                    conflictExpireTime,
                    conflictVer,
                    addPrevVal,
                    entry.partition(),
                    prevVal,
                    updateCntr,
                    lsnrs != null);
            }
            else if (lsnrs != null && dhtNodes.size() == 1) {
                try {
                    cctx.continuousQueries().onEntryUpdated(
                        lsnrs,
                        entry.key(),
                        val,
                        prevVal,
                        entry.key().internal() || !cctx.userCache(),
                        entry.partition(),
                        true,
                        false,
                        updateCntr,
                        updateReq.topologyVersion());
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to send continuous query message. [key=" + entry.key() + ", newVal="
                        + val + ", err=" + e + "]");
                }
            }
        }
    }

    /**
     * @param readers Entry readers.
     * @param entry Entry.
     * @param val Value.
     * @param entryProcessor Entry processor..
     * @param ttl TTL for near cache update (optional).
     * @param expireTime Expire time for near cache update (optional).
     */
    public void addNearWriteEntries(Iterable<UUID> readers,
        GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        keys.add(entry.key());

        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        for (UUID nodeId : readers) {
            GridDhtAtomicUpdateRequest updateReq = mappings.get(nodeId);

            if (updateReq == null) {
                ClusterNode node = cctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                updateReq = new GridDhtAtomicUpdateRequest(
                    cctx.cacheId(),
                    nodeId,
                    futVer,
                    writeVer,
                    syncMode,
                    topVer,
                    forceTransformBackups,
                    this.updateReq.subjectId(),
                    this.updateReq.taskNameHash(),
                    forceTransformBackups ? this.updateReq.invokeArguments() : null,
                    cctx.deploymentEnabled(),
                    this.updateReq.keepBinary());

                mappings.put(nodeId, updateReq);
            }

            if (nearReadersEntries == null)
                nearReadersEntries = new HashMap<>();

            nearReadersEntries.put(entry.key(), entry);

            updateReq.addNearWriteValue(entry.key(),
                val,
                entryProcessor,
                ttl,
                expireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeAtomicFuture(version());

            if (err != null) {
                if (!mappings.isEmpty() && lsnrs != null) {
                    Collection<KeyCacheObject> hndKeys = new ArrayList<>(keys.size());

                    exit: for (GridDhtAtomicUpdateRequest req : mappings.values()) {
                        for (int i = 0; i < req.size(); i++) {
                            KeyCacheObject key = req.key(i);

                            if (!hndKeys.contains(key)) {
                                updateRes.addFailedKey(key, err);

                                cctx.continuousQueries().skipUpdateEvent(
                                    lsnrs,
                                    key,
                                    req.partitionId(i),
                                    req.updateCounter(i),
                                    updateReq.topologyVersion());

                                hndKeys.add(key);

                                if (hndKeys.size() == keys.size())
                                    break exit;
                            }
                        }
                    }
                }
                else
                    for (KeyCacheObject key : keys)
                        updateRes.addFailedKey(key, err);
            }
            else {
                if (lsnrs != null) {
                    Collection<KeyCacheObject> hndKeys = new ArrayList<>(keys.size());

                    exit: for (GridDhtAtomicUpdateRequest req : mappings.values()) {
                        for (int i = 0; i < req.size(); i++) {
                            KeyCacheObject key = req.key(i);

                            if (!hndKeys.contains(key)) {
                                try {
                                    cctx.continuousQueries().onEntryUpdated(
                                        lsnrs,
                                        key,
                                        req.value(i),
                                        req.localPreviousValue(i),
                                        key.internal() || !cctx.userCache(),
                                        req.partitionId(i),
                                        true,
                                        false,
                                        req.updateCounter(i),
                                        updateReq.topologyVersion());
                                }
                                catch (IgniteCheckedException e) {
                                    U.warn(log, "Failed to send continuous query message. [key=" + key +
                                        ", newVal=" + req.value(i) +
                                        ", err=" + e + "]");
                                }

                                hndKeys.add(key);

                                if (hndKeys.size() == keys.size())
                                    break exit;
                            }
                        }
                    }
                }
            }

            if (updateReq.writeSynchronizationMode() == FULL_SYNC)
                completionCb.apply(updateReq, updateRes);

            return true;
        }

        return false;
    }

    /**
     * Sends requests to remote nodes.
     */
    public void map() {
        if (!mappings.isEmpty()) {
            for (GridDhtAtomicUpdateRequest req : mappings.values()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending DHT atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                    cctx.io().send(req.nodeId(), req, cctx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException ignored) {
                    U.warn(log, "Failed to send update request to backup node because it left grid: " +
                        req.nodeId());

                    registerResponse(req.nodeId());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send update request to backup node (did node leave the grid?): "
                        + req.nodeId(), e);

                    registerResponse(req.nodeId());
                }
            }
        }
        else
            onDone();

        // Send response right away if no ACKs from backup is required.
        // Backups will send ACKs anyway, future will be completed after all backups have replied.
        if (updateReq.writeSynchronizationMode() != FULL_SYNC)
            completionCb.apply(updateReq, updateRes);
    }

    /**
     * Callback for backup update response.
     *
     * @param nodeId Backup node ID.
     * @param updateRes Update response.
     */
    public void onResult(UUID nodeId, GridDhtAtomicUpdateResponse updateRes) {
        if (log.isDebugEnabled())
            log.debug("Received DHT atomic update future result [nodeId=" + nodeId + ", updateRes=" + updateRes + ']');

        if (updateRes.error() != null)
            this.updateRes.addFailedKeys(updateRes.failedKeys(), updateRes.error());

        if (!F.isEmpty(updateRes.nearEvicted())) {
            for (KeyCacheObject key : updateRes.nearEvicted()) {
                GridDhtCacheEntry entry = nearReadersEntries.get(key);

                try {
                    entry.removeReader(nodeId, updateRes.messageId());
                }
                catch (GridCacheEntryRemovedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Entry with evicted reader was removed [entry=" + entry + ", err=" + e + ']');
                }
            }
        }

        registerResponse(nodeId);
    }

    /**
     * Deferred update response.
     *
     * @param nodeId Backup node ID.
     */
    public void onResult(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Received deferred DHT atomic update future result [nodeId=" + nodeId + ']');

        registerResponse(nodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateFuture.class, this);
    }
}
