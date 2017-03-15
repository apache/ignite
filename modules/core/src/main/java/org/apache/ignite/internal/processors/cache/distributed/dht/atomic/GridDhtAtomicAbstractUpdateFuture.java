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
import java.util.List;
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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * DHT atomic cache backup update future.
 */
public abstract class GridDhtAtomicAbstractUpdateFuture extends GridFutureAdapter<Void>
    implements GridCacheAtomicFuture<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger. */
    protected static IgniteLogger log;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Write version. */
    protected final GridCacheVersion writeVer;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Future version. */
    protected final GridCacheVersion futVer;

    /** Completion callback. */
    @GridToStringExclude
    private final CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicUpdateResponse> completionCb;

    /** Update request. */
    protected final GridNearAtomicAbstractUpdateRequest updateReq;

    /** Update response. */
    final GridNearAtomicUpdateResponse updateRes;

    /** Mappings. */
    @GridToStringInclude
    protected Map<UUID, GridDhtAtomicAbstractUpdateRequest> mappings;

    /** Continuous query closures. */
    private Collection<CI1<Boolean>> cntQryClsrs;

    /** */
    private final boolean waitForExchange;

    /** Response count. */
    private volatile int resCnt;

    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    protected GridDhtAtomicAbstractUpdateFuture(
        GridCacheContext cctx,
        CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq,
        GridNearAtomicUpdateResponse updateRes
    ) {
        this.cctx = cctx;

        this.futVer = cctx.isLocalNode(updateRes.nodeId()) ?
            cctx.versions().next(updateReq.topologyVersion()) : // Generate new if request mapped to local.
            updateReq.futureVersion();
        this.updateReq = updateReq;
        this.completionCb = completionCb;
        this.updateRes = updateRes;
        this.writeVer = writeVer;

        waitForExchange = !(updateReq.topologyLocked() || (updateReq.fastMap() && !updateReq.clientRequest()));

        if (log == null) {
            msgLog = cctx.shared().atomicMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridDhtAtomicUpdateFuture.class);
        }
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        if (waitForExchange && updateReq.topologyVersion().compareTo(topVer) < 0)
            return this;

        return null;
    }

    /**
     * @param clsr Continuous query closure.
     */
    public final void addContinuousQueryClosure(CI1<Boolean> clsr) {
        assert !isDone() : this;

        if (cntQryClsrs == null)
            cntQryClsrs = new ArrayList<>(10);

        cntQryClsrs.add(clsr);
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
    @SuppressWarnings("ForLoopReplaceableByForEach")
    final void addWriteEntry(GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr) {
        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        List<ClusterNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        addDhtKey(entry.key(), dhtNodes);

        for (int i = 0; i < dhtNodes.size(); i++) {
            ClusterNode node = dhtNodes.get(i);

            UUID nodeId = node.id();

            if (!nodeId.equals(cctx.localNodeId())) {
                GridDhtAtomicAbstractUpdateRequest updateReq = mappings.get(nodeId);

                if (updateReq == null) {
                    updateReq = createRequest(
                        node,
                        futVer,
                        writeVer,
                        syncMode,
                        topVer,
                        ttl,
                        conflictExpireTime,
                        conflictVer);

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
                    updateCntr);
            }
        }
    }

    /**
     * @param key Key.
     * @param dhtNodes DHT nodes.
     */
    protected abstract void addDhtKey(KeyCacheObject key, List<ClusterNode> dhtNodes);

    /**
     * @param key Key.
     * @param readers Near cache readers.
     */
    protected abstract void addNearKey(KeyCacheObject key, Collection<UUID> readers);

    /**
     * @param readers Entry readers.
     * @param entry Entry.
     * @param val Value.
     * @param entryProcessor Entry processor..
     * @param ttl TTL for near cache update (optional).
     * @param expireTime Expire time for near cache update (optional).
     */
    final void addNearWriteEntries(Collection<UUID> readers,
        GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        addNearKey(entry.key(), readers);

        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        for (UUID nodeId : readers) {
            GridDhtAtomicAbstractUpdateRequest updateReq = mappings.get(nodeId);

            if (updateReq == null) {
                ClusterNode node = cctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                updateReq = createRequest(
                    node,
                    futVer,
                    writeVer,
                    syncMode,
                    topVer,
                    ttl,
                    expireTime,
                    null);

                mappings.put(nodeId, updateReq);
            }

            addNearReaderEntry(entry);

            updateReq.addNearWriteValue(entry.key(),
                val,
                entryProcessor,
                ttl,
                expireTime);
        }
    }

    /**
     * adds new nearReader.
     *
     * @param entry GridDhtCacheEntry.
     */
    protected abstract void addNearReaderEntry(GridDhtCacheEntry entry);

    /**
     * @return Write version.
     */
    final GridCacheVersion writeVersion() {
        return writeVer;
    }

    /** {@inheritDoc} */
    @Override public final IgniteUuid futureId() {
        return futVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public final GridCacheVersion version() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public final boolean onNodeLeft(UUID nodeId) {
        boolean res = registerResponse(nodeId);

        if (res && msgLog.isDebugEnabled()) {
            msgLog.debug("DTH update fut, node left [futId=" + futVer + ", writeVer=" + writeVer +
                ", node=" + nodeId + ']');
        }

        return res;
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if request found.
     */
    final boolean registerResponse(UUID nodeId) {
        int resCnt0;

        GridDhtAtomicAbstractUpdateRequest req = mappings != null ? mappings.get(nodeId) : null;

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

    /**
     * Sends requests to remote nodes.
     */
    final void map() {
        if (!F.isEmpty(mappings)) {
            for (GridDhtAtomicAbstractUpdateRequest req : mappings.values()) {
                try {
                    cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("DTH update fut, sent request [futId=" + futVer +
                            ", writeVer=" + writeVer + ", node=" + req.nodeId() + ']');
                    }
                }
                catch (ClusterTopologyCheckedException ignored) {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("DTH update fut, failed to send request, node left [futId=" + futVer +
                            ", writeVer=" + writeVer + ", node=" + req.nodeId() + ']');
                    }

                    registerResponse(req.nodeId());
                }
                catch (IgniteCheckedException ignored) {
                    U.error(msgLog, "Failed to send request [futId=" + futVer +
                        ", writeVer=" + writeVer + ", node=" + req.nodeId() + ']');

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
     * Deferred update response.
     *
     * @param nodeId Backup node ID.
     */
    public final void onResult(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Received deferred DHT atomic update future result [nodeId=" + nodeId + ']');

        registerResponse(nodeId);
    }

    /**
     * @param node Node.
     * @param futVer Future version.
     * @param writeVer Update version.
     * @param syncMode Write synchronization mode.
     * @param topVer Topology version.
     * @param ttl TTL.
     * @param conflictExpireTime Conflict expire time.
     * @param conflictVer Conflict version.
     * @return Request.
     */
    protected abstract GridDhtAtomicAbstractUpdateRequest createRequest(
        ClusterNode node,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer
    );

    /**
     * Callback for backup update response.
     *
     * @param nodeId Backup node ID.
     * @param updateRes Update response.
     */
    public abstract void onResult(UUID nodeId, GridDhtAtomicUpdateResponse updateRes);

    /**
     * @param updateRes Response.
     * @param err Error.
     */
    protected abstract void addFailedKeys(GridNearAtomicUpdateResponse updateRes, Throwable err);

    /** {@inheritDoc} */
    @Override public final boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeAtomicFuture(version());

            boolean suc = err == null;

            if (!suc)
                addFailedKeys(updateRes, err);

            if (cntQryClsrs != null) {
                for (CI1<Boolean> clsr : cntQryClsrs)
                    clsr.apply(suc);
            }

            if (updateReq.writeSynchronizationMode() == FULL_SYNC)
                completionCb.apply(updateReq, updateRes);

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
}
