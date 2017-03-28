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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Base for near atomic update futures.
 */
public abstract class GridNearAtomicAbstractUpdateFuture extends GridFutureAdapter<Object>
    implements GridCacheAtomicFuture<Object> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Cache. */
    protected final GridDhtAtomicCache cache;

    /** Write synchronization mode. */
    protected final CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    protected final GridCacheOperation op;

    /** Optional arguments for entry processor. */
    protected final Object[] invokeArgs;

    /** Return value require flag. */
    protected final boolean retval;

    /** Raw return value flag. */
    protected final boolean rawRetval;

    /** Expiry policy. */
    protected final ExpiryPolicy expiryPlc;

    /** Optional filter. */
    protected final CacheEntryPredicate[] filter;

    /** Subject ID. */
    protected final UUID subjId;

    /** Task name hash. */
    protected final int taskNameHash;

    /** Skip store flag. */
    protected final boolean skipStore;

    /** Keep binary flag. */
    protected final boolean keepBinary;

    /** Wait for topology future flag. */
    protected final boolean waitTopFut;

    /** Near cache flag. */
    protected final boolean nearEnabled;

    /** Mutex to synchronize state updates. */
    protected final Object mux = new Object();

    /** Topology locked flag. Set if atomic update is performed inside a TX or explicit lock. */
    protected boolean topLocked;

    /** Remap count. */
    @GridToStringInclude
    protected int remapCnt;

    /** Current topology version. */
    @GridToStringInclude
    protected AffinityTopologyVersion topVer = AffinityTopologyVersion.ZERO;

    /** */
    @GridToStringInclude
    protected AffinityTopologyVersion remapTopVer;

    /** Error. */
    @GridToStringInclude
    protected CachePartialUpdateCheckedException err;

    /** Future ID. */
    @GridToStringInclude
    protected Long futId;

    /** Operation result. */
    protected GridCacheReturn opRes;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cache Cache.
     * @param syncMode Synchronization mode.
     * @param op Operation.
     * @param invokeArgs Invoke arguments.
     * @param retval Return value flag.
     * @param rawRetval Raw return value flag.
     * @param expiryPlc Expiry policy.
     * @param filter Filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Remap count.
     * @param waitTopFut Wait topology future flag.
     */
    protected GridNearAtomicAbstractUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable Object[] invokeArgs,
        boolean retval,
        boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        int remapCnt,
        boolean waitTopFut
    ) {
        if (log == null) {
            msgLog = cctx.shared().atomicMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridFutureAdapter.class);
        }

        this.cctx = cctx;
        this.cache = cache;
        this.syncMode = syncMode;
        this.op = op;
        this.invokeArgs = invokeArgs;
        this.retval = retval;
        this.rawRetval = rawRetval;
        this.expiryPlc = expiryPlc;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.waitTopFut = waitTopFut;

        nearEnabled = CU.isNearEnabled(cctx);

        if (!waitTopFut)
            remapCnt = 1;

        this.remapCnt = remapCnt;
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        return null;
    }

    /**
     * @param req Request.
     */
    void sendCheckUpdateRequest(GridNearAtomicCheckUpdateRequest req) {
        try {
            cctx.io().send(req.updateRequest().nodeId(), req, cctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException e) {
            onSendError(req, e);
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * Performs future mapping.
     */
    public final void map() {
        AffinityTopologyVersion topVer = cctx.shared().lockedTopologyVersion(null);

        if (topVer == null)
            mapOnTopology();
        else {
            topLocked = true;

            // Cannot remap.
            remapCnt = 1;

            map(topVer);
        }
    }

    /**
     * @param topVer Topology version.
     */
    protected abstract void map(AffinityTopologyVersion topVer);

    /**
     * Maps future on ready topology.
     */
    protected abstract void mapOnTopology();

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /**
     * @return {@code True} future is stored by {@link GridCacheMvccManager#addAtomicFuture}.
     */
    final boolean storeFuture() {
        return syncMode != FULL_ASYNC;
    }

    /**
     * Maps future to single node.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    final void sendSingleRequest(UUID nodeId, GridNearAtomicAbstractUpdateRequest req) {
        if (cctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req,
                new GridDhtAtomicCache.UpdateReplyClosure() {
                    @Override public void apply(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        if (syncMode != FULL_ASYNC)
                            onPrimaryResponse(res.nodeId(), res, false);
                        else if (res.remapTopologyVersion() != null)
                            ((GridDhtAtomicCache)cctx.cache()).remapToNewPrimary(req);
                    }
                });
        }
        else {
            try {
                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near update fut, sent request [futId=" + req.futureId() +
                        ", node=" + req.nodeId() + ']');
                }
            }
            catch (IgniteCheckedException e) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near update fut, failed to send request [futId=" + req.futureId() +
                        ", node=" + req.nodeId() +
                        ", err=" + e + ']');
                }

                onSendError(req, e);
            }
        }
    }

    /**
     * Response callback.
     *
     * @param nodeId Node ID.
     * @param res Update response.
     * @param nodeErr {@code True} if response was created on node failure.
     */
    public abstract void onPrimaryResponse(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr);

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public abstract void onDhtResponse(UUID nodeId, GridDhtAtomicNearResponse res);

    /**
     * @param req Request.
     * @param res Response.
     */
    final void onPrimaryError(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
        assert res.error() != null;

        if (err == null)
            err = new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

        Collection<KeyCacheObject> keys0 = res.failedKeys() != null ? res.failedKeys() : req.keys();

        Collection<Object> keys = new ArrayList<>(keys0.size());

        for (KeyCacheObject key : keys0)
            keys.add(cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false));

        err.add(keys, res.error(), req.topologyVersion());
    }

    /**
     * @param req Request.
     * @return Response to notify about primary failure.
     */
    final GridNearAtomicUpdateResponse primaryFailedResponse(GridNearAtomicAbstractUpdateRequest req) {
        assert req.response() == null : req;
        assert req.nodeId() != null : req;

        if (msgLog.isDebugEnabled()) {
            msgLog.debug("Near update fut, node left [futId=" + req.futureId() +
                ", node=" + req.nodeId() + ']');
        }

        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
            req.nodeId(),
            req.futureId(),
            req.partition(),
            true,
            cctx.deploymentEnabled());

        ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Primary node left grid " +
            "before response is received: " + req.nodeId());

        e.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(req.topologyVersion()));

        res.addFailedKeys(req.keys(), e);

        return res;
    }

    /**
     * @param req Request.
     * @param e Error.
     */
    final void onSendError(GridNearAtomicAbstractUpdateRequest req, IgniteCheckedException e) {
        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
            req.nodeId(),
            req.futureId(),
            req.partition(),
            e instanceof ClusterTopologyCheckedException,
            cctx.deploymentEnabled());

        res.addFailedKeys(req.keys(), e);

        onPrimaryResponse(req.nodeId(), res, true);
    }

    /**
     * @param req Request.
     * @param e Error.
     */
    private void onSendError(GridNearAtomicCheckUpdateRequest req, IgniteCheckedException e) {
        GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
            req.updateRequest().nodeId(),
            req.futureId(),
            req.partition(),
            e instanceof ClusterTopologyCheckedException,
            cctx.deploymentEnabled());

        res.addFailedKeys(req.updateRequest().keys(), e);

        onPrimaryResponse(req.updateRequest().nodeId(), res, true);
    }

    /**
     *
     */
    static class PrimaryRequestState {
        /** */
        final GridNearAtomicAbstractUpdateRequest req;

        /** */
        @GridToStringInclude
        Set<UUID> dhtNodes;

        /** */
        @GridToStringInclude
        private Set<UUID> rcvd;

        /** */
        private boolean hasRes;

        /**
         * @param req Request.
         * @param nodes Affinity nodes.
         * @param single {@code True} if created for sigle-key operation.
         */
        PrimaryRequestState(GridNearAtomicAbstractUpdateRequest req, List<ClusterNode> nodes, boolean single) {
            assert req != null && req.nodeId() != null : req;

            this.req = req;

            if (req.initMappingLocally()) {
                if (single) {
                    if (nodes.size() > 1) {
                        dhtNodes = U.newHashSet(nodes.size() - 1);

                        for (int i = 1; i < nodes.size(); i++)
                            dhtNodes.add(nodes.get(i).id());
                    }
                    else
                        dhtNodes = Collections.emptySet();
                }
                else {
                    dhtNodes = new HashSet<>();

                    for (int i = 1; i < nodes.size(); i++)
                        dhtNodes.add(nodes.get(i).id());
                }
            }
        }

        /**
         * @return Primary node ID.
         */
        UUID primaryId() {
            return req.nodeId();
        }

        /**
         * @param nodes Nodes.
         */
        void addMapping(List<ClusterNode> nodes) {
            assert req.initMappingLocally();

            for (int i = 1; i < nodes.size(); i++)
                dhtNodes.add(nodes.get(i).id());
        }

        /**
         * @param cctx Context.
         * @return Check result.
         */
        DhtLeftResult checkDhtNodes(GridCacheContext cctx) {
            assert req.initMappingLocally() : req;

            if (finished())
                return DhtLeftResult.NOT_DONE;

            boolean finished = false;

            for (Iterator<UUID> it = dhtNodes.iterator(); it.hasNext();) {
                UUID nodeId = it.next();

                if (!cctx.discovery().alive(nodeId)) {
                    it.remove();

                    if (finished()) {
                        finished = true;

                        break;
                    }
                }
            }

            if (finished)
                return DhtLeftResult.DONE;

            if (dhtNodes.isEmpty())
                return !req.needPrimaryResponse() ? DhtLeftResult.ALL_RCVD_CHECK_PRIMARY : DhtLeftResult.NOT_DONE;

            return DhtLeftResult.NOT_DONE;
        }

        /**
         * @return {@code True} if all expected responses are received.
         */
        private boolean finished() {
            if (req.writeSynchronizationMode() == PRIMARY_SYNC)
                return hasRes;

            return (dhtNodes != null && dhtNodes.isEmpty()) && hasRes;
        }

        /**
         * @return Request if need process primary fail response, {@code null} otherwise.
         */
        @Nullable GridNearAtomicAbstractUpdateRequest onPrimaryFail() {
            if (finished())
                return null;

            /*
             * When primary failed, even if primary response is received, it is possible it failed to send
             * request to backup(s), need remap operation.
             */
            if (req.fullSync() && !req.nodeFailedResponse()) {
                req.resetResponse();

                return req;
            }

            return req.response() == null ? req : null;
        }

        /**
         * @param nodeId Node ID.
         * @param res Response.
         * @return Request if need process primary response, {@code null} otherwise.
         */
        @Nullable GridNearAtomicAbstractUpdateRequest processPrimaryResponse(UUID nodeId, GridNearAtomicUpdateResponse res) {
            assert req.nodeId().equals(nodeId);

            if (res.nodeLeftResponse())
                return onPrimaryFail();

            if (finished())
                return null;

            return req.response() == null ? req : null;
        }

        /**
         * @param nodeId Node ID.
         * @return {@code True} if request processing finished.
         */
        DhtLeftResult onDhtNodeLeft(UUID nodeId) {
            if (req.writeSynchronizationMode() != FULL_SYNC || dhtNodes == null || finished())
                return DhtLeftResult.NOT_DONE;

            if (dhtNodes.remove(nodeId) && dhtNodes.isEmpty()) {
                if (hasRes)
                    return DhtLeftResult.DONE;
                else
                    return !req.needPrimaryResponse() ? DhtLeftResult.ALL_RCVD_CHECK_PRIMARY : DhtLeftResult.NOT_DONE;
            }

            return DhtLeftResult.NOT_DONE;
        }

        /**
         * @param nodeId Node ID.
         * @param res Response.
         * @return {@code True} if request processing finished.
         */
        boolean onDhtResponse(UUID nodeId, GridDhtAtomicNearResponse res) {
            assert req.writeSynchronizationMode() == FULL_SYNC : req;

            if (finished())
                return false;

            if (res.hasResult())
                hasRes = true;

            if (dhtNodes == null) {
                if (rcvd == null)
                    rcvd = new HashSet<>();

                rcvd.add(nodeId);

                return false;
            }

            return dhtNodes.remove(nodeId) && finished();
        }

        /**
         * @param res Response.
         * @param cctx Cache context.
         * @return {@code True} if request processing finished.
         */
        boolean onPrimaryResponse(GridNearAtomicUpdateResponse res, GridCacheContext cctx) {
            assert !finished() : this;

            hasRes = true;

            boolean onRes = req.onResponse(res);

            assert onRes;

            if (res.error() != null || res.remapTopologyVersion() != null) {
                dhtNodes = Collections.emptySet(); // Mark as finished.

                return true;
            }

            assert res.returnValue() != null : res;

            if (res.dhtNodes() != null)
                initDhtNodes(res.dhtNodes(), cctx);

            return finished();
        }

        /**
         * @param nodeIds Node IDs.
         * @param cctx Context.
         */
        private void initDhtNodes(List<UUID> nodeIds, GridCacheContext cctx) {
            assert dhtNodes == null || req.initMappingLocally();

            Set<UUID> dhtNodes0 = dhtNodes;

            dhtNodes = null;

            for (UUID dhtNodeId : nodeIds) {
                if (F.contains(rcvd, dhtNodeId))
                    continue;

                if (req.initMappingLocally() && !F.contains(dhtNodes0, dhtNodeId))
                    continue;

                if (cctx.discovery().node(dhtNodeId) != null) {
                    if (dhtNodes == null)
                        dhtNodes = U.newHashSet(nodeIds.size());

                    dhtNodes.add(dhtNodeId);
                }
            }

            if (dhtNodes == null)
                dhtNodes = Collections.emptySet();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PrimaryRequestState.class, this,
                "primary", primaryId(),
                "needPrimaryRes", req.needPrimaryResponse(),
                "primaryRes", req.response() != null,
                "done", finished());
        }
    }

    /**
     *
     */
    enum DhtLeftResult {
        /** All responses and operation result are received. */
        DONE,

        /** Not all responses are received. */
        NOT_DONE,

        /**
         * All backups failed and response from primary is not required,
         * in this case in FULL_SYNC mode need send additional request
         * on primary to ensure FULL_SYNC guarantee.
         */
        ALL_RCVD_CHECK_PRIMARY
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicAbstractUpdateFuture.class, this, super.toString());
    }
}
