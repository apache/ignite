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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * Base for near atomic update futures.
 */
public abstract class GridNearAtomicAbstractUpdateFuture extends GridCacheFutureAdapter<Object>
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

    /** Recovery flag. */
    protected final boolean recovery;

    /** Near cache flag. */
    protected final boolean nearEnabled;

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

    /** Future ID, changes when operation is remapped. */
    @GridToStringInclude
    protected long futId;

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
     * @param recovery {@code True} if cache operation is called in recovery mode.
     * @param remapCnt Remap count.
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
        boolean recovery,
        int remapCnt
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
        this.recovery = recovery;

        nearEnabled = CU.isNearEnabled(cctx);

        this.remapCnt = remapCnt;
    }

    /**
     * @return {@code True} if future was initialized and waits for responses.
     */
    final boolean futureMapped() {
        return topVer != AffinityTopologyVersion.ZERO;
    }

    /**
     * @param futId Expected future ID.
     * @return {@code True} if future was initialized with the same ID.
     */
    final boolean checkFutureId(long futId) {
        return topVer != AffinityTopologyVersion.ZERO && this.futId == futId;
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
            completeFuture(null, e, req.futureId());
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
            cache.updateAllAsyncInternal(cctx.localNode(), req,
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
     * @param ret Result.
     * @param err Error.
     * @param futId Not null ID if need remove future.
     */
    final void completeFuture(@Nullable GridCacheReturn ret, Throwable err, @Nullable Long futId) {
        Object retval = ret == null ? null : rawRetval ? ret : (this.retval || op == TRANSFORM) ?
                cctx.unwrapBinaryIfNeeded(ret.value(), keepBinary) : ret.success();

        if (op == TRANSFORM && retval == null)
            retval = Collections.emptyMap();

        if (futId != null)
            cctx.mvcc().removeAtomicFuture(futId);

        super.onDone(retval, err);
    }

    /** {@inheritDoc} */
    @Override public final boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        assert err != null : "onDone should be called only to finish future with error on cache/node stop";

        Long futId = null;

        synchronized (this) {
            if (futureMapped()) {
                futId = this.futId;

                topVer = AffinityTopologyVersion.ZERO;
                this.futId = 0;
            }
        }

        if (super.onDone(null, err)) {
            if (futId != null)
                cctx.mvcc().removeAtomicFuture(futId);

            return true;
        }

        return false;
    }

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

        Collection<Object> failedToUnwrapKeys = null;

        Exception suppressedErr = null;

        for (KeyCacheObject key : keys0) {
            try {
                keys.add(cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false));
            }
            catch (BinaryInvalidTypeException e) {
                keys.add(cctx.toCacheKeyObject(key));

                if (log.isDebugEnabled()) {
                    if (failedToUnwrapKeys == null)
                        failedToUnwrapKeys = new ArrayList<>();

                    // To limit keys count in log message.
                    if (failedToUnwrapKeys.size() < 5)
                        failedToUnwrapKeys.add(key);
                }

                suppressedErr = e;
            }
            catch (Exception e) {
                keys.add(cctx.toCacheKeyObject(key));

                suppressedErr = e;
            }
        }

        if (failedToUnwrapKeys != null) {
            log.warning("Failed to unwrap keys: " + failedToUnwrapKeys +
                " (the binary objects will be used instead).");
        }

        IgniteCheckedException error = res.error();

        if (suppressedErr != null)
            error.addSuppressed(suppressedErr);

        err.add(keys, error, req.topologyVersion());
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
    static class NodeResult {
        /** */
        boolean rcvd;

        /**
         * @param rcvd Result received flag.
         */
        NodeResult(boolean rcvd) {
            this.rcvd = rcvd;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Result [rcvd=" + rcvd + ']';
        }
    }

    /**
     *
     */
    static class PrimaryRequestState {
        /** */
        final GridNearAtomicAbstractUpdateRequest req;

        /** */
        @GridToStringInclude
        Map<UUID, NodeResult> mappedNodes;

        /** */
        @GridToStringInclude
        private int expCnt = -1;

        /** */
        @GridToStringInclude
        private int rcvdCnt;

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
                        mappedNodes = U.newHashMap(nodes.size() - 1);

                        for (int i = 1; i < nodes.size(); i++)
                            mappedNodes.put(nodes.get(i).id(), new NodeResult(false));
                    }
                    else
                        mappedNodes = Collections.emptyMap();
                }
                else {
                    mappedNodes = new HashMap<>();

                    for (int i = 1; i < nodes.size(); i++)
                        mappedNodes.put(nodes.get(i).id(), new NodeResult(false));
                }

                expCnt = mappedNodes.size();
            }
        }

        /**
         *
         */
        void resetLocalMapping() {
            assert req.initMappingLocally() : req;

            mappedNodes = null;

            expCnt = -1;

            req.needPrimaryResponse(true);
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
                mappedNodes.put(nodes.get(i).id(), new NodeResult(false));

            expCnt = mappedNodes.size();
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

            for (Map.Entry<UUID, NodeResult> e : mappedNodes.entrySet()) {
                NodeResult res = e.getValue();

                if (res.rcvd)
                    continue;

                UUID nodeId = e.getKey();

                if (!cctx.discovery().alive(nodeId)) {
                    res.rcvd = true;

                    rcvdCnt++;

                    if (finished()) {
                        finished = true;

                        break;
                    }
                }
            }

            if (finished)
                return DhtLeftResult.DONE;

            if (rcvdCnt == expCnt)
                return !req.needPrimaryResponse() ? DhtLeftResult.ALL_RCVD_CHECK_PRIMARY : DhtLeftResult.NOT_DONE;

            return DhtLeftResult.NOT_DONE;
        }

        /**
         * @return {@code True} if all expected responses are received.
         */
        private boolean finished() {
            if (req.writeSynchronizationMode() == PRIMARY_SYNC)
                return hasRes;

            return (expCnt == rcvdCnt) && hasRes;
        }

        /**
         * @return Request if need process primary fail response, {@code null} otherwise.
         */
        @Nullable GridNearAtomicAbstractUpdateRequest onPrimaryFail() {
            if (finished() || req.nodeFailedResponse())
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
            if (req.writeSynchronizationMode() != FULL_SYNC || mappedNodes == null || finished())
                return DhtLeftResult.NOT_DONE;

            NodeResult res = mappedNodes.get(nodeId);

            if (res != null && !res.rcvd) {
                res.rcvd = true;

                rcvdCnt++;

                if (rcvdCnt == expCnt) {
                    if (hasRes)
                        return DhtLeftResult.DONE;
                    else
                        return !req.needPrimaryResponse() ?
                            DhtLeftResult.ALL_RCVD_CHECK_PRIMARY : DhtLeftResult.NOT_DONE;
                }
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

            if (mappedNodes == null) {
                assert expCnt == -1 : expCnt;

                mappedNodes = new HashMap<>();

                mappedNodes.put(nodeId, new NodeResult(true));

                rcvdCnt++;

                return false;
            }

            NodeResult nodeRes = mappedNodes.get(nodeId);

            if (nodeRes != null) {
                if (nodeRes.rcvd)
                    return false;

                nodeRes.rcvd = true;

                rcvdCnt++;
            }
            else {
                if (!hasRes) // Do not finish future until primary response received and mapping is known.
                    expCnt = -1;

                mappedNodes.put(nodeId, new NodeResult(true));

                rcvdCnt++;
            }

            return finished();
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
                expCnt = -1; // Mark as finished.

                return true;
            }

            assert res.returnValue() != null : res;

            if (res.mapping() != null)
                initMapping(res.mapping(), cctx);

            return finished();
        }

        /**
         * @param nodeIds Node IDs.
         * @param cctx Context.
         */
        private void initMapping(List<UUID> nodeIds, GridCacheContext cctx) {
            if (nodeIds.isEmpty() && req.initMappingLocally()) {
                mappedNodes = U.newHashMap(nodeIds.size());

                rcvdCnt = 0;
            }

            assert rcvdCnt <= nodeIds.size();

            expCnt = nodeIds.size();

            if (mappedNodes == null)
                mappedNodes = U.newHashMap(nodeIds.size());

            for (int i = 0; i < nodeIds.size(); i++) {
                UUID nodeId = nodeIds.get(i);

                if (!mappedNodes.containsKey(nodeId)) {
                    NodeResult res = new NodeResult(false);

                    mappedNodes.put(nodeId, res);

                    if (cctx.discovery().node(nodeId) == null) {
                        res.rcvd = true;

                        rcvdCnt++;
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            Set<UUID> rcvd = null;
            Set<UUID> nonRcvd = null;

            if (mappedNodes != null) {
                for (Map.Entry<UUID, NodeResult> e : mappedNodes.entrySet()) {
                    if (e.getValue().rcvd) {
                        if (rcvd == null)
                            rcvd = new HashSet<>();

                        rcvd.add(e.getKey());
                    }
                    else {
                        if (nonRcvd == null)
                            nonRcvd = new HashSet<>();

                        nonRcvd.add(e.getKey());
                    }
                }
            }

            return "Primary [id=" + primaryId() +
                ", opRes=" + hasRes +
                ", expCnt=" + expCnt +
                ", rcvdCnt=" + rcvdCnt +
                ", primaryRes=" + (req.response() != null) +
                ", done=" + finished() +
                ", waitFor=" + nonRcvd +
                ", rcvd=" + rcvd + ']';
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
