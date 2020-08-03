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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.EntryProcessorResourceInjectorProxy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicUpdateFuture extends GridNearAtomicAbstractUpdateFuture {
    /** Keys */
    private Collection<?> keys;

    /** Values. */
    private Collection<?> vals;

    /** Conflict put values. */
    private Collection<GridCacheDrInfo> conflictPutVals;

    /** Conflict remove values. */
    private Collection<GridCacheVersion> conflictRmvVals;

    /** Mappings if operations is mapped to more than one node. */
    @GridToStringInclude
    private Map<UUID, PrimaryRequestState> mappings;

    /** Keys to remap. */
    @GridToStringInclude
    private Collection<KeyCacheObject> remapKeys;

    /** Not null is operation is mapped to single node. */
    @GridToStringInclude
    private PrimaryRequestState singleReq;

    /** */
    private int resCnt;

    /**
     * @param cctx Cache context.
     * @param cache Cache instance.
     * @param syncMode Write synchronization mode.
     * @param op Update operation.
     * @param keys Keys to update.
     * @param vals Values or transform closure.
     * @param invokeArgs Optional arguments for entry processor.
     * @param conflictPutVals Conflict put values (optional).
     * @param conflictRmvVals Conflict remove values (optional).
     * @param retval Return value require flag.
     * @param rawRetval {@code True} if should return {@code GridCacheReturn} as future result.
     * @param expiryPlc Expiry policy explicitly specified for cache operation.
     * @param filter Entry filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Maximum number of retries.
     */
    public GridNearAtomicUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        Collection<?> keys,
        @Nullable Collection<?> vals,
        @Nullable Object[] invokeArgs,
        @Nullable Collection<GridCacheDrInfo> conflictPutVals,
        @Nullable Collection<GridCacheVersion> conflictRmvVals,
        final boolean retval,
        final boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        final CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        boolean recovery,
        int remapCnt
    ) {
        super(cctx,
            cache,
            syncMode,
            op,
            invokeArgs,
            retval,
            rawRetval,
            expiryPlc,
            filter,
            subjId,
            taskNameHash,
            skipStore,
            keepBinary,
            recovery,
            remapCnt);

        assert vals == null || vals.size() == keys.size();
        assert conflictPutVals == null || conflictPutVals.size() == keys.size();
        assert conflictRmvVals == null || conflictRmvVals.size() == keys.size();
        assert subjId != null;

        this.keys = keys;
        this.vals = vals;
        this.conflictPutVals = conflictPutVals;
        this.conflictRmvVals = conflictRmvVals;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;
        AffinityTopologyVersion remapTopVer0 = null;

        boolean rcvAll = false;

        List<GridNearAtomicCheckUpdateRequest> checkReqs = null;

        long futId;

        synchronized (this) {
            if (!futureMapped())
                return false;

            futId = this.futId;

            if (singleReq != null) {
                if (singleReq.req.nodeId.equals(nodeId)) {
                    GridNearAtomicAbstractUpdateRequest req = singleReq.onPrimaryFail();

                    if (req != null) {
                        rcvAll = true;

                        GridNearAtomicUpdateResponse res = primaryFailedResponse(req);

                        singleReq.onPrimaryResponse(res, cctx);

                        onPrimaryError(req, res);
                    }
                }
                else {
                    DhtLeftResult res = singleReq.onDhtNodeLeft(nodeId);

                    if (res == DhtLeftResult.DONE)
                        rcvAll = true;
                    else if (res == DhtLeftResult.ALL_RCVD_CHECK_PRIMARY)
                        checkReqs = Collections.singletonList(new GridNearAtomicCheckUpdateRequest(singleReq.req));
                }

                if (rcvAll) {
                    opRes0 = opRes;
                    err0 = err;
                    remapTopVer0 = onAllReceived();
                }
            }
            else {
                if (mappings == null)
                    return false;

                for (Map.Entry<UUID, PrimaryRequestState> e : mappings.entrySet()) {
                    assert e.getKey().equals(e.getValue().req.nodeId());

                    PrimaryRequestState reqState = e.getValue();

                    boolean reqDone = false;

                    if (e.getKey().equals(nodeId)) {
                        GridNearAtomicAbstractUpdateRequest req = reqState.onPrimaryFail();

                        if (req != null) {
                            reqDone = true;

                            GridNearAtomicUpdateResponse res = primaryFailedResponse(req);

                            reqState.onPrimaryResponse(res, cctx);

                            onPrimaryError(req, res);
                        }
                    }
                    else {
                        DhtLeftResult res = reqState.onDhtNodeLeft(nodeId);

                        if (res == DhtLeftResult.DONE)
                            reqDone = true;
                        else if (res == DhtLeftResult.ALL_RCVD_CHECK_PRIMARY) {
                            if (checkReqs == null)
                                checkReqs = new ArrayList<>();

                            checkReqs.add(new GridNearAtomicCheckUpdateRequest(reqState.req));
                        }
                    }

                    if (reqDone) {
                        assert mappings.size() > resCnt : "[mappings=" + mappings.size() + ", cnt=" + resCnt + ']';

                        resCnt++;

                        if (mappings.size() == resCnt) {
                            rcvAll = true;

                            opRes0 = opRes;
                            err0 = err;
                            remapTopVer0 = onAllReceived();

                            break;
                        }
                    }
                }
            }
        }

        if (checkReqs != null) {
            assert !rcvAll;

            for (int i = 0; i < checkReqs.size(); i++)
                sendCheckUpdateRequest(checkReqs.get(i));
        }
        else if (rcvAll)
            finishUpdateFuture(opRes0, err0, remapTopVer0, futId);

        return false;
    }

    /** {@inheritDoc} */
    @Override public void onDhtResponse(UUID nodeId, GridDhtAtomicNearResponse res) {
        GridCacheReturn opRes0;
        CachePartialUpdateCheckedException err0;
        AffinityTopologyVersion remapTopVer0;

        synchronized (this) {
            if (!checkFutureId(res.futureId()))
                return;

            PrimaryRequestState reqState;

            if (singleReq != null) {
                assert singleReq.req.nodeId().equals(res.primaryId());

                if (opRes == null && res.hasResult())
                    opRes = res.result();

                if (singleReq.onDhtResponse(nodeId, res)) {
                    opRes0 = opRes;
                    err0 = err;
                    remapTopVer0 = onAllReceived();
                }
                else
                    return;
            }
            else {
                reqState = mappings != null ? mappings.get(res.primaryId()) : null;

                if (reqState != null) {
                    if (opRes == null && res.hasResult())
                        opRes = res.result();

                    if (reqState.onDhtResponse(nodeId, res)) {
                        assert mappings.size() > resCnt : "[mappings=" + mappings.size() + ", cnt=" + resCnt + ']';

                        resCnt++;

                        if (mappings.size() == resCnt) {
                            opRes0 = opRes;
                            err0 = err;
                            remapTopVer0 = onAllReceived();
                        }
                        else
                            return;
                    }
                    else
                        return;
                }
                else
                    return;
            }
        }

        UpdateErrors errors = res.errors();

        if (errors != null) {
            assert errors.error() != null;

            completeFuture(null, errors.error(), res.futureId());

            return;
        }

        finishUpdateFuture(opRes0, err0, remapTopVer0, res.futureId());
    }

    /** {@inheritDoc} */
    @Override public void onPrimaryResponse(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr) {
        GridNearAtomicAbstractUpdateRequest req;

        AffinityTopologyVersion remapTopVer0 = null;

        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;

        boolean rcvAll;

        synchronized (this) {
            if (!checkFutureId(res.futureId()))
                return;

            if (singleReq != null) {
                req = singleReq.processPrimaryResponse(nodeId, res);

                if (req == null)
                    return;

                rcvAll = singleReq.onPrimaryResponse(res, cctx);
            }
            else {
                if (mappings == null)
                    return;

                PrimaryRequestState reqState = mappings.get(nodeId);

                if (reqState == null)
                    return;

                req = reqState.processPrimaryResponse(nodeId, res);

                if (req != null) {
                    if (reqState.onPrimaryResponse(res, cctx)) {
                        assert mappings.size() > resCnt : "[mappings=" + mappings.size() + ", cnt=" + resCnt + ']';

                        resCnt++;

                        rcvAll = mappings.size() == resCnt;
                    }
                    else {
                        assert mappings.size() > resCnt : "[mappings=" + mappings.size() + ", cnt=" + resCnt + ']';

                        rcvAll = false;
                    }
                }
                else
                    return;
            }

            assert req.topologyVersion().equals(topVer) : req;

            if (res.remapTopologyVersion() != null) {
                assert !req.topologyVersion().equals(res.remapTopologyVersion());

                if (remapKeys == null)
                    remapKeys = U.newHashSet(req.size());

                remapKeys.addAll(req.keys());

                if (remapTopVer == null || remapTopVer.compareTo(res.remapTopologyVersion()) < 0)
                    remapTopVer = req.topologyVersion();
            }
            else if (res.error() != null)
                onPrimaryError(req, res);
            else {
                GridCacheReturn ret = res.returnValue();

                if (op == TRANSFORM) {
                    if (ret != null) {
                        assert ret.value() == null || ret.value() instanceof Map : ret.value();

                        if (ret.value() != null) {
                            if (opRes != null)
                                opRes.mergeEntryProcessResults(ret);
                            else
                                opRes = ret;
                        }
                    }
                }
                else
                    opRes = ret;
            }

            if (rcvAll) {
                remapTopVer0 = onAllReceived();

                if (remapTopVer0 == null) {
                    err0 = err;
                    opRes0 = opRes;
                }
            }
        }

        if (res.error() != null && res.failedKeys() == null) {
            completeFuture(null, res.error(), res.futureId());

            return;
        }

        if (rcvAll && nearEnabled) {
            if (mappings != null) {
                for (PrimaryRequestState reqState : mappings.values()) {
                    GridNearAtomicUpdateResponse res0 = reqState.req.response();

                    assert res0 != null : reqState;

                    updateNear(reqState.req, res0);
                }
            }
            else if (!nodeErr)
                updateNear(req, res);
        }

        if (remapTopVer0 != null) {
            waitAndRemap(remapTopVer0);

            return;
        }

        if (rcvAll)
            completeFuture(opRes0, err0, res.futureId());
    }

    private void waitAndRemap(AffinityTopologyVersion remapTopVer) {
        assert remapTopVer != null;

        if (topLocked) {
            assert !F.isEmpty(remapKeys) : remapKeys;

            CachePartialUpdateCheckedException e =
                new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

            ClusterTopologyCheckedException cause = new ClusterTopologyCheckedException(
                "Failed to update keys, topology changed while execute atomic update inside transaction.");

            cause.retryReadyFuture(cctx.shared().exchange().affinityReadyFuture(remapTopVer));

            e.add(remapKeys, cause);

            completeFuture(null, e, null);

            return;
        }

        IgniteInternalFuture<AffinityTopologyVersion> fut = cctx.shared().exchange().affinityReadyFuture(remapTopVer);

        if (fut == null)
            fut = new GridFinishedFuture<>(remapTopVer);

        fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
            @Override public void apply(final IgniteInternalFuture<AffinityTopologyVersion> fut) {
                cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        mapOnTopology();
                    }
                });
            }
        });
    }

    /**
     * @return Non null topology version if update should be remapped.
     */
    @Nullable private AffinityTopologyVersion onAllReceived() {
        assert Thread.holdsLock(this);
        assert futureMapped() : this;

        AffinityTopologyVersion remapTopVer0 = null;

        if (remapKeys != null) {
            assert remapTopVer != null;

            remapTopVer0 = remapTopVer;
        }
        else {
            if (err != null &&
                X.hasCause(err, CachePartialUpdateCheckedException.class) &&
                X.hasCause(err, ClusterTopologyCheckedException.class) &&
                storeFuture() &&
                --remapCnt > 0) {
                ClusterTopologyCheckedException topErr = X.cause(err, ClusterTopologyCheckedException.class);

                if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                    CachePartialUpdateCheckedException cause =
                        X.cause(err, CachePartialUpdateCheckedException.class);

                    assert cause != null && cause.topologyVersion() != null : err;
                    assert remapKeys == null;
                    assert remapTopVer == null;

                    remapTopVer = remapTopVer0 =
                        new AffinityTopologyVersion(cause.topologyVersion().topologyVersion() + 1);

                    err = null;

                    Collection<Object> failedKeys = cause.failedKeys();

                    remapKeys = new ArrayList<>(failedKeys.size());

                    for (Object key : failedKeys)
                        remapKeys.add(cctx.toCacheKeyObject(key));
                }
            }
        }

        if (remapTopVer0 != null) {
            cctx.mvcc().removeAtomicFuture(futId);

            topVer = AffinityTopologyVersion.ZERO;
            futId = 0;

            remapTopVer = null;
        }

        return remapTopVer0;
    }

    /**
     * @param opRes Operation result.
     * @param err Operation error.
     * @param remapTopVer Not-null topology version if need remap update.
     * @param futId Future ID.
     */
    private void finishUpdateFuture(GridCacheReturn opRes,
        CachePartialUpdateCheckedException err,
        @Nullable AffinityTopologyVersion remapTopVer,
        long futId) {
        if (nearEnabled) {
            if (mappings != null) {
                for (PrimaryRequestState reqState : mappings.values()) {
                    GridNearAtomicUpdateResponse res0 = reqState.req.response();

                    assert res0 != null : reqState;

                    updateNear(reqState.req, res0);
                }
            }
            else {
                assert singleReq != null && singleReq.req.response() != null;

                updateNear(singleReq.req, singleReq.req.response());
            }
        }

        if (remapTopVer != null) {
            assert !F.isEmpty(remapKeys);

            waitAndRemap(remapTopVer);

            return;
        }

        completeFuture(opRes, err, futId);
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    private void updateNear(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
        assert nearEnabled;

        if (res.remapTopologyVersion() != null)
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /** {@inheritDoc} */
    @Override protected void mapOnTopology() {
        AffinityTopologyVersion topVer;

        if (cache.topology().stopping()) {
            completeFuture(
                null,
                cctx.shared().cache().isCacheRestarting(cache.name()) ?
                    new IgniteCacheRestartingException(cache.name()) :
                    new CacheStoppedException(cache.name()),
                null);

            return;
        }

        GridDhtTopologyFuture fut = cache.topology().topologyVersionFuture();

        if (fut.isDone()) {
            Throwable err = fut.validateCache(cctx, recovery, false, null, keys);

            if (err != null) {
                completeFuture(null, err, null);

                return;
            }

            topVer = fut.topologyVersion();
        }
        else {
            assert !topLocked : this;

            fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            mapOnTopology();
                        }
                    });
                }
            });

            return;
        }

        map(topVer, remapKeys);
    }

    /**
     * Sends messages to remote nodes and updates local cache.
     *
     * @param mappings Mappings to send.
     */
    private void sendUpdateRequests(Map<UUID, PrimaryRequestState> mappings) {
        UUID locNodeId = cctx.localNodeId();

        GridNearAtomicAbstractUpdateRequest locUpdate = null;

        // Send messages to remote nodes first, then run local update.
        for (PrimaryRequestState reqState : mappings.values()) {
            GridNearAtomicAbstractUpdateRequest req = reqState.req;

            if (locNodeId.equals(req.nodeId())) {
                assert locUpdate == null : "Cannot have more than one local mapping [locUpdate=" + locUpdate +
                    ", req=" + req + ']';

                locUpdate = req;
            }
            else {
                try {
                    if (req.initMappingLocally() && reqState.mappedNodes.isEmpty())
                        reqState.resetLocalMapping();

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

        if (locUpdate != null) {
            cache.updateAllAsyncInternal(cctx.localNode(), locUpdate,
                new GridDhtAtomicCache.UpdateReplyClosure() {
                    @Override public void apply(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        if (syncMode != FULL_ASYNC)
                            onPrimaryResponse(res.nodeId(), res, false);
                        else if (res.remapTopologyVersion() != null)
                            ((GridDhtAtomicCache)cctx.cache()).remapToNewPrimary(req);
                    }
                });
        }

        if (syncMode == FULL_ASYNC)
            completeFuture(new GridCacheReturn(cctx, true, true, null, true), null, null);
    }

    /** {@inheritDoc} */
    @Override protected void map(AffinityTopologyVersion topVer) {
        map(topVer, null);
    }

    /**
     * @param topVer Topology version.
     * @param remapKeys Keys to remap.
     */
    private void map(AffinityTopologyVersion topVer, @Nullable Collection<KeyCacheObject> remapKeys) {
        Collection<ClusterNode> topNodes = CU.affinityNodes(cctx, topVer);

        if (F.isEmpty(topNodes)) {
            completeFuture(null,
                new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes left the grid)."),
                null);

            return;
        }

        long futId = cctx.mvcc().nextAtomicId();

        Exception err = null;
        PrimaryRequestState singleReq0 = null;
        Map<UUID, PrimaryRequestState> mappings0 = null;

        int size = keys.size();

        boolean mappingKnown = cctx.topology().rebalanceFinished(topVer);

        try {
            if (size == 1) {
                assert remapKeys == null || remapKeys.size() == 1;

                singleReq0 = mapSingleUpdate(topVer, futId, mappingKnown);
            }
            else {
                Map<UUID, PrimaryRequestState> pendingMappings = mapUpdate(topNodes,
                    topVer,
                    futId,
                    remapKeys,
                    mappingKnown);

                if (pendingMappings.size() == 1)
                    singleReq0 = F.firstValue(pendingMappings);
                else {
                    mappings0 = pendingMappings;

                    assert !mappings0.isEmpty() || size == 0 : this;
                }
            }

            synchronized (this) {
                assert topVer.topologyVersion() > 0 : topVer;
                assert this.topVer == AffinityTopologyVersion.ZERO : this;

                this.topVer = topVer;
                this.futId = futId;

                resCnt = 0;

                singleReq = singleReq0;
                mappings = mappings0;

                this.remapKeys = null;
            }

            if (storeFuture() && !cctx.mvcc().addAtomicFuture(futId, this)) {
                assert isDone();

                return;
            }
        }
        catch (Exception e) {
            err = e;
        }

        if (err != null) {
            completeFuture(null, err, futId);

            return;
        }

        // Optimize mapping for single key.
        if (singleReq0 != null)
            sendSingleRequest(singleReq0.req.nodeId(), singleReq0.req);
        else {
            assert mappings0 != null;

            if (size == 0) {
                completeFuture(new GridCacheReturn(cctx, true, true, null, true), null, futId);

                return;
            }
            else
                sendUpdateRequests(mappings0);
        }

        if (syncMode == FULL_ASYNC) {
            completeFuture(new GridCacheReturn(cctx, true, true, null, true), null, futId);

            return;
        }

        if (mappingKnown && syncMode == FULL_SYNC && cctx.discovery().topologyVersion() != topVer.topologyVersion())
            checkDhtNodes(futId);
    }

    private void checkDhtNodes(long futId) {
        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;
        AffinityTopologyVersion remapTopVer0 = null;

        List<GridNearAtomicCheckUpdateRequest> checkReqs = null;

        boolean rcvAll = false;

        synchronized (this) {
            if (!checkFutureId(futId))
                return;

            if (singleReq != null) {
                if (!singleReq.req.initMappingLocally())
                    return;

                DhtLeftResult res = singleReq.checkDhtNodes(cctx);

                if (res == DhtLeftResult.DONE) {
                    opRes0 = opRes;
                    err0 = err;
                    remapTopVer0 = onAllReceived();
                }
                else if (res == DhtLeftResult.ALL_RCVD_CHECK_PRIMARY)
                    checkReqs = Collections.singletonList(new GridNearAtomicCheckUpdateRequest(singleReq.req));
                else
                    return;
            }
            else {
                if (mappings != null) {
                    for (PrimaryRequestState reqState : mappings.values()) {
                        if (!reqState.req.initMappingLocally())
                            continue;

                        DhtLeftResult res = reqState.checkDhtNodes(cctx);

                        if (res == DhtLeftResult.DONE) {
                            assert mappings.size() > resCnt : "[mappings=" + mappings.size() + ", cnt=" + resCnt + ']';

                            resCnt++;

                            if (mappings.size() == resCnt) {
                                rcvAll = true;

                                opRes0 = opRes;
                                err0 = err;
                                remapTopVer0 = onAllReceived();

                                break;
                            }
                        }
                        else if (res == DhtLeftResult.ALL_RCVD_CHECK_PRIMARY) {
                            if (checkReqs == null)
                                checkReqs = new ArrayList<>(mappings.size());

                            checkReqs.add(new GridNearAtomicCheckUpdateRequest(reqState.req));
                        }
                    }
                }
                else
                    return;
            }
        }

        if (checkReqs != null) {
            assert !rcvAll;

            for (int i = 0; i < checkReqs.size(); i++)
                sendCheckUpdateRequest(checkReqs.get(i));
        }
        else if (rcvAll)
            finishUpdateFuture(opRes0, err0, remapTopVer0, futId);
    }

    /**
     * @param topNodes Cache nodes.
     * @param topVer Topology version.
     * @param futId Future ID.
     * @param remapKeys Keys to remap.
     * @return Mapping.
     * @throws Exception If failed.
     */
    private Map<UUID, PrimaryRequestState> mapUpdate(Collection<ClusterNode> topNodes,
        AffinityTopologyVersion topVer,
        Long futId,
        @Nullable Collection<KeyCacheObject> remapKeys,
        boolean mappingKnown) throws Exception {
        Iterator<?> it = null;

        if (vals != null)
            it = vals.iterator();

        Iterator<GridCacheDrInfo> conflictPutValsIt = null;

        if (conflictPutVals != null)
            conflictPutValsIt = conflictPutVals.iterator();

        Iterator<GridCacheVersion> conflictRmvValsIt = null;

        if (conflictRmvVals != null)
            conflictRmvValsIt = conflictRmvVals.iterator();

        Map<UUID, PrimaryRequestState> pendingMappings = U.newHashMap(topNodes.size());

        // Create mappings first, then send messages.
        for (Object key : keys) {
            if (key == null)
                throw new NullPointerException("Null key.");

            Object val;
            GridCacheVersion conflictVer;
            long conflictTtl;
            long conflictExpireTime;

            if (vals != null) {
                val = it.next();
                conflictVer = null;
                conflictTtl = CU.TTL_NOT_CHANGED;
                conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;

                if (val == null)
                    throw new NullPointerException("Null value.");
            }
            else if (conflictPutVals != null) {
                GridCacheDrInfo conflictPutVal = conflictPutValsIt.next();

                val = conflictPutVal.valueEx();
                conflictVer = conflictPutVal.version();
                conflictTtl = conflictPutVal.ttl();
                conflictExpireTime = conflictPutVal.expireTime();
            }
            else if (conflictRmvVals != null) {
                val = null;
                conflictVer = conflictRmvValsIt.next();
                conflictTtl = CU.TTL_NOT_CHANGED;
                conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;
            }
            else {
                val = null;
                conflictVer = null;
                conflictTtl = CU.TTL_NOT_CHANGED;
                conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;
            }

            if (val == null && op != GridCacheOperation.DELETE)
                continue;

            KeyCacheObject cacheKey = cctx.toCacheKeyObject(key);

            if (remapKeys != null && !remapKeys.contains(cacheKey))
                continue;

            if (op != TRANSFORM) {
                val = cctx.toCacheObject(val);

                if (op == CREATE || op == UPDATE)
                    cctx.validateKeyAndValue(cacheKey, (CacheObject)val);
            }
            else
                val = EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), (EntryProcessor)val);

            List<ClusterNode> nodes = cctx.affinity().nodesByKey(cacheKey, topVer);

            if (F.isEmpty(nodes))
                throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                    "(all partition nodes left the grid).");

            ClusterNode primary = nodes.get(0);

            boolean needPrimaryRes = !mappingKnown || primary.isLocal() || nearEnabled;

            UUID nodeId = primary.id();

            PrimaryRequestState mapped = pendingMappings.get(nodeId);

            if (mapped == null) {
                byte flags = GridNearAtomicAbstractUpdateRequest.flags(nearEnabled,
                    topLocked,
                    retval,
                    mappingKnown,
                    needPrimaryRes,
                    skipStore,
                    keepBinary,
                    recovery);

                GridNearAtomicFullUpdateRequest req = new GridNearAtomicFullUpdateRequest(
                    cctx.cacheId(),
                    nodeId,
                    futId,
                    topVer,
                    syncMode,
                    op,
                    expiryPlc,
                    invokeArgs,
                    filter,
                    subjId,
                    taskNameHash,
                    flags,
                    cctx.deploymentEnabled(),
                    keys.size());

                mapped = new PrimaryRequestState(req, nodes, false);

                pendingMappings.put(nodeId, mapped);
            }

            if (mapped.req.initMappingLocally())
                mapped.addMapping(nodes);

            mapped.req.addUpdateEntry(cacheKey, val, conflictTtl, conflictExpireTime, conflictVer);
        }

        return pendingMappings;
    }

    /**
     * @param topVer Topology version.
     * @param futId Future ID.
     * @param mappingKnown {@code True} if update mapping is known locally.
     * @return Request.
     * @throws Exception If failed.
     */
    private PrimaryRequestState mapSingleUpdate(AffinityTopologyVersion topVer, Long futId, boolean mappingKnown)
        throws Exception {
        Object key = F.first(keys);

        Object val;
        GridCacheVersion conflictVer;
        long conflictTtl;
        long conflictExpireTime;

        if (vals != null) {
            // Regular PUT.
            val = F.first(vals);
            conflictVer = null;
            conflictTtl = CU.TTL_NOT_CHANGED;
            conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;
        }
        else if (conflictPutVals != null) {
            // Conflict PUT.
            GridCacheDrInfo conflictPutVal = F.first(conflictPutVals);

            val = conflictPutVal.valueEx();
            conflictVer = conflictPutVal.version();
            conflictTtl = conflictPutVal.ttl();
            conflictExpireTime = conflictPutVal.expireTime();
        }
        else if (conflictRmvVals != null) {
            // Conflict REMOVE.
            val = null;
            conflictVer = F.first(conflictRmvVals);
            conflictTtl = CU.TTL_NOT_CHANGED;
            conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;
        }
        else {
            // Regular REMOVE.
            val = null;
            conflictVer = null;
            conflictTtl = CU.TTL_NOT_CHANGED;
            conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;
        }

        // We still can get here if user pass map with single element.
        if (key == null)
            throw new NullPointerException("Null key.");

        if (val == null && op != GridCacheOperation.DELETE)
            throw new NullPointerException("Null value.");

        KeyCacheObject cacheKey = cctx.toCacheKeyObject(key);

        if (op != TRANSFORM) {
            val = cctx.toCacheObject(val);

            if (op == CREATE || op == UPDATE)
                cctx.validateKeyAndValue(cacheKey, (CacheObject)val);
        }
        else
            val = EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), (EntryProcessor)val);

        List<ClusterNode> nodes = cctx.affinity().nodesByKey(cacheKey, topVer);

        if (F.isEmpty(nodes))
            throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                    "(all partition nodes left the grid).");

        ClusterNode primary = nodes.get(0);

        boolean needPrimaryRes = !mappingKnown || primary.isLocal() || nodes.size() == 1 || nearEnabled;

        byte flags = GridNearAtomicAbstractUpdateRequest.flags(nearEnabled,
            topLocked,
            retval,
            mappingKnown,
            needPrimaryRes,
            skipStore,
            keepBinary,
            recovery);

        GridNearAtomicFullUpdateRequest req = new GridNearAtomicFullUpdateRequest(
            cctx.cacheId(),
            primary.id(),
            futId,
            topVer,
            syncMode,
            op,
            expiryPlc,
            invokeArgs,
            filter,
            subjId,
            taskNameHash,
            flags,
            cctx.deploymentEnabled(),
            1);

        req.addUpdateEntry(cacheKey,
            val,
            conflictTtl,
            conflictExpireTime,
            conflictVer);

        return new PrimaryRequestState(req, nodes, true);
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridNearAtomicUpdateFuture.class, this, super.toString());
    }
}
