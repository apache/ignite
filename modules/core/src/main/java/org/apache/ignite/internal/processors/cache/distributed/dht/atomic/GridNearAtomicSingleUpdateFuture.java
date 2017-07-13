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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
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
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicSingleUpdateFuture extends GridNearAtomicAbstractUpdateFuture {
    /** Keys */
    private Object key;

    /** Values. */
    private Object val;

    /** */
    private PrimaryRequestState reqState;

    /**
     * @param cctx Cache context.
     * @param cache Cache instance.
     * @param syncMode Write synchronization mode.
     * @param op Update operation.
     * @param key Keys to update.
     * @param val Values or transform closure.
     * @param invokeArgs Optional arguments for entry processor.
     * @param retval Return value require flag.
     * @param rawRetval {@code True} if should return {@code GridCacheReturn} as future result.
     * @param expiryPlc Expiry policy explicitly specified for cache operation.
     * @param filter Entry filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param recovery {@code True} if cache operation is called in recovery mode.
     * @param remapCnt Maximum number of retries.
     */
    public GridNearAtomicSingleUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        Object key,
        @Nullable Object val,
        @Nullable Object[] invokeArgs,
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

        assert subjId != null;

        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;
        AffinityTopologyVersion remapTopVer0 = null;

        GridNearAtomicCheckUpdateRequest checkReq = null;

        boolean rcvAll = false;

        long futId;

        synchronized (this) {
            if (!futureMapped())
                return false;

            futId = this.futId;

            if (reqState.req.nodeId.equals(nodeId)) {
                GridNearAtomicAbstractUpdateRequest req = reqState.onPrimaryFail();

                if (req != null) {
                    GridNearAtomicUpdateResponse res = primaryFailedResponse(req);

                    rcvAll = true;

                    reqState.onPrimaryResponse(res, cctx);

                    onPrimaryError(req, res);
                }
            }
            else {
                DhtLeftResult res = reqState.onDhtNodeLeft(nodeId);

                if (res == DhtLeftResult.DONE)
                    rcvAll = true;
                else if (res == DhtLeftResult.ALL_RCVD_CHECK_PRIMARY)
                    checkReq = new GridNearAtomicCheckUpdateRequest(reqState.req);
                else
                    return false;
            }

            if (rcvAll) {
                opRes0 = opRes;
                err0 = err;
                remapTopVer0 = onAllReceived();
            }
        }

        if (checkReq != null)
            sendCheckUpdateRequest(checkReq);
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

            assert reqState != null;
            assert reqState.req.nodeId().equals(res.primaryId());

            if (opRes == null && res.hasResult())
                opRes = res.result();

            if (reqState.onDhtResponse(nodeId, res)) {
                opRes0 = opRes;
                err0 = err;
                remapTopVer0 = onAllReceived();
            }
            else
                return;
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
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    @Override public void onPrimaryResponse(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr) {
        GridNearAtomicAbstractUpdateRequest req;

        AffinityTopologyVersion remapTopVer0;

        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;

        synchronized (this) {
            if (!checkFutureId(res.futureId()))
                return;

            req = reqState.processPrimaryResponse(nodeId, res);

            if (req == null)
                return;

            boolean remapKey = res.remapTopologyVersion() != null;

            if (remapKey) {
                assert !req.topologyVersion().equals(res.remapTopologyVersion());

                assert remapTopVer == null : remapTopVer;

                remapTopVer = res.remapTopologyVersion();
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

                assert reqState != null;

                if (!reqState.onPrimaryResponse(res, cctx))
                    return;
            }

            remapTopVer0 = onAllReceived();

            if (remapTopVer0 == null) {
                err0 = err;
                opRes0 = opRes;
            }
        }

        if (res.error() != null && res.failedKeys() == null) {
            completeFuture(null, res.error(), res.futureId());

            return;
        }

        if (remapTopVer0 != null) {
            waitAndRemap(remapTopVer0);

            return;
        }

        if (nearEnabled && !nodeErr)
            updateNear(req, res);

        completeFuture(opRes0, err0, res.futureId());
    }

    /**
     * @return Non-null topology version if update should be remapped.
     */
    private AffinityTopologyVersion onAllReceived() {
        assert Thread.holdsLock(this);
        assert futureMapped() : this;

        AffinityTopologyVersion remapTopVer0 = null;

        if (remapTopVer == null) {
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

                    remapTopVer0 = new AffinityTopologyVersion(cause.topologyVersion().topologyVersion() + 1);

                    err = null;
                }
            }
        }
        else
            remapTopVer0 = remapTopVer;

        if (remapTopVer0 != null) {
            cctx.mvcc().removeAtomicFuture(futId);

            reqState = null;
            topVer = AffinityTopologyVersion.ZERO;
            futId = 0;

            remapTopVer = null;
        }

        return remapTopVer0;
    }

    /**
     * @param remapTopVer New topology version.
     */
    private void waitAndRemap(AffinityTopologyVersion remapTopVer) {
        if (topLocked) {
            CachePartialUpdateCheckedException e =
                new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

            ClusterTopologyCheckedException cause = new ClusterTopologyCheckedException(
                "Failed to update keys, topology changed while execute atomic update inside transaction.");

            cause.retryReadyFuture(cctx.affinity().affinityReadyFuture(remapTopVer));

            e.add(Collections.singleton(cctx.toCacheKeyObject(key)), cause);

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
            completeFuture(null,new CacheStoppedException(
                cache.name()),
                null);

            return;
        }

        GridDhtTopologyFuture fut = cache.topology().topologyVersionFuture();

        if (fut.isDone()) {
            Throwable err = fut.validateCache(cctx, recovery, /*read*/false, key, null);

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

        map(topVer);
    }

    /** {@inheritDoc} */
    @Override protected void map(AffinityTopologyVersion topVer) {
        long futId = cctx.mvcc().nextAtomicId();

        Exception err = null;
        PrimaryRequestState reqState0 = null;

        try {
            reqState0 = mapSingleUpdate(topVer, futId);

            synchronized (this) {
                assert topVer.topologyVersion() > 0 : topVer;
                assert this.topVer == AffinityTopologyVersion.ZERO : this;

                this.topVer = topVer;
                this.futId = futId;

                reqState = reqState0;
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
        sendSingleRequest(reqState0.req.nodeId(), reqState0.req);

        if (syncMode == FULL_ASYNC) {
            completeFuture(new GridCacheReturn(cctx, true, true, null, true), null, null);

            return;
        }

        if (reqState0.req.initMappingLocally() && (cctx.discovery().topologyVersion() != topVer.topologyVersion()))
            checkDhtNodes(futId);
    }

    /**
     * @param futId Future ID.
     */
    private void checkDhtNodes(long futId) {
        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;
        AffinityTopologyVersion remapTopVer0 = null;

        GridNearAtomicCheckUpdateRequest checkReq = null;

        synchronized (this) {
            if (!checkFutureId(futId))
                return;

            assert reqState != null;

            DhtLeftResult res = reqState.checkDhtNodes(cctx);

            if (res == DhtLeftResult.DONE) {
                opRes0 = opRes;
                err0 = err;
                remapTopVer0 = onAllReceived();
            }
            else if (res == DhtLeftResult.ALL_RCVD_CHECK_PRIMARY)
                checkReq = new GridNearAtomicCheckUpdateRequest(reqState.req);
            else
                return;
        }

        if (checkReq != null)
            sendCheckUpdateRequest(checkReq);
        else
            finishUpdateFuture(opRes0, err0, remapTopVer0, futId);
    }

    /**
     * @param topVer Topology version.
     * @param futId Future ID.
     * @return Request.
     * @throws Exception If failed.
     */
    private PrimaryRequestState mapSingleUpdate(AffinityTopologyVersion topVer, long futId)
        throws Exception {
        if (key == null)
            throw new NullPointerException("Null key.");

        Object val = this.val;

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

        boolean mappingKnown = cctx.topology().rebalanceFinished(topVer);

        List<ClusterNode> nodes = cctx.affinity().nodesByKey(cacheKey, topVer);

        if (F.isEmpty(nodes))
            throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid).");

        ClusterNode primary = nodes.get(0);

        boolean needPrimaryRes = !mappingKnown || primary.isLocal() || nodes.size() == 1 || nearEnabled;

        GridNearAtomicAbstractUpdateRequest req;

        byte flags = GridNearAtomicAbstractUpdateRequest.flags(nearEnabled,
            topLocked,
            retval,
            mappingKnown,
            needPrimaryRes,
            skipStore,
            keepBinary,
            recovery);

        if (canUseSingleRequest()) {
            if (op == TRANSFORM) {
                req = new GridNearAtomicSingleUpdateInvokeRequest(
                    cctx.cacheId(),
                    primary.id(),
                    futId,
                    topVer,
                    syncMode,
                    op,
                    invokeArgs,
                    subjId,
                    taskNameHash,
                    flags,
                    cctx.deploymentEnabled());
            }
            else {
                if (filter == null || filter.length == 0) {
                    req = new GridNearAtomicSingleUpdateRequest(
                        cctx.cacheId(),
                        primary.id(),
                        futId,
                        topVer,
                        syncMode,
                        op,
                        subjId,
                        taskNameHash,
                        flags,
                        cctx.deploymentEnabled());
                }
                else {
                    req = new GridNearAtomicSingleUpdateFilterRequest(
                        cctx.cacheId(),
                        primary.id(),
                        futId,
                        topVer,
                        syncMode,
                        op,
                        filter,
                        subjId,
                        taskNameHash,
                        flags,
                        cctx.deploymentEnabled());
                }
            }
        }
        else {
            req = new GridNearAtomicFullUpdateRequest(
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
        }

        req.addUpdateEntry(cacheKey,
            val,
            CU.TTL_NOT_CHANGED,
            CU.EXPIRE_TIME_CALCULATE,
            null);

        return new PrimaryRequestState(req, nodes, true);
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
        if (remapTopVer != null) {
            waitAndRemap(remapTopVer);

            return;
        }

        if (nearEnabled) {
            assert reqState.req.response() != null;

            updateNear(reqState.req, reqState.req.response());
        }

        completeFuture(opRes, err, futId);
    }

    /**
     * @return {@code True} can use 'single' update requests.
     */
    private boolean canUseSingleRequest() {
        return expiryPlc == null;
    }

    /** {@inheritDoc} */
    public synchronized String toString() {
        return S.toString(GridNearAtomicSingleUpdateFuture.class, this, super.toString());
    }
}
