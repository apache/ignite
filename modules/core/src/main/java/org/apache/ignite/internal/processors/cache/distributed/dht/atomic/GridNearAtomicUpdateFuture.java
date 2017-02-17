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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.EntryProcessorResourceInjectorProxy;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicUpdateFuture extends GridNearAtomicAbstractUpdateFuture {
    /** Fast map flag. */
    private final boolean fastMap;

    /** Keys */
    private Collection<?> keys;

    /** Values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<?> vals;

    /** Conflict put values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<GridCacheDrInfo> conflictPutVals;

    /** Conflict remove values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<GridCacheVersion> conflictRmvVals;

    /** Mappings if operations is mapped to more than one node. */
    @GridToStringInclude
    private Map<UUID, GridNearAtomicFullUpdateRequest> mappings;

    /** Keys to remap. */
    private Collection<KeyCacheObject> remapKeys;

    /** Not null is operation is mapped to single node. */
    private GridNearAtomicFullUpdateRequest singleReq;

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
     * @param waitTopFut If {@code false} does not wait for affinity change future.
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
        int remapCnt,
        boolean waitTopFut
    ) {
        super(cctx, cache, syncMode, op, invokeArgs, retval, rawRetval, expiryPlc, filter, subjId, taskNameHash,
            skipStore, keepBinary, remapCnt, waitTopFut);

        assert vals == null || vals.size() == keys.size();
        assert conflictPutVals == null || conflictPutVals.size() == keys.size();
        assert conflictRmvVals == null || conflictRmvVals.size() == keys.size();
        assert subjId != null;

        this.keys = keys;
        this.vals = vals;
        this.conflictPutVals = conflictPutVals;
        this.conflictRmvVals = conflictRmvVals;

        fastMap = cache.isFastMap(filter, op);
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        synchronized (mux) {
            return futVer;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        GridNearAtomicUpdateResponse res = null;

        GridNearAtomicFullUpdateRequest req;

        synchronized (mux) {
            if (singleReq != null)
                req = singleReq.nodeId().equals(nodeId) ? singleReq : null;
            else
                req = mappings != null ? mappings.get(nodeId) : null;

            if (req != null && req.response() == null) {
                res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                    nodeId,
                    req.futureVersion(),
                    cctx.deploymentEnabled());

                ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Primary node left grid " +
                    "before response is received: " + nodeId);

                e.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(req.topologyVersion()));

                res.addFailedKeys(req.keys(), e);
            }
        }

        if (res != null) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near update fut, node left [futId=" + req.futureVersion() +
                    ", writeVer=" + req.updateVersion() +
                    ", node=" + nodeId + ']');
            }

            onResult(nodeId, res, true);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        // Wait fast-map near atomic update futures in CLOCK mode.
        if (fastMap) {
            GridFutureAdapter<Void> fut;

            synchronized (mux) {
                if (this.topVer != AffinityTopologyVersion.ZERO && this.topVer.compareTo(topVer) < 0) {
                    if (topCompleteFut == null)
                        topCompleteFut = new GridFutureAdapter<>();

                    fut = topCompleteFut;
                }
                else
                    fut = null;
            }

            if (fut != null && isDone()) {
                fut.onDone();

                return null;
            }

            return fut;
        }

        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        assert res == null || res instanceof GridCacheReturn;

        GridCacheReturn ret = (GridCacheReturn)res;

        Object retval =
            res == null ? null : rawRetval ? ret : (this.retval || op == TRANSFORM) ?
                cctx.unwrapBinaryIfNeeded(ret.value(), keepBinary) : ret.success();

        if (op == TRANSFORM && retval == null)
            retval = Collections.emptyMap();

        if (super.onDone(retval, err)) {
            GridCacheVersion futVer = onFutureDone();

            if (futVer != null)
                cctx.mvcc().removeAtomicFuture(futVer);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    @Override public void onResult(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr) {
        GridNearAtomicFullUpdateRequest req;

        AffinityTopologyVersion remapTopVer = null;

        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;

        boolean rcvAll;

        GridFutureAdapter<?> fut0 = null;

        synchronized (mux) {
            if (!res.futureVersion().equals(futVer))
                return;

            if (singleReq != null) {
                if (!singleReq.nodeId().equals(nodeId))
                    return;

                req = singleReq;

                singleReq = null;

                rcvAll = true;
            }
            else {
                req = mappings != null ? mappings.get(nodeId) : null;

                if (req != null && req.onResponse(res)) {
                    resCnt++;

                    rcvAll = mappings.size() == resCnt;
                }
                else
                    return;
            }

            assert req != null && req.topologyVersion().equals(topVer) : req;

            if (res.remapKeys() != null) {
                assert !fastMap || cctx.kernalContext().clientNode();

                if (remapKeys == null)
                    remapKeys = U.newHashSet(res.remapKeys().size());

                remapKeys.addAll(res.remapKeys());

                if (mapErrTopVer == null || mapErrTopVer.compareTo(req.topologyVersion()) < 0)
                    mapErrTopVer = req.topologyVersion();
            }
            else if (res.error() != null) {
                if (res.failedKeys() != null) {
                    if (err == null)
                        err = new CachePartialUpdateCheckedException(
                            "Failed to update keys (retry update if possible).");

                    Collection<Object> keys = new ArrayList<>(res.failedKeys().size());

                    for (KeyCacheObject key : res.failedKeys())
                        keys.add(cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false));

                    err.add(keys, res.error(), req.topologyVersion());
                }
            }
            else {
                if (!req.fastMap() || req.hasPrimary()) {
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
            }

            if (rcvAll) {
                if (remapKeys != null) {
                    assert mapErrTopVer != null;

                    remapTopVer = cctx.shared().exchange().topologyVersion();
                }
                else {
                    if (err != null &&
                        X.hasCause(err, CachePartialUpdateCheckedException.class) &&
                        X.hasCause(err, ClusterTopologyCheckedException.class) &&
                        storeFuture() &&
                        --remapCnt > 0) {
                        ClusterTopologyCheckedException topErr =
                            X.cause(err, ClusterTopologyCheckedException.class);

                        if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                            CachePartialUpdateCheckedException cause =
                                X.cause(err, CachePartialUpdateCheckedException.class);

                            assert cause != null && cause.topologyVersion() != null : err;

                            remapTopVer =
                                new AffinityTopologyVersion(cause.topologyVersion().topologyVersion() + 1);

                            err = null;

                            Collection<Object> failedKeys = cause.failedKeys();

                            remapKeys = new ArrayList<>(failedKeys.size());

                            for (Object key : failedKeys)
                                remapKeys.add(cctx.toCacheKeyObject(key));

                            updVer = null;
                        }
                    }
                }

                if (remapTopVer == null) {
                    err0 = err;
                    opRes0 = opRes;
                }
                else {
                    fut0 = topCompleteFut;

                    topCompleteFut = null;

                    cctx.mvcc().removeAtomicFuture(futVer);

                    futVer = null;
                    topVer = AffinityTopologyVersion.ZERO;
                }
            }
        }

        if (res.error() != null && res.failedKeys() == null) {
            onDone(res.error());

            return;
        }

        if (rcvAll && nearEnabled) {
            if (mappings != null) {
                for (GridNearAtomicFullUpdateRequest req0 : mappings.values()) {
                    GridNearAtomicUpdateResponse res0 = req0.response();

                    assert res0 != null : req0;

                    updateNear(req0, res0);
                }
            }
            else if (!nodeErr)
                updateNear(req, res);
        }

        if (remapTopVer != null) {
            if (fut0 != null)
                fut0.onDone();

            if (!waitTopFut) {
                onDone(new GridCacheTryPutFailedException());

                return;
            }

            if (topLocked) {
                assert !F.isEmpty(remapKeys) : remapKeys;

                CachePartialUpdateCheckedException e =
                    new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

                ClusterTopologyCheckedException cause = new ClusterTopologyCheckedException(
                    "Failed to update keys, topology changed while execute atomic update inside transaction.");

                cause.retryReadyFuture(cctx.affinity().affinityReadyFuture(remapTopVer));

                e.add(remapKeys, cause);

                onDone(e);

                return;
            }

            IgniteInternalFuture<AffinityTopologyVersion> fut =
                cctx.shared().exchange().affinityReadyFuture(remapTopVer);

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

            return;
        }

        if (rcvAll)
            onDone(opRes0, err0);
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    private void updateNear(GridNearAtomicFullUpdateRequest req, GridNearAtomicUpdateResponse res) {
        assert nearEnabled;

        if (res.remapKeys() != null || !req.hasPrimary())
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /** {@inheritDoc} */
    @Override protected void mapOnTopology() {
        AffinityTopologyVersion topVer;
        GridCacheVersion futVer;

        cache.topology().readLock();

        try {
            if (cache.topology().stopping()) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                    cache.name()));

                return;
            }

            GridDhtTopologyFuture fut = cache.topology().topologyVersionFuture();

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx);

                if (err != null) {
                    onDone(err);

                    return;
                }

                topVer = fut.topologyVersion();

                futVer = addAtomicFuture(topVer);
            }
            else {
                if (waitTopFut) {
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
                }
                else
                    onDone(new GridCacheTryPutFailedException());

                return;
            }
        }
        finally {
            cache.topology().readUnlock();
        }

        if (futVer != null)
            map(topVer, futVer, remapKeys);
    }

    /**
     * Sends messages to remote nodes and updates local cache.
     *
     * @param mappings Mappings to send.
     */
    private void doUpdate(Map<UUID, GridNearAtomicFullUpdateRequest> mappings) {
        UUID locNodeId = cctx.localNodeId();

        GridNearAtomicFullUpdateRequest locUpdate = null;

        // Send messages to remote nodes first, then run local update.
        for (GridNearAtomicFullUpdateRequest req : mappings.values()) {
            if (locNodeId.equals(req.nodeId())) {
                assert locUpdate == null : "Cannot have more than one local mapping [locUpdate=" + locUpdate +
                    ", req=" + req + ']';

                locUpdate = req;
            }
            else {
                try {
                    cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Near update fut, sent request [futId=" + req.futureVersion() +
                            ", writeVer=" + req.updateVersion() +
                            ", node=" + req.nodeId() + ']');
                    }
                }
                catch (IgniteCheckedException e) {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Near update fut, failed to send request [futId=" + req.futureVersion() +
                            ", writeVer=" + req.updateVersion() +
                            ", node=" + req.nodeId() +
                            ", err=" + e + ']');
                    }

                    onSendError(req, e);
                }
            }
        }

        if (locUpdate != null) {
            cache.updateAllAsyncInternal(cctx.localNodeId(), locUpdate,
                new CI2<GridNearAtomicFullUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicFullUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onResult(res.nodeId(), res, false);
                    }
                });
        }

        if (syncMode == FULL_ASYNC)
            onDone(new GridCacheReturn(cctx, true, true, null, true));
    }

    /** {@inheritDoc} */
    @Override protected void map(AffinityTopologyVersion topVer, GridCacheVersion futVer) {
        map(topVer, futVer, null);
    }

    /**
     * @param topVer Topology version.
     * @param futVer Future ID.
     * @param remapKeys Keys to remap.
     */
    void map(AffinityTopologyVersion topVer,
        GridCacheVersion futVer,
        @Nullable Collection<KeyCacheObject> remapKeys) {
        Collection<ClusterNode> topNodes = CU.affinityNodes(cctx, topVer);

        if (F.isEmpty(topNodes)) {
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid)."));

            return;
        }

        GridCacheVersion updVer;

        // Assign version on near node in CLOCK ordering mode even if fastMap is false.
        if (cctx.config().getAtomicWriteOrderMode() == CLOCK) {
            updVer = this.updVer;

            if (updVer == null) {
                updVer = futVer;

                if (log.isDebugEnabled())
                    log.debug("Assigned fast-map version for update on near node: " + updVer);
            }
        }
        else
            updVer = null;

        Exception err = null;
        GridNearAtomicFullUpdateRequest singleReq0 = null;
        Map<UUID, GridNearAtomicFullUpdateRequest> mappings0 = null;

        int size = keys.size();

        try {
            if (size == 1 && !fastMap) {
                assert remapKeys == null || remapKeys.size() == 1;

                singleReq0 = mapSingleUpdate(topVer, futVer, updVer);
            }
            else {
                Map<UUID, GridNearAtomicFullUpdateRequest> pendingMappings = mapUpdate(topNodes,
                    topVer,
                    futVer,
                    updVer,
                    remapKeys);

                if (pendingMappings.size() == 1)
                    singleReq0 = F.firstValue(pendingMappings);
                else {
                    if (syncMode == PRIMARY_SYNC) {
                        mappings0 = U.newHashMap(pendingMappings.size());

                        for (GridNearAtomicFullUpdateRequest req : pendingMappings.values()) {
                            if (req.hasPrimary())
                                mappings0.put(req.nodeId(), req);
                        }
                    }
                    else
                        mappings0 = pendingMappings;

                    assert !mappings0.isEmpty() || size == 0 : this;
                }
            }

            synchronized (mux) {
                assert this.futVer == futVer || (this.isDone() && this.error() != null);
                assert this.topVer == topVer;

                this.updVer = updVer;

                resCnt = 0;

                singleReq = singleReq0;
                mappings = mappings0;

                this.remapKeys = null;
            }
        }
        catch (Exception e) {
            err = e;
        }

        if (err != null) {
            onDone(err);

            return;
        }

        // Optimize mapping for single key.
        if (singleReq0 != null)
            mapSingle(singleReq0.nodeId(), singleReq0);
        else {
            assert mappings0 != null;

            if (size == 0)
                onDone(new GridCacheReturn(cctx, true, true, null, true));
            else
                doUpdate(mappings0);
        }
    }

    /**
     * @return Future version.
     */
    private GridCacheVersion onFutureDone() {
        GridCacheVersion ver0;

        GridFutureAdapter<Void> fut0;

        synchronized (mux) {
            fut0 = topCompleteFut;

            topCompleteFut = null;

            ver0 = futVer;

            futVer = null;
        }

        if (fut0 != null)
            fut0.onDone();

        return ver0;
    }

    /**
     * @param topNodes Cache nodes.
     * @param topVer Topology version.
     * @param futVer Future version.
     * @param updVer Update version.
     * @param remapKeys Keys to remap.
     * @return Mapping.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private Map<UUID, GridNearAtomicFullUpdateRequest> mapUpdate(Collection<ClusterNode> topNodes,
        AffinityTopologyVersion topVer,
        GridCacheVersion futVer,
        @Nullable GridCacheVersion updVer,
        @Nullable Collection<KeyCacheObject> remapKeys) throws Exception {
        Iterator<?> it = null;

        if (vals != null)
            it = vals.iterator();

        Iterator<GridCacheDrInfo> conflictPutValsIt = null;

        if (conflictPutVals != null)
            conflictPutValsIt = conflictPutVals.iterator();

        Iterator<GridCacheVersion> conflictRmvValsIt = null;

        if (conflictRmvVals != null)
            conflictRmvValsIt = conflictRmvVals.iterator();

        Map<UUID, GridNearAtomicFullUpdateRequest> pendingMappings = U.newHashMap(topNodes.size());

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

            if (op != TRANSFORM)
                val = cctx.toCacheObject(val);
            else
                val = EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), (EntryProcessor)val);

            List<ClusterNode> affNodes = mapKey(cacheKey, topVer);

            if (affNodes.isEmpty())
                throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                    "(all partition nodes left the grid).");

            int i = 0;

            for (int n = 0; n < affNodes.size(); n++) {
                ClusterNode affNode = affNodes.get(n);

                if (affNode == null)
                    throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                        "(all partition nodes left the grid).");

                UUID nodeId = affNode.id();

                GridNearAtomicFullUpdateRequest mapped = pendingMappings.get(nodeId);

                if (mapped == null) {
                    mapped = new GridNearAtomicFullUpdateRequest(
                        cctx.cacheId(),
                        nodeId,
                        futVer,
                        fastMap,
                        updVer,
                        topVer,
                        topLocked,
                        syncMode,
                        op,
                        retval,
                        expiryPlc,
                        invokeArgs,
                        filter,
                        subjId,
                        taskNameHash,
                        skipStore,
                        keepBinary,
                        cctx.kernalContext().clientNode(),
                        cctx.deploymentEnabled(),
                        keys.size());

                    pendingMappings.put(nodeId, mapped);
                }

                mapped.addUpdateEntry(cacheKey, val, conflictTtl, conflictExpireTime, conflictVer, i == 0);

                i++;
            }
        }

        return pendingMappings;
    }

    /**
     * @param topVer Topology version.
     * @param futVer Future version.
     * @param updVer Update version.
     * @return Request.
     * @throws Exception If failed.
     */
    private GridNearAtomicFullUpdateRequest mapSingleUpdate(AffinityTopologyVersion topVer,
        GridCacheVersion futVer,
        @Nullable GridCacheVersion updVer) throws Exception {
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

        if (op != TRANSFORM)
            val = cctx.toCacheObject(val);
        else
            val = EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), (EntryProcessor)val);

        ClusterNode primary = cctx.affinity().primaryByPartition(cacheKey.partition(), topVer);

        if (primary == null)
            throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid).");

        GridNearAtomicFullUpdateRequest req = new GridNearAtomicFullUpdateRequest(
            cctx.cacheId(),
            primary.id(),
            futVer,
            fastMap,
            updVer,
            topVer,
            topLocked,
            syncMode,
            op,
            retval,
            expiryPlc,
            invokeArgs,
            filter,
            subjId,
            taskNameHash,
            skipStore,
            keepBinary,
            cctx.kernalContext().clientNode(),
            cctx.deploymentEnabled(),
            1);

        req.addUpdateEntry(cacheKey,
            val,
            conflictTtl,
            conflictExpireTime,
            conflictVer,
            true);

        return req;
    }

    /**
     * Maps key to nodes. If filters are absent and operation is not TRANSFORM, then we can assign version on near
     * node and send updates in parallel to all participating nodes.
     *
     * @param key Key to map.
     * @param topVer Topology version to map.
     * @return Collection of nodes to which key is mapped.
     */
    private List<ClusterNode> mapKey(KeyCacheObject key, AffinityTopologyVersion topVer) {
        GridCacheAffinityManager affMgr = cctx.affinity();

        // If we can send updates in parallel - do it.
        return fastMap ? cctx.topology().nodes(affMgr.partition(key), topVer) :
            Collections.singletonList(affMgr.primaryByKey(key, topVer));
    }

    /** {@inheritDoc} */
    public String toString() {
        synchronized (mux) {
            return S.toString(GridNearAtomicUpdateFuture.class, this, super.toString());
        }
    }
}
