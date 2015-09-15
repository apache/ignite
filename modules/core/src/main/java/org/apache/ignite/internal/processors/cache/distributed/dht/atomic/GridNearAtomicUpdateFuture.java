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
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicUpdateFuture extends GridFutureAdapter<Object>
    implements GridCacheAtomicFuture<Object>{
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Cache. */
    private GridDhtAtomicCache cache;

    /** Update operation. */
    private final GridCacheOperation op;

    /** Keys */
    private Collection<?> keys;

    /** Values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<?> vals;

    /** Optional arguments for entry processor. */
    private Object[] invokeArgs;

    /** Conflict put values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<GridCacheDrInfo> conflictPutVals;

    /** Conflict remove values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<GridCacheVersion> conflictRmvVals;

    /** Return value require flag. */
    private final boolean retval;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Optional filter. */
    private final CacheEntryPredicate[] filter;

    /** Write synchronization mode. */
    private final CacheWriteSynchronizationMode syncMode;

    /** Raw return value flag. */
    private final boolean rawRetval;

    /** Fast map flag. */
    private final boolean fastMap;

    /** Near cache flag. */
    private final boolean nearEnabled;

    /** Subject ID. */
    private final UUID subjId;

    /** Task name hash. */
    private final int taskNameHash;

    /** Topology locked flag. Set if atomic update is performed inside a TX or explicit lock. */
    private boolean topLocked;

    /** Skip store flag. */
    private final boolean skipStore;

    /** Wait for topology future flag. */
    private final boolean waitTopFut;

    /** Remap count. */
    private int remapCnt;

    /** State. */
    private final UpdateState state;

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
        int remapCnt,
        boolean waitTopFut
    ) {
        this.rawRetval = rawRetval;

        assert vals == null || vals.size() == keys.size();
        assert conflictPutVals == null || conflictPutVals.size() == keys.size();
        assert conflictRmvVals == null || conflictRmvVals.size() == keys.size();
        assert subjId != null;

        this.cctx = cctx;
        this.cache = cache;
        this.syncMode = syncMode;
        this.op = op;
        this.keys = keys;
        this.vals = vals;
        this.invokeArgs = invokeArgs;
        this.conflictPutVals = conflictPutVals;
        this.conflictRmvVals = conflictRmvVals;
        this.retval = retval;
        this.expiryPlc = expiryPlc;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.waitTopFut = waitTopFut;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridFutureAdapter.class);

        fastMap = F.isEmpty(filter) && op != TRANSFORM && cctx.config().getWriteSynchronizationMode() == FULL_SYNC &&
            cctx.config().getAtomicWriteOrderMode() == CLOCK &&
            !(cctx.writeThrough() && cctx.config().getInterceptor() != null);

        nearEnabled = CU.isNearEnabled(cctx);

        if (!waitTopFut)
            remapCnt = 1;

        this.remapCnt = remapCnt;

        state = new UpdateState();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return state.futureVersion();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends ClusterNode> nodes() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return {@code True} if this future should block partition map exchange.
     */
    private boolean waitForPartitionExchange() {
        // Wait fast-map near atomic update futures in CLOCK mode.
        return fastMap;
    }

    /** {@inheritDoc} */
    @Override public Collection<?> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        state.onNodeLeft(nodeId);

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

    /**
     * Performs future mapping.
     */
    public void map() {
        AffinityTopologyVersion topVer = null;

        IgniteInternalTx tx = cctx.tm().anyActiveThreadTx(null);

        if (tx != null && tx.topologyVersionSnapshot() != null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer == null)
            topVer = cctx.mvcc().lastExplicitLockTopologyVersion(Thread.currentThread().getId());

        if (topVer == null)
            mapOnTopology();
        else {
            topLocked = true;

            // Cannot remap.
            remapCnt = 1;

            state.map(topVer);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        if (waitForPartitionExchange()) {
            GridFutureAdapter<Void> fut = state.completeFuture(topVer);

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
            res == null ? null : rawRetval ? ret : (this.retval || op == TRANSFORM) ? ret.value() : ret.success();

        if (op == TRANSFORM && retval == null)
            retval = Collections.emptyMap();

        if (super.onDone(retval, err)) {
            GridCacheVersion futVer = state.onFutureDone();

            if (futVer != null)
                cctx.mvcc().removeAtomicFuture(futVer);

            return true;
        }

        return false;
    }

    /**
     * Response callback.
     *
     * @param nodeId Node ID.
     * @param res Update response.
     */
    public void onResult(UUID nodeId, GridNearAtomicUpdateResponse res) {
        state.onResult(nodeId, res, false);
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    private void updateNear(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
        if (!nearEnabled || !req.hasPrimary())
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /**
     * Maps future on ready topology.
     */
    private void mapOnTopology() {
        cache.topology().readLock();

        AffinityTopologyVersion topVer = null;

        try {
            if (cache.topology().stopping()) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                    cache.name()));

                return;
            }

            GridDhtTopologyFuture fut = cache.topology().topologyVersionFuture();

            if (fut.isDone()) {
                if (!fut.isCacheTopologyValid(cctx)) {
                    onDone(new IgniteCheckedException("Failed to perform cache operation (cache topology is not valid): " +
                        cctx.name()));

                    return;
                }

                topVer = fut.topologyVersion();
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

        state.map(topVer);
    }

    /**
     * @return {@code True} future is stored by {@link GridCacheMvccManager#addAtomicFuture}.
     */
    private boolean storeFuture() {
        return cctx.config().getAtomicWriteOrderMode() == CLOCK || syncMode != FULL_ASYNC;
    }

    /**
     * Maps key to nodes. If filters are absent and operation is not TRANSFORM, then we can assign version on near
     * node and send updates in parallel to all participating nodes.
     *
     * @param key Key to map.
     * @param topVer Topology version to map.
     * @param fastMap Flag indicating whether mapping is performed for fast-circuit update.
     * @return Collection of nodes to which key is mapped.
     */
    private Collection<ClusterNode> mapKey(
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        boolean fastMap
    ) {
        GridCacheAffinityManager affMgr = cctx.affinity();

        // If we can send updates in parallel - do it.
        return fastMap ?
            cctx.topology().nodes(affMgr.partition(key), topVer) :
            Collections.singletonList(affMgr.primary(key, topVer));
    }

    /**
     * Maps future to single node.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void mapSingle(UUID nodeId, GridNearAtomicUpdateRequest req) {
        if (cctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req,
                new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onResult(res.nodeId(), res);
                    }
                });
        }
        else {
            try {
                if (log.isDebugEnabled())
                    log.debug("Sending near atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (syncMode == FULL_ASYNC)
                    onDone(new GridCacheReturn(cctx, true, null, true));
            }
            catch (IgniteCheckedException e) {
                state.onSendError(req, e);
            }
        }
    }

    /**
     * Sends messages to remote nodes and updates local cache.
     *
     * @param mappings Mappings to send.
     */
    private void doUpdate(Map<UUID, GridNearAtomicUpdateRequest> mappings) {
        UUID locNodeId = cctx.localNodeId();

        GridNearAtomicUpdateRequest locUpdate = null;

        // Send messages to remote nodes first, then run local update.
        for (GridNearAtomicUpdateRequest req : mappings.values()) {
            if (locNodeId.equals(req.nodeId())) {
                assert locUpdate == null : "Cannot have more than one local mapping [locUpdate=" + locUpdate +
                    ", req=" + req + ']';

                locUpdate = req;
            }
            else {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending near atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                    cctx.io().send(req.nodeId(), req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    state.onSendError(req, e);
                }
            }
        }

        if (locUpdate != null) {
            cache.updateAllAsyncInternal(cctx.localNodeId(), locUpdate,
                new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onResult(res.nodeId(), res);
                    }
                });
        }

        if (syncMode == FULL_ASYNC)
            onDone(new GridCacheReturn(cctx, true, null, true));
    }

    /**
     *
     */
    private class UpdateState {
        /** Current topology version. */
        private AffinityTopologyVersion topVer = AffinityTopologyVersion.ZERO;

        /** */
        private GridCacheVersion updVer;

        /** Topology version when got mapping error. */
        private AffinityTopologyVersion mapErrTopVer;

        /** Mappings if operations is mapped to more than one node. */
        @GridToStringInclude
        private Map<UUID, GridNearAtomicUpdateRequest> mappings;

        /** Error. */
        private CachePartialUpdateCheckedException err;

        /** Future ID. */
        private GridCacheVersion futVer;

        /** Completion future for a particular topology version. */
        private GridFutureAdapter<Void> topCompleteFut;

        /** Keys to remap. */
        private Collection<KeyCacheObject> remapKeys;

        /** Not null is operation is mapped to single node. */
        private GridNearAtomicUpdateRequest singleReq;

        /** Operation result. */
        private GridCacheReturn opRes;

        /**
         * @return Future version.
         */
        @Nullable synchronized GridCacheVersion futureVersion() {
            return futVer;
        }

        /**
         * @param nodeId Left node ID.
         */
        void onNodeLeft(UUID nodeId) {
            GridNearAtomicUpdateResponse res = null;

            synchronized (this) {
                GridNearAtomicUpdateRequest req;

                if (singleReq != null)
                    req = singleReq.nodeId().equals(nodeId) ? singleReq : null;
                else
                    req = mappings != null ? mappings.get(nodeId) : null;

                if (req != null) {
                    res = new GridNearAtomicUpdateResponse(cctx.cacheId(), nodeId, req.futureVersion());

                    res.addFailedKeys(req.keys(), new ClusterTopologyCheckedException("Primary node left grid before " +
                        "response is received: " + nodeId));
                }
            }

            if (res != null)
                onResult(nodeId, res, true);
        }

        /**
         * @param nodeId Node ID.
         * @param res Response.
         * @param nodeErr {@code True} if response was created on node failure.
         */
        void onResult(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr) {
            GridNearAtomicUpdateRequest req;

            AffinityTopologyVersion remapTopVer = null;

            GridCacheReturn opRes0 = null;
            CachePartialUpdateCheckedException err0 = null;

            boolean rcvAll;

            GridFutureAdapter<?> fut0 = null;

            synchronized (this) {
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
                    req = mappings != null ? mappings.remove(nodeId) : null;

                    if (req != null)
                        rcvAll = mappings.isEmpty();
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
                    if (res.failedKeys() != null)
                        addFailedKeys(res.failedKeys(), req.topologyVersion(), res.error());
                }
                else {
                    if (!req.fastMap() || req.hasPrimary()) {
                        GridCacheReturn ret = res.returnValue();

                        if (op == TRANSFORM) {
                            if (ret != null)
                                addInvokeResults(ret);
                        }
                        else
                            opRes = ret;
                    }
                }

                if (rcvAll) {
                    if (remapKeys != null) {
                        assert mapErrTopVer != null;

                        remapTopVer = new AffinityTopologyVersion(mapErrTopVer.topologyVersion() + 1);
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

            if (!nodeErr && res.remapKeys() == null)
                updateNear(req, res);

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

                IgniteInternalFuture<AffinityTopologyVersion> fut = cctx.affinity().affinityReadyFuture(remapTopVer);

                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(final IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                            @Override public void run() {
                                try {
                                    AffinityTopologyVersion topVer = fut.get();

                                    map(topVer);
                                }
                                catch (IgniteCheckedException e) {
                                    onDone(e);
                                }
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
         * @param req Request.
         * @param e Error.
         */
        void onSendError(GridNearAtomicUpdateRequest req, IgniteCheckedException e) {
            synchronized (this) {
                GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                    req.nodeId(),
                    req.futureVersion());

                res.addFailedKeys(req.keys(), e);

                onResult(req.nodeId(), res, true);
            }
        }

        /**
         * @param topVer Topology version.
         */
        void map(AffinityTopologyVersion topVer) {
            Collection<ClusterNode> topNodes = CU.affinityNodes(cctx, topVer);

            if (F.isEmpty(topNodes)) {
                onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                    "left the grid)."));

                return;
            }

            Exception err = null;
            Map<UUID, GridNearAtomicUpdateRequest> pendingMappings = null;

            int size = keys.size();

            synchronized (this) {
                assert futVer == null : this;
                assert this.topVer == AffinityTopologyVersion.ZERO : this;

                this.topVer = topVer;

                futVer = cctx.versions().next(topVer);

                if (storeFuture())
                    cctx.mvcc().addAtomicFuture(futVer, GridNearAtomicUpdateFuture.this);

                // Assign version on near node in CLOCK ordering mode even if fastMap is false.
                if (updVer == null)
                    updVer = cctx.config().getAtomicWriteOrderMode() == CLOCK ? cctx.versions().next(topVer) : null;

                if (updVer != null && log.isDebugEnabled())
                    log.debug("Assigned fast-map version for update on near node: " + updVer);

                try {
                    if (size == 1 && !fastMap) {
                        assert remapKeys == null || remapKeys.size() == 1;

                        singleReq = mapSingleUpdate();
                    }
                    else {
                        pendingMappings = mapUpdate(topNodes);

                        if (pendingMappings.size() == 1)
                            singleReq = F.firstValue(pendingMappings);
                        else {
                            if (syncMode == PRIMARY_SYNC) {
                                mappings = U.newHashMap(pendingMappings.size());

                                for (GridNearAtomicUpdateRequest req : pendingMappings.values()) {
                                    if (req.hasPrimary())
                                        mappings.put(req.nodeId(), req);
                                }
                            }
                            else
                                mappings = new HashMap<>(pendingMappings);

                            assert !mappings.isEmpty() || size == 0 : GridNearAtomicUpdateFuture.this;
                        }
                    }

                    remapKeys = null;
                }
                catch (Exception e) {
                    err = e;
                }
            }

            if (err != null) {
                onDone(err);

                return;
            }

            // Optimize mapping for single key.
            if (singleReq != null)
                mapSingle(singleReq.nodeId(), singleReq);
            else {
                assert pendingMappings != null;

                if (size == 0)
                    onDone(new GridCacheReturn(cctx, true, null, true));
                else
                    doUpdate(pendingMappings);
            }
        }

        /**
         * @param topVer Topology version.
         * @return Future.
         */
        @Nullable synchronized GridFutureAdapter<Void> completeFuture(AffinityTopologyVersion topVer) {
            if (this.topVer == AffinityTopologyVersion.ZERO)
                return null;

            if (this.topVer.compareTo(topVer) < 0) {
                if (topCompleteFut == null)
                    topCompleteFut = new GridFutureAdapter<>();

                return topCompleteFut;
            }

            return null;
        }

        /**
         * @return Future version.
         */
        GridCacheVersion onFutureDone() {
            GridCacheVersion ver0;

            GridFutureAdapter<Void> fut0;

            synchronized (this) {
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
         * @return Mapping.
         * @throws Exception If failed.
         */
        private Map<UUID, GridNearAtomicUpdateRequest> mapUpdate(Collection<ClusterNode> topNodes) throws Exception {
            Iterator<?> it = null;

            if (vals != null)
                it = vals.iterator();

            Iterator<GridCacheDrInfo> conflictPutValsIt = null;

            if (conflictPutVals != null)
                conflictPutValsIt = conflictPutVals.iterator();

            Iterator<GridCacheVersion> conflictRmvValsIt = null;

            if (conflictRmvVals != null)
                conflictRmvValsIt = conflictRmvVals.iterator();

            Map<UUID, GridNearAtomicUpdateRequest> pendingMappings = U.newHashMap(topNodes.size());

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
                    GridCacheDrInfo conflictPutVal =  conflictPutValsIt.next();

                    val = conflictPutVal.value();
                    conflictVer = conflictPutVal.version();
                    conflictTtl =  conflictPutVal.ttl();
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

                Collection<ClusterNode> affNodes = mapKey(cacheKey, topVer, fastMap);

                if (affNodes.isEmpty())
                    throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                        "(all partition nodes left the grid).");

                int i = 0;

                for (ClusterNode affNode : affNodes) {
                    if (affNode == null)
                        throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                            "(all partition nodes left the grid).");

                    UUID nodeId = affNode.id();

                    GridNearAtomicUpdateRequest mapped = pendingMappings.get(nodeId);

                    if (mapped == null) {
                        mapped = new GridNearAtomicUpdateRequest(
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
                            cctx.kernalContext().clientNode());

                        pendingMappings.put(nodeId, mapped);
                    }

                    mapped.addUpdateEntry(cacheKey, val, conflictTtl, conflictExpireTime, conflictVer, i == 0);

                    i++;
                }
            }

            return pendingMappings;
        }

        /**
         * @return Request.
         * @throws Exception If failed.
         */
        private GridNearAtomicUpdateRequest mapSingleUpdate() throws Exception {
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

                val = conflictPutVal.value();
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

            ClusterNode primary = cctx.affinity().primary(cacheKey, topVer);

            if (primary == null)
                throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                    "left the grid).");

            GridNearAtomicUpdateRequest req = new GridNearAtomicUpdateRequest(
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
                cctx.kernalContext().clientNode());

            req.addUpdateEntry(cacheKey,
                val,
                conflictTtl,
                conflictExpireTime,
                conflictVer,
                true);

            return req;
        }

        /**
         * @param ret Result from single node.
         */
        @SuppressWarnings("unchecked")
        private void addInvokeResults(GridCacheReturn ret) {
            assert op == TRANSFORM : op;
            assert ret.value() == null || ret.value() instanceof Map : ret.value();

            if (ret.value() != null) {
                if (opRes != null)
                    opRes.mergeEntryProcessResults(ret);
                else
                    opRes = ret;
            }
        }

        /**
         * @param failedKeys Failed keys.
         * @param topVer Topology version for failed update.
         * @param err Error cause.
         */
        private void addFailedKeys(Collection<KeyCacheObject> failedKeys,
            AffinityTopologyVersion topVer,
            Throwable err) {
            CachePartialUpdateCheckedException err0 = this.err;

            if (err0 == null)
                err0 = this.err = new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

            Collection<Object> keys = new ArrayList<>(failedKeys.size());

            for (KeyCacheObject key : failedKeys)
                keys.add(key.value(cctx.cacheObjectContext(), false));

            err0.add(keys, err, topVer);
        }

        /** {@inheritDoc} */
        @Override public synchronized  String toString() {
            return S.toString(UpdateState.class, this);
        }
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridNearAtomicUpdateFuture.class, this, super.toString());
    }
}
