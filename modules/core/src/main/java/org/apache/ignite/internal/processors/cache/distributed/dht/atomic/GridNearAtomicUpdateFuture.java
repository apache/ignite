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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicUpdateFuture<K, V> extends GridFutureAdapter<Object>
    implements GridCacheAtomicFuture<K, Object>{
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Cache context. */
    private final GridCacheContext<K, V> cctx;

    /** Cache. */
    private GridDhtAtomicCache<K, V> cache;

    /** Future ID. */
    private volatile GridCacheVersion futVer;

    /** Update operation. */
    private final GridCacheOperation op;

    /** Keys */
    private Collection<? extends K> keys;

    /** Values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<?> vals;

    /** Optional arguments for entry processor. */
    private Object[] invokeArgs;

    /** DR put values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<GridCacheDrInfo<V>> drPutVals;

    /** DR remove values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Collection<GridCacheVersion> drRmvVals;

    /** Mappings. */
    @GridToStringInclude
    private final ConcurrentMap<UUID, GridNearAtomicUpdateRequest<K, V>> mappings;

    /** Error. */
    private volatile CachePartialUpdateCheckedException err;

    /** Operation result. */
    private volatile GridCacheReturn<Object> opRes;

    /** Return value require flag. */
    private final boolean retval;

    /** Cached entry if keys size is 1. */
    private GridCacheEntryEx<K, V> cached;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Future map topology version. */
    private long topVer;

    /** Optional filter. */
    private final IgnitePredicate<Cache.Entry<K, V>>[] filter;

    /** Write synchronization mode. */
    private final CacheWriteSynchronizationMode syncMode;

    /** If this future mapped to single node. */
    private volatile Boolean single;

    /** If this future is mapped to a single node, this field will contain that node ID. */
    private UUID singleNodeId;

    /** Single update request. */
    private GridNearAtomicUpdateRequest<K, V> singleReq;

    /** Raw return value flag. */
    private boolean rawRetval;

    /** Fast map flag. */
    private final boolean fastMap;

    /** Near cache flag. */
    private final boolean nearEnabled;

    /** Subject ID. */
    private final UUID subjId;

    /** Task name hash. */
    private final int taskNameHash;

    /** Map time. */
    private volatile long mapTime;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateFuture() {
        cctx = null;
        mappings = null;
        futVer = null;
        retval = false;
        fastMap = false;
        expiryPlc = null;
        filter = null;
        syncMode = null;
        op = null;
        nearEnabled = false;
        subjId = null;
        taskNameHash = 0;
    }

    /**
     * @param cctx Cache context.
     * @param cache Cache instance.
     * @param op Update operation.
     * @param syncMode Write synchronization mode.
     * @param keys Keys to update.
     * @param vals Values or transform closure.
     * @param invokeArgs Optional arguments for entry processor.
     * @param drPutVals DR put values (optional).
     * @param drRmvVals DR remove values (optional).
     * @param retval Return value require flag.
     * @param rawRetval {@code True} if should return {@code GridCacheReturn} as future result.
     * @param cached Cached entry if keys size is 1.
     * @param expiryPlc Expiry policy explicitly specified for cache operation.
     * @param filter Entry filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridNearAtomicUpdateFuture(
        GridCacheContext<K, V> cctx,
        GridDhtAtomicCache<K, V> cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        Collection<? extends K> keys,
        @Nullable Collection<?> vals,
        @Nullable Object[] invokeArgs,
        @Nullable Collection<GridCacheDrInfo<V>> drPutVals,
        @Nullable Collection<GridCacheVersion> drRmvVals,
        final boolean retval,
        final boolean rawRetval,
        @Nullable GridCacheEntryEx<K, V> cached,
        @Nullable ExpiryPolicy expiryPlc,
        final IgnitePredicate<Cache.Entry<K, V>>[] filter,
        UUID subjId,
        int taskNameHash
    ) {
        super(cctx.kernalContext());

        this.rawRetval = rawRetval;

        assert vals == null || vals.size() == keys.size();
        assert drPutVals == null || drPutVals.size() == keys.size();
        assert drRmvVals == null || drRmvVals.size() == keys.size();
        assert cached == null || keys.size() == 1;
        assert subjId != null;

        this.cctx = cctx;
        this.cache = cache;
        this.syncMode = syncMode;
        this.op = op;
        this.keys = keys;
        this.vals = vals;
        this.invokeArgs = invokeArgs;
        this.drPutVals = drPutVals;
        this.drRmvVals = drRmvVals;
        this.retval = retval;
        this.cached = cached;
        this.expiryPlc = expiryPlc;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        log = U.logger(ctx, logRef, GridFutureAdapter.class);

        mappings = new ConcurrentHashMap8<>(keys.size(), 1.0f);

        fastMap = F.isEmpty(filter) && op != TRANSFORM && cctx.config().getWriteSynchronizationMode() == FULL_SYNC &&
            cctx.config().getAtomicWriteOrderMode() == CLOCK &&
            !(cctx.writeThrough() && cctx.config().getInterceptor() != null);

        nearEnabled = CU.isNearEnabled(cctx);
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
    @Override public Collection<? extends ClusterNode> nodes() {
        return F.view(F.viewReadOnly(mappings.keySet(), U.id2Node(cctx.kernalContext())), F.notNull());
    }

    /** {@inheritDoc} */
    @Override public boolean waitForPartitionExchange() {
        // Wait fast-map near atomic update futures in CLOCK mode.
        return fastMap;
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends K> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        Boolean single0 = single;

        if (single0 != null && single0) {
            if (singleNodeId.equals(nodeId)) {
                onDone(addFailedKeys(
                    singleReq.keys(),
                    new ClusterTopologyCheckedException("Primary node left grid before response is received: " + nodeId)));

                return true;
            }

            return false;
        }

        GridNearAtomicUpdateRequest<K, V> req = mappings.get(nodeId);

        if (req != null) {
            addFailedKeys(req.keys(), new ClusterTopologyCheckedException("Primary node left grid before response is " +
                "received: " + nodeId));

            mappings.remove(nodeId);

            checkComplete();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void checkTimeout(long timeout) {
        long mapTime0 = mapTime;

        if (mapTime0 > 0 && U.currentTimeMillis() > mapTime0 + timeout)
            onDone(new CacheAtomicUpdateTimeoutCheckedException("Cache update timeout out " +
                "(consider increasing networkTimeout configuration property)."));
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
        mapOnTopology(keys, false, null);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        assert res == null || res instanceof GridCacheReturn;

        GridCacheReturn ret = (GridCacheReturn)res;

        Object retval = res == null ? null : rawRetval ? ret : this.retval ? ret.value() : ret.success();

        if (op == TRANSFORM && retval == null)
            retval = Collections.emptyMap();

        if (super.onDone(retval, err)) {
            cctx.mvcc().removeAtomicFuture(version());

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
    public void onResult(UUID nodeId, GridNearAtomicUpdateResponse<K, V> res) {
        if (res.remapKeys() != null) {
            assert cctx.config().getAtomicWriteOrderMode() == PRIMARY;

            mapOnTopology(res.remapKeys(), true, nodeId);

            return;
        }

        Boolean single0 = single;

        if (single0 != null && single0) {
            assert singleNodeId.equals(nodeId) : "Invalid response received for single-node mapped future " +
                "[singleNodeId=" + singleNodeId + ", nodeId=" + nodeId + ", res=" + res + ']';

            updateNear(singleReq, res);

            if (res.error() != null)
                onDone(addFailedKeys(res.failedKeys(), res.error()));
            else {
                GridCacheReturn<Object> opRes0 = opRes = res.returnValue();

                onDone(opRes0);
            }
        }
        else {
            GridNearAtomicUpdateRequest<K, V> req = mappings.get(nodeId);

            if (req != null) { // req can be null if onResult is being processed concurrently with onNodeLeft.
                updateNear(req, res);

                if (res.error() != null)
                    addFailedKeys(req.keys(), res.error());
                else {
                    if (op == TRANSFORM) {
                        assert !req.fastMap();

                        if (res.returnValue() != null)
                            addInvokeResults(res.returnValue());
                    }
                    else if (req.fastMap() && req.hasPrimary())
                        opRes = res.returnValue();
                }

                mappings.remove(nodeId);
            }

            checkComplete();
        }
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    private void updateNear(GridNearAtomicUpdateRequest<K, V> req, GridNearAtomicUpdateResponse<K, V> res) {
        if (!nearEnabled || !req.hasPrimary())
            return;

        GridNearAtomicCache<K, V> near = (GridNearAtomicCache<K, V>)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /**
     * Maps future on ready topology.
     *
     * @param keys Keys to map.
     * @param remap Boolean flag indicating if this is partial future remap.
     * @param oldNodeId Old node ID if remap.
     */
    private void mapOnTopology(final Collection<? extends K> keys, final boolean remap, final UUID oldNodeId) {
        cache.topology().readLock();

        GridDiscoveryTopologySnapshot snapshot = null;

        try {
            GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

            if (fut.isDone()) {
                if (futVer == null)
                    // Assign future version in topology read lock before first exception may be thrown.
                    futVer = cctx.versions().next(topVer);

                // We are holding topology read lock and current topology is ready, we can start mapping.
                snapshot = fut.topologySnapshot();
            }
            else {
                fut.listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                    @Override public void apply(IgniteInternalFuture<Long> t) {
                        mapOnTopology(keys, remap, oldNodeId);
                    }
                });

                return;
            }

            topVer = snapshot.topologyVersion();

            mapTime = U.currentTimeMillis();

            if (!remap && (cctx.config().getAtomicWriteOrderMode() == CLOCK || syncMode != FULL_ASYNC))
                cctx.mvcc().addAtomicFuture(version(), this);
        }
        catch (IgniteCheckedException e) {
            onDone(new IgniteCheckedException("Failed to get topology snapshot for update operation: " + this, e));

            return;
        }
        finally {
            cache.topology().readUnlock();
        }

        map0(snapshot, keys, remap, oldNodeId);
    }

    /**
     * Checks if future is ready to be completed.
     */
    private synchronized void checkComplete() {
        if ((syncMode == FULL_ASYNC && cctx.config().getAtomicWriteOrderMode() == PRIMARY) || mappings.isEmpty()) {
            CachePartialUpdateCheckedException err0 = err;

            if (err0 != null)
                onDone(err0);
            else
                onDone(opRes);
        }
    }

    /**
     * @param topSnapshot Topology snapshot to map on.
     * @param keys Keys to map.
     * @param remap Flag indicating if this is partial remap for this future.
     * @param oldNodeId Old node ID if was remap.
     */
    private void map0(GridDiscoveryTopologySnapshot topSnapshot,
        Collection<? extends K> keys,
        boolean remap,
        @Nullable UUID oldNodeId) {
        assert oldNodeId == null || remap;

        long topVer = topSnapshot.topologyVersion();

        Collection<ClusterNode> topNodes = CU.affinityNodes(cctx, topVer);

        if (F.isEmpty(topNodes)) {
            onDone(new ClusterTopologyCheckedException("Failed to map keys for cache (all partition nodes left the grid)."));

            return;
        }

        CacheConfiguration ccfg = cctx.config();

        // Assign version on near node in CLOCK ordering mode even if fastMap is false.
        GridCacheVersion updVer = ccfg.getAtomicWriteOrderMode() == CLOCK ? cctx.versions().next(topVer) : null;

        if (updVer != null && log.isDebugEnabled())
            log.debug("Assigned fast-map version for update on near node: " + updVer);

        if (keys.size() == 1 && !fastMap && (single == null || single)) {
            K key = F.first(keys);

            Object val;
            long drTtl;
            long drExpireTime;
            GridCacheVersion drVer;

            if (vals != null) {
                val = F.first(vals);
                drTtl = -1;
                drExpireTime = -1;
                drVer = null;
            }
            else if (drPutVals != null) {
                GridCacheDrInfo<V> drPutVal =  F.first(drPutVals);

                val = drPutVal.value();
                drTtl = drPutVal.ttl();
                drExpireTime = drPutVal.expireTime();
                drVer = drPutVal.version();
            }
            else if (drRmvVals != null) {
                val = null;
                drTtl = -1;
                drExpireTime = -1;
                drVer = F.first(drRmvVals);
            }
            else {
                val = null;
                drTtl = -1;
                drExpireTime = -1;
                drVer = null;
            }

            // We still can get here if user pass map with single element.
            if (key == null) {
                NullPointerException err = new NullPointerException("Null key.");

                onDone(err);

                throw err;
            }

            if (val == null && op != GridCacheOperation.DELETE) {
                NullPointerException err = new NullPointerException("Null value.");

                onDone(err);

                throw err;
            }

            if (cctx.portableEnabled()) {
                key = (K)cctx.marshalToPortable(key);

                if (op != TRANSFORM)
                    val = cctx.marshalToPortable(val);
            }

            Collection<ClusterNode> primaryNodes = mapKey(key, topVer, fastMap);

            // One key and no backups.
            assert primaryNodes.size() == 1 : "Should be mapped to single node: " + primaryNodes;

            ClusterNode primary = F.first(primaryNodes);

            GridNearAtomicUpdateRequest<K, V> req = new GridNearAtomicUpdateRequest<>(
                cctx.cacheId(),
                primary.id(),
                futVer,
                fastMap,
                updVer,
                topSnapshot.topologyVersion(),
                syncMode,
                op,
                retval,
                op == TRANSFORM && cctx.hasFlag(FORCE_TRANSFORM_BACKUP),
                expiryPlc,
                invokeArgs,
                filter,
                subjId,
                taskNameHash);

            req.addUpdateEntry(key, val, drTtl, drExpireTime, drVer, true);

            single = true;

            // Optimize mapping for single key.
            mapSingle(primary.id(), req);

            return;
        }

        Iterator<?> it = null;

        if (vals != null)
            it = vals.iterator();

        Iterator<GridCacheDrInfo<V>> drPutValsIt = null;

        if (drPutVals != null)
            drPutValsIt = drPutVals.iterator();

        Iterator<GridCacheVersion> drRmvValsIt = null;

        if (drRmvVals != null)
            drRmvValsIt = drRmvVals.iterator();

        Map<UUID, GridNearAtomicUpdateRequest<K, V>> pendingMappings = new HashMap<>(topNodes.size(), 1.0f);

        // Must do this in synchronized block because we need to atomically remove and add mapping.
        // Otherwise checkComplete() may see empty intermediate state.
        synchronized (this) {
            if (remap)
                removeMapping(oldNodeId);

            // Create mappings first, then send messages.
            for (K key : keys) {
                if (key == null) {
                    NullPointerException err = new NullPointerException("Null key.");

                    onDone(err);

                    throw err;
                }

                Object val;
                long drTtl;
                long drExpireTime;
                GridCacheVersion drVer;

                if (vals != null) {
                    val = it.next();
                    drTtl = -1;
                    drExpireTime = -1;
                    drVer = null;

                    if (val == null) {
                        NullPointerException err = new NullPointerException("Null value.");

                        onDone(err);

                        throw err;
                    }
                }
                else if (drPutVals != null) {
                    GridCacheDrInfo<V> drPutVal =  drPutValsIt.next();

                    val = drPutVal.value();
                    drTtl = drPutVal.ttl();
                    drExpireTime = drPutVal.expireTime();
                    drVer = drPutVal.version();
                }
                else if (drRmvVals != null) {
                    val = null;
                    drTtl = -1;
                    drExpireTime = -1;
                    drVer = drRmvValsIt.next();
                }
                else {
                    val = null;
                    drTtl = -1;
                    drExpireTime = -1;
                    drVer = null;
                }

                if (val == null && op != GridCacheOperation.DELETE)
                    continue;

                if (cctx.portableEnabled()) {
                    key = (K)cctx.marshalToPortable(key);

                    if (op != TRANSFORM)
                    val = cctx.marshalToPortable(val);
                }

                Collection<ClusterNode> affNodes = mapKey(key, topVer, fastMap);

                int i = 0;

                for (ClusterNode affNode : affNodes) {
                    UUID nodeId = affNode.id();

                    GridNearAtomicUpdateRequest<K, V> mapped = pendingMappings.get(nodeId);

                    if (mapped == null) {
                        mapped = new GridNearAtomicUpdateRequest<>(
                            cctx.cacheId(),
                            nodeId,
                            futVer,
                            fastMap,
                            updVer,
                            topSnapshot.topologyVersion(),
                            syncMode,
                            op,
                            retval,
                            op == TRANSFORM && cctx.hasFlag(FORCE_TRANSFORM_BACKUP),
                            expiryPlc,
                            invokeArgs,
                            filter,
                            subjId,
                            taskNameHash);

                        pendingMappings.put(nodeId, mapped);

                        GridNearAtomicUpdateRequest<K, V> old = mappings.put(nodeId, mapped);

                        assert old == null || (old != null && remap) :
                            "Invalid mapping state [old=" + old + ", remap=" + remap + ']';
                    }

                    mapped.addUpdateEntry(key, val, drTtl, drExpireTime, drVer, i == 0);

                    i++;
                }
            }
        }

        if ((single == null || single) && pendingMappings.size() == 1) {
            Map.Entry<UUID, GridNearAtomicUpdateRequest<K, V>> entry = F.first(pendingMappings.entrySet());

            single = true;

            mapSingle(entry.getKey(), entry.getValue());

            return;
        }
        else
            single = false;

        doUpdate(pendingMappings);
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
    private Collection<ClusterNode> mapKey(K key, long topVer, boolean fastMap) {
        GridCacheAffinityManager<K, V> affMgr = cctx.affinity();

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
    private void mapSingle(UUID nodeId, GridNearAtomicUpdateRequest<K, V> req) {
        singleNodeId = nodeId;
        singleReq = req;

        if (ctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req, cached,
                new CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>>() {
                    @Override public void apply(GridNearAtomicUpdateRequest<K, V> req,
                        GridNearAtomicUpdateResponse<K, V> res) {
                        assert res.futureVersion().equals(futVer);

                        onResult(res.nodeId(), res);
                    }
                });
        }
        else {
            try {
                if (log.isDebugEnabled())
                    log.debug("Sending near atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (syncMode == FULL_ASYNC && cctx.config().getAtomicWriteOrderMode() == PRIMARY)
                    onDone(new GridCacheReturn<V>(null, true));
            }
            catch (IgniteCheckedException e) {
                onDone(addFailedKeys(req.keys(), e));
            }
        }
    }

    /**
     * Sends messages to remote nodes and updates local cache.
     *
     * @param mappings Mappings to send.
     */
    private void doUpdate(Map<UUID, GridNearAtomicUpdateRequest<K, V>> mappings) {
        UUID locNodeId = cctx.localNodeId();

        GridNearAtomicUpdateRequest<K, V> locUpdate = null;

        // Send messages to remote nodes first, then run local update.
        for (GridNearAtomicUpdateRequest<K, V> req : mappings.values()) {
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
                    addFailedKeys(req.keys(), e);

                    removeMapping(req.nodeId());
                }

                if (syncMode == PRIMARY_SYNC && !req.hasPrimary())
                    removeMapping(req.nodeId());
            }
        }

        if (syncMode == FULL_ASYNC)
            // In FULL_ASYNC mode always return (null, true).
            opRes = new GridCacheReturn<>(null, true);

        if (locUpdate != null) {
            cache.updateAllAsyncInternal(cctx.localNodeId(), locUpdate, cached,
                new CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>>() {
                    @Override public void apply(GridNearAtomicUpdateRequest<K, V> req,
                        GridNearAtomicUpdateResponse<K, V> res) {
                        assert res.futureVersion().equals(futVer);

                        onResult(res.nodeId(), res);
                    }
                });
        }

        checkComplete();
    }

    /**
     * Removes mapping from future mappings map.
     *
     * @param nodeId Node ID to remove mapping for.
     */
    private void removeMapping(UUID nodeId) {
        mappings.remove(nodeId);
    }

    /**
     * @param ret Result from single node.
     */
    private synchronized void addInvokeResults(GridCacheReturn<Object> ret) {
        assert op == TRANSFORM : op;
        assert ret.value() == null || ret.value() instanceof Map : ret.value();

        if (ret.value() != null) {
            if (opRes != null) {
                Map<Object, Object> map = (Map<Object, Object>)opRes.value();

                map.putAll((Map<Object, Object>)ret.value());
            }
            else
                opRes = ret;
        }
    }

    /**
     * @param failedKeys Failed keys.
     * @param err Error cause.
     * @return Root {@link org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException}.
     */
    private synchronized IgniteCheckedException addFailedKeys(Collection<K> failedKeys, Throwable err) {
        CachePartialUpdateCheckedException err0 = this.err;

        if (err0 == null)
            err0 = this.err = new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

        err0.add(failedKeys, err);

        return err0;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridNearAtomicUpdateFuture.class, this, super.toString());
    }
}
