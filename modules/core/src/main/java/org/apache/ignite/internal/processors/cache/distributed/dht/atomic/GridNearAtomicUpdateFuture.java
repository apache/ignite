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
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

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

    /** Future ID. */
    private volatile GridCacheVersion futVer;

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

    /** Mappings. */
    @GridToStringInclude
    private ConcurrentMap<UUID, GridNearAtomicUpdateRequest> mappings;

    /** Error. */
    private volatile CachePartialUpdateCheckedException err;

    /** Operation result. */
    private volatile GridCacheReturn opRes;

    /** Return value require flag. */
    private final boolean retval;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Future map topology version. */
    private volatile AffinityTopologyVersion topVer = AffinityTopologyVersion.ZERO;

    /** Completion future for a particular topology version. */
    private GridFutureAdapter<Void> topCompleteFut;

    /** Optional filter. */
    private final CacheEntryPredicate[] filter;

    /** Write synchronization mode. */
    private final CacheWriteSynchronizationMode syncMode;

    /** If this future mapped to single node. */
    private volatile Boolean single;

    /** If this future is mapped to a single node, this field will contain that node ID. */
    private UUID singleNodeId;

    /** Single update request. */
    private GridNearAtomicUpdateRequest singleReq;

    /** Raw return value flag. */
    private final boolean rawRetval;

    /** Fast map flag. */
    private final boolean fastMap;

    /** */
    private boolean fastMapRemap;

    /** */
    private GridCacheVersion updVer;

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
    private AtomicInteger remapCnt;

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

        mappings = new ConcurrentHashMap8<>(keys.size(), 1.0f);

        fastMap = F.isEmpty(filter) && op != TRANSFORM && cctx.config().getWriteSynchronizationMode() == FULL_SYNC &&
            cctx.config().getAtomicWriteOrderMode() == CLOCK &&
            !(cctx.writeThrough() && cctx.config().getInterceptor() != null);

        nearEnabled = CU.isNearEnabled(cctx);

        this.remapCnt = new AtomicInteger(remapCnt);
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

    /**
     * @return {@code True} if this future should block partition map exchange.
     */
    private boolean waitForPartitionExchange() {
        // Wait fast-map near atomic update futures in CLOCK mode.
        return fastMap;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public Collection<?> keys() {
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

        GridNearAtomicUpdateRequest req = mappings.get(nodeId);

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

        IgniteInternalTx tx = cctx.tm().anyActiveThreadTx();

        if (tx != null && tx.topologyVersionSnapshot() != null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer == null)
            topVer = cctx.mvcc().lastExplicitLockTopologyVersion(Thread.currentThread().getId());

        if (topVer == null)
            mapOnTopology(null, false, null);
        else {
            topLocked = true;

            // Cannot remap.
            remapCnt.set(1);

            map0(topVer, null, false, null);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        if (waitForPartitionExchange() && topologyVersion().compareTo(topVer) < 0) {
            GridFutureAdapter<Void> fut = null;

            synchronized (this) {
                if (this.topVer == AffinityTopologyVersion.ZERO)
                    return null;

                if (this.topVer.compareTo(topVer) < 0) {
                    if (topCompleteFut == null)
                        topCompleteFut = new GridFutureAdapter<>();

                    fut = topCompleteFut;
                }
            }

            if (fut != null && isDone())
                fut.onDone();

            return fut;
        }

        return null;
    }

    /**
     * @param failed Keys to remap.
     */
    private void remap(Collection<?> failed) {
        GridCacheVersion futVer0 = futVer;

        if (futVer0 == null || cctx.mvcc().removeAtomicFuture(futVer0) == null)
            return;

        Collection<Object> remapKeys = new ArrayList<>(failed.size());
        Collection<Object> remapVals = vals != null ? new ArrayList<>(failed.size()) : null;
        Collection<GridCacheDrInfo> remapConflictPutVals = conflictPutVals != null ? new ArrayList<GridCacheDrInfo>(failed.size()) : null;
        Collection<GridCacheVersion> remapConflictRmvVals = conflictRmvVals != null ? new ArrayList<GridCacheVersion>(failed.size()) : null;

        Iterator<?> keyIt = keys.iterator();
        Iterator<?> valsIt = vals != null ? vals.iterator() : null;
        Iterator<GridCacheDrInfo> conflictPutValsIt = conflictPutVals != null ? conflictPutVals.iterator() : null;
        Iterator<GridCacheVersion> conflictRmvValsIt = conflictRmvVals != null ? conflictRmvVals.iterator() : null;

        for (Object key : failed) {
            while (keyIt.hasNext()) {
                Object nextKey = keyIt.next();
                Object nextVal = valsIt != null ? valsIt.next() : null;
                GridCacheDrInfo nextConflictPutVal = conflictPutValsIt != null ? conflictPutValsIt.next() : null;
                GridCacheVersion nextConflictRmvVal = conflictRmvValsIt != null ? conflictRmvValsIt.next() : null;

                if (F.eq(key, nextKey)) {
                    remapKeys.add(nextKey);

                    if (remapVals != null)
                        remapVals.add(nextVal);

                    if (remapConflictPutVals != null)
                        remapConflictPutVals.add(nextConflictPutVal);

                    if (remapConflictRmvVals != null)
                        remapConflictRmvVals.add(nextConflictRmvVal);

                    break;
                }
            }
        }

        keys = remapKeys;
        vals = remapVals;
        conflictPutVals = remapConflictPutVals;
        conflictRmvVals = remapConflictRmvVals;

        single = null;
        futVer = null;
        err = null;
        opRes = null;

        GridFutureAdapter<Void> fut0;

        synchronized (this) {
            mappings = new ConcurrentHashMap8<>(keys.size(), 1.0f);

            topVer = AffinityTopologyVersion.ZERO;

            fut0 = topCompleteFut;

            topCompleteFut = null;
        }

        if (fut0 != null)
            fut0.onDone();

        singleNodeId = null;
        singleReq = null;
        fastMapRemap = false;
        updVer = null;
        topLocked = false;

        map();
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

        if (err != null && X.hasCause(err, CachePartialUpdateCheckedException.class) &&
            X.hasCause(err, ClusterTopologyCheckedException.class) &&
            storeFuture() &&
            remapCnt.decrementAndGet() > 0) {

            CachePartialUpdateCheckedException cause = X.cause(err, CachePartialUpdateCheckedException.class);

            if (F.isEmpty(cause.failedKeys()))
                cause.printStackTrace();

            remap(cause.failedKeys());

            return false;
        }

        if (super.onDone(retval, err)) {
            if (futVer != null)
                cctx.mvcc().removeAtomicFuture(version());

            GridFutureAdapter<Void> fut0;

            synchronized (this) {
                fut0 = topCompleteFut;
            }

            if (fut0 != null)
                fut0.onDone();

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
        if (res.remapKeys() != null) {
            assert !fastMap || cctx.kernalContext().clientNode();

            Collection<KeyCacheObject> remapKeys = fastMap ? null : res.remapKeys();

            mapOnTopology(remapKeys, true, nodeId);

            return;
        }

        GridCacheReturn ret = res.returnValue();

        Boolean single0 = single;

        if (single0 != null && single0) {
            assert singleNodeId.equals(nodeId) : "Invalid response received for single-node mapped future " +
                "[singleNodeId=" + singleNodeId + ", nodeId=" + nodeId + ", res=" + res + ']';

            updateNear(singleReq, res);

            if (res.error() != null)
                onDone(res.failedKeys() != null ? addFailedKeys(res.failedKeys(), res.error()) : res.error());
            else {
                if (op == TRANSFORM) {
                    if (ret != null)
                        addInvokeResults(ret);

                    onDone(opRes);
                }
                else {
                    GridCacheReturn opRes0 = opRes = ret;

                    onDone(opRes0);
                }
            }
        }
        else {
            GridNearAtomicUpdateRequest req = mappings.get(nodeId);

            if (req != null) { // req can be null if onResult is being processed concurrently with onNodeLeft.
                updateNear(req, res);

                if (res.error() != null)
                    addFailedKeys(req.keys(), res.error());
                else {
                    if (op == TRANSFORM) {
                        assert !req.fastMap();

                        if (ret != null)
                            addInvokeResults(ret);
                    }
                    else if (req.fastMap() && req.hasPrimary())
                        opRes = ret;
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
    private void updateNear(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
        if (!nearEnabled || !req.hasPrimary())
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /**
     * Maps future on ready topology.
     *
     * @param keys Keys to map.
     * @param remap Boolean flag indicating if this is partial future remap.
     * @param oldNodeId Old node ID if remap.
     */
    private void mapOnTopology(final Collection<?> keys, final boolean remap, final UUID oldNodeId) {
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
                    fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                            cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                                @Override public void run() {
                                    mapOnTopology(keys, remap, oldNodeId);
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

        map0(topVer, keys, remap, oldNodeId);
    }

    /**
     * Checks if future is ready to be completed.
     */
    private void checkComplete() {
        boolean remap = false;

        synchronized (this) {
            if (topVer != AffinityTopologyVersion.ZERO &&
                ((syncMode == FULL_ASYNC && cctx.config().getAtomicWriteOrderMode() == PRIMARY) || mappings.isEmpty())) {
                CachePartialUpdateCheckedException err0 = err;

                if (err0 != null)
                    onDone(err0);
                else {
                    if (fastMapRemap) {
                        assert cctx.kernalContext().clientNode();

                        remap = true;
                    }
                    else
                        onDone(opRes);
                }
            }
        }

        if (remap)
            mapOnTopology(null, true, null);
    }

    /**
     * @return {@code True} future is stored by {@link GridCacheMvccManager#addAtomicFuture}.
     */
    private boolean storeFuture() {
        return cctx.config().getAtomicWriteOrderMode() == CLOCK || syncMode != FULL_ASYNC;
    }

    /**
     * @param topVer Topology version.
     * @param remapKeys Keys to remap or {@code null} to map all keys.
     * @param remap Flag indicating if this is partial remap for this future.
     * @param oldNodeId Old node ID if was remap.
     */
    private void map0(
        AffinityTopologyVersion topVer,
        @Nullable Collection<?> remapKeys,
        boolean remap,
        @Nullable UUID oldNodeId) {
        assert oldNodeId == null || remap || fastMapRemap;

        Collection<ClusterNode> topNodes = CU.affinityNodes(cctx, topVer);

        if (F.isEmpty(topNodes)) {
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid)."));

            return;
        }

        if (futVer == null)
            // Assign future version in topology read lock before first exception may be thrown.
            futVer = cctx.versions().next(topVer);

        if (!remap && storeFuture())
            cctx.mvcc().addAtomicFuture(version(), this);

        CacheConfiguration ccfg = cctx.config();

        // Assign version on near node in CLOCK ordering mode even if fastMap is false.
        if (updVer == null)
            updVer = ccfg.getAtomicWriteOrderMode() == CLOCK ? cctx.versions().next(topVer) : null;

        if (updVer != null && log.isDebugEnabled())
            log.debug("Assigned fast-map version for update on near node: " + updVer);

        if (keys.size() == 1 && !fastMap && (single == null || single)) {
            assert remapKeys == null || remapKeys.size() == 1 : remapKeys;

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
            if (key == null) {
                NullPointerException err = new NullPointerException("Null key.");

                onDone(err);

                return;
            }

            if (val == null && op != GridCacheOperation.DELETE) {
                NullPointerException err = new NullPointerException("Null value.");

                onDone(err);

                return;
            }

            KeyCacheObject cacheKey = cctx.toCacheKeyObject(key);

            if (op != TRANSFORM)
                val = cctx.toCacheObject(val);

            ClusterNode primary = cctx.affinity().primary(cacheKey, topVer);

            if (primary == null) {
                onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                    "left the grid)."));

                return;
            }

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

            synchronized (this) {
                this.topVer = topVer;

                single = true;
            }

            // Optimize mapping for single key.
            mapSingle(primary.id(), req);

            return;
        }

        Iterator<?> it = null;

        if (vals != null)
            it = vals.iterator();

        Iterator<GridCacheDrInfo> conflictPutValsIt = null;

        if (conflictPutVals != null)
            conflictPutValsIt = conflictPutVals.iterator();

        Iterator<GridCacheVersion> conflictRmvValsIt = null;

        if (conflictRmvVals != null)
            conflictRmvValsIt = conflictRmvVals.iterator();

        Map<UUID, GridNearAtomicUpdateRequest> pendingMappings = new HashMap<>(topNodes.size(), 1.0f);

        // Must do this in synchronized block because we need to atomically remove and add mapping.
        // Otherwise checkComplete() may see empty intermediate state.
        synchronized (this) {
            if (oldNodeId != null)
                removeMapping(oldNodeId);

            // For fastMap mode wait for all responses before remapping.
            if (remap && fastMap && !mappings.isEmpty()) {
                fastMapRemap = true;

                return;
            }

            // Create mappings first, then send messages.
            for (Object key : keys) {
                if (key == null) {
                    NullPointerException err = new NullPointerException("Null key.");

                    onDone(err);

                    return;
                }

                Object val;
                GridCacheVersion conflictVer;
                long conflictTtl;
                long conflictExpireTime;

                if (vals != null) {
                    val = it.next();
                    conflictVer = null;
                    conflictTtl = CU.TTL_NOT_CHANGED;
                    conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;

                    if (val == null) {
                        NullPointerException err = new NullPointerException("Null value.");

                        onDone(err);

                        return;
                    }
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

                if (affNodes.isEmpty()) {
                    onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                        "(all partition nodes left the grid)."));

                    return;
                }

                int i = 0;

                for (ClusterNode affNode : affNodes) {
                    if (affNode == null) {
                        onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                            "(all partition nodes left the grid)."));

                        return;
                    }

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

                        GridNearAtomicUpdateRequest old = mappings.put(nodeId, mapped);

                        assert old == null || (old != null && remap) :
                            "Invalid mapping state [old=" + old + ", remap=" + remap + ']';
                    }

                    mapped.addUpdateEntry(cacheKey, val, conflictTtl, conflictExpireTime, conflictVer, i == 0);

                    i++;
                }
            }

            this.topVer = topVer;

            fastMapRemap = false;
        }

        if ((single == null || single) && pendingMappings.size() == 1) {
            Map.Entry<UUID, GridNearAtomicUpdateRequest> entry = F.first(pendingMappings.entrySet());

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
        singleNodeId = nodeId;
        singleReq = req;

        if (cctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req,
                new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicUpdateRequest req,
                        GridNearAtomicUpdateResponse res) {
                        assert res.futureVersion().equals(futVer) : futVer;

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
                    onDone(new GridCacheReturn(cctx, true, null, true));
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
                    addFailedKeys(req.keys(), e);

                    removeMapping(req.nodeId());
                }

                if (syncMode == PRIMARY_SYNC && !req.hasPrimary())
                    removeMapping(req.nodeId());
            }
        }

        if (syncMode == FULL_ASYNC)
            // In FULL_ASYNC mode always return (null, true).
            opRes = new GridCacheReturn(cctx, true, null, true);

        if (locUpdate != null) {
            cache.updateAllAsyncInternal(cctx.localNodeId(), locUpdate,
                new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicUpdateRequest req,
                        GridNearAtomicUpdateResponse res) {
                        assert res.futureVersion().equals(futVer) : futVer;

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
    @SuppressWarnings("unchecked")
    private synchronized void addInvokeResults(GridCacheReturn ret) {
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
     * @param err Error cause.
     * @return Root {@link org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException}.
     */
    private synchronized IgniteCheckedException addFailedKeys(Collection<KeyCacheObject> failedKeys, Throwable err) {
        CachePartialUpdateCheckedException err0 = this.err;

        if (err0 == null)
            err0 = this.err = new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

        Collection<Object> keys = new ArrayList<>(failedKeys.size());

        for (KeyCacheObject key : failedKeys)
            keys.add(key.value(cctx.cacheObjectContext(), false));

        err0.add(keys, err);

        return err0;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridNearAtomicUpdateFuture.class, this, super.toString());
    }
}
