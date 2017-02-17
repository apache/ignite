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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 *
 */
public class GridPartitionedSingleGetFuture extends GridFutureAdapter<Object> implements GridCacheFuture<Object>,
    CacheGetFuture {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final IgniteProductVersion SINGLE_GET_MSG_SINCE = IgniteProductVersion.fromString("1.5.0");

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Context. */
    private final GridCacheContext cctx;

    /** Key. */
    private final KeyCacheObject key;

    /** Read through flag. */
    private final boolean readThrough;

    /** Force primary flag. */
    private final boolean forcePrimary;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Trackable flag. */
    private boolean trackable;

    /** Subject ID. */
    private final UUID subjId;

    /** Task name. */
    private final String taskName;

    /** Whether to deserialize binary objects. */
    private boolean deserializeBinary;

    /** Skip values flag. */
    private boolean skipVals;

    /** Expiry policy. */
    private IgniteCacheExpiryPolicy expiryPlc;

    /** Flag indicating that get should be done on a locked topology version. */
    private final boolean canRemap;

    /** */
    private final boolean needVer;

    /** */
    private final boolean keepCacheObjects;

    /** */
    @GridToStringInclude
    private ClusterNode node;

    /**
     * @param cctx Context.
     * @param key Key.
     * @param topVer Topology version.
     * @param readThrough Read through flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even if called on backup node.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param canRemap Flag indicating whether future can be remapped on a newer topology version.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObjects Keep cache objects flag.
     */
    public GridPartitionedSingleGetFuture(
        GridCacheContext cctx,
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        boolean readThrough,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean canRemap,
        boolean needVer,
        boolean keepCacheObjects
    ) {
        assert key != null;

        AffinityTopologyVersion lockedTopVer = cctx.shared().lockedTopologyVersion(null);

        if (lockedTopVer != null) {
            topVer = lockedTopVer;

            canRemap = false;
        }

        this.cctx = cctx;
        this.key = key;
        this.readThrough = readThrough;
        this.forcePrimary = forcePrimary;
        this.subjId = subjId;
        this.taskName = taskName;
        this.deserializeBinary = deserializeBinary;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;
        this.canRemap = canRemap;
        this.needVer = needVer;
        this.keepCacheObjects = keepCacheObjects;
        this.topVer = topVer;

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridPartitionedSingleGetFuture.class);
    }

    /**
     *
     */
    public void init() {
        AffinityTopologyVersion topVer = this.topVer.topologyVersion() > 0 ? this.topVer :
            canRemap ? cctx.affinity().affinityTopologyVersion() : cctx.shared().exchange().readyAffinityVersion();

        map(topVer);
    }

    /**
     * @param topVer Topology version.
     */
    @SuppressWarnings("unchecked")
    private void map(AffinityTopologyVersion topVer) {
        ClusterNode node = mapKeyToNode(topVer);

        if (node == null) {
            assert isDone() : this;

            return;
        }

        if (isDone())
            return;

        if (node.isLocal()) {
            Map<KeyCacheObject, Boolean> map = Collections.singletonMap(key, false);

            final GridDhtFuture<Collection<GridCacheEntryInfo>> fut = cctx.dht().getDhtAsync(node.id(),
                -1,
                map,
                readThrough,
                topVer,
                subjId,
                taskName == null ? 0 : taskName.hashCode(),
                expiryPlc,
                skipVals);

            final Collection<Integer> invalidParts = fut.invalidPartitions();

            if (!F.isEmpty(invalidParts)) {
                AffinityTopologyVersion updTopVer = cctx.discovery().topologyVersionEx();

                assert updTopVer.compareTo(topVer) > 0 : "Got invalid partitions for local node but topology " +
                    "version did not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                    ", invalidParts=" + invalidParts + ']';

                // Remap recursively.
                map(updTopVer);
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<Collection<GridCacheEntryInfo>>>() {
                    @Override public void apply(IgniteInternalFuture<Collection<GridCacheEntryInfo>> fut) {
                        try {
                            Collection<GridCacheEntryInfo> infos = fut.get();

                            assert F.isEmpty(infos) || infos.size() == 1 : infos;

                            setResult(F.first(infos));
                        }
                        catch (Exception e) {
                            U.error(log, "Failed to get values from dht cache [fut=" + fut + "]", e);

                            onDone(e);
                        }
                    }
                });
            }
        }
        else {
            synchronized (this) {
                assert this.node == null;

                this.topVer = topVer;
                this.node = node;
            }

            if (!trackable) {
                trackable = true;

                cctx.mvcc().addFuture(this, futId);
            }

            GridCacheMessage req;

            if (node.version().compareTo(SINGLE_GET_MSG_SINCE) >= 0) {
                req = new GridNearSingleGetRequest(cctx.cacheId(),
                    futId.localId(),
                    key,
                    readThrough,
                    topVer,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forCreate() : -1L,
                    expiryPlc != null ? expiryPlc.forAccess() : -1L,
                    skipVals,
                    /**add reader*/false,
                    needVer,
                    cctx.deploymentEnabled());
            }
            else {
                Map<KeyCacheObject, Boolean> map = Collections.singletonMap(key, false);

                req = new GridNearGetRequest(
                    cctx.cacheId(),
                    futId,
                    futId,
                    cctx.versions().next(),
                    map,
                    readThrough,
                    topVer,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forCreate() : -1L,
                    expiryPlc != null ? expiryPlc.forAccess() : -1L,
                    skipVals,
                    cctx.deploymentEnabled());
            }

            try {
                cctx.io().send(node, req, cctx.ioPolicy());
            }
            catch (IgniteCheckedException e) {
                if (e instanceof ClusterTopologyCheckedException)
                    onNodeLeft(node.id());
                else
                    onDone(e);
            }
        }
    }

    /**
     * @param topVer Topology version.
     * @return Primary node or {@code null} if future was completed.
     */
    @Nullable private ClusterNode mapKeyToNode(AffinityTopologyVersion topVer) {
        int part = cctx.affinity().partition(key);

        List<ClusterNode> affNodes = cctx.affinity().nodesByPartition(part, topVer);

        if (affNodes.isEmpty()) {
            onDone(serverNotFoundError(topVer));

            return null;
        }

        boolean fastLocGet = (!forcePrimary || affNodes.get(0).isLocal()) &&
            cctx.allowFastLocalRead(part, affNodes, topVer);

        if (fastLocGet && localGet(topVer, part))
            return null;

        ClusterNode affNode = affinityNode(affNodes);

        if (affNode == null) {
            onDone(serverNotFoundError(topVer));

            return null;
        }

        return affNode;
    }

    /**
     * @param topVer Topology version.
     * @param part Partition.
     * @return {@code True} if future completed.
     */
    private boolean localGet(AffinityTopologyVersion topVer, int part) {
        assert cctx.affinityNode() : this;

        GridDhtCacheAdapter colocated = cctx.dht();

        while (true) {
            GridCacheEntryEx entry;

            try {
                entry = colocated.context().isSwapOrOffheapEnabled() ? colocated.entryEx(key) :
                    colocated.peekEx(key);

                // If our DHT cache do has value, then we peek it.
                if (entry != null) {
                    boolean isNew = entry.isNewLocked();

                    CacheObject v = null;
                    GridCacheVersion ver = null;

                    if (needVer) {
                        EntryGetResult res = entry.innerGetVersioned(
                            null,
                            null,
                            /*swap*/true,
                            /*unmarshal*/true,
                            /**update-metrics*/false,
                            /*event*/!skipVals,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            true,
                            null);

                        if (res != null) {
                            v = res.value();
                            ver = res.version();
                        }
                    }
                    else {
                        v = entry.innerGet(
                            null,
                            null,
                            /*swap*/true,
                            /*read-through*/false,
                            /**update-metrics*/false,
                            /*event*/!skipVals,
                            /*temporary*/false,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            true);
                    }

                    colocated.context().evicts().touch(entry, topVer);

                    // Entry was not in memory or in swap, so we remove it from cache.
                    if (v == null) {
                        if (isNew && entry.markObsoleteIfEmpty(ver))
                            colocated.removeEntry(entry);
                    }
                    else {
                        if (!skipVals && cctx.config().isStatisticsEnabled())
                            cctx.cache().metrics0().onRead(true);

                        if (!skipVals)
                            setResult(v, ver);
                        else
                            setSkipValueResult(true, ver);

                        return true;
                    }
                }

                boolean topStable = cctx.isReplicated() || topVer.equals(cctx.topology().topologyVersion());

                // Entry not found, complete future with null result if topology did not change and there is no store.
                if (!cctx.readThroughConfigured() && (topStable || partitionOwned(part))) {
                    if (!skipVals && cctx.config().isStatisticsEnabled())
                        colocated.metrics0().onRead(false);

                    if (skipVals)
                        setSkipValueResult(false, null);
                    else
                        setResult(null, null);

                    return true;
                }

                return false;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op, will retry.
            }
            catch (GridDhtInvalidPartitionException ignored) {
                return false;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                return true;
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearSingleGetResponse res) {
        if (!processResponse(nodeId) ||
            !checkError(res.error(), res.invalidPartitions(), res.topologyVersion(), nodeId))
            return;

        Message res0 = res.result();

        if (needVer) {
            CacheVersionedValue verVal = (CacheVersionedValue)res0;

            if (verVal != null) {
                if (skipVals)
                    setSkipValueResult(true, verVal.version());
                else
                    setResult(verVal.value() , verVal.version());
            }
            else {
                if (skipVals)
                    setSkipValueResult(false, null);
                else
                    setResult(null , null);
            }
        }
        else {
            if (skipVals)
                setSkipValueResult(res.containsValue(), null);
            else
                setResult((CacheObject)res0, null);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Result.
     */
    @Override public void onResult(UUID nodeId, GridNearGetResponse res) {
        if (!processResponse(nodeId) ||
            !checkError(res.error(), !F.isEmpty(res.invalidPartitions()), res.topologyVersion(), nodeId))
            return;

        Collection<GridCacheEntryInfo> infos = res.entries();

        assert F.isEmpty(infos) || infos.size() == 1 : infos;

        setResult(F.first(infos));
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if should process received response.
     */
    private boolean processResponse(UUID nodeId) {
        synchronized (this) {
            if (node != null && node.id().equals(nodeId)) {
                node = null;

                return true;
            }
        }

        return false;
    }

    /**
     * @param err Error.
     * @param invalidParts Invalid partitions error flag.
     * @param rmtTopVer Received topology version.
     * @param nodeId Node ID.
     * @return {@code True} if should process received response.
     */
    private boolean checkError(@Nullable IgniteCheckedException err,
        boolean invalidParts,
        AffinityTopologyVersion rmtTopVer,
        UUID nodeId) {
        if (err != null) {
            onDone(err);

            return false;
        }

        if (invalidParts) {
            assert !rmtTopVer.equals(AffinityTopologyVersion.ZERO);

            if (rmtTopVer.compareTo(topVer) <= 0) {
                // Fail the whole get future.
                onDone(new IgniteCheckedException("Failed to process invalid partitions response (remote node reported " +
                    "invalid partitions but remote topology version does not differ from local) " +
                    "[topVer=" + topVer + ", rmtTopVer=" + rmtTopVer + ", part=" + cctx.affinity().partition(key) +
                    ", nodeId=" + nodeId + ']'));

                return false;
            }

            if (canRemap) {
                IgniteInternalFuture<AffinityTopologyVersion> topFut = cctx.affinity().affinityReadyFuture(rmtTopVer);

                topFut.listen(new CIX1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void applyx(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            AffinityTopologyVersion topVer = fut.get();

                            remap(topVer);
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                    }
                });

            }
            else
                map(topVer);

            return false;
        }

        return true;
    }

    /**
     * @param info Entry info.
     */
    private void setResult(@Nullable GridCacheEntryInfo info) {
        assert info == null || skipVals == (info.value() == null);

        if (skipVals) {
            if (info != null)
                setSkipValueResult(true, info.version());
            else
                setSkipValueResult(false, null);
        }
        else {
            if (info != null)
                setResult(info.value(), info.version());
            else
                setResult(null, null);
        }
    }

    /**
     * @param res Result.
     * @param ver Version.
     */
    private void setSkipValueResult(boolean res, @Nullable GridCacheVersion ver) {
        assert skipVals;

        if (needVer) {
            assert ver != null || !res;

            onDone(new T2<>(res, ver));
        }
        else
            onDone(res);
    }

    /**
     * @param val Value.
     * @param ver Version.
     */
    private void setResult(@Nullable CacheObject val, @Nullable GridCacheVersion ver) {
        try {
            assert !skipVals;

            if (val != null) {
                if (!keepCacheObjects) {
                    Object res = cctx.unwrapBinaryIfNeeded(val, !deserializeBinary);

                    onDone(needVer ? new T2<>(res, ver) : res);
                }
                else
                    onDone(needVer ? new T2<>(val, ver) : val);
            }
            else
                onDone(null);
        }
        catch (Exception e) {
            onDone(e);
        }
    }

    /**
     * @param part Partition.
     * @return {@code True} if partition is in owned state.
     */
    private boolean partitionOwned(int part) {
        return cctx.topology().partitionState(cctx.localNodeId(), part) == OWNING;
    }

    /**
     * @param topVer Topology version.
     * @return Exception.
     */
    private ClusterTopologyServerNotFoundException serverNotFoundError(AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
            "(all partition nodes left the grid) [topVer=" + topVer + ", cache=" + cctx.name() + ']');
    }

    /**
     * Affinity node to send get request to.
     *
     * @param affNodes All affinity nodes.
     * @return Affinity node to get key from.
     */
    @Nullable private ClusterNode affinityNode(List<ClusterNode> affNodes) {
        if (!canRemap) {
            for (ClusterNode node : affNodes) {
                if (cctx.discovery().alive(node))
                    return node;
            }

            return null;
        }
        else
            return affNodes.get(0);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (!processResponse(nodeId))
            return false;

        if (canRemap) {
            final AffinityTopologyVersion updTopVer = new AffinityTopologyVersion(
                Math.max(topVer.topologyVersion() + 1, cctx.discovery().topologyVersion()));

            cctx.affinity().affinityReadyFuture(updTopVer).listen(
                new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            remap(updTopVer);
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                    }
                });
        }
        else
            remap(topVer);

        return true;
    }

    /**
     * @param topVer Topology version.
     */
    private void remap(final AffinityTopologyVersion topVer) {
        cctx.closures().runLocalSafe(new Runnable() {
            @Override public void run() {
                map(topVer);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Object res, Throwable err) {
        if (super.onDone(res, err)) {
            // Don't forget to clean up.
            if (trackable)
                cctx.mvcc().removeFuture(futId);

            cctx.dht().sendTtlUpdateRequest(expiryPlc);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionedSingleGetFuture.class, this);
    }
}
