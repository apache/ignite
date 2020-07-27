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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
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
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheUtils.BackupPostProcessingClosure;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_NEAR_GET_MAX_REMAPS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public class GridPartitionedSingleGetFuture extends GridCacheFutureAdapter<Object>
    implements CacheGetFuture, IgniteDiagnosticAware {
    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Maximum number of attempts to remap key to the same primary node. */
    protected static final int MAX_REMAP_CNT = getInteger(IGNITE_NEAR_GET_MAX_REMAPS, DFLT_MAX_REMAP_CNT);

    /** Remap count updater. */
    protected static final AtomicIntegerFieldUpdater<GridPartitionedSingleGetFuture> REMAP_CNT_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridPartitionedSingleGetFuture.class, "remapCnt");

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
    private boolean recovery;

    /** */
    @GridToStringInclude
    private ClusterNode node;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** Post processing closure. */
    private volatile BackupPostProcessingClosure postProcessingClos;

    /** Transaction label. */
    private final String txLbl;

    /** Invalid mappened nodes. */
    private Set<ClusterNode> invalidNodes = Collections.emptySet();

    /** Remap count. */
    protected volatile int remapCnt;

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
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObjects Keep cache objects flag.
     * @param txLbl Transaction label.
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
        boolean needVer,
        boolean keepCacheObjects,
        boolean recovery,
        String txLbl,
        @Nullable MvccSnapshot mvccSnapshot
    ) {
        assert key != null;
        assert mvccSnapshot == null || cctx.mvccEnabled();

        AffinityTopologyVersion lockedTopVer = cctx.shared().lockedTopologyVersion(null);

        if (lockedTopVer != null) {
            topVer = lockedTopVer;

            canRemap = false;
        }
        else
            canRemap = true;

        this.cctx = cctx;
        this.key = key;
        this.readThrough = readThrough;
        this.forcePrimary = forcePrimary;
        this.subjId = subjId;
        this.taskName = taskName;
        this.deserializeBinary = deserializeBinary;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;
        this.needVer = needVer;
        this.keepCacheObjects = keepCacheObjects;
        this.recovery = recovery;
        this.topVer = topVer;
        this.mvccSnapshot = mvccSnapshot;

        this.txLbl = txLbl;

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridPartitionedSingleGetFuture.class);
    }

    /**
     * Initialize future.
     */
    public void init() {
        AffinityTopologyVersion mappingTopVer;

        if (topVer.topologyVersion() > 0)
            mappingTopVer = topVer;
        else {
            mappingTopVer = canRemap ?
                cctx.affinity().affinityTopologyVersion() :
                cctx.shared().exchange().readyAffinityVersion();
        }

        map(mappingTopVer);
    }

    /**
     * @param topVer Topology version.
     */
    @SuppressWarnings("unchecked")
    private void map(AffinityTopologyVersion topVer) {
        GridDhtPartitionsExchangeFuture fut = cctx.shared().exchange().lastTopologyFuture();

        // Finished DHT future is required for topology validation.
        if (!fut.isDone()) {
            if (fut.initialVersion().after(topVer) || (fut.exchangeActions() != null && fut.exchangeActions().hasStop()))
                fut = cctx.shared().exchange().lastFinishedFuture();
            else {
                fut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        if (fut.error() != null)
                            onDone(fut.error());
                        else {
                            cctx.closures().runLocalSafe(new GridPlainRunnable() {
                                @Override public void run() {
                                    map(topVer);
                                }
                            }, true);
                        }
                    }
                });

                return;
            }
        }

        if (!validate(fut))
            return;

        ClusterNode node = mapKeyToNode(topVer);

        if (node == null) {
            assert isDone() : this;

            return;
        }

        if (isDone())
            return;

        // Read value if node is localNode.
        if (node.isLocal()) {
            GridDhtFuture<GridCacheEntryInfo> fut0 = cctx.dht()
                .getDhtSingleAsync(
                    node.id(),
                    -1,
                    key,
                    false,
                    readThrough,
                    topVer,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc,
                    skipVals,
                    recovery,
                    txLbl,
                    mvccSnapshot
                );

            Collection<Integer> invalidParts = fut0.invalidPartitions();

            if (!F.isEmpty(invalidParts)) {
                addNodeAsInvalid(node);

                AffinityTopologyVersion updTopVer = cctx.shared().exchange().readyAffinityVersion();

                // Remap recursively.
                map(updTopVer);
            }
            else {
                fut0.listen(f -> {
                    try {
                        GridCacheEntryInfo info = f.get();

                        setResult(info);
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to get values from dht cache [fut=" + fut0 + "]", e);

                        onDone(e);
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

            registrateFutureInMvccManager(this);

            boolean needVer = this.needVer;

            BackupPostProcessingClosure postClos = CU.createBackupPostProcessingClosure(topVer, log,
                cctx, key, expiryPlc, readThrough && cctx.readThroughConfigured(), skipVals);

            if (postClos != null) {
                // Need version to correctly store value.
                needVer = true;

                postProcessingClos = postClos;
            }

            GridCacheMessage req = new GridNearSingleGetRequest(
                cctx.cacheId(),
                futId.localId(),
                key,
                readThrough,
                topVer,
                subjId,
                taskName == null ? 0 : taskName.hashCode(),
                expiryPlc != null ? expiryPlc.forCreate() : -1L,
                expiryPlc != null ? expiryPlc.forAccess() : -1L,
                skipVals,
                /*add reader*/false,
                needVer,
                cctx.deploymentEnabled(),
                recovery,
                txLbl,
                mvccSnapshot
            );

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

        // Failed if none affinity node found by assignment.
        if (affNodes.isEmpty()) {
            onDone(serverNotFoundError(part, topVer));

            return null;
        }

        // Try to read key locally if we can.
        if (tryLocalGet(key, part, topVer, affNodes))
            return null;

        ClusterNode affNode = cctx.selectAffinityNodeBalanced(affNodes, getInvalidNodes(), part, canRemap,
            forcePrimary);

        // Failed if none balanced node found.
        if (affNode == null) {
            onDone(serverNotFoundError(part, topVer));

            return null;
        }

        return affNode;
    }

    /**
     *
     * @param key Key.
     * @param part Partition.
     * @param topVer Topology version.
     * @param affNodes Affynity nodes.
     */
    private boolean tryLocalGet(
        KeyCacheObject key,
        int part,
        AffinityTopologyVersion topVer,
        List<ClusterNode> affNodes
    ) {
        // Local get cannot be used with MVCC as local node can contain some visible version which is not latest.
        boolean fastLocGet = !cctx.mvccEnabled() &&
            (!forcePrimary || affNodes.get(0).isLocal()) &&
            cctx.reserveForFastLocalGet(part, topVer);

        if (fastLocGet) {
            try {
                if (localGet(topVer, key, part))
                    return true;
            }
            finally {
                cctx.releaseForFastLocalGet(part, topVer);
            }
        }

        return false;
    }

    /**
     * @param topVer Topology version.
     * @param part Partition.
     * @return {@code True} if future completed.
     */
    private boolean localGet(AffinityTopologyVersion topVer, KeyCacheObject key, int part) {
        assert cctx.affinityNode() : this;

        GridDhtCacheAdapter colocated = cctx.dht();

        boolean readNoEntry = cctx.readNoEntry(expiryPlc, false);
        boolean evt = !skipVals;

        while (true) {
            cctx.shared().database().checkpointReadLock();

            try {
                CacheObject v = null;
                GridCacheVersion ver = null;

                boolean skipEntry = readNoEntry;

                if (readNoEntry) {
                    KeyCacheObject key0 = (KeyCacheObject)cctx.cacheObjects().prepareForCache(key, cctx);

                    CacheDataRow row = mvccSnapshot != null ?
                        cctx.offheap().mvccRead(cctx, key0, mvccSnapshot) :
                        cctx.offheap().read(cctx, key0);

                    if (row != null) {
                        long expireTime = row.expireTime();

                        if (expireTime == 0 || expireTime > U.currentTimeMillis()) {
                            v = row.value();

                            if (needVer)
                                ver = row.version();

                            if (evt) {
                                cctx.events().readEvent(key,
                                    null,
                                    txLbl,
                                    row.value(),
                                    subjId,
                                    taskName,
                                    !deserializeBinary);
                            }
                        }
                        else
                            skipEntry = false;
                    }
                }

                if (!skipEntry) {
                    GridCacheEntryEx entry = colocated.entryEx(key);

                    // If our DHT cache do has value, then we peek it.
                    if (entry != null) {
                        boolean isNew = entry.isNewLocked();

                        if (needVer) {
                            EntryGetResult res = entry.innerGetVersioned(
                                null,
                                null,
                                /*update-metrics*/false,
                                /*event*/evt,
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
                                /*read-through*/false,
                                /*update-metrics*/false,
                                /*event*/evt,
                                subjId,
                                null,
                                taskName,
                                expiryPlc,
                                true);
                        }

                        entry.touch();

                        // Entry was not in memory or in swap, so we remove it from cache.
                        if (v == null) {
                            if (isNew && entry.markObsoleteIfEmpty(ver))
                                colocated.removeEntry(entry);
                        }
                    }
                }

                if (v != null) {
                    if (!skipVals && cctx.statisticsEnabled())
                        cctx.cache().metrics0().onRead(true);

                    if (!skipVals)
                        setResult(v, ver);
                    else
                        setSkipValueResult(true, ver);

                    return true;
                }

                boolean topStable = cctx.isReplicated() || topVer.equals(cctx.topology().lastTopologyChangeVersion());

                // Entry not found, complete future with null result if topology did not change and there is no store.
                if (!cctx.readThroughConfigured() && (topStable || partitionOwned(part))) {
                    if (!skipVals && cctx.statisticsEnabled())
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
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param fut Future.
     */
    private void registrateFutureInMvccManager(GridCacheFuture<?> fut) {
        if (!trackable) {
            trackable = true;

            cctx.mvcc().addFuture(fut, futId);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearSingleGetResponse res) {
        // Brake here if response from unexpected node.
        if (!processResponse(nodeId))
            return;

        // Brake here if exception was throws on remote node or
        // parition on remote node is invalid.
        if (!checkError(nodeId, res.invalidPartitions(), res.topologyVersion(), res.error()))
            return;

        Message res0 = res.result();

        if (needVer) {
            CacheVersionedValue verVal = (CacheVersionedValue)res0;

            if (verVal != null) {
                if (skipVals)
                    setSkipValueResult(true, verVal.version());
                else
                    setResult(verVal.value(), verVal.version());
            }
            else {
                if (skipVals)
                    setSkipValueResult(false, null);
                else
                    setResult(null, null);
            }
        }
        else {
            if (skipVals)
                setSkipValueResult(res.containsValue(), null);
            else if (readThrough && res0 instanceof CacheVersionedValue) {
                // Could be versioned value for store in backup.
                CacheVersionedValue verVal = (CacheVersionedValue)res0;

                setResult(verVal.value(), verVal.version());
            }
            else
                setResult((CacheObject)res0, null);
        }
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridNearGetResponse res) {
        // Brake here if response from unexpected node.
        if (!processResponse(nodeId))
            return;

        // Brake here if exception was throws on remote node or
        // parition on remote node is invalid.
        if (!checkError(nodeId, !F.isEmpty(res.invalidPartitions()), res.topologyVersion(), res.error()))
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
    private boolean checkError(
        UUID nodeId,
        boolean invalidParts,
        AffinityTopologyVersion rmtTopVer,
        @Nullable IgniteCheckedException err
    ) {
        if (err != null) {
            onDone(err);

            return false;
        }

        if (invalidParts) {
            addNodeAsInvalid(cctx.node(nodeId));

            if (canRemap) {
                awaitVersionAndRemap(rmtTopVer);
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

            onDone(new EntryGetResult(res, ver));
        }
        else
            onDone(res);
    }

    /**
     * @param val Value.
     * @param ver Version.
     */
    private void setResult(@Nullable CacheObject val, @Nullable GridCacheVersion ver) {
        cctx.shared().database().checkpointReadLock();

        try {
            assert !skipVals;

            if (val != null) {
                if (postProcessingClos != null)
                    postProcessingClos.apply(val, ver);

                if (!keepCacheObjects) {
                    Object res = cctx.unwrapBinaryIfNeeded(val, !deserializeBinary);

                    onDone(needVer ? new EntryGetResult(res, ver) : res);
                }
                else
                    onDone(needVer ? new EntryGetResult(val, ver) : val);
            }
            else
                onDone(null);
        }
        catch (Exception e) {
            onDone(e);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
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
     * @param node Invalid node.
     */
    private synchronized void addNodeAsInvalid(ClusterNode node) {
        if (invalidNodes == Collections.<ClusterNode>emptySet())
            invalidNodes = new HashSet<>();

        invalidNodes.add(node);
    }

    /**
     * @return Set of invalid cluster nodes.
     */
    protected synchronized Set<ClusterNode> getInvalidNodes() {
        return invalidNodes;
    }

    /**
     * @param topVer Topology version.
     */
    private boolean checkRetryPermits(AffinityTopologyVersion topVer) {
        if (topVer.equals(this.topVer))
            return true;

        if (REMAP_CNT_UPD.incrementAndGet(this) > MAX_REMAP_CNT) {
            ClusterNode node0 = node;

            onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " +
                MAX_REMAP_CNT + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                (node0 != null ? U.toShortString(node0) : node0) + ", invalidNodes=" + invalidNodes + ']'));

            return false;
        }

        return true;
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Exception.
     */
    private ClusterTopologyServerNotFoundException serverNotFoundError(int part, AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
            "(all partition nodes left the grid) [topVer=" + topVer + ", part=" + part + ", cache=" + cctx.name() + ']');
    }

    /**
     * @param topFut Ready topology future for validation.
     * @return True if validate success, False is not.
     */
    private boolean validate(GridDhtTopologyFuture topFut) {
        assert topFut.isDone() : topFut;

        if (!checkRetryPermits(topFut.topologyVersion()))
            return false;

        Throwable error = topFut.validateCache(cctx, recovery, true, key, null);

        if (error != null) {
            onDone(error);

            return false;
        }
        else
            return true;
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
            long maxTopVer = Math.max(topVer.topologyVersion() + 1, cctx.discovery().topologyVersion());

            awaitVersionAndRemap(new AffinityTopologyVersion(maxTopVer));
        }
        else
            remap(topVer);

        return true;
    }

    /**
     * @param topVer Topology version.
     */
    private void awaitVersionAndRemap(AffinityTopologyVersion topVer) {
        IgniteInternalFuture<AffinityTopologyVersion> awaitTopologyVersionFuture =
            cctx.shared().exchange().affinityReadyFuture(topVer);

        awaitTopologyVersionFuture.listen(f -> {
            try {
                remap(f.get());
            }
            catch (IgniteCheckedException e) {
                onDone(e);
            }
        });
    }

    /**
     * @param topVer Topology version.
     */
    private void remap(final AffinityTopologyVersion topVer) {
        cctx.closures().runLocalSafe(new Runnable() {
            @Override public void run() {
                // If topology changed reset collection of invalid nodes.
                synchronized (this) {
                    invalidNodes = Collections.emptySet();
                }

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

            if (!(err instanceof NodeStoppingException))
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
    @Override public void addDiagnosticRequest(IgniteDiagnosticPrepareContext ctx) {
        if (!isDone()) {
            UUID nodeId;
            AffinityTopologyVersion topVer;

            synchronized (this) {
                nodeId = node != null ? node.id() : null;
                topVer = this.topVer;
            }

            if (nodeId != null)
                ctx.basicInfo(nodeId, "GridPartitionedSingleGetFuture waiting for " +
                    "response [node=" + nodeId +
                    ", key=" + key +
                    ", futId=" + futId +
                    ", topVer=" + topVer + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionedSingleGetFuture.class, this, "super", super.toString());
    }
}
