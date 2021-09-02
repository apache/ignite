/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.ClusterSnapshotFuture;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.IgniteFeatures.SNAPSHOT_RESTORE_CACHE_GROUP;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;

/**
 * Distributed process to restore cache group from the snapshot.
 *
 *
 *
 * Snapshot recovery with transfer partitions.
 *
 * TODO: Some notes left to do for the restore using rebalancing:
 *  1. Disable WAL for starting caches.
 *  (done) 2. Own partitions on exchange (like the rebalancing does).
 *  (done) 3. Handle and fire affinity change message when waitInfo becomes empty.
 *  (done) 4. Load partitions from remote nodes.
 *  (no need) 5. Set partition to MOVING state during copy from snapshot and read partition Metas.
 *  (done) 6. Replace index if topology match partitions.
 *  7. Rebuild index if need.
 *  8. Check the partition reservation during the restore if a next exchange occurs (see message:
 *  Cache groups were not reserved [[[grpId=-903566235, grpName=shared], reason=Checkpoint was marked
 *  as inapplicable for historical rebalancing]])
 *  9. Add cache destroy rollback procedure if loading was unsuccessful.
 *  10. Crash-recovery when node crashes with started, but not loaded caches.
 *  11. Do not allow schema changes during the restore procedure.
 *  12. Restore busy lock should throw exception if a lock acquire fails.
 *
 * Other strategies to be memento to:
 *
 * 1. Load partitions from snapshot, then start cache groups
 * - registered cache groups calculated from discovery thread, no affinity cache data can be calculated without it
 * - index rebuild may be started only after cache group start, will it require cache availability using disabled proxies?
 * - Can affinity be calculated on each node? Node attributes may cause some issues.
 *
 *
 * 2. Start cache groups disabled, then load partitions from snapshot
 * - What would happen with partition counters on primaries and backups? (probably not required)
 * - Should the partition map be updated on client nodes?
 * - When partition is loaded from snapshot, should we update partition counters under checkpoint?
 *
 * 3. Start cache disabled and load data from each partition using partition iterator (defragmentation out of the box + index rebuild)
 * 3a. Transfer snapshot partition files from remove node and read them as local data.
 * 3b. Read partitions on remote node and transfer data via Supply cache message.
 * - cacheId is not stored in the partition file, so it's required to obtain it from the cache data tree
 * - data streamer load job is not suitable for caches running with disabled cache proxies
 * - too many dirty pages can lead to data eviction to ssd
 *
 * 4. Start cache and load partitions as it rebalancing do (transfer files)
 * - There is no lazy init for index partition
 * - node2part must be updated on forceRecreatePartition method call
 * - do not own partitions after cache start
 * - update node2part map after partitions loaded from source and start refreshPartitions/affinityChange (each node sends
 *   a single message with its own cntrMap to the coordinator node, than coordinator resends aggregated message to the whole
 *   cluster nodes on update incomeCntrMap).
 * - Two strategies for the restore on the same topology with indexes: start cache group from preloaded files or start cache
 *   and then re-init index for started cache group. The second case will require data structures re-initialization on the fly.
 *
 */
public class SnapshotRestoreProcess implements PartitionsExchangeAware {
    /** Temporary cache directory prefix. */
    public static final String TMP_PREFIX = "_tmp_snp_restore_";

    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Snapshot restore operation finish message. */
    private static final String OP_FINISHED_MSG = "Cache groups have been successfully restored from the snapshot";

    /** Snapshot restore operation failed message. */
    private static final String OP_FAILED_MSG = "Failed to restore snapshot cache groups";

    /** Prefix for meta store records which means that cache restore is in progress. */
    private static final String RESTORE_KEY_PREFIX = "restore-cache-group-";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotRestoreOperationResponse> prepareRestoreProc;

    /**Cache group restore cache start phase. */
    private final DistributedProcess<UUID, Boolean> preloadProc;

    /** Cache group restore cache start phase. */
    private final DistributedProcess<UUID, Boolean> cacheStartProc;

    /** Cache group restore rollback phase. */
    private final DistributedProcess<UUID, Boolean> rollbackRestoreProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Future to be completed when the cache restore process is complete (this future will be returned to the user). */
    private volatile ClusterSnapshotFuture fut;

    /** Snapshot restore operation context. */
    private volatile SnapshotRestoreContext opCtx;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotRestoreProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        prepareRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, this::prepare, this::finishPrepare);

        preloadProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD, this::preload, this::finishPreload);

        cacheStartProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_START, this::cacheStart, this::finishCacheStart);

        rollbackRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK, this::rollback, this::finishRollback);
    }

    /**
     * Cleanup temporary directories if any exists.
     *
     * @throws IgniteCheckedException If it was not possible to delete some temporary directory.
     */
    protected void cleanup() throws IgniteCheckedException {
        FilePageStoreManager pageStore = (FilePageStoreManager)ctx.cache().context().pageStore();

        File dbDir = pageStore.workDir();

        for (File dir : dbDir.listFiles(dir -> dir.isDirectory() && dir.getName().startsWith(TMP_PREFIX))) {
            if (!U.delete(dir)) {
                throw new IgniteCheckedException("Unable to remove temporary directory, " +
                    "try deleting it manually [dir=" + dir + ']');
            }
        }
    }

    /**
     * Start cache group restore operation.
     *
     * @param snpName Snapshot name.
     * @param cacheGrpNames Cache groups to be restored or {@code null} to restore all cache groups from the snapshot.
     * @return Future that will be completed when the restore operation is complete and the cache groups are started.
     */
    public IgniteFuture<Void> start(String snpName, @Nullable Collection<String> cacheGrpNames) {
        IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();
        ClusterSnapshotFuture fut0;

        try {
            if (ctx.clientNode())
                throw new IgniteException(OP_REJECT_MSG + "Client and daemon nodes can not perform this operation.");

            DiscoveryDataClusterState clusterState = ctx.state().clusterState();

            if (clusterState.state() != ClusterState.ACTIVE || clusterState.transition())
                throw new IgniteException(OP_REJECT_MSG + "The cluster should be active.");

            if (!clusterState.hasBaselineTopology())
                throw new IgniteException(OP_REJECT_MSG + "The baseline topology is not configured for cluster.");

            if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), SNAPSHOT_RESTORE_CACHE_GROUP))
                throw new IgniteException(OP_REJECT_MSG + "Not all nodes in the cluster support restore operation.");

            if (snpMgr.isSnapshotCreating())
                throw new IgniteException(OP_REJECT_MSG + "A cluster snapshot operation is in progress.");

            synchronized (this) {
                if (restoringSnapshotName() != null)
                    throw new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed.");

                fut = new ClusterSnapshotFuture(UUID.randomUUID(), snpName);

                fut0 = fut;
            }
        }
        catch (IgniteException e) {
            snpMgr.recordSnapshotEvent(
                snpName,
                OP_FAILED_MSG + ": " + e.getMessage(),
                EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED
            );

            return new IgniteFinishedFutureImpl<>(e);
        }

        fut0.listen(f -> {
            if (f.error() != null) {
                snpMgr.recordSnapshotEvent(
                    snpName,
                    OP_FAILED_MSG + ": " + f.error().getMessage() + " [reqId=" + fut0.rqId + "].",
                    EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED
                );
            }
            else {
                snpMgr.recordSnapshotEvent(
                    snpName,
                    OP_FINISHED_MSG + " [reqId=" + fut0.rqId + "].",
                    EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED
                );
            }
        });

        String msg = "Cluster-wide snapshot restore operation started [reqId=" + fut0.rqId + ", snpName=" + snpName +
            (cacheGrpNames == null ? "" : ", grps=" + cacheGrpNames) + ']';

        if (log.isInfoEnabled())
            log.info(msg);

        snpMgr.recordSnapshotEvent(snpName, msg, EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED);

        snpMgr.checkSnapshot(snpName, cacheGrpNames, true).listen(f -> {
            if (f.error() != null) {
                finishProcess(fut0.rqId, f.error());

                return;
            }

            if (!F.isEmpty(f.result().exceptions())) {
                finishProcess(fut0.rqId, F.first(f.result().exceptions().values()));

                return;
            }

            if (fut0.interruptEx != null) {
                finishProcess(fut0.rqId, fut0.interruptEx);

                return;
            }

            Set<UUID> dataNodes = new HashSet<>();
            Set<String> snpBltNodes = null;
            Map<ClusterNode, List<SnapshotMetadata>> metas = f.result().metas();
            Map<Integer, String> reqGrpIds = cacheGrpNames == null ? Collections.emptyMap() :
                cacheGrpNames.stream().collect(Collectors.toMap(CU::cacheId, v -> v));

            for (Map.Entry<ClusterNode, List<SnapshotMetadata>> entry : metas.entrySet()) {
                SnapshotMetadata meta = F.first(entry.getValue());

                assert meta != null : entry.getKey().id();

                if (snpBltNodes == null)
                    snpBltNodes = new HashSet<>(meta.baselineNodes());

                dataNodes.add(entry.getKey().id());

                reqGrpIds.keySet().removeAll(meta.partitions().keySet());
            }

            if (snpBltNodes == null) {
                finishProcess(fut0.rqId, new IllegalArgumentException(OP_REJECT_MSG + "No snapshot data " +
                    "has been found [groups=" + reqGrpIds.values() + ", snapshot=" + snpName + ']'));

                return;
            }

            if (!reqGrpIds.isEmpty()) {
                finishProcess(fut0.rqId, new IllegalArgumentException(OP_REJECT_MSG + "Cache group(s) was not " +
                    "found in the snapshot [groups=" + reqGrpIds.values() + ", snapshot=" + snpName + ']'));

                return;
            }
            Collection<UUID> bltNodes = F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
                F.node2id(),
                (node) -> CU.baselineNode(node, ctx.state().clusterState()));

            SnapshotOperationRequest req =
                new SnapshotOperationRequest(fut0.rqId, F.first(dataNodes), snpName, cacheGrpNames, new HashSet<>(bltNodes));

            prepareRestoreProc.start(req.requestId(), req);
        });

        return new IgniteFutureImpl<>(fut0);
    }

    /**
     * Get the name of the snapshot currently being restored
     *
     * @return Name of the snapshot currently being restored or {@code null} if the restore process is not running.
     */
    public @Nullable String restoringSnapshotName() {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null)
            return opCtx0.snpName;

        ClusterSnapshotFuture fut0 = fut;

        return fut0 != null ? fut0.name : null;
    }

    /**
     * @param opCtx Restoring context.
     * @return The request id of restoring snapshot operation.
     */
    private @Nullable UUID restoringId(@Nullable SnapshotRestoreContext opCtx) {
        return opCtx == null ? null : opCtx.reqId;
    }

    /**
     * @param restoreId Process owner id.
     * @return {@code true} if partition states of given cache groups must be reset
     * to the initial {@link GridDhtPartitionState#MOVING} state.
     */
    public boolean requirePartitionLoad(CacheConfiguration<?, ?> ccfg, UUID restoreId) {
        if (restoreId == null)
            return false;

        SnapshotRestoreContext opCtx0 = opCtx;

        if (!restoreId.equals(restoringId(opCtx0)))
            return false;

        if (!isRestoring(ccfg, opCtx0))
            return false;

        // Called from the discovery thread. It's safe to call all the methods reading
        // the snapshot context, since the context changed only through the discovery thread also.
        return !opCtx0.sameTop;
    }

    /**
     * @param ccfg Cache configuration.
     * @return {@code True} if the cache or group with the specified name is currently being restored.
     */
    public boolean isRestoring(CacheConfiguration<?, ?> ccfg) {
        return isRestoring(ccfg, opCtx);
    }

    /**
     * Check if the cache or group with the specified name is currently being restored from the snapshot.
     * @param opCtx Restoring context.
     * @param ccfg Cache configuration.
     * @return {@code True} if the cache or group with the specified name is currently being restored.
     */
    private boolean isRestoring(CacheConfiguration<?, ?> ccfg, @Nullable SnapshotRestoreContext opCtx) {
        assert ccfg != null;

        if (opCtx == null)
            return false;

        Map<Integer, StoredCacheData> cacheCfgs = opCtx.cfgs;

        String cacheName = ccfg.getName();
        String grpName = ccfg.getGroupName();

        int cacheId = CU.cacheId(cacheName);

        if (cacheCfgs.containsKey(cacheId))
            return true;

        for (File grpDir : opCtx.dirs) {
            String locGrpName = FilePageStoreManager.cacheGroupName(grpDir);

            if (grpName != null) {
                if (cacheName.equals(locGrpName))
                    return true;

                if (CU.cacheId(locGrpName) == CU.cacheId(grpName))
                    return true;
            }
            else if (CU.cacheId(locGrpName) == cacheId)
                return true;
        }

        return false;
    }

    /**
     * @param reqId Request ID.
     * @return Server nodes on which a successful start of the cache(s) is required, if any of these nodes fails when
     *         starting the cache(s), the whole procedure is rolled back.
     */
    public Set<UUID> cacheStartRequiredAliveNodes(IgniteUuid reqId) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null || !reqId.globalId().equals(opCtx0.reqId))
            return Collections.emptySet();

        return Collections.unmodifiableSet(opCtx0.nodes);
    }

    /**
     * Finish local cache group restore process.
     *
     * @param reqId Request ID.
     */
    private void finishProcess(UUID reqId) {
        finishProcess(reqId, null);
    }

    /**
     * Finish local cache group restore process.
     *
     * @param reqId Request ID.
     * @param err Error, if any.
     */
    private void finishProcess(UUID reqId, @Nullable Throwable err) {
        if (err != null)
            log.error(OP_FAILED_MSG + " [reqId=" + reqId + "].", err);
        else if (log.isInfoEnabled())
            log.info(OP_FINISHED_MSG + " [reqId=" + reqId + "].");

        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null && reqId.equals(opCtx0.reqId))
            opCtx = null;

        synchronized (this) {
            ClusterSnapshotFuture fut0 = fut;

            if (fut0 != null && reqId.equals(fut0.rqId)) {
                fut = null;

                ctx.pools().getSystemExecutorService().submit(() -> fut0.onDone(null, err));
            }
        }
    }

    /**
     * Node left callback.
     *
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null && opCtx0.nodes.contains(leftNodeId)) {
            opCtx0.err.compareAndSet(null, new ClusterTopologyCheckedException(OP_REJECT_MSG +
                "Required node has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Cancel the currently running local restore procedure.
     *
     * @param reason Interruption reason.
     * @param snpName Snapshot name.
     * @return Future that will be finished when process the process is complete. The result of this future will be
     * {@code false} if the restore process with the specified snapshot name is not running at all.
     */
    public IgniteFuture<Boolean> cancel(IgniteCheckedException reason, String snpName) {
        SnapshotRestoreContext opCtx0;
        ClusterSnapshotFuture fut0 = null;

        synchronized (this) {
            opCtx0 = opCtx;

            if (fut != null && fut.name.equals(snpName)) {
                fut0 = fut;

                fut0.interruptEx = reason;
            }
        }

        boolean ctxStop = opCtx0 != null && opCtx0.snpName.equals(snpName);

        if (ctxStop)
            interrupt(opCtx0, reason);

        return fut0 == null ? new IgniteFinishedFutureImpl<>(ctxStop) :
            new IgniteFutureImpl<>(fut0.chain(f -> true));
    }

    /**
     * Interrupt the currently running local restore procedure.
     *
     * @param reason Interruption reason.
     */
    public void interrupt(IgniteCheckedException reason) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null)
            interrupt(opCtx0, reason);
    }

    /**
     * Interrupt the currently running local restore procedure.
     *
     * @param opCtx Snapshot restore operation context.
     * @param reason Interruption reason.
     */
    private void interrupt(SnapshotRestoreContext opCtx, IgniteCheckedException reason) {
        opCtx.err.compareAndSet(null, reason);

        IgniteFuture<?> stopFut;

        synchronized (this) {
            stopFut = opCtx.stopFut;
        }

        if (stopFut != null)
            stopFut.get();
    }

    /**
     * Ensures that a cache with the specified name does not exist locally.
     *
     * @param name Cache name.
     */
    private void ensureCacheAbsent(String name) {
        int id = CU.cacheId(name);

        if (ctx.cache().cacheGroupDescriptors().containsKey(id) || ctx.cache().cacheDescriptor(id) != null) {
            throw new IgniteIllegalStateException("Cache \"" + name +
                "\" should be destroyed manually before perform restore operation.");
        }
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestoreOperationResponse> prepare(SnapshotOperationRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        try {
            DiscoveryDataClusterState state = ctx.state().clusterState();
            IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();

            if (state.state() != ClusterState.ACTIVE || state.transition())
                throw new IgniteCheckedException(OP_REJECT_MSG + "The cluster should be active.");

            if (snpMgr.isSnapshotCreating())
                throw new IgniteCheckedException(OP_REJECT_MSG + "A cluster snapshot operation is in progress.");

            for (UUID nodeId : req.nodes()) {
                ClusterNode node = ctx.discovery().node(nodeId);

                if (node == null || !CU.baselineNode(node, state) || !ctx.discovery().alive(node)) {
                    throw new IgniteCheckedException(
                        OP_REJECT_MSG + "Required node has left the cluster [nodeId-" + nodeId + ']');
                }
            }

            SnapshotRestoreContext opCtx0 = prepareContext(req);

            synchronized (this) {
                opCtx = opCtx0;

                ClusterSnapshotFuture fut0 = fut;

                if (fut0 != null)
                    opCtx0.errHnd.accept(fut0.interruptEx);
            }

            if (opCtx0.dirs.isEmpty())
                return new GridFinishedFuture<>();

            // Ensure that shared cache groups has no conflicts.
            for (StoredCacheData cfg : opCtx0.cfgs.values()) {
                ensureCacheAbsent(cfg.config().getName());

                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getGroupName());
            }

            if (ctx.isStopping())
                throw new NodeStoppingException("The node is stopping: " + ctx.localNodeId());

            if (log.isInfoEnabled()) {
                log.info("Starting local snapshot prepare restore operation" +
                    " [reqId=" + req.requestId() +
                    ", snapshot=" + req.snapshotName() +
                    ", cache(s)=" + F.viewReadOnly(opCtx0.cfgs.values(), data -> data.config().getName()) + ']');
            }

            return new GridFinishedFuture<>(new SnapshotRestoreOperationResponse(opCtx.cfgs.values(),
                opCtx.metasPerNode.get(ctx.localNodeId())));
        }
        catch (IgniteIllegalStateException | IgniteCheckedException | RejectedExecutionException e) {
            log.error("Unable to restore cache group(s) from the snapshot " +
                "[reqId=" + req.requestId() + ", snapshot=" + req.snapshotName() + ']', e);

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param cacheDir Cache directory.
     * @return Temporary directory.
     */
    private static File formatTmpDirName(File cacheDir) {
        return new File(cacheDir.getParent(), TMP_PREFIX + cacheDir.getName());
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @return Snapshot restore operation context.
     * @throws IgniteCheckedException If failed.
     */
    private SnapshotRestoreContext prepareContext(SnapshotOperationRequest req) throws IgniteCheckedException {
        if (opCtx != null) {
            throw new IgniteCheckedException(OP_REJECT_MSG +
                "The previous snapshot restore operation was not completed.");
        }
        GridCacheSharedContext<?, ?> cctx = ctx.cache().context();

        List<SnapshotMetadata> metas = cctx.snapshotMgr().readSnapshotMetadatas(req.snapshotName());

        if (F.first(metas) == null) {
            return new SnapshotRestoreContext(req, Collections.emptyList(), Collections.emptyMap(), cctx.localNodeId(),
                Collections.emptyList());
        }

        if (F.first(metas).pageSize() != cctx.database().pageSize()) {
            throw new IgniteCheckedException("Incompatible memory page size " +
                "[snapshotPageSize=" + F.first(metas).pageSize() +
                ", local=" + cctx.database().pageSize() +
                ", snapshot=" + req.snapshotName() +
                ", nodeId=" + cctx.localNodeId() + ']');
        }

        Set<File> cacheDirs = new HashSet<>();
        Map<String, StoredCacheData> cfgsByName = new HashMap<>();
        FilePageStoreManager pageStore = (FilePageStoreManager)cctx.pageStore();

        // Collect the cache configurations and prepare a temporary directory for copying files.
        // Metastorage can be restored only manually by directly copying files.
        for (SnapshotMetadata meta : metas) {
            for (File snpCacheDir : cctx.snapshotMgr().snapshotCacheDirectories(req.snapshotName(), meta.folderName(),
                name -> !METASTORAGE_CACHE_NAME.equals(name))) {
                String grpName = FilePageStoreManager.cacheGroupName(snpCacheDir);

                if (!F.isEmpty(req.groups()) && !req.groups().contains(grpName))
                    continue;

                File cacheDir = pageStore.cacheWorkDir(snpCacheDir.getName().startsWith(CACHE_GRP_DIR_PREFIX), grpName);

                if (cacheDir.exists()) {
                    if (!cacheDir.isDirectory()) {
                        throw new IgniteCheckedException("Unable to restore cache group, file with required directory " +
                            "name already exists [group=" + grpName + ", file=" + cacheDir + ']');
                    }

                    if (cacheDir.list().length > 0) {
                        throw new IgniteCheckedException("Unable to restore cache group, directory is not empty " +
                            "[group=" + grpName + ", dir=" + cacheDir + ']');
                    }

                    if (!cacheDir.delete()) {
                        throw new IgniteCheckedException("Unable to remove empty cache directory " +
                            "[group=" + grpName + ", dir=" + cacheDir + ']');
                    }
                }

                File tmpCacheDir = formatTmpDirName(cacheDir);

                if (tmpCacheDir.exists()) {
                    throw new IgniteCheckedException("Unable to restore cache group, temp directory already exists " +
                        "[group=" + grpName + ", dir=" + tmpCacheDir + ']');
                }

                cacheDirs.add(cacheDir);

                pageStore.readCacheConfigurations(snpCacheDir, cfgsByName);
            }
        }

        Map<Integer, StoredCacheData> cfgsById =
            cfgsByName.values().stream().collect(Collectors.toMap(v -> CU.cacheId(v.config().getName()), v -> v));

        return new SnapshotRestoreContext(req, cacheDirs, cfgsById, cctx.localNodeId(), metas);
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestoreOperationResponse> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Exception failure = F.first(errs.values());

        assert opCtx0 != null || failure != null : "Context has not been created on the node " + ctx.localNodeId();

        if (opCtx0 == null || !reqId.equals(opCtx0.reqId)) {
            finishProcess(reqId, failure);

            return;
        }

        if (failure == null)
            failure = checkNodeLeft(opCtx0.nodes, res.keySet());

        // Context has been created - should rollback changes cluster-wide.
        if (failure != null) {
            opCtx0.errHnd.accept(failure);

            return;
        }

        Map<Integer, StoredCacheData> globalCfgs = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestoreOperationResponse> e : res.entrySet()) {
            if (e.getValue().ccfgs != null) {
                for (StoredCacheData cacheData : e.getValue().ccfgs)
                    globalCfgs.put(CU.cacheId(cacheData.config().getName()), cacheData);
            }

            opCtx0.metasPerNode.computeIfAbsent(e.getKey(), id -> new ArrayList<>())
                .addAll(e.getValue().metas);
        }

        opCtx0.cfgs = globalCfgs;
        opCtx0.sameTop = sameTopology(opCtx0.nodes, opCtx0.metasPerNode);

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            preloadProc.start(reqId, reqId);
    }

    /**
     * @param nodes Nodes that have to alive to complete restore operation.
     * @return {@code true} if the snapshot and current cluster topologies are compatible.
     */
    private boolean sameTopology(Set<UUID> nodes, Map<UUID, ArrayList<SnapshotMetadata>> metas) {
        Set<String> clusterBlts = nodes.stream()
            .map(n -> ctx.discovery().node(n).consistentId().toString())
            .collect(Collectors.toSet());

        // Snapshot baseline nodes.
        List<SnapshotMetadata> nodeMetas = F.first(metas.values());

        if (nodeMetas == null)
            return false;

        Set<String> snpBlts = F.first(nodeMetas).baselineNodes();

        if (!clusterBlts.containsAll(snpBlts))
            return false;

        // Each node must have it's own local copy of a snapshot.
        for (Map.Entry<UUID, ArrayList<SnapshotMetadata>> e : metas.entrySet()) {
            String consId = ctx.discovery().node(e.getKey()).consistentId().toString();

            // Local node metadata is always on the first place of a list.
            SnapshotMetadata meta = F.first(e.getValue());

            if (meta == null || !meta.consistentId().equals(consId))
                return false;
        }

        return true;
    }

    /**
     * @param reqId Request id.
     * @return Future which will be completed when the preload ends.
     */
    private IgniteInternalFuture<Boolean> preload(UUID reqId) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;
        GridFutureAdapter<Boolean> retFut = new GridFutureAdapter<>();

        if (opCtx0 == null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot restore process has incorrect restore state: " + reqId));

        try {
            if (ctx.isStopping())
                throw new NodeStoppingException("Node is stopping: " + ctx.localNodeId());

            IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();
            opCtx0.stopFut = new IgniteFutureImpl<>(retFut.chain(f -> null));

            CompletableFuture<Void> metaFut = ctx.localNodeId().equals(opCtx0.opNodeId) ?
                CompletableFuture.runAsync(
                    () -> {
                        try {
                            SnapshotMetadata meta = F.first(opCtx0.metasPerNode.get(opCtx0.opNodeId));

                            File binDir = binaryWorkDir(snpMgr.snapshotLocalDir(opCtx0.snpName).getAbsolutePath(),
                                meta.folderName());

                            ctx.cacheObjects().updateMetadata(binDir, opCtx0.stopChecker);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }, snpMgr.snapshotExecutorService())
                    .whenComplete((r, t) -> opCtx0.errHnd.accept(t)) :
                CompletableFuture.completedFuture(null);

            CompletableFuture<Void> partFut = CompletableFuture.completedFuture(null);

            if (opCtx0.sameTop) {
                List<CompletableFuture<Path>> futs = new ArrayList<>();
                String pdsFolderName = ctx.pdsFolderResolver().resolveFolders().folderName();

                for (File cacheDir : opCtx0.dirs) {
                    File snpCacheDir = new File(ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx0.snpName),
                        Paths.get(databaseRelativePath(pdsFolderName), cacheDir.getName()).toString());

                    if (!snpCacheDir.exists())
                        throw new IgniteCheckedException("Snapshot directory doesn't exist: " + snpCacheDir);

                    File tmpCacheDir = formatTmpDirName(cacheDir);

                    tmpCacheDir.mkdir();

                    for (File snpFile : snpCacheDir.listFiles()) {
                        CompletableFuture<Path> fut;

                        copyFileLocal(snpMgr, opCtx0, snpFile,
                            Paths.get(tmpCacheDir.getAbsolutePath(), snpFile.getName()),
                            fut = new CompletableFuture<>());

                        futs.add(fut);
                    }
                }

                int size = futs.size();

                partFut = CompletableFuture.allOf(futs.toArray(new CompletableFuture[size]))
                    .runAfterBothAsync(metaFut, () -> {
                        try {
                            if (opCtx0.stopChecker.getAsBoolean())
                                throw new IgniteInterruptedException("The operation has been stopped on temporary directory switch.");

                            for (File src : opCtx0.dirs)
                                Files.move(formatTmpDirName(src).toPath(), src.toPath(), StandardCopyOption.ATOMIC_MOVE);
                        }
                        catch (IOException e) {
                            throw new IgniteException(e);
                        }
                    }, snpMgr.snapshotExecutorService())
                    .whenComplete((r, t) -> opCtx0.errHnd.accept(t))
                    // Complete the local rebalance cache future, since the data is loaded.
                    .thenAccept(r -> opCtx0.cacheRebalanceFut.onDone(true));
            }

            CompletableFuture.allOf(metaFut, partFut)
                .whenComplete((res, t) -> {
                    if (t == null) {
                        retFut.onDone(true);
                    }
                    else {
                        log.error("Unable to restore cache group(s) from a snapshot " +
                            "[reqId=" + opCtx.reqId + ", snapshot=" + opCtx.snpName + ']', t);

                        retFut.onDone(t);
                    }
                });

        }
        catch (Exception ex) {
            opCtx0.errHnd.accept(ex);

            return new GridFinishedFuture<>(ex);
        }

        return retFut;
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPreload(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        // TODO Remove duplicated code which handles distributed process finish action.
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Exception failure = F.first(errs.values());

        assert opCtx0 != null || failure != null : "Context has not been created on the node " + ctx.localNodeId();

        if (opCtx0 == null || !reqId.equals(opCtx0.reqId)) {
            finishProcess(reqId, failure);

            return;
        }

        if (failure == null)
            failure = checkNodeLeft(opCtx0.nodes, res.keySet());

        // Context has been created - should rollback changes cluster-wide.
        if (failure != null) {
            opCtx0.errHnd.accept(failure);

            if (U.isLocalNodeCoordinator(ctx.discovery()))
                rollbackRestoreProc.start(reqId, reqId);

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            cacheStartProc.start(reqId, reqId);
    }

    /**
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> cacheStart(UUID reqId) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;

        Throwable err = opCtx0.err.get();

        if (err != null)
            return new GridFinishedFuture<>(err);

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return opCtx0.cacheRebalanceFut;

        Collection<StoredCacheData> ccfgs = opCtx0.cfgs.values();

        if (log.isInfoEnabled()) {
            log.info("Starting restored caches " +
                "[reqId=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName +
                ", caches=" + F.viewReadOnly(ccfgs, c -> c.config().getName()) + ']');
        }

        // We set the topology node IDs required to successfully start the cache, if any of the required nodes leave
        // the cluster during the cache startup, the whole procedure will be rolled back.
        GridCompoundFuture<Boolean, Boolean> awaitBoth = new GridCompoundFuture<>();

        // TODO WAL must be disabled also at startup.
        // TODO Exclude resending partitions if restore is in progress.
        awaitBoth.add(ctx.cache().dynamicStartCachesByStoredConf(ccfgs, true, true, true,
            IgniteUuid.fromUuid(reqId)));
        awaitBoth.add(opCtx0.cacheRebalanceFut);

        awaitBoth.markInitialized();

        return awaitBoth;
    }

    /** {@inheritDoc} */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture exchFut) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;
        Set<Integer> grpIdsToStart = getCachesLoadingFromSnapshot(exchFut, opCtx0);

        if (F.isEmpty(grpIdsToStart))
            return;

        assert opCtx0 != null;
        assert !opCtx0.sameTop : "WAL must be disabled only for caches restoring from snapshot taken on another cluster: " + opCtx0;

        // This is happened on the exchange which has been initiated by a dynamic cache start message and intend to switch
        // off the WAL for cache groups loading from a snapshot.
        for (CacheGroupContext grp : F.view(ctx.cache().cacheGroups(), g -> grpIdsToStart.contains(g.groupId()))) {
            assert grp.localWalEnabled() : grp.cacheOrGroupName();

            // Check partitions have not even been created yet, so the PartitionMetaStateRecord won't be logged to the WAL.
            for (int p = 0; p < grp.topology().partitions(); p++)
                assert grp.topology().localPartition(p) == null : p;

            grp.localWalEnabled(false, true);
        }
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishCacheStart(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Exception failure = errs.values().stream().findFirst().
            orElse(checkNodeLeft(opCtx0.nodes, res.keySet()));

        if (failure == null) {
            finishProcess(reqId);

            // TODO Is it correct to call it here?
            ctx.cache().restartProxies();

            return;
        }

        opCtx0.err.compareAndSet(null, failure);

        // TODO Do we need to remove cache from the waitInfo rebalance groups prior to cache actually rollback fired?

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, reqId);
    }

    /**
     * @param grps Ordered list of cache groups sorted by priority.
     * @param exchFut Exchange future.
     */
    public void onRebalanceReady(Set<CacheGroupContext> grps, @Nullable GridDhtPartitionsExchangeFuture exchFut) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Set<Integer> exchGrpIds = getCachesLoadingFromSnapshot(exchFut, opCtx0);

        Set<CacheGroupContext> filtered = grps.stream()
            .filter(Objects::nonNull)
            .filter(g -> exchGrpIds.contains(g.groupId()))
            .collect(Collectors.toSet());

        // Restore requests has been already processed at previous exchange.
        if (filtered.isEmpty())
            return;

        assert opCtx0 != null;

        // First preload everything from the local node.
        List<SnapshotMetadata> locMetas = opCtx0.metasPerNode.get(ctx.localNodeId());

        Map<Integer, Set<Integer>> notScheduled = new HashMap<>();

        // Register partitions to be processed.
        for (CacheGroupContext grp : filtered) {
            if (F.isEmpty(locMetas))
                break;

            notScheduled.put(grp.groupId(),
                affinityPartitions(grp.affinity(), ctx.cache().context().localNode(), Integer::new));

            Set<Integer> leftParts = notScheduled.get(grp.groupId());

            assert !leftParts.contains(INDEX_PARTITION);

            if (F.isEmpty(leftParts)) {
                opCtx0.cacheRebalanceFut.onDone();

                continue;
            }

            Set<PartitionRestoreLifecycleFuture> partLfs = U.newHashSet(leftParts.size());

            for (Integer partId : leftParts) {
                // Affinity node partitions are inited on exchange.
                GridDhtLocalPartition part = grp.topology().localPartition(partId);
                PartitionRestoreLifecycleFuture lf = PartitionRestoreLifecycleFuture.create(grp, log, partId);

                // Start partition eviction first.
                if (part == null)
                    lf.cleared.complete(null);
                else {
                    part.clearAsync().listen(f -> {
                        // This future must clear all heap cache entries too from the GridDhtLocalPartition map.
                        if (f.error() == null)
                            lf.cleared.complete(f.result());
                        else
                            lf.cleared.completeExceptionally(f.error());
                    });
                }

                partLfs.add(lf);
            }

            // TODO partitions may not be even created in a snapshot.

            for (SnapshotMetadata meta : locMetas) {
                if (leftParts.isEmpty())
                    break;

                leftParts.removeIf(partId -> {
                    boolean canRestore = ofNullable(meta.partitions().get(grp.groupId()))
                        .orElse(Collections.emptySet())
                        .contains(partId);

                    if (canRestore) {
                        File cacheDir = ((FilePageStoreManager)grp.shared().pageStore()).cacheWorkDir(grp.sharedGroup(),
                            grp.cacheOrGroupName());

                        File snpCacheDir = new File(grp.shared().snapshotMgr().snapshotLocalDir(opCtx.snpName),
                            Paths.get(databaseRelativePath(meta.folderName()), cacheDir.getName()).toString());

                        File snpFile = new File(snpCacheDir, FilePageStoreManager.getPartitionFileName(partId));
                        Path target0 = Paths.get(cacheDir.getAbsolutePath(),
                            TMP_PREFIX + FilePageStoreManager.getPartitionFileName(partId));

                        assert snpCacheDir.exists() : "node=" + grp.shared().localNodeId() + ", dir=" + snpCacheDir;

                        copyFileLocal(grp.shared().snapshotMgr(),
                            opCtx,
                            snpFile,
                            target0,
                            findLoadFuture(partLfs, partId));
                    }

                    return canRestore;
                });
            }

            // TODO: IGNITE-11075 Rebuild index over a partition file.
            Map<Integer, CompletableFuture<Void>> indexRebuildCaches = grp.caches().stream()
                .collect(Collectors.toMap(GridCacheContext::cacheId, id -> new CompletableFuture<>()));

            CompletableFuture<Void> indexCacheGroupRebFut = allOfFailFast(indexRebuildCaches.values())
                .thenRunAsync(() -> {
                    // Force new checkpoint to make sure owning state is captured.
                    CheckpointProgress cp = ctx.cache().context().database()
                        .forceCheckpoint("Snapshot restore procedure triggered WAL enabling: " + grp.cacheOrGroupName());

                    cp.onStateChanged(PAGE_SNAPSHOT_TAKEN, () -> grp.localWalEnabled(true, true));
                });

            // This will not be fired if partitions loading future completes with an exception.
            CompletableFuture<Void> partsInited = allOfFailFast(partLfs)
                .thenRunAsync(() -> scheduleIndexRebuild(grp.shared().kernalContext(),
                    grp.caches(),
                    opCtx0.err,
                    indexRebuildCaches::get));

            opCtx0.locProgress.put(
                CacheRestoreLifecycleFuture.create(grp,
                    partsInited,
                    indexCacheGroupRebFut,
                    () -> {
                        // Initialization action when cache group partitions fully initialized.
                        // It is safe to own all persistence cache partitions here, since partitions state
                        // are already located on the disk.
                        grp.topology().ownMoving();

                        if (log.isDebugEnabled()) {
                            log.debug("Partitions have been scheduled to resend [reason=" +
                                "Group durability restored, name=" + grp.cacheOrGroupName() + ']');
                        }

                        grp.shared().exchange().refreshPartitions(Collections.singleton(grp));
                    }),
                partLfs);
        }

        // Load other partitions from remote nodes.
        try {
            for (Map.Entry<UUID, Map<Integer, Set<Integer>>> m :
                snapshotAffinity(opCtx0.metasPerNode,
                    (grpId, partId) -> notScheduled.get(grpId) != null && notScheduled.get(grpId).contains(partId)).entrySet()) {
                if (m.getKey().equals(ctx.localNodeId()))
                    continue;

                ctx.cache().context().snapshotMgr()
                    .requestRemoteSnapshot(m.getKey(),
                        opCtx0.snpName,
                        m.getValue(),
                        opCtx0.stopChecker,
                        (snpFile, t) -> {
                            if (t == null) {
                                int grpId = CU.cacheId(cacheGroupName(snpFile.getParentFile()));
                                int partId = partId(snpFile.getName());

                                CacheRestoreLifecycleFuture plf = F.find(opCtx0.locProgress.keySet(),
                                    null,
                                    new IgnitePredicate<CacheRestoreLifecycleFuture>() {
                                        @Override public boolean apply(CacheRestoreLifecycleFuture f) {
                                            return f.grp.groupId() == grpId;
                                        }
                                    });

                                assert plf != null : snpFile.getAbsolutePath();

                                CacheGroupContext grp0 = plf.grp;
                                CompletableFuture<Path> partFut = findLoadFuture(opCtx0.locProgress.get(plf), partId);

                                assert partFut != null : snpFile.getAbsolutePath();

                                File cacheDir = ((FilePageStoreManager)grp0.shared().pageStore())
                                    .cacheWorkDir(grp0.sharedGroup(),
                                    grp0.cacheOrGroupName());

                                Path target0 = Paths.get(cacheDir.getAbsolutePath(), TMP_PREFIX + snpFile.getName());

                                try {
                                    IgniteSnapshotManager.copy(grp0.shared().snapshotMgr().ioFactory(),
                                        snpFile,
                                        target0.toFile(),
                                        snpFile.length());

                                    partFut.complete(target0);
                                }
                                catch (Exception e) {
                                    partFut.completeExceptionally(e);
                                }
                            }
                            else
                                opCtx0.errHnd.accept(t);
                        });

                removeAllValues(notScheduled, m.getValue());
            }

            assert F.size(notScheduled.values(), p -> !p.isEmpty()) == 0 : "[notScheduled=" + notScheduled + ']';
        }
        catch (IgniteCheckedException e) {
            opCtx0.errHnd.accept(e);
        }

        // Complete cache future.
        allOfFailFast(opCtx0.locProgress.keySet()).whenComplete((res, t) -> {
            if (t == null)
                opCtx0.cacheRebalanceFut.onDone(true);
            else
                opCtx0.cacheRebalanceFut.onDone(t);
        });
    }

    /**
     * @param metas Map of snapshot metadata distribution across the cluster.
     * @return Map of cache partitions per each node.
     */
    private static Map<UUID, Map<Integer, Set<Integer>>> snapshotAffinity(
        Map<UUID, ArrayList<SnapshotMetadata>> metas,
        BiPredicate<Integer, Integer> filter
    ) {
        Map<UUID, Map<Integer, Set<Integer>>> nodeToSnp = new HashMap<>();

        for (Map.Entry<UUID, ArrayList<SnapshotMetadata>> e : metas.entrySet()) {
            UUID nodeId = e.getKey();

            for (SnapshotMetadata meta : ofNullable(e.getValue()).orElse(new ArrayList<>())) {
                Map<Integer, Set<Integer>> parts = ofNullable(meta.partitions()).orElse(Collections.emptyMap());

                for (Map.Entry<Integer, Set<Integer>> metaParts : parts.entrySet()) {
                    for (Integer partId : metaParts.getValue()) {
                        if (filter.test(metaParts.getKey(), partId)) {
                            nodeToSnp.computeIfAbsent(nodeId, n -> new HashMap<>())
                                .computeIfAbsent(metaParts.getKey(), k -> new HashSet<>())
                                .add(partId);
                        }
                    }
                }
            }
        }

        List<UUID> list = new ArrayList<>(nodeToSnp.keySet());
        Collections.shuffle(list);

        Map<UUID, Map<Integer, Set<Integer>>> shuffleMap = new LinkedHashMap<>();
        list.forEach(k -> shuffleMap.put(k, nodeToSnp.get(k)));

        return shuffleMap;
    }

    /**
     * @param from The source map to remove elements from.
     * @param upd The map to check.
     */
    private static void removeAllValues(Map<Integer, Set<Integer>> from, Map<Integer, Set<Integer>> upd) {
        for (Map.Entry<Integer, Set<Integer>> e : upd.entrySet())
            from.getOrDefault(e.getKey(), Collections.emptySet()).removeAll(e.getValue());
    }

    /**
     * @param reqNodes Set of required topology nodes.
     * @param respNodes Set of responding topology nodes.
     * @return Error, if no response was received from the required topology node.
     */
    private Exception checkNodeLeft(Set<UUID> reqNodes, Set<UUID> respNodes) {
        if (!respNodes.containsAll(reqNodes)) {
            Set<UUID> leftNodes = new HashSet<>(reqNodes);

            leftNodes.removeAll(respNodes);

            return new ClusterTopologyCheckedException(OP_REJECT_MSG +
                "Required node has left the cluster [nodeId=" + leftNodes + ']');
        }

        return null;
    }

    /**
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> rollback(UUID reqId) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null || F.isEmpty(opCtx0.dirs))
            return new GridFinishedFuture<>();

        GridFutureAdapter<Boolean> retFut = new GridFutureAdapter<>();

        synchronized (this) {
            opCtx0.stopFut = new IgniteFutureImpl<>(retFut.chain(f -> null));

            try {
                ctx.cache().context().snapshotMgr().snapshotExecutorService().execute(() -> {
                    if (log.isInfoEnabled()) {
                        log.info("Removing restored cache directories [reqId=" + reqId +
                            ", snapshot=" + opCtx0.snpName + ", dirs=" + opCtx0.dirs + ']');
                    }

                    IgniteCheckedException ex = null;

                    for (File cacheDir : opCtx0.dirs) {
                        File tmpCacheDir = formatTmpDirName(cacheDir);

                        if (tmpCacheDir.exists() && !U.delete(tmpCacheDir)) {
                            log.error("Unable to perform rollback routine completely, cannot remove temp directory " +
                                "[reqId=" + reqId + ", snapshot=" + opCtx0.snpName + ", dir=" + tmpCacheDir + ']');

                            ex = new IgniteCheckedException("Unable to remove temporary cache directory " + cacheDir);
                        }

                        if (cacheDir.exists() && !U.delete(cacheDir)) {
                            log.error("Unable to perform rollback routine completely, cannot remove cache directory " +
                                "[reqId=" + reqId + ", snapshot=" + opCtx0.snpName + ", dir=" + cacheDir + ']');

                            ex = new IgniteCheckedException("Unable to remove cache directory " + cacheDir);
                        }
                    }

                    if (ex != null)
                        retFut.onDone(ex);
                    else
                        retFut.onDone(true);
                });
            }
            catch (RejectedExecutionException e) {
                log.error("Unable to perform rollback routine, task has been rejected " +
                    "[reqId=" + reqId + ", snapshot=" + opCtx0.snpName + ']');

                retFut.onDone(e);
            }
        }

        return retFut;
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishRollback(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        if (!errs.isEmpty()) {
            log.warning("Some nodes were unable to complete the rollback routine completely, check the local log " +
                "files for more information [nodeIds=" + errs.keySet() + ']');
        }

        SnapshotRestoreContext opCtx0 = opCtx;

        if (!res.keySet().containsAll(opCtx0.nodes)) {
            Set<UUID> leftNodes = new HashSet<>(opCtx0.nodes);

            leftNodes.removeAll(res.keySet());

            log.warning("Some of the nodes left the cluster and were unable to complete the rollback" +
                " operation [reqId=" + reqId + ", snapshot=" + opCtx0.snpName + ", node(s)=" + leftNodes + ']');
        }

        finishProcess(reqId, opCtx0.err.get());
    }

    /**
     * @param mgr Ignite snapshot manager.
     * @param opCtx Snapshot operation context.
     * @param snpFile Snapshot file to copy.
     * @param target Destination path.
     * @param fut Future which will handle the copy results.
     */
    private static void copyFileLocal(
        IgniteSnapshotManager mgr,
        SnapshotRestoreContext opCtx,
        File snpFile,
        Path target,
        CompletableFuture<Path> fut
    ) {
        CompletableFuture.supplyAsync(
            () -> {
                if (opCtx.stopChecker.getAsBoolean())
                    throw new IgniteInterruptedException("The operation has been stopped on copy file: " + snpFile.getAbsolutePath());

                if (Thread.interrupted())
                    throw new IgniteInterruptedException("Thread has been interrupted: " + Thread.currentThread().getName());

                if (!snpFile.exists()) {
                    throw new IgniteException("Partition snapshot file doesn't exist [snpName=" + opCtx.snpName +
                        ", snpDir=" + snpFile.getAbsolutePath() + ", name=" + snpFile.getName() + ']');
                }

                IgniteSnapshotManager.copy(mgr.ioFactory(), snpFile, target.toFile(), snpFile.length());

                return target;
            }, mgr.snapshotExecutorService())
            .whenComplete((r, t) -> opCtx.errHnd.accept(t))
            .whenComplete((res, t) -> {
                if (t == null)
                    fut.complete(res);
                else
                    fut.completeExceptionally(t);
            });
    }

    /**
     * @param affCache Affinity cache.
     * @param node Cluster node to get assigned partitions.
     * @return The set of partitions assigned to the given node.
     */
    private static <T> Set<T> affinityPartitions(
        GridAffinityAssignmentCache affCache,
        ClusterNode node,
        IntFunction<T> factory
    ) {
        return IntStream.range(0, affCache.partitions())
            .filter(p -> affCache.idealAssignment().assignment().get(p).contains(node))
            .mapToObj(factory)
            .collect(Collectors.toSet());
    }

    /**
     * @param action Action to execute.
     * @param ex Consumer which accepts exceptional execution result.
     */
    private static <T> void handleException(Callable<T> action, Consumer<Throwable> ex) {
        try {
            action.call();
        }
        catch (Throwable t) {
            ex.accept(t);
        }
    }

    /**
     * @param lcs Collection of partition context.
     * @param partId Partition id to find.
     * @return Load future.
     */
    private static @Nullable CompletableFuture<Path> findLoadFuture(Set<PartitionRestoreLifecycleFuture> lcs, int partId) {
        return ofNullable(F.find(lcs, null, (IgnitePredicate<? super PartitionRestoreLifecycleFuture>)f -> f.partId == partId))
            .map(c -> c.loaded)
            .orElse(null);
    }

    /**
     * @param fut Current exchange future.
     * @param ctx Current snapshot restore context.
     * @return The set of cache groups needs to be processed.
     */
    private Set<Integer> getCachesLoadingFromSnapshot(GridDhtPartitionsExchangeFuture fut, SnapshotRestoreContext ctx) {
        if (fut == null || fut.exchangeActions() == null || ctx == null)
            return Collections.emptySet();

        return fut.exchangeActions().cacheGroupsToStart(this::requirePartitionLoad)
            .stream()
            .map(g -> g.descriptor().groupId())
            .collect(Collectors.toSet());
    }

    /**
     * @param futs Collection of futures to chain.
     * @param <T> Result type.
     * @return Completable future waits for all of.
     */
    private static <T extends CompletableFuture<?>> CompletableFuture<Void> allOfFailFast(Collection<T> futs) {
        CompletableFuture<?>[] out = new CompletableFuture[futs.size()];
        CompletableFuture<Void> result = CompletableFuture.allOf(futs.toArray(out));

        // This is a mix of allOf() and anyOf() where the returned future completes normally as soon as all the elements
        // complete normally, or it completes exceptionally as soon as any of the elements complete exceptionally.
        Stream.of(out)
            .forEach(f -> f.exceptionally(e -> {
                result.completeExceptionally(e);

                return null;
            }));

        return result;
    }

    /**
     * @param first Ignite internal future.
     * @param second Completable future to chain.
     */
    private static void chain(@Nullable IgniteInternalFuture<?> first, CompletableFuture<?> second) {
        if (first == null)
            first = new GridFinishedFuture<>();

        first.listen(f -> {
            if (f.error() == null)
                second.complete(null);
            else
                second.completeExceptionally(f.error());
        });
    }

    /**
     * @param ctx Grid kernal context.
     * @param ctxs Cache contexts related to cache group.
     * @param cancelTok Cancellation token.
     * @param comFut Resolver for cache group future.
     */
    private void scheduleIndexRebuild(
        GridKernalContext ctx,
        Collection<GridCacheContext> ctxs,
        AtomicReference<Throwable> cancelTok,
        Function<Integer, CompletableFuture<Void>> comFut
    ) {
        Set<Integer> cacheIds = ctxs.stream().map(GridCacheContext::cacheId).collect(toSet());

        Set<Integer> rejected = ctx.query().prepareRebuildIndexes(cacheIds);

        assert F.isEmpty(rejected) : rejected;

        for (GridCacheContext<?, ?> cacheCtx : ctxs) {
            assert ctx.query().rebuildIndexesCompleted(cacheCtx) : cacheCtx;

            chain(ctx.query().rebuildIndexesFromHash(cacheCtx,
                    true,
                    new IndexRebuildCancelToken(cancelTok)),
                comFut.apply(cacheCtx.cacheId()));
        }
    }

    /**
     * Cache group restore from snapshot operation context.
     */
    private static class SnapshotRestoreContext {
        /** Request ID. */
        private final UUID reqId;

        /** Snapshot name. */
        private final String snpName;

        /** Baseline node IDs that must be alive to complete the operation. */
        private final Set<UUID> nodes;

        /** Operational node id. */
        private final UUID opNodeId;

        /** List of restored cache group directories. */
        private final Collection<File> dirs;

        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** Future which will be completed when cache started and preloaded. */
        private final GridFutureAdapter<Boolean> cacheRebalanceFut = new GridFutureAdapter<>();

        /** Distribution of snapshot metadata files across the cluster. */
        private final Map<UUID, ArrayList<SnapshotMetadata>> metasPerNode = new HashMap<>();

        /** Context error handler. */
        private final Consumer<Throwable> errHnd = (ex) -> err.compareAndSet(null, ex);

        /** Stop condition checker. */
        private final BooleanSupplier stopChecker = () -> err.get() != null;

        /** Progress of processing cache group partitions on the local node.*/
        private final Map<CacheRestoreLifecycleFuture, Set<PartitionRestoreLifecycleFuture>> locProgress = new HashMap<>();

        /** Cache ID to configuration mapping. */
        private volatile Map<Integer, StoredCacheData> cfgs;

        // TODO check the stopFut setting in case of some operation require cancellation performs.
        /** Graceful shutdown future. */
        private volatile IgniteFuture<?> stopFut;

        /** {@code true} if restore procedure can be performed on the same cluster topology with copying indexes. */
        private volatile boolean sameTop;

        /**
         * @param req Request to prepare cache group restore from the snapshot.
         * @param dirs List of cache group names to restore from the snapshot.
         * @param cfgs Cache ID to configuration mapping.
         */
        protected SnapshotRestoreContext(
            SnapshotOperationRequest req,
            Collection<File> dirs,
            Map<Integer, StoredCacheData> cfgs,
            UUID locNodeId,
            List<SnapshotMetadata> locMetas
        ) {
            reqId = req.requestId();
            snpName = req.snapshotName();
            opNodeId = req.operationalNodeId();
            nodes = new HashSet<>(req.nodes());

            this.dirs = dirs;
            this.cfgs = cfgs;

            metasPerNode.computeIfAbsent(locNodeId, id -> new ArrayList<>())
                .addAll(locMetas);
        }
    }

    /** Snapshot operation prepare response. */
    private static class SnapshotRestoreOperationResponse implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Cache configurations on local node. */
        private ArrayList<StoredCacheData> ccfgs;

        /** Snapshot metadata files on local node. */
        private ArrayList<SnapshotMetadata> metas;

        /**
         * @param ccfgs Cache configurations on local node.
         * @param metas Snapshot metadata files on local node.
         */
        public SnapshotRestoreOperationResponse(
            Collection<StoredCacheData> ccfgs,
            Collection<SnapshotMetadata> metas
        ) {
            this.ccfgs = new ArrayList<>(ccfgs);
            this.metas = new ArrayList<>(metas);
        }
    }

    /** */
    private static class CacheRestoreLifecycleFuture extends CompletableFuture<Void> implements PartitionsExchangeAware {
        /** Cache group context. */
        private final CacheGroupContext grp;

        /** Future which will be completed when all related to cache group partitions are inited. */
        private final CompletableFuture<Void> partsInited;

        /** Future which will be completed when late affinity assignment on this cache group occurs. */
        private final CompletableFuture<Void> rebalanced = new CompletableFuture<>();

        /** An index rebuild futures for each cache in given cache group. */
        private final CompletableFuture<Void> indexCacheGroupRebFut;

        /**
         * @param grp Cache group context.
         * @param partsInited Original future to listen to.
         */
        private CacheRestoreLifecycleFuture(
            CacheGroupContext grp,
            CompletableFuture<Void> partsInited,
            CompletableFuture<Void> indexCacheGroupRebFut
        ) {
            this.grp = grp;
            this.partsInited = partsInited;
            this.indexCacheGroupRebFut = indexCacheGroupRebFut;
        }

        /**
         * @param grp Cache group context.
         * @param partsInited Future which completes when partitions are inited.
         * @param resendAct Action to do cache group initialization.
         * @return Future which will be completed when cache group processing ends.
         */
        public static CacheRestoreLifecycleFuture create(
            CacheGroupContext grp,
            CompletableFuture<Void> partsInited,
            CompletableFuture<Void> indexCacheGroupRebFut,
            Runnable resendAct
        ) {
            assert !grp.isLocal();
            assert grp.shared().database() instanceof GridCacheDatabaseSharedManager;
            assert grp.topology() instanceof GridDhtPartitionTopologyImpl;

            CacheRestoreLifecycleFuture cl = new CacheRestoreLifecycleFuture(grp, partsInited, indexCacheGroupRebFut);

            grp.shared().exchange().registerExchangeAwareComponent(cl);

            partsInited.thenRun(resendAct);

            allOfFailFast(Arrays.asList(partsInited, cl.rebalanced, indexCacheGroupRebFut))
                .whenComplete((r, t) -> {
                   if (t == null)
                       cl.complete(r);
                   else
                       cl.completeExceptionally(t);

                    grp.shared().exchange().unregisterExchangeAwareComponent(cl);
                });

            return cl;
        }

        /** {@inheritDoc} */
        @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
            // Handle for the late affinity assignment message on partitions load complete.
            assert partsInited.isDone();

            CacheAffinityChangeMessage msg = fut.affinityChangeMessage();

            if (msg == null || F.isEmpty(msg.cacheDeploymentIds()))
                return;

            IgniteUuid deploymentId = msg.cacheDeploymentIds().get(grp.groupId());
            CacheGroupDescriptor desc = grp.shared().affinity().cacheGroups().get(grp.groupId());

            if (deploymentId == null || desc == null)
                return;

            if (deploymentId.equals(desc.deploymentId())) {
                if (fut.rebalanced())
                    rebalanced.complete(null);
                else
                    rebalanced.completeExceptionally(new IgniteException("Unable to complete late affinity assignment switch: " +
                        grp.groupId()));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            CacheRestoreLifecycleFuture f = (CacheRestoreLifecycleFuture)o;

            return grp.groupId() == f.grp.groupId();
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(grp.groupId());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CacheRestoreLifecycleFuture{" +
                "grp=" + grp +
                ",\n inited=" + partsInited +
                ",\n rebalanced=" + rebalanced +
                ",\n indexRebuild=" + indexCacheGroupRebFut +
                ",\n super=" + super.toString() +
                '}';
        }
    }

    /** Future will be completed when partition processing ends. */
    private static class PartitionRestoreLifecycleFuture extends CompletableFuture<Void> implements CheckpointListener {
        /** Cache group context related to the partition. */
        private final CacheGroupContext grp;

        /** Partition id. */
        private final int partId;

        /** Future will be finished when the partition eviction process ends. */
        private final CompletableFuture<Void> cleared;

        /** Future will be finished when the partition preloading ends. */
        private final CompletableFuture<Path> loaded;

        /** Future will be finished when the partition initialized under checkpoint thread. */
        private final CompletableFuture<Void> inited;

        /** PageMemory will be invalidated and PageStore will be truncated with this tag. */
        private final AtomicReference<Integer> truncatedTag = new AtomicReference<>();

        /** Partition high watermark counter to ensure the absence of update on partition being switching. */
        private final AtomicReference<Long> partHwm = new AtomicReference<>();

        /**
         * @param grp Cache group context related to the partition.
         * @param partId Partition id.
         * @param cleared Future will be finished when the partition eviction process ends.
         * @param loaded Future will be finished when the partition preloading ends.
         * @param inited Future will be finished when the partition initialized under checkpoint thread.
         */
        private PartitionRestoreLifecycleFuture(
            CacheGroupContext grp,
            int partId,
            CompletableFuture<Void> cleared,
            CompletableFuture<Path> loaded,
            CompletableFuture<Void> inited
        ) {
            this.grp = grp;
            this.partId = partId;
            this.cleared = cleared;
            this.loaded = loaded;
            this.inited = inited;
        }

        /**
         * @param grp Cache group context related to the partition.
         * @param partId Partition id.
         * @return A future which will be completed when partition processing ends (partition is loaded and initialized).
         */
        public static PartitionRestoreLifecycleFuture create(CacheGroupContext grp, IgniteLogger log, int partId) {
            assert !grp.isLocal();
            assert grp.shared().database() instanceof GridCacheDatabaseSharedManager;
            assert grp.topology() instanceof GridDhtPartitionTopologyImpl;

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)grp.shared().database();

            CompletableFuture<Void> cleared = new CompletableFuture<>();
            CompletableFuture<Path> loaded = new CompletableFuture<>();
            CompletableFuture<Void> inited = new CompletableFuture<>();

            PartitionRestoreLifecycleFuture pf = new PartitionRestoreLifecycleFuture(grp, partId, cleared, loaded, inited);

            // Only when the old partition eviction completes and the new partition file loaded
            // we should start a new data storage initialization. This will not be fired if any of
            // dependent futures completes with an exception.
            loaded.thenAcceptBothAsync(cleared, (path, ignore) -> db.addCheckpointListener(pf));
            inited.thenRun(() -> db.removeCheckpointListener(pf))
                .whenComplete((r, t) -> {
                    if (t == null && log.isDebugEnabled())
                        log.debug("Partition has been initialized successfully on cache restore [grp=" + grp.cacheOrGroupName() +
                            ", partId=" + partId + ", hwm=" + pf.partHwm.get() + ']');
                });

            CompletableFuture
                .allOf(cleared, loaded, inited)
                .whenComplete((r, t) -> {
                    if (t == null)
                        pf.complete(r);
                    else
                        pf.completeExceptionally(t);
                });

            return pf;
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            handleException(() -> {
                assert loaded.isDone();
                assert cleared.isDone();

                // Affinity node partitions are inited on exchange.
                GridDhtLocalPartition part = grp.topology().localPartition(partId);
                PageMemoryEx pageMemory = (PageMemoryEx)grp.dataRegion().pageMemory();

                // We must provide additional guarantees for the absence of PageMemory updates and new GridCacheMapEntry heap
                // entries creation in partition map until current lifecycle ends. There are few options here to do this:
                //
                // 1. The EVICTED status of partitions guarantee us that there are no updates on it. As opposed to the MOVING
                //    partitions they still have new entries to be added (e.g. the rebalance process).
                // 2. For the file rebalance procedure (IEP-28) such guarantees may be achieved by creating a dedicated
                //    temporary WAL to forward updates to, so new updates will not affect PageMemory.
                // 3. The snapshot restore guarantee the absence of updates by disabling cache proxies on snapshot restore,
                //    so these caches will not be available for users to operate.
                assert part != null : "Partition must be initialized prior to swapping cache data store: " + partId;
                assert part.state() == GridDhtPartitionState.MOVING : "Only MOVING partitions allowed: " + part.state();
                assert part.internalSize() == 0 : "Partition map must clear all heap entries prior to invalidation: " + partId;

                boolean success0 = partHwm.compareAndSet(null, part.reservedCounter());
                assert success0 : partId;

                int tag = pageMemory.invalidate(grp.groupId(), partId);

                grp.shared().pageStore().truncate(grp.groupId(), partId, tag);

                boolean success = truncatedTag.compareAndSet(null, tag);
                assert success : partId;

                return null;
            }, inited::completeExceptionally);
        }

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            // TODO Should we clean the 'dirty' flag here, so pages won't be collected by the checkpoint?
            // TODO cancelOrWaitPartitionDestroy should we wait for partition destroying?
            // TODO prevent page store initialization on write if tag has been incremented.

            if (inited.isCompletedExceptionally())
                return;

            // We are swapping cache data stores for partition. Some of them may be processed in another listeners of
            // the checkpoint thread.
            handleException(() -> {
                GridDhtLocalPartition prevPart = grp.topology().localPartition(partId);

                assert prevPart.internalSize() == 0 : "Partition map must clear all heap entries prior to invalidation: " + partId;
                assert prevPart.reservations() == 0 : "Partition must have no reservations prior to data store swap: " + partId;
                assert partHwm.get().equals(prevPart.reservedCounter()) :
                    "Partition counter changed due to some of updates occurred [prev=" + partHwm.get() +
                        ", new=" + prevPart.updateCounter() + ']';

                prevPart.dataStore().markDestroyed();

                // Dirty pages will be collected further under checkpoint write-lock and won't be flushed to the disk
                // due to the 'tag' is used.
                PageStore prevStore = grp.shared().pageStore().recreate(grp.groupId(), partId, truncatedTag.get(), loaded.get());

                boolean exists = prevStore.exists();

                assert !exists : prevStore;

                GridDhtLocalPartition part = ((GridDhtPartitionTopologyImpl)grp.topology()).doForcePartitionCreate(partId,
                    (s) -> s != GridDhtPartitionState.MOVING,
                    (prevState, newPart) -> {
                        assert prevState == GridDhtPartitionState.MOVING : "Previous partition must has MOVING state.";
                        assert newPart.reservations() == 0;
                        assert newPart.internalSize() == 0;
                    });

                assert !((GridCacheOffheapManager.GridCacheDataStore)part.dataStore()).inited() :
                    "Swapped datastore must not be initialized under the checkpoint write lock: " + partId;

                return null;
            }, inited::completeExceptionally);

            // There are still some dirty pages related to processing partition available in the PageMemory.

            // Loaded:
            // - need to set MOVING states to loading partitions.
            // - need to acquire partition counters from each part

            // The process of re-init options:
            // 1. Clear heap entries from GridDhtLocalPartition, swap datastore, re-init counters
            // 2. Re-create the whole GridDhtLocalPartition from scratch in GridDhtPartitionTopologyImpl
            // as it the eviction does

            // The re-init notes:
            // - clearAsync() should move the clearVer in clearAll() to the end.
            // - How to handle updates on partition prior to the storage switch? (the same as waitPartitionRelease()?)
            // - destroyCacheDataStore() calls and removes a data store from partDataStores under lock.
            // - CacheDataStore markDestroyed() may be called prior to checkpoint?
            // - Does new pages on acquirePage will be read from new page store after tag has been incremented?
            // - invalidate() returns a new tag -> no updates will be written to page store.
            // - Check GridDhtLocalPartition.isEmpty and all heap rows are cleared
            // - Do we need to call ClearSegmentRunnable with predicate to clear outdated pages?
            // - getOrCreatePartition() resets also partition counters of new partitions can be updated
            // only on cp-write-lock (GridDhtLocalPartition ?).
            // - update the cntrMap in the GridDhtTopology prior to partition creation
            // - WAL logged PartitionMetaStateRecord on GridDhtLocalPartition creation. Do we need it for re-init?
            // - check there is no reservations on MOVING partition during the switch procedure
            // - we can applyUpdateCounters() from exchange thread on coordinator to sync cntrMap and
            // locParts in GridDhtPartitionTopologyImpl
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            if (inited.isCompletedExceptionally())
                return;

            GridDhtLocalPartition part = grp.topology().localPartition(partId);
            part.dataStore().init();

            partHwm.set(part.reservedCounter());

            inited.complete(null);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PartitionRestoreLifecycleFuture lifecycle = (PartitionRestoreLifecycleFuture)o;

            return grp.groupId() == lifecycle.grp.groupId() && partId == lifecycle.partId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(grp.groupId(), partId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PartitionRestoreLifecycle{" +
                "\n, grpName=" + grp.cacheOrGroupName() +
                "\n, partId=" + partId +
                "\n, cleared=" + cleared +
                "\n, loaded=" + loaded +
                "\n, inited=" + inited +
                "\n, truncatedTag=" + truncatedTag +
                "\n, super=" + super.toString() +
                '}';
        }
    }
}
