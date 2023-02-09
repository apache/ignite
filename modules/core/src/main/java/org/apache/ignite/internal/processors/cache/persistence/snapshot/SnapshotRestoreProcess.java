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
import java.nio.ByteBuffer;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheStripedExecutor;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.ClusterSnapshotFuture;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.IgniteFeatures.SNAPSHOT_RESTORE_CACHE_GROUP;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_START_RECORD;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_STOP;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_INCREMENTAL_SNAPSHOT_START;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreProcess {
    /** Temporary cache directory prefix. */
    public static final String TMP_CACHE_DIR_PREFIX = "_tmp_snp_restore_";

    /** Snapshot restore metrics prefix. */
    public static final String SNAPSHOT_RESTORE_METRICS = "snapshot-restore";

    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Snapshot restore operation finish message. */
    private static final String OP_FINISHED_MSG = "Cache groups have been successfully restored from the snapshot";

    /** Snapshot restore operation failed message. */
    private static final String OP_FAILED_MSG = "Failed to restore snapshot cache groups";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotRestoreOperationResponse> prepareRestoreProc;

    /** Cache group restore preload partitions phase. */
    private final DistributedProcess<UUID, Boolean> preloadProc;

    /** Cache group restore cache start phase. */
    private final DistributedProcess<UUID, Boolean> cacheStartProc;

    /** Cache group restore cache stop phase. */
    private final DistributedProcess<UUID, Boolean> cacheStopProc;

    /** Incremental snapshot restore phase. */
    private final DistributedProcess<UUID, Boolean> incSnpRestoreProc;

    /** Cache group restore rollback phase. */
    private final DistributedProcess<UUID, Boolean> rollbackRestoreProc;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final ThreadLocal<ByteBuffer> locBuff;

    /** Future to be completed when the cache restore process is complete (this future will be returned to the user). */
    private volatile ClusterSnapshotFuture fut;

    /** Current snapshot restore operation context (will be {@code null} when the operation is not running). */
    private volatile SnapshotRestoreContext opCtx;

    /** Last snapshot restore operation context (saves the metrics of the last operation). */
    private volatile SnapshotRestoreContext lastOpCtx = new SnapshotRestoreContext();

    /**
     * @param ctx Kernal context.
     * @param locBuff Thread local page buffer.
     */
    public SnapshotRestoreProcess(GridKernalContext ctx, ThreadLocal<ByteBuffer> locBuff) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        this.locBuff = locBuff;

        prepareRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, this::prepare, this::finishPrepare);

        preloadProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD, this::preload, this::finishPreload);

        cacheStartProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_START, this::cacheStart, this::finishCacheStart);

        cacheStopProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_STOP, this::cacheStop, this::finishCacheStop);

        incSnpRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_INCREMENTAL_SNAPSHOT_START, this::incrementalSnapshotRestore, this::finishIncrementalSnapshotRestore);

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

        for (File dir : dbDir.listFiles(dir -> dir.isDirectory() && dir.getName().startsWith(TMP_CACHE_DIR_PREFIX))) {
            if (!U.delete(dir)) {
                throw new IgniteCheckedException("Unable to remove temporary directory, " +
                    "try deleting it manually [dir=" + dir + ']');
            }
        }
    }

    /**
     * Register local metrics.
     */
    protected void registerMetrics() {
        assert !ctx.clientNode();

        MetricRegistry mreg = ctx.metric().registry(SNAPSHOT_RESTORE_METRICS);

        mreg.register("startTime", () -> lastOpCtx.startTime,
            "The system time of the start of the cluster snapshot restore operation on this node.");
        mreg.register("endTime", () -> lastOpCtx.endTime,
            "The system time when the restore operation of a cluster snapshot on this node ended.");
        mreg.register("snapshotName", () -> lastOpCtx.snpName, String.class,
            "The snapshot name of the last running cluster snapshot restore operation on this node.");
        mreg.register("requestId", () -> Optional.ofNullable(lastOpCtx.reqId).map(UUID::toString).orElse(""),
            String.class, "The request ID of the last running cluster snapshot restore operation on this node.");
        mreg.register("error", () -> Optional.ofNullable(lastOpCtx.err.get()).map(Throwable::toString).orElse(""),
            String.class, "Error message of the last running cluster snapshot restore operation on this node.");
        mreg.register("totalPartitions", () -> lastOpCtx.totalParts,
            "The total number of partitions to be restored on this node.");
        mreg.register("processedPartitions", () -> lastOpCtx.processedParts.get(),
            "The number of processed partitions on this node.");
    }

    /**
     * Start cache group restore operation.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param cacheGrpNames Cache groups to be restored or {@code null} to restore all cache groups from the snapshot.
     * @param incIdx Index of incremental snapshot.
     * @return Future that will be completed when the restore operation is complete and the cache groups are started.
     */
    public IgniteFutureImpl<Void> start(String snpName, @Nullable String snpPath, @Nullable Collection<String> cacheGrpNames, int incIdx) {
        IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();
        ClusterSnapshotFuture fut0;

        try {
            if (ctx.clientNode())
                throw new IgniteException(OP_REJECT_MSG + "Client nodes can not perform this operation.");

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
            (cacheGrpNames == null ? "" : ", caches=" + cacheGrpNames) + (incIdx > 0 ? ", incIdx=" + incIdx : "") + ']';

        if (log.isInfoEnabled())
            log.info(msg);

        snpMgr.recordSnapshotEvent(snpName, msg, EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED);

        snpMgr.checkSnapshot(snpName, snpPath, cacheGrpNames, true, incIdx).listen(f -> {
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
                dataNodes.add(entry.getKey().id());

                for (SnapshotMetadata meta : entry.getValue()) {
                    assert meta != null : entry.getKey().id();

                    if (snpBltNodes == null)
                        snpBltNodes = new HashSet<>(meta.baselineNodes());

                    reqGrpIds.keySet().removeAll(meta.partitions().keySet());
                }
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

            Collection<UUID> bltNodes = F.viewReadOnly(ctx.discovery().discoCache().aliveBaselineNodes(), F.node2id());

            SnapshotOperationRequest req = new SnapshotOperationRequest(
                fut0.rqId,
                F.first(dataNodes),
                snpName,
                snpPath,
                cacheGrpNames,
                new HashSet<>(bltNodes),
                false,
                incIdx
            );

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

        return new HashSet<>(opCtx0.nodes());
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

        if (opCtx0 != null && reqId.equals(opCtx0.reqId)) {
            opCtx = null;

            opCtx0.endTime = U.currentTimeMillis();
        }

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

        if (opCtx0 != null && opCtx0.nodes().contains(leftNodeId)) {
            opCtx0.err.compareAndSet(null, new ClusterTopologyCheckedException(OP_REJECT_MSG +
                "Required node has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Cancel the currently running local restore procedure.
     *
     * @param reqId Snapshot operation request ID.
     * @param snpName Snapshot name.
     * @return Future that will be finished when process the process is complete. The result of this future will be
     * {@code false} if the restore process with the specified snapshot name is not running at all.
     */
    public IgniteFuture<Boolean> cancel(UUID reqId, @Deprecated String snpName) {
        assert (reqId == null && snpName != null) || (reqId != null && snpName == null);

        IgniteCheckedException reason = new IgniteCheckedException("Operation has been canceled by the user.");
        SnapshotRestoreContext opCtx0;
        ClusterSnapshotFuture fut0 = null;

        synchronized (this) {
            opCtx0 = opCtx;

            if (fut != null && (fut.rqId.equals(reqId) || fut.name.equals(snpName))) {
                fut0 = fut;

                fut0.interruptEx = reason;
            }
        }

        boolean ctxStop = opCtx0 != null && (opCtx0.reqId.equals(reqId) || opCtx0.snpName.equals(snpName));

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

            if (ctx.encryption().isMasterKeyChangeInProgress()) {
                return new GridFinishedFuture<>(new IgniteCheckedException(OP_REJECT_MSG + "Master key changing " +
                    "process is not finished yet."));
            }

            if (ctx.encryption().reencryptionInProgress()) {
                return new GridFinishedFuture<>(new IgniteCheckedException(OP_REJECT_MSG + "Caches re-encryption " +
                    "process is not finished yet."));
            }

            for (UUID nodeId : req.nodes()) {
                ClusterNode node = ctx.discovery().node(nodeId);

                if (node == null || !CU.baselineNode(node, state) || !ctx.discovery().alive(node)) {
                    throw new IgniteCheckedException(
                        OP_REJECT_MSG + "Required node has left the cluster [nodeId-" + nodeId + ']');
                }
            }

            if (log.isInfoEnabled()) {
                log.info("Starting local snapshot prepare restore operation" +
                    " [reqId=" + req.requestId() +
                    ", snapshot=" + req.snapshotName() +
                    ", caches=" + req.groups() + ']');
            }

            List<SnapshotMetadata> locMetas = snpMgr.readSnapshotMetadatas(req.snapshotName(), req.snapshotPath());

            SnapshotRestoreContext opCtx0 = prepareContext(req, locMetas);

            synchronized (this) {
                lastOpCtx = opCtx = opCtx0;

                ClusterSnapshotFuture fut0 = fut;

                if (fut0 != null)
                    opCtx0.errHnd.accept(fut0.interruptEx);
            }

            // Ensure that shared cache groups has no conflicts.
            for (StoredCacheData cfg : opCtx0.cfgs.values()) {
                ensureCacheAbsent(cfg.config().getName());

                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getGroupName());
            }

            if (ctx.isStopping())
                throw new NodeStoppingException("The node is stopping: " + ctx.localNodeId());

            return new GridFinishedFuture<>(new SnapshotRestoreOperationResponse(opCtx0.cfgs.values(), locMetas));
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
    static File formatTmpDirName(File cacheDir) {
        return new File(cacheDir.getParent(), TMP_CACHE_DIR_PREFIX + cacheDir.getName());
    }

    /**
     * @param tmpCacheDir Temporary cache directory.
     * @return Cache or group id.
     */
    static int groupIdFromTmpDir(File tmpCacheDir) {
        assert tmpCacheDir.getName().startsWith(TMP_CACHE_DIR_PREFIX) : tmpCacheDir;

        String cacheGrpName = tmpCacheDir.getName().substring(TMP_CACHE_DIR_PREFIX.length());

        return CU.cacheId(cacheGroupName(new File(tmpCacheDir.getParentFile(), cacheGrpName)));
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @param metas Local snapshot metadatas.
     * @return Snapshot restore operation context.
     * @throws IgniteCheckedException If failed.
     */
    private SnapshotRestoreContext prepareContext(
        SnapshotOperationRequest req,
        Collection<SnapshotMetadata> metas
    ) throws IgniteCheckedException {
        if (opCtx != null) {
            throw new IgniteCheckedException(OP_REJECT_MSG +
                "The previous snapshot restore operation was not completed.");
        }
        GridCacheSharedContext<?, ?> cctx = ctx.cache().context();

        // Collection of baseline nodes that must survive and additional discovery data required for the affinity calculation.
        DiscoCache discoCache = ctx.discovery().discoCache();

        if (!F.transform(discoCache.aliveBaselineNodes(), F.node2id()).containsAll(req.nodes()))
            throw new IgniteCheckedException("Restore context cannot be inited since the required baseline nodes missed: " + discoCache);

        DiscoCache discoCache0 = discoCache.copy(discoCache.version(), null);

        if (F.isEmpty(metas))
            return new SnapshotRestoreContext(req, discoCache0, Collections.emptyMap());

        if (F.first(metas).pageSize() != cctx.database().pageSize()) {
            throw new IgniteCheckedException("Incompatible memory page size " +
                "[snapshotPageSize=" + F.first(metas).pageSize() +
                ", local=" + cctx.database().pageSize() +
                ", snapshot=" + req.snapshotName() +
                ", nodeId=" + cctx.localNodeId() + ']');
        }

        Map<String, StoredCacheData> cfgsByName = new HashMap<>();
        FilePageStoreManager pageStore = (FilePageStoreManager)cctx.pageStore();
        GridLocalConfigManager locCfgMgr = cctx.cache().configManager();

        // Collect the cache configurations and prepare a temporary directory for copying files.
        // Metastorage can be restored only manually by directly copying files.
        boolean skipCompressCheck = false;
        for (SnapshotMetadata meta : metas) {
            for (File snpCacheDir : cctx.snapshotMgr().snapshotCacheDirectories(req.snapshotName(), req.snapshotPath(), meta.folderName(),
                name -> !METASTORAGE_CACHE_NAME.equals(name))) {
                String grpName = FilePageStoreManager.cacheGroupName(snpCacheDir);

                if (!F.isEmpty(req.groups()) && !req.groups().contains(grpName))
                    continue;

                if (!skipCompressCheck && meta.isGroupWithCompresion(CU.cacheId(grpName))) {
                    try {
                        File path = ctx.pdsFolderResolver().resolveFolders().persistentStoreRootPath();

                        ctx.compress().checkPageCompressionSupported(path.toPath(), meta.pageSize());
                    }
                    catch (Exception e) {
                        String grpWithCompr = req.groups().stream().filter(s -> meta.isGroupWithCompresion(CU.cacheId(grpName)))
                            .collect(Collectors.joining(", "));

                        String msg = "Requested cache groups [" + grpWithCompr + "] for restore " +
                            "from snapshot '" + meta.snapshotName() + "' are compressed while " +
                            "disk page compression is disabled. To restore these groups please " +
                            "start Ignite with configured disk page compression";

                        throw new IgniteCheckedException(msg);
                    }
                    finally {
                        skipCompressCheck = true;
                    }
                }

                File cacheDir = pageStore.cacheWorkDir(snpCacheDir.getName().startsWith(CACHE_GRP_DIR_PREFIX), grpName);

                if (cacheDir.exists()) {
                    if (!cacheDir.isDirectory()) {
                        throw new IgniteCheckedException("Unable to restore cache group, file with required directory " +
                            "name already exists [group=" + grpName + ", file=" + cacheDir + ']');
                    }

                    if (cacheDir.list().length > 0) {
                        throw new IgniteCheckedException("Unable to restore cache group - directory is not empty. " +
                            "Cache group should be destroyed manually before perform restore operation " +
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

                locCfgMgr.readCacheConfigurations(snpCacheDir, cfgsByName);
            }
        }

        Map<Integer, StoredCacheData> cfgsById =
            cfgsByName.values().stream().collect(Collectors.toMap(v -> CU.cacheId(v.config().getName()), v -> v));

        return new SnapshotRestoreContext(req, discoCache0, cfgsById);
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
            failure = checkNodeLeft(opCtx0.nodes(), res.keySet());

        // Context has been created - should rollback changes cluster-wide.
        if (failure != null) {
            opCtx0.errHnd.accept(failure);

            return;
        }

        Map<Integer, StoredCacheData> globalCfgs = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestoreOperationResponse> e : res.entrySet()) {
            if (e.getValue().ccfgs != null) {
                for (StoredCacheData cacheData : e.getValue().ccfgs) {
                    globalCfgs.put(CU.cacheId(cacheData.config().getName()), cacheData);

                    opCtx0.dirs.add(((FilePageStoreManager)ctx.cache().context().pageStore()).cacheWorkDir(cacheData.config()));
                }
            }

            if (!F.isEmpty(e.getValue().metas)) {
                e.getValue().metas.stream().filter(SnapshotMetadata::hasCompressedGroups)
                    .forEach(meta -> meta.cacheGroupIds().stream().filter(meta::isGroupWithCompresion)
                        .forEach(opCtx0::addCompressedGroup));
            }

            opCtx0.metasPerNode.put(e.getKey(), new ArrayList<>(e.getValue().metas));
        }

        opCtx0.cfgs = globalCfgs;

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            preloadProc.start(reqId, reqId);
    }

    /**
     * @param ccfg Cache configuration.
     * @param cache Discovery cache.
     * @return Map of affinity per each cache group.
     */
    private static GridAffinityAssignmentCache calculateAffinity(
        GridKernalContext ctx,
        CacheConfiguration<?, ?> ccfg,
        DiscoCache cache
    ) {
        GridAffinityAssignmentCache affCache = GridAffinityAssignmentCache.create(ctx, ccfg.getAffinity(), ccfg);

        affCache.calculate(cache.version(), null, cache);

        return affCache;
    }

    /**
     * @param metas List of snapshot metadata to check.
     * @param grpId Group id.
     * @param parts Set of partitions to search for.
     * @return Snapshot metadata which contains a full set of given partitions or {@code null} the otherwise.
     */
    private static @Nullable SnapshotMetadata findMetadataWithSamePartitions(
        List<SnapshotMetadata> metas,
        int grpId,
        Set<Integer> parts
    ) {
        assert !F.isEmpty(parts) && !parts.contains(INDEX_PARTITION) : parts;

        // Try to copy everything right from the single snapshot part.
        for (SnapshotMetadata meta : metas) {
            Set<Integer> grpParts = meta.partitions().get(grpId);
            Set<Integer> grpWoIndex = grpParts == null ? Collections.emptySet() : new HashSet<>(grpParts);

            grpWoIndex.remove(INDEX_PARTITION);

            if (grpWoIndex.equals(parts))
                return meta;
        }

        return null;
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

        if (opCtx0.dirs.isEmpty())
            return new GridFinishedFuture<>();

        try {
            if (ctx.isStopping())
                throw new NodeStoppingException("Node is stopping: " + ctx.localNodeId());

            Set<SnapshotMetadata> allMetas =
                opCtx0.metasPerNode.values().stream().flatMap(List::stream).collect(Collectors.toSet());

            AbstractSnapshotVerificationTask.checkMissedMetadata(allMetas);

            IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();

            synchronized (this) {
                opCtx0.stopFut = new IgniteFutureImpl<>(retFut.chain(f -> null));
            }

            if (log.isInfoEnabled()) {
                log.info("Starting snapshot preload operation to restore cache groups " +
                    "[reqId=" + reqId +
                    ", snapshot=" + opCtx0.snpName +
                    ", caches=" + F.transform(opCtx0.dirs, FilePageStoreManager::cacheGroupName) + ']');
            }

            File snpDir = snpMgr.snapshotLocalDir(opCtx0.snpName, opCtx0.snpPath);

            CompletableFuture<Void> metaFut = ctx.localNodeId().equals(opCtx0.opNodeId) ?
                CompletableFuture.runAsync(
                    () -> {
                        try {
                            SnapshotMetadata meta = F.first(opCtx0.metasPerNode.get(opCtx0.opNodeId));

                            File binDir = binaryWorkDir(snpDir.getAbsolutePath(), meta.folderName());

                            ctx.cacheObjects().updateMetadata(binDir, opCtx0.stopChecker);
                        }
                        catch (Throwable t) {
                            log.error("Unable to perform metadata update operation for the cache groups restore process", t);

                            opCtx0.errHnd.accept(t);
                        }
                    }, snpMgr.snapshotExecutorService()) : CompletableFuture.completedFuture(null);

            Map<String, GridAffinityAssignmentCache> affCache = new HashMap<>();

            for (StoredCacheData data : opCtx0.cfgs.values()) {
                affCache.computeIfAbsent(CU.cacheOrGroupName(data.config()),
                    grp -> calculateAffinity(ctx, data.config(), opCtx0.discoCache));
            }

            Map<Integer, Set<PartitionRestoreFuture>> allParts = new HashMap<>();
            Map<Integer, Set<PartitionRestoreFuture>> rmtLoadParts = new HashMap<>();
            ClusterNode locNode = ctx.cache().context().localNode();
            List<SnapshotMetadata> locMetas = opCtx0.metasPerNode.get(locNode.id());

            // First preload everything from the local node.
            for (File dir : opCtx0.dirs) {
                String cacheOrGrpName = cacheGroupName(dir);
                int grpId = CU.cacheId(cacheOrGrpName);

                File tmpCacheDir = formatTmpDirName(dir);
                tmpCacheDir.mkdir();

                Set<PartitionRestoreFuture> leftParts;

                // Partitions contained in the snapshot.
                Set<Integer> availParts = new HashSet<>();

                for (SnapshotMetadata meta : allMetas) {
                    Set<Integer> parts = meta.partitions().get(grpId);

                    if (parts != null)
                        availParts.addAll(parts);
                }

                List<List<ClusterNode>> assignment = affCache.get(cacheOrGrpName).idealAssignment().assignment();

                Set<PartitionRestoreFuture> partFuts = availParts
                    .stream()
                    .filter(p -> p != INDEX_PARTITION && assignment.get(p).contains(locNode))
                    .map(p -> new PartitionRestoreFuture(p, opCtx0.processedParts))
                    .collect(Collectors.toSet());

                allParts.put(grpId, partFuts);
                rmtLoadParts.put(grpId, leftParts = new HashSet<>(partFuts));

                if (leftParts.isEmpty())
                    continue;

                SnapshotMetadata full = findMetadataWithSamePartitions(locMetas,
                    grpId,
                    leftParts.stream().map(p -> p.partId).collect(Collectors.toSet()));

                for (SnapshotMetadata meta : full == null ? locMetas : Collections.singleton(full)) {
                    if (leftParts.isEmpty())
                        break;

                    File snpCacheDir = new File(snpDir,
                        Paths.get(databaseRelativePath(meta.folderName()), dir.getName()).toString());

                    leftParts.removeIf(partFut -> {
                        boolean doCopy = ofNullable(meta.partitions().get(grpId))
                            .orElse(Collections.emptySet())
                            .contains(partFut.partId);

                        if (doCopy) {
                            copyLocalAsync(opCtx0, snpCacheDir, tmpCacheDir, partFut);
                        }

                        return doCopy;
                    });

                    if (meta == full) {
                        assert leftParts.isEmpty() : leftParts;

                        if (log.isInfoEnabled()) {
                            log.info("The snapshot was taken on the same cluster topology. The index will be copied to " +
                                "restoring cache group if necessary [reqId=" + reqId + ", snapshot=" + opCtx0.snpName +
                                ", dir=" + dir.getName() + ']');
                        }

                        File idxFile = new File(snpCacheDir, FilePageStoreManager.getPartitionFileName(INDEX_PARTITION));

                        if (idxFile.exists()) {
                            PartitionRestoreFuture idxFut;

                            allParts.computeIfAbsent(grpId, g -> new HashSet<>())
                                .add(idxFut = new PartitionRestoreFuture(INDEX_PARTITION, opCtx0.processedParts));

                            copyLocalAsync(opCtx0, snpCacheDir, tmpCacheDir, idxFut);
                        }
                    }
                }
            }

            // Load other partitions from remote nodes.
            List<PartitionRestoreFuture> rmtAwaitParts = rmtLoadParts.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            // This is necessary for sending only one partitions request per each cluster node.
            Map<UUID, Map<Integer, Set<Integer>>> snpAff = snapshotAffinity(
                opCtx0.metasPerNode.entrySet()
                    .stream()
                    .filter(e -> !e.getKey().equals(ctx.localNodeId()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                (grpId, partId) -> rmtLoadParts.get(grpId) != null &&
                    rmtLoadParts.get(grpId).remove(new PartitionRestoreFuture(partId, opCtx0.processedParts)));

            try {
                if (log.isInfoEnabled() && !snpAff.isEmpty()) {
                    log.info("Trying to request partitions from remote nodes " +
                        "[reqId=" + reqId +
                        ", snapshot=" + opCtx0.snpName +
                        ", map=" + snpAff.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            e -> partitionsMapToString(e.getValue()))) + ']');
                }

                for (Map.Entry<UUID, Map<Integer, Set<Integer>>> m : snpAff.entrySet()) {
                    ctx.cache().context().snapshotMgr()
                        .requestRemoteSnapshotFiles(m.getKey(),
                            opCtx0.reqId,
                            opCtx0.snpName,
                            opCtx0.snpPath,
                            m.getValue(),
                            opCtx0.stopChecker,
                            (snpFile, t) -> {
                                if (opCtx0.stopChecker.getAsBoolean())
                                    throw new IgniteInterruptedException("Snapshot remote operation request cancelled.");

                                if (t != null) {
                                    opCtx0.errHnd.accept(t);
                                    completeListExceptionally(rmtAwaitParts, t);

                                    return;
                                }

                                int grpId = groupIdFromTmpDir(snpFile.getParentFile());
                                int partId = partId(snpFile.getName());

                                PartitionRestoreFuture partFut = F.find(allParts.get(grpId), null,
                                    fut -> fut.partId == partId);

                                assert partFut != null : snpFile.getAbsolutePath();

                                if (!opCtx0.isGroupCompressed(grpId)) {
                                    partFut.complete(snpFile.toPath());

                                    return;
                                }

                                CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            punchHole(grpId, partId, snpFile);

                                            partFut.complete(snpFile.toPath());
                                        }
                                        catch (Throwable t0) {
                                            opCtx0.errHnd.accept(t0);

                                            completeListExceptionally(rmtAwaitParts, t0);
                                        }
                                    },
                                    snpMgr.snapshotExecutorService()
                                );
                            });
                }
            }
            catch (IgniteCheckedException e) {
                opCtx0.errHnd.accept(e);
                completeListExceptionally(rmtAwaitParts, e);
            }

            List<PartitionRestoreFuture> allPartFuts = allParts.values().stream().flatMap(Collection::stream)
                .collect(Collectors.toList());

            int size = allPartFuts.size();

            opCtx0.totalParts = size;

            CompletableFuture.allOf(allPartFuts.toArray(new CompletableFuture[size]))
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
                .whenComplete((res, t) -> {
                    Throwable t0 = ofNullable(opCtx0.err.get()).orElse(t);

                    if (t0 == null)
                        retFut.onDone(true);
                    else {
                        log.error("Unable to restore cache group(s) from a snapshot " +
                            "[reqId=" + opCtx.reqId + ", snapshot=" + opCtx.snpName + ']', t0);

                        retFut.onDone(t0);
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
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Exception failure = errs.values().stream().findFirst().
            orElse(checkNodeLeft(opCtx0.nodes(), res.keySet()));

        opCtx0.errHnd.accept(failure);

        if (failure != null) {
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
            return new GridFinishedFuture<>();

        Collection<StoredCacheData> ccfgs = opCtx0.cfgs.values();

        if (log.isInfoEnabled()) {
            log.info("Starting restored caches " +
                "[reqId=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName +
                ", caches=" + F.viewReadOnly(ccfgs, c -> c.config().getName()) + ']');
        }

        // We set the topology node IDs required to successfully start the cache, if any of the required nodes leave
        // the cluster during the cache startup, the whole procedure will be rolled back.
        return ctx.cache().dynamicStartCachesByStoredConf(ccfgs, true, true, false,
            IgniteUuid.fromUuid(reqId));
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
            orElse(checkNodeLeft(opCtx0.nodes(), res.keySet()));

        if (failure == null) {
            if (opCtx0.incIdx > 0) {
                if (U.isLocalNodeCoordinator(ctx.discovery()))
                    incSnpRestoreProc.start(reqId, reqId);

                return;
            }

            finishProcess(reqId, null);

            return;
        }

        opCtx0.err.compareAndSet(null, failure);

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, reqId);
    }

    /**
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> cacheStop(UUID reqId) {
        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;

        Collection<String> stopCaches = opCtx0.cfgs.values()
            .stream()
            .map(c -> c.config().getName())
            .collect(Collectors.toSet());

        if (log.isInfoEnabled())
            log.info("Stopping caches [reqId=" + opCtx0.reqId + ", snp=" + opCtx0.snpName + ", caches=" + stopCaches + ']');

        // Skip deleting cache files as they will be removed during rollback.
        return ctx.cache().dynamicDestroyCaches(stopCaches, false, false)
            .chain(fut -> {
                if (fut.error() != null)
                    throw F.wrap(fut.error());
                else
                    return true;
            });
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishCacheStop(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        if (!errs.isEmpty()) {
            SnapshotRestoreContext opCtx0 = opCtx;

            log.error("Failed to stop caches during a snapshot rollback routine " +
                "[reqId=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ", err=" + errs + ']');
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, reqId);
    }

    /**
     * Inits restoring incremental snapshot.
     *
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> incrementalSnapshotRestore(UUID reqId) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;

        if (log.isInfoEnabled()) {
            log.info("Starting incremental snapshot restore operation " +
                "[reqId=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ", incIdx=" + opCtx0.incIdx +
                ", caches=" + F.viewReadOnly(opCtx0.cfgs, c -> c.config().getName()) + ']');
        }

        GridFutureAdapter<Boolean> res = new GridFutureAdapter<>();

        ctx.pools().getSnapshotExecutorService().submit(() -> {
            try {
                Set<Integer> cacheIds = opCtx0.cfgs.keySet();

                walEnabled(false, cacheIds);

                restoreIncrementalSnapshot(opCtx0.snpName, opCtx0.snpPath, cacheIds, opCtx0.incIdx);

                walEnabled(true, cacheIds);

                CheckpointProgress cp = ctx.cache().context().database()
                    .forceNewCheckpoint("Incremental snapshot restored.", (fut) -> {
                        if (fut.error() != null)
                            res.onDone(fut.error());
                        else
                            res.onDone(true);
                    });

                if (cp == null)
                    res.onDone(new IgniteCheckedException("Node is stopping."));
            }
            catch (Throwable e) {
                res.onDone(e);
            }
        });

        return res;
    }

    /**
     * Restore incremental snapshot.
     *
     * @param snpName Base snapshot name.
     * @param snpPath Base snapshot path.
     * @param cacheIds Restoring cache IDs.
     * @param incIdx Index of incremental snapshot.
     */
    private void restoreIncrementalSnapshot(
        String snpName,
        String snpPath,
        Set<Integer> cacheIds,
        int incIdx
    ) throws IgniteCheckedException, IOException {
        File[] segments = walSegments(snpName, snpPath, incIdx);

        UUID incSnpId = ctx.cache().context().snapshotMgr()
            .readIncrementalSnapshotMetadata(snpName, snpPath, incIdx)
            .requestId();

        IncrementalSnapshotFinishRecord incSnpFinRec = readFinishRecord(segments[segments.length - 1], incSnpId);

        if (incSnpFinRec == null)
            throw new IgniteCheckedException("System WAL record for incremental snapshot wasn't found [id=" + incSnpId + ']');

        CacheStripedExecutor exec = new CacheStripedExecutor(ctx.pools().getStripedExecutorService());

        long start = U.currentTimeMillis();

        LongAdder applied = new LongAdder();

        Set<WALRecord.RecordType> recTypes = new HashSet<>(F.asList(
            CLUSTER_SNAPSHOT,
            INCREMENTAL_SNAPSHOT_START_RECORD,
            INCREMENTAL_SNAPSHOT_FINISH_RECORD,
            DATA_RECORD_V2));

        // Create a single WAL iterator for 2 steps: finding ClusterSnapshotRecord and applying incremental snapshots.
        // TODO: Fix it after resolving https://issues.apache.org/jira/browse/IGNITE-18718.
        try (WALIterator it = walIter(log, recTypes, segments)) {
            boolean snpRecReached = false;

            // Step 1. Skips applying WAL until base snapshot record has been reached.
            while (it.hasNext()) {
                WALRecord rec = it.next().getValue();

                if (rec.type() == CLUSTER_SNAPSHOT) {
                    if (((ClusterSnapshotRecord)rec).clusterSnapshotName().equals(snpName)) {
                        snpRecReached = true;

                        break;
                    }
                }
            }

            if (!snpRecReached)
                throw new IgniteCheckedException("System WAL record for full snapshot wasn't found [snpName=" + snpName + ']');

            UUID prevIncSnpId = incIdx > 1
                ? ctx.cache().context().snapshotMgr().readIncrementalSnapshotMetadata(snpName, snpPath, incIdx - 1).requestId()
                : null;

            IgnitePredicate<GridCacheVersion> txVerFilter = prevIncSnpId != null
                ? txVer -> true : txVer -> !incSnpFinRec.excluded().contains(txVer);

            // Step 2. Apply incremental snapshots.
            while (it.hasNext()) {
                WALRecord rec = it.next().getValue();

                if (rec.type() == INCREMENTAL_SNAPSHOT_START_RECORD) {
                    IncrementalSnapshotStartRecord startRec = (IncrementalSnapshotStartRecord)rec;

                    if (startRec.id().equals(incSnpFinRec.id()))
                        txVerFilter = v -> incSnpFinRec.included().contains(v);
                }
                else if (rec.type() == INCREMENTAL_SNAPSHOT_FINISH_RECORD) {
                    IncrementalSnapshotFinishRecord finRec = (IncrementalSnapshotFinishRecord)rec;

                    if (finRec.id().equals(prevIncSnpId))
                        txVerFilter = txVer -> !incSnpFinRec.excluded().contains(txVer);
                }
                else if (rec.type() == DATA_RECORD_V2) {
                    DataRecord data = (DataRecord)rec;

                    for (DataEntry e: data.writeEntries()) {
                        // That is OK to restore only part of transaction related to a specified cache group,
                        // because a full snapshot restoring does the same.
                        if (!cacheIds.contains(e.cacheId()) || !txVerFilter.apply(e.nearXidVersion()))
                            continue;

                        GridCacheContext<?, ?> cacheCtx = ctx.cache().context().cacheContext(e.cacheId());
                        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ctx.cache().context().database();

                        exec.submit(() -> {
                            try {
                                applyDataEntry(dbMgr, cacheCtx, e);

                                applied.increment();
                            }
                            catch (IgniteCheckedException err) {
                                U.error(log, "Failed to apply data entry, dataEntry=" + e + ", ptr=" + data.position());

                                exec.onError(err);
                            }
                        }, cacheCtx.groupId(), e.partitionId());
                    }
                }
            }
        }

        exec.awaitApplyComplete();

        // Close partition counter gaps that can exists due to some transactions excluded from incremental snapshot.
        for (int cacheId: cacheIds) {
            GridCacheContext<?, ?> cacheCtx = ctx.cache().context().cacheContext(cacheId);

            for (int part = 0; part < cacheCtx.topology().partitions(); part++) {
                int partId = part;

                exec.submit(() -> cacheCtx.topology().localPartition(partId).finalizeUpdateCounters(), cacheCtx.groupId(), part);
            }
        }

        if (log.isInfoEnabled()) {
            log.info("Finished restore incremental snapshot [updatesApplied=" + applied.longValue() +
                ", time=" + (U.currentTimeMillis() - start) + " ms]");
        }
    }

    /** @return {@link IncrementalSnapshotFinishRecord} for specified snapshot, or {@code null} if not found. */
    private @Nullable IncrementalSnapshotFinishRecord readFinishRecord(File segment, UUID incSnpId) throws IgniteCheckedException {
        try (WALIterator it = walIter(log, Collections.singleton(INCREMENTAL_SNAPSHOT_FINISH_RECORD), segment)) {
            while (it.hasNext()) {
                IncrementalSnapshotFinishRecord finRec = (IncrementalSnapshotFinishRecord)it.next().getValue();

                if (finRec.id().equals(incSnpId))
                    return finRec;
            }
        }

        return null;
    }

    /** @return WAL segments to restore for specified incremental index since the base snapshot. */
    private File[] walSegments(String snpName, String snpPath, int incIdx) throws IgniteCheckedException {
        File[] segments = null;

        for (int i = 1; i <= incIdx; i++) {
            File incSnpDir = ctx.cache().context().snapshotMgr().incrementalSnapshotLocalDir(snpName, snpPath, i);

            if (!incSnpDir.exists())
                throw new IgniteCheckedException("Incremental snapshot doesn't exists [dir=" + incSnpDir + ']');

            File[] incSegs = incSnpDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

            if (incSegs == null)
                throw new IgniteCheckedException("Failed to list WAL segments from snapshot directory [dir=" + incSnpDir + ']');

            if (segments == null)
                segments = incSegs;
            else {
                int segLen = segments.length;

                segments = Arrays.copyOf(segments, segLen + incSegs.length);

                System.arraycopy(incSegs, 0, segments, segLen, incSegs.length);
            }
        }

        if (F.isEmpty(segments)) {
            throw new IgniteCheckedException("No WAL segments found for incremental snapshot " +
                "[snpName=" + snpName + ", snpPath=" + snpPath + ", incIdx=" + incIdx + ']');
        }

        return segments;
    }

    /**
     * Enable or disable WAL while restoring incremental snapshot.
     *
     * @param enabled Enable or disable WAL for caches.
     * @param cacheIds Restoring cache IDs.
     */
    private void walEnabled(boolean enabled, Set<Integer> cacheIds) {
        for (Integer cacheId: cacheIds) {
            int grpId = ctx.cache().cacheDescriptor(cacheId).groupId();

            CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

            assert grp != null : "cacheId=" + cacheId + " grpId=" + grpId;

            grp.localWalEnabled(enabled, false);
        }
    }

    /**
     * @param dbMgr Database manager.
     * @param cacheCtx Cache context to apply an update.
     * @param dataEntry Data entry to apply.
     * @throws IgniteCheckedException If failed to apply entry.
     */
    private void applyDataEntry(
        GridCacheDatabaseSharedManager dbMgr,
        GridCacheContext<?, ?> cacheCtx,
        DataEntry dataEntry
    ) throws IgniteCheckedException {
        int partId = dataEntry.partitionId();

        if (partId == -1) {
            U.warn(log, "Partition isn't set for read data entry [entry=" + dataEntry + ']');

            partId = cacheCtx.affinity().partition(dataEntry.key());
        }

        // Local partitions has already created after caches started.
        GridDhtLocalPartition locPart = cacheCtx.topology().localPartition(partId);

        assert locPart != null : "Missed local partition " + partId;

        if (dbMgr.applyDataEntry(cacheCtx, locPart, dataEntry))
            cacheCtx.offheap().dataStore(locPart).updateCounter(dataEntry.partitionCounter() - 1, 1);
    }

    /**
     * @param log Ignite logger.
     * @param types WAL record types to read.
     * @param segments WAL segments.
     * @return Iterator over WAL segments.
     */
    public WALIterator walIter(IgniteLogger log, Set<WALRecord.RecordType> types, File... segments) throws IgniteCheckedException {
        return new IgniteWalIteratorFactory(log)
            .iterator(new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filter((recType, recPtr) -> types.contains(recType))
                .sharedContext(ctx.cache().context())
                .filesOrDirs(segments));
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishIncrementalSnapshotRestore(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Exception failure = errs.values().stream().findFirst().
            orElse(checkNodeLeft(opCtx0.nodes(), res.keySet()));

        if (failure == null) {
            finishProcess(reqId, null);

            return;
        }

        opCtx0.err.compareAndSet(null, failure);

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            cacheStopProc.start(reqId, reqId);
    }

    /**
     * @param metas Map of snapshot metadata distribution across the cluster.
     * @return Map of cache partitions per each node.
     */
    private static Map<UUID, Map<Integer, Set<Integer>>> snapshotAffinity(
        Map<UUID, List<SnapshotMetadata>> metas,
        BiPredicate<Integer, Integer> filter
    ) {
        Map<UUID, Map<Integer, Set<Integer>>> nodeToSnp = new HashMap<>();

        List<UUID> nodes = new ArrayList<>(metas.keySet());
        Collections.shuffle(nodes);

        Map<UUID, List<SnapshotMetadata>> shuffleMetas = new LinkedHashMap<>();
        nodes.forEach(k -> shuffleMetas.put(k, metas.get(k)));

        for (Map.Entry<UUID, List<SnapshotMetadata>> e : shuffleMetas.entrySet()) {
            UUID nodeId = e.getKey();

            for (SnapshotMetadata meta : ofNullable(e.getValue()).orElse(Collections.emptyList())) {
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

        return nodeToSnp;
    }

    /**
     * @param reqNodes Set of required topology nodes.
     * @param respNodes Set of responding topology nodes.
     * @return Error, if no response was received from the required topology node.
     */
    private Exception checkNodeLeft(Collection<UUID> reqNodes, Set<UUID> respNodes) {
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
        }

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

        if (!res.keySet().containsAll(opCtx0.nodes())) {
            Set<UUID> leftNodes = new HashSet<>(opCtx0.nodes());

            leftNodes.removeAll(res.keySet());

            log.warning("Some of the nodes left the cluster and were unable to complete the rollback" +
                " operation [reqId=" + reqId + ", snapshot=" + opCtx0.snpName + ", node(s)=" + leftNodes + ']');
        }

        finishProcess(reqId, opCtx0.err.get());
    }

    /**
     * @param opCtx Snapshot operation context.
     * @param srcDir Snapshot directory to copy from.
     * @param targetDir Destination directory to copy to.
     */
    private void copyLocalAsync(
        SnapshotRestoreContext opCtx,
        File srcDir,
        File targetDir,
        PartitionRestoreFuture partFut
    ) {
        File snpFile = new File(srcDir, FilePageStoreManager.getPartitionFileName(partFut.partId));
        Path partFile = Paths.get(targetDir.getAbsolutePath(), FilePageStoreManager.getPartitionFileName(partFut.partId));
        int grpId = groupIdFromTmpDir(targetDir);

        IgniteSnapshotManager snapMgr = ctx.cache().context().snapshotMgr();

        CompletableFuture<Path> copyPartFut = CompletableFuture.supplyAsync(() -> {
            if (opCtx.stopChecker.getAsBoolean())
                throw new IgniteInterruptedException("The operation has been stopped on copy file: " + snpFile.getAbsolutePath());

            if (Thread.interrupted())
                throw new IgniteInterruptedException("Thread has been interrupted: " + Thread.currentThread().getName());

            if (!snpFile.exists()) {
                throw new IgniteException("Partition snapshot file doesn't exist [snpName=" + opCtx.snpName +
                    ", snpDir=" + snpFile.getAbsolutePath() + ", name=" + snpFile.getName() + ']');
            }

            IgniteSnapshotManager.copy(snapMgr.ioFactory(), snpFile, partFile.toFile(), snpFile.length());

            return partFile;
        }, snapMgr.snapshotExecutorService());

        if (opCtx.isGroupCompressed(grpId)) {
            copyPartFut = copyPartFut.thenComposeAsync(
                p -> {
                    CompletableFuture<Path> result = new CompletableFuture<>();
                    try {
                        punchHole(grpId, partFut.partId, partFile.toFile());

                        result.complete(partFile);
                    }
                    catch (Throwable t) {
                        result.completeExceptionally(t);
                    }
                    return result;
                },
                snapMgr.snapshotExecutorService()
            );
        }

        copyPartFut.whenComplete((r, t) -> opCtx.errHnd.accept(t))
            .whenComplete((r, t) -> {
                if (t == null)
                    partFut.complete(partFile);
                else
                    partFut.completeExceptionally(t);
            });
    }

    /** */
    private void punchHole(int grpId, int partId, File partFile) throws Exception {
        FilePageStoreManager storeMgr = (FilePageStoreManager)ctx.cache().context().pageStore();
        FileVersionCheckingFactory factory = storeMgr.getPageStoreFactory(grpId, null);

        try (FilePageStore pageStore = (FilePageStore)factory.createPageStore(getTypeByPartId(partId), partFile, val -> {})) {
            pageStore.init();

            ByteBuffer buf = locBuff.get();

            long pageId = PageIdUtils.pageId(partId, (byte)0, 0);

            for (int pageNo = 0; pageNo < pageStore.pages(); pageId++, pageNo++) {
                if (opCtx.stopChecker.getAsBoolean()) {
                    throw new IgniteInterruptedException("The operation has been stopped while punching holes in file: "
                        + partFile.getAbsolutePath());
                }

                if (Thread.interrupted())
                    throw new IgniteInterruptedException("Thread has been interrupted: " + Thread.currentThread().getName());

                buf.clear();

                pageStore.read(pageId, buf, true);

                if (PageIO.getCompressionType(buf) == CompressionProcessor.UNCOMPRESSED_PAGE)
                    continue;

                int comprPageSz = PageIO.getCompressedSize(buf);

                if (comprPageSz < pageStore.getPageSize())
                    pageStore.punchHole(pageId, comprPageSz);
            }
        }
    }

    /**
     * @param col Collection of sets to complete.
     * @param ex Exception to set.
     */
    private static void completeListExceptionally(List<PartitionRestoreFuture> col, Throwable ex) {
        for (PartitionRestoreFuture f : col)
            f.completeExceptionally(ex);
    }

    /**
     * @param map Map of partitions and cache groups.
     * @return String representation.
     */
    private static String partitionsMapToString(Map<Integer, Set<Integer>> map) {
        return map.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> S.toStringSortedDistinct(e.getValue())))
            .toString();
    }

    /**
     * Cache group restore from snapshot operation context.
     */
    private static class SnapshotRestoreContext {
        /** Request ID. */
        private final UUID reqId;

        /** Snapshot name. */
        private final String snpName;

        /** Snapshot directory path. */
        private final String snpPath;

        /** Baseline discovery cache for node IDs that must be alive to complete the operation.*/
        private final DiscoCache discoCache;

        /** Operational node id. */
        private final UUID opNodeId;

        /** Index of incremental snapshot, {@code 0} if no incremental snapshot to restore. */
        private final int incIdx;

        /**
         * Set of restored cache groups path on local node. Collected when all cache configurations received
         * from the <tt>prepare</tt> distributed process.
         */
        private final Set<File> dirs = new HashSet<>();

        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** Distribution of snapshot metadata files across the cluster. */
        private final Map<UUID, List<SnapshotMetadata>> metasPerNode = new HashMap<>();

        /** Context error handler. */
        private final Consumer<Throwable> errHnd = (ex) -> err.compareAndSet(null, ex);

        /** Stop condition checker. */
        private final BooleanSupplier stopChecker = () -> err.get() != null;

        /** Compressed groups. */
        private final Set<Integer> comprGrps = new HashSet<>();

        /** Cache ID to configuration mapping. */
        private volatile Map<Integer, StoredCacheData> cfgs = Collections.emptyMap();

        /** Graceful shutdown future. */
        private volatile IgniteFuture<?> stopFut;

        /** Operation start time. */
        private final long startTime;

        /** Number of processed (copied) partitions. */
        private final AtomicInteger processedParts = new AtomicInteger(0);

        /** Total number of partitions to be restored. */
        private volatile int totalParts = -1;

        /** Operation end time. */
        private volatile long endTime;

        /** Creates an empty context. */
        protected SnapshotRestoreContext() {
            reqId = null;
            snpName = "";
            startTime = 0;
            opNodeId = null;
            discoCache = null;
            snpPath = null;
            incIdx = 0;
        }

        /**
         * @param req Request to prepare cache group restore from the snapshot.
         * @param discoCache Baseline discovery cache for node IDs that must be alive to complete the operation.
         * @param cfgs Cache ID to configuration mapping.
         */
        protected SnapshotRestoreContext(
            SnapshotOperationRequest req,
            DiscoCache discoCache,
            Map<Integer, StoredCacheData> cfgs
        ) {
            reqId = req.requestId();
            snpName = req.snapshotName();
            snpPath = req.snapshotPath();
            opNodeId = req.operationalNodeId();
            incIdx = req.incrementIndex();
            startTime = U.currentTimeMillis();

            this.discoCache = discoCache;
            this.cfgs = cfgs;
        }

        /**
         * @return Required baseline nodeIds that must be alive to complete restore operation.
         */
        public Collection<UUID> nodes() {
            return F.transform(discoCache.aliveBaselineNodes(), F.node2id());
        }

        /** */
        public boolean isGroupCompressed(int grpId) {
            return comprGrps.contains(grpId);
        }

        /** */
        void addCompressedGroup(int grpId) {
            comprGrps.add(grpId);
        }
    }

    /** Snapshot operation prepare response. */
    private static class SnapshotRestoreOperationResponse implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Cache configurations on local node. */
        private final List<StoredCacheData> ccfgs;

        /** Snapshot metadata files on local node. */
        private final List<SnapshotMetadata> metas;

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

    /** Future will be completed when partition processing ends. */
    private static class PartitionRestoreFuture extends CompletableFuture<Path> {
        /** Partition id. */
        private final int partId;

        /** Counter of the total number of processed partitions. */
        private final AtomicInteger cntr;

        /**
         * @param partId Partition id.
         * @param cntr Counter of the total number of processed partitions.
         */
        private PartitionRestoreFuture(int partId, AtomicInteger cntr) {
            this.partId = partId;
            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public boolean complete(Path path) {
            cntr.incrementAndGet();

            return super.complete(path);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PartitionRestoreFuture future = (PartitionRestoreFuture)o;

            return partId == future.partId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(partId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PartitionRestoreFuture.class, this);
        }
    }
}
