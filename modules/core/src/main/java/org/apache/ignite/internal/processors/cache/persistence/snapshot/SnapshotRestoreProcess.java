///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.SNAPSHOT_RESTORE_CACHE_GROUP;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreProcess {
    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotRestorePrepareRequest, ArrayList<StoredCacheData>> prepareRestoreProc;

    /** Cache group restore cache start phase. */
    private final DistributedProcess<UUID, Boolean> cacheStartProc;

    /** Cache group restore rollback phase. */
    private final DistributedProcess<SnapshotRestoreRollbackRequest, SnapshotRestoreRollbackResponse> rollbackRestoreProc;

    /** Logger. */
    private final IgniteLogger log;

    /** The future to be completed when the cache restore process is complete. */
    private volatile GridFutureAdapter<Void> fut;

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

        cacheStartProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_START, this::cacheStart, this::finishCacheStart);

        rollbackRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK, this::rollback, this::finishRollback);
    }

    /**
     * Start cache group restore operation.
     *
     * @param snpName Snapshot name.
     * @param cacheGrpNames Name of the cache groups for restore.
     * @return Future that will be completed when the restore operation is complete and the cache groups are started.
     */
    public IgniteFuture<Void> start(String snpName, Collection<String> cacheGrpNames) {
        if (ctx.clientNode()) {
            return new IgniteFinishedFutureImpl<>(new UnsupportedOperationException("Client and daemon nodes can not " +
                "perform this operation."));
        }

        synchronized (this) {
            GridFutureAdapter<Void> fut0 = fut;

            if (opCtx != null || (fut0 != null && !fut0.isDone())) {
                return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                    "The previous snapshot restore operation was not completed."));
            }

            fut = new GridFutureAdapter<>();
        }

        DiscoveryDataClusterState clusterState = ctx.state().clusterState();

        if (clusterState.state() != ClusterState.ACTIVE || clusterState.transition())
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        if (!clusterState.hasBaselineTopology()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "The baseline topology is not configured for cluster."));
        }

        IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();

        if (snpMgr.isSnapshotCreating()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "A cluster snapshot operation is in progress."));
        }

        if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), SNAPSHOT_RESTORE_CACHE_GROUP))
            throw new IgniteException("Not all nodes in the cluster support a snapshot restore operation.");

        snpMgr.collectSnapshotMetadata(snpName).listen(
            f -> {
                if (f.error() != null) {
                    fut.onDone(f.error());

                    return;
                }

                Set<UUID> dataNodes = new LinkedHashSet<>();
                Map<ClusterNode, List<SnapshotMetadata>> metas = f.result();
                Map<Integer, String> reqGrpIds = cacheGrpNames.stream().collect(Collectors.toMap(CU::cacheId, v -> v));

                for (Map.Entry<ClusterNode, List<SnapshotMetadata>> entry : metas.entrySet()) {
                    SnapshotMetadata meta = F.first(entry.getValue());

                    assert meta != null : entry.getKey().id();

                    if (!entry.getKey().consistentId().equals(meta.consistentId()))
                        continue;

                    dataNodes.add(entry.getKey().id());

                    reqGrpIds.keySet().removeAll(meta.partitions().keySet());
                }

                if (!reqGrpIds.isEmpty()) {
                    fut.onDone(new IllegalArgumentException(OP_REJECT_MSG + "Cache group(s) was not found in the " +
                        "snapshot [groups=" + reqGrpIds.values() + ", snapshot=" + snpName + ']'));

                    return;
                }

                dataNodes.add(ctx.localNodeId());

                snpMgr.runSnapshotVerfification(metas).listen(
                    f0 -> {
                        if (f0.error() != null) {
                            fut.onDone(f0.error());

                            return;
                        }

                        IdleVerifyResultV2 res = f0.result();

                        if (!F.isEmpty(res.exceptions()) || res.hasConflicts()) {
                            StringBuilder sb = new StringBuilder();

                            res.print(sb::append, true);

                            fut.onDone(new IgniteException(sb.toString()));

                            return;
                        }

                        SnapshotRestorePrepareRequest req = new SnapshotRestorePrepareRequest(UUID.randomUUID(),
                            snpName, dataNodes, cacheGrpNames, F.first(dataNodes));

                        prepareRestoreProc.start(req.requestId(), req);
                    }
                );
            }
        );

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Check if snapshot restore process is currently running.
     *
     * @return {@code True} if the snapshot restore operation is in progress.
     */
    public boolean isSnapshotRestoring() {
        return opCtx != null;
    }

    /**
     * Check if the cache or group with the specified name is currently being restored from the snapshot.
     *
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @return {@code True} if the cache or group with the specified name is currently being restored.
     */
    public boolean isCacheRestoring(String cacheName, @Nullable String grpName) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null)
            return false;

        Map<Integer, StoredCacheData> cacheCfgs = opCtx0.cfgs;

        int cacheId = CU.cacheId(cacheName);

        if (cacheCfgs.containsKey(cacheId))
            return true;

        for (File grpDir : opCtx0.dirs) {
            String locGrpName = FilePageStoreManager.cacheGroupName(grpDir);

            if (grpName == null) {
                if (CU.cacheId(locGrpName) == cacheId)
                    return true;
            }
            else {
                if (cacheName.equals(locGrpName))
                    return true;

                if (CU.cacheId(locGrpName) == CU.cacheId(grpName))
                    return true;
            }
        }

        return false;
    }

    /**
     * Finish local cache group restore process.
     */
    private void finishProcess() {
        finishProcess(null);
    }

    /**
     * Finish local cache group restore process.
     *
     * @param err Error, if any.
     */
    private void finishProcess(@Nullable Throwable err) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (err != null) {
            log.error("Failed to restore snapshot cache group" + (opCtx0 == null ? "" :
                " [requestID=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ']'), err);
        }
        else if (log.isInfoEnabled()) {
            log.info("Successfully restored cache group(s) from the snapshot" + (opCtx0 == null ? "" :
                " [requestID=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ']'));
        }

        GridFutureAdapter<Void> fut0 = fut;

        if (fut0 != null)
            fut0.onDone(null, err);

        opCtx = null;
    }

    /**
     * Node left callback.
     *
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null && opCtx0.nodes.contains(leftNodeId)) {
            opCtx0.err.compareAndSet(null, new IgniteException(OP_REJECT_MSG +
                "Server node(s) has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Abort the currently running restore procedure (if any).
     *
     * @param reason Interruption reason.
     */
    public void stop(Exception reason) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null)
            opCtx0.err.compareAndSet(null, reason);
    }

    /**
     * Ensures that a cache with the specified name does not exist locally.
     *
     * @param name Cache name.
     */
    private void ensureCacheAbsent(String name) {
        int id = CU.cacheId(name);

        if (ctx.cache().cacheGroupDescriptors().containsKey(id) || ctx.cache().cacheDescriptor(id) != null) {
            throw new IllegalStateException("Cache \"" + name +
                "\" should be destroyed manually before perform restore operation.");
        }
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @return Result future.
     */
    private IgniteInternalFuture<ArrayList<StoredCacheData>> prepare(SnapshotRestorePrepareRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        try {
            opCtx = prepareContext(req);
        } catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }

        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0.dirs.isEmpty())
            return new GridFinishedFuture<>();

        if (log.isInfoEnabled()) {
            log.info("Starting local snapshot restore operation [requestID=" + req.requestId() +
                ", snapshot=" + req.snapshotName() + ", group(s)=" + req.groups() + ']');
        }

        GridFutureAdapter<ArrayList<StoredCacheData>> retFut = new GridFutureAdapter<>();

        ctx.cache().context().snapshotMgr().snapshotExecutorService().execute(() -> {
            try {
                restore(opCtx0, ctx.localNodeId().equals(req.updateMetaNodeId()));

                Throwable err = opCtx0.err.get();

                if (err == null) {
                    retFut.onDone(new ArrayList<>(opCtx0.cfgs.values()));

                    return;
                }

                log.error("Snapshot restore process has been interrupted " +
                    "[requestID=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ']', err);

                rollback(opCtx0);

                retFut.onDone(err);
            }
            catch (Throwable t) {
                retFut.onDone(t);
            }
        });

        return retFut;
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @return Snapshot restore operation context.
     * @throws IgniteCheckedException If failed.
     */
    private SnapshotRestoreContext prepareContext(SnapshotRestorePrepareRequest req) throws IgniteCheckedException {
        if (isSnapshotRestoring())
            throw new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed.");

        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.state() != ClusterState.ACTIVE || state.transition())
            throw new IgniteException(OP_REJECT_MSG + "The cluster should be active.");

        if (!allNodesInBaselineAndAlive(req.nodes()))
            throw new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster.");

        for (String grpName : req.groups())
            ensureCacheAbsent(grpName);

        GridCacheSharedContext<?, ?> cctx = ctx.cache().context();

        SnapshotMetadata meta = F.first(cctx.snapshotMgr().readSnapshotMetadatas(req.snapshotName()));

        if (meta != null && meta.consistentId().equals(cctx.localNode().consistentId().toString())
            && meta.pageSize() != cctx.database().pageSize()) {
            throw new IgniteCheckedException("Incompatible memory page size " +
                "[snapshotPageSize=" + meta.pageSize() +
                ", local=" + cctx.database().pageSize() +
                ", snapshot=" + req.snapshotName() +
                ", nodeId=" + cctx.localNodeId() + ']');
        }

        String pdsFolderName = ctx.pdsFolderResolver().resolveFolders().folderName();
        List<File> cacheDirs = new ArrayList<>();
        Map<String, StoredCacheData> cfgsByName = new HashMap<>();

        // Collect cache configuration(s) and verify cache groups page size.
        for (File snpCacheDir : cctx.snapshotMgr().snapshotCacheDirectories(req.snapshotName(), pdsFolderName)) {
            String grpName = FilePageStoreManager.cacheGroupName(snpCacheDir);

            if (!req.groups().contains(grpName))
                continue;

            ((FilePageStoreManager)cctx.pageStore()).readCacheConfigurations(snpCacheDir, cfgsByName);

            File cacheDir = U.resolveWorkDirectory(ctx.config().getWorkDirectory(),
                Paths.get(databaseRelativePath(pdsFolderName), snpCacheDir.getName()).toString(), false);

            if (!cacheDir.exists())
                cacheDir.mkdir();
            else if (cacheDir.list().length > 0) {
                throw new IgniteCheckedException("Unable to restore cache group, directory is not empty " +
                    "[group=" + grpName + ", dir=" + cacheDir + ']');
            }

            cacheDirs.add(cacheDir);
        }

        Map<Integer, StoredCacheData> cfgsById = cfgsByName.isEmpty() ? Collections.emptyMap() :
            cfgsByName.values().stream().collect(Collectors.toMap(v -> CU.cacheId(v.config().getName()), v -> v));

        return new SnapshotRestoreContext(req.requestId(), req.snapshotName(), req.nodes(), cacheDirs, cfgsById);
    }

    /**
     * Copy partition files and update binary metadata.
     *
     * @param opCtx Snapshot restore operation context.
     * @param updateMeta Update binary metadata flag.
     * @throws IgniteCheckedException If failed.
     */
    protected void restore(SnapshotRestoreContext opCtx, boolean updateMeta) throws IgniteCheckedException {
        BooleanSupplier stopChecker = () -> opCtx.err.get() != null;
        String pdsFolderName = ctx.pdsFolderResolver().resolveFolders().folderName();

        if (updateMeta) {
            File binDir = binaryWorkDir(
                ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx.snpName).getAbsolutePath(), pdsFolderName);

            if (stopChecker.getAsBoolean())
                return;

            // Check binary metadata compatibility.
            ctx.cacheObjects().checkMetadata(binDir);

            // Cluster-wide update binary metadata.
            ctx.cacheObjects().updateMetadata(binDir, stopChecker);
        }

        for (File cacheDir : opCtx.dirs) {
            File snpCacheDir = new File(ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx.snpName),
                Paths.get(databaseRelativePath(pdsFolderName), cacheDir.getName()).toString());

            try {
                if (log.isInfoEnabled())
                    log.info("Copying files of the cache group [from=" + snpCacheDir + ", to=" + cacheDir + ']');

                for (File snpFile : snpCacheDir.listFiles()) {
                    if (stopChecker.getAsBoolean())
                        return;

                    File target = new File(cacheDir, snpFile.getName());

                    if (log.isDebugEnabled()) {
                        log.debug("Copying file from the snapshot " +
                            "[snapshot=" + opCtx.snpName +
                            ", src=" + snpFile +
                            ", target=" + target + "]");
                    }

                    Files.copy(snpFile.toPath(), target.toPath());
                }
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Unable to copy file [snapshot=" + opCtx.snpName +
                    ", grp=" + FilePageStoreManager.cacheGroupName(cacheDir) + ']', e);
            }
        }
    }

    /**
     * Rollback changes made by process in specified cache group.
     *
     * @param opCtx Snapshot restore operation context.
     */
    private void rollback(@Nullable SnapshotRestoreContext opCtx) {
        if (opCtx == null || F.isEmpty(opCtx.dirs))
            return;

        if (log.isInfoEnabled())
            log.info("Performing local rollback routine for restored cache groups [requestID=" + opCtx.reqId + ']');

        try {
            for (File cacheDir : opCtx.dirs) {
                if (!cacheDir.exists())
                    continue;

                if (log.isInfoEnabled())
                    log.info("Cleaning up directory " + cacheDir);

                U.delete(cacheDir);
            }
        }
        catch (Exception e) {
            log.error("Failed to perform rollback [requestID=" + opCtx.reqId + ", snapshot=" + opCtx.snpName + ']', e);
        }
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, ArrayList<StoredCacheData>> res, Map<UUID, Exception> errs) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (ctx.clientNode())
            return;

        Exception failure = checkFailure(errs, opCtx0, res.keySet());

        if (failure == null) {
            assert opCtx0 != null : ctx.localNodeId();

            Map<Integer, StoredCacheData> globalCfgs = new HashMap<>();

            for (List<StoredCacheData> storedCfgs : res.values()) {
                if (storedCfgs == null)
                    continue;

                for (StoredCacheData cacheData : storedCfgs)
                    globalCfgs.put(CU.cacheId(cacheData.config().getName()), cacheData);
            }

            opCtx0.cfgs = globalCfgs;

            if (U.isLocalNodeCoordinator(ctx.discovery()))
                cacheStartProc.start(reqId, reqId);

            return;
        }

        if (opCtx0 == null)
            finishProcess(failure);
        else // Remove files asynchronously.
            ctx.cache().context().snapshotMgr().snapshotExecutorService().execute(() -> {
                rollback(opCtx0);

                finishProcess(failure);
            });
    }

    /**
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> cacheStart(UUID reqId) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null)
            return new GridFinishedFuture<>();

        if (!reqId.equals(opCtx0.reqId))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return new GridFinishedFuture<>();

        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.state() != ClusterState.ACTIVE || state.transition())
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        Throwable err = opCtx0.err.get();

        if (err != null)
            return new GridFinishedFuture<>(err);

        if (!allNodesInBaselineAndAlive(opCtx0.nodes))
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster."));

        GridFutureAdapter<Boolean> retFut = new GridFutureAdapter<>();

        try {
            Collection<StoredCacheData> ccfgs = opCtx0.cfgs.values();

            // Ensure that shared cache groups has no conflicts before start caches.
            for (StoredCacheData cfg : ccfgs) {
                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getName());
            }

            if (log.isInfoEnabled()) {
                log.info("Starting restored caches " +
                    "[requestID=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName +
                    ", caches=" + F.viewReadOnly(ccfgs, c -> c.config().getName()) + ']');
            }

            ctx.cache().dynamicStartCachesByStoredConf(ccfgs, true, true, false, null, true, opCtx0.nodes).listen(
                f -> {
                    if (f.error() != null) {
                        log.error("Unable to start restored caches [requestID=" + opCtx0.reqId +
                            ", snapshot=" + opCtx0.snpName + ']', f.error());

                        retFut.onDone(f.error());
                    }
                    else
                        retFut.onDone(true);
                }
            );
        } catch (Exception e) {
            log.error("Unable to restore cache group(s) from snapshot " +
                "[requestID=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ']', e);

            return new GridFinishedFuture<>(e);
        }

        return retFut;
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishCacheStart(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null || !reqId.equals(opCtx0.reqId))
            return;

        Exception failure = checkFailure(errs, opCtx0, res.keySet());

        if (failure == null) {
            finishProcess();

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, new SnapshotRestoreRollbackRequest(reqId, failure));
    }

    /**
     * Check the response for probable failures.
     *
     * @param errs Errors.
     * @param opCtx Snapshot restore operation context.
     * @param respNodes Set of responding topology nodes.
     * @return Error, if any.
     */
    private Exception checkFailure(Map<UUID, Exception> errs, SnapshotRestoreContext opCtx, Set<UUID> respNodes) {
        Exception err = F.first(errs.values());

        if (err == null && opCtx != null && !respNodes.containsAll(opCtx.nodes)) {
            Set<UUID> leftNodes = new HashSet<>(opCtx.nodes);

            leftNodes.removeAll(respNodes);

            err = new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster [nodeId=" + leftNodes + ']');
        }

        return err;
    }

    /**
     * @param req Request to rollback cache group restore process.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestoreRollbackResponse> rollback(SnapshotRestoreRollbackRequest req) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null || !req.requestId().equals(opCtx0.reqId))
            return new GridFinishedFuture<>();

        if (!opCtx0.nodes.contains(ctx.localNodeId()))
            return new GridFinishedFuture<>();

        rollback(opCtx0);

        return new GridFinishedFuture<>(new SnapshotRestoreRollbackResponse(req.error()));
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishRollback(UUID reqId, Map<UUID, SnapshotRestoreRollbackResponse> res, Map<UUID, Exception> errs) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null || !reqId.equals(opCtx0.reqId))
            return;

        SnapshotRestoreRollbackResponse resp = F.first(F.viewReadOnly(res.values(), v -> v, Objects::nonNull));

        finishProcess(resp.error());
    }

    /**
     * @param nodeIds Set of required baseline node IDs.
     * @return {@code True} if all of the specified nodes present in baseline and alive.
     */
    private boolean allNodesInBaselineAndAlive(Set<UUID> nodeIds) {
        for (UUID nodeId : nodeIds) {
            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null || !CU.baselineNode(node, ctx.state().clusterState()) || !ctx.discovery().alive(node))
                return false;
        }

        return true;
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

        /** List of restored cache group directories. */
        private final Collection<File> dirs;

        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** Cache ID to configuration mapping. */
        private volatile Map<Integer, StoredCacheData> cfgs;

        /**
         * @param reqId Request ID.
         * @param snpName Snapshot name.
         * @param nodes Baseline node IDs that must be alive to complete the operation.
         * @param dirs List of cache group names to restore from the snapshot.
         * @param cfgs Cache ID to configuration mapping.
         */
        protected SnapshotRestoreContext(UUID reqId, String snpName, Collection<UUID> nodes, Collection<File> dirs,
            Map<Integer, StoredCacheData> cfgs) {
            this.reqId = reqId;
            this.snpName = snpName;
            this.nodes = new HashSet<>(nodes);
            this.dirs = dirs;
            this.cfgs = cfgs;
        }
    }
}
