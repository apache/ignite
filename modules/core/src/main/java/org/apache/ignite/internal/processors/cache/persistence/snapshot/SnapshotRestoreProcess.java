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
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.apache.ignite.internal.util.GridConcurrentHashSet;
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
    private volatile GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

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

        fut.onDone();
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
            IgniteInternalFuture<Void> fut0 = fut;

            if (!fut0.isDone()) {
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
     * Check if the cache group restore process is currently running.
     *
     * @return {@code True} if cache group restore process is currently running.
     */
    public boolean inProgress(@Nullable String cacheName) {
        SnapshotRestoreContext opCtx0 = opCtx;

        return !staleProcess(fut, opCtx0) && (cacheName == null || opCtx0.containsCache(cacheName));
    }

    /**
     * @param fut The future of cache snapshot restore operation.
     * @param opCtx Snapshot restore operation context.
     * @return {@code True} if the future completed or not initiated.
     */
    public boolean staleProcess(IgniteInternalFuture<Void> fut, SnapshotRestoreContext opCtx) {
        return fut.isDone() || opCtx == null;
    }

    /**
     * Node left callback.
     *
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null && opCtx0.nodes().contains(leftNodeId)) {
            opCtx0.interrupt(new IgniteException(OP_REJECT_MSG +
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
            opCtx0.interrupt(reason);
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

        if (inProgress(null)) {
            return new GridFinishedFuture<>(
                new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed."));
        }

        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.state() != ClusterState.ACTIVE || state.transition())
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        // Skip creating future on initiator.
        if (fut.isDone())
            fut = new GridFutureAdapter<>();

        opCtx = new SnapshotRestoreContext(req.requestId(), req.snapshotName(), req.groups(), req.nodes());

        fut.listen(f -> opCtx = null);

        try {
            Map<String, StoredCacheData> cfgMap = new HashMap<>();
            GridCacheSharedContext<?, ?> cctx = ctx.cache().context();
            String folderName = ctx.pdsFolderResolver().resolveFolders().folderName();

            SnapshotMetadata meta = F.first(cctx.snapshotMgr().readSnapshotMetadatas(req.snapshotName()));

            if (meta == null || !meta.consistentId().equals(cctx.localNode().consistentId().toString()))
                return new GridFinishedFuture<>();

            if (meta.pageSize() != cctx.database().pageSize()) {
                throw new IgniteCheckedException("Incompatible memory page size " +
                    "[snapshotPageSize=" + meta.pageSize() +
                    ", local=" + cctx.database().pageSize() +
                    ", snapshot=" + req.snapshotName() +
                    ", nodeId=" + cctx.localNodeId() + ']');
            }

            SnapshotRestoreContext opCtx0 = opCtx;

            // Collect cache configuration(s) and verify cache groups page size.
            for (File cacheDir : cctx.snapshotMgr().snapshotCacheDirectories(req.snapshotName(), folderName)) {
                String grpName = FilePageStoreManager.cacheGroupName(cacheDir);

                if (!opCtx0.groups().contains(grpName))
                    continue;

                ((FilePageStoreManager)cctx.pageStore()).readCacheConfigurations(cacheDir, cfgMap);
            }

            ArrayList<StoredCacheData> ccfgs = new ArrayList<>(cfgMap.values());

            if (ccfgs.isEmpty())
                return new GridFinishedFuture<>();

            if (!allNodesInBaselineAndAlive(req.nodes()))
                throw new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster.");

            for (String grpName : opCtx0.groups())
                ensureCacheAbsent(grpName);

            File binDir = binaryWorkDir(cctx.snapshotMgr().snapshotLocalDir(req.snapshotName()).getAbsolutePath(), folderName);

            GridFutureAdapter<ArrayList<StoredCacheData>> retFut = new GridFutureAdapter<>();

            cctx.snapshotMgr().snapshotExecutorService().execute(() -> {
                try {
                    ctx.cacheObjects().checkMetadata(binDir);

                    restore(opCtx0, binDir, ctx.localNodeId().equals(req.performNodeId()));

                    if (!opCtx0.interrupted()) {
                        retFut.onDone(ccfgs);

                        return;
                    }

                    log.error("Snapshot restore process has been interrupted " +
                        "[groups=" + opCtx0.groups() + ", snapshot=" + opCtx0.snapshotName() + ']', opCtx0.error());

                    rollback(opCtx0);

                    retFut.onDone(opCtx0.error());

                }
                catch (Throwable t) {
                    retFut.onDone(t);
                }
            });

            return retFut;
        } catch (Exception e) {
            log.error("Unable to restore cache group(s) from snapshot " +
                "[groups=" + req.groups() + ", snapshot=" + req.snapshotName() + ']', e);

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Restore specified cache groups from the local snapshot directory.
     *
     * @param updateMetadata Update binary metadata flag.
     * @throws IgniteCheckedException If failed.
     */
    protected void restore(SnapshotRestoreContext opCtx, File binDir, boolean updateMetadata) throws IgniteCheckedException {
        if (opCtx.interrupted())
            return;

        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();
        String folderName = ctx.pdsFolderResolver().resolveFolders().folderName();

        if (updateMetadata) {
            assert binDir.exists();

            ctx.cacheObjects().updateMetadata(binDir, opCtx::interrupted);
        }

        for (File snpCacheDir : snapshotMgr.snapshotCacheDirectories(opCtx.snapshotName(), folderName)) {
            String grpName = FilePageStoreManager.cacheGroupName(snpCacheDir);

            if (!opCtx.groups().contains(grpName))
                continue;

            File cacheDir = U.resolveWorkDirectory(ctx.config().getWorkDirectory(),
                Paths.get(databaseRelativePath(folderName), snpCacheDir.getName()).toString(), false);

            if (!cacheDir.exists())
                cacheDir.mkdir();
            else
            if (cacheDir.list().length > 0) {
                throw new IgniteCheckedException("Unable to restore cache group, directory is not empty " +
                    "[group=" + grpName + ", dir=" + cacheDir + ']');
            }

            opCtx.pushDir(cacheDir);

            restoreCacheGroupFiles(opCtx.snapshotName(), grpName, cacheDir, snpCacheDir, opCtx::interrupted);
        }
    }

    /**
     * @param snpName Snapshot name.
     * @param grpName Cache group name.
     * @param cacheDir Cache group directory.
     * @param snpCacheDir Cache group directory in snapshot.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @throws IgniteCheckedException If failed.
     */
    protected void restoreCacheGroupFiles(String snpName, String grpName, File cacheDir, File snpCacheDir,
        BooleanSupplier stopChecker) throws IgniteCheckedException {
        try {
            if (log.isInfoEnabled())
                log.info("Copying files of the cache group [from=" + snpCacheDir + ", to=" + cacheDir + ']');

            for (File snpFile : snpCacheDir.listFiles()) {
                if (stopChecker.getAsBoolean())
                    return;

                File target = new File(cacheDir, snpFile.getName());

                if (log.isDebugEnabled()) {
                    log.debug("Copying file from the snapshot " +
                        "[snapshot=" + snpName +
                        ", grp=" + grpName +
                        ", src=" + snpFile +
                        ", target=" + target + "]");
                }

                Files.copy(snpFile.toPath(), target.toPath());
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to copy file [snapshot=" + snpName + ", grp=" + grpName + ']', e);
        }
    }

    /**
     * Rollback changes made by process in specified cache group.
     */
    protected void rollback(SnapshotRestoreContext opCtx) {
        File rmvDir;

        while ((rmvDir = opCtx.popDir()) != null) {
            if (!U.delete(rmvDir))
                log.error("Unable to delete restored cache directory [dir=" + rmvDir + ']');
        }
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, ArrayList<StoredCacheData>> res, Map<UUID, Exception> errs) {
        GridFutureAdapter<Void> fut0 = fut;
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleProcess(fut0, opCtx0))
            return;

        Exception failure = checkFailure(errs, opCtx0.nodes(), res.keySet());

        if (failure == null) {
            Map<String, StoredCacheData> filteredCfgs = new HashMap<>();

            for (List<StoredCacheData> storedCfgs : res.values()) {
                if (storedCfgs == null)
                    continue;

                for (StoredCacheData cacheData : storedCfgs)
                    filteredCfgs.put(cacheData.config().getName(), cacheData);
            }

            opCtx.configs(filteredCfgs.values());

            if (U.isLocalNodeCoordinator(ctx.discovery()))
                cacheStartProc.start(reqId, reqId);

            return;
        }

        // Remove files asynchronously.
        ctx.cache().context().snapshotMgr().snapshotExecutorService().execute(() -> {
            rollback(opCtx0);

            fut0.onDone(failure);
        });
    }

    /**
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> cacheStart(UUID reqId) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleProcess(fut, opCtx0))
            return new GridFinishedFuture<>();

        if (!reqId.equals(opCtx0.requestId()))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return new GridFinishedFuture<>();

        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.state() != ClusterState.ACTIVE || state.transition())
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        if (opCtx0.interrupted())
            return new GridFinishedFuture<>(opCtx0.error());

        if (!allNodesInBaselineAndAlive(opCtx0.nodes()))
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster."));

        GridFutureAdapter<Boolean> retFut = new GridFutureAdapter<>();

        try {
            Collection<StoredCacheData> ccfgs = opCtx0.configs();

            // Ensure that shared cache groups has no conflicts before start caches.
            for (StoredCacheData cfg : ccfgs) {
                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getName());
            }

            if (log.isInfoEnabled()) {
                log.info("Starting restored caches " +
                    "[snapshot=" + opCtx0.snapshotName() +
                    ", caches=" + F.viewReadOnly(opCtx0.configs(), c -> c.config().getName()) + ']');
            }

            ctx.cache().dynamicStartCachesByStoredConf(ccfgs, true, true, false, null, true, opCtx0.nodes()).listen(
                f -> {
                    if (f.error() != null) {
                        log.error("Unable to start restored caches [groups=" + opCtx0.groups() +
                            ", snapshot=" + opCtx0.snapshotName() + ']', f.error());

                        retFut.onDone(f.error());
                    }
                    else
                        retFut.onDone(true);
                }
            );
        } catch (Exception e) {
            log.error("Unable to restore cache group(s) from snapshot " +
                "[groups=" + opCtx0.groups() + ", snapshot=" + opCtx0.snapshotName() + ']', e);

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
        GridFutureAdapter<Void> fut0 = fut;
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleProcess(fut0, opCtx0) || !reqId.equals(opCtx0.requestId()))
            return;

        Exception failure = checkFailure(errs, opCtx0.nodes(), res.keySet());

        if (failure == null) {
            fut0.onDone();

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, new SnapshotRestoreRollbackRequest(reqId, failure));
    }

    /**
     * Check the response for probable failures.
     *
     * @param errs Errors.
     * @param expNodes Expected set of responding topology nodes.
     * @param respNodes Set of responding topology nodes.
     * @return Error, if any.
     */
    private Exception checkFailure(Map<UUID, Exception> errs, Set<UUID> expNodes, Set<UUID> respNodes) {
        Exception err = F.first(errs.values());

        if (err == null && !respNodes.containsAll(expNodes)) {
            Set<UUID> leftNodes = new HashSet<>(expNodes);

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

        if (staleProcess(fut, opCtx0) || !req.requestId().equals(opCtx0.requestId()))
            return new GridFinishedFuture<>();

        if (!opCtx0.nodes().contains(ctx.localNodeId()))
            return new GridFinishedFuture<>();

        if (log.isInfoEnabled())
            log.info("Performing rollback routine for restored cache groups [groups=" + opCtx0.groups() + ']');

        rollback(opCtx0);

        return new GridFinishedFuture<>(new SnapshotRestoreRollbackResponse(req.error()));
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishRollback(UUID reqId, Map<UUID, SnapshotRestoreRollbackResponse> res, Map<UUID, Exception> errs) {
        GridFutureAdapter<Void> fut0 = fut;
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleProcess(fut0, opCtx0) || !reqId.equals(opCtx0.requestId()))
            return;

        SnapshotRestoreRollbackResponse resp = F.first(F.viewReadOnly(res.values(), v -> v, Objects::nonNull));

        fut0.onDone(resp.error());
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
        private final Set<UUID> reqNodes;

        /** List of cache group names to restore from the snapshot. */
        private final Set<String> grps;

        /** Set of processed cache IDs. */
        private final Set<Integer> cacheIds = new GridConcurrentHashSet<>();

        /** Directories to clean up if the restore procedure fails. */
        private final Queue<File> grpDirs = new ConcurrentLinkedQueue<>();

        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        /** Collection of cache configurations */
        private volatile List<StoredCacheData> ccfgs;

        /**
         * @param reqId Request ID.
         * @param snpName Snapshot name.
         * @param grps List of cache group names to restore from the snapshot.
         * @param reqNodes Baseline node IDs that must be alive to complete the operation.
         */
        protected SnapshotRestoreContext(UUID reqId, String snpName, Collection<String> grps, Set<UUID> reqNodes) {
            this.reqId = reqId;
            this.reqNodes = new HashSet<>(reqNodes);
            this.snpName = snpName;

            Set<String> grps0 = new HashSet<>();

            for (String grpName : grps) {
                grps0.add(grpName);
                cacheIds.add(CU.cacheId(grpName));
            }

            this.grps = grps0;
        }

        /** @return Request ID. */
        protected UUID requestId() {
            return reqId;
        }

        /** @return Baseline node IDs that must be alive to complete the operation. */
        protected Set<UUID> nodes() {
            return Collections.unmodifiableSet(reqNodes);
        }

        /** @return Snapshot name. */
        protected String snapshotName() {
            return snpName;
        }

        /**
         * @return List of cache group names to restore from the snapshot.
         */
        protected Set<String> groups() {
            return grps;
        }

        /**
         * @param name Cache name.
         * @return {@code True} if the cache with the specified name is currently being restored.
         */
        protected boolean containsCache(String name) {
            return cacheIds.contains(CU.cacheId(name));
        }

        /**
         * @param ccfgs Collection of cache configurations.
         */
        protected void configs(Collection<StoredCacheData> ccfgs) {
            List<StoredCacheData> ccfgs0 = new ArrayList<>(ccfgs.size());

            for (StoredCacheData cacheData : ccfgs) {
                ccfgs0.add(cacheData);

                if (cacheData.config().getGroupName() != null)
                    cacheIds.add(CU.cacheId(cacheData.config().getName()));
            }

            this.ccfgs = ccfgs0;
        }

        /** @return Collection of cache configurations */
        protected Collection<StoredCacheData> configs() {
            return ccfgs;
        }

        /**
         * @param err Error.
         * @return {@code True} if operation has been interrupted by this call.
         */
        protected boolean interrupt(Exception err) {
            return errRef.compareAndSet(null, err);
        }

        /**
         * @return Interrupted flag.
         */
        protected boolean interrupted() {
            return error() != null;
        }

        /**
         * @return Error if operation was interrupted, otherwise {@code null}.
         */
        protected @Nullable Throwable error() {
            return errRef.get();
        }

        /**
         * @param cacheDir Directory to clean up if the restore procedure fails
         */
        protected void pushDir(File cacheDir) {
            grpDirs.offer(cacheDir);
        }

        /**
         * @return Directory to clean up or {@code null} if nothing to clean up.
         */
        private @Nullable File popDir() {
            return grpDirs.poll();
        }
    }
}
