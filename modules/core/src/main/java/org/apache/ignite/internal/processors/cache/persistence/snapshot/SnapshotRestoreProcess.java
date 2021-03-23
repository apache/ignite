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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
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
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
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
    private final DistributedProcess<SnapshotRestoreRequest, ArrayList<StoredCacheData>> prepareRestoreProc;

    /** Cache group restore cache start phase. */
    private final DistributedProcess<UUID, Boolean> cacheStartProc;

    /** Cache group restore rollback phase. */
    private final DistributedProcess<UUID, Boolean> rollbackRestoreProc;

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

            if (ctx.cache().context().snapshotMgr().isSnapshotCreating())
                throw new IgniteException(OP_REJECT_MSG + "A cluster snapshot operation is in progress.");

            synchronized (this) {
                GridFutureAdapter<Void> fut0 = fut;

                if (isRestoring() || (fut0 != null && !fut0.isDone()))
                    throw new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed.");

                fut = new GridFutureAdapter<>();
            }
        } catch (IgniteException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }

        ctx.cache().context().snapshotMgr().collectSnapshotMetadata(snpName).listen(
            f -> {
                if (f.error() != null) {
                    fut.onDone(f.error());

                    return;
                }

                Set<UUID> dataNodes = new HashSet<>();
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

                ctx.cache().context().snapshotMgr().runSnapshotVerfification(metas).listen(
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

                        SnapshotRestoreRequest req = new SnapshotRestoreRequest(UUID.randomUUID(),
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
    public boolean isRestoring() {
        return opCtx != null;
    }

    /**
     * Check if the cache or group with the specified name is currently being restored from the snapshot.
     *
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @return {@code True} if the cache or group with the specified name is currently being restored.
     */
    public boolean isRestoring(String cacheName, @Nullable String grpName) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null)
            return false;

        Map<Integer, StoredCacheData> cacheCfgs = opCtx0.cfgs;

        int cacheId = CU.cacheId(cacheName);

        if (cacheCfgs.containsKey(cacheId))
            return true;

        for (File grpDir : opCtx0.dirs) {
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
    private void finishProcess(@Nullable Exception err) {
        SnapshotRestoreContext opCtx0 = opCtx;

        String details = opCtx0 == null ? "" : " [reqId=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ']';

        if (err != null)
            log.error("Failed to restore snapshot cache group" + details, err);
        else if (log.isInfoEnabled())
            log.info("Successfully restored cache group(s) from the snapshot" + details);

        opCtx = null;

        GridFutureAdapter<Void> fut0 = fut;

        if (fut0 != null)
            fut0.onDone(null, err);
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
            throw new IgniteIllegalStateException("Cache \"" + name +
                "\" should be destroyed manually before perform restore operation.");
        }
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @return Result future.
     */
    private IgniteInternalFuture<ArrayList<StoredCacheData>> prepare(SnapshotRestoreRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        try {
            DiscoveryDataClusterState state = ctx.state().clusterState();

            if (state.state() != ClusterState.ACTIVE || state.transition())
                throw new IgniteCheckedException(OP_REJECT_MSG + "The cluster should be active.");

            if (ctx.cache().context().snapshotMgr().isSnapshotCreating())
                throw new IgniteCheckedException(OP_REJECT_MSG + "A cluster snapshot operation is in progress.");

            for (UUID nodeId : req.nodes()) {
                ClusterNode node = ctx.discovery().node(nodeId);

                if (node == null || !CU.baselineNode(node, state) || !ctx.discovery().alive(node)) {
                    throw new IgniteCheckedException(
                        OP_REJECT_MSG + "Required node has left the cluster [nodeId-" + nodeId + ']');
                }
            }

            for (String grpName : req.groups())
                ensureCacheAbsent(grpName);

            opCtx = prepareContext(req);

            SnapshotRestoreContext opCtx0 = opCtx;

            if (opCtx0.dirs.isEmpty())
                return new GridFinishedFuture<>();

            // Ensure that shared cache groups has no conflicts.
            for (StoredCacheData cfg : opCtx0.cfgs.values()) {
                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getName());
            }

            if (log.isInfoEnabled()) {
                log.info("Starting local snapshot restore operation [reqId=" + req.requestId() +
                    ", snapshot=" + req.snapshotName() + ", group(s)=" + req.groups() + ']');
            }

            boolean updateMeta = ctx.localNodeId().equals(req.updateMetaNodeId());
            Consumer<Exception> errHnd = (ex) -> opCtx.err.compareAndSet(null, ex);
            BooleanSupplier stopChecker = () -> {
                if (opCtx.err.get() != null)
                    return true;

                if (Thread.currentThread().isInterrupted()) {
                    errHnd.accept(new IgniteInterruptedCheckedException("Thread has been interrupted."));

                    return true;
                }

                return false;
            };

            GridFutureAdapter<ArrayList<StoredCacheData>> retFut = new GridFutureAdapter<>();

            restoreAsync(opCtx0.snpName, opCtx0.dirs, updateMeta, stopChecker, errHnd).thenAccept(res -> {
                Throwable err = opCtx.err.get();

                if (err != null) {
                    log.error("Unable to restore cache group(s) from the snapshot " +
                        "[reqId=" + opCtx.reqId + ", snapshot=" + opCtx.snpName + ']', err);

                    retFut.onDone(err);
                } else
                    retFut.onDone(new ArrayList<>(opCtx.cfgs.values()));
            });

            return retFut;
        } catch (IgniteIllegalStateException | IgniteCheckedException | RejectedExecutionException e) {
            log.error("Unable to restore cache group(s) from the snapshot " +
                "[reqId=" + req.requestId() + ", snapshot=" + req.snapshotName() + ']', e);

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Copy partition files and update binary metadata.
     *
     * @param snpName Snapshot name.
     * @param dirs Cache directories to restore from the snapshot.
     * @param updateMeta Update binary metadata flag.
     * @param stopChecker Process interrupt checker.
     * @param errHnd Error handler.
     * @throws IgniteCheckedException If failed.
     */
    private CompletableFuture<Void> restoreAsync(
        String snpName,
        Collection<File> dirs,
        boolean updateMeta,
        BooleanSupplier stopChecker,
        Consumer<Exception> errHnd
    ) throws IgniteCheckedException {
        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();
        String pdsFolderName = ctx.pdsFolderResolver().resolveFolders().folderName();

        List<CompletableFuture<Void>> futs = new ArrayList<>();

        if (updateMeta) {
            File binDir = binaryWorkDir(snapshotMgr.snapshotLocalDir(snpName).getAbsolutePath(), pdsFolderName);

            futs.add(CompletableFuture.runAsync(() -> {
                try {
                    ctx.cacheObjects().updateMetadata(binDir, stopChecker);
                }
                catch (IgniteCheckedException e) {
                    errHnd.accept(e);
                }
            }, snapshotMgr.snapshotExecutorService()));
        }

        for (File cacheDir : dirs) {
            File snpCacheDir = new File(ctx.cache().context().snapshotMgr().snapshotLocalDir(snpName),
                Paths.get(databaseRelativePath(pdsFolderName), cacheDir.getName()).toString());

            assert snpCacheDir.exists() : "node=" + ctx.localNodeId() + ", dir=" + snpCacheDir;

            for (File snpFile : snpCacheDir.listFiles()) {
                futs.add(CompletableFuture.runAsync(() -> {
                    if (stopChecker.getAsBoolean())
                        return;

                    File target = new File(cacheDir, snpFile.getName());

                    if (log.isDebugEnabled()) {
                        log.debug("Copying file from the snapshot " +
                            "[snapshot=" + snpName +
                            ", src=" + snpFile +
                            ", target=" + target + "]");
                    }

                    try {
                        Files.copy(snpFile.toPath(), target.toPath());
                    }
                    catch (IOException e) {
                        errHnd.accept(e);
                    }
                }, ctx.cache().context().snapshotMgr().snapshotExecutorService()));
            }
        }

        int futsSize = futs.size();

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]));
    }

    /**
     * @param req Request to prepare cache group restore from the snapshot.
     * @return Snapshot restore operation context.
     * @throws IgniteCheckedException If failed.
     */
    private SnapshotRestoreContext prepareContext(SnapshotRestoreRequest req) throws IgniteCheckedException {
        if (isRestoring()) {
            throw new IgniteCheckedException(OP_REJECT_MSG +
                "The previous snapshot restore operation was not completed.");
        }

        GridCacheSharedContext<?, ?> cctx = ctx.cache().context();

        SnapshotMetadata meta = F.first(cctx.snapshotMgr().readSnapshotMetadatas(req.snapshotName()));

        if (meta == null || !meta.consistentId().equals(cctx.localNode().consistentId().toString()))
            return new SnapshotRestoreContext(req, Collections.emptyList(), Collections.emptyMap());

        if (meta.pageSize() != cctx.database().pageSize()) {
            throw new IgniteCheckedException("Incompatible memory page size " +
                "[snapshotPageSize=" + meta.pageSize() +
                ", local=" + cctx.database().pageSize() +
                ", snapshot=" + req.snapshotName() +
                ", nodeId=" + cctx.localNodeId() + ']');
        }

        List<File> cacheDirs = new ArrayList<>();
        Map<String, StoredCacheData> cfgsByName = new HashMap<>();
        FilePageStoreManager pageStore = (FilePageStoreManager)cctx.pageStore();

        // Collect cache configuration(s) and verify cache groups page size.
        for (File snpCacheDir : cctx.snapshotMgr().snapshotCacheDirectories(req.snapshotName(), meta.folderName())) {
            String grpName = FilePageStoreManager.cacheGroupName(snpCacheDir);

            if (!req.groups().contains(grpName))
                continue;

            File cacheDir = pageStore.cacheWorkDir(snpCacheDir.getName().startsWith(CACHE_GRP_DIR_PREFIX), grpName);

            if (!cacheDir.exists())
                cacheDir.mkdir();
            else if (cacheDir.list().length > 0) {
                throw new IgniteCheckedException("Unable to restore cache group, directory is not empty " +
                    "[group=" + grpName + ", dir=" + cacheDir + ']');
            }

            cacheDirs.add(cacheDir);

            pageStore.readCacheConfigurations(snpCacheDir, cfgsByName);
        }

        Map<Integer, StoredCacheData> cfgsById = cfgsByName.isEmpty() ? Collections.emptyMap() :
            cfgsByName.values().stream().collect(Collectors.toMap(v -> CU.cacheId(v.config().getName()), v -> v));

        return new SnapshotRestoreContext(req, cacheDirs, cfgsById);
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, ArrayList<StoredCacheData>> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        Exception failure = F.first(errs.values());

        assert opCtx0 != null || failure != null : ctx.localNodeId();

        if (opCtx0 == null) {
            finishProcess(failure);

            return;
        }

        if (failure == null)
            failure = checkNodeLeft(opCtx0.nodes, res.keySet());

        // Context has been created - should rollback changes cluster-wide.
        if (failure != null) {
            opCtx0.err.compareAndSet(null, failure);

            if (U.isLocalNodeCoordinator(ctx.discovery()))
                rollbackRestoreProc.start(reqId, reqId);

            return;
        }

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
    }

    /**
     * @param reqId Request ID.
     * @return Result future.
     */
    private IgniteInternalFuture<Boolean> cacheStart(UUID reqId) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 == null) {
            return new GridFinishedFuture<>(new IgniteIllegalStateException("Context has not been created on server " +
                "node during prepare operation [reqId=" + reqId + ", nodeId=" + ctx.localNodeId() + ']'));
        }

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
        return ctx.cache().dynamicStartCachesByStoredConf(ccfgs, true, true, false, null, true, opCtx0.nodes);
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
            finishProcess();

            return;
        }

        opCtx0.err.compareAndSet(null, failure);

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, reqId);
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

        if (F.isEmpty(opCtx0.dirs))
            return new GridFinishedFuture<>();

        if (log.isInfoEnabled()) {
            log.info("Performing local rollback routine for restored cache groups " +
                "[reqId=" + opCtx0.reqId + ", snapshot=" + opCtx0.snpName + ']');
        }

        for (File cacheDir : opCtx0.dirs) {
            if (!cacheDir.exists())
                continue;

            if (log.isInfoEnabled())
                log.info("Cleaning up directory " + cacheDir);

            U.delete(cacheDir);
        }

        return new GridFinishedFuture<>(true);
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishRollback(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        if (ctx.clientNode())
            return;

        SnapshotRestoreContext opCtx0 = opCtx;

        if (!res.keySet().containsAll(opCtx0.nodes)) {
            Set<UUID> leftNodes = new HashSet<>(opCtx0.nodes);

            leftNodes.removeAll(res.keySet());

            log.warning("Some of the nodes left the cluster and were unable to complete the rollback" +
                " operation [reqId=" + reqId + ", snapshot=" + opCtx0.snpName + ", node(s)=" + leftNodes + ']');
        }

        finishProcess(opCtx0.err.get());
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
        private final AtomicReference<Exception> err = new AtomicReference<>();

        /** Cache ID to configuration mapping. */
        private volatile Map<Integer, StoredCacheData> cfgs;

        /**
         * @param req Request to prepare cache group restore from the snapshot.
         * @param dirs List of cache group names to restore from the snapshot.
         * @param cfgs Cache ID to configuration mapping.
         */
        protected SnapshotRestoreContext(SnapshotRestoreRequest req, Collection<File> dirs,
            Map<Integer, StoredCacheData> cfgs) {
            reqId = req.requestId();
            snpName = req.snapshotName();
            nodes = new HashSet<>(req.nodes());

            this.dirs = dirs;
            this.cfgs = cfgs;
        }
    }
}
