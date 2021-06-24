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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.ClusterSnapshotFuture;
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
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.SNAPSHOT_RESTORE_CACHE_GROUP;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreProcess {
    /** Temporary cache directory prefix. */
    public static final String TMP_CACHE_DIR_PREFIX = "_tmp_snp_restore_";

    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotOperationRequest, ArrayList<StoredCacheData>> prepareRestoreProc;

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

        for (File dir : dbDir.listFiles(dir -> dir.isDirectory() && dir.getName().startsWith(TMP_CACHE_DIR_PREFIX))) {
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

            if (ctx.cache().context().snapshotMgr().isSnapshotCreating())
                throw new IgniteException(OP_REJECT_MSG + "A cluster snapshot operation is in progress.");

            synchronized (this) {
                if (restoringSnapshotName() != null)
                    throw new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed.");

                fut = new ClusterSnapshotFuture(UUID.randomUUID(), snpName);

                fut0 = fut;
            }
        }
        catch (IgniteException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }

        ctx.cache().context().snapshotMgr().checkSnapshot(snpName, cacheGrpNames).listen(f -> {
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

                if (!entry.getKey().consistentId().equals(meta.consistentId()))
                    continue;

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

            Collection<String> bltNodes = F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
                node -> node.consistentId().toString(), (node) -> CU.baselineNode(node, ctx.state().clusterState()));

            snpBltNodes.removeAll(bltNodes);

            if (!snpBltNodes.isEmpty()) {
                finishProcess(fut0.rqId, new IgniteIllegalStateException(OP_REJECT_MSG + "Some nodes required to " +
                    "restore a cache group are missing [nodeId(s)=" + snpBltNodes + ", snapshot=" + snpName + ']'));

                return;
            }

            IdleVerifyResultV2 res = f.result().idleVerifyResult();

            if (!F.isEmpty(res.exceptions()) || res.hasConflicts()) {
                StringBuilder sb = new StringBuilder();

                res.print(sb::append, true);

                finishProcess(fut0.rqId, new IgniteException(sb.toString()));

                return;
            }

            SnapshotOperationRequest req = new SnapshotOperationRequest(
                fut0.rqId, F.first(dataNodes), snpName, cacheGrpNames, dataNodes);

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
     * Check if the cache or group with the specified name is currently being restored from the snapshot.
     *
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @return {@code True} if the cache or group with the specified name is currently being restored.
     */
    public boolean isRestoring(String cacheName, @Nullable String grpName) {
        assert cacheName != null;

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
            log.error("Failed to restore snapshot cache group [reqId=" + reqId + ']', err);
        else if (log.isInfoEnabled())
            log.info("Successfully restored cache group(s) from the snapshot [reqId=" + reqId + ']');

        SnapshotRestoreContext opCtx0 = opCtx;

        if (opCtx0 != null && reqId.equals(opCtx0.reqId))
            opCtx = null;

        synchronized (this) {
            ClusterSnapshotFuture fut0 = fut;

            if (fut0 != null && reqId.equals(fut0.rqId)) {
                fut = null;

                ctx.getSystemExecutorService().submit(() -> fut0.onDone(null, err));
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
    private IgniteInternalFuture<ArrayList<StoredCacheData>> prepare(SnapshotOperationRequest req) {
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

            SnapshotRestoreContext opCtx0 = prepareContext(req);

            synchronized (this) {
                opCtx = opCtx0;

                ClusterSnapshotFuture fut0 = fut;

                if (fut0 != null && fut0.interruptEx != null)
                    opCtx0.err.compareAndSet(null, fut0.interruptEx);
            }

            if (opCtx0.dirs.isEmpty())
                return new GridFinishedFuture<>();

            // Ensure that shared cache groups has no conflicts.
            for (StoredCacheData cfg : opCtx0.cfgs.values()) {
                ensureCacheAbsent(cfg.config().getName());

                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getGroupName());
            }

            if (log.isInfoEnabled()) {
                log.info("Starting local snapshot restore operation" +
                    " [reqId=" + req.requestId() +
                    ", snapshot=" + req.snapshotName() +
                    ", cache(s)=" + F.viewReadOnly(opCtx0.cfgs.values(), data -> data.config().getName()) + ']');
            }

            Consumer<Throwable> errHnd = (ex) -> opCtx.err.compareAndSet(null, ex);
            BooleanSupplier stopChecker = () -> opCtx.err.get() != null;
            GridFutureAdapter<ArrayList<StoredCacheData>> retFut = new GridFutureAdapter<>();

            if (ctx.isStopping())
                throw new NodeStoppingException("Node is stopping.");

            opCtx0.stopFut = new IgniteFutureImpl<>(retFut.chain(f -> null));

            restoreAsync(opCtx0.snpName, opCtx0.dirs, ctx.localNodeId().equals(req.operationalNodeId()), stopChecker, errHnd)
                .thenAccept(res -> {
                    try {
                        Throwable err = opCtx.err.get();

                        if (err != null)
                            throw err;

                        for (File src : opCtx0.dirs)
                            Files.move(formatTmpDirName(src).toPath(), src.toPath(), StandardCopyOption.ATOMIC_MOVE);
                    }
                    catch (Throwable t) {
                        log.error("Unable to restore cache group(s) from the snapshot " +
                            "[reqId=" + opCtx.reqId + ", snapshot=" + opCtx.snpName + ']', t);

                        retFut.onDone(t);

                        return;
                    }

                    retFut.onDone(new ArrayList<>(opCtx.cfgs.values()));
                });

            return retFut;
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
    private File formatTmpDirName(File cacheDir) {
        return new File(cacheDir.getParent(), TMP_CACHE_DIR_PREFIX + cacheDir.getName());
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
        Consumer<Throwable> errHnd
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
                catch (Throwable t) {
                    errHnd.accept(t);
                }
            }, snapshotMgr.snapshotExecutorService()));
        }

        for (File cacheDir : dirs) {
            File tmpCacheDir = formatTmpDirName(cacheDir);
            File snpCacheDir = new File(ctx.cache().context().snapshotMgr().snapshotLocalDir(snpName),
                Paths.get(databaseRelativePath(pdsFolderName), cacheDir.getName()).toString());

            assert snpCacheDir.exists() : "node=" + ctx.localNodeId() + ", dir=" + snpCacheDir;

            for (File snpFile : snpCacheDir.listFiles()) {
                futs.add(CompletableFuture.runAsync(() -> {
                    if (stopChecker.getAsBoolean())
                        return;

                    try {
                        if (Thread.interrupted())
                            throw new IgniteInterruptedCheckedException("Thread has been interrupted.");

                        File target = new File(tmpCacheDir, snpFile.getName());

                        if (log.isDebugEnabled()) {
                            log.debug("Copying file from the snapshot " +
                                "[snapshot=" + snpName +
                                ", src=" + snpFile +
                                ", target=" + target + "]");
                        }

                        IgniteSnapshotManager.copy(snapshotMgr.ioFactory(), snpFile, target, snpFile.length());
                    }
                    catch (Throwable t) {
                        errHnd.accept(t);
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
    private SnapshotRestoreContext prepareContext(SnapshotOperationRequest req) throws IgniteCheckedException {
        if (opCtx != null) {
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

        // Collect the cache configurations and prepare a temporary directory for copying files.
        // Metastorage can be restored only manually by directly copying files.
        for (File snpCacheDir : cctx.snapshotMgr().snapshotCacheDirectories(req.snapshotName(), meta.folderName(),
            name -> !METASTORAGE_CACHE_NAME.equals(name)))
        {
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

            if (!tmpCacheDir.mkdir()) {
                throw new IgniteCheckedException("Unable to restore cache group, cannot create temp directory " +
                    "[group=" + grpName + ", dir=" + tmpCacheDir + ']');
            }

            cacheDirs.add(cacheDir);

            pageStore.readCacheConfigurations(snpCacheDir, cfgsByName);
        }

        Map<Integer, StoredCacheData> cfgsById =
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

        assert opCtx0 != null || failure != null : "Context has not been created on the node " + ctx.localNodeId();

        if (opCtx0 == null || !reqId.equals(opCtx0.reqId)) {
            finishProcess(reqId, failure);

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
        return ctx.cache().dynamicStartCachesByStoredConf(ccfgs, true, true, false, IgniteUuid.fromUuid(reqId));
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

        /** Graceful shutdown future. */
        private volatile IgniteFuture<?> stopFut;

        /**
         * @param req Request to prepare cache group restore from the snapshot.
         * @param dirs List of cache group names to restore from the snapshot.
         * @param cfgs Cache ID to configuration mapping.
         */
        protected SnapshotRestoreContext(SnapshotOperationRequest req, Collection<File> dirs,
            Map<Integer, StoredCacheData> cfgs) {
            reqId = req.requestId();
            snpName = req.snapshotName();
            nodes = new HashSet<>(req.nodes());

            this.dirs = dirs;
            this.cfgs = cfgs;
        }
    }
}
