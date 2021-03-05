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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
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
    private final DistributedProcess<SnapshotRestorePrepareRequest, Boolean> prepareRestoreProc;

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

        if (ctx.cache().context().snapshotMgr().isSnapshotCreating()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "A cluster snapshot operation is in progress."));
        }

        if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), SNAPSHOT_RESTORE_CACHE_GROUP))
            throw new IgniteException("Not all nodes in the cluster support a snapshot restore operation.");

        Collection<ClusterNode> bltNodes = F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            node -> node, (node) -> CU.baselineNode(node, ctx.state().clusterState()));

        Set<UUID> bltNodeIds = new HashSet<>(F.viewReadOnly(bltNodes, F.node2id()));

        ((ClusterGroupAdapter)ctx.cluster().get().forNodeIds(bltNodeIds)).compute().executeAsync(
            new SnapshotRestoreVerificationTask(snpName, cacheGrpNames), null).listen(
            f -> {
                try {
                    Map.Entry<UUID, List<StoredCacheData>> firstNodeRes = F.first(f.get().entrySet());

                    Set<String> foundGrps = firstNodeRes == null ? Collections.emptySet() : firstNodeRes.getValue().stream()
                        .map(v -> v.config().getGroupName() != null ? v.config().getGroupName() : v.config().getName())
                        .collect(Collectors.toSet());

                    if (!foundGrps.containsAll(cacheGrpNames)) {
                        Set<String> missedGroups = new HashSet<>(cacheGrpNames);

                        missedGroups.removeAll(foundGrps);

                        fut.onDone(new IllegalArgumentException(OP_REJECT_MSG + "Cache group(s) was not found in the " +
                            "snapshot [groups=" + missedGroups + ", snapshot=" + snpName + ']'));

                        return;
                    }

                    HashSet<UUID> reqNodes = new HashSet<>(f.get().keySet());

                    reqNodes.add(ctx.localNodeId());

                    SnapshotRestorePrepareRequest req = new SnapshotRestorePrepareRequest(UUID.randomUUID(), snpName,
                        reqNodes, firstNodeRes.getValue(), firstNodeRes.getKey());

                    prepareRestoreProc.start(req.requestId(), req);
                } catch (Throwable t) {
                    fut.onDone(new IgniteException(OP_REJECT_MSG + t.getMessage(), t));
                }
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
    private IgniteInternalFuture<Boolean> prepare(SnapshotRestorePrepareRequest req) {
        if (!req.nodes().contains(ctx.localNodeId()))
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

        opCtx = new SnapshotRestoreContext(req.requestId(), req.snapshotName(), req.nodes(), req.configs());

        fut.listen(f -> opCtx = null);

        if (!allNodesInBaselineAndAlive(req.nodes()))
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster."));

        SnapshotRestoreContext opCtx0 = opCtx;

        GridFutureAdapter<Boolean> retFut = new GridFutureAdapter<>();

        try {
            for (String grpName : opCtx0.groups())
                ensureCacheAbsent(grpName);

            for (StoredCacheData cfg : opCtx0.configs()) {
                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getName());
            }

            if (!ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx0.snapshotName()).exists())
                return new GridFinishedFuture<>();

            boolean updateMeta = ctx.localNodeId().equals(req.updateMetaNodeId());

            ctx.getSystemExecutorService().submit(() -> {
                try {
                    opCtx0.restore(updateMeta);

                    if (!opCtx0.interrupted()) {
                        retFut.onDone(true);

                        return;
                    }

                    log.error("Snapshot restore process has been interrupted " +
                        "[groups=" + opCtx0.groups() + ", snapshot=" + opCtx0.snapshotName() + ']', opCtx0.error());

                    opCtx0.rollback();

                    retFut.onDone(opCtx0.error());

                }
                catch (Throwable t) {
                    retFut.onDone(t);
                }
            });

            return retFut;
        } catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, Boolean> res, Map<UUID, Exception> errs) {
        GridFutureAdapter<Void> fut0 = fut;
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleProcess(fut0, opCtx))
            return;

        Exception failure = F.first(errs.values());

        if (failure == null && !res.keySet().containsAll(opCtx0.nodes())) {
            Set<UUID> leftNodes = new HashSet<>(opCtx0.nodes());

            leftNodes.removeAll(res.keySet());

            failure = new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster [nodeId=" + leftNodes + ']');
        }

        if (failure != null) {
            opCtx.rollback();

            fut0.onDone(failure);

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

        if (log.isInfoEnabled()) {
            log.info("Starting restored caches " +
                "[snapshot=" + opCtx0.snapshotName() +
                ", caches=" + F.viewReadOnly(opCtx0.configs(), c -> c.config().getName()) + ']');
        }

        ctx.cache().dynamicStartCachesByStoredConf(opCtx.configs(), true, true, false, null, true, opCtx0.nodes()).listen(
            f -> {
                if (f.error() != null) {
                    log.error("Unable to start restored caches.", f.error());

                    retFut.onDone(f.error());
                }
                else
                    retFut.onDone(true);
            }
        );

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

        Exception failure = F.first(errs.values());

        if (failure == null && !res.keySet().containsAll(opCtx0.nodes())) {
            Set<UUID> leftNodes = new HashSet<>(opCtx0.nodes());

            leftNodes.removeAll(res.keySet());

            failure = new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster [nodeId=" + leftNodes + ']');
        }

        if (failure != null) {
            if (U.isLocalNodeCoordinator(ctx.discovery()))
                rollbackRestoreProc.start(reqId, new SnapshotRestoreRollbackRequest(reqId, failure));

            return;
        }

        fut0.onDone();
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

        opCtx0.rollback();

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
    private class SnapshotRestoreContext {
        /** Request ID. */
        private final UUID reqId;

        /** Snapshot name. */
        private final String snpName;

        /** Baseline node IDs that must be alive to complete the operation. */
        private final Set<UUID> reqNodes;

        /** List of processed cache IDs. */
        private final Set<Integer> cacheIds = new HashSet<>();

        /** Cache configurations. */
        private final List<StoredCacheData> ccfgs;

        /** Restored cache groups. */
        private final Map<String, List<File>> grps = new ConcurrentHashMap<>();

        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        /**
         * @param reqId Request ID.
         * @param snpName Snapshot name.
         * @param reqNodes Baseline node IDs that must be alive to complete the operation.
         * @param cfgs Stored cache configurations.
         */
        protected SnapshotRestoreContext(UUID reqId, String snpName, Set<UUID> reqNodes, List<StoredCacheData> cfgs) {
            ccfgs = new ArrayList<>(cfgs);

            for (StoredCacheData cacheData : cfgs) {
                String cacheName = cacheData.config().getName();

                cacheIds.add(CU.cacheId(cacheName));

                boolean shared = cacheData.config().getGroupName() != null;

                grps.computeIfAbsent(shared ? cacheData.config().getGroupName() : cacheName, v -> new ArrayList<>());

                if (shared)
                    cacheIds.add(CU.cacheId(cacheData.config().getGroupName()));
            }

            this.reqId = reqId;
            this.reqNodes = new HashSet<>(reqNodes);
            this.snpName = snpName;
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
            return grps.keySet();
        }

        /**
         * @param name Cache name.
         * @return {@code True} if the cache with the specified name is currently being restored.
         */
        protected boolean containsCache(String name) {
            return cacheIds.contains(CU.cacheId(name));
        }

        /** @return Cache configurations. */
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
         * Restore specified cache groups from the local snapshot directory.
         *
         * @param updateMetadata Update binary metadata flag.
         * @throws IgniteCheckedException If failed.
         */
        protected void restore(boolean updateMetadata) throws IgniteCheckedException {
            if (interrupted())
                return;

            IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();
            String folderName = ctx.pdsFolderResolver().resolveFolders().folderName();

            if (updateMetadata) {
                File binDir = binaryWorkDir(snapshotMgr.snapshotLocalDir(snpName).getAbsolutePath(), folderName);

                if (!binDir.exists()) {
                    throw new IgniteCheckedException("Unable to update cluster metadata from snapshot, " +
                        "directory doesn't exists [snapshot=" + snpName + ", dir=" + binDir + ']');
                }

                ctx.cacheObjects().updateMetadata(binDir, this::interrupted);
            }

            for (File grpDir : snapshotMgr.snapshotCacheDirectories(snpName, folderName)) {
                String grpName = FilePageStoreManager.cacheGroupName(grpDir);

                if (!groups().contains(grpName))
                    continue;

                snapshotMgr.restoreCacheGroupFiles(snpName, grpName, grpDir, this::interrupted, grps.get(grpName));
            }
        }

        /**
         * Rollback changes made by process in specified cache group.
         */
        protected void rollback() {
            if (groups().isEmpty())
                return;

            List<String> grpNames = new ArrayList<>(groups());

            for (String grpName : grpNames) {
                List<File> files = grps.remove(grpName);

                if (files == null)
                    continue;

                List<File> dirs = new ArrayList<>();

                for (File file : files) {
                    if (!file.exists())
                        continue;

                    if (file.isDirectory())
                        dirs.add(file);

                    if (!file.delete())
                        log.warning("Unable to delete a file created during a cache restore operation [file=" + file + ']');
                }

                for (File dir : dirs) {
                    if (!dir.delete())
                        log.warning("Unable to delete a folder created during a cache restore operation [file=" + dir + ']');
                }
            }
        }
    }
}
