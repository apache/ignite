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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
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

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreCacheGroupProcess {
    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> prepareRestoreProc;

    /** Cache group restore cache start phase. */
    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> cacheStartProc;

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
    public SnapshotRestoreCacheGroupProcess(GridKernalContext ctx) {
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

        IgniteInternalFuture<Void> fut0 = fut;

        if (!fut0.isDone()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "The previous snapshot restore operation was not completed."));
        }

        DiscoveryDataClusterState clusterState = ctx.state().clusterState();

        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        if (!clusterState.hasBaselineTopology()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "The baseline topology is not configured for cluster."));
        }

        if (ctx.cache().context().snapshotMgr().isSnapshotCreating()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "A cluster snapshot operation is in progress."));
        }

        Set<UUID> bltNodeIds = baselineNodes();

        fut = new GridFutureAdapter<>();

        ((ClusterGroupAdapter)ctx.cluster().get().forNodeIds(bltNodeIds)).compute().executeAsync(
            new SnapshotRestoreVerificatioTask(), new SnapshotRestoreVerificationArg(snpName, cacheGrpNames)).listen(
            f -> {
                try {
                    SnapshotRestoreVerificationResult res = f.get();

                    Set<String> foundGrps = res == null ? Collections.emptySet() : res.configs().stream()
                        .map(v -> v.config().getGroupName() != null ? v.config().getGroupName() : v.config().getName())
                        .collect(Collectors.toSet());

                    if (!foundGrps.containsAll(cacheGrpNames)) {
                        Set<String> missedGroups = new HashSet<>(cacheGrpNames);

                        missedGroups.removeAll(foundGrps);

                        fut.onDone(new IllegalArgumentException(OP_REJECT_MSG +
                            "Cache group(s) was not found in the snapshot [groups=" +
                            F.concat(missedGroups, ", ") + ", snapshot=" + snpName + ']'));

                        return;
                    }

                    SnapshotRestoreRequest req = new SnapshotRestoreRequest(
                        UUID.randomUUID(), snpName, bltNodeIds, res.configs(), res.localNodeId());

                    prepareRestoreProc.start(req.requestId(), req);
                } catch (Throwable t) {
                    fut.onDone(new IgniteException(OP_REJECT_MSG + t.getMessage(), t));
                }
            }
        );

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * @return Set of current baseline node IDs.
     */
    private Set<UUID> baselineNodes() {
        return new HashSet<>(F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id(), (node) -> CU.baselineNode(node, ctx.state().clusterState())));
    }

    /**
     * Check if the cache group restore process is currently running.
     *
     * @return {@code True} if cache group restore process is currently running.
     */
    public boolean inProgress(@Nullable String cacheName) {
        IgniteInternalFuture<Void> fut0 = fut;

        return !staleFuture(fut0) && (cacheName == null || opCtx.containsCache(cacheName));
    }

    /**
     * @param fut The future of cache snapshot restore operation.
     * @return {@code True} if the future completed or not initiated.
     */
    public boolean staleFuture(IgniteInternalFuture<Void> fut) {
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
     * @throws IllegalStateException If cache with the specified name already exists.
     */
    private void ensureCacheAbsent(String name) throws IllegalStateException {
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
    private IgniteInternalFuture<SnapshotRestoreResponse> prepare(SnapshotRestoreRequest req) {
        if (!req.nodes().contains(ctx.localNodeId()))
            return new GridFinishedFuture<>();

        if (inProgress(null)) {
            return new GridFinishedFuture<>(
                new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed."));
        }

        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            return new GridFinishedFuture<>(new IllegalStateException(OP_REJECT_MSG + "The cluster should be active."));

        // Skip creating future on initiator.
        if (fut.isDone())
            fut = new GridFutureAdapter<>();

        opCtx = new SnapshotRestoreContext(req.requestId(), req.snapshotName(), new HashSet<>(req.nodes()), req.configs(), ctx);

        fut.listen(f -> opCtx = null);

        if (!baselineNodes().containsAll(req.nodes())) {
            return new GridFinishedFuture<>(
                new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster."));
        }

        SnapshotRestoreContext opCtx0 = opCtx;

        GridFutureAdapter<SnapshotRestoreResponse> retFut = new GridFutureAdapter<>();

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
                    opCtx0.restore(updateMeta, opCtx0::interrupted);

                    if (opCtx0.interrupted()) {
                        log.error("Snapshot restore process has been interrupted " +
                            "[groups=" + opCtx0.groups() + ", snapshot=" + opCtx0.snapshotName() + ']', opCtx0.error());

                        opCtx0.rollback();

                        retFut.onDone(opCtx0.error());
                    }
                    else
                        retFut.onDone();
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
    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestoreResponse> res, Map<UUID, Exception> errs) {
        GridFutureAdapter<Void> fut0 = fut;

        if (fut0.isDone() || !reqId.equals(opCtx.requestId()))
            return;

        Exception failure = F.first(errs.values());

        if (failure != null) {
            opCtx.rollback();

            fut0.onDone(failure);

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            cacheStartProc.start(reqId, new SnapshotRestoreRequest(reqId, null, null, null, null));
    }

    /**
     * @param req Request to start restored cache groups.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestoreResponse> cacheStart(SnapshotRestoreRequest req) {
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleFuture(fut))
            return new GridFinishedFuture<>();

        if (!req.requestId().equals(opCtx0.requestId()))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return new GridFinishedFuture<>();

        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            return new GridFinishedFuture<>(new IgniteCheckedException(OP_REJECT_MSG + "The cluster should be active."));

        if (opCtx0.interrupted())
            return new GridFinishedFuture<>(opCtx0.error());

        if (!baselineNodes().containsAll(opCtx0.nodes()))
            return new GridFinishedFuture<>(new IgniteException(OP_REJECT_MSG + "Server node(s) has left the cluster."));

        GridFutureAdapter<SnapshotRestoreResponse> retFut = new GridFutureAdapter<>();

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
                    retFut.onDone();
            }
        );

        return retFut;
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishCacheStart(UUID reqId, Map<UUID, SnapshotRestoreResponse> res, Map<UUID, Exception> errs) {
        GridFutureAdapter<Void> fut0 = fut;
        SnapshotRestoreContext opCtx0 = opCtx;

        if (staleFuture(fut0) || !reqId.equals(opCtx0.requestId()))
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
        if (staleFuture(fut) || !req.requestId().equals(opCtx.requestId()))
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx0 = opCtx;

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

        if (staleFuture(fut0) || !reqId.equals(opCtx.requestId()))
            return;

        SnapshotRestoreRollbackResponse resp = F.first(F.viewReadOnly(res.values(), v -> v, Objects::nonNull));

        fut0.onDone(resp.error());
    }
}
