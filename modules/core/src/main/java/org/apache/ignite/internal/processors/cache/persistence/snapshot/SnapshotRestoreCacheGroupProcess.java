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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreCacheGroupProcess {
    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotRestorePrepareRequest, SnapshotRestorePrepareResponse> prepareRestoreProc;

    /** Cache group restore perform phase. */
    private final DistributedProcess<SnapshotRestorePerformRequest, SnapshotRestorePerformResponse> performRestoreProc;

    /** Logger. */
    private final IgniteLogger log;

    /** The future to be completed when the cache restore process is complete. */
    private volatile RestoreSnapshotFuture fut = new RestoreSnapshotFuture();

    /**
     * @param ctx Kernal context.
     */
    public SnapshotRestoreCacheGroupProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        prepareRestoreProc =
            new DistributedProcess<>(ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, this::prepare, this::finishPrepare);
        performRestoreProc =
            new DistributedProcess<>(ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM, this::perform, this::finishPerform);

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

        if (!clusterState.state().active())
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        if (!clusterState.hasBaselineTopology()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "The baseline topology is not configured for cluster."));
        }

        if (ctx.cache().context().snapshotMgr().isSnapshotCreating()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "A cluster snapshot operation is in progress."));
        }

        Set<UUID> srvNodeIds = new HashSet<>(F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id(), (node) -> CU.baselineNode(node, ctx.state().clusterState())));

        SnapshotRestorePrepareRequest req =
            new SnapshotRestorePrepareRequest(UUID.randomUUID(), snpName, cacheGrpNames, srvNodeIds);

        fut = new RestoreSnapshotFuture();

        prepareRestoreProc.start(req.requestId(), req);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Check if the cache group restore process is currently running.
     *
     * @return {@code True} if cache group restore process is currently running.
     */
    public boolean inProgress(@Nullable String cacheName) {
        RestoreSnapshotFuture fut0 = fut;

        return !staleFuture(fut0) && (cacheName == null || fut0.context().containsCache(cacheName));
    }

    /**
     * @param fut The future of cache snapshot restore operation.
     * @return {@code True} if the future completed or not initiated.
     */
    public boolean staleFuture(RestoreSnapshotFuture fut) {
        return fut.isDone() || fut.context() == null;
    }

    /**
     * @param cacheName Started cache name.
     * @param grpName Started cache group name.
     * @param err Error if any.
     */
    public void handleCacheStart(String cacheName, @Nullable String grpName, @Nullable Throwable err) {
        RestoreSnapshotFuture fut0 = fut;

        if (staleFuture(fut0))
            return;

        fut0.context().processCacheStart(cacheName, grpName, err, ctx.getSystemExecutorService(), fut0);
    }

    /**
     * Node left callback.
     *
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        RestoreSnapshotFuture fut0 = fut;

        if (staleFuture(fut0))
            return;

        if (fut0.context().nodes().contains(leftNodeId)) {
            fut0.onDone(new IgniteException(OP_REJECT_MSG +
                "Baseline node has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Abort the currently running restore procedure (if any).
     *
     * @param reason Interruption reason.
     */
    public void stop(String reason) {
        RestoreSnapshotFuture fut0 = fut;

        if (staleFuture(fut0))
            return;

        fut0.onDone(new IgniteCheckedException("Restore process has been interrupted: " + reason));
    }

    /**
     * Cache group napshot restore single node validation phase.
     *
     * @param req Request to prepare snapshot restore.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestorePrepareResponse> prepare(SnapshotRestorePrepareRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        if (inProgress(null)) {
            return new GridFinishedFuture<>(
                new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed."));
        }

        if (!ctx.state().clusterState().state().active())
            return new GridFinishedFuture<>(new IllegalStateException(OP_REJECT_MSG + "The cluster should be active."));

        // Skip creating future on initiator.
        if (fut.isDone())
            fut = new RestoreSnapshotFuture();

        IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();

        fut.init(new SnapshotRestoreContext(
            req.requestId(), req.snapshotName(), req.requiredNodes(), req.groups(), snpMgr));

        if (!snpMgr.snapshotLocalDir(req.snapshotName()).exists())
            return new GridFinishedFuture<>();

        GridFutureAdapter<SnapshotRestorePrepareResponse> retFut = new GridFutureAdapter<>();

        ctx.getSystemExecutorService().submit(() -> {
            try {
                SnapshotRestorePrepareResponse res = prepare0(req);

                retFut.onDone(res);
            }
            catch (BinaryObjectException e) {
                log.warning(OP_REJECT_MSG + "Incompatible binary types found", e);

                retFut.onDone(new IgniteException(OP_REJECT_MSG + "Incompatible binary types found: " + e.getMessage()));
            }
            catch (Throwable t) {
                retFut.onDone(t);
            }
        });

        return retFut;
    }

    /**
     * Reads locally stored cache configurations and verifies that the binary metadata can be merged from the snapshot.
     *
     * @param req Request to prepare snapshot restore.
     * @return Response to prepare snapshot restore.
     * @throws IgniteCheckedException If failed.
     */
    private @Nullable SnapshotRestorePrepareResponse prepare0(
        SnapshotRestorePrepareRequest req
    ) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Preparing to restore cache groups [groups=" + F.concat(req.groups(), ", ") + ']');

        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();

        Map<String, StoredCacheData> cacheCfgs = new HashMap<>();
        Map<String, Set<Integer>> partIds = new HashMap<>();

        // Collect cache configuration(s).
        for (String grpName : req.groups()) {
            File cacheDir = snapshotMgr.resolveSnapshotCacheDir(req.snapshotName(), grpName);

            if (!cacheDir.exists())
                return null;

            FilePageStoreManager pageStoreMgr = (FilePageStoreManager)ctx.cache().context().pageStore();

            pageStoreMgr.readCacheConfigurations(cacheDir, cacheCfgs);

            partIds.put(grpName, pageStoreMgr.scanPartitionIds(cacheDir));
        }

        if (cacheCfgs.isEmpty())
            return null;

        Set<String> cacheNames = new HashSet<>(req.groups());

        cacheNames.addAll(cacheCfgs.keySet());

        for (String cacheName : cacheNames)
            ensureCacheAbsent(cacheName);

        RestoreSnapshotFuture fut0 = fut;

        ctx.cache().context().snapshotMgr().mergeSnapshotMetadata(req.snapshotName(), true, false, fut0::interrupted);

        return new SnapshotRestorePrepareResponse(new ArrayList<>(cacheCfgs.values()), partIds);
    }

    /**
     * Ensures that a cache with the specified name does not exist locally.
     *
     * @param name Cache name.
     * @throws IllegalStateException If cache with the specified name already exists.
     */
    private void ensureCacheAbsent(String name) throws IllegalStateException {
        int id = CU.cacheId(name);

        if (ctx.cache().cacheDescriptor(id) != null || ctx.cache().cacheGroupDescriptor(id) != null) {
            throw new IllegalStateException("Cache \"" + name +
                "\" should be destroyed manually before perform restore operation.");
        }
    }

    /**
     * Completes the verification phase and starts the restore performing phase if there were no errors.
     *
     * @param reqId Request ID.
     * @param results Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestorePrepareResponse> results, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || fut0.interrupted() || !reqId.equals(fut0.context().requestId()))
            return;

        if (!errs.isEmpty()) {
            fut0.onDone(F.firstValue(errs));

            return;
        }

        SnapshotRestoreContext opCtx = fut0.context();

        // First node with snapshot data.
        UUID firstSnapshotDataNode = null;

        try {
            Map<String, Set<Integer>> grpPartIds = new HashMap<>();
            List<StoredCacheData> ccfgs = null;

            for (Map.Entry<UUID, SnapshotRestorePrepareResponse> entry : results.entrySet()) {
                SnapshotRestorePrepareResponse res = entry.getValue();

                if (res == null)
                    continue;

                if (firstSnapshotDataNode == null) {
                    firstSnapshotDataNode = entry.getKey();
                    ccfgs = res.configs();
                }
                else if (res.configs().size() != res.configs().size()) {
                    throw new IllegalStateException("Count of cache configs in shared group mismatch [" +
                        "node1=" + firstSnapshotDataNode + ", cnt=" + ccfgs.size() +
                        ", node2=" + entry.getKey() + ", cnt=" + res.configs().size() +
                        ", snapshot=" + opCtx.snapshotName() + ']');
                }

                for (Map.Entry<String, Set<Integer>> e : res.partIds().entrySet())
                    grpPartIds.computeIfAbsent(e.getKey(), v -> new HashSet<>()).addAll(e.getValue());
            }

            if (!grpPartIds.keySet().containsAll(opCtx.groups())) {
                Set<String> missedGroups = new HashSet<>(opCtx.groups());

                missedGroups.removeAll(grpPartIds.keySet());

                throw new IllegalArgumentException("Cache group(s) not found in snapshot [groups=" +
                    F.concat(missedGroups, ", ") + ", snapshot=" + opCtx.snapshotName() + ']');
            }

            for (StoredCacheData cacheData : ccfgs) {
                CacheConfiguration<?, ?> ccfg = cacheData.config();

                String grpName = ccfg.getGroupName() != null ? ccfg.getGroupName() : ccfg.getName();

                Set<Integer> partIds = grpPartIds.get(grpName);

                int reqParts = ccfg.getAffinity().partitions();
                int availParts = partIds.size();

                if (reqParts != availParts) {
                    throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [" +
                        "required=" + reqParts +
                        ", avail=" + availParts +
                        ", group=" + grpName +
                        ", snapshot=" + opCtx.snapshotName() + ']');
                }

                opCtx.addCacheData(cacheData);
            }
        }
        catch (Exception e) {
            fut0.onDone(e);

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()) && !fut0.isDone())
            performRestoreProc.start(reqId, new SnapshotRestorePerformRequest(reqId, firstSnapshotDataNode));
    }

    /**
     * @param req Request to perform snapshot restore.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestorePerformResponse> perform(SnapshotRestorePerformRequest req) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || fut0.interrupted())
            return new GridFinishedFuture<>();

        SnapshotRestoreContext opCtx = fut0.context();

        if (!req.requestId().equals(opCtx.requestId()))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        GridFutureAdapter<SnapshotRestorePerformResponse> retFut = new GridFutureAdapter<>();

        try {
            if (!ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx.snapshotName()).exists())
                return new GridFinishedFuture<>();

            for (StoredCacheData cfg : opCtx.configs()) {
                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getName());
            }

            boolean updateMeta = ctx.localNodeId().equals(req.updateMetaNodeId());

            ctx.getSystemExecutorService().submit(() -> {
                try {
                    opCtx.restore(updateMeta, fut0::interrupted);

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
    private void finishPerform(UUID reqId, Map<UUID, SnapshotRestorePerformResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || fut0.interrupted() || !reqId.equals(fut0.context().requestId()))
            return;

        Exception failure = F.first(errs.values());

        if (failure != null) {
            fut0.onDone(failure);

            return;
        }

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return;

        ctx.cache().dynamicStartCachesByStoredConf(fut0.context().configs(), true, true, false, null, true);
    }

    /** */
    private class RestoreSnapshotFuture extends GridFutureAdapter<Void> {
        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        /** Snapshot restore operation context. */
        @GridToStringInclude
        private volatile SnapshotRestoreContext ctx;

        /**
         * @return Cache group restore from snapshot operation context.
         */
        public SnapshotRestoreContext context() {
            return ctx;
        }

        /**
         * @param ctx Cache group restore from snapshot operation context.
         */
        public void init(SnapshotRestoreContext ctx) {
            this.ctx = ctx;
        }

        /**
         * @return Interrupted flag.
         */
        public boolean interrupted() {
            return errRef.get() != null;
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
            if (err == null)
                return super.onDone(res, err, cancel);

            if (errRef.compareAndSet(null, err)) {
                SnapshotRestoreContext opCtx0 = ctx;

                Set<String> grpNames = opCtx0.groups();

                log.error("Snapshot restore process has been interrupted " +
                    "[groups=" + grpNames + ", snapshot=" + opCtx0.snapshotName() + ']', err);

                for (String grpName : grpNames)
                    opCtx0.rollback(grpName);

                return super.onDone(res, err, cancel);
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RestoreSnapshotFuture.class, this);
        }
    }
}
