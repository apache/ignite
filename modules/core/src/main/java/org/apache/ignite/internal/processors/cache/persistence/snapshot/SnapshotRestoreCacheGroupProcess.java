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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
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

    /** Restore operation lock. */
    private final ReentrantLock rollbackLock = new ReentrantLock();

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

        String grpName0 = grpName != null ? grpName : cacheName;

        PendingStartCacheGroup pendingGrp = fut0.context().pendingStartCaches.get(grpName0);

        // If any of shared caches has been started - we cannot rollback changes.
        if (pendingGrp.caches.remove(cacheName) && err == null)
            pendingGrp.canRollback = false;

        if (!pendingGrp.caches.isEmpty())
            return;

        if (pendingGrp.canRollback && err != null && fut.context().rollbackContext() != null) {
            ctx.getSystemExecutorService().submit(() -> {
                fut0.onDone(err);
            });

            return;
        }

        fut0.context().pendingStartCaches.remove(grpName0);

        if (fut0.context().pendingStartCaches.isEmpty())
            fut0.onDone();
    }

    /**
     * Rollback changes made by process.
     *
     * @param opCtx Restore operation context.
     * @param grpName Cache group name.
     * @return {@code True} if changes were rolled back, {@code False} if changes have been already rolled back.
     */
    public boolean rollbackChanges(OperationContext opCtx, String grpName) {
        rollbackLock.lock();

        try {
//            if (staleFuture(fut))
//                return false;

            List<File> createdFiles = opCtx.rollbackContext().remove(grpName);

            if (F.isEmpty(createdFiles))
                return false;

            ctx.cache().context().snapshotMgr().rollbackRestoreOperation(createdFiles);
        } finally {
            rollbackLock.unlock();
        }

        return true;
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
            fut.onDone(new IgniteException(OP_REJECT_MSG +
                "Baseline node has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Abort the currently running restore procedure (if any).
     *
     * @param reason Interruption reason.
     */
    public void stop(String reason) {
        if (ctx.clientNode())
            return;

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

        if (inProgress(null))
            return errResponse(OP_REJECT_MSG + "The previous snapshot restore operation was not completed.");

        if (!ctx.state().clusterState().state().active())
            return errResponse(new IllegalStateException(OP_REJECT_MSG + "The cluster should be active."));

        // Skip creating future on initiator.
        if (fut.isDone())
            fut = new RestoreSnapshotFuture();

        fut.init(req);

        if (!ctx.cache().context().snapshotMgr().snapshotLocalDir(req.snapshotName()).exists())
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

        List<CacheGroupSnapshotDetails> grpCfgs = new ArrayList<>();

        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();

        // Collect cache configuration(s).
        for (String cacheName : req.groups()) {
            CacheGroupSnapshotDetails grpCfg = snapshotMgr.readCacheGroupDetails(req.snapshotName(), cacheName);

            if (grpCfg != null)
                grpCfgs.add(grpCfg);
        }

        if (grpCfgs.isEmpty())
            return null;

        RestoreSnapshotFuture fut0 = fut;

        ctx.cache().context().snapshotMgr().mergeSnapshotMetadata(req.snapshotName(), true, false, fut0::interrupted);

        return new SnapshotRestorePrepareResponse(grpCfgs);
    }

    /**
     * Completes the verification phase and starts the restore performing phase if there were no errors.
     *
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestorePrepareResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.interrupted())
            return;

        if (!errs.isEmpty()) {
            completeFuture(reqId, errs, fut0);

            return;
        }

        UUID updateMetadataNode = null;

        for (Map.Entry<UUID, SnapshotRestorePrepareResponse> entry : res.entrySet()) {
            SnapshotRestorePrepareResponse resp = entry.getValue();

            if (resp != null && !F.isEmpty(resp.groups())) {
                updateMetadataNode = entry.getKey();

                break;
            }
        }

        OperationContext opCtx = fut0.context();

        Set<String> notFoundGroups = new HashSet<>(opCtx.groups());

        try {
            Collection<CacheGroupSnapshotDetails> grpsDetails = mergeNodeResults(res);

            List<StoredCacheData> cacheCfgs = new ArrayList<>();

            for (CacheGroupSnapshotDetails grpDetails : grpsDetails) {
                StoredCacheData cdata = F.first(grpDetails.configs());

                if (cdata == null)
                    continue;

                int reqParts = cdata.config().getAffinity().partitions();
                int availParts = grpDetails.parts().size();

                if (reqParts != availParts) {
                    throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [" +
                        "required=" + reqParts + ", avail=" + availParts + ", grp=" + grpDetails.groupName() + ']');
                }

                notFoundGroups.remove(grpDetails.groupName());

                PendingStartCacheGroup pendingGrp = opCtx.pendingStartCaches.get(grpDetails.groupName());

                for (StoredCacheData cacheData : grpDetails.configs()) {
                    String cacheName = cacheData.config().getName();

                    if (!F.isEmpty(cacheData.config().getGroupName())) {
                        opCtx.addCacheId(CU.cacheId(cacheName));
                        pendingGrp.caches.add(cacheName);
                    }

                    cacheCfgs.add(cacheData);

                    CacheGroupDescriptor desc = ctx.cache().cacheGroupDescriptor(CU.cacheId(cacheName));

                    if (desc != null) {
                        throw new IllegalStateException("Cache \"" + desc.cacheOrGroupName() +
                            "\" should be destroyed manually before perform restore operation.");
                    }
                }
            }

            if (!notFoundGroups.isEmpty()) {
                throw new IllegalArgumentException("Cache group(s) not found in snapshot [groups=" +
                    F.concat(notFoundGroups, ", ") + ", snapshot=" + opCtx.snapshotName() + ']');
            }

            Set<UUID> srvNodeIds = new HashSet<>(F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
                F.node2id(),
                (node) -> CU.baselineNode(node, ctx.state().clusterState())));

            Set<UUID> reqNodes = new HashSet<>(opCtx.nodes());

            reqNodes.removeAll(srvNodeIds);

            if (!reqNodes.isEmpty()) {
                throw new IllegalStateException("Unable to perform a restore operation, server node(s) left " +
                    "the cluster [nodeIds=" + F.concat(reqNodes, ", ") + ']');
            }

            opCtx.startConfigs(cacheCfgs);
        }
        catch (Exception e) {
            fut0.onDone(e);

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()) && !fut0.isDone())
            performRestoreProc.start(reqId, new SnapshotRestorePerformRequest(reqId, updateMetadataNode));
    }

    /**
     * @param res Results from multiple nodes.
     * @return A collection that contains information about the snapshot cache group(s) on all nodes.
     */
    private Collection<CacheGroupSnapshotDetails> mergeNodeResults(Map<UUID, SnapshotRestorePrepareResponse> res) {
        Map<String, T2<UUID, CacheGroupSnapshotDetails>> globalDetails = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestorePrepareResponse> entry : res.entrySet()) {
            UUID currNodeId = entry.getKey();
            SnapshotRestorePrepareResponse singleResp = entry.getValue();

            if (singleResp == null)
                continue;

            for (CacheGroupSnapshotDetails nodeDetails : singleResp.groups()) {
                T2<UUID, CacheGroupSnapshotDetails> clusterDetailsPair = globalDetails.get(nodeDetails.groupName());

                String grpName = nodeDetails.groupName();

                if (clusterDetailsPair == null) {
                    globalDetails.put(grpName, new T2<>(currNodeId, nodeDetails));

                    continue;
                }

                CacheGroupSnapshotDetails clusterDetails = clusterDetailsPair.get2();

                int currCfgCnt = nodeDetails.configs().size();
                int savedCfgCnt = clusterDetails.configs().size();

                if (currCfgCnt != savedCfgCnt) {
                    throw new IllegalStateException("Count of cache configs in shared group mismatch [" +
                        "node1=" + clusterDetailsPair.get1() + ", cnt=" + savedCfgCnt +
                        ", node2=" + currNodeId + ", cnt=" + nodeDetails.configs().size() + ']');
                }

                clusterDetails.parts().addAll(nodeDetails.parts());
            }
        }

        return F.viewReadOnly(globalDetails.values(), IgniteBiTuple::get2);
    }

    /**
     * @param req Request to perform snapshot restore.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestorePerformResponse> perform(SnapshotRestorePerformRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        OperationContext opCtx = fut.context();

        if (!req.requestId().equals(opCtx.requestId()))
            return errResponse("Unknown snapshot restore operation was rejected.");

        GridFutureAdapter<SnapshotRestorePerformResponse> retFut = new GridFutureAdapter<>();

        if (!ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx.snapshotName()).exists())
            return new GridFinishedFuture<>();

        ctx.getSystemExecutorService().submit(() -> {
            try {
                restore0(opCtx.snapshotName(), opCtx.groups(), req.updateMetaNodeId(), opCtx.rollbackContext());

                retFut.onDone();
            } catch (Throwable t) {
                retFut.onDone(t);
            }
        });

        return retFut;
    }

    /**
     * @param snpName Snapshot name.
     * @param groups List of cache group names to restore from the snapshot.
     * @param updateMetaNodeId Node ID from which to update the binary metadata.
     * @param opCtx Restore operation context.
     * @throws IgniteCheckedException If failed.
     */
    private void restore0(
        String snpName,
        Set<String> groups,
        UUID updateMetaNodeId,
        RestoreOperationContext opCtx
    ) throws IgniteCheckedException {
        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();

        RestoreSnapshotFuture fut0 = fut;

        if (ctx.localNodeId().equals(updateMetaNodeId) && !fut0.interrupted())
            snapshotMgr.mergeSnapshotMetadata(snpName, false, true, fut0::interrupted);

        for (String grpName : groups) {
            rollbackLock.lock();

            try {
                if (fut0.interrupted())
                    return;

                List<File> newFiles = new ArrayList<>();

                opCtx.put(grpName, newFiles);

                snapshotMgr.restoreCacheGroupFiles(snpName, grpName, newFiles);
            }
            finally {
                rollbackLock.unlock();
            }
        }
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPerform(UUID reqId, Map<UUID, SnapshotRestorePerformResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || fut0.interrupted())
            return;

        Exception failure = F.first(errs.values());

        if (failure != null) {
            fut0.onDone(failure);

            return;
        }

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return;

        ctx.cache().dynamicStartCachesByStoredConf(fut0.context().startConfigs(), true, true, false, null, true);
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     * @param fut Key change future.
     * @return {@code True} if future was completed by this call.
     */
    private boolean completeFuture(UUID reqId, Map<UUID, Exception> err, RestoreSnapshotFuture fut) {
        if (!reqId.equals(fut.id()) || fut.isDone())
            return false;

        return !F.isEmpty(err) ? fut.onDone(F.firstValue(err)) : fut.onDone();
    }

    /**
     * @param msg Error message.
     * @param <T> Type of the future.
     * @return Failed with the specified error message future.
     */
    private <T> IgniteInternalFuture<T> errResponse(String msg) {
        return errResponse(new IgniteException(msg));
    }

    /**
     * @param ex Exception.
     * @param <T> Type of the future.
     * @return Failed with the specified exception future.
     */
    private <T> IgniteInternalFuture<T> errResponse(Exception ex) {
        return new GridFinishedFuture<>(ex);
    }

    /** */
    private static class PendingStartCacheGroup {
        volatile boolean canRollback = true;

        Set<String> caches = new GridConcurrentHashSet<>();
    }

    /** */
    private static class RestoreOperationContext {
        private final Map<String, List<File>> newGrpFiles = new HashMap<>();

        public List<File> get(String grpName) {
            return newGrpFiles.get(grpName);
        }

        public List<File> remove(String grpName) {
            return newGrpFiles.remove(grpName);
        }

        public boolean isEmpty() {
            return newGrpFiles.isEmpty();
        }

        public void put(String grpName, List<File> files) {
            newGrpFiles.put(grpName, files);
        }
    }

    private static class OperationContext {
        private final RestoreOperationContext rollbackCtx = new RestoreOperationContext();

        private final Map<String, PendingStartCacheGroup> pendingStartCaches = new ConcurrentHashMap<>();

        private final Set<Integer> cacheIds = new GridConcurrentHashSet<>();

        private final String snpName;

        private final UUID requestId;

        private final Set<UUID> reqNodes;

        private volatile Collection<StoredCacheData> cacheCfgsToStart;

        public OperationContext(UUID requestId, String snpName, Set<UUID> reqNodes, Collection<String> grps) {
            for (String grpName : grps) {
                cacheIds.add(CU.cacheId(grpName));

                pendingStartCaches.put(grpName, new PendingStartCacheGroup());
            }

            this.requestId = requestId;
            this.reqNodes = reqNodes;
            this.snpName = snpName;
        }

        public boolean containsCache(String name) {
            return cacheIds.contains(CU.cacheId(name));
        }

        public void addCacheId(int cacheId) {
            cacheIds.add(cacheId);
        }

        public void startConfigs(Collection<StoredCacheData> ccfgs) {
            cacheCfgsToStart = ccfgs;
        }

        public Collection<StoredCacheData> startConfigs() {
            return cacheCfgsToStart;
        }

        public RestoreOperationContext rollbackContext() {
            return rollbackCtx;
        }

        public Set<UUID> nodes() {
            return reqNodes;
        }

        public String snapshotName() {
            return snpName;
        }

        /** @return Request ID. */
        public UUID requestId() {
            return requestId;
        }

        public Set<String> groups() {
            return pendingStartCaches.keySet();
        }
    }

    /** */
    private class RestoreSnapshotFuture extends GridFutureAdapter<Void> {
        private volatile OperationContext ctx;

        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        public UUID id() {
            return ctx != null ? ctx.requestId() : null;
        }

        public OperationContext context() {
            return ctx;
        }

        public void init(SnapshotRestorePrepareRequest req) {
            ctx = new OperationContext(req.requestId(), req.snapshotName(), req.requiredNodes(), req.groups());
        }

        public boolean interrupted() {
            return errRef.get() != null;
        }

        @Override protected boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
            if (err == null)
                return super.onDone(res, err, cancel);

            if (errRef.compareAndSet(null, err)) {
                OperationContext opCtx0 = ctx;

                Set<String> grpNames = opCtx0.groups();

                log.warning("Snapshot restore process has been interrupted [grps=" + grpNames + ']');

                for (String grpName : grpNames)
                    rollbackChanges(opCtx0, grpName);

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
