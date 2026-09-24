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
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry.SNAPSHOT_DELETE_FEATURE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.DELETE_SNAPSHOT;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;

/**
 * Distributed process to delete a cluster snapshot. The operation is rejected if any concurrent snapshot operation is
 * active.
 */
public class SnapshotDeleteProcess {
    /** Reject operation messages. */
    private static final String OP_REJECT_MSG = "Snapshot deletion was rejected. ";

    /** Kernal context. */
    private final GridKernalContext kctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private volatile boolean interrupted;

    /** Cluster-wide operation futures per request id on certain node. */
    private final Map<UUID, GridFutureAdapter<SnapshotDeleteProcessResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** Process requests per snapshot name on each server node. */
    private final Set<SnapshotDeleteRequest> requests = ConcurrentHashMap.newKeySet();

    /** The distributed process. */
    private final DistributedProcess<SnapshotDeleteRequest, SnapshotDeleteResponse> distrProc;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotDeleteProcess(GridKernalContext ctx) {
        this.kctx = ctx;

        log = ctx.log(getClass());

        distrProc = new DistributedProcess<>(ctx, DELETE_SNAPSHOT, this::deletePhase, this::reducePhase);
    }

    /**
     * Starts the cluster snapshot delete process.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path (optional).
     * @return Future that will be completed when the snapshot is deleted.
     */
    public IgniteFuture<SnapshotDeleteProcessResult> start(String snpName, @Nullable String snpPath) {
        var clusterOpFut = new GridFutureAdapter<SnapshotDeleteProcessResult>();

        if (!kctx.rollingUpgrade().features().isActive(SNAPSHOT_DELETE_FEATURE)) {
            clusterOpFut.onDone(new IgniteIllegalStateException(OP_REJECT_MSG +
                "The snapshot deletion feature isn't activated yet [snpName=" + snpName + ", snpPath=" + snpPath + ']'));

            return new IgniteFutureImpl<>(clusterOpFut);
        }

        UUID reqId = UUID.randomUUID();

        clusterOpFut.listen(fut -> clusterOpFuts.remove(reqId));

        try {
            synchronized (clusterOpFuts) {
                if (interrupted || kctx.isStopping())
                    throw new NodeStoppingException("Failed to start snapshot delete process: node is stopping.");

                clusterOpFuts.put(reqId, clusterOpFut);
            }

            SnapshotDeleteRequest req = new SnapshotDeleteRequest(reqId, snpName, snpPath);

            distrProc.start(reqId, req);
        }
        catch (Throwable t) {
            log.error("Failed to start distributed delete snapshot process [snpName=" + snpName + ", snpPath=" + snpPath + ']', t);

            clusterOpFut.onDone(t);
        }

        return new IgniteFutureImpl<>(clusterOpFut);
    }

    /** */
    private IgniteInternalFuture<SnapshotDeleteResponse> deletePhase(UUID ignored, SnapshotDeleteRequest req) {
        if (interrupted || kctx.isStopping()) {
            return new GridFinishedFuture<>(new NodeStoppingException(OP_REJECT_MSG +
                " Node is stopping [req=" + req + ']'));
        }

        if (kctx.cluster().get().localNode().isClient())
            return new GridFinishedFuture<>(new SnapshotDeleteResponse());

        kctx.security().authorize(ADMIN_SNAPSHOT);

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        var curCreateRq = snpMgr.currentCreateRequest();

        if (curCreateRq != null && curCreateRq.snpName.equalsIgnoreCase(req.snpName)) {
            return new GridFinishedFuture<>(new IgniteIllegalStateException(OP_REJECT_MSG +
                "Snapshot with this name is being created [req=" + req + ']'));
        }

        if (snpMgr.isRestoring(req.snpName)) {
            return new GridFinishedFuture<>(new IgniteIllegalStateException(OP_REJECT_MSG +
                "Snapshot with this name is being restored [req=" + req + ']'));
        }

        if (snpMgr.isSnapshotChecking(req.snpName)) {
            return new GridFinishedFuture<>(new IgniteIllegalStateException(OP_REJECT_MSG +
                "Snapshot with this name is being checked [req=" + req + ']'));
        }

        try {
            File path = resolvePath(req.snpPath);

            req.resolvedPath = path;

            if (!requests.add(req)) {
                return new GridFinishedFuture<>(new IgniteIllegalStateException("Deletion of the snapshot has already " +
                    "started [req=" + req + ']'));
            }

            SnapshotFileTree snpFiles = new SnapshotFileTree(kctx, req.snpName, path.getAbsolutePath());

            // We need to find and read snapshot metas to ensure the content is a snapshot. Also, the metas contain
            // initial cluster topology and actual snasphot folder names.
            List<SnapshotMetadata> locMetas = kctx.cache().context().snapshotMgr().readSnapshotMetadatas(snpFiles, false);

            if (locMetas.isEmpty()) {
                requests.remove(req);

                log.warning("Snapshot deletion won't process, no snapshot metadata found [req=" + req + ']');

                return new GridFinishedFuture<>(new SnapshotDeleteResponse(SnapshotDeleteResponse.DeleteStatus.NOT_FOUND, null));
            }

            // Future to delete snapshot contents according to snapshot metadatas.
            GridCompoundFuture<SnapshotDeleteResponse, SnapshotDeleteResponse> resultFut =
                new GridCompoundFuture<>(new MetaFuturesReducer());

            resultFut.listen(fut -> requests.remove(req));

            File path0 = path;

            for (var meta : locMetas) {
                GridFutureAdapter<SnapshotDeleteResponse> perMetaFut = new GridFutureAdapter<>();

                kctx.pools().getSnapshotExecutorService().submit(() -> {
                    try {
                        AtomicBoolean foundFlag = new AtomicBoolean();

                        // Read file tree of the snapshot.
                        var byMetaSft = new SnapshotFileTree(kctx, req.snpName, path0.getAbsolutePath(), meta.folderName(),
                            meta.consId);

                        boolean deleted = snpMgr.deleteLocalSnapshot(byMetaSft, foundFlag);

                        SnapshotDeleteResponse.DeleteStatus status;

                        if (foundFlag.get()) {
                            if (deleted && log.isInfoEnabled())
                                log.info("Snapshot successfully deleted [req=" + req + ']');
                            else if (!deleted)
                                log.warning("Snapshot deleted not completely [req=" + req + ']');

                            status = deleted
                                ? SnapshotDeleteResponse.DeleteStatus.DELETED
                                : SnapshotDeleteResponse.DeleteStatus.PARTLY;
                        }
                        else {
                            if (log.isInfoEnabled())
                                log.info("Snapshot not found to delete [req=" + req + ']');

                            status = SnapshotDeleteResponse.DeleteStatus.NOT_FOUND;
                        }

                        perMetaFut.onDone(new SnapshotDeleteResponse(status, meta.bltNodes));
                    }
                    catch (Throwable e) {
                        perMetaFut.onDone(e);
                    }
                });

                resultFut.add(perMetaFut);
            }

            resultFut.markInitialized();

            if (log.isInfoEnabled())
                log.info("Deletion of snapshot initialized [req=" + req + ']');

            return resultFut;
        }
        catch (Throwable t) {
            requests.remove(req);

            log.warning("An error occurred during snapshot deletion [req=" + req + ']', t);

            return new GridFinishedFuture<>(t);
        }
    }

    /** */
    private File resolvePath(@Nullable String path) throws IOException {
        var res = kctx.pdsFolderResolver().fileTree().snapshotsRoot();

        if (path != null) {
            File reqPath = new File(path);

            res = reqPath.isAbsolute() ? reqPath : new File(res, path);
        }

        return res.getCanonicalFile();
    }

    /** */
    private void reducePhase(UUID reqId, Map<UUID, SnapshotDeleteResponse> results, Map<UUID, Throwable> errors) {
        var clusterOpFut = clusterOpFuts.get(reqId);

        if (clusterOpFut == null)
            return;

        assert clusterOpFut != null;

        try {
            var errP = F.isEmpty(errors) ? null : F.first(errors.entrySet());

            if (errP != null) {
                log.warning("Snapshot deletion finished with an error [reqId=" + reqId + ", nodeId="
                    + errP.getKey() + ", err='" + errP.getValue().getMessage() + "']", errP.getValue());

                clusterOpFut.onDone(errP.getValue());

                return;
            }

            Map<UUID, String> completedNodes = U.newHashMap(results.size());
            Map<UUID, String> uncompletedNodes = U.newHashMap(results.size());
            Map<UUID, String> emptyNodes = U.newHashMap(results.size());
            var snpNodes = new HashSet<String>();

            results.forEach((nodeId, nodeRes) -> {
                if (!F.isEmpty(nodeRes.nodeIds))
                    snpNodes.addAll(nodeRes.nodeIds);

                if (nodeRes.status != null) {
                    switch (nodeRes.status) {
                        case NOT_FOUND:
                            emptyNodes.put(nodeId, consistentId(nodeId));
                            break;
                        case DELETED:
                            completedNodes.put(nodeId, consistentId(nodeId));
                            break;
                        case PARTLY:
                            uncompletedNodes.put(nodeId, consistentId(nodeId));
                            break;
                        default:
                            throw new IgniteIllegalStateException("Unknown snapshot deletion node result, [nodeRes=" +
                                nodeRes + ", nodeId=" + nodeId + ']');
                    }
                }
            });

            kctx.discovery().baselineNodes(kctx.discovery().topologyVersionEx()).stream()
                .map(bn -> bn.consistentId().toString()).toList().forEach(snpNodes::remove);

            clusterOpFut.onDone(new SnapshotDeleteProcessResult(completedNodes, uncompletedNodes, emptyNodes, snpNodes));
        }
        catch (Throwable t) {
            clusterOpFut.onDone(t);
        }
    }

    /** */
    private String consistentId(UUID nodeId) {
        var node = kctx.discovery().node(nodeId);

        if (node == null)
            node = kctx.discovery().historicalNode(nodeId);

        return node == null ? "" : node.consistentId().toString();
    }

    /** */
    public boolean isDeleting(String snpName, @Nullable String snpPath) {
        var rq = new SnapshotDeleteRequest(null, snpName, snpPath);

        try {
            rq.resolvedPath = resolvePath(rq.snpPath);
        }
        catch (IOException ignored) {
            return false;
        }

        return requests.contains(rq);
    }

    /**
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        synchronized (clusterOpFuts) {
            interrupted = true;
        }

        clusterOpFuts.forEach((reqId, clusterOpFut) -> clusterOpFut.onDone(err));

        clusterOpFuts.clear();
    }

    /** */
    private static class MetaFuturesReducer implements IgniteReducer<SnapshotDeleteResponse, SnapshotDeleteResponse> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private @Nullable SnapshotDeleteResponse.DeleteStatus status;

        /** */
        private final Collection<String> nodeIds = new HashSet<>();

        /** {@inheritDoc} */
        @Override public boolean collect(SnapshotDeleteResponse res) {
            assert res != null;

            synchronized (this) {
                if (!F.isEmpty(res.nodeIds))
                    nodeIds.addAll(res.nodeIds);

                if (status == null || status == res.status)
                    status = res.status;
                else
                    status = SnapshotDeleteResponse.DeleteStatus.PARTLY;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public SnapshotDeleteResponse reduce() {
            return new SnapshotDeleteResponse(status == null ? SnapshotDeleteResponse.DeleteStatus.NOT_FOUND : status, nodeIds);
        }
    }
}
