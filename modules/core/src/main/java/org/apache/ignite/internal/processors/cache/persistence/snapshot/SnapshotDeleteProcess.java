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

import java.util.ArrayList;
import java.util.Map;
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
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.DELETE_SNAPSHOT;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;

/**
 * Distributed process to delete a cluster snapshot. The operation is rejected if any concurrent snapshot operation is
 * active.
 */
public class SnapshotDeleteProcess {
    /** Reject operation messages. */
    private static final String OP_REJECT_MSG = "Snapshot deletion was rejected.";

    /** Kernal context. */
    private final GridKernalContext kctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private volatile boolean interrupted;

    /** Cluster-wide operation futures per request id on certain node. */
    private final Map<UUID, GridFutureAdapter<SnapshotDeleteProcessResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** Process requests per id on eah baseline node. */
    private final Map<UUID, SnapshotDeleteRequest> requests = new ConcurrentHashMap<>();

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
        UUID reqId = UUID.randomUUID();

        var clusterOpFut = new GridFutureAdapter<SnapshotDeleteProcessResult>();

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

    /** Local phase: delete the snapshot directory on a node. */
    private IgniteInternalFuture<SnapshotDeleteResponse> deletePhase(UUID ignored, SnapshotDeleteRequest req) {
        if (kctx.isStopping()) {
            return new GridFinishedFuture<>(new NodeStoppingException(OP_REJECT_MSG +
                " Node is stopping [req=" + req + ']'));
        }

        if (kctx.cluster().get().localNode().isClient())
            return new GridFinishedFuture<>(new SnapshotDeleteResponse(null));

        kctx.security().authorize(ADMIN_SNAPSHOT);

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        var curCreateRq = snpMgr.currentCreateRequest();

        if (curCreateRq != null && curCreateRq.snpName.equals(req.snpName)) {
            return new GridFinishedFuture<>(new IllegalStateException(OP_REJECT_MSG +
                " Snapshot with this name is being created [req=" + req + ']'));
        }

        if (snpMgr.isRestoring(req.snpName)) {
            return new GridFinishedFuture<>(new IllegalStateException(OP_REJECT_MSG +
                " Snapshot with this name is being restored [req=" + req + ']'));
        }

        if (snpMgr.isSnapshotChecking(req.snpName)) {
            return new GridFinishedFuture<>(new IllegalStateException(OP_REJECT_MSG +
                " Snapshot with this name is being checked [req=" + req + ']'));
        }

        if (requests.putIfAbsent(req.reqId, req) != null) {
            return new GridFinishedFuture<>(new IllegalStateException("Deletion of the snapshot has already started [req="
                + req + ']'));
        }

        try {
            AtomicBoolean foundFlag = new AtomicBoolean();

            boolean deleted = snpMgr.deleteLocalSnapshot(new SnapshotFileTree(kctx, req.snpName, req.snpPath), foundFlag);

            SnapshotDeleteResponse.SnapshotDeleteStatus res;

            if (foundFlag.get()) {
                if (deleted && log.isInfoEnabled())
                    log.info("Snapshot successfully deleted, req=" + req);
                else if (!deleted)
                    log.warning("Snapshot deleted not completely, req=" + req);

                res = deleted
                    ? SnapshotDeleteResponse.SnapshotDeleteStatus.DELETED
                    : SnapshotDeleteResponse.SnapshotDeleteStatus.PARTLY_DELETED;
            }
            else {
                if (log.isInfoEnabled())
                    log.info("Snapshot not found to delete, req=" + req);

                res = SnapshotDeleteResponse.SnapshotDeleteStatus.NOT_FOUND;
            }

            return new GridFinishedFuture<>(new SnapshotDeleteResponse(res));
        }
        catch (Throwable t) {
            log.error("An error occured during snapshot deletion, req=" + req, t);

            return new GridFinishedFuture<>(t);
        }
        finally {
            requests.remove(req.reqId);
        }
    }

    /** Coordinator finish: aggregate node results and complete the user future. */
    private void reducePhase(UUID reqId, Map<UUID, SnapshotDeleteResponse> results, Map<UUID, Throwable> errors) {
        var clusterOpFut = clusterOpFuts.get(reqId);

        if (clusterOpFut == null)
            return;

        try {
            var errP = F.isEmpty(errors) ? null : F.first(errors.entrySet());

            if (errP != null) {
                log.warning("Snapshot deletion finished with an error [reqId=" + reqId + ", nodeId="
                    + errP.getKey() + ", err='" + errP.getValue().getMessage() + "']", errP.getValue());

                clusterOpFut.onDone(errP.getValue());

                return;
            }

            var completedNodes = new ArrayList<UUID>(results.size());
            var uncompletedNodes = new ArrayList<UUID>(results.size());
            var emptyNodes = new ArrayList<UUID>(results.size());

            results.forEach((nodeId, nodeRes) -> {
                if (nodeRes.res != null) {
                    switch (nodeRes.res) {
                        case NOT_FOUND:
                            emptyNodes.add(nodeId);
                            break;
                        case DELETED:
                            completedNodes.add(nodeId);
                            break;
                        case PARTLY_DELETED:
                            uncompletedNodes.add(nodeId);
                            break;
                        default:
                            throw new IgniteIllegalStateException("Unknown snapshot deletion node result, [nodeRes=" +
                                nodeRes + ", nodeId=" + nodeId + ']');
                    }
                }
            });

            clusterOpFut.onDone(new SnapshotDeleteProcessResult(
                completedNodes.isEmpty() ? null : completedNodes,
                uncompletedNodes.isEmpty() ? null : uncompletedNodes,
                emptyNodes.isEmpty() ? null : emptyNodes
            ));
        }
        catch (Throwable t) {
            clusterOpFut.onDone(t);
        }
    }

    /** */
    public boolean isSnapshotDeleting(String name) {
        return false;
    }

    /**
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        // Prevents starting new processes in #prepareAndCheckMetas.
        synchronized (clusterOpFuts) {
            interrupted = true;
        }

        clusterOpFuts.forEach((reqId, fut) -> fut.onDone(err));

        clusterOpFuts.clear();;
    }
}
