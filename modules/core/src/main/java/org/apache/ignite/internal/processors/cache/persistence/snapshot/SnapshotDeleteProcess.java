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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.DELETE_SNAPSHOT;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.node2id;

/**
 * Distributed process to delete a cluster snapshot. The snapshot data is spread across the baseline nodes,
 * so each node removes its local snapshot directory. The operation is rejected if any of the following
 * conflicts is detected: the snapshot is being created, restored or checked.
 */
public class SnapshotDeleteProcess {
    /** Reject operation messages. */
    private static final String OP_REJECT_MSG = "Snapshot deletion was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Cluster-wide operation contexts per request id. */

    private final Map<UUID, DeleteContext> contexts = new ConcurrentHashMap<>();

    /** Delete snapshot phase subprocess. */
    private final DistributedProcess<SnapshotDeleteProcessRequest, SnapshotDeleteProcessResponse> deleteProc;

    /** Stop node lock. */
    private boolean nodeStopping;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotDeleteProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        deleteProc = new DistributedProcess<>(ctx, DELETE_SNAPSHOT, this::deleteLocalSnapshot, this::reduceAndFinish);
    }

    /**
     * Stops all the running processes with the provided exception.
     *
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        contexts.forEach((reqId, c) -> c.fut.onDone(err));
    }

    /**
     * Starts the cluster snapshot delete process.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path (optional).
     * @return Future that will be completed when the snapshot is deleted on all the baseline nodes.
     */
    public IgniteFutureImpl<Void> start(String snpName, @Nullable String snpPath) {
        assert !F.isEmpty(snpName);

        UUID reqId = UUID.randomUUID();

        Set<UUID> requiredNodes = new HashSet<>(
            F.viewReadOnly(ctx.discovery().discoCache().aliveBaselineNodes(), node2id()));

        SnapshotDeleteProcessRequest req = new SnapshotDeleteProcessRequest(reqId, snpName, snpPath, requiredNodes);

        GridFutureAdapter<Void> clusterOpFut = new GridFutureAdapter<>();

        DeleteContext dctx = new DeleteContext(req, clusterOpFut);

        contexts.put(reqId, dctx);

        clusterOpFut.listen(fut -> contexts.remove(reqId));

        deleteProc.start(reqId, req);

        return new IgniteFutureImpl<>(clusterOpFut);
    }

    /**
     * @param snpName Snapshot name.
     * @return {@code True} if a delete operation for the snapshot is in progress.
     */
    boolean isSnapshotDeleting() {
        return !contexts.isEmpty();
    }

    /** Local phase: delete the snapshot directory on the node. */
    private IgniteInternalFuture<SnapshotDeleteProcessResponse> проверь илиdeleteLocalSnapshot(
        UUID ignored,
        SnapshotDeleteProcessRequest req
    ) {
        if (!baseline(ctx.localNodeId()))
            return new GridFinishedFuture<>(new SnapshotDeleteProcessResponse(false));

        IgniteSnapshotManager snpMgr = ctx.cache().context().snapshotMgr();

        String snpName = req.snapshotName();

        if (snpMgr.isSnapshotCreating())
            return new GridFinishedFuture<>(new ClusterTopologyCheckedException(
                OP_REJECT_MSG + "a snapshot operation is in progress [snapshot=" + snpName + ']'));

        if (snpMgr.isRestoring(snpName))
            return new GridFinishedFuture<>(new ClusterTopologyCheckedException(
                OP_REJECT_MSG + "the snapshot is being restored [snapshot=" + snpName + ']'));

        if (snpMgr.isSnapshotChecking(snpName))
            return new GridFinishedFuture<>(new ClusterTopologyCheckedException(
                OP_REJECT_MSG + "the snapshot is being checked [snapshot=" + snpName + ']'));

        boolean deleted = snpMgr.deleteSnapshotLocal(snpName, req.snapshotPath());

        if (log.isInfoEnabled()) {
            log.info("Snapshot delete operation [snapshot=" + snpName +
                ", snpPath=" + req.snapshotPath() + ", node=" + ctx.localNodeId() + ", deleted=" + deleted + ']');
        }

        return new GridFinishedFuture<>(new SnapshotDeleteProcessResponse(deleted));
    }

    /** Coordinator finish: aggregate node results and complete the user future. */
    private void reduceAndFinish(
        UUID reqId,
        Map<UUID, SnapshotDeleteProcessResponse> results,
        Map<UUID, Throwable> errors
    ) {
        DeleteContext dctx = contexts.get(reqId);

        if (dctx == null)
            return;

        try {
            if (!errors.isEmpty())
                throw F.firstValue(errors);

            ClusterTopologyCheckedException ex = checkNodeLeft(dctx.req.nodes(), results.keySet());

            if (ex != null)
                throw ex;

            dctx.fut.onDone();
        }
        catch (Throwable th) {
            dctx.fut.onDone(th);
        }
    }

    /**
     * @param reqNodes Set of required topology nodes.
     * @param respNodes Set of responded topology nodes.
     * @return Error, if no response was received from a required topology node.
     */
    private static @Nullable ClusterTopologyCheckedException checkNodeLeft(Set<UUID> reqNodes, Set<UUID> respNodes) {
        if (!respNodes.containsAll(reqNodes)) {
            Set<UUID> leftNodes = new HashSet<>(reqNodes);

            leftNodes.removeAll(respNodes);

            return new ClusterTopologyCheckedException("Snapshot deletion stopped. " +
                "Required node has left the cluster [nodeId=" + leftNodes + ']');
        }

        return null;
    }

    /** @return {@code True} if the local node is a baseline node. */
    private boolean baseline(UUID nodeId) {
        ClusterNode node = ctx.cluster().get().node(nodeId);

        return node != null && CU.baselineNode(node, ctx.state().clusterState());
    }

    /** Delete operation context. */
    private static final class DeleteContext {
        /** Request. */
        private final SnapshotDeleteProcessRequest req;

        /** Cluster operation future. */
        private final GridFutureAdapter<Void> fut;

        /** */
        private DeleteContext(SnapshotDeleteProcessRequest req, GridFutureAdapter<Void> fut) {
            this.req = req;
            this.fut = fut;
        }
    }
}
