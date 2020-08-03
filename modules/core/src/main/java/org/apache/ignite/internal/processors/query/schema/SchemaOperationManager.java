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

package org.apache.ignite.internal.processors.query.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Schema operation manager.
 */
public class SchemaOperationManager {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Query processor. */
    private final GridQueryProcessor qryProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Operation handler. */
    private final SchemaOperationWorker worker;

    /** Mutex for concurrency control. */
    private final Object mux = new Object();

    /** Participants. */
    private Collection<UUID> nodeIds;

    /** Node results. */
    private Map<UUID, SchemaOperationException> nodeRess;

    /** Current coordinator node. */
    private ClusterNode crd;

    /** Whether coordinator state is mapped. */
    private boolean crdMapped;

    /** Coordinator finished flag. */
    private boolean crdFinished;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param qryProc Query processor.
     * @param worker Operation handler.
     * @param crd Coordinator node.
     */
    public SchemaOperationManager(GridKernalContext ctx, GridQueryProcessor qryProc, SchemaOperationWorker worker,
        @Nullable ClusterNode crd) {
        assert !ctx.clientNode() || crd == null;

        this.ctx = ctx;

        log = ctx.log(SchemaOperationManager.class);

        this.qryProc = qryProc;
        this.worker = worker;

        synchronized (mux) {
            this.crd = crd;

            prepareCoordinator();
        }
    }

    /**
     * Map operation handling.
     */
    @SuppressWarnings("unchecked")
    public void start() {
        worker.start();

        synchronized (mux) {
            worker.future().listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    onLocalNodeFinished(fut);
                }
            });
        }
    }

    /**
     * Handle local node finish.
     *
     * @param fut Future.
     */
    private void onLocalNodeFinished(IgniteInternalFuture fut) {
        assert fut.isDone();

        if (ctx.clientNode())
            return;

        SchemaOperationException err;

        try {
            fut.get();

            err = null;
        }
        catch (Exception e) {
            err = QueryUtils.wrapIfNeeded(e);
        }

        synchronized (mux) {
            if (isLocalCoordinator())
                onNodeFinished(ctx.localNodeId(), err);
            else
                qryProc.sendStatusMessage(crd.id(), operationId(), err);
        }
    }

    /**
     * Handle node finish.
     *
     * @param nodeId Node ID.
     * @param err Error.
     */
    public void onNodeFinished(UUID nodeId, @Nullable SchemaOperationException err) {
        synchronized (mux) {
            assert isLocalCoordinator();

            if (nodeRess.containsKey(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Received duplicate result [opId=" + operationId() + ", nodeId=" + nodeId +
                        ", err=" + err + ']');

                return;
            }

            if (nodeIds.contains(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Received result [opId=" + operationId() + ", nodeId=" + nodeId + ", err=" + err + ']');

                nodeRess.put(nodeId, err);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Received result from non-tracked node (joined after operation started, will ignore) " +
                        "[opId=" + operationId() + ", nodeId=" + nodeId + ", err=" + err + ']');
            }

            checkFinished();
        }
    }

    /**
     * Handle node leave event.
     *
     * @param nodeId ID of the node that has left the grid.
     * @param curCrd Current coordinator node.
     */
    public void onNodeLeave(UUID nodeId, ClusterNode curCrd) {
        synchronized (mux) {
            assert crd != null;

            if (F.eq(nodeId, crd.id())) {
                // Coordinator has left!
                crd = curCrd;

                prepareCoordinator();
            }
            else if (isLocalCoordinator()) {
                // Other node has left, remove it from the coordinator's wait set.
                // Handle this as success.
                if (nodeIds.remove(nodeId))
                    nodeRess.remove(nodeId);
            }

            IgniteInternalFuture fut = worker().future();

            if (fut.isDone())
                onLocalNodeFinished(fut);

            checkFinished();
        }
    }

    /**
     * Check if operation finished.
     */
    private void checkFinished() {
        assert Thread.holdsLock(mux);

        if (isLocalCoordinator()) {
            if (crdFinished)
                return;

            if (nodeIds.size() == nodeRess.size()) {
                // Initiate finish request.
                SchemaOperationException err = null;

                for (Map.Entry<UUID, SchemaOperationException> nodeRes : nodeRess.entrySet()) {
                    if (nodeRes.getValue() != null) {
                        err = nodeRes.getValue();

                        break;
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Collected all results, about to send finish message [opId=" + operationId() +
                        ", err=" + err + ']');

                crdFinished = true;

                qryProc.onCoordinatorFinished(worker.operation(), err);
            }
        }
    }

    /**
     * Prepare topology state in case local node is coordinator.
     *
     * @return {@code True} if state was changed by this call.
     */
    private boolean prepareCoordinator() {
        if (isLocalCoordinator() && !crdMapped) {
            // Initialize local structures.
            nodeIds = new HashSet<>();
            nodeRess = new HashMap<>();

            for (ClusterNode alive : ctx.discovery().aliveServerNodes())
                nodeIds.add(alive.id());

            if (log.isDebugEnabled())
                log.debug("Mapped participating nodes on coordinator [opId=" + operationId() +
                    ", crdNodeId=" + ctx.localNodeId() + ", nodes=" + nodeIds + ']');

            crdMapped = true;

            return true;
        }

        return false;
    }

    /**
     * Check if current node is local coordinator.
     *
     * @return {@code True} if coordinator.
     */
    private boolean isLocalCoordinator() {
        assert Thread.holdsLock(mux);

        return crd != null && crd.isLocal();
    }

    /**
     * @return Worker.
     */
    public SchemaOperationWorker worker() {
        return worker;
    }

    /**
     * @return Operation ID.
     */
    private UUID operationId() {
        return worker.operation().id();
    }
}
