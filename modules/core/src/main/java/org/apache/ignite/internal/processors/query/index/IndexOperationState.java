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

package org.apache.ignite.internal.processors.query.index;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.index.message.IndexFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.index.message.IndexOperationStatusRequest;
import org.apache.ignite.internal.processors.query.index.operation.IndexAbstractOperation;
import org.apache.ignite.internal.processors.query.index.operation.IndexCreateOperation;
import org.apache.ignite.internal.processors.query.index.operation.IndexDropOperation;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.internal.GridTopic.TOPIC_DYNAMIC_SCHEMA;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;

/**
 * Current index operation state.
 */
public class IndexOperationState {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Query processor. */
    private final GridQueryProcessor qryProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Type. */
    private final QueryTypeDescriptorImpl type;

    /** Operation handler. */
    private final IndexOperationHandler hnd;

    /** Mutex for concurrency control. */
    private final Object mux = new Object();

    /** Whether node is coordinator. */
    private boolean crd;

    /** Participants. */
    private Collection<UUID> nodeIds;

    /** Node results. */
    private Map<UUID, String> nodeRess;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param qryProc Query processor.
     * @param type Affected type.
     * @param hnd Operation handler.
     */
    public IndexOperationState(GridKernalContext ctx, GridQueryProcessor qryProc, QueryTypeDescriptorImpl type,
        IndexOperationHandler hnd) {
        this.ctx = ctx;

        log = ctx.log(IndexOperationState.class);

        this.qryProc = qryProc;
        this.type = type;
        this.hnd = hnd;
    }

    /**
     * Map operation handling.
     */
    @SuppressWarnings("unchecked")
    public void tryMap() {
        synchronized (mux) {
            if (isLocalCoordinator()) {
                // Initialize local structure.
                crd = true;
                nodeIds = new HashSet<>();
                nodeRess = new HashMap<>();

                // Send remote requests.
                IndexOperationStatusRequest req =
                    new IndexOperationStatusRequest(ctx.localNodeId(), hnd.operation().operationId());

                for (ClusterNode alive : ctx.discovery().aliveServerNodes()) {
                    nodeIds.add(alive.id());

                    if (!alive.isLocal()) {
                        try {
                            ctx.io().sendToGridTopic(alive, TOPIC_DYNAMIC_SCHEMA, req, QUERY_POOL);
                        }
                        catch (IgniteCheckedException e) {
                            // Node has left the grid.
                            nodeIds.remove(alive.id());
                        }
                    }
                }

                // Listen for local completion.
                hnd.future().listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture fut) {
                        try {
                            fut.get();

                            onNodeFinished(ctx.localNodeId(), null);
                        }
                        catch (Exception e) {
                            onNodeFinished(ctx.localNodeId(), e.getMessage());
                        }
                    }
                });
            }
        }
    }

    /**
     * Handle node finish.
     *
     * @param nodeId Node ID.
     * @param errMsg Error message.
     */
    public void onNodeFinished(UUID nodeId, String errMsg) {
        synchronized (mux) {
            if (nodeRess.containsKey(nodeId))
                return;

            nodeRess.put(nodeId, errMsg);

            checkFinished();
        }
    }

    /**
     * Handle node leave event.
     *
     * @param nodeId Node ID.
     */
    public void onNodeLeave(UUID nodeId) {
        synchronized (mux) {
            if (crd) {
                // Handle this as success.
                if (nodeIds.remove(nodeId))
                    nodeRess.remove(nodeId);

                checkFinished();
            }
            else
                // We can become coordinator, so try remap.
                tryMap();
        }
    }

    /**
     * Handle status request.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("unchecked")
    public void onStatusRequest(final UUID nodeId) {
        hnd.future().listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override public void apply(IgniteInternalFuture fut) {
                String errMsg = null;

                try {
                    fut.get();
                }
                catch (Exception e) {
                    errMsg = e.getMessage();
                }

                qryProc.sendStatusResponse(nodeId, hnd.operation().operationId(), errMsg);
            }
        });
    }

    /**
     * Callback invoked when finish confirmation is received.
     *
     * @param msg Message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void onFinish(IndexFinishDiscoveryMessage msg) {
        IgniteInternalFuture fut = hnd.future();

        assert fut.isDone();

        if (fut.error() == null && !msg.hasError()) {
            IndexAbstractOperation op = msg.operation();

            try {
                if (op instanceof IndexCreateOperation) {
                    IndexCreateOperation op0 = (IndexCreateOperation) op;

                    QueryUtils.processDynamicIndexChange(op0.indexName(), op0.index(), type);
                }
                else {
                    assert op instanceof IndexDropOperation;

                    QueryUtils.processDynamicIndexChange(op.indexName(), null, type);
                }
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to finish index operation [opId=" + op.operationId() + " op=" + op + ']', e);
            }
        }
    }

    /**
     * Find current coordinator.
     *
     * @return {@code True} if node is coordinator.
     */
    private boolean isLocalCoordinator() {
        ClusterNode res = null;

        for (ClusterNode node : ctx.discovery().aliveServerNodes()) {
            if (res == null || res.order() > node.order())
                res = node;
        }

        assert res != null; // Operation state can only exist on server nodes.

        return F.eq(ctx.localNodeId(), res.id());
    }

    /**
     * Check if operation finished.
     */
    private void checkFinished() {
        assert Thread.holdsLock(mux);
        assert crd;

        if (nodeIds.size() == nodeRess.size()) {
            // Initiate finish request.
            UUID errNodeId = null;
            String errNodeMsg = null;

            for (Map.Entry<UUID, String> nodeRes : nodeRess.entrySet()) {
                if (nodeRes.getValue() != null) {
                    errNodeId = nodeRes.getKey();
                    errNodeMsg = nodeRes.getValue();

                    break;
                }
            }

            IndexFinishDiscoveryMessage msg = new IndexFinishDiscoveryMessage(hnd.operation(), errNodeId, errNodeMsg);

            try {
                ctx.discovery().sendCustomEvent(msg);
            }
            catch (Exception e) {
                // Failed to send finish message over discovery. This is something unrecoverable.
                U.warn(log, "Failed to send index finish discovery message [op=" + hnd.operation() + ']', e);
            }
        }
    }
}