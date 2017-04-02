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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.index.message.IndexOperationStatusRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

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
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class IndexOperationManager {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Query processor. */
    private final GridQueryProcessor qryProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Operation handler. */
    private final IndexOperationWorker worker;

    /** Mutex for concurrency control. */
    private final Object mux = new Object();

    /** Participants. */
    private Collection<UUID> nodeIds;

    /** Node results. */
    private Map<UUID, SchemaOperationException> nodeRess;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param qryProc Query processor.
     * @param worker Operation handler.
     */
    public IndexOperationManager(GridKernalContext ctx, GridQueryProcessor qryProc, IndexOperationWorker worker) {
        this.ctx = ctx;

        log = ctx.log(IndexOperationManager.class);

        this.qryProc = qryProc;
        this.worker = worker;
    }

    /**
     * Map operation handling.
     */
    @SuppressWarnings("unchecked")
    public void map() {
        worker.start();

        synchronized (mux) {
            if (qryProc.coordinator()) {
                // Initialize local structures.
                nodeIds = new HashSet<>();
                nodeRess = new HashMap<>();

                // Send remote requests.
                IndexOperationStatusRequest req =
                    new IndexOperationStatusRequest(ctx.localNodeId(), operationId());

                for (ClusterNode alive : ctx.discovery().aliveServerNodes())
                    nodeIds.add(alive.id());

                if (log.isDebugEnabled())
                    log.debug("Mapped participating nodes on coordinator [opId=" + operationId() +
                        ", crdNodeId=" + ctx.localNodeId() + ", nodes=" + nodeIds + ']');

                // Send requests to remote nodes.
                for (UUID nodeId : nodeIds) {
                    if (!F.eq(ctx.localNodeId(), nodeId)) {
                        try {
                            ctx.io().sendToGridTopic(nodeId, TOPIC_DYNAMIC_SCHEMA, req, QUERY_POOL);
                        }
                        catch (IgniteCheckedException e) {
                            onNodeLeave(nodeId);
                        }
                    }
                }

                // Listen for local completion.
                worker.future().listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture fut) {
                        try {
                            fut.get();

                            onNodeFinished(ctx.localNodeId(), null);
                        }
                        catch (Exception e) {
                            onNodeFinished(ctx.localNodeId(), QueryUtils.wrapIfNeeded(e));
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
     * @param err Error.
     */
    public void onNodeFinished(UUID nodeId, @Nullable SchemaOperationException err) {
        synchronized (mux) {
            if (nodeRess.containsKey(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Received duplicate result [opId=" + operationId() + ", nodeId=" + nodeId +
                        ", err=" + err + ']');

                return;
            }

            log.debug("Received result [opId=" + operationId() + ", nodeId=" + nodeId + ", err=" + err + ']');

            nodeRess.put(nodeId, err);

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
            if (qryProc.coordinator()) {
                // Handle this as success.
                if (nodeIds.remove(nodeId))
                    nodeRess.remove(nodeId);

                checkFinished();
            }
            else
                // We can become coordinator, so try remap.
                map();
        }
    }

    /**
     * Handle status request.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("unchecked")
    public void onStatusRequest(final UUID nodeId) {
        worker.future().listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override public void apply(IgniteInternalFuture fut) {
                Exception err = null;

                try {
                    fut.get();
                }
                catch (Exception e) {
                    err = e;
                }

                qryProc.sendStatusResponse(nodeId, operationId(), QueryUtils.wrapIfNeeded(err));
            }
        });
    }

    /**
     * Check if operation finished.
     */
    private void checkFinished() {
        assert Thread.holdsLock(mux);

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

            qryProc.onCoordinatorFinished(operationId(), err);
        }
    }

    /**
     * @return Worker.
     */
    public IndexOperationWorker worker() {
        return worker;
    }

    /**
     * @return Operation ID.
     */
    private UUID operationId() {
        return worker.operation().id();
    }
}