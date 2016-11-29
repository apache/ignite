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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;

/**
 *
 */
public class GridDhtAssignmentMultiFetchFuture extends GridDhtAssignmentAbstractFetchFuture<GridDhtAffinityMultiAssignmentResponse> {

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final IgniteProductVersion MULTI_MESSAGE_SINCE = IgniteProductVersion.fromString("1.8.0");

    /** */
    public static final int NO_CACHE = 0;


    /** */
    private final List<Integer> cacheIds;

    /**
     * @param ctx Context.
     * @param topVer Topology version.
     */
    public GridDhtAssignmentMultiFetchFuture(
        GridCacheSharedContext ctx,
        AffinityTopologyVersion topVer,
        List<Integer> cacheIds
    ) {
        super(ctx, topVer, NO_CACHE);
        this.cacheIds = cacheIds;

        availableNodes = new LinkedList<>(ctx.discovery().serverNodes(topVer));
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    @Override public void onResponse(UUID nodeId, GridDhtAffinityMultiAssignmentResponse res) {

        if (!res.topologyVersion().equals(key.get2())) {
            if (log.isDebugEnabled())
                log.debug("Received affinity assignment for wrong topology version (will ignore) " +
                    "[node=" + nodeId + ", res=" + res + ", topVer=" + key.get2() + ']');

            return;
        }

        GridDhtAffinityMultiAssignmentResponse res0 = null;

        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(nodeId))
                res0 = res;
        }

        if (res0 != null)
            onDone(res);
    }

    /**
     * Requests affinity from next node in the list.
     */
    @Override protected void requestFromNextNode() {
        boolean complete;

        IgniteLogger log0 = log;

            while (!availableNodes.isEmpty()) {
                ClusterNode node = availableNodes.poll();

                if (node.isLocal()) {
                    if (log0.isDebugEnabled())
                        log0.debug("Now I am coordinator");
                    pendingNode = null;
                    break;
                }

                if (!canUseMultiRequest(node)) {
                    if (log0.isDebugEnabled())
                        log0.debug("Node is too old, fallback");
                    pendingNode = null;
                    break;
                }

                try {
                    if (log0.isDebugEnabled())
                        log0.debug("Sending affinity fetch request to coordinator node [locNodeId=" + ctx.localNodeId() +
                            ", node=" + node + ']');

                    ctx.io().send(node, new GridDhtAffinityMultiAssignmentRequest(key.get2(), cacheIds),
                        AFFINITY_POOL);

                    pendingNode = node;

                    break;
                }
                catch (ClusterTopologyCheckedException ignored) {
                    U.warn(log0, "Failed to request affinity assignment from coordinator node (node left grid, will " +
                        "try again): " + node);
                }
                catch (IgniteCheckedException e) {
                    U.error(log0, "Failed to request affinity assignment from coordinator node" + node, e);
                    break;
                }
            }

            complete = pendingNode == null;

        // Failed getting affinity from coordinator
        if (complete)
            onDone((GridDhtAffinityMultiAssignmentResponse)null);
    }

    /**
     * @param node Node.
     * @return {@code True} if node supports multi request.
     */
    private boolean canUseMultiRequest(ClusterNode node) {
        return node.version().compareToIgnoreTimestamp(MULTI_MESSAGE_SINCE) >= 0;
    }

}