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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteProductVersion;

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

    /** */
    private ClusterNode answeredNode;

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
        GridDhtAffinityMultiAssignmentResponse res0 = null;

        synchronized (mux) {
            if (pendingNode != null && pendingNode.id().equals(nodeId)) {
                res0 = res;
                answeredNode = pendingNode;
            }

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

        synchronized (mux) {
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

                if (sendRequest(node, new GridDhtAffinityMultiAssignmentRequest(key.get2(), cacheIds))) {
                    pendingNode = node;
                    break;
                }
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

    /**
     * @return Node answered to request.
     */
    public ClusterNode answeredNode() {
        synchronized (mux) {
            return answeredNode;
        }
    }
}