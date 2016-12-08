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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Future that fetches affinity assignment from remote cache nodes.
 */
public class GridDhtAssignmentFetchFuture extends GridDhtAssignmentAbstractFetchFuture<GridDhtAffinityAssignmentResponse> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param ctx Context.
     * @param cacheName Cache name.
     * @param topVer Topology version.
     */
    public GridDhtAssignmentFetchFuture(
        GridCacheSharedContext ctx,
        String cacheName,
        AffinityTopologyVersion topVer
    ) {
        super(ctx, topVer, CU.cacheId(cacheName));

        Collection<ClusterNode> availableNodes = ctx.discovery().cacheAffinityNodes(cacheName, topVer);

        LinkedList<ClusterNode> tmp = new LinkedList<>();

        for (ClusterNode node : availableNodes) {
            if (!node.isLocal() && ctx.discovery().alive(node))
                tmp.add(node);
        }

        Collections.sort(tmp, GridNodeOrderComparator.INSTANCE);

        this.availableNodes = tmp;

    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    @Override public void onResponse(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
        if (!res.topologyVersion().equals(key.get2())) {
            if (log.isDebugEnabled())
                log.debug("Received affinity assignment for wrong topology version (will ignore) " +
                    "[node=" + nodeId + ", res=" + res + ", topVer=" + key.get2() + ']');

            return;
        }

        GridDhtAffinityAssignmentResponse res0 = null;

        synchronized (mux) {
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

        synchronized (mux) {
            while (!availableNodes.isEmpty()) {
                ClusterNode node = availableNodes.poll();
                if (sendRequest(node, new GridDhtAffinityAssignmentRequest(key.get1(), key.get2()))) {
                    pendingNode = node;
                    break;
                }
            }

            complete = pendingNode == null;
        }

        // No more nodes left, complete future with null outside of synchronization.
        // Affinity should be calculated from scratch.
        if (complete)
            onDone((GridDhtAffinityAssignmentResponse)null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAssignmentFetchFuture.class, this);
    }
}