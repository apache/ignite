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
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;

/**
 * Future that fetches affinity assignment from remote cache nodes.
 */
public class GridDhtAssignmentFetchFuture extends GridFutureAdapter<GridDhtAffinityAssignmentResponse> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** */
    private final GridCacheSharedContext ctx;

    /** List of available nodes this future can fetch data from. */
    @GridToStringInclude
    private Queue<ClusterNode> availableNodes;

    /** Pending node from which response is being awaited. */
    private ClusterNode pendingNode;

    /** */
    @GridToStringInclude
    private final T2<Integer, AffinityTopologyVersion> key;

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
        this.ctx = ctx;
        int cacheId = CU.cacheId(cacheName);
        this.key = new T2<>(cacheId, topVer);

        Collection<ClusterNode> availableNodes = ctx.discovery().cacheAffinityNodes(cacheId, topVer);

        LinkedList<ClusterNode> tmp = new LinkedList<>();

        for (ClusterNode node : availableNodes) {
            if (!node.isLocal() && ctx.discovery().alive(node))
                tmp.add(node);
        }

        Collections.sort(tmp, GridNodeOrderComparator.INSTANCE);

        this.availableNodes = tmp;

        if (log == null)
            log = U.logger(ctx.kernalContext(), logRef, GridDhtAssignmentFetchFuture.class);
    }

    /**
     * Initializes fetch future.
     */
    public void init() {
        ctx.affinity().addDhtAssignmentFetchFuture(this);

        requestFromNextNode();
    }

    /**
     * @return Future key.
     */
    public T2<Integer, AffinityTopologyVersion> key() {
        return key;
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public void onResponse(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
        if (!res.topologyVersion().equals(key.get2())) {
            if (log.isDebugEnabled())
                log.debug("Received affinity assignment for wrong topology version (will ignore) " +
                    "[node=" + nodeId + ", res=" + res + ", topVer=" + key.get2() + ']');

            return;
        }

        GridDhtAffinityAssignmentResponse res0 = null;

        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(nodeId))
                res0 = res;
        }

        if (res0 != null)
            onDone(res);
    }

    /**
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(leftNodeId)) {
                availableNodes.remove(pendingNode);

                pendingNode = null;
            }
        }

        requestFromNextNode();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable GridDhtAffinityAssignmentResponse res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            ctx.affinity().removeDhtAssignmentFetchFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Requests affinity from next node in the list.
     */
    private void requestFromNextNode() {
        boolean complete;

        // Avoid 'protected field is accessed in synchronized context' warning.
        IgniteLogger log0 = log;

        synchronized (this) {
            while (!availableNodes.isEmpty()) {
                ClusterNode node = availableNodes.poll();

                try {
                    if (log0.isDebugEnabled())
                        log0.debug("Sending affinity fetch request to remote node [locNodeId=" + ctx.localNodeId() +
                            ", node=" + node + ']');

                    ctx.io().send(node, new GridDhtAffinityAssignmentRequest(key.get1(), key.get2()),
                        AFFINITY_POOL);

                    // Close window for listener notification.
                    if (ctx.discovery().node(node.id()) == null) {
                        U.warn(log0, "Failed to request affinity assignment from remote node (node left grid, will " +
                            "continue to another node): " + node);

                        continue;
                    }

                    pendingNode = node;

                    break;
                }
                catch (ClusterTopologyCheckedException ignored) {
                    U.warn(log0, "Failed to request affinity assignment from remote node (node left grid, will " +
                        "continue to another node): " + node);
                }
                catch (IgniteCheckedException e) {
                    U.error(log0, "Failed to request affinity assignment from remote node (will " +
                        "continue to another node): " + node, e);
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
