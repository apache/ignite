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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;

/**
 * Future that fetches all ready assignment from remote cache nodes.
 */
public class GridDhtReadyAssignmentsFetchFuture extends GridFutureAdapter<Map<UUID, AtomicReference<GridDhtAffinityAssignmentResponse>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** */
    private final GridCacheSharedContext ctx;

    /** Ready mappings. */
    private ConcurrentMap<UUID, AtomicReference<GridDhtAffinityAssignmentResponse>> data;

    /** */
    @GridToStringInclude
    private final T2<Integer, AffinityTopologyVersion> key;

    /**
     * @param ctx Context.
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @param discoCache Discovery cache.
     */
    public GridDhtReadyAssignmentsFetchFuture(
            GridCacheSharedContext ctx,
            String cacheName,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache
    ) {
        this.ctx = ctx;
        this.key = new T2<>(CU.cacheId(cacheName), topVer);

        Collection<ClusterNode> availableNodes = discoCache.cacheAffinityNodes(cacheName);

        //LinkedList<ClusterNode> tmp = new LinkedList<>();

        data = new ConcurrentHashMap<>();

        for (ClusterNode node : availableNodes) {
            if (!node.isLocal() && ctx.discovery().alive(node))
                data.put(node.id(), new AtomicReference<GridDhtAffinityAssignmentResponse>());
        }

        //Collections.sort(tmp, GridNodeOrderComparator.INSTANCE);

        if (log == null)
            log = U.logger(ctx.kernalContext(), logRef, GridDhtReadyAssignmentsFetchFuture.class);
    }

    /**
     * Initializes fetch future.
     */
    public void init() {
        ctx.affinity().addDhtReadyAssignmentsFetchFuture(this);

        fetchAll();
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

        AtomicReference<GridDhtAffinityAssignmentResponse> ref = data.get(nodeId);

        boolean isSet = ref.compareAndSet(null, res);

        if (isSet) {
            if (isCompleted())
                onDone(data);
        }
    }

    /** */
    private boolean isCompleted() {
        return data.isEmpty() || F.view(data, new IgnitePredicate<UUID>() {
            @Override public boolean apply(UUID uuid) {
                AtomicReference<GridDhtAffinityAssignmentResponse> ref = data.get(uuid);

                return ref != null && ref.get() == null;
            }
        }).isEmpty();
    }

    /**
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        data.remove(leftNodeId);

        if (isCompleted())
            onDone(data);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Map<UUID, AtomicReference<GridDhtAffinityAssignmentResponse>> res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            ctx.affinity().removeDhtReadyAssignmentsFetchFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Requests affinity from next node in the list.
     */
    private void fetchAll() {
        // Avoid 'protected field is accessed in synchronized context' warning.
        IgniteLogger log0 = log;

        Iterator<UUID> it = data.keySet().iterator();

        while (it.hasNext()) {
            UUID nodeId = it.next();

            ClusterNode node = ctx.discovery().node(nodeId);

            // Close window for listener notification.
            if (node == null) {
                U.warn(log0, "Failed to request affinity assignment from remote node (node left grid, will " +
                        "continue to another node): " + node);

                it.remove();

                continue;
            }

            try {
                if (log0.isDebugEnabled())
                    log0.debug("Sending affinity fetch request to remote node [locNodeId=" + ctx.localNodeId() +
                            ", node=" + node + ']');

                GridDhtAffinityAssignmentRequest msg = new GridDhtAffinityAssignmentRequest(key.get1(), key.get2());

                msg.ready(true);

                ctx.io().send(node, msg, AFFINITY_POOL);
            } catch (ClusterTopologyCheckedException ignored) {
                U.warn(log0, "Failed to request affinity assignment from remote node (node left grid, will " +
                        "continue to another node): " + node);

                it.remove();
            } catch (IgniteCheckedException e) {
                U.error(log0, "Failed to request affinity assignment from remote node (will " +
                        "continue to another node): " + node, e);

                it.remove();
            }
        }

        // No more nodes left, complete future with null outside of synchronization.
        // Affinity should be calculated from scratch.
        if (isCompleted())
            onDone(data);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtReadyAssignmentsFetchFuture.class, this);
    }
}