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

package org.apache.ignite.internal.managers.loadbalancer;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.spi.loadbalancing.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Load balancing manager.
 */
public class GridLoadBalancerManager extends GridManagerAdapter<LoadBalancingSpi> {
    /**
     * @param ctx Grid kernal context.
     */
    public GridLoadBalancerManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getLoadBalancingSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param ses Task session.
     * @param top Task topology.
     * @param job Job to balance.
     * @return Next balanced node.
     * @throws IgniteCheckedException If anything failed.
     */
    public ClusterNode getBalancedNode(GridTaskSessionImpl ses, List<ClusterNode> top, ComputeJob job)
        throws IgniteCheckedException {
        assert ses != null;
        assert top != null;
        assert job != null;

        // Check cache affinity routing first.
        ClusterNode affNode = cacheAffinityNode(ses.deployment(), job, top);

        if (affNode != null) {
            if (log.isDebugEnabled())
                log.debug("Found affinity node for the job [job=" + job + ", affNode=" + affNode.id() + "]");

            return affNode;
        }

        return getSpi(ses.getLoadBalancingSpi()).getBalancedNode(ses, top, job);
    }

    /**
     * @param ses Grid task session.
     * @param top Task topology.
     * @return Load balancer.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    public ComputeLoadBalancer getLoadBalancer(final GridTaskSessionImpl ses, final List<ClusterNode> top) {
        assert ses != null;

        // Return value is not intended for sending over network.
        return new GridLoadBalancerAdapter() {
            @Nullable @Override public ClusterNode getBalancedNode(ComputeJob job, @Nullable Collection<ClusterNode> exclNodes)
                throws IgniteCheckedException {
                A.notNull(job, "job");

                if (F.isEmpty(exclNodes))
                    return GridLoadBalancerManager.this.getBalancedNode(ses, top, job);

                List<ClusterNode> nodes = F.loseList(top, true, exclNodes);

                if (nodes.isEmpty())
                    return null;

                // Exclude list of nodes from topology.
                return GridLoadBalancerManager.this.getBalancedNode(ses, nodes, job);
            }
        };
    }

    /**
     * @param dep Deployment.
     * @param job Grid job.
     * @param nodes Topology nodes.
     * @return Cache affinity node or {@code null} if this job is not routed with cache affinity key.
     * @throws IgniteCheckedException If failed to determine whether to use affinity routing.
     */
    @Nullable private ClusterNode cacheAffinityNode(GridDeployment dep, ComputeJob job, Collection<ClusterNode> nodes)
        throws IgniteCheckedException {
        assert dep != null;
        assert job != null;
        assert nodes != null;

        if (log.isDebugEnabled())
            log.debug("Looking for cache affinity node [job=" + job + "]");

        Object key = dep.annotatedValue(job, GridCacheAffinityKeyMapped.class);

        if (key == null)
            return null;

        String cacheName = (String)dep.annotatedValue(job, GridCacheName.class);

        if (log.isDebugEnabled())
            log.debug("Affinity properties [key=" + key + ", cacheName=" + cacheName + "]");

        try {
            ClusterNode node = ctx.affinity().mapKeyToNode(cacheName, key);

            if (node == null)
                throw new IgniteCheckedException("Failed to map key to node (is cache with given name started?) [gridName=" +
                    ctx.gridName() + ", key=" + key + ", cacheName=" + cacheName +
                    ", nodes=" + U.toShortString(nodes) + ']');

            if (!nodes.contains(node))
                throw new IgniteCheckedException("Failed to map key to node (projection nodes do not contain affinity node) " +
                    "[gridName=" + ctx.gridName() + ", key=" + key + ", cacheName=" + cacheName +
                    ", nodes=" + U.toShortString(nodes) + ", node=" + U.toShortString(node) + ']');

            return node;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to map affinity key to node for job [gridName=" + ctx.gridName() +
                ", job=" + job + ']', e);
        }
    }
}
