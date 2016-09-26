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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Load balancing manager.
 */
public class GridLoadBalancerManager extends GridManagerAdapter<LoadBalancingSpi> {
    /** */
    private static final String DFLT_LOAD_BALANCING_SPI =  RoundRobinLoadBalancingSpi.class.getName();

    /** Cache for internal task names. */
    private final Map<String, Boolean> internalTasks = new ConcurrentHashMap<>();

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

        internalTasks.clear();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param ses Task session.
     * @param top Task topology.
     * @param job Job to balance.
     * @return Next balanced node.
     * @throws IgniteException If anything failed.
     */
    public ClusterNode getBalancedNode(GridTaskSessionImpl ses, List<ClusterNode> top, ComputeJob job)
        throws IgniteException {
        assert ses != null;
        assert top != null;
        assert job != null;

        String spi = ses.getLoadBalancingSpi();

        if (!DFLT_LOAD_BALANCING_SPI.equals(spi)) {
            String taskCls = ses.getTaskClassName();

            Boolean internalTask = internalTasks.get(taskCls);

            if (internalTask == null) {
                try {
                    internalTask = U.hasAnnotation(Class.forName(taskCls), GridInternal.class);
                }
                catch (ClassNotFoundException ignored) {
                    internalTask = false;
                }

                internalTasks.put(taskCls, internalTask);
            }

            if (internalTask)
                return getSpi(DFLT_LOAD_BALANCING_SPI).getBalancedNode(ses, top, job);
        }

        return getSpi(spi).getBalancedNode(ses, top, job);
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
            @Nullable @Override public ClusterNode getBalancedNode(ComputeJob job, @Nullable Collection<ClusterNode> exclNodes) {
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
}
