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

package org.apache.ignite.internal.processors.platform.compute;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.LoadBalancerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Interop multi-closure task with node balancing.
 */
@ComputeTaskNoResultCache
public class PlatformBalancingMultiClosureTask extends PlatformAbstractTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** Jobs. */
    private Collection<PlatformJob> jobs;

    /** Load balancer. */
    @SuppressWarnings("UnusedDeclaration")
    @LoadBalancerResource
    private ComputeLoadBalancer lb;

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param taskPtr Task pointer.
     */
    public PlatformBalancingMultiClosureTask(PlatformContext ctx, long taskPtr) {
        super(ctx, taskPtr);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        assert !F.isEmpty(jobs) : "Jobs emptiness must be checked in native platform.";

        if (!F.isEmpty(subgrid)) {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(jobs.size(), 1);

            for (PlatformJob job : jobs)
                map.put(job, lb.getBalancedNode(job, null));

            return map;
        }
        else
            return Collections.emptyMap();
    }

    /**
     * @param jobs Jobs.
     */
    public void jobs(Collection<PlatformJob> jobs) {
        this.jobs = jobs;
    }
}