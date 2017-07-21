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

package org.apache.loadtests.direct.singlesplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.LoadBalancerResource;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Single split test task.
 */
public class SingleSplitTestTask extends ComputeTaskAdapter<Integer, Integer> {
    /** */
    @TaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    @LoadBalancerResource
    private ComputeLoadBalancer balancer;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) {
        assert !subgrid.isEmpty() : "Subgrid cannot be empty.";

        Map<ComputeJobAdapter, ClusterNode> jobs = new HashMap<>(subgrid.size());

        taskSes.setAttribute("1st", "1");
        taskSes.setAttribute("2nd", "2");

        Collection<UUID> assigned = new ArrayList<>(subgrid.size());

        for (int i = 0; i < arg; i++) {
            ComputeJobAdapter job = new ComputeJobAdapter(1) {
                /** */
                @TaskSessionResource
                private ComputeTaskSession jobSes;

                /** {@inheritDoc} */
                @Override public Object execute() {
                    assert jobSes != null;

                    Integer arg = this.<Integer>argument(0);

                    assert arg != null;

                    return new SingleSplitTestJobTarget().executeLoadTestJob(arg, jobSes);
                }
            };

            ClusterNode node = balancer.getBalancedNode(job, null);

            assert node != null;

            assigned.add(node.id());

            jobs.put(job, node);
        }

        taskSes.setAttribute("nodes", assigned);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        int retVal = 0;

        for (ComputeJobResult res : results) {
            assert res.getException() == null : "Load test jobs can never fail: " + res;

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}