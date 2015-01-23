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

package org.apache.ignite.loadtests.direct.newnodes;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;

import java.io.*;
import java.util.*;

/**
 * Single split on new nodes test task.
 */
public class GridSingleSplitNewNodesTestTask extends ComputeTaskAdapter<Integer, Integer> {
    /** */
    @IgniteTaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    @IgniteLoadBalancerResource
    private ComputeLoadBalancer balancer;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) throws IgniteCheckedException {
        assert !subgrid.isEmpty() : "Subgrid cannot be empty.";

        Map<ComputeJobAdapter, ClusterNode> jobs = new HashMap<>(subgrid.size());

        taskSes.setAttribute("1st", "1");
        taskSes.setAttribute("2nd", "2");

        Collection<UUID> assigned = new ArrayList<>(subgrid.size());

        for (int i = 0; i < arg; i++) {
            ComputeJobAdapter job = new ComputeJobAdapter(1) {
                /** */
                @IgniteTaskSessionResource
                private ComputeTaskSession jobSes;

                /** {@inheritDoc} */
                @Override public Serializable execute() throws IgniteCheckedException {
                    assert jobSes != null;

                    Integer arg = this.<Integer>argument(0);

                    assert arg != null;

                    return new GridSingleSplitNewNodesTestJobTarget().executeLoadTestJob(arg, jobSes);
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
    @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        int retVal = 0;

        for (ComputeJobResult res : results) {
            assert res.getData() != null : "Load test should return result: " + res;

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}
