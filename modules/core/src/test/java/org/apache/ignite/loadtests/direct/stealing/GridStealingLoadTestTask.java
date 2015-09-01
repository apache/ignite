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

package org.apache.ignite.loadtests.direct.stealing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Stealing load test task.
 */
public class GridStealingLoadTestTask extends ComputeTaskAdapter<UUID, Integer> {
    /** */
    @TaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    private UUID stealingNodeId;

    /** */
    private int stolenJobs;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) {
        assert arg != null;
        assert subgrid.size() > 1: "Test requires at least 2 nodes. One with load and another one to steal.";

        int jobsNum = subgrid.size();

        Map<GridStealingLoadTestJob, ClusterNode> map = new HashMap<>(jobsNum);

        stealingNodeId = arg;

        Iterator<ClusterNode> iter = subgrid.iterator();

        Collection<UUID> assigned = new ArrayList<>(subgrid.size());

        for (int i = 0; i < jobsNum; i++) {
            ClusterNode node = null;

            boolean nextNodeFound = false;

            while (iter.hasNext() && !nextNodeFound) {
                node = iter.next();

                // Do not map jobs to the stealing node.
                if (!node.id().equals(stealingNodeId))
                    nextNodeFound = true;

                // Recycle iterator.
                if (!iter.hasNext())
                    iter = subgrid.iterator();
            }

            assert node != null;

            assigned.add(node.id());

            map.put(new GridStealingLoadTestJob(), node);
        }

        taskSes.setAttribute("nodes", assigned);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        assert results != null;

        for (ComputeJobResult res : results) {
            if (res.getData() != null && stealingNodeId.equals(res.getData()))
                stolenJobs++;
        }

        return stolenJobs;
    }
}