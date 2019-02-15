/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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