/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.loadtests.direct.multisplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Load test task.
 */
public class GridLoadTestTask extends ComputeTaskAdapter<Integer, Integer> {
    /** Injected job context. */
    @TaskSessionResource
    private ComputeTaskSession ctx;

    /** */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) {
        assert arg != null;
        assert arg > 1;

        Map<GridLoadTestJob, ClusterNode> map = new HashMap<>(subgrid.size());

        Iterator<ClusterNode> iter = subgrid.iterator();

        Collection<UUID> assigned = new ArrayList<>(subgrid.size());

        for (int i = 0; i < arg; i++) {
            // Recycle iterator.
            if (!iter.hasNext())
                iter = subgrid.iterator();

            ClusterNode node = iter.next();

            assigned.add(node.id());

            map.put(new GridLoadTestJob(arg - 1), node);
        }

        ctx.setAttribute("nodes", assigned);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        assert results != null;

        int retVal = 0;

        for (ComputeJobResult res : results) {
            assert res.getException() == null : "Load test jobs can never fail: " + ctx;
            assert res.getData() != null;

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}