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

package org.apache.ignite.hadoop.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.hadoop.HadoopInputSplit;
import org.apache.ignite.hadoop.HadoopJob;
import org.apache.ignite.hadoop.HadoopMapReducePlan;
import org.apache.ignite.hadoop.HadoopMapReducePlanner;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopDefaultMapReducePlan;
import org.jetbrains.annotations.Nullable;

/**
 * Round-robin mr planner.
 */
public class HadoopTestRoundRobinMrPlanner implements HadoopMapReducePlanner {
    /** {@inheritDoc} */
    @Override public HadoopMapReducePlan preparePlan(HadoopJob job, Collection<ClusterNode> top,
        @Nullable HadoopMapReducePlan oldPlan) throws IgniteCheckedException {
        if (top.isEmpty())
            throw new IllegalArgumentException("Topology is empty");

        // Has at least one element.
        Iterator<ClusterNode> it = top.iterator();

        Map<UUID, Collection<HadoopInputSplit>> mappers = new HashMap<>();

        for (HadoopInputSplit block : job.input()) {
            ClusterNode node = it.next();

            Collection<HadoopInputSplit> nodeBlocks = mappers.get(node.id());

            if (nodeBlocks == null) {
                nodeBlocks = new ArrayList<>();

                mappers.put(node.id(), nodeBlocks);
            }

            nodeBlocks.add(block);

            if (!it.hasNext())
                it = top.iterator();
        }

        int[] rdc = new int[job.reducers()];

        for (int i = 0; i < rdc.length; i++)
            rdc[i] = i;

        return new HadoopDefaultMapReducePlan(mappers, Collections.singletonMap(it.next().id(), rdc));
    }
}