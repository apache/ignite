/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.planner.GridHadoopDefaultMapReducePlan;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Round-robin mr planner.
 */
public class GridHadoopTestRoundRobinMrPlanner implements GridHadoopMapReducePlanner {
    /** {@inheritDoc} */
    @Override public GridHadoopMapReducePlan preparePlan(GridHadoopJob job, Collection<ClusterNode> top,
        @Nullable GridHadoopMapReducePlan oldPlan) throws GridException {
        if (top.isEmpty())
            throw new IllegalArgumentException("Topology is empty");

        // Has at least one element.
        Iterator<ClusterNode> it = top.iterator();

        Map<UUID, Collection<GridHadoopInputSplit>> mappers = new HashMap<>();

        for (GridHadoopInputSplit block : job.input()) {
            ClusterNode node = it.next();

            Collection<GridHadoopInputSplit> nodeBlocks = mappers.get(node.id());

            if (nodeBlocks == null) {
                nodeBlocks = new ArrayList<>();

                mappers.put(node.id(), nodeBlocks);
            }

            nodeBlocks.add(block);

            if (!it.hasNext())
                it = top.iterator();
        }

        int[] rdc = new int[job.info().reducers()];

        for (int i = 0; i < rdc.length; i++)
            rdc[i] = i;

        return new GridHadoopDefaultMapReducePlan(mappers, Collections.singletonMap(it.next().id(), rdc));
    }
}
