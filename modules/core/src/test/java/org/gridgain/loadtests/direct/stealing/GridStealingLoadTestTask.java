/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.stealing;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;

import java.util.*;

/**
 * Stealing load test task.
 */
public class GridStealingLoadTestTask extends ComputeTaskAdapter<UUID, Integer> {
    /** */
    @IgniteTaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    private UUID stealingNodeId;

    /** */
    private int stolenJobs;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) throws IgniteCheckedException {
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
    @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        assert results != null;

        for (ComputeJobResult res : results) {
            if (res.getData() != null && stealingNodeId.equals(res.getData()))
                stolenJobs++;
        }

        return stolenJobs;
    }
}
