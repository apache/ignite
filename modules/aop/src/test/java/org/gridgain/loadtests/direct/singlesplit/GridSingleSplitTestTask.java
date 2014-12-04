/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.singlesplit;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;

import java.util.*;

/**
 * Single split test task.
 */
public class GridSingleSplitTestTask extends GridComputeTaskAdapter<Integer, Integer> {
    /** */
    @GridTaskSessionResource
    private GridComputeTaskSession taskSes;

    /** */
    @GridLoadBalancerResource
    private GridComputeLoadBalancer balancer;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) throws GridException {
        assert !subgrid.isEmpty() : "Subgrid cannot be empty.";

        Map<ComputeJobAdapter, ClusterNode> jobs = new HashMap<>(subgrid.size());

        taskSes.setAttribute("1st", "1");
        taskSes.setAttribute("2nd", "2");

        Collection<UUID> assigned = new ArrayList<>(subgrid.size());

        for (int i = 0; i < arg; i++) {
            ComputeJobAdapter job = new ComputeJobAdapter(1) {
                /** */
                @GridTaskSessionResource private GridComputeTaskSession jobSes;

                /** {@inheritDoc} */
                @Override public Object execute() throws GridException {
                    assert jobSes != null;

                    Integer arg = this.<Integer>argument(0);

                    assert arg != null;

                    return new GridSingleSplitTestJobTarget().executeLoadTestJob(arg, jobSes);
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
    @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
        int retVal = 0;

        for (GridComputeJobResult res : results) {
            assert res.getException() == null : "Load test jobs can never fail: " + res;

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}
