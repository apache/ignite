/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.newnodes;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

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
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) throws GridException {
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
                @Override public Serializable execute() throws GridException {
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
    @Override public Integer reduce(List<ComputeJobResult> results) throws GridException {
        int retVal = 0;

        for (ComputeJobResult res : results) {
            assert res.getData() != null : "Load test should return result: " + res;

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}
