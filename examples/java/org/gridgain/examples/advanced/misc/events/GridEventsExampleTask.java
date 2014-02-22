// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.misc.events;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Example task used to generate events for event demonstration example.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridEventsExampleTask extends GridComputeTaskAdapter<String, Void> {
    /** Injected load balancer. */
    @GridLoadBalancerResource
    private GridComputeLoadBalancer balancer;

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, String arg) throws GridException {
        GridComputeJob job = new GridComputeJobAdapter() {
            @Nullable
            @Override public Serializable execute() {
                System.out.println(">>> Executing event example job on this node.");

                // This job does not return any result.
                return null;
            }
        };

        Map<GridComputeJob, GridNode> jobs = new HashMap<>(1);

        // Pick the next best balanced node for the job.
        jobs.put(job, balancer.getBalancedNode(job, null));

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
