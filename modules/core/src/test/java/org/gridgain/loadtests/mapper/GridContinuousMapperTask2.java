/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.mapper;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task.
 */
public class GridContinuousMapperTask2 extends GridComputeTaskAdapter<int[], Integer> {
    /** Grid. */
    @GridInstanceResource
    private Ignite g;

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable int[] jobIds)
        throws GridException {
        Map<GridComputeJob, GridNode> mappings = new HashMap<>(jobIds.length);

        Iterator<GridNode> nodeIter = g.cluster().forRemotes().nodes().iterator();

        for (int jobId : jobIds) {
            GridComputeJob job = new GridComputeJobAdapter(jobId) {
                @GridInstanceResource
                private Ignite g;

                @Override public Object execute() {
                    Integer jobId = argument(0);

                    X.println(">>> Received job for ID: " + jobId);

                    return g.cache("replicated").peek(jobId);
                }
            };

            // If only local node in the grid.
            if (g.cluster().nodes().size() == 1)
                mappings.put(job, g.cluster().localNode());
            else {
                GridNode n = nodeIter.hasNext() ? nodeIter.next() :
                    (nodeIter = g.cluster().forRemotes().nodes().iterator()).next();

                mappings.put(job, n);
            }
        }

        return mappings;
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        TestObject o = res.getData();

        X.println("Received job result from node [resId=" + o.getId() + ", node=" + res.getNode().id() + ']');

        return super.result(res, rcvd);
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
        X.println(">>> Reducing task...");

        return null;
    }
}
