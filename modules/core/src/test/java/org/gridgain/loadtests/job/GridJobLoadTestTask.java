/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.job;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.compute.GridComputeJobResultPolicy.*;

/**
 * Test task for {@link GridJobLoadTest}
 */
public class GridJobLoadTestTask extends GridComputeTaskAdapter<GridJobLoadTestParams, Integer> {
    /**{@inheritDoc} */
    @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable GridJobLoadTestParams arg)
        throws GridException {
        assert !subgrid.isEmpty();

        Map<GridComputeJob, ClusterNode> jobs = new HashMap<>();

        for (int i = 0; i < arg.getJobsCount(); i++)
            jobs.put(
                new GridJobLoadTestJob(
                    /*only on the first step*/i == 0,
                    arg.getJobFailureProbability(),
                    arg.getExecutionDuration(),
                    arg.getCompletionDelay()),
                subgrid.get(0));

        return jobs;
    }

    /**
     * Always trying to failover job, except failed assertions.
     *
     * {@inheritDoc}
     */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        return res.getException() == null ? WAIT :
            res.getException().getCause() instanceof AssertionError ? REDUCE : FAILOVER;
    }

    /**{@inheritDoc} */
    @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
        int sum = 0;

        for (GridComputeJobResult r: results) {
            if (!r.isCancelled() && r.getException() == null)
                sum += r.<Integer>getData();
        }

        return sum;
    }
}
