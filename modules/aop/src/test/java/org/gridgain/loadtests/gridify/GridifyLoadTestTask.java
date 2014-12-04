/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.gridify;

import org.apache.ignite.compute.*;
import org.apache.ignite.compute.gridify.*;
import org.gridgain.grid.*;

import java.io.*;
import java.util.*;

/**
 * Gridify load test task.
 */
public class GridifyLoadTestTask extends ComputeTaskSplitAdapter<GridifyArgument, Integer> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, GridifyArgument arg) throws GridException {
        assert gridSize > 0 : "Subgrid cannot be empty.";

        int jobsNum = (Integer)arg.getMethodParameters()[0];

        assert jobsNum > 0;

        Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < jobsNum; i++)
            jobs.add(new ComputeJobAdapter(1) {
                @Override public Serializable execute() {
                    Integer arg = this.<Integer>argument(0);

                    assert arg != null;

                    return new GridifyLoadTestJobTarget().executeLoadTestJob(arg);
                }
            });

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) throws GridException {
        int retVal = 0;

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                throw new GridException("Received exception in reduce method (load test jobs can never fail): " + res,
                    res.getException());
            }

            retVal += (Integer)res.getData();
        }

        return retVal;
    }
}
