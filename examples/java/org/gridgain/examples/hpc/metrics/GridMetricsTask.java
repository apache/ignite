// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.metrics;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Metrics example task that demonstrates how {@link GridNode#metrics()}
 * can be used to task execution.
 * <p>
 * For the purpose of example, this task inspects all nodes in its topology
 * and sends a job to a node only if number of processors on that node
 * is greater than {@code 1} and current CPU load on that node is less
 * than {@code 50%}. If no node falls into such criteria, then local
 * node is used for execution.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMetricsTask extends GridComputeTaskAdapter<Object, Object> {
    /** Injected grid instance. */
    @GridInstanceResource
    private Grid grid;

    /**
     * This task will create jobs and send them to remote nodes only
     * if remote node has more than 1 processor and CPU load on remote
     * node is less than 50%.
     * <p>
     * If none of the nodes fall under criteria above, then job will be
     * executed locally.
     *
     * @param subgrid Task node topology.
     * @param arg Task argument (ignored for this example).
     * @return {@link org.gridgain.grid.compute.GridComputeJob} instances mapped to nodes for execution.
     * @throws GridException If map operation failed.
     */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
        Map<GridComputeJobAdapter, GridNode> jobs = new HashMap<>(subgrid.size());

        for (GridNode node : subgrid) {
            // Get metrics for given node.
            GridNodeMetrics metrics = node.metrics();

            System.out.println("Checking node metrics [nodeId=" + node.id() +
                ", cpuLoad=" + metrics.getCurrentCpuLoad() + ", cpus=" + metrics.getTotalCpus() + ']');

            // For the sake of this example, we only send a job to a node
            // if it has more than one processor and if it's CPU is less than 50% loaded.
            if (metrics.getTotalCpus() > 1 && metrics.getCurrentCpuLoad() < 0.5)
                jobs.put(new GridMetricsJob(), node);
        }

        // If no node qualified for job execution because either
        // number of processors was 1 or CPU load was greater than
        // 50%, then execute the job locally.
        if (jobs.isEmpty())
            jobs.put(new GridMetricsJob(), grid.localNode());

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        // Nothing to reduce.
        return null;
    }

    /**
     * Example job to demonstrate node metrics usage.
     * The execution simply prints out the metrics for
     * the local node.
     *
     * @author @java.author
     * @version @java.version
     */
    private static class GridMetricsJob extends GridComputeJobAdapter {
        /** Injected grid instance. */
        @GridInstanceResource
        private Grid grid;

        /**
         * For the purpose of this example, we simply print out metrics
         * for the node this job is running on.
         *
         * @return {@code null} as the job simply prints out metrics on
         *      local node.
         */
        @Override public Serializable execute() {
            // Simply print out metrics for the node this job is running on
            // and return none.
            System.out.println("Printing node metrics from grid job [nodeId=" + grid.localNode().id() +
                ", nodeMetrics=" + grid.localNode().metrics() + ']');

            // Nothing to return.
            return null;
        }
    }
}

