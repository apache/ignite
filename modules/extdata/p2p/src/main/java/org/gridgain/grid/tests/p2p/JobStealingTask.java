package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Task for testing job stealing.
 */
public class JobStealingTask extends GridComputeTaskAdapter<Object, Map<UUID, Integer>> {
    /** Number of jobs to spawn from task. */
    private static final int N_JOBS = 4;

    /** Grid. */
    @GridInstanceResource
    private Ignite ignite;

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) throws GridException {
        assert !subgrid.isEmpty();

        Map<GridComputeJobAdapter, ClusterNode> map = U.newHashMap(subgrid.size());

        // Put all jobs onto one node.
        for (int i = 0; i < N_JOBS; i++)
            map.put(new GridJobStealingJob(5000L), subgrid.get(0));

        return map;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SuspiciousMethodCalls")
    @Override public Map<UUID, Integer> reduce(List<GridComputeJobResult> results) throws GridException {
        Map<UUID, Integer> ret = U.newHashMap(results.size());

        for (GridComputeJobResult res : results) {
            log.info("Job result: " + res.getData());

            UUID resUuid = (UUID)res.getData();

            ret.put(resUuid,
                ret.containsKey(resUuid) ? ret.get(resUuid) + 1 : 1);
        }

        return ret;
    }

    /**
     * Job stealing job.
     */
    private static final class GridJobStealingJob extends GridComputeJobAdapter {
        /** Injected grid. */
        @GridInstanceResource
        private Ignite ignite;

        /** Logger. */
        @GridLoggerResource
        private GridLogger log;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            log.info("Started job on node: " + ignite.cluster().localNode().id());

            try {
                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                log.info("Job got interrupted on node: " + ignite.cluster().localNode().id());

                throw new GridException("Job got interrupted.", e);
            }
            finally {
                log.info("Job finished on node: " + ignite.cluster().localNode().id());
            }

            return ignite.cluster().localNode().id();
        }
    }
}
