package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Task for testing job stealing.
 */
public class JobStealingTask extends ComputeTaskAdapter<Object, Map<UUID, Integer>> {
    /** Number of jobs to spawn from task. */
    private static final int N_JOBS = 4;

    /** Grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) throws IgniteCheckedException {
        assert !subgrid.isEmpty();

        Map<ComputeJobAdapter, ClusterNode> map = U.newHashMap(subgrid.size());

        // Put all jobs onto one node.
        for (int i = 0; i < N_JOBS; i++)
            map.put(new GridJobStealingJob(5000L), subgrid.get(0));

        return map;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SuspiciousMethodCalls")
    @Override public Map<UUID, Integer> reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        Map<UUID, Integer> ret = U.newHashMap(results.size());

        for (ComputeJobResult res : results) {
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
    private static final class GridJobStealingJob extends ComputeJobAdapter {
        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws IgniteCheckedException {
            log.info("Started job on node: " + ignite.cluster().localNode().id());

            try {
                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                log.info("Job got interrupted on node: " + ignite.cluster().localNode().id());

                throw new IgniteCheckedException("Job got interrupted.", e);
            }
            finally {
                log.info("Job finished on node: " + ignite.cluster().localNode().id());
            }

            return ignite.cluster().localNode().id();
        }
    }
}
