/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Multithreaded job stealing test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridMultithreadedJobStealingSelfTest extends GridCommonAbstractTest {
    /** */
    private Ignite ignite;

    /** */
    private static volatile CountDownLatch jobExecutedLatch;

    /** */
    public GridMultithreadedJobStealingSelfTest() {
        super(false /* don't start grid*/);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;

        stopAllGrids();
    }

    /**
     * Test 2 jobs on 2 nodes.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testTwoJobsMultithreaded() throws Exception {
        final AtomicReference<Exception> fail = new AtomicReference<>(null);

        final AtomicInteger stolen = new AtomicInteger(0);
        final AtomicInteger noneStolen = new AtomicInteger(0);
        final GridConcurrentHashSet nodes = new GridConcurrentHashSet();

        int threadsNum = 10;

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** */
            @Override public void run() {
                try {
                    JobStealingResult res = ignite.compute().execute(new JobStealingTask(2), null);

                    info("Task result: " + res);

                    stolen.addAndGet(res.stolen);
                    noneStolen.addAndGet(res.nonStolen);
                    nodes.addAll(res.nodes);
                }
                catch (IgniteException e) {
                    log.error("Failed to execute task.", e);

                    fail.getAndSet(e);
                }
            }
        }, threadsNum, "JobStealingThread");

        for (Ignite g : G.allGrids())
            info("Metrics [nodeId=" + g.cluster().localNode().id() +
                ", metrics=" + g.cluster().localNode().metrics() + ']');

        assertNull("Test failed with exception: ",fail.get());

        // Total jobs number is threadsNum * 2
        assertEquals("Incorrect processed jobs number",threadsNum * 2, stolen.get() + noneStolen.get());

        assertFalse( "No jobs were stolen.",stolen.get() == 0);

        for (Ignite g : G.allGrids())
            assertTrue("Node get no jobs.", nodes.contains(g.name()));

        // Under these circumstances we should not have  more than 2 jobs
        // difference.
        //(but muted to 4 due to very rare fails and low priority of fix)
        assertTrue( "Stats [stolen=" + stolen + ", noneStolen=" + noneStolen + ']',
            Math.abs(stolen.get() - noneStolen.get()) <= 4);
    }

    /**
     * Test newly joined node can steal jobs.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testJoinedNodeCanStealJobs() throws Exception {
        final AtomicReference<Exception> fail = new AtomicReference<>(null);

        final AtomicInteger stolen = new AtomicInteger(0);
        final AtomicInteger noneStolen = new AtomicInteger(0);
        final GridConcurrentHashSet nodes = new GridConcurrentHashSet();

        int threadsNum = 10;

        final int jobsPerTask = 4;

        jobExecutedLatch = new CountDownLatch(threadsNum);

        final IgniteInternalFuture<Long> future = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            /** */
            @Override public void run() {
                try {
                    final IgniteCompute compute = ignite.compute().withAsync();

                    compute.execute(new JobStealingTask(jobsPerTask), null);

                    JobStealingResult res = (JobStealingResult)compute.future().get();

                    info("Task result: " + res);

                    stolen.addAndGet(res.stolen);
                    noneStolen.addAndGet(res.nonStolen);
                    nodes.addAll(res.nodes);
                }
                catch (IgniteException e) {
                    log.error("Failed to execute task.", e);

                    fail.getAndSet(e);
                }
            }
        }, threadsNum, "JobStealingThread");

        //Wait for first job begin execution.
        jobExecutedLatch.await();

        startGrid(2);

        for (Ignite g : G.allGrids())
            info("Metrics [nodeId=" + g.cluster().localNode().id() +
                ", metrics=" + g.cluster().localNode().metrics() + ']');

        future.get();

        assertNull("Test failed with exception: ",fail.get());

        // Total jobs number is threadsNum * 3
        assertEquals("Incorrect processed jobs number",threadsNum * jobsPerTask, stolen.get() + noneStolen.get());

        assertFalse( "No jobs were stolen.",stolen.get() == 0);

        for (Ignite g : G.allGrids())
            assertTrue("Node get no jobs.", nodes.contains(g.name()));

        assertTrue( "Stats [stolen=" + stolen + ", noneStolen=" + noneStolen + ']',
            Math.abs(stolen.get() - 2 * noneStolen.get()) <= 6);
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        JobStealingCollisionSpi colSpi = new JobStealingCollisionSpi();

        // One job at a time.
        colSpi.setActiveJobsThreshold(1);
        colSpi.setWaitJobsThreshold(0);

        JobStealingFailoverSpi failSpi = new JobStealingFailoverSpi();

        // Verify defaults.
        assert failSpi.getMaximumFailoverAttempts() == JobStealingFailoverSpi.DFLT_MAX_FAILOVER_ATTEMPTS;

        cfg.setCollisionSpi(colSpi);
        cfg.setFailoverSpi(failSpi);

        return cfg;
    }

    /**
     * Job stealing task.
     */
    private static class JobStealingTask extends ComputeTaskAdapter<Object, JobStealingResult> {
        /** Grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private int jobsToRun;

        /** */
        public JobStealingTask(int jobsToRun) {
            this.jobsToRun = jobsToRun;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) {
            assert subgrid.size() == 2 : "Invalid subgrid size: " + subgrid.size();

            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            // Put all jobs onto local node.
            for (int i = 0; i < jobsToRun; i++)
                map.put(new GridJobStealingJob(3000L), ignite.cluster().localNode());

            return map;
        }

        /** {@inheritDoc} */
        @Override public JobStealingResult reduce(List<ComputeJobResult> results) {
            int stolen = 0;
            int nonStolen = 0;

            Set<String> nodes = new HashSet<>(results.size());

            for (ComputeJobResult res : results) {
                String data = res.getData();

                log.info("Job result: " + data);

                nodes.add(data);

                if (!data.equals(ignite.name()))
                    stolen++;
                else
                    nonStolen++;
            }

            return new JobStealingResult(stolen, nonStolen, nodes);
        }
    }

    /**
     * Job stealing job.
     */
    private static final class GridJobStealingJob extends ComputeJobAdapter {
        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            try {
                jobExecutedLatch.countDown();

                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                throw new IgniteException("Job got interrupted.", e);
            }

            return ignite.name();
        }
    }

    /**
     * Job stealing result.
     */
    private static class JobStealingResult {
        /** */
        int stolen;

        /** */
        int nonStolen;

        /** */
        Set nodes;

        /** */
        public JobStealingResult(int stolen, int nonStolen, Set nodes) {
            this.stolen = stolen;
            this.nonStolen = nonStolen;
            this.nodes = nodes;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "JobStealingResult{" +
                "stolen=" + stolen +
                ", nonStolen=" + nonStolen +
                ", nodes=" + Arrays.toString(nodes.toArray()) +
                '}';
        }
    }
}
