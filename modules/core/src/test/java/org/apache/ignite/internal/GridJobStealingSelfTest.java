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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Job stealing test.
 */
@SuppressWarnings("unchecked")
@GridCommonTest(group = "Kernal Self")
public class GridJobStealingSelfTest extends GridCommonAbstractTest {
    /** Task execution timeout in milliseconds. */
    private static final int TASK_EXEC_TIMEOUT_MS = 50000;

    /** */
    private Ignite ignite1;

    /** */
    private Ignite ignite2;

    /** Job distribution map. Records which job has run on which node. */
    private static Map<UUID, Collection<ComputeJob>> jobDistrMap = new HashMap<>();

    /** */
    public GridJobStealingSelfTest() {
        super(false /* don't start grid*/);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        jobDistrMap.clear();

        ignite1 = startGrid(1);

        ignite2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignite1 = null;
        ignite2 = null;
    }

    /**
     * Test 2 jobs on 1 node.
     *
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testTwoJobs() throws IgniteCheckedException {
        executeAsync(ignite1.compute(), new JobStealingSingleNodeTask(2), null).get(TASK_EXEC_TIMEOUT_MS);

        // Verify that 1 job was stolen by second node.
        assertEquals(2, jobDistrMap.keySet().size());
        assertEquals(1, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
        assertEquals(1, jobDistrMap.get(ignite2.cluster().localNode().id()).size());
    }

    /**
     * Test 2 jobs on 1 node with null predicate.
     *
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testTwoJobsNullPredicate() throws IgniteCheckedException {
        executeAsync(ignite1.compute(), new JobStealingSingleNodeTask(2), null).get(TASK_EXEC_TIMEOUT_MS);

        // Verify that 1 job was stolen by second node.
        assertEquals(2, jobDistrMap.keySet().size());
        assertEquals(1, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
        assertEquals(1, jobDistrMap.get(ignite2.cluster().localNode().id()).size());
    }

    /**
     * Test 2 jobs on 1 node with null predicate using string task name.
     *
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testTwoJobsTaskNameNullPredicate() throws IgniteCheckedException {
        executeAsync(ignite1.compute(), JobStealingSingleNodeTask.class.getName(), null).get(TASK_EXEC_TIMEOUT_MS);

        // Verify that 1 job was stolen by second node.
        assertEquals(2, jobDistrMap.keySet().size());
        assertEquals(1, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
        assertEquals(1, jobDistrMap.get(ignite2.cluster().localNode().id()).size());
    }

    /**
     * Test 2 jobs on 1 node when one of the predicates is null.
     *
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testTwoJobsPartiallyNullPredicate() throws IgniteCheckedException {
        IgnitePredicate<ClusterNode> topPred =  new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode e) {
                    return ignite2.cluster().localNode().id().equals(e.id()); // Limit projection with only grid2.
                }
            };

        executeAsync(compute(ignite1.cluster().forPredicate(topPred)).withTimeout(TASK_EXEC_TIMEOUT_MS),
            new JobStealingSpreadTask(2), null).get(TASK_EXEC_TIMEOUT_MS);

        assertEquals(1, jobDistrMap.keySet().size());
        assertEquals(2, jobDistrMap.get(ignite2.cluster().localNode().id()).size());
        assertFalse(jobDistrMap.containsKey(ignite1.cluster().localNode().id()));
    }

    /**
     * Tests that projection predicate is taken into account by Stealing SPI.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionPredicate() throws Exception {
        final Ignite ignite3 = startGrid(3);

        executeAsync(compute(ignite1.cluster().forPredicate(new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode e) {
                return ignite1.cluster().localNode().id().equals(e.id()) ||
                    ignite3.cluster().localNode().id().equals(e.id()); // Limit projection with only grid1 or grid3 node.
            }
        })), new JobStealingSpreadTask(4), null).get(TASK_EXEC_TIMEOUT_MS);

        // Verify that jobs were run only on grid1 and grid3 (not on grid2)
        assertEquals(2, jobDistrMap.keySet().size());
        assertEquals(2, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
        assertEquals(2, jobDistrMap.get(ignite3.cluster().localNode().id()).size());
        assertFalse(jobDistrMap.containsKey(ignite2.cluster().localNode().id()));
    }

    /**
     * Tests that projection predicate is taken into account by Stealing SPI,
     * and that jobs in projection can steal tasks from each other.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionPredicateInternalStealing() throws Exception {
        final Ignite ignite3 = startGrid(3);

        waitForTopology(3);

        final UUID node1 = ignite1.cluster().localNode().id();
        final UUID node3 = ignite3.cluster().localNode().id();

        IgnitePredicate<ClusterNode> p = new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode e) {
                return node1.equals(e.id()) ||
                    node3.equals(e.id()); // Limit projection with only grid1 or grid3 node.
            }
        };

        executeAsync(compute(ignite1.cluster().forPredicate(p)), new JobStealingSingleNodeTask(4), null).get(TASK_EXEC_TIMEOUT_MS);

        // Verify that jobs were run only on grid1 and grid3 (not on grid2)
        assertEquals(2, jobDistrMap.keySet().size());
        assertFalse(jobDistrMap.containsKey(ignite2.cluster().localNode().id()));
    }

    /**
     * Tests that a job is not cancelled if there are no
     * available thief nodes in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeTopology() throws Exception {
        IgnitePredicate<ClusterNode> p = new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode e) {
                return ignite1.cluster().localNode().id().equals(e.id()); // Limit projection with only grid1 node.
            }
        };

        executeAsync(compute(ignite1.cluster().forPredicate(p)), new JobStealingSpreadTask(2), null).
            get(TASK_EXEC_TIMEOUT_MS);

        assertEquals(1, jobDistrMap.keySet().size());
        assertEquals(2, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
    }

    /**
     * Tests that a job is not cancelled if there are no
     * available thief nodes in projection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeProjection() throws Exception {
        ClusterGroup prj = ignite1.cluster().forNodeIds(Collections.singleton(ignite1.cluster().localNode().id()));

        executeAsync(compute(prj), new JobStealingSpreadTask(2), null).get(TASK_EXEC_TIMEOUT_MS);

        assertEquals(1, jobDistrMap.keySet().size());
        assertEquals(2, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
    }

    /**
     * Tests that a job is not cancelled if there are no
     * available thief nodes in projection. Uses null predicate.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNodeProjectionNullPredicate() throws Exception {
        ClusterGroup prj = ignite1.cluster().forNodeIds(Collections.singleton(ignite1.cluster().localNode().id()));

        executeAsync(compute(prj).withTimeout(TASK_EXEC_TIMEOUT_MS), new JobStealingSpreadTask(2), null).
            get(TASK_EXEC_TIMEOUT_MS);

        assertEquals(1, jobDistrMap.keySet().size());
        assertEquals(2, jobDistrMap.get(ignite1.cluster().localNode().id()).size());
    }

    /**
     * Tests job stealing with peer deployment and different class loaders.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testProjectionPredicateDifferentClassLoaders() throws Exception {
        final Ignite ignite3 = startGrid(3);

        URL[] clsLdrUrls;
        try {
            clsLdrUrls = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }

        ClassLoader ldr1 = new URLClassLoader(clsLdrUrls, getClass().getClassLoader());

        Class taskCls = ldr1.loadClass("org.apache.ignite.tests.p2p.JobStealingTask");
        Class nodeFilterCls = ldr1.loadClass("org.apache.ignite.tests.p2p.ExcludeNodeFilter");

        IgnitePredicate<ClusterNode> nodeFilter = (IgnitePredicate<ClusterNode>)nodeFilterCls
            .getConstructor(UUID.class).newInstance(ignite2.cluster().localNode().id());

        Map<UUID, Integer> ret = (Map<UUID, Integer>)executeAsync(compute(ignite1.cluster().forPredicate(nodeFilter)),
            taskCls, null).get(TASK_EXEC_TIMEOUT_MS);

        assert ret != null;
        assert ret.get(ignite1.cluster().localNode().id()) != null && ret.get(ignite1.cluster().localNode().id()) == 2 :
            ret.get(ignite1.cluster().localNode().id());
        assert ret.get(ignite3.cluster().localNode().id()) != null && ret.get(ignite3.cluster().localNode().id()) == 2 :
            ret.get(ignite3.cluster().localNode().id());
    }

    /**
     * @throws Exception If fatiled.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12629")
    @Test
    public void testJobStealingMbeanValidity() throws Exception {
        String[] beansToValidate = new String[] {
            "org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi$JobStealingCollisionSpiMBeanImpl",
            "org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi$JobStealingFailoverSpiMBeanImpl"};

        validateMbeans(ignite1, beansToValidate);
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
     * Job stealing task, that spreads jobs equally over the grid.
     */
    private static class JobStealingSpreadTask extends ComputeTaskAdapter<Object, Object> {
        /** Grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Number of jobs to spawn from task. */
        protected final int nJobs;

        /**
         * Constructs a new task instance.
         *
         * @param nJobs Number of jobs to spawn from this task.
         */
        JobStealingSpreadTask(int nJobs) {
            this.nJobs = nJobs;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) {
            //assert subgrid.size() == 2 : "Invalid subgrid size: " + subgrid.size();

            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            Iterator<ClusterNode> subIter = subgrid.iterator();

            // Spread jobs over subgrid.
            for (int i = 0; i < nJobs; i++) {
                if (!subIter.hasNext())
                    subIter = subgrid.iterator(); // wrap around

                map.put(new GridJobStealingJob(5000L), subIter.next());
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            for (ComputeJobResult res : results) {
                log.info("Job result: " + res.getData());
            }

            return null;
        }
    }

    /**
     * Job stealing task, that puts all jobs onto one node.
     */
    private static class JobStealingSingleNodeTask extends JobStealingSpreadTask {
        /** {@inheritDoc} */
        JobStealingSingleNodeTask(int nJobs) {
            super(nJobs);
        }

        /**
         * Default constructor.
         *
         * Uses 2 jobs.
         */
        JobStealingSingleNodeTask() {
            super(2);
        }

        /** {@inheritDoc} */
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
            assert subgrid.size() > 1 : "Invalid subgrid size: " + subgrid.size();

            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            // Put all jobs onto one node.
            for (int i = 0; i < nJobs; i++)
                map.put(new GridJobStealingJob(5000L), subgrid.get(0));

            return map;
        }
    }

    /**
     * Job stealing job.
     */
    private static final class GridJobStealingJob extends ComputeJobAdapter {
        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            log.info("Started job on node: " + ignite.cluster().localNode().id());

            if (!jobDistrMap.containsKey(ignite.cluster().localNode().id())) {
                Collection<ComputeJob> jobs = new ArrayList<>();
                jobs.add(this);

                jobDistrMap.put(ignite.cluster().localNode().id(), jobs);
            }
            else
                jobDistrMap.get(ignite.cluster().localNode().id()).add(this);

            try {
                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                log.info("Job got interrupted on node: " + ignite.cluster().localNode().id());

                throw new IgniteException("Job got interrupted.", e);
            }
            finally {
                log.info("Job finished on node: " + ignite.cluster().localNode().id());
            }

            return ignite.cluster().localNode().id();
        }
    }
}
