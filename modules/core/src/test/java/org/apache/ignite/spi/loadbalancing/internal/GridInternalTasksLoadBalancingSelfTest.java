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

package org.apache.ignite.spi.loadbalancing.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.node.VisorNodePingTask;
import org.apache.ignite.internal.visor.node.VisorNodePingTaskArg;
import org.apache.ignite.internal.visor.node.VisorNodePingTaskResult;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test that will start two nodes with custom load balancing SPI and execute {@link GridInternal} task on it.
 */
public class GridInternalTasksLoadBalancingSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Expected job result. */
    private static final String JOB_RESULT = "EXPECTED JOB RESULT";

    /** Expected task result. */
    private static final String TASK_RESULT = JOB_RESULT + JOB_RESULT;

    /** */
    private static Ignite ignite;

    /** If {@code true} then special custom load balancer SPI will be used. */
    private static boolean customLoadBalancer = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ignite = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (customLoadBalancer)
            cfg.setLoadBalancingSpi(new CustomLoadBalancerSpi());

        return cfg;
    }

    /**
     * This test execute internal tasks over grid with custom balancer.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testInternalTaskBalancing() throws Exception {
        customLoadBalancer = true;

        ignite = startGrids(GRID_CNT);

        // Task with GridInternal should pass.
        assertEquals(TASK_RESULT, ignite.compute().execute(GridInternalTestTask.class.getName(), null));

        // Visor task should pass.
        UUID nid = ignite.cluster().localNode().id();

        VisorNodePingTaskResult ping = ignite.compute()
            .execute(VisorNodePingTask.class.getName(),
                new VisorTaskArgument<>(nid, new VisorNodePingTaskArg(nid), false));

        assertTrue(ping.isAlive());

        // Custom task should fail, because special test load balancer SPI returns null as balanced node.
        try {
            ignite.compute().execute(CustomTestTask.class.getName(), null);
        }
        catch (IgniteException e) {
            assertTrue(e.getMessage().startsWith("Node can not be null [mappedJob=org.apache.ignite.spi.loadbalancing.internal.GridInternalTasksLoadBalancingSelfTest$CustomTestJob"));
        }
    }

    /**
     * This test execute internal tasks over grid with default balancer.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testInternalTaskDefaultBalancing() throws Exception {
        customLoadBalancer = false;

        ignite = startGrids(GRID_CNT);

        // Task with GridInternal should pass.
        assertEquals(TASK_RESULT, ignite.compute().execute(GridInternalTestTask.class.getName(), null));

        // Visor task should pass.
        UUID nid = ignite.cluster().localNode().id();

        VisorNodePingTaskResult ping = ignite.compute()
            .execute(VisorNodePingTask.class.getName(),
                new VisorTaskArgument<>(nid, new VisorNodePingTaskArg(nid), false));

        assertTrue(ping.isAlive());

        // Custom task should pass.
        assertEquals(TASK_RESULT, ignite.compute().execute(CustomTestTask.class.getName(), null));
    }

    /**
     * Test task.
     */
    private static class CustomTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++)
                jobs.add(new CustomTestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            assert results != null;

            String res = "";

            for (ComputeJobResult jobRes : results)
                res += jobRes.getData();

            return res;
        }
    }

    /**
     * Test job.
     */
    private static class CustomTestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public String execute() {
            return JOB_RESULT;
        }
    }


    /**
     * Test task marked with @GridInternal.
     */
    @GridInternal
    private static class GridInternalTestTask extends CustomTestTask{

    }

    /**
     * Special test balancer that will do not any balancing.
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class CustomLoadBalancerSpi extends IgniteSpiAdapter implements LoadBalancingSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ClusterNode getBalancedNode(ComputeTaskSession ses, List<ClusterNode> top, ComputeJob job) throws IgniteException {
            return null; // Intentionally return null.
        }
    }
}
