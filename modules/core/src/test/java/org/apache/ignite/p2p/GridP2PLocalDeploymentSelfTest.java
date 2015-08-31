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

package org.apache.ignite.p2p;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test to make sure that if job executes on the same node, it reuses the same class loader as task.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ObjectEquality"})
@GridCommonTest(group = "P2P")
public class GridP2PLocalDeploymentSelfTest extends GridCommonAbstractTest {
    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

    /** */
    private static ClassLoader jobLdr;

    /** */
    private static ClassLoader taskLdr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Process one test.
     * @param depMode deployment mode.
     * @throws Exception if error occur.
     */
    private void processSharedModeTest(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ignite1.compute().execute(TestTask.class, ignite2.cluster().localNode().id());

            ClassLoader saveTaskLdr = taskLdr;
            ClassLoader saveJobLdr = jobLdr;

            ignite2.compute().execute(TestTask.class, ignite1.cluster().localNode().id());

            assert saveTaskLdr == jobLdr;
            assert saveJobLdr == taskLdr;
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception if error occur.
     */
    @SuppressWarnings({"unchecked"})
    public void testLocalDeployment() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            ClassLoader ldr1 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());
            ClassLoader ldr2 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());
            ClassLoader ldr3 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());

            Class taskCls = ldr1.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

            ignite1.compute().execute(taskCls, ignite1.cluster().localNode().id());

            taskCls = ldr2.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

            Integer res1 = (Integer)ignite2.compute().execute(taskCls, ignite1.cluster().localNode().id());

            taskCls = ldr3.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

            Integer res2 = (Integer)ignite3.compute().execute(taskCls, ignite1.cluster().localNode().id());

            assert !res1.equals(res2); // Resources are not same.
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * Process one test.
     * @param depMode deployment mode.
     * @throws Exception if error occur.
     */
    @SuppressWarnings({"unchecked"})
    private void processIsolatedModeTest(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader ldr1 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());
            ClassLoader ldr2 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());

            Class task1 = ldr1.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");
            Class task2 = ldr2.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            Integer res2 = (Integer)ignite2.compute().execute(task2, ignite1.cluster().localNode().id());

            assert !res1.equals(res2); // Class loaders are not same.

            assert !res1.equals(System.identityHashCode(ldr1));
            assert !res2.equals(System.identityHashCode(ldr2));
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processIsolatedModeTest(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processIsolatedModeTest(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processSharedModeTest(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processSharedModeTest(DeploymentMode.SHARED);
    }

    /**
     * Simple resource.
     */
    public static class UserResource {
        // No-op.
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class TestTask extends ComputeTaskAdapter<UUID, Serializable> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(final List<ClusterNode> subgrid, UUID arg) {
            taskLdr = getClass().getClassLoader();

            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg))
                    return Collections.singletonMap(new DeployementTestJob(arg), node);
            }

            throw new IgniteException("Failed to find target node: " + arg);
        }

        /** {@inheritDoc} */
        @Override public int[] reduce(List<ComputeJobResult> results) {
            assert results.size() == 1;

            assert taskLdr == getClass().getClassLoader();

            return null;
        }

        /**
         * Simple job class.
         */
        public static class DeployementTestJob extends ComputeJobAdapter {
            /** Ignite instance. */
            @IgniteInstanceResource
            private Ignite ignite;

            /**
             * @param nodeId Node ID for node this job is supposed to execute on.
             */
            public DeployementTestJob(UUID nodeId) { super(nodeId); }

            /** {@inheritDoc} */
            @Override public Serializable execute() {
                assert ignite.configuration().getNodeId().equals(argument(0));

                jobLdr = getClass().getClassLoader();

                return null;
            }
        }
    }
}