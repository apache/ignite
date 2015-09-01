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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test to make sure that if job executes on the same node, it reuses the same class loader as task.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PJobClassLoaderSelfTest extends GridCommonAbstractTest {
    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

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
    private void processTest(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite = startGrid(1);

            ignite.compute().execute(Task.class, null);
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTest(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTest(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTest(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTest(DeploymentMode.SHARED);
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
    public static class Task extends ComputeTaskSplitAdapter<Object, Object> {
        /**
         * ClassLoader loaded task.
         */
        private static ClassLoader ldr;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            assert gridSize == 1;

            ldr = getClass().getClassLoader();

            return Collections.singletonList(new ComputeJobAdapter() {
                    /** {@inheritDoc} */
                    @SuppressWarnings({"ObjectEquality"})
                    public Serializable execute() {
                        assert getClass().getClassLoader() == ldr;

                        return null;
                    }
                });
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            // Nothing to reduce.
            return null;
        }
    }
}