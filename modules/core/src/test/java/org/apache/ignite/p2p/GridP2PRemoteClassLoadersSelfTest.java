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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PRemoteClassLoadersSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(
            GridP2PRemoteTestTask.class.getName(),
            GridP2PRemoteTestTask1.class.getName(),
            GridP2PRemoteTestJob.class.getName(),
            GridP2PRemoteClassLoadersSelfTest.class.getName()
        );

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed..
     */
    @SuppressWarnings("unchecked")
    private void processTestSameRemoteClassLoader(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            GridP2PTestStaticVariable.staticVar = 0;

            Ignite ignite1 = startGrid(1);
            startGrid(2);

            waitForRemoteNodes(ignite1, 1);

            ClassLoader tstClsLdr =
                new GridTestClassLoader(
                    Collections.<String, String>emptyMap(), getClass().getClassLoader(),
                    GridP2PRemoteTestTask.class.getName(), GridP2PRemoteTestTask1.class.getName(),
                    GridP2PRemoteTestJob.class.getName());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr.loadClass(GridP2PRemoteTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> task2 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr.loadClass(GridP2PRemoteTestTask1.class.getName());

            Object res1 = ignite1.compute().execute(task1.newInstance(), null);

            Object res2 = ignite1.compute().execute(task2.newInstance(), null);

            info("Check results.");

            // One remote p2p class loader
            assert res1 != null : "res1 != null";
            assert res1 instanceof Long : "res1 instanceof Long != true";
            assert res1.equals(0L): "Expected 0, got " + res1;

            // The same remote p2p class loader.
            assert res2 != null : "res2 != null";
            assert res2 instanceof Long : "res2 instanceof Long != true";
            assert res2.equals(1L) : "Expected 1 got " + res2;

            info("Tests passed.");
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestDifferentRemoteClassLoader(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            GridP2PTestStaticVariable.staticVar = 0;

            Ignite ignite1 = startGrid(1);
            startGrid(2);

            waitForRemoteNodes(ignite1, 1);

            ClassLoader tstClsLdr1 =
                new GridTestClassLoader(
                    Collections.EMPTY_MAP, getClass().getClassLoader(),
                    GridP2PRemoteTestTask.class.getName(), GridP2PRemoteTestJob.class.getName()
                );

            ClassLoader tstClsLdr2 =
                new GridTestClassLoader(
                    Collections.EMPTY_MAP, getClass().getClassLoader(),
                    GridP2PRemoteTestTask1.class.getName(), GridP2PRemoteTestJob.class.getName());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr1.loadClass(GridP2PRemoteTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> task2 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr2.loadClass(GridP2PRemoteTestTask1.class.getName());

            Object res1 = ignite1.compute().execute(task1.newInstance(), null);

            Object res2 = ignite1.compute().execute(task2.newInstance(), null);

            info("Check results.");

            // One remote p2p class loader
            assert res1 != null : "res1 != null";
            assert res1 instanceof Long : "res1 instanceof Long != true";
            assert res1.equals(0L): "Invalid res2 value: " + res1;

            // Another remote p2p class loader.
            assert res2 != null : "res2 == null";
            assert res2 instanceof Long : "res2 instanceof Long != true";
            assert res2.equals(0L) : "Invalid res2 value: " + res2;

            info("Tests passed.");
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderPrivateMode() throws Exception {
        processTestSameRemoteClassLoader(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderIsolatedMode() throws Exception {
        processTestSameRemoteClassLoader(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testDifferentClassLoaderPrivateMode() throws Exception {
        processTestDifferentRemoteClassLoader(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testDifferentClassLoaderIsolatedMode() throws Exception {
        processTestDifferentRemoteClassLoader(DeploymentMode.ISOLATED);
    }

    /**
     * Static variable holder class.
     */
    public static final class GridP2PTestStaticVariable {
        /** */
        @SuppressWarnings({"PublicField"})
        public static long staticVar;

        /**
         * Enforces singleton.
         */
        private GridP2PTestStaticVariable() {
            // No-op.
        }
    }

    /**
     * P2P test job.
     */
    public static class GridP2PRemoteTestJob extends ComputeJobAdapter {
        /**
         * @param arg Argument.
         */
        public GridP2PRemoteTestJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            // Return next value.
            return GridP2PTestStaticVariable.staticVar++;
        }
    }

    /**
     * P2P test task.
     */
    public static class GridP2PRemoteTestTask extends ComputeTaskAdapter<Serializable, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Serializable arg) {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

            for (ClusterNode node : subgrid) {
                if (!node.id().equals(ignite.configuration().getNodeId()))
                    map.put(new GridP2PRemoteTestJob(null) , node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results.size() == 1;

            ComputeJobResult res = results.get(0);

            if (log.isInfoEnabled())
                log.info("Got job result for aggregation: " + res);

            if (res.getException() != null)
                throw res.getException();

            return res.getData();
        }
    }

    /**
     * P2p test task.
     */
    public static class GridP2PRemoteTestTask1 extends GridP2PRemoteTestTask {
        // No-op.
    }
}