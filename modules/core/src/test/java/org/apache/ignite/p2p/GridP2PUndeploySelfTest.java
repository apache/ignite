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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PUndeploySelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** Class Name of task. */
    private static final String TEST_TASK_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** */
    private Map<String, LocalDeploymentSpi> spis = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName,
        IgniteTestResources rsrcs) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName, rsrcs);

        LocalDeploymentSpi spi = new LocalDeploymentSpi();

        spis.put(gridName, spi);

        cfg.setDeploymentSpi(spi);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestUndeployLocalTasks(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader tstClsLdr = new GridTestClassLoader(GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>)tstClsLdr.loadClass(GridP2PTestTask.class.getName());

            ignite1.compute().localDeployTask(task1, tstClsLdr);

            ignite1.compute().execute(task1.getName(), 1);

            ignite2.compute().localDeployTask(task1, tstClsLdr);

            ignite2.compute().execute(task1.getName(), 2);

            LocalDeploymentSpi spi1 = spis.get(ignite1.name());
            LocalDeploymentSpi spi2 = spis.get(ignite2.name());

            assert spi1.findResource(task1.getName()) != null;
            assert spi2.findResource(task1.getName()) != null;

            assert ignite1.compute().localTasks().containsKey(task1.getName());
            assert ignite2.compute().localTasks().containsKey(task1.getName());

            ignite2.compute().undeployTask(task1.getName());

            // Wait for undeploy.
            Thread.sleep(1000);

            assert spi1.findResource(task1.getName()) == null;
            assert spi2.findResource(task1.getName()) == null;

            assert !ignite1.compute().localTasks().containsKey(task1.getName());
            assert !ignite2.compute().localTasks().containsKey(task1.getName());
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
    private void processTestUndeployP2PTasks(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader ldr = new URLClassLoader(new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))},
                GridP2PSameClassLoaderSelfTest.class.getClassLoader());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>)ldr.loadClass(TEST_TASK_NAME);

            ignite1.compute().localDeployTask(task1, ldr);

            ignite1.compute().execute(task1.getName(), ignite2.cluster().localNode().id());

            LocalDeploymentSpi spi1 = spis.get(ignite1.name());
            LocalDeploymentSpi spi2 = spis.get(ignite2.name());

            assert spi1.findResource(task1.getName()) != null;

            assert ignite1.compute().localTasks().containsKey(task1.getName());

            // P2P deployment will not deploy task into the SPI.
            assert spi2.findResource(task1.getName()) == null;

            ignite1.compute().undeployTask(task1.getName());

            // Wait for undeploy.
            Thread.sleep(1000);

            assert spi1.findResource(task1.getName()) == null;
            assert spi2.findResource(task1.getName()) == null;

            assert !ignite1.compute().localTasks().containsKey(task1.getName());
            assert !ignite2.compute().localTasks().containsKey(task1.getName());

            spis = null;
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#PRIVATE} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalPrivateMode() throws Exception {
        processTestUndeployLocalTasks(DeploymentMode.PRIVATE);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#ISOLATED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalIsolatedMode() throws Exception {
        processTestUndeployLocalTasks(DeploymentMode.ISOLATED);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalContinuousMode() throws Exception {
        processTestUndeployLocalTasks(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#SHARED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalSharedMode() throws Exception {
        processTestUndeployLocalTasks(DeploymentMode.SHARED);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#PRIVATE} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PPrivateMode() throws Exception {
        processTestUndeployP2PTasks(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PIsolatedMode() throws Exception {
        processTestUndeployP2PTasks(DeploymentMode.ISOLATED);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PContinuousMode() throws Exception {
        processTestUndeployP2PTasks(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#SHARED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PSharedMode() throws Exception {
        processTestUndeployP2PTasks(DeploymentMode.SHARED);
    }
}