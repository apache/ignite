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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.gridify.Gridify;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class P2PGridifySelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName());

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestBothNodesDeploy(DeploymentMode depMode) throws Exception {
        int res = 0;

        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            startGrid(2);

            GridTestClassLoader tstClsLdr = new GridTestClassLoader(
                Collections.singletonMap("org/apache/ignite/p2p/p2p.properties", "resource=loaded"),
                getClass().getClassLoader(),
                GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName()
            );

            Class<? extends ComputeTask<?, ?>> taskCls1 = (Class<? extends ComputeTask<?, ?>>)tstClsLdr.loadClass(
                GridP2PTestTask.class.getName());

            ignite1.compute().localDeployTask(taskCls1, taskCls1.getClassLoader());

            res = executeGridify(1);

            ignite1.compute().undeployTask(taskCls1.getName());
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }

        // P2P deployment
        assert res == 10 : "Invalid gridify result: " + res;
    }

    /**
     * @param res Result.
     * @return The same value as parameter has.
     */
    @Gridify(taskName = "org.apache.ignite.p2p.GridP2PTestTask",
        gridName="org.apache.ignite.p2p.GridP2PGridifySelfTest1")
    public int executeGridify(int res) {
        return res;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    private void processTestGridifyResource(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            startGrid(1);

            Integer res = executeGridifyResource(1);

            // P2P deployment
            assert res != null : "res != null";
            assert res == 1 : "Unexpected result [res=" + res + ", expected=0]";

            info("Tests passed.");
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * Note that this method sends instance of test class to remote node.
     * Be sure that this instance does not have none-serializable fields or references
     * to the objects that could not be instantiated like class loaders and so on.
     *
     * @param res Result.
     * @return The same value as parameter has.
     */
    @Gridify(gridName="org.apache.ignite.p2p.GridP2PGridifySelfTest1")
    public Integer executeGridifyResource(int res) {
        String path = "org/apache/ignite/p2p/p2p.properties";

        GridTestClassLoader tstClsLdr = new GridTestClassLoader(
            GridP2PTestTask.class.getName(),
            GridP2PTestJob.class.getName()
        );

        // Test property file load.
        byte [] bytes = new byte[20];

        try (InputStream in = tstClsLdr.getResourceAsStream(path)) {
            if (in == null) {
                System.out.println("Resource could not be loaded: " + path);

                return -2;
            }

            in.read(bytes);
        }
        catch (IOException e) {
            System.out.println("Failed to read from resource stream: " + e.getMessage());

            return -3;
        }

        String rsrcVal = new String(bytes).trim();

        System.out.println("Remote resource content is : " + rsrcVal);

        if (!rsrcVal.equals("resource=loaded")) {
            System.out.println("Invalid loaded resource value: " + rsrcVal);

            return -4;
        }

        return res;
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourcePrivateMode() throws Exception {
        processTestGridifyResource(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourceIsolatedMode() throws Exception {
        processTestGridifyResource(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourceContinuousMode() throws Exception {
        processTestGridifyResource(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourceSharedMode() throws Exception {
        processTestGridifyResource(DeploymentMode.SHARED);
    }
}