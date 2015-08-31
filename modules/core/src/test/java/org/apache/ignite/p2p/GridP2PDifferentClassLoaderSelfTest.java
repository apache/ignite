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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test P2P deployment tasks which loaded from different class loaders.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown"})
@GridCommonTest(group = "P2P")
public class GridP2PDifferentClassLoaderSelfTest extends GridCommonAbstractTest {
    /**
     * Class Name of task 1.
     */
    private static final String TEST_TASK1_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /**
     * Class Name of task 2.
     */
    private static final String TEST_TASK2_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath2";

    /**
     * URL of classes.
     */
    private static final URL[] URLS;

    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

    /**
     * Initialize URLs.
     */
    static {
        try {
            URLS = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Test.
     * @param isSameTask whether load same task or different task
     * @param expectEquals whether expected
     * @throws Exception if error occur
     */
    @SuppressWarnings({"ObjectEquality", "unchecked"})
    private void processTest(boolean isSameTask, boolean expectEquals) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            Class task1;
            Class task2;

            if (isSameTask) {
                ClassLoader ldr1 = new URLClassLoader(URLS, getClass().getClassLoader());
                ClassLoader ldr2 = new URLClassLoader(URLS, getClass().getClassLoader());

                task1 = ldr1.loadClass(TEST_TASK1_NAME);
                task2 = ldr2.loadClass(TEST_TASK1_NAME);
            }
            else {
                ClassLoader ldr1 = new GridTestExternalClassLoader(URLS, TEST_TASK2_NAME);
                ClassLoader ldr2 = new GridTestExternalClassLoader(URLS, TEST_TASK1_NAME);

                task1 = ldr1.loadClass(TEST_TASK1_NAME);
                task2 = ldr2.loadClass(TEST_TASK2_NAME);
            }

            // Execute task1 and task2 from node1 on node2 and make sure that they reuse same class loader on node2.
            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());
            Integer res2 = (Integer)ignite1.compute().execute(task2, ignite2.cluster().localNode().id());

            if (expectEquals)
                assert res1.equals(res2);
            else
                assert !res1.equals(res2);
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
        depMode = DeploymentMode.PRIVATE;

        processTest(false, false);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        depMode = DeploymentMode.ISOLATED;

        processTest(false, false);
    }

    /**
     * Test {@link org.apache.ignite.configuration.DeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest(false, false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest(false, false);
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeployPrivateMode() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        processTest(true, false);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeployIsolatedMode() throws Exception {
        depMode = DeploymentMode.ISOLATED;

        processTest(true, false);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeployContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest(true, false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeploySharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest(true, false);
    }

    /**
     * Return true if and only if all elements of array are different.
     *
     * @param m1 array 1.
     * @param m2 array 2.
     * @return true if all elements of array are different.
     */
    private boolean isNotSame(int[] m1, int[] m2) {
        assert m1.length == m2.length;
        assert m1.length == 2;

        return m1[0] != m2[0] && m1[1] != m2[1];
    }
}