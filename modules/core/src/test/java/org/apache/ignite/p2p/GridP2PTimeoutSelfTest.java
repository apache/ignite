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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.config.*;
import org.apache.ignite.testframework.junits.common.*;

import java.net.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PTimeoutSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private IgniteDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName());

        cfg.setDeploymentMode(depMode);

        cfg.setNetworkTimeout(1000);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTest(IgniteDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            String path = GridTestProperties.getProperty("p2p.uri.cls");

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {new URL(path)});

            Class task1 = ldr.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");
            Class task2 = ldr.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath2");

            ldr.setTimeout(100);

            g1.compute().execute(task1, g2.cluster().localNode().id());

            ldr.setTimeout(2000);

            try {
                g1.compute().execute(task2, g2.cluster().localNode().id());

                assert false; // Timeout exception must be thrown.
            }
            catch (IgniteCheckedException ignored) {
                // Throwing exception is a correct behaviour.
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processFilterTest(IgniteDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite = startGrid(1);

            startGrid(2);

            ignite.compute().execute(GridP2PTestTask.class, 777); // Create events.

            String path = GridTestProperties.getProperty("p2p.uri.cls");

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {new URL(path)});

            Class filter1 = ldr.loadClass("org.gridgain.grid.tests.p2p.GridP2PEventFilterExternalPath1");
            Class filter2 = ldr.loadClass("org.gridgain.grid.tests.p2p.GridP2PEventFilterExternalPath2");

            ldr.setTimeout(100);

            ignite.events().remoteQuery((IgnitePredicate<IgniteEvent>) filter1.newInstance(), 0);

            ldr.setTimeout(2000);

            try {
                ignite.events().remoteQuery((IgnitePredicate<IgniteEvent>) filter2.newInstance(), 0);

                assert false; // Timeout exception must be thrown.
            }
            catch (IgniteCheckedException ignored) {
                // Throwing exception is a correct behaviour.
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTest(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTest(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTest(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTest(IgniteDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterPrivateMode() throws Exception {
        processFilterTest(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterIsolatedMode() throws Exception {
        processFilterTest(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterContinuousMode() throws Exception {
        processFilterTest(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterSharedMode() throws Exception {
        processFilterTest(IgniteDeploymentMode.SHARED);
    }
}
