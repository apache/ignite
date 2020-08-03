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

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests affinity and affinity mapper P2P loading.
 */
public class GridPeerDeploymentRetryTest extends GridCommonAbstractTest {
    /** */
    private static final String EXT_TASK_CLASS_NAME = "org.apache.ignite.tests.p2p.CacheDeploymentTestTask2";

    /** URL of classes. */
    private static final URL[] URLS;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     *
     */
    public GridPeerDeploymentRetryTest() {
        super(false);
    }

    /**
     * Test {@link DeploymentMode#PRIVATE} mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testPrivateMode() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        deploymentTest();
    }

    /**
     * Test {@link DeploymentMode#ISOLATED} mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testIsolatedMode() throws Exception {
        depMode = DeploymentMode.ISOLATED;

        deploymentTest();
    }

    /**
     * Test {@link DeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        deploymentTest();
    }

    /**
     * Test {@link DeploymentMode#SHARED} mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        deploymentTest();
    }

    /** @throws Exception If failed. */
    private void deploymentTest() throws Exception {
        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        try {
            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(URLS);

            TestRecordingCommunicationSpi rec1 =
                (TestRecordingCommunicationSpi)g1.configuration().getCommunicationSpi();

            rec1.blockMessages((node, message) -> message instanceof GridDeploymentResponse);

            ComputeTask<Object, ?> task = (ComputeTask<Object, ?>)ldr.loadClass(EXT_TASK_CLASS_NAME).newInstance();

            ClusterNode node = g1.cluster().node(g2.cluster().localNode().id());

            try {
                g1.compute(g1.cluster().forRemotes()).execute(task, node);

                fail("Exception should be thrown");
            }
            catch (IgniteException ignore) {
                // Expected exception.
            }

            rec1.stopBlock(false, null, true, true);

            g1.compute(g1.cluster().forRemotes()).execute(task, node);
        }
        finally {
            stopAllGrids(true);
        }
    }
}
