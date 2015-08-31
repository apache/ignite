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
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test P2P class loading in SHARED_CLASSLOADER_UNDEPLOY mode.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PNodeLeftSelfTest extends GridCommonAbstractTest {
    /** */
    private static final ClassLoader urlClsLdr1;

    /** */
    static {
        String path = GridTestProperties.getProperty("p2p.uri.cls");

        try {
            urlClsLdr1 = new URLClassLoader(
                new URL[] { new URL(path) },
                GridP2PNodeLeftSelfTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create URL: " + path, e);
        }
    }

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
     * Test undeploy task.
     * @param isExpectUndeploy Whether undeploy is expected.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void processTest(boolean isExpectUndeploy) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Class task1 = urlClsLdr1.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            Thread.sleep(1000);

            // Task will be deployed after stop node1
            Integer res2 = (Integer)ignite3.compute().execute(task1, ignite2.cluster().localNode().id());

            if (isExpectUndeploy)
                assert !res1.equals(res2);
            else
                assert res1.equals(res2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest(false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest(true);
    }
}