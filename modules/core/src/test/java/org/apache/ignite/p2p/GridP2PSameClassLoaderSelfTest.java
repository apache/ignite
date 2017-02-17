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
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test P2P deployment tasks which loaded from different class loaders.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown"})
@GridCommonTest(group = "P2P")
public class GridP2PSameClassLoaderSelfTest extends GridCommonAbstractTest {
    /** Class Name of task 1. */
    private static final String TEST_TASK1_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** Class Name of task 2. */
    private static final String TEST_TASK2_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath2";

    /** */
    private static final TcpDiscoveryIpFinder FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final ClassLoader CLASS_LOADER;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** */
    static {
        String path = GridTestProperties.getProperty("p2p.uri.cls");

        try {
            CLASS_LOADER = new URLClassLoader(new URL[] {new URL(path)},
                GridP2PSameClassLoaderSelfTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create URL: " + path, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(FINDER);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * Test.
     * @throws Exception if error occur
     */
    @SuppressWarnings({"unchecked"})
    private void processTest() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            assert GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return ignite1.cluster().nodes().size() == 3;
                }
            }, 20000L);

            Class task1 = CLASS_LOADER.loadClass(TEST_TASK1_NAME);
            Class task2 = CLASS_LOADER.loadClass(TEST_TASK2_NAME);

            // Execute task1 and task2 from node1 on node2 and make sure that they reuse same class loader on node2.
            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());
            Integer res2 = (Integer)ignite1.compute().execute(task2, ignite2.cluster().localNode().id());

            assert res1.equals(res2); // Class loaders are same

            Integer res3 = (Integer)ignite3.compute().execute(task1, ignite2.cluster().localNode().id());
            Integer res4 = (Integer)ignite3.compute().execute(task2, ignite2.cluster().localNode().id());

            assert res3.equals(res4);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        processTest();
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        depMode = DeploymentMode.ISOLATED;

        processTest();
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest();
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
