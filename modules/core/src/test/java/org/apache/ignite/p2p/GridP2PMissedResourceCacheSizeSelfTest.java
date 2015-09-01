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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@GridCommonTest(group = "P2P")
public class GridP2PMissedResourceCacheSizeSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME1 = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** Task name. */
    private static final String TASK_NAME2 = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath2";

    /** Filter name. */
    private static final String FILTER_NAME1 = "org.apache.ignite.tests.p2p.P2PEventFilterExternalPath1";

    /** Filter name. */
    private static final String FILTER_NAME2 = "org.apache.ignite.tests.p2p.P2PEventFilterExternalPath2";

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** */
    private int missedRsrcCacheSize;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName());

        cfg.setDeploymentMode(depMode);

        cfg.setPeerClassLoadingMissedResourcesCacheSize(missedRsrcCacheSize);

        cfg.setCacheConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * Task execution here throws {@link IgniteCheckedException}.
     * This is correct behavior.
     *
     * @param g1 Grid 1.
     * @param g2 Grid 2.
     * @param task Task to execute.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void executeFail(Ignite g1, Ignite g2, Class task) {
        try {
            g1.compute().execute(task, g2.cluster().localNode().id());

            assert false; // Exception must be thrown.
        }
        catch (IgniteException e) {
            // Throwing exception is a correct behaviour.
            info("Received correct exception: " + e);
        }
    }

    /**
     * Querying events here throws {@link IgniteCheckedException}.
     * This is correct behavior.
     *
     * @param g Grid.
     * @param filter Event filter.
     */
    private void executeFail(ClusterGroup g, IgnitePredicate<Event> filter) {
        try {
            g.ignite().events(g).remoteQuery(filter, 0);

            assert false; // Exception must be thrown.
        }
        catch (IgniteException e) {
            // Throwing exception is a correct behaviour.
            info("Received correct exception: " + e);
        }
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processSize0Test(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        missedRsrcCacheSize = 0;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            String path = GridTestProperties.getProperty("p2p.uri.cls");

            info("Using path: " + path);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {
                new URL(path)
            });

            Class task = ldr.loadClass(TASK_NAME1);

            ignite1.compute().localDeployTask(task, task.getClassLoader());

            ldr.setExcludeClassNames(TASK_NAME1);

            executeFail(ignite1, ignite2, task);

            ldr.setExcludeClassNames();

            ignite1.compute().execute(task, ignite2.cluster().localNode().id());
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * TODO https://issues.apache.org/jira/browse/IGNITE-603
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
//    private void processSize2Test(GridDeploymentMode depMode) throws Exception {
//        this.depMode = depMode;
//
//        missedResourceCacheSize = 2;
//
//        try {
//            Grid g1 = startGrid(1);
//            Grid g2 = startGrid(2);
//
//            String path = GridTestProperties.getProperty("p2p.uri.cls");
//
//            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {new URL(path)});
//
//            Class task1 = ldr.loadClass(TASK_NAME1);
//            Class task2 = ldr.loadClass(TASK_NAME2);
//            GridPredicate<GridEvent> filter1 = (GridPredicate<GridEvent>)ldr.loadClass(FILTER_NAME1).newInstance();
//            GridPredicate<GridEvent> filter2 = (GridPredicate<GridEvent>)ldr.loadClass(FILTER_NAME2).newInstance();
//
//            g1.execute(GridP2PTestTask.class, 777).get(); // Create events.
//
//            g1.deployTask(task1);
//            g1.deployTask(task2);
//            g1.queryEvents(filter1, 0, F.<GridNode>localNode(g1)); // Deploy filter1.
//            g1.queryEvents(filter2, 0, F.<GridNode>localNode(g2)); // Deploy filter2.
//
//            ldr.setExcludeClassNames(TASK_NAME1, TASK_NAME2, FILTER_NAME1, FILTER_NAME2);
//
//            executeFail(g1, filter1);
//            executeFail(g1, g2, task1);
//
//            ldr.setExcludeClassNames();
//
//            executeFail(g1, filter1);
//            executeFail(g1, g2, task1);
//
//            ldr.setExcludeClassNames(TASK_NAME1, TASK_NAME2, FILTER_NAME1, FILTER_NAME2);
//
//            executeFail(g1, filter2);
//            executeFail(g1, g2, task2);
//
//            ldr.setExcludeClassNames();
//
//            executeFail(g1, filter2);
//            executeFail(g1, g2, task2);
//
//            g1.queryEvents(filter1, 0, F.<GridNode>alwaysTrue());
//
//            g1.execute(task1, g2.localNode().id()).get();
//        }
//        finally {
//            stopGrid(1);
//            stopGrid(2);
//        }
//    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0PrivateMode() throws Exception {
        processSize0Test(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0IsolatedMode() throws Exception {
        processSize0Test(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0ContinuousMode() throws Exception {
        processSize0Test(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0SharedMode() throws Exception {
        processSize0Test(DeploymentMode.SHARED);
    }
    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2PrivateMode() throws Exception {
//        processSize2Test(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2IsolatedMode() throws Exception {
//        processSize2Test(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2ContinuousMode() throws Exception {
//        processSize2Test(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2SharedMode() throws Exception {
//        processSize2Test(GridDeploymentMode.SHARED);
    }
}