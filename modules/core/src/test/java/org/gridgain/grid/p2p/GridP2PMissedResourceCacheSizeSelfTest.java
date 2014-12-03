/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;

/**
 *
 */
@GridCommonTest(group = "P2P")
public class GridP2PMissedResourceCacheSizeSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME1 = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** Task name. */
    private static final String TASK_NAME2 = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath2";

    /** Filter name. */
    private static final String FILTER_NAME1 = "org.gridgain.grid.tests.p2p.GridP2PEventFilterExternalPath1";

    /** Filter name. */
    private static final String FILTER_NAME2 = "org.gridgain.grid.tests.p2p.GridP2PEventFilterExternalPath2";

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** */
    private int missedRsrcCacheSize;

    /** */
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName());

        cfg.setDeploymentMode(depMode);

        cfg.setPeerClassLoadingMissedResourcesCacheSize(missedRsrcCacheSize);

        cfg.setCacheConfiguration();

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * Task execution here throws {@link GridException}.
     * This is correct behavior.
     *
     * @param g1 Grid 1.
     * @param g2 Grid 2.
     * @param task Task to execute.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void executeFail(Grid g1, Grid g2, Class task) {
        try {
            g1.compute().execute(task, g2.cluster().localNode().id());

            assert false; // Exception must be thrown.
        }
        catch (GridException e) {
            // Throwing exception is a correct behaviour.
            info("Received correct exception: " + e);
        }
    }

    /**
     * Querying events here throws {@link GridException}.
     * This is correct behavior.
     *
     * @param g Grid.
     * @param filter Event filter.
     */
    private void executeFail(GridProjection g, GridPredicate<GridEvent> filter) {
        try {
            g.grid().events(g).remoteQuery(filter, 0);

            assert false; // Exception must be thrown.
        }
        catch (GridException e) {
            // Throwing exception is a correct behaviour.
            info("Received correct exception: " + e);
        }
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processSize0Test(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        missedRsrcCacheSize = 0;

        try {
            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            String path = GridTestProperties.getProperty("p2p.uri.cls");

            info("Using path: " + path);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {
                new URL(path)
            });

            Class task = ldr.loadClass(TASK_NAME1);

            grid1.compute().localDeployTask(task, task.getClassLoader());

            ldr.setExcludeClassNames(TASK_NAME1);

            executeFail(grid1, grid2, task);

            ldr.setExcludeClassNames();

            grid1.compute().execute(task, grid2.cluster().localNode().id());
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * TODO GG-3804
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
        processSize0Test(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0IsolatedMode() throws Exception {
        processSize0Test(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0ContinuousMode() throws Exception {
        processSize0Test(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0SharedMode() throws Exception {
        processSize0Test(GridDeploymentMode.SHARED);
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
