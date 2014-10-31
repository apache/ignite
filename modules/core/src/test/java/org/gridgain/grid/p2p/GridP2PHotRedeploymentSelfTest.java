/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 *
 */
@SuppressWarnings({"JUnitTestClassNamingConvention", "ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PHotRedeploymentSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** P2P timeout. */
    private static final long P2P_TIMEOUT = 1000;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(TASK_NAME);

        cfg.setDeploymentMode(depMode);

        cfg.setNetworkTimeout(P2P_TIMEOUT);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    private void processTestHotRedeployment(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            waitForDiscovery(grid1, grid2);

            ClassLoader ldr = getExternalClassLoader();

            Class<? extends GridComputeTask<Object, int[]>> taskCls =
                (Class<? extends GridComputeTask<Object, int[]>>)ldr.loadClass(TASK_NAME);

            int[] res1 = grid1.compute().execute(taskCls, Collections.singletonList(grid2.cluster().localNode().id()));

            info("Result1: " + Arrays.toString(res1));

            assert res1 != null;

            grid1.compute().localDeployTask(taskCls, taskCls.getClassLoader());

            int[] res2 = (int[])grid1.compute().execute(taskCls.getName(),
                Collections.singletonList(grid2.cluster().localNode().id()));

            info("Result2: " + Arrays.toString(res2));

            assert res2 != null;

            assert res1[0] == res2[0];
            assert res1[1] == res2[1];
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
    private void processTestClassLoaderHotRedeployment(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            waitForDiscovery(grid1, grid2);

            ClassLoader ldr1 = getExternalClassLoader();
            ClassLoader ldr2 = getExternalClassLoader();

            Class<? extends GridComputeTask<Object, int[]>> taskCls1 =
                (Class<? extends GridComputeTask<Object, int[]>>)ldr1.loadClass(TASK_NAME);
            Class<? extends GridComputeTask<Object, int[]>> taskCls2 =
                (Class<? extends GridComputeTask<Object, int[]>>)ldr2.loadClass(TASK_NAME);

            // Check that different instances used.
            assert taskCls1.getClassLoader() != taskCls2.getClassLoader();
            assert taskCls1 != taskCls2;

//            final AtomicBoolean undeployed = new AtomicBoolean(false);
//
//            grid2.events().localListen(new GridLocalEventListener() {
//                @Override public void onEvent(GridEvent evt) {
//                    if (evt.type() == EVT_TASK_UNDEPLOYED) {
//                        assert ((GridDeploymentEvent)evt).alias().equals(TASK_NAME);
//
//                        undeployed.set(true);
//                    }
//                }
//            }, EVT_TASK_UNDEPLOYED);

            grid2.compute().localDeployTask(taskCls1, taskCls1.getClassLoader());

            int[] res1 = grid1.compute().execute(taskCls1, Collections.singletonList(grid2.cluster().localNode().id()));

            assert res1 != null;

            info("Result1: " + Arrays.toString(res1));

            int[] res2 = grid1.compute().execute(taskCls2, Collections.singletonList(grid2.cluster().localNode().id()));

            assert res2 != null;

            info("Result2: " + Arrays.toString(res2));

            assert res1[0] != res2[0];
            assert res1[1] != res2[1];

//            Thread.sleep(P2P_TIMEOUT * 2);
//
//            assert undeployed.get();
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
    public void testSameClassLoaderIsolatedMode() throws Exception {
        processTestHotRedeployment(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderIsolatedClassLoaderMode() throws Exception {
        processTestHotRedeployment(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderContinuousMode() throws Exception {
        processTestHotRedeployment(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderSharedMode() throws Exception {
        processTestHotRedeployment(GridDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentPrivateMode() throws Exception {
        processTestClassLoaderHotRedeployment(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentIsolatedMode() throws Exception {
        processTestClassLoaderHotRedeployment(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentContinuousMode() throws Exception {
        processTestClassLoaderHotRedeployment(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentSharedMode() throws Exception {
        processTestClassLoaderHotRedeployment(GridDeploymentMode.SHARED);
    }
}
