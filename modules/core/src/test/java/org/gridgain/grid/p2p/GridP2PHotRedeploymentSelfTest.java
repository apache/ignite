/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
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
    private IgniteDeploymentMode depMode;

    /** P2P timeout. */
    private static final long P2P_TIMEOUT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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
    private void processTestHotRedeployment(IgniteDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            waitForDiscovery(ignite1, ignite2);

            ClassLoader ldr = getExternalClassLoader();

            Class<? extends ComputeTask<Object, Integer>> taskCls =
                (Class<? extends ComputeTask<Object, Integer>>)ldr.loadClass(TASK_NAME);

            Integer res1 = ignite1.compute().
                execute(taskCls, Collections.singletonList(ignite2.cluster().localNode().id()));

            info("Result1: " + res1);

            assert res1 != null;

            ignite1.compute().localDeployTask(taskCls, taskCls.getClassLoader());

            Integer res2 = (Integer)ignite1.compute().execute(taskCls.getName(),
                Collections.singletonList(ignite2.cluster().localNode().id()));

            info("Result2: " + res2);

            assert res2 != null;

            assert res1.equals(res2);
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
    private void processTestClassLoaderHotRedeployment(IgniteDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            waitForDiscovery(ignite1, ignite2);

            ClassLoader ldr1 = getExternalClassLoader();
            ClassLoader ldr2 = getExternalClassLoader();

            Class<? extends ComputeTask<Object, Integer>> taskCls1 =
                (Class<? extends ComputeTask<Object, Integer>>)ldr1.loadClass(TASK_NAME);
            Class<? extends ComputeTask<Object, Integer>> taskCls2 =
                (Class<? extends ComputeTask<Object, Integer>>)ldr2.loadClass(TASK_NAME);

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

            ignite2.compute().localDeployTask(taskCls1, taskCls1.getClassLoader());

            Integer res1 = ignite1.compute().execute(taskCls1, Collections.singletonList(ignite2.cluster().localNode().id()));

            assert res1 != null;

            info("Result1: " + res1);

            Integer res2 = ignite1.compute().execute(taskCls2, Collections.singletonList(ignite2.cluster().localNode().id()));

            assert res2 != null;

            info("Result2: " + res2);

            assert !res1.equals(res2);

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
        processTestHotRedeployment(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderIsolatedClassLoaderMode() throws Exception {
        processTestHotRedeployment(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderContinuousMode() throws Exception {
        processTestHotRedeployment(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderSharedMode() throws Exception {
        processTestHotRedeployment(IgniteDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentPrivateMode() throws Exception {
        processTestClassLoaderHotRedeployment(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentIsolatedMode() throws Exception {
        processTestClassLoaderHotRedeployment(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentContinuousMode() throws Exception {
        processTestClassLoaderHotRedeployment(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testNewClassLoaderHotRedeploymentSharedMode() throws Exception {
        processTestClassLoaderHotRedeployment(IgniteDeploymentMode.SHARED);
    }
}
