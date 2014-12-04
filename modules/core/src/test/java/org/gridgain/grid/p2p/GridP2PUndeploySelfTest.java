/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.spi.deployment.local.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PUndeploySelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** Class Name of task. */
    private static final String TEST_TASK_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** */
    private Map<String, GridLocalDeploymentSpi> spis = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName,
        GridTestResources rsrcs) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName, rsrcs);

        GridLocalDeploymentSpi spi = new GridLocalDeploymentSpi();

        spis.put(gridName, spi);

        cfg.setDeploymentSpi(spi);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestUndeployLocalTasks(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader tstClsLdr = new GridTestClassLoader(GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName());

            Class<? extends GridComputeTask<?, ?>> task1 =
                (Class<? extends GridComputeTask<?, ?>>)tstClsLdr.loadClass(GridP2PTestTask.class.getName());

            ignite1.compute().localDeployTask(task1, tstClsLdr);

            ignite1.compute().execute(task1.getName(), 1);

            ignite2.compute().localDeployTask(task1, tstClsLdr);

            ignite2.compute().execute(task1.getName(), 2);

            GridLocalDeploymentSpi spi1 = spis.get(ignite1.name());
            GridLocalDeploymentSpi spi2 = spis.get(ignite2.name());

            assert spi1.findResource(task1.getName()) != null;
            assert spi2.findResource(task1.getName()) != null;

            assert ignite1.compute().localTasks().containsKey(task1.getName());
            assert ignite2.compute().localTasks().containsKey(task1.getName());

            ignite2.compute().undeployTask(task1.getName());

            // Wait for undeploy.
            Thread.sleep(1000);

            assert spi1.findResource(task1.getName()) == null;
            assert spi2.findResource(task1.getName()) == null;

            assert !ignite1.compute().localTasks().containsKey(task1.getName());
            assert !ignite2.compute().localTasks().containsKey(task1.getName());
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
    @SuppressWarnings("unchecked")
    private void processTestUndeployP2PTasks(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader ldr = new URLClassLoader(new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))},
                GridP2PSameClassLoaderSelfTest.class.getClassLoader());

            Class<? extends GridComputeTask<?, ?>> task1 =
                (Class<? extends GridComputeTask<?, ?>>)ldr.loadClass(TEST_TASK_NAME);

            ignite1.compute().localDeployTask(task1, ldr);

            ignite1.compute().execute(task1.getName(), ignite2.cluster().localNode().id());

            GridLocalDeploymentSpi spi1 = spis.get(ignite1.name());
            GridLocalDeploymentSpi spi2 = spis.get(ignite2.name());

            assert spi1.findResource(task1.getName()) != null;

            assert ignite1.compute().localTasks().containsKey(task1.getName());

            // P2P deployment will not deploy task into the SPI.
            assert spi2.findResource(task1.getName()) == null;

            ignite1.compute().undeployTask(task1.getName());

            // Wait for undeploy.
            Thread.sleep(1000);

            assert spi1.findResource(task1.getName()) == null;
            assert spi2.findResource(task1.getName()) == null;

            assert !ignite1.compute().localTasks().containsKey(task1.getName());
            assert !ignite2.compute().localTasks().containsKey(task1.getName());

            spis = null;
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * Test {@link GridDeploymentMode#PRIVATE} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalPrivateMode() throws Exception {
        processTestUndeployLocalTasks(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test {@link GridDeploymentMode#ISOLATED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalIsolatedMode() throws Exception {
        processTestUndeployLocalTasks(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test {@link GridDeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalContinuousMode() throws Exception {
        processTestUndeployLocalTasks(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test {@link GridDeploymentMode#SHARED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployLocalSharedMode() throws Exception {
        processTestUndeployLocalTasks(GridDeploymentMode.SHARED);
    }

    /**
     * Test {@link GridDeploymentMode#PRIVATE} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PPrivateMode() throws Exception {
        processTestUndeployP2PTasks(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PIsolatedMode() throws Exception {
        processTestUndeployP2PTasks(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test {@link GridDeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PContinuousMode() throws Exception {
        processTestUndeployP2PTasks(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test {@link GridDeploymentMode#SHARED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployP2PSharedMode() throws Exception {
        processTestUndeployP2PTasks(GridDeploymentMode.SHARED);
    }
}
