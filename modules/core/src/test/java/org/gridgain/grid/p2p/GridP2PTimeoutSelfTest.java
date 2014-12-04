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
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PTimeoutSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

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
    private void processTest(GridDeploymentMode depMode) throws Exception {
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
            catch (GridException ignored) {
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
    private void processFilterTest(GridDeploymentMode depMode) throws Exception {
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

            ignite.events().remoteQuery((IgnitePredicate<GridEvent>) filter1.newInstance(), 0);

            ldr.setTimeout(2000);

            try {
                ignite.events().remoteQuery((IgnitePredicate<GridEvent>) filter2.newInstance(), 0);

                assert false; // Timeout exception must be thrown.
            }
            catch (GridException ignored) {
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
        processTest(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTest(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTest(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTest(GridDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterPrivateMode() throws Exception {
        processFilterTest(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterIsolatedMode() throws Exception {
        processFilterTest(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterContinuousMode() throws Exception {
        processFilterTest(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testFilterSharedMode() throws Exception {
        processFilterTest(GridDeploymentMode.SHARED);
    }
}
