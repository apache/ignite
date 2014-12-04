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
import org.apache.ignite.compute.gridify.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PGridifySelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName());

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestBothNodesDeploy(GridDeploymentMode depMode) throws Exception {
        int res = 0;

        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            startGrid(2);

            GridTestClassLoader tstClsLdr = new GridTestClassLoader(
                Collections.singletonMap("org/gridgain/grid/p2p/p2p.properties", "resource=loaded"),
                getClass().getClassLoader(),
                GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName()
            );

            Class<? extends ComputeTask<?, ?>> taskCls1 = (Class<? extends ComputeTask<?, ?>>)tstClsLdr.loadClass(
                GridP2PTestTask.class.getName());

            ignite1.compute().localDeployTask(taskCls1, taskCls1.getClassLoader());

            res = executeGridify(1);

            ignite1.compute().undeployTask(taskCls1.getName());
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }

        // P2P deployment
        assert res == 10 : "Invalid gridify result: " + res;
    }

    /**
     * @param res Result.
     * @return The same value as parameter has.
     */
    @Gridify(taskName = "org.gridgain.grid.p2p.GridP2PTestTask",
        gridName="org.gridgain.grid.p2p.GridP2PGridifySelfTest1")
    public int executeGridify(int res) {
        return res;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    private void processTestGridifyResource(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            startGrid(1);

            Integer res = executeGridifyResource(1);

            // P2P deployment
            assert res != null : "res != null";
            assert res == 1 : "Unexpected result [res=" + res + ", expected=0]";

            info("Tests passed.");
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * Note that this method sends instance of test class to remote node.
     * Be sure that this instance does not have none-serializable fields or references
     * to the objects that could not be instantiated like class loaders and so on.
     *
     * @param res Result.
     * @return The same value as parameter has.
     */
    @Gridify(gridName="org.gridgain.grid.p2p.GridP2PGridifySelfTest1")
    public Integer executeGridifyResource(int res) {
        String path = "org/gridgain/grid/p2p/p2p.properties";

        GridTestClassLoader tstClsLdr = new GridTestClassLoader(
            GridP2PTestTask.class.getName(),
            GridP2PTestJob.class.getName()
        );

        // Test property file load.
        byte [] bytes = new byte[20];

        try (InputStream in = tstClsLdr.getResourceAsStream(path)) {
            if (in == null) {
                System.out.println("Resource could not be loaded: " + path);

                return -2;
            }

            in.read(bytes);
        }
        catch (IOException e) {
            System.out.println("Failed to read from resource stream: " + e.getMessage());

            return -3;
        }

        String rsrcVal = new String(bytes).trim();

        System.out.println("Remote resource content is : " + rsrcVal);

        if (!rsrcVal.equals("resource=loaded")) {
            System.out.println("Invalid loaded resource value: " + rsrcVal);

            return -4;
        }

        return res;
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTestBothNodesDeploy(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTestBothNodesDeploy(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTestBothNodesDeploy(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTestBothNodesDeploy(GridDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourcePrivateMode() throws Exception {
        processTestGridifyResource(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourceIsolatedMode() throws Exception {
        processTestGridifyResource(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourceContinuousMode() throws Exception {
        processTestGridifyResource(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testResourceSharedMode() throws Exception {
        processTestGridifyResource(GridDeploymentMode.SHARED);
    }
}
