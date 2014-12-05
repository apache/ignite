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
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;
import java.net.*;
import java.util.*;

/**
 * Test P2P deployment tasks which loaded from different class loaders.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown"})
@GridCommonTest(group = "P2P")
public class GridP2PDifferentClassLoaderSelfTest extends GridCommonAbstractTest {
    /**
     * Class Name of task 1.
     */
    private static final String TEST_TASK1_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /**
     * Class Name of task 2.
     */
    private static final String TEST_TASK2_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath2";

    /**
     * URL of classes.
     */
    private static final URL[] URLS;

    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private IgniteDeploymentMode depMode;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Test.
     * @param isSameTask whether load same task or different task
     * @param expectEquals whether expected
     * @throws Exception if error occur
     */
    @SuppressWarnings({"ObjectEquality", "unchecked"})
    private void processTest(boolean isSameTask, boolean expectEquals) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            Class task1;
            Class task2;

            if (isSameTask) {
                ClassLoader ldr1 = new URLClassLoader(URLS, getClass().getClassLoader());
                ClassLoader ldr2 = new URLClassLoader(URLS, getClass().getClassLoader());

                task1 = ldr1.loadClass(TEST_TASK1_NAME);
                task2 = ldr2.loadClass(TEST_TASK1_NAME);
            }
            else {
                ClassLoader ldr1 = new GridTestExternalClassLoader(URLS, TEST_TASK2_NAME);
                ClassLoader ldr2 = new GridTestExternalClassLoader(URLS, TEST_TASK1_NAME);

                task1 = ldr1.loadClass(TEST_TASK1_NAME);
                task2 = ldr2.loadClass(TEST_TASK2_NAME);
            }

            // Execute task1 and task2 from node1 on node2 and make sure that they reuse same class loader on node2.
            int[] res1 = (int[]) ignite1.compute().execute(task1, ignite2.cluster().localNode().id());
            int[] res2 = (int[]) ignite1.compute().execute(task2, ignite2.cluster().localNode().id());

            if (expectEquals)
                assert Arrays.equals(res1, res2);
            else
                assert isNotSame(res1, res2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        depMode = IgniteDeploymentMode.PRIVATE;

        processTest(false, false);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        depMode = IgniteDeploymentMode.ISOLATED;

        processTest(false, false);
    }

    /**
     * Test {@link org.apache.ignite.configuration.IgniteDeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = IgniteDeploymentMode.CONTINUOUS;

        processTest(false, false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = IgniteDeploymentMode.SHARED;

        processTest(false, false);
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeployPrivateMode() throws Exception {
        depMode = IgniteDeploymentMode.PRIVATE;

        processTest(true, false);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeployIsolatedMode() throws Exception {
        depMode = IgniteDeploymentMode.ISOLATED;

        processTest(true, false);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeployContinuousMode() throws Exception {
        depMode = IgniteDeploymentMode.CONTINUOUS;

        processTest(true, false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRedeploySharedMode() throws Exception {
        depMode = IgniteDeploymentMode.SHARED;

        processTest(true, false);
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
