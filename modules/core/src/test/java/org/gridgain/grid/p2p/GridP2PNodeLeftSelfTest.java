/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;
import java.net.*;
import java.util.*;

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
    private GridDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

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
            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);
            Grid grid3 = startGrid(3);

            Class task1 = urlClsLdr1.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            int[] res1 = (int[])grid1.compute().execute(task1, grid2.cluster().localNode().id());

            stopGrid(1);

            Thread.sleep(1000);

            // Task will be deployed after stop node1
            int[] res2 = (int[])grid3.compute().execute(task1, grid2.cluster().localNode().id());

            if (isExpectUndeploy)
                assert isNotSame(res1, res2);
            else
                assert Arrays.equals(res1, res2);
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
        depMode = GridDeploymentMode.CONTINUOUS;

        processTest(false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        processTest(true);
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
