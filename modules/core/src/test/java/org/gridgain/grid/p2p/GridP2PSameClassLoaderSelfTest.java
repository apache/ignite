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
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;

/**
 * Test P2P deployment tasks which loaded from different class loaders.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown"})
@GridCommonTest(group = "P2P")
public class GridP2PSameClassLoaderSelfTest extends GridCommonAbstractTest {
    /** Class Name of task 1. */
    private static final String TEST_TASK1_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** Class Name of task 2. */
    private static final String TEST_TASK2_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath2";

    /** */
    private static final ClassLoader CLASS_LOADER;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private IgniteDeploymentMode depMode;

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

        ((GridTcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * Test.
     * @param isIsolatedDifferentTask Isolated different task flag.
     * @param isIsolatedDifferentNode Isolated different mode flag.
     * @throws Exception if error occur
     */
    @SuppressWarnings({"unchecked"})
    private void processTest(boolean isIsolatedDifferentTask, boolean isIsolatedDifferentNode) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Class task1 = CLASS_LOADER.loadClass(TEST_TASK1_NAME);
            Class task2 = CLASS_LOADER.loadClass(TEST_TASK2_NAME);

            // Execute task1 and task2 from node1 on node2 and make sure that they reuse same class loader on node2.
            int[] res1 = (int[]) ignite1.compute().execute(task1, ignite2.cluster().localNode().id());
            int[] res2 = (int[]) ignite1.compute().execute(task2, ignite2.cluster().localNode().id());

            if (isIsolatedDifferentTask) {
                assert res1[0] != res2[0]; // Resources are not same
                assert res1[1] == res2[1]; // Class loaders are same
            }
            else
                assert Arrays.equals(res1, res2);

            int[] res3 = (int[]) ignite3.compute().execute(task1, ignite2.cluster().localNode().id());
            int[] res4 = (int[]) ignite3.compute().execute(task2, ignite2.cluster().localNode().id());

            if (isIsolatedDifferentTask) {
                assert res3[0] != res4[0]; // Resources are not same
                assert res3[1] == res4[1]; // Class loaders are same
            }
            else
                assert Arrays.equals(res3, res4);

            if (isIsolatedDifferentNode)
                assert isNotSame(res1, res4);
            else
                assert Arrays.equals(res1, res4);
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
        depMode = IgniteDeploymentMode.PRIVATE;

        processTest(true, true);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        depMode = IgniteDeploymentMode.ISOLATED;

        processTest(false, true);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
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
