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
    private IgniteDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Class task1 = urlClsLdr1.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            Integer res1 = ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            Thread.sleep(1000);

            // Task will be deployed after stop node1
            Integer res2 = ignite3.compute().execute(task1, ignite2.cluster().localNode().id());

            if (isExpectUndeploy)
                assert !res1.equals(res2);
            else
                assert res1.equals(res2);
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
        depMode = IgniteDeploymentMode.CONTINUOUS;

        processTest(false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = IgniteDeploymentMode.SHARED;

        processTest(true);
    }
}
