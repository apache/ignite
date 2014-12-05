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
import org.gridgain.grid.*;
import org.gridgain.grid.spi.failover.never.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;

/**
 * Common test for deploy modes.
 */
abstract class GridAbstractMultinodeRedeployTest extends GridCommonAbstractTest {
    /** Number of iterations. */
    private static final int ITERATIONS = 1000;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private IgniteDeploymentMode depMode;

    /** */
    private static final String TASK_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        cfg.setFailoverSpi(new NeverFailoverSpi());

        cfg.setNetworkTimeout(10000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return getTestDurationInSeconds() * 2 * 1000;
    }

    /**
     * @return Time for load test in seconds.
     */
    private int getTestDurationInSeconds() {
        return 30;
    }

    /**
     * @return Number of threads for the test.
     */
    private int getThreadCount() {
        return 10;
    }

    /**
     * @param depMode deployment mode.
     * @throws Throwable If task execution failed.
     */
    protected void processTest(IgniteDeploymentMode depMode) throws Throwable {
        this.depMode = depMode;

        try {
            final Ignite ignite1 = startGrid(1);
            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            for (int i = 0; i < ITERATIONS; i++) {
                ignite1.compute().localDeployTask(loadTaskClass(), loadTaskClass().getClassLoader());
                ignite2.compute().localDeployTask(loadTaskClass(), loadTaskClass().getClassLoader());

                ComputeTaskFuture<int[]> fut1 = executeAsync(ignite1.compute(), TASK_NAME, Arrays.<UUID>asList(
                    ignite1.cluster().localNode().id(),
                    ignite2.cluster().localNode().id(),
                    ignite3.cluster().localNode().id()));

                ComputeTaskFuture<int[]> fut2 = executeAsync(ignite2.compute(), TASK_NAME, Arrays.<UUID>asList(
                    ignite1.cluster().localNode().id(),
                    ignite2.cluster().localNode().id(),
                    ignite3.cluster().localNode().id()));

                int[] res1 = fut1.get();
                int[] res2 = fut2.get();

                if (res1 == null || res2 == null || res1.length != 2 || res2.length != 2)
                    throw new GridException("Received wrong result.");
            }
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * @return Loaded class.
     * @throws Exception Thrown if any exception occurs.
     */
    @SuppressWarnings({"unchecked"})
    private Class<? extends ComputeTask<int[], ?>> loadTaskClass() throws Exception {
        return (Class<? extends ComputeTask<int[], ?>>)new GridTestExternalClassLoader(new URL[]{
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}).loadClass(TASK_NAME);
    }
}
