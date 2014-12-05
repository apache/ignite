/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.redeploy;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.loadtest.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 * Single splits redeploy load test.
 */
@GridCommonTest(group = "Load Test")
public class GridSingleSplitsRedeployLoadTest extends GridCommonAbstractTest {
    /** Load test task type ID. */
    public static final String TASK_NAME = "org.gridgain.grid.tests.p2p.GridSingleSplitTestTask";

    /** */
    public GridSingleSplitsRedeployLoadTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        CommunicationSpi commSpi = new TcpCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        GridDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        cfg.setDiscoverySpi(discoSpi);

        ((ThreadPoolExecutor)cfg.getExecutorService()).prestartAllCoreThreads();

        cfg.setDeploymentMode(IgniteDeploymentMode.CONTINUOUS);

        return cfg;
    }

    /**
     * @return Test timeout.
     */
    @Override protected long getTestTimeout() {
        return (getTestDurationInMinutes() + 1) * 60 * 1000;
    }

    /**
     * @return Time for load test in minutes.
     */
    private int getTestDurationInMinutes() {
        return Integer.valueOf(GridTestProperties.getProperty("load.test.duration"));
    }

    /**
     * @return Number of threads for the test.
     */
    private int getThreadCount() {
        //return 1;
        return Integer.valueOf(GridTestProperties.getProperty("load.test.threadnum"));
    }

    /**
     * Load test grid.
     *
     * @throws Exception If task execution failed.
     */
    public void testLoad() throws Exception {
        final Ignite ignite = G.grid(getTestGridName());

        final long end = getTestDurationInMinutes() * 60 * 1000 + System.currentTimeMillis();

        ignite.compute().localDeployTask(loadTaskClass(), loadTaskClass().getClassLoader());

        info("Load test will be executed for '" + getTestDurationInMinutes() + "' mins.");
        info("Thread count: " + getThreadCount());

        final GridLoadTestStatistics stats = new GridLoadTestStatistics();

        new Thread(new Runnable() {
            /** {@inheritDoc} */
            @SuppressWarnings("BusyWait")
            @Override public void run() {
                try {
                    while (end - System.currentTimeMillis() > 0) {
                        Class<? extends ComputeTask<?, ?>> cls = loadTaskClass();

                        // info("Deploying class: " + cls);

                        ignite.compute().localDeployTask(cls, cls.getClassLoader());

                        Thread.sleep(1000);
                    }
                }
                catch (Exception e) {
                    error("Failed to deploy grid task.", e);

                    fail();
                }
            }

        },  "grid-notaop-deploy-load-test").start();


        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    int levels = 3;

                    while (end - System.currentTimeMillis() > 0) {
                        long start = System.currentTimeMillis();

                        // info("Executing task: " + TASK_NAME);

                        ComputeTaskFuture<Integer> fut = ignite.compute().execute(TASK_NAME, levels);

                        int res = fut.get();

                        if (res != levels)
                            fail("Received wrong result [expected=" + levels + ", actual=" + res + ']');

                        long taskCnt = stats.onTaskCompleted(fut, levels, System.currentTimeMillis() - start);

                        if (taskCnt % 500 == 0)
                            info(stats.toString());
                    }
                }
                catch (GridException e) {
                    error("Failed to execute grid task.", e);

                    fail();
                }
            }
        }, getThreadCount(), "grid-notaop-load-test");

        info("Final test statistics: " + stats);
    }

    /**
     * @return Loaded task class.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked"})
    private Class<? extends ComputeTask<?, ?>> loadTaskClass() throws Exception {
        return (Class<? extends ComputeTask<?, ?>>)getExternalClassLoader().loadClass(TASK_NAME);
    }
}
