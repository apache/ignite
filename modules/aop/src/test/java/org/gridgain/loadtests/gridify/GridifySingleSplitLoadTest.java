/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.gridify;

import org.apache.log4j.*;
import org.gridgain.grid.*;
import org.gridgain.grid.loadtest.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.grid.spi.communication.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;
import java.util.concurrent.*;

/**
 * Gridify single split load test.
 */
@SuppressWarnings({"CatchGenericClass"})
@GridCommonTest(group = "Load Test")
public class GridifySingleSplitLoadTest extends GridCommonAbstractTest {
    /** */
    public GridifySingleSplitLoadTest() {
        super(true);
    }


    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public String getTestGridName() {
        // Gridify task has empty grid name by default so we need to change it
        // here.
        return null;
    }


    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration() throws Exception {
        GridConfiguration cfg = super.getConfiguration();

        /* Uncomment following code if you start it manually. */
            GridCommunicationSpi commSpi = new GridTcpCommunicationSpi();

            cfg.setCommunicationSpi(commSpi);

            GridDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

            cfg.setDiscoverySpi(discoSpi);
        /*
         */
        @SuppressWarnings("TypeMayBeWeakened")
        GridLog4jLogger log = (GridLog4jLogger)cfg.getGridLogger();

        log.getLogger("org.gridgain.grid").setLevel(Level.INFO);

        ((ThreadPoolExecutor)cfg.getExecutorService()).prestartAllCoreThreads();

        return cfg;
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
        return Integer.valueOf(GridTestProperties.getProperty("load.test.threadnum"));
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return (getTestDurationInMinutes() + 1) * 60 * 1000;
    }

    /**
     * Load test grid.
     *
     * @throws Exception If task execution failed.
     */
    @SuppressWarnings("unchecked")
    public void testGridifyLoad() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridifyLoadTestTask.class, GridifyLoadTestTask.class.getClassLoader());

        final long end = getTestDurationInMinutes() * 60 * 1000 + System.currentTimeMillis();

        // Warm up.
        new GridifyLoadTestJobTarget().executeLoadTestJob(3);

        info("Load test will be executed for '" + getTestDurationInMinutes() + "' mins.");
        info("Thread count: " + getThreadCount());

        final GridLoadTestStatistics stats = new GridLoadTestStatistics();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                while (end - System.currentTimeMillis() > 0) {
                    int levels = 3;

                    int exp = factorial(levels);

                    long start = System.currentTimeMillis();

                    int res = new GridifyLoadTestJobTarget().executeLoadTestJob(exp);

                    if (res != exp)
                        fail("Received wrong result [expected=" + exp + ", actual=" + res + ']');

                    long taskCnt = stats.onTaskCompleted(null, exp, System.currentTimeMillis() - start);

                    if (taskCnt % 500 == 0)
                        info(stats.toString());
                }
            }

        }, getThreadCount(), "grid-load-test-thread");

        info("Final test statistics: " + stats);
    }

    /**
     * Calculates factorial.
     *
     * @param num Factorial to calculate.
     * @return Factorial for the number passed in.
     */
    private int factorial(int num) {
        assert num > 0;

        return num == 1 ? 1 : num * factorial(num - 1);
    }
}
