/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.newnodes;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.thread.*;
import org.gridgain.grid.loadtest.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Base class for single split on new nodes tests.
 */
@GridCommonTest(group = "Load Test")
public abstract class GridSingleSplitsNewNodesAbstractLoadTest extends GridCommonAbstractTest {
    /**
     * @param cfg Current configuration.
     * @return Configured discovery spi.
     */
    protected abstract DiscoverySpi getDiscoverySpi(IgniteConfiguration cfg);

    /**
     * @return Discovery spi heartbeat frequency.
     */
    protected abstract int getHeartbeatFrequency();

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi());

        cfg.setDiscoverySpi(getDiscoverySpi(cfg));

        cfg.setMetricsHistorySize(1000);

        // Set up new executor service because we have 1 per test and thus all
        // nodes have the same executor service. As soon as node get stopped
        // it stops executor service and may fail active nodes.
        cfg.setExecutorService(new IgniteThreadPoolExecutor());

        ((ThreadPoolExecutor)cfg.getExecutorService()).prestartAllCoreThreads();

        return cfg;
    }

    /** {@inheritDoc} */
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
        return Integer.valueOf(GridTestProperties.getProperty("load.test.threadnum"));
    }

    /**
     * @return Number of nodes to start/stop.
     */
    protected int getNodeCount() {
        return Integer.valueOf(GridTestProperties.getProperty("load.test.nodenum"));
    }

    /**
     * Load test grid.
     *
     * @throws Exception If task execution failed.
     */
    public void testLoad() throws Exception {
        final Ignite ignite = startGrid(getTestGridName());

        try {
            final long end = getTestDurationInMinutes() * 60 * 1000 + System.currentTimeMillis();

            // Warm up.
            ignite.compute().execute(GridSingleSplitNewNodesTestTask.class.getName(), 3);

            info("Load test will be executed for '" + getTestDurationInMinutes() + "' mins.");
            info("Thread count: " + getThreadCount());

            final GridLoadTestStatistics stats = new GridLoadTestStatistics();
            final AtomicInteger gridIdx = new AtomicInteger(0);

            for (int i = 0; i < getNodeCount(); i++) {
                new Thread(new Runnable() {
                    /** {@inheritDoc} */
                    @SuppressWarnings("BusyWait")
                    @Override public void run() {
                        try {
                            while (end - System.currentTimeMillis() > 0
                                && !Thread.currentThread().isInterrupted()) {
                                int idx = gridIdx.incrementAndGet();

                                startGrid(idx);

                                Thread.sleep(getHeartbeatFrequency() * 3);

                                stopGrid(idx);

                                Thread.sleep(getHeartbeatFrequency() * 3);
                            }
                        }
                        catch (Throwable e) {
                            error("Failed to start new node.", e);

                            fail();
                        }
                    }

                }, "grid-notaop-nodes-load-test").start();
            }

            GridTestUtils.runMultiThreaded(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    IgniteCompute comp = ignite.compute().enableAsync();

                    while (end - System.currentTimeMillis() > 0
                        && !Thread.currentThread().isInterrupted()) {
                        long start = System.currentTimeMillis();

                        try {
                            int levels = 3;

                            comp.execute(new GridSingleSplitNewNodesTestTask(), levels);

                            ComputeTaskFuture<Integer> fut = comp.future();

                            int res = fut.get();

                            if (res != levels)
                                fail("Received wrong result [expected=" + levels + ", actual=" + res + ']');

                            long taskCnt =
                                stats.onTaskCompleted(fut, levels, System.currentTimeMillis() - start);

                            if (taskCnt % 500 == 0)
                                info(stats.toString());
                        }
                        catch (Throwable e) {
                            error("Failed to execute grid task.", e);

                            fail();
                        }
                    }
                }
            }, getThreadCount(), "grid-notaop-load-test");
            info("Final test statistics: " + stats);
        }
        finally {
            G.stop(getTestGridName(), false);
        }
    }
}
