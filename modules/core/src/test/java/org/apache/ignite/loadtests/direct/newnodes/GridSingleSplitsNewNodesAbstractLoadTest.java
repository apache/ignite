/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.loadtests.direct.newnodes;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.loadtests.GridLoadTestStatistics;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Base class for single split on new nodes tests.
 */
@GridCommonTest(group = "Load Test")
@RunWith(JUnit4.class)
public abstract class GridSingleSplitsNewNodesAbstractLoadTest extends GridCommonAbstractTest {
    /**
     * @param cfg Current configuration.
     * @return Configured discovery spi.
     */
    protected abstract DiscoverySpi getDiscoverySpi(IgniteConfiguration cfg);

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi());

        cfg.setDiscoverySpi(getDiscoverySpi(cfg));

        cfg.setMetricsHistorySize(1000);

        cfg.setPublicThreadPoolSize(100);

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
    @Test
    public void testLoad() throws Exception {
        final Ignite ignite = startGrid(getTestIgniteInstanceName());

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

                                Thread.sleep(grid(idx).configuration().getMetricsUpdateFrequency() * 3);

                                stopGrid(idx);

                                Thread.sleep(grid(idx).configuration().getMetricsUpdateFrequency() * 3);
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
                    while (end - System.currentTimeMillis() > 0
                        && !Thread.currentThread().isInterrupted()) {
                        long start = System.currentTimeMillis();

                        try {
                            int levels = 3;

                            ComputeTaskFuture<Integer> fut = ignite.compute().executeAsync(
                                new GridSingleSplitNewNodesTestTask(), levels);

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
            G.stop(getTestIgniteInstanceName(), false);
        }
    }
}
