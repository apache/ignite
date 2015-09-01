/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.loadtests.direct.singlesplit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.loadtest.GridLoadTestStatistics;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.log4j.Level;

/**
 * Single split load test.
 */
@GridCommonTest(group = "Load Test")
public class SingleSplitsLoadTest extends GridCommonAbstractTest {
    /** */
    public SingleSplitsLoadTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setCommunicationSpi(new TcpCommunicationSpi());
        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        Log4JLogger log = (Log4JLogger)cfg.getGridLogger().getLogger(null);

        log.setLevel(Level.INFO);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return (getTestDurationInMinutes() + 5) * 60 * 1000;
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
     * Load test grid.
     *
     * @throws Exception If task execution failed.
     */
    public void testLoad() throws Exception {
        final Ignite ignite = G.ignite(getTestGridName());

        final long end = getTestDurationInMinutes() * 60 * 1000 + System.currentTimeMillis();

        // Warm up.
        ignite.compute().withTimeout(5000).execute(SingleSplitTestTask.class.getName(), 3);

        info("Load test will be executed for '" + getTestDurationInMinutes() + "' mins.");
        info("Thread count: " + getThreadCount());

        final GridLoadTestStatistics stats = new GridLoadTestStatistics();

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                while (end - System.currentTimeMillis() > 0) {
                    long start = System.currentTimeMillis();

                    try {
                        int levels = 20;

                        IgniteCompute comp = ignite.compute().withAsync();

                        comp.execute(new SingleSplitTestTask(), levels);

                        ComputeTaskFuture<Integer> fut = comp.future();

                        int res = fut.get();

                        if (res != levels)
                            fail("Received wrong result [expected=" + levels + ", actual=" + res + ']');

                        long taskCnt = stats.onTaskCompleted(fut, levels, System.currentTimeMillis() - start);

                        if (taskCnt % 500 == 0)
                            info(stats.toString());
                    }
                    catch (IgniteException e) {
                        error("Failed to execute grid task.", e);

                        fail();
                    }
                }
            }
        }, getThreadCount(), "grid-notaop-load-test");

        info("Final test statistics: " + stats);
    }
}