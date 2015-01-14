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

package org.gridgain.loadtests.gridify;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.logger.log4j.*;
import org.apache.log4j.*;
import org.gridgain.grid.loadtest.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
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
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        /* Uncomment following code if you start it manually. */
            CommunicationSpi commSpi = new TcpCommunicationSpi();

            cfg.setCommunicationSpi(commSpi);

            DiscoverySpi discoSpi = new TcpDiscoverySpi();

            cfg.setDiscoverySpi(discoSpi);
        /*
         */
        @SuppressWarnings("TypeMayBeWeakened")
        IgniteLog4jLogger log = (IgniteLog4jLogger)cfg.getGridLogger();

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
        Ignite ignite = G.ignite(getTestGridName());

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
