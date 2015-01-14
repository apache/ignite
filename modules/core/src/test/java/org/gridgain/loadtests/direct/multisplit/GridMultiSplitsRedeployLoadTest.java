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

package org.gridgain.loadtests.direct.multisplit;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.loadtest.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 * Multi splits redeploy load test.
 */
@GridCommonTest(group = "Load Test")
public class GridMultiSplitsRedeployLoadTest extends GridCommonAbstractTest {
    /** Load test task type ID. */
    public static final String TASK_TYPE_ID = GridLoadTestTask.class.getName();

    /** */
    public GridMultiSplitsRedeployLoadTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        ((ThreadPoolExecutor)cfg.getExecutorService()).prestartAllCoreThreads();

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
        return Integer.valueOf(GridTestProperties.getProperty("load.test.threadnum"));
    }

    /**
     * Load test grid.
     *
     * @throws Exception If task execution failed.
     */
    public void testLoad() throws Exception {
        final Ignite ignite = G.ignite(getTestGridName());

        deployTask(ignite);

        final long end = getTestDurationInMinutes() * 60 * 1000 + System.currentTimeMillis();

        // Warm up.
        ignite.compute().withTimeout(10000).execute(TASK_TYPE_ID, 3);

        info("Load test will be executed for '" + getTestDurationInMinutes() + "' mins.");
        info("Thread count: " + getThreadCount());

        final GridLoadTestStatistics stats = new GridLoadTestStatistics();

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                while (end - System.currentTimeMillis() > 0) {
                    int levels = 3;

                    int exp = factorial(levels);

                    long start = System.currentTimeMillis();

                    try {
                        ComputeTaskFuture<Integer> fut = ignite.compute().withTimeout(10000).
                            execute(TASK_TYPE_ID, levels);

                        int res = fut.get();

                        if (res != exp)
                            fail("Received wrong result [expected=" + exp + ", actual=" + res + ']');

                        long taskCnt = stats.onTaskCompleted(fut, exp, System.currentTimeMillis() - start);

                        if (taskCnt % 100 == 0) {
                            try {
                                deployTask(ignite);
                            }
                            catch (IgniteCheckedException e) {
                                error("Failed to deploy grid task.", e);

                                fail();
                            }
                        }

                        if (taskCnt % 500 == 0)
                            info(stats.toString());
                    }
                    catch (IgniteCheckedException e) {
                        error("Failed to execute grid task.", e);

                        fail();
                    }
                }
            }
        }, getThreadCount(), "grid-notaop-load-test");

        info("Final test statistics: " + stats);
    }

    /**
     * @param ignite Grid.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void deployTask(Ignite ignite) throws IgniteCheckedException {
        ignite.compute().localDeployTask(GridLoadTestTask.class, GridLoadTestTask.class.getClassLoader());
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
