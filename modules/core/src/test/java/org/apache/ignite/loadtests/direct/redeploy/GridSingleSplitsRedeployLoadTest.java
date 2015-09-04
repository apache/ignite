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

package org.apache.ignite.loadtests.direct.redeploy;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.loadtest.GridLoadTestStatistics;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Single splits redeploy load test.
 */
@GridCommonTest(group = "Load Test")
public class GridSingleSplitsRedeployLoadTest extends GridCommonAbstractTest {
    /** Load test task type ID. */
    public static final String TASK_NAME = "org.apache.ignite.tests.p2p.SingleSplitTestTask";

    /** */
    public GridSingleSplitsRedeployLoadTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        CommunicationSpi commSpi = new TcpCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        DiscoverySpi discoSpi = new TcpDiscoverySpi();

        cfg.setDiscoverySpi(discoSpi);

        cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);

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
        final Ignite ignite = G.ignite(getTestGridName());

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
                catch (IgniteException e) {
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