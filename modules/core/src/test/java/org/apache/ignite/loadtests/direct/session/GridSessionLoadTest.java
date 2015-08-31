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

package org.apache.ignite.loadtests.direct.session;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.loadtest.GridLoadTestStatistics;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Session load test.
 */
@GridCommonTest(group = "Load Test")
public class GridSessionLoadTest extends GridCommonAbstractTest {
    /** */
    public GridSessionLoadTest() {
        super(/*start Grid*/true);
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
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testSessionLoad() throws Exception {
        final Ignite ignite = G.ignite(getTestGridName());

        assert ignite != null;

        ignite.compute().localDeployTask(GridSessionLoadTestTask.class, GridSessionLoadTestTask.class.getClassLoader());

        final long end = getTestDurationInMinutes() * 60 * 1000 + System.currentTimeMillis();

        info("Load test will be executed for '" + getTestDurationInMinutes() + "' mins.");
        info("Thread count: " + getThreadCount());

        final GridLoadTestStatistics stats = new GridLoadTestStatistics();

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    while (end - System.currentTimeMillis() > 0) {
                        long start = System.currentTimeMillis();

                        ComputeTaskFuture<?> fut = ignite.compute().withTimeout(10000).
                            execute(GridSessionLoadTestTask.class.getName(), ignite.cluster().nodes().size());

                        Object res = fut.get();

                        assert (Boolean)res;

                        long taskCnt = stats.onTaskCompleted(fut, 1, System.currentTimeMillis() - start);

                        if (taskCnt % 500 == 0)
                            info(stats.toString());
                    }
                }
                catch (Throwable e) {
                    error("Load test failed.", e);

                    failed.set(true);
                }
            }
        }, getThreadCount(), "grid-load-test-thread");

        info("Final test statistics: " + stats);

        if (failed.get())
            fail();
    }
}