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

package org.apache.loadtests.gridify;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.loadtests.GridLoadTestStatistics;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * Gridify single split load test.
 */
@GridCommonTest(group = "Load Test")
public class GridifySingleSplitLoadTest extends GridCommonAbstractTest {
    /** */
    public GridifySingleSplitLoadTest() {
        super(true);
    }


    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        // Gridify task has empty Ignite instance name by default so we need to change it
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
        Log4JLogger log = (Log4JLogger)cfg.getGridLogger();

        log.getLogger("org.apache.ignite").setLevel(Level.INFO);

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
    @Test
    public void testGridifyLoad() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

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
