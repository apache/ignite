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

package org.apache.ignite.p2p;

import java.net.URL;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.failover.never.NeverFailoverSpi;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Common test for deploy modes.
 */
abstract class GridAbstractMultinodeRedeployTest extends GridCommonAbstractTest {
    /** Number of iterations. */
    private static final int ITERATIONS = 1000;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** */
    private static final String TASK_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentMode(depMode);

        cfg.setFailoverSpi(new NeverFailoverSpi());

        cfg.setNetworkTimeout(10000);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Throwable If task execution failed.
     */
    protected void processTest(DeploymentMode depMode) throws Throwable {
        this.depMode = depMode;

        try {
            final Ignite ignite1 = startGrid(1);
            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            for (int i = 0; i < ITERATIONS; i++) {
                ignite1.compute().localDeployTask(loadTaskClass(), loadTaskClass().getClassLoader());
                ignite2.compute().localDeployTask(loadTaskClass(), loadTaskClass().getClassLoader());

                ComputeTaskFuture<Integer> fut1 = executeAsync(ignite1.compute(), TASK_NAME, Arrays.asList(
                    ignite1.cluster().localNode().id(),
                    ignite2.cluster().localNode().id(),
                    ignite3.cluster().localNode().id()));

                ComputeTaskFuture<Integer> fut2 = executeAsync(ignite2.compute(), TASK_NAME, Arrays.asList(
                    ignite1.cluster().localNode().id(),
                    ignite2.cluster().localNode().id(),
                    ignite3.cluster().localNode().id()));

                Integer res1 = fut1.get(5000);
                Integer res2 = fut2.get(5000);

                if (res1 == null || res2 == null)
                    throw new IgniteCheckedException("Received wrong result.");
            }
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * @return Loaded class.
     * @throws Exception Thrown if any exception occurs.
     */
    private Class<? extends ComputeTask<int[], ?>> loadTaskClass() throws Exception {
        return (Class<? extends ComputeTask<int[], ?>>)new GridTestExternalClassLoader(new URL[]{
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}).loadClass(TASK_NAME);
    }
}