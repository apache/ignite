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

package org.apache.ignite.p2p;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PDoubleDeploymentSelfTest extends GridCommonAbstractTest {
    /** Deployment mode. */
    private DeploymentMode depMode;

    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName());

        // Test requires SHARED mode to test local deployment priority over p2p.
        cfg.setDeploymentMode(depMode);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestBothNodesDeploy(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader ldr = new GridTestClassLoader(
                Collections.singletonMap("org/apache/ignite/p2p/p2p.properties", "resource=loaded"),
                GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName()
            );

            Class<? extends ComputeTask<?, ?>> taskCls =
                (Class<? extends ComputeTask<?, ?>>)ldr.loadClass(GridP2PTestTask.class.getName());

            ignite1.compute().localDeployTask(taskCls, ldr);

            Integer res1 = (Integer) ignite1.compute().execute(taskCls.getName(), 1);

            ignite1.compute().undeployTask(taskCls.getName());

            // Wait here 1 sec before the deployment as we have async undeploy.
            Thread.sleep(1000);

            ignite1.compute().localDeployTask(taskCls, ldr);
            ignite2.compute().localDeployTask(taskCls, ldr);

            Integer res2 = (Integer) ignite2.compute().execute(taskCls.getName(), 2);

            info("Checking results...");

            assert res1 == 10 : "Invalid res1 value: " + res1;
            assert res2 == 20 : "Invalid res1 value: " + res1;

            info("Tests passed.");
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.SHARED);
    }
}