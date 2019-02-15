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

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
@RunWith(JUnit4.class)
public class GridP2PDoubleDeploymentSelfTest extends GridCommonAbstractTest {
    /** Deployment mode. */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName());

        // Test requires SHARED mode to test local deployment priority over p2p.
        cfg.setDeploymentMode(depMode);

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
    @Test
    public void testPrivateMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testIsolatedMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testContinuousMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testSharedMode() throws Exception {
        processTestBothNodesDeploy(DeploymentMode.SHARED);
    }
}
