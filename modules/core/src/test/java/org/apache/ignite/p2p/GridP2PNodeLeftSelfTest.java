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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Test P2P class loading in SHARED_CLASSLOADER_UNDEPLOY mode.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PNodeLeftSelfTest extends GridCommonAbstractTest {
    /** */
    private static final ClassLoader urlClsLdr1;

    /** */
    static {
        String path = GridTestProperties.getProperty("p2p.uri.cls");

        try {
            urlClsLdr1 = new URLClassLoader(
                new URL[] { new URL(path) },
                GridP2PNodeLeftSelfTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create URL: " + path, e);
        }
    }

    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Test undeploy task.
     * @param isExpectUndeploy Whether undeploy is expected.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void processTest(boolean isExpectUndeploy) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Class task1 = urlClsLdr1.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            Thread.sleep(1000);

            // Task will be deployed after stop node1
            Integer res2 = (Integer)ignite3.compute().execute(task1, ignite2.cluster().localNode().id());

            if (isExpectUndeploy)
                assert !res1.equals(res2);
            else
                assert res1.equals(res2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest(false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest(true);
    }
}
