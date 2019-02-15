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

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.GridTaskUriDeploymentDeadlockSelfTest;
import org.apache.ignite.p2p.GridP2PDisabledSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentClassLoaderMultiThreadedSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentClassLoaderSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentClassloaderRegisterSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentConfigSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentFileProcessorSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentMd5CheckSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentMultiScannersErrorThrottlingTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentMultiScannersSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentSimpleSelfTest;
import org.apache.ignite.spi.deployment.uri.scanners.file.GridFileDeploymentSelfTest;
import org.apache.ignite.spi.deployment.uri.scanners.file.GridFileDeploymentUndeploySelfTest;
import org.apache.ignite.spi.deployment.uri.scanners.http.GridHttpDeploymentSelfTest;

/**
 * Tests against {@link org.apache.ignite.spi.deployment.uri.UriDeploymentSpi}.
 */
public class IgniteUriDeploymentTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("URI Deployment Spi Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentSimpleSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentClassloaderRegisterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentFileProcessorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentClassLoaderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentClassLoaderMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentMultiScannersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentConfigSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridFileDeploymentUndeploySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridHttpDeploymentSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridFileDeploymentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentMultiScannersErrorThrottlingTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUriDeploymentMd5CheckSelfTest.class));

        // GAR Ant task tests.
        suite.addTest(IgniteToolsSelfTestSuite.suite());

        suite.addTest(new JUnit4TestAdapter(GridTaskUriDeploymentDeadlockSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridP2PDisabledSelfTest.class));

        return suite;
    }
}
