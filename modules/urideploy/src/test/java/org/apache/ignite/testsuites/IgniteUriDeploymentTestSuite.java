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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.GridTaskUriDeploymentDeadlockSelfTest;
import org.apache.ignite.p2p.GridP2PDisabledSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentClassLoaderMultiThreadedSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentClassLoaderSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentClassloaderRegisterSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentConfigSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentFileProcessorSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentMultiScannersSelfTest;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentSimpleSelfTest;
import org.apache.ignite.spi.deployment.uri.scanners.file.GridFileDeploymentUndeploySelfTest;
import org.apache.ignite.spi.deployment.uri.scanners.http.GridHttpDeploymentSelfTest;

/**
 * Tests against {@link org.apache.ignite.spi.deployment.uri.UriDeploymentSpi}.
 */
public class IgniteUriDeploymentTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("URI Deployment Spi Test Suite");

        suite.addTest(new TestSuite(GridUriDeploymentConfigSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentSimpleSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassloaderRegisterSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentFileProcessorSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassLoaderMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentMultiScannersSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentConfigSelfTest.class));

        suite.addTest(new TestSuite(GridFileDeploymentUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridHttpDeploymentSelfTest.class));

        // GAR Ant task tests.
        suite.addTest(IgniteToolsSelfTestSuite.suite());

        suite.addTestSuite(GridTaskUriDeploymentDeadlockSelfTest.class);
        suite.addTest(new TestSuite(GridP2PDisabledSelfTest.class));

        return suite;
    }
}