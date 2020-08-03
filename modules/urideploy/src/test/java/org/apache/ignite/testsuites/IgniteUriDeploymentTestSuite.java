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

import org.apache.ignite.internal.GridTaskUriDeploymentDeadlockSelfTest;
import org.apache.ignite.p2p.ClassLoadingProblemExceptionTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests against {@link org.apache.ignite.spi.deployment.uri.UriDeploymentSpi}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridUriDeploymentConfigSelfTest.class,
    GridUriDeploymentSimpleSelfTest.class,
    GridUriDeploymentClassloaderRegisterSelfTest.class,
    GridUriDeploymentFileProcessorSelfTest.class,
    GridUriDeploymentClassLoaderSelfTest.class,
    GridUriDeploymentClassLoaderMultiThreadedSelfTest.class,
    GridUriDeploymentMultiScannersSelfTest.class,
    GridUriDeploymentConfigSelfTest.class,

    GridFileDeploymentUndeploySelfTest.class,
    GridHttpDeploymentSelfTest.class,

    GridFileDeploymentSelfTest.class,
    GridUriDeploymentMultiScannersErrorThrottlingTest.class,
    GridUriDeploymentMd5CheckSelfTest.class,

    // GAR Ant task tests.
    IgniteToolsSelfTestSuite.class,

    GridTaskUriDeploymentDeadlockSelfTest.class,
    GridP2PDisabledSelfTest.class,
    ClassLoadingProblemExceptionTest.class
})
public class IgniteUriDeploymentTestSuite {
}
