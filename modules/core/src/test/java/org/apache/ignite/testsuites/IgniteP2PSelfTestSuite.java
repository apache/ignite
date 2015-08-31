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
import org.apache.ignite.internal.managers.deployment.GridDeploymentMessageCountSelfTest;
import org.apache.ignite.p2p.GridP2PClassLoadingSelfTest;
import org.apache.ignite.p2p.GridP2PContinuousDeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PDifferentClassLoaderSelfTest;
import org.apache.ignite.p2p.GridP2PDoubleDeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PHotRedeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PJobClassLoaderSelfTest;
import org.apache.ignite.p2p.GridP2PLocalDeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PMissedResourceCacheSizeSelfTest;
import org.apache.ignite.p2p.GridP2PNodeLeftSelfTest;
import org.apache.ignite.p2p.GridP2PRecursionTaskSelfTest;
import org.apache.ignite.p2p.GridP2PRemoteClassLoadersSelfTest;
import org.apache.ignite.p2p.GridP2PSameClassLoaderSelfTest;
import org.apache.ignite.p2p.GridP2PTimeoutSelfTest;
import org.apache.ignite.p2p.GridP2PUndeploySelfTest;

/**
 * P2P test suite.
 */
public class IgniteP2PSelfTestSuite extends TestSuite {
    /**
     * @return P2P tests suite.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ProhibitedExceptionDeclared"})
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite P2P Test Suite");

        suite.addTest(new TestSuite(GridP2PDoubleDeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridP2PHotRedeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridP2PClassLoadingSelfTest.class));
        suite.addTest(new TestSuite(GridP2PUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridP2PRemoteClassLoadersSelfTest.class));
        suite.addTest(new TestSuite(GridP2PNodeLeftSelfTest.class));
        suite.addTest(new TestSuite(GridP2PDifferentClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridP2PSameClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridP2PJobClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridP2PRecursionTaskSelfTest.class));
        suite.addTest(new TestSuite(GridP2PLocalDeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridP2PTimeoutSelfTest.class));
        suite.addTest(new TestSuite(GridP2PMissedResourceCacheSizeSelfTest.class));
        suite.addTest(new TestSuite(GridP2PContinuousDeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridDeploymentMessageCountSelfTest.class));

        return suite;
    }
}