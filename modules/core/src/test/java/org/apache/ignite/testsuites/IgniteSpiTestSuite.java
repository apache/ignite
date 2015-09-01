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
import org.apache.ignite.internal.managers.GridNoopManagerSelfTest;

/**
 * Grid SPI test suite.
 */
public class IgniteSpiTestSuite extends TestSuite {
    /**
     * @return All SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite SPIs Test Suite");

        // Failover.
        suite.addTest(IgniteSpiFailoverSelfTestSuite.suite());

        // Collision.
        suite.addTest(IgniteSpiCollisionSelfTestSuite.suite());

        // Event storage.
        suite.addTest(IgniteSpiEventStorageSelfTestSuite.suite());

        // Load Balancing.
        suite.addTest(IgniteSpiLoadBalancingSelfTestSuite.suite());

        // Swap space.
        suite.addTest(IgniteSpiSwapSpaceSelfTestSuite.suite());

        // Checkpoints.
        suite.addTest(IgniteSpiCheckpointSelfTestSuite.suite());

        // Deployment
        suite.addTest(IgniteSpiDeploymentSelfTestSuite.suite());

        // Discovery.
        suite.addTest(IgniteSpiDiscoverySelfTestSuite.suite());

        // Communication.
        suite.addTest(IgniteSpiCommunicationSelfTestSuite.suite());

        // Indexing.
        suite.addTest(IgniteSpiIndexingSelfTestSuite.suite());

        // All other tests.
        suite.addTestSuite(GridNoopManagerSelfTest.class);

        return suite;
    }
}