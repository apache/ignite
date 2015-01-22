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

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.apache.ignite.internal.managers.*;
import org.gridgain.testsuites.*;

/**
 * Grid SPI test suite.
 */
public class GridSpiTestSuite extends TestSuite {
    /**
     * @return All SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain SPIs Test Suite");

        // Failover.
        suite.addTest(GridSpiFailoverSelfTestSuite.suite());

        // Collision.
        suite.addTest(GridSpiCollisionSelfTestSuite.suite());

        // Event storage.
        suite.addTest(GridSpiEventStorageSelfTestSuite.suite());

        // Load Balancing.
        suite.addTest(GridSpiLoadBalancingSelfTestSuite.suite());

        // Swap space.
        suite.addTest(GridSpiSwapSpaceSelfTestSuite.suite());

        // Checkpoints.
        suite.addTest(GridSpiCheckpointSelfTestSuite.suite());

        // Deployment
        suite.addTest(GridSpiDeploymentSelfTestSuite.suite());

        // Discovery.
        suite.addTest(GridSpiDiscoverySelfTestSuite.suite());

        // Communication.
        suite.addTest(GridSpiCommunicationSelfTestSuite.suite());

        // Indexing.
        suite.addTest(GridSpiIndexingSelfTestSuite.suite());

        // All other tests.
        suite.addTestSuite(GridNoopManagerSelfTest.class);

        return suite;
    }
}
