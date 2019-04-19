/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.managers.GridManagerLocalMessageListenerSelfTest;
import org.apache.ignite.internal.managers.GridNoopManagerSelfTest;
import org.apache.ignite.spi.encryption.KeystoreEncryptionSpiSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Grid SPI test suite.
 */
@RunWith(AllTests.class)
public class IgniteSpiTestSuite {
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

        // Checkpoints.
        suite.addTest(IgniteSpiCheckpointSelfTestSuite.suite());

        // Deployment
        suite.addTest(IgniteSpiDeploymentSelfTestSuite.suite());

        // Discovery.
        suite.addTest(IgniteSpiDiscoverySelfTestSuite.suite());

        // Communication.
        suite.addTest(IgniteSpiCommunicationSelfTestSuite.suite());

        // All other tests.
        suite.addTest(new JUnit4TestAdapter(GridNoopManagerSelfTest.class));

        // Local Message Listener tests.
        suite.addTest(new JUnit4TestAdapter(GridManagerLocalMessageListenerSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(KeystoreEncryptionSpiSelfTest.class));

        return suite;
    }
}
