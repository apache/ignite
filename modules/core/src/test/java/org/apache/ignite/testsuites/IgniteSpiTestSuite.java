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

import org.apache.ignite.internal.managers.GridManagerLocalMessageListenerSelfTest;
import org.apache.ignite.internal.managers.GridNoopManagerSelfTest;
import org.apache.ignite.spi.encryption.KeystoreEncryptionSpiSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Grid SPI test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Failover.
    IgniteSpiFailoverSelfTestSuite.class,

    // Collision.
    IgniteSpiCollisionSelfTestSuite.class,

    // Event storage.
    IgniteSpiEventStorageSelfTestSuite.class,

    // Load Balancing.
    IgniteSpiLoadBalancingSelfTestSuite.class,

    // Checkpoints.
    IgniteSpiCheckpointSelfTestSuite.class,

    // Deployment
    IgniteSpiDeploymentSelfTestSuite.class,

    // Discovery.
    IgniteSpiDiscoverySelfTestSuite.class,

    // Communication.
    IgniteSpiCommunicationSelfTestSuite.class,

    // All other tests.
    GridNoopManagerSelfTest.class,

    // Local Message Listener tests.
    GridManagerLocalMessageListenerSelfTest.class,

    KeystoreEncryptionSpiSelfTest.class
})
public class IgniteSpiTestSuite {
}
