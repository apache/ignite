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
import org.apache.ignite.internal.processors.cache.distributed.CacheBaselineTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteBaselineAffinityTopologyActivationTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteStandByClusterTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinActiveNodeToActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinActiveNodeToInActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinInActiveNodeToActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinInActiveNodeToInActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.reconnect.IgniteStandByClientReconnectTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.reconnect.IgniteStandByClientReconnectToNewClusterTest;

/**
 *
 */
public class IgniteStandByClusterWithPersistenceSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Activate/DeActivate Cluster Test Suite");

        suite.addTestSuite(JoinActiveNodeToActiveClusterWithPersistence.class);
        suite.addTestSuite(JoinActiveNodeToInActiveClusterWithPersistence.class);
        suite.addTestSuite(JoinInActiveNodeToActiveClusterWithPersistence.class);
        suite.addTestSuite(JoinInActiveNodeToInActiveClusterWithPersistence.class);

        suite.addTestSuite(CacheBaselineTopologyTest.class);

        suite.addTestSuite(IgniteBaselineAffinityTopologyActivationTest.class);

        suite.addTestSuite(IgniteStandByClientReconnectTest.class);
        suite.addTestSuite(IgniteStandByClientReconnectToNewClusterTest.class);
        suite.addTestSuite(IgniteStandByClusterTest.class);

        return suite;
    }
}
