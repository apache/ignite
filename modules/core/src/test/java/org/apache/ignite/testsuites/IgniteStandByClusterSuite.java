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
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateDataStreamerTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateDataStructureTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateFailOverTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateServiceTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteStandByClusterTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.JoinActiveNodeToActiveCluster;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.JoinActiveNodeToInActiveCluster;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.JoinInActiveNodeToActiveCluster;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.JoinInActiveNodeToInActiveCluster;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinActiveNodeToActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinActiveNodeToInActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinInActiveNodeToActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence.JoinInActiveNodeToInActiveClusterWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.reconnect.IgniteStandByClientReconnectTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.reconnect.IgniteStandByClientReconnectToNewClusterTest;

/**
 *
 */
public class IgniteStandByClusterSuite extends TestSuite {
    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Activate/DeActivate Cluster Test Suit");

        suite.addTestSuite(IgniteStandByClusterTest.class);
        suite.addTestSuite(IgniteStandByClientReconnectTest.class);
        suite.addTestSuite(IgniteStandByClientReconnectToNewClusterTest.class);

        suite.addTestSuite(JoinActiveNodeToActiveCluster.class);
        suite.addTestSuite(JoinActiveNodeToInActiveCluster.class);
        suite.addTestSuite(JoinInActiveNodeToActiveCluster.class);
        suite.addTestSuite(JoinInActiveNodeToInActiveCluster.class);

        suite.addTestSuite(JoinActiveNodeToActiveClusterWithPersistence.class);
        suite.addTestSuite(JoinActiveNodeToInActiveClusterWithPersistence.class);
        suite.addTestSuite(JoinInActiveNodeToActiveClusterWithPersistence.class);
        suite.addTestSuite(JoinInActiveNodeToInActiveClusterWithPersistence.class);

        suite.addTestSuite(IgniteChangeGlobalStateTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateCacheTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateDataStructureTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateDataStreamerTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateFailOverTest.class);
        suite.addTestSuite(IgniteChangeGlobalStateServiceTest.class);

        return suite;
    }
}
