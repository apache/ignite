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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheBaselineTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteBaselineAffinityTopologyActivationTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateDataStreamerTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteChangeGlobalStateFailOverTest;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.IgniteNoParrallelClusterIsAllowedTest;
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
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Activate/DeActivate Cluster Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteClusterActivateDeactivateTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteStandByClusterTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteStandByClientReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteStandByClientReconnectToNewClusterTest.class));

        suite.addTest(new JUnit4TestAdapter(JoinActiveNodeToActiveCluster.class));
        suite.addTest(new JUnit4TestAdapter(JoinActiveNodeToInActiveCluster.class));
        suite.addTest(new JUnit4TestAdapter(JoinInActiveNodeToActiveCluster.class));
        suite.addTest(new JUnit4TestAdapter(JoinInActiveNodeToInActiveCluster.class));

        suite.addTest(new JUnit4TestAdapter(JoinActiveNodeToActiveClusterWithPersistence.class));
        suite.addTest(new JUnit4TestAdapter(JoinActiveNodeToInActiveClusterWithPersistence.class));
        suite.addTest(new JUnit4TestAdapter(JoinInActiveNodeToActiveClusterWithPersistence.class));
        suite.addTest(new JUnit4TestAdapter(JoinInActiveNodeToInActiveClusterWithPersistence.class));

//TODO https://issues.apache.org/jira/browse/IGNITE-9081 suite.addTest(new JUnit4TestAdapter(IgniteChangeGlobalStateTest.class));
//TODO https://issues.apache.org/jira/browse/IGNITE-9081 suite.addTest(new JUnit4TestAdapter(IgniteChangeGlobalStateCacheTest.class));
//TODO https://issues.apache.org/jira/browse/IGNITE-9081 suite.addTest(new JUnit4TestAdapter(IgniteChangeGlobalStateDataStructureTest.class));
//TODO https://issues.apache.org/jira/browse/IGNITE-9081 suite.addTest(new JUnit4TestAdapter(IgniteChangeGlobalStateServiceTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteChangeGlobalStateDataStreamerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteChangeGlobalStateFailOverTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteNoParrallelClusterIsAllowedTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheBaselineTopologyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBaselineAffinityTopologyActivationTest.class));

        return suite;
    }
}
