/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 *
 */
@RunWith(AllTests.class)
public class IgniteStandByClusterSuite {
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
