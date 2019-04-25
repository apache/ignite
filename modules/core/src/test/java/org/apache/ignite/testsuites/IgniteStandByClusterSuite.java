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
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteClusterActivateDeactivateTest.class,

    IgniteStandByClusterTest.class,
    IgniteStandByClientReconnectTest.class,
    IgniteStandByClientReconnectToNewClusterTest.class,

    JoinActiveNodeToActiveCluster.class,
    JoinActiveNodeToInActiveCluster.class,
    JoinInActiveNodeToActiveCluster.class,
    JoinInActiveNodeToInActiveCluster.class,

    JoinActiveNodeToActiveClusterWithPersistence.class,
    JoinActiveNodeToInActiveClusterWithPersistence.class,
    JoinInActiveNodeToActiveClusterWithPersistence.class,
    JoinInActiveNodeToInActiveClusterWithPersistence.class,

//TODO https://issues.apache.org/jira/browse/IGNITE-9081 IgniteChangeGlobalStateTest.class,
//TODO https://issues.apache.org/jira/browse/IGNITE-9081 IgniteChangeGlobalStateCacheTest.class,
//TODO https://issues.apache.org/jira/browse/IGNITE-9081 IgniteChangeGlobalStateDataStructureTest.class,
//TODO https://issues.apache.org/jira/browse/IGNITE-9081 IgniteChangeGlobalStateServiceTest.class,

    IgniteChangeGlobalStateDataStreamerTest.class,
    IgniteChangeGlobalStateFailOverTest.class,

    IgniteNoParrallelClusterIsAllowedTest.class,

    CacheBaselineTopologyTest.class,
    IgniteBaselineAffinityTopologyActivationTest.class
})
public class IgniteStandByClusterSuite {
}
