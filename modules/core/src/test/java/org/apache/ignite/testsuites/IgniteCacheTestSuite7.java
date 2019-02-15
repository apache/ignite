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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.authentication.Authentication1kUsersNodeRestartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationConfigurationClusterTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationOnNotActiveClusterTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNPEOnStartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNodeRestartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDataRegionConfigurationTest;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsMBeanTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsManageTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartFailWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.WalModeChangeAdvancedSelfTest;
import org.apache.ignite.internal.processors.cache.WalModeChangeCoordinatorNotAffinityNodeSelfTest;
import org.apache.ignite.internal.processors.cache.WalModeChangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.Cache64kPartitionsTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDataLossOnPartitionMoveTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRentingStateRepairTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheStartWithLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionCountersTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingWithAsyncClearingTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionMultinodeMixedRegionsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheAssignmentNodeRestartsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.CheckpointBufferDeadlockTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithPrimaryIndexCorruptionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackAsyncWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxWithSmallTimeoutAndContentionOneKeyTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite7 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        GridTestUtils.addTestIfNeeded(suite, CheckpointBufferDeadlockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheStartWithLoadTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationConfigurationClusterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationOnNotActiveClusterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNPEOnStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, Authentication1kUsersNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheDataRegionConfigurationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, WalModeChangeAdvancedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, WalModeChangeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, WalModeChangeCoordinatorNotAffinityNodeSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, Cache64kPartitionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingPartitionCountersTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingWithAsyncClearingTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheAssignmentNodeRestartsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackAsyncWithPersistenceTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheGroupMetricsMBeanTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheMetricsManageTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PageEvictionMultinodeMixedRegionsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartFailWithPersistenceTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxWithSmallTimeoutAndContentionOneKeyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheRentingStateRepairTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TransactionIntegrityWithPrimaryIndexCorruptionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDataLossOnPartitionMoveTest.class, ignoredTests);

        return suite;
    }
}
