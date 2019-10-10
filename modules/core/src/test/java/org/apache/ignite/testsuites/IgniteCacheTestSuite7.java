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

import java.util.Set;
import junit.framework.TestSuite;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite.
 */
@RunWith(AllTests.class)
public class IgniteCacheTestSuite7 {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static TestSuite suite(Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("IgniteCache With Persistence Test Suite");

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
