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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNodeRestartTest;
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

//        GridTestUtils.addTestIfNeeded(suite, CheckpointBufferDeadlockTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteCacheStartWithLoadTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, AuthenticationConfigurationClusterTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, AuthenticationOnNotActiveClusterTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNodeRestartTest.class, ignoredTests);

//        GridTestUtils.addTestIfNeeded(suite, AuthenticationProcessorNPEOnStartTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, Authentication1kUsersNodeRestartTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheDataRegionConfigurationTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, WalModeChangeAdvancedSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalModeChangeSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalModeChangeCoordinatorNotAffinityNodeSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, Cache64kPartitionsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingPartitionCountersTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingWithAsyncClearingTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheAssignmentNodeRestartsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackAsyncWithPersistenceTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheGroupMetricsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CacheMetricsManageTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageEvictionMultinodeMixedRegionsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartFailWithPersistenceTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxWithSmallTimeoutAndContentionOneKeyTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheRentingStateRepairTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TransactionIntegrityWithPrimaryIndexCorruptionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CacheDataLossOnPartitionMoveTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheConfigurationSerializationOnDiscoveryTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CacheConfigurationSerializationOnExchangeTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CachePartitionLostWhileClearingTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxCrossCacheMapOnInvalidTopologyTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxCrossCacheRemoteMultiplePartitionReservationTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, GridTransactionsSystemUserTimeMetricsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, SafeLogTxFinishErrorTest.class, ignoredTests);

        return suite;
    }
}
