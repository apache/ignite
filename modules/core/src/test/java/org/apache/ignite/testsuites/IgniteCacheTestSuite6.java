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
import org.apache.ignite.internal.processors.cache.datastructures.IgniteExchangeLatchManagerCoordinatorFailTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite6 {
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

//        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionEvictionDuringReadThroughSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteOptimisticTxSuspendResumeTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePessimisticTxSuspendResumeTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheExchangeMergeTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PendingExchangeTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ExchangeMergeStaleServerNodesTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ClientFastReplyCoordinatorFailureTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutNoDeadlockDetectionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutNearCacheTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteCacheThreadLocalTxTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackAsyncTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackAsyncNearCacheTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTopologyChangeTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutOnePhaseCommitTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxOptimisticPrepareOnUnstableTopologyTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxLabelTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnIncorrectParamsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxStateChangeEventTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxMultiCacheAsyncOpsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxOnCachesStartTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TxOnCachesStopTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteCacheMultiClientsStartTest.class, ignoredTests);
//
////        TODO enable this test after IGNITE-6753, now it takes too long
////        GridTestUtils.addTestIfNeeded(suite, IgniteOutOfMemoryPropagationTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CacheIgniteOutOfMemoryExceptionTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, ReplicatedAtomicCacheGetsDistributionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ReplicatedTransactionalOptimisticCacheGetsDistributionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ReplicatedTransactionalPessimisticCacheGetsDistributionTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, PartitionedAtomicCacheGetsDistributionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionedTransactionalOptimisticCacheGetsDistributionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionedTransactionalPessimisticCacheGetsDistributionTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxOptimisticOnPartitionExchangeTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxOptimisticReadThroughTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteExchangeLatchManagerCoordinatorFailTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteExchangeLatchManagerDiscoHistoryTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ExchangeLatchManagerTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, PartitionsExchangeCoordinatorFailoverTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CacheTryLockMultithreadedTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheParallelStartTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheNoAffinityExchangeTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CachePartitionLossWithRestartsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, TxLocalDhtMixedCacheModesTest.class, ignoredTests);

        //GridTestUtils.addTestIfNeeded(suite, CacheClientsConcurrentStartTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingOrderingTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, IgniteCacheClientMultiNodeUpdateTopologyLockTest.class, ignoredTests);

//        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnMapOnInvalidTopologyTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, PartitionsExchangeAwareTest.class, ignoredTests);

        return suite;
    }
}
