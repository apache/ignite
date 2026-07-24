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
import org.apache.ignite.cache.affinity.PendingExchangeTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityCoordinatorInitTest;
import org.apache.ignite.internal.processors.cache.CacheIgniteOutOfMemoryExceptionTest;
import org.apache.ignite.internal.processors.cache.CacheNoAffinityExchangeTest;
import org.apache.ignite.internal.processors.cache.ClientFastReplyCoordinatorFailureTest;
import org.apache.ignite.internal.processors.cache.IgniteOutOfMemoryPropagationTest;
import org.apache.ignite.internal.processors.cache.PartitionedAtomicCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedTransactionalPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionsExchangeCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.SysCacheInconsistencyInternalKeyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteExchangeLatchManagerCoordinatorFailTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheClientsConcurrentStartTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheExchangeMergeTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionLossWithRestartsTest;
import org.apache.ignite.internal.processors.cache.distributed.ExchangeMergeStaleServerNodesTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionEvictionDuringReadThroughSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientMultiNodeUpdateTopologyLockTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheThreadLocalTxTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteOptimisticTxSuspendResumeTest;
import org.apache.ignite.internal.processors.cache.distributed.OnePhaseCommitAndNodeLeftTest;
import org.apache.ignite.internal.processors.cache.distributed.PartitionsExchangeAwareTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingOrderingTest;
import org.apache.ignite.internal.processors.cache.transactions.StartImplicitlyTxOnStopCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionContextCleanupTest;
import org.apache.ignite.internal.processors.cache.transactions.TxLabelTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackAsyncNearCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackAsyncTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTimeoutTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.transactions.TxSavepointItTest;
import org.apache.ignite.internal.processors.cache.transactions.TxTimeoutOnInitializationTest;
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

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionEvictionDuringReadThroughSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOptimisticTxSuspendResumeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheExchangeMergeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OnePhaseCommitAndNodeLeftTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PendingExchangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ExchangeMergeStaleServerNodesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClientFastReplyCoordinatorFailureTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheThreadLocalTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackAsyncTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackAsyncNearCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTopologyChangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxTimeoutOnInitializationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxLabelTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxSavepointItTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteOutOfMemoryPropagationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheIgniteOutOfMemoryExceptionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PartitionedAtomicCacheGetsDistributionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionedTransactionalPessimisticCacheGetsDistributionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteExchangeLatchManagerCoordinatorFailTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PartitionsExchangeCoordinatorFailoverTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheNoAffinityExchangeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CachePartitionLossWithRestartsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheClientsConcurrentStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingOrderingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClientMultiNodeUpdateTopologyLockTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PartitionsExchangeAwareTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, SysCacheInconsistencyInternalKeyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, StartImplicitlyTxOnStopCacheTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TransactionContextCleanupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheAffinityCoordinatorInitTest.class, ignoredTests);

        return suite;
    }
}
