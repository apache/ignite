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
import org.apache.ignite.internal.processors.cache.PartitionedTransactionalOptimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedAtomicCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedTransactionalOptimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedTransactionalPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheExchangeMergeMdcTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheParallelStartTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheTryLockMultithreadedTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMultiClientsStartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgnitePessimisticTxSuspendResumeTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManagerTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.IgniteExchangeLatchManagerDiscoHistoryTest;
import org.apache.ignite.internal.processors.cache.transactions.TxMultiCacheAsyncOpsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOnCachesStartTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOnCachesStopTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticOnPartitionExchangeTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticPrepareOnUnstableTopologyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticReadThroughTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackDuringPreparingTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnIncorrectParamsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnMapOnInvalidTopologyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTimeoutNearCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTimeoutNoDeadlockDetectionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTimeoutOnePhaseCommitTest;
import org.apache.ignite.internal.processors.cache.transactions.TxStateChangeEventTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Split off from {@link IgniteCacheTestSuite6} to reduce the single-suite runtime in CI;
 * contains an independent subset of the same test classes.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite16 {
    /**
     * @return Test suite.
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

        GridTestUtils.addTestIfNeeded(suite, IgnitePessimisticTxSuspendResumeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheExchangeMergeMdcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutNoDeadlockDetectionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutNearCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnTimeoutOnePhaseCommitTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxOptimisticPrepareOnUnstableTopologyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnIncorrectParamsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxStateChangeEventTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxMultiCacheAsyncOpsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxOnCachesStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxOnCachesStopTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheMultiClientsStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ReplicatedAtomicCacheGetsDistributionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ReplicatedTransactionalOptimisticCacheGetsDistributionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ReplicatedTransactionalPessimisticCacheGetsDistributionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionedTransactionalOptimisticCacheGetsDistributionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxOptimisticOnPartitionExchangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxOptimisticReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteExchangeLatchManagerDiscoHistoryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ExchangeLatchManagerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTryLockMultithreadedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheParallelStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackOnMapOnInvalidTopologyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRollbackDuringPreparingTest.class, ignoredTests);

        return suite;
    }
}
